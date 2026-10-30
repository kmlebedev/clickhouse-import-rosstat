// Package transaq агрегирует рыночную ленту Transaq (таблицы transaq_* пишет
// внешний сервис transaq-clickhouse-exporter) в производные витрины проекта.
package transaq

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	transaqNetFlowsTable = "transaq_net_flows"
	// Окно пересчёта: бакет пересчитывается, если в него могли прийти поздние
	// сделки или дубли до background merge. Повтор — замена версии по loaded_at.
	transaqNetFlowsLookback = 15 * time.Minute
	// Чанк агрегации ограничивает число бакетов в одном запросе и батче:
	// обычный прогон — окно 15 минут = один чанк, догон после простоя режется по часу.
	transaqNetFlowsChunk = time.Hour
	// Контекст для lagInFrame: сделка на границе чанка должна видеть предыдущую,
	// иначе флаг разрыва потеряется на стыке.
	transaqNetFlowsScanLookback = 10 * time.Minute
	// Предупреждение о тяжёлом первом прогоне (спека §3.5).
	transaqNetFlowsLargeLenta = 50_000_000
)

// Биржевое время: в таблицах transaq_* и в бакетах — Europe/Moscow.
var moscowLocation = time.FixedZone("MSK", 3*60*60)

// transaqNetFlowsDdl — канон ARCHITECTURE.md §6.2. Ключ (sec_code, board, minute)
// совпадает с ключом витрины: бакет — единица пересчёта.
const transaqNetFlowsDdl = `CREATE TABLE IF NOT EXISTS %s (
    minute DateTime('Europe/Moscow'),
    sec_code LowCardinality(String),
    board LowCardinality(String),
    buy_lots UInt64,
    sell_lots UInt64,
    buy_value_rub Float64,
    sell_value_rub Float64,
    trade_count UInt32,
    gap_suspected UInt8,
    loaded_at DateTime('Europe/Moscow')
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (sec_code, board, minute)`

// readLentaBoundsQuery — границы доступной ленты и её размер: по размеру
// решается только предупреждение о тяжёлом первом прогоне.
const readLentaBoundsQuery = `SELECT count(), min(time), max(time) FROM transaq_trades`

// readWatermarkQuery — последняя посчитанная минута. count() отличает пустую
// таблицу от настоящего 1970-01-01 в max(): у пустой агрегат отдаёт дефолт.
const readWatermarkQuery = `SELECT count(), max(minute) FROM transaq_net_flows`

// readNetFlowBucketsQuery агрегирует окно в ClickHouse: лента не течёт в Go
// построчно. Порядок подстановок — scanFrom, scanTo, from, to.
//
// dedup — дубли до background merge: полный ключ сортировки transaq_trades с
// полезной нагрузкой, любая версия строки пригодна.
// securities_lots — lotsize из снимочного каталога; FINAL снимает версии, max
// сводит несколько классов одного (seccode, board).
// gap_suspected — эвристика разрыва: пауза больше 10 минут между соседними
// сделками внутри 10:00–19:00 МСК помечает бакет, в котором торг возобновился.
const readNetFlowBucketsQuery = `
WITH dedup AS (
    SELECT secid, board, sec_code, trade_no, time, buy_sell, price, quantity
    FROM transaq_trades
    WHERE time >= ? AND time < ?
    GROUP BY secid, board, sec_code, trade_no, time, buy_sell, price, quantity
),
securities_lots AS (
    SELECT seccode, board, max(lotsize) AS lotsize
    FROM transaq_securities FINAL
    GROUP BY seccode, board
),
priced AS (
    SELECT d.sec_code AS sec_code,
           d.board AS board,
           d.time AS time,
           d.buy_sell AS buy_sell,
           toFloat64(d.price) AS price,
           toUInt64(d.quantity) AS lots,
           if(l.lotsize = 0, 0, toFloat64(l.lotsize)) AS lotsize,
           lagInFrame(d.time, 1, d.time) OVER (PARTITION BY d.sec_code, d.board ORDER BY d.time) AS prev_time
    FROM dedup AS d
    LEFT JOIN securities_lots AS l ON l.seccode = d.sec_code AND l.board = d.board
)
SELECT toStartOfMinute(time) AS minute,
       toString(sec_code) AS sec_code,
       board,
       toUInt64(sumIf(lots, buy_sell = 'B')) AS buy_lots,
       toUInt64(sumIf(lots, buy_sell = 'S')) AS sell_lots,
       sumIf(price * lots * lotsize, buy_sell = 'B') AS buy_value_rub,
       sumIf(price * lots * lotsize, buy_sell = 'S') AS sell_value_rub,
       toUInt32(count()) AS trade_count,
       toUInt8(max(multiIf(time - prev_time > 600 AND toHour(time) BETWEEN 10 AND 18, 1, 0))) AS gap_suspected
FROM priced
WHERE time >= ? AND time < ?
GROUP BY minute, sec_code, board
ORDER BY minute, sec_code, board`

// transaqNetFlows — минутные бакеты покупок/продаж из ленты публичных сделок.
type transaqNetFlows struct{}

func (s *transaqNetFlows) Name() string { return transaqNetFlowsTable }

// netFlowWindow — полуинтервал [From, To): границы в секундах биржевого времени.
type netFlowWindow struct {
	From time.Time
	To   time.Time
}

// netFlowRow — строка бакета до записи.
type netFlowRow struct {
	Minute       time.Time
	SecCode      string
	Board        string
	BuyLots      uint64
	SellLots     uint64
	BuyValueRub  float64
	SellValueRub float64
	TradeCount   uint32
	GapSuspected uint8
}

// planNetFlowsWindows режет окно на полуинтервалы не длиннее chunk. Пустое окно
// (from не раньше to) — nil: это no-op, а не ошибка прогона.
func planNetFlowsWindows(from, to time.Time, chunk time.Duration) []netFlowWindow {
	if chunk <= 0 {
		chunk = transaqNetFlowsChunk
	}
	if !from.Before(to) {
		return nil
	}
	windows := make([]netFlowWindow, 0, int(to.Sub(from)/chunk)+1)
	for start := from; start.Before(to); start = start.Add(chunk) {
		end := start.Add(chunk)
		if end.After(to) {
			end = to
		}
		windows = append(windows, netFlowWindow{From: start, To: end})
	}
	return windows
}

// netFlowsFrom — начало окна пересчёта: watermark минус запас на поздние сделки,
// но не раньше начала доступной ленты. Без watermark (первый прогон) — вся лента.
func netFlowsFrom(watermark, lentaMin time.Time, hasWatermark bool) time.Time {
	if !hasWatermark {
		return lentaMin
	}
	from := watermark.Add(-transaqNetFlowsLookback)
	if from.Before(lentaMin) {
		return lentaMin
	}
	return from
}

func (s *transaqNetFlows) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	if err := conn.Exec(ctx, fmt.Sprintf(transaqNetFlowsDdl, transaqNetFlowsTable)); err != nil {
		return 0, fmt.Errorf("%s: create table: %w", transaqNetFlowsTable, err)
	}

	lentaCount, lentaMin, lentaMax, err := readLentaBounds(ctx, conn)
	if err != nil {
		return 0, err
	}
	if lentaCount == 0 {
		// Пустая лента — штатное состояние вне сессии и без подписки; это не ошибка.
		log.Infof("%s: transaq_trades пуста — no-op", transaqNetFlowsTable)
		return 0, s.publishViews(ctx, conn, 0)
	}
	if lentaCount > transaqNetFlowsLargeLenta {
		log.Warnf("%s: в ленте %d строк (>%d): первый прогон может быть тяжёлым",
			transaqNetFlowsTable, lentaCount, transaqNetFlowsLargeLenta)
	}

	watermark, hasWatermark, err := readWatermark(ctx, conn)
	if err != nil {
		return 0, err
	}
	from := netFlowsFrom(watermark, lentaMin, hasWatermark)
	windows := planNetFlowsWindows(from, lentaMax.Add(time.Second), transaqNetFlowsChunk)
	if len(windows) == 0 {
		log.Infof("%s: сделок новее %s нет — no-op", transaqNetFlowsTable, from.Format(time.DateTime))
		return 0, s.publishViews(ctx, conn, 0)
	}

	loadedAt := time.Now().In(moscowLocation)
	var total int64
	for _, window := range windows {
		rows, err := readNetFlowBuckets(ctx, conn, window)
		if err != nil {
			return total, err
		}
		if len(rows) == 0 {
			continue
		}
		if err = writeNetFlowBuckets(ctx, conn, rows, loadedAt); err != nil {
			return total, err
		}
		total += int64(len(rows))
	}

	if err = s.publishViews(ctx, conn, total); err != nil {
		return total, err
	}
	return total, nil
}

func readLentaBounds(ctx context.Context, conn driver.Conn) (count uint64, from, to time.Time, err error) {
	row := conn.QueryRow(ctx, readLentaBoundsQuery)
	if err = row.Scan(&count, &from, &to); err != nil {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("%s: read lenta bounds: %w", transaqNetFlowsTable, err)
	}
	return count, from, to, nil
}

func readWatermark(ctx context.Context, conn driver.Conn) (watermark time.Time, ok bool, err error) {
	var count uint64
	row := conn.QueryRow(ctx, readWatermarkQuery)
	if err = row.Scan(&count, &watermark); err != nil {
		return time.Time{}, false, fmt.Errorf("%s: read watermark: %w", transaqNetFlowsTable, err)
	}
	return watermark, count > 0, nil
}

func readNetFlowBuckets(ctx context.Context, conn driver.Conn, window netFlowWindow) ([]netFlowRow, error) {
	rows, err := conn.Query(ctx, readNetFlowBucketsQuery,
		window.From.Add(-transaqNetFlowsScanLookback), window.To, window.From, window.To)
	if err != nil {
		return nil, fmt.Errorf("%s: read buckets %s…%s: %w", transaqNetFlowsTable,
			window.From.Format(time.DateTime), window.To.Format(time.DateTime), err)
	}
	defer func() { _ = rows.Close() }()

	var result []netFlowRow
	for rows.Next() {
		var row netFlowRow
		if err = rows.Scan(&row.Minute, &row.SecCode, &row.Board, &row.BuyLots, &row.SellLots,
			&row.BuyValueRub, &row.SellValueRub, &row.TradeCount, &row.GapSuspected); err != nil {
			return nil, fmt.Errorf("%s: scan bucket: %w", transaqNetFlowsTable, err)
		}
		result = append(result, row)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: read buckets rows: %w", transaqNetFlowsTable, err)
	}
	return result, nil
}

// writeNetFlowBuckets пишет один чанк одним батчем: при обычном прогоне чанк
// один, батч тоже. Abort обязателен на путях до Send — неотправленный батч
// держит соединение вне пула драйвера.
func writeNetFlowBuckets(ctx context.Context, conn driver.Conn, rows []netFlowRow, loadedAt time.Time) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+transaqNetFlowsTable)
	if err != nil {
		return fmt.Errorf("%s: prepare batch: %w", transaqNetFlowsTable, err)
	}
	for _, row := range rows {
		if err = batch.Append(row.Minute, row.SecCode, row.Board, row.BuyLots, row.SellLots,
			row.BuyValueRub, row.SellValueRub, row.TradeCount, row.GapSuspected, loadedAt); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("%s: append %s/%s/%s: %w", transaqNetFlowsTable,
				row.SecCode, row.Board, row.Minute.Format(time.DateTime), err)
		}
	}
	if err = batch.Send(); err != nil {
		return fmt.Errorf("%s: send batch: %w", transaqNetFlowsTable, err)
	}
	return nil
}

// publishViews публикует витрины и на no-op прогоне: витрина обязана существовать
// даже тогда, когда лента пуста (вне сессии), иначе MCP-агент её не увидит.
func (s *transaqNetFlows) publishViews(ctx context.Context, conn driver.Conn, rows int64) error {
	for _, view := range []util.View{transaqNetFlowsView, transaqNetFlowsDailyView} {
		created, err := util.CreateView(ctx, conn, view)
		if err != nil {
			return fmt.Errorf("%s: view %s: %w", transaqNetFlowsTable, view.Name, err)
		}
		if !created {
			log.Warnf("%s: view %s пропущена — нет исходной таблицы", transaqNetFlowsTable, view.Name)
			continue
		}
		log.Debugf("%s: view %s создана (строк в прогоне: %d)", transaqNetFlowsTable, view.Name, rows)
	}
	return nil
}
