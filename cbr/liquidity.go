package cbr

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// Ликвидность банковского сектора ЦБ — две HTML-формы UniDbQuery:
// дневной ряд (bliquidity) и периоды усреднения (BSliquidityMA). Обе таблицы —
// .table-wrapper table.data с иерархическим заголовком (colspan/rowspan), поэтому
// util.ParseHTMLTable заголовки не раскрывает: привязка значений идёт по индексу
// колонки, а имена рядов заданы кодом. Разведка — research/2026-11-14-cbr-html-tables-recon.md §3.
const (
	cbrLiquidityTable  = "cbr_liquidity"
	cbrLiquidityInsert = "INSERT INTO " + cbrLiquidityTable

	// cbrLiquidityDailyUrl и cbrLiquidityAvgUrl — страницы форм; окно дописывает
	// util.UniDbQueryURL.
	cbrLiquidityDailyUrl = "https://www.cbr.ru/hd_base/bliquidity/"
	cbrLiquidityAvgUrl   = "https://www.cbr.ru/hd_base/BSliquidityMA/"

	// Начало рядов: дневной — 01.01.2014 (первая дата, которую отдаёт форма на пробу
	// 01.01.2014..31.01.2014), периоды усреднения — 10.01.2018.
	cbrLiquidityDailyFrom = "2014-01-01"
	cbrLiquidityAvgFrom   = "2018-01-10"

	// cbrLiquidityChunkPause — пауза между окнами: серия запросов к одной форме не должна
	// упираться в троттлинг ЦБ. Длина окна — общая с КБД (cbrZcycChunkYears, 4 года):
	// у форм ЦБ один и тот же лимит диапазона, и zcycChunks уже режет бэкфилл.
	cbrLiquidityChunkPause = 500 * time.Millisecond

	cbrLiquidityTableSelector = "table.data"
	cbrLiquidityTimeLayout    = "02.01.2006"

	// Виды рядов: дневной срез и периоды усреднения одной формой DDL.
	cbrLiquidityKindDaily = "daily"
	cbrLiquidityKindAvg   = "averaging"
)

// cbrLiquidityDdl — канонический DDL. Ключ (kind, series, date): дату дневного ряда и
// дату первого дня периода усреднения нельзя различить без вида.
const cbrLiquidityDdl = "CREATE TABLE IF NOT EXISTS %s (" +
	" kind LowCardinality(String)," +
	" series LowCardinality(String)," +
	" date Date," +
	" value Float64" +
	") ENGINE = ReplacingMergeTree ORDER BY (kind, series, date);"

// cbrLiquidityDailySeries — колонки дневной таблицы после «Даты», в порядке формы (14).
// Заголовок формы иерархический, поэтому имена заданы кодом: «дефицит (+) / профицит (-)»
// — знак из заголовка колонки 1.
var cbrLiquidityDailySeries = []string{
	"deficit",
	"deficit_ex_corr",
	"cb_claims",
	"cb_claims_auction_repo",
	"cb_claims_auction_loans",
	"cb_claims_standing_repo",
	"cb_claims_standing_loans",
	"cb_liabilities",
	"cb_liabilities_deposit_auction",
	"cb_liabilities_deposit_standing",
	"cb_liabilities_kobr",
	"nonstandard_repo",
	"corr_accounts",
	"required_reserves",
}

// cbrLiquidityAvgSeries — колонки таблицы периодов усреднения после даты (11). Колонки
// 9–10 — базисные пункты, остальные — млрд руб.
var cbrLiquidityAvgSeries = []string{
	"corr_accounts_avg",
	"required_reserves_special",
	"structural_deficit",
	"liquidity_need",
	"absorbed_auctions",
	"provided_auctions",
	"standing_net",
	"special_net",
	"key_rate_change_bp",
	"ruonia_spread_bp",
	"ruonia_debt",
}

// cbrLiquidityPoint — одна точка ряда: вид (daily/averaging), имя ряда, дата и значение.
type cbrLiquidityPoint struct {
	kind   string
	series string
	date   time.Time
	value  float64
}

// cbrLiquidityValue разбирает значение ячейки формы: пробелы (включая неразрывный и
// узкий) — разделители тысяч, запятая — десятичный разделитель, прочерк — отсутствие
// значения (не ноль: КОБР и ДМ публикуются не каждый день).
func cbrLiquidityValue(raw string) (float64, bool, error) {
	trimmed := strings.TrimSpace(raw)
	switch trimmed {
	case "", "—", "–", "-", "x", "х":
		return 0, false, nil
	}
	normalized := strings.NewReplacer(" ", "", " ", "", " ", "").Replace(trimmed)
	normalized = strings.ReplaceAll(normalized, ",", ".")
	value, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0, false, fmt.Errorf("не число: %w", err)
	}
	return value, true, nil
}

// cbrLiquidityRows разбирает страницу формы в точки. Нет таблицы, но страница — форма
// UniDbQuery с пустым результатом — nil, nil (штатный пустой диапазон); сломанная
// страница/WAF — ошибка. Короткая строка — ошибка разметки: молча сдвинутые колонки
// записали бы дефицит в обязательства.
func cbrLiquidityRows(body []byte, kind string, series []string) ([]cbrLiquidityPoint, error) {
	table, err := util.ParseHTMLTable(body, cbrLiquidityTableSelector)
	if err != nil {
		if errors.Is(err, util.ErrHTMLTableNotFound) && util.IsUniDbQueryPage(body) {
			return nil, nil
		}
		return nil, fmt.Errorf("cbr_liquidity: %w", err)
	}
	points := make([]cbrLiquidityPoint, 0, len(table.Rows)*len(series))
	for _, row := range table.Rows {
		if len(row) == 0 {
			continue
		}
		rawDate := strings.TrimSpace(row[0])
		if rawDate == "" {
			continue
		}
		date, err := time.Parse(cbrLiquidityTimeLayout, rawDate)
		if err != nil {
			return nil, fmt.Errorf("cbr_liquidity: дата %q: %w", rawDate, err)
		}
		if len(row) < len(series)+1 {
			return nil, fmt.Errorf("cbr_liquidity: в строке %s %d ячеек, ожидалось не меньше %d — разметка формы изменилась", rawDate, len(row), len(series)+1)
		}
		for i, name := range series {
			value, ok, err := cbrLiquidityValue(row[i+1])
			if err != nil {
				return nil, fmt.Errorf("cbr_liquidity: значение %q (%s, %s): %w", row[i+1], rawDate, name, err)
			}
			if !ok {
				continue
			}
			points = append(points, cbrLiquidityPoint{kind: kind, series: name, date: date, value: value})
		}
	}
	return points, nil
}

// cbrLiquiditySources — обе формы с их началом истории и набором колонок.
var cbrLiquiditySources = []struct {
	kind   string
	url    string
	from   string
	series []string
}{
	{kind: cbrLiquidityKindDaily, url: cbrLiquidityDailyUrl, from: cbrLiquidityDailyFrom, series: cbrLiquidityDailySeries},
	{kind: cbrLiquidityKindAvg, url: cbrLiquidityAvgUrl, from: cbrLiquidityAvgFrom, series: cbrLiquidityAvgSeries},
}

// cbrLiquidityCraw — инкрементальный импорт обеих форм: для каждого вида max(date) своей
// таблицы → окна по cbrLiquidityChunkYears → один батч. Все окна разбираются до первого
// PrepareBatch: пустое историческое окно не должно записать частичный бэкфилл. Точки вне
// запрошенного окна отбрасываются — ЦБ на пустой/будущий диапазон отдаёт последнюю строку.
func cbrLiquidityCraw(_ string, conn driver.Conn) (count int64, err error) {
	var all []cbrLiquidityPoint
	for _, source := range cbrLiquiditySources {
		var last time.Time
		if err = conn.QueryRow(ctx, "SELECT max(date) FROM "+cbrLiquidityTable+" FINAL WHERE kind = ?", source.kind).Scan(&last); err != nil {
			return 0, fmt.Errorf("%s: max(date) вида %s: %w", cbrLiquidityTable, source.kind, err)
		}
		last = zcycDateUTC(last)
		historyFrom, err := time.Parse("2006-01-02", source.from)
		if err != nil {
			return 0, fmt.Errorf("%s: начало истории %q: %w", cbrLiquidityTable, source.from, err)
		}
		firstRun := last.Before(historyFrom)
		start := historyFrom
		if !firstRun {
			start = last.AddDate(0, 0, 1)
		}
		to := zcycDateUTC(time.Now())
		if start.After(to) {
			log.Infof("%s: вид %s — новых данных нет", cbrLiquidityTable, source.kind)
			continue
		}
		chunks := zcycChunks(start, to)
		pointsOfKind := 0
		for i, chunk := range chunks {
			if i > 0 {
				time.Sleep(cbrLiquidityChunkPause)
			}
			begin, end := chunk[0], chunk[1]
			window := begin.Format("2006-01-02") + ".." + end.Format("2006-01-02")
			url, err := util.UniDbQueryURL(source.url, begin, end)
			if err != nil {
				return 0, fmt.Errorf("%s: окно %s: %w", cbrLiquidityTable, window, err)
			}
			page, err := util.GetPage(url)
			if err != nil {
				return 0, fmt.Errorf("%s: окно %s, %s: %w", cbrLiquidityTable, window, url, err)
			}
			points, err := cbrLiquidityRows(page, source.kind, source.series)
			if err != nil {
				return 0, fmt.Errorf("%s: окно %s, %s: %w", cbrLiquidityTable, window, url, err)
			}
			kept := cbrLiquidityPointsInRange(points, begin, end)
			if len(kept) == 0 && i < len(chunks)-1 {
				return 0, fmt.Errorf("%s: пустое историческое окно %s (%s): нет точек в диапазоне", cbrLiquidityTable, window, url)
			}
			pointsOfKind += len(kept)
			all = append(all, kept...)
		}
		if firstRun && pointsOfKind == 0 {
			return 0, fmt.Errorf("%s: пустой бэкфилл вида %s за %s..%s", cbrLiquidityTable, source.kind,
				chunks[0][0].Format("2006-01-02"), chunks[len(chunks)-1][1].Format("2006-01-02"))
		}
	}
	if len(all) == 0 {
		log.Infof("%s: новых данных нет", cbrLiquidityTable)
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, cbrLiquidityInsert)
	if err != nil {
		return 0, err
	}
	for _, point := range all {
		if err = batch.Append(point.kind, point.series, point.date, point.value); err != nil {
			if abortErr := batch.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки Append: %v", cbrLiquidityTable, abortErr)
			}
			return 0, fmt.Errorf("%s: Append (%s, %s, %s): %w", cbrLiquidityTable, point.kind, point.series,
				point.date.Format("2006-01-02"), err)
		}
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("%s: %d точек", cbrLiquidityTable, count)
	return count, nil
}

// cbrLiquidityPointsInRange оставляет точки окна [from, to] включительно: ЦБ на пустой
// диапазон отдаёт последнюю доступную строку, и она не является точкой этого окна.
func cbrLiquidityPointsInRange(points []cbrLiquidityPoint, from, to time.Time) []cbrLiquidityPoint {
	var kept []cbrLiquidityPoint
	for _, point := range points {
		if point.date.Before(from) || point.date.After(to) {
			continue
		}
		kept = append(kept, point)
	}
	return kept
}

type cbrLiquidity struct {
	util.ClickHouseImport
}

// cbrLiquidityImporter — ликвидность банковского сектора; регистрируется реестром
// cbr/importers.go. Точки контроля публикации нет: URL каждого окна содержит сегодняшнюю
// дату (как у cbr_zcyc, cbr_key_rate, cbr_ruania).
var cbrLiquidityImporter = &cbrLiquidity{ClickHouseImport: util.ClickHouseImport{
	TableName:   cbrLiquidityTable,
	CreateTable: []string{cbrLiquidityDdl},
	DataUrl:     cbrLiquidityDailyUrl,
	CrawFunc:    cbrLiquidityCraw,
}}
