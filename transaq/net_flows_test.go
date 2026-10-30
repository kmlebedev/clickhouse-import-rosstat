package transaq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func msk(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05", value, moscowLocation)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

// planNetFlowsWindows — границы окна: обычный прогон (15 минут) даёт один чанк,
// догон после простоя режется по часам без дыр и наложений на стыках.
func TestPlanNetFlowsWindows(t *testing.T) {
	tests := []struct {
		name   string
		from   string
		to     string
		chunk  time.Duration
		expect [][2]string
	}{
		{
			name:   "обычное окно 15 минут",
			from:   "2026-10-30 10:00:00",
			to:     "2026-10-30 10:15:00",
			chunk:  time.Hour,
			expect: [][2]string{{"2026-10-30 10:00:00", "2026-10-30 10:15:00"}},
		},
		{
			name:   "догон за 1 час 5 минут",
			from:   "2026-10-30 10:00:00",
			to:     "2026-10-30 11:05:00",
			chunk:  time.Hour,
			expect: [][2]string{{"2026-10-30 10:00:00", "2026-10-30 11:00:00"}, {"2026-10-30 11:00:00", "2026-10-30 11:05:00"}},
		},
		{
			name:   "ровно на границе чанка",
			from:   "2026-10-30 10:00:00",
			to:     "2026-10-30 11:00:00",
			chunk:  time.Hour,
			expect: [][2]string{{"2026-10-30 10:00:00", "2026-10-30 11:00:00"}},
		},
		{
			name:   "пустое окно — no-op",
			from:   "2026-10-30 10:00:00",
			to:     "2026-10-30 10:00:00",
			chunk:  time.Hour,
			expect: nil,
		},
		{
			name:   "нулевой chunk — час по умолчанию",
			from:   "2026-10-30 10:00:00",
			to:     "2026-10-30 10:30:00",
			chunk:  0,
			expect: [][2]string{{"2026-10-30 10:00:00", "2026-10-30 10:30:00"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			windows := planNetFlowsWindows(msk(t, test.from), msk(t, test.to), test.chunk)
			if len(windows) != len(test.expect) {
				t.Fatalf("окон %d, want %d: %+v", len(windows), len(test.expect), windows)
			}
			for i, want := range test.expect {
				if got := windows[i].From.Format(time.DateTime); got != want[0] {
					t.Errorf("окно %d: From = %s, want %s", i, got, want[0])
				}
				if got := windows[i].To.Format(time.DateTime); got != want[1] {
					t.Errorf("окно %d: To = %s, want %s", i, got, want[1])
				}
			}
		})
	}
}

// netFlowsFrom — начало пересчёта: watermark минус 15 минут, но не раньше
// доступной ленты; первый прогон берёт ленту целиком.
func TestNetFlowsFrom(t *testing.T) {
	lentaMin := msk(t, "2026-10-30 10:00:00")
	tests := []struct {
		name         string
		watermark    time.Time
		hasWatermark bool
		want         string
	}{
		{"первый прогон", time.Time{}, false, "2026-10-30 10:00:00"},
		{"watermark внутри ленты", msk(t, "2026-10-30 12:00:00"), true, "2026-10-30 11:45:00"},
		{"watermark у начала ленты", msk(t, "2026-10-30 10:05:00"), true, "2026-10-30 10:00:00"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := netFlowsFrom(test.watermark, lentaMin, test.hasWatermark)
			if formatted := got.Format(time.DateTime); formatted != test.want {
				t.Fatalf("from = %s, want %s", formatted, test.want)
			}
		})
	}
}

// Канон ARCHITECTURE.md §6.2: ключ бакета и версия строки — не косметика.
// Смена ключа ломает идемпотентность пересчёта, смена версии — замену бакета.
func TestTransaqNetFlowsDdlPinsCanonicalKey(t *testing.T) {
	for _, want := range []string{
		"minute DateTime('Europe/Moscow')",
		"sec_code LowCardinality(String)",
		"board LowCardinality(String)",
		"buy_lots UInt64",
		"sell_lots UInt64",
		"buy_value_rub Float64",
		"sell_value_rub Float64",
		"trade_count UInt32",
		"gap_suspected UInt8",
		"loaded_at DateTime('Europe/Moscow')",
		"ENGINE = ReplacingMergeTree(loaded_at)",
		"ORDER BY (sec_code, board, minute)",
	} {
		if !strings.Contains(transaqNetFlowsDdl, want) {
			t.Errorf("DDL transaq_net_flows обязан содержать %q: без него копия расходится с каноном ARCHITECTURE.md §6.2", want)
		}
	}
	if !strings.Contains(transaqNetFlowsDdl, "CREATE TABLE IF NOT EXISTS %s") {
		t.Error("DDL обязан быть в шаблоне с подстановкой имени: литерал без подстановки даёт мусор в имени таблицы")
	}
}

// Витрины — контракт для MCP-агента: нетто выводится со знаком (без переполнения
// UInt64), FINAL снимает версии, у каждой колонки есть комментарий.
func TestTransaqNetFlowsViews(t *testing.T) {
	if transaqNetFlowsView.Name != "v_transaq_net_flows" {
		t.Fatalf("Name = %q, want v_transaq_net_flows", transaqNetFlowsView.Name)
	}
	if transaqNetFlowsDailyView.Name != "v_transaq_net_flows_daily" {
		t.Fatalf("Name = %q, want v_transaq_net_flows_daily", transaqNetFlowsDailyView.Name)
	}
	for _, view := range []struct {
		name     string
		selected []string
	}{{
		name: transaqNetFlowsView.Name,
		selected: []string{"toInt64(buy_lots) - toInt64(sell_lots) AS net_lots",
			"buy_value_rub - sell_value_rub AS net_value_rub", "FROM transaq_net_flows FINAL"},
	}, {
		name: transaqNetFlowsDailyView.Name,
		selected: []string{"toDate(minute) AS date", "toUInt32(count()) AS minutes_with_trades",
			"min(minute) AS first_trade", "FROM transaq_net_flows FINAL"},
	}} {
		var sql string
		if view.name == transaqNetFlowsView.Name {
			sql = transaqNetFlowsView.Select
		} else {
			sql = transaqNetFlowsDailyView.Select
		}
		for _, fragment := range view.selected {
			if !strings.Contains(sql, fragment) {
				t.Errorf("%s: в Select нет %q", view.name, fragment)
			}
		}
		if strings.Contains(sql, "buy_lots - sell_lots") {
			t.Errorf("%s: нетто считается вычитанием UInt64 и переполняется на продажах; нужен toInt64", view.name)
		}
	}

	for column := range transaqNetFlowsView.Columns {
		if strings.TrimSpace(transaqNetFlowsView.Columns[column]) == "" {
			t.Errorf("колонка %s витрины %s без комментария", column, transaqNetFlowsView.Name)
		}
	}
	for _, column := range []string{"minute", "sec_code", "board", "buy_lots", "sell_lots",
		"net_lots", "buy_value_rub", "sell_value_rub", "net_value_rub", "trade_count",
		"gap_suspected", "loaded_at"} {
		if _, ok := transaqNetFlowsView.Columns[column]; !ok {
			t.Errorf("в комментариях %s нет колонки %q", transaqNetFlowsView.Name, column)
		}
	}
	for _, column := range []string{"date", "sec_code", "board", "buy_lots", "sell_lots",
		"net_lots", "buy_value_rub", "sell_value_rub", "net_value_rub", "trade_count",
		"minutes_with_trades", "gap_suspected", "first_trade", "last_trade"} {
		if _, ok := transaqNetFlowsDailyView.Columns[column]; !ok {
			t.Errorf("в комментариях %s нет колонки %q", transaqNetFlowsDailyView.Name, column)
		}
	}
}

// fakeConn отдаёт заранее собранные ответы на три запроса импортёра.
type fakeConn struct {
	driver.Conn
	lentaCount  uint64
	lentaFrom   time.Time
	lentaTo     time.Time
	watermark   *time.Time
	buckets     []netFlowRow
	batch       *fakeBatch
	prepares    int
	execs       int
	queryArgLen []int
}

func (c *fakeConn) Exec(context.Context, string, ...any) error {
	c.execs++
	return nil
}

func (c *fakeConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.prepares++
	return c.batch, nil
}

func (c *fakeConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	switch {
	case strings.Contains(query, "min(time)"):
		return &fakeRow{values: []any{c.lentaCount, c.lentaFrom, c.lentaTo}}
	case strings.Contains(query, "max(minute)"):
		if c.watermark == nil {
			return &fakeRow{values: []any{uint64(0), c.lentaFrom}}
		}
		return &fakeRow{values: []any{uint64(1), *c.watermark}}
	case strings.Contains(query, "system.tables"):
		return &fakeRow{values: []any{uint64(0)}}
	default:
		return &fakeRow{err: fmt.Errorf("неожиданный QueryRow: %s", query)}
	}
}

func (c *fakeConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	if !strings.Contains(query, "FROM transaq_trades") {
		return nil, fmt.Errorf("неожиданный Query: %s", query)
	}
	c.queryArgLen = append(c.queryArgLen, len(args))
	rows := make([][]any, 0, len(c.buckets))
	for _, bucket := range c.buckets {
		rows = append(rows, []any{bucket.Minute, bucket.SecCode, bucket.Board, bucket.BuyLots,
			bucket.SellLots, bucket.BuyValueRub, bucket.SellValueRub, bucket.TradeCount, bucket.GapSuspected})
	}
	return &fakeRows{data: rows}, nil
}

type fakeRow struct {
	values []any
	err    error
}

func (r *fakeRow) Err() error { return r.err }

func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i := range dest {
		if i >= len(r.values) {
			return fmt.Errorf("scan: нет значения %d", i)
		}
		if err := assign(dest[i], r.values[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeRow) ScanStruct(any) error { return errors.New("ScanStruct не поддержан") }

type fakeRows struct {
	driver.Rows
	data [][]any
	next int
	err  error
}

func (r *fakeRows) Next() bool {
	if r.next >= len(r.data) {
		return false
	}
	r.next++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.data[r.next-1]
	for i := range dest {
		if err := assign(dest[i], row[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return r.err }

func assign(dest, value any) error {
	switch target := dest.(type) {
	case *uint64:
		*target = value.(uint64)
	case *uint32:
		*target = value.(uint32)
	case *uint8:
		*target = value.(uint8)
	case *float64:
		*target = value.(float64)
	case *string:
		*target = value.(string)
	case *time.Time:
		*target = value.(time.Time)
	default:
		return fmt.Errorf("scan: неподдержанный приёмник %T", dest)
	}
	return nil
}

type fakeBatch struct {
	driver.Batch
	appended [][]any
	sends    int
	aborts   int
	appendEr error
	sendEr   error
}

func (b *fakeBatch) Append(values ...any) error {
	if b.appendEr != nil {
		return b.appendEr
	}
	b.appended = append(b.appended, values)
	return nil
}

func (b *fakeBatch) Send() error {
	b.sends++
	return b.sendEr
}

func (b *fakeBatch) Abort() error {
	b.aborts++
	return nil
}

// Пустая лента — штатное состояние вне сессии: прогон не ошибка и не гейт данных.
func TestImportNoOpOnEmptyLenta(t *testing.T) {
	conn := &fakeConn{batch: &fakeBatch{}}
	count, err := (&transaqNetFlows{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 0 {
		t.Fatalf("rows = %d, want 0", count)
	}
	if conn.prepares != 0 {
		t.Fatalf("при пустой ленте батч не должен открываться: prepares = %d", conn.prepares)
	}
}

// Нет сделок новее окна пересчёта — no-op, а не полный пересчёт и не ошибка.
func TestImportNoOpWhenNoNewTrades(t *testing.T) {
	watermark := msk(t, "2026-10-30 12:00:00")
	conn := &fakeConn{
		lentaCount: 10,
		lentaFrom:  msk(t, "2026-10-30 10:00:00"),
		lentaTo:    msk(t, "2026-10-30 11:00:00"),
		watermark:  &watermark,
		batch:      &fakeBatch{},
	}
	count, err := (&transaqNetFlows{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 0 || conn.prepares != 0 {
		t.Fatalf("rows = %d, prepares = %d; ожидался no-op", count, conn.prepares)
	}
}

// Бакеты пишутся одним батчем: значения доходят до Append без потерь, запрос
// агрегации получает четыре параметра окна (scan-from, scan-to, from, to).
func TestImportWritesBuckets(t *testing.T) {
	minute := msk(t, "2026-10-30 10:00:00")
	conn := &fakeConn{
		lentaCount: 2,
		lentaFrom:  minute,
		lentaTo:    minute.Add(3 * time.Minute),
		batch:      &fakeBatch{},
		buckets: []netFlowRow{{
			Minute: minute, SecCode: "SBER", Board: "TQBR",
			BuyLots: 110, SellLots: 40, BuyValueRub: 110000, SellValueRub: 40000,
			TradeCount: 3, GapSuspected: 0,
		}},
	}
	count, err := (&transaqNetFlows{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 1 {
		t.Fatalf("rows = %d, want 1", count)
	}
	if conn.batch.sends != 1 || len(conn.batch.appended) != 1 {
		t.Fatalf("sends = %d, appended = %d; ожидался один батч с одной строкой", conn.batch.sends, len(conn.batch.appended))
	}
	row := conn.batch.appended[0]
	if row[1] != "SBER" || row[2] != "TQBR" || row[3].(uint64) != 110 || row[4].(uint64) != 40 {
		t.Fatalf("первая строка батча собрана неверно: %+v", row)
	}
	if row[5].(float64) != 110000 || row[7].(uint32) != 3 {
		t.Fatalf("рублёвый объём или число сделок потерялись: %+v", row)
	}
	for _, argLen := range conn.queryArgLen {
		if argLen != 4 {
			t.Fatalf("запрос агрегации получил %d параметров, want 4", argLen)
		}
	}
}

// Сбой Append обязан закрыть батч: иначе соединение остаётся вне пула драйвера.
func TestImportAbortsBatchOnAppendError(t *testing.T) {
	minute := msk(t, "2026-10-30 10:00:00")
	conn := &fakeConn{
		lentaCount: 1,
		lentaFrom:  minute,
		lentaTo:    minute,
		batch:      &fakeBatch{appendEr: errors.New("append: сбой сериализации")},
		buckets: []netFlowRow{{
			Minute: minute, SecCode: "SBER", Board: "TQBR", BuyLots: 1, TradeCount: 1,
		}},
	}
	_, err := (&transaqNetFlows{}).Import(context.Background(), conn)
	if err == nil {
		t.Fatal("Import вернул nil, ожидалась ошибка Append")
	}
	if conn.batch.aborts != 1 {
		t.Fatalf("aborts = %d, want 1", conn.batch.aborts)
	}
	if conn.batch.sends != 0 {
		t.Fatalf("sends = %d, want 0: после ошибки Append батч не отправляется", conn.batch.sends)
	}
}
