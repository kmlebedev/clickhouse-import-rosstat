package bold

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

// Тесты импортёра офлайн: источник — httptest-сервер по фикстурам bold/testdata, база —
// фейковое соединение (прецедент — wgc/etf_test.go). Ни сети, ни ClickHouse здесь нет, поэтому
// пинятся все четыре вида контракта: пути и число запросов, гвардейские SQL, режим (инкремент
// или полная история) и состав обоих батчей.

// boldTestServer — сервер «путь эндпоинта → тело ответа». Он же счётчик обращений: контракт
// «не больше 4 запросов за прогон» держится именно им (и тестом TestBoldEtfFetchCountsRequests).
// Путь без фикстуры отдаёт 404: тест, зашедший не туда (например, полный режим в «-latest»),
// увидит это как ошибку импорта, а не как тихий успех.
type boldTestServer struct {
	srv    *httptest.Server
	mu     sync.Mutex
	paths  []string
	bodies map[string][]byte
}

func boldNewTestServer(t *testing.T, bodies map[string][]byte) *boldTestServer {
	t.Helper()
	server := &boldTestServer{bodies: bodies}
	server.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		server.mu.Lock()
		server.paths = append(server.paths, path)
		body, ok := server.bodies[path]
		server.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("нет фикстуры для " + path))
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.srv.Close)
	return server
}

// base — база в том же виде, что boldBaseURL: путями эндпоинтов её дополняет импортёр.
func (s *boldTestServer) base() string { return s.srv.URL + "/" }

// requested — обращения в порядке прихода: по ним проверяются и пути, и их число.
func (s *boldTestServer) requested() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.paths...)
}

func (s *boldTestServer) requestedSorted() []string {
	paths := s.requested()
	sort.Strings(paths)
	return paths
}

// boldLatestBodies и boldHistoryBodies — четыре ответа каждого режима: путь инкремента и путь
// полной истории различаются только эндпоинтами (три файла истории против трёх *-latest).
func boldLatestBodies(t *testing.T) map[string][]byte {
	t.Helper()
	return map[string][]byte{
		boldPathUnitsLatest: testFixture(t, "bold_units_latest.json"),
		boldPathAumLatest:   testFixture(t, "bold_aum_latest.json"),
		boldPathFlowsLatest: testFixture(t, "bold_flows_latest.json"),
		boldPathSummary:     testFixture(t, "bold_summary.json"),
	}
}

func boldHistoryBodies(t *testing.T) map[string][]byte {
	t.Helper()
	return map[string][]byte{
		boldPathUnits:   testFixture(t, "bold_units_history.json"),
		boldPathAum:     testFixture(t, "bold_aum_history.json"),
		boldPathFlows:   testFixture(t, "bold_flows_history.json"),
		boldPathSummary: testFixture(t, "bold_summary.json"),
	}
}

// boldAllBodies — все восемь ответов: полный прогон Import начинается с четырёх запросов
// «последнего дня» (они дают дату для решения no-op и для пер-фондового среза) и только при
// пропуске дней уходит на файлы истории — ещё четыре запроса.
func boldAllBodies(t *testing.T) map[string][]byte {
	t.Helper()
	bodies := boldLatestBodies(t)
	for path, body := range boldHistoryBodies(t) {
		bodies[path] = body
	}
	return bodies
}

// boldMutateHistory пересобирает тело файла истории из фикстуры. Отличие от testMutateJSON
// (правка полей на месте) — возможность удалять строки: проверки Ruling 5 строятся на дне,
// которого в одном из файлов нет.
func boldMutateHistory(t *testing.T, fixture string, fn func(data []map[string]any) []map[string]any) []byte {
	t.Helper()
	var doc struct {
		Version string           `json:"version"`
		Updated string           `json:"updated"`
		Data    []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(testFixture(t, fixture), &doc); err != nil {
		t.Fatalf("разбор фикстуры %s: %v", fixture, err)
	}
	doc.Data = fn(doc.Data)
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("сборка тела из %s: %v", fixture, err)
	}
	return body
}

// boldMutateLatestDate ставит ответу «последнего дня» другую дату: так проверяется сверка дат
// трёх файлов (Review Focus 2).
func boldMutateLatestDate(t *testing.T, fixture, date string) []byte {
	t.Helper()
	var row map[string]any
	if err := json.Unmarshal(testFixture(t, fixture), &row); err != nil {
		t.Fatalf("разбор фикстуры %s: %v", fixture, err)
	}
	row["date"] = date
	body, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("сборка тела из %s: %v", fixture, err)
	}
	return body
}

// boldFakeRow — ответ maxOrNull: NULL (нулевая дата или отсутствие значения) либо значение.
// Scan проверяет тип приёмника и только его: NULL обязан приезжать в указатель, и попытка
// сканировать его в time.Time / float64 — ошибка теста, а не тихий ноль (прецедент — etfFakeRow
// в wgc/etf_test.go: именно здесь лежала эвристика «пустая таблица — год 1970»).
type boldFakeRow struct {
	maxDate *time.Time
	value   *float64
	err     error
}

func (r boldFakeRow) Err() error { return r.err }

func (r boldFakeRow) ScanStruct(any) error {
	return errors.New("фейковая строка: ScanStruct не используется")
}

func (r boldFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("фейковая строка: ожидался один приёмник, получено %d", len(dest))
	}
	switch d := dest[0].(type) {
	case **time.Time:
		*d = r.maxDate
	case **float64:
		*d = r.value
	default:
		return fmt.Errorf("фейковая строка: приёмник %T не поддержан (NULL сканируется только в указатели)", dest[0])
	}
	return nil
}

func boldFakeDate(t *testing.T, date string) driver.Row {
	t.Helper()
	d := testDate(t, date)
	return boldFakeRow{maxDate: &d}
}

func boldFakeNull() driver.Row { return boldFakeRow{} }

func boldFakeValue(v float64) driver.Row { return boldFakeRow{value: &v} }

func boldFakeError(message string) driver.Row { return boldFakeRow{err: errors.New(message)} }

// boldFakeBatch — батч в память: тест проверяет состав и число вставленных строк.
type boldFakeBatch struct {
	driver.Batch
	query string
	rows  [][]any
	sent  bool
}

func (b *boldFakeBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}

func (b *boldFakeBatch) Send() error { b.sent = true; return nil }

func (b *boldFakeBatch) Abort() error { return nil }

// boldFakeConn — фейковое соединение: встраивание driver.Conn оставляет неиспользуемые методы
// nil (паника, если тест зайдёт дальше сценария), а переопределены Exec, QueryRow и PrepareBatch.
// rows — очередь ответов QueryRow: пустая очередь отдаёт NULL, то есть «таблицы пусты».
type boldFakeConn struct {
	driver.Conn
	mu      sync.Mutex
	calls   []string
	rows    []driver.Row
	batches []*boldFakeBatch
}

func newBoldFakeConn(rows ...driver.Row) *boldFakeConn {
	return &boldFakeConn{rows: rows}
}

func (c *boldFakeConn) record(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *boldFakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.record("exec: " + query)
	return nil
}

func (c *boldFakeConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	c.record("queryrow: " + query)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.rows) == 0 {
		return boldFakeNull()
	}
	row := c.rows[0]
	c.rows = c.rows[1:]
	return row
}

func (c *boldFakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.record("preparebatch: " + query)
	batch := &boldFakeBatch{query: query}
	c.mu.Lock()
	c.batches = append(c.batches, batch)
	c.mu.Unlock()
	return batch, nil
}

func (c *boldFakeConn) callList() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// batchFor ищет батч по тексту INSERT: за прогон их три (macro_series, bold_fund_snapshot и
// series_catalog каталога), и различить их можно только запросом.
func (c *boldFakeConn) batchFor(t *testing.T, query string) *boldFakeBatch {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range c.batches {
		if b.query == query {
			return b
		}
	}
	t.Fatalf("батч %q не открывался; открытые: %v", query, c.callList())
	return nil
}

// boldTestBatchValue достаёт значение точки из строки батча macro_series по (series, date).
func boldTestBatchValue(t *testing.T, batch *boldFakeBatch, series, date string) float64 {
	t.Helper()
	want := testDate(t, date)
	for _, row := range batch.rows {
		if row[1] != series {
			continue
		}
		if d, ok := row[2].(time.Time); ok && d.Equal(want) {
			value, ok := row[3].(float64)
			if !ok {
				t.Fatalf("значение точки %s@%s типа %T", series, date, row[3])
			}
			return value
		}
	}
	t.Fatalf("в батче нет точки %s@%s (%d строк)", series, date, len(batch.rows))
	return 0
}

func boldTestCallIndex(calls []string, fragment string) int {
	for i, call := range calls {
		if strings.Contains(call, fragment) {
			return i
		}
	}
	return -1
}

func TestBoldEtfFlowsName(t *testing.T) {
	if got := (&boldEtfFlows{}).Name(); got != "bold_etf_flows" {
		t.Fatalf("Name() = %q, ожидалось bold_etf_flows", got)
	}
}

// TestBoldEtfFlowsIsRegistered: без записи в chimport.Stats импортёр не найдётся ни в dagu, ни в
// `make import STAT=bold_etf_flows` (прецедент — polyus/registry_test.go). Пакет подключается к
// main.go пустым импортом, поэтому единственный след регистрации — этот init().
func TestBoldEtfFlowsIsRegistered(t *testing.T) {
	for _, stat := range chimport.Stats {
		if stat.Name() != boldEtfName {
			continue
		}
		if _, ok := stat.(*boldEtfFlows); !ok {
			t.Fatalf("под именем %q зарегистрирован %T, ожидался *boldEtfFlows", boldEtfName, stat)
		}
		return
	}
	names := make([]string, 0, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		names = append(names, stat.Name())
	}
	t.Fatalf("импортёр %q не зарегистрирован в chimport.Stats: %v", boldEtfName, names)
}

// TestBoldNeedsFullFetch: порог 96 ч — граница «инкремент ещё справляется / уже нет». Пятница →
// понедельник это 3 дня, и полный путь для них не нужен: Δ считается от стока, прочитанного из
// ClickHouse. Саботаж «убрать порог» (всегда true) валит случаи +1/+3/+4 дня.
func TestBoldNeedsFullFetch(t *testing.T) {
	friday := testDate(t, boldPinDate) // 2026-10-09, пятница
	cases := []struct {
		name   string
		latest string
		stored time.Time
		want   bool
	}{
		{"ряды пусты", boldPinDate, time.Time{}, true},
		{"+1 день (сб)", "2026-10-10", friday, false},
		{"+3 дня (пн, длинные выходные)", "2026-10-12", friday, false},
		{"+4 дня — ровно порог", "2026-10-13", friday, false},
		{"+5 дней — пропуск дней доказан", "2026-10-14", friday, true},
		{"дата источника совпала", boldPinDate, friday, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := boldNeedsFullFetch(testDate(t, c.latest), c.stored); got != c.want {
				t.Errorf("boldNeedsFullFetch(%s, %s) = %t, ожидалось %t",
					c.latest, boldFormatDate(c.stored), got, c.want)
			}
		})
	}
}

// TestBoldIsPreviousTradingDay (Ruling 22): гейт инкремента ETF — сохранённый максимум обязан быть
// непосредственно предыдущим ТОРГОВЫМ днём даты источника. Сравниваются календарные даты, выходные
// шаг назад перепрыгивает (источник их не публикует), а пропущенный торговый день не перепрыгивает —
// именно это делает многодневный Δ невозможным. Саботаж «убрать скип выходных» валит кейс
// «пятница → понедельник»: без скипа предыдущим днём понедельника считается воскресенье.
func TestBoldIsPreviousTradingDay(t *testing.T) {
	cases := []struct {
		name   string
		latest string
		stored time.Time
		want   bool
	}{
		{"четверг → пятница (соседние торговые дни)", boldPinDate, testDate(t, boldPinPrior), true},
		{"пятница → понедельник (выходные перепрыгнуты)", "2026-10-12", testDate(t, boldPinDate), true},
		{"понедельник → вторник", "2026-10-13", testDate(t, "2026-10-12"), true},
		{"пропущен один торговый день (07.10 → 09.10)", boldPinDate, testDate(t, "2026-10-07"), false},
		{"пропущены два торговых дня (06.10 → 09.10)", boldPinDate, testDate(t, "2026-10-06"), false},
		{"ряды пусты (нулевой максимум)", boldPinDate, time.Time{}, false},
		{"дата источника совпала с максимумом", boldPinDate, testDate(t, boldPinDate), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := boldIsPreviousTradingDay(testDate(t, c.latest), c.stored); got != c.want {
				t.Errorf("boldIsPreviousTradingDay(%s, %s) = %t, ожидалось %t",
					c.latest, boldFormatDate(c.stored), got, c.want)
			}
		})
	}
}

// boldTestNow — фиксированный «день прогона» тестов: сутки после даты фикстур (2026-10-09) и
// меньше порога застоя. Часы прогона — шов `now`, и тесты обязаны давать импортёру
// детерминированный вход: с реальными часами лог (а в перспективе и поведение warn-а) зависел бы
// от дня запуска тестов, а «2026-10-16» в таблице ниже однажды наступил бы.
var boldTestNow = time.Date(2026, time.October, 10, 9, 0, 0, 0, time.UTC)

// boldTestClock — часы для `boldEtfFlows{now: …}`: всегда boldTestNow.
func boldTestClock() func() time.Time { return func() time.Time { return boldTestNow } }

// boldTestStat — импортёр прогона: httptest-сервер и фиксированные часы (см. boldTestNow).
func boldTestStat(server *boldTestServer) boldEtfFlows {
	return boldEtfFlows{apiBase: server.base(), now: boldTestClock()}
}

// TestBoldSourceLooksStaleAndClock — warn о застое источника (чистое решение) и шов часов
// прогона. Оба входа фиксированы: реальные часы здесь не читаются вовсе, поэтому тест не зависит
// от дня запуска.
func TestBoldSourceLooksStaleAndClock(t *testing.T) {
	latest := testDate(t, boldPinDate)
	cases := []struct {
		name string
		now  time.Time
		want bool
	}{
		{"T+1 — норма", testDate(t, "2026-10-10"), false},
		{"понедельник после пятницы", testDate(t, "2026-10-12"), false},
		{"ровно порог (4 дня)", testDate(t, "2026-10-13"), false},
		{"неделя застоя", testDate(t, "2026-10-16"), true},
		{"часы не заданы", time.Time{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := boldSourceLooksStale(latest, c.now); got != c.want {
				t.Errorf("boldSourceLooksStale(%s, %s) = %t, ожидалось %t",
					latest.Format(boldDateLayout), c.now.Format(boldDateLayout), got, c.want)
			}
		})
	}

	// Шов обязан отдавать подставленное время как есть. Ветка «now == nil → time.Now» здесь не
	// проверяется: её проверка читала бы реальные часы и зависела бы от них (а покрыта она всеми
	// тестами Import выше, которые ходят через тот же шов с фиксированным временем).
	if got := (&boldEtfFlows{now: boldTestClock()}).clock(); !got.Equal(boldTestNow) {
		t.Errorf("clock() с подставленными часами = %s, ожидалось %s", got, boldTestNow)
	}
}

// TestBoldEtfFetchLatestMismatchedDatesFails: даты трёх ответов «последнего дня» обязаны
// совпасть — срез фондов пишется под датой строки, и расхождение означало бы запись под чужой
// день. Саботаж «убрать сверку» этот тест валит.
func TestBoldEtfFetchLatestMismatchedDatesFails(t *testing.T) {
	bodies := boldLatestBodies(t)
	bodies[boldPathAumLatest] = boldMutateLatestDate(t, "bold_aum_latest.json", boldPinPrior)
	server := boldNewTestServer(t, bodies)

	_, _, err := boldEtfFetchLatest(server.base())
	if err == nil {
		t.Fatal("расхождение дат units-latest и aum-latest обязано быть ошибкой")
	}
	for _, fragment := range []string{boldPathUnitsLatest, boldPinDate, boldPathAumLatest, boldPinPrior} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("ошибка не называет %q: %v", fragment, err)
		}
	}
}

// TestBoldEtfFetchLatestMismatchedPricesFails: цена дня входит множителем в etf_bold_flow_usd_mln,
// поэтому «цена из одного файла, окна из другого» — тоже ошибка, а не тихий выбор одной из двух.
func TestBoldEtfFetchLatestMismatchedPricesFails(t *testing.T) {
	bodies := boldLatestBodies(t)
	var row map[string]any
	if err := json.Unmarshal(bodies[boldPathFlowsLatest], &row); err != nil {
		t.Fatalf("разбор bold_flows_latest.json: %v", err)
	}
	row["price"] = 4300.5
	body, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("сборка тела flows-latest: %v", err)
	}
	bodies[boldPathFlowsLatest] = body
	server := boldNewTestServer(t, bodies)

	if _, _, err := boldEtfFetchLatest(server.base()); err == nil {
		t.Fatal("расхождение цены дня между ответами обязано быть ошибкой")
	} else if !strings.Contains(err.Error(), "цена дня разошлась") {
		t.Errorf("ошибка не о расхождении цены: %v", err)
	}
}

// TestBoldEtfFullFetchUsesHistoryPaths: полный режим спрашивает файлы истории и срез — и ни
// одного «-latest»: смешение режимов дало бы точки из двух публикаций и разный состав ряда.
func TestBoldEtfFullFetchUsesHistoryPaths(t *testing.T) {
	server := boldNewTestServer(t, boldHistoryBodies(t))

	if _, _, err := boldEtfFullFetch(server.base()); err != nil {
		t.Fatalf("boldEtfFullFetch: %v", err)
	}

	want := []string{boldPathFlows, boldPathAum, boldPathUnits, boldPathSummary}
	sort.Strings(want)
	got := server.requestedSorted()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("пути полного режима: %v, ожидались %v", got, want)
	}
	for _, path := range got {
		if strings.Contains(path, "-latest") {
			t.Errorf("полный режим запросил %q — в нём есть «-latest»", path)
		}
	}
}

// TestBoldEtfFetchCountsRequests держит контракт «не больше 4 запросов за прогон»: инкремент —
// три *-latest и срез, полный режим — три файла истории и срез.
func TestBoldEtfFetchCountsRequests(t *testing.T) {
	latest := boldNewTestServer(t, boldLatestBodies(t))
	if _, _, err := boldEtfFetchLatest(latest.base()); err != nil {
		t.Fatalf("boldEtfFetchLatest: %v", err)
	}
	if got := latest.requested(); len(got) != 4 {
		t.Errorf("запросов инкремента: %d (%v), ожидалось 4", len(got), got)
	}

	full := boldNewTestServer(t, boldHistoryBodies(t))
	if _, _, err := boldEtfFullFetch(full.base()); err != nil {
		t.Fatalf("boldEtfFullFetch: %v", err)
	}
	if got := full.requested(); len(got) != 4 {
		t.Errorf("запросов полного режима: %d (%v), ожидалось 4", len(got), got)
	}
}

// TestBoldEtfHistoryMergesByDate: три файла сливаются по дате, и день, несущий только цену
// (2015-01-01 — первые 64 дня истории), в ряды этого импортёра не идёт: цену ведёт
// bold_gold_price, а в семи рядах контура такой день не даёт ни одной точки.
func TestBoldEtfHistoryMergesByDate(t *testing.T) {
	server := boldNewTestServer(t, boldHistoryBodies(t))

	rows, err := boldEtfHistory(server.base())
	if err != nil {
		t.Fatalf("boldEtfHistory: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("дней с ETF-полями: %d, ожидалось 2 (08.10 и 09.10): %v", len(rows), testDescribeRows(rows))
	}
	if rows[0].Date != boldPinPrior || rows[1].Date != boldPinDate {
		t.Fatalf("порядок строк: %s, %s — ожидался по дате", rows[0].Date, rows[1].Date)
	}
	for _, row := range rows {
		if row.Units == nil || row.Aum == nil || row.Culm7 == nil || row.Culm30 == nil || row.Culm90 == nil || row.Price == nil {
			t.Fatalf("строка %s слита неполно: %+v", row.Date, row)
		}
	}

	points := testPoints(t, rows, nil, time.Time{})
	for _, p := range points {
		if p.Date.Equal(testDate(t, boldPinEmpty)) {
			t.Errorf("точка %s за %s: день без units/aum в ряды попасть не может", p.Series, boldPinEmpty)
		}
	}
	// Пины Task 1 на слитой истории: 12 точек, поток 09.10 считается от строки 08.10.
	if len(points) != 12 {
		t.Errorf("точек: %d, ожидалось 12 (%s)", len(points), testDescribe(points))
	}
	testNear(t, "flow 09.10", testPoint(t, points, boldSeriesFlowTonnes, boldPinDate).Value, 4.233, 1e-3)
}

// testDescribeRows — строки истории в сообщение об ошибке.
func testDescribeRows(rows []boldRow) string {
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, row.Date)
	}
	return strings.Join(parts, ", ")
}

// TestBoldEtfFullFetchRejectsPartialDay (Ruling 5): день, где есть units, но нет aum, — ошибка с
// датой. Тихий пропуск дал бы ряд стока без ряда AUM, и «полдня» ушло бы в базу как факт.
func TestBoldEtfFullFetchRejectsPartialDay(t *testing.T) {
	bodies := boldHistoryBodies(t)
	bodies[boldPathAum] = boldMutateHistory(t, "bold_aum_history.json", func(data []map[string]any) []map[string]any {
		out := make([]map[string]any, 0, len(data))
		for _, row := range data {
			if row["date"] == boldPinDate {
				continue
			}
			out = append(out, row)
		}
		return out
	})
	server := boldNewTestServer(t, bodies)

	_, _, err := boldEtfFullFetch(server.base())
	if err == nil {
		t.Fatal("день с units, но без aum обязан быть ошибкой")
	}
	for _, fragment := range []string{boldPinDate, "неполон", "units", "aum"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("ошибка не называет %q: %v", fragment, err)
		}
	}
}

// TestBoldEtfFullFetchRejectsConflictingBetweenFiles (Ruling 5, дубликат даты между файлами):
// одно поле, пришедшее из двух файлов истории с разными значениями, — ошибка с датой. Источник
// публикует одну и ту же цену дня во всех файлах, поэтому расхождение означает рассогласованную
// пару файлов, а молчаливый выбор одного из двух значений спрятал бы её.
func TestBoldEtfFullFetchRejectsConflictingBetweenFiles(t *testing.T) {
	bodies := boldHistoryBodies(t)
	bodies[boldPathAum] = boldMutateHistory(t, "bold_aum_history.json", func(data []map[string]any) []map[string]any {
		for _, row := range data {
			if row["date"] == boldPinPrior {
				row["units"] = boldPinAPrior + 1000 // units пришёл ещё и из файла aum, и с другим значением
			}
		}
		return data
	})
	server := boldNewTestServer(t, bodies)

	_, _, err := boldEtfFullFetch(server.base())
	if err == nil {
		t.Fatal("поле units из двух файлов с разными значениями обязано быть ошибкой")
	}
	for _, fragment := range []string{boldPinPrior, "units", boldPathUnits, boldPathAum} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("ошибка не называет %q: %v", fragment, err)
		}
	}
}

// TestBoldStoredMaxDateIsMinOfSeriesAndFunds (Ruling 7): сохранённый максимум — минимум из
// максимума рядов macro_series и максимума bold_fund_snapshot. Максимум здесь оставил бы
// пер-фондовый срез позади навсегда: батч рядов прошёл, батч фондов упал — и следующий прогон
// счёл бы день обработанным.
func TestBoldStoredMaxDateIsMinOfSeriesAndFunds(t *testing.T) {
	cases := []struct {
		name       string
		seriesMax  string
		fundsMax   string
		want       string
		wantNotMax string
	}{
		{"ряды свежее среза (батч фондов упал)", boldPinDate, boldPinPrior, boldPinPrior, boldPinDate},
		{"срез свежее рядов", boldPinPrior, boldPinDate, boldPinPrior, boldPinDate},
		{"ряды пусты", "", boldPinDate, "", boldPinDate},
		{"срез пуст (таблицы нет)", boldPinDate, "", "", boldPinDate},
		{"обе пусты", "", "", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row := func(date string) driver.Row {
				if date == "" {
					return boldFakeNull()
				}
				return boldFakeDate(t, date)
			}
			conn := newBoldFakeConn(row(c.seriesMax), row(c.fundsMax))

			got, err := boldStoredMaxDate(context.Background(), conn)
			if err != nil {
				t.Fatalf("boldStoredMaxDate: %v", err)
			}
			if c.want == "" {
				if !got.IsZero() {
					t.Fatalf("максимум = %s, ожидалось нулевое время", got.Format(boldDateLayout))
				}
			} else if !got.Equal(testDate(t, c.want)) {
				t.Fatalf("максимум = %s, ожидалось %s (минимум из двух таблиц)", got.Format(boldDateLayout), c.want)
			}
			if c.wantNotMax != "" && got.Equal(testDate(t, c.wantNotMax)) {
				t.Fatalf("максимум = %s — это максимум одной таблицы, а не минимум двух", c.wantNotMax)
			}
			if got := conn.callList(); len(got) != 2 {
				t.Errorf("обращений к соединению: %v, ожидались два запроса максимума", got)
			}
		})
	}
}

// TestBoldStoredMaxDateMissingFundsTableIsZero: на чистой базе bold_fund_snapshot ещё нет, и
// UNKNOWN_TABLE — это «данных нет» (нулевое время), а не ошибка чтения: иначе первый прогон
// получал бы ложный warn о непрочитанном максимуме при здоровом источнике.
func TestBoldStoredMaxDateMissingFundsTableIsZero(t *testing.T) {
	conn := newBoldFakeConn(
		boldFakeDate(t, boldPinDate),
		boldFakeError("Code: 60. DB::Exception: Table default.bold_fund_snapshot does not exist. (UNKNOWN_TABLE)"),
	)

	got, err := boldStoredMaxDate(context.Background(), conn)
	if err != nil {
		t.Fatalf("отсутствующая таблица среза обязана давать нулевое время, а не ошибку: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("максимум = %s, ожидалось нулевое время", got.Format(boldDateLayout))
	}
}

// TestBoldStoredMaxDateReadErrorIsError: настоящая ошибка чтения максимума не проглатывается —
// её решает вызывающий (warn и полный путь), и подмена «ошибка = пусто» спрятала бы сломанное
// соединение за полной историей.
func TestBoldStoredMaxDateReadErrorIsError(t *testing.T) {
	conn := newBoldFakeConn(boldFakeError("connection refused"))

	if _, err := boldStoredMaxDate(context.Background(), conn); err == nil {
		t.Fatal("ошибка чтения максимума обязана быть ошибкой")
	}
}

// TestBoldStoredQueriesPinSql держит текст гвардейских запросов: у kimi_reader доступа к
// macro_series и bold_fund_snapshot нет, поэтому проверить их иначе нечем. Пин заодно держит
// экранирование «_» в LIKE: без «\» шаблон поймал бы чужие ряды.
func TestBoldStoredQueriesPinSql(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{
			"максимум рядов",
			boldStoredSeriesMaxQuery,
			"SELECT maxOrNull(date) FROM macro_series FINAL WHERE source = 'bold' AND series LIKE 'etf\\_bold\\_%'",
		},
		{
			"максимум среза",
			boldStoredFundMaxQuery,
			"SELECT maxOrNull(date) FROM bold_fund_snapshot FINAL",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("запрос:\n  %s\nожидался:\n  %s", c.got, c.want)
			}
		})
	}
}

// TestBoldStoredUnitsPinsQueryAndConverts: запрос за предыдущим стоком пинится целиком (вместе с
// датой), а значение конвертируется из тонн базы обратно в унции источника — иначе Δ оказалась бы
// в 32150 раз меньше унции. NULL — ошибка «нет запасов», а не ноль: поток молча стал бы нулём.
func TestBoldStoredUnitsPinsQueryAndConverts(t *testing.T) {
	conn := newBoldFakeConn(boldFakeValue(ozToTonnes(boldPinAPrior)))

	units, err := boldStoredUnits(context.Background(), conn, testDate(t, boldPinPrior))
	if err != nil {
		t.Fatalf("boldStoredUnits: %v", err)
	}
	testNear(t, "сток 08.10 в унциях", units, boldPinAPrior, 1e-3)

	want := "SELECT maxOrNull(value) FROM macro_series FINAL WHERE source = 'bold' AND series = '" +
		boldSeriesHoldingsTonnes + "' AND date = '" + boldPinPrior + "'"
	if got := conn.callList(); len(got) != 1 || got[0] != "queryrow: "+want {
		t.Errorf("обращения к соединению: %v, ожидался один запрос\n  queryrow: %s", got, want)
	}

	null := newBoldFakeConn(boldFakeNull())
	if _, err := boldStoredUnits(context.Background(), null, testDate(t, boldPinPrior)); err == nil {
		t.Fatal("отсутствие предыдущего стока обязано быть ошибкой, а не нулём")
	} else if !strings.Contains(err.Error(), "нет запасов") || !strings.Contains(err.Error(), boldPinPrior) {
		t.Errorf("ошибка не называет отсутствие запасов и дату: %v", err)
	}
}

// TestBoldEtfImportFullWritesAllSeries: пустое соединение (обе таблицы пусты) уводит импорт на
// полную историю — в macro_series идут точки семи рядов, в bold_fund_snapshot — 73 строки, а
// каталог пишется после обоих Send.
func TestBoldEtfImportFullWritesAllSeries(t *testing.T) {
	server := boldNewTestServer(t, boldAllBodies(t))
	conn := newBoldFakeConn() // пустая очередь — оба максимума NULL
	stat := boldTestStat(server)

	count, err := stat.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 12 {
		t.Errorf("Import вернул %d строк, ожидалось 12 — точки за 08.10 и 09.10", count)
	}

	macro := conn.batchFor(t, boldMacroSeriesInsert)
	if len(macro.rows) != 12 {
		t.Fatalf("строк в macro_series: %d, ожидалось 12", len(macro.rows))
	}
	series := map[string]bool{}
	for _, row := range macro.rows {
		if row[0] != boldSource {
			t.Fatalf("source строки = %v, ожидался %q", row[0], boldSource)
		}
		name, ok := row[1].(string)
		if !ok {
			t.Fatalf("series строки типа %T", row[1])
		}
		series[name] = true
		if _, ok := row[2].(time.Time); !ok {
			t.Fatalf("date строки типа %T", row[2])
		}
		if _, ok := row[3].(float64); !ok {
			t.Fatalf("value строки типа %T", row[3])
		}
	}
	if len(series) != 7 {
		t.Fatalf("рядов в батче: %d (%v), ожидалось 7", len(series), series)
	}
	// Пины Task 1 на тех же фикстурах: сток 09.10 и поток дня из истории (Δ к 08.10).
	testNear(t, "holdings 09.10", boldTestBatchValue(t, macro, boldSeriesHoldingsTonnes, boldPinDate), 3520.318, 1e-3)
	testNear(t, "flow 09.10", boldTestBatchValue(t, macro, boldSeriesFlowTonnes, boldPinDate), 4.233, 1e-3)
	testNear(t, "flow usd 09.10", boldTestBatchValue(t, macro, boldSeriesFlowUsdMln, boldPinDate), 573.75, 1e-2)

	funds := conn.batchFor(t, boldFundInsert)
	if len(funds.rows) != 73 {
		t.Fatalf("строк в bold_fund_snapshot: %d, ожидалось 73", len(funds.rows))
	}
	if !macro.sent || !funds.sent {
		t.Error("батчи обязаны быть отправлены (Send)")
	}
	for _, row := range funds.rows {
		if !row[0].(time.Time).Equal(testDate(t, boldPinDate)) {
			t.Fatalf("дата строки среза = %v, ожидалась %s", row[0], boldPinDate)
		}
		if row[1] == "" || row[2] == "" {
			t.Fatalf("строка среза без тикера или имени: %v", row)
		}
	}
	// Ключ (date, ticker, name) обязан разнести три GLD (Ruling 3): все три строки в батче.
	gld := 0
	var spdrUnits float64
	for _, row := range funds.rows {
		if row[1] != "GLD" {
			continue
		}
		gld++
		if row[2] == "SPDR Gold Shares" {
			spdrUnits = row[3].(float64)
		}
	}
	if gld != 3 {
		t.Errorf("строк с тикером GLD: %d, ожидалось 3 (тикер не уникален — ключ включает name)", gld)
	}
	if spdrUnits != 33552628 {
		t.Errorf("units_oz SPDR Gold Shares = %v, ожидалось 33552628", spdrUnits)
	}

	// Обе DDL идут до батчей, каталог — после Send.
	calls := conn.callList()
	fundDdl := boldTestCallIndex(calls, "exec: CREATE TABLE IF NOT EXISTS "+boldFundTable)
	macroDdl := boldTestCallIndex(calls, "exec: CREATE TABLE IF NOT EXISTS "+macroSeriesTable)
	firstBatch := boldTestCallIndex(calls, "preparebatch: "+boldMacroSeriesInsert)
	catalog := boldTestCallIndex(calls, "preparebatch: INSERT INTO series_catalog")
	if fundDdl < 0 || macroDdl < 0 || firstBatch < 0 || catalog < 0 {
		t.Fatalf("порядок обращений: %v", calls)
	}
	if fundDdl > firstBatch || macroDdl > firstBatch {
		t.Errorf("DDL обязаны идти до батчей: %v", calls)
	}
	if fundDdl > macroDdl {
		t.Errorf("DDL среза ожидался до DDL macro_series: %v", calls)
	}
	if catalog < firstBatch {
		t.Errorf("каталог обязан писаться после батчей: %v", calls)
	}
	// Полный прогон — 8 запросов: четыре «последнего дня» (дата для среза и решение no-op) и
	// четыре файла полного режима. Контракт «не больше 4 запросов» — про режим, и он пинится
	// TestBoldEtfFetchCountsRequests.
	if got := server.requested(); len(got) != 8 {
		t.Errorf("запросов за полный прогон: %d (%v), ожидалось 8 (4 последнего дня + 4 истории)", len(got), got)
	}
}

// TestBoldEtfImportIncrementalWritesOneDay: соединение знает максимум 08.10 в обеих таблицах
// (S = F = 08.10 — инкремент разрешён) и сток за 08.10 — пишутся только точки 09.10, а поток
// считается от стока из ClickHouse (+4.233 т и 573.75 млн $). Саботаж «взять предыдущие унции из
// ответа источника» валит именно этот тест: в ответе units-latest есть только units дня, и Δ
// стала бы нулём. Саботаж «снять условие seriesMax != fundMax» валит соседний
// TestBoldEtfImportFullFetchWhenSeriesAndFundsDiverge: здесь расхождения нет по построению.
func TestBoldEtfImportIncrementalWritesOneDay(t *testing.T) {
	server := boldNewTestServer(t, boldLatestBodies(t))
	// В базе ряд etf_bold_holdings_tonnes в тоннах: 113 044 763 oz (= boldPinAPrior) — это
	// 3516,0815 т, и запрос возвращает именно тонны.
	conn := newBoldFakeConn(
		boldFakeDate(t, boldPinPrior), // максимум рядов
		boldFakeDate(t, boldPinPrior), // столько же в срезе: stored = 08.10, расхождения нет
		boldFakeValue(ozToTonnes(boldPinAPrior)),
	)
	stat := boldTestStat(server)

	count, err := stat.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 7 {
		t.Errorf("Import вернул %d строк, ожидалось 7 точек за 09.10", count)
	}

	macro := conn.batchFor(t, boldMacroSeriesInsert)
	if len(macro.rows) != 7 {
		t.Fatalf("строк в macro_series: %d, ожидалось 7 (инкремент пишет один день)", len(macro.rows))
	}
	for _, row := range macro.rows {
		if !row[2].(time.Time).Equal(testDate(t, boldPinDate)) {
			t.Fatalf("точка за %v: инкремент обязан писать только дату источника %s", row[2], boldPinDate)
		}
	}
	testNear(t, "flow 09.10", boldTestBatchValue(t, macro, boldSeriesFlowTonnes, boldPinDate), 4.233, 1e-3)
	testNear(t, "flow usd 09.10", boldTestBatchValue(t, macro, boldSeriesFlowUsdMln, boldPinDate), 573.75, 1e-2)
	testNear(t, "holdings 09.10", boldTestBatchValue(t, macro, boldSeriesHoldingsTonnes, boldPinDate), 3520.318, 1e-3)

	funds := conn.batchFor(t, boldFundInsert)
	if len(funds.rows) != 73 {
		t.Errorf("строк в bold_fund_snapshot: %d, ожидалось 73", len(funds.rows))
	}
	want := []string{boldPathUnitsLatest, boldPathAumLatest, boldPathFlowsLatest, boldPathSummary}
	sort.Strings(want)
	if got := server.requestedSorted(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("пути инкремента: %v, ожидались %v", got, want)
	}
	// Гвардейский запрос за предыдущим стоком обязан быть в обращениях, и он один.
	if got := boldTestCallIndex(conn.callList(), boldSeriesHoldingsTonnes); got < 0 {
		t.Errorf("запрос за предыдущим стоком не выполнялся: %v", conn.callList())
	}
}

// TestBoldEtfImportFullFetchWhenSeriesAndFundsDiverge (Ruling 8): сохранённые максимумы ряда и
// среза разошлись (ряды за 09.10, срез за 08.10 — частичный сбой прошлого прогона) — прогон
// обязан пойти ПОЛНЫМ путём, а не инкрементом. Инкремент посчитал бы Δ от ряда, отставшего на
// торговый день (09.10 против 08.10), и молча выдал бы многодневный поток за дневной; полный путь
// пишет и ряды, и срез за дату источника, выравнивая S = F = L.
func TestBoldEtfImportFullFetchWhenSeriesAndFundsDiverge(t *testing.T) {
	server := boldNewTestServer(t, boldAllBodies(t))
	// Два максимума читаются всегда (Ruling 7); третьего ответа (за стоком) быть не должно:
	// инкремент не исполняется, и фейковое соединение отдало бы NULL, что уронило бы импорт.
	conn := newBoldFakeConn(
		boldFakeDate(t, boldPinDate),  // ряды: 09.10
		boldFakeDate(t, boldPinPrior), // срез: 08.10 — расхождение
	)
	stat := boldTestStat(server)

	count, err := stat.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 12 {
		t.Errorf("Import вернул %d строк, ожидалось 12 — полный путь пишет оба дня истории, а не один", count)
	}

	macro := conn.batchFor(t, boldMacroSeriesInsert)
	if len(macro.rows) != 12 {
		t.Fatalf("строк в macro_series: %d, ожидалось 12 (полный путь: 08.10 + 09.10)", len(macro.rows))
	}
	// Срез переписан за дату источника — после прогона S = F = 09.10, и инкремент снова возможен.
	funds := conn.batchFor(t, boldFundInsert)
	if len(funds.rows) != 73 {
		t.Fatalf("строк в bold_fund_snapshot: %d, ожидалось 73", len(funds.rows))
	}
	if !funds.rows[0][0].(time.Time).Equal(testDate(t, boldPinDate)) {
		t.Errorf("дата среза = %v, ожидалась дата источника %s (полный путь выравнивает S и F)", funds.rows[0][0], boldPinDate)
	}

	// Оба максимума прочитаны, DDL выполнены, оба батча открыты — и ни одного запроса за стоком:
	// расхождение уводит на полный путь до инкрементной ветки.
	calls := conn.callList()
	for _, fragment := range []string{"queryrow: " + boldStoredSeriesMaxQuery, "queryrow: " + boldStoredFundMaxQuery} {
		if boldTestCallIndex(calls, fragment) < 0 {
			t.Errorf("не прочитан максимум %q: %v", fragment, calls)
		}
	}
	if boldTestCallIndex(calls, boldSeriesHoldingsTonnes) >= 0 {
		t.Errorf("запрос за предыдущим стоком на полном пути: %v", calls)
	}
	for _, fragment := range []string{
		"exec: CREATE TABLE IF NOT EXISTS " + boldFundTable,
		"exec: CREATE TABLE IF NOT EXISTS " + macroSeriesTable,
		"preparebatch: " + boldMacroSeriesInsert,
		"preparebatch: " + boldFundInsert,
	} {
		if boldTestCallIndex(calls, fragment) < 0 {
			t.Errorf("полный путь обязан сделать %q: %v", fragment, calls)
		}
	}

	// Полный прогон начался с четырёх запросов «последнего дня» и ушёл на файлы истории: все
	// четыре пути истории запрошены ровно по разу.
	requested := server.requested()
	counts := map[string]int{}
	for _, path := range requested {
		counts[path]++
	}
	for _, path := range []string{boldPathUnits, boldPathAum, boldPathFlows} {
		if counts[path] != 1 {
			t.Errorf("путь %q запрошен %d раз (все запросы: %v), ожидался один запрос истории", path, counts[path], requested)
		}
	}
	for _, path := range []string{boldPathUnitsLatest, boldPathAumLatest, boldPathFlowsLatest} {
		if counts[path] != 1 {
			t.Errorf("путь %q запрошен %d раз (все запросы: %v), ожидался один запрос последнего дня", path, counts[path], requested)
		}
	}
}

// TestBoldEtfImportFullFetchWhenPreviousDayMissing (Ruling 22): сохранённый максимум обеих половин
// равен 07.10, а источник отдаёт 09.10 — соседний торговый день максимума (08.10) пропущен, поэтому
// инкремент запрещён: он посчитал бы Δ за два торговых дня и выдал бы её за дневной поток, а день
// 08.10 не попал бы ни в один из семи рядов (до случайного полного прогона). Прогон обязан пойти
// ПОЛНЫМ путём и записать промежуточный день из файлов истории. Саботаж «вернуть в гейт
// boldNeedsFullFetch» (разрыв 09.10 − 07.10 = 48 ч < порога 96 ч) валит именно этот тест.
func TestBoldEtfImportFullFetchWhenPreviousDayMissing(t *testing.T) {
	server := boldNewTestServer(t, boldAllBodies(t))
	// Оба максимума — 07.10, расхождения половин нет: сработать обязан именно гейт соседнего дня.
	// Третий ответ (сток за 07.10) в норме НЕ читается — он здесь затем, чтобы саботаж «вернуть в гейт
	// пороговую boldNeedsFullFetch» упал на арифметике (инкремент записал бы 7 строк с многодневной Δ
	// вместо 12 строк полного пути), а не на «нет запасов»: так тест ловит именно подмену потока.
	conn := newBoldFakeConn(
		boldFakeDate(t, "2026-10-07"),
		boldFakeDate(t, "2026-10-07"),
		boldFakeValue(ozToTonnes(boldPinAPrior)),
	)
	stat := boldTestStat(server)

	count, err := stat.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 12 {
		t.Errorf("Import вернул %d строк, ожидалось 12 — полный путь пишет и промежуточный день 08.10", count)
	}
	// На полном пути инкрементная ветка не исполняется, поэтому сток за 07.10 не читается.
	if boldTestCallIndex(conn.callList(), boldSeriesHoldingsTonnes) >= 0 {
		t.Errorf("полный путь читал сток за 07.10: %v", conn.callList())
	}

	// Главное утверждение теста: пропущенный день обязан появиться в батче, а не потеряться.
	macro := conn.batchFor(t, boldMacroSeriesInsert)
	days := map[string]int{}
	for _, row := range macro.rows {
		days[row[2].(time.Time).Format(boldDateLayout)]++
	}
	if days[boldPinPrior] == 0 {
		t.Errorf("промежуточного дня %s в батче нет (дни: %v) — пропущенный торговый день потерян", boldPinPrior, days)
	}
	if days[boldPinDate] == 0 {
		t.Errorf("дня источника %s в батче нет (дни: %v)", boldPinDate, days)
	}
	if len(days) != 2 {
		t.Errorf("дней в батче: %v, ожидались ровно два — 08.10 и 09.10", days)
	}

	// Полный путь: четыре ответа «последнего дня» плюс четыре файла истории (summary запрашивается
	// и там, и там — он несёт срез фондов).
	want := []string{
		boldPathUnitsLatest, boldPathAumLatest, boldPathFlowsLatest, boldPathSummary,
		boldPathUnits, boldPathAum, boldPathFlows, boldPathSummary,
	}
	sort.Strings(want)
	if got := server.requestedSorted(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("пути полного прогона: %v, ожидались %v", got, want)
	}
}

// TestBoldEtfImportNoopWhenUpToDate: дата источника не новее сохранённого максимума — 0 строк,
// ни одной ошибки, и данные не трогаются: ни DDL обеих таблиц, ни батчей INSERT в них, ни запроса
// за предыдущим стоком. Каталог рядов при этом публикуется — предусловие ветки непустой
// сохранённый максимум, то есть ряды в таблице уже есть, и их описания обязаны быть актуальны без
// новых данных (иначе правка текста каталога доезжала бы до агента только вместе с новыми точками).
func TestBoldEtfImportNoopWhenUpToDate(t *testing.T) {
	server := boldNewTestServer(t, boldLatestBodies(t))
	conn := newBoldFakeConn(boldFakeDate(t, boldPinDate), boldFakeDate(t, boldPinDate))
	stat := boldTestStat(server)

	count, err := stat.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк, ожидалось 0", count)
	}
	calls := conn.callList()
	for _, fragment := range []string{
		"exec: CREATE TABLE IF NOT EXISTS " + boldFundTable,
		"exec: CREATE TABLE IF NOT EXISTS " + macroSeriesTable,
		"preparebatch: " + boldMacroSeriesInsert,
		"preparebatch: " + boldFundInsert,
	} {
		if boldTestCallIndex(calls, fragment) >= 0 {
			t.Errorf("no-op прогон тронул данные (%q): %v", fragment, calls)
		}
	}
	if boldTestCallIndex(calls, boldSeriesHoldingsTonnes) >= 0 {
		t.Errorf("no-op прогон читал предыдущий сток: %v", calls)
	}
	for _, fragment := range []string{"queryrow: " + boldStoredSeriesMaxQuery, "queryrow: " + boldStoredFundMaxQuery} {
		if boldTestCallIndex(calls, fragment) < 0 {
			t.Errorf("no-op прогон обязан прочитать максимум %q: %v", fragment, calls)
		}
	}

	// Каталог опубликован целиком: батч series_catalog (текст INSERT задаёт util.UpsertSeriesCatalog)
	// открыт, заполнен семью рядами и отправлен — иначе исправленный origin не доехал бы до агента.
	catalog := conn.batchFor(t, "INSERT INTO series_catalog")
	if !catalog.sent {
		t.Error("батч каталога на no-op прогоне не отправлен")
	}
	if len(catalog.rows) != 7 {
		t.Errorf("строк каталога на no-op прогоне: %d, ожидалось 7", len(catalog.rows))
	}

	if got := server.requested(); len(got) != 4 {
		t.Errorf("запросов на no-op прогоне: %d (%v)", len(got), got)
	}
}

// TestBoldEtfImportMissingUnitsAtMaxFails: сохранённый максимум есть, а точки стока за него нет —
// ошибка импорта, а не поток = 0. Ноль вместо отсутствующих данных и есть та молчаливая подмена,
// от которой предостерегает Review Focus 4.
func TestBoldEtfImportMissingUnitsAtMaxFails(t *testing.T) {
	server := boldNewTestServer(t, boldLatestBodies(t))
	conn := newBoldFakeConn(
		boldFakeDate(t, boldPinPrior),
		boldFakeDate(t, boldPinPrior),
		boldFakeNull(), // точки etf_bold_holdings_tonnes за 08.10 нет
	)
	stat := boldTestStat(server)

	_, err := stat.Import(context.Background(), conn)
	if err == nil {
		t.Fatal("отсутствие предыдущего стока обязано останавливать импорт")
	}
	for _, fragment := range []string{"нет запасов", boldSeriesHoldingsTonnes, boldPinPrior} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("ошибка не называет %q: %v", fragment, err)
		}
	}
	for _, call := range conn.callList() {
		if strings.HasPrefix(call, "preparebatch: ") {
			t.Errorf("батч открывался до проверки предыдущего стока: %v", conn.callList())
		}
	}
}

// TestBoldEtfImportEmptyResponseFails: пустое тело ответа — ошибка импорта (fail-fast, урок eia):
// тихий ноль оставил бы dagu зелёным, а ряды — без обновления. Ошибка обязана называть эндпоинт:
// по «пустое тело» без имени файла не понять, какой из четырёх ответов сломался.
func TestBoldEtfImportEmptyResponseFails(t *testing.T) {
	bodies := boldLatestBodies(t)
	bodies[boldPathUnitsLatest] = nil // пустое тело
	server := boldNewTestServer(t, bodies)
	stat := boldTestStat(server)

	_, err := stat.Import(context.Background(), newBoldFakeConn())
	if err == nil {
		t.Fatal("пустое тело ответа обязано быть ошибкой")
	}
	if !strings.Contains(err.Error(), boldPathUnitsLatest) || !strings.Contains(err.Error(), "пустое тело") {
		t.Errorf("ошибка не называет эндпоинт и причину: %v", err)
	}
}

// TestBoldWriteKeepsInputOrder: батчи пишутся в отсортированном порядке, но срез вызывающего
// сортировка не меняет — побочный эффект на аргументе ищут потом в чужом вызове, а не здесь
// (саботаж «сортировать на месте», то есть без slices.Clone, валит этот тест).
func TestBoldWriteKeepsInputOrder(t *testing.T) {
	points := []point{
		{Series: boldSeriesHoldingsTonnes, Date: testDate(t, boldPinDate), Value: 3520.318},
		{Series: boldSeriesAumUsdBn, Date: testDate(t, boldPinPrior), Value: 469.508},
		{Series: boldSeriesAumUsdBn, Date: testDate(t, boldPinDate), Value: 476.678},
	}
	conn := newBoldFakeConn()
	if _, err := boldWritePoints(context.Background(), conn, points); err != nil {
		t.Fatalf("boldWritePoints: %v", err)
	}
	wantPoints := []string{boldSeriesHoldingsTonnes, boldSeriesAumUsdBn, boldSeriesAumUsdBn}
	for i, p := range points {
		if p.Series != wantPoints[i] {
			t.Fatalf("срез точек переставлен: %v (позиция %d = %s)", points, i, p.Series)
		}
	}
	macro := conn.batchFor(t, boldMacroSeriesInsert)
	if got := macro.rows[0][1]; got != boldSeriesAumUsdBn {
		t.Errorf("первая строка батча — %v, ожидался ряд %s (батч обязан быть отсортирован)", got, boldSeriesAumUsdBn)
	}

	funds := []boldFundPoint{
		{Ticker: "SPDR", Name: "SPDR Gold Shares"},
		{Ticker: "GLD", Name: "KTAM Gold ETF Tracker Class GLD (Bangkok)"},
		{Ticker: "GLD", Name: "NewGold Issuer Ltd"},
	}
	conn = newBoldFakeConn()
	if err := boldWriteFunds(context.Background(), conn, funds); err != nil {
		t.Fatalf("boldWriteFunds: %v", err)
	}
	wantFunds := []string{"SPDR", "GLD", "GLD"}
	for i, f := range funds {
		if f.Ticker != wantFunds[i] {
			t.Fatalf("срез фондов переставлен: %v (позиция %d = %s)", funds, i, f.Ticker)
		}
	}
	batch := conn.batchFor(t, boldFundInsert)
	if got := batch.rows[0][1]; got != "GLD" {
		t.Errorf("первая строка батча фондов — %v, ожидался тикер GLD (батч обязан быть отсортирован)", got)
	}
}

// TestBoldEtfCatalogDescribesEverySeries: семь записей каталога, все поля заполнены, имена
// совпадают с рядами контура, а описания несут оговорки спеки §4.4 — иначе агент прочитает ряд
// без «не складывать с WGC» и «окна календарные». Здесь же origin сверяется с общим boldETFOrigin:
// пути в нём обязаны быть боевыми (с суффиксом .json), а не «как в статье».
func TestBoldEtfCatalogDescribesEverySeries(t *testing.T) {
	want := []string{
		boldSeriesAumUsdBn, boldSeriesFlow30dTonnes, boldSeriesFlow7dTonnes, boldSeriesFlow90dTonnes,
		boldSeriesFlowTonnes, boldSeriesFlowUsdMln, boldSeriesHoldingsTonnes,
	}
	meta := boldETFSeriesMeta()
	if len(meta) != 7 {
		t.Fatalf("записей каталога: %d, ожидалось 7", len(meta))
	}
	for _, entry := range meta {
		if entry.Source != boldSource {
			t.Errorf("%s: source = %q, ожидался %q", entry.Series, entry.Source, boldSource)
		}
		if entry.Series == "" || entry.Title == "" || entry.Unit == "" || entry.Origin == "" || entry.Description == "" {
			t.Errorf("%s: пустое поле каталога: %+v", entry.Series, entry)
		}
		if entry.Frequency != "D" {
			t.Errorf("%s: frequency = %q, ожидалось D", entry.Series, entry.Frequency)
		}
		for _, fragment := range []string{"bold.report", "83 %", "не складывать", "календарные"} {
			if !strings.Contains(entry.Description, fragment) {
				t.Errorf("%s: описание не содержит %q", entry.Series, fragment)
			}
		}
		// Origin собирается из констант boldPath* и обязан называть все боевые эндпоинты — они
		// оканчиваются на .json (TestBoldEndpointsPinPaths), а путь без суффикса источник отдаёт
		// 403: агент, читающий каталог, получил бы нерабочее происхождение (fix round 2, Ruling 16).
		if entry.Origin != boldETFOrigin {
			t.Errorf("%s: origin = %q, ожидалась общая строка boldETFOrigin", entry.Series, entry.Origin)
		}
		for _, path := range []string{boldPathUnits, boldPathAum, boldPathFlows, boldPathSummary, boldPathPrice} {
			if !strings.Contains(entry.Origin, path) {
				t.Errorf("%s: origin %q не называет эндпоинт %q", entry.Series, entry.Origin, path)
			}
		}
	}

	names := make([]string, 0, len(meta))
	for _, entry := range meta {
		names = append(names, entry.Series)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("ряды каталога: %v, ожидались %v", names, want)
	}
}
