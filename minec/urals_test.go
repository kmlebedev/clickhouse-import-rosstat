package minec

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const testListingURL = "https://example.test/materials.html"

// testArticleFixtures — ключ в href статьи → реальная страница месяца. Ключи
// взяты из слагов листинга 2026 года: месяцы 2026-06 … 2026-10.
var testArticleFixtures = map[string]string{
	"za_oktyabr_2026":  "yurals_2026_10.html",
	"za_sentyabr_2026": "yurals_2026_sentyabr.html",
	"za_avgust_2026":   "yurals_2026_avgust.html",
	"za_iyul_2026":     "yurals_2026_iyul.html",
	"za_iyun_2026":     "yurals_2026_iyun.html",
}

// pageSource — подмена транспорта: карта URL → байты фикстуры и счётчики
// обращений. Сеть в тестах не используется (стандарт §11.3).
type pageSource struct {
	pages map[string][]byte
	calls map[string]int
}

func (s *pageSource) get(url string) ([]byte, error) {
	s.calls[url]++
	body, ok := s.pages[url]
	if !ok {
		return nil, fmt.Errorf("нет страницы %s", url)
	}
	return body, nil
}

// installPages подменяет транспорт на карту фикстур и отключает паузу между
// запросами; восстановление — через t.Cleanup.
func installPages(t *testing.T, pages map[string][]byte) *pageSource {
	t.Helper()
	src := &pageSource{pages: pages, calls: make(map[string]int)}
	oldGet, oldPause := minecGetPage, minecPagePause
	minecGetPage = src.get
	minecPagePause = 0
	t.Cleanup(func() {
		minecGetPage = oldGet
		minecPagePause = oldPause
	})
	return src
}

// testPageSet собирает карту: страницы листинга по номерам и статьи месяцев
// первой страницы (href берётся из реального листинга, а не пишется руками).
func testPageSet(t *testing.T, listingPages map[int]string) map[string][]byte {
	t.Helper()
	pages := make(map[string][]byte, len(listingPages)+len(testArticleFixtures))
	for page, fixture := range listingPages {
		pages[minecPageURL(testListingURL, page)] = readFixture(t, fixture)
	}
	first, err := minecParseListing(readFixture(t, "materials_p1.html"), true)
	if err != nil {
		t.Fatalf("листинг первой страницы: %v", err)
	}
	for _, link := range first.urals {
		for substr, fixture := range testArticleFixtures {
			if !strings.Contains(link.href, substr) {
				continue
			}
			articleURL, err := minecArticleURL(testListingURL, link.href)
			if err != nil {
				t.Fatalf("адрес статьи %s: %v", link.href, err)
			}
			pages[articleURL] = readFixture(t, fixture)
		}
	}
	return pages
}

func pointsByMonth(points []minecPoint) map[string]float64 {
	byMonth := make(map[string]float64, len(points))
	for _, point := range points {
		byMonth[point.date.Format("2006-01")] = point.value
	}
	return byMonth
}

// TestMinecCollectIncremental — гейт start=2026-06: первая страница даёт пять
// новых месяцев (2026-06 … 2026-10), вторая — только известные (2026-01 … 2026-05),
// они не скачиваются, и обход останавливается на второй странице.
func TestMinecCollectIncremental(t *testing.T) {
	src := installPages(t, testPageSet(t, map[int]string{
		1: "materials_p1.html",
		2: "materials_p2.html",
		3: "materials_end.html",
	}))
	points, err := minecCollect(testListingURL, mustMonth(t, 2026, time.June))
	if err != nil {
		t.Fatalf("minecCollect: %v", err)
	}
	want := map[string]float64{
		"2026-10": 101.46,
		"2026-09": 92.08,
		"2026-08": 67.11,
		"2026-07": 59.02,
		"2026-06": 63.52,
	}
	got := pointsByMonth(points)
	if len(got) != len(want) {
		t.Fatalf("точек %d (%v), ожидалось %d", len(got), got, len(want))
	}
	for month, value := range want {
		if got[month] != value {
			t.Errorf("%s = %v, ожидалось %v", month, got[month], value)
		}
	}
	if n := src.calls[minecPageURL(testListingURL, 1)]; n != 1 {
		t.Errorf("первая страница листинга запрошена %d раз", n)
	}
	if n := src.calls[minecPageURL(testListingURL, 2)]; n != 1 {
		t.Errorf("вторая страница листинга запрошена %d раз", n)
	}
	if n := src.calls[minecPageURL(testListingURL, 3)]; n != 0 {
		t.Errorf("третья страница листинга запрошена %d раз: обход не остановился на известном месяце", n)
	}
	if n := articleCalls(src); n != 5 {
		t.Errorf("статей скачано %d, ожидалось 5: известные месяцы второй страницы не должны запрашиваться", n)
	}
}

// TestMinecCollectNoop — повтор в том же месяце: start новее последнего
// опубликованного, ни одна статья не запрашивается, точка не пишется.
func TestMinecCollectNoop(t *testing.T) {
	src := installPages(t, testPageSet(t, map[int]string{1: "materials_p1.html"}))
	points, err := minecCollect(testListingURL, mustMonth(t, 2026, time.November))
	if err != nil {
		t.Fatalf("minecCollect: %v", err)
	}
	if len(points) != 0 {
		t.Errorf("точек %d, ожидалось 0", len(points))
	}
	if n := articleCalls(src); n != 0 {
		t.Errorf("статей скачано %d, ожидалось 0", n)
	}
	if n := src.calls[minecPageURL(testListingURL, 1)]; n != 1 {
		t.Errorf("первая страница листинга запрошена %d раз, ожидался один", n)
	}
}

// TestMinecCollectInPageGate — гейт внутри страницы: start=2026-09, на первой
// странице скачиваются только октябрь и сентябрь, обход не идёт на вторую.
func TestMinecCollectInPageGate(t *testing.T) {
	src := installPages(t, testPageSet(t, map[int]string{1: "materials_p1.html"}))
	points, err := minecCollect(testListingURL, mustMonth(t, 2026, time.September))
	if err != nil {
		t.Fatalf("minecCollect: %v", err)
	}
	got := pointsByMonth(points)
	if len(got) != 2 || got["2026-10"] != 101.46 || got["2026-09"] != 92.08 {
		t.Errorf("точки %v, ожидались 2026-09 и 2026-10", got)
	}
	if n := articleCalls(src); n != 2 {
		t.Errorf("статей скачано %d, ожидалось 2", n)
	}
	if n := src.calls[minecPageURL(testListingURL, 2)]; n != 0 {
		t.Errorf("вторая страница листинга запрошена %d раз", n)
	}
}

// TestMinecCollectBackfill — пустая таблица: обход идёт до страницы «\n\n» и
// добирает всю страницу истории. Пустое тело — конец списка, а не ошибка.
func TestMinecCollectBackfill(t *testing.T) {
	src := installPages(t, testPageSet(t, map[int]string{
		1: "materials_p1.html",
		2: "materials_end.html",
	}))
	points, err := minecCollect(testListingURL, time.Time{})
	if err != nil {
		t.Fatalf("minecCollect: %v", err)
	}
	if len(points) != 5 {
		t.Errorf("точек %d, ожидалось 5", len(points))
	}
	if n := src.calls[minecPageURL(testListingURL, 2)]; n != 1 {
		t.Errorf("вторая страница листинга запрошена %d раз", n)
	}
}

// TestMinecCollectRejectsEmptyFirstPage — пустая первая страница — ошибка:
// иначе подменённый листинг выглядел бы как «истории нет» и бэкфилл молчал.
func TestMinecCollectRejectsEmptyFirstPage(t *testing.T) {
	installPages(t, testPageSet(t, map[int]string{1: "materials_end.html"}))
	if _, err := minecCollect(testListingURL, time.Time{}); err == nil {
		t.Fatal("пустая первая страница листинга разобрана без ошибки")
	}
}

// TestMinecCollectDuplicateMonth — один месяц двумя разными ссылками: источник
// противоречит сам себе, писать точку наугад нельзя.
func TestMinecCollectDuplicateMonth(t *testing.T) {
	dup := bytes.Replace(
		readFixture(t, "materials_p1.html"),
		[]byte("za_oktyabr_2026_goda.html"),
		[]byte("za_oktyabr_2026_goda_dup.html"),
		1,
	)
	pages := testPageSet(t, map[int]string{1: "materials_p1.html"})
	pages[minecPageURL(testListingURL, 2)] = dup
	installPages(t, pages)
	_, err := minecCollect(testListingURL, mustMonth(t, 2026, time.January))
	if err == nil {
		t.Fatal("дубль месяца с разными ссылками разобран без ошибки")
	}
	if !strings.Contains(err.Error(), "встречается дважды") {
		t.Errorf("ошибка не называет дубль: %v", err)
	}
}

// articleCalls — сколько раз запрашивались статьи (всё, что не листинг).
func articleCalls(src *pageSource) int {
	calls := 0
	for url, n := range src.calls {
		if !strings.Contains(url, "materials.html?page=") {
			calls += n
		}
	}
	return calls
}

// TestMinecGate — начало импорта по состоянию таблицы: пустая — вся история,
// иначе следующий месяц; дата max(date) может прийти в таймзоне сессии.
func TestMinecGate(t *testing.T) {
	if got := minecGate(0, time.Time{}); !got.IsZero() {
		t.Errorf("пустая таблица дала старт %s, ожидалась нулевая дата", got.Format("2006-01"))
	}
	last := time.Date(2026, time.October, 1, 0, 0, 0, 0, time.FixedZone("UTC+5", 5*3600))
	if got := minecGate(104, last); !got.Equal(mustMonth(t, 2026, time.November)) {
		t.Errorf("max(date)=2026-10 дал старт %s, ожидался 2026-11", got.Format("2006-01"))
	}
	// Декабрь переходит в январь следующего года.
	if got := minecGate(1, mustMonth(t, 2026, time.December)); !got.Equal(mustMonth(t, 2027, time.January)) {
		t.Errorf("max(date)=2026-12 дал старт %s, ожидался 2027-01", got.Format("2006-01"))
	}
}

// TestMinecStartMonthEnv — MINEC_URALS_FROM переопределяет гейт и служит путём
// ремонта; кривое значение — ошибка прогона, а не молчаливый полный бэкфилл.
func TestMinecStartMonthEnv(t *testing.T) {
	t.Setenv(minecFromEnv, "2018-05")
	start, err := minecStartMonth(context.Background(), nil)
	if err != nil {
		t.Fatalf("minecStartMonth: %v", err)
	}
	if !start.Equal(mustMonth(t, 2018, time.May)) {
		t.Errorf("старт %s, ожидался 2018-05", start.Format("2006-01"))
	}
	t.Setenv(minecFromEnv, "2018-13")
	if _, err := minecStartMonth(context.Background(), nil); err == nil {
		t.Error("некорректный MINEC_URALS_FROM разобран без ошибки")
	}
}

// minecTestBatch — заглушка driver.Batch: запоминает строки вместо записи в
// ClickHouse (стандарт §11.6).
type minecTestBatch struct {
	driver.Batch
	rows      [][]any
	appendErr error
	sent      bool
	aborted   bool
}

func (b *minecTestBatch) Append(v ...any) error {
	if b.appendErr != nil {
		return b.appendErr
	}
	b.rows = append(b.rows, v)
	return nil
}

func (b *minecTestBatch) Send() error  { b.sent = true; return nil }
func (b *minecTestBatch) Abort() error { b.aborted = true; return nil }
func (b *minecTestBatch) Flush() error { return nil }
func (b *minecTestBatch) Rows() int    { return len(b.rows) }

// minecTestConn — заглушка соединения: PrepareBatch отдаёт объявленный батч.
type minecTestConn struct {
	driver.Conn
	batch *minecTestBatch
	query string
}

func (c *minecTestConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.query = query
	return c.batch, nil
}

// TestMinecInsertPoints — все точки уходят одним батчем в macro_series, форма
// строки (source, series, date, value) — контракт таблицы.
func TestMinecInsertPoints(t *testing.T) {
	batch := &minecTestBatch{}
	conn := &minecTestConn{batch: batch}
	points := []minecPoint{
		{date: mustMonth(t, 2026, time.September), value: 92.08},
		{date: mustMonth(t, 2026, time.October), value: 101.46},
	}
	count, err := minecInsertPoints(context.Background(), conn, points)
	if err != nil {
		t.Fatalf("minecInsertPoints: %v", err)
	}
	if count != 2 {
		t.Errorf("count %d, ожидалось 2", count)
	}
	if !batch.sent {
		t.Error("батч не отправлен")
	}
	if conn.query != "INSERT INTO macro_series" {
		t.Errorf("запрос %q", conn.query)
	}
	if len(batch.rows) != 2 {
		t.Fatalf("строк в батче %d, ожидалось 2", len(batch.rows))
	}
	row := batch.rows[1]
	if len(row) != 4 {
		t.Fatalf("строка из %d значений, ожидалось 4", len(row))
	}
	if row[0] != minecSource || row[1] != minecUralsSeries {
		t.Errorf("строка %v, ожидались source=%s series=%s", row, minecSource, minecUralsSeries)
	}
	date, ok := row[2].(time.Time)
	if !ok || !date.Equal(mustMonth(t, 2026, time.October)) {
		t.Errorf("дата %v, ожидался 2026-10-01", row[2])
	}
	if row[3] != 101.46 {
		t.Errorf("значение %v, ожидалось 101.46", row[3])
	}
}

// TestMinecInsertPointsAbort — ошибка Append закрывает батч: соединение с
// открытым батчем не вернулось бы в пул.
func TestMinecInsertPointsAbort(t *testing.T) {
	batch := &minecTestBatch{appendErr: fmt.Errorf("append failed")}
	conn := &minecTestConn{batch: batch}
	_, err := minecInsertPoints(context.Background(), conn, []minecPoint{{date: mustMonth(t, 2026, time.October), value: 101.46}})
	if err == nil {
		t.Fatal("ошибка Append не вернулась")
	}
	if !batch.aborted {
		t.Error("батч не закрыт Abort после ошибки Append")
	}
	if batch.sent {
		t.Error("батч отправлен после ошибки Append")
	}
}
