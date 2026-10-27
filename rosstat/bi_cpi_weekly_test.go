package rosstat

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

func readBICpiWeeklyFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// withoutBICpiWeeklyBackoff убирает паузы между попытками: тест проверяет число повторов, а не
// их длительность (2 с + 5 с на каждый сбой растянули бы пакет).
func withoutBICpiWeeklyBackoff(t *testing.T) {
	t.Helper()
	sleep := biCpiWeeklySleep
	biCpiWeeklySleep = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { biCpiWeeklySleep = sleep })
}

func biCpiWeeklyDateOnly(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func TestBICpiWeeklyParseRussianFixture(t *testing.T) {
	body := readBICpiWeeklyFixture(t, "bi_cpi_weekly.csv")
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyParseCSV: %v", err)
	}
	want := []biCpiWeeklyPoint{
		{date: biCpiWeeklyDateOnly(2019, 1, 9), value: 100.5},
		{date: biCpiWeeklyDateOnly(2022, 1, 14), value: 100.1},
		{date: biCpiWeeklyDateOnly(2026, 10, 12), value: 100.11},
		{date: biCpiWeeklyDateOnly(2026, 10, 19), value: 100.05},
	}
	if len(points) != len(want) {
		t.Fatalf("точек %d, ожидалось %d: %+v", len(points), len(want), points)
	}
	for i := range want {
		if !points[i].date.Equal(want[i].date) || points[i].value != want[i].value {
			t.Errorf("точка %d = %s %.2f, ожидалось %s %.2f", i, points[i].date.Format("2006-01-02"), points[i].value, want[i].date.Format("2006-01-02"), want[i].value)
		}
	}
}

func TestBICpiWeeklyParseEnglishFixture(t *testing.T) {
	body := readBICpiWeeklyFixture(t, "bi_cpi_weekly_en.csv")
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyParseCSV: %v", err)
	}
	if len(points) != 4 {
		t.Fatalf("точек %d, ожидалось 4: %+v", len(points), points)
	}
	if !points[3].date.Equal(biCpiWeeklyDateOnly(2026, 10, 19)) || points[3].value != 100.05 {
		t.Errorf("spot = %s %.2f, ожидалось 2026-10-19 100.05", points[3].date.Format("2006-01-02"), points[3].value)
	}
}

func TestBICpiWeeklyAppendWritesAggregateSeries(t *testing.T) {
	body := readBICpiWeeklyFixture(t, "bi_cpi_weekly.csv")
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyParseCSV: %v", err)
	}
	rec := &appendRecorder{}
	if err = biCpiWeeklyAppend(points, rec); err != nil {
		t.Fatalf("biCpiWeeklyAppend: %v", err)
	}
	if len(rec.rows) != 4 {
		t.Fatalf("Append вызван %d раз, ожидалось 4", len(rec.rows))
	}
	for _, row := range rec.rows {
		if row.series != ipcWeeksBulletinsAggregateSeries {
			t.Errorf("series = %q, ожидался %q", row.series, ipcWeeksBulletinsAggregateSeries)
		}
		if row.valueType != "float64" {
			t.Errorf("valueType = %s, ожидался float64", row.valueType)
		}
	}
}

func TestBICpiWeeklyParseSortsAndDedupes(t *testing.T) {
	body := []byte(`"period","id_indicator","territory","value"
"12.10.26","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.11"
"09.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.5"
"09.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.6"
`)
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyParseCSV: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("точек %d, ожидалось 2: %+v", len(points), points)
	}
	if !points[0].date.Equal(biCpiWeeklyDateOnly(2019, 1, 9)) || points[0].value != 100.6 {
		t.Errorf("первая точка = %s %.2f, ожидалось 2019-01-09 100.6 (последнее значение дубликата)", points[0].date.Format("2006-01-02"), points[0].value)
	}
	if !points[1].date.Equal(biCpiWeeklyDateOnly(2026, 10, 12)) || points[1].value != 100.11 {
		t.Errorf("вторая точка = %s %.2f, ожидалось 2026-10-12 100.11", points[1].date.Format("2006-01-02"), points[1].value)
	}
}

func TestBICpiWeeklyParseAllowsNewYearGap(t *testing.T) {
	body := []byte(`"period","id_indicator","territory","value"
"09.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.5"
"09.01.20","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.4"
`)
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyParseCSV: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("точек %d, ожидалось 2: %+v", len(points), points)
	}
}

func TestBICpiWeeklyParseErrors(t *testing.T) {
	header := `"period","id_indicator","territory","value"` + "\n"
	cases := map[string]string{
		"empty":            "",
		"header only":      header,
		"bad date":         header + `"not-a-date","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.5"` + "\n",
		"bad value":        header + `"09.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","abc"` + "\n",
		"NaN":              header + `"09.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","NaN"` + "\n",
		"Inf":              header + `"09.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","+Inf"` + "\n",
		"wrong first date": header + `"10.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.5"` + "\n",
		"wrong indicator":  header + `"09.01.19","Другой показатель","Российская Федерация","100.5"` + "\n",
		"wrong territory":  header + `"09.01.19","Оценка индекса потребительских цен за неделю, в %","Германия","100.5"` + "\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := biCpiWeeklyParseCSV([]byte(body)); err == nil {
				t.Fatalf("ожидалась ошибка, получен успешный разбор")
			}
		})
	}
}

func TestBICpiWeeklyExportPath(t *testing.T) {
	body := []byte(`//OK[1,["tmp/expe7371a09-0ba7-48f4-bb81-2b4847df6ff7/Индекс потребительских цен.csv"],0,7]`)
	path, err := biCpiWeeklyExportPath(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyExportPath: %v", err)
	}
	want := "tmp/expe7371a09-0ba7-48f4-bb81-2b4847df6ff7/Индекс потребительских цен.csv"
	if path != want {
		t.Errorf("path = %q, ожидался %q", path, want)
	}
}

func TestBICpiWeeklyExportPathErrors(t *testing.T) {
	for name, body := range map[string]string{
		"not ok":    `//EX[1,["err"]]`,
		"not json":  `//OKnot-json`,
		"no path":   `//OK[1,[],0,7]`,
		"file only": `//OK[1,["tmp/exp123/file.txt"],0,7]`,
		"traversal": `//OK[1,["tmp/exp123/../../etc/passwd.csv"],0,7]`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := biCpiWeeklyExportPath([]byte(body)); err == nil {
				t.Fatalf("ожидалась ошибка для %q", body)
			}
		})
	}
}

func TestBICpiWeeklyPublicURL(t *testing.T) {
	csvURL, err := biCpiWeeklyPublicURL("tmp/exp123/Индекс потребительских цен.csv")
	if err != nil {
		t.Fatalf("biCpiWeeklyPublicURL: %v", err)
	}
	u, err := url.Parse(csvURL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	if u.Host != "bi.rosstat.gov.ru" {
		t.Errorf("host = %q", u.Host)
	}
	if want := "/biportal/tmp/exp123/Индекс потребительских цен.csv"; u.Path != want {
		t.Errorf("path = %q, ожидался %q", u.Path, want)
	}
	if !strings.Contains(csvURL, "%20") || !strings.Contains(csvURL, "%D0%98") {
		t.Errorf("URL не закодирован: %s", csvURL)
	}
}

func TestBICpiWeeklyPublicURLRejects(t *testing.T) {
	for _, path := range []string{
		"tmp/exp123/../../etc/passwd.csv",
		"tmp/exp123/file.txt",
		"other/file.csv",
		"",
	} {
		if _, err := biCpiWeeklyPublicURL(path); err == nil {
			t.Errorf("ожидалась ошибка для пути %q", path)
		}
	}
}

func TestBICpiWeeklyAppendReturnsBatchError(t *testing.T) {
	body := readBICpiWeeklyFixture(t, "bi_cpi_weekly.csv")
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyParseCSV: %v", err)
	}
	rec := &appendRecorder{appendErr: os.ErrClosed}
	if err = biCpiWeeklyAppend(points, rec); err == nil {
		t.Fatal("ожидалась ошибка Append")
	}
}

func TestBICpiWeeklyParseCRLFAndBOM(t *testing.T) {
	body := append([]byte("\ufeff"), []byte(strings.ReplaceAll(`"period","id_indicator","territory","value"
"09.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.5"
`, "\n", "\r\n"))...)
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyParseCSV: %v", err)
	}
	if len(points) != 1 || points[0].value != 100.5 {
		t.Fatalf("точки = %+v", points)
	}
}

// withBICpiWeeklyServer подменяет endpoint экспорта и базовый URL публичного tmp-CSV
// httptest-сервером и возвращает их обратно после теста.
func withBICpiWeeklyServer(t *testing.T, server *httptest.Server) {
	t.Helper()
	oldExport, oldBase := biCpiWeeklyExportURL, biCpiWeeklyPublicBase
	biCpiWeeklyExportURL = server.URL + "/grid"
	biCpiWeeklyPublicBase = server.URL
	t.Cleanup(func() {
		biCpiWeeklyExportURL = oldExport
		biCpiWeeklyPublicBase = oldBase
	})
}

// hijackClose рвёт соединение без ответа — ровно то, что портал сделал 2026-10-21 (`EOF`).
func hijackClose(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Errorf("hijack: %v", err)
		return
	}
	_ = conn.Close()
}

func TestBICpiWeeklyCSVURL(t *testing.T) {
	withoutBICpiWeeklyBackoff(t)
	fixture := readBICpiWeeklyFixture(t, "bi_cpi_weekly.csv")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/grid":
			if r.Method != http.MethodPost {
				t.Errorf("метод %q, ожидался POST", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`//OK[1,["tmp/exp123/Индекс потребительских цен.csv"],0,7]`))
		case strings.HasSuffix(r.URL.Path, "/Индекс потребительских цен.csv"):
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write(fixture)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	withBICpiWeeklyServer(t, server)

	csvURL, err := biCpiWeeklyCSVURL(context.Background())
	if err != nil {
		t.Fatalf("biCpiWeeklyCSVURL: %v", err)
	}
	if !strings.HasPrefix(csvURL, server.URL+"/biportal/tmp/exp123/") {
		t.Fatalf("URL %q не ведёт на тестовый tmp-путь", csvURL)
	}
	body, err := util.GetFileBytes(csvURL)
	if err != nil {
		t.Fatalf("GetFileBytes: %v", err)
	}
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyParseCSV: %v", err)
	}
	if len(points) != 4 {
		t.Fatalf("точек %d, ожидалось 4", len(points))
	}
}

// TestBICpiWeeklyExportRetriesOnEOF — портал рвёт соединение один раз (ровно так дефект и
// выглядел 2026-10-21): вторая попытка обязана восстановить прогон, а не уронить его.
func TestBICpiWeeklyExportRetriesOnEOF(t *testing.T) {
	withoutBICpiWeeklyBackoff(t)
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			hijackClose(t, w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`//OK[1,["tmp/exp123/Индекс потребительских цен.csv"],0,7]`))
	}))
	defer server.Close()
	withBICpiWeeklyServer(t, server)

	csvURL, err := biCpiWeeklyCSVURL(context.Background())
	if err != nil {
		t.Fatalf("biCpiWeeklyCSVURL после повтора: %v", err)
	}
	if !strings.HasPrefix(csvURL, server.URL+"/biportal/tmp/exp123/") {
		t.Fatalf("URL %q не ведёт на тестовый tmp-путь", csvURL)
	}
	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Fatalf("попыток %d, ожидалось 2 (один обрыв + успех)", got)
	}
}

// TestBICpiWeeklyExportFailsAfterAttempts — источник рвёт всегда: ошибка обязана быть явной,
// называть число попыток и не превращаться в «0 строк».
func TestBICpiWeeklyExportFailsAfterAttempts(t *testing.T) {
	withoutBICpiWeeklyBackoff(t)
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempts, 1)
		hijackClose(t, w)
	}))
	defer server.Close()
	withBICpiWeeklyServer(t, server)

	_, err := biCpiWeeklyCSVURL(context.Background())
	if err == nil {
		t.Fatal("ожидалась ошибка после исчерпания попыток")
	}
	if got := atomic.LoadInt32(&attempts); got != int32(biCpiWeeklyAttempts) {
		t.Errorf("попыток %d, ожидалось %d", got, biCpiWeeklyAttempts)
	}
	if !strings.Contains(err.Error(), "попыток") {
		t.Errorf("ошибка %q не называет число попыток", err)
	}
}

func TestBICpiWeeklyCSVURLRPCError(t *testing.T) {
	withoutBICpiWeeklyBackoff(t)
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempts, 1)
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer server.Close()
	withBICpiWeeklyServer(t, server)

	if _, err := biCpiWeeklyCSVURL(context.Background()); err == nil {
		t.Fatal("ожидалась ошибка на не-2xx RPC")
	}
	if got := atomic.LoadInt32(&attempts); got != int32(biCpiWeeklyAttempts) {
		t.Errorf("RPC-вызовов %d, ожидалось %d", got, biCpiWeeklyAttempts)
	}
}

func TestBICpiWeeklyParseRequiresIndicatorHeader(t *testing.T) {
	body := []byte(`"period","territory","value"
"09.01.19","Российская Федерация","100.5"
`)
	if _, err := biCpiWeeklyParseCSV(body); err == nil {
		t.Fatal("ожидалась ошибка: в шапке нет id_indicator")
	}
}

func TestBICpiWeeklyParseStableDuplicate(t *testing.T) {
	body := []byte(`"period","id_indicator","territory","value"
"09.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.5"
"03.02.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.9"
"27.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.1"
"03.02.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.2"
"20.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.6"
"13.01.19","Оценка индекса потребительских цен за неделю, в %","Российская Федерация","100.7"
`)
	points, err := biCpiWeeklyParseCSV(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyParseCSV: %v", err)
	}
	if len(points) != 5 {
		t.Fatalf("точек %d, ожидалось 5 (дубликат схлопнут): %+v", len(points), points)
	}
	last := points[len(points)-1]
	if !last.date.Equal(biCpiWeeklyDateOnly(2019, 2, 3)) || last.value != 100.2 {
		t.Errorf("last = %s %.2f, ожидалось 2019-02-03 100.2 (последнее значение дубликата)", last.date.Format("2006-01-02"), last.value)
	}
}

// TestBICpiWeeklyExportPathLiveFixture — разбор живого ответа RPC (снят 2026-10-27):
// отдельная фикстура держит контракт экспорта независимо от inline-строк остальных тестов.
func TestBICpiWeeklyExportPathLiveFixture(t *testing.T) {
	body := readBICpiWeeklyFixture(t, "bi_cpi_weekly_rpc_ok.txt")
	path, err := biCpiWeeklyExportPath(body)
	if err != nil {
		t.Fatalf("biCpiWeeklyExportPath: %v", err)
	}
	if !strings.HasPrefix(path, "tmp/exp") || !strings.HasSuffix(path, "Индекс потребительских цен.csv") {
		t.Fatalf("путь %q не похож на живой экспорт", path)
	}
}

// TestBICpiWeeklyFingerprint — версия содержимого, а не файла: постоянна на повторе, не зависит
// от порядка строк (портал отдаёт их произвольно) и меняется от значения точки.
func TestBICpiWeeklyFingerprint(t *testing.T) {
	first := biCpiWeeklyPoint{date: biCpiWeeklyDateOnly(2019, 1, 9), value: 100.5}
	last := biCpiWeeklyPoint{date: biCpiWeeklyDateOnly(2026, 10, 12), value: 100.11}
	base := []biCpiWeeklyPoint{first, last}

	fingerprint := biCpiWeeklyFingerprint(base)
	if len(fingerprint) != 64 {
		t.Errorf("длина отпечатка %d, ожидалось 64 (sha256 hex)", len(fingerprint))
	}
	if again := biCpiWeeklyFingerprint(base); again != fingerprint {
		t.Errorf("отпечаток нестабилен: %s != %s", again, fingerprint)
	}
	if reordered := biCpiWeeklyFingerprint([]biCpiWeeklyPoint{last, first}); reordered != fingerprint {
		t.Errorf("отпечаток зависит от порядка строк: %s != %s", reordered, fingerprint)
	}
	changed := biCpiWeeklyFingerprint([]biCpiWeeklyPoint{{date: first.date, value: 100.6}, last})
	if changed == fingerprint {
		t.Error("отпечаток не изменился при смене значения точки")
	}
	if biCpiWeeklyFingerprint(nil) != biCpiWeeklyFingerprint([]biCpiWeeklyPoint{}) {
		t.Error("отпечаток пустого ряда нестабилен")
	}
}

// TestBICpiWeeklyShouldSkip — no-op только при известной версии и совпавшем числе точек;
// расхождение уводит в полный импорт (self-heal), а неизвестная версия — всегда импорт.
func TestBICpiWeeklyShouldSkip(t *testing.T) {
	cases := []struct {
		name        string
		known       bool
		knownPoints uint64
		dbPoints    uint64
		want        bool
	}{
		{"версия неизвестна", false, 0, 0, false},
		{"версия неизвестна, точки есть", false, 399, 399, false},
		{"версия известна, точки совпали", true, 399, 399, true},
		{"версия известна, точек меньше", true, 399, 398, false},
		{"версия известна, точек больше", true, 398, 399, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := biCpiWeeklyShouldSkip(tc.known, tc.knownPoints, tc.dbPoints); got != tc.want {
				t.Errorf("biCpiWeeklyShouldSkip(%v, %d, %d) = %v, ожидалось %v",
					tc.known, tc.knownPoints, tc.dbPoints, got, tc.want)
			}
		})
	}
}

// biCpiWeeklyTestBatch — батч для тестов Import(): appendRecorder пишет строки, но не умеет
// притворяться пустым и не считает Abort/Send. Встраивание оставляет остальные методы батча.
type biCpiWeeklyTestBatch struct {
	*appendRecorder
	rowsOverride int
	rowsSet      bool
	aborts       int
	sends        int
}

func (b *biCpiWeeklyTestBatch) Rows() int {
	if b.rowsSet {
		return b.rowsOverride
	}
	return b.appendRecorder.Rows()
}

func (b *biCpiWeeklyTestBatch) Abort() error { b.aborts++; return nil }

func (b *biCpiWeeklyTestBatch) Send() error { b.sends++; return nil }

// biCpiWeeklyFakeConn — соединение для тестов Import() без БД (стандарт §11.3): отвечает на
// DDL/Exec, версию и count из заготовленных значений и отдаёт подставленные батчи (точки и
// строка версии). Встраивание driver.Conn оставляет необъявленные методы nil — их вызов паникой
// покажет обращение, которого фейк не обещал (прецедент — dipFakeConn, cbr/dip_bulletin_test.go).
type biCpiWeeklyFakeConn struct {
	driver.Conn
	execs      []string
	batchQuery string
	batches    int
	batch      driver.Batch
	pubBatch   driver.Batch // приёмник INSERT INTO source_publications
	queryRows  []uint64     // ответы QueryRow по порядку: сначала версия, затем count
	queryIdx   int
	rowErr     error // ошибка первого QueryRow (версия неизвестна), дальше ответы штатные
}

func (c *biCpiWeeklyFakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return nil
}

func (c *biCpiWeeklyFakeConn) QueryRow(_ context.Context, _ string, _ ...any) driver.Row {
	points := uint64(0)
	if c.queryIdx < len(c.queryRows) {
		points = c.queryRows[c.queryIdx]
	}
	err := error(nil)
	if c.queryIdx == 0 {
		err = c.rowErr
	}
	c.queryIdx++
	return biCpiWeeklyFakeRow{points: points, err: err}
}

func (c *biCpiWeeklyFakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.batches++
	c.batchQuery = query
	if strings.Contains(query, util.PublicationsTable) {
		return c.pubBatch, nil
	}
	return c.batch, nil
}

// biCpiWeeklyFakeRow — одна uint64-колонка: и число точек версии, и count() читаются в *uint64.
type biCpiWeeklyFakeRow struct {
	driver.Row
	points uint64
	err    error
}

func (r biCpiWeeklyFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("biCpiWeeklyFakeRow: Scan с %d аргументами, ожидался 1", len(dest))
	}
	points, ok := dest[0].(*uint64)
	if !ok {
		return fmt.Errorf("biCpiWeeklyFakeRow: приёмник %T, ожидался *uint64", dest[0])
	}
	*points = r.points
	return nil
}

// newBICpiWeeklyImportFixture поднимает тестовый экспорт BI и возвращает фейковое соединение:
// Import() идёт своим циклом, поэтому без подмены endpoint тест ходил бы в сеть.
func newBICpiWeeklyImportFixture(t *testing.T) *biCpiWeeklyFakeConn {
	t.Helper()
	withoutBICpiWeeklyBackoff(t)
	fixture := readBICpiWeeklyFixture(t, "bi_cpi_weekly.csv")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/grid" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`//OK[1,["tmp/exp123/Индекс потребительских цен.csv"],0,7]`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/Индекс потребительских цен.csv") {
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write(fixture)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	withBICpiWeeklyServer(t, server)
	return &biCpiWeeklyFakeConn{}
}

// TestBICpiWeeklyImportNoOpWritesNothing — версия известна и count() сошёлся: Import возвращает
// 0 без INSERT (в логе `Imported 0 rows`), но перепубликовывает производные. Без AfterImport в
// тестовом импортёре производные не трогаются, и это заодно проверяет nil-guard publish().
func TestBICpiWeeklyImportNoOpWritesNothing(t *testing.T) {
	conn := newBICpiWeeklyImportFixture(t)
	conn.queryRows = []uint64{4, 4}
	batch := &biCpiWeeklyTestBatch{appendRecorder: &appendRecorder{}}
	conn.batch = batch
	importer := &BICpiWeeklyStat{ClickHouseImport: util.ClickHouseImport{TableName: rosstatBICpiWeeklyTable}}

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import на известной версии: %v", err)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк, ожидалось 0 (no-op)", count)
	}
	if conn.batches != 0 {
		t.Errorf("PrepareBatch вызван %d раз, ожидалось 0: no-op не пишет в таблицу", conn.batches)
	}
	if batch.sends != 0 || batch.aborts != 0 {
		t.Errorf("Send=%d Abort=%d, ожидалось 0/0", batch.sends, batch.aborts)
	}
}

// TestBICpiWeeklyImportEmptyInsertIsError — инвариант приёмки: полный прогон, не записавший
// ни одной точки, обязан упасть, а не отчитаться «Imported 0 rows». Батч притворяется пустым,
// хотя точки в него уже ушли: так проверяется именно гейт `inserted == 0`, а не парсер.
func TestBICpiWeeklyImportEmptyInsertIsError(t *testing.T) {
	conn := newBICpiWeeklyImportFixture(t)
	conn.rowErr = sql.ErrNoRows // версия неизвестна — полный путь
	conn.batch = &biCpiWeeklyTestBatch{appendRecorder: &appendRecorder{}, rowsOverride: 0, rowsSet: true}
	importer := &BICpiWeeklyStat{ClickHouseImport: util.ClickHouseImport{TableName: rosstatBICpiWeeklyTable}}

	_, err := importer.Import(context.Background(), conn)
	if err == nil {
		t.Fatal("ожидалась ошибка: полный прогон без точек")
	}
	if !strings.Contains(err.Error(), "не дал точек") {
		t.Errorf("ошибка %q не называет пустой прогон", err)
	}
}

// TestBICpiWeeklyImportAbortsBatchOnAppendError — сбой Append закрывает батч (правило 2) и не
// доходит до Send: иначе соединение драйвера осталось бы в пуле с открытым батчем.
func TestBICpiWeeklyImportAbortsBatchOnAppendError(t *testing.T) {
	conn := newBICpiWeeklyImportFixture(t)
	conn.rowErr = sql.ErrNoRows
	batch := &biCpiWeeklyTestBatch{appendRecorder: &appendRecorder{appendErr: os.ErrClosed}}
	conn.batch = batch
	importer := &BICpiWeeklyStat{ClickHouseImport: util.ClickHouseImport{TableName: rosstatBICpiWeeklyTable}}

	if _, err := importer.Import(context.Background(), conn); err == nil {
		t.Fatal("ожидалась ошибка Append")
	}
	if batch.aborts != 1 {
		t.Errorf("Abort вызван %d раз, ожидался 1", batch.aborts)
	}
	if batch.sends != 0 {
		t.Errorf("Send вызван %d раз, ожидалось 0", batch.sends)
	}
}

// TestBICpiWeeklyImportRejectsRestoreMode — архив версий BI не ведётся, поэтому режим
// восстановления обязан отказать до DDL и до сети, а не пойти молча полным живым импортом.
func TestBICpiWeeklyImportRejectsRestoreMode(t *testing.T) {
	t.Setenv(util.EnvArchiveAction, util.ArchiveActionRestore)
	conn := &biCpiWeeklyFakeConn{}
	importer := &BICpiWeeklyStat{ClickHouseImport: util.ClickHouseImport{TableName: rosstatBICpiWeeklyTable}}

	_, err := importer.Import(context.Background(), conn)
	if err == nil {
		t.Fatal("ожидалась ошибка режима восстановления")
	}
	if !strings.Contains(err.Error(), "режим восстановления") {
		t.Errorf("ошибка %q не называет режим восстановления", err)
	}
	if len(conn.execs) != 0 || conn.batches != 0 {
		t.Errorf("в режиме restore дошли до БД: Exec=%d PrepareBatch=%d", len(conn.execs), conn.batches)
	}
}

// TestBICpiWeeklyFingerprintPinned — литерал отпечатка: каноническая строка (дата, кратчайшее
// представление float) закреплена здесь. Смена формата значения (`%v`, `.17g`, локаль) сменила бы
// версию всех точек и дала бы однократный полный импорт — тест обязан упасть раньше.
func TestBICpiWeeklyFingerprintPinned(t *testing.T) {
	points := []biCpiWeeklyPoint{
		{date: biCpiWeeklyDateOnly(2019, 1, 9), value: 100.5},
		{date: biCpiWeeklyDateOnly(2026, 10, 19), value: 100.05},
	}
	const want = "fc0b6cb45ef1c41de7d83c2fc79a48dbca5d6dcf0e862097d85dc8ffe79f1a80"
	if got := biCpiWeeklyFingerprint(points); got != want {
		t.Fatalf("отпечаток %q, ожидался %q", got, want)
	}
}

// biCpiWeeklyPermissiveBatch — приёмник строки версии: util.SavePublication шлёт шесть колонок,
// поэтому appendRecorder (строго три) здесь не годится.
type biCpiWeeklyPermissiveBatch struct {
	driver.Batch
	appends int
	sends   int
	aborts  int
}

func (b *biCpiWeeklyPermissiveBatch) Append(...any) error { b.appends++; return nil }
func (b *biCpiWeeklyPermissiveBatch) Rows() int           { return 1 }
func (b *biCpiWeeklyPermissiveBatch) Send() error         { b.sends++; return nil }
func (b *biCpiWeeklyPermissiveBatch) Abort() error        { b.aborts++; return nil }

// TestBICpiWeeklyImportWritesFullPath — полный путь (версия неизвестна): все точки уходят в батч
// своей таблицы, Send один, строка версии пишется после точек, Import возвращает их число.
func TestBICpiWeeklyImportWritesFullPath(t *testing.T) {
	conn := newBICpiWeeklyImportFixture(t)
	conn.rowErr = sql.ErrNoRows     // версия неизвестна — полный путь
	conn.queryRows = []uint64{0, 4} // count() после записи
	batch := &biCpiWeeklyTestBatch{appendRecorder: &appendRecorder{}}
	pub := &biCpiWeeklyPermissiveBatch{}
	conn.batch, conn.pubBatch = batch, pub
	importer := &BICpiWeeklyStat{ClickHouseImport: util.ClickHouseImport{TableName: rosstatBICpiWeeklyTable}}

	count, err := importer.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import полного пути: %v", err)
	}
	if count != 4 {
		t.Errorf("Import вернул %d строк, ожидалось 4", count)
	}
	if len(batch.rows) != 4 || batch.sends != 1 {
		t.Errorf("точек в батче %d, Send=%d, ожидалось 4/1", len(batch.rows), batch.sends)
	}
	if conn.batches != 2 {
		t.Errorf("PrepareBatch вызван %d раз, ожидалось 2 (точки + версия)", conn.batches)
	}
	if pub.appends != 1 || pub.sends != 1 {
		t.Errorf("строка версии: Append=%d Send=%d, ожидалось 1/1", pub.appends, pub.sends)
	}
}
