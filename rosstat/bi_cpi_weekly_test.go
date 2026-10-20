package rosstat

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if len(points) != 3 {
		t.Fatalf("точек %d, ожидалось 3: %+v", len(points), points)
	}
	if !points[2].date.Equal(biCpiWeeklyDateOnly(2026, 10, 12)) || points[2].value != 100.11 {
		t.Errorf("spot = %s %.2f, ожидалось 2026-10-12 100.11", points[2].date.Format("2006-01-02"), points[2].value)
	}
}

func TestBICpiWeeklyBodyFuncWritesAggregateSeries(t *testing.T) {
	body := readBICpiWeeklyFixture(t, "bi_cpi_weekly.csv")
	rec := &appendRecorder{}
	if err := biCpiWeeklyImport(body, rec); err != nil {
		t.Fatalf("biCpiWeeklyImport: %v", err)
	}
	if len(rec.rows) != 3 {
		t.Fatalf("Append вызван %d раз, ожидалось 3", len(rec.rows))
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

func TestBICpiWeeklyImportReturnsBatchError(t *testing.T) {
	body := readBICpiWeeklyFixture(t, "bi_cpi_weekly.csv")
	rec := &appendRecorder{appendErr: os.ErrClosed}
	if err := biCpiWeeklyImport(body, rec); err == nil {
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

func TestBICpiWeeklyCSVURL(t *testing.T) {
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

	oldExport, oldBase := biCpiWeeklyExportURL, biCpiWeeklyPublicBase
	biCpiWeeklyExportURL = server.URL + "/grid"
	biCpiWeeklyPublicBase = server.URL
	t.Cleanup(func() {
		biCpiWeeklyExportURL = oldExport
		biCpiWeeklyPublicBase = oldBase
	})

	csvURL, err := biCpiWeeklyCSVURL()
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
	if len(points) != 3 {
		t.Fatalf("точек %d, ожидалось 3", len(points))
	}
}

func TestBICpiWeeklyCSVURLRPCError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer server.Close()

	oldExport, oldBase := biCpiWeeklyExportURL, biCpiWeeklyPublicBase
	biCpiWeeklyExportURL = server.URL + "/grid"
	biCpiWeeklyPublicBase = server.URL
	t.Cleanup(func() {
		biCpiWeeklyExportURL = oldExport
		biCpiWeeklyPublicBase = oldBase
	})

	if _, err := biCpiWeeklyCSVURL(); err == nil {
		t.Fatal("ожидалась ошибка на не-2xx RPC")
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
