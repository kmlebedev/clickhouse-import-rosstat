package fns

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// fnsFormsPath — путь страницы форм на тестовом сервере: тот же, что у источника,
// потому что годовые ссылки разрешаются относительно него.
const fnsFormsPath = "/rn77/related_activities/statistics_and_analytics/forms/"

// fnsFixturePage — HTML-фикстура страницы источника.
func fnsFixturePage(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", name, err)
	}
	return data
}

// fnsCounters — счётчик обращений тестового сервера: по нему видно, скачал ли
// импортёр лишний архив и не ходит ли он за страницами лет раньше стартового.
type fnsCounters struct {
	lock  sync.Mutex
	pages int
	files int
	years map[int]int
}

// fnsTestServer — сервер с разметкой страницы форм, страницей года и архивом вместо
// настоящих nalog.gov.ru. overrides задаёт содержимое по точному пути; остальное
// отдаётся фикстурами.
func fnsTestServer(t *testing.T, overrides map[string][]byte) (*httptest.Server, *fnsCounters) {
	t.Helper()
	counters := &fnsCounters{years: map[int]int{}}
	forms := fnsFixturePage(t, "forms_page.html")
	year2026 := fnsFixturePage(t, "year_page_2026.html")
	year2021 := fnsFixturePage(t, "year_page_2021.html")
	archive := fnsFixturePage(t, "1nm010926reg.zip")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if body, ok := overrides[path]; ok {
			fnsWriteRespond(w, path, body, counters)
			return
		}
		switch {
		case path == fnsFormsPath:
			fnsWriteRespond(w, path, forms, counters)
		case path == fnsFormsPath+"16619097/":
			fnsWriteRespond(w, path, year2026, counters)
		case path == fnsFormsPath+"10973378/":
			fnsWriteRespond(w, path, year2021, counters)
		case strings.HasPrefix(path, fnsFormsPath):
			// Страница года, которой нет среди фикстур: пустой список архивов.
			id := strings.Trim(path[len(fnsFormsPath):], "/")
			if year, err := strconv.Atoi(id); err == nil {
				counters.lock.Lock()
				counters.years[year]++
				counters.lock.Unlock()
			}
			fnsWriteRespond(w, path, []byte("<html><body>пусто</body></html>"), counters)
		case strings.HasSuffix(path, fnsRegZipMarker):
			fnsWriteRespond(w, path, archive, counters)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server, counters
}

// fnsWriteRespond — ответ с учётом обращений: страницы считаются страницами, архивы —
// архивами.
func fnsWriteRespond(w http.ResponseWriter, path string, body []byte, counters *fnsCounters) {
	counters.lock.Lock()
	if strings.HasSuffix(path, fnsRegZipMarker) {
		counters.files++
	} else {
		counters.pages++
	}
	counters.lock.Unlock()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(body)
}

// TestFnsYearPages — со строки 1-НМ блока сводных отчётов снимаются годовые страницы;
// ссылки открытых данных в той же строке (абсолютные URL) в выборку не попадают.
func TestFnsYearPages(t *testing.T) {
	const formsURL = "https://www.nalog.gov.ru" + fnsFormsPath
	years, err := fnsYearPages(fnsFixturePage(t, "forms_page.html"), formsURL)
	if err != nil {
		t.Fatalf("годовые ссылки: %v", err)
	}
	if len(years) != 21 {
		t.Errorf("годовых ссылок %d, ожидалось 21 (2006…2026)", len(years))
	}
	got := map[int]string{}
	for _, year := range years {
		if previous, ok := got[year.year]; ok {
			t.Errorf("год %d встречается дважды: %q и %q", year.year, previous, year.url)
		}
		got[year.year] = year.url
		if !strings.HasPrefix(year.url, formsURL) || !strings.HasSuffix(year.url, "/") {
			t.Errorf("год %d: адрес %q не достроен от страницы форм", year.year, year.url)
		}
		if strings.Contains(year.url, "opendata") {
			t.Errorf("год %d: адрес %q — ссылка открытых данных, а не страница года", year.year, year.url)
		}
	}
	// Адреса сверяются с живым наблюдением: id страниц непрозрачны и меняются ежегодно.
	for year, want := range map[int]string{
		2020: "9777509", 2021: "10973378", 2022: "11912732", 2023: "13669389",
		2024: "14969328", 2025: "16031004", 2026: "16619097",
	} {
		if !strings.HasSuffix(got[year], want+"/") {
			t.Errorf("год %d: ссылка %q, ожидалась …/%s/", year, got[year], want)
		}
	}
}

// TestFnsYearPagesFailFast — нет строки формы или нет ни одной годовой ссылки в ней —
// разметка изменилась: прогон обязан упасть, а не отчитаться «нулём месяцев».
func TestFnsYearPagesFailFast(t *testing.T) {
	const formsURL = "https://www.nalog.gov.ru" + fnsFormsPath
	page := string(fnsFixturePage(t, "forms_page.html"))
	if _, err := fnsYearPages([]byte(strings.ReplaceAll(page, "1-НМ", "9-ХМ")), formsURL); err == nil {
		t.Error("строка формы не найдена, а ошибки нет")
	}
	stripped := page
	for _, id := range []string{"3832158", "3832160", "3832163", "3827841", "3827842", "3800184",
		"4001025", "4185658", "4621635", "5432274", "6040192", "6772396", "7600100", "8824368",
		"9777509", "10973378", "11912732", "13669389", "14969328", "16031004", "16619097"} {
		stripped = strings.ReplaceAll(stripped, `href="`+id+`/"`, `href="#"`)
	}
	if _, err := fnsYearPages([]byte(stripped), formsURL); err == nil {
		t.Error("годовые ссылки срезаны, а ошибки нет — импортёр отчитался бы нулём точек")
	}
}

// TestFnsMonthliesFixturePages — разбор списков архивов с двух живых страниц года:
// 2026 даёт восемь месяцев (состояние на 2026-11-03), 2021 — двенадцать, из них
// четыре месяца приходят только в форме ревизий `ut_…`.
func TestFnsMonthliesFixturePages(t *testing.T) {
	artifacts := fnsMonthlies(fnsFixturePage(t, "year_page_2026.html"), "https://www.nalog.gov.ru"+fnsFormsPath+"16619097/")
	if len(artifacts) != 8 {
		t.Fatalf("артефактов %d, ожидалось 8", len(artifacts))
	}
	months := map[string]bool{}
	for _, artifact := range artifacts {
		months[artifact.month.Format("2006-01")] = true
		if artifact.revision {
			t.Errorf("%s помечен ревизией, а в имени нет ut", artifact.name)
		}
		if !strings.HasPrefix(artifact.url, "https://www.nalog.gov.ru/html/sites/") {
			t.Errorf("адрес %q не достроен от хоста страницы", artifact.url)
		}
	}
	for _, month := range []string{"2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06", "2026-07", "2026-08"} {
		if !months[month] {
			t.Errorf("месяц %s не найден среди артефактов", month)
		}
	}

	early := fnsMonthlies(fnsFixturePage(t, "year_page_2021.html"), "https://www.nalog.gov.ru"+fnsFormsPath+"10973378/")
	if len(early) != 12 {
		t.Fatalf("артефактов 2021 года %d, ожидалось 12", len(early))
	}
	revisions := map[string]bool{}
	for _, artifact := range early {
		if artifact.revision {
			revisions[artifact.month.Format("2006-01")] = true
		}
	}
	for _, month := range []string{"2021-01", "2021-02", "2021-03", "2021-04"} {
		if !revisions[month] {
			t.Errorf("месяц %s 2021 года не помечен ревизией — разбор имени ut_… сломался", month)
		}
	}
}

// TestFnsMonthliesNormalizesBackslash — в выгрузке 2018 года источник печатает путь с
// обратным слэшем (`/docs\otchet\…`); без нормализации url.Parse сохранил бы его в
// адресе, и скачивание падало бы на 404 вместо импорта.
func TestFnsMonthliesNormalizesBackslash(t *testing.T) {
	page := []byte(`<html><body><a href="/html/sites/www.new.nalog.ru/docs\otchet\1nm010218reg.zip">2018</a></body></html>`)
	artifacts := fnsMonthlies(page, "https://www.nalog.gov.ru"+fnsFormsPath+"3827842/")
	if len(artifacts) != 1 {
		t.Fatalf("артефактов %d, ожидался 1", len(artifacts))
	}
	if got := artifacts[0].month.Format("2006-01"); got != "2018-01" {
		t.Errorf("отчётный месяц %s, ожидался 2018-01", got)
	}
	if strings.Contains(artifacts[0].url, `\`) {
		t.Errorf("в адресе остался обратный слэш: %q", artifacts[0].url)
	}
}

// TestFnsCollectArtifactsSkipsKnownMonths — инкремент: месяц, который уже лежит в
// таблице, не скачивается. Известными считаются семь месяцев 2026 года из восьми на
// странице, поэтому ожидаётся ровно один августовский архив.
func TestFnsCollectArtifactsSkipsKnownMonths(t *testing.T) {
	server, counters := fnsTestServer(t, nil)
	start, err := fnsParseMonth("2026-01")
	if err != nil {
		t.Fatalf("разбор старта: %v", err)
	}
	known := map[string]bool{}
	for _, month := range []string{"2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06", "2026-07"} {
		known[month] = true
	}
	artifacts, err := fnsCollectArtifacts(server.URL+fnsFormsPath, start, known)
	if err != nil {
		t.Fatalf("сбор артефактов: %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("артефактов %d, ожидался 1 (август 2026)", len(artifacts))
	}
	if got := artifacts[0].month.Format("2006-01"); got != "2026-08" {
		t.Errorf("месяц %s, ожидался 2026-08", got)
	}
	counters.lock.Lock()
	defer counters.lock.Unlock()
	if counters.files != 0 {
		t.Errorf("скачано %d архивов на этапе сбора: известные месяцы не должны даже запрашиваться", counters.files)
	}
	// Страниц лет раньше стартового сервер не видел вовсе: 2006…2025 отсечены по году.
	for year, hits := range counters.years {
		if year < start.Year() {
			t.Errorf("страница %d года запрошена %d раз при старте %s", year, hits, start.Format("2006-01"))
		}
	}
	if counters.pages < 2 {
		t.Errorf("запрошено %d страниц, ожидалось минимум 2 (формы + год)", counters.pages)
	}
}

// TestFnsCollectArtifactsPrefersRevision — оригинал и ревизия одного месяца
// одновременно на живой странице не встречаются (источник подменяет файл), но если
// появятся, выбран быть должен уточнённый отчёт, а не первый попавшийся.
func TestFnsCollectArtifactsPrefersRevision(t *testing.T) {
	page := []byte(`<html><body>
<a href="/html/sites/x/1nm010226reg.zip">февраль</a>
<a href="/html/sites/x/ut_1nm010226reg.zip">февраль уточнённый</a>
</body></html>`)
	server, _ := fnsTestServer(t, map[string][]byte{fnsFormsPath + "16619097/": page})
	start, err := fnsParseMonth("2026-01")
	if err != nil {
		t.Fatalf("разбор старта: %v", err)
	}
	artifacts, err := fnsCollectArtifacts(server.URL+fnsFormsPath, start, map[string]bool{})
	if err != nil {
		t.Fatalf("сбор артефактов: %v", err)
	}
	var chosen *fnsArtifact
	for i := range artifacts {
		if artifacts[i].month.Format("2006-01") == "2026-01" {
			chosen = &artifacts[i]
		}
	}
	if chosen == nil {
		t.Fatal("январь 2026 не выбран среди артефактов")
	}
	if !strings.HasPrefix(chosen.name, "ut_") {
		t.Errorf("для месяца 2026-01 выбран %q, а ожидалась ревизия ut_1nm010226reg.zip", chosen.name)
	}
}

// TestFnsStartMonth — решение о старте: env важнее таблицы, пустая таблица даёт
// начало истории источника, непустая — следующий месяц после последнего записанного.
func TestFnsStartMonth(t *testing.T) {
	maxMonth := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		from    string
		known   map[string]bool
		want    string
		wantErr bool
	}{
		{name: "пустая таблица — начало истории", from: "", known: map[string]bool{}, want: "2020-01"},
		{name: "последний месяц — следующий", from: "", known: map[string]bool{"2026-08": true}, want: "2026-09"},
		{name: "env переопределяет гейт включительно", from: "2024-05", known: map[string]bool{"2026-08": true}, want: "2024-05"},
		{name: "env с мусором — ошибка", from: "2024-05-01", known: map[string]bool{}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, err := fnsStartMonth(tc.from, tc.known, maxMonth)
			if tc.wantErr {
				if err == nil {
					t.Fatal("ожидалась ошибка разбора env, а старт получен")
				}
				return
			}
			if err != nil {
				t.Fatalf("старт: %v", err)
			}
			if got := start.Format("2006-01"); got != tc.want {
				t.Errorf("старт %s, ожидался %s", got, tc.want)
			}
		})
	}
}

// TestFnsTransportIsUtil — страницы ФНС отдаёт только браузерным User-Agent, а
// транспорт утилиты ещё и ставит национальные сертификаты. Проверка привязывает
// пакет к util.HttpClient: прямая работа с net/http здесь не появилась бы молча.
func TestFnsTransportIsUtil(t *testing.T) {
	var agent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agent = r.Header.Get("User-Agent")
		_, _ = fmt.Fprint(w, `<html><body><a href="16619097/">2026</a></body></html>`)
	}))
	defer server.Close()
	if _, err := util.GetPage(server.URL + fnsFormsPath); err != nil {
		t.Fatalf("util.GetPage не смог скачать страницу: %v", err)
	}
	if !strings.Contains(agent, "Mozilla/5.0") {
		t.Errorf("User-Agent %q — не браузерный", agent)
	}
}
