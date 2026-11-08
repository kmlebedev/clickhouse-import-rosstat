package rosstat

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// readIpcWeeksNewsFixture читает текстовый или HTML-снимок реального источника.
func readIpcWeeksNewsFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("testdata/%s: %v", name, err)
	}
	return body
}

// TestIpcWeeksNewsParseFixtures — разбор двух живых PDF (2026-10-26 и 2026-11-02):
// по 108 точек, все имена канонизированы в существующий каталог, значения сверены с
// таблицей PDF и (для 10-26) с живой v_rosstat_macro.
func TestIpcWeeksNewsParseFixtures(t *testing.T) {
	cases := []struct {
		file  string
		date  string
		spots map[string]float64
	}{
		{
			file: "ipc_weeks_news_2026_11_02.txt",
			date: "2026-11-02",
			spots: map[string]float64{
				"Говядина (кроме бескостного мяса), кг": 100.05,
				"Бензин автомобильный":                  97.96,
				"Водка, л": 100.03,
				"Метамизол натрия, 10 таблеток":                                      99.77,
				"Ренгалин, 20 таблеток":                                              99.70,
				"Хлеб из ржаной муки и из смеси муки ржаной и пшеничной, кг":         100.12,
				"Молоко питьевое цельное ультрапастеризованное 2,5-3,2% жирности, л": 100.36,
				// Ряд-приёмник «Поездки в отдельные страны Юго-Восточной Азии»: с 30.03.2026
				// Росстат заменил им «Поездку на отдых в ОАЭ», поэтому в PDF есть только он.
				"Поездки в отдельные страны Юго-Восточной Азии, поездка": 98.32,
			},
		},
		{
			file: "ipc_weeks_news_2026_10_26.txt",
			date: "2026-10-26",
			spots: map[string]float64{
				"Говядина (кроме бескостного мяса), кг": 100.15,
				"Бензин автомобильный":                  98.37,
				"Водка, л": 99.98,
				"Метамизол натрия, 10 таблеток":                              99.93,
				"Ренгалин, 20 таблеток":                                      99.89,
				"Хлеб из ржаной муки и из смеси муки ржаной и пшеничной, кг": 100.29,
				"Поездки в отдельные страны Юго-Восточной Азии, поездка":     98.73,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.date, func(t *testing.T) {
			points, err := ipcWeeksNewsParse(string(readIpcWeeksNewsFixture(t, tc.file)))
			if err != nil {
				t.Fatalf("ipcWeeksNewsParse: %v", err)
			}
			if len(points) != 108 {
				t.Fatalf("точек %d, ожидалось 108", len(points))
			}
			bySeries := make(map[string]float64, len(points))
			for _, point := range points {
				if got := point.date.Format("2006-01-02"); got != tc.date {
					t.Errorf("ряд %q: дата %s, ожидалась %s", point.product, got, tc.date)
				}
				if !ipcWeeksKnownProducts[point.product] {
					t.Errorf("ряд %q не описан в ipcWeeksProducts", point.product)
				}
				if _, dup := bySeries[point.product]; dup {
					t.Errorf("ряд %q встречается дважды", point.product)
				}
				bySeries[point.product] = point.value
			}
			for series, want := range tc.spots {
				got, ok := bySeries[series]
				if !ok {
					t.Errorf("ряд %q не найден", series)
					continue
				}
				if diff := got - want; diff > 0.001 || diff < -0.001 {
					t.Errorf("ряд %q = %v, ожидалось %v", series, got, want)
				}
			}
		})
	}
}

// TestIpcWeeksNewsReleases — карточки ленты: месячная карточка отсекается маркером,
// относительный URL разрешается, PDF-признак выставлен.
func TestIpcWeeksNewsReleases(t *testing.T) {
	releases, err := ipcWeeksNewsReleases(readIpcWeeksNewsFixture(t, "ipc_weeks_news_page.html"))
	if err != nil {
		t.Fatalf("ipcWeeksNewsReleases: %v", err)
	}
	if len(releases) != 3 {
		t.Fatalf("выпусков %d, ожидалось 3 (месячная карточка не в счёт): %+v", len(releases), releases)
	}
	want := []struct {
		date string
		url  string
		pdf  bool
	}{
		{"2026-11-02", "https://rosstat.gov.ru/storage/mediabank/ipc11_02.pdf", true},
		{"2026-10-26", "https://rosstat.gov.ru/storage/mediabank/168ozen28.pdf", true},
		{"2026-03-10", "https://rosstat.gov.ru/storage/mediabank/35_13-03-2026.html", false},
	}
	for i, expected := range want {
		if got := releases[i].Date.Format("2006-01-02"); got != expected.date {
			t.Errorf("выпуск %d: дата %s, ожидалась %s", i, got, expected.date)
		}
		if releases[i].URL != expected.url {
			t.Errorf("выпуск %d: URL %q, ожидался %q", i, releases[i].URL, expected.url)
		}
		if releases[i].PDF != expected.pdf {
			t.Errorf("выпуск %d: PDF=%v, ожидалось %v", i, releases[i].PDF, expected.pdf)
		}
	}
}

// TestIpcWeeksNewsParseErrors — fail-fast на сломанной вёрстке/данных.
func TestIpcWeeksNewsParseErrors(t *testing.T) {
	cases := map[string]string{
		"нет якоря": `2 ноября 2026 г.
Товар 100,00 100,00 100,00`,
		"нет даты": `Индексы потребительских цен
Товар 100,00 100,00 100,00`,
		"нет строк": `Индексы потребительских цен
2 ноября 2026 г.
к предыдущей к концу
дате октября декабря
регистрации 2026 г. 2025 г.`,
		"битое значение": `Индексы потребительских цен
2 ноября 2026 г.
Товар 100,0x 100,00 100,00`,
		"дубль ряда": `Индексы потребительских цен
2 ноября 2026 г.
Бананы 100,00 100,00 100,00
Бананы, кг 100,10 100,00 100,00`,
		"разные даты страниц": `Индексы потребительских цен
2 ноября 2026 г.
Товар 100,00 100,00 100,00
Индексы потребительских цен
26 октября 2026 г.
Товар 2 100,00 100,00 100,00`,
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ipcWeeksNewsParse(text); err == nil {
				t.Fatal("ожидалась ошибка")
			}
		})
	}
}

// TestIpcWeeksNewsParseNoDataRowDoesNotBreakTable — «…» в первой колонке пропускает точку,
// но не обрывает таблицу и не приклеивает строку к следующему товару.
func TestIpcWeeksNewsParseNoDataRowDoesNotBreakTable(t *testing.T) {
	text := `Индексы потребительских цен
2 ноября 2026 г.
Бананы 100,00 100,00 100,00
Кефир … 100,00 100,00
Бензин автомобильный 99,00 100,00 100,00`
	points, err := ipcWeeksNewsParse(text)
	if err != nil {
		t.Fatalf("ipcWeeksNewsParse: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("точек %d, ожидалось 2: %+v", len(points), points)
	}
	if points[1].product != "Бензин автомобильный" || points[1].value != 99 {
		t.Errorf("последняя точка %+v, ожидался «Бензин автомобильный» 99", points[1])
	}
}

// TestIpcWeeksNewsFallbackRowDateMismatch — дата таблицы обязана совпасть с датой выпуска.
func TestIpcWeeksNewsFallbackRowDateMismatch(t *testing.T) {
	points, err := ipcWeeksNewsParse(string(readIpcWeeksNewsFixture(t, "ipc_weeks_news_2026_11_02.txt")))
	if err != nil {
		t.Fatalf("ipcWeeksNewsParse: %v", err)
	}
	if _, err = ipcWeeksNewsFallbackRows(points, time.Date(2026, time.October, 26, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("ожидалась ошибка расхождения даты")
	}
	rows, err := ipcWeeksNewsFallbackRows(points, time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ipcWeeksNewsFallbackRows: %v", err)
	}
	if len(rows) != 108 {
		t.Fatalf("строк %d, ожидалось 108", len(rows))
	}
}

// TestIpcWeeksNewsAppendRow — три колонки rosstat_ipc_weeks; значение сужается до Float32.
func TestIpcWeeksNewsAppendRow(t *testing.T) {
	rec := &appendRecorder{}
	err := ipcWeeksNewsAppendRow(rec, util.FallbackRow{
		Series: "Бананы, кг",
		Date:   time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC),
		Value:  100.5,
	})
	if err != nil {
		t.Fatalf("ipcWeeksNewsAppendRow: %v", err)
	}
	if len(rec.rows) != 1 {
		t.Fatalf("строк %d, ожидалась 1", len(rec.rows))
	}
	if rec.rows[0].series != "Бананы, кг" || rec.rows[0].value != 100.5 {
		t.Errorf("строка %+v, ожидалась «Бананы, кг» 100.5", rec.rows[0])
	}
}

// TestIpcWeeksNewsSeriesMapCanonical — каждое каноническое имя карты описано в каталоге,
// и каждая строка PDF-фикстуры либо в карте, либо в общей карте бюллетеней.
func TestIpcWeeksNewsSeriesMapCanonical(t *testing.T) {
	for source, canonical := range ipcWeeksNewsSeriesBySourceName {
		if !ipcWeeksKnownProducts[canonical] {
			t.Errorf("ключ %q → %q: канонического имени нет в ipcWeeksProducts", source, canonical)
		}
	}
}

// TestIpcWeeksNewsURL — запрос недельной ленты, а не месячной.
func TestIpcWeeksNewsURL(t *testing.T) {
	feedURL, err := ipcWeeksNewsURL()
	if err != nil {
		t.Fatalf("ipcWeeksNewsURL: %v", err)
	}
	for _, want := range []string{"central-news", "per_page=20", "date_from=", "date_to="} {
		if !strings.Contains(feedURL, want) {
			t.Errorf("URL %q не содержит %q", feedURL, want)
		}
	}
	if !strings.Contains(feedURL, "%D0%9E%D0%B1+%D0%BE%D1%86%D0%B5%D0%BD%D0%BA%D0%B5") {
		t.Errorf("URL %q не содержит закодированный поисковый запрос «Об оценке»", feedURL)
	}
}

// TestIpcWeeksNewsDocumentsUnavailable — недоступная лента не превращается в пустой список.
func TestIpcWeeksNewsDocumentsUnavailable(t *testing.T) {
	old := ipcWeeksNewsURL
	ipcWeeksNewsURL = func() (string, error) {
		return "", fmt.Errorf("тестовая ошибка")
	}
	t.Cleanup(func() { ipcWeeksNewsURL = old })

	if _, err := ipcWeeksNewsDocuments(context.Background()); err == nil {
		t.Fatal("ожидалась ошибка URL ленты")
	}
}

// TestIpcWeeksPetrolTitleDate — дата из титула PDF нефтепродуктов, а не из карточки ленты:
// карточка 28.10.2026 подписана «с 27 октября по 2 ноября», а PDF внутри — за 20–26 октября.
func TestIpcWeeksPetrolTitleDate(t *testing.T) {
	for _, tc := range []struct {
		file string
		want string
	}{
		{"ipc_weeks_petrol_2026_11_02.txt", "2026-11-02"},
		{"ipc_weeks_petrol_2026_10_26.txt", "2026-10-26"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			date, err := ipcWeeksPetrolTitleDate(string(readIpcWeeksNewsFixture(t, tc.file)))
			if err != nil {
				t.Fatalf("ipcWeeksPetrolTitleDate: %v", err)
			}
			if got := date.Format("2006-01-02"); got != tc.want {
				t.Errorf("дата %s, ожидалась %s", got, tc.want)
			}
		})
	}
}

// TestIpcWeeksPetrolParse — три марки бензина, значение — текущая неделя (вторая колонка).
func TestIpcWeeksPetrolParse(t *testing.T) {
	for _, tc := range []struct {
		file   string
		date   string
		values map[string]float64
	}{
		{
			file: "ipc_weeks_petrol_2026_11_02.txt",
			date: "2026-11-02",
			values: map[string]float64{
				ipcWeeksPetrolAI92: 97.8,
				ipcWeeksPetrolAI95: 98.0,
				ipcWeeksPetrolAI98: 99.8,
			},
		},
		{
			file: "ipc_weeks_petrol_2026_10_26.txt",
			date: "2026-10-26",
			values: map[string]float64{
				ipcWeeksPetrolAI92: 98.2,
				ipcWeeksPetrolAI95: 98.4,
				ipcWeeksPetrolAI98: 100.1,
			},
		},
	} {
		t.Run(tc.date, func(t *testing.T) {
			expected := time.Date(2026, time.October, 26, 0, 0, 0, 0, time.UTC)
			if tc.date == "2026-11-02" {
				expected = time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC)
			}
			rows, err := ipcWeeksPetrolParse(string(readIpcWeeksNewsFixture(t, tc.file)), expected)
			if err != nil {
				t.Fatalf("ipcWeeksPetrolParse: %v", err)
			}
			if len(rows) != 3 {
				t.Fatalf("строк %d, ожидалось 3", len(rows))
			}
			for _, row := range rows {
				if got := row.Date.Format("2006-01-02"); got != tc.date {
					t.Errorf("ряд %q: дата %s, ожидалась %s", row.Series, got, tc.date)
				}
				want, ok := tc.values[row.Series]
				if !ok {
					t.Errorf("неожиданный ряд %q", row.Series)
					continue
				}
				if math.Abs(row.Value-want) > 0.001 {
					t.Errorf("ряд %q = %v, ожидалось %v", row.Series, row.Value, want)
				}
			}
		})
	}
}

// TestIpcWeeksPetrolParseMismatch — титул PDF не совпал с датой документа — ошибка.
func TestIpcWeeksPetrolParseMismatch(t *testing.T) {
	text := string(readIpcWeeksNewsFixture(t, "ipc_weeks_petrol_2026_11_02.txt"))
	if _, err := ipcWeeksPetrolParse(text, time.Date(2026, time.October, 26, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("ожидалась ошибка расхождения даты")
	}
}

// TestIpcWeeksNewsPDFLinksPetrol — из ленты нефтепродуктов берутся только PDF, CPI-ссылка
// с другим маркером отсекается.
func TestIpcWeeksNewsPDFLinksPetrol(t *testing.T) {
	links, err := ipcWeeksNewsPDFLinks(
		readIpcWeeksNewsFixture(t, "ipc_weeks_petrol_page.html"),
		ipcWeeksPetrolTitleMarker, ipcWeeksPetrolContext)
	if err != nil {
		t.Fatalf("ipcWeeksNewsPDFLinks: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("ссылок %d, ожидалось 2: %v", len(links), links)
	}
	if !strings.HasSuffix(links[0], "n-ben11_02.pdf") || !strings.HasSuffix(links[1], "169n-ben28.pdf") {
		t.Errorf("ссылки %v, ожидались n-ben11_02.pdf и 169n-ben28.pdf", links)
	}
}

// TestIpcWeeksPetrolSeriesInCatalog — три марки уже описаны в каталоге XLSX.
func TestIpcWeeksPetrolSeriesInCatalog(t *testing.T) {
	for _, series := range []string{ipcWeeksPetrolAI92, ipcWeeksPetrolAI95, ipcWeeksPetrolAI98} {
		if !ipcWeeksKnownProducts[series] {
			t.Errorf("ряда %q нет в ipcWeeksProducts", series)
		}
	}
}
