package domrf

import (
	"testing"

	"github.com/xuri/excelize/v2"
)

// TestDomrfMarketPoints — разбор реального выпуска «Общей таблицы по РФ» (01.10.2026). Числа —
// литералы из выпуска: тест, сверяющий значение с константой пакета, прошёл бы и после подмены
// колонки.
func TestDomrfMarketPoints(t *testing.T) {
	xlsx, err := excelize.OpenFile("testdata/stat_series.xlsx")
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer func() { _ = xlsx.Close() }()
	points, err := domrfMarketPoints(xlsx)
	if err != nil {
		t.Fatalf("domrfMarketPoints: %v", err)
	}
	if len(points) < 4000 {
		t.Fatalf("точек %d, ожидалось не меньше 4000", len(points))
	}
	bySeries := make(map[string][]domrfMarketPoint, len(domrfMarketSeriesList))
	for _, point := range points {
		bySeries[point.Series] = append(bySeries[point.Series], point)
	}
	for _, series := range domrfMarketSeriesList {
		if len(bySeries[series.Series]) == 0 {
			t.Errorf("ряд %s без точек", series.Series)
		}
	}
	if len(bySeries) != len(domrfMarketSeriesList) {
		t.Errorf("рядов с точками %d, ожидалось %d", len(bySeries), len(domrfMarketSeriesList))
	}

	checks := []struct {
		series string
		date   string
		value  float64
	}{
		{"issued_volume_total", "2026-09-01", 2959618},
		{"issued_volume_ddu", "2026-09-01", 1349429},
		{"issued_count_total", "2026-09-01", 662658},
		{"issued_count_ddu", "2026-09-01", 234004},
		{"rate_primary", "2026-09-01", 7.33},
		{"rate_secondary", "2026-09-01", 12.44902990987482},
		{"portfolio_total", "2026-09-01", 24749151.16333434},
		{"overdue_share_total", "2026-09-01", 0.01209349532172308},
	}
	for _, check := range checks {
		value, ok := findPoint(bySeries[check.series], check.date)
		if !ok {
			t.Errorf("%s: нет точки %s", check.series, check.date)
			continue
		}
		if value != check.value {
			t.Errorf("%s %s = %v, ожидалось %v", check.series, check.date, value, check.value)
		}
	}

	// Стартовая дата колонки — часть контракта каталога: ДДУ появляется с 2016-02, ставки
	// «месяц/с начала года» — с 2019-02, объём выдачи — с 2006-01.
	starts := map[string]string{
		"issued_volume_total": "2006-01-01",
		"issued_count_total":  "2009-01-01",
		"issued_count_ddu":    "2016-02-01",
		"rate_primary":        "2016-02-01",
		"rate_month":          "2009-02-01",
		"rate_ytd":            "2006-01-01",
		"portfolio_domrf":     "2011-01-01",
	}
	for series, want := range starts {
		got := bySeries[series][0].Date.Format("2006-01-02")
		if got != want {
			t.Errorf("%s: первая точка %s, ожидалось %s", series, got, want)
		}
	}
	last := bySeries["issued_volume_total"][len(bySeries["issued_volume_total"])-1]
	if got := last.Date.Format("2006-01-02"); got != "2026-09-01" {
		t.Errorf("последняя точка %s, ожидалось 2026-09-01", got)
	}
}

func findPoint(points []domrfMarketPoint, date string) (float64, bool) {
	for _, point := range points {
		if point.Date.Format("2006-01-02") == date {
			return point.Value, true
		}
	}
	return 0, false
}

// domrfTestHeader собирает шапку из путей: каждая колонка — свой путь, строки — уровни.
func domrfTestHeader(paths [][]string) [][]string {
	header := make([][]string, domrfMarketHeaderRows)
	for row := range header {
		header[row] = make([]string, len(paths))
	}
	for col, path := range paths {
		for row, label := range path {
			header[row][col] = label
		}
	}
	return header
}

func domrfTestPaths() [][]string {
	paths := make([][]string, 0, len(domrfMarketSeriesList))
	for _, series := range domrfMarketSeriesList {
		paths = append(paths, series.Path)
	}
	return paths
}

func TestDomrfMarketColumns(t *testing.T) {
	columns, err := domrfMarketColumns(domrfTestHeader(domrfTestPaths()))
	if err != nil {
		t.Fatalf("domrfMarketColumns: %v", err)
	}
	if len(columns) != len(domrfMarketSeriesList) {
		t.Fatalf("колонок %d, ожидалось %d", len(columns), len(domrfMarketSeriesList))
	}
	if columns["rate_primary"] != 22 {
		t.Errorf("rate_primary в колонке %d, ожидалось 22", columns["rate_primary"])
	}
}

func TestDomrfMarketColumnsRejectsBrokenHeader(t *testing.T) {
	paths := domrfTestPaths()
	duplicated := append([][]string{}, paths...)
	duplicated[1] = duplicated[0]
	if _, err := domrfMarketColumns(domrfTestHeader(duplicated)); err == nil {
		t.Error("дубль пути в шапке не пойман")
	}
	missing := append([][]string{}, paths...)
	missing = missing[:len(missing)-1]
	if _, err := domrfMarketColumns(domrfTestHeader(missing)); err == nil {
		t.Error("пропавшая колонка не поймана")
	}
}

func TestDomrfMarketNumber(t *testing.T) {
	cases := []struct {
		value   string
		percent bool
		want    float64
		ok      bool
		err     bool
	}{
		{"662658", false, 662658, true, false},
		{"4 466,28", false, 4466.28, true, false},
		{"4\u00a0466,28", false, 4466.28, true, false},
		{"1,020,743", false, 1020743, true, false},
		{"56,341", false, 56341, true, false},
		{"14,9", false, 14.9, true, false},
		{"0,125", true, 0.125, true, false},
		{"0", false, 0, true, false},
		{"0.01209", true, 0.01209, true, false},
		{"0.10%", true, 0.001, true, false},
		{"10.08", false, 10.08, true, false},
		{"10.08%", false, 0, false, true},
		{"X", false, 0, false, false},
		{"x", false, 0, false, false},
		{"х", false, 0, false, false},
		{"-", false, 0, false, false},
		{"", false, 0, false, false},
		{"нет", false, 0, false, true},
	}
	for _, check := range cases {
		number, ok, err := domrfMarketNumber(check.value, check.percent)
		if (err != nil) != check.err {
			t.Errorf("%q: ошибка %v, ожидалась %v", check.value, err, check.err)
			continue
		}
		if ok != check.ok || (ok && number != check.want) {
			t.Errorf("%q: (%v, %v), ожидалось (%v, %v)", check.value, number, ok, check.want, check.ok)
		}
	}
}

func TestDomrfMarketDate(t *testing.T) {
	for _, value := range []string{"46266", "01.09.2026", "2026-09-01", "2026-09-01 00:00:00", "09.2026"} {
		date, err := domrfMarketDate(value)
		if err != nil {
			t.Errorf("%q: %v", value, err)
			continue
		}
		if got := date.Format("2006-01-02"); got != "2026-09-01" {
			t.Errorf("%q: %s, ожидалось 2026-09-01", value, got)
		}
	}
	if _, err := domrfMarketDate("сентябрь"); err == nil {
		t.Error("неразобранная дата не поймана")
	}
}
