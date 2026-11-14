package cbr

import (
	"os"
	"testing"
)

// cbrOndkpFixture — прочитанный файл фикстуры корпуса ОНДКП.
func cbrOndkpFixture(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", path, err)
	}
	return body
}

// TestCbrOndkpForecast2027 — приёмка T1: числа прогноза 2027 совпадают с PDF. Таблица
// Табл. 3.2 выпуска 2027: среднегодовая ключевая ставка 12,5–13,5 (2027), 9,0–10,0 (2028),
// 7,5–8,5 (2029); инфляция и ВВП — по три года. Факт 2024/2025 в прогноз не попадает.
func TestCbrOndkpForecast2027(t *testing.T) {
	rows, err := ondkpForecastRows(cbrOndkpFixture(t, "testdata/ondkp_2027_table.txt"), 2027)
	if err != nil {
		t.Fatalf("разбор таблицы 2027: %v", err)
	}
	if len(rows) != 9 {
		t.Fatalf("строк %d, want 9 (3 индикатора × 3 года)", len(rows))
	}
	byKey := map[string][2]float64{}
	for _, row := range rows {
		byKey[row.indicator+"/"+itoa(row.year)] = [2]float64{row.lo, row.hi}
	}
	want := map[string][2]float64{
		"key_rate/2027":  {12.5, 13.5},
		"key_rate/2028":  {9.0, 10.0},
		"key_rate/2029":  {7.5, 8.5},
		"inflation/2027": {5.2, 5.7},
		"inflation/2028": {4.0, 4.0},
		"inflation/2029": {4.0, 4.0},
		"gdp/2027":       {1.0, 2.0},
		"gdp/2028":       {1.5, 2.5},
		"gdp/2029":       {1.5, 2.5},
	}
	for key, expected := range want {
		got, ok := byKey[key]
		if !ok {
			t.Errorf("точки %q нет", key)
			continue
		}
		if got[0] != expected[0] || got[1] != expected[1] {
			t.Errorf("%s = %v..%v, want %v..%v", key, got[0], got[1], expected[0], expected[1])
		}
	}
}

// TestCbrOndkpForecast2022 — проект 2022 разбирается теми же правилами (другая ширина
// таблицы: пять колонок годов).
func TestCbrOndkpForecast2022(t *testing.T) {
	rows, err := ondkpForecastRows(cbrOndkpFixture(t, "testdata/ondkp_2022_table.txt"), 2022)
	if err != nil {
		t.Fatalf("разбор таблицы 2022: %v", err)
	}
	if len(rows) != 9 {
		t.Fatalf("строк %d, want 9", len(rows))
	}
	byKey := map[string][2]float64{}
	for _, row := range rows {
		byKey[row.indicator+"/"+itoa(row.year)] = [2]float64{row.lo, row.hi}
	}
	if got := byKey["key_rate/2022"]; got != [2]float64{7.3, 8.3} {
		t.Errorf("key_rate/2022 = %v, want 7.3..8.3", got)
	}
	if got := byKey["key_rate/2024"]; got != [2]float64{5.0, 6.0} {
		t.Errorf("key_rate/2024 = %v, want 5.0..6.0", got)
	}
}

// TestCbrOndkpForecastNoTable: у выпуска без раздела базового сценария (2015) прогнозных
// строк нет, и это не ошибка: ограничение корпуса, а не сбой разбора.
func TestCbrOndkpForecastNoTable(t *testing.T) {
	rows, err := ondkpForecastRows(cbrOndkpFixture(t, "testdata/ondkp_2015_notable.txt"), 2015)
	if err != nil {
		t.Fatalf("разбор текста 2015: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("строк %d, want 0: у выпусков 2000–2019 единой таблицы прогноза нет", len(rows))
	}
}

// TestCbrOndkpRange — значения и диапазоны, включая отрицательные границы в скобках.
func TestCbrOndkpRange(t *testing.T) {
	cases := []struct {
		raw    string
		lo, hi float64
		ok     bool
	}{
		{"12,5", 12.5, 12.5, true},
		{"12,5 – 13,5", 12.5, 13.5, true},
		{"12,5–13,5", 12.5, 13.5, true},
		{"(-0,5)–0,5", -0.5, 0.5, true},
		{"(-4,0) – (-3,0)", -4.0, -3.0, true},
		{"-1,2", -1.2, -1.2, true},
		{"нет", 0, 0, false},
	}
	for _, c := range cases {
		lo, hi, ok := ondkpRange(c.raw)
		if ok != c.ok {
			t.Errorf("%q: ok=%v, want %v", c.raw, ok, c.ok)
			continue
		}
		if ok && (lo != c.lo || hi != c.hi) {
			t.Errorf("%q = %v..%v, want %v..%v", c.raw, lo, hi, c.lo, c.hi)
		}
	}
}

// TestCbrOndkpReleases — реестр корпуса со страницы индекса: 28 выпусков 2000–2027, три
// формата, 2022 — только проект, выпуски 2021+ приходят страницами (pageUrl), 2010 —
// PDF, а не ZIP.
func TestCbrOndkpReleases(t *testing.T) {
	releases, err := ondkpReleases(cbrOndkpFixture(t, "testdata/ondkp_index.html"))
	if err != nil {
		t.Fatalf("разбор страницы индекса: %v", err)
	}
	if len(releases) != 28 {
		t.Fatalf("выпусков %d, want 28 (2000–2027)", len(releases))
	}
	byYear := map[int]ondkpRelease{}
	for _, release := range releases {
		byYear[release.year] = release
	}
	if _, ok := byYear[2000]; !ok || byYear[2000].format != cbrOndkpFormatZipDoc {
		t.Errorf("выпуск 2000: %+v, want zip-doc", byYear[2000])
	}
	if byYear[2010].format != cbrOndkpFormatPDF || byYear[2010].pageUrl != "" {
		t.Errorf("выпуск 2010: %+v, want прямое PDF (артефакт лучше ZIP)", byYear[2010])
	}
	if byYear[2022].pageUrl == "" {
		t.Errorf("выпуск 2022: pageUrl пуст, want страницу выпуска (проект документа лежит на ней, формат уточняется в Import)")
	}
	if byYear[2027].pageUrl == "" {
		t.Errorf("выпуск 2027: pageUrl пуст, want страницу выпуска")
	}
	for year := 2000; year <= 2027; year++ {
		if _, ok := byYear[year]; !ok {
			t.Errorf("в реестре нет выпуска %d", year)
		}
	}
	tuple := byYear[2001]
	if tuple.publishedAt == nil || tuple.publishedAt.Format("2006-01-02") != "2001-01-05" {
		t.Errorf("дата публикации 2001 = %v, want 2001-01-05", tuple.publishedAt)
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := make([]byte, 0, 6)
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}
