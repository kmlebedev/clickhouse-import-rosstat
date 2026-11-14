package cbr

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
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

// TestCbrOndkpYearU16 — границы года: 1990..2100 проходит, всё вне диапазона — ошибка, а не
// завёрнутый UInt16. 1999 лежит внутри диапазона 1990..2100 (корпус начинается с 2000, но
// границы взяты с запасом) и проходит.
func TestCbrOndkpYearU16(t *testing.T) {
	for _, year := range []int{ondkpYearMin, 1999, 2027, 2099, ondkpYearMax} {
		got, err := ondkpYearU16(year)
		if err != nil {
			t.Errorf("ondkpYearU16(%d): %v, want nil", year, err)
			continue
		}
		if int(got) != year {
			t.Errorf("ondkpYearU16(%d) = %d, want %d", year, got, year)
		}
	}
	for _, year := range []int{ondkpYearMin - 1, ondkpYearMax + 1, 70000, -1, math.MaxInt} {
		if _, err := ondkpYearU16(year); err == nil {
			t.Errorf("ondkpYearU16(%d): nil error, want ошибку диапазона %d–%d", year, ondkpYearMin, ondkpYearMax)
		}
	}
}

// TestCbrOndkpYearOf — год из группы регулярного выражения: Atoi с проверкой диапазона.
// Переполнение Atoi и мусор — ошибка, а не ноль.
func TestCbrOndkpYearOf(t *testing.T) {
	for _, c := range []struct {
		digits string
		want   int
	}{
		{"1990", 1990},
		{"2027", 2027},
		{"2099", 2099},
		{"2100", 2100},
	} {
		year, err := ondkpYearOf(c.digits)
		if err != nil {
			t.Errorf("ondkpYearOf(%q): %v, want nil", c.digits, err)
			continue
		}
		if year != c.want {
			t.Errorf("ondkpYearOf(%q) = %d, want %d", c.digits, year, c.want)
		}
	}
	for _, digits := range []string{"1989", "2101", "70000", "-1", "99999999999999999999", "20x7", ""} {
		if _, err := ondkpYearOf(digits); err == nil {
			t.Errorf("ondkpYearOf(%q): nil error, want ошибку", digits)
		}
	}
}

// TestCbrOndkpCountU16 — forecast_rows перед вставкой: диапазон UInt16 и отрицательные
// значения проверяются до приведения.
func TestCbrOndkpCountU16(t *testing.T) {
	for _, c := range []struct {
		count int
		want  uint16
	}{
		{0, 0},
		{9, 9},
		{math.MaxUint16, math.MaxUint16},
	} {
		got, err := ondkpCountU16(c.count)
		if err != nil {
			t.Errorf("ondkpCountU16(%d): %v, want nil", c.count, err)
			continue
		}
		if got != c.want {
			t.Errorf("ondkpCountU16(%d) = %d, want %d", c.count, got, c.want)
		}
	}
	for _, count := range []int{-1, math.MaxUint16 + 1} {
		if _, err := ondkpCountU16(count); err == nil {
			t.Errorf("ondkpCountU16(%d): nil error, want ошибку", count)
		}
	}
}

// TestCbrOndkpAnalyzeReleasePDFTextFailure — сбой извлечения текста PDF (нет pdftotext) —
// ошибка выпуска, а не проглоченное предупреждение: иначе выпуск попал бы в реестр с
// forecast_rows=0, а гейт по release_year заморозил бы дыру навсегда. Тест на уровне ветки
// разбора артефакта: реальный pdftotext не нужен, утилита подменена отсутствующим именем.
func TestCbrOndkpAnalyzeReleasePDFTextFailure(t *testing.T) {
	previous := util.PDFTool
	util.PDFTool = "cbr-ondkp-pdftotext-missing"
	t.Cleanup(func() { util.PDFTool = previous })
	release := ondkpRelease{
		year:   2027,
		url:    "https://www.cbr.ru/Content/Document/File/1/on_2027(2028-2029).pdf",
		format: cbrOndkpFormatPDF,
	}
	analyzed, rows, err := ondkpAnalyzeRelease(context.Background(), release, []byte("%PDF-1.4\n1 0 obj\n"))
	if err == nil {
		t.Fatal("сбой pdftotext проглочен: выпуск разобран без ошибки")
	}
	if rows != nil {
		t.Errorf("строк %d, want 0: при сбое извлечения прогноза быть не должно", len(rows))
	}
	if analyzed != (ondkpRelease{}) {
		t.Errorf("выпуск %+v, want нулевой: при ошибке выпуск не регистрируется", analyzed)
	}
	if !strings.Contains(err.Error(), "извлечение текста PDF") {
		t.Errorf("ошибка %q не называет извлечение текста PDF", err)
	}
	if !strings.Contains(err.Error(), util.PDFTool) {
		t.Errorf("ошибка %q не называет утилиту %q", err, util.PDFTool)
	}
}

// TestCbrOndkpAnalyzeReleaseNotPDF — WAF/HTML вместо файла: util.PDFText распознаёт подмену
// до exec, и это тоже ошибка выпуска, а не «0 строк».
func TestCbrOndkpAnalyzeReleaseNotPDF(t *testing.T) {
	release := ondkpRelease{
		year:   2027,
		url:    "https://www.cbr.ru/Content/Document/File/1/on_2027(2028-2029).pdf",
		format: cbrOndkpFormatPDF,
	}
	_, _, err := ondkpAnalyzeRelease(context.Background(), release, []byte(`<html><body>Доступ ограничен</body></html>`))
	if err == nil {
		t.Fatal("HTML-заглушка принята за PDF: ошибки нет")
	}
	if !strings.Contains(err.Error(), "не PDF") {
		t.Errorf("ошибка %q не называет подмену файла", err)
	}
}

// TestCbrOndkpAnalyzeReleaseZipDoc — ZIP с .doc разбирать нечем по формату: прогноза нет,
// ошибки нет, pdftotext не вызывается (подмена отсутствующим бинарником это доказывает).
func TestCbrOndkpAnalyzeReleaseZipDoc(t *testing.T) {
	previous := util.PDFTool
	util.PDFTool = "cbr-ondkp-pdftotext-missing"
	t.Cleanup(func() { util.PDFTool = previous })
	release := ondkpRelease{
		year:   2009,
		url:    "https://www.cbr.ru/Queries/XsltBlock/File/1/-1/0",
		format: cbrOndkpFormatZipDoc,
	}
	analyzed, rows, err := ondkpAnalyzeRelease(context.Background(), release, []byte("PK\x03\x04"))
	if err != nil {
		t.Fatalf("ZIP-выпуск: %v, want nil: у zip-doc нет прогнозной таблицы по формату", err)
	}
	if rows != nil {
		t.Errorf("строк %d, want 0", len(rows))
	}
	if analyzed.forecastRows != 0 || analyzed.bytes != 4 {
		t.Errorf("выпуск %+v: want forecastRows=0, bytes=4", analyzed)
	}
	if analyzed.sha256 == "" {
		t.Error("sha256 не посчитан: атрибуты артефакта обязаны заполняться и для ZIP")
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
