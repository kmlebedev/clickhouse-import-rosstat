package cbr

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// moBrFixturePath — урезанная копия full.xlsx (листы «1», «3», «16» — все три формы
// разметки: два годовых листа и спецлист без годов); тесты идут из каталога пакета.
const moBrFixturePath = "testdata/mo_br_full.xlsx"

// Числа фикстуры (заморожены Task 1): 114 рядов / 2058 точек, по листам 54 / 54 / 6 рядов
// и 900 / 900 / 258 точек; волн 43 (2021-05-01 … 2026-09-01).
const (
	moBrFixtureSeries = 114
	moBrFixturePoints = 2058
)

// openMoBrFixture открывает фикстуру макроэкономического опроса; книга закрывается по
// завершении теста.
func openMoBrFixture(t *testing.T) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(moBrFixturePath)
	if err != nil {
		t.Fatalf("открытие %s: %v", moBrFixturePath, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// moBrBySeries — точки, сгруппированные по имени ряда.
func moBrBySeries(pts []moBrPoint) map[string][]moBrPoint {
	out := make(map[string][]moBrPoint)
	for _, p := range pts {
		out[p.Series] = append(out[p.Series], p)
	}
	return out
}

// moBrValueAt — значение ряда на дату волны.
func moBrValueAt(pts []moBrPoint, at time.Time) (float64, bool) {
	for _, p := range pts {
		if p.Date.Equal(at) {
			return p.Value, true
		}
	}
	return 0, false
}

// TestParseMoBrFixtureRows — сквозная проверка разбора фикстуры: 114 рядов и 2058 точек,
// по листам 54 / 54 / 6 рядов, волны — 43 первых числа месяца от 2021-05-01 до 2026-09-01.
func TestParseMoBrFixtureRows(t *testing.T) {
	f := openMoBrFixture(t)

	pts, metas, err := parseMoBr(f)
	if err != nil {
		t.Fatalf("parseMoBr: %v", err)
	}
	if len(pts) != moBrFixturePoints {
		t.Errorf("всего точек: %d, ожидалось %d", len(pts), moBrFixturePoints)
	}
	if len(metas) != moBrFixtureSeries {
		t.Errorf("записей каталога: %d, ожидалось %d", len(metas), moBrFixtureSeries)
	}

	series := moBrBySeries(pts)
	if len(series) != moBrFixtureSeries {
		t.Errorf("рядов в точках: %d, ожидалось %d", len(series), moBrFixtureSeries)
	}

	prefixes := map[string]struct{ series, points int }{
		"cpi_dec":      {54, 900}, // лист «1»: 6 статистик × 9 лет
		"key_rate_avg": {54, 900}, // лист «3»: 6 статистик × 9 лет
		"neutral_rate": {6, 258},  // лист «16»: 6 статистик, без годов
	}
	totalSeries, totalPoints := 0, 0
	for prefix, want := range prefixes {
		gotSeries, gotPoints := 0, 0
		for name, pts := range series {
			if strings.HasPrefix(name, prefix+"_") {
				gotSeries++
				gotPoints += len(pts)
			}
		}
		if gotSeries != want.series || gotPoints != want.points {
			t.Errorf("показатель %s: %d рядов / %d точек, ожидалось %d / %d",
				prefix, gotSeries, gotPoints, want.series, want.points)
		}
		totalSeries += gotSeries
		totalPoints += gotPoints
	}
	if totalSeries != len(series) || totalPoints != len(pts) {
		t.Errorf("неожиданные ряды вне трёх показателей фикстуры: рядов %d из %d, точек %d из %d",
			totalSeries, len(series), totalPoints, len(pts))
	}

	// Волны: 43 различных первых числа месяца; крайние — по спеке.
	waveSet := make(map[time.Time]bool)
	var first, last time.Time
	for _, p := range pts {
		if p.Date.Day() != 1 {
			t.Errorf("ряд %s: дата волны %s — не первое число месяца", p.Series, p.Date.Format("2006-01-02"))
		}
		if !waveSet[p.Date] {
			waveSet[p.Date] = true
			if first.IsZero() || p.Date.Before(first) {
				first = p.Date
			}
			if last.IsZero() || p.Date.After(last) {
				last = p.Date
			}
		}
	}
	if len(waveSet) != 43 {
		t.Errorf("различных волн: %d, ожидалось 43", len(waveSet))
	}
	if want := utcDate(2021, time.May, 1); !first.Equal(want) {
		t.Errorf("первая волна %s, ожидалась %s", first.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	if want := utcDate(2026, time.September, 1); !last.Equal(want) {
		t.Errorf("последняя волна %s, ожидалась %s", last.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

// TestParseMoBrSpotValues — точечные значения последних волн (доказывают raw-чтение ячеек
// и раскладку имён рядов): медианы и число ответов сентябрьской волны 2026-09-01 на листах
// «1», «3», «16» плюс один спот процентиля.
func TestParseMoBrSpotValues(t *testing.T) {
	f := openMoBrFixture(t)

	pts, _, err := parseMoBr(f)
	if err != nil {
		t.Fatalf("parseMoBr: %v", err)
	}
	series := moBrBySeries(pts)

	const eps = 1e-6
	spots := []struct {
		series string
		date   time.Time
		want   float64
	}{
		{"key_rate_avg_2026_median", utcDate(2026, time.September, 1), 14.5},
		{"key_rate_avg_2027_median", utcDate(2026, time.September, 1), 12.43},
		{"cpi_dec_2026_median", utcDate(2026, time.September, 1), 6.6},
		{"neutral_rate_median", utcDate(2026, time.September, 1), 8.0},
		{"cpi_dec_2027_median", utcDate(2026, time.September, 1), 4.564565807129625},
		{"key_rate_avg_2026_p10", utcDate(2026, time.September, 1), 14.460438356164385},
		{"key_rate_avg_2026_answers", utcDate(2026, time.September, 1), 32},
		{"cpi_dec_2026_answers", utcDate(2026, time.September, 1), 33},
		{"neutral_rate_answers", utcDate(2026, time.September, 1), 29},
	}
	for _, s := range spots {
		got, ok := moBrValueAt(series[s.series], s.date)
		if !ok {
			t.Errorf("ряд %s: нет точки на %s", s.series, s.date.Format("2006-01-02"))
			continue
		}
		if math.Abs(got-s.want) > eps {
			t.Errorf("ряд %s %s: значение %.12f, ожидалось %.12f",
				s.series, s.date.Format("2006-01-02"), got, s.want)
		}
	}
}

// TestParseMoBrSkipsDashes — «-» в ячейке волны означает отсутствие точки (пропуск), а не
// ноль: заполненность строк-лет блока «Медиана» листа «3» сохраняется точно, нулевых
// значений в фикстуре нет вовсе.
func TestParseMoBrSkipsDashes(t *testing.T) {
	f := openMoBrFixture(t)

	pts, _, err := parseMoBr(f)
	if err != nil {
		t.Fatalf("parseMoBr: %v", err)
	}
	series := moBrBySeries(pts)

	// Медиана листа «3» по годам: 2021 — 5 заполненных волн, 2026 — 25, 2027 — 18.
	counts := map[string]int{
		"key_rate_avg_2021_median": 5,
		"key_rate_avg_2026_median": 25,
		"key_rate_avg_2027_median": 18,
	}
	for name, want := range counts {
		if got := len(series[name]); got != want {
			t.Errorf("ряд %s: %d точек, ожидалось %d", name, got, want)
		}
	}
	for _, p := range pts {
		if p.Value == 0 {
			t.Errorf("ряд %s %s: значение 0 — «-» превратился в точку", p.Series, p.Date.Format("2006-01-02"))
		}
	}
}

// TestParseMoBrMetaTwoSided — множество Series точек и множество Series каталога совпадают
// в обе стороны (каталог строится из фактически собранных точек), у каждой записи заполнены
// все поля каталога, источник и частота — константы макроэкономического опроса.
func TestParseMoBrMetaTwoSided(t *testing.T) {
	f := openMoBrFixture(t)

	pts, metas, err := parseMoBr(f)
	if err != nil {
		t.Fatalf("parseMoBr: %v", err)
	}

	fromPoints := make(map[string]bool, len(pts))
	for _, p := range pts {
		fromPoints[p.Series] = true
	}
	fromMeta := make(map[string]bool, len(metas))
	bySeries := make(map[string]bool, len(metas))
	for _, m := range metas {
		if m.Source != moBrSource {
			t.Errorf("series %q: source %q, ожидалось %q", m.Series, m.Source, moBrSource)
		}
		if m.Frequency != moBrFrequency {
			t.Errorf("series %q: частота %q, ожидалось %q", m.Series, m.Frequency, moBrFrequency)
		}
		for field, v := range map[string]string{
			"series": m.Series, "title": m.Title, "unit": m.Unit,
			"origin": m.Origin, "description": m.Description,
		} {
			if strings.TrimSpace(v) == "" {
				t.Errorf("series %q: пустое поле %s", m.Series, field)
			}
		}
		if !strings.Contains(m.Origin, "лист «") {
			t.Errorf("series %q: origin %q не называет лист", m.Series, m.Origin)
		}
		if fromMeta[m.Series] {
			t.Errorf("series %q описан в каталоге дважды", m.Series)
		}
		fromMeta[m.Series] = true
		bySeries[m.Series] = true
	}
	origins := make(map[string]string, len(metas))
	descriptions := make(map[string]string, len(metas))
	for _, m := range metas {
		origins[m.Series] = m.Origin
		descriptions[m.Series] = m.Description
	}

	// Origin называет источник, лист и блок статистики; у годовых рядов — ещё и год,
	// у листов 16–17 года нет.
	wantOrigins := map[string]string{
		"key_rate_avg_2026_median": "ЦБ, макроэкономический опрос (full.xlsx, mo_br), лист «3», блок «Медиана», год 2026",
		"neutral_rate_median":      "ЦБ, макроэкономический опрос (full.xlsx, mo_br), лист «16», блок «Медиана»",
	}
	for name, want := range wantOrigins {
		if got := origins[name]; got != want {
			t.Errorf("ряд %s: origin %q, ожидался %q", name, got, want)
		}
	}
	// Description годового ряда называет прогнозный год и расшифровку статистики,
	// у листа без годов год не упоминается вовсе.
	if got := descriptions["cpi_dec_2027_p10"]; !strings.Contains(got, "2027") || !strings.Contains(got, "процентиль") {
		t.Errorf("ряд cpi_dec_2027_p10: description %q не называет год и статистику", got)
	}
	if got, want := descriptions["neutral_rate_answers"],
		"прогноз аналитиков: Нейтральная ключевая ставка, % годовых; число ответов участников опроса по этому показателю (не прогноз)"; got != want {
		t.Errorf("ряд neutral_rate_answers: description %q, ожидалось %q", got, want)
	}

	for _, name := range []string{"key_rate_avg_2026_median", "cpi_dec_2027_p10", "neutral_rate_answers"} {
		if !bySeries[name] {
			t.Errorf("ряд %s отсутствует в каталоге", name)
		}
	}

	for s := range fromPoints {
		if !fromMeta[s] {
			t.Errorf("ряд %s есть в точках, но не описан в каталоге", s)
		}
	}
	for s := range fromMeta {
		if !fromPoints[s] {
			t.Errorf("ряд %s описан в каталоге, но точек нет", s)
		}
	}
}

// TestParseMoBrFailFast — разбор падает с ошибкой (а не отдаёт частичные данные) на каждом
// классе дефектов книги: неполная полная книга (есть навигационный лист, нет листов реестра),
// нет шапки волн, нет блоков статистик, 0 точек, незнакомый лист-показатель. Ошибка обязана
// называть место дефекта — иначе подтест прошёл бы на любой другой ошибке разбора.
func TestParseMoBrFailFast(t *testing.T) {
	// fail — проверка, что ошибка есть и называет ожидаемые места дефекта.
	fail := func(t *testing.T, err error, wants ...string) {
		t.Helper()
		if err == nil {
			t.Fatal("ожидалась ошибка, получено nil")
		}
		for _, want := range wants {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ошибка %q не упоминает %q", err, want)
			}
		}
	}
	// newBook — пустая in-memory книга с заданными листами (Sheet1 удаляется).
	newBook := func(t *testing.T, sheets ...string) *excelize.File {
		t.Helper()
		f := excelize.NewFile()
		t.Cleanup(func() { _ = f.Close() })
		for _, sheet := range sheets {
			if _, err := f.NewSheet(sheet); err != nil {
				t.Fatalf("NewSheet(%q): %v", sheet, err)
			}
		}
		// Sheet1 удаляется после рабочих листов: excelize молча не удаляет единственный
		// лист книги, и он остался бы первым в GetSheetList.
		if err := f.DeleteSheet("Sheet1"); err != nil {
			t.Fatalf("DeleteSheet(Sheet1): %v", err)
		}
		return f
	}

	t.Run("FullBookMissingIndicatorSheet", func(t *testing.T) {
		// Копия фикстуры + навигационный лист = «полная книга» ЦБ; лист «3» удалён, поэтому
		// разбор обязан перечислить отсутствующие листы реестра.
		f := openMoBrFixture(t)
		if err := f.DeleteSheet("3"); err != nil {
			t.Fatalf("DeleteSheet: %v", err)
		}
		if _, err := f.NewSheet("Навигация (Navigation) "); err != nil {
			t.Fatalf("NewSheet: %v", err)
		}
		_, _, err := parseMoBr(f)
		fail(t, err, "«3»", "«2»", "листов-показателей")
	})

	t.Run("FullBookWithoutData", func(t *testing.T) {
		// Все 17 листов реестра и навигационный лист, но данных и шапки волн нет.
		names := make([]string, 0, len(moBrIndicators)+1)
		for _, ind := range moBrIndicators {
			names = append(names, ind.Sheet)
		}
		names = append(names, "Навигация (Navigation) ")
		f := newBook(t, names...)
		_, _, err := parseMoBr(f)
		fail(t, err, "шапка волн", `лист "1"`)
	})

	t.Run("NoStatBlocks", func(t *testing.T) {
		// Шапка волн есть, меток статистик в колонке B нет.
		f := newBook(t, "1")
		for cell, v := range map[string]string{"E6": "44317", "F6": "44378", "G6": "44440"} {
			if err := f.SetCellValue("1", cell, v); err != nil {
				t.Fatalf("SetCellValue(%s): %v", cell, err)
			}
		}
		_, _, err := parseMoBr(f)
		fail(t, err, "не найдено ни одного блока", `лист "1"`)
	})

	t.Run("ZeroPoints", func(t *testing.T) {
		// Шапка волн и метка блока есть, но все значения волн — «-».
		f := newBook(t, "1")
		cells := map[string]string{
			"E6": "44317", "F6": "44378", "G6": "44440",
			"B8": "Медиана", "D8": "44561", "E8": "-", "F8": "-", "G8": "-",
		}
		for cell, v := range cells {
			if err := f.SetCellValue("1", cell, v); err != nil {
				t.Fatalf("SetCellValue(%s): %v", cell, err)
			}
		}
		_, _, err := parseMoBr(f)
		fail(t, err, "0 точек", `лист "1"`)
	})

	t.Run("NonNumericValue", func(t *testing.T) {
		// Непустое значение волны, которое не число и не «-», — ошибка разбора с адресом
		// ячейки: молча превратить его в пропуск нельзя.
		f := newBook(t, "1")
		cells := map[string]string{
			"E6": "44317", "F6": "44378", "G6": "44440",
			"B8": "Медиана", "D8": "44561", "E8": "н/д", "F8": "3.5", "G8": "-",
		}
		for cell, v := range cells {
			if err := f.SetCellValue("1", cell, v); err != nil {
				t.Fatalf("SetCellValue(%s): %v", cell, err)
			}
		}
		_, _, err := parseMoBr(f)
		fail(t, err, "E8", "не является числом", `лист "1"`)
	})

	t.Run("UnknownIndicatorSheet", func(t *testing.T) {
		f := openMoBrFixture(t)
		if _, err := f.NewSheet("99"); err != nil {
			t.Fatalf("NewSheet: %v", err)
		}
		_, _, err := parseMoBr(f)
		fail(t, err, "неизвестный лист-показатель", "99")
	})
}
