package cbr

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// fixturePath — урезанная копия mp_survey_data.xlsx (4 листа); тесты идут из каталога пакета.
const fixturePath = "testdata/mp_survey_data.xlsx"

func utcDate(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func openFixture(t *testing.T) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(fixturePath)
	if err != nil {
		t.Fatalf("открытие %s: %v", fixturePath, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func bySeries(pts []surveyPoint) map[string][]surveyPoint {
	out := make(map[string][]surveyPoint)
	for _, p := range pts {
		out[p.Series] = append(out[p.Series], p)
	}
	return out
}

func firstLast(pts []surveyPoint) (time.Time, time.Time) {
	if len(pts) == 0 {
		return time.Time{}, time.Time{}
	}
	first, last := pts[0].Date, pts[0].Date
	for _, p := range pts {
		if p.Date.Before(first) {
			first = p.Date
		}
		if p.Date.After(last) {
			last = p.Date
		}
	}
	return first, last
}

func valueAt(pts []surveyPoint, at time.Time) (float64, bool) {
	for _, p := range pts {
		if p.Date.Equal(at) {
			return p.Value, true
		}
	}
	return 0, false
}

// TestParseSurveyFixture — сквозная проверка парсера на реальной урезанной копии книги
// мониторинга: 13 рядов, 2084 точки, крайние даты и spot-значения (полная точность
// доказывает raw-чтение ячеек, а не форматированных строк).
func TestParseSurveyFixture(t *testing.T) {
	f := openFixture(t)

	pts, err := parseSurvey(f, mpSurveySeries)
	if err != nil {
		t.Fatalf("parseSurvey: %v", err)
	}
	if len(pts) != 2084 {
		t.Errorf("всего точек: %d, ожидалось 2084", len(pts))
	}

	counts := map[string]int{
		"business_climate":          296,
		"business_climate_sa":       296,
		"price_expectations":        296,
		"price_expectations_sa":     296,
		"credit_conditions":         296,
		"price_growth_expected":     80,
		"price_growth_expected_sa":  80,
		"capacity_utilization":      98,
		"capacity_utilization_sa":   98,
		"staff_availability":        26,
		"staff_availability_sa":     26,
		"headcount_expectations":    98,
		"headcount_expectations_sa": 98,
	}
	series := bySeries(pts)
	for name, want := range counts {
		if got := len(series[name]); got != want {
			t.Errorf("ряд %s: %d точек, ожидалось %d", name, got, want)
		}
	}
	for name := range series {
		if _, ok := counts[name]; !ok {
			t.Errorf("неожиданный ряд в результате: %s", name)
		}
	}

	spans := []struct {
		series    string
		wantFirst time.Time
		wantLast  time.Time
	}{
		{"business_climate", utcDate(2002, time.January, 1), utcDate(2026, time.August, 1)},
		{"business_climate_sa", utcDate(2002, time.January, 1), utcDate(2026, time.August, 1)},
		{"price_expectations", utcDate(2002, time.January, 1), utcDate(2026, time.August, 1)},
		{"price_expectations_sa", utcDate(2002, time.January, 1), utcDate(2026, time.August, 1)},
		{"credit_conditions", utcDate(2002, time.January, 1), utcDate(2026, time.August, 1)},
		{"price_growth_expected", utcDate(2020, time.January, 1), utcDate(2026, time.August, 1)},
		{"price_growth_expected_sa", utcDate(2020, time.January, 1), utcDate(2026, time.August, 1)},
		{"capacity_utilization", utcDate(2002, time.April, 1), utcDate(2026, time.July, 1)},
		{"capacity_utilization_sa", utcDate(2002, time.April, 1), utcDate(2026, time.July, 1)},
		{"staff_availability", utcDate(2020, time.April, 1), utcDate(2026, time.July, 1)},
		{"staff_availability_sa", utcDate(2020, time.April, 1), utcDate(2026, time.July, 1)},
		{"headcount_expectations", utcDate(2002, time.April, 1), utcDate(2026, time.July, 1)},
		{"headcount_expectations_sa", utcDate(2002, time.April, 1), utcDate(2026, time.July, 1)},
	}
	for _, s := range spans {
		first, last := firstLast(series[s.series])
		if !first.Equal(s.wantFirst) {
			t.Errorf("ряд %s: первая дата %s, ожидалась %s", s.series, first.Format("2006-01-02"), s.wantFirst.Format("2006-01-02"))
		}
		if !last.Equal(s.wantLast) {
			t.Errorf("ряд %s: последняя дата %s, ожидалась %s", s.series, last.Format("2006-01-02"), s.wantLast.Format("2006-01-02"))
		}
	}

	const eps = 1e-6
	spots := []struct {
		series string
		date   time.Time
		want   float64
	}{
		{"price_expectations_sa", utcDate(2026, time.August, 1), 22.655674},
		{"price_expectations_sa", utcDate(2026, time.July, 1), 21.100159},
		{"business_climate_sa", utcDate(2026, time.August, 1), -2.260690},
		{"price_growth_expected_sa", utcDate(2026, time.August, 1), 6.984156},
		{"capacity_utilization_sa", utcDate(2026, time.July, 1), 76.9190833585828},
		{"capacity_utilization", utcDate(2026, time.July, 1), 77.025394},
		{"staff_availability_sa", utcDate(2026, time.July, 1), -19.712443},
		{"headcount_expectations_sa", utcDate(2026, time.July, 1), 1.055521},
	}
	for _, s := range spots {
		got, ok := valueAt(series[s.series], s.date)
		if !ok {
			t.Errorf("ряд %s: нет точки на %s", s.series, s.date.Format("2006-01-02"))
			continue
		}
		if math.Abs(got-s.want) > eps {
			t.Errorf("ряд %s %s: значение %.12f, ожидалось %.12f", s.series, s.date.Format("2006-01-02"), got, s.want)
		}
	}
}

// TestParseSurveySkipsEmptyCells — пустая ячейка значения не превращается в ноль и не
// роняет разбор: точка просто пропускается (у остальных рядов точки сохраняются).
func TestParseSurveySkipsEmptyCells(t *testing.T) {
	f := openFixture(t)
	if err := f.SetCellValue("Экономика всего", "J101", ""); err != nil {
		t.Fatalf("очистка J101: %v", err)
	}

	pts, err := parseSurvey(f, mpSurveySeries)
	if err != nil {
		t.Fatalf("parseSurvey: %v", err)
	}
	if len(pts) != 2083 {
		t.Errorf("всего точек: %d, ожидалось 2083", len(pts))
	}
	series := bySeries(pts)
	if got := len(series["price_expectations"]); got != 295 {
		t.Errorf("price_expectations: %d точек, ожидалось 295", got)
	}
	if got := len(series["price_expectations_sa"]); got != 296 {
		t.Errorf("price_expectations_sa: %d точек, ожидалось 296", got)
	}
}

// TestParseSurveyFailFast — разбор падает с ошибкой (а не отдаёт частичные данные) на
// каждом классе дефектов книги: нет листа, нет маркера блока, нет колонки, колонка
// неоднозначна, слишком мало строк данных. Ошибка должна называть место дефекта — иначе
// подтест прошёл бы на любой другой ошибке разбора.
func TestParseSurveyFailFast(t *testing.T) {
	// wantIn — подстрока ошибки, указывающая на ожидаемое место дефекта.
	fail := func(t *testing.T, err error, wantIn string) {
		t.Helper()
		if err == nil {
			t.Fatal("ожидалась ошибка, получено nil")
		}
		if !strings.Contains(err.Error(), wantIn) {
			t.Errorf("ошибка %q не упоминает %q", err, wantIn)
		}
	}

	t.Run("MissingSheet", func(t *testing.T) {
		f := openFixture(t)
		if err := f.DeleteSheet("Загрузка произв мощностей"); err != nil {
			t.Fatalf("DeleteSheet: %v", err)
		}
		_, err := parseSurvey(f, mpSurveySeries)
		fail(t, err, "Загрузка произв мощностей")
	})

	t.Run("MissingMarker", func(t *testing.T) {
		f := openFixture(t)
		if err := f.SetCellValue("Экономика всего", "C3", ""); err != nil {
			t.Fatalf("очистка C3: %v", err)
		}
		_, err := parseSurvey(f, mpSurveySeries)
		fail(t, err, "Исходные данные")
	})

	t.Run("MissingColumn", func(t *testing.T) {
		f := openFixture(t)
		if err := f.SetCellValue("Экономика всего", "J4", "Другой заголовок"); err != nil {
			t.Fatalf("правка J4: %v", err)
		}
		_, err := parseSurvey(f, mpSurveySeries)
		fail(t, err, "price_expectations")
	})

	t.Run("AmbiguousColumn", func(t *testing.T) {
		f := openFixture(t)
		if err := f.SetCellValue("Загрузка произв мощностей", "C4", "Экономика всего (дубль)"); err != nil {
			t.Fatalf("правка C4: %v", err)
		}
		_, err := parseSurvey(f, mpSurveySeries)
		fail(t, err, "capacity_utilization")
	})

	t.Run("TooFewRows", func(t *testing.T) {
		f := excelize.NewFile()
		t.Cleanup(func() { _ = f.Close() })
		if err := f.DeleteSheet("Sheet1"); err != nil {
			t.Fatalf("DeleteSheet(Sheet1): %v", err)
		}
		if _, err := f.NewSheet("Экономика всего"); err != nil {
			t.Fatalf("NewSheet: %v", err)
		}
		cells := map[string]string{
			"C3": "Исходные данные",
			"P3": "Сезонно скорректированные данные",
			"C4": "Индикатор бизнес-климата Банка России",
			"J4": "Ценовые ожидания предприятий на следующие 3 месяца, баланс ответов",
			"A5": "37257",
			"C5": "2.96",
			"J5": "36.40",
		}
		for cell, v := range cells {
			if err := f.SetCellValue("Экономика всего", cell, v); err != nil {
				t.Fatalf("SetCellValue(%s): %v", cell, err)
			}
		}
		_, err := parseSurvey(f, mpSurveySeries)
		fail(t, err, "мало строк")
	})
}
