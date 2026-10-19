package cbr

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// industryFixturePath — полная публичная книга mp_survey_data.xlsx (id 135603), скачанная
// 2026-10-19: 31 лист, 2 193 461 байт, sha256 c5ad8996bda9f21715a05de3bbe98d1511fb6c160b064f1e9c1090eb501c945b.
// Тест читает только 16 отраслевых листов; остальные листы книги игнорируются.
const industryFixturePath = "testdata/mp_survey_industry.xlsx"

func openIndustryFixture(t *testing.T) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(industryFixturePath)
	if err != nil {
		t.Fatalf("открытие %s: %v", industryFixturePath, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// TestParseSurveyIndustryFixture — сквозная проверка разбора 16 отраслевых листов на полной
// реальной книге: 368 рядов, 102 384 точки, даты/пропуски N/Z и spot-значения raw/SA.
func TestParseSurveyIndustryFixture(t *testing.T) {
	f := openIndustryFixture(t)

	pts, err := parseSurvey(f, mpSurveyIndustrySeries)
	if err != nil {
		t.Fatalf("parseSurvey: %v", err)
	}
	if len(pts) != 102384 {
		t.Errorf("всего точек: %d, ожидалось 102384", len(pts))
	}

	series := bySeries(pts)
	if len(series) != 368 {
		t.Errorf("рядов: %d, ожидалось 368", len(series))
	}

	var creditConditions int
	for name, points := range series {
		if !strings.HasPrefix(name, "industry_") {
			t.Errorf("ряд %s без префикса industry_", name)
		}
		wantCount, wantFirst := 297, utcDate(2002, time.January, 1)
		if strings.Contains(name, "_price_growth_expected") {
			wantCount, wantFirst = 81, utcDate(2020, time.January, 1)
		}
		if len(points) != wantCount {
			t.Errorf("ряд %s: %d точек, ожидалось %d", name, len(points), wantCount)
		}
		first, last := firstLast(points)
		if !first.Equal(wantFirst) {
			t.Errorf("ряд %s: первая дата %s, ожидалась %s", name, first.Format("2006-01-02"), wantFirst.Format("2006-01-02"))
		}
		if !last.Equal(utcDate(2026, time.September, 1)) {
			t.Errorf("ряд %s: последняя дата %s, ожидалась 2026-09-01", name, last.Format("2006-01-02"))
		}
		if strings.HasSuffix(name, "_credit_conditions") {
			creditConditions++
		}
		if strings.HasSuffix(name, "_credit_conditions_sa") {
			t.Errorf("ряд %s: у условий кредитования не должно быть SA-пары", name)
		}
	}
	if creditConditions != 16 {
		t.Errorf("рядов credit_conditions: %d, ожидалось 16 (по одному на лист)", creditConditions)
	}

	const eps = 1e-6
	spots := []struct {
		series string
		date   time.Time
		want   float64
	}{
		{"industry_total_ibk", utcDate(2026, time.September, 1), 1.55627937374226},
		{"industry_total_ibk", utcDate(2002, time.January, 1), -0.0321376505404771},
		{"industry_mining_price_expectations", utcDate(2026, time.September, 1), 6.89655172413794},
		{"industry_mining_price_expectations_sa", utcDate(2026, time.September, 1), 8.868355744567},
		{"industry_services_cost_change", utcDate(2026, time.September, 1), 37.3328877005348},
		{"industry_total_price_growth_expected", utcDate(2026, time.September, 1), 6.184698248960752},
		{"industry_total_price_growth_expected_sa", utcDate(2026, time.September, 1), 7.743586534122584},
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

// TestParseSurveyIndustryFailFast — ошибки разбора отраслевого листа называют место дефекта:
// лист, ряд или неоднозначную колонку. Разбор не отдаёт частичные точки.
func TestParseSurveyIndustryFailFast(t *testing.T) {
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
		f := openIndustryFixture(t)
		if err := f.DeleteSheet("Добыча"); err != nil {
			t.Fatalf("DeleteSheet: %v", err)
		}
		_, err := parseSurvey(f, mpSurveyIndustrySeries)
		fail(t, err, "Добыча")
	})

	t.Run("MissingColumn", func(t *testing.T) {
		f := openIndustryFixture(t)
		if err := f.SetCellValue("Промышленность", "J4", "Другой заголовок"); err != nil {
			t.Fatalf("правка J4: %v", err)
		}
		_, err := parseSurvey(f, mpSurveyIndustrySeries)
		fail(t, err, "industry_total_price_expectations")
	})

	t.Run("AmbiguousColumn", func(t *testing.T) {
		f := openIndustryFixture(t)
		if err := f.SetCellValue("Услуги", "N4", "Как изменились издержки производства"); err != nil {
			t.Fatalf("правка N4: %v", err)
		}
		_, err := parseSurvey(f, mpSurveyIndustrySeries)
		fail(t, err, "industry_services_cost_change")
	})
}
