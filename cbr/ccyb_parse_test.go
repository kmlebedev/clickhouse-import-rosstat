package cbr

import (
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// TestCcybParse держит разбор книги индикаторов кредитного цикла: 24 ряда,
// 201 месяц, 3 910 непустых точек и стабильные имена рядов. Фикстура — реальный
// файл ЦБ (ccyb_indicators_time_series.xlsx), а не синтетическая книга.
func TestCcybParse(t *testing.T) {
	xlsx, err := excelize.OpenFile("testdata/ccyb_indicators_time_series.xlsx")
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer func() { _ = xlsx.Close() }()

	points, err := cbrCcybParse(xlsx)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(points) != 3910 {
		t.Fatalf("точек %d, want 3910", len(points))
	}

	series := make(map[string]ccybPoint)
	dates := make(map[time.Time]bool)
	seen := make(map[string]bool)
	for _, p := range points {
		if seen[p.series+"|"+p.date.Format("2006-01-02")] {
			t.Fatalf("дубль точки %s/%s", p.series, p.date.Format("2006-01-02"))
		}
		seen[p.series+"|"+p.date.Format("2006-01-02")] = true
		series[p.series] = p
		dates[p.date] = true
	}
	if len(series) != 24 {
		t.Fatalf("рядов %d, want 24", len(series))
	}
	if len(dates) != 201 {
		t.Fatalf("месяцев %d, want 201", len(dates))
	}
	if !dates[time.Date(2008, 2, 1, 0, 0, 0, 0, time.UTC)] {
		t.Fatal("нет первой точки 2008-02-01")
	}
	if !dates[time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC)] {
		t.Fatal("нет последней точки 2024-10-01")
	}

	n10 := series["n10_capital"]
	if n10.indicatorNo != 1 || n10.group != "bank_capital" {
		t.Fatalf("n10_capital: no=%d group=%q", n10.indicatorNo, n10.group)
	}
	if got := pointAt(points, "n10_capital", 2008, 2); got != 15.66713221319927 {
		t.Fatalf("n10_capital 2008-02 = %v", got)
	}
	if got := pointAt(points, "n10_capital", 2024, 10); got != 12.14543965113844 {
		t.Fatalf("n10_capital 2024-10 = %v", got)
	}
	// Кредитные гэпы начинаются позже остальных рядов; проверяем, что первая
	// непустая точка вообще есть, не фиксируя её календарный месяц. Ноль —
	// настоящее значение гэпа, поэтому отсутствие точки проверяется флагом.
	foundGap := false
	for _, p := range points {
		if p.series == "credit_gap_broad" {
			foundGap = true
			break
		}
	}
	if !foundGap {
		names := make([]string, 0, len(series))
		for name := range series {
			names = append(names, name)
		}
		t.Fatalf("credit_gap_broad не имеет точек; ряды: %v", names)
	}
}

func pointAt(points []ccybPoint, series string, year int, month time.Month) float64 {
	for _, p := range points {
		if p.series == series && p.date.Year() == year && p.date.Month() == month {
			return p.value
		}
	}
	return 0
}
