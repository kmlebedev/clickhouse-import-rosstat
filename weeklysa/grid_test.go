package weeklysa

import (
	"math"
	"testing"
	"time"
)

func weeklyDate(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func almostEqual(t *testing.T, got, want, tolerance float64, label string) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s: получено %v, ожидалось %v (±%v)", label, got, want, tolerance)
	}
}

func TestMergeSourcesBIPriority(t *testing.T) {
	bulletin := []rawPoint{
		{Date: weeklyDate(2019, 1, 9), Value: 100.1},
		{Date: weeklyDate(2019, 1, 16), Value: 100.2},
	}
	bi := []rawPoint{
		{Date: weeklyDate(2019, 1, 9), Value: 100.3},
		{Date: weeklyDate(2019, 1, 23), Value: 100.4},
	}
	merged, err := mergeSources(bulletin, bi)
	if err != nil {
		t.Fatalf("mergeSources: %v", err)
	}
	if len(merged) != 3 {
		t.Fatalf("точек %d, ожидалось 3", len(merged))
	}
	want := []struct {
		date  time.Time
		value float64
	}{
		{weeklyDate(2019, 1, 9), 100.3},
		{weeklyDate(2019, 1, 16), 100.2},
		{weeklyDate(2019, 1, 23), 100.4},
	}
	for i, item := range want {
		if !merged[i].Date.Equal(item.date) {
			t.Errorf("точка %d: дата %s, ожидалась %s", i, merged[i].Date.Format("2006-01-02"), item.date.Format("2006-01-02"))
		}
		almostEqual(t, merged[i].Value, item.value, 1e-12, "значение")
	}
}

func TestMergeSourcesRejectsDuplicatesWithinSource(t *testing.T) {
	duplicate := []rawPoint{
		{Date: weeklyDate(2019, 1, 9), Value: 100.1},
		{Date: weeklyDate(2019, 1, 9), Value: 100.2},
	}
	if _, err := mergeSources(duplicate, nil); err == nil {
		t.Fatal("ожидалась ошибка дубля в бюллетенях")
	}
	if _, err := mergeSources(nil, duplicate); err == nil {
		t.Fatal("ожидалась ошибка дубля в BI")
	}
}

func TestMergeSourcesRejectsNonPositive(t *testing.T) {
	if _, err := mergeSources([]rawPoint{{Date: weeklyDate(2019, 1, 9), Value: 0}}, nil); err == nil {
		t.Fatal("ожидалась ошибка неположительного значения")
	}
}

func TestValidateSeries(t *testing.T) {
	valid := []rawPoint{
		{Date: weeklyDate(2009, 4, 6), Value: 100},
		{Date: weeklyDate(2009, 4, 13), Value: 100.1},
		{Date: weeklyDate(2009, 4, 20), Value: 100.2},
	}
	if err := validateSeries(valid, weeklySAFirstDate, 3); err != nil {
		t.Fatalf("valid: %v", err)
	}
	if err := validateSeries(valid[:2], weeklySAFirstDate, 3); err == nil {
		t.Fatal("ожидалась ошибка порога")
	}
	wrongFirst := append([]rawPoint(nil), valid...)
	wrongFirst[0].Date = weeklyDate(2009, 4, 7)
	if err := validateSeries(wrongFirst, weeklySAFirstDate, 3); err == nil {
		t.Fatal("ожидалась ошибка первого дня")
	}
	notIncreasing := append([]rawPoint(nil), valid...)
	notIncreasing[1].Date = notIncreasing[0].Date
	if err := validateSeries(notIncreasing, weeklySAFirstDate, 3); err == nil {
		t.Fatal("ожидалась ошибка возрастаемости")
	}
}

func TestBuildGridInterpolates14DayGap(t *testing.T) {
	points := []rawPoint{
		{Date: weeklyDate(2020, 1, 6), Value: 100},
		{Date: weeklyDate(2020, 1, 20), Value: 110},
	}
	grid, err := buildGrid(points)
	if err != nil {
		t.Fatalf("buildGrid: %v", err)
	}
	if len(grid) != 3 {
		t.Fatalf("точек сетки %d, ожидалось 3", len(grid))
	}
	wantDates := []time.Time{weeklyDate(2020, 1, 6), weeklyDate(2020, 1, 13), weeklyDate(2020, 1, 20)}
	for i, want := range wantDates {
		if !grid[i].Date.Equal(want) {
			t.Errorf("сетка %d: %s, ожидалась %s", i, grid[i].Date.Format("2006-01-02"), want.Format("2006-01-02"))
		}
	}
	almostEqual(t, grid[0].Value, 0, 1e-12, "L0")
	almostEqual(t, grid[1].Value, math.Log(1.1)/2, 1e-12, "L1")
	almostEqual(t, grid[2].Value, math.Log(1.1), 1e-12, "L2")
}

func TestFourWeekIndex28Days(t *testing.T) {
	grid := make([]rawPoint, 5)
	for i := range grid {
		grid[i] = rawPoint{
			Date:  weeklyDate(2020, 1, 6).AddDate(0, 0, 7*i),
			Value: float64(i) * math.Log(1.01),
		}
	}
	index, err := fourWeekIndex(grid)
	if err != nil {
		t.Fatalf("fourWeekIndex: %v", err)
	}
	if len(index) != 1 {
		t.Fatalf("точек %d, ожидалась 1", len(index))
	}
	if !index[0].Date.Equal(weeklyDate(2020, 2, 3)) {
		t.Fatalf("дата %s, ожидалась 2020-02-03", index[0].Date.Format("2006-01-02"))
	}
	almostEqual(t, index[0].Value, math.Pow(1.01, 4)*100, 1e-9, "4-нед. индекс")
}

func TestMonthlyLastInMonthDropsPartialMonth(t *testing.T) {
	index := []rawPoint{
		{Date: weeklyDate(2020, 1, 6), Value: 100},
		{Date: weeklyDate(2020, 1, 27), Value: 101},
		{Date: weeklyDate(2020, 2, 3), Value: 102},
		{Date: weeklyDate(2020, 2, 24), Value: 103},
		{Date: weeklyDate(2020, 3, 2), Value: 104},
		{Date: weeklyDate(2020, 3, 30), Value: 105},
		{Date: weeklyDate(2020, 4, 6), Value: 106},
	}
	months, err := monthlyLastInMonth(index, weeklyDate(2020, 4, 6), weeklyDate(2020, 1, 1), 3)
	if err != nil {
		t.Fatalf("monthlyLastInMonth: %v", err)
	}
	if len(months) != 3 {
		t.Fatalf("месяцев %d, ожидалось 3 (апрель неполный)", len(months))
	}
	want := []struct {
		date  time.Time
		value float64
	}{
		{weeklyDate(2020, 1, 31), 101},
		{weeklyDate(2020, 2, 29), 103},
		{weeklyDate(2020, 3, 31), 105},
	}
	for i, item := range want {
		if !months[i].Date.Equal(item.date) {
			t.Errorf("месяц %d: %s, ожидался %s", i, months[i].Date.Format("2006-01-02"), item.date.Format("2006-01-02"))
		}
		almostEqual(t, months[i].Value, item.value, 1e-12, "значение")
	}
}

func TestMonthlyLastInMonthKeepsMonthEnd(t *testing.T) {
	index := []rawPoint{
		{Date: weeklyDate(2020, 1, 6), Value: 100},
		{Date: weeklyDate(2020, 1, 27), Value: 101},
		{Date: weeklyDate(2020, 2, 3), Value: 102},
		{Date: weeklyDate(2020, 2, 24), Value: 103},
		{Date: weeklyDate(2020, 3, 2), Value: 104},
		{Date: weeklyDate(2020, 3, 30), Value: 105},
	}
	months, err := monthlyLastInMonth(index, weeklyDate(2020, 3, 31), weeklyDate(2020, 1, 1), 3)
	if err != nil {
		t.Fatalf("monthlyLastInMonth: %v", err)
	}
	if len(months) != 3 {
		t.Fatalf("месяцев %d, ожидалось 3", len(months))
	}
}

func TestMonthlyLastInMonthRejectsGap(t *testing.T) {
	index := []rawPoint{
		{Date: weeklyDate(2020, 1, 6), Value: 100},
		{Date: weeklyDate(2020, 3, 2), Value: 104},
	}
	if _, err := monthlyLastInMonth(index, weeklyDate(2020, 4, 6), weeklyDate(2020, 1, 1), 2); err == nil {
		t.Fatal("ожидалась ошибка пропуска месяца")
	}
}

func TestMonthlyLastInMonthRejectsWrongFirstMonth(t *testing.T) {
	index := []rawPoint{
		{Date: weeklyDate(2020, 1, 6), Value: 100},
		{Date: weeklyDate(2020, 2, 3), Value: 102},
	}
	if _, err := monthlyLastInMonth(index, weeklyDate(2020, 3, 6), weeklyDate(2020, 2, 1), 2); err == nil {
		t.Fatal("ожидалась ошибка первого месяца")
	}
}

func TestComputeMetrics(t *testing.T) {
	months := []monthPoint{
		{Date: weeklyDate(2020, 1, 31), Value: 100.5, AsOf: weeklyDate(2020, 1, 27)},
		{Date: weeklyDate(2020, 2, 29), Value: 101.2, AsOf: weeklyDate(2020, 2, 24)},
	}
	sa := []saPoint{
		{Date: weeklyDate(2020, 1, 31), SA: 100},
		{Date: weeklyDate(2020, 2, 29), SA: 101},
	}
	metrics, err := computeMetrics(months, sa)
	if err != nil {
		t.Fatalf("computeMetrics: %v", err)
	}
	if len(metrics) != 2 {
		t.Fatalf("метрик %d, ожидалось 2", len(metrics))
	}
	almostEqual(t, metrics[0].SAIndex, 100, 1e-12, "sa_index 1")
	almostEqual(t, metrics[0].SA4W, 0, 1e-12, "sa_4w 1")
	almostEqual(t, metrics[0].SAAR, 0, 1e-12, "saar 1")
	almostEqual(t, metrics[1].SAIndex, 101, 1e-12, "sa_index 2")
	almostEqual(t, metrics[1].SA4W, 1, 1e-12, "sa_4w 2")
	almostEqual(t, metrics[1].SAAR, (math.Pow(1.01, 13)-1)*100, 1e-9, "saar 2")
}

func TestComputeMetricsRejectsDateMismatch(t *testing.T) {
	months := []monthPoint{{Date: weeklyDate(2020, 1, 31), AsOf: weeklyDate(2020, 1, 27)}}
	sa := []saPoint{{Date: weeklyDate(2020, 2, 29), SA: 100}}
	if _, err := computeMetrics(months, sa); err == nil {
		t.Fatal("ожидалась ошибка несовпадения дат")
	}
}

func TestPartialMonth(t *testing.T) {
	index := []rawPoint{
		{Date: weeklyDate(2020, 1, 6), Value: 100},
		{Date: weeklyDate(2020, 1, 27), Value: 101},
		{Date: weeklyDate(2020, 2, 3), Value: 102},
		{Date: weeklyDate(2020, 2, 24), Value: 103},
	}
	partial, err := partialMonth(index, weeklyDate(2020, 2, 24))
	if err != nil {
		t.Fatalf("partialMonth: %v", err)
	}
	if partial == nil {
		t.Fatal("ожидалась nowcast-строка неполного февраля")
	}
	if !partial.Date.Equal(weeklyDate(2020, 2, 29)) || !partial.AsOf.Equal(weeklyDate(2020, 2, 24)) {
		t.Fatalf("date/as_of = %s/%s, ожидались 2020-02-29/2020-02-24",
			partial.Date.Format("2006-01-02"), partial.AsOf.Format("2006-01-02"))
	}
	if !partial.Partial || partial.Value != 103 {
		t.Fatalf("partial/value = %v/%v, ожидались true/103", partial.Partial, partial.Value)
	}
	if full, err := partialMonth(index, weeklyDate(2020, 2, 29)); err != nil || full != nil {
		t.Fatalf("полный месяц: partial=%v, err=%v; ожидался nil/nil", full, err)
	}
}
