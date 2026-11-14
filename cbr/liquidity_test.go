package cbr

import (
	"math"
	"os"
	"testing"
	"time"
)

// cbrLiquidityFixtureBody читает сохранённую страницу формы: обе фикстуры — живые
// ответы cbr.ru (дата скачивания 2026-11-14), поэтому тест не ходит в сеть.
func cbrLiquidityFixtureBody(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", path, err)
	}
	return body
}

// TestCbrLiquidityDailyFixture: дневная форма 01.01.2014..31.01.2014 разбирается в
// 18 торговых дней × 14 рядов = 252 точки; пустых значений в январской выгрузке нет,
// поэтому короткая строка формы не съедает ряд.
func TestCbrLiquidityDailyFixture(t *testing.T) {
	points, err := cbrLiquidityRows(cbrLiquidityFixtureBody(t, "testdata/liquidity_daily.html"), cbrLiquidityKindDaily, cbrLiquidityDailySeries)
	if err != nil {
		t.Fatalf("разбор дневной формы: %v", err)
	}
	if len(points) != 252 {
		t.Fatalf("точек %d, want 252 (18 дней × 14 рядов)", len(points))
	}
	dates := map[string]struct{}{}
	byKey := map[string]float64{}
	for _, point := range points {
		if point.kind != cbrLiquidityKindDaily {
			t.Errorf("вид точки %q, want %q", point.kind, cbrLiquidityKindDaily)
		}
		dates[point.date.Format("2006-01-02")] = struct{}{}
		byKey[point.date.Format("2006-01-02")+"/"+point.series] = point.value
	}
	if len(dates) != 18 {
		t.Errorf("дат %d, want 18", len(dates))
	}
	// Спот последнего дня выгрузки (01.01.2014): дефицит (+) и его слагаемые. Значения —
	// из ячеек формы; перестановка колонок (например, «аукционы» и «постоянное действие»)
	// ловится здесь.
	spot := map[string]float64{
		"deficit":                  3481.8,
		"deficit_ex_corr":          3957.7,
		"cb_claims":                4471.8,
		"cb_claims_auction_repo":   2883.4,
		"cb_claims_auction_loans":  590.2,
		"cb_claims_standing_repo":  292.7,
		"cb_claims_standing_loans": 705.4,
		"cb_liabilities":           517.6,
		"cb_liabilities_kobr":      0,
		"nonstandard_repo":         3.6,
		"corr_accounts":            1270,
		"required_reserves":        794.1,
	}
	for series, want := range spot {
		got, ok := byKey["2014-01-01/"+series]
		if !ok {
			t.Errorf("ряда %q за 2014-01-01 нет", series)
			continue
		}
		if got != want {
			t.Errorf("%s за 2014-01-01 = %v, want %v", series, got, want)
		}
	}
	// Балансовая проверка формы: требования ЦБ = сумма четырёх инструментов, а обязательства
	// = депозиты аукционов + депозиты постоянного действия + КОБР. Ошибка привязки колонок
	// ломает тождество.
	if math.Abs((byKey["2014-01-01/cb_claims_auction_repo"]+byKey["2014-01-01/cb_claims_auction_loans"]+
		byKey["2014-01-01/cb_claims_standing_repo"]+byKey["2014-01-01/cb_claims_standing_loans"])-
		byKey["2014-01-01/cb_claims"]) > 0.2 {
		t.Errorf("сумма инструментов требований не сходится с итогом: %v",
			byKey["2014-01-01/cb_claims"])
	}
}

// TestCbrLiquidityAvgFixture: форма периодов усреднения за 10.01.2018 даёт 11 точек одного
// периода; проверяются знак структурного дефицита и колонки-базисные пункты.
func TestCbrLiquidityAvgFixture(t *testing.T) {
	points, err := cbrLiquidityRows(cbrLiquidityFixtureBody(t, "testdata/liquidity_avg.html"), cbrLiquidityKindAvg, cbrLiquidityAvgSeries)
	if err != nil {
		t.Fatalf("разбор формы периодов усреднения: %v", err)
	}
	if len(points) != 11 {
		t.Fatalf("точек %d, want 11", len(points))
	}
	bySeries := map[string]float64{}
	for _, point := range points {
		if !point.date.Equal(time.Date(2018, time.January, 10, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("дата точки %s, want 2018-01-10", point.date.Format("2006-01-02"))
		}
		bySeries[point.series] = point.value
	}
	spot := map[string]float64{
		"corr_accounts_avg":         2011.1,
		"required_reserves_special": 506.2,
		"structural_deficit":        -2803.1,
		"liquidity_need":            -3126.7,
		"absorbed_auctions":         2848.6,
		"key_rate_change_bp":        0,
		"ruonia_spread_bp":          -64,
		"ruonia_debt":               164.8,
	}
	for series, want := range spot {
		if got := bySeries[series]; got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
}

// TestCbrLiquidityValue: форматы ячеек формы — пробелы внутри числа как разделитель тысяч,
// запятая как десятичный разделитель, прочерк как отсутствие значения (не ноль).
func TestCbrLiquidityValue(t *testing.T) {
	cases := []struct {
		raw     string
		value   float64
		present bool
	}{
		{"3 481,8", 3481.8, true},
		{"3 481,8", 3481.8, true},
		{"-2 803,1", -2803.1, true},
		{"0", 0, true},
		{"—", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		value, present, err := cbrLiquidityValue(c.raw)
		if err != nil {
			t.Errorf("%q: %v", c.raw, err)
			continue
		}
		if present != c.present || value != c.value {
			t.Errorf("%q = (%v, %v), want (%v, %v)", c.raw, value, present, c.value, c.present)
		}
	}
	if _, _, err := cbrLiquidityValue("нет данных"); err == nil {
		t.Error("нечисловая ячейка разобрана без ошибки")
	}
}
