package dcf

import (
	"math"
	"testing"
)

func TestNdpiPerOzAroundThreshold(t *testing.T) {
	p := defaultParams // threshold 1900, surcharge 0.10, base 0 (база — допущение, в тесте явно 0)
	p.NdpiBaseUSDPerOz = 0

	cases := []struct {
		name string
		gold float64
		want float64
	}{
		{"ниже порога", 1500, 0},
		{"ровно на пороге", 1900, 0},
		{"выше порога", 4000, 210}, // 0.10 × (4000 − 1900)
		{"чуть выше порога", 1900.5, 0.05},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ndpiPerOz(c.gold, p); math.Abs(got-c.want) > 1e-9 {
				t.Fatalf("ndpiPerOz(%v) = %v, want %v", c.gold, got, c.want)
			}
		})
	}
}

func TestEscalateAccumulatesIPC(t *testing.T) {
	cases := []struct {
		name      string
		ipc       []float64
		yearIndex int
		want      float64
	}{
		{"нулевой ИПЦ — без изменений", []float64{0, 0, 0}, 2, 100},
		{"год 0 — база как есть", []float64{0.10, 0.10}, 0, 100},
		{"один год роста", []float64{0.10, 0.10}, 1, 110},
		{"два года накопления", []float64{0.10, 0.10}, 2, 121},
		{"индекс за пределами ряда", []float64{0.10}, 5, 110},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := escalate(100, c.ipc, c.yearIndex); math.Abs(got-c.want) > 1e-9 {
				t.Fatalf("escalate = %v, want %v", got, c.want)
			}
		})
	}
}

func TestNpvLOMFixedPlan(t *testing.T) {
	plan := []MinePlanYear{
		{Year: 2027, ProductionKoz: 100, AISC: 1000, CapexSustaining: 50},
		{Year: 2028, ProductionKoz: 100, AISC: 1000},
		{Year: 2029, ProductionKoz: 100, AISC: 1000, ClosureCosts: -20},
	}
	deck := []DeckYear{{2027, 4000}, {2028, 4000}, {2029, 4000}}
	p := defaultParams
	p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

	// 767.0 млн = 229 + 279 + 259: capex/closure заданы в млн USD (спек §3.3),
	// выручка и НДПИ — в тыс. USD (koz × USD/oz), к млн приводится итог.
	// Прежний эталон брифа (795.95) дважды ошибался: capex 50 вычитался как
	// 50 тыс. вместо 50 млн, а год 2 вычитал НДПИ второй раз.
	if got := npvLOM(plan, deck, 0, nil, p); math.Abs(got-767.0) > 0.01 {
		t.Fatalf("npvLOM = %v, want 767.0", got)
	}
}

func TestNpvLOMClosureTailReducesValue(t *testing.T) {
	// Два НЕЗАВИСИМЫХ литерала: общий слайс (withTail := plan) делит массив,
	// и мутация withTail[0] меняла бы и plan — тогда оба вызова получали бы
	// хвост, и проверка была бы пустой (with == without).
	without := []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000}}
	with := []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000, ClosureCosts: -20}}
	deck := []DeckYear{{2027, 4000}}
	p := defaultParams
	p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

	withoutNPV := npvLOM(without, deck, 0, nil, p)
	withNPV := npvLOM(with, deck, 0, nil, p)
	if !(withNPV < withoutNPV) {
		t.Fatalf("хвост закрытия обязан уменьшать NPV: got %v, want < %v", withNPV, withoutNPV)
	}
}

func TestNpvLOMHigherRateLowersValue(t *testing.T) {
	plan := []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000},
		{Year: 2028, ProductionKoz: 100, AISC: 1000}}
	deck := []DeckYear{{2027, 4000}, {2028, 4000}}
	p := defaultParams
	p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

	low := npvLOM(plan, deck, 0.05, nil, p)
	high := npvLOM(plan, deck, 0.20, nil, p)
	if !(high < low) {
		t.Fatalf("npvLOM не убывает по ставке: 20%% = %v, 5%% = %v", high, low)
	}
}

func TestTwoContoursGiveDifferentNPV(t *testing.T) {
	plan := []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000},
		{Year: 2028, ProductionKoz: 100, AISC: 1000}}
	deck := []DeckYear{{2027, 4000}, {2028, 4000}}
	p := defaultParams
	p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

	rates := DiscountRates{Industrial: 0.05, Local: 0.16} // локальный контур — ОФЗ + премии
	industrial := npvLOM(plan, deck, rates.Industrial, nil, p)
	local := npvLOM(plan, deck, rates.Local, nil, p)
	if !(local < industrial) {
		t.Fatalf("локальная ставка выше индустриальной — её NPV обязан быть ниже: local %v, industrial %v",
			local, industrial)
	}
}

// TestNpvLOMClosureTailOnlyInFinalYear страхует от регрессии, при которой хвост
// закрытия применяется в КАЖДОМ году плана, а не только в последнем. В fixed-plan
// тесте ранние годы несут ClosureCosts == 0, поэтому такой мутант там незаметен:
// NPV всё равно 767.0. Здесь ранний год получает −100 млн и обязан быть проигнорирован,
// значит NPV должен совпасть с вариантом без раннего хвоста.
func TestNpvLOMClosureTailOnlyInFinalYear(t *testing.T) {
	withEarly := []MinePlanYear{
		{Year: 2027, ProductionKoz: 100, AISC: 1000, ClosureCosts: -100}, // должен игнорироваться
		{Year: 2028, ProductionKoz: 100, AISC: 1000},
		{Year: 2029, ProductionKoz: 100, AISC: 1000, ClosureCosts: -20},
	}
	noEarly := []MinePlanYear{
		{Year: 2027, ProductionKoz: 100, AISC: 1000}, // тот же план, ранний хвост = 0
		{Year: 2028, ProductionKoz: 100, AISC: 1000},
		{Year: 2029, ProductionKoz: 100, AISC: 1000, ClosureCosts: -20},
	}
	deck := []DeckYear{{2027, 4000}, {2028, 4000}, {2029, 4000}}
	p := defaultParams
	p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

	got := npvLOM(withEarly, deck, 0, nil, p)
	want := npvLOM(noEarly, deck, 0, nil, p)
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("хвост закрытия учтён не только в последнем году: с ранним хвостом %v, без %v", got, want)
	}
}

// TestNpvLOMWorkingCapitalReducesValue страхует ветку ΔWC: при WorkingCapitalDays > 0
// поток обязан уменьшаться, причём ровно на загрузку оборотного капитала. Считаем
// вручную (год один, rate=0, налог=0; НДПИ не ноль — надбавка 0.10 с цены выше
// порога 1900 действует даже при нулевой базе, отсюда 21 000 тыс.):
//
//	R    = 100 koz × (4000 − 1000)           = 300000 тыс. USD
//	НДПИ = 100 koz × 0.10×(4000−1900)         =  21000 тыс. USD
//	база = (R − НДПИ)/1000                    = 279.0 млн
//	ΔWC  = R × 60/365                         = 49315.0684931... тыс. USD
//	NPV  = 279.0 − 49315.0684931/1000         = 229.6849315... млн
func TestNpvLOMWorkingCapitalReducesValue(t *testing.T) {
	plan := []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000}}
	deck := []DeckYear{{2027, 4000}}

	noWC := defaultParams
	noWC.ProfitTaxPct, noWC.NdpiBaseUSDPerOz = 0, 0

	withWC := noWC
	withWC.WorkingCapitalDays = 60

	base := npvLOM(plan, deck, 0, nil, noWC)
	got := npvLOM(plan, deck, 0, nil, withWC)
	if !(got < base) {
		t.Fatalf("ΔWC обязан уменьшать NPV: с ΔWC %v, без %v", got, base)
	}

	// Ветка ΔWC изолирована: вычитаем ровно R×days/365/1000 млн из базы без ΔWC.
	const revenueThousand = 300000.0 // R, тыс. USD
	want := base - revenueThousand*60/365/1000
	if math.Abs(got-want) > 0.01 {
		t.Fatalf("npvLOM с ΔWC = %v, want %v (база %v)", got, want, base)
	}
}
