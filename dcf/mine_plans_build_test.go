package dcf

import "testing"

// TestBuildPlanYearsHorizon — число строк равно сроку службы, хвост закрытия
// только в последнем году, AISC = TCC + клин.
func TestBuildPlanYearsHorizon(t *testing.T) {
	seed := assetPlanSeed{
		Company: "PLZL", Asset: "OLIMPIADA", BaseYear: 2025, MineLifeYears: 10,
		TCC: 773, CapexSustaining: 450, ClosureCosts: -50,
	}
	years := buildPlanYears(seed, 926.5)
	if len(years) != 10 {
		t.Fatalf("len = %d, want 10 (срок службы MOPs)", len(years))
	}
	if years[0].Year != 2025 || years[9].Year != 2034 {
		t.Fatalf("годы %d..%d, want 2025..2034", years[0].Year, years[9].Year)
	}
	if years[0].AISC != 773+698.0 {
		t.Fatalf("AISC = %v, want %v (TCC + клин 2025)", years[0].AISC, 773+698.0)
	}
	for i, y := range years[:9] {
		if y.ClosureCosts != 0 {
			t.Errorf("год %d: ClosureCosts = %v, want 0 (хвост только в последнем)", i, y.ClosureCosts)
		}
	}
	if years[9].ClosureCosts != -50 {
		t.Fatalf("последний год: ClosureCosts = %v, want -50", years[9].ClosureCosts)
	}
}

// TestBuildPlanYearsHalt — консервация Вернинского даёт нулевую добычу,
// но строка остаётся: пропуск сдвинул бы нумерацию лет плана.
func TestBuildPlanYearsHalt(t *testing.T) {
	seed := assetPlanSeed{
		Company: "PLZL", Asset: "VERNINSKOYE2", BaseYear: 2025, MineLifeYears: 15,
		TCC: 735, HaltFrom: 2025, HaltTo: 2028,
	}
	years := buildPlanYears(seed, 271.7)
	if len(years) != 15 {
		t.Fatalf("len = %d, want 15: консервация не сокращает план", len(years))
	}
	for _, y := range years {
		if y.Year >= 2025 && y.Year <= 2028 && y.ProductionKoz != 0 {
			t.Errorf("год %d: ProductionKoz = %v, want 0 (карьер законсервирован)", y.Year, y.ProductionKoz)
		}
		if y.Year == 2029 && y.ProductionKoz == 0 {
			t.Errorf("год 2029: добыча должна возобновиться")
		}
	}
}

// TestBuildPlanYearsNoFact — отсутствие факта не превращается в ноль.
func TestBuildPlanYearsNoFact(t *testing.T) {
	seed := assetPlanSeed{Company: "PLZL", Asset: "X", BaseYear: 2025, MineLifeYears: 5, TCC: 700}
	if got := buildPlanYears(seed, 0); got != nil {
		t.Fatalf("нулевой факт: got %v, want nil (строки не пишем)", got)
	}
}
