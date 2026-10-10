package dcf

import "testing"

// TestPolyusAssetPlansCoverage — состав сида: 7 рудников + Сухой Лог, горизонты
// совпадают с публичными сроками службы MOPs, версии источника названы.
func TestPolyusAssetPlansCoverage(t *testing.T) {
	want := map[string]int{
		"OLIMPIADA": 10, "BLAGODATNOYE": 13, "NATALKA": 24,
		"VERNINSKOYE2": 15, "KURANAKH": 15,
	}
	got := map[string]int{}
	for _, p := range polyusAssetPlans {
		if p.Company != "PLZL" {
			t.Errorf("%s: Company = %q, want PLZL", p.Asset, p.Company)
		}
		if p.MineLifeYears <= 0 {
			t.Errorf("%s: MineLifeYears = %d, want > 0", p.Asset, p.MineLifeYears)
		}
		got[p.Asset] = p.MineLifeYears
	}
	for asset, years := range want {
		if got[asset] != years {
			t.Errorf("%s: MineLifeYears = %d, want %d (Годовой обзор 2025, сроки MOPs)", asset, got[asset], years)
		}
	}
	if len(polyusAssetPlans) != 8 {
		t.Fatalf("состав сида = %d активов, want 8 (7 рудников + Сухой Лог)", len(polyusAssetPlans))
	}
}

// TestSustainingWedgeFromReporting — клин AISC−TCC берётся из отчёта 2025,
// а не из устаревшей версии 2024 (там было 384).
func TestSustainingWedgeFromReporting(t *testing.T) {
	if sustainingWedgeUSDPerOz != 698.0 {
		t.Fatalf("sustainingWedgeUSDPerOz = %v, want 698 (AISC 1437 − TCC 739, Годовой обзор 2025)",
			sustainingWedgeUSDPerOz)
	}
}
