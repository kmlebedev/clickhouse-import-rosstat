package dcf

import "testing"

// assetPlanSeedWant — ожидаемые значения полей одного актива, сверенные вручную
// по Годовому обзору 2025 (TCC — стр. 5/31, сроки службы — стр. 115, capex — стр. 33)
// и презентации «Ключевые проекты роста» (декабрь 2024, Сухой Лог — стр. 15/20/22).
//
// Ключ карты — ИМЯ актива, а не индекс и не длина среза: опечатка в имени,
// дубликат имени или переименование актива проваливают тест, а не проходят мимо
// него. Zero-значения здесь — ожидаемые нули (для титимухты/западного
// CapexSustaining=0 действительно «не выделено»), поэтому в таблице явно
// перечислены ВСЕ поля, а не только ненулевые.
type assetPlanSeedWant struct {
	life              int
	tcc               float64
	capexSustaining   float64
	projectCapex      float64
	profileStartYear  uint16
	haltFrom, haltTo  uint16
	productionProfile []float64 // nil — профиль не задан
}

// wantAssetPlanSeed — восемь активов сида с точными значениями. Это главная
// защита от «тихой» правки числа: юнит-ошибка или опечатка (773 → 737) должна
// ломать тест, а не менять NPV в тысячи раз незаметно.
var wantAssetPlanSeed = map[string]assetPlanSeedWant{
	"OLIMPIADA":    {life: 10, tcc: 773, capexSustaining: 450, projectCapex: 0},
	"BLAGODATNOYE": {life: 13, tcc: 691, capexSustaining: 370, projectCapex: 0},
	"NATALKA":      {life: 24, tcc: 671, capexSustaining: 198, projectCapex: 0},
	"VERNINSKOYE2": {life: 15, tcc: 735, capexSustaining: 125, projectCapex: 0, haltFrom: 2024, haltTo: 2028},
	"KURANAKH":     {life: 15, tcc: 925, capexSustaining: 362, projectCapex: 0},
	"TITIMUKHTA":   {life: 13, tcc: 773, capexSustaining: 0, projectCapex: 0},
	"ZAPADNOYE":    {life: 15, tcc: 671, capexSustaining: 0, projectCapex: 0},
	"Sukhoi Log": {
		life: 10, tcc: 735, capexSustaining: 0, projectCapex: 6000,
		profileStartYear: 2028,
		productionProfile: []float64{
			2300, 2800, 2300, 2800, 2300, 2800, 2300, 2800, 2300, 2800,
		},
	},
}

// wantAssetNames — точный состав сида: 7 рудников + Сухой Лог. Отдельный список,
// а не len(wantAssetPlanSeed), чтобы проверка состава была явной и читаемой.
var wantAssetNames = []string{
	"OLIMPIADA", "BLAGODATNOYE", "NATALKA", "VERNINSKOYE2",
	"KURANAKH", "TITIMUKHTA", "ZAPADNOYE", "Sukhoi Log",
}

// TestPolyusAssetPlansValues — value-level таблица: по каждому из восьми активов
// сверяются MineLifeYears, TCC, CapexSustaining, ProjectCapex и (где заданы)
// HaltFrom/HaltTo/ProfileStartYear. Плюс инварианты состава: Company="PLZL" у
// всех, MineLifeYears > 0, ровно восемь уникальных имён, ни лишних, ни
// пропущенных. Ключи — имена активов, поэтому переименование или дубликат имени
// проваливает тест независимо от длины среза.
func TestPolyusAssetPlansValues(t *testing.T) {
	if len(polyusAssetPlans) != len(wantAssetNames) {
		t.Fatalf("состав сида = %d активов, want %d (7 рудников + Сухой Лог)",
			len(polyusAssetPlans), len(wantAssetNames))
	}

	seen := map[string]bool{}
	for _, p := range polyusAssetPlans {
		if p.Company != "PLZL" {
			t.Errorf("%s: Company = %q, want PLZL", p.Asset, p.Company)
		}
		if p.MineLifeYears <= 0 {
			t.Errorf("%s: MineLifeYears = %d, want > 0", p.Asset, p.MineLifeYears)
		}
		if seen[p.Asset] {
			t.Errorf("%s: актив встречается в сиде больше одного раза (дубликат имени)", p.Asset)
		}
		seen[p.Asset] = true

		w, ok := wantAssetPlanSeed[p.Asset]
		if !ok {
			t.Errorf("%s: неожиданный актив — нет в wantAssetPlanSeed; обнови таблицу ожиданий вместе с сидом", p.Asset)
			continue
		}
		if p.MineLifeYears != w.life {
			t.Errorf("%s: MineLifeYears = %d, want %d (Годовой обзор 2025, сроки службы MOPs)", p.Asset, p.MineLifeYears, w.life)
		}
		if p.TCC != w.tcc {
			t.Errorf("%s: TCC = %v, want %v USD/унц (Годовой обзор 2025, стр. 5/31)", p.Asset, p.TCC, w.tcc)
		}
		if p.CapexSustaining != w.capexSustaining {
			t.Errorf("%s: CapexSustaining = %v, want %v млн USD (Годовой обзор 2025, стр. 33)", p.Asset, p.CapexSustaining, w.capexSustaining)
		}
		if p.ProjectCapex != w.projectCapex {
			t.Errorf("%s: ProjectCapex = %v, want %v млн USD", p.Asset, p.ProjectCapex, w.projectCapex)
		}
		if p.ProfileStartYear != w.profileStartYear {
			t.Errorf("%s: ProfileStartYear = %d, want %d", p.Asset, p.ProfileStartYear, w.profileStartYear)
		}
		if p.HaltFrom != w.haltFrom {
			t.Errorf("%s: HaltFrom = %d, want %d", p.Asset, p.HaltFrom, w.haltFrom)
		}
		if p.HaltTo != w.haltTo {
			t.Errorf("%s: HaltTo = %d, want %d", p.Asset, p.HaltTo, w.haltTo)
		}
		if !equalFloats(p.ProductionProfile, w.productionProfile) {
			t.Errorf("%s: ProductionProfile = %v, want %v", p.Asset, p.ProductionProfile, w.productionProfile)
		}
	}

	for _, name := range wantAssetNames {
		if !seen[name] {
			t.Errorf("%s: актива нет в сиде, а он есть в списке ожидаемых имён", name)
		}
	}
}

// TestSukhoiLogProductionProfile — профиль Сухого Лога: ровно 10 значений,
// чередование 2300/2800 (нижняя/верхняя граница публичного диапазона 2,3-2,8 млн
// унц). Отдельный тест, потому что именно профиль задаёт горизонт актива: десять
// значений = десять лет плана (MineLifeYears), и лишняя/потерянная итерация
// сдвинула бы NPV.
func TestSukhoiLogProductionProfile(t *testing.T) {
	var log *assetPlanSeed
	for i := range polyusAssetPlans {
		if polyusAssetPlans[i].Asset == "Sukhoi Log" {
			log = &polyusAssetPlans[i]
			break
		}
	}
	if log == nil {
		t.Fatal("Sukhoi Log: актив не найден в сиде")
	}
	if len(log.ProductionProfile) != 10 {
		t.Fatalf("Sukhoi Log: ProductionProfile = %d значений, want 10 (первые 10 лет работы)", len(log.ProductionProfile))
	}
	if len(log.ProductionProfile) != log.MineLifeYears {
		t.Errorf("Sukhoi Log: ProductionProfile = %d значений, но MineLifeYears = %d (профиль задаёт длину горизонта)",
			len(log.ProductionProfile), log.MineLifeYears)
	}
	for i, v := range log.ProductionProfile {
		want := 2300.0
		if i%2 == 1 {
			want = 2800.0
		}
		if v != want {
			t.Errorf("Sukhoi Log: ProductionProfile[%d] = %v, want %v (чередование 2300/2800)", i, v, want)
		}
	}
}

// TestSeedClosureCostsAreUnsetInvariant — фиксирует задокументированный инвариант
// сида: ClosureCosts == 0 у ВСЕХ восьми активов, и этот ноль означает «не
// задано» (per-asset провижен на рекультивацию Полюс не публикует), а не
// «закрытие бесплатно».
//
// Смысл теста — упасть громко в тот момент, когда кто-то выставит СМЕСЬ нулей и
// ненулевых значений, не пересмотрев решение о распределении: частично
// заполненный столбец ClosureCosts выглядел бы как измеренный, и модель молча
// считала бы закрытие бесплатным на остальных активах. Пока распределение не
// утверждено, допустимо ровно два состояния: все нули (текущее) или все
// ненулевые (после утверждения базы распределения) — этот тест ловит промежуточное.
func TestSeedClosureCostsAreUnsetInvariant(t *testing.T) {
	var nonzero []string
	for _, p := range polyusAssetPlans {
		if p.ClosureCosts != 0 {
			nonzero = append(nonzero, p.Asset)
		}
	}
	if len(nonzero) != 0 {
		t.Fatalf("ClosureCosts задан у %v, но per-asset провижен ещё не распределён: "+
			"если это осознанное обновление — заполни значения у ВСЕХ восьми активов и "+
			"обнови этот тест вместе с docs/DCF_DATA_COVERAGE.md; "+
			"частично заполненный ClosureCosts читается как измеренный (см. doc-комментарий к полю)",
			nonzero)
	}
}

// equalFloats — поэлементное сравнение, где nil ≡ пустой слайс (сид не задаёт
// профиль через nil, но это не должно быть расхождением, если кто-то поставит
// пустой литерал).
func equalFloats(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestSustainingWedgeFromReporting — клин AISC−TCC берётся из отчёта 2025,
// а не из устаревшей версии 2024 (там было 384).
func TestSustainingWedgeFromReporting(t *testing.T) {
	if sustainingWedgeUSDPerOz != 698.0 {
		t.Fatalf("sustainingWedgeUSDPerOz = %v, want 698 (AISC 1437 − TCC 739, Годовой обзор 2025)",
			sustainingWedgeUSDPerOz)
	}
}
