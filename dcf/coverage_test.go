package dcf

import "testing"

// TestUncoveredPlanYears — гард делает пропуск года видимым.
func TestUncoveredPlanYears(t *testing.T) {
	plans := []MinePlanRecord{
		{Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2026}, {Year: 2027}}},
	}
	decks := map[string][]DeckYear{"spot_flat": {{Year: 2026, GoldUSD: 4000}}}

	got := uncoveredPlanYears(plans, decks)
	if len(got) != 1 || got[0] != "A:2027" {
		t.Fatalf("uncoveredPlanYears = %v, want [A:2027]", got)
	}

	full := map[string][]DeckYear{"spot_flat": {{2026, 4000}, {2027, 4100}}}
	if got := uncoveredPlanYears(plans, full); len(got) != 0 {
		t.Fatalf("полное покрытие: %v, want пусто", got)
	}
}

// TestCheckYearCoverage — полное отсутствие пересечения это ошибка, частичное — warn.
func TestCheckYearCoverage(t *testing.T) {
	plans := []MinePlanRecord{{Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2030}}}}

	if err := checkYearCoverage(plans, map[string][]DeckYear{"spot_flat": {{Year: 2026}}}); err == nil {
		t.Fatal("ни один год плана не покрыт: обязана быть ошибка, иначе непустые данные " +
			"дадут нулевой NAV с успешным логом")
	}
	if err := checkYearCoverage(plans, map[string][]DeckYear{"spot_flat": {{Year: 2030}}}); err != nil {
		t.Fatalf("покрытый год не должен быть ошибкой: %v", err)
	}
	if err := checkYearCoverage(nil, nil); err != nil {
		t.Fatalf("пустой план — не ошибка (обрабатывается раньше в Import): %v", err)
	}
}
