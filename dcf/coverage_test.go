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

// TestUncoveredPlanYearsJunkYear — Finding 1 (вторая половина): мусорный год
// непокрыт ПО ОПРЕДЕЛЕНИЮ, даже когда дек стоит ровно на него.
//
// Это то состояние, которое в реальном прогоне даёт успешный сид на 2100 и
// «покрытый» год: год-в-год гард не должен считать его планом. Тест закрепляет
// именно эту границу — цена на мусорный год не делает его законным.
func TestUncoveredPlanYearsJunkYear(t *testing.T) {
	plans := []MinePlanRecord{
		{Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2100}}},
	}
	decks := map[string][]DeckYear{"spot_flat": {{Year: 2100, GoldUSD: 4000}}}

	got := uncoveredPlanYears(plans, decks)
	if len(got) != 1 || got[0] != "A:2100" {
		t.Fatalf("uncoveredPlanYears = %v, want [A:2100] — дек на 2100 не покрывает мусорный год", got)
	}

	if err := checkYearCoverage(plans, decks); err == nil {
		t.Fatal("план только из мусорного года обязан быть ошибкой, а не успешным прогоном")
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

// TestCheckYearCoverageSharedYear — два актива на общем году (штатная раскладка
// mine_plans: engine_test — два актива на 2027, seed_test — на 2028) не должны
// путать решение гарда: непокрытых ПАР больше, чем годов плана, поэтому гард
// обязан решать по пересечению множеств годов, а не по длинам срезов.
func TestCheckYearCoverageSharedYear(t *testing.T) {
	// Оба актива на 2027, деков на 2027 нет: непокрыто НИЧЕГО, значит ошибка
	// (иначе расчёт молча запишет нулевой NPV).
	noneCovered := []MinePlanRecord{
		{Company: "PLZL", Asset: "Olimpiada", Years: []MinePlanYear{{Year: 2027}}},
		{Company: "PLZL", Asset: "Blagodatnoye", Years: []MinePlanYear{{Year: 2027}}},
	}
	if err := checkYearCoverage(noneCovered, map[string][]DeckYear{"spot_flat": {{Year: 2026}}}); err == nil {
		t.Fatal("два актива на одном непокрытом году: обязана быть ошибка — " +
			"иначе guard пропустит расчёт с нулевым NAV")
	}

	// A{2026,2027}, B{2027}, дек только на 2026: 2026 покрыт корректно, значит это
	// частичное покрытие — warn-and-continue, а не падение на recoverable-состоянии БД.
	partial := []MinePlanRecord{
		{Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2026}, {Year: 2027}}},
		{Company: "PLZL", Asset: "B", Years: []MinePlanYear{{Year: 2027}}},
	}
	if err := checkYearCoverage(partial, map[string][]DeckYear{"spot_flat": {{Year: 2026}}}); err != nil {
		t.Fatalf("частичное покрытие (2026 покрыт) — только warn, расчёт продолжается: %v", err)
	}

	// Один общий год покрыт у двух активов — тоже не ошибка.
	sharedCovered := noneCovered
	if err := checkYearCoverage(sharedCovered, map[string][]DeckYear{"spot_flat": {{Year: 2027}}}); err != nil {
		t.Fatalf("общий год обоих активов покрыт: %v", err)
	}
}
