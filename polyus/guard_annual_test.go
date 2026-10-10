package polyus

import "testing"

// TestGuardKeepsAnnualPeriods держит годовые периоды: 2014FY и 2013FY обязаны
// доходить до витрины.
//
// Это та регрессия, ради которой старый фильтр и был переписан: фильтр по
// глобальному списку полугодий (2026H1, 2025H1) отбрасывал ВСЕ записи годового
// MD&A за 2014 — годовой период в список полугодий не попадает в принципе, и
// отчёт целиком исчезал из витрины.
//
// Новый guard не отбрасывает по периоду вовсе: колоночная модель кладёт значение
// по его X, поэтому годовой период не может оказаться «не тем» периодом.
// Проверяется это на разборе, а не через список периодов: раньше тест брал шапку
// страницы (headerPeriods) и решал по ней; теперь такого решения нет.
//
// Замерено по фикстуре testdata/press_release_hist_p1.tsv (страница 4 отчёта
// FY2014) — колонки шапки и строки «Total revenue»:
//
//	шапка:      FY 2014   FY 2013   [y-o-y change]   2H 2014   1H 2014
//	данные:       2 239     2 329          (4%)        1 232      1 007
//
// Все три периода с числами — 2014FY, 2013FY и 2014H2 — настоящие колонки
// отчёта, и каждая обязана дойти до batch. Колонка изменения (4%) периода не
// даёт и потому в разборе не появляется как период.
func TestGuardKeepsAnnualPeriods(t *testing.T) {
	const path = "testdata/press_release_hist_p1.tsv"

	records, err := parseKPIPage(path, "https://example.invalid/fy2014.pdf", 4)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("fixture parsed to zero records: the test lost its subject")
	}

	kept, dropped := guardRecords(records)

	if dropped != 0 {
		t.Errorf("guard dropped %d records, want 0: nothing may be dropped by period", dropped)
	}
	if len(kept) != len(records) {
		t.Fatalf("kept %d records, want all %d", len(kept), len(records))
	}

	annual := 0
	for _, r := range kept {
		if r.Period == "2014FY" || r.Period == "2013FY" {
			annual++
		}
	}
	if annual == 0 {
		t.Fatal("annual periods were dropped — guard regressed to whitelist behaviour")
	}

	// Годовое значение не подменено колонкой изменения: у «Total revenue» год
	// 2014 равен 2 239, тогда как (4%) — это колонка изменения.
	revenueAnnual := 0
	for _, r := range kept {
		if r.Period != "2014FY" || (r.Metric != "revenue" && r.Metric != "total_revenue") {
			continue
		}

		revenueAnnual++

		if r.Value != 2239 {
			t.Errorf("%s 2014FY = %v, want 2239", r.Metric, r.Value)
		}
	}
	if revenueAnnual != 1 {
		t.Errorf("revenue 2014FY records = %d, want 1", revenueAnnual)
	}

	// Годовой период не один: в отчёте за FY2014 есть и сравнительный FY2013.
	revenuePrior := 0
	for _, r := range kept {
		if r.Period == "2013FY" && (r.Metric == "revenue" || r.Metric == "total_revenue") {
			revenuePrior++

			if r.Value != 2329 {
				t.Errorf("%s 2013FY = %v, want 2329", r.Metric, r.Value)
			}
		}
	}
	if revenuePrior != 1 {
		t.Errorf("revenue 2013FY records = %d, want 1", revenuePrior)
	}
}
