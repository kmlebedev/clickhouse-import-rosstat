package polyus

import "testing"

// TestGuardKeepsAnnualPeriods держит критерий фильтра: он обязан оценивать
// периоды по шапке самого отчёта, а не по глобальному списку полугодий.
//
// Годовой MD&A за 2014 включён в reports как обычный KPI-отчёт. Старый фильтр по
// глобальному списку полугодий (2026H1, 2025H1) отбрасывал ВСЕ 21 запись отчёта
// (kept=0): годовой период в список полугодий попасть не может в принципе, и
// отчёт целиком исчезал из витрины, а отброс шёл на уровне Debug. Этот тест
// падает на старом фильтре и проходит на новом.
//
// Что именно легитимно в этом отчёте — проверено по шапке и строке данных
// фикстуры:
//
//	шапка:      FY 2014   FY 2013   [y-o-y change]   2H 2014   1H 2014
//	данные:       2 239     2 329          (4%)        1 232      1 007
//	токен:           0         1            2            3          4
//
// parseKPIPage находит только три колонки и нумерует их подряд (0, 1, 2),
// поэтому третий токен — «Изм. за год» — достаётся третьей колонке, и период
// этой колонки недостоверен. Годовой период отчёта (2014FY, токен 0) стоит на
// своём месте и обязан выжить: именно его существование ломало глобальный
// список.
//
// Второй найденный период (2014H2) несёт значение 2 329, то есть данные колонки
// «FY 2013»: это тоже подмена, но устранить её одним индексным сдвигом нельзя —
// период 2013FY был потерян ещё при разборе шапки. Он остаётся в витрине и
// вынесен в отчёт как известное ограничение; тест его не закрепляет.
func TestGuardKeepsAnnualPeriods(t *testing.T) {
	// Страница 1 фикстуры FY2014 — та же, что у включённого отчёта
	// testdata/press_release_hist_p1.txt в pages.go.
	const path = "testdata/press_release_hist_p1.txt"

	records, err := parseKPIPage(path, "u", 1)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("fixture parsed to zero records: the test lost its subject")
	}

	periods := headerPeriods(path)
	if len(periods) == 0 {
		t.Fatal("fixture header yielded no periods: the test lost its subject")
	}
	kept, dropped := guardKPIRecords(records, periods)

	// Отчёт, чьи периоды легитимны, не может быть выброшен целиком — это и был
	// основной дефект: молчаливая потеря всего отчёта.
	if len(kept) == 0 {
		t.Fatalf(
			"guard dropped every record of the FY2014 report (kept=0, dropped=%d): a whole enabled report vanished",
			dropped,
		)
	}

	// Годовой период легитимен и обязан выжить: он стоит на нулевом токене,
	// который ни один сдвиг не задевает.
	annual := 0
	for _, r := range kept {
		if r.Period == "2014FY" {
			annual++
		}
	}
	if annual == 0 {
		t.Error("no 2014FY record survived: the annual period is legitimate and must be kept")
	}

	// Значение годового периода не подменено: у Total revenue год 2014 равен
	// 2 239, тогда как «(4%)» — это колонка изменения.
	revenueAnnual := 0
	for _, r := range kept {
		if r.Metric == "revenue" && r.Period == "2014FY" {
			revenueAnnual++
			if r.Value != 2239 {
				t.Errorf("revenue 2014FY = %v, want 2239", r.Value)
			}
		}
	}
	if revenueAnnual != 1 {
		t.Errorf("revenue 2014FY records = %d, want 1", revenueAnnual)
	}

	// Подменённая колонка отброшена — та самая, чей токен 2 держит «(4%)».
	bad := untrustworthyPeriod(periods)
	if bad == "" {
		t.Fatal("untrustworthyPeriod returned empty: the guard has nothing to drop")
	}
	for _, r := range kept {
		if r.Period == bad {
			t.Errorf("metric %s period %s = %v survived, want dropped", r.Metric, r.Period, r.Value)
		}
	}
	if dropped != 7 {
		t.Errorf("dropped = %d, want 7 (one record per metric from the change column)", dropped)
	}
}
