package polyus

import "testing"

// TestGuardKPIChangeColumns держит защиту от подмены периодов в KPI-шапке.
//
// В русских пресс-релизах (например, за 1 п/г 2026) шапка содержит шесть колонок
// — «1 п/г 2026», «1 п/г 2025», «Изм. за год», «2 п/г 2025», «Изм. за п/г», — а
// parseKPIPage различает только три: слова «Изм. …» периодом не становятся, и
// процент изменения за год встаёт в слот 2025H2. На фикстуре
// testdata/press_reliz_1h26_p1.txt это даёт gold_output 2025H2 = -2 (это «(2%)»
// из колонки «Изм. за год»), gold_sold 2025H2 = -20 («(20%)») и ещё семь таких
// же записей: значения, которых в отчёте за 2 п/г 2025 нет вовсе.
//
// Юнит-тест на сам разбор такую подмену не поймает: parseKPIPage ведёт себя
// ровно так, как написано, и «правильное» поведение здесь — вопрос того, какие
// записи попадают в витрину. Поэтому проверяется именно фильтр, который стоит
// между разбором и batch.Append, и проверяется на том, что первым элементом
// рушит защиту: если фильтра нет, тест падает на gold_output 2025H2 = -2.
//
// Легитимные периоды (2026H1, 2025H1) обязаны выжить: их токены стоят на своих
// местах, и отбрасываются они только вместе с метрикой, чей период подменён.
// Что фильтр не выкидывает целый отчёт с другим типом периода — отдельный тест
// TestGuardKeepsAnnualPeriods.
func TestGuardKPIChangeColumns(t *testing.T) {
	const path = "testdata/press_reliz_1h26_p1.txt"

	records, err := parseKPIPage(path, "u", 1)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}

	periods := headerPeriods(path)
	kept, dropped := guardKPIRecords(records, periods)

	// Подменённый период — последняя колонка шапки: она и есть та, чьи значения
	// parseKPIPage берёт из следующей колонки таблицы через неопознанное
	// «Изм. за год».
	if got := untrustworthyPeriod(periods); got != "2025H2" {
		t.Fatalf("untrustworthyPeriod = %q, want 2025H2", got)
	}

	// Ни один подменённый период не попал в batch.
	for _, r := range kept {
		if r.Period == untrustworthyPeriod(periods) {
			t.Errorf(
				"period %s of metric %s reached the batch but its values came from a change column",
				r.Period,
				r.Metric,
			)
		}
	}

	// Каждая отброшенная запись принадлежит подменённому периоду: легитимные
	// периоды обязаны дойти до batch.
	if dropped == 0 {
		t.Fatal("no records dropped: the change-column guard is not wired in")
	}
	bad := untrustworthyPeriod(periods)
	for _, r := range records {
		if r.Period != bad {
			continue
		}
		found := false
		for _, keptRecord := range kept {
			if keptRecord == r {
				found = true
			}
		}
		if found {
			t.Errorf("metric %s period %s survived the guard", r.Metric, r.Period)
		}
	}

	// Точные значения: gold_output 2025H2 = -2 — это «(2%)» из колонки
	// «Изм. за год», и оно обязано быть отброшено вместе с остальными записями
	// этой метрики за 2025H2.
	var sawChangeColumnValue bool
	for _, r := range records {
		if r.Metric == "gold_output" && r.Period == "2025H2" && r.Value == -2 {
			sawChangeColumnValue = true
		}
	}
	if !sawChangeColumnValue {
		t.Fatal("fixture no longer produces gold_output 2025H2 = -2: the test lost its subject")
	}
	for _, r := range kept {
		if r.Metric == "gold_output" && r.Period == "2025H2" {
			t.Errorf("gold_output 2025H2 = %v reached the batch, want dropped", r.Value)
		}
	}

	// Легитимные периоды выживают и несут верные значения.
	want := map[string]float64{
		"gold_output/2026H1":       1287,
		"gold_output/2025H1":       1311,
		"gold_sold/2026H1":         950,
		"tcc_per_ounce/2026H1":     1069,
		"revenue/2026H1":           4674,
		"profit_for_period/2026H1": 829,
	}
	got := make(map[string]float64)
	for _, r := range kept {
		got[r.Metric+"/"+r.Period] = r.Value
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %v, want %v (legitimate period was dropped or mangled)", key, got[key], value)
		}
	}
}

// TestBatchKeysAreUnique держит слияние дублей, которое ClickHouse делает молча.
//
// Записи разных отчётов за один и тот же период дают один и тот же ключ
// ORDER BY (company, metric, period): KPI-релиз 1 п/г 2026 и МСФО-отчёт за тот же
// 2026H1 оба печатают eps_basic и eps_diluted за 2026H1. Сливать их нельзя — в
// релизе это 0,87 (прибыль на акцию по «1 п/г»), в МСФО 0,87 (за полугодие по
// МСБУ 33), и это разные величины с одной меткой. В живом прогоне такой батч из
// 28 записей ClickHouse записал как 25 строк («Wrote block with 25 rows ... rows/cols
// 28/9» в clickhouse.log), то есть три записи исчезли, а лог импортёра о них
// отчитался.
//
// Поэтому batch.Append вызывается только для ключа, которого в батче ещё не было:
// дубли считаются и логируются, а счётчик импортированных строк совпадает с
// числом сохранённых строк.
func TestBatchKeysAreUnique(t *testing.T) {
	dedup := batchDedup{seen: make(map[batchKey]bool)}

	records := []MetricRecord{
		{Metric: "eps_basic", Period: "2026H1", Value: 0.87},
		{Metric: "eps_diluted", Period: "2026H1", Value: 0.87},
		{Metric: "eps_basic", Period: "2026H1", Value: 0.87},
		{Metric: "gold_output", Period: "2026H1", Value: 1287},
	}

	var added []string
	for _, record := range records {
		if dedup.add(record) {
			added = append(added, record.Metric)
		}
	}

	want := []string{"eps_basic", "eps_diluted", "gold_output"}
	if len(added) != len(want) {
		t.Fatalf("batchDedup.add accepted %d records, want %d: %v", len(added), len(want), added)
	}
	for i := range want {
		if added[i] != want[i] {
			t.Errorf("added[%d] = %q, want %q", i, added[i], want[i])
		}
	}
	if dedup.duplicates != 1 {
		t.Errorf("duplicates = %d, want 1", dedup.duplicates)
	}
}

// TestBatchKeysCountRows держит счётчик строк импортёра: он равен числу вызовов
// batch.Append, то есть совпадает с batch.Rows(). Без слияния ключей импортёр
// отчитывался бы о 28 строках, записав 25.
func TestBatchKeysCountRows(t *testing.T) {
	// Ровно тот случай из живого прогона: две метрики, приходящие и из релиза, и
	// из МСФО за один период.
	records := []MetricRecord{
		{Metric: "eps_basic", Period: "2026H1", Value: 0.87},
		{Metric: "eps_diluted", Period: "2026H1", Value: 0.87},
		{Metric: "gold_output", Period: "2026H1", Value: 1287},
		{Metric: "eps_basic", Period: "2026H1", Value: 0.87},
	}

	// Тот же путь, что у Import: счётчик инкрементируется на каждом принятом
	// batch.Append, а не на каждой разобранной записи.
	dedup := batchDedup{seen: make(map[batchKey]bool)}
	count := 0
	for _, record := range records {
		if !dedup.add(record) {
			continue
		}
		count++
	}

	if count != 3 {
		t.Errorf("appended %d rows, want 3: appended rows must match rows stored by ClickHouse", count)
	}
	if count != len(records)-dedup.duplicates {
		t.Errorf("count = %d, but records(%d) - duplicates(%d) = %d", count, len(records), dedup.duplicates, len(records)-dedup.duplicates)
	}
}
