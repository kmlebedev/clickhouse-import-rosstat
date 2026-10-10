package polyus

import "testing"

// Два разных документа за один период обязаны оба дойти до батча, даже если у них
// совпал вид документа. Это офлайн-аналог живой потери: KPI-релизы FY2023 и FY2024
// несут один source_kind = "kpi" и печатают за 2023FY разные числа (2902 и 2799).
//
// Пока ключ нёс только (company, metric, period, source_kind), второй релиз
// отбрасывался как «повтор», и спор двух документов, ради которого витрина и
// заводилась, оставался невидимым. Различает их source_url.
func TestBatchDedupKeepsBothDocumentsOfSameKind(t *testing.T) {
	var d batchDedup

	fy2023 := MetricRecord{
		Company:    "PLZL",
		Metric:     "gold_output",
		Period:     "2023FY",
		SourceKind: "kpi",
		SourceURL:  "https://polyus.com/en/investors/results-and-reports/fy2023.pdf",
	}
	fy2024 := fy2023
	// Тот же показатель, тот же период, тот же вид документа — отличается только
	// документ. Период намеренно одинаковый: именно он и есть предмет сверки.
	fy2024.SourceURL = "https://polyus.com/en/investors/results-and-reports/fy2024.pdf"

	if !d.add(fy2023) {
		t.Fatal("the FY2023 release must enter the batch")
	}
	if !d.add(fy2024) {
		t.Fatal("the FY2024 release must also enter the batch: it is another document, not a repeat. " +
			"Without source_url in batchKey its value for 2023FY is dropped silently, " +
			"and v_company_metric_sources loses the disagreement between the two releases")
	}
	if d.duplicates != 0 {
		t.Fatalf("duplicates = %d, want 0: the two records differ by source_url", d.duplicates)
	}

	// Настоящий повтор — те же пять полей целиком — по-прежнему отбрасывается:
	// ключ различает документы, а не отменяет дедупликацию.
	if d.add(fy2023) {
		t.Fatal("an exact repeat of the same five fields must be rejected")
	}
	if d.duplicates != 1 {
		t.Fatalf("duplicates = %d, want 1", d.duplicates)
	}
}
