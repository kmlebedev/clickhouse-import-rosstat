package polyus

import (
	"fmt"
	"strings"
	"testing"
)

// Two documents printing the same metric for the same period must both
// survive the batch: the key carries the document kind.
func TestBatchDedupKeepsBothSourceKinds(t *testing.T) {
	var d batchDedup

	kpiRecord := MetricRecord{Company: "PLZL", Metric: "gold_output", Period: "2023FY", SourceKind: "kpi"}
	ifrsRecord := MetricRecord{Company: "PLZL", Metric: "gold_output", Period: "2023FY", SourceKind: "ifrs"}

	if !d.add(kpiRecord) {
		t.Fatal("first record with source_kind=kpi must enter the batch")
	}
	if !d.add(ifrsRecord) {
		t.Fatal("same key with source_kind=ifrs must also enter the batch: the key carries the source")
	}
	if d.duplicates != 0 {
		t.Fatalf("duplicates = %d, want 0: the two records differ by source_kind", d.duplicates)
	}
	if d.add(kpiRecord) {
		t.Fatal("a true repeat of the same source_kind must be rejected")
	}
	if d.duplicates != 1 {
		t.Fatalf("duplicates = %d, want 1", d.duplicates)
	}
}

// The importer writes the shared table, and the DDL carries the document
// kind in ORDER BY — without it the two documents collapse again.
func TestFinancialMetricsTargetsCompanyFinancials(t *testing.T) {
	if financialMetricsTable != "company_financials" {
		t.Fatalf("table = %q, want company_financials", financialMetricsTable)
	}
	for _, want := range []string{"source_kind", "ORDER BY (company, metric, period, source_kind)"} {
		if !strings.Contains(financialMetricsCreateTable, want) {
			t.Fatalf("DDL missing %q:\n%s", want, financialMetricsCreateTable)
		}
	}

	// Имя таблицы в DDL стоит на месте %s: импортёр подставляет его сам через
	// fmt.Sprintf. Поэтому identity проверки — разрешённый DDL, а не шаблон.
	if resolved := fmt.Sprintf(financialMetricsCreateTable, financialMetricsTable); !strings.Contains(resolved, "company_financials") {
		t.Fatalf("resolved DDL does not name the table:\n%s", resolved)
	}
}
