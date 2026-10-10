package polyus

import "testing"

// TestEnabledReports держит таблицу отчётов: у включённого отчёта обязаны быть
// корректные Kind/Lang и непустой список страниц — иначе отчёт молча даст ноль
// записей (extractPDF вызывается без страниц) или упадёт в разборе.
func TestEnabledReports(t *testing.T) {
	var enabled int
	for _, r := range reports {
		if r.Enabled {
			enabled++
			if r.Kind != "kpi" && r.Kind != "ifrs" {
				t.Errorf("report %s has invalid Kind %q", r.URL, r.Kind)
			}
			if r.Lang != "en" && r.Lang != "ru" {
				t.Errorf("report %s has invalid Lang %q", r.URL, r.Lang)
			}
			if len(r.Pages) == 0 {
				t.Errorf("report %s is enabled but has no pages", r.URL)
			}
		}
	}
	if enabled == 0 {
		t.Fatal("no enabled reports")
	}
}

func TestDisabledReportsHavePeriods(t *testing.T) {
	for _, r := range reports {
		if r.Period == "" {
			t.Errorf("report %s has empty Period", r.URL)
		}
	}
}
