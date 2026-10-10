package polyus

import "testing"

func TestParseIFRSReport(t *testing.T) {
	records, err := parseIFRSPage("testdata/en_msfo_p6.txt", "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}
	got := map[string]float64{}
	for _, r := range records {
		got[r.Metric] = r.Value
	}
	// значения из otchetnost-6m2026_eng.pdf, страница 6 (в млн долларов США)
	if got["total_revenue"] != 4674 {
		t.Errorf("total_revenue = %v, want 4674", got["total_revenue"])
	}
	if got["profit_for_period"] != 829 {
		t.Errorf("profit_for_period = %v, want 829", got["profit_for_period"])
	}
	if got["eps_basic"] != 0.87 {
		t.Errorf("eps_basic = %v, want 0.87", got["eps_basic"])
	}
}

// TestParseIFRSReportFullPage держит весь набор статей страницы 6, а не только
// три проверяемые выше: сокращённая метка не должна перехватывать строку
// длинной, а скобки — превращаться в положительное значение.
func TestParseIFRSReportFullPage(t *testing.T) {
	records, err := parseIFRSPage("testdata/en_msfo_p6.txt", "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}

	got := map[string]float64{}
	for _, r := range records {
		got[r.Metric] = r.Value
		if r.Company != "PLZL" {
			t.Errorf("company for %s = %q, want PLZL", r.Metric, r.Company)
		}
		if r.Period != "2026H1" {
			t.Errorf("period for %s = %q, want 2026H1", r.Metric, r.Period)
		}
	}

	want := map[string]float64{
		"gold_sales":         4569,
		"other_sales":        105,
		"total_revenue":      4674,
		"operating_expenses": -3405,
		"profit_before_tax":  1269,
		"income_tax_expense": -440,
		"profit_for_period":  829,
		"eps_basic":          0.87,
		"eps_diluted":        0.87,
	}

	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %v, want %v", name, got[name], value)
		}
	}

	if len(records) != len(want) {
		t.Errorf("got %d records, want %d: %v", len(records), len(want), got)
	}
}

// TestParseIFRSProfitForPeriodNotAttributable держит перекрытие меток: строка
// "Profit for the period attributable to:" начинается с метки profit_for_period,
// но чисел не содержит, поэтому записью стать не должна — иначе строка
// «Profit for the period attributable to:» подменила бы собой настоящий
// profit_for_period (или добавила бы дубль без значения).
func TestParseIFRSProfitForPeriodNotAttributable(t *testing.T) {
	records, err := parseIFRSPage("testdata/en_msfo_p6.txt", "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}

	profitForPeriod := 0
	for _, r := range records {
		if r.Metric == "profit_for_period" {
			profitForPeriod++
			if r.Value != 829 {
				t.Errorf("profit_for_period = %v, want 829", r.Value)
			}
		}
	}
	if profitForPeriod != 1 {
		t.Errorf("profit_for_period records = %d, want 1 (attributable line must not add a duplicate)", profitForPeriod)
	}

	// Строка взвешенного числа акций начинается с того же маркера списка, что и
	// eps, но метку eps_basic не содержит: метрики по ней быть не должно.
	for _, r := range records {
		if r.Metric == "eps_basic" && r.Value == 950004 {
			t.Error("weighted average share count leaked into eps_basic")
		}
	}
}

// TestPeriodTypeCanonical проверяет вывод типа периода из канонической формы:
// МСФО-парсер получает период параметром уже склеенным ("2026H1"), а parsePeriod
// принимает только форму релиза ("1H2026").
func TestPeriodTypeCanonical(t *testing.T) {
	cases := map[string]string{
		"2026H1": "H",
		"2025H2": "H",
		"2026FY": "FY",
		"2021Q4": "Q",
		"":       "",
		"xxxxH1": "",
		"2026h1": "",
	}

	for period, want := range cases {
		if got := periodType(period); got != want {
			t.Errorf("periodType(%q) = %q, want %q", period, got, want)
		}
	}
}

func TestParseIFRSOnKPIPage(t *testing.T) {
	// перекрёстный случай: KPI-страница, поданная МСФО-парсеру
	records, err := parseIFRSPage("testdata/press_reliz_1h26_p1.txt", "https://example.invalid/x.pdf", 1, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage must not fail on a KPI page: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected no IFRS metrics on a KPI page, got %d", len(records))
	}
}
