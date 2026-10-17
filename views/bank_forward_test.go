package views

import (
	"strings"
	"testing"
)

// selectedBankForwardAliases — явный список колонок выдачи
// v_bank_loan_portfolio_forward. selectedColumns здесь не годится: он разбирает
// только простой SELECT, а витрина начинается с WITH и содержит три CTE, которые
// он принял бы за несколько верхних SELECT'ов. Поэтому список alias'ов задан
// руками и сверяется с SQL точечно.
var selectedBankForwardAliases = []string{
	"bank",
	"report_month",
	"metric",
	"source_kind",
	"value_bln_rub",
	"mom_pct",
	"yoy_pct",
	"publish_date",
	"event_status",
	"date_status",
	"cbr_credit_total_bln_rub",
	"cbr_mom_pct",
	"cbr_yoy_pct",
	"bank_share_of_cbr_pct",
}

// Контракт секторной форвардной витрины кредитного портфеля: имя, ровно три
// таблицы-источника в порядке гейта util.CreateView, 14 колонок с комментариями
// и ключевые элементы расчёта в SQL. Проверки текстуальные: в тестовой среде
// нет ClickHouse, поэтому сам расчёт исполняется только живым прогоном.
func TestBankLoanPortfolioForwardViewContract(t *testing.T) {
	v := bankLoanPortfolioForwardView

	if v.Name != "v_bank_loan_portfolio_forward" {
		t.Fatalf("Name = %q, want v_bank_loan_portfolio_forward", v.Name)
	}

	wantTables := []string{"company_financials", "events_calendar", "cbr_credit_m2x"}
	if len(v.Tables) != len(wantTables) {
		t.Fatalf("Tables = %v, want %v", v.Tables, wantTables)
	}
	for i, table := range wantTables {
		if v.Tables[i] != table {
			t.Errorf("Tables[%d] = %q, want %q", i, v.Tables[i], table)
		}
	}

	selectText := collapseSpaces(v.Select)
	for _, marker := range []string{
		"v_company_financials",
		"events_calendar",
		"cbr_credit_m2x",
		"period_type = 'M'",
		"'customer_loans'",
		"'loans_corporations'",
		"'loans_individuals'",
		"lagInFrame",
		"JSONExtractString(threshold, 'bank')",
		"subtractMonths(date, 1)",
		"/ 1000",
		"WINDOW w",
	} {
		if !strings.Contains(selectText, marker) {
			t.Errorf("SELECT does not contain %q", marker)
		}
	}

	if len(selectedBankForwardAliases) != 14 {
		t.Fatalf("alias list has %d entries, want 14", len(selectedBankForwardAliases))
	}
	if len(v.Columns) != len(selectedBankForwardAliases) {
		t.Fatalf("Columns has %d entries, want %d", len(v.Columns), len(selectedBankForwardAliases))
	}

	for _, alias := range selectedBankForwardAliases {
		comment, ok := v.Columns[alias]
		if !ok {
			t.Errorf("column %q is selected but has no comment", alias)

			continue
		}
		if comment == "" {
			t.Errorf("column %q has an empty comment", alias)
		}
		// Первые колонки выдаются квалифицированными именами b.<alias>, остальные —
		// выражением с явным AS <alias>.
		if alias == "bank" || alias == "report_month" || alias == "metric" || alias == "source_kind" {
			if !strings.Contains(selectText, "b."+alias) {
				t.Errorf("SELECT does not select b.%s", alias)
			}
		} else if !strings.Contains(selectText, " AS "+alias) {
			t.Errorf("SELECT does not alias a column AS %s", alias)
		}
	}

	for alias := range v.Columns {
		if !contains(selectedBankForwardAliases, alias) {
			t.Errorf("comment for %q, which the SELECT does not return", alias)
		}
	}
}
