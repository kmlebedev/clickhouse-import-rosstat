package views

import (
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

// selectedKeyRateDashboardAliases — явный список колонок выдачи v_key_rate_dashboard.
// selectedColumns здесь не годится: он разбирает только простой SELECT, а витрина
// начинается с WITH, содержит десять one-row CTE и UNION ALL из двух десятков веток,
// которые хелпер принял бы за несколько верхних SELECT'ов. Поэтому список alias'ов
// задан руками и сверяется с SQL точечно.
var selectedKeyRateDashboardAliases = []string{
	"contour",
	"indicator",
	"value",
	"unit",
	"as_of",
	"target_date",
	"threshold",
	"flag",
	"stale",
	"error_bp",
	"probabilities",
	"model",
	"data_version",
	"prompt_version",
}

// Контракт сводного дашборда контура ключевой ставки: имя, ровно 11 сырых
// таблиц-источников в порядке гейта util.CreateView, 14 колонок с комментариями
// и ключевые элементы расчёта в SQL. Проверки текстуальные: в тестовой среде нет
// ClickHouse, поэтому сам расчёт исполняется только живым прогоном.
func TestKeyRateDashboardViewContract(t *testing.T) {
	v := keyRateDashboardView

	if v.Name != "v_key_rate_dashboard" {
		t.Fatalf("Name = %q, want v_key_rate_dashboard", v.Name)
	}

	// Порядок таблиц — порядок гейта util.CreateView: витрина создаётся только
	// когда на месте все 11 сырых источников; ни одной чужой витрины в списке нет.
	wantTables := []string{
		"cbr_key_rate",
		"cbr_indicators_cpd",
		"cbr_infl_exp",
		"rosstat_ipc_mes",
		"ofz_curve",
		"cbr_ruania",
		"macro_series",
		"keyrate_futures",
		"keyrate_contract",
		"events_calendar",
		"forecast_log",
	}
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
		"'spot'",
		"'fundamental'",
		"'market'",
		"'divergence'",
		"'forecast'",
		"'calendar'",
		"'base_ipc_saar'",
		"'keyrate_implied_change_bp'",
		"'ofz_slope_10y_1y_bp'",
		"as_1y",
		"as_3y",
		"as_5y",
		"as_10y",
		"least(",
		"count() = 12",
		"uniqExact(toStartOfMonth(",
		"subtractMonths",
		"dateDiff('month'",
		"if(d.target_date = d.next_date",
		"row_number()",
		"argMax",
		"'next_meeting'",
		"error_bp",
		"LIKE 'cbr_rate:%'",
		"metric NOT LIKE '%:shadow'",
		"metric NOT LIKE '%:canary'",
		"pow(1 +",
		"FINAL",
		"minOrNull",
		"nullIf",
	} {
		if !strings.Contains(selectText, marker) {
			t.Errorf("SELECT does not contain %q", marker)
		}
	}

	if len(selectedKeyRateDashboardAliases) != 14 {
		t.Fatalf("alias list has %d entries, want 14", len(selectedKeyRateDashboardAliases))
	}
	if len(v.Columns) != len(selectedKeyRateDashboardAliases) {
		t.Fatalf("Columns has %d entries, want %d", len(v.Columns), len(selectedKeyRateDashboardAliases))
	}

	for _, alias := range selectedKeyRateDashboardAliases {
		comment, ok := v.Columns[alias]
		if !ok {
			t.Errorf("column %q is selected but has no comment", alias)

			continue
		}
		if strings.TrimSpace(comment) == "" {
			t.Errorf("column %q has an empty comment", alias)
		}
		// Каждая ветка UNION ALL подписывает колонки явно, поэтому alias обязан
		// встречаться либо как AS <alias>, либо как SELECT <alias> (первая колонка).
		// Проверка по границам слова, а не подстрокой: « AS flag » не должно
		// засчитываться за « AS flagged », а « AS value » — за « AS value_x ».
		aliasPattern := regexp.MustCompile(`(?i)\bAS\s+` + regexp.QuoteMeta(alias) + `\b`)
		selectPattern := regexp.MustCompile(`(?i)\bSELECT\s+` + regexp.QuoteMeta(alias) + `\b`)
		if !aliasPattern.MatchString(selectText) && !selectPattern.MatchString(selectText) {
			t.Errorf("SELECT does not alias a column AS %s", alias)
		}
	}

	for alias := range v.Columns {
		if !contains(selectedKeyRateDashboardAliases, alias) {
			t.Errorf("comment for %q, which the SELECT does not return", alias)
		}
	}

	// Витрина обязана читать сырьё: FROM v_ означал бы зависимость от порядка
	// создания чужих витрин, а гейт util.CreateView проверяет только таблицы.
	if strings.Contains(selectText, "FROM v_") {
		t.Error("SELECT must read raw tables, not other views: found FROM v_")
	}

	if strings.Contains(selectText, "fmt.Print") {
		t.Error("SELECT must not contain fmt.Print")
	}
}

// Витрина обязана быть зарегистрирована в импортёре key_rate_views: без этого её
// никто не создаст, и грант в sql/mcp_kimi_reader.sql упадёт на несуществующей
// витрине. Проверка идёт по САМОМУ списку импортёра (keyRateViewsList) — см.
// комментарий у TestDcfAssumptionsViewIsBuiltByGoldViews.
func TestKeyRateDashboardViewIsBuiltByKeyRateViews(t *testing.T) {
	var found bool

	for _, v := range keyRateViewsList {
		if v.Name == keyRateDashboardView.Name {
			found = true
		}
	}

	if !found {
		t.Fatalf("%s must be created by the key_rate_views importer: it is missing from keyRateViewsList", keyRateDashboardView.Name)
	}

	if keyRateDashboardView.Comment == "" {
		t.Fatalf("%s has no table comment", keyRateDashboardView.Name)
	}
}

func TestKeyRateViewsImporterName(t *testing.T) {
	if got := (&keyRateViews{}).Name(); got != "key_rate_views" {
		t.Fatalf("Name() = %q, want key_rate_views", got)
	}
}

// Импортёр обязан быть зарегистрирован: без записи в chimport.Stats его не
// выберет ни CLICKHOUSE_IMPORT_STAT, ни make import STAT=key_rate_views, и
// витрина не появится.
func TestKeyRateViewsImporterIsRegistered(t *testing.T) {
	for _, s := range chimport.Stats {
		if s.Name() == "key_rate_views" {
			return
		}
	}

	var names []string
	for _, s := range chimport.Stats {
		names = append(names, s.Name())
	}

	t.Fatalf("key_rate_views is not registered in chimport.Stats; registered: %v", names)
}
