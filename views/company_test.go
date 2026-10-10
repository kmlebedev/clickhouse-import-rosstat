package views

import (
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Each view must name all three of its sources in the ORDER BY / resolution
// key, and the comments must reach the agent: a view without comments is
// documented nowhere the agent can look.
func TestCompanyViewsAreDocumented(t *testing.T) {
	for _, v := range []util.View{companyFinancialsView, companyMetricSourcesView, companyOperatingView} {
		if v.Comment == "" {
			t.Fatalf("view %s has no table comment", v.Name)
		}
		if v.Select == "" {
			t.Fatalf("view %s has no SELECT", v.Name)
		}
		if len(v.Columns) == 0 {
			t.Fatalf("view %s has no column comments", v.Name)
		}
	}
}

// The resolved view must prefer the audited form; the sources view must not
// apply any preference, otherwise a disagreement becomes invisible.
func TestCompanyFinancialsResolutionPrefersIFRS(t *testing.T) {
	if !strings.Contains(companyFinancialsView.Select, "'ifrs'") {
		t.Fatal("v_company_financials must reference source_kind 'ifrs' in its resolution")
	}
	for _, marker := range []string{"argMax", "row_number", "LIMIT 1 BY", "argMin"} {
		if strings.Contains(companyMetricSourcesView.Select, marker) {
			t.Fatalf("v_company_metric_sources must list every source, found %q", marker)
		}
	}
}

// Резолюция spec §3.4 живёт в SQL и юнит-тестом не исполняется (в тестовой среде
// нет ClickHouse). Здесь проверяется, что она ВЫРАЖЕНА именно так и в том
// порядке: перепутанный порядок полей кортежа отдал бы спор не аудиту, а
// пресс-релизу, и заметить это можно было бы только по данным.
func TestCompanyFinancialsResolutionOrder(t *testing.T) {
	selectText := collapseSpaces(companyFinancialsView.Select)

	if !strings.Contains(selectText, "argMax(value, (source_kind = 'ifrs', loaded_at, source_url)) AS r_value") {
		t.Fatalf(
			"v_company_financials must resolve on (source_kind = 'ifrs', loaded_at, source_url), got: %s",
			selectText,
		)
	}
	// ifrs в ключе, а не сравнение в WHERE: WHERE отбросил бы пресс-релиз вместо
	// того, чтобы дать ему проиграть (и оставил бы период без строки, если
	// аудированной версии у него нет).
	if !strings.Contains(selectText, "source_kind = 'ifrs'") {
		t.Error("v_company_financials must compare source_kind = 'ifrs' inside the resolution key")
	}
	// Кортеж как второй аргумент argMax, а не отдельный row_number: витрина
	// остаётся потоковой агрегацией над FINAL.
	if strings.Contains(selectText, "row_number") {
		t.Error("v_company_financials must not use row_number() over FINAL")
	}
	if !strings.Contains(selectText, "FROM company_financials FINAL") {
		t.Error("v_company_financials must read company_financials FINAL")
	}
	if !strings.Contains(selectText, "GROUP BY company, metric, period") {
		t.Error("v_company_financials must collapse to one row per (company, metric, period)")
	}
}

// Регрессия на ILLEGAL_AGGREGATION: агрегат нельзя проецировать под именем своей
// же исходной колонки. ClickHouse привязывает такое имя внутри кортежа-ключа к
// алиасу агрегата, а не к колонке таблицы, и отказывается создавать витрину
// («Code: 184 ... is found inside another aggregate function»). Проверка
// текстуальная: имя колонки во внутреннем SELECT обязано отличаться от имени её
// агрегата.
func TestCompanyFinancialsAggregatesDoNotShadowColumnNames(t *testing.T) {
	inner := innerSelect(companyFinancialsView.Select)
	if inner == "" {
		t.Fatal("v_company_financials has no inner SELECT")
	}

	shadowed := 0

	for _, line := range strings.Split(inner, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if !strings.HasPrefix(line, "argMax(") {
			continue
		}

		idx := strings.LastIndex(line, " AS ")
		if idx < 0 {
			// Компания, metric и period идут во внутренний SELECT без агрегата —
			// это и есть случай, когда совпадение имён безвредно.
			continue
		}

		column := strings.TrimSpace(line[strings.Index(line, "(")+1 : strings.Index(line, ",")])
		alias := strings.TrimSpace(line[idx+4:])
		if alias != column {
			continue
		}

		shadowed++
		t.Errorf(
			"aggregate %s is aliased to its own column name (ILLEGAL_AGGREGATION): project it under an r_* alias and rename it in the outer SELECT",
			column,
		)
	}

	if shadowed == 0 && !strings.Contains(inner, " AS r_") {
		t.Error("no r_* alias found: the inner aggregates must not carry the view's column names")
	}

	// Наружу колонки обязаны вернуться под своими настоящими именами.
	for _, alias := range []string{
		"r_period_type AS period_type",
		"r_source_kind AS source_kind",
		"r_value AS value",
		"r_unit AS unit",
		"r_source_url AS source_url",
		"r_source_page AS source_page",
		"r_loaded_at AS loaded_at",
	} {
		if !strings.Contains(collapseSpaces(companyFinancialsView.Select), alias) {
			t.Errorf("outer SELECT must map %q", alias)
		}
	}
}

// innerSelect возвращает текст первого подзапроса в скобках, то есть внутренний
// SELECT витрины: агрегаты и их псевдонимы живут именно там.
func innerSelect(selectText string) string {
	start := strings.Index(selectText, "FROM (")
	if start < 0 {
		return ""
	}

	end := strings.LastIndex(selectText, "\n)")
	if end < start {
		return ""
	}

	return selectText[start+len("FROM (") : end]
}

// Витрины читают сырые таблицы, поэтому гейт util.CreateView (он пропускает
// витрину, пока нет ни одной из Tables) обязан называть именно те таблицы, что
// стоят в SELECT: чужое имя отложило бы витрину до импорта, который к ней
// отношения не имеет, а отсутствующее — создало бы её до появления источника.
func TestCompanyViewsNameTheirSources(t *testing.T) {
	cases := []struct {
		view   util.View
		tables []string
	}{
		{companyFinancialsView, []string{companyFinancialsTable}},
		{companyMetricSourcesView, []string{companyFinancialsTable}},
		{companyOperatingView, []string{"databook_polyus"}},
	}

	for _, tc := range cases {
		if len(tc.view.Tables) != len(tc.tables) {
			t.Errorf("%s: Tables = %v, want %v", tc.view.Name, tc.view.Tables, tc.tables)

			continue
		}

		for i, table := range tc.tables {
			if tc.view.Tables[i] != table {
				t.Errorf("%s: Tables[%d] = %q, want %q", tc.view.Name, i, tc.view.Tables[i], table)
			}

			if !strings.Contains(tc.view.Select, table) {
				t.Errorf("%s: SELECT does not read %q", tc.view.Name, table)
			}
		}
	}
}

// Каждая колонка витрины должна быть прокомментирована: комментарий колонки —
// единственное место, где агент читает её смысл, и колонка без него пропадёт из
// выдачи инструмента.
func TestCompanyViewColumnsAreCommented(t *testing.T) {
	for _, v := range []util.View{companyFinancialsView, companyMetricSourcesView, companyOperatingView} {
		columns := selectedColumns(v.Select)
		if len(columns) == 0 {
			t.Fatalf("%s: no selected columns recognised", v.Name)
		}

		for _, column := range columns {
			if _, ok := v.Columns[column]; !ok {
				t.Errorf("%s: column %q has no comment", v.Name, column)
			}
		}

		for column := range v.Columns {
			if !contains(columns, column) {
				t.Errorf("%s: comment for %q, which the SELECT does not return", v.Name, column)
			}
		}
	}
}

// Тест на «первое имя строки SELECT» держит то, что DDL витрин исполняется как
// есть, без подстановки имени: шаблон с %s оставил бы в ClickHouse таблицу с
// литеральным «%s» в имени, а строка ORDER BY несёт ключ ReplacingMergeTree, и
// разойтись с DDL импортёра polyus/ она не имеет права.
func TestCompanyViewsDDL(t *testing.T) {
	stmts := companyViewsDDL()
	if len(stmts) == 0 {
		t.Fatal("companyViewsDDL returned no statements")
	}

	table := stmts[0]
	if !strings.Contains(table, "CREATE TABLE IF NOT EXISTS "+companyFinancialsTable) {
		t.Fatalf("first statement must create %s, got %q", companyFinancialsTable, table)
	}
	if strings.Contains(table, "%s") {
		t.Errorf("the table statement must carry the resolved name, not a %s placeholder", "%s")
	}
	if !strings.Contains(table, "ORDER BY (company, metric, period, source_kind)") {
		t.Error("table statement must key on (company, metric, period, source_kind)")
	}
	if !strings.Contains(table, "ENGINE = ReplacingMergeTree(loaded_at)") {
		t.Error("table statement must use ReplacingMergeTree(loaded_at)")
	}

	// Имя таблицы обязано встречаться и в витринах, которые её читают.
	for _, v := range []util.View{companyFinancialsView, companyMetricSourcesView} {
		if !strings.Contains(v.Select, companyFinancialsTable) {
			t.Errorf("%s must read %s", v.Name, companyFinancialsTable)
		}
	}
}

// selectedColumns достаёт идентификаторы колонок верхнего SELECT. Разбор простой
// и нарочно узкий: он смотрит только на строки первого SELECT до FROM на верхнем
// уровне и молча пропускает всё, что не похоже на «имя» или «выражение AS имя» —
// псевдонимы пробы ради, а не потому, что текст вьюхи заранее известен.
func selectedColumns(selectText string) []string {
	var (
		columns []string
		depth   int
	)

	for _, raw := range strings.Split(selectText, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}

		// Строка SELECT подзапроса открывает скобку: её колонки относятся к
		// внутреннему запросу, а не к выдаче витрины.
		if strings.HasPrefix(line, "FROM") || strings.HasPrefix(line, ")") {
			depth--
			if depth < 0 {
				break
			}

			continue
		}
		if strings.HasPrefix(line, "SELECT") {
			if depth > 0 || strings.Contains(line, "(") {
				depth++

				continue
			}
			// Верхний SELECT: его список и есть выдача.
			continue
		}
		if depth > 0 {
			continue
		}

		line = strings.TrimSuffix(line, ",")
		if idx := strings.LastIndex(line, " AS "); idx >= 0 {
			columns = append(columns, strings.TrimSpace(line[idx+4:]))

			continue
		}
		if isIdentifier(line) {
			columns = append(columns, line)
		}
	}

	return columns
}

func isIdentifier(s string) bool {
	if s == "" {
		return false
	}

	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
		default:
			return false
		}
	}

	return true
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}

	return false
}

// collapseSpaces убирает переводы строк и повторные пробелы: длинные SQL-строки
// витрин перенесены по строкам, и поиск подстроки по сырому тексту ловил бы
// форматирование, а не содержание.
func collapseSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
