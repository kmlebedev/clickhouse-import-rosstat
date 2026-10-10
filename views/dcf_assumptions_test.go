package views

import (
	"strings"
	"testing"
)

// Витрина — выход DCF-модели для агента, и наполняет её другой импортёр
// (dcf_engine), поэтому гейт util.CreateView обязан ждать именно nav_by_asset:
// чужое или отсутствующее имя либо отложило бы витрину до импорта, который к ней
// отношения не имеет, либо создало бы её раньше источника. Имя сверяется с
// SELECT: таблица-источник обязана быть той же, что читает витрина.
func TestDcfAssumptionsViewNamesItsSource(t *testing.T) {
	if dcfAssumptionsView.Name != "v_dcf_assumptions" {
		t.Fatalf("Name = %q, want v_dcf_assumptions", dcfAssumptionsView.Name)
	}

	if len(dcfAssumptionsView.Tables) != 1 || dcfAssumptionsView.Tables[0] != "nav_by_asset" {
		t.Fatalf("Tables = %v, want [nav_by_asset]: гейт CreateView пропустит витрину, пока нет таблицы-источника", dcfAssumptionsView.Tables)
	}

	if !strings.Contains(dcfAssumptionsView.Select, "FROM nav_by_asset FINAL") {
		t.Error("SELECT must read nav_by_asset FINAL: ReplacingMergeTree без FINAL отдаёт несколько версий строки одного (run_id, deck, asset, contour)")
	}
}

// Каждая колонка витрины должна быть прокомментирована, и наоборот: комментарий
// колонки — единственное место, где агент читает её смысл через MCP. Обратная
// проверка (комментарий к колонке, которой в выдаче нет) не менее важна: такой
// комментарий не привязан ни к чему, а ALTER TABLE ... COMMENT COLUMN на
// отсутствующей колонке ClickHouse отвергает на живом прогоне, не в тесте.
//
// Колонки берутся из самого SELECT хелпером selectedColumns, а не списком в
// тесте: список в тесте пропустил бы колонку, добавленную в SELECT позже.
func TestDcfAssumptionsViewColumnsAreCommented(t *testing.T) {
	v := dcfAssumptionsView

	columns := selectedColumns(v.Select)
	if len(columns) == 0 {
		t.Fatalf("%s: no selected columns recognised", v.Name)
	}

	for _, column := range columns {
		if strings.TrimSpace(v.Columns[column]) == "" {
			t.Errorf("%s: column %q has no comment — агент не поймёт её через MCP", v.Name, column)
		}
	}

	for column := range v.Columns {
		if !contains(columns, column) {
			t.Errorf("%s: comment for %q, which the SELECT does not return", v.Name, column)
		}
	}
}

// Разрез «три дека × два контура» — не украшение витрины, а её смысл: без
// разбивки по контурам ставки сопоставление с глобальным P/NAV невозможно
// (roadmap §13.1), а оконный итог обязан считаться внутри той же тройки, иначе
// сумма сложит разные ставки или разные ценовые сценарии.
func TestDcfAssumptionsViewTotalsWithinDeckAndContour(t *testing.T) {
	selectText := collapseSpaces(dcfAssumptionsView.Select)

	if !strings.Contains(selectText, "sum(npv_usd_mln) OVER (PARTITION BY run_id, deck, contour) AS nav_total_usd_mln") {
		t.Fatalf(
			"nav_total_usd_mln must be summed per (run_id, deck, contour), got: %s",
			selectText,
		)
	}

	for _, column := range []string{"run_id", "deck", "contour", "asset", "npv_usd_mln"} {
		if !strings.Contains(selectText, column) {
			t.Errorf("SELECT must return %q: строка витрины — один актив в разрезе (deck, contour)", column)
		}
	}
}

// nav_by_asset агенту не выдаётся (AGENTS.md, правило 11): читать её витрина
// обязана через DEFINER = default, и признак этого — именно SECURITY DEFINER в
// CREATE VIEW. util.CreateView ставит его сам, поэтому в Select его быть не
// должно, а в Tables источник обязан называться так же, как в SELECT.
func TestDcfAssumptionsViewReadsThroughDefiner(t *testing.T) {
	for _, marker := range []string{"SQL SECURITY", "DEFINER"} {
		if strings.Contains(dcfAssumptionsView.Select, marker) {
			t.Errorf("Select must not carry %q: util.CreateView adds DEFINER = default SQL SECURITY DEFINER itself", marker)
		}
	}
}

// Витрина обязана быть зарегистрирована в импортёре gold_views: без этого её
// никто не создаст, и грант в sql/mcp_kimi_reader.sql упадёт на несуществующей
// витрине. Проверка идёт по САМОМУ списку импортёра (goldViewsList), а не по
// списку, написанному здесь: свой список в тесте проверял бы сам себя — витрина,
// выпавшая из goldViewsList, оставляла бы такой тест зелёным.
func TestDcfAssumptionsViewIsBuiltByGoldViews(t *testing.T) {
	var found bool

	for _, v := range goldViewsList {
		if v.Name == dcfAssumptionsView.Name {
			found = true
		}
	}

	if !found {
		t.Fatalf("%s must be created by the gold_views importer: it is missing from goldViewsList", dcfAssumptionsView.Name)
	}

	if dcfAssumptionsView.Comment == "" {
		t.Fatalf("%s has no table comment", dcfAssumptionsView.Name)
	}
}
