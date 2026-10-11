package views

import (
	"strings"
	"testing"
)

// Витрина обязана быть зарегистрирована в импортёре gold_views: без этого её
// никто не создаст, и грант в sql/mcp_kimi_reader.sql упадёт на несуществующей
// витрине. Проверка идёт по САМОМУ списку импортёра (goldViewsList) — см.
// комментарий у TestDcfAssumptionsViewIsBuiltByGoldViews.
func TestGoldForecastsViewIsBuiltByGoldViews(t *testing.T) {
	var found bool

	for _, v := range goldViewsList {
		if v.Name == goldForecastsView.Name {
			found = true
		}
	}

	if !found {
		t.Fatalf("%s must be created by the gold_views importer: it is missing from goldViewsList", goldForecastsView.Name)
	}

	if strings.TrimSpace(goldForecastsView.Comment) == "" {
		t.Fatalf("%s has no table comment", goldForecastsView.Name)
	}
}

// Комментарий колонки — единственное место, где агент читает её смысл через MCP.
// Колонки берутся из самого SELECT хелпером selectedColumns, а не списком в тесте:
// список в тесте пропустил бы колонку, добавленную в SELECT позже. Обратная
// проверка (комментарий к колонке, которой в выдаче нет) ловит опечатку, которую
// ClickHouse отверг бы только на живом прогоне.
func TestGoldForecastsViewColumnsAreCommented(t *testing.T) {
	v := goldForecastsView

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

// FINAL обязателен: gold_forecasts — ReplacingMergeTree по (author, published,
// horizon, scenario), и без FINAL повторный сид/повторный POST добавил бы в выдачу
// консенсуса обе версии строки. nullIf( — конвенция DDL «0 = поле не задано»
// (point-прогноз хранит low/high = 0, банковский range — point = 0, банковские
// строки — probability = 0); агент не должен читать эти нули как цены и вероятности.
//
// midpoint (Review Focus 5) — единая точка сравнения: point, если он задан, иначе
// середина диапазона; формула проверяется текстом SELECT, потому что иначе её
// семантику видно только на живом прогоне. ORDER BY фиксирует порядок выдачи.
func TestGoldForecastsViewSelectShape(t *testing.T) {
	v := goldForecastsView
	selectText := collapseSpaces(v.Select)

	if !strings.Contains(selectText, "FROM gold_forecasts FINAL") {
		t.Fatalf("%s: SELECT must read gold_forecasts FINAL, got: %s", v.Name, selectText)
	}

	if !strings.Contains(selectText, "nullIf(") {
		t.Fatalf("%s: SELECT must turn zero («поле не задано» в DDL) into NULL via nullIf, got: %s", v.Name, selectText)
	}

	if !strings.Contains(selectText, "if(point > 0, point, nullIf((low + high) / 2, 0)) AS midpoint") {
		t.Fatalf("%s: midpoint must be point when it is set, else (low + high) / 2, got: %s", v.Name, selectText)
	}

	if !strings.Contains(selectText, "ORDER BY published DESC, author, horizon, scenario") {
		t.Fatalf("%s: SELECT must order rows by published DESC, author, horizon, scenario, got: %s", v.Name, selectText)
	}

	if len(v.Tables) != 1 || v.Tables[0] != "gold_forecasts" {
		t.Fatalf("%s tables = %v, want [gold_forecasts]", v.Name, v.Tables)
	}
}
