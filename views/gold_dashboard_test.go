package views

import (
	"strings"
	"testing"
)

// Витрина обязана быть зарегистрирована в импортёре gold_views: без этого её
// никто не создаст, и грант в sql/mcp_kimi_reader.sql упадёт на несуществующей
// витрине. Проверка идёт по САМОМУ списку импортёра (goldViewsList) — см.
// комментарий у TestDcfAssumptionsViewIsBuiltByGoldViews.
func TestGoldDashboardViewIsBuiltByGoldViews(t *testing.T) {
	var found bool

	for _, v := range goldViewsList {
		if v.Name == goldDashboardView.Name {
			found = true
		}
	}

	if !found {
		t.Fatalf("%s must be created by the gold_views importer: it is missing from goldViewsList", goldDashboardView.Name)
	}

	if goldDashboardView.Comment == "" {
		t.Fatalf("%s has no table comment", goldDashboardView.Name)
	}
}

// Комментарий колонки — единственное место, где агент читает её смысл через MCP.
// Колонки берутся из самого SELECT хелпером selectedColumns, а не списком в тесте:
// список в тесте пропустил бы колонку, добавленную в SELECT позже. Обратная
// проверка (комментарий к колонке, которой в выдаче нет) ловит опечатку, которую
// ClickHouse отверг бы только на живом прогоне.
func TestGoldDashboardViewColumnsAreCommented(t *testing.T) {
	v := goldDashboardView

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

// Приоритет источников — смысл витрины: distillate_stocks пишет импортёр eia
// (канонический ряд), webbridge зарезервирован под сбор, manual — фолбэк, если
// импортёр не отработал. Порядок подстановок в coalesce И ЕСТЬ приоритет:
// переставленные местами ветки молча переключили бы дашборд на устаревший ручной
// ввод. Порядок в WHERE ... IN не проверяется — он на приоритет не влияет.
//
// Проверяется КАЖДЫЙ блок coalesce(...) — их три (n, value, as_of), и приоритет
// задаётся порядком веток внутри блока. Поиск первого вхождения подстроки по
// всему SELECT этого не поймал бы: переставленные ветки в value/as_of остаются
// незамеченными, пока в блоке n порядок верен.
func TestGoldDashboardViewSourcePriority(t *testing.T) {
	selectText := collapseSpaces(goldDashboardView.Select)

	sources := []string{"eia", "webbridge", "manual"}
	complete := 0

	for _, block := range strings.Split(selectText, "coalesce(")[1:] {
		prev, seen := -1, 0

		for _, source := range sources {
			idx := strings.Index(block, "source = '"+source+"'")
			if idx < 0 {
				continue
			}

			seen++
			if idx < prev {
				t.Errorf("source %s must come before %s in coalesce block: приоритет eia > webbridge > manual, got: %s",
					sources[seen-2], source, block)
			}
			prev = idx
		}

		if seen == len(sources) {
			complete++
		}
	}

	if complete == 0 {
		t.Fatalf("%s: no coalesce block picks all three sources (eia > webbridge > manual), got: %s", goldDashboardView.Name, selectText)
	}
}

// maxIf при отсутствии совпадений возвращает 0, и coalesce не провалился бы на
// следующий источник по приоритету: выбранное значение подменялось бы нулём.
// maxIfOrNull возвращает NULL и передаёт выбор дальше — только он и допустим.
func TestGoldDashboardViewUsesMaxIfOrNull(t *testing.T) {
	selectText := collapseSpaces(goldDashboardView.Select)

	if !strings.Contains(selectText, "maxIfOrNull(") {
		t.Fatalf("%s: SELECT must pick values by source with maxIfOrNull, got: %s", goldDashboardView.Name, selectText)
	}

	if strings.Contains(selectText, "maxIf(") {
		t.Fatalf("%s: maxIf returns 0 when nothing matches and coalesce would not fall through to the next source", goldDashboardView.Name)
	}

	if !strings.Contains(selectText, "GROUP BY series, source") {
		t.Fatalf("%s: nested SELECT must aggregate by (series, source) before picking a source, got: %s", goldDashboardView.Name, selectText)
	}
}
