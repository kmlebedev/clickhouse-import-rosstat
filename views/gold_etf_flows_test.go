package views

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Витрина обязана быть зарегистрирована в импортёре gold_views: без этого её
// никто не создаст, и грант в sql/mcp_kimi_reader.sql упадёт на несуществующей
// витрине. Проверка идёт по САМОМУ списку импортёра (goldViewsList) — см.
// комментарий у TestDcfAssumptionsViewIsBuiltByGoldViews.
func TestGoldEtfFlowsViewIsBuiltByGoldViews(t *testing.T) {
	var found bool

	for _, v := range goldViewsList {
		if v.Name == goldEtfFlowsView.Name {
			found = true
		}
	}

	if !found {
		t.Fatalf("%s must be created by the gold_views importer: it is missing from goldViewsList", goldEtfFlowsView.Name)
	}

	if strings.TrimSpace(goldEtfFlowsView.Comment) == "" {
		t.Fatalf("%s has no table comment", goldEtfFlowsView.Name)
	}
}

// Комментарий колонки — единственное место, где агент читает её смысл через MCP.
// Колонки берутся из самого SELECT хелпером selectedColumns, а не списком в тесте:
// список в тесте пропустил бы колонку, добавленную в SELECT позже. Обратная
// проверка (комментарий к колонке, которой в выдаче нет) ловит опечатку, которую
// ClickHouse отверг бы только на живом прогоне. Отдельно проверяется, что в выдаче
// есть все четыре метрики набора: пивот, потерявший метрику, молча отдаёт вечный
// NULL вместо ряда — именно так исчезает измерение, а не строка.
func TestGoldEtfFlowsViewColumnsAreCommented(t *testing.T) {
	v := goldEtfFlowsView

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

	for _, metric := range wgcEtfMetrics {
		if !contains(columns, metric) {
			t.Errorf("%s: metric column %q is missing from the SELECT", v.Name, metric)
		}
	}
}

// Приоритет источников — смысл витрины: webbridge пишет сессионный сбор WGC,
// manual — ручной фолбэк. Порядок подстановок в coalesce И ЕСТЬ приоритет;
// переставленные ветки молча подменили бы канонический ряд ручным вводом.
// maxIf при отсутствии совпадений возвращает 0, и coalesce не провалился бы на
// следующий источник: выбранное значение подменялось бы нулём на «потока не было»
// — только maxIfOrNull возвращает NULL. Внутренний GROUP BY date, series нужен,
// чтобы свести версии ReplacingMergeTree по источнику ДО выбора приоритета.
func TestGoldEtfFlowsViewSourcePriority(t *testing.T) {
	v := goldEtfFlowsView
	selectText := collapseSpaces(v.Select)

	if !strings.Contains(selectText, "maxIfOrNull(") {
		t.Fatalf("%s: SELECT must pick values by source with maxIfOrNull, got: %s", v.Name, selectText)
	}

	if strings.Contains(selectText, "maxIf(") {
		t.Fatalf("%s: maxIf returns 0 when nothing matches and coalesce would not fall through to the next source", v.Name)
	}

	var complete int

	for _, block := range strings.Split(selectText, "coalesce(")[1:] {
		webbridge := strings.Index(block, "source = 'webbridge'")
		manual := strings.Index(block, "source = 'manual'")

		if webbridge < 0 || manual < 0 {
			continue
		}

		complete++

		if webbridge > manual {
			t.Errorf("%s: source webbridge must come before manual in coalesce block: приоритет webbridge > manual, got: %s", v.Name, block)
		}
	}

	if complete == 0 {
		t.Fatalf("%s: no coalesce block picks webbridge and manual (приоритет webbridge > manual), got: %s", v.Name, selectText)
	}

	if !strings.Contains(selectText, "GROUP BY date, series") {
		t.Fatalf("%s: nested SELECT must aggregate by (date, series) before picking a source, got: %s", v.Name, selectText)
	}

	if !strings.Contains(selectText, "GROUP BY date, region") {
		t.Fatalf("%s: pivot must return one row per (date, region), got: %s", v.Name, selectText)
	}
}

// Список 20 рядов — данные витрины и каталога, а не текст: из них собираются и
// IN (...)-фильтр, и mapping-массив ARRAY JOIN. Тест сверяет константы набора
// с их же произведением (4 метрики × 5 регионов) и с wgcEtfSeriesName: забытый
// в одной из констант регион или метрика дал бы 19 рядов без единой ошибки SQL.
func TestGoldEtfFlowsSeriesSetCoversEveryRegionAndMetric(t *testing.T) {
	const want = 20

	if got := len(wgcEtfRegions) * len(wgcEtfMetrics); got != want {
		t.Fatalf("набор WGC Gold ETF Flows — 4 метрики × 5 регионов = %d рядов, а константы дают %d", want, got)
	}

	names := wgcEtfSeriesNames()
	if len(names) != want {
		t.Fatalf("wgcEtfSeriesNames() = %d имён, want %d", len(names), want)
	}

	seen := make(map[string]bool, want)

	for _, name := range names {
		if seen[name] {
			t.Errorf("ряд %s встречается в наборе дважды", name)
		}

		seen[name] = true
	}

	for _, region := range wgcEtfRegions {
		for _, metric := range wgcEtfMetrics {
			if !seen[wgcEtfSeriesName(metric, region)] {
				t.Errorf("ряд %s отсутствует в wgcEtfSeriesNames()", wgcEtfSeriesName(metric, region))
			}
		}
	}
}

// Каталог-сид — единственное место, где агент узнаёт, что измеряет ряд и в каких
// единицах (правило 11 AGENTS.md): у source='webbridge' нет импортёра, который
// вызвал бы util.UpsertSeriesCatalog, поэтому описание вносится сидом. Ряд, не
// описанный в каталоге, для агента не существует — числа он прочитает, а семантику
// нет. Сверка идёт по САМОМУ файлу сида, а не по копии списка в тесте.
func TestGoldEtfFlowsCatalogSeedDescribesEverySeries(t *testing.T) {
	const seedPath = "../sql/wgc_etf_flows_catalog.sql"

	data, err := os.ReadFile(filepath.Join("..", "sql", "wgc_etf_flows_catalog.sql"))
	if err != nil {
		t.Fatalf("каталог-сид %s не читается: %v", seedPath, err)
	}

	seed := string(data)
	names := wgcEtfSeriesNames()

	for _, name := range names {
		if !strings.Contains(seed, "('webbridge', '"+name+"'") {
			t.Errorf("ряд %s не описан в %s: агент не увидит его в v_series_catalog", name, seedPath)
		}
	}

	if got := strings.Count(seed, "'webbridge', 'etf_"); got != len(names) {
		t.Errorf("%s: %d записей source='webbridge', ожидалось ровно %d — по одному описанию на ряд", seedPath, got, len(names))
	}
}
