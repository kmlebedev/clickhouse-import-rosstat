package views

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// boldSeriesDeclaration — объявление имени ряда ETF-контура в файле импортёра:
// boldSeriesX = "etf_bold_…". Разбор узкий и нарочно: он ищет ровно константы набора,
// а не любую строку с префиксом (правило «похоже на имя ряда» поймало бы и ряд чужого
// контура). Сверка идёт по САМОМУ файлу импортёра, а не по копии списка в тесте: тест,
// повторяющий список витрины, проверял бы сам себя — переименованный в bold/ ряд
// оставил бы витрину с вечным NULL и зелёный тест.
var boldSeriesDeclaration = regexp.MustCompile(`boldSeries\w+\s*=\s*"(etf_bold_[a-z0-9_]+)"`)

// boldEtfFlowsDailyColumns / boldFundsDailyColumns — колонки витрин в порядке
// выдачи: порядок — контракт (спека §4.4), а не деталь реализации, потому что агент
// читает витрину целиком (SELECT *) и перестановка колонок меняет заголовок, на
// который он опирается. Списки дублируют бриф намеренно: сверка «SELECT ↔ Columns»
// ловит забытый комментарий, но не подмену смысла колонки.
var (
	boldEtfFlowsDailyColumns = []string{
		"date", "flow_tonnes", "flow_usd_mln", "flow_7d_tonnes", "flow_30d_tonnes",
		"flow_90d_tonnes", "holdings_tonnes", "aum_usd_bn",
	}
	boldFundsDailyColumns = []string{
		"date", "ticker", "name", "units_oz", "aum_usd", "price_usd",
		"units_change_oz", "aum_change_usd", "shares", "shares_change",
	}
)

// boldImporterSeriesNames достаёт имена рядов etf_bold_* из объявлений импортёра
// (../bold/parse.go). Пустой результат — провал, а не пропуск: разбор, разошедшийся с
// файлом, обязан быть виден (иначе тест «сверял» бы витрину с пустым списком).
func boldImporterSeriesNames(t *testing.T) []string {
	t.Helper()

	const path = "../bold/parse.go"

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("файл импортёра %s не читается: %v", path, err)
	}

	matches := boldSeriesDeclaration.FindAllStringSubmatch(string(data), -1)
	names := make([]string, 0, len(matches))

	for _, match := range matches {
		names = append(names, match[1])
	}

	if len(names) == 0 {
		t.Fatalf("%s: не найдено ни одного объявления boldSeries… = \"etf_bold_…\" — правило разбора разошлось с файлом", path)
	}

	return names
}

// Форма витрины дневного агрегата: имя, таблица-источник и то, без чего пивот теряет
// данные молча. FINAL обязателен по определению ReplacingMergeTree (без него
// вложенный подзапрос читал бы физический хвост таблицы); maxIfOrNull, а не maxIf:
// maxIf при отсутствии совпадений возвращает 0 и подменил бы «ряда за этот день нет»
// нулевым потоком — выдуманным нулём вместо отсутствия данных; GROUP BY date — одна
// строка на день.
func TestGoldEtfFlowsDailyViewShape(t *testing.T) {
	v := goldEtfFlowsDailyView
	selectText := collapseSpaces(v.Select)

	if v.Name != "v_gold_etf_flows_daily" {
		t.Fatalf("Name = %q, want v_gold_etf_flows_daily", v.Name)
	}

	if len(v.Tables) != 1 || v.Tables[0] != "macro_series" {
		t.Fatalf("%s tables = %v, want [macro_series]", v.Name, v.Tables)
	}

	if !strings.Contains(selectText, "macro_series FINAL") {
		t.Fatalf("%s: SELECT must read macro_series FINAL (ReplacingMergeTree читается через FINAL), got: %s", v.Name, selectText)
	}

	if !strings.Contains(selectText, "source = 'bold'") {
		t.Fatalf("%s: SELECT must filter source = 'bold', got: %s", v.Name, selectText)
	}

	for _, pair := range boldETFSeries {
		if !strings.Contains(selectText, "'"+pair.Series+"'") {
			t.Errorf("%s: ряд %s отсутствует в SELECT — колонка %s осталась бы вечным NULL", v.Name, pair.Series, pair.Metric)
		}
	}

	if !strings.Contains(selectText, "maxIfOrNull(") {
		t.Fatalf("%s: SELECT must pick values with maxIfOrNull, got: %s", v.Name, selectText)
	}

	if strings.Contains(selectText, "maxIf(") {
		t.Fatalf("%s: maxIf returns 0 when nothing matches and «ряда за день нет» стало бы нулевым потоком", v.Name)
	}

	if !strings.Contains(selectText, "GROUP BY date") {
		t.Fatalf("%s: pivot must return one row per day, got: %s", v.Name, selectText)
	}
}

// Форма пер-фондовой витрины: имя, таблица-источник и FINAL по её ключу
// (date, ticker, name). Порядок строк — date DESC, ticker, name: второй и третий
// ключи обязательны, потому что тикер у источника не уникален (Ruling 3: GLD
// встречается трижды), и без name строки одного дня шли бы в произвольном порядке.
func TestBoldFundsDailyViewShape(t *testing.T) {
	v := boldFundsDailyView
	selectText := collapseSpaces(v.Select)

	if v.Name != "v_gold_etf_funds_daily" {
		t.Fatalf("Name = %q, want v_gold_etf_funds_daily", v.Name)
	}

	if len(v.Tables) != 1 || v.Tables[0] != "bold_fund_snapshot" {
		t.Fatalf("%s tables = %v, want [bold_fund_snapshot]", v.Name, v.Tables)
	}

	if !strings.Contains(selectText, "bold_fund_snapshot FINAL") {
		t.Fatalf("%s: SELECT must read bold_fund_snapshot FINAL, got: %s", v.Name, selectText)
	}

	if !strings.Contains(selectText, "ORDER BY date DESC, ticker, name") {
		t.Fatalf("%s: SELECT must order rows by date DESC, ticker, name, got: %s", v.Name, selectText)
	}
}

// Витрины обязаны быть зарегистрированы в импортёре gold_views: без этого их никто
// не создаст, и грант в sql/mcp_kimi_reader.sql упадёт на несуществующей витрине.
// Проверка идёт по САМОМУ списку импортёра (goldViewsList) — см. комментарий у
// TestDcfAssumptionsViewIsBuiltByGoldViews. Таблица-источник обязана быть названа и
// в Tables, и в самом SELECT: гейт CreateView считает Tables в system.tables и
// молча пропускает витрину, пока таблицы нет, а расхождение имён дало бы витрину,
// которая создалась и не читается.
func TestBoldViewsRegistered(t *testing.T) {
	for _, tc := range []struct {
		view   util.View
		tables []string
	}{
		{goldEtfFlowsDailyView, []string{"macro_series"}},
		{boldFundsDailyView, []string{"bold_fund_snapshot"}},
	} {
		var found bool

		for _, registered := range goldViewsList {
			if registered.Name == tc.view.Name {
				found = true
			}
		}

		if !found {
			t.Errorf("%s must be created by the gold_views importer: it is missing from goldViewsList", tc.view.Name)

			continue
		}

		if len(tc.view.Tables) != len(tc.tables) {
			t.Errorf("%s: Tables = %v, want %v", tc.view.Name, tc.view.Tables, tc.tables)

			continue
		}

		for i, table := range tc.tables {
			if tc.view.Tables[i] != table {
				t.Errorf("%s: Tables[%d] = %q, want %q", tc.view.Name, i, tc.view.Tables[i], table)
			}

			if !strings.Contains(tc.view.Select, table) {
				t.Errorf("%s: SELECT не читает таблицу-источник %q из Tables", tc.view.Name, table)
			}
		}
	}
}

// Комментарий таблицы и каждой колонки — требование правила 11: через MCP агент
// видит только их (SQL-код витрины читателю недоступен). Поэтому проверяется и
// непустота Comment, и полное покрытие Columns: колонка без комментария — это число
// без смысла. Колонки берутся из самого SELECT хелпером selectedColumns, а не
// списком в тесте: список в тесте пропустил бы колонку, добавленную в SELECT позже.
// Обратная проверка (комментарий к колонке, которой в выдаче нет) ловит опечатку,
// которую ClickHouse отверг бы только на живом прогоне. Число колонок (8 и 10) —
// контракт спеки §4.4, поэтому сверяется явно, а не «сколько получилось».
func TestBoldViewsHaveComments(t *testing.T) {
	for _, tc := range []struct {
		view    util.View
		columns []string
	}{
		{goldEtfFlowsDailyView, boldEtfFlowsDailyColumns},
		{boldFundsDailyView, boldFundsDailyColumns},
	} {
		if strings.TrimSpace(tc.view.Comment) == "" {
			t.Errorf("%s has no table comment", tc.view.Name)
		}

		columns := selectedColumns(tc.view.Select)
		if len(columns) != len(tc.columns) {
			t.Errorf("%s: SELECT возвращает %d колонок (%v), want %d (%v)", tc.view.Name, len(columns), columns, len(tc.columns), tc.columns)
		}

		for i, want := range tc.columns {
			if i >= len(columns) {
				break
			}

			if columns[i] != want {
				t.Errorf("%s: колонка %d = %q, want %q (порядок выдачи — контракт витрины)", tc.view.Name, i, columns[i], want)
			}
		}

		for _, column := range columns {
			if strings.TrimSpace(tc.view.Columns[column]) == "" {
				t.Errorf("%s: column %q has no comment — агент не поймёт её через MCP", tc.view.Name, column)
			}
		}

		for column := range tc.view.Columns {
			if !contains(columns, column) {
				t.Errorf("%s: comment for %q, which the SELECT does not return", tc.view.Name, column)
			}
		}
	}
}

// Семь рядов набора — данные витрины, а не текст: из boldETFSeries собираются и
// IN (...)-фильтр, и mapping-массив пивота, и разметка колонок. Тест держит три
// стороны вместе: (1) набор полон и без дублей, (2) каждый ряд и каждая метрика
// реально стоят в SELECT витрины (иначе колонка молча отдавала бы вечный NULL),
// (3) имена совпадают с объявлениями импортёра (../bold/parse.go) — файла, который
// эти ряды пишет, а не копии списка в тесте.
func TestBoldEtfFlowsDailySeriesSetMatchesViewAndImporter(t *testing.T) {
	const want = 7

	if len(boldETFSeries) != want {
		t.Fatalf("набор boldETFSeries = %d пар, want %d (спека §4.1: 7 рядов ETF-контура)", len(boldETFSeries), want)
	}

	selectText := collapseSpaces(goldEtfFlowsDailyView.Select)
	series := make([]string, 0, want)
	metrics := make([]string, 0, want)

	for _, pair := range boldETFSeries {
		series = append(series, pair.Series)
		metrics = append(metrics, pair.Metric)

		if !strings.Contains(selectText, "('"+pair.Series+"', '"+pair.Metric+"')") {
			t.Errorf("%s: в mapping-массиве нет пары (%s, %s)", goldEtfFlowsDailyView.Name, pair.Series, pair.Metric)
		}

		if !strings.Contains(selectText, ") AS "+pair.Metric) {
			t.Errorf("%s: метрика %q не разложена в колонку — значение ряда %s в выдачу не попадает", goldEtfFlowsDailyView.Name, pair.Metric, pair.Series)
		}
	}

	for _, list := range []struct {
		what  string
		items []string
	}{{"ряд", series}, {"метрика", metrics}} {
		seen := make(map[string]bool, len(list.items))

		for _, item := range list.items {
			if seen[item] {
				t.Errorf("%s %q встречается в наборе дважды", list.what, item)
			}

			seen[item] = true
		}
	}

	importer := boldImporterSeriesNames(t)
	sort.Strings(series)
	sort.Strings(importer)

	if strings.Join(series, ",") != strings.Join(importer, ",") {
		t.Errorf("ряды витрины %v не совпадают с рядами импортёра (../bold/parse.go) %v", series, importer)
	}
}

// Порядок выдачи детерминирован — паритет с v_gold_etf_flows (у неё ORDER BY date
// DESC, region): агент читает витрину без ORDER BY в своём запросе, и «плавающий»
// порядок строк делал бы выдачу невоспроизводимой между прогонами.
func TestBoldViewsOrderIsDeterministic(t *testing.T) {
	for _, tc := range []struct {
		view  util.View
		order string
	}{
		{goldEtfFlowsDailyView, "ORDER BY date DESC"},
		{boldFundsDailyView, "ORDER BY date DESC, ticker, name"},
	} {
		selectText := collapseSpaces(tc.view.Select)

		if !strings.Contains(selectText, tc.order) {
			t.Errorf("%s: SELECT must order rows by %s, got: %s", tc.view.Name, strings.TrimPrefix(tc.order, "ORDER BY "), selectText)
		}
	}
}
