package views

import (
	"regexp"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/wgc"
)

// cbGoldDynamicsColumns — колонки витрины в порядке выдачи: это контракт витрины
// (спека §5), а не деталь реализации. Порядок обязателен: агент читает выдачу
// целиком (SELECT *), и перестановка колонок меняет заголовок, на который он
// опирается. Список в тесте намеренно дублирует бриф: сверка «SELECT ↔ Columns»
// ловит забытый комментарий, но не подмену смысла колонки.
var cbGoldDynamicsColumns = []string{
	"iso3",
	"country",
	"last_change_date",
	"mom_change_tonnes",
	"ytd_change_tonnes",
	"holdings_date",
	"holdings_tonnes",
}

// cbGoldMappingPairs — пары (iso3, страна) mapping-массива витрины. Регулярка ищет
// ровно кортеж из двух строковых литералов, начинающийся с трёхбуквенного ISO: имена
// рядов ('cb_gold_change_tonnes_POL') и значения фильтров ('wgc') под неё не попадают,
// потому что после трёх букв в них идёт не кавычка.
var cbGoldMappingPairs = regexp.MustCompile(`\('([A-Z]{3})', '([^']+)'\)`)

// Витрина обязана быть зарегистрирована в импортёре gold_views: без этого её
// никто не создаст, и грант в sql/mcp_kimi_reader.sql упадёт на несуществующей
// витрине. Проверка идёт по САМОМУ списку импортёра (goldViewsList) — см.
// комментарий у TestDcfAssumptionsViewIsBuiltByGoldViews. Таблица одна —
// macro_series, поэтому витрина не может «пропуститься» на неполном наборе таблиц.
func TestCbGoldDynamicsViewIsBuiltByGoldViews(t *testing.T) {
	var found bool

	for _, v := range goldViewsList {
		if v.Name == cbGoldDynamicsView.Name {
			found = true
		}
	}

	if !found {
		t.Fatalf("%s must be created by the gold_views importer: it is missing from goldViewsList", cbGoldDynamicsView.Name)
	}

	if strings.TrimSpace(cbGoldDynamicsView.Comment) == "" {
		t.Fatalf("%s has no table comment", cbGoldDynamicsView.Name)
	}

	if len(cbGoldDynamicsView.Tables) != 1 || cbGoldDynamicsView.Tables[0] != "macro_series" {
		t.Fatalf("%s tables = %v, want [macro_series]", cbGoldDynamicsView.Name, cbGoldDynamicsView.Tables)
	}
}

// Комментарий колонки — единственное место, где агент читает её смысл через MCP.
// Колонки берутся из самого SELECT хелпером selectedColumns, а не списком в тесте:
// список в тесте пропустил бы колонку, добавленную в SELECT позже. Обратная
// проверка (комментарий к колонке, которой в выдаче нет) ловит опечатку, которую
// ClickHouse отверг бы только на живом прогоне. Отдельно сверяется порядок и состав
// колонок: строка на страну с MoM/YTD и уровнями — это и есть контракт витрины.
func TestCbGoldDynamicsViewColumnsAreCommented(t *testing.T) {
	v := cbGoldDynamicsView

	columns := selectedColumns(v.Select)
	if len(columns) == 0 {
		t.Fatalf("%s: no selected columns recognised", v.Name)
	}

	if len(columns) != len(cbGoldDynamicsColumns) {
		t.Fatalf("%s: SELECT returns %d columns %v, want %d %v", v.Name, len(columns), columns, len(cbGoldDynamicsColumns), cbGoldDynamicsColumns)
	}

	for i, want := range cbGoldDynamicsColumns {
		if columns[i] != want {
			t.Errorf("%s: column %d is %q, want %q (порядок колонок — контракт витрины)", v.Name, i, columns[i], want)
		}
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

// Порядок выдачи детерминирован — паритет с v_gold_etf_flows/v_gold_forecasts: агент
// читает витрину без ORDER BY в своём запросе, и «плавающий» порядок строк делал бы
// выдачу невоспроизводимой между прогонами. Оба ряда присоединяются LEFT JOIN от
// словаря стран: страна без ряда обязана получить NULL, а не пропасть из выдачи, —
// поэтому строк ровно 7 (одна на страну), и LEFT JOIN их два. Фильтр source = 'wgc'
// стоит в каждой ветке: у чужого источника может лежать ряд с тем же именем, и
// ветка без фильтра подмешала бы его значения.
func TestCbGoldDynamicsViewOrderIsDeterministic(t *testing.T) {
	v := cbGoldDynamicsView
	selectText := collapseSpaces(v.Select)

	if !strings.Contains(selectText, "ORDER BY iso3") {
		t.Fatalf("%s: SELECT must order rows by iso3, got: %s", v.Name, selectText)
	}

	if got := strings.Count(selectText, "LEFT JOIN"); got != 2 {
		t.Errorf("%s: %d LEFT JOIN, want 2 — одна строка на страну словаря", v.Name, got)
	}

	if !strings.Contains(selectText, "macro_series FINAL") {
		t.Errorf("%s: SELECT must read the current version of macro_series (FINAL), got: %s", v.Name, selectText)
	}

	if got := strings.Count(selectText, "source = 'wgc'"); got != 2 {
		t.Errorf("%s: %d фильтров source = 'wgc', want 2 — по одному на каждую ветку рядов", v.Name, got)
	}
}

// Словарь стран и список рядов — данные витрины, а не текст: ISO-коды попадают в
// mapping-массив, имена рядов — в IN-фильтры. Копии словаря в тесте нет: источник —
// wgc.CbCountries()/wgc.CbSeriesNames(), те же функции, из которых витрина собирает
// SQL. Потерянный ряд или страна не ломают запрос — они молча выкидывают строку из
// выдачи или вечный NULL вместо метрики, поэтому сверяются все 14 рядов и все 7 пар.
// Отдельно сверяется отсечение iso3 от имени ряда: length(...) в substring обязан
// совпадать с префиксом метрики, иначе iso3 (и весь LEFT JOIN) поедут при полностью
// валидном SQL.
func TestCbGoldDynamicsViewCoversEveryCountryAndSeries(t *testing.T) {
	v := cbGoldDynamicsView
	selectText := collapseSpaces(v.Select)
	countries := wgc.CbCountries()

	pairs := cbGoldMappingPairs.FindAllStringSubmatch(selectText, -1)
	if len(pairs) != len(countries) {
		t.Fatalf("%s: mapping-массив содержит %d пар, want %d (словарь wgc.CbCountries())", v.Name, len(pairs), len(countries))
	}

	for i, c := range countries {
		if pairs[i][1] != c.ISO || pairs[i][2] != c.Country {
			t.Errorf("%s: пара %d = (%s, %s), want (%s, %s) — порядок словаря стран",
				v.Name, i, pairs[i][1], pairs[i][2], c.ISO, c.Country)
		}
	}

	for _, name := range wgc.CbSeriesNames() {
		if !strings.Contains(selectText, "'"+name+"'") {
			t.Errorf("%s: ряд %q не входит в SELECT — метрика страны молча станет NULL", v.Name, name)

			continue
		}

		for _, c := range countries {
			if !strings.HasSuffix(name, c.ISO) {
				continue
			}

			prefix := strings.TrimSuffix(name, c.ISO)
			if !strings.Contains(selectText, "substring(series, length('"+prefix+"') + 1)") {
				t.Errorf("%s: ряд %s: iso3 не отсекается по префиксу %q", v.Name, name, prefix)
			}
		}
	}
}

// YTD — сумма дельт с 01.01 года ПОСЛЕДНЕЙ точки каждого ряда: у стран разные
// последние месяцы (у RUS пропуски, у UZB ряд короче), и «общий» год выборки считал
// бы отставшим странам YTD от чужой даты. Поэтому год берётся у max(date) самого ряда
// (окно по series), а MoM — значение последней дельты (argMax по дате). Вложенную
// агрегацию (агрегат внутри sumIf) ClickHouse не принимает: дата последней точки
// приходит из подзапроса столбцом, и регресс-страж ниже фиксирует именно эту форму.
func TestCbGoldDynamicsViewComputesYtdFromEachSeriesLastDate(t *testing.T) {
	v := cbGoldDynamicsView
	selectText := collapseSpaces(v.Select)

	if !strings.Contains(selectText, "sumIf(") || !strings.Contains(selectText, "toStartOfYear(") {
		t.Fatalf("%s: YTD не считается суммой дельт с начала года последней точки, got: %s", v.Name, selectText)
	}

	if strings.Contains(selectText, "toStartOfYear(max(") {
		t.Fatalf("%s: агрегат внутри sumIf ClickHouse не принимает — дата последней точки обязана быть столбцом подзапроса", v.Name)
	}

	if !strings.Contains(selectText, "OVER (PARTITION BY series)") {
		t.Fatalf("%s: год YTD обязан быть годом последней даты КАЖДОГО ряда (окно по series), got: %s", v.Name, selectText)
	}

	if !strings.Contains(selectText, "argMax(") {
		t.Fatalf("%s: mom_change_tonnes — значение последней дельты (argMax по дате), got: %s", v.Name, selectText)
	}
}

// LEFT JOIN ClickHouse по умолчанию (join_use_nulls = 0) заполняет несовпавшую строку
// значениями по умолчанию: 0 у Float64, 1970-01-01 у Date32. Страна без ряда получила
// бы «нулевое изменение резервов» вместо отсутствия данных — ровно то, что конвенция
// репозитория запрещает (v_gold_dashboard решает это тем же if(n > 0, …, NULL)).
// Поэтому каждая колонка, приходящая из присоединённых подзапросов, обязана
// превращать отсутствие ряда в NULL; счётчик n приходит из тех же подзапросов.
func TestCbGoldDynamicsViewMissingSeriesBecomeNull(t *testing.T) {
	v := cbGoldDynamicsView

	head, _, found := strings.Cut(v.Select, "\nFROM ")
	if !found {
		t.Fatalf("%s: верхний FROM не найден — тест не смог разобрать список колонок", v.Name)
	}

	if got := strings.Count(collapseSpaces(v.Select), "count() AS n"); got != 2 {
		t.Errorf("%s: %d счётчиков n, want 2 — признак наличия ряда нужен в обеих ветках", v.Name, got)
	}

	for _, column := range cbGoldDynamicsColumns {
		if column == "iso3" || column == "country" {
			continue // колонки словаря стран, а не присоединённых рядов
		}

		var line string

		for _, raw := range strings.Split(head, "\n") {
			trimmed := strings.TrimSpace(raw)
			if strings.HasSuffix(trimmed, " AS "+column+",") || strings.HasSuffix(trimmed, " AS "+column) {
				line = trimmed
			}
		}

		if line == "" {
			t.Fatalf("%s: колонка %q не найдена в верхнем SELECT", v.Name, column)
		}

		if !strings.Contains(line, "if(") || !strings.Contains(line, "NULL") {
			t.Errorf("%s: колонка %q не превращает отсутствие ряда в NULL: %s", v.Name, column, line)
		}
	}
}
