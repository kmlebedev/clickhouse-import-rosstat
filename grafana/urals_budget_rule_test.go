package main

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// uralsBudgetRulePanelTitles — состав панелей в порядке следования (он же порядок id 1..7):
// два ECharts-графика, четыре stat-снимка и месячный бар (спека §3.4). Заголовки rolling-панелей
// нейтральные: SQL берёт последний опубликованный месяц, конкретика as-of — в описаниях.
var uralsBudgetRulePanelTitles = []string{
	"Urals в постоянных рублях (база — последний опубликованный ИПЦ)",
	"Бюджетное правило: цена отсечения $59",
	"Urals, $/барр. — последний опубликованный месяц",
	"Курс ЦБ, ₽/$ — последний опубликованный месяц",
	"Urals в рублях, ₽/барр. — последний опубликованный месяц",
	"Нефтегазовые доходы, млрд ₽ — янв–сен 2026",
	"Нефтегазовые доходы федерального бюджета по месяцам, млрд ₽",
}

// uralsBudgetRulePanelSpans — ширина панелей в колонках сетки Grafana (24): графики во всю
// ширину, stat — по четверти, месячный бар — во всю ширину.
var uralsBudgetRulePanelSpans = []uint32{12, 12, 6, 6, 6, 6, spanFull}

// uralsBudgetRulePanelHeights — высота панелей в строках сетки: графики 10, stat 4, бар 8.
var uralsBudgetRulePanelHeights = []uint32{10, 10, 4, 4, 4, 4, 8}

func builtUralsBudgetRule(t *testing.T) dashboard.Dashboard {
	t.Helper()
	d, err := NewUralsBudgetRule().Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestUralsBudgetRuleComposition фиксирует состав дашборда: 7 панелей id 1–7 в заданном порядке,
// ширины и высоты из спеки §3.4, типы панелей (2 ECharts, 4 stat, 1 barchart). Проверка идёт
// через panelsOf: ряды — такие же элементы d.Panels и в счёт панелей не входят.
func TestUralsBudgetRuleComposition(t *testing.T) {
	board := builtUralsBudgetRule(t)
	if err := Validate(board, UralsBudgetRuleUID); err != nil {
		t.Fatalf("дашборд %s не прошёл валидацию: %v", UralsBudgetRuleUID, err)
	}
	panels := panelsOf(board)
	if len(panels) != len(uralsBudgetRulePanelTitles) {
		t.Fatalf("панелей %d, ожидалось %d", len(panels), len(uralsBudgetRulePanelTitles))
	}
	wantTypes := []string{EChartsPanelType, EChartsPanelType, "stat", "stat", "stat", "stat", "barchart"}
	for i, p := range panels {
		id := uint32(i + 1)
		if p.Id == nil || *p.Id != id {
			t.Fatalf("панель %d: id %v, ожидался %d в порядке следования", i+1, p.Id, id)
		}
		if got := panelTitle(p); got != uralsBudgetRulePanelTitles[i] {
			t.Fatalf("панель %d: заголовок %q, ожидался %q", i+1, got, uralsBudgetRulePanelTitles[i])
		}
		if p.Type != wantTypes[i] {
			t.Fatalf("панель %d: тип %q, ожидался %q", i+1, p.Type, wantTypes[i])
		}
		if p.GridPos == nil {
			t.Fatalf("панель %d: не задана позиция в сетке", i+1)
		}
		if got := p.GridPos.W; got != uralsBudgetRulePanelSpans[i] {
			t.Fatalf("панель %d: ширина %d, ожидалась %d", i+1, got, uralsBudgetRulePanelSpans[i])
		}
		if got := p.GridPos.H; got != uralsBudgetRulePanelHeights[i] {
			t.Fatalf("панель %d: высота %d, ожидалась %d", i+1, got, uralsBudgetRulePanelHeights[i])
		}
	}
}

// TestUralsBudgetRuleRows: два ряда с id 21/22 вне диапазона панелей 1..7 — нулевой или совпавший
// с панелью id Grafana перезапишет своим, и артефакт разойдётся с кодом.
func TestUralsBudgetRuleRows(t *testing.T) {
	board := builtUralsBudgetRule(t)
	var titles []string
	var ids []uint32
	for _, entry := range board.Panels {
		if entry.RowPanel == nil {
			continue
		}
		if entry.RowPanel.Id <= 7 {
			t.Fatalf("ряд %q: id %d попадает в диапазон панелей 1..7", optionalString(entry.RowPanel.Title), entry.RowPanel.Id)
		}
		titles = append(titles, optionalString(entry.RowPanel.Title))
		ids = append(ids, entry.RowPanel.Id)
	}
	want := []string{"Реальная цена Urals и бюджетное правило", "Последний опубликованный месяц и нефтегазовые доходы"}
	wantIDs := []uint32{21, 22}
	if len(titles) != len(want) {
		t.Fatalf("рядов %d, ожидалось %d: %v", len(titles), len(want), titles)
	}
	for i, got := range titles {
		if got != want[i] {
			t.Fatalf("ряд %d: %q, ожидался %q", i+1, got, want[i])
		}
		if ids[i] != wantIDs[i] {
			t.Fatalf("ряд %d: id %d, ожидался %d", i+1, ids[i], wantIDs[i])
		}
	}
}

// TestUralsBudgetRuleValidateUID проверяет параметризацию Validate: дашборд проходит проверку со
// своим uid и отвергается с чужим — иначе опечатка в uid прошла бы молча.
func TestUralsBudgetRuleValidateUID(t *testing.T) {
	board := builtUralsBudgetRule(t)
	if err := Validate(board, UralsBudgetRuleUID); err != nil {
		t.Fatalf("дашборд %s не прошёл валидацию: %v", UralsBudgetRuleUID, err)
	}
	if err := Validate(board, TransaqTradesUID); err == nil {
		t.Fatal("дашборд принят с чужим uid: проверка uid не параметризована")
	}
}

// uralsBudgetRuleTargetContract — ожидаемый контракт таргета панели: витрины, которые он читает,
// наличие $__dateFilter и его выражение. Stat-снимки П3–П6 фильтра диапазона не несут: окно
// дашборда не должно менять снимок последнего опубликованного месяца (спека §4.1).
type uralsBudgetRuleTargetContract struct {
	id      uint32
	views   []string
	filters []string
}

// TestUralsBudgetRuleTargets: каждый таргет — CHQuery формата table (все панели категориальные:
// ECharts, stat и barchart), читает только витрины v_* (FROM по сырым таблицам запрещён), на
// графиках П1/П2/П7 стоит $__dateFilter на сырой date, LIMIT нет ни у одного таргета. CTE
// (ipc/base/usd/...) — не таблицы: проверка отличает их по определению «name AS (».
func TestUralsBudgetRuleTargets(t *testing.T) {
	board := builtUralsBudgetRule(t)
	cases := []uralsBudgetRuleTargetContract{
		{1, []string{"v_minec_macro", "v_cbr_macro", "v_fedstat_ipc"}, []string{"$__dateFilter(f.date)"}},
		{2, []string{"v_minec_macro"}, []string{"$__dateFilter(date)"}},
		{3, []string{"v_minec_macro"}, nil},
		{4, []string{"v_cbr_macro", "v_minec_macro"}, nil},
		{5, []string{"v_minec_macro", "v_cbr_macro", "v_fedstat_ipc"}, nil},
		{6, []string{"v_minfin_budget"}, nil},
		{7, []string{"v_minfin_budget"}, []string{"$__dateFilter(date)"}},
	}
	for _, tc := range cases {
		panel := panelByID(t, board, tc.id)
		if len(panel.Targets) != 1 {
			t.Fatalf("панель %d: таргетов %d, ожидался 1", tc.id, len(panel.Targets))
		}
		query, ok := chQuery(panel.Targets[0])
		if !ok {
			t.Fatalf("панель %d: таргет не является запросом ClickHouse", tc.id)
		}
		if query.Format != queryFormatTable || query.QueryType != queryTypeTable {
			t.Fatalf("панель %d: формат %d/%q, ожидался %d/%q", tc.id, query.Format, query.QueryType, queryFormatTable, queryTypeTable)
		}
		for _, view := range tc.views {
			if !strings.Contains(query.RawSql, "FROM "+view) {
				t.Fatalf("панель %d: запрос не читает витрину %s:\n%s", tc.id, view, query.RawSql)
			}
		}
		if !strings.Contains(query.RawSql, "FROM v_") {
			t.Fatalf("панель %d: запрос без чтения витрины v_*: %s", tc.id, query.RawSql)
		}
		for _, filter := range tc.filters {
			if got := strings.Count(query.RawSql, filter); got != 1 {
				t.Fatalf("панель %d: %q встречается %d раз, ожидался 1:\n%s", tc.id, filter, got, query.RawSql)
			}
		}
		if len(tc.filters) == 0 && strings.Contains(query.RawSql, "$__") {
			t.Fatalf("панель %d: фильтр диапазона у снимка запрещён:\n%s", tc.id, query.RawSql)
		}
		if strings.Contains(strings.ToUpper(query.RawSql), "LIMIT") {
			t.Fatalf("панель %d: LIMIT на панели запрещён:\n%s", tc.id, query.RawSql)
		}
		assertNoRawTables(t, tc.id, query.RawSql)
	}
}

// assertNoRawTables ловит FROM по сырой таблице: каждое имя после FROM — либо витрина v_*, либо
// CTE этого же запроса (имя определено как «name AS (»).
func assertNoRawTables(t *testing.T, panelID uint32, sql string) {
	t.Helper()
	from := regexp.MustCompile(`(?i)FROM\s+([A-Za-z_][A-Za-z0-9_]*)`)
	for _, match := range from.FindAllStringSubmatch(sql, -1) {
		table := match[1]
		if strings.HasPrefix(table, "v_") || strings.Contains(sql, table+" AS (") {
			continue
		}
		t.Fatalf("панель %d: FROM по сырой таблице %q:\n%s", panelID, table, sql)
	}
}

// TestUralsBudgetRuleColumnContract: ECharts-таргеты П1/П2 и stat-снимок П5 отдают ровно те
// колонки, по именам которых читает JS (спека §4.2/§4.3, снимок П5), а П1/П5 защищены от потери
// оконного anyLast и настройки join_use_nulls: колонка «Оценка» (is_estimate, только П1) и
// SETTINGS join_use_nulls = 1 обязательны — без настройки реальный темп П5 теряется.
func TestUralsBudgetRuleColumnContract(t *testing.T) {
	board := builtUralsBudgetRule(t)
	cases := []struct {
		id      uint32
		columns []string
		sql     []string
	}{
		{
			1,
			[]string{
				`"Месяц"`, `"Год"`, `"Месяц года"`, `"Реальная цена, ₽/барр."`,
				`"Сезонная норма 2018–2022, ₽/барр."`, `"Оценка"`,
			},
			[]string{
				`toUInt8(ipc_level IS NULL) AS is_estimate`,
				`f.is_estimate AS "Оценка"`,
				"SETTINGS join_use_nulls = 1",
				"anyLast(ipc_level) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
				"argMax(level, date) AS level",
			},
		},
		{
			2,
			[]string{`"Месяц"`, `"Urals, $/барр."`, `"В бюджет (≤ $59)"`, `"В ФНБ (> $59)"`},
			[]string{`least(value, 59)`, `greatest(value - 59, 0)`},
		},
		{
			5,
			[]string{`"₽/барр."`, `"г/г номинально, %"`, `"г/г реально, %"`},
			[]string{
				"SETTINGS join_use_nulls = 1",
				"anyLast(ipc_level) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
				"argMax(level, date) AS level",
				"addYears((SELECT date FROM cur), -1)",
			},
		},
	}
	for _, tc := range cases {
		panel := panelByID(t, board, tc.id)
		query, ok := chQuery(panel.Targets[0])
		if !ok {
			t.Fatalf("панель %d: таргет не является запросом ClickHouse", tc.id)
		}
		for _, column := range tc.columns {
			if !strings.Contains(query.RawSql, column) {
				t.Fatalf("панель %d: в SQL нет колонки %s:\n%s", tc.id, column, query.RawSql)
			}
		}
		for _, want := range tc.sql {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель %d: в SQL нет %q:\n%s", tc.id, want, query.RawSql)
			}
		}
	}
}

// TestUralsBudgetRuleJSColumnAliases — перекрёстная проверка JS ↔ SQL по алиасам колонок П1/П2.
// Направление SQL → JS: каждый ожидаемый алиас контракта обязан встречаться в getOption (JS читает
// поле по имени через column(...)/field(...)). Направление JS → SQL: каждая ссылка на поле в JS
// обязана быть алиасом таргета. Дрейф имени (правка SQL без JS или наоборот) иначе не виден: JS
// молча возвращает {} и панель рисуется пустой, а не падает.
func TestUralsBudgetRuleJSColumnAliases(t *testing.T) {
	board := builtUralsBudgetRule(t)
	cases := []struct {
		id      uint32
		aliases []string
	}{
		{
			1,
			[]string{
				"Год", "Месяц года", "Реальная цена, ₽/барр.",
				"Сезонная норма 2018–2022, ₽/барр.", "Оценка",
			},
		},
		{
			2,
			[]string{"Месяц", "Urals, $/барр.", "В бюджет (≤ $59)", "В ФНБ (> $59)"},
		},
	}
	fieldRef := regexp.MustCompile(`(?:column|field)\('([^']+)'\)`)
	for _, tc := range cases {
		panel := panelByID(t, board, tc.id)
		query, ok := chQuery(panel.Targets[0])
		if !ok {
			t.Fatalf("панель %d: таргет не является запросом ClickHouse", tc.id)
		}
		js := uralsBudgetEChartsOptions(t, panel).GetOption
		refs := fieldRef.FindAllStringSubmatch(js, -1)
		if len(refs) == 0 {
			t.Fatalf("панель %d: getOption не читает ни одного поля по имени:\n%s", tc.id, js)
		}
		for _, alias := range tc.aliases {
			if !strings.Contains(query.RawSql, `"`+alias+`"`) {
				t.Fatalf("панель %d: в SQL нет алиаса %q:\n%s", tc.id, alias, query.RawSql)
			}
			if !strings.Contains(js, "'"+alias+"'") {
				t.Fatalf("панель %d: getOption не читает алиас %q — панель молча вернёт {}:\n%s", tc.id, alias, js)
			}
		}
		for _, ref := range refs {
			if !strings.Contains(query.RawSql, `"`+ref[1]+`"`) {
				t.Fatalf("панель %d: getOption читает поле %q, которого нет в SQL — панель молча вернёт {}:\n%s", tc.id, ref[1], query.RawSql)
			}
		}
	}
}

// uralsBudgetEChartsOptions читает options ECharts-панели из артефакта (то, что реально уедет в
// Grafana): editorMode/renderer/getOption формата плагина 7.2.5.
func uralsBudgetEChartsOptions(t *testing.T, p *dashboard.Panel) echartsPanelOptions {
	t.Helper()
	raw, err := json.Marshal(p.Options)
	if err != nil {
		t.Fatal(err)
	}
	var opts echartsPanelOptions
	if err := json.Unmarshal(raw, &opts); err != nil {
		t.Fatalf("панель %q: options не разбираются: %v (%s)", panelTitle(p), err, raw)
	}
	return opts
}

// TestUralsBudgetRuleEChartsJS: обе ECharts-панели читают данные только из data frames
// (context.panel.data), литеральных данных и SQL в JS нет; цифры публикации (101.46, 8418, 6812)
// в JS не вписываются — они приходят из SQL. Массивов данных нет ни в одной серии: единственный
// разрешённый литеральный массив — конфиг опорной линии markLine П2 ($59, не данные).
func TestUralsBudgetRuleEChartsJS(t *testing.T) {
	board := builtUralsBudgetRule(t)
	banned := []string{
		"101.46", "8418", "8 418", "6812", "6 812",
		"FROM v_", "$__dateFilter", "$__timeFilter",
		"v_minec_macro", "v_cbr_macro", "v_fedstat_ipc", "v_minfin_budget",
	}
	for _, id := range []uint32{1, 2} {
		panel := panelByID(t, board, id)
		if panel.PluginVersion == nil || *panel.PluginVersion != EChartsPluginVersion {
			t.Fatalf("панель %d: pluginVersion %v, ожидался %s", id, panel.PluginVersion, EChartsPluginVersion)
		}
		opts := uralsBudgetEChartsOptions(t, panel)
		if opts.EditorMode != "code" || opts.Renderer != "canvas" {
			t.Fatalf("панель %d: editorMode=%q renderer=%q, ожидалось code/canvas", id, opts.EditorMode, opts.Renderer)
		}
		if !strings.Contains(opts.GetOption, "context.panel.data") {
			t.Fatalf("панель %d: getOption не читает data frames запроса", id)
		}
		for _, literal := range banned {
			if strings.Contains(opts.GetOption, literal) {
				t.Fatalf("панель %d: getOption содержит %q — данные и SQL живут в таргете", id, literal)
			}
		}
		// markLine-конфиг П2 — не данные: вырезаем его и требуем, чтобы других литеральных
		// массивов в data-полях не осталось ни у одной ECharts-панели.
		withoutMarkLine := strings.ReplaceAll(opts.GetOption, "data: [{ yAxis: BUDGET_RULE_CUTOFF }]", "")
		if strings.Contains(withoutMarkLine, "data: [") {
			t.Fatalf("панель %d: в getOption литеральный массив данных:\n%s", id, opts.GetOption)
		}
	}
}

// TestUralsBudgetRulePanel2BudgetRule: П2 — бюджетное правило $59. Числовое значение 59 задано
// только константой BUDGET_RULE_CUTOFF, и markLine берёт её (имена колонок с $59 приходят из SQL
// и значениями не являются); константа обязана нести комментарий-источник (параметр Минфина, не
// ряд витрины), подпись последней точки — endLabel, разрывы не соединяются.
func TestUralsBudgetRulePanel2BudgetRule(t *testing.T) {
	panel := panelByID(t, builtUralsBudgetRule(t), 2)
	js := uralsBudgetEChartsOptions(t, panel).GetOption
	for _, want := range []string{
		"markLine",
		"BUDGET_RULE_CUTOFF = 59",
		"yAxis: BUDGET_RULE_CUTOFF",
		"Бюджетное правило: $",
		"endLabel",
		"stack: 'rule'",
		"connectNulls: false",
		"axisValue",
		"Минфин",
		"2027",
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("П2: getOption без %q:\n%s", want, js)
		}
	}
	if got := strings.Count(js, "BUDGET_RULE_CUTOFF = 59"); got != 1 {
		t.Fatalf("П2: присваивание константы встречается %d раз, ожидалось 1", got)
	}
	if strings.Contains(js, "yAxis: 59") {
		t.Fatalf("П2: markLine задаёт 59 литералом, а не константой:\n%s", js)
	}
}

// TestUralsBudgetRulePanel1Estimate: строки с «Оценка» = 1 (месяц без свежего ИПЦ) в линии годов
// НЕ входят: ветка оценки делает continue до записи в lines, а сама строка уходит отдельной
// scatter-серией с полым маркером (symbol emptyCircle). Это защита от потери оконного anyLast
// и join_use_nulls.
func TestUralsBudgetRulePanel1Estimate(t *testing.T) {
	panel := panelByID(t, builtUralsBudgetRule(t), 1)
	js := uralsBudgetEChartsOptions(t, panel).GetOption
	for _, want := range []string{
		"'Оценка'",
		"symbol: 'emptyCircle'",
		"connectNulls: false",
		"estimates.push",
		"type: 'scatter'",
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("П1: getOption без %q:\n%s", want, js)
		}
	}
	estimateBranch := "if (Number(estimate[i]) === 1) {"
	index := strings.Index(js, estimateBranch)
	if index < 0 {
		t.Fatalf("П1: getOption не различает оценочные строки:\n%s", js)
	}
	tail := js[index:]
	pushAt := strings.Index(tail, "estimates.push")
	continueAt := strings.Index(tail, "continue;")
	lineAt := strings.Index(tail, "lines[y][m] = real[i]")
	if pushAt < 0 || continueAt < 0 || lineAt < 0 {
		t.Fatalf("П1: в ветке оценки нет push/continue/записи в линии:\n%s", tail)
	}
	if !(pushAt < continueAt && continueAt < lineAt) {
		t.Fatalf("П1: оценочная строка попадает в линию года (порядок push=%d continue=%d line=%d)", pushAt, continueAt, lineAt)
	}
	// Tooltip оценочной точки: items scatter-серии — объекты {value, year, month}, а items линий —
	// числа. Ветка обязана читать p.data.value[1]/p.data.month/p.data.year, иначе hover октября
	// печатает «Оценка Янв: NaN ₽/барр.» (проверка Array.isArray здесь мертва для обеих форм).
	tooltipBranch := "if (p.data && p.data.value) {"
	branchAt := strings.Index(js, tooltipBranch)
	if branchAt < 0 {
		t.Fatalf("П1: tooltip не различает object-item оценочной точки:\n%s", js)
	}
	branch := js[branchAt:]
	if end := strings.Index(branch, "}"); end >= 0 {
		branch = branch[:end]
	}
	for _, want := range []string{"p.data.value[1]", "p.data.month - 1", "p.data.year"} {
		if !strings.Contains(branch, want) {
			t.Fatalf("П1: tooltip оценочной точки не читает %q:\n%s", want, branch)
		}
	}
	if strings.Contains(js, "Array.isArray(p.data)") {
		t.Fatalf("П1: tooltip проверяет Array.isArray(p.data) — items линий это числа, а оценки — объект:\n%s", js)
	}
}

// TestUralsBudgetRuleUnits: единицы панелей по спеке §3.5. П5/П6/П7 — plain number (units.Number):
// currencyRUB SI-скейлит ≥1000 («₽8.42K»/«₽5.47K»), а единица уже стоит в заголовке («₽/барр.»,
// «млрд ₽»); П5/П6 несут ещё и проценты — у процентных колонок override percent по имени поля
// (иначе темп печатался бы как число без знака %). Override сверяется по имени алиаса SQL:
// matcher byName сопоставляется с именем поля, которое даёт алиас.
func TestUralsBudgetRuleUnits(t *testing.T) {
	board := builtUralsBudgetRule(t)
	want := map[uint32]string{
		1: units.Rubles,
		2: units.Dollars,
		3: units.Dollars,
		4: units.Short,
		5: units.Number,
		6: units.Number,
		7: units.Number,
	}
	for id, unit := range want {
		panel := panelByID(t, board, id)
		if panel.FieldConfig == nil || panel.FieldConfig.Defaults.Unit == nil {
			t.Fatalf("панель %d: панельная единица не задана", id)
		}
		if got := *panel.FieldConfig.Defaults.Unit; got != unit {
			t.Fatalf("панель %d: единица %q, ожидалась %q", id, got, unit)
		}
	}
	assertUnitOverride(t, panelByID(t, board, 5), "г/г номинально, %", units.Percent)
	assertUnitOverride(t, panelByID(t, board, 5), "г/г реально, %", units.Percent)
	assertUnitOverride(t, panelByID(t, board, 6), "г/г, %", units.Percent)
	if defaults := panelByID(t, board, 4).FieldConfig.Defaults; defaults.Decimals == nil || *defaults.Decimals != 2 {
		t.Fatal("панель 4: decimals не 2 — SQL уже округляет курс до 2 знаков, иначе stat печатает 82.9700")
	}
	if got := len(panelByID(t, board, 5).FieldConfig.Overrides); got != 2 {
		t.Fatalf("панель 5: override'ов %d, ожидалось 2 (два г/г темпа)", got)
	}
	if got := len(panelByID(t, board, 6).FieldConfig.Overrides); got != 1 {
		t.Fatalf("панель 6: override'ов %d, ожидался 1 (темп г/г)", got)
	}
}

// TestUralsBudgetRulePanel7Legend: П7 сравнивает 2025 и 2026 — без легенды два ряда не различить,
// а дефолт barchart-панели — showLegend false. Легенда включается явно и ставится снизу.
func TestUralsBudgetRulePanel7Legend(t *testing.T) {
	panel := panelByID(t, builtUralsBudgetRule(t), 7)
	raw, err := json.Marshal(panel.Options)
	if err != nil {
		t.Fatal(err)
	}
	var opts struct {
		Legend common.VizLegendOptions `json:"legend"`
	}
	if err := json.Unmarshal(raw, &opts); err != nil {
		t.Fatalf("П7: options не разбираются: %v (%s)", err, raw)
	}
	if !opts.Legend.ShowLegend {
		t.Fatalf("П7: легенда выключена — два ряда 2025/2026 не различить (%s)", raw)
	}
	if got := opts.Legend.Placement; got != common.LegendPlacementBottom {
		t.Fatalf("П7: размещение легенды %q, ожидалось %q", got, common.LegendPlacementBottom)
	}
}

// assertUnitOverride требует override unit по имени поля: у П5/П6 панельная единица — plain
// number, и без override процентные колонки печатались бы числами без знака %.
func assertUnitOverride(t *testing.T, p *dashboard.Panel, fieldName, wantUnit string) {
	t.Helper()
	for _, override := range p.FieldConfig.Overrides {
		name, ok := override.Matcher.Options.(*string)
		if override.Matcher.Id != "byName" || !ok || name == nil || *name != fieldName {
			continue
		}
		for _, property := range override.Properties {
			if property.Id == "unit" && property.Value == wantUnit {
				return
			}
		}
		t.Fatalf("панель %q: у поля %q нет unit=%q", panelTitle(p), fieldName, wantUnit)
	}
	t.Fatalf("панель %q: нет override по имени %q", panelTitle(p), fieldName)
}

// TestUralsBudgetRuleDatasourceConstant: датасорс каждой панели — константы контура; иначе панель
// уедет на чужой датасорс без грантов на витрины.
func TestUralsBudgetRuleDatasourceConstant(t *testing.T) {
	for _, p := range panelsOf(builtUralsBudgetRule(t)) {
		if p.Datasource == nil || p.Datasource.Uid == nil || *p.Datasource.Uid != DatasourceUID {
			t.Fatalf("панель %q без константного UID датасорса", panelTitle(p))
		}
		if p.Datasource.Type == nil || *p.Datasource.Type != DatasourceType {
			t.Fatalf("панель %q: тип датасорса %v", panelTitle(p), p.Datasource.Type)
		}
	}
}

// TestUralsBudgetRuleGenerateDeterministic: две сборки подряд дают побайтно одинаковый манифест
// (на детерминизме стоит CI-проверка diff артефакта).
func TestUralsBudgetRuleGenerateDeterministic(t *testing.T) {
	d1, err := NewUralsBudgetRule().Build()
	if err != nil {
		t.Fatal(err)
	}
	d2, err := NewUralsBudgetRule().Build()
	if err != nil {
		t.Fatal(err)
	}
	b1, err := json.MarshalIndent(Manifest(d1), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b2, err := json.MarshalIndent(Manifest(d2), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatal("генерация недетерминирована")
	}
}
