package main

import (
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// bondAttractivenessRowTitles — ряды дашборда в порядке следования (пункт ofz-market/1.8, спека §3):
// чек-лист «все триггеры», шесть снимков, три графика ставки и инфляции, два графика кривой, три
// графика вкладов и альтернативы, четыре графика средств клиентов SORS.
var bondAttractivenessRowTitles = []string{
	"Триггеры статьи: облигации и акции",
	"Светофор статьи",
	"Ставка и инфляция",
	"Кривая ОФЗ",
	"Вклады, кредиты, альтернатива",
	"Средства клиентов банков (ЦБ, SORS)",
}

func builtBondAttractiveness(t *testing.T) dashboard.Dashboard {
	t.Helper()
	return mustBuild(t, NewBondAttractiveness())
}

// TestBondAttractivenessHasTwentyOnePanels фиксирует состав дашборда: 21 панель — чек-лист
// триггеров, 6 снимков, 12 графиков и две иллюстрации тезисов (50, 51). Уменьшение или рост
// числа панелей меняет контракт спеки и должен быть виден.
func TestBondAttractivenessHasTwentyOnePanels(t *testing.T) {
	if panels := panelsOf(builtBondAttractiveness(t)); len(panels) != 21 {
		t.Fatalf("панелей %d, ожидалось 21", len(panels))
	}
}

// TestBondAttractivenessRows: пять рядов, заголовки и порядок — как в спеке §3, id рядов вне
// диапазона панелей 1..18 (нулевой id Grafana заменит своим, и артефакт разойдётся с кодом).
func TestBondAttractivenessRows(t *testing.T) {
	var titles []string
	for _, entry := range builtBondAttractiveness(t).Panels {
		if entry.RowPanel == nil {
			continue
		}
		if entry.RowPanel.Id <= 18 {
			t.Fatalf("ряд %q: id %d попадает в диапазон панелей 1..18", optionalString(entry.RowPanel.Title), entry.RowPanel.Id)
		}
		titles = append(titles, optionalString(entry.RowPanel.Title))
	}
	if len(titles) != len(bondAttractivenessRowTitles) {
		t.Fatalf("рядов %d, ожидалось %d: %v", len(titles), len(bondAttractivenessRowTitles), titles)
	}
	for i, got := range titles {
		if got != bondAttractivenessRowTitles[i] {
			t.Fatalf("ряд %d: %q, ожидался %q", i+1, got, bondAttractivenessRowTitles[i])
		}
	}
}

// TestBondAttractivenessTimeseriesUseTimeFilter: каждый график уважает диапазон дашборда —
// $__timeFilter по своей временной колонке. Снимки (stat) не проверяются: у них время не ось,
// и диапазон к «последней точке» неприменим.
func TestBondAttractivenessTimeseriesUseTimeFilter(t *testing.T) {
	for _, p := range panelsOf(builtBondAttractiveness(t)) {
		if string(p.Type) != "timeseries" {
			continue
		}
		if len(p.Targets) == 0 {
			t.Fatalf("у графика %q нет запросов", panelTitle(p))
		}
		for _, target := range p.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("график %q: запрос не является запросом ClickHouse", panelTitle(p))
			}
			if !strings.Contains(query.RawSql, "$__timeFilter") {
				t.Fatalf("график %q (%s): запрос без $__timeFilter:\n%s", panelTitle(p), query.RefId, query.RawSql)
			}
			if strings.Contains(strings.ToUpper(query.RawSql), "LIMIT") {
				t.Fatalf("график %q (%s): LIMIT обрезает ряд:\n%s", panelTitle(p), query.RefId, query.RawSql)
			}
			if strings.Contains(query.RawSql, "WITH FILL") {
				t.Fatalf("график %q (%s): WITH FILL подставляет нули вместо неизвестных данных:\n%s", panelTitle(p), query.RefId, query.RawSql)
			}
		}
	}
}

// TestBondAttractivenessOnlyViews: каждый таргет читает только витрины v_* — сырые cbr_* / ofz_*
// агенту не выданы, и Validate такое не пропустит. Проверка по всем панелям, включая stat.
func TestBondAttractivenessOnlyViews(t *testing.T) {
	for _, panel := range panelsOf(builtBondAttractiveness(t)) {
		for _, target := range panel.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %q: таргет не является запросом ClickHouse", panelTitle(panel))
			}
			if !strings.Contains(query.RawSql, "FROM v_") {
				t.Fatalf("панель %q: запрос читает не витрину:\n%s", panelTitle(panel), query.RawSql)
			}
			for _, raw := range []string{"FROM cbr_", "JOIN cbr_", "FROM ofz_", "JOIN ofz_"} {
				if strings.Contains(query.RawSql, raw) {
					t.Fatalf("панель %q: запрос читает сырую таблицу %s:\n%s", panelTitle(panel), raw, query.RawSql)
				}
			}
		}
	}
}

// TestBondAttractivenessLoansAreSeparateTargets — главная ловушка данных: розничные ставки ЦБ
// заканчиваются декабрём 2025, а maxIf без совпавших строк возвращает 0. Сводный запрос нарисовал
// бы нулевую ставку по 2026 году. Поэтому у панели 13 два таргета, и каждый фильтрует свой ряд.
func TestBondAttractivenessLoansAreSeparateTargets(t *testing.T) {
	panel := panelByID(t, builtBondAttractiveness(t), 13)
	if len(panel.Targets) != 2 {
		t.Fatalf("панель 13: таргетов %d, ожидалось 2 (юрлица и физлица отдельно)", len(panel.Targets))
	}
	var corp, retail bool
	for _, target := range panel.Targets {
		query, ok := chQuery(target)
		if !ok {
			t.Fatal("панель 13: таргет не является запросом ClickHouse")
		}
		hasCorp := strings.Contains(query.RawSql, "series = 'Всего: свыше 1 года'")
		hasRetail := strings.Contains(query.RawSql, "series = 'Всего (кредиты физлицам): свыше 1 года'")
		if hasCorp == hasRetail {
			t.Fatalf("панель 13: таргет %s должен читать ровно один ряд, а не оба/ни один:\n%s", query.RefId, query.RawSql)
		}
		corp = corp || hasCorp
		retail = retail || hasRetail
	}
	if !corp || !retail {
		t.Fatalf("панель 13: ряды не разделены (юрлица=%v, физлица=%v)", corp, retail)
	}
}

// TestBondAttractivenessDepositOfzSpreadMonthly — вклады и ОФЗ лежат на разных датах (вклад —
// конец месяца, ОФЗ — торговый день), поэтому спред считается по месяцам; JOIN по точной дате
// вернул бы пустой ответ (проверено 2026-11-08: 2026-08-30 — воскресенье).
func TestBondAttractivenessDepositOfzSpreadMonthly(t *testing.T) {
	query := singleTargetSQL(t, builtBondAttractiveness(t), 14)
	for _, want := range []string{"toStartOfMonth(date)", "INNER JOIN", "от 181 дня до 1 года", "argMax(ofz_1y, date)"} {
		if !strings.Contains(query, want) {
			t.Fatalf("панель 14: в запросе нет %q:\n%s", want, query)
		}
	}
}

// TestBondAttractivenessHouseholdsGrowthWindowOutside: lagInFrame обязан считаться по ПОЛНОМУ ряду,
// иначе первые 12 месяцев окна остались бы без г/г. Фильтр $__timeFilter стоит один раз — снаружи
// подзапроса, по вычисленному time.
func TestBondAttractivenessHouseholdsGrowthWindowOutside(t *testing.T) {
	query := singleTargetSQL(t, builtBondAttractiveness(t), 16)
	if got := strings.Count(query, "$__timeFilter"); got != 1 {
		t.Fatalf("панель 16: $__timeFilter встречается %d раз, ожидался 1 (только снаружи):\n%s", got, query)
	}
	if !strings.Contains(query, ")\nWHERE $__timeFilter(time)") {
		t.Fatalf("панель 16: фильтр должен стоять снаружи подзапроса, по time:\n%s", query)
	}
	if strings.Count(query, "lagInFrame") != 2 {
		t.Fatalf("панель 16: ожидались два lagInFrame (г/г и м/м):\n%s", query)
	}
}

// TestBondAttractivenessSorsSeriesAsTargets: ряды SORS начинаются в разные даты (эскроу и вклады
// без эскроу — с 2019-02), поэтому уровни идут отдельными таргетами; сводный maxIf до старта ряда
// дал бы ноль. Панель 15 — четыре таргета, панель 17 — два.
func TestBondAttractivenessSorsSeriesAsTargets(t *testing.T) {
	cases := map[uint32]struct {
		targets int
		series  []string
	}{
		15: {4, []string{"clients_total", "households_deposits_escrow", "corp_deposits", "org_accounts"}},
		17: {2, []string{"escrow_accounts", "ip_accounts"}},
	}
	for id, tc := range cases {
		panel := panelByID(t, builtBondAttractiveness(t), id)
		if len(panel.Targets) != tc.targets {
			t.Fatalf("панель %d: таргетов %d, ожидалось %d", id, len(panel.Targets), tc.targets)
		}
		var sql strings.Builder
		for _, target := range panel.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %d: таргет не является запросом ClickHouse", id)
			}
			sql.WriteString(query.RawSql)
		}
		for _, series := range tc.series {
			if !strings.Contains(sql.String(), "series = '"+series+"'") {
				t.Fatalf("панель %d: нет отдельного таргета для ряда %s", id, series)
			}
		}
		if strings.Contains(sql.String(), "maxIf") {
			t.Fatalf("панель %d: уровни SORS не должны сводиться maxIf — до старта ряда это ноль", id)
		}
	}
}

// TestBondAttractivenessThresholds: пороги статьи — 10.43 у ставки (средневзвешенная с 2013),
// 0 у спредов и 11/12 у вкладов и кредитов. Порог живёт в steps панели; пропажа порога меняет
// смысл «светофора» молча.
func TestBondAttractivenessThresholds(t *testing.T) {
	cases := map[uint32][]float64{
		1:  {10.4314},
		3:  {0},
		5:  {11, 12},
		6:  {11, 12},
		11: {0},
		14: {0},
	}
	for id, want := range cases {
		panel := panelByID(t, builtBondAttractiveness(t), id)
		steps := panel.FieldConfig.Defaults.Thresholds.Steps
		var values []float64
		for _, step := range steps {
			if step.Value != nil {
				values = append(values, *step.Value)
			}
		}
		if len(values) != len(want) {
			t.Fatalf("панель %d: порогов %d, ожидалось %d: %v", id, len(values), len(want), values)
		}
		for i, v := range want {
			if values[i] != v {
				t.Fatalf("панель %d: порог %d = %v, ожидался %v", id, i+1, values[i], v)
			}
		}
	}
}

// TestBondAttractivenessUnits: ставки — percent, спреды — short (п.п. в заголовке), уровни SORS —
// currencyRUB при значениях в трлн (единица в заголовке), темпы и доля — percent.
func TestBondAttractivenessUnits(t *testing.T) {
	cases := map[uint32]string{
		1:  units.Percent,
		2:  units.Short,
		3:  units.Short,
		4:  units.Percent,
		5:  units.Percent,
		6:  units.Percent,
		7:  units.Percent,
		8:  units.Percent,
		9:  units.Short,
		10: units.Percent,
		11: units.Short,
		12: units.Percent,
		13: units.Percent,
		14: units.Short,
		15: units.Rubles,
		16: units.Percent,
		17: units.Rubles,
		18: units.Percent,
	}
	for id, want := range cases {
		panel := panelByID(t, builtBondAttractiveness(t), id)
		got := panel.FieldConfig.Defaults.Unit
		if got == nil || *got != want {
			t.Fatalf("панель %d: unit %v, ожидался %q", id, got, want)
		}
	}
}

// TestBondAttractivenessNoTemplating: переменных шаблона нет — окно задаётся дашбордом, ряды
// фиксированы витринами.
func TestBondAttractivenessNoTemplating(t *testing.T) {
	if list := builtBondAttractiveness(t).Templating.List; len(list) != 0 {
		t.Fatalf("в дашборде %d переменных шаблона, ожидалось 0", len(list))
	}
}

// TestBondAttractivenessValidateUID проверяет параметризацию Validate: опечатка в uid иначе
// прошла бы молча, и при деплое один дашборд затёр бы другой.
func TestBondAttractivenessValidateUID(t *testing.T) {
	board := builtBondAttractiveness(t)
	if err := Validate(board, BondAttractivenessUID); err != nil {
		t.Fatalf("дашборд bond-attractiveness не прошёл валидацию: %v", err)
	}
	if err := Validate(board, GoldThesisUID); err == nil {
		t.Fatal("дашборд принят с чужим uid: проверка uid не параметризована")
	}
}

// TestBondAttractivenessTriggerChecklist — панель 19: один снимок со всеми триггерами статьи
// сразу для облигаций и акций. Девять числовых полей одной строки (стат-плитки), вердикт — из
// value mapping'а. Таблица не используется: табличный формат плагина со строковыми колонками
// вешает датасорс на стенде (проверено 2026-11-08).
func TestBondAttractivenessTriggerChecklist(t *testing.T) {
	panel := panelByID(t, builtBondAttractiveness(t), 19)
	if string(panel.Type) != "stat" {
		t.Fatalf("панель 19: тип %q, ожидался stat", panel.Type)
	}
	query := singleTargetSQL(t, builtBondAttractiveness(t), 19)
	for _, want := range []string{
		`AS "Ключ vs среднее"`, `AS "ОФЗ 10л vs ключ"`, `AS "ОФЗ 1г vs ключ"`,
		`AS "Вклады 11–12%"`, `AS "Кредиты 11–12%"`, `AS "Отток вкладов"`,
		`AS "Ключ ≤ 12%"`, `AS "Реальная ставка"`, `AS "Новостройки vs ИПЦ*"`,
		`AS "Ипотека: вторичка − первичка, п.п."`,
		"toUInt8(if(", "'cbr_key_rate'", "'от 181 дня до 1 года'", "'Всего: свыше 1 года'",
		"households_deposits_escrow", "v_domrf_price_index", "v_domrf_mortgage",
		"rate_primary", "rate_secondary", "$__timeFilter(date)",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("панель 19: в запросе нет %q:\n%s", want, query)
		}
	}
	if strings.Contains(query, "UNION ALL") {
		t.Fatalf("панель 19: ожидалась одна строка с девятью полями, а не UNION:\n%s", query)
	}
	if strings.Contains(query, "Nullable") {
		t.Fatalf("панель 19: NULL-флаги не используются — вердикт кодируется 0/1/2:\n%s", query)
	}
}

// TestBondAttractivenessCreditDepositPanel — панель 50: иллюстрация «при 11–12% по кредитам
// начинается отток». Три таргета, а не сводный maxIf: розница обрывается в декабре 2025, и
// maxIf по двум рядам дал бы нулевую ставку по 2026 году. Пороги 11/12 — линии полосы.
func TestBondAttractivenessCreditDepositPanel(t *testing.T) {
	panel := panelByID(t, builtBondAttractiveness(t), 50)
	if len(panel.Targets) != 3 {
		t.Fatalf("панель 50: таргетов %d, ожидалось 3", len(panel.Targets))
	}
	var sql strings.Builder
	for _, target := range panel.Targets {
		query, ok := chQuery(target)
		if !ok {
			t.Fatal("панель 50: таргет не является запросом ClickHouse")
		}
		sql.WriteString(query.RawSql)
	}
	for _, want := range []string{
		"series = 'Всего: свыше 1 года'",
		"series = 'Всего (кредиты физлицам): свыше 1 года'",
		"households_deposits_escrow", "lagInFrame",
	} {
		if !strings.Contains(sql.String(), want) {
			t.Fatalf("панель 50: в запросах нет %q", want)
		}
	}
	if strings.Contains(sql.String(), "maxIf(value, series") {
		t.Fatalf("панель 50: сводный maxIf по рядам дал бы ноль там, где ряд кончился")
	}
	steps := panel.FieldConfig.Defaults.Thresholds.Steps
	var values []float64
	for _, step := range steps {
		if step.Value != nil {
			values = append(values, *step.Value)
		}
	}
	if len(values) != 2 || values[0] != 11 || values[1] != 12 {
		t.Fatalf("панель 50: пороги %v, ожидались 11 и 12", values)
	}
}

// TestBondAttractivenessRhetoricPanel — панель 51: иллюстрация «жёсткая риторика». Ставка,
// реальная ставка и вменённая ставка рынка точкой на дате решения (decision_date, не дата
// торгов); точка рисуется без линии.
func TestBondAttractivenessRhetoricPanel(t *testing.T) {
	panel := panelByID(t, builtBondAttractiveness(t), 51)
	if len(panel.Targets) != 3 {
		t.Fatalf("панель 51: таргетов %d, ожидалось 3", len(panel.Targets))
	}
	var implied string
	for _, target := range panel.Targets {
		query, ok := chQuery(target)
		if !ok {
			t.Fatal("панель 51: таргет не является запросом ClickHouse")
		}
		if strings.Contains(query.RawSql, "v_keyrate_expectations") {
			implied = query.RawSql
		}
	}
	for _, want := range []string{"decision_date AS time", "argMax(settle, date)", "secid = 'KKZ6'"} {
		if !strings.Contains(implied, want) {
			t.Fatalf("панель 51: в запросе вменённой ставки нет %q:\n%s", want, implied)
		}
	}
	var hasPoints bool
	for _, o := range panel.FieldConfig.Overrides {
		if o.Matcher.Options != nil && strings.HasPrefix(*o.Matcher.Options.(*string), "Вменённая ставка") {
			hasPoints = true
		}
	}
	if !hasPoints {
		t.Fatal("панель 51: у вменённой ставки нет override — точку нужно рисовать без линии")
	}
}

// singleTargetSQL возвращает единственный запрос панели: у панелей с одним таргетом второй был бы
// дублем источника.
func singleTargetSQL(t *testing.T, d dashboard.Dashboard, id uint32) string {
	t.Helper()
	panel := panelByID(t, d, id)
	if len(panel.Targets) != 1 {
		t.Fatalf("панель %d: таргетов %d, ожидался 1", id, len(panel.Targets))
	}
	query, ok := chQuery(panel.Targets[0])
	if !ok {
		t.Fatalf("панель %d: таргет не является запросом ClickHouse", id)
	}
	return query.RawSql
}
