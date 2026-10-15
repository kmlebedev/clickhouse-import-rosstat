package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// keyRateRowTitles — ряды дашборда в порядке следования (id 21–24). Собраны все четыре ряда спеки:
// список фиксирует и заголовки, и порядок сетки, а id (вне диапазона панелей 1..19) проверяет
// TestKeyRatePublicationsRowTitles.
var keyRateRowTitles = []string{
	"Ставка и инфляция",
	"Деньги, кредит, бюджет, труд",
	"Рыночный контур",
	"Сигнал, консенсус, календарь",
}

func builtKeyRatePublications(t *testing.T) dashboard.Dashboard {
	t.Helper()
	d, err := NewKeyRatePublications().Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestKeyRatePublicationsHasNineteenPanels фиксирует состав дашборда: собраны ряды 21 (панели 1–5),
// 22 (панели 6–10), 23 (панели 11–14) и 24 (панели 15–19) — 19 панелей, полный контракт спеки §5.
func TestKeyRatePublicationsHasNineteenPanels(t *testing.T) {
	if panels := panelsOf(builtKeyRatePublications(t)); len(panels) != 19 {
		t.Fatalf("панелей %d, ожидалось 19", len(panels))
	}
}

// TestKeyRatePublicationsRowTitles: ряды нумеруются вне диапазона панелей 1..19 — нулевой
// или совпавший id Grafana перезапишет своим, и артефакт разойдётся с кодом.
func TestKeyRatePublicationsRowTitles(t *testing.T) {
	var titles []string
	for _, entry := range builtKeyRatePublications(t).Panels {
		if entry.RowPanel == nil {
			continue
		}
		if entry.RowPanel.Id < 20 {
			t.Fatalf("ряд %q: id %d попадает в диапазон панелей 1..19", optionalString(entry.RowPanel.Title), entry.RowPanel.Id)
		}
		titles = append(titles, optionalString(entry.RowPanel.Title))
	}
	if len(titles) != len(keyRateRowTitles) {
		t.Fatalf("рядов %d, ожидалось %d: %v", len(titles), len(keyRateRowTitles), titles)
	}
	for i, got := range titles {
		if got != keyRateRowTitles[i] {
			t.Fatalf("ряд %d: %q, ожидался %q", i+1, got, keyRateRowTitles[i])
		}
	}
}

// TestKeyRateAllTargetsUseViews: у датасорса стенда нет грантов на сырые таблицы (в UI-экспорте
// запросы как раз на сырых), поэтому каждый запрос каждой панели обязан читать витрину v_*.
func TestKeyRateAllTargetsUseViews(t *testing.T) {
	for _, p := range panelsOf(builtKeyRatePublications(t)) {
		if len(p.Targets) == 0 {
			t.Fatalf("у панели %q нет запросов", panelTitle(p))
		}
		for _, target := range p.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(p))
			}
			if !strings.Contains(query.RawSql, "FROM v_") {
				t.Fatalf("панель %q: запрос %q читает не витрину: %s", panelTitle(p), query.RefId, query.RawSql)
			}
		}
	}
}

// TestKeyRatePanelIDsUnique проверяет и раскладку id (через Validate), и параметризацию
// Validate: тот же дашборд с чужим uid обязан быть отвергнут, иначе опечатка в uid прошла
// бы молча и один дашборд затёр бы другой при деплое.
func TestKeyRatePanelIDsUnique(t *testing.T) {
	board := builtKeyRatePublications(t)
	if err := Validate(board, KeyRatePublicationsUID); err != nil {
		t.Fatalf("дашборд key-rate-publications не прошёл валидацию: %v", err)
	}
	if err := Validate(board, GoldThesisUID); err == nil {
		t.Fatal("дашборд принят с чужим uid: проверка uid не параметризована")
	}
}

// TestKeyRateWeeklyYearOverlay: ряд B панели 2 — календарное наложение прошлого года.
// Время сдвигается на +1 год (date_add), а год отбирается ровно прошлый (toYear(now()) - 1):
// поменяй одно без другого — наложение покажет не тот год, и «прошлый год» окажется текущим.
func TestKeyRateWeeklyYearOverlay(t *testing.T) {
	panel := panelByID(t, builtKeyRatePublications(t), 2)
	if len(panel.Targets) != 2 {
		t.Fatalf("у панели %q %d запросов, ожидалось 2 (текущий и прошлый год)", panelTitle(panel), len(panel.Targets))
	}
	query, ok := chQuery(panel.Targets[1])
	if !ok {
		t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(panel))
	}
	for _, want := range []string{"date_add(YEAR, 1, date)", "toYear(now()) - 1"} {
		if !strings.Contains(query.RawSql, want) {
			t.Fatalf("панель %q: запрос %q не содержит %q: %s", panelTitle(panel), query.RefId, want, query.RawSql)
		}
	}
}

// TestKeyRateWeeklyJoinCatalog: недельные ряды отбираются каталогом (frequency = 'W'), а не
// списком имён в SQL. JOIN по (source, series) обязателен в обоих недельных запросах: товар
// без строки каталога молча выпал бы из avg/count, и среднее считалось бы по неполному набору.
func TestKeyRateWeeklyJoinCatalog(t *testing.T) {
	board := builtKeyRatePublications(t)
	for _, id := range []uint32{2, 3} {
		panel := panelByID(t, board, id)
		for _, target := range panel.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(panel))
			}
			for _, want := range []string{"JOIN v_series_catalog", "frequency = 'W'"} {
				if !strings.Contains(query.RawSql, want) {
					t.Fatalf("панель %q: запрос %q не содержит %q: %s", panelTitle(panel), query.RefId, want, query.RawSql)
				}
			}
		}
	}
}

// TestKeyRateUnits: SQL панели 1 отдаёт проценты годовых (percent), панелей 4 и 6 — доли
// (pow(...) - 1, value/lag - 1), которые Grafana как percentunit домножит на 100 и напечатает
// «3.2%». У панели 9 серия A — проценты, а зарплата (B) — доля: её единица задаётся пер-серийным
// override'ом, там же правая ось (шкалы безработицы и роста зарплаты несопоставимы). Панель 5:
// C — пункты (short), панель 13: RGBI — уровень индекса (short) против процентов теноров.
// Перепутанные единицы дают ошибку чтения на два порядка.
func TestKeyRateUnits(t *testing.T) {
	board := builtKeyRatePublications(t)
	if unit := panelByID(t, board, 4).FieldConfig.Defaults.Unit; unit == nil || *unit != units.PercentUnit {
		t.Fatalf("панель 4: unit %v, ожидался %q", unit, units.PercentUnit)
	}
	if unit := panelByID(t, board, 1).FieldConfig.Defaults.Unit; unit == nil || *unit != units.Percent {
		t.Fatalf("панель 1: unit %v, ожидался %q", unit, units.Percent)
	}
	if unit := panelByID(t, board, 6).FieldConfig.Defaults.Unit; unit == nil || *unit != units.PercentUnit {
		t.Fatalf("панель 6: unit %v, ожидался %q (YoY — доля)", unit, units.PercentUnit)
	}
	labour := panelByID(t, board, 9)
	if unit := labour.FieldConfig.Defaults.Unit; unit == nil || *unit != units.Percent {
		t.Fatalf("панель 9: unit %v, ожидался %q (безработица — проценты)", unit, units.Percent)
	}
	if got := overridePropByRefID(labour, "B", "unit"); got != units.PercentUnit {
		t.Fatalf("панель 9: unit серии B = %v, ожидался %q (зарплата — доля YoY)", got, units.PercentUnit)
	}
	if got := overridePropByRefID(labour, "B", "custom.axisPlacement"); got != "right" {
		t.Fatalf("панель 9: ось серии B = %v, ожидалась \"right\" (шкалы безработицы и роста зарплаты несопоставимы)", got)
	}
	expectations := panelByID(t, board, 5)
	if unit := expectations.FieldConfig.Defaults.Unit; unit == nil || *unit != units.Percent {
		t.Fatalf("панель 5: unit %v, ожидался %q (ожидания населения — проценты)", unit, units.Percent)
	}
	if got := overridePropByRefID(expectations, "C", "unit"); got != units.Short {
		t.Fatalf("панель 5: unit серии C = %v, ожидался %q (ценовые ожидания бизнеса — пункты)", got, units.Short)
	}
	trajectory := panelByID(t, board, 11)
	if unit := trajectory.FieldConfig.Defaults.Unit; unit == nil || *unit != units.Percent {
		t.Fatalf("панель 11: unit %v, ожидался %q (вменённая ставка — проценты годовых)", unit, units.Percent)
	}
	curve := panelByID(t, board, 13)
	if unit := curve.FieldConfig.Defaults.Unit; unit == nil || *unit != units.Percent {
		t.Fatalf("панель 13: unit %v, ожидался %q (доходности теноров — проценты годовых)", unit, units.Percent)
	}
	if got := overridePropByRefID(curve, "A", "unit"); got != units.Short {
		t.Fatalf("панель 13: unit серии A = %v, ожидался %q (RGBI — уровень индекса, а не проценты)", got, units.Short)
	}
	ruonia := panelByID(t, board, 14)
	if unit := ruonia.FieldConfig.Defaults.Unit; unit == nil || *unit != units.Percent {
		t.Fatalf("панель 14: unit %v, ожидался %q (RUONIA, ставка и спред — проценты и п.п.)", unit, units.Percent)
	}
}

// overridePropByRefID возвращает значение свойства override'а серии по refId: пер-серийные
// единица и ось живут только в properties override'а, и опечатка в refId молча оставила бы
// серию с панельным дефолтом.
func overridePropByRefID(p *dashboard.Panel, refID, propID string) any {
	for _, override := range p.FieldConfig.Overrides {
		if override.Matcher.Id != "byFrameRefID" {
			continue
		}
		name, ok := override.Matcher.Options.(*string)
		if !ok || name == nil || *name != refID {
			continue
		}
		for _, prop := range override.Properties {
			if prop.Id == propID {
				return prop.Value
			}
		}
	}
	return nil
}

// TestKeyRateThresholdsStyleSet: без custom.thresholdsStyle Grafana не рисует порог (по умолчанию
// off), а пороги — смысловые границы: 4% (цель ЦБ по инфляции) у панелей 2 и 4, нуль (граница
// роста и сжатия) у панели 6. Порог без стиля молча пропал бы с графика. Значение шага — доля
// (0.04 = 4%; 0 = граница знаковых темпов), как и значения серий этих панелей.
func TestKeyRateThresholdsStyleSet(t *testing.T) {
	board := builtKeyRatePublications(t)
	for _, tc := range []struct {
		panelID uint32
		step    float64
	}{{2, 0.04}, {4, 0.04}, {6, 0}} {
		panel := panelByID(t, board, tc.panelID)
		if mode := thresholdStyleMode(t, panel); mode == "" || mode == "off" {
			t.Fatalf("панель %d: thresholdsStyle.mode=%q, ожидался не off", tc.panelID, mode)
		}
		if !hasThresholdStep(panel, tc.step) {
			t.Fatalf("панель %d: нет шага порога %v", tc.panelID, tc.step)
		}
	}
}

// TestKeyRateZeroThresholdIsSignColored: шаги порога панели 6 размечают знак темпа — ниже нуля
// (сжатие) красный, от нуля (рост) зелёный. Те же значения порога с обратными цветами читались бы
// ровно наоборот.
func TestKeyRateZeroThresholdIsSignColored(t *testing.T) {
	thresholds := panelByID(t, builtKeyRatePublications(t), 6).FieldConfig.Defaults.Thresholds
	if thresholds == nil {
		t.Fatal("у панели 6 нет порогов")
	}
	steps := thresholds.Steps
	if len(steps) != 2 || steps[0].Value != nil || steps[0].Color != "red" ||
		steps[1].Value == nil || *steps[1].Value != 0 || steps[1].Color != "green" {
		t.Fatalf("шаги порога панели 6 = %+v, ожидались [{nil red} {0 green}]", steps)
	}
}

// hasThresholdStep ищет шаг порога по значению: пороги задаются панельными thresholds,
// а не override'ом — на панели один смысловой порог.
func hasThresholdStep(p *dashboard.Panel, value float64) bool {
	thresholds := p.FieldConfig.Defaults.Thresholds
	if thresholds == nil {
		return false
	}
	for _, step := range thresholds.Steps {
		if step.Value != nil && *step.Value == value {
			return true
		}
	}
	return false
}

// TestKeyRateGenerateDeterministic — как TestGenerateDeterministic в gold: на детерминизме
// генерации стоит CI-дифф артефакта, поэтому две сборки обязаны дать байт-идентичный JSON.
func TestKeyRateGenerateDeterministic(t *testing.T) {
	d1, err := NewKeyRatePublications().Build()
	if err != nil {
		t.Fatal(err)
	}
	d2, err := NewKeyRatePublications().Build()
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := json.MarshalIndent(Manifest(d1), "", "  ")
	b2, _ := json.MarshalIndent(Manifest(d2), "", "  ")
	if !bytes.Equal(b1, b2) {
		t.Fatal("генерация недетерминирована")
	}
}

// TestKeyRateYoYPanelsFilterOutsideWindow: YoY панелей 6 (М2 и кредит) и 9-B (зарплата) считается
// по ПОЛНОМУ ряду, а $__timeFilter стоит после закрывающей скобки подзапроса — иначе первые 12
// точек были бы посчитаны по обрезанному окну. Делитель обёрнут в nullIf: в первых 12 строках
// lagInFrame даёт 0, и без него YoY = +Inf (широкое окно ломало бы ось). Образец — золотой
// TestYoYPanelsFilterOutsideWindow; панель 9-B адресуется по refId: безработица (A) — уровень,
// а не темп, и подзапросу с лагом там не место.
func TestKeyRateYoYPanelsFilterOutsideWindow(t *testing.T) {
	board := builtKeyRatePublications(t)
	for _, tc := range []struct {
		panelID uint32
		refIDs  []string
	}{
		{6, []string{"A", "B"}},
		{9, []string{"B"}},
	} {
		panel := panelByID(t, board, tc.panelID)
		for _, refID := range tc.refIDs {
			query := targetByRefID(t, panel, refID)
			if got := strings.Count(query.RawSql, "$__timeFilter"); got != 1 {
				t.Fatalf("панель %d: запрос %q: $__timeFilter встречается %d раз, ожидался 1 (только снаружи)", tc.panelID, refID, got)
			}
			if !strings.Contains(query.RawSql, "$__timeFilter(time)") {
				t.Fatalf("панель %d: запрос %q фильтрует время не по внешнему time: %s", tc.panelID, refID, query.RawSql)
			}
			if !strings.Contains(query.RawSql, "nullIf(lagInFrame(value, 12) OVER (ORDER BY date), 0)") {
				t.Fatalf("панель %d: запрос %q не защищён от деления на ноль в первых 12 строках (нужен nullIf): %s", tc.panelID, refID, query.RawSql)
			}
			// Скобку подзапроса ищем вместе с фильтром: у самого $__timeFilter тоже есть ")".
			if !strings.Contains(query.RawSql, ")\nWHERE $__timeFilter(time)") {
				t.Fatalf("панель %d: запрос %q фильтрует окно внутри подзапроса, а не после его закрывающей скобки: %s", tc.panelID, refID, query.RawSql)
			}
		}
	}
}

// targetByRefID возвращает запрос панели по refId: тесты YoY адресуют конкретную серию
// (панель 9 — только B), а не «все таргеты панели».
func targetByRefID(t *testing.T, p *dashboard.Panel, refID string) CHQuery {
	t.Helper()
	for _, target := range p.Targets {
		query, ok := chQuery(target)
		if !ok {
			t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(p))
		}
		if query.RefId == refID {
			return query
		}
	}
	t.Fatalf("панель %q: запроса с refId %q нет", panelTitle(p), refID)
	return CHQuery{}
}

// TestKeyRateBudgetPanelNamesConsolidatedBudget: панель 8 читает minfin_mesyats — это
// консолидированный бюджет РФ и государственных внебюджетных фондов, а не федеральный
// (федеральный — source = 'minfin', и отдельного ряда его дефицита в контуре пока нет — фаза 2).
// Заголовок и Description обязаны называть охват: без этого числа прочтут как федеральный бюджет.
func TestKeyRateBudgetPanelNamesConsolidatedBudget(t *testing.T) {
	panel := panelByID(t, builtKeyRatePublications(t), 8)
	title := optionalString(panel.Title)
	if !strings.Contains(title, "Консолидированный") {
		t.Fatalf("заголовок панели 8 %q не называет охват (консолидированный бюджет)", title)
	}
	description := optionalString(panel.Description)
	for _, want := range []string{"minfin_mesyats", "федеральн", "дефицит", "контуре", "млрд руб."} {
		if !strings.Contains(description, want) {
			t.Fatalf("Description панели 8 не содержит %q: %s", want, description)
		}
	}
}

// assertPointsOnly проверяет оба свойства «серия рисуется только точками» (pointsOnlyProps золотого
// дашборда): custom.lineWidth 0 и custom.showPoints always. Свойства ищутся в override'е конкретной
// серии: опечатка в refId молча оставила бы серию сплошной линией по одной точке.
func assertPointsOnly(t *testing.T, p *dashboard.Panel, refID string) {
	t.Helper()
	if got := overridePropByRefID(p, refID, "custom.lineWidth"); got != 0 {
		t.Fatalf("панель %q: custom.lineWidth серии %s = %v, ожидался 0 (серия из одной точки — не линия)", panelTitle(p), refID, got)
	}
	if got := overridePropByRefID(p, refID, "custom.showPoints"); got != "always" {
		t.Fatalf("панель %q: custom.showPoints серии %s = %v, ожидалось \"always\"", panelTitle(p), refID, got)
	}
}

// TestKeyRateTrajectoryUsesKnownSecids: панель 11 — три известные серии KEYRATE (KKV6, KKZ6, KKG7),
// у каждой свой secid в предикате. Список серий точечно обновляется при появлении новых (переменные
// темплейтинга — вне v1); пропущенный или перепутанный secid оставил бы серию молча пустой.
func TestKeyRateTrajectoryUsesKnownSecids(t *testing.T) {
	panel := panelByID(t, builtKeyRatePublications(t), 11)
	if len(panel.Targets) != 3 {
		t.Fatalf("у панели %q %d запросов, ожидалось 3 (KKV6, KKZ6, KKG7)", panelTitle(panel), len(panel.Targets))
	}
	for _, tc := range []struct{ refID, secid string }{{"A", "KKV6"}, {"B", "KKZ6"}, {"C", "KKG7"}} {
		query := targetByRefID(t, panel, tc.refID)
		for _, want := range []string{
			"FROM v_keyrate_expectations", "secid = '" + tc.secid + "'", "$__timeFilter(date)", "ORDER BY date",
		} {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель 11: запрос %q не содержит %q: %s", tc.refID, want, query.RawSql)
			}
		}
	}
}

// TestKeyRateSparseSeriesPoints: разрежённые серии рисуются точками, а не линией — панель 11
// (KKG7: дальняя серия без сделок, одно наблюдение) и панель 13 (теноры кривой ОФЗ — снимок по одной
// точке, история наполняется). Сплошная линия по единственной точке не читается. У KKG7 ещё и
// фиксированный цвет: цвет точечной серии не должен зависеть от порядка таргетов в палитре.
func TestKeyRateSparseSeriesPoints(t *testing.T) {
	board := builtKeyRatePublications(t)
	for _, tc := range []struct {
		panelID uint32
		refIDs  []string
	}{
		{11, []string{"C"}},
		{13, []string{"B", "C", "D", "E"}},
	} {
		panel := panelByID(t, board, tc.panelID)
		for _, refID := range tc.refIDs {
			assertPointsOnly(t, panel, refID)
		}
	}
	color, ok := overridePropByRefID(panelByID(t, board, 11), "C", "color").(dashboard.FieldColor)
	if !ok || color.Mode != dashboard.FieldColorModeIdFixed || color.FixedColor == nil {
		t.Fatalf("панель 11: цвет серии C = %#v, ожидался fixed (палитра classic красит по порядку таргетов)", color)
	}
}

// TestKeyRateCurveCoversFourTenors: панель 13 — RGBI (A) и четыре тенора кривой (B..E) с алиасами
// «1y, %» и т.д.: алиас — единственное, чем серии одного вида отличаются в легенде (без него было бы
// четыре одинаковых «yield»), а тенор без алиаса молча терял бы подпись.
func TestKeyRateCurveCoversFourTenors(t *testing.T) {
	panel := panelByID(t, builtKeyRatePublications(t), 13)
	if len(panel.Targets) != 5 {
		t.Fatalf("у панели %q %d запросов, ожидалось 5 (RGBI и четыре тенора)", panelTitle(panel), len(panel.Targets))
	}
	rgbi := targetByRefID(t, panel, "A")
	for _, want := range []string{"FROM v_ofz_curve", "tenor = 'RGBI'", "$__timeFilter(date)", "ORDER BY date"} {
		if !strings.Contains(rgbi.RawSql, want) {
			t.Fatalf("панель 13: запрос A не содержит %q: %s", want, rgbi.RawSql)
		}
	}
	for _, tc := range []struct{ refID, tenor string }{{"B", "1y"}, {"C", "3y"}, {"D", "5y"}, {"E", "10y"}} {
		query := targetByRefID(t, panel, tc.refID)
		for _, want := range []string{"tenor = '" + tc.tenor + "'", `AS "` + tc.tenor + `, %"`} {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель 13: запрос %q не содержит %q: %s", tc.refID, want, query.RawSql)
			}
		}
	}
}

// TestKeyRateSnapshotIsTableQuery: панель 12 — таблица: плагину нужен queryType "table" и формат 1,
// иначе строковая колонка «Серия» и построчный срез уехали бы в timeseries-формат. Колонки — ровно
// из спеки, и значение стоит рядом с ликвидностью: вменённая ставка без объёма читается как
// торгуемая, а дальняя KKG7 (объём 0) — как рыночная; срез берётся подзапросом по max(date).
func TestKeyRateSnapshotIsTableQuery(t *testing.T) {
	panel := panelByID(t, builtKeyRatePublications(t), 12)
	if len(panel.Targets) != 1 {
		t.Fatalf("у панели %q %d запросов, ожидался 1 (срез витрины одним подзапросом)", panelTitle(panel), len(panel.Targets))
	}
	query, ok := chQuery(panel.Targets[0])
	if !ok {
		t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(panel))
	}
	if query.Format != queryFormatTable || query.QueryType != queryTypeTable {
		t.Fatalf("панель 12: format=%d queryType=%q, ожидались %d/%q", query.Format, query.QueryType, queryFormatTable, queryTypeTable)
	}
	for _, want := range []string{
		`series_name AS "Серия"`,
		`decision_date AS "Заседание"`,
		`settle AS "Вменённая ставка, %"`,
		`expected_change_bp AS "Ожидаемый шаг, б.п."`,
		`volume AS "Объём"`,
		`openposition AS "Открытый интерес"`,
		"WHERE date = (SELECT max(date) FROM v_keyrate_expectations)",
		"ORDER BY decision_date",
	} {
		if !strings.Contains(query.RawSql, want) {
			t.Fatalf("панель 12: SQL не содержит %q: %s", want, query.RawSql)
		}
	}
}

// TestKeyRateRuoniaSpreadSelfJoin: спред RUONIA к ключевой считается в SQL самоджойном витрины по
// дате: RUONIA дневная, ключевая меняется решениями, и клиентское вычитание двух серий дало бы спред
// к ставке другой даты. Знак спреда подписан в Description (минус — рынок ниже ключевой).
func TestKeyRateRuoniaSpreadSelfJoin(t *testing.T) {
	panel := panelByID(t, builtKeyRatePublications(t), 14)
	spread := targetByRefID(t, panel, "C")
	for _, want := range []string{
		"INNER JOIN v_cbr_macro AS k ON k.date = r.date",
		"k.series = 'cbr_key_rate'",
		"r.series = 'cbr_ruania_ruo'",
		"r.value - k.value",
		"$__timeFilter(r.date)",
		"ORDER BY r.date",
	} {
		if !strings.Contains(spread.RawSql, want) {
			t.Fatalf("панель 14: запрос C не содержит %q: %s", want, spread.RawSql)
		}
	}
	description := optionalString(panel.Description)
	for _, want := range []string{"п.п.", "минус"} {
		if !strings.Contains(description, want) {
			t.Fatalf("Description панели 14 не объясняет знак и единицу спреда (%q нет): %s", want, description)
		}
	}
}

// TestKeyRateNoLimitOnTimeseries: LIMIT на временном ряду — анти-паттерн UI-экспорта: он молча
// обрезает историю, и «последние N точек» выглядят как весь ряд. LIMIT допустим только у таблиц
// (панели 16/18/19 — размер карточки); временным рядам он не нужен: срез панели 12 берёт последнюю
// дату подзапросом max(date), каналы 17 — последнее заседание подзапросом max(meeting_date).
func TestKeyRateNoLimitOnTimeseries(t *testing.T) {
	for _, p := range panelsOf(builtKeyRatePublications(t)) {
		for _, target := range p.Targets {
			query, ok := chQuery(target)
			if !ok {
				t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(p))
			}
			if query.QueryType != queryTypeTimeseries {
				continue
			}
			if strings.Contains(query.RawSql, "LIMIT") {
				t.Fatalf("панель %q: у таргета %q LIMIT в timeseries-запросе: %s", panelTitle(p), query.RefId, query.RawSql)
			}
		}
	}
}

// TestKeyRateConsensusPanelTargets: панель 15 — шесть рядов v_cbr_mo_br одной формы: медианы
// ключевой ставки и инфляции на 2026/2027 плюс интервал 2027 (p10/p90). Алиасы — дословно
// из спеки §5: по ним серии различаются в легенде (иначе было бы шесть одинаковых «value»),
// а код ряда — единственная связь с витриной, и опечатка оставила бы серию молча пустой.
// Цвета зафиксированы у всех шести серий: без этого раскладку перекрашивал бы порядок таргетов
// в палитре classic, а ставка и инфляция двух горизонтов читались бы как четыре безымянных кривых.
func TestKeyRateConsensusPanelTargets(t *testing.T) {
	panel := panelByID(t, builtKeyRatePublications(t), 15)
	if len(panel.Targets) != 6 {
		t.Fatalf("у панели %q %d запросов, ожидалось 6 (медианы 2026/2027 и интервал 2027)", panelTitle(panel), len(panel.Targets))
	}
	if unit := panel.FieldConfig.Defaults.Unit; unit == nil || *unit != units.Percent {
		t.Fatalf("панель 15: unit %v, ожидался %q (консенсус — уровни, проценты годовых)", unit, units.Percent)
	}
	for _, tc := range []struct{ refID, series, alias string }{
		{"A", "key_rate_avg_2026_median", "Ключевая, 2026 (медиана)"},
		{"B", "key_rate_avg_2027_median", "Ключевая, 2027 (медиана)"},
		{"C", "cpi_avg_2026_median", "Инфляция, 2026 (медиана)"},
		{"D", "cpi_avg_2027_median", "Инфляция, 2027 (медиана)"},
		{"E", "key_rate_avg_2027_p10", "Интервал 2027, p10"},
		{"F", "key_rate_avg_2027_p90", "Интервал 2027, p90"},
	} {
		query := targetByRefID(t, panel, tc.refID)
		for _, want := range []string{
			"FROM v_cbr_mo_br",
			"series = '" + tc.series + "'",
			`AS "` + tc.alias + `"`,
			"$__timeFilter(date)",
			"ORDER BY date",
		} {
			if !strings.Contains(query.RawSql, want) {
				t.Fatalf("панель 15: запрос %q не содержит %q: %s", tc.refID, want, query.RawSql)
			}
		}
		color, ok := overridePropByRefID(panel, tc.refID, "color").(dashboard.FieldColor)
		if !ok || color.Mode != dashboard.FieldColorModeIdFixed || color.FixedColor == nil {
			t.Fatalf("панель 15: цвет серии %s = %#v, ожидался fixed (палитра classic красит по порядку таргетов)", tc.refID, color)
		}
	}
}

// TestKeyRateSignalRowUsesTableQueries: панели 16–19 — таблицы, и плагину нужен queryType "table"
// с форматом 1, иначе строковые колонки («Сигнал», «Фрагмент», «Событие») уехали бы в timeseries-формат.
// У каждой панели один таргет на своей витрине. LIMIT у таблиц — размер карточки, а не обрезка
// истории: 3 последних резюме (16), 10 ближайших заседаний (18), 15 свежих новостей (19);
// у каналов (17) LIMIT нет — последнее заседание выбирается подзапросом max(meeting_date).
func TestKeyRateSignalRowUsesTableQueries(t *testing.T) {
	board := builtKeyRatePublications(t)
	for _, tc := range []struct {
		panelID uint32
		view    string
		limit   string
	}{
		{16, "v_cbr_mp_summary", "LIMIT 3"},
		{17, "v_cbr_mp_summary_channels", ""},
		{18, "v_events_calendar", "LIMIT 10"},
		{19, "v_news_events", "LIMIT 15"},
	} {
		panel := panelByID(t, board, tc.panelID)
		if len(panel.Targets) != 1 {
			t.Fatalf("у панели %q %d запросов, ожидался 1", panelTitle(panel), len(panel.Targets))
		}
		query, ok := chQuery(panel.Targets[0])
		if !ok {
			t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(panel))
		}
		if query.Format != queryFormatTable || query.QueryType != queryTypeTable {
			t.Fatalf("панель %d: format=%d queryType=%q, ожидались %d/%q (строковые колонки в timeseries не укладываются)", tc.panelID, query.Format, query.QueryType, queryFormatTable, queryTypeTable)
		}
		if !strings.Contains(query.RawSql, "FROM "+tc.view) {
			t.Fatalf("панель %d: запрос читает не %s: %s", tc.panelID, tc.view, query.RawSql)
		}
		if tc.limit == "" {
			if strings.Contains(query.RawSql, "LIMIT") {
				t.Fatalf("панель %d: LIMIT в запросе каналов — последнее заседание берётся подзапросом max(): %s", tc.panelID, query.RawSql)
			}
			continue
		}
		if !strings.Contains(query.RawSql, tc.limit) {
			t.Fatalf("панель %d: запрос не содержит %q: %s", tc.panelID, tc.limit, query.RawSql)
		}
	}
}
