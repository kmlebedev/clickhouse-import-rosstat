package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// keyRateRowTitles — ряды дашборда в порядке следования (id 21–24). Готовы ряды 21–22;
// список растёт вместе с панелями (T3–T4).
var keyRateRowTitles = []string{
	"Ставка и инфляция",
	"Деньги, кредит, бюджет, труд",
}

func builtKeyRatePublications(t *testing.T) dashboard.Dashboard {
	t.Helper()
	d, err := NewKeyRatePublications().Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestKeyRatePublicationsPanelCount фиксирует состав дашборда: собраны ряд 21 (панели 1–5)
// и ряд 22 (панели 6–10) — 10 панелей (растёт до 19 в T3–T4).
func TestKeyRatePublicationsPanelCount(t *testing.T) {
	if panels := panelsOf(builtKeyRatePublications(t)); len(panels) != 10 {
		t.Fatalf("панелей %d, ожидалось 10", len(panels))
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
// override'ом, там же правая ось (шкалы безработицы и роста зарплаты несопоставимы).
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
