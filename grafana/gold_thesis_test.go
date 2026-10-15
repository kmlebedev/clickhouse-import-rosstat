package main

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/resource"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// goldThesisPanelTitles — состав панелей Task 4 в порядке следования (он же порядок id 1..9).
var goldThesisPanelTitles = []string{
	"Цена золота и пол $4 000",
	"Золото vs DXY",
	"Золото vs DFII10",
	"Crack ULSD и запасы дистиллятов",
	"CPI/PPI отпечатки, YoY",
	"ETF-потоки по месяцам, мир (bar)",
	"ЦБ: покупки YTD, т (bar)",
	"Консенсус Q4-2026 и own-сетка (bar)",
	"Точность прошлых прогнозов (table)",
}

// goldThesisRowTitles — ряды дашборда в порядке следования (id 11..15).
var goldThesisRowTitles = []string{
	"Цена и пол",
	"Дизельный канал",
	"Инфляция",
	"Спрос: ETF и ЦБ",
	"Прогноз и точность",
}

func builtGoldThesis(t *testing.T) dashboard.Dashboard {
	t.Helper()
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestGoldThesisHasNinePanels считает панели через panelsOf: ряды — такие же элементы
// d.Panels, и len(d.Panels) вернул бы 9 панелей + 5 рядов.
func TestGoldThesisHasNinePanels(t *testing.T) {
	panels := panelsOf(builtGoldThesis(t))
	if len(panels) != 9 {
		t.Fatalf("панелей %d, ожидалось 9", len(panels))
	}
	for i, p := range panels {
		if got := panelTitle(p); got != goldThesisPanelTitles[i] {
			t.Fatalf("панель %d: заголовок %q, ожидался %q", i+1, got, goldThesisPanelTitles[i])
		}
	}
}

// TestPanelIDsAreUniqueAndSet фиксирует ручную нумерацию панелей: id 1..9, каждый ровно раз
// (SDK id не присваивает, а нулевой Grafana перезапишет своими).
func TestPanelIDsAreUniqueAndSet(t *testing.T) {
	seen := make(map[uint32]string, 9)
	for _, p := range panelsOf(builtGoldThesis(t)) {
		if p.Id == nil {
			t.Fatalf("панель %q без id", panelTitle(p))
		}
		id := *p.Id
		if id < 1 || id > 9 {
			t.Fatalf("панель %q: id %d вне диапазона 1..9", panelTitle(p), id)
		}
		if owner, duplicate := seen[id]; duplicate {
			t.Fatalf("id %d повторяется: %s и %s", id, owner, panelTitle(p))
		}
		seen[id] = panelTitle(p)
	}
	for id := uint32(1); id <= 9; id++ {
		if _, ok := seen[id]; !ok {
			t.Fatalf("панели с id %d нет", id)
		}
	}
}

// TestAllTargetsUseViews: датасорс стенда не имеет грантов на сырые таблицы, поэтому
// каждый запрос каждой панели обязан читать витрину v_*.
func TestAllTargetsUseViews(t *testing.T) {
	for _, p := range panelsOf(builtGoldThesis(t)) {
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

// TestGoldThesisRowTitles фиксирует ряды и их id: ряды нумеруются вне диапазона панелей.
func TestGoldThesisRowTitles(t *testing.T) {
	var titles []string
	for _, entry := range builtGoldThesis(t).Panels {
		if entry.RowPanel == nil {
			continue
		}
		if entry.RowPanel.Id < 10 {
			t.Fatalf("ряд %q: id %d попадает в диапазон панелей 1..9", optionalString(entry.RowPanel.Title), entry.RowPanel.Id)
		}
		titles = append(titles, optionalString(entry.RowPanel.Title))
	}
	if len(titles) != len(goldThesisRowTitles) {
		t.Fatalf("рядов %d, ожидалось %d: %v", len(titles), len(goldThesisRowTitles), titles)
	}
	for i, got := range titles {
		if got != goldThesisRowTitles[i] {
			t.Fatalf("ряд %d: %q, ожидался %q", i+1, got, goldThesisRowTitles[i])
		}
	}
}

// TestPricePanelShowsThresholdLine: без custom.thresholdsStyle Grafana не рисует линию порога
// (по умолчанию стиль off), а линия $4 000 — часть приёмки панели 1.
func TestPricePanelShowsThresholdLine(t *testing.T) {
	mode := thresholdStyleMode(t, panelByID(t, builtGoldThesis(t), 1))
	if mode != "line" {
		t.Fatalf("у панели 1 thresholdsStyle.mode=%q, ожидался \"line\" (линия порога $4 000)", mode)
	}
}

// TestYoYPanelsFilterOutsideWindow: YoY панели 5 считается по полному ряду, а фильтр времени
// применяется снаружи — иначе первые 12 точек были бы посчитаны по обрезанному окну.
// Делитель обёрнут в nullIf: в первых 12 строках lagInFrame даёт 0, и без этого YoY = +Inf.
func TestYoYPanelsFilterOutsideWindow(t *testing.T) {
	panel := panelByID(t, builtGoldThesis(t), 5)
	if len(panel.Targets) != 2 {
		t.Fatalf("у панели %q %d запросов, ожидалось 2 (CPI и PPI)", panelTitle(panel), len(panel.Targets))
	}
	for _, target := range panel.Targets {
		query, ok := chQuery(target)
		if !ok {
			t.Fatalf("панель %q: запрос не является запросом ClickHouse", panelTitle(panel))
		}
		if got := strings.Count(query.RawSql, "$__timeFilter"); got != 1 {
			t.Fatalf("панель %q: запрос %q: $__timeFilter встречается %d раз, ожидался 1 (только снаружи)", panelTitle(panel), query.RefId, got)
		}
		if !strings.Contains(query.RawSql, "$__timeFilter(time)") {
			t.Fatalf("панель %q: запрос %q фильтрует время не по внешнему time: %s", panelTitle(panel), query.RefId, query.RawSql)
		}
		if !strings.Contains(query.RawSql, "nullIf(lagInFrame(value, 12) OVER (ORDER BY date), 0)") {
			t.Fatalf("панель %q: запрос %q не защищён от деления на ноль в первых 12 строках (нужен nullIf)", panelTitle(panel), query.RefId)
		}
	}
}

// TestForecastAccuracyUnitScopedToErrorColumn: unit percent нужен колонке ошибки, а панельный
// default задел бы predicted/actual (USD/oz) из той же таблицы. Поэтому единица идёт override'ом
// по имени колонки — и имя в override обязано совпадать с алиасом в SQL, иначе override молча
// не сработает: matcher byName сопоставляется с именем поля, которое даёт алиас.
func TestForecastAccuracyUnitScopedToErrorColumn(t *testing.T) {
	panel := panelByID(t, builtGoldThesis(t), 9)
	if unit := panel.FieldConfig.Defaults.Unit; unit != nil {
		t.Fatalf("панельный unit=%q: percent унаследовали бы predicted/actual (USD/oz)", *unit)
	}
	overrides := panel.FieldConfig.Overrides
	if len(overrides) != 1 {
		t.Fatalf("override'ов %d, ожидался 1 (unit колонки ошибки)", len(overrides))
	}
	matcher := overrides[0].Matcher
	name, ok := matcher.Options.(*string)
	if matcher.Id != "byName" || !ok || name == nil || *name != errorColumnAlias {
		t.Fatalf("override %+v: ожидался byName по %q", matcher, errorColumnAlias)
	}
	properties := overrides[0].Properties
	if len(properties) != 1 || properties[0].Id != "unit" || properties[0].Value != units.Percent {
		t.Fatalf("свойства override'а %+v, ожидалось unit=%q", properties, units.Percent)
	}
	query, ok := chQuery(panel.Targets[0])
	if !ok {
		t.Fatal("запрос панели 9 не является запросом ClickHouse")
	}
	if !strings.Contains(query.RawSql, "error_pct AS "+strconv.Quote(errorColumnAlias)) {
		t.Fatalf("в SQL нет алиаса %q — override не сработает: %s", errorColumnAlias, query.RawSql)
	}
}

// panelByID возвращает панель по id: нумерация задаётся вручную, уникальность держит Validate.
func panelByID(t *testing.T, d dashboard.Dashboard, id uint32) *dashboard.Panel {
	t.Helper()
	for _, p := range panelsOf(d) {
		if p.Id != nil && *p.Id == id {
			return p
		}
	}
	t.Fatalf("панель с id %d не найдена", id)
	return nil
}

// thresholdStyleMode читает custom.thresholdsStyle.mode из JSON панели: тест проверяет то,
// что реально уедет в артефакт, а не Go-структуру (поле Custom у FieldConfig — any).
func thresholdStyleMode(t *testing.T, p *dashboard.Panel) string {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		FieldConfig struct {
			Defaults struct {
				Custom struct {
					ThresholdsStyle struct {
						Mode string `json:"mode"`
					} `json:"thresholdsStyle"`
				} `json:"custom"`
			} `json:"defaults"`
		} `json:"fieldConfig"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}
	return parsed.FieldConfig.Defaults.Custom.ThresholdsStyle.Mode
}

func TestManifestWrapper(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	m := Manifest(d)
	if m.ApiVersion != ApiVersion || m.Kind != resource.DashboardKind || m.Metadata.Name != GoldThesisUID {
		t.Fatalf("обёртка: %+v", m)
	}
}

func TestPricePanelUsesDatasourceConstant(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	panels := panelsOf(d)
	if len(panels) == 0 {
		t.Fatal("панелей нет")
	}
	for _, p := range panels {
		if p.Datasource == nil || p.Datasource.Uid == nil || *p.Datasource.Uid != DatasourceUID {
			t.Fatalf("панель %q без константного UID датасорса", panelTitle(p))
		}
		if p.Datasource.Type == nil || *p.Datasource.Type != DatasourceType {
			t.Fatalf("панель %q: тип датасорса %v", panelTitle(p), p.Datasource.Type)
		}
	}
}

func TestGenerateDeterministic(t *testing.T) {
	d1, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	d2, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := json.MarshalIndent(Manifest(d1), "", "  ")
	b2, _ := json.MarshalIndent(Manifest(d2), "", "  ")
	if !bytes.Equal(b1, b2) {
		t.Fatal("генерация недетерминирована")
	}
}
