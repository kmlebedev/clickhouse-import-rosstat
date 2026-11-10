package main

// Общие тестовые хелперы пакета: сборка дашборда и поиск панелей, таргетов и override-свойств.
// Хелпер, нужный тестам одного дашборда, живёт в его *_test.go.

import (
	"encoding/json"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// mustBuild собирает дашборд из builder'а и валит тест при ошибке сборки.
func mustBuild(t *testing.T, builder *dashboard.DashboardBuilder) dashboard.Dashboard {
	t.Helper()
	d, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
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

// TestZeroThresholds — порог по нулю: цвет ниже нуля задаёт вызывающий, от нуля — зелёный.
func TestZeroThresholds(t *testing.T) {
	cfg, err := zeroThresholds("blue").Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != dashboard.ThresholdsModeAbsolute {
		t.Fatalf("mode = %q, ожидался absolute", cfg.Mode)
	}
	if len(cfg.Steps) != 2 {
		t.Fatalf("шагов %d, ожидалось 2", len(cfg.Steps))
	}
	if cfg.Steps[0].Value != nil || cfg.Steps[0].Color != "blue" {
		t.Fatalf("базовый шаг = %+v, ожидался {nil blue}", cfg.Steps[0])
	}
	if cfg.Steps[1].Value == nil || *cfg.Steps[1].Value != 0 || cfg.Steps[1].Color != "green" {
		t.Fatalf("шаг нуля = %+v, ожидался {0 green}", cfg.Steps[1])
	}
}

// TestTimeseriesDefaults — общая база графиков: датасорс стенда, расчёты легенды из аргумента,
// линия 1 px без заливки и соединения пропусков.
func TestTimeseriesDefaults(t *testing.T) {
	p, err := timeseriesDefaults(timeseries.NewPanelBuilder(), "sum", "max").Build()
	if err != nil {
		t.Fatal(err)
	}
	if p.Datasource == nil || p.Datasource.Uid == nil || *p.Datasource.Uid != DatasourceUID {
		t.Fatalf("датасорс = %+v, ожидался uid %q", p.Datasource, DatasourceUID)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Options struct {
			Legend struct {
				Calcs       []string `json:"calcs"`
				DisplayMode string   `json:"displayMode"`
				Placement   string   `json:"placement"`
			} `json:"legend"`
		} `json:"options"`
		FieldConfig struct {
			Defaults struct {
				Custom struct {
					LineWidth   float64 `json:"lineWidth"`
					FillOpacity float64 `json:"fillOpacity"`
					SpanNulls   bool    `json:"spanNulls"`
				} `json:"custom"`
			} `json:"defaults"`
		} `json:"fieldConfig"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}
	legend := parsed.Options.Legend
	if len(legend.Calcs) != 2 || legend.Calcs[0] != "sum" || legend.Calcs[1] != "max" {
		t.Fatalf("calcs легенды = %v, ожидались [sum max]", legend.Calcs)
	}
	if legend.DisplayMode != "table" || legend.Placement != "bottom" {
		t.Fatalf("легенда = %s/%s, ожидалась table/bottom", legend.DisplayMode, legend.Placement)
	}
	custom := parsed.FieldConfig.Defaults.Custom
	if custom.LineWidth != 1 || custom.FillOpacity != 0 || custom.SpanNulls {
		t.Fatalf("custom = %+v, ожидались lineWidth 1, fillOpacity 0, spanNulls false", custom)
	}
}
