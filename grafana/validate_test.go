package main

import (
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/cog/variants"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
)

func TestValidateAcceptsGenerated(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(d); err != nil {
		t.Fatalf("сгенерированный дашборд не прошёл валидацию: %v", err)
	}
}

func TestValidateRejectsForeignDatasource(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}
	foreign := "bdx7lxrdelxq8b"
	firstPanel(t, &d).Datasource = &common.DataSourceRef{Type: ptr(DatasourceType), Uid: &foreign}
	if err := Validate(d); err == nil {
		t.Fatal("чужой UID датасорса не отвергнут")
	}
}

// TestGeneratedIDsAreUniqueAndSet фиксирует раскладку id: панели 1..9 (Task 4), ряды 11..15.
// Нулевой id у ряда (как в первой версии артефакта) Grafana трактует как «id не задан»,
// поэтому ряды нумеруются вне диапазона панелей.
func TestGeneratedIDsAreUniqueAndSet(t *testing.T) {
	d, err := NewGoldThesis().Build()
	if err != nil {
		t.Fatal(err)
	}

	seen := make(map[uint32]string)
	rows := 0
	for _, entry := range d.Panels {
		switch {
		case entry.Panel != nil:
			if entry.Panel.Id == nil || *entry.Panel.Id == 0 {
				t.Fatalf("панель %q без id", panelTitle(entry.Panel))
			}
			label := "панель " + panelTitle(entry.Panel)
			if owner, duplicate := seen[*entry.Panel.Id]; duplicate {
				t.Fatalf("id %d повторяется: %s и %s", *entry.Panel.Id, owner, label)
			}
			seen[*entry.Panel.Id] = label
		case entry.RowPanel != nil:
			rows++
			if entry.RowPanel.Id == 0 {
				t.Fatalf("ряд %q без id", optionalString(entry.RowPanel.Title))
			}
			if entry.RowPanel.Id < 10 {
				t.Fatalf("ряд %q: id %d попадает в диапазон панелей 1..9", optionalString(entry.RowPanel.Title), entry.RowPanel.Id)
			}
			label := "ряд " + optionalString(entry.RowPanel.Title)
			if owner, duplicate := seen[entry.RowPanel.Id]; duplicate {
				t.Fatalf("id %d повторяется: %s и %s", entry.RowPanel.Id, owner, label)
			}
			seen[entry.RowPanel.Id] = label
		default:
			t.Fatal("элемент дашборда без панели и ряда")
		}
	}
	if rows != 5 {
		t.Fatalf("рядов %d, ожидалось 5: %v", rows, goldThesisRowTitles)
	}
}

// TestValidateRejects — таблица негативных правил Validate: каждое нарушение обязано вернуть ошибку.
func TestValidateRejects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, d *dashboard.Dashboard)
	}{
		{
			name: "чужой uid дашборда",
			mutate: func(_ *testing.T, d *dashboard.Dashboard) {
				d.Uid = ptr("foreign-dashboard")
			},
		},
		{
			name: "uid дашборда не задан",
			mutate: func(_ *testing.T, d *dashboard.Dashboard) {
				d.Uid = nil
			},
		},
		{
			name: "панелей нет",
			mutate: func(_ *testing.T, d *dashboard.Dashboard) {
				d.Panels = nil
			},
		},
		{
			name: "панель без датасорса",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				firstPanel(t, d).Datasource = nil
			},
		},
		{
			name: "неверный тип датасорса",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				firstPanel(t, d).Datasource = &common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(DatasourceUID)}
			},
		},
		{
			name: "панель без id",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				firstPanel(t, d).Id = nil
			},
		},
		{
			name: "нулевой id панели",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				firstPanel(t, d).Id = ptr(uint32(0))
			},
		},
		{
			name: "дубль id панели",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				panel, err := goldPricePanel().Build()
				if err != nil {
					t.Fatal(err)
				}
				d.Panels = append(d.Panels, dashboard.PanelOrRowPanel{Panel: &panel})
			},
		},
		{
			name: "нулевой id ряда",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				row, err := dashboard.NewRowBuilder("Ряд без id").Build()
				if err != nil {
					t.Fatal(err)
				}
				d.Panels = append(d.Panels, dashboard.PanelOrRowPanel{RowPanel: &row})
			},
		},
		{
			name: "дубль id ряда",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				row, err := dashboard.NewRowBuilder("Копия ряда").Id(11).Build()
				if err != nil {
					t.Fatal(err)
				}
				d.Panels = append(d.Panels, dashboard.PanelOrRowPanel{RowPanel: &row})
			},
		},
		{
			name: "у панели нет запросов",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				firstPanel(t, d).Targets = nil
			},
		},
		{
			name: "target не CHQuery",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				firstPanel(t, d).Targets = []variants.Dataquery{foreignQuery{}}
			},
		},
		{
			name: "rawSql пустой",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				firstPanel(t, d).Targets = []variants.Dataquery{CHQuery{RefId: "A"}}
			},
		},
		{
			name: "rawSql читает сырую таблицу",
			mutate: func(t *testing.T, d *dashboard.Dashboard) {
				query, err := NewCHQuery("A", "SELECT time, value FROM macro_series").Format(0).QueryType("timeseries").Build()
				if err != nil {
					t.Fatal(err)
				}
				firstPanel(t, d).Targets = []variants.Dataquery{query}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := NewGoldThesis().Build()
			if err != nil {
				t.Fatal(err)
			}
			tt.mutate(t, &d)
			if err := Validate(d); err == nil {
				t.Fatal("нарушение не отвергнуто")
			}
		})
	}
}

// firstPanel возвращает первую панель дашборда для негативных сценариев.
func firstPanel(t *testing.T, d *dashboard.Dashboard) *dashboard.Panel {
	t.Helper()
	panels := panelsOf(*d)
	if len(panels) == 0 {
		t.Fatal("в дашборде нет панелей")
	}
	return panels[0]
}

// foreignQuery — dataquery чужого плагина: Validate обязан отвергнуть такой target.
type foreignQuery struct{}

func (foreignQuery) ImplementsDataqueryVariant() {}

func (foreignQuery) Equals(variants.Dataquery) bool { return false }

func (foreignQuery) DataqueryType() string { return "prometheus" }

func (foreignQuery) Validate() error { return nil }
