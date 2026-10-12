package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/grafana/grafana-foundation-sdk/go/cog/variants"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
)

// Validate проверяет дашборд по правилам контура:
//   - uid дашборда равен DashboardUID;
//   - у каждой панели датасорс — константы DatasourceType/DatasourceUID;
//   - id панелей заданы, непусты и уникальны;
//   - каждый запрос — CHQuery с непустым rawSql, читающим витрину v_*.
//
// Панели, вложенные в свёрнутый ряд, проверяются наравне с верхнеуровневыми.
func Validate(d dashboard.Dashboard) error {
	if d.Uid == nil {
		return errors.New("у дашборда не задан uid")
	}
	if *d.Uid != DashboardUID {
		return fmt.Errorf("uid дашборда %q, ожидался %q", *d.Uid, DashboardUID)
	}

	panels := panelsOf(d)
	if len(panels) == 0 {
		return errors.New("в дашборде нет панелей")
	}

	ids := make(map[uint32]struct{}, len(panels))
	for _, p := range panels {
		if err := validateDatasource(p); err != nil {
			return err
		}
		if err := validateTargets(p); err != nil {
			return err
		}
		if p.Id == nil || *p.Id == 0 {
			return fmt.Errorf("у панели %q не задан id", panelTitle(p))
		}
		if _, duplicate := ids[*p.Id]; duplicate {
			return fmt.Errorf("id панели %q (%d) повторяется", panelTitle(p), *p.Id)
		}
		ids[*p.Id] = struct{}{}
	}

	return nil
}

// panelsOf перечисляет панели дашборда в порядке следования, пропуская ряды.
func panelsOf(d dashboard.Dashboard) []*dashboard.Panel {
	var panels []*dashboard.Panel
	for _, entry := range d.Panels {
		if entry.Panel != nil {
			panels = append(panels, entry.Panel)
		}
		if entry.RowPanel != nil {
			for i := range entry.RowPanel.Panels {
				panels = append(panels, &entry.RowPanel.Panels[i])
			}
		}
	}
	return panels
}

func validateDatasource(p *dashboard.Panel) error {
	if p.Datasource == nil {
		return fmt.Errorf("у панели %q не задан датасорс", panelTitle(p))
	}
	if p.Datasource.Type == nil || *p.Datasource.Type != DatasourceType {
		return fmt.Errorf("у панели %q тип датасорса %s, ожидался %q", panelTitle(p), optionalString(p.Datasource.Type), DatasourceType)
	}
	if p.Datasource.Uid == nil || *p.Datasource.Uid != DatasourceUID {
		return fmt.Errorf("у панели %q uid датасорса %s, ожидался %q", panelTitle(p), optionalString(p.Datasource.Uid), DatasourceUID)
	}
	return nil
}

func validateTargets(p *dashboard.Panel) error {
	if len(p.Targets) == 0 {
		return fmt.Errorf("у панели %q нет запросов", panelTitle(p))
	}
	for i, target := range p.Targets {
		query, ok := chQuery(target)
		if !ok {
			return fmt.Errorf("панель %q: запрос %d не является запросом ClickHouse", panelTitle(p), i+1)
		}
		if query.RawSql == "" {
			return fmt.Errorf("панель %q: у запроса %q пустой rawSql", panelTitle(p), query.RefId)
		}
		if !strings.Contains(query.RawSql, "FROM v_") {
			return fmt.Errorf("панель %q: запрос %q читает не витрину v_*", panelTitle(p), query.RefId)
		}
	}
	return nil
}

// chQuery приводит target к CHQuery: билдер отдаёт значение, разбор JSON плагина — указатель.
func chQuery(target variants.Dataquery) (CHQuery, bool) {
	switch query := target.(type) {
	case CHQuery:
		return query, true
	case *CHQuery:
		if query == nil {
			return CHQuery{}, false
		}
		return *query, true
	default:
		return CHQuery{}, false
	}
}

func panelTitle(p *dashboard.Panel) string {
	return optionalString(p.Title)
}

func optionalString(value *string) string {
	if value == nil {
		return "<не задано>"
	}
	return *value
}
