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
//   - id заданы, непусты и уникальны у всех элементов верхнего уровня (панели и ряды),
//     а также у панелей, вложенных в свёрнутые ряды;
//   - у каждой панели датасорс — константы DatasourceType/DatasourceUID;
//   - каждый запрос — CHQuery с непустым rawSql, читающим витрину v_*.
func Validate(d dashboard.Dashboard) error {
	if d.Uid == nil {
		return errors.New("у дашборда не задан uid")
	}
	if *d.Uid != DashboardUID {
		return fmt.Errorf("uid дашборда %q, ожидался %q", *d.Uid, DashboardUID)
	}
	if err := validateIDs(d); err != nil {
		return err
	}

	panels := panelsOf(d)
	if len(panels) == 0 {
		return errors.New("в дашборде нет панелей")
	}
	for _, p := range panels {
		if err := validateDatasource(p); err != nil {
			return err
		}
		if err := validateTargets(p); err != nil {
			return err
		}
	}

	return nil
}

// validateIDs требует непустых уникальных id у панелей и рядов: ряд — такой же элемент
// дашборда, и нулевой id Grafana заменит сама (артефакт разойдётся с кодом).
func validateIDs(d dashboard.Dashboard) error {
	ids := make(map[uint32]string, len(d.Panels))
	for _, entry := range d.Panels {
		if entry.Panel != nil {
			if err := claimPanelID(ids, entry.Panel); err != nil {
				return err
			}
		}
		if entry.RowPanel != nil {
			if err := claimID(ids, entry.RowPanel.Id, fmt.Sprintf("ряд %q", optionalString(entry.RowPanel.Title))); err != nil {
				return err
			}
			for i := range entry.RowPanel.Panels {
				if err := claimPanelID(ids, &entry.RowPanel.Panels[i]); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func claimPanelID(ids map[uint32]string, p *dashboard.Panel) error {
	if p.Id == nil {
		return fmt.Errorf("у панели %q не задан id", panelTitle(p))
	}
	return claimID(ids, *p.Id, fmt.Sprintf("панель %q", panelTitle(p)))
}

func claimID(ids map[uint32]string, id uint32, label string) error {
	if id == 0 {
		return fmt.Errorf("у %s нулевой id", label)
	}
	if owner, duplicate := ids[id]; duplicate {
		return fmt.Errorf("id %d повторяется: %s и %s", id, owner, label)
	}
	ids[id] = label
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
