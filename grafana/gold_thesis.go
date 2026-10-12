package main

import (
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/resource"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// goldPriceSQL — цена COMEX-фронта (витрина золота). Читаются только витрины v_*:
// у датасорса стенда нет грантов на сырые таблицы.
const goldPriceSQL = `SELECT
    date AS time,
    value AS "Цена, USD/oz"
FROM v_gold_prices
WHERE source = 'gold'
  AND series = 'comex_front'
  AND $__timeFilter(date)
ORDER BY date
`

// Id панелей дашборда — 1..9 (Task 4), рядов — 11..15: нулевой id Grafana трактует
// как «не задан», поэтому ряды нумеруются вне диапазона панелей.
const firstRowID = 11

// NewGoldThesis собирает дашборд «Золото — тезисы статьи» (§4 золотого роадмапа).
// Task 4 добавит остальные ряды и панели; здесь — ряд «Цена и пол» и панель 1.
func NewGoldThesis() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Золото — тезисы статьи").
		Uid(DashboardUID).
		Tags([]string{"gold", "article"}).
		Refresh("5m").
		Time("now-1y", "now").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Цена и пол").Id(firstRowID)).
		WithPanel(goldPricePanel())
}

// goldPricePanel — панель 1: цена золота и порог $4 000 (зелёный → красный).
// Id задан явно: Validate требует непустых уникальных id панелей (Task 4 нумерует их 1..9).
func goldPricePanel() *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Id(1).
		Title("Цена золота и пол $4 000").
		Datasource(common.DataSourceRef{Type: ptr(DatasourceType), Uid: ptr(DatasourceUID)}).
		WithTarget(NewCHQuery("A", goldPriceSQL).Format(0).QueryType("timeseries")).
		Thresholds(dashboard.NewThresholdsConfigBuilder().
			Mode(dashboard.ThresholdsModeAbsolute).
			Steps([]dashboard.Threshold{
				{Value: nil, Color: "green"},
				{Value: ptr(4000.0), Color: "red"},
			}))
}

// Manifest оборачивает дашборд в K8s-стиль ресурс, который принимает gcx resources push.
// metadata.name и spec.uid равны DashboardUID (инвариант проверяет Validate).
func Manifest(d dashboard.Dashboard) resource.Manifest {
	return resource.Manifest{
		ApiVersion: ApiVersion,
		Kind:       resource.DashboardKind,
		Metadata:   resource.Metadata{Name: DashboardUID},
		Spec:       d,
	}
}

// ptr возвращает указатель на значение: необязательные поля builders SDK — указатели.
func ptr[T any](v T) *T {
	return &v
}
