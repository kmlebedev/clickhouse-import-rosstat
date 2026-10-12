package main

import (
	"fmt"

	"github.com/grafana/grafana-foundation-sdk/go/barchart"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/resource"
	"github.com/grafana/grafana-foundation-sdk/go/table"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// Панели читают только витрины v_*: у датасорса стенда нет грантов на сырые таблицы.
// Форматы запросов подтверждены живым экспортом плагина 4.3.2 (grafana/clickhouse.go):
// 0 + timeseries — временные ряды (панели 1–6), 1 + table — категориальные бары и таблица (7–9).
const (
	queryFormatTimeseries = 0
	queryFormatTable      = 1
	queryTypeTimeseries   = "timeseries"
	queryTypeTable        = "table"
)

// Ширина панелей в колонках сетки Grafana (всего 24): ряд 1 — три панели по 8, остальные — по 12.
const (
	spanThird = 8
	spanHalf  = 12
)

// Id панелей дашборда — 1..9, рядов — 11..15: нулевой id Grafana трактует как «не задан»
// и заменит своим, поэтому ряды нумеруются вне диапазона панелей.
const firstRowID = 11

// goldPriceTemplate — единственное определение SQL цены COMEX-фронта. Панели 1–3 отличаются
// алиасом серии, «касания пола» — дополнительным фильтром по значению: подстановки %s.
const goldPriceTemplate = `SELECT
    date AS time,
    value AS %s
FROM v_gold_prices
WHERE source = 'gold'
  AND series = 'comex_front'
  AND $__timeFilter(date)
%sORDER BY date
`

// goldPriceSQL собирает SQL цены COMEX-фронта по шаблону (DRY: панели 1–3 переиспользуют его).
func goldPriceSQL(alias, extraFilter string) string {
	return fmt.Sprintf(goldPriceTemplate, alias, extraFilter)
}

// dxySQL — индекс доллара из витрины индикаторов. Фильтра времени нет намеренно: ряд сейчас
// из одной точки (история наполняется еженедельным вводом), окно прятало бы её.
const dxySQL = `SELECT
    as_of AS time,
    value AS "DXY"
FROM v_gold_dashboard
WHERE indicator = 'dxy'
`

// crackUlsdSQL — прокси крэк-спреда ULSD: как и dxy, одна точка без фильтра времени.
const crackUlsdSQL = `SELECT
    as_of AS time,
    value AS "Crack ULSD, USD/барр."
FROM v_gold_dashboard
WHERE indicator = 'crack_ulsd_proxy'
`

// dfii10SQL — реальная доходность TIPS (10 лет): вторая ось панели 3.
const dfii10SQL = `SELECT
    date AS time,
    value AS "DFII10, %"
FROM v_fred_macro
WHERE series = 'DFII10'
  AND $__timeFilter(date)
ORDER BY date
`

// distillateStocksSQL — запасы дистиллятов EIA: знаменатель дизельного канала.
const distillateStocksSQL = `SELECT
    date AS time,
    value AS "Запасы дистиллятов, тыс. барр."
FROM v_eia_petroleum
WHERE series = 'distillate_stocks'
  AND $__timeFilter(date)
ORDER BY date
`

// yoyTemplate — YoY по ПОЛНОМУ ряду, фильтр времени снаружи: если отфильтровать окно внутри,
// первые 12 точек считались бы по обрезанному ряду. Делитель обёрнут в nullIf: в первых
// 12 строках lagInFrame даёт 0, и без него YoY = +Inf (широкое окно ломало бы ось).
// %s — алиас и код ряда BLS.
const yoyTemplate = `SELECT
    time,
    yoy AS %s
FROM (
    SELECT
        date AS time,
        value / nullIf(lagInFrame(value, 12) OVER (ORDER BY date), 0) - 1 AS yoy
    FROM v_bls_macro
    WHERE series = '%s'
)
WHERE $__timeFilter(time)
ORDER BY time
`

// yoySQL собирает запрос YoY по шаблону: ряд и алиас — единственные различия панели 5.
func yoySQL(series, alias string) string {
	return fmt.Sprintf(yoyTemplate, alias, series)
}

// etfFlowsSQL — месячные потоки ETF (мир): время сворачивается до начала месяца.
const etfFlowsSQL = `SELECT
    toStartOfMonth(date) AS time,
    flow_usd_mln AS "Поток ETF, млн $"
FROM v_gold_etf_flows
WHERE region = 'world'
  AND $__timeFilter(date)
ORDER BY time
`

// cbPurchasesSQL — покупки ЦБ за год к дате: сортировка по значению даёт ранжированные бары.
const cbPurchasesSQL = `SELECT
    country AS "Страна",
    ytd_change_tonnes AS "YTD, т"
FROM v_cb_gold_dynamics
ORDER BY ytd_change_tonnes
`

// forecastConsensusSQL — консенсус на Q4-2026: бар по середине диапазона, low/high — справка.
const forecastConsensusSQL = `SELECT
    concat(author, ' / ', scenario) AS "Прогноз",
    midpoint AS "Середина, USD/oz",
    low,
    high
FROM v_gold_forecasts
WHERE horizon = 'Q4-2026'
ORDER BY midpoint
`

// forecastAccuracySQL — разрешённые прогнозы прошлого: таблица «обещали / вышло».
const forecastAccuracySQL = `SELECT
    metric, forecast_date, target_date, predicted, actual, error_pct
FROM v_forecast_accuracy
WHERE is_resolved = 1
ORDER BY target_date
`

// datasourceRef — ссылка на датасорс стенда: у всех панелей только эти константы (проверяет Validate).
func datasourceRef() common.DataSourceRef {
	return common.DataSourceRef{Type: ptr(DatasourceType), Uid: ptr(DatasourceUID)}
}

// timeseriesQuery — запрос временного ряда (формат 0, панели 1–6).
func timeseriesQuery(refID, sql string) *CHQueryBuilder {
	return NewCHQuery(refID, sql).Format(queryFormatTimeseries).QueryType(queryTypeTimeseries)
}

// tableQuery — категориальный/табличный запрос (формат 1, панели 7–9).
func tableQuery(refID, sql string) *CHQueryBuilder {
	return NewCHQuery(refID, sql).Format(queryFormatTable).QueryType(queryTypeTable)
}

// pointsOnlyProps — серия рисуется только точками: у dxy и crack по одной точке,
// сплошная линия по ним не читается (линия появится с накоплением истории).
func pointsOnlyProps() []dashboard.DynamicConfigValue {
	return []dashboard.DynamicConfigValue{
		{Id: "custom.lineWidth", Value: 0},
		{Id: "custom.showPoints", Value: "always"},
	}
}

// axisPlacementRightProp — серия на правой оси: у неё другая размерность (индекс, % или $/барр.).
func axisPlacementRightProp() dashboard.DynamicConfigValue {
	return dashboard.DynamicConfigValue{Id: "custom.axisPlacement", Value: "right"}
}

// rightAxisPointsProps — правая ось + только точки (серии dxy и crack).
func rightAxisPointsProps() []dashboard.DynamicConfigValue {
	return append([]dashboard.DynamicConfigValue{axisPlacementRightProp()}, pointsOnlyProps()...)
}

// crackProps — свойства серии B панели 4: правая ось, точки (см. pointsOnlyProps) и пороги
// $30/$50 линией: порог панели можно задать только override'ом отдельной серии.
func crackProps() []dashboard.DynamicConfigValue {
	props := rightAxisPointsProps()
	return append(props,
		dashboard.DynamicConfigValue{Id: "thresholds", Value: crackThresholds()},
		dashboard.DynamicConfigValue{Id: "custom.thresholdsStyle", Value: thresholdsStyle{Mode: "line"}},
	)
}

// thresholdsStyle — значение поля custom.thresholdsStyle плагина дашборда: у SDK своей
// структуры для него нет, а Grafana ждёт объект {"mode": ...}.
type thresholdsStyle struct {
	Mode string `json:"mode"`
}

// priceFloorThresholds — порог «пола» $4 000 панели 1 (steps: green → red).
func priceFloorThresholds() *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{
			{Value: nil, Color: "green"},
			{Value: ptr(4000.0), Color: "red"},
		})
}

// crackThresholds — пороги дизельного канала $30/$50 (steps: green → yellow → red).
func crackThresholds() dashboard.ThresholdsConfig {
	return dashboard.ThresholdsConfig{
		Mode: dashboard.ThresholdsModeAbsolute,
		Steps: []dashboard.Threshold{
			{Value: nil, Color: "green"},
			{Value: ptr(30.0), Color: "yellow"},
			{Value: ptr(50.0), Color: "red"},
		},
	}
}

// cbPurchaseThresholds — цвет баров ЦБ по знаку: продажи (минус) красные, покупки зелёные.
func cbPurchaseThresholds() *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{
			{Value: nil, Color: "red"},
			{Value: ptr(0.0), Color: "green"},
		})
}

// NewGoldThesis собирает дашборд «Золото — тезисы статьи» (§4 золотого роадмапа): 9 панелей
// в 5 рядах. Порядок вызовов WithRow/WithPanel задаёт и порядок сетки, и нумерацию панелей 1..9.
func NewGoldThesis() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Золото — тезисы статьи").
		Uid(DashboardUID).
		Tags([]string{"gold", "article"}).
		Refresh("5m").
		Time("now-1y", "now").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Цена и пол").Id(firstRowID)).
		WithPanel(goldPricePanel()).
		WithPanel(goldVsDxyPanel()).
		WithPanel(goldVsDfii10Panel()).
		WithRow(dashboard.NewRowBuilder("Дизельный канал").Id(firstRowID + 1)).
		WithPanel(crackULSDPanel()).
		WithRow(dashboard.NewRowBuilder("Инфляция").Id(firstRowID + 2)).
		WithPanel(inflationPanel()).
		WithRow(dashboard.NewRowBuilder("Спрос: ETF и ЦБ").Id(firstRowID + 3)).
		WithPanel(etfFlowsPanel()).
		WithPanel(cbPurchasesPanel()).
		WithRow(dashboard.NewRowBuilder("Прогноз и точность").Id(firstRowID + 4)).
		WithPanel(forecastConsensusPanel()).
		WithPanel(forecastAccuracyPanel())
}

// goldPricePanel — панель 1: цена золота и порог $4 000 (зелёный → красный).
// Серия B — только касания пола, точками. ThresholdsStyle обязателен: по умолчанию
// Grafana пороги не рисует (off), и линия «пола» $4 000 пропала бы.
// Id задан явно: Validate требует непустых уникальных id панелей.
func goldPricePanel() *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Id(1).
		Title("Цена золота и пол $4 000").
		Span(spanThird).
		Datasource(datasourceRef()).
		WithTarget(timeseriesQuery("A", goldPriceSQL(`"Цена, USD/oz"`, ""))).
		WithTarget(timeseriesQuery("B", goldPriceSQL(`"Касания пола $4 000"`, "  AND value < 4000\n"))).
		Thresholds(priceFloorThresholds()).
		ThresholdsStyle(common.NewGraphThresholdsStyleConfigBuilder().Mode(common.GraphThresholdsStyleModeLine)).
		OverrideByQuery("B", pointsOnlyProps())
}

// goldVsDxyPanel — панель 2: золото против индекса доллара (точки, правая ось).
func goldVsDxyPanel() *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Id(2).
		Title("Золото vs DXY").
		Span(spanThird).
		Datasource(datasourceRef()).
		WithTarget(timeseriesQuery("A", goldPriceSQL(`"Золото, USD/oz"`, ""))).
		WithTarget(timeseriesQuery("B", dxySQL)).
		OverrideByQuery("B", rightAxisPointsProps())
}

// goldVsDfii10Panel — панель 3: золото против реальной доходности TIPS (правая ось).
func goldVsDfii10Panel() *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Id(3).
		Title("Золото vs DFII10").
		Span(spanThird).
		Datasource(datasourceRef()).
		WithTarget(timeseriesQuery("A", goldPriceSQL(`"Золото, USD/oz"`, ""))).
		WithTarget(timeseriesQuery("B", dfii10SQL)).
		OverrideByQuery("B", []dashboard.DynamicConfigValue{axisPlacementRightProp()})
}

// crackULSDPanel — панель 4: крэк-спред ULSD (правая ось, пороги $30/$50) и запасы дистиллятов.
func crackULSDPanel() *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Id(4).
		Title("Crack ULSD и запасы дистиллятов").
		Span(spanHalf).
		Datasource(datasourceRef()).
		WithTarget(timeseriesQuery("A", distillateStocksSQL)).
		WithTarget(timeseriesQuery("B", crackUlsdSQL)).
		OverrideByQuery("B", crackProps())
}

// inflationPanel — панель 5: YoY CPI и PPI. Значение — доля (value/lag - 1), поэтому
// единица percentunit: Grafana домножает её на 100 и печатает «3.2%».
func inflationPanel() *timeseries.PanelBuilder {
	return timeseries.NewPanelBuilder().
		Id(5).
		Title("CPI/PPI отпечатки, YoY").
		Span(spanHalf).
		Datasource(datasourceRef()).
		WithTarget(timeseriesQuery("A", yoySQL("CUSR0000SA0", `"CPI SA, YoY"`))).
		WithTarget(timeseriesQuery("B", yoySQL("WPSFD4", `"PPI FD, YoY"`))).
		Unit(units.PercentUnit)
}

// etfFlowsPanel — панель 6: месячные потоки ETF (мир) барами.
func etfFlowsPanel() *barchart.PanelBuilder {
	return barchart.NewPanelBuilder().
		Id(6).
		Title("ETF-потоки по месяцам, мир (bar)").
		Span(spanHalf).
		Datasource(datasourceRef()).
		WithTarget(timeseriesQuery("A", etfFlowsSQL))
}

// cbPurchasesPanel — панель 7: покупки ЦБ за год к дате, цвет бара по знаку (красный/зелёный).
func cbPurchasesPanel() *barchart.PanelBuilder {
	return barchart.NewPanelBuilder().
		Id(7).
		Title("ЦБ: покупки YTD, т (bar)").
		Span(spanHalf).
		Datasource(datasourceRef()).
		XField("Страна").
		WithTarget(tableQuery("A", cbPurchasesSQL)).
		Thresholds(cbPurchaseThresholds()).
		ColorScheme(dashboard.NewFieldColorBuilder().Mode(dashboard.FieldColorModeIdThresholds))
}

// forecastConsensusPanel — панель 8: консенсус Q4-2026 (бар по середине диапазона).
// Факт не дублируется — он на панели 1.
func forecastConsensusPanel() *barchart.PanelBuilder {
	return barchart.NewPanelBuilder().
		Id(8).
		Title("Консенсус Q4-2026 и own-сетка (bar)").
		Span(spanHalf).
		Datasource(datasourceRef()).
		XField("Прогноз").
		WithTarget(tableQuery("A", forecastConsensusSQL))
}

// forecastAccuracyPanel — панель 9: таблица «обещали / вышло» по разрешённым прогнозам.
func forecastAccuracyPanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Id(9).
		Title("Точность прошлых прогнозов (table)").
		Span(spanHalf).
		Datasource(datasourceRef()).
		WithTarget(tableQuery("A", forecastAccuracySQL))
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
