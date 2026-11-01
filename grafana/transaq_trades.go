package main

import (
	"github.com/grafana/grafana-foundation-sdk/go/bargauge"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// TransaqTradesUID — uid дашборда ленты сделок Transaq (пункт ofz-market/1.6). Артефакт —
// dashboard/<uid>.json; uid попадает и в spec, и в metadata.name манифеста.
const TransaqTradesUID = "transaq-trades"

// Id рядов вне диапазона панелей 1..5: нулевой id Grafana заменит своим, а совпавший с панелью
// сделает артефакт неотличимым от кода.
const firstTransaqTradesRowID = 21

// spanFull — ширина графиков во всю сетку Grafana (24 колонки).
const spanFull = 24

// Запросы панелей 1–2 читают ДНЕВНУЮ витрину: top-10 за диапазон — это сумма по дням, и
// сканировать минуты незачем (time-series практика «зерно под задачу», спека §7). $__dateFilter —
// макрос плагина для Date-колонки; $__timeFilter здесь дал бы неявный каст Date → DateTime.
const (
	ttNetBuysSQL = `SELECT
    sec_code AS "Инструмент",
    sum(net_value_rub) AS "Нетто покупок, ₽"
FROM v_transaq_net_flows_daily
WHERE board = 'TQBR'
  AND $__dateFilter(date)
GROUP BY sec_code
HAVING sum(net_value_rub) > 0
ORDER BY "Нетто покупок, ₽" DESC
LIMIT 10
`

	ttNetSellsSQL = `SELECT
    sec_code AS "Инструмент",
    -sum(net_value_rub) AS "Нетто продаж, ₽"
FROM v_transaq_net_flows_daily
WHERE board = 'TQBR'
  AND $__dateFilter(date)
GROUP BY sec_code
HAVING sum(net_value_rub) < 0
ORDER BY "Нетто продаж, ₽" DESC
LIMIT 10
`

	// Графики читают минутную витрину; $__timeInterval разворачивается в toStartOfInterval с
	// адаптивным шагом панели. LIMIT на графике нет: ряд не обрезается, агрегация — в ClickHouse.
	ttNetFlowSQL = `SELECT
    $__timeInterval(minute) AS time,
    sum(net_value_rub) AS "Нетто, ₽"
FROM v_transaq_net_flows
WHERE board = 'TQBR'
  AND $__timeFilter(minute)
GROUP BY time
ORDER BY time
`

	ttImoexVolumesSQL = `SELECT
    $__timeInterval(minute) AS time,
    sum(buy_value_rub) AS "Покупки, ₽",
    -sum(sell_value_rub) AS "Продажи, ₽"
FROM v_transaq_net_flows
WHERE board = 'TQBR'
  AND $__timeFilter(minute)
GROUP BY time
ORDER BY time
`

	ttOfzVolumesSQL = `SELECT
    $__timeInterval(minute) AS time,
    sum(buy_value_rub) AS "Покупки, ₽",
    -sum(sell_value_rub) AS "Продажи, ₽"
FROM v_transaq_net_flows
WHERE board = 'TQOB'
  AND $__timeFilter(minute)
GROUP BY time
ORDER BY time
`
)

// NewTransaqTrades собирает дашборд «Сделки MOEX — нетто покупок и продаж» (пункт ofz-market/1.6,
// спека 2026-10-31-grafana-transaq-trades-design.md §5): 5 панелей в 2 рядах. Ряд 21 «Итоги
// периода» — top-10 нетто покупок/продаж IMOEX по дневной витрине; ряд 22 «Объёмы по интервалам» —
// нетто-поток IMOEX и покупки/продажи IMOEX и ОФЗ по минутной витрине. Порядок WithRow/WithPanel
// задаёт нумерацию, Span — ширину в сетке 24.
//
// Окно по умолчанию — 7 дней: «итоги дня» на нерабочий день были бы пусты, а неделя покрывает
// последнюю торговую сессию. Refresh 5m — лента обновляется импортёром каждые 15 минут.
func NewTransaqTrades() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Сделки MOEX — нетто покупок и продаж").
		Uid(TransaqTradesUID).
		Tags([]string{"transaq", "moex", "ofz-market"}).
		Refresh("5m").
		Time("now-7d", "now").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Итоги периода").Id(firstTransaqTradesRowID)).
		WithPanel(ttNetBuysPanel()).
		WithPanel(ttNetSellsPanel()).
		WithRow(dashboard.NewRowBuilder("Объёмы по интервалам").Id(firstTransaqTradesRowID + 1)).
		WithPanel(ttNetFlowPanel()).
		WithPanel(ttImoexVolumesPanel()).
		WithPanel(ttOfzVolumesPanel())
}

// ttPanelDefaults — визуальные конвенции графиков: палитра classic, легенда снизу таблицей с
// calcs, tooltip по одной серии, линия шириной 1 без заливки, градиента и соединения разрывов.
// SpanNulls=false — пропуск в витрине (нет сделок или разрыв ленты) честно остаётся разрывом:
// нули не подставляются, иначе отсутствие данных читалось бы как нулевой объём.
func ttPanelDefaults(builder *timeseries.PanelBuilder) *timeseries.PanelBuilder {
	return builder.
		Datasource(datasourceRef()).
		ColorScheme(dashboard.NewFieldColorBuilder().Mode(dashboard.FieldColorModeIdPaletteClassic)).
		Legend(common.NewVizLegendOptionsBuilder().
			ShowLegend(true).
			Placement(common.LegendPlacementBottom).
			DisplayMode(common.LegendDisplayModeTable).
			Calcs([]string{"sum", "mean", "max"})).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeSingle).
			Sort(common.SortOrderNone)).
		LineWidth(1).
		LineInterpolation(common.LineInterpolationSmooth).
		FillOpacity(0).
		GradientMode(common.GraphGradientModeNone).
		SpanNulls(common.BoolOrFloat64{Bool: ptr(false)}).
		Unit(units.Rubles).
		Decimals(0)
}

// ttBargaugeDefaults — общие настройки top-10 панелей: горизонтальные бары, значения красит
// значение, шкала тянется на всю высоту. ReduceOptions.values=true — в бары идут значения строк
// табличного ответа, а не вычисляемая агрегация.
func ttBargaugeDefaults(builder *bargauge.PanelBuilder) *bargauge.PanelBuilder {
	return builder.
		Datasource(datasourceRef()).
		Orientation(common.VizOrientationHorizontal).
		DisplayMode(common.BarGaugeDisplayModeBasic).
		ValueMode(common.BarGaugeValueModeColor).
		ShowUnfilled(true).
		ReduceOptions(common.NewReduceDataOptionsBuilder().Values(true)).
		Unit(units.Rubles).
		Decimals(0)
}

// ttNetBuysPanel — панель 1: top-10 нетто покупок в режиме TQBR за выбранный период. Значение —
// оценка рублей через lotsize, а не биржевой оборот; ETF (TQTF) в TQBR не попадают.
func ttNetBuysPanel() *bargauge.PanelBuilder {
	return ttBargaugeDefaults(bargauge.NewPanelBuilder()).
		Id(1).
		Title("Нетто покупки в IMOEX, ₽").
		Span(spanHalf).
		Height(9).
		WithTarget(tableQuery("A", ttNetBuysSQL)).
		Description("Сумма нетто-объёма (оценка покупок минус продаж) по коду бумаги за выбранный период; " +
			"режим торгов TQBR — акции IMOEX. Топ-10 по величине нетто покупок. Рублёвая оценка — " +
			"Σ цена × лоты × lotsize из снимка transaq_securities, а не биржевой оборот: сделки без " +
			"lotsize в сумму не входят, ETF (TQTF) в TQBR не попадают. Только фактические сделки " +
			"ленты: разрыв сессии не догоняется, провал может означать пропуск данных, а не рынок. " +
			"Это аналитика, а не сигнал и не рекомендация.")
}

// ttNetSellsPanel — панель 2: top-10 нетто продаж в TQBR. Знак снят намеренно: бар показывает
// величину перевеса продавцов, а не отрицательное число.
func ttNetSellsPanel() *bargauge.PanelBuilder {
	return ttBargaugeDefaults(bargauge.NewPanelBuilder()).
		Id(2).
		Title("Нетто продажи в IMOEX, ₽").
		Span(spanHalf).
		Height(9).
		WithTarget(tableQuery("A", ttNetSellsSQL)).
		Description("Сумма нетто-объёма (продажи минус покупки) по коду бумаги за выбранный период; " +
			"режим торгов TQBR — акции IMOEX. Топ-10 по величине перевеса продавцов; значения " +
			"показаны положительными, знак минуса снят намеренно. Рублёвая оценка — Σ цена × лоты × " +
			"lotsize из снимка transaq_securities: сделки без lotsize в сумму не входят. Только " +
			"фактические сделки ленты, разрывы не догоняются. Это аналитика, а не сигнал и не рекомендация.")
}

// ttNetFlowPanel — панель 3: нетто buy − sell по интервалам для IMOEX. Нулевая линия разделяет
// перевес покупателей и продавцов; бакеты адаптивные ($__timeInterval).
func ttNetFlowPanel() *timeseries.PanelBuilder {
	return ttPanelDefaults(timeseries.NewPanelBuilder()).
		Id(3).
		Title("Нетто-поток IMOEX по интервалам, ₽").
		Span(spanFull).
		Height(8).
		WithTarget(timeseriesQuery("A", ttNetFlowSQL)).
		Description("Нетто buy − sell по бакетам, которые Grafana выбирает под ширину панели и диапазон " +
			"($__timeInterval). Нулевая линия разделяет перевес покупателей и продавцов. Пропуск в " +
			"ряду — нет сделок в бакете или разрыв ленты: нули не подставляются и линия через пропуск " +
			"не проводится. Значение — рублёвая оценка через lotsize; это не биржевой оборот и не сигнал.")
}

// ttImoexVolumesPanel — панель 4: покупки и продажи IMOEX по инициативной стороне сделки.
// Продажи отрицательные, чтобы перевес читался по нулевой линии, как в источнике.
func ttImoexVolumesPanel() *timeseries.PanelBuilder {
	return ttPanelDefaults(timeseries.NewPanelBuilder()).
		Id(4).
		Title("Покупки и продажи IMOEX по интервалам, ₽").
		Span(spanFull).
		Height(8).
		WithTarget(timeseriesQuery("A", ttImoexVolumesSQL)).
		Description("Покупки и продажи по инициативной стороне сделки (buy_sell ленты) в рублях; продажи " +
			"показаны со знаком минус, чтобы перевес читался по нулевой линии. Это не оборот MOEX, а " +
			"оценка по сделкам, попавшим в ленту Transaq: разрывы не догоняются, нули не подставляются.")
}

// ttOfzVolumesPanel — панель 5: то же для режима TQOB. Заменяет ETF-панель источника: подписка
// стенда собирает IMOEX + ОФЗ, а TQTF в ленте нет — ETF-панель была бы всегда пустой.
func ttOfzVolumesPanel() *timeseries.PanelBuilder {
	return ttPanelDefaults(timeseries.NewPanelBuilder()).
		Id(5).
		Title("Покупки и продажи ОФЗ (TQOB) по интервалам, ₽").
		Span(spanFull).
		Height(8).
		WithTarget(timeseriesQuery("A", ttOfzVolumesSQL)).
		Description("Покупки и продажи по инициативной стороне сделки в режиме TQOB — государственные " +
			"облигации (ОФЗ). Продажи показаны со знаком минус. Для облигаций лоты и денежный объём " +
			"не совпадают, а lotsize берётся из снимка каталога, поэтому значения — ориентир, а не " +
			"точный оборот; разрывы ленты не догоняются.")
}
