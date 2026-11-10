package main

import (
	"fmt"

	"github.com/grafana/grafana-foundation-sdk/go/barchart"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/table"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// Id панелей дашборда — 1..14, рядов — 21..26: нулевой id Grafana трактует как «не задан»
// и заменит своим, поэтому ряды нумеруются вне диапазона панелей.
const firstRowID = 21

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

// errorColumnAlias — имя колонки ошибки панели 9: знак и «%» подписаны прямо в алиасе, чтобы
// «+3,47 %» не читалось как модуль ошибки. Та же строка нужна override'у единицы измерения
// (сопоставление идёт по имени поля), поэтому алиас живёт константой, а не дублируется.
const errorColumnAlias = "Ошибка, % (+ = прогноз выше факта)"

// forecastAccuracySQL — разрешённые прогнозы прошлого: таблица «обещали / вышло».
// error_pct в витрине знаковая — (predicted − actual) / actual × 100 (views/forecast_accuracy.go).
var forecastAccuracySQL = fmt.Sprintf(`SELECT
    metric, forecast_date, target_date, predicted, actual,
    error_pct AS %q
FROM v_forecast_accuracy
WHERE is_resolved = 1
ORDER BY target_date
`, errorColumnAlias)

// usRatesSQL — номинальная доходность UST 10 лет: серия A панели 10 (ставочный контур США).
const usRatesDgs10SQL = `SELECT
    date AS time,
    value AS "DGS10, %"
FROM v_fred_macro
WHERE series = 'DGS10'
  AND $__timeFilter(date)
ORDER BY date
`

// usRatesDfii10SQL — реальная доходность TIPS 10 лет: серия B панели 10.
// DFII10 панели 3 не переиспользуется: у серии панели 10 имя без привязки к золоту.
const usRatesDfii10SQL = `SELECT
    date AS time,
    value AS "DFII10, %"
FROM v_fred_macro
WHERE series = 'DFII10'
  AND $__timeFilter(date)
ORDER BY date
`

// usRatesT5yieSQL — 5-летние инфляционные ожидания рынка (breakeven): серия C панели 10.
const usRatesT5yieSQL = `SELECT
    date AS time,
    value AS "T5YIE, %"
FROM v_fred_macro
WHERE series = 'T5YIE'
  AND $__timeFilter(date)
ORDER BY date
`

// brentSQL — Brent фронт-месяца MOEX BR (settle): серия B панели 11. Это НЕ спот и не
// back-adjusted ряд: на роллах возможен разрыв (та же оговорка, что у панели 29
// key-rate-publications). Статейный спот Brent ($94,62 на 04.11) этим рядом заменять нельзя.
const brentSQL = `SELECT
    date AS time,
    value AS "Brent, USD/барр."
FROM v_moex_market
WHERE series = 'brent_front'
  AND $__timeFilter(date)
ORDER BY date
`

// verifyIndicatorsSQL — дашборд верификации сценария статьи §6: одна строка на индикатор.
// Значения приходят из приоритетного источника (eia > webbridge > manual); NULL — ни одного
// источника нет. stale = 1 — последнее наблюдение старше 14 дней (фолбэк-сбор отстал).
const verifyIndicatorsSQL = `SELECT
    indicator,
    value,
    threshold,
    flag,
    as_of,
    stale
FROM v_gold_dashboard
ORDER BY indicator
`

// etfDailyFlowsSQL — дневной поток 73 фондов bold.report, тонны: серия A (бары) панели 12.
const etfDailyFlowsSQL = `SELECT
    date AS time,
    flow_tonnes AS "Поток, т"
FROM v_gold_etf_flows_daily
WHERE $__timeFilter(date)
ORDER BY date
`

// etfDailyHoldingsSQL — совокупные запасы 73 фондов bold.report, тонны: серия B (линия,
// правая ось) панели 12. Агрегат ≈83 % мирового рынка WGC — с витриной v_gold_etf_flows
// (все фонды мира, месячная частота) не складывать и не сравнивать напрямую.
const etfDailyHoldingsSQL = `SELECT
    date AS time,
    holdings_tonnes AS "Запасы, т"
FROM v_gold_etf_flows_daily
WHERE $__timeFilter(date)
ORDER BY date
`

// cbQuarterlySQL — покупки ЦБ-7 по кварталам для grouped bar панели 13. Сортировка по
// убыванию III квартала повторяет таблицу «Семь стран» статьи; NULL страны без дельт
// уходят вниз (NULL меньше любого числа — при DESC ниже всех числовых строк).
const cbQuarterlySQL = `SELECT
    country AS "Страна",
    q_change_tonnes AS "III кв, т",
    prev_q_change_tonnes AS "II кв, т",
    ytd_change_tonnes AS "Янв–сен, т",
    mom_change_tonnes AS "Сентябрь, т",
    holdings_tonnes AS "Запасы, т"
FROM v_cb_gold_dynamics
ORDER BY q_change_tonnes DESC
`

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

// NewGoldThesis собирает дашборд «Золото — тезисы статьи» (§4 золотого роадмапа):
// 14 панелей в 6 рядах. Порядок вызовов WithRow/WithPanel задаёт порядок сетки; id панелей
// заданы вручную (1–14) и не обязаны следовать порядку сетки: панели 10–14 — ECharts-панели
// обновления статьи 2026-11-05 (gold-thesis/phase-4-followup-9, #533).
func NewGoldThesis() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Золото — тезисы статьи").
		Uid(GoldThesisUID).
		Tags([]string{"gold", "article"}).
		Refresh("5m").
		Time("now-1y", "now").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Цена и пол").Id(firstRowID)).
		WithPanel(goldPricePanel()).
		WithPanel(goldVsDxyPanel()).
		WithPanel(goldVsDfii10Panel()).
		WithRow(dashboard.NewRowBuilder("Ставки, нефть и верификация").Id(firstRowID + 5)).
		WithPanel(usRatesPanel()).
		WithPanel(goldVsBrentPanel()).
		WithPanel(verifyIndicatorsPanel()).
		WithRow(dashboard.NewRowBuilder("Дизельный канал").Id(firstRowID + 1)).
		WithPanel(crackULSDPanel()).
		WithRow(dashboard.NewRowBuilder("Инфляция").Id(firstRowID + 2)).
		WithPanel(inflationPanel()).
		WithRow(dashboard.NewRowBuilder("Спрос: ETF и ЦБ").Id(firstRowID + 3)).
		WithPanel(etfFlowsPanel()).
		WithPanel(cbPurchasesPanel()).
		WithPanel(etfDailyPanel()).
		WithPanel(cbQuarterlyPanel()).
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
		ThresholdsStyle(thresholdLineStyle()).
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
// Знак ошибки и её единица подписаны явно: колонка знаковая (плюс — прогноз выше факта),
// без подписи и percent её читают как модуль ошибки без размерности. Единица — override'ом
// по имени колонки, а не панельным Unit: панельный default percent унаследовали бы и колонки
// predicted/actual (USD/oz) из той же таблицы.
func forecastAccuracyPanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Id(9).
		Title("Точность прошлых прогнозов (table)").
		Description("Ошибка прогноза знаковая: error_pct = (predicted − actual) / actual × 100, «+» — прогноз выше факта. "+
			"Источник — витрина v_forecast_accuracy, только resolved-прогнозы (is_resolved = 1: факт уже записан forecast_resolve).").
		Span(spanHalf).
		Datasource(datasourceRef()).
		WithTarget(tableQuery("A", forecastAccuracySQL)).
		OverrideByName(errorColumnAlias, []dashboard.DynamicConfigValue{{Id: "unit", Value: units.Percent}})
}

// usRatesPanel — панель 10 (ECharts): ставочный контур США из v_fred_macro — номинальная
// доходность UST 10 лет, реальная доходность TIPS (DFII10) и 5-летние инфляционные
// ожидания рынка. Покрывает цифры статьи 2026-11-05 (DGS10 4,889%; T5YIE 2,39→2,30);
// все три серии — % годовых одного порядка, общая ось.
func usRatesPanel() *dashboard.PanelBuilder {
	return echartsPanel(10,
		"Ставочный контур США: DGS10 / DFII10 / T5YIE",
		"Доходности и ожидания из v_fred_macro, % годовых: DGS10 — номинальная доходность "+
			"10-летних казначейских облигаций, DFII10 — реальная доходность TIPS 10 лет, "+
			"T5YIE — 5-летняя breakeven-инфляция (ожидания рынка). Разложение номинальной "+
			"доходности: DGS10 ≈ DFII10 + ожидания инфляции. Все серии — уровни в % годовых.",
		usRatesEChartsJS,
		timeseriesQuery("A", usRatesDgs10SQL),
		timeseriesQuery("B", usRatesDfii10SQL),
		timeseriesQuery("C", usRatesT5yieSQL),
	).Span(spanThird)
}

// goldVsBrentPanel — панель 11 (ECharts): золото против Brent. Brent — фронт-месяц
// фьючерса MOEX BR (v_moex_market, settle), НЕ спот и не back-adjusted: на роллах
// возможен разрыв. Статейный спот ($94,62 на 04.11.2026) этим рядом не воспроизводится.
func goldVsBrentPanel() *dashboard.PanelBuilder {
	return echartsPanel(11,
		"Золото vs Brent",
		"Золото (v_gold_prices, COMEX front, левая ось, USD/oz) против Brent (v_moex_market, "+
			"series = brent_front, правая ось, USD/барр.). Brent — фронт-месяц фьючерса MOEX BR "+
			"(settle), НЕ спот и не back-adjusted: на роллах возможен разрыв ценовых стыков. "+
			"Свежесть ряда Brent — зона ответственности контура MOEX (dagu/moex.yaml), не группы "+
			"gold_thesis_sources.",
		goldVsBrentEChartsJS,
		timeseriesQuery("A", goldPriceSQL(`"Золото, USD/oz"`, "")),
		timeseriesQuery("B", brentSQL),
	).Span(spanThird)
}

// verifyIndicatorsPanel — панель 14 (ECharts): индикаторы верификации сценария статьи §6
// (v_gold_dashboard) — gauge-тройка DXY / FedWatch / crack с порогами и текстовая строка
// для остальных индикаторов. Значения приоритетного источника (eia > webbridge > manual);
// «нет данных» — ни одного источника; «(устарело)» — последнее наблюдение старше 14 дней.
// Пороги шкал — текст сценария статьи, а не данные: они заданы в threshold витрины.
func verifyIndicatorsPanel() *dashboard.PanelBuilder {
	return echartsPanel(14,
		"Индикаторы верификации сценария",
		"Дашборд верификации сценария статьи §6 (v_gold_dashboard): DXY (>102 — порог модели), "+
			"CME FedWatch на повышение в декабре (>65% — сценарное переключение, >80% — алерт), "+
			"crack-прокси ULSD (>50 USD/барр. — медвежий сигнал). Значения — из приоритетного "+
			"источника (eia > webbridge > manual): «нет данных» — ни одного источника; «(устарело)» "+
			"— последнее наблюдение старше 14 дней. Ряды dxy / fedwatch_dec_hike / crack_ulsd_proxy "+
			"обновляет сессионный сбор WebBridge или ручной ввод, не группа gold_thesis_sources. "+
			"Запасы дистиллятов и месячный ETF-поток — строкой под шкалами.",
		verifyIndicatorsEChartsJS,
		tableQuery("A", verifyIndicatorsSQL),
	).Span(spanThird)
}

// etfDailyPanel — панель 12 (ECharts): дневной поток и запасы 73 золотых ETF контура
// bold.report (ByteTree). Агрегат ≈83% мирового рынка WGC — с месячной витриной
// v_gold_etf_flows (панель 6, все фонды мира) не складывать и не сравнивать напрямую.
func etfDailyPanel() *dashboard.PanelBuilder {
	return echartsPanel(12,
		"ETF-потоки дневные, bold (73 фонда)",
		"Дневной агрегат 73 физически обеспеченных золотых ETF контура bold.report (ByteTree), "+
			"≈83% мирового рынка WGC: бары — дневной поток (Δ запасов, тонны), линия правой оси — "+
			"совокупные запасы (тонны). Покрывает цифры статьи 2026-11-05: +110,3 т 02.11 (вероятная "+
			"техническая ошибка источника — максимум двух лет −43,6 т), −8,8 т 03.11, запасы "+
			"3 528→3 638 т. С месячной витриной WGC (панель 6) не складывать и не сравнивать "+
			"напрямую: там все фонды мира и месячная частота. Ряд пишет импортёр bold_etf_flows.",
		etfDailyEChartsJS,
		timeseriesQuery("A", etfDailyFlowsSQL),
		timeseriesQuery("B", etfDailyHoldingsSQL),
	).Span(spanHalf)
}

// cbQuarterlyPanel — панель 13 (ECharts): покупки семи ЦБ по кварталам (расширенная
// v_cb_gold_dynamics: Q, Q-1, YTD, MoM + запасы). Повторяет таблицу «Семь стран» статьи
// 2026-11-05; кварталы считаются по месячным дельтам WGC от последней точки каждой страны
// (у RUS пропуски 2025-12…2026-03 — по фактическим точкам).
func cbQuarterlyPanel() *dashboard.PanelBuilder {
	return echartsPanel(13,
		"ЦБ: покупки по кварталам, 7 стран",
		"Покупки семи центральных банков (v_cb_gold_dynamics, WGC Gold Reserves by Country): "+
			"бары — III квартал, II квартал, январь–сентябрь (YTD) и последний месяц (MoM, тонны), "+
			"линия правой оси — запасы на 30.09 (тонны). Сортировка по убыванию III квартала "+
			"повторяет таблицу «Семь стран» статьи 2026-11-05. Кварталы — суммы месячных дельт от "+
			"последней точки страны; у RUS пропуски 2025-12…2026-03, поэтому кварталы/YTD по "+
			"фактическим точкам. NULL (нет данных) на графике не рисуется, не ноль.",
		cbQuarterlyEChartsJS,
		tableQuery("A", cbQuarterlySQL),
	).Span(spanHalf)
}

// usRatesEChartsJS — getOption панели 10: три линии % годовых из фреймов A/B/C.
const usRatesEChartsJS = `const byRef = {};
for (const s of context.panel.data.series) {
  if (s && s.refId) { byRef[s.refId] = s; }
}
function lineData(refId) {
  const s = byRef[refId];
  if (!s) { return []; }
  const t = s.fields.find((f) => f.type === 'time');
  const v = s.fields.find((f) => f.type === 'number');
  if (!t || !v) { return []; }
  const out = [];
  for (let i = 0; i < s.length; i++) {
    if (t.values[i] != null && v.values[i] != null) { out.push([t.values[i], v.values[i]]); }
  }
  return out;
}
const defs = [
  { ref: 'A', name: 'DGS10 — номинальная, %', color: '#1971c2' },
  { ref: 'B', name: 'DFII10 — реальная (TIPS), %', color: '#f08c00' },
  { ref: 'C', name: 'T5YIE — ожидания 5 лет, %', color: '#2f9e44' }
];
const series = [];
for (const d of defs) {
  const data = lineData(d.ref);
  if (data.length) {
    series.push({
      name: d.name, type: 'line', data: data,
      showSymbol: false, smooth: true,
      lineStyle: { width: 1.5, color: d.color }, itemStyle: { color: d.color }
    });
  }
}
if (!series.length) { return {}; }
return {
  legend: { top: 0, textStyle: { fontSize: 11, color: '#495057' } },
  grid: { top: 34, left: 52, right: 20, bottom: 28 },
  tooltip: {
    trigger: 'axis',
    valueFormatter: function (v) { return v == null ? '—' : Number(v).toFixed(2) + ' %'; }
  },
  xAxis: { type: 'time' },
  yAxis: { type: 'value', scale: true, axisLabel: { formatter: function (v) { return v.toFixed(1); } } },
  series: series
};`

// goldVsBrentEChartsJS — getOption панели 11: золото (левая ось) и Brent (правая ось).
const goldVsBrentEChartsJS = `const byRef = {};
for (const s of context.panel.data.series) {
  if (s && s.refId) { byRef[s.refId] = s; }
}
function lineData(refId) {
  const s = byRef[refId];
  if (!s) { return []; }
  const t = s.fields.find((f) => f.type === 'time');
  const v = s.fields.find((f) => f.type === 'number');
  if (!t || !v) { return []; }
  const out = [];
  for (let i = 0; i < s.length; i++) {
    if (t.values[i] != null && v.values[i] != null) { out.push([t.values[i], v.values[i]]); }
  }
  return out;
}
const gold = lineData('A');
const brent = lineData('B');
if (!gold.length && !brent.length) { return {}; }
const series = [];
if (gold.length) {
  series.push({
    name: 'Золото (COMEX front), USD/oz', type: 'line', data: gold,
    showSymbol: false, smooth: true, lineStyle: { width: 1.5, color: '#d4a017' }, itemStyle: { color: '#d4a017' }
  });
}
if (brent.length) {
  series.push({
    name: 'Brent (MOEX front), USD/барр.', type: 'line', data: brent, yAxisIndex: 1,
    showSymbol: false, smooth: true, lineStyle: { width: 1.5, color: '#1971c2' }, itemStyle: { color: '#1971c2' }
  });
}
return {
  legend: { top: 0, textStyle: { fontSize: 11, color: '#495057' } },
  grid: { top: 34, left: 60, right: 60, bottom: 28 },
  tooltip: { trigger: 'axis', valueFormatter: function (v) { return v == null ? '—' : Number(v).toFixed(1); } },
  xAxis: { type: 'time' },
  yAxis: [
    { type: 'value', scale: true, name: 'USD/oz', nameTextStyle: { fontSize: 10, color: '#868e96' } },
    { type: 'value', scale: true, name: 'USD/барр.', nameTextStyle: { fontSize: 10, color: '#868e96' }, splitLine: { show: false } }
  ],
  series: series
};`

// etfDailyEChartsJS — getOption панели 12: дневной поток барами (левая ось) и запасы
// линией (правая ось) из фреймов A/B.
const etfDailyEChartsJS = `const byRef = {};
for (const s of context.panel.data.series) {
  if (s && s.refId) { byRef[s.refId] = s; }
}
function lineData(refId) {
  const s = byRef[refId];
  if (!s) { return []; }
  const t = s.fields.find((f) => f.type === 'time');
  const v = s.fields.find((f) => f.type === 'number');
  if (!t || !v) { return []; }
  const out = [];
  for (let i = 0; i < s.length; i++) {
    if (t.values[i] != null && v.values[i] != null) { out.push([t.values[i], v.values[i]]); }
  }
  return out;
}
const flow = lineData('A');
const hold = lineData('B');
if (!flow.length && !hold.length) { return {}; }
const series = [];
if (flow.length) {
  series.push({
    name: 'Поток, т (лев. ось)', type: 'bar', data: flow, barMaxWidth: 6,
    itemStyle: { color: '#4dabf7' }
  });
}
if (hold.length) {
  series.push({
    name: 'Запасы, т (прав. ось)', type: 'line', data: hold, yAxisIndex: 1,
    showSymbol: false, lineStyle: { width: 1.5, color: '#e8590c' }, itemStyle: { color: '#e8590c' }
  });
}
return {
  legend: { top: 0, textStyle: { fontSize: 11, color: '#495057' } },
  grid: { top: 34, left: 56, right: 56, bottom: 28 },
  tooltip: {
    trigger: 'axis',
    valueFormatter: function (v) { return v == null ? '—' : Number(v).toFixed(1) + ' т'; }
  },
  xAxis: { type: 'time' },
  yAxis: [
    { type: 'value', scale: true, name: 'Поток, т', nameTextStyle: { fontSize: 10, color: '#868e96' } },
    { type: 'value', scale: true, name: 'Запасы, т', nameTextStyle: { fontSize: 10, color: '#868e96' }, splitLine: { show: false } }
  ],
  series: series
};`

// cbQuarterlyEChartsJS — getOption панели 13: grouped bar кварталов/YTD/MoM и линия
// запасов из одного табличного фрейма (колонки — русские алиасы запроса).
const cbQuarterlyEChartsJS = `const s = context.panel.data.series[0];
if (!s) { return {}; }
function col(name) {
  const f = s.fields.find((x) => x.name === name);
  return f ? f.values : [];
}
const countries = col('Страна');
if (!countries.length) { return {}; }
function nums(vals) {
  const out = [];
  for (let i = 0; i < countries.length; i++) {
    out.push(vals[i] == null ? null : Number(vals[i]));
  }
  return out;
}
return {
  legend: { top: 0, textStyle: { fontSize: 11, color: '#495057' } },
  grid: { top: 34, left: 56, right: 56, bottom: 44 },
  tooltip: {
    trigger: 'axis',
    axisPointer: { type: 'shadow' },
    valueFormatter: function (v) { return v == null ? 'нет данных' : Number(v).toFixed(1) + ' т'; }
  },
  xAxis: { type: 'category', data: countries, axisLabel: { interval: 0, rotate: 20, fontSize: 11 } },
  yAxis: [
    { type: 'value', scale: true, name: 'Δ резервов, т', nameTextStyle: { fontSize: 10, color: '#868e96' } },
    { type: 'value', scale: true, name: 'Запасы, т', nameTextStyle: { fontSize: 10, color: '#868e96' }, splitLine: { show: false } }
  ],
  series: [
    { name: 'III кв, т', type: 'bar', data: nums(col('III кв, т')), itemStyle: { color: '#2f9e44' } },
    { name: 'II кв, т', type: 'bar', data: nums(col('II кв, т')), itemStyle: { color: '#74c0fc' } },
    { name: 'Янв–сен, т', type: 'bar', data: nums(col('Янв–сен, т')), itemStyle: { color: '#f08c00' } },
    { name: 'Сентябрь, т', type: 'bar', data: nums(col('Сентябрь, т')), itemStyle: { color: '#9775fa' } },
    {
      name: 'Запасы, т (прав. ось)', type: 'line', data: nums(col('Запасы, т')), yAxisIndex: 1,
      lineStyle: { width: 1.5, color: '#495057' }, itemStyle: { color: '#495057' }
    }
  ]
};`

// verifyIndicatorsEChartsJS — getOption панели 14: gauge-тройка DXY / FedWatch / crack
// из табличного фрейма v_gold_dashboard + строка под шкалами для остальных индикаторов.
// Пороги шкал (102 / 65+80 / 30+50) — текст сценария статьи §6, заданный в threshold
// витрины, а не данные рядов.
const verifyIndicatorsEChartsJS = `const s = context.panel.data.series[0];
if (!s) { return {}; }
function col(name) {
  const f = s.fields.find((x) => x.name === name);
  return f ? f.values : [];
}
const ind = col('indicator');
const val = col('value');
const stl = col('stale');
const asof = col('as_of');
function rowOf(code) {
  const i = ind.indexOf(code);
  if (i < 0) { return null; }
  return { value: val[i], stale: stl[i] === 1, asOf: asof[i] };
}
function fmtDate(v) {
  if (v == null) { return ''; }
  if (typeof v === 'number') { return new Date(v).toISOString().slice(0, 10); }
  return String(v).slice(0, 10);
}
function gauge(code, title, min, max, zones, unit, center) {
  const r = rowOf(code);
  const has = r && r.value != null;
  return {
    type: 'gauge', center: center, radius: '62%',
    min: min, max: max, startAngle: 210, endAngle: -30,
    axisLine: { lineStyle: { width: 10, color: zones } },
    pointer: has ? { length: '58%', width: 3 } : { show: false },
    axisTick: { distance: -14, length: 4, lineStyle: { color: '#ffffff', width: 1 } },
    splitLine: { distance: -14, length: 14, lineStyle: { color: '#ffffff', width: 2 } },
    axisLabel: { distance: 16, fontSize: 9, color: '#868e96' },
    title: { fontSize: 11, color: '#495057', offsetCenter: [0, '30%'] },
    detail: {
      fontSize: 15, color: '#212529', offsetCenter: [0, '56%'],
      formatter: function (v) {
        if (!has) { return 'нет данных'; }
        return Number(v).toFixed(unit === '%' ? 0 : 1) + (unit ? ' ' + unit : '');
      }
    },
    data: [{ value: has ? Number(r.value) : min, name: title + (r && r.stale ? ' (устарело)' : '') + (has && fmtDate(r.asOf) ? ' — ' + fmtDate(r.asOf) : '') }]
  };
}
const rest = [];
for (const code of ['distillate_stocks', 'etf_flows_month']) {
  const r = rowOf(code);
  if (r && r.value != null) {
    rest.push(code === 'distillate_stocks'
      ? 'Запасы дистиллятов: ' + Number(r.value).toLocaleString('ru-RU') + ' тыс. барр.' + (r.stale ? ' (устарело)' : '') + (fmtDate(r.asOf) ? ' — ' + fmtDate(r.asOf) : '')
      : 'ETF-поток месяца: ' + Number(r.value).toLocaleString('ru-RU') + ' млрд $' + (r.stale ? ' (устарело)' : '') + (fmtDate(r.asOf) ? ' — ' + fmtDate(r.asOf) : ''));
  }
}
return {
  series: [
    gauge('dxy', 'DXY (порог 102)', 90, 115, [[0.48, '#2f9e44'], [1, '#e03131']], '', ['18%', '58%']),
    gauge('fedwatch_dec_hike', 'FedWatch дек. (>65/>80)', 0, 100, [[0.65, '#2f9e44'], [0.8, '#f08c00'], [1, '#e03131']], '%', ['50%', '58%']),
    gauge('crack_ulsd_proxy', 'Crack ULSD (>50)', 0, 120, [[0.25, '#2f9e44'], [0.4167, '#f08c00'], [1, '#e03131']], '', ['82%', '58%'])
  ],
  graphic: rest.length ? [{ type: 'text', left: 10, bottom: 4, style: { text: rest.join('   |   '), fontSize: 10, color: '#868e96' } }] : []
};`
