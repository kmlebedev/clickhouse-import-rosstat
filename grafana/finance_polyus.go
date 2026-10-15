package main

import (
	"fmt"

	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// FinancePolyusUID — uid дашборда «Полюс (PLZL) — финансы и прогноз»: попадает и в spec, и в
// metadata.name (инвариант Manifest), и в имя файла артефакта.
const FinancePolyusUID = "finance-polyus"

// firstFinancePolyusRowID — id первого ряда (31): нулевой или совпавший с панелью id Grafana
// трактует как «не задан» и заменит своим, поэтому ряды 31–34 нумеруются вне диапазона панелей 1..19.
const firstFinancePolyusRowID = 31

// SQL-контракт ряда 31 (спека §5, дословно). Панели читают только витрины v_*: у датасорса стенда
// нет грантов на сырые таблицы (легаси-экспорт читал cbr_gold, cbr_currency_usd, databook_polyus,
// polyus_financial_metrics), это же требует Validate.
//
// Шаблоны «дата + значение» свои у каждой витрины, хотя форма у них общая и с key-rate совпадает:
// SQL панели — дословный контракт своей спеки, и правка шаблона одного дашборда не должна молча
// менять запросы другого.

// fpCbrGoldRubSQL — учётная цена золота ЦБ: единственный ряд ₽/г, из которого вместе с курсом
// собирается паритет $/oz (панель 2-A).
const fpCbrGoldRubSQL = `SELECT date AS time, value AS "Учётная цена ЦБ, ₽/г"
FROM v_cbr_macro
WHERE series = 'cbr_gold' AND $__timeFilter(date)
ORDER BY date
`

// fpDcfParitySQL — паритет учётной цены ЦБ в $/oz: цена ₽/г делится на курс ТОГО ЖЕ дня и умножается
// на граммы в тройской унции. Самоджойн по дате, а не две клиентские серии: ряды ЦБ разной длины, и
// деление взяло бы курс другой даты. INNER JOIN, а не LEFT: день без курса — не «паритет с NaN».
const fpDcfParitySQL = `SELECT r.date AS time, r.value / usd.value * 31.1034768 AS "ЦБ-паритет, $/oz"
FROM v_cbr_macro AS r
INNER JOIN v_cbr_macro AS usd ON usd.date = r.date AND usd.series = 'cbr_currency_usd'
WHERE r.series = 'cbr_gold' AND $__timeFilter(r.date)
ORDER BY time
`

// fpGoldPriceTemplate — срез v_gold_prices «дата + значение» по коду серии: три рыночных ряда панели 2
// (мосбиржа, COMEX-фронт, bold.report) одной формой — различаются серия и алиас. %s по порядку:
// алиас серии, код серии.
const fpGoldPriceTemplate = `SELECT date AS time, value AS %s
FROM v_gold_prices
WHERE source = 'gold' AND series = '%s' AND $__timeFilter(date)
ORDER BY date
`

// fpGoldPriceSQL собирает запрос рыночного ряда золота по шаблону.
func fpGoldPriceSQL(series, alias string) string {
	return fmt.Sprintf(fpGoldPriceTemplate, alias, series)
}

// fpUsdRubSQL — официальный курс ЦБ: знаменатель паритета золота и вход рублёвой переоценки DCF.
const fpUsdRubSQL = `SELECT date AS time, value AS "USD/RUB"
FROM v_cbr_macro
WHERE series = 'cbr_currency_usd' AND $__timeFilter(date)
ORDER BY date
`

// fpKeyRateSQL — ключевая ставка: локальная безрисковая ставка контура DCF, уровень на дату решений.
const fpKeyRateSQL = `SELECT date AS time, value AS "Ключевая ставка, %"
FROM v_cbr_macro
WHERE series = 'cbr_key_rate' AND $__timeFilter(date)
ORDER BY date
`

// fpOfzTenorTemplate — срез v_ofz_curve «дата + доходность» по тенору: четыре тенора кривой ОФЗ одной
// формой. %s по порядку: алиас серии, тенор.
const fpOfzTenorTemplate = `SELECT date AS time, yield AS %s
FROM v_ofz_curve
WHERE tenor = '%s' AND $__timeFilter(date)
ORDER BY date
`

// fpOfzTenorSQL собирает запрос доходности тенора по шаблону.
func fpOfzTenorSQL(tenor, alias string) string {
	return fmt.Sprintf(fpOfzTenorTemplate, alias, tenor)
}

// fpFredTemplate — срез v_fred_macro «дата + значение» по коду ряда: глобальный контур ставок
// (реальная доходность, номинальная доходность, ставка ФРС, инфляционные ожидания). %s по порядку:
// алиас серии, код ряда.
const fpFredTemplate = `SELECT date AS time, value AS %s
FROM v_fred_macro
WHERE series = '%s' AND $__timeFilter(date)
ORDER BY date
`

// fpFredSQL собирает запрос ряда FRED по шаблону.
func fpFredSQL(series, alias string) string {
	return fmt.Sprintf(fpFredTemplate, alias, series)
}

// fpPlzlCloseSQL — цена акции Полюса: рынок, к которому относится прогноз (close — цена закрытия, ₽).
const fpPlzlCloseSQL = `SELECT date AS time, close AS "PLZL, ₽"
FROM v_stock_prices
WHERE code = 'PLZL' AND $__timeFilter(date)
ORDER BY date
`

// fpPanelDefaults — общие визуальные конвенции панелей дашборда: палитра classic, легенда снизу
// таблицей с calcs (последнее/среднее/максимум), tooltip по одной серии, линия шириной 1 без заливки,
// градиента и соединения разрывов (разрыв ряда честно остаётся разрывом). Те же конвенции, что у
// key-rate (спека §6, «визуал переносим»), но своим хелпером: финансы Полюса могут разойтись с
// публикациями ставки, и правка конвенций одного дашборда не должна молча менять артефакт другого.
// Панели добавляют свои Id/Title/Description/Span и таргеты поверх.
func fpPanelDefaults(builder *timeseries.PanelBuilder) *timeseries.PanelBuilder {
	return builder.
		Datasource(datasourceRef()).
		ColorScheme(dashboard.NewFieldColorBuilder().Mode(dashboard.FieldColorModeIdPaletteClassic)).
		Legend(common.NewVizLegendOptionsBuilder().
			ShowLegend(true).
			Placement(common.LegendPlacementBottom).
			DisplayMode(common.LegendDisplayModeTable).
			Calcs([]string{"last", "mean", "max"})).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeSingle).
			Sort(common.SortOrderNone)).
		LineWidth(1).
		FillOpacity(0).
		GradientMode(common.GraphGradientModeNone).
		SpanNulls(common.BoolOrFloat64{Bool: ptr(false)})
}

// fpFixedColorProp — свойство override'а с фиксированным цветом серии: палитра classic красит серии
// по порядку таргетов, и без фиксации цвет тенора менялся бы при добавлении ряда. Форма свойства —
// как у krFixedColorProp key-rate: держим её своим хелпером, чтобы правка конвенций одного дашборда
// не меняла молча артефакт другого.
func fpFixedColorProp(color string) dashboard.DynamicConfigValue {
	return dashboard.DynamicConfigValue{
		Id:    "color",
		Value: dashboard.FieldColor{Mode: dashboard.FieldColorModeIdFixed, FixedColor: ptr(color)},
	}
}

// NewFinancePolyus собирает дашборд «Полюс (PLZL) — финансы и прогноз» (спека §4–§5). Собран ряд 31
// «Золото, ставки, рынок» (панели 1–6) — макро-входы прогноза: учётная цена золота в ₽/г и $/oz,
// курс, ставки локального и глобального контура, цена акции. Порядок вызовов WithRow/WithPanel
// задаёт и порядок сетки, и нумерацию панелей.
func NewFinancePolyus() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Полюс (PLZL) — финансы и прогноз").
		Uid(FinancePolyusUID).
		Tags([]string{"polyus", "dcf", "article"}).
		Refresh("1h").
		Time("now-5y", "now").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Золото, ставки, рынок").Id(firstFinancePolyusRowID)).
		WithPanel(fpGoldRubPanel()).
		WithPanel(fpGoldOzPanel()).
		WithPanel(fpUsdRubPanel()).
		WithPanel(fpRatesOfzPanel()).
		WithPanel(fpGlobalRatesPanel()).
		WithPanel(fpPlzlPricePanel())
}

// fpGoldRubPanel — панель 1: учётная цена золота ЦБ в ₽/г. Рубли с копейками (decimals 2): цена
// котируется до копеек, и без фиксации Grafana округляла бы её по ширине оси.
func fpGoldRubPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(1).
		Title("Учётная цена золота ЦБ, ₽/г").
		Span(spanHalf).
		Description("Учётная цена золота Банка России (v_cbr_macro, series = 'cbr_gold') — уровень " +
			"на дату, ₽ за грамм. Это цена для расчётов ЦБ, а не биржевая котировка: из неё и " +
			"официального курса собирается ЦБ-паритет $/oz панели 2. Значения — ₽/г.").
		WithTarget(timeseriesQuery("A", fpCbrGoldRubSQL)).
		Unit(units.Rubles).
		Decimals(2)
}

// fpGoldOzPanel — панель 2: золото в $/oz во всю ширину — четыре длинных дневных ряда, которые
// сравниваются между собой (цены на одной шкале). A — ЦБ-паритет (самоджойн витрины, см.
// fpDcfParitySQL), B/C/D — рыночные ряды v_gold_prices; одна единица на панель: $ за тройскую унцию
// у всех четырёх, пер-серийные override'ы единиц не нужны.
func fpGoldOzPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(2).
		Title("Золото, $/oz: паритет ЦБ и рынок").
		Span(24).
		Description("Золото в долларах за тройскую унцию. A — ЦБ-паритет: учётная цена ₽/г ÷ курс " +
			"USD/RUB × 31.1034768 (граммов в тройской унции) — цена, которая следует из двух рядов " +
			"ЦБ, а не из сделок; джойн по дате в SQL, потому что ряды ЦБ разной длины. B — МосБиржа " +
			"фикс (v_gold_prices, series = 'moex_fix_usd'), C — COMEX-фронт ('comex_front'), " +
			"D — bold.report ('bold'; в v_series_catalog титул источника bold.report) — рыночные " +
			"котировки. История у B/C с 2024, у D — с 2014 (охват источника): короткий ряд — данные, " +
			"а не пропуск. Все значения — $/oz.").
		WithTarget(timeseriesQuery("A", fpDcfParitySQL)).
		WithTarget(timeseriesQuery("B", fpGoldPriceSQL("moex_fix_usd", `"МосБиржа фикс, $/oz"`))).
		WithTarget(timeseriesQuery("C", fpGoldPriceSQL("comex_front", `"COMEX-фронт, $/oz"`))).
		WithTarget(timeseriesQuery("D", fpGoldPriceSQL("bold", `"bold.report, $/oz"`))).
		Unit(units.Dollars)
}

// fpUsdRubPanel — панель 3: официальный курс USD/RUB. Единица short с четырьмя знаками, а не
// currencyRUB: курс — число рублей за доллар, и печатать «83,92 ₽» рядом с ценами в ₽ читалось бы
// как сумма, а не как котировка.
func fpUsdRubPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(3).
		Title("Курс USD/RUB").
		Span(spanHalf).
		Description("Официальный курс доллара к рублю (v_cbr_macro, series = 'cbr_currency_usd') — " +
			"₽ за 1 USD, уровень на дату. Знаменатель ЦБ-паритета золота (панель 2) и вход рублёвой " +
			"переоценки в DCF-модели. Единица — число с четырьмя знаками (decimals 4): ЦБ котирует " +
			"курс с четырьмя знаками после запятой.").
		WithTarget(timeseriesQuery("A", fpUsdRubSQL)).
		Unit(units.Short).
		Decimals(4)
}

// fpRatesOfzPanel — панель 4: ставка ЦБ и кривая ОФЗ — локальный контур ставок DCF. Теноры B..E
// сейчас по одному наблюдению (снимок кривой, история наполняется), поэтому точки с фикс-цветом:
// сплошная линия по одной точке не читается, а цвет кодирует срок (оттенок синего растёт с тенором)
// и не должен зависеть от порядка таргетов в палитре classic.
func fpRatesOfzPanel() *timeseries.PanelBuilder {
	panel := fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(4).
		Title("Ставка ЦБ и кривая ОФЗ (локальный контур DCF)").
		Span(spanHalf).
		Description("Локальный контур ставок DCF: ключевая ставка ЦБ (v_cbr_macro, series = " +
			"'cbr_key_rate', % годовых — уровень на дату решений) и доходности кривой бескупонной " +
			"доходности ОФЗ (v_ofz_curve; теноры 1y/3y/5y/10y, % годовых). Теноры — снимок по одному " +
			"наблюдению (история кривой наполняется): они нарисованы точками, а не линиями, и цвет " +
			"фиксирован по сроку. Ставка и кривая — входы ставки дисконтирования модели.").
		WithTarget(timeseriesQuery("A", fpKeyRateSQL)).
		Unit(units.Percent)
	for _, tenor := range []struct {
		refID, code, alias, color string
	}{
		{"B", "1y", `"ОФЗ 1y, %"`, "super-light-blue"},
		{"C", "3y", `"ОФЗ 3y, %"`, "light-blue"},
		{"D", "5y", `"ОФЗ 5y, %"`, "blue"},
		{"E", "10y", `"ОФЗ 10y, %"`, "dark-blue"},
	} {
		panel = panel.
			WithTarget(timeseriesQuery(tenor.refID, fpOfzTenorSQL(tenor.code, tenor.alias))).
			OverrideByQuery(tenor.refID, append(pointsOnlyProps(), fpFixedColorProp(tenor.color)))
	}
	return panel
}

// fpGlobalRatesPanel — панель 5: глобальный контур ставок — реальная доходность TIPS, номинальная
// доходность UST, ставка ФРС и инфляционные ожидания. Все четыре ряда — уровни в % годовых, одна
// единица и одна шкала: пары «номинальная − реальная = ожидания» разложимы прямо на графике.
func fpGlobalRatesPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(5).
		Title("Глобальный контур: реальная ставка и ожидания").
		Span(spanHalf).
		Description("Глобальный контур ставок (v_fred_macro, % годовых; уровни на дату, не темпы): " +
			"DFII10 — реальная доходность TIPS 10 лет, DGS10 — номинальная доходность UST 10 лет, " +
			"FEDFUNDS — эффективная ставка ФРС, T5YIE — 5-летние инфляционные ожидания рынка " +
			"(breakeven). DGS10 − DFII10 ≈ T5YIE — разложение номинальной доходности на реальную " +
			"ставку и ожидания; с локальными ставками панели 4 эти уровни сопоставимы только как " +
			"ориентир другой денежной единицы.").
		WithTarget(timeseriesQuery("A", fpFredSQL("DFII10", `"DFII10, %"`))).
		WithTarget(timeseriesQuery("B", fpFredSQL("DGS10", `"UST 10y, %"`))).
		WithTarget(timeseriesQuery("C", fpFredSQL("FEDFUNDS", `"Fed Funds, %"`))).
		WithTarget(timeseriesQuery("D", fpFredSQL("T5YIE", `"5y breakeven, %"`))).
		Unit(units.Percent)
}

// fpPlzlPricePanel — панель 6: цена акции PLZL. Рынок, к которому относится прогноз: цена
// сопоставляется с НАВ на акцию последнего прогона модели и с ценой золота.
func fpPlzlPricePanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(6).
		Title("PLZL: цена акции").
		Span(spanHalf).
		Description("Цена акции Полюса (v_stock_prices, code = 'PLZL', close — цена закрытия, ₽ за " +
			"акцию). Рынок, к которому относится прогноз: цена сопоставляется с НАВ на акцию " +
			"последнего прогона модели и с ценой золота; сама она — не прогноз и не оценка.").
		WithTarget(timeseriesQuery("A", fpPlzlCloseSQL)).
		Unit(units.Rubles)
}
