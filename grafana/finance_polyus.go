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

// FinancePolyusUID — uid дашборда «Полюс (PLZL) — финансы и прогноз»: попадает и в spec, и в
// metadata.name (инвариант Manifest), и в имя файла артефакта.
const FinancePolyusUID = "finance-polyus"

// firstFinancePolyusRowID — id первого ряда (31): нулевой или совпавший с панелью id Grafana
// трактует как «не задан» и заменит своим, поэтому ряды 31–34 нумеруются вне диапазона панелей 1..19.
const firstFinancePolyusRowID = 31

// secondFinancePolyusRowID — id ряда 32 «Операционные показатели»: нумерация рядов идёт подряд вне
// диапазона панелей (см. firstFinancePolyusRowID).
const secondFinancePolyusRowID = 32

// thirdFinancePolyusRowID — id ряда 33 «Финансовые показатели»: нумерация рядов идёт подряд вне
// диапазона панелей (см. firstFinancePolyusRowID).
const thirdFinancePolyusRowID = 33

// fourthFinancePolyusRowID — id ряда 34 «Прогноз и точность»: нумерация рядов идёт подряд вне
// диапазона панелей (см. firstFinancePolyusRowID).
const fourthFinancePolyusRowID = 34

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

// SQL-контракт ряда 32 (спека §5, дословно). Ряд читает операционный датапак Полюса через одну
// витрину v_company_operating: ключ серии — тройка asset + metric + frequency, и частота обязана
// стоять в WHERE (у годового, полугодового и квартального ряда одной метрики совпадают и metric,
// и date). Метрики — только из датапака: дефект легаси-экспорта здесь был LIMIT на timeseries и
// деление полугодий на два (v_company_operating хранит значение ЗА период).

// fpConsolidatedAsset — строка сводных итогов компании в v_company_operating: уровень компании
// (панели 7, 9, 10, 11). У панели 8 — уровень месторождений, и консолидированный ряд там исключён.
const fpConsolidatedAsset = "CONSOLIDATED OPERATING RESULTS"

// fpGoldOutputMetric — выпуск аффинированного золота, koz: единственная метрика панелей 7 и 8.
const fpGoldOutputMetric = "Total refined gold output"

// fpOperatingTemplate — срез v_company_operating «дата + значение» по активу, метрике и частоте: одна
// форма у всех панелей ряда. %s по порядку: алиас серии, актив, метрика, частота. Алиас приходит
// строкой в кавычках (… AS "Год, koz"): единица измерения подписана прямо в алиасе, потому что у
// панели одна единица Grafana на показатели разных шкал (koz, kT, тыс. м³, м³/т, г/т, %).
const fpOperatingTemplate = `SELECT date AS time, value AS %s
FROM v_company_operating
WHERE company = 'PLZL' AND asset = '%s'
  AND metric = '%s' AND frequency = '%s'
  AND $__timeFilter(date)
ORDER BY date
`

// fpOperatingSQL собирает запрос операционного показателя по шаблону.
func fpOperatingSQL(asset, metric, frequency, alias string) string {
	return fmt.Sprintf(fpOperatingTemplate, alias, asset, metric, frequency)
}

// SQL-контракт ряда 33 (спека §5, дословно). Ряд читает финансовую отчётность Полюса через витрину
// v_company_financials по паре (metric, period_type): у capex и gold_sold полугодовые и годовые
// строки лежат на те же годы, поэтому тип периода обязателен в WHERE. Ось времени здесь
// ВЫЧИСЛЯЕТСЯ из строки периода (`period`: '2024H1' → 1 января, '2024H2' → 1 июля, '2024FY' → 1
// января года), а $__timeFilter не применяется осознанно: ось производная, и окно дашборда клипает
// построенный ряд на клиенте (паттерн dxy/crack золотого дашборда; оговорка — в Description каждой
// панели ряда). Значения — Nullable(Float64): coalesce не нужен, прочерк документа остаётся
// разрывом ряда, а не нулём.

// fpFinancialsHalfYearTemplate — срез v_company_financials «time + value» по метрике за полугодия:
// ось ветвится multiIf по хвосту строки периода (H1 → 1 января, H2 → 1 июля). %s по порядку: алиас
// серии, метрика. Единицы ($ млн, koz, $/oz) подписаны в алиасах — как у шаблона ряда 32.
const fpFinancialsHalfYearTemplate = `SELECT
    multiIf(
        endsWith(period, 'H1'), toDate(concat(substring(period, 1, 4), '-01-01')),
        toDate(concat(substring(period, 1, 4), '-07-01'))
    ) AS time,
    value AS %s
FROM v_company_financials
WHERE company = 'PLZL' AND metric = '%s' AND period_type = 'H'
ORDER BY time
`

// fpFinancialsHalfYearSQL собирает запрос полугодового показателя отчётности по шаблону.
func fpFinancialsHalfYearSQL(metric, alias string) string {
	return fmt.Sprintf(fpFinancialsHalfYearTemplate, alias, metric)
}

// fpFinancialsYearTemplate — тот же срез за год: у period_type = 'FY' ось — одна ветвь, 1 января
// года периода. %s по порядку: алиас серии, метрика.
const fpFinancialsYearTemplate = `SELECT toDate(concat(substring(period, 1, 4), '-01-01')) AS time, value AS %s
FROM v_company_financials
WHERE company = 'PLZL' AND metric = '%s' AND period_type = 'FY'
ORDER BY time
`

// fpFinancialsYearSQL собирает запрос годового показателя отчётности по шаблону.
func fpFinancialsYearSQL(metric, alias string) string {
	return fmt.Sprintf(fpFinancialsYearTemplate, alias, metric)
}

// SQL-контракт ряда 34 (спека §5, дословно). Ряд читает прогнозный контур: волну прогнозов
// (v_gold_forecasts), снимок последнего прогона модели (v_model_inputs) и точность прошлых
// прогнозов (v_forecast_accuracy). Точки own-сценариев НЕ вшиваются в SQL (анти-паттерн
// легаси-экспорта — `UNION ALL SELECT toDateTime('2026-12-30') → 2500`): дата точки вычисляется
// из ярлыка горизонта, а сами значения приходят из витрины.

// fpForecastScenarioTemplate — точка own-сценария: горизонт маппится в календарную дату по самому
// ярлыку, без хардкода годов. `Q#-YYYY` и `YE-YYYY` → 31 декабря года (год — последние четыре
// символа ярлыка через substring(horizon, length(horizon) - 3, 4)), голый `YYYY` → 31 декабря года;
// ярлыки без календарного маппинга (`12M`, `short`) дают NULL и отсекаются фильтром time IS NOT
// NULL — их значения видны в баре панели 17. Волна прогнозов одна на точку: `published` вынесен в
// подвыборку, а внешний предикат берёт последнюю волну (`max(published)`) — иначе вторая волна
// поставила бы на ту же дату горизонта вторую точку. %s по порядку: алиас серии, код сценария.
const fpForecastScenarioTemplate = `SELECT time, midpoint AS %s
FROM (
    SELECT
        multiIf(
            match(horizon, '^(Q[1-4]|YE)-[0-9]{4}$'),
                toDate(concat(substring(horizon, length(horizon) - 3, 4), '-12-31')),
            match(horizon, '^[0-9]{4}$'), toDate(concat(horizon, '-12-31')),
            NULL
        ) AS time,
        midpoint, scenario, author, published
    FROM v_gold_forecasts
)
WHERE author = 'own' AND scenario = '%s' AND time IS NOT NULL
  AND published = (SELECT max(published) FROM v_gold_forecasts)
ORDER BY time
`

// fpForecastScenarioSQL собирает запрос точки own-сценария по шаблону.
func fpForecastScenarioSQL(scenario, alias string) string {
	return fmt.Sprintf(fpForecastScenarioTemplate, alias, scenario)
}

// fpForecastConsensusSQL — бар сценариев и банковского консенсуса: ярлык прогноза собирается из
// автора, сценария, горизонта и вероятности. Горизонт в подписи обязателен: в одной волне банки
// повторяют пару author/scenario на разные сроки («hsbc / point» и «jpm / point» встречаются
// дважды), и без него строки неразличимы. Середину диапазона витрина считает сама (point, иначе
// середина low/high, иначе единственная заданная граница), поэтому бар рисуется по midpoint.
// Вероятность публикуется только у own-сценариев: у банковских строк она NULL и печатается
// прочерком. Волна одна — последняя по published: иначе бар смешал бы прогнозы разных дат.
const fpForecastConsensusSQL = `SELECT
    concat(author, ' / ', scenario, ' [', horizon, '] (', ifNull(toString(toInt32(probability)), '—'), '%)') AS "Прогноз",
    midpoint AS "Середина, $/oz",
    low,
    high
FROM v_gold_forecasts
WHERE published = (SELECT max(published) FROM v_gold_forecasts)
ORDER BY midpoint
`

// fpModelRunSQL — снимок последнего прогона модели: одна строка, поэтому ORDER BY здесь нечему
// упорядочивать (панель 18 — таблица, а не временной ряд). Апсайд в SQL не считается: мост
// «НАВ → цена акции» — остаток фазы 3 роадмапа, и числа показываются как записаны в источнике.
const fpModelRunSQL = `SELECT
    toString(last_run_id) AS "Прогон",
    last_run_date AS "Дата прогона",
    last_nav_per_share AS "НАВ/акция, ₽",
    plzl_close AS "Цена PLZL, ₽",
    gold_moex_fix_usd AS "Золото (fix), $/oz",
    usdrub AS "USD/RUB",
    cbr_key_rate AS "Ключевая, %",
    ofz_5y AS "ОФЗ 5y, %"
FROM v_model_inputs
`

// fpForecastAccuracySQL — точность прошлых прогнозов золота: обе строки волны, включая ещё не
// разрешённую (у неё actual/error_pct пусты до прогона forecast_resolve) — фильтр is_resolved здесь
// осознанно не ставится, поэтому неразрешённая строка видна с прочерками. error_pct в витрине
// знаковая: (predicted − actual) / actual × 100.
const fpForecastAccuracySQL = `SELECT
    forecast_date AS "Дата прогноза",
    target_date AS "Цель",
    predicted AS "Прогноз, $/oz",
    actual AS "Факт, $/oz",
    round(error_pct, 2) AS "Ошибка, %",
    is_resolved AS "Разрешён"
FROM v_forecast_accuracy
WHERE metric = 'xau_q_avg'
ORDER BY target_date DESC
`

// fpOwnScenarios — own-сценарии панели 16 в порядке серий B..D. Цвета кодируют смысл сценария
// (bull — рост, bear — падение, base — центральный), а не порядок таргетов: палитра classic красит
// серии по позиции, и без фиксации цвет точки менялся бы при добавлении ряда. Список повторяют
// тесты панели 16 (TestFinancePolyusForecastTargets, TestFinancePolyusHorizonMapping,
// TestFinancePolyusForecastScenarioPoints): перепутанный сценарий, алиас или цвет обязан ловиться
// тестом, а не только этим комментарием.
var fpOwnScenarios = []struct{ refID, scenario, alias, color string }{
	{"B", "bull", `"own: bull"`, "green"},
	{"C", "base", `"own: base"`, "blue"},
	{"D", "bear", `"own: bear"`, "red"},
}

// fpPanelHeight — высота панелей ряда в строках сетки Grafana: 9 — дефолт NewGridPos() SDK и высота
// панелей соседних дашбордов (gold/key-rate).
const fpPanelHeight = 9

// fpPanelGridPos — явная позиция панели в сетке дашборда (x, y — в строках сетки, w — в колонках).
// Авто-layout SDK ставит панель в курсор, оставшийся от предыдущей, и широкая панель 2 (24 колонки)
// уехала бы в x=12 — за правую границу сетки (24 колонки). Явно заданный GridPos авто-layout не
// переписывает: он трогает только позицию с x=0 и y=0 («не задана»), поэтому у панелей ряда y >= 1.
func fpPanelGridPos(x, y, w uint32) dashboard.GridPos {
	return dashboard.GridPos{X: x, Y: y, W: w, H: fpPanelHeight}
}

// fpRowGridPos — явная позиция ряда в сетке: ряд — полоса во всю ширину (24 колонки) высотой 1.
// Авто-layout ставит ряд в курсор панелей (currentY + высота последней панели), а у явно
// разложенных рядов этот курсор уже уехал вниз: ряд 32 встал бы на y=28 — поверх панели 6. Явно
// заданный GridPos SDK не переписывает: пересчёт трогает только x=0 И y=0, и у первого ряда (31)
// он даёт ровно тот же прямоугольник.
func fpRowGridPos(y uint32) dashboard.GridPos {
	return dashboard.GridPos{X: 0, Y: y, W: 24, H: 1}
}

// fpPanelDefaults — общие визуальные конвенции панелей дашборда: палитра classic, легенда снизу
// таблицей с calcs (последнее/среднее/максимум), tooltip по одной серии, линия шириной 1 без заливки,
// градиента и соединения разрывов (разрыв ряда честно остаётся разрывом). Те же конвенции, что у
// key-rate (спека §6, «визуал переносим»), но своим хелпером: финансы Полюса могут разойтись с
// публикациями ставки, и правка конвенций одного дашборда не должна молча менять артефакт другого.
// Панели добавляют свои Id/Title/Description/GridPos и таргеты поверх.
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

// NewFinancePolyus собирает дашборд «Полюс (PLZL) — финансы и прогноз» (спека §4–§5). Собраны ряды 31
// «Золото, ставки, рынок» (панели 1–6), 32 «Операционные показатели» (панели 7–11), 33 «Финансовые
// показатели» (панели 12–15) и 34 «Прогноз и точность» (панели 16–19): первый — макро-входы прогноза
// (учётная цена золота в ₽/г и $/oz, курс, ставки локального и глобального контура, цена акции),
// второй — операционные итоги датапака (выпуск золота компанией и по активам, руда, вскрыша,
// содержание и извлечение), третий — отчётность (капекс, выручка и EBITDA, продажи золота, TCC на
// унцию), четвёртый — прогнозный контур (факт и точки own-сценариев, бар сценариев и банковского
// консенсуса, снимок последнего прогона модели и точность прошлых прогнозов). Порядок вызовов
// WithRow/WithPanel задаёт нумерацию панелей, а место в сетке — явный GridPos у каждого элемента: у
// панелей fpPanelGridPos, у рядов fpRowGridPos. Ряд 31 раскладывается своей таблицей (верхняя пара —
// цена ₽/г (1) и курс (3), ниже панель 2 во всю ширину, затем ставки (4/5) и цена акции (6)); ряд 32
// начинается за последней панелью ряда 31 (y=37), внутри — две широкие панели выпуска золота (7/8) и
// пары руды (9/10) и качества (11); ряд 33 — за панелью 11 (y=74), внутри пара капекса и годовых
// показателей (12/13) и пара продаж золота и TCC (14/15); ряд 34 — за панелью 15 (y=93), внутри пара
// веера и бара (16/17) и пара таблиц — прогона и точности (18/19). Окно дашборда — «пять лет назад…
// год вперёд»: прогнозные точки ряда 34 лежат НА ДАТАХ ГОРИЗОНТОВ, то есть в будущем (own-сценарии
// волны 2026-10-07 — на 2026-12-31), и с правой границей «now» Grafana отрезала бы их — панель 16
// открывалась бы без своего главного содержания. Год вперёд покрывает текущие точки и горизонты до
// ~октября 2027; панели без будущих данных просто получают справа пустой участок оси. Точные
// координаты панелей фиксирует TestFinancePolyusSpecConventions.
func NewFinancePolyus() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Полюс (PLZL) — финансы и прогноз").
		Uid(FinancePolyusUID).
		Tags([]string{"polyus", "dcf", "article"}).
		Refresh("1h").
		Time("now-5y", "now+1y").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Золото, ставки, рынок").Id(firstFinancePolyusRowID).GridPos(fpRowGridPos(0))).
		WithPanel(fpGoldRubPanel()).
		WithPanel(fpGoldOzPanel()).
		WithPanel(fpUsdRubPanel()).
		WithPanel(fpRatesOfzPanel()).
		WithPanel(fpGlobalRatesPanel()).
		WithPanel(fpPlzlPricePanel()).
		// Ряд 32 — за последней панелью ряда 31 (28 + 9 = 37): без явной позиции авто-layout поставил
		// бы его в курсор панелей, то есть на y=28 — поверх панели 6.
		WithRow(dashboard.NewRowBuilder("Операционные показатели").Id(secondFinancePolyusRowID).GridPos(fpRowGridPos(37))).
		WithPanel(fpGoldOutputPanel()).
		WithPanel(fpGoldOutputByAssetPanel()).
		WithPanel(fpOrePanel()).
		WithPanel(fpStrippingPanel()).
		WithPanel(fpGradeRecoveryPanel()).
		// Ряд 33 — за последней панелью ряда 32 (65 + 9 = 74): без явной позиции авто-layout поставил
		// бы его в курсор панелей, то есть на y=65 — поверх панели 11.
		WithRow(dashboard.NewRowBuilder("Финансовые показатели").Id(thirdFinancePolyusRowID).GridPos(fpRowGridPos(74))).
		WithPanel(fpCapexPanel()).
		WithPanel(fpRevenueEbitdaPanel()).
		WithPanel(fpGoldSoldPanel()).
		WithPanel(fpTccPanel()).
		// Ряд 34 — за последней панелью ряда 33 (84 + 9 = 93): без явной позиции авто-layout поставил
		// бы его в курсор панелей, то есть на y=84 — поверх панели 15.
		WithRow(dashboard.NewRowBuilder("Прогноз и точность").Id(fourthFinancePolyusRowID).GridPos(fpRowGridPos(93))).
		WithPanel(fpGoldForecastPanel()).
		WithPanel(fpForecastConsensusPanel()).
		WithPanel(fpModelRunPanel()).
		WithPanel(fpForecastAccuracyPanel())
}

// fpGoldRubPanel — панель 1: учётная цена золота ЦБ в ₽/г. Рубли с копейками (decimals 2): цена
// котируется до копеек, и без фиксации Grafana округляла бы её по ширине оси. В сетке — левая панель
// верхней пары: цена ₽/г и курс (панель 3) читаются вместе — из них собирается паритет $/oz.
func fpGoldRubPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(1).
		Title("Учётная цена золота ЦБ, ₽/г").
		GridPos(fpPanelGridPos(0, 1, spanHalf)).
		Description("Учётная цена золота Банка России (v_cbr_macro, series = 'cbr_gold') — уровень " +
			"на дату, ₽ за грамм. Это цена для расчётов ЦБ, а не биржевая котировка: из неё и " +
			"официального курса собирается ЦБ-паритет $/oz панели 2. Значения — ₽/г.").
		WithTarget(timeseriesQuery("A", fpCbrGoldRubSQL)).
		Unit(units.Rubles).
		Decimals(2)
}

// fpGoldOzPanel — панель 2: золото в $/oz во всю ширину — четыре длинных дневных ряда, которые
// сравниваются между собой (цены на одной шкале); 24 колонки не встают в пару панелей, поэтому у неё
// своя строка сетки. A — ЦБ-паритет (самоджойн витрины, см. fpDcfParitySQL), B/C/D — рыночные ряды
// v_gold_prices; одна единица на панель: $ за тройскую унцию у всех четырёх, пер-серийные override'ы
// единиц не нужны.
func fpGoldOzPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(2).
		Title("Золото, $/oz: паритет ЦБ и рынок").
		GridPos(fpPanelGridPos(0, 10, 24)).
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

// fpUsdRubPanel — панель 3: официальный курс USD/RUB — правая панель верхней пары (рядом учётная
// цена, панель 1). Единица short с четырьмя знаками, а не currencyRUB: курс — число рублей за
// доллар, и печатать «83,92 ₽» рядом с ценами в ₽ читалось бы как сумма, а не как котировка.
func fpUsdRubPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(3).
		Title("Курс USD/RUB").
		GridPos(fpPanelGridPos(12, 1, spanHalf)).
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
// и не должен зависеть от порядка таргетов в палитре classic. В сетке — левая панель пары ставок
// (рядом глобальный контур, панель 5).
func fpRatesOfzPanel() *timeseries.PanelBuilder {
	panel := fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(4).
		Title("Ставка ЦБ и кривая ОФЗ (локальный контур DCF)").
		GridPos(fpPanelGridPos(0, 19, spanHalf)).
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
// В сетке — правая панель пары ставок: локальный и глобальный контуры рядом.
func fpGlobalRatesPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(5).
		Title("Глобальный контур: реальная ставка и ожидания").
		GridPos(fpPanelGridPos(12, 19, spanHalf)).
		Description("Глобальный контур ставок (v_fred_macro, % годовых; уровни на дату, не темпы): " +
			"DFII10 — реальная доходность TIPS 10 лет, DGS10 — номинальная доходность UST 10 лет, " +
			"FEDFUNDS — эффективная ставка ФРС, T5YIE — 5-летние инфляционные ожидания рынка " +
			"(breakeven). DGS10 − DFII10 ≈ T5YIE — разложение номинальной доходности на реальную " +
			"ставку и ожидания. Единицы те же, что у панели 4 (% годовых), но контур другой: это " +
			"ставки ФРС и рынка США против ключевой ставки ЦБ в рублях — другой контур ставки, а не " +
			"вменённая траектория ставки ЦБ.").
		WithTarget(timeseriesQuery("A", fpFredSQL("DFII10", `"DFII10, %"`))).
		WithTarget(timeseriesQuery("B", fpFredSQL("DGS10", `"UST 10y, %"`))).
		WithTarget(timeseriesQuery("C", fpFredSQL("FEDFUNDS", `"Fed Funds, %"`))).
		WithTarget(timeseriesQuery("D", fpFredSQL("T5YIE", `"5y breakeven, %"`))).
		Unit(units.Percent)
}

// fpPlzlPricePanel — панель 6: цена акции PLZL, последняя панель ряда. Рынок, к которому относится
// прогноз: цена сопоставляется с НАВ на акцию последнего прогона модели и с ценой золота.
func fpPlzlPricePanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(6).
		Title("PLZL: цена акции").
		GridPos(fpPanelGridPos(0, 28, spanHalf)).
		Description("Цена акции Полюса (v_stock_prices, code = 'PLZL', close — цена закрытия, ₽ за " +
			"акцию). Рынок, к которому относится прогноз: цена сопоставляется с НАВ на акцию " +
			"последнего прогона модели и с ценой золота; сама она — не прогноз и не оценка.").
		WithTarget(timeseriesQuery("A", fpPlzlCloseSQL)).
		Unit(units.Rubles)
}

// fpGoldOutputPanel — панель 7: выпуск аффинированного золота компании (сводные итоги), одна метрика
// в трёх частотах датапака — год, полугодие, квартал. Единица у всех трёх одна (koz), поэтому шкала
// общая: ряды сравниваются как уровни за период, а не суммируются. Панель во всю ширину (24 колонки)
// — своя строка сетки: три ряда одной метрики видны только на широкой панели.
func fpGoldOutputPanel() *timeseries.PanelBuilder {
	panel := fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(7).
		Title("Выпуск золота: год, полугодия, кварталы").
		GridPos(fpPanelGridPos(0, 38, 24)).
		Description("Выпуск аффинированного золота компании (v_company_operating: asset = " +
			"'CONSOLIDATED OPERATING RESULTS', metric = 'Total refined gold output') в тысячах " +
			"тройских унций (koz) тремя частотами датапака: A — год, B — полугодие, C — квартал. " +
			"Значение — выпуск ЗА период, а не накопленный итог, поэтому ряды стоят на одной оси как " +
			"уровни и не суммируются между собой. Покрытие частот разное: кварталы в датапаке " +
			"заканчиваются 2022-01, полугодия и годы идут до 2026-01 — обрыв квартального ряда в " +
			"2022-м это данные источника, а не пропуск и не дефект дашборда.").
		Unit(units.Short)
	for _, freq := range []struct {
		refID, code, alias string
	}{
		{"A", "ANNUAL", `"Год, koz"`},
		{"B", "SEMI-ANNUAL", `"Полугодия, koz"`},
		{"C", "QUARTERLY", `"Кварталы, koz"`},
	} {
		panel = panel.WithTarget(timeseriesQuery(freq.refID,
			fpOperatingSQL(fpConsolidatedAsset, fpGoldOutputMetric, freq.code, freq.alias)))
	}
	return panel
}

// fpGoldOutputAssets — активы панели 8 в порядке серий A..I с их метрикой выпуска. Восемь активов
// считаются 'Total refined gold output': у шести серии идут до 2026-01, у ZAPADNOYE история
// обрывается на 2024-01, а у Sukhoi Log строк ещё нет (проект — см. комментарий в таблице).
// ALLUVIALS — единственный со своей метрикой 'Refined gold production'. Сводных итогов компании
// ('CONSOLIDATED OPERATING RESULTS') в списке нет: они на панели 7, и компания с активом на одной оси
// читались бы как двойной счёт. Оговорки по охвату (ZAPADNOYE → 2024; Sukhoi Log без строк) — в
// Description панели: обрыв и пустота это охват источника, а не ноль добычи.
var fpGoldOutputAssets = []struct{ refID, asset, metric string }{
	{"A", "OLIMPIADA", fpGoldOutputMetric},
	{"B", "BLAGODATNOYE", fpGoldOutputMetric},
	{"C", "NATALKA", fpGoldOutputMetric},
	{"D", "KURANAKH", fpGoldOutputMetric},
	{"E", "VERNINSKOYE2", fpGoldOutputMetric},
	// ALLUVIALS — россыпной актив: в датапаке его выпуск ведётся строкой 'Refined gold production'
	// (2008→2024, 17 годовых точек), а 'Total refined gold output' для него не публикуется вовсе.
	{"F", "ALLUVIALS", "Refined gold production"},
	// Sukhoi Log — проект до запуска ЗИФ: метрики выпуска в датапаке нет (только руда и вскрыша),
	// поэтому серия пуста до появления данных; метрика — как у остальных рудников.
	{"G", "Sukhoi Log", fpGoldOutputMetric},
	{"H", "TITIMUKHTA", fpGoldOutputMetric},
	{"I", "ZAPADNOYE", fpGoldOutputMetric},
}

// fpGoldOutputByAssetPanel — панель 8: тот же выпуск золота, но годовым срезом по активам — девять
// серий вместо одной. Алиас каждой серии — «<актив>, koz»: без имени актива в легенде было бы девять
// одинаковых «value». Панель во всю ширину (24 колонки) — девять серий и своя строка сетки.
func fpGoldOutputByAssetPanel() *timeseries.PanelBuilder {
	panel := fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(8).
		Title("Выпуск золота по активам (год), koz").
		GridPos(fpPanelGridPos(0, 47, 24)).
		Description("Выпуск золота по активам, koz/год (v_company_operating, frequency = 'ANNUAL'): " +
			"восемь активов считаются метрикой 'Total refined gold output' (аффинированное золото), " +
			"ALLUVIALS — своей 'Refined gold production' (россыпь в датапаке ведётся отдельной строкой). " +
			"Девять серий — по одной на актив; консолидированный итог компании ('CONSOLIDATED " +
			"OPERATING RESULTS') исключён: он на панели 7, и уровень компании на одной оси с активами " +
			"читался бы как двойной счёт выпуска. Оговорки по охвату: история ZAPADNOYE заканчивается " +
			"2024 годом, а у Sukhoi Log строк выпуска в датапаке нет — проект до запуска ЗИФ, серия " +
			"появится вместе с данными; обрыв и пустота — это охват источника, а не дефект дашборда и " +
			"не ноль добычи. Значения — ЗА год, в одних единицах, поэтому серии сопоставимы напрямую.").
		Unit(units.Short)
	for _, target := range fpGoldOutputAssets {
		panel = panel.WithTarget(timeseriesQuery(target.refID,
			fpOperatingSQL(target.asset, target.metric, "ANNUAL", fmt.Sprintf(`"%s, koz"`, target.asset))))
	}
	return panel
}

// fpOrePanel — панель 9: добыча и переработка руды компании за год, kT — левая панель пары (рядом
// вскрыша, панель 10). Обе серии — объёмы в одной единице (тыс. т), поэтому сопоставимы напрямую:
// разрыв между добычей и переработкой — это склад руды, а не ошибка масштаба.
func fpOrePanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(9).
		Title("Руда: добыто и переработано (год), kT").
		GridPos(fpPanelGridPos(0, 56, spanHalf)).
		Description("Руда компании за год, тысячи тонн (v_company_operating: asset = 'CONSOLIDATED " +
			"OPERATING RESULTS', frequency = 'ANNUAL'): A — добыто руды ('Ore mined'), B — " +
			"переработано руды ('Ore processed'). Обе серии — объёмы в одной единице (kT), поэтому " +
			"сопоставимы напрямую, и разрыв между ними читается как склад руды, а не как разница " +
			"масштаба. Значения — ЗА год, а не накопленный итог.").
		WithTarget(timeseriesQuery("A", fpOperatingSQL(fpConsolidatedAsset, "Ore mined", "ANNUAL", `"Добыто руды, kT"`))).
		WithTarget(timeseriesQuery("B", fpOperatingSQL(fpConsolidatedAsset, "Ore processed", "ANNUAL", `"Переработано руды, kT"`))).
		Unit(units.Short)
}

// fpStrippingPanel — панель 10: вскрыша и коэффициент вскрыши за год. A — объём вскрыши (тыс. м³),
// B — коэффициент вскрыши (м³/т): размерности разные (объём и отношение), поэтому B — на правой оси
// (axisPlacementRightProp), иначе м³/т лежали бы линией у нуля. В сетке — правая панель пары,
// рядом с рудой (панель 9): вскрыша — часть перемещённой горной массы.
func fpStrippingPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(10).
		Title("Вскрыша и коэффициент вскрыши").
		GridPos(fpPanelGridPos(12, 56, spanHalf)).
		Description("Вскрышные работы компании за год (v_company_operating: asset = 'CONSOLIDATED "+
			"OPERATING RESULTS', frequency = 'ANNUAL'): A — вскрыша ('Stripping', тыс. м³), B — "+
			"коэффициент вскрыши ('Stripping ratio', м³/т) — сколько кубометров породы снято на "+
			"тонну руды. Размерности серий разные (объём против отношения), поэтому B нарисован на "+
			"правой оси: на левой с тыс. м³ он был бы нечитаемой линией у нуля. Значения — ЗА год.").
		WithTarget(timeseriesQuery("A", fpOperatingSQL(fpConsolidatedAsset, "Stripping", "ANNUAL", `"Вскрыша, тыс. м³"`))).
		WithTarget(timeseriesQuery("B", fpOperatingSQL(fpConsolidatedAsset, "Stripping ratio", "ANNUAL", `"Коэффициент вскрыши, м³/т"`))).
		OverrideByQuery("B", []dashboard.DynamicConfigValue{axisPlacementRightProp()}).
		Unit(units.Short)
}

// fpGradeRecoveryPanel — панель 11: содержание золота в руде и извлечение за год. A — г/т, B — %:
// размерности разные, поэтому извлечение — на правой оси (axisPlacementRightProp). Пара — вход
// металлургического баланса: содержание говорит, сколько металла в руде, извлечение — какая доля из
// него уходит в продукт.
func fpGradeRecoveryPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(11).
		Title("Содержание и извлечение").
		GridPos(fpPanelGridPos(0, 65, spanHalf)).
		Description("Качество руды и металлургия компании за год (v_company_operating: asset = "+
			"'CONSOLIDATED OPERATING RESULTS', frequency = 'ANNUAL'): A — среднее содержание золота в "+
			"добытой руде ('Average grade in ore mined', г/т), B — извлечение ('Recovery', %) — какая "+
			"доля металла уходит в продукт. Размерности серий разные, поэтому B нарисован на правой "+
			"оси: на левой шкале с г/т проценты были бы линией у нуля. A и B вместе — вход "+
			"металлургического баланса: падение содержания при том же выпуске означает рост объёмов "+
			"переработки. Значения — ЗА год.").
		WithTarget(timeseriesQuery("A", fpOperatingSQL(fpConsolidatedAsset, "Average grade in ore mined", "ANNUAL", `"Содержание в руде, г/т"`))).
		WithTarget(timeseriesQuery("B", fpOperatingSQL(fpConsolidatedAsset, "Recovery", "ANNUAL", `"Извлечение, %"`))).
		OverrideByQuery("B", []dashboard.DynamicConfigValue{axisPlacementRightProp()}).
		Unit(units.Short)
}

// fpCapexPanel — панель 12: капекс за полугодия (A) и год (B), $ млн — левая панель верхней пары
// ряда 33 (рядом годовые выручка и EBITDA, панель 13): капекс — инвестиционный выход DCF, выручка и
// EBITDA — база операционного результата. Ось времени строится из строки периода, а не из даты
// наблюдения (см. комментарий к шаблонам ряда): полугодовая серия ветвится multiIf по H1/H2,
// годовая — одна ветвь на 1 января.
func fpCapexPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(12).
		Title("Капекс, $ млн: полугодия и год").
		GridPos(fpPanelGridPos(0, 75, spanHalf)).
		Description("Капитальные затраты Полюса (v_company_financials, metric = 'capex'), $ млн: A — " +
			"полугодия (period_type = 'H'), B — год ('FY'). Ось времени вычисляется из строки периода " +
			"(`period`), а не из даты наблюдения: '2024H1' → 1 января, '2024H2' → 1 июля, '2024FY' → " +
			"1 января года. $__timeFilter в запросе не применяется осознанно (тот же приём, что у " +
			"dxy/crack золотого дашборда): окно дашборда клипает вывод на клиенте. Значения — из " +
			"отчётных периодов, уровни ЗА период (не накопленный итог).").
		WithTarget(timeseriesQuery("A", fpFinancialsHalfYearSQL("capex", `"Капекс, полугодие, $ млн"`))).
		WithTarget(timeseriesQuery("B", fpFinancialsYearSQL("capex", `"Капекс, год, $ млн"`))).
		Unit(units.Dollars)
}

// fpRevenueEbitdaPanel — панель 13: выручка и скорректированная EBITDA за год, $ млн. Обе серии —
// годовые ('FY'), поэтому ось у них одна ветвь без multiIf. Ряд EBITDA короче: метрика заведена в
// датапаке с 2023FY — оговорка об этом в Description, потому что обрыв читается как дефект.
func fpRevenueEbitdaPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(13).
		Title("Выручка и EBITDA (год), $ млн").
		GridPos(fpPanelGridPos(12, 75, spanHalf)).
		Description("Выручка и скорректированная EBITDA Полюса за год (v_company_financials, " +
			"period_type = 'FY'), $ млн: A — 'revenue', B — 'adjusted_ebitda'. Ось времени вычисляется " +
			"из строки периода (`period`): '2024FY' → 1 января 2024 года; $__timeFilter в запросе не " +
			"применяется осознанно (тот же приём, что у dxy/crack золотого дашборда) — окно дашборда " +
			"клипает вывод на клиенте. Ряд B короче: 'adjusted_ebitda' заведён в датапаке с 2023FY, и " +
			"его начало — охват источника, а не пропуск и не дефект дашборда. Значения — уровни ЗА год.").
		WithTarget(timeseriesQuery("A", fpFinancialsYearSQL("revenue", `"Выручка, $ млн"`))).
		WithTarget(timeseriesQuery("B", fpFinancialsYearSQL("adjusted_ebitda", `"Скорр. EBITDA, $ млн"`))).
		Unit(units.Dollars)
}

// fpGoldSoldPanel — панель 14: продажи золота (koz) за полугодия и год — левая панель нижней пары
// ряда 33 (рядом TCC, панель 15). Продажи — отгруженный металл, а не выпуск (выпуск — панели 7/8).
// Единица short, как у выпуска: koz — не валюта, currencyUSD дал бы «$» на тысячах унций.
func fpGoldSoldPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(14).
		Title("Продажи золота, koz").
		GridPos(fpPanelGridPos(0, 84, spanHalf)).
		Description("Продажи золота Полюса (v_company_financials, metric = 'gold_sold'), тыс. тройских " +
			"унций (koz): A — полугодия (period_type = 'H'), B — год ('FY'). Ось времени вычисляется из " +
			"строки периода (`period`): '2024H1' → 1 января, '2024H2' → 1 июля, '2024FY' → 1 января " +
			"года. $__timeFilter в запросе не применяется осознанно (тот же приём, что у dxy/crack " +
			"золотого дашборда): окно дашборда клипает вывод на клиенте. Продажи — отгруженный металл, " +
			"а не выпуск (выпуск — панели 7 и 8): значения из отчётных периодов, уровни ЗА период, а не " +
			"накопленный итог.").
		WithTarget(timeseriesQuery("A", fpFinancialsHalfYearSQL("gold_sold", `"Продажи, полугодие, koz"`))).
		WithTarget(timeseriesQuery("B", fpFinancialsYearSQL("gold_sold", `"Продажи, год, koz"`))).
		Unit(units.Short)
}

// fpTccPanel — панель 15: TCC на унцию, $/oz — единственная серия ряда и только отчётные полугодия.
// Точки вместо линии (pointsOnlyProps): ряд короткий и полугодовой, и сплошная линия по нему
// читалась бы как непрерывный ряд. AISC в витринах не публикуется — оговорка в Description.
func fpTccPanel() *timeseries.PanelBuilder {
	return fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(15).
		Title("TCC на унцию, $/oz").
		GridPos(fpPanelGridPos(12, 84, spanHalf)).
		Description("TCC (total cash costs) на унцию проданного золота (v_company_financials, metric = "+
			"'tcc_per_ounce'), $/oz, только отчётные полугодия (period_type = 'H'): короткий ряд — по "+
			"отчётным полугодиям, поэтому серия нарисована точками, а не линией; охват полугодий — в "+
			"данных, панель его не фиксирует. Ось времени вычисляется из строки периода (`period`): "+
			"'YYYYH1' → 1 января, 'YYYYH2' → 1 июля; $__timeFilter в запросе не применяется осознанно — "+
			"окно дашборда клипает вывод на клиенте. AISC в контуре считается вне витрин: в "+
			"v_company_financials он не публикуется.").
		WithTarget(timeseriesQuery("A", fpFinancialsHalfYearSQL("tcc_per_ounce", `"TCC, $/oz"`))).
		OverrideByQuery("A", pointsOnlyProps()).
		Unit(units.Dollars)
}

// fpGoldForecastPanel — панель 16: веер прогноза. A — факт (дневной ряд COMEX-фронта, форма панели
// 2-C), B/C/D — точки own-сценариев на дате горизонта: точки, а не линия, потому что горизонт у
// сценария один и траектории между фактом и прогнозом в витрине нет; цвет фиксирован по сценарию
// (см. fpOwnScenarios). В сетке — левая панель пары ряда 34, рядом бар сценариев и консенсуса (17).
func fpGoldForecastPanel() *timeseries.PanelBuilder {
	panel := fpPanelDefaults(timeseries.NewPanelBuilder()).
		Id(16).
		Title("Золото, $/oz: факт и прогнозные точки (own-сценарии)").
		GridPos(fpPanelGridPos(0, 94, spanHalf)).
		Description("Прогноз актуальной волны — последней по `published` (v_gold_forecasts, $/oz): " +
			"A — факт, цена COMEX-фронта (v_gold_prices, series = 'comex_front'), дневным рядом; " +
			"B/C/D — точки собственных сценариев той же волны (author = 'own'): bull («own: bull»), " +
			"base и bear. Дата точки вычисляется из самого ярлыка горизонта, без хардкода годов: " +
			"`Q#-YYYY` и `YE-YYYY` → 31 декабря года, голый `YYYY` → 31 декабря года; ярлыки `12M` и " +
			"`short` в календарь не маппятся и точками не рисуются. Их заменяет панель 17: там видны " +
			"и вероятности сценариев, и диапазоны low/high, и немэппируемые горизонты — горизонт " +
			"стоит прямо в подписи бара. Точки лежат в будущем (даты горизонтов), поэтому окно " +
			"дашборда продлено на год вперёд (`now+1y`) — иначе Grafana отрезала бы их. Точка — " +
			"уровень на дату горизонта, а не траектория: сети между точками нет. Значения — $/oz.").
		WithTarget(timeseriesQuery("A", fpGoldPriceSQL("comex_front", `"COMEX-фронт, $/oz"`))).
		Unit(units.Dollars)
	for _, scenario := range fpOwnScenarios {
		panel = panel.
			WithTarget(timeseriesQuery(scenario.refID, fpForecastScenarioSQL(scenario.scenario, scenario.alias))).
			OverrideByQuery(scenario.refID, append(pointsOnlyProps(), fpFixedColorProp(scenario.color)))
	}
	return panel
}

// fpForecastConsensusPanel — панель 17: бар сценариев и банковского консенсуса. Подпись бара —
// ярлык прогноза (XField «Прогноз», как у gold-панели 8): автор, сценарий, горизонт и вероятность
// прямо в подписи — без горизонта повторяющиеся пары author/scenario («hsbc / point», «jpm /
// point») были бы неразличимы. Данные — форма 1 + table: строковые колонки в timeseries-формат не
// укладываются. В сетке — правая панель пары ряда 34, рядом веер (16).
func fpForecastConsensusPanel() *barchart.PanelBuilder {
	return barchart.NewPanelBuilder().
		Id(17).
		Title("Прогноз цены золота: сценарии и консенсус (bar)").
		GridPos(fpPanelGridPos(12, 94, spanHalf)).
		Description("Прогноз цены золота актуальной волны — последней по `published` " +
			"(v_gold_forecasts, $/oz): бар по середине диапазона (midpoint) — у точки это сама " +
			"точка, у банковского диапазона середина low/high, у единственной заданной границы сама " +
			"граница. Подпись бара собирается из автора, сценария, горизонта и вероятности: " +
			"горизонт обязателен — в одной волне банки дают по два прогноза на разные сроки (" +
			"например, «hsbc / point» на 2027 и на YE-2026), и без него строки были бы одинаковыми. " +
			"Сценарии собственной сетки (author = 'own': bull, base, bear) подписаны вместе с " +
			"вероятностью (25/40/35%); у банковского консенсуса (citi, goldman, hsbc, jpm, ubs) " +
			"вероятность не публикуется — в подписи прочерк. Горизонты `12M` и `short` видны здесь " +
			"в подписи и точками на панели 16 не рисуются: у них нет календарного маппинга. " +
			"low и high — справка к бару: у точечных прогнозов они пусты. Значения — $/oz.").
		Datasource(datasourceRef()).
		XField("Прогноз").
		WithTarget(tableQuery("A", fpForecastConsensusSQL)).
		Unit(units.Dollars)
}

// fpModelRunPanel — панель 18: снимок последнего прогона DCF-модели одной строкой. Значения — как
// записаны в источнике: апсайд (мост «НАВ → цена акции») в v1 не считается — это остаток фазы 3
// роадмапа, и таблица показывает НАВ и цену рядом, не выводя из них оценку. В сетке — левая
// панель нижней пары ряда 34, рядом точность прошлых прогнозов (19).
func fpModelRunPanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Id(18).
		Title("Последний прогон модели (table)").
		GridPos(fpPanelGridPos(0, 103, spanHalf)).
		Description("Снимок последнего прогона DCF-модели (v_model_inputs, одна строка): прогон, " +
			"дата, НАВ на акцию (₽), цена PLZL (₽) и рыночные входы модели — золото (fix, $/oz), " +
			"курс USD/RUB, ключевая ставка (%), ОФЗ 5 лет (%). Разница НАВ и цены показана как " +
			"есть: апсайд НЕ считается — мост «НАВ → цена акции» остаётся остатком фазы 3 " +
			"роадмапа. Числа показаны так, как записаны в источнике (точность Float32 не " +
			"пересчитывается). Единицы колонок подписаны в их заголовках — панельной единицы у " +
			"таблицы нет.").
		Datasource(datasourceRef()).
		WithTarget(tableQuery("A", fpModelRunSQL))
}

// fpForecastAccuracyPanel — панель 19: точность прошлых прогнозов. Фильтра is_resolved в SQL нет
// осознанно: показываются обе строки волны, включая неразрешённую — факт, ошибка и статус у неё
// пусты до прогона forecast_resolve, и это видно в таблице, а не скрыто. В сетке — правая панель
// нижней пары ряда 34, рядом снимок прогона (18).
func fpForecastAccuracyPanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Id(19).
		Title("Точность прошлых прогнозов (table)").
		GridPos(fpPanelGridPos(12, 103, spanHalf)).
		Description("Точность прошлых прогнозов (v_forecast_accuracy, metric = 'xau_q_avg'): обе " +
			"строки волны, включая ещё не разрешённую — у неё факт, ошибка и статус пусты, пока " +
			"forecast_resolve не сверит цель с фактом. «Ошибка, %» знаковая: (прогноз − факт) / " +
			"факт × 100, «+» — прогноз выше факта. Строки отсортированы по цели " +
			"(target_date DESC) — свежая цель сверху.").
		Datasource(datasourceRef()).
		WithTarget(tableQuery("A", fpForecastAccuracySQL))
}
