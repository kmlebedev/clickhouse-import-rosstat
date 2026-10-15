package main

import (
	"fmt"

	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/table"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// KeyRatePublicationsUID — uid дашборда «Ключевая ставка — публикации» (контур ставки): попадает
// и в spec, и в metadata.name (инвариант Manifest), и в имя файла артефакта.
const KeyRatePublicationsUID = "key-rate-publications"

// firstKeyRateRowID — id первого ряда (21): нулевой или совпавший с панелью id Grafana трактует
// как «не задан» и заменит своим, поэтому ряды нумеруются вне диапазона панелей 1..19.
const firstKeyRateRowID = 21

// krMacroValueTemplate — срез витрины v_cbr_macro «дата + значение» по коду ряда: так читаются
// ключевая ставка (панель 1) и инфляционные ожидания (панель 5) — форма одна, различаются только
// код ряда и алиас. Первый %s — алиас серии, второй — код ряда.
const krMacroValueTemplate = `SELECT date AS time, value AS %s
FROM v_cbr_macro
WHERE series = '%s' AND $__timeFilter(date)
ORDER BY date
`

// krMacroValueSQL собирает запрос ряда v_cbr_macro по шаблону.
func krMacroValueSQL(series, alias string) string {
	return fmt.Sprintf(krMacroValueTemplate, alias, series)
}

// krDecisionsSQL — точки решений ЦБ: дата заседания и принятая ставка из сводки макроопроса.
// Фильтр — по meeting_date: время серии это дата заседания, а не дата публикации резюме.
const krDecisionsSQL = `SELECT meeting_date AS time, decision_rate AS "Решение ЦБ, % годовых"
FROM v_cbr_mp_summary
WHERE $__timeFilter(meeting_date)
ORDER BY meeting_date
`

// krWeeklyAnnualizedTemplate — годовое выражение недельного индекса: средний недельный индекс
// (100 = без изменения) возводится в степень 52 — 52 недели в году. %s по порядку: выражение
// времени, алиас серии, предикат года и колонка сортировки — ряды A/B различаются только ими
// (B — календарное наложение: время сдвинуто на +1 год, чтобы прошлый год лёг поверх текущего).
const krWeeklyAnnualizedTemplate = `SELECT %s AS time, pow(avg(value) / 100, 52) - 1 AS %s
FROM v_rosstat_macro AS w
INNER JOIN v_series_catalog AS c ON c.source = w.source AND c.series = w.series
WHERE c.frequency = 'W' AND toYear(date) = %s
GROUP BY date
ORDER BY %s
`

// krWeeklyAnnualizedSQL собирает запрос годового выражения по шаблону.
func krWeeklyAnnualizedSQL(timeExpr, alias, yearFilter, orderBy string) string {
	return fmt.Sprintf(krWeeklyAnnualizedTemplate, timeExpr, alias, yearFilter, orderBy)
}

// krWeeklyRisersSQL — широта недельного роста: countIf считает недельные ряды, подорожавшие
// за неделю (индекс > 100). Это доля, а не индекс, поэтому единица панели — percent.
const krWeeklyRisersSQL = `SELECT date AS time, countIf(value > 100) / count() * 100 AS "Доля подорожавших, %"
FROM v_rosstat_macro AS w
INNER JOIN v_series_catalog AS c ON c.source = w.source AND c.series = w.series
WHERE c.frequency = 'W' AND $__timeFilter(date)
GROUP BY date
ORDER BY date
`

// krCPIAnnualizedSQL — ИПЦ: в витрине индекс потребительских цен (100 = без изменения),
// поэтому годовое выражение — pow(индекс/100, 12) - 1.
const krCPIAnnualizedSQL = `SELECT date AS time, pow(value / 100, 12) - 1 AS "ИПЦ, годовое выражение"
FROM v_rosstat_macro
WHERE series = 'Индексы потребительских цен на товары и услуги по Российской Федерации'
  AND $__timeFilter(date)
ORDER BY date
`

// krBaseInflationAnnualizedSQL — базовая инфляция: в витрине месячный прирост в % (м/м),
// поэтому годовое выражение — pow(1 + прирост/100, 12) - 1. Формула другая, чем у ИПЦ
// (индекс против темпа): читать базовую как индекс нельзя, и это подписано в Description панели 4.
const krBaseInflationAnnualizedSQL = `SELECT date AS time, pow(1 + value / 100, 12) - 1 AS "Базовая инфляция, годовое выражение"
FROM v_cbr_macro
WHERE series = 'Базовый ИПЦ' AND $__timeFilter(date)
ORDER BY date
`

// krBusinessExpectationsSQL — ценовые ожидания бизнеса (SA) в пунктах: единственная серия панели 5
// не в процентах, поэтому её единица задаётся пер-серийным override'ом (см. krExpectationsPanel).
const krBusinessExpectationsSQL = `SELECT date AS time, value AS "Ценовые ожидания бизнеса (SA), пункты"
FROM v_cbr_survey
WHERE series = 'price_expectations_sa' AND $__timeFilter(date)
ORDER BY date
`

// krYoYTemplate — YoY месячного ряда по ПОЛНОМУ ряду: если отфильтровать окно внутри подзапроса,
// первые 12 точек считались бы по обрезанному ряду. Лаг 12 — год для месячных данных; фильтр
// времени поэтому снаружи — по time подзапроса, а не по date источника. Делитель обёрнут в nullIf:
// в первых 12 строках lagInFrame даёт 0, и без него YoY = +Inf (широкое окно ломало бы ось).
// %s по порядку: алиас серии, витрина, код ряда. Форма — как yoyTemplate золотого дашборда.
const krYoYTemplate = `SELECT time, yoy AS %s
FROM (
    SELECT date AS time, value / nullIf(lagInFrame(value, 12) OVER (ORDER BY date), 0) - 1 AS yoy
    FROM %s
    WHERE series = '%s'
)
WHERE $__timeFilter(time)
ORDER BY time
`

// krYoYSQL собирает запрос YoY по шаблону: панель 6 (М2 и кредит, v_cbr_macro) и панель 9-B
// (зарплата, v_rosstat_macro) различаются только витриной, кодом ряда и алиасом.
func krYoYSQL(view, series, alias string) string {
	return fmt.Sprintf(krYoYTemplate, alias, view, series)
}

// krSalarySeries — код ряда среднемесячной зарплаты в v_rosstat_macro: длинное имя, которое
// в SQL панели 9-B легко набрать с опечаткой, а неверное имя оставило бы серию молча пустой.
const krSalarySeries = "Среднемесячная номинальная начисленная заработная плата работников по полному кругу организаций"

// krUnemploymentSQL — уровень безработицы: единственная серия панели 9 в процентах (уровень,
// а не темп), поэтому в общий YoY-шаблон она не идёт.
const krUnemploymentSQL = `SELECT date AS time, value AS "Уровень безработицы, %"
FROM v_rosstat_macro
WHERE series = 'Уровень безработицы' AND $__timeFilter(date)
ORDER BY date
`

// krBudgetTemplate — срез v_minfin_budget по источнику и коду ряда: все три серии панели 8 — из
// minfin_mesyats, единственного источника с рядом дефицита (в minfin его нет, а одноимённые ряды
// там — федеральный бюджет, значения не совпадают). %s по порядку: алиас серии, код ряда.
const krBudgetTemplate = `SELECT date AS time, value AS %s
FROM v_minfin_budget
WHERE source = 'minfin_mesyats' AND series = '%s'
  AND $__timeFilter(date)
ORDER BY date
`

// krBudgetSQL собирает запрос бюджета по шаблону.
func krBudgetSQL(series, alias string) string {
	return fmt.Sprintf(krBudgetTemplate, alias, series)
}

// krSberSpendingSQL — недельные потребительские расходы Сбера (nowcast) в % г/г: витрина уже
// хранит готовый темп, преобразований в SQL нет, поэтому единица панели 10 — percent.
// Оговорка про лицензию источника — в Description панели.
const krSberSpendingSQL = `SELECT date AS time, value AS "Расходы Сбера, % г/г"
FROM v_bank_sber_spending
WHERE source = 'sber_izmenenie_trat' AND series = 'Все категории'
  AND $__timeFilter(date)
ORDER BY date
`

// krKeyrateTemplate — срез витрины v_keyrate_expectations «дата + вменённая ставка» по коду серии
// фьючерса KEYRATE: панель 11 читает три известные серии одной формой (различаются secid и алиас).
// settle — расчётная цена дня, вменённый рынком уровень ключевой ставки на дату заседания
// (не close: без сделок close не отражает ожиданий). %s по порядку: алиас серии, код серии.
const krKeyrateTemplate = `SELECT date AS time, settle AS %s
FROM v_keyrate_expectations
WHERE secid = '%s' AND $__timeFilter(date)
ORDER BY date
`

// krKeyrateSQL собирает запрос траектории по шаблону.
func krKeyrateSQL(secid, alias string) string {
	return fmt.Sprintf(krKeyrateTemplate, alias, secid)
}

// krKeyrateSnapshotSQL — последний срез витрины KEYRATE, по строке на серию. Срез берётся
// подзапросом max(date), а не LIMIT: нужна последняя ДАТА витрины (строка данных), а не первая
// строка какой-то сортировки (LIMIT на временном ряду — анти-паттерн UI-экспорта).
// Ликвидность стоит рядом со значением (чек-лист KEY_RATE_PUBLICATIONS §9): volume = 0 значит,
// что сделок не было и settle серии за этот день — не рыночное ожидание, а оценка; openposition —
// индикатор ликвидности серии. expected_change_bp — шаг к последней ИЗВЕСТНОЙ ставке cbr_key_rate,
// а не к ставке на дату заседания, и при нулевом объёме нерепрезентативен.
const krKeyrateSnapshotSQL = `SELECT series_name AS "Серия", decision_date AS "Заседание", settle AS "Вменённая ставка, %",
       expected_change_bp AS "Ожидаемый шаг, б.п.", volume AS "Объём", openposition AS "Открытый интерес"
FROM v_keyrate_expectations
WHERE date = (SELECT max(date) FROM v_keyrate_expectations)
ORDER BY decision_date
`

// krOfzYieldTemplate — срез витрины v_ofz_curve «дата + доходность» по тенору: у кривой 1y/3y/5y/10y
// это доходность G-curve (% годовых), у RGBI — уровень индекса облигаций федерального займа
// (пункты, единица short). %s по порядку: алиас серии, тенор.
const krOfzYieldTemplate = `SELECT date AS time, yield AS %s
FROM v_ofz_curve
WHERE tenor = '%s' AND $__timeFilter(date)
ORDER BY date
`

// krOfzYieldSQL собирает запрос кривой по шаблону.
func krOfzYieldSQL(tenor, alias string) string {
	return fmt.Sprintf(krOfzYieldTemplate, alias, tenor)
}

// krRuoniaSpreadSQL — спред RUONIA к ключевой ставке самоджойном витрины по дате: RUONIA дневная,
// ключевая меняется решениями, и клиентское вычитание двух серий дало бы спред к ставке другой даты.
// Знак: минус — ставка денежного рынка ниже ключевой (ликвидность мягкая), плюс — выше (жёсткая).
// Фильтр окна — по r.date (левая таблица): обе таблицы среза одной витрины, даты совпадают джойном.
const krRuoniaSpreadSQL = `SELECT r.date AS time, r.value - k.value AS "Спред RUONIA к ключевой, п.п."
FROM v_cbr_macro AS r
INNER JOIN v_cbr_macro AS k ON k.date = r.date AND k.series = 'cbr_key_rate'
WHERE r.series = 'cbr_ruania_ruo' AND $__timeFilter(r.date)
ORDER BY r.date
`

// krPanelDefaults — общие визуальные конвенции дашборда (выведены из UI-экспорта): палитра classic,
// легенда снизу таблицей с calcs (последнее/среднее/максимум), tooltip по одной серии, линия
// шириной 1 без заливки, градиента и соединения разрывов (spanNulls false — разрыв недельного
// ряда честно остаётся разрывом). Панели добавляют свои Id/Title/Description/Span и таргеты поверх:
// так конвенции не разъезжаются между рядами по мере роста дашборда.
func krPanelDefaults(builder *timeseries.PanelBuilder) *timeseries.PanelBuilder {
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

// krFixedColorProp — свойство override'а с фиксированным цветом серии: палитра classic красит
// серии по порядку, и без фиксации цвет решения ЦБ/пунктов ожиданий менялся бы при добавлении таргета.
func krFixedColorProp(color string) dashboard.DynamicConfigValue {
	return dashboard.DynamicConfigValue{
		Id:    "color",
		Value: dashboard.FieldColor{Mode: dashboard.FieldColorModeIdFixed, FixedColor: ptr(color)},
	}
}

// krPastYearProps — свойства серии прошлого года панели 2: пунктир из UI-экспорта, чтобы
// календарное наложение не читалось как ещё один ряд текущего года.
func krPastYearProps() []dashboard.DynamicConfigValue {
	return []dashboard.DynamicConfigValue{{
		Id:    "custom.lineStyle",
		Value: common.LineStyle{Dash: []float64{10, 10}, Fill: ptr(common.LineStyleFillDash)},
	}}
}

// krFourPercentThresholds — порог 4% годовых панелей 2 и 4: цель ЦБ по инфляции. Значения серий —
// доли (percentunit), поэтому шаг порога тоже доля, а не 4. Ступени super-light-* — как в UI-экспорте,
// где этим порогом размечен недельный ИПЦ.
func krFourPercentThresholds() *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{
			{Value: nil, Color: "super-light-green"},
			{Value: ptr(0.04), Color: "super-light-red"},
		})
}

// krThresholdLineStyle — стиль отображения порога: ThresholdsStyle обязателен, иначе Grafana
// порог не рисует (по умолчанию off), и линия цели 4% пропала бы с графика.
func krThresholdLineStyle() *common.GraphThresholdsStyleConfigBuilder {
	return common.NewGraphThresholdsStyleConfigBuilder().Mode(common.GraphThresholdsStyleModeLine)
}

// krZeroThresholds — нулевой порог панели 6: темп г/г выше нуля (рост) зелёный, ниже (сжатие)
// красный. Как и 4%-ный порог, задаётся панельными thresholds и рисуется только вместе со стилем
// line (krThresholdLineStyle); значения серий — доли, поэтому граница тоже 0, а не «0%».
func krZeroThresholds() *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{
			{Value: nil, Color: "red"},
			{Value: ptr(0.0), Color: "green"},
		})
}

// NewKeyRatePublications собирает дашборд «Ключевая ставка — публикации». Собраны ряды 21 «Ставка
// и инфляция» (панели 1–5), 22 «Деньги, кредит, бюджет, труд» (панели 6–10) и 23 «Рыночный контур»
// (панели 11–14); ряд 24 добавляется следующей задачей. Порядок вызовов WithRow/WithPanel задаёт
// и порядок сетки, и нумерацию панелей 1..19.
func NewKeyRatePublications() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Ключевая ставка — публикации").
		Uid(KeyRatePublicationsUID).
		Tags([]string{"key-rate", "publications"}).
		Refresh("30m").
		Time("now-3y", "now").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Ставка и инфляция").Id(firstKeyRateRowID)).
		WithPanel(krRatePanel()).
		WithPanel(krWeeklyAnnualizedPanel()).
		WithPanel(krWeeklyRisersPanel()).
		WithPanel(krInflationPanel()).
		WithPanel(krExpectationsPanel()).
		WithRow(dashboard.NewRowBuilder("Деньги, кредит, бюджет, труд").Id(firstKeyRateRowID + 1)).
		WithPanel(krMoneyCreditPanel()).
		WithPanel(krCreditRatesPanel()).
		WithPanel(krBudgetPanel()).
		WithPanel(krLabourPanel()).
		WithPanel(krSberSpendingPanel()).
		WithRow(dashboard.NewRowBuilder("Рыночный контур").Id(firstKeyRateRowID + 2)).
		WithPanel(krKeyrateTrajectoryPanel()).
		WithPanel(krKeyrateSnapshotPanel()).
		WithPanel(krOfzCurvePanel()).
		WithPanel(krRuoniaPanel())
}

// krRatePanel — панель 1: ключевая ставка линией и точки решений ЦБ. Ставка — уровень на дату,
// точки — принятые решения: вместе видно, как решения двигали ставку. Серия B — только точки
// (решение это событие, а не непрерывный ряд) с фикс-цветом: её цвет не должен зависеть от порядка
// таргетов в палитре.
func krRatePanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(1).
		Title("Ключевая ставка и решения ЦБ").
		Span(spanHalf).
		Description("Ключевая ставка ЦБ (v_cbr_macro, series = 'cbr_key_rate') — уровень на дату; "+
			"точки — принятые решения по датам заседаний (v_cbr_mp_summary.decision_rate). "+
			"Значения — % годовых.").
		WithTarget(timeseriesQuery("A", krMacroValueSQL("cbr_key_rate", `"Ключевая ставка, % годовых"`))).
		WithTarget(timeseriesQuery("B", krDecisionsSQL)).
		Unit(units.Percent).
		OverrideByQuery("B", append(pointsOnlyProps(), krFixedColorProp("purple")))
}

// krWeeklyAnnualizedPanel — панель 2: годовое выражение среднего недельного индекса (×52 недели);
// ряд B — прошлый год пунктиром поверх текущего (календарное наложение). Порог 4% — цель ЦБ.
func krWeeklyAnnualizedPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(2).
		Title("Недельные цены: годовое выражение").
		Span(spanHalf).
		Description("Средний недельный индекс цен недельных товарных рядов (v_rosstat_macro, отбор — "+
			"v_series_catalog.frequency = 'W') возведён в степень 52: «если бы такие недели шли весь год». "+
			"SAAR считает модель, здесь — годовое выражение, а не прогноз. Пунктир — прошлый год, "+
			"сдвинутый на +1 год для наложения на текущий. Порог 4% — цель ЦБ.").
		WithTarget(timeseriesQuery("A", krWeeklyAnnualizedSQL("date", `"Текущий год, годовое выражение"`, "toYear(now())", "date"))).
		WithTarget(timeseriesQuery("B", krWeeklyAnnualizedSQL("date_add(YEAR, 1, date)", `"Прошлый год, годовое выражение"`, "toYear(now()) - 1", "time"))).
		Unit(units.PercentUnit).
		Thresholds(krFourPercentThresholds()).
		ThresholdsStyle(krThresholdLineStyle()).
		OverrideByQuery("B", krPastYearProps())
}

// krWeeklyRisersPanel — панель 3: широта недельного роста — доля позиций, подорожавших за неделю.
func krWeeklyRisersPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(3).
		Title("Недельные цены: доля подорожавших позиций").
		Span(spanHalf).
		Description("Доля недельных товарных рядов, подорожавших за неделю (индекс > 100), в %: " +
			"100% — подорожало всё, 50% — половина позиций. Источник — v_rosstat_macro с отбором " +
			"недельных рядов по v_series_catalog.frequency = 'W'. Это доля, а не индекс.").
		WithTarget(timeseriesQuery("A", krWeeklyRisersSQL)).
		Unit(units.Percent)
}

// krInflationPanel — панель 4: ИПЦ и базовая инфляция в годовом выражении. Формулы разные
// (ИПЦ — индекс, базовая — месячный прирост), и это подписано в Description: перепутать —
// значит прочитать базовую как индекс. Значения — доли, поэтому percentunit. Порог 4% — цель ЦБ.
func krInflationPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(4).
		Title("Инфляция: ИПЦ и базовая").
		Span(spanHalf).
		Description("Годовое выражение месячных данных, формулы разные: ИПЦ — индекс (100 = без изменения), " +
			"pow(индекс/100, 12) - 1; базовая инфляция — месячный прирост в %, pow(1 + прирост/100, 12) - 1. " +
			"Базовую нельзя читать как индекс. Источники — v_rosstat_macro (ИПЦ) и v_cbr_macro (Базовый ИПЦ).").
		WithTarget(timeseriesQuery("A", krCPIAnnualizedSQL)).
		WithTarget(timeseriesQuery("B", krBaseInflationAnnualizedSQL)).
		Unit(units.PercentUnit).
		Thresholds(krFourPercentThresholds()).
		ThresholdsStyle(krThresholdLineStyle())
}

// krExpectationsPanel — панель 5: инфляционные ожидания населения и ценовые ожидания бизнеса.
// Серии A/B — проценты годовых (панельный unit percent), серия C — ПУНКТЫ: единица задаётся
// пер-серийным override'ом (short), иначе пункты читались бы как проценты. Цвет C зафиксирован:
// серия другой размерности не должна менять цвет от порядка таргетов в палитре.
func krExpectationsPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(5).
		Title("Инфляционные и ценовые ожидания").
		Span(24).
		Description("Наблюдаемая и ожидаемая инфляция населения (v_cbr_macro, % годовых) и ценовые "+
			"ожидания бизнеса (v_cbr_survey.price_expectations_sa, ПУНКТЫ — не %): у серии C единица "+
			"override'ом (short), сравнивать её значения с процентами нельзя, читать — динамику.").
		WithTarget(timeseriesQuery("A", krMacroValueSQL("наблюдаемая инфляция", `"Наблюдаемая инфляция, % годовых"`))).
		WithTarget(timeseriesQuery("B", krMacroValueSQL("ожидаемая инфляция", `"Ожидаемая инфляция, % годовых"`))).
		WithTarget(timeseriesQuery("C", krBusinessExpectationsSQL)).
		Unit(units.Percent).
		OverrideByQuery("C", []dashboard.DynamicConfigValue{
			{Id: "unit", Value: units.Short},
			krFixedColorProp("text"),
		})
}

// krMoneyCreditPanel — панель 6: годовые темпы денежной массы (М2 SA) и кредита экономике.
// YoY — окном по полному ряду (см. krYoYTemplate), значения — доли, поэтому percentunit и порог
// нуля тоже доля: линией размечается граница роста (зелёный) и сжатия (красный).
func krMoneyCreditPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(6).
		Title("Денежная масса и кредит, г/г").
		Span(spanHalf).
		Description("Годовые темпы денежной массы М2, сезонно скорректированной (v_cbr_macro, series = 'М2 SA'), " +
			"и кредита экономике — value к значению 12 месяцев назад − 1. YoY считается окном по полному " +
			"ряду, фильтр окна — снаружи подзапроса: при обрезке первые 12 месяцев считались бы по неполному " +
			"ряду. Значения — доли (Grafana печатает «3.2%»): выше нуля — рост, ниже — сжатие.").
		WithTarget(timeseriesQuery("A", krYoYSQL("v_cbr_macro", "М2 SA", `"М2, г/г"`))).
		WithTarget(timeseriesQuery("B", krYoYSQL("v_cbr_macro", "Кредит экономике", `"Кредит экономике, г/г"`))).
		Unit(units.PercentUnit).
		Thresholds(krZeroThresholds()).
		ThresholdsStyle(krThresholdLineStyle())
}

// krCreditRatesPanel — панель 7: трансмиссия ключевой ставки в кредитные — уровень ключевой против
// ставок по кредитам юрлицам до 1 года (по кругу «Всего» и по СМП). Все серии — % годовых (percent):
// уровни сопоставимы напрямую, долей и наложений здесь нет.
func krCreditRatesPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(7).
		Title("Кредитные ставки и ключевая").
		Span(spanHalf).
		Description("Трансмиссия ключевой ставки в кредитные: ключевая (v_cbr_macro, series = 'cbr_key_rate') " +
			"и средневзвешенные ставки по кредитам, предоставленным нефинансовым организациям в рублях " +
			"сроком до 1 года — по кругу «Всего» и по субъектам МСП (ряды ЦБ, месячные, дата — предпоследний " +
			"день отчётного месяца). Все значения — % годовых.").
		WithTarget(timeseriesQuery("A", krMacroValueSQL("cbr_key_rate", `"Ключевая ставка, % годовых"`))).
		WithTarget(timeseriesQuery("B", krMacroValueSQL("Всего: до 1 года, включая «до востребования»", `"Всего: до 1 года, % годовых"`))).
		WithTarget(timeseriesQuery("C", krMacroValueSQL("СМП: до 1 года, включая «до востребования»", `"СМП: до 1 года, % годовых"`))).
		Unit(units.Percent)
}

// krBudgetPanel — панель 8: исполнение консолидированного бюджета — доходы, расходы и сальдо.
// Все три серии — из minfin_mesyats: только там есть «Дефицит (-)/Профицит (+)» (у федерального
// minfin отдельного ряда дефицита в контуре пока нет — фаза 2). Поэтому и заголовок, и Description
// называют охват явно: без этого числа прочли бы как федеральный бюджет, а его расходы/доходы —
// другой ряд с тем же названием. Единица — млрд руб. (currencyRUB); значения помесячные,
// а не накопленные.
func krBudgetPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(8).
		Title("Консолидированный бюджет: доходы, расходы, дефицит").
		Span(spanHalf).
		Description("Помесячное исполнение консолидированного бюджета РФ и государственных внебюджетных " +
			"фондов — НЕ федерального бюджета (v_minfin_budget, source = 'minfin_mesyats' — единственный " +
			"источник с рядом дефицита). Одноимённые ряды «Доходы, всего»/«Расходы, всего» есть и в " +
			"source = 'minfin' — это федеральный бюджет, значения не совпадают, и состоит он только из " +
			"доходов и расходов: отдельного ряда дефицита федерального бюджета в контуре пока нет (фаза 2). " +
			"Значения — млрд руб. ЗА МЕСЯЦ, не накопленным итогом (год — сумма 12 месяцев); сальдо знаковое: " +
			"минус — дефицит, плюс — профицит.").
		WithTarget(timeseriesQuery("A", krBudgetSQL("Расходы, всего", `"Расходы, всего, млрд руб."`))).
		WithTarget(timeseriesQuery("B", krBudgetSQL("Доходы, всего", `"Доходы, всего, млрд руб."`))).
		WithTarget(timeseriesQuery("C", krBudgetSQL("Дефицит (-)/Профицит (+)", `"Дефицит (-)/Профицит (+), млрд руб."`))).
		Unit(units.Rubles)
}

// krLabourPanel — панель 9: безработица (уровень, percent) и зарплата в годовом выражении (доля,
// percentunit). Единица серии B задана пер-серийным override'ом — без него доля печаталась бы как
// процент (ошибка в 100 раз); там же правая ось: 3–6% безработицы и двузначный рост зарплаты
// на одной шкале нечитаемы. Серия A — уровень, а не темп, поэтому идёт без lagInFrame.
func krLabourPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(9).
		Title("Рынок труда: безработица и зарплата").
		Span(spanHalf).
		Description("Рынок труда: уровень безработицы (v_rosstat_macro, % рабочей силы; дата — последний "+
			"день отчётного месяца) и среднемесячная номинальная зарплата в годовом выражении (value к "+
			"значению год назад − 1) — YoY окном по полному ряду, фильтр окна снаружи подзапроса. "+
			"У зарплаты другая единица (доля, percentunit) и своя шкала: override'ом она уезжает на правую "+
			"ось, сравнивать её уровень с процентами безработицы нельзя.").
		WithTarget(timeseriesQuery("A", krUnemploymentSQL)).
		WithTarget(timeseriesQuery("B", krYoYSQL("v_rosstat_macro", krSalarySeries, `"Зарплата, г/г"`))).
		Unit(units.Percent).
		OverrideByQuery("B", []dashboard.DynamicConfigValue{
			{Id: "unit", Value: units.PercentUnit},
			axisPlacementRightProp(),
		})
}

// krSberSpendingPanel — панель 10: недельные расходы Сбера (nowcast) во всю ширину — недельный ряд
// с разрывами читается шире (span 24, как у панели 5). Витрина хранит готовый % г/г, поэтому
// единица — percent и никаких преобразований.
func krSberSpendingPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(10).
		Title("Расходы Сбера (nowcast), % г/г").
		Span(24).
		Description("SberIndex: изменение потребительских расходов год к году, недельно " +
			"(v_bank_sber_spending, source = 'sber_izmenenie_trat', series = 'Все категории'). Значение — " +
			"% к сопоставимой ISO-неделе прошлого года (положительное — рост): высокочастотный nowcast " +
			"потребительского спроса, а не официальная статистика. Источник — СберИндекс: атрибуция " +
			"обязательна, лицензия и условия использования не подтверждены (KEY_RATE_MODEL §4) — читать " +
			"как индикативный ориентир.").
		WithTarget(timeseriesQuery("A", krSberSpendingSQL)).
		Unit(units.Percent)
}

// krKeyrateTrajectoryPanel — панель 11: вменённая рынком траектория ключевой ставки по фьючерсам
// KEYRATE — три известные серии (KKV6/KKZ6/KKG7), точка на каждый торговый день. Значения — settle:
// уровень ставки на дату заседания, а не темп, поэтому единица percent (никаких pow здесь нет).
// Дальняя KKG7 неликвидна (сделок нет, объём нулевой): её единственная точка рисуется точкой,
// а не линией, и цветом, не зависящим от порядка таргетов. Список серий обновляется точечно при
// появлении новых (переменные темплейтинга — вне v1); про ликвидность среза — панель 12.
func krKeyrateTrajectoryPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(11).
		Title("KEYRATE: вменённая траектория").
		Span(spanHalf).
		Description("Вменённая рынком траектория ключевой ставки по фьючерсам KEYRATE "+
			"(v_keyrate_expectations, settle — расчётная цена дня): уровень ключевой ставки на дату "+
			"заседания ЦБ, % годовых. KKV6 — заседание 10.2026, KKZ6 — 12.2026, KKG7 — 02.2027. "+
			"KKG7 — дальняя серия без сделок (объём 0): её точка — оценка, а не торгуемое ожидание, "+
			"поэтому она нарисована точкой, а не линией, и сравнивать её с ликвидными сериями нельзя "+
			"(ликвидность среза — панель 12). close не используется: без сделок он не отражает "+
			"ожиданий. settle — вменённое рынком значение, а не прогноз и не ценовое обязательство.").
		WithTarget(timeseriesQuery("A", krKeyrateSQL("KKV6", `"KEYRATE-10.26, % годовых"`))).
		WithTarget(timeseriesQuery("B", krKeyrateSQL("KKZ6", `"KEYRATE-12.26, % годовых"`))).
		WithTarget(timeseriesQuery("C", krKeyrateSQL("KKG7", `"KEYRATE-2.27, % годовых"`))).
		Unit(units.Percent).
		OverrideByQuery("C", append(pointsOnlyProps(), krFixedColorProp("orange")))
}

// krKeyrateSnapshotPanel — панель 12: таблица последнего среза KEYRATE — значение рядом с
// ликвидностью (чек-лист KEY_RATE_PUBLICATIONS §9): объём и открытый интерес отвечают на вопрос,
// можно ли верить вменённой ставке. Срез — подзапросом max(date), не LIMIT: дата последнего
// торгового дня, а не произвольная строка сортировки. Table-формат (1) обязателен: строковая
// колонка «Серия» и построчный срез в timeseries-формат не укладываются.
func krKeyrateSnapshotPanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Id(12).
		Title("KEYRATE: последний срез и ликвидность").
		Span(spanHalf).
		Description("Последний срез витрины KEYRATE (v_keyrate_expectations, date = max(date)): " +
			"по строке на серию фьючерса. settle — вменённая рынком ключевая ставка на дату " +
			"заседания (%), expected_change_bp — вменённый шаг в базисных пунктах К ПОСЛЕДНЕЙ " +
			"известной ставке cbr_key_rate (не к ставке на дату заседания), объём — сделки за день " +
			"(контракты), открытый интерес — позиции на конец дня (индикатор ликвидности серии). " +
			"Ликвидность обязана стоять рядом со значением (KEY_RATE_PUBLICATIONS §9): объём 0 " +
			"означает, что сделок не было и settle серии за этот день — не рыночное ожидание, а " +
			"оценка; шаг при нулевом объёме нерепрезентативен. NULL в шаге — ставка ещё не загружена, " +
			"и это «неизвестно», а не нулевое изменение.").
		Datasource(datasourceRef()).
		WithTarget(tableQuery("A", krKeyrateSnapshotSQL))
}

// krOfzCurvePanel — панель 13: доходности кривой ОФЗ и уровень RGBI. Теноры 1y/3y/5y/10y сейчас —
// снимок по одному наблюдению (история кривой наполняется), поэтому точки, а не линии. RGBI —
// уровень индекса в пунктах, а не доходность: его единица задана пер-серийным override'ом unit=short,
// иначе панельные проценты напечатали бы индекс как «96% годовых».
func krOfzCurvePanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(13).
		Title("Кривая ОФЗ и RGBI").
		Span(spanHalf).
		Description("Кривая бескупонной доходности ОФЗ (v_ofz_curve; теноры 1y/3y/5y/10y — "+
			"% годовых) и уровень индекса RGBI. RGBI — ПУНКТЫ индекса облигаций федерального займа "+
			"(от 100 при номинале), а не доходность: единица задана пер-серийным override'ом (short), "+
			"сопоставлять его значения с процентами теноров нельзя — читается динамика каждой серии. "+
			"Теноры сейчас по одному наблюдению (снимок, история наполняется): точки, не линии.").
		WithTarget(timeseriesQuery("A", krOfzYieldSQL("RGBI", `"RGBI"`))).
		WithTarget(timeseriesQuery("B", krOfzYieldSQL("1y", `"1y, %"`))).
		WithTarget(timeseriesQuery("C", krOfzYieldSQL("3y", `"3y, %"`))).
		WithTarget(timeseriesQuery("D", krOfzYieldSQL("5y", `"5y, %"`))).
		WithTarget(timeseriesQuery("E", krOfzYieldSQL("10y", `"10y, %"`))).
		Unit(units.Percent).
		OverrideByQuery("A", []dashboard.DynamicConfigValue{{Id: "unit", Value: units.Short}}).
		OverrideByQuery("B", pointsOnlyProps()).
		OverrideByQuery("C", pointsOnlyProps()).
		OverrideByQuery("D", pointsOnlyProps()).
		OverrideByQuery("E", pointsOnlyProps())
}

// krRuoniaPanel — панель 14: ставка денежного рынка RUONIA, ключевая ставка и спред между ними.
// Спред считается в SQL самоджойном витрины по дате (krRuoniaSpreadSQL): RUONIA дневная, ключевая
// меняется решениями, и клиентское вычитание дало бы спред к ставке другой даты. Знак спреда
// объяснён в Description: «минус» на этой панели — не ошибка данных, а мягкая ликвидность.
func krRuoniaPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(14).
		Title("RUONIA и спред к ключевой").
		Span(spanHalf).
		Description("RUONIA — ставка денежного рынка по сделкам овернайт (v_cbr_macro, series = " +
			"'cbr_ruania_ruo', % годовых), ключевая — уровень решений ЦБ (series = 'cbr_key_rate'). " +
			"Спред RUONIA − ключевая (п.п.) считается в SQL самоджойном витрины по дате: серии разной " +
			"частоты, и вычитать их на клиенте значило бы взять ставку не той даты. Знак: минус — " +
			"банки занимают дешевле ключевой (ликвидность мягкая), плюс — дороже (жёсткая); спред — " +
			"разница уровней, и со ставками его модуль сравнивать нельзя.").
		WithTarget(timeseriesQuery("A", krMacroValueSQL("cbr_ruania_ruo", `"RUONIA, % годовых"`))).
		WithTarget(timeseriesQuery("B", krMacroValueSQL("cbr_key_rate", `"Ключевая ставка, % годовых"`))).
		WithTarget(timeseriesQuery("C", krRuoniaSpreadSQL)).
		Unit(units.Percent)
}
