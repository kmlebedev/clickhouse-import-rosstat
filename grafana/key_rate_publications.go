package main

import (
	"fmt"

	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
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

// NewKeyRatePublications собирает дашборд «Ключевая ставка — публикации». В этой задаче собран
// ряд 21 «Ставка и инфляция» (панели 1–5); ряды 22–24 добавляются следующими задачами. Порядок
// вызовов WithRow/WithPanel задаёт и порядок сетки, и нумерацию панелей 1..19.
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
		WithPanel(krExpectationsPanel())
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
