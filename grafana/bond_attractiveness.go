package main

import (
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/stat"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

// BondAttractivenessUID — uid дашборда «Привлекательность облигаций» (пункт ofz-market/1.8):
// попадает и в spec, и в metadata.name манифеста, и в имя файла артефакта.
const BondAttractivenessUID = "bond-attractiveness"

// Id рядов вне диапазона панелей 1..18: нулевой id Grafana заменит своим, а совпавший с панелью
// сделает артефакт неотличимым от кода.
const (
	baTriggerRowID = 20
	baStatRowID    = 21
	baRatesRowID   = 22
	baCurveRowID   = 23
	baDepositRowID = 24
	baFundsRowID   = 25
)

// Средневзвешенная ключевая ставка с 13.09.2013 (10.4314% на 2026-11-08) — не литерал, а расчёт
// на момент запроса: значение смещается при добавлении новых дней, и порог «долгового режима»
// обязан ехать вместе с ним. Те же 5.5% × 4 дня добавлены к ряду, который начинается 17.09.2013.
const (
	baKeyRateSeries  = "cbr_key_rate"
	baKeyRateStart   = "2013-09-17"
	baKeyRateFirst   = 5.5
	baKeyRatePreDays = 4
)

// baKeyRateAndAvgSQL — панель 7: дневная ключевая ставка и средневзвешенная с 2013 одной строкой
// на дату. CROSS JOIN с одной строкой расчёта повторяет среднюю на каждую дату — это линия
// «средневзвешенной» без второго источника и без литерала в коде.
const baKeyRateAndAvgSQL = `WITH a AS (
    SELECT round((sum(value * dateDiff('day', d0, d1)) + 5.5 * 4) / (sum(dateDiff('day', d0, d1)) + 4), 4) AS avg_rate
    FROM (
        SELECT value, date AS d0,
               leadInFrame(date, 1, addDays(today(), 1))
                 OVER (ORDER BY date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS d1
        FROM v_cbr_macro
        WHERE series = 'cbr_key_rate' AND date >= '2013-09-17'
    )
)
SELECT k.date AS time,
       k.value AS "Ключевая ставка, %",
       a.avg_rate AS "Средневзвешенная с 2013, %"
FROM v_cbr_macro AS k
CROSS JOIN a
WHERE k.series = 'cbr_key_rate' AND $__timeFilter(k.date)
ORDER BY time
`

// baKeyRateSeriesSQL — панели 1 и 7 (второй таргет не нужен: средняя уже в первом).
const baKeyRateSeriesSQL = `SELECT date AS time, value AS "Ключевая ставка, %"
FROM v_cbr_macro
WHERE series = 'cbr_key_rate' AND $__timeFilter(date)
ORDER BY time
`

// baKeyRateLatestSQL — панель 1 (stat): последняя точка без фильтра диапазона. Снимок не должен
// зависеть от окна дашборда: «ставка сейчас» — это последняя точка, а не последняя в окне.
const baKeyRateLatestSQL = `SELECT argMax(value, date) AS "Ключевая ставка, %"
FROM v_cbr_macro
WHERE series = 'cbr_key_rate'
`

const baRealKeyLatestSQL = `SELECT argMax(key_rate, date) - argMax(inflation_yoy, date) AS "Реальная ставка, п.п."
FROM v_ofz_signal
`

const baOfzSpreadLatestSQL = `SELECT argMax(ofz_10y_minus_key_rate_pp, date) AS "ОФЗ 10л − ключ, п.п."
FROM v_ofz_signal
`

const baOfzOneYearLatestSQL = `SELECT argMax(ofz_1y, date) AS "ОФЗ 1 год, %"
FROM v_ofz_signal
`

const baDepositOneYearLatestSQL = `SELECT argMax(value, date) AS "Вклад 181 дн–1 год, %"
FROM v_cbr_macro
WHERE series = 'от 181 дня до 1 года'
`

const baCorpLoanLatestSQL = `SELECT argMax(value, date) AS "Кредит юрлицам >1 года, %"
FROM v_cbr_macro
WHERE series = 'Всего: свыше 1 года'
`

const baKeyRateVsCpiSQL = `SELECT date AS time,
       key_rate AS "Ключевая ставка, %",
       inflation_yoy AS "ИПЦ, % г/г"
FROM v_ofz_signal
WHERE $__timeFilter(date)
ORDER BY date
`

const baRealKeyRateSQL = `SELECT date AS time,
       key_rate - inflation_yoy AS "Реальная ключевая ставка, п.п."
FROM v_ofz_signal
WHERE $__timeFilter(date)
ORDER BY date
`

const baOfzCurveSQL = `SELECT date AS time,
       ofz_1y AS "ОФЗ 1 год",
       ofz_2y AS "ОФЗ 2 года",
       ofz_10y AS "ОФЗ 10 лет",
       key_rate AS "Ключевая ставка"
FROM v_ofz_signal
WHERE $__timeFilter(date)
ORDER BY date
`

// baOfzSpreadsSQL — панель 11: спреды и наклон в п.п. Ноль — граница «кривая выше ключа»
// (долговой режим) и «кривая ниже ключа» (по статье — момент перехода в активы).
const baOfzSpreadsSQL = `SELECT date AS time,
       ofz_2y_minus_key_rate_pp AS "ОФЗ 2г − ключ",
       ofz_10y_minus_key_rate_pp AS "ОФЗ 10л − ключ",
       slope_10y_1y_pp AS "Наклон 10л − 1г"
FROM v_ofz_signal
WHERE $__timeFilter(date)
ORDER BY date
`

// baDepositsSQL — панель 12: пять сроков вкладов одной строкой на месяц. maxIf, а не отдельные
// таргеты: все пять рядов лежат на одной месячной сетке (проверено 2026-06…2026-08), поэтому
// пропусков, которые maxIf превратил бы в ноль, здесь нет.
const baDepositsSQL = `SELECT date AS time,
       maxIf(value, series = 'от 31 до 90 дней') AS "31–90 дней",
       maxIf(value, series = 'от 91 до 180 дней') AS "91–180 дней",
       maxIf(value, series = 'от 181 дня до 1 года') AS "181 день – 1 год",
       maxIf(value, series = 'от 1 года до 3 лет') AS "1–3 года",
       maxIf(value, series = 'свыше 3 лет') AS "свыше 3 лет"
FROM v_cbr_macro
WHERE series IN ('от 31 до 90 дней', 'от 91 до 180 дней', 'от 181 дня до 1 года',
                 'от 1 года до 3 лет', 'свыше 3 лет')
  AND $__timeFilter(date)
GROUP BY date
ORDER BY time
`

// baCorpLoansSQL и baRetailLoansSQL — панель 13, ДВА таргета. Розничные ставки в источнике ЦБ
// заканчиваются декабрём 2025; сводный maxIf по двум рядам нарисовал бы по 2026 году нулевую
// ставку (maxIf без совпавших строк возвращает 0, а не NULL), поэтому ряды не смешиваются:
// линия розницы честно обрывается на последней точке.
const baCorpLoansSQL = `SELECT date AS time, value AS "Кредиты юрлицам, >1 года"
FROM v_cbr_macro
WHERE series = 'Всего: свыше 1 года' AND $__timeFilter(date)
ORDER BY date
`

const baRetailLoansSQL = `SELECT date AS time, value AS "Кредиты физлицам, >1 года"
FROM v_cbr_macro
WHERE series = 'Всего (кредиты физлицам): свыше 1 года' AND $__timeFilter(date)
ORDER BY date
`

// baDepositOfzSpreadSQL — панель 14: вклад минус ОФЗ 1 год. Даты разные (вклад — конец месяца,
// ОФЗ — торговый день), поэтому обе стороны сводятся к месяцу: JOIN по точной дате пуст
// (2026-08-30 у вкладов — воскресенье).
const baDepositOfzSpreadSQL = `SELECT o.m AS time,
       d.value - o.ofz_1y AS "Вклад 1 год − ОФЗ 1 год, п.п."
FROM (
    SELECT toStartOfMonth(date) AS m, argMax(ofz_1y, date) AS ofz_1y
    FROM v_ofz_signal
    WHERE $__timeFilter(date)
    GROUP BY m
) AS o
INNER JOIN (
    SELECT toStartOfMonth(date) AS m, argMax(value, date) AS value
    FROM v_cbr_macro
    WHERE series = 'от 181 дня до 1 года'
    GROUP BY m
) AS d ON o.m = d.m
ORDER BY time
`

// baClientsSQL, baHouseholdsSQL, baCorpDepositsSQL, baOrgAccountsSQL — панель 15, четыре таргета.
// Отдельный таргет на ряд, а не maxIf: ряды SORS начинаются в разные даты (эскроу и вклады без
// эскроу — с 2019-02), и maxIf до старта ряда дал бы ноль вместо отсутствия линии.
// Значения источника — млн руб., панель показывает трлн ₽ (/1e6).
const baClientsSQL = `SELECT date AS time, value / 1e6 AS "Средства клиентов, всего"
FROM v_cbr_macro
WHERE series = 'clients_total' AND $__timeFilter(date)
ORDER BY date
`

const baHouseholdsSQL = `SELECT date AS time, value / 1e6 AS "Вклады физлиц (с эскроу)"
FROM v_cbr_macro
WHERE series = 'households_deposits_escrow' AND $__timeFilter(date)
ORDER BY date
`

const baCorpDepositsSQL = `SELECT date AS time, value / 1e6 AS "Депозиты юрлиц"
FROM v_cbr_macro
WHERE series = 'corp_deposits' AND $__timeFilter(date)
ORDER BY date
`

const baOrgAccountsSQL = `SELECT date AS time, value / 1e6 AS "Счета организаций"
FROM v_cbr_macro
WHERE series = 'org_accounts' AND $__timeFilter(date)
ORDER BY date
`

// baHouseholdsGrowthSQL — панель 16: темпы вкладов физлиц. lagInFrame считается по ПОЛНОМУ ряду,
// фильтр диапазона стоит снаружи: иначе первые 12 месяцев окна остались бы без г/г.
const baHouseholdsGrowthSQL = `SELECT time,
       yoy AS "Вклады физлиц, % г/г",
       mom AS "Вклады физлиц, % м/м"
FROM (
    SELECT date AS time,
           round((value / lagInFrame(value, 12) OVER (ORDER BY date) - 1) * 100, 2) AS yoy,
           round((value / lagInFrame(value, 1) OVER (ORDER BY date) - 1) * 100, 2) AS mom
    FROM v_cbr_macro
    WHERE series = 'households_deposits_escrow'
)
WHERE $__timeFilter(time)
ORDER BY time
`

const baEscrowSQL = `SELECT date AS time, value / 1e6 AS "Счета эскроу"
FROM v_cbr_macro
WHERE series = 'escrow_accounts' AND $__timeFilter(date)
ORDER BY date
`

const baIPAccountsSQL = `SELECT date AS time, value / 1e6 AS "Средства ИП"
FROM v_cbr_macro
WHERE series = 'ip_accounts' AND $__timeFilter(date)
ORDER BY date
`

// baFxShareSQL — панель 18: доля инвалюты и драгоценных металлов в средствах клиентов. Оба ряда
// есть на каждой дате, поэтому maxIf здесь безопасен.
const baFxShareSQL = `SELECT date AS time,
       maxIf(value, series = 'clients_total_fx') / maxIf(value, series = 'clients_total') * 100 AS "Доля инвалюты и металлов, %"
FROM v_cbr_macro
WHERE series IN ('clients_total', 'clients_total_fx') AND $__timeFilter(date)
GROUP BY date
ORDER BY time
`

// baCreditDepositSQL* — панель 50 «Кредиты и вклады: механизм оттока». Три отдельных таргета,
// а не один maxIf: розничные ставки ЦБ заканчиваются декабрём 2025, и сводная агрегация нарисовала
// бы по 2026 году нулевую ставку. Третий таргет — г/г вкладов физлиц (lagInFrame по полному ряду,
// фильтр окна снаружи): отрицательное значение означало бы отток, которого статья ждёт при 11–12%.
const baCorpLoanRateSQL = `SELECT date AS time, value AS "Кредиты юрлицам >1 года, %"
FROM v_cbr_macro
WHERE series = 'Всего: свыше 1 года' AND $__timeFilter(date)
ORDER BY date
`

const baRetailLoanRateSQL = `SELECT date AS time, value AS "Кредиты физлицам >1 года, %"
FROM v_cbr_macro
WHERE series = 'Всего (кредиты физлицам): свыше 1 года' AND $__timeFilter(date)
ORDER BY date
`

const baDepositGrowthSQL = `SELECT time, yoy AS "Вклады физлиц, % г/г"
FROM (
    SELECT date AS time,
           round((value / lagInFrame(value, 12) OVER (ORDER BY date) - 1) * 100, 2) AS yoy
    FROM v_cbr_macro
    WHERE series = 'households_deposits_escrow'
)
WHERE $__timeFilter(time)
ORDER BY time
`

// baRhetoricSQL* — панель 51 «Жёсткая риторика: уровень и цикл». Ключевая ставка (шаг решений),
// реальная ставка и вменённая ставка рынка на ближайшее заседание: последняя точка фьючерса
// KEYRATE-12.26 ставится на дату решения 2026-12-18, а не на дату торгов, — иначе точка уехала бы
// в прошлое и не читалась рядом с текущей ставкой.
const baRhetoricKeyRateSQL = `SELECT date AS time, value AS "Ключевая ставка, %"
FROM v_cbr_macro
WHERE series = 'cbr_key_rate' AND $__timeFilter(date)
ORDER BY date
`

const baRhetoricRealRateSQL = `SELECT date AS time, key_rate - inflation_yoy AS "Реальная ставка, п.п."
FROM v_ofz_signal
WHERE $__timeFilter(date)
ORDER BY date
`

const baRhetoricImpliedSQL = `SELECT decision_date AS time, argMax(settle, date) AS "Вменённая ставка на ближайшее заседание, %"
FROM v_keyrate_expectations
WHERE secid = 'KKZ6' AND $__timeFilter(date)
GROUP BY decision_date
ORDER BY time
`

// NewBondAttractiveness собирает дашборд «Привлекательность облигаций» (пункт ofz-market/1.8,
// спека 2026-11-08-grafana-bond-attractiveness-design.md §3–§5): 19 панелей в 6 рядах. Первый
// ряд — чек-лист «Все триггеры статьи»: одна таблица сразу для облигаций и акций; дальше —
// светофор из шести снимков, ставка и инфляция, кривая ОФЗ, вклады и кредиты, средства клиентов.
//
// Окно по умолчанию — с 2019-01-01: раньше начинается только ключевая ставка, а кривая ОФЗ
// (v_ofz_signal) и SORS-вклады без эскроу — с 2018-11/2019-02. Refresh 1h: дневные ряды
// обновляются импортёрами, месячные — раз в месяц.
func NewBondAttractiveness() *dashboard.DashboardBuilder {
	return dashboard.NewDashboardBuilder("Привлекательность облигаций — ставка, ОФЗ, вклады").
		Uid(BondAttractivenessUID).
		Tags([]string{"ofz-market", "cbr", "bonds"}).
		Refresh("1h").
		Time("2019-01-01", "now").
		Timezone(common.TimeZoneBrowser).
		WithRow(dashboard.NewRowBuilder("Триггеры статьи: облигации и акции").Id(baTriggerRowID)).
		WithPanel(baTriggersPanel()).
		WithRow(dashboard.NewRowBuilder("Светофор статьи").Id(baStatRowID)).
		WithPanel(baKeyRateStatPanel()).
		WithPanel(baRealKeyStatPanel()).
		WithPanel(baOfzSpreadStatPanel()).
		WithPanel(baOfzOneYearStatPanel()).
		WithPanel(baDepositStatPanel()).
		WithPanel(baCorpLoanStatPanel()).
		WithRow(dashboard.NewRowBuilder("Ставка и инфляция").Id(baRatesRowID)).
		WithPanel(baKeyRateAndAvgPanel()).
		WithPanel(baKeyRateVsCpiPanel()).
		WithPanel(baRealKeyRatePanel()).
		WithPanel(baRhetoricPanel()).
		WithRow(dashboard.NewRowBuilder("Кривая ОФЗ").Id(baCurveRowID)).
		WithPanel(baOfzCurvePanel()).
		WithPanel(baOfzSpreadsPanel()).
		WithRow(dashboard.NewRowBuilder("Вклады, кредиты, альтернатива").Id(baDepositRowID)).
		WithPanel(baDepositsPanel()).
		WithPanel(baLoansPanel()).
		WithPanel(baCreditDepositPanel()).
		WithPanel(baDepositOfzSpreadPanel()).
		WithRow(dashboard.NewRowBuilder("Средства клиентов банков (ЦБ, SORS)").Id(baFundsRowID)).
		WithPanel(baClientsPanel()).
		WithPanel(baHouseholdsGrowthPanel()).
		WithPanel(baEscrowPanel()).
		WithPanel(baFxSharePanel())
}

// baStatDefaults — общие настройки шести снимков (statDefaults) плюс размер снимка в сетке:
// снимок не зависит от окна дашборда.
func baStatDefaults(builder *stat.PanelBuilder) *stat.PanelBuilder {
	return statDefaults(builder).
		Height(baStatHeight).
		Span(baStatSpan)
}

// baPanelDefaults — конвенции графиков контура: база timeseriesDefaults (SpanNulls=false — пропуск в
// витрине остаётся разрывом: розница после 2025-12, эскроу до 2019-02, нули не подставляются),
// легенда lastNotNull/min/max, сглаженная линия и полная ширина.
func baPanelDefaults(builder *timeseries.PanelBuilder) *timeseries.PanelBuilder {
	return timeseriesDefaults(builder, "lastNotNull", "min", "max").
		LineInterpolation(common.LineInterpolationSmooth).
		Height(baPanelHeight).
		Span(spanFull)
}

// baRegimeThresholds — порог средневзвешенной ставки: ниже — «дешёвые деньги» (синий), выше —
// «долговой рынок» (зелёный). Цвет здесь режим, а не оценка «покупай/продавай».
func baRegimeThresholds() *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{
			{Value: nil, Color: "blue"},
			{Value: ptr(10.4314), Color: "green"},
		})
}

// baDepositThresholds — полоса статьи 11–12% по вкладам и кредитам: ниже — синий, в полосе —
// зелёный, выше — оранжевый (порог перехода пройден).
func baDepositThresholds() *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{
			{Value: nil, Color: "blue"},
			{Value: ptr(11.0), Color: "green"},
			{Value: ptr(12.0), Color: "orange"},
		})
}

// baKeyRateStatPanel — панель 1: ключевая ставка сейчас. Порог 10.43 — средневзвешенная с 2013:
// по правилу статьи выше неё рынок долговой.
func baKeyRateStatPanel() *stat.PanelBuilder {
	return baStatDefaults(stat.NewPanelBuilder()).
		Id(1).
		Title("Ключевая ставка, %").
		Unit(units.Percent).
		Decimals(2).
		Thresholds(baRegimeThresholds()).
		WithTarget(tableQuery("A", baKeyRateLatestSQL)).
		Description("Последняя точка ключевой ставки ЦБ (v_cbr_macro, series = 'cbr_key_rate'), " +
			"снимок без фильтра диапазона. Порог 10.43 — средневзвешенная по дням ставка с " +
			"13.09.2013 (расчёт панели 7): зелёный — ставка выше средневзвешенной, «долговой " +
			"режим»; синий — ниже, «дешёвые деньги». Цвет — режим, не рекомендация. " +
			"На 2026-11-08 — 14.00% (2026-11-06).")
}

// baRealKeyStatPanel — панель 2: реальная ключевая ставка (ключ минус ИПЦ г/г).
func baRealKeyStatPanel() *stat.PanelBuilder {
	return baStatDefaults(stat.NewPanelBuilder()).
		Id(2).
		Title("Реальная ставка, п.п.").
		Unit(units.Short).
		Decimals(2).
		Thresholds(zeroThresholds("blue")).
		WithTarget(tableQuery("A", baRealKeyLatestSQL)).
		Description("Ключевая ставка минус годовая инфляция (v_ofz_signal: key_rate − " +
			"inflation_yoy), последняя точка. Положительная — денежная политика жёсткая. " +
			"На 2026-11-08 — 7.65 п.п. (ставка 14.00, ИПЦ 6.35% г/г).")
}

// baOfzSpreadStatPanel — панель 3: спред 10-летней ОФЗ к ключу.
func baOfzSpreadStatPanel() *stat.PanelBuilder {
	return baStatDefaults(stat.NewPanelBuilder()).
		Id(3).
		Title("ОФЗ 10 лет − ключ, п.п.").
		Unit(units.Short).
		Decimals(2).
		Thresholds(zeroThresholds("blue")).
		WithTarget(tableQuery("A", baOfzSpreadLatestSQL)).
		Description("Доходность 10-летней ОФЗ минус ключевая ставка (v_ofz_signal), последняя " +
			"точка. По правилу статьи «переход в активы» начинается, когда доходность облигаций " +
			"опускается НИЖЕ ключа: сейчас спред положительный — триггер не наступил. " +
			"На 2026-11-08 — +2.75 п.п.")
}

// baOfzOneYearStatPanel — панель 4: доходность короткой ОФЗ — «рынок долга» на ближнем конце.
func baOfzOneYearStatPanel() *stat.PanelBuilder {
	return baStatDefaults(stat.NewPanelBuilder()).
		Id(4).
		Title("ОФЗ 1 год, %").
		Unit(units.Percent).
		Decimals(2).
		WithTarget(tableQuery("A", baOfzOneYearLatestSQL)).
		Description("Доходность ОФЗ 1 год (G-curve МосБиржи, v_ofz_signal), последняя точка. " +
			"Ближний конец кривой — альтернатива ключевой ставке и вкладам до года. " +
			"На 2026-11-08 — 14.16%.")
}

// baDepositStatPanel — панель 5: вклад на год против полосы 11–12% из статьи.
func baDepositStatPanel() *stat.PanelBuilder {
	return baStatDefaults(stat.NewPanelBuilder()).
		Id(5).
		Title("Вклад 181 дн–1 год, %").
		Unit(units.Percent).
		Decimals(2).
		Thresholds(baDepositThresholds()).
		WithTarget(tableQuery("A", baDepositOneYearLatestSQL)).
		Description("Средневзвешенная ставка по вкладам физлиц на срок 181 день – 1 год " +
			"(v_cbr_macro, cbr_bank_int_rate), последняя точка месячного ряда. Полоса статьи " +
			"11–12%: в ней вклады перестают быть интересными и начинается переток в активы; " +
			"синий — ниже полосы, зелёный — в полосе, оранжевый — выше. " +
			"На 2026-11-08 — 12.17% (2026-08-30), то есть у верхней границы полосы.")
}

// baCorpLoanStatPanel — панель 6: ставка по кредитам юрлицам против той же полосы.
func baCorpLoanStatPanel() *stat.PanelBuilder {
	return baStatDefaults(stat.NewPanelBuilder()).
		Id(6).
		Title("Кредит ЮЛ >1 года, %").
		Unit(units.Percent).
		Decimals(2).
		Thresholds(baDepositThresholds()).
		WithTarget(tableQuery("A", baCorpLoanLatestSQL)).
		Description("Средневзвешенная ставка по кредитам юрлицам на срок свыше 1 года " +
			"(v_cbr_macro, cbr_int_rate_loans), последняя точка. Ориентир статьи — 11–12%: при " +
			"таких ставках начинается отток вкладов и рост спроса на кредиты. " +
			"На 2026-11-08 — 12.98% (2026-08-30), чуть выше полосы.")
}

// baKeyRateAndAvgPanel — панель 7: ставка против средневзвешенной с 2013. Порог панели — та же
// средневзвешенная линией, но она приходит серией из SQL: значение пересчитывается на момент
// запроса и не расходится с данными.
func baKeyRateAndAvgPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(7).
		Title("Ключевая ставка и средневзвешенная с 2013, %").
		Unit(units.Percent).
		Decimals(2).
		WithTarget(timeseriesQuery("A", baKeyRateAndAvgSQL)).
		Description("Дневная ключевая ставка и средневзвешенная по дням ставка с 13.09.2013, " +
			"пересчитываемая на момент запроса. Выше средней — «долговой режим» по правилу статьи, " +
			"ниже — «дешёвые деньги». Средняя смещается при добавлении новых дней: это ожидаемо. " +
			"На 2026-11-08 — 10.4314%.")
}

// baKeyRateVsCpiPanel — панель 8: ставка против инфляции.
func baKeyRateVsCpiPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(8).
		Title("Ключевая ставка и ИПЦ, %").
		Unit(units.Percent).
		Decimals(2).
		WithTarget(timeseriesQuery("A", baKeyRateVsCpiSQL)).
		Description("Ключевая ставка и годовая инфляция (v_ofz_signal). Разрыв между линиями — " +
			"реальная жёсткость политики (панель 9). ИПЦ здесь — годовой темп, не индекс.")
}

// baRealKeyRatePanel — панель 9: реальная ставка.
func baRealKeyRatePanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(9).
		Title("Реальная ключевая ставка, п.п.").
		Unit(units.Short).
		Decimals(2).
		WithTarget(timeseriesQuery("A", baRealKeyRateSQL)).
		Description("Ключевая ставка минус ИПЦ г/г (v_ofz_signal). Ноль — граница " +
			"нейтральной политики; положительная реальная ставка — жёсткая. " +
			"На 2026-11-08 — +7.65 п.п.")
}

// baOfzCurvePanel — панель 10: кривая ОФЗ против ключа.
func baOfzCurvePanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(10).
		Title("Кривая ОФЗ и ключевая ставка, %").
		Unit(units.Percent).
		Decimals(2).
		WithTarget(timeseriesQuery("A", baOfzCurveSQL)).
		Description("Доходности ОФЗ 1г/2г/10л (G-curve МосБиржи) и ключевая ставка. Пока кривая " +
			"выше ключа, длинный долг даёт надбавку за дюрацию; когда она опускается ниже ключа, " +
			"по статье начинается переток в активы. На 2026-11-08 — 14.16 / 14.99 / 16.75% " +
			"против ключа 14.00%.")
}

// baOfzSpreadsPanel — панель 11: спреды к ключу и наклон кривой.
func baOfzSpreadsPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(11).
		Title("Спреды ОФЗ к ключу и наклон кривой, п.п.").
		Unit(units.Short).
		Decimals(2).
		Thresholds(zeroThresholds("blue")).
		ThresholdsStyle(thresholdLineStyle()).
		WithTarget(timeseriesQuery("A", baOfzSpreadsSQL)).
		Description("ОФЗ 2г − ключ, ОФЗ 10л − ключ и наклон 10л − 1г (v_ofz_signal). Нулевая линия: " +
			"выше — кривая дороже ключа (долговой режим), ниже — переток в активы по статье. " +
			"Рост наклона при снижении ставки — премия за длинный долг. " +
			"На 2026-11-08 — +0.99 / +2.75 / +2.59 п.п.")
}

// baDepositsPanel — панель 12: вклады по срокам (полоса 11–12% видна глазами по уровню).
func baDepositsPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(12).
		Title("Вклады физлиц по срокам, %").
		Unit(units.Percent).
		Decimals(2).
		WithTarget(timeseriesQuery("A", baDepositsSQL)).
		Description("Средневзвешенные ставки по вкладам физлиц в рублях по срокам " +
			"(v_cbr_macro, cbr_bank_int_rate). Месячный ряд ЦБ, последняя точка — 2026-08-30: " +
			"31–90 дн 12.42%, 91–180 дн 12.68%, 181 дн–1 г 12.17%, 1–3 г 10.90%, >3 л 8.87%. " +
			"Полоса статьи 11–12% пройдена на сроках до года; длинные вклады ниже неё.")
}

// baLoansPanel — панель 13: кредиты юрлицам и физлицам. Розница обрывается в декабре 2025 —
// источник ЦБ заканчивается этой датой; разрыв честный, нулей нет.
func baLoansPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(13).
		Title("Ставки по кредитам, %").
		Unit(units.Percent).
		Decimals(2).
		WithTarget(timeseriesQuery("A", baCorpLoansSQL)).
		WithTarget(timeseriesQuery("B", baRetailLoansSQL)).
		Description("Средневзвешенные ставки по кредитам на срок свыше 1 года: юрлица " +
			"(cbr_int_rate_loans) и физлица (cbr_int_rate_loans_ind). Ориентир статьи — 11–12%: " +
			"при таких ставках начинается отток вкладов. Юрлица — 12.98% (2026-08-30), физлица — " +
			"14.43% (2025-12-30; 2026 года в источнике ЦБ нет, линия обрывается, а не падает в ноль).")
}

// baDepositOfzSpreadPanel — панель 14: альтернатива «вклад или ОФЗ».
func baDepositOfzSpreadPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(14).
		Title("Вклад 1 год − ОФЗ 1 год, п.п.").
		Unit(units.Short).
		Decimals(2).
		Thresholds(zeroThresholds("blue")).
		ThresholdsStyle(thresholdLineStyle()).
		WithTarget(timeseriesQuery("A", baDepositOfzSpreadSQL)).
		Description("Вклад на 181 день – 1 год минус доходность ОФЗ 1 год, по месяцам " +
			"(v_cbr_macro + v_ofz_signal: toStartOfMonth и argMax с обеих сторон — даты источника " +
			"разные). Положительный спред — вклад выгоднее короткой ОФЗ; отрицательный — ОФЗ. " +
			"На 2026-08 — −1.46 п.п. (вклад 12.17, ОФЗ 13.63).")
}

// baClientsPanel — панель 15: уровни средств клиентов банков (SORS).
func baClientsPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(15).
		Title("Средства клиентов банков, трлн ₽").
		Unit(units.Rubles).
		Decimals(1).
		WithTarget(timeseriesQuery("A", baClientsSQL)).
		WithTarget(timeseriesQuery("B", baHouseholdsSQL)).
		WithTarget(timeseriesQuery("C", baCorpDepositsSQL)).
		WithTarget(timeseriesQuery("D", baOrgAccountsSQL)).
		Description("Средства клиентов банков РФ по SORS (ЦБ, cbr_sors_funds; лист «итого», млн руб. " +
			"→ трлн ₽): всего, вклады физлиц с эскроу, депозиты юрлиц, счета организаций. " +
			"Это НЕ денежный агрегат «депозиты населения» — другой контур и другой охват. " +
			"На 2026-10-01: 154.2 / 74.6 / 45.3 / 24.0 трлн ₽.")
}

// baHouseholdsGrowthPanel — панель 16: темпы вкладов физлиц — тезис «отток вкладов».
func baHouseholdsGrowthPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(16).
		Title("Вклады физлиц: темпы, %").
		Unit(units.Percent).
		Decimals(2).
		WithTarget(timeseriesQuery("A", baHouseholdsGrowthSQL)).
		Description("Г/г и м/м темпы вкладов физлиц с эскроу (SORS). Отрицательный г/г означал бы " +
			"отток вкладов; на 2026-11-08 г/г +9.77%, м/м +0.21% — оттока нет. Расчёт — " +
			"lagInFrame по полному ряду, фильтр диапазона снаружи.")
}

// baEscrowPanel — панель 17: эскроу и средства ИП.
func baEscrowPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(17).
		Title("Счета эскроу и средства ИП, трлн ₽").
		Unit(units.Rubles).
		Decimals(2).
		WithTarget(timeseriesQuery("A", baEscrowSQL)).
		WithTarget(timeseriesQuery("B", baIPAccountsSQL)).
		Description("Средства на счетах эскроу (с 2019-02) и средства индивидуальных " +
			"предпринимателей (с 2013-02) по SORS. Ряды начинаются в разные даты — каждая линия " +
			"стартует со своей первой точки, нулей до старта нет. На 2026-10-01: эскроу 7.60, " +
			"ИП 2.82 трлн ₽.")
}

// baFxSharePanel — панель 18: доля инвалюты и металлов в средствах клиентов.
func baFxSharePanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(18).
		Title("Доля инвалюты и металлов в средствах клиентов, %").
		Unit(units.Percent).
		Decimals(2).
		WithTarget(timeseriesQuery("A", baFxShareSQL)).
		Description("Отношение средств клиентов в иностранной валюте и драгоценных металлах к " +
			"итогу (SORS, листы «в инвалюте» и «итого»). Показывает валютную структуру " +
			"банковских пассивов — контекст для решения «рублёвый долг vs валюта». " +
			"На 2026-10-01 — 9.34%.")
}

// baStatHeight — высота снимков и baStatSpan — их ширина: шесть панелей по 8 колонок — три в ряд
// на сетке 24. При 4 колонках заголовок «Ключевая ставка, %» обрезается до «Клю…» (проверено
// скриншотом стенда 2026-11-08).
const (
	baStatHeight  = 4
	baStatSpan    = 8
	baPanelHeight = 8
)

// baTriggersSQL — панель 19, чек-лист статьи одним снимком. Одна строка и десять числовых
// колонок: имя колонки — триггер, значение — вердикт (1 — в пользу облигаций, 2 — в пользу
// акций, 0 — триггер к этой стороне не относится); последняя колонка — не вердикт, а спред
// ставок «вторичка − первичка»: ДОМ.РФ публикует индекс цен только по первичке, и льготная
// ипотека искажает её на величину этого спреда. Числовые колонки, а не строки, — потому что
// табличный формат плагина со строковыми колонками вешает датасорс на стенде (проверено
// 2026-11-08: панели 19 и 43 висят в «Cancel»), а stat-панель с числовыми полями и value
// mapping'ом рисует тот же чек-лист без таблицы.
//
// Порядок колонок и пороги — из статьи: 10.43% (средневзвешенная с 2013), полоса 11–12%,
// ключ ≤ 12%, доходность облигаций ниже ключа, отток вкладов, реальная ставка, инфляция
// активов. Средневзвешенная считается как на панели 7 (те же 5.5% × 4 дня до старта ряда).
// Г/г вкладов и новостроек — lagInFrame по полному ряду, берётся последняя точка окна.
const baTriggersSQL = `WITH m AS (
    SELECT
        (SELECT argMax(value, date) FROM v_cbr_macro WHERE series = 'cbr_key_rate' AND $__timeFilter(date)) AS key_rate,
        (SELECT round((sum(value * dateDiff('day', d0, d1)) + 5.5 * 4) / (sum(dateDiff('day', d0, d1)) + 4), 4)
         FROM (
             SELECT value, date AS d0,
                    leadInFrame(date, 1, addDays(today(), 1))
                      OVER (ORDER BY date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS d1
             FROM v_cbr_macro WHERE series = 'cbr_key_rate' AND date >= '2013-09-17'
         )) AS avg_rate,
        (SELECT argMax(ofz_1y, date) FROM v_ofz_signal WHERE $__timeFilter(date)) AS ofz_1y,
        (SELECT argMax(ofz_10y, date) FROM v_ofz_signal WHERE $__timeFilter(date)) AS ofz_10y,
        (SELECT argMax(value, date) FROM v_cbr_macro WHERE series = 'от 181 дня до 1 года' AND $__timeFilter(date)) AS dep_1y,
        (SELECT argMax(value, date) FROM v_cbr_macro WHERE series = 'Всего: свыше 1 года' AND $__timeFilter(date)) AS loan_corp,
        (SELECT argMax(inflation_yoy, date) FROM v_ofz_signal WHERE $__timeFilter(date)) AS cpi,
        (SELECT round((value / lagInFrame(value, 12) OVER (ORDER BY date) - 1) * 100, 2)
         FROM v_cbr_macro WHERE series = 'households_deposits_escrow' AND $__timeFilter(date)
         ORDER BY date DESC LIMIT 1) AS hh_yoy,
        (SELECT round((index_value / lagInFrame(index_value, 12) OVER (ORDER BY date) - 1) * 100, 2)
         FROM v_domrf_price_index
         WHERE region = 'Российская Федерация' AND room_count = 'all' AND $__timeFilter(date)
         ORDER BY date DESC LIMIT 1) AS re_yoy,
        (SELECT argMax(value, date) FROM v_domrf_mortgage WHERE series = 'rate_primary' AND $__timeFilter(date)) AS rate_primary,
        (SELECT argMax(value, date) FROM v_domrf_mortgage WHERE series = 'rate_secondary' AND $__timeFilter(date)) AS rate_secondary
)
SELECT
    toUInt8(if(key_rate > avg_rate, 1, 2)) AS "Ключ vs среднее",
    toUInt8(if(ofz_10y >= key_rate, 1, 2)) AS "ОФЗ 10л vs ключ",
    toUInt8(if(ofz_1y >= key_rate, 1, 2)) AS "ОФЗ 1г vs ключ",
    toUInt8(if(dep_1y > 12, 1, 2)) AS "Вклады 11–12%",
    toUInt8(if(loan_corp > 12, 1, 2)) AS "Кредиты 11–12%",
    toUInt8(if(hh_yoy >= 0, 1, 2)) AS "Отток вкладов",
    toUInt8(if(key_rate > 12, 1, 2)) AS "Ключ ≤ 12%",
    toUInt8(if(key_rate - cpi > 0, 1, 2)) AS "Реальная ставка",
    toUInt8(if(re_yoy > cpi, 2, 0)) AS "Новостройки vs ИПЦ*",
    round(rate_secondary - rate_primary, 2) AS "Ипотека: вторичка − первичка, п.п."
FROM m
`

// baSideMapping — вердикт чек-листа: 1 — «облигации» (зелёный), 2 — «акции» (синий),
// 0 — «—» (триггер к этой стороне не относится). Mappings, а не текст в SQL: стат-панель
// красит плитку цветом маппинга, и вердикт читается без легенды.
func baSideMapping() dashboard.ValueMapping {
	return dashboard.ValueMapOrRangeMapOrRegexMapOrSpecialValueMap{
		ValueMap: &dashboard.ValueMap{
			Type: dashboard.MappingTypeValueToText,
			Options: map[string]dashboard.ValueMappingResult{
				"1": {Text: ptr("облигации"), Color: ptr("green"), Index: ptr(int32(0))},
				"2": {Text: ptr("акции"), Color: ptr("blue"), Index: ptr(int32(1))},
				"0": {Text: ptr("—"), Color: ptr("text"), Index: ptr(int32(2))},
			},
		},
	}
}

// baTriggersPanel — панель 19: единственное место, где все девять триггеров статьи видны сразу —
// и для облигаций, и для акций. Девять числовых полей одной строки = девять плиток: имя поля —
// триггер, значение — вердикт из маппинга. Таблица здесь не используется сознательно: табличный
// формат плагина со строковыми колонками вешает датасорс на стенде (проверено 2026-11-08).
// Фильтра диапазона в SQL нет у снимков? Есть: каждый подзапрос уважает окно дашборда.
func baTriggersPanel() *stat.PanelBuilder {
	return statDefaults(stat.NewPanelBuilder()).
		Id(19).
		Title("Все триггеры статьи: облигации и акции").
		Span(spanFull).
		Height(8).
		ColorMode(common.BigValueColorModeValue).
		Mappings([]dashboard.ValueMapping{baSideMapping()}).
		OverrideByName("Ипотека: вторичка − первичка, п.п.", []dashboard.DynamicConfigValue{
			{Id: "unit", Value: units.Short},
			{Id: "decimals", Value: 2},
			{Id: "thresholds", Value: dashboard.ThresholdsConfig{
				Mode: dashboard.ThresholdsModeAbsolute,
				Steps: []dashboard.Threshold{
					{Value: nil, Color: "green"},
					{Value: ptr(0.0), Color: "orange"},
				},
			}},
		}).
		WithTarget(NewCHQuery("A", baTriggersSQL).Format(queryFormatTable).QueryType("sql")).
		Description("Чек-лист статьи на текущий момент: десять плиток, в каждой — условие из статьи " +
			"и вердикт. «Облигации» (зелёный) — условие говорит в пользу облигаций, «акции» (синий) — " +
			"в пользу акций, «—» — триггер к этой стороне не относится (инфляция активов — довод за " +
			"акции, но не довод против облигаций). Пороги: средневзвешенная ставка 10.43% с 2013, " +
			"полоса 11–12% по вкладам и кредитам, ключ ≤ 12% как условие разбора отчётностей, " +
			"доходность облигаций ниже ключа как условие перехода в активы, отток вкладов (г/г < 0), " +
			"положительная реальная ставка, инфляция новостроек выше ИПЦ. * Новостройки искажены " +
			"льготной ипотекой: ДОМ.РФ публикует индекс цен только по первичному рынку, вторички в " +
			"источнике нет — ряд цен вторичного рынка (Росстат, ЕМИСС) заводится отдельным шагом. " +
			"Пока искажение видно по спреду ставок: «Ипотека: вторичка − первичка» — при льготной " +
			"ипотеке первичка дешевле вторички на несколько п.п. (оранжевый — искажение). " +
			"Значения — на момент запроса в выбранном окне; это сводка условий, а не инвестиционная " +
			"рекомендация.")
}

// baBandThresholds — полоса статьи 11–12% линиями: ниже 11 — зелёный, 11–12 — жёлтый, выше 12 —
// красный. Для панели 50 это не оценка, а границы условия: пока ставки выше полосы, механизм
// оттока по статье ещё не включился.
func baBandThresholds() *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{
			{Value: nil, Color: "green"},
			{Value: ptr(11.0), Color: "yellow"},
			{Value: ptr(12.0), Color: "red"},
		})
}

// baCreditDepositPanel — панель 50: иллюстрация тезиса «при 11–12% по кредитам начинается отток».
// Кредиты юрлицам и физлицам (два таргета — розница обрывается в декабре 2025) и г/г вкладов
// физлиц: если бы механизм работал, линия вкладов ушла бы ниже нуля при входе ставок в полосу.
func baCreditDepositPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(50).
		Title("Кредиты и вклады: механизм оттока").
		Unit(units.Percent).
		Decimals(2).
		Thresholds(baBandThresholds()).
		ThresholdsStyle(thresholdLineStyle()).
		WithTarget(timeseriesQuery("A", baCorpLoanRateSQL)).
		WithTarget(timeseriesQuery("B", baRetailLoanRateSQL)).
		WithTarget(timeseriesQuery("C", baDepositGrowthSQL)).
		Description("Иллюстрация тезиса «при 11–12% по кредитам начинается отток вкладов». " +
			"Ставки по кредитам юрлицам и физлицам на срок свыше 1 года (v_cbr_macro) и годовой " +
			"темп вкладов физлиц с эскроу (SORS). Полоса 11–12% — линии порогов. " +
			"На 2026-11-08: юрлица 12.98% (2026-08-30), физлица 14.43% (2025-12-30 — источник ЦБ " +
			"заканчивается этой датой, линия обрывается), вклады +9.77% г/г (2026-10-01). " +
			"Механизм не подтверждается: ставки выше полосы, а вклады растут, а не убывают. " +
			"Это иллюстрация условий, а не инвестиционная рекомендация.")
}

// baRhetoricThresholds — линия 12%: условие статьи «ключ ≤ 12% — время разбора отчётностей».
func baRhetoricThresholds() *dashboard.ThresholdsConfigBuilder {
	return dashboard.NewThresholdsConfigBuilder().
		Mode(dashboard.ThresholdsModeAbsolute).
		Steps([]dashboard.Threshold{
			{Value: nil, Color: "green"},
			{Value: ptr(12.0), Color: "orange"},
		})
}

// baRhetoricPanel — панель 51: иллюстрация тезиса «жёсткая риторика». Уровень виден по ключевой
// и реальной ставке, цикл — по шагу ключевой ставки, ожидание рынка — точкой на дате заседания.
func baRhetoricPanel() *timeseries.PanelBuilder {
	return baPanelDefaults(timeseries.NewPanelBuilder()).
		Id(51).
		Title("Жёсткая риторика: уровень и цикл").
		Unit(units.Percent).
		Decimals(2).
		Thresholds(baRhetoricThresholds()).
		ThresholdsStyle(thresholdLineStyle()).
		WithTarget(timeseriesQuery("A", baRhetoricKeyRateSQL)).
		WithTarget(timeseriesQuery("B", baRhetoricRealRateSQL)).
		WithTarget(timeseriesQuery("C", baRhetoricImpliedSQL)).
		OverrideByName("Вменённая ставка на ближайшее заседание, %", pointsOnlyProps()).
		Description("Иллюстрация тезиса «сейчас риторика жёсткая». Ключевая ставка (шаг решений), " +
			"реальная ставка (ключ минус ИПЦ г/г) и вменённая ставка рынка на ближайшее заседание " +
			"(фьючерс KEYRATE-12.26, последняя точка на дате решения 2026-12-18). " +
			"Уровень жёсткий: 14.00% при ИПЦ 6.35% — реальная +7.65 п.п. Цикл понижательный: " +
			"21% с октября 2024 до июня 2025, затем снижение до 14% к 27.07.2026 и пауза; " +
			"рынок ждёт −7 б.п. на 18.12.2026 (13.93%). Линия 12% — условие статьи «время " +
			"отчётностей». Значения — на момент запроса в выбранном окне.")
}
