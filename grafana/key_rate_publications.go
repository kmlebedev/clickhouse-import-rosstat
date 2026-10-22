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
// как «не задан» и заменит своим, поэтому ряды нумеруются вне диапазона панелей 1..19, 25–33, 35.
const firstKeyRateRowID = 21

// channelRowID — id пятого ряда (34): он идёт после панелей 29–33, поэтому не может взять
// следующий по порядку id 25 — тот уже занят панелью долгового контура ОФЗ.
const channelRowID = 34

// pricesRowID — id шестого ряда (40): панели 36–39 заняли id, поэтому ряд берёт следующий
// свободный вне диапазона панелей — иначе Grafana трактует совпавший id как «не задан».
const pricesRowID = 40

// krWeeklyHistoryStart — начало сопоставимого недельного ряда: лист nedel_Ipc.xlsx
// (rosstat_ipc_weeks) начинается 2022-01-10, а до этой даты недельные ряды собраны из
// бюллетеней «Оперативной информации» с другим набором позиций. Средняя по разному набору
// несравнима, поэтому полный ряд и 4-недельное окно стартуют здесь.
const krWeeklyHistoryStart = "2022-01-10"

// krWeeklyAggregateSeries — официальная недельная оценка ИПЦ (агрегатный ряд бюллетеней и
// BI-выгрузки). Это отдельная серия каталога, а не средняя по товарам: у неё своя история, и
// она отстаёт от товарного слоя на неделю.
const krWeeklyAggregateSeries = "Индекс потребительских цен (недельная оценка)"

// krFedstatRegion — регион ценовой панели: 2008–2022 годы в источнике — «Российская Федерация»,
// 2026 — «без учёта новых субъектов». В панель берём только первый: иначе линия получила бы
// разрыв 2023–2025 и смену территориального определения.
const krFedstatRegion = "Российская Федерация"

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
// (100 = без изменения) возводится в степень 52 — 52 недели в году. %s по порядку: алиас серии,
// выражение времени и предикат года — ряды A/B различаются только ими (B — календарное
// наложение: время сдвинуто на +1 год, чтобы прошлый год лёг поверх текущего). $__timeFilter —
// по вычисленному time, а не по сырому date: у ряда B time = date + 1 год, и фильтр по date
// выбросил бы прошлый год из окна дашборда.
const krWeeklyAnnualizedTemplate = `SELECT time, pow(avg(value) / 100, 52) - 1 AS %s
FROM (
    SELECT %s AS time, value
    FROM v_rosstat_macro AS w
    INNER JOIN v_series_catalog AS c ON c.source = w.source AND c.series = w.series
    WHERE c.frequency = 'W' AND toYear(date) = %s
)
WHERE $__timeFilter(time)
GROUP BY time
ORDER BY time
`

// krWeeklyAnnualizedSQL собирает запрос годового выражения по шаблону.
func krWeeklyAnnualizedSQL(timeExpr, alias, yearFilter string) string {
	return fmt.Sprintf(krWeeklyAnnualizedTemplate, alias, timeExpr, yearFilter)
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

// krWeeklyHistorySQL — полный ряд недельного индекса панели 36: средняя по недельным товарным
// рядам, годовое выражение (степень 52). Отличие от панели 2 — нет годового фильтра: ряд идёт
// с krWeeklyHistoryStart до последней недели, поэтому на графике видна вся история с 2022.
const krWeeklyHistorySQL = `SELECT date AS time, pow(avg(value) / 100, 52) - 1 AS "Недельное, годовое выражение"
FROM v_rosstat_macro AS w
INNER JOIN v_series_catalog AS c ON c.source = w.source AND c.series = w.series
WHERE c.frequency = 'W' AND date >= '` + krWeeklyHistoryStart + `' AND $__timeFilter(date)
GROUP BY date
ORDER BY date
`

// krWeeklyGoodsIndexSQL — подзапрос 4-недельной средней: индекс по дате, усреднённый по всем
// недельным товарным рядам каталога (та же база, что у панели 2).
const krWeeklyGoodsIndexSQL = `SELECT date AS time, avg(value) AS idx
FROM v_rosstat_macro AS w
INNER JOIN v_series_catalog AS c ON c.source = w.source AND c.series = w.series
WHERE c.frequency = 'W' AND date >= '` + krWeeklyHistoryStart + `'
GROUP BY date`

// krWeeklyAggregateIndexSQL — подзапрос 4-недельной средней по официальному агрегату: одна
// серия, поэтому idx — само значение. JOIN каталога оставлен намеренно: он подтверждает, что
// ряд недельный, и держит правило «недельные ряды отбираются каталогом» в каждом недельном запросе.
const krWeeklyAggregateIndexSQL = `SELECT date AS time, value AS idx
FROM v_rosstat_macro AS w
INNER JOIN v_series_catalog AS c ON c.source = w.source AND c.series = w.series
WHERE c.frequency = 'W' AND c.series = '` + krWeeklyAggregateSeries + `' AND date >= '` + krWeeklyHistoryStart + `'`

// krWeeklyAvg4Template — 4-недельная средняя недельного индекса, приведённая к году: окно —
// четыре последние недели, внутри — геометрическое среднее (avg логарифмов + exp), затем
// степень 52 («если бы средний за 4 недели недельный темп держался весь год»). Эквивалент —
// накопленный за 4 недели рост в степень 13. n = 4 отбрасывает первые три точки: окно по 1–3
// неделям дало бы ложный отсчёт. %s по порядку: алиас серии, подзапрос «дата + индекс».
const krWeeklyAvg4Template = `SELECT time, y AS %s
FROM (
    SELECT time,
           pow(exp(avg(ln(idx / 100)) OVER (ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)), 52) - 1 AS y,
           count() OVER (ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS n
    FROM (
        %s
    )
)
WHERE n = 4 AND $__timeFilter(time)
ORDER BY time
`

// krWeeklyAvg4SQL собирает запрос 4-недельной средней: панель 36 читает обе базы одной формой.
func krWeeklyAvg4SQL(indexSubquery, alias string) string {
	return fmt.Sprintf(krWeeklyAvg4Template, alias, indexSubquery)
}

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

// krWeeklySAARSQL — недельный SA-агрегат ИПЦ (v_weekly_cpi_sa): value в процентах,
// панель 4 в percentunit (доли), поэтому /100. Ряд включает nowcast текущего месяца
// (partial = 1): последняя точка — оценка, а не официальный SA ЦБ; октябрь 2026
// искажён тарифной неделей 2026-10-05, это headline-агрегат, не core.
// Время nowcast-точки — as_of, а не конец месяца: метка date у неполного месяца
// лежит в будущем (2026-10-31 при now 2026-10-22), и фильтр по date вырезал бы
// её из окна — nowcast не был бы виден на дашборде вовсе.
const krWeeklySAARSQL = `SELECT if(partial = 1, as_of, date) AS time, value / 100 AS "Недельный SA-агрегат ИПЦ, SAAR"
FROM v_weekly_cpi_sa
WHERE series = 'weekly_cpi:saar' AND $__timeFilter(as_of)
ORDER BY time
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

// krBankLoansYoYTemplate — годовой темп кредитного портфеля банка: сумма кредитов ЮЛ и ФЛ считается
// ДО расчёта темпа (lagInFrame по сумме, а не среднее из двух темпов), окно — по полному ряду,
// фильтр времени снаружи подзапроса. Обе метрики — gross: customer_loans в витрине netto и у Сбера
// обрывается 2026-06, поэтому в один ряд с gross не смешивается. %s по порядку: алиас серии, банк.
const krBankLoansYoYTemplate = `SELECT time, yoy AS %s
FROM (
    SELECT report_month AS time,
           sum(value_bln_rub) / nullIf(lagInFrame(sum(value_bln_rub), 12) OVER (ORDER BY report_month), 0) - 1 AS yoy
    FROM v_bank_loan_portfolio_forward
    WHERE bank = '%s' AND metric IN ('loans_corporations', 'loans_individuals')
    GROUP BY report_month
)
WHERE $__timeFilter(time)
ORDER BY time
`

// krBankLoansYoYSQL собирает запрос годового темпа кредитов банка по шаблону.
func krBankLoansYoYSQL(bank, alias string) string {
	return fmt.Sprintf(krBankLoansYoYTemplate, alias, bank)
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

// krOfzCoverageSQL — покрытие спроса на аукционе: v_ofz_stress, одна точка на аукцион.
// NULL — аукцион не состоялся/сделок не было; фильтр окна по date.
const krOfzCoverageSQL = `SELECT date AS time, coverage_ratio AS "Покрытие спроса, доля"
FROM v_ofz_stress
WHERE coverage_ratio IS NOT NULL AND $__timeFilter(date)
ORDER BY date
`

// krOfzCutoffSpreadSQL — доходность отсечения минус ближайший тенор КБД ЦБ на дату
// аукциона, п.п.; кривая ЦБ — прокси вторичного рынка, а не сделки.
const krOfzCutoffSpreadSQL = `SELECT date AS time, cutoff_minus_secondary_pp AS "Отсечение − КБД, п.п."
FROM v_ofz_stress
WHERE cutoff_minus_secondary_pp IS NOT NULL AND $__timeFilter(date)
ORDER BY date
`

// krOfzDebtServiceSQL — доля обслуживания долга в доходах федерального бюджета;
// значение месячное и повторяется на аукционах месяца, поэтому max() на дату.
const krOfzDebtServiceSQL = `SELECT date AS time, max(debt_service_to_revenue) AS "Обслуживание / доходы"
FROM v_ofz_stress
WHERE debt_service_to_revenue IS NOT NULL AND $__timeFilter(date)
GROUP BY date
ORDER BY date
`

// krOfzStockChangeSQL — изменение номинала ОФЗ к предыдущему месяцу, млрд руб.;
// значение месячное, max() на дату аукциона убирает повтор по выпускам.
const krOfzStockChangeSQL = `SELECT date AS time, max(ofz_stock_change) AS "Δ запаса ОФЗ, млрд руб."
FROM v_ofz_stress
WHERE ofz_stock_change IS NOT NULL AND $__timeFilter(date)
GROUP BY date
ORDER BY date
`

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

// krConsensusTemplate — срез витрины макроопроса v_cbr_mo_br «дата + значение» по коду ряда:
// панель 15 читает шесть рядов одной формой (медианы ключевой ставки и инфляции на 2026/2027
// и границы интервала 2027). Значения витрины — уровни в % годовых, преобразований в SQL нет,
// поэтому единица панели percent. %s по порядку: алиас серии, код ряда.
const krConsensusTemplate = `SELECT date AS time, value AS %s
FROM v_cbr_mo_br
WHERE series = '%s' AND $__timeFilter(date)
ORDER BY date
`

// krConsensusSQL собирает запрос консенсуса по шаблону.
func krConsensusSQL(series, alias string) string {
	return fmt.Sprintf(krConsensusTemplate, alias, series)
}

// krMpSummaryTableSQL — последние резюме ЦБ: решение заседания, его шаг и сигнал одной таблицей,
// свежие сверху. LIMIT 3 здесь — размер карточки, а не обрезка истории: три последних резюме
// отвечают на вопрос «что решили и каким сигналом сопроводили»; история решений — точки панели 1.
const krMpSummaryTableSQL = `SELECT meeting_date AS "Заседание", published AS "Публикация", decision_action AS "Действие",
       decision_rate AS "Ставка, %", decision_delta_bp AS "Шаг, б.п.", signal AS "Сигнал"
FROM v_cbr_mp_summary
ORDER BY meeting_date DESC
LIMIT 3
`

// krMpChannelsSQL — каналы рисков последнего резюме. method = 'regex' — детерминированный слой
// разметки (LLM-строки того же вида в витрине есть, но берётся только regex); kind ограничен
// risk_pro/risk_dez — риски обоих знаков. Последнее заседание выбирается подзапросом
// max(meeting_date), а не LIMIT: LIMIT взял бы первые строки сортировки по каналу, а не строки
// последнего резюме. LIMIT у этого запроса нет вовсе (спека §5).
const krMpChannelsSQL = `SELECT channel AS "Канал", kind AS "Вид", text AS "Фрагмент"
FROM v_cbr_mp_summary_channels
WHERE method = 'regex' AND kind IN ('risk_pro', 'risk_dez')
  AND meeting_date = (SELECT max(meeting_date) FROM v_cbr_mp_summary_channels)
ORDER BY channel, kind
`

// krEventsCalendarSQL — календарь заседаний ЦБ: только будущие события (event_date >= today())
// и только category = 'cbr'. LIMIT 10 — размер карточки: десять ближайших событий, полный
// календарь читается запросом к витрине.
const krEventsCalendarSQL = `SELECT event_date AS "Дата", event_time AS "Время", title AS "Событие", threshold AS "Порог"
FROM v_events_calendar
WHERE category = 'cbr' AND event_date >= today()
ORDER BY event_date
LIMIT 10
`

// krNewsFeedSQL — событийный фон: последние 15 заголовков ленты, свежие сверху. Отбора по теме
// и значимости нет — это фон вокруг ставки, а не календарь (календарь — панель 18).
const krNewsFeedSQL = `SELECT date_msk AS "Дата", source AS "Источник", title AS "Событие"
FROM v_news_events
ORDER BY date_msk DESC
LIMIT 15
`

// krBrentSQL — Brent панели 29: фронт-месяц фьючерса MOEX BR (settle), а не спот. Ряд непрерывен
// по контрактам, но на роллах возможен разрыв — поэтому линия, а не интерполяция; оговорка
// «не спот» обязательна в Description панели.
const krBrentSQL = `SELECT date AS time, value AS "Brent (фронт-месяц), USD/барр."
FROM v_moex_market
WHERE series = 'brent_front' AND $__timeFilter(date)
ORDER BY date
`

// krBopSeriesTemplate — срез v_cbr_bop по источнику и коду ряда: у двух выгрузок платёжного
// баланса одинаковые имена рядов, но разные единицы (млн и млрд USD), поэтому source входит
// в запрос наравне с series. Первый %s — выражение значения, второй — алиас, далее source и series.
const krBopSeriesTemplate = `SELECT date AS time, %s AS %s
FROM v_cbr_bop
WHERE source = '%s' AND series = '%s' AND $__timeFilter(date)
ORDER BY date
`

// krBopSeriesSQL собирает запрос платёжного баланса по шаблону.
func krBopSeriesSQL(valueExpr, alias, source, series string) string {
	return fmt.Sprintf(krBopSeriesTemplate, valueExpr, alias, source, series)
}

// krSurveySeriesTemplate — срез v_cbr_survey по коду ряда: панель 32 читает балансовые индексы
// и загрузку мощностей одной формой. %s по порядку: алиас серии, код ряда.
const krSurveySeriesTemplate = `SELECT date AS time, value AS %s
FROM v_cbr_survey
WHERE series = '%s' AND $__timeFilter(date)
ORDER BY date
`

// krSurveySeriesSQL собирает запрос мониторинга предприятий по шаблону.
func krSurveySeriesSQL(series, alias string) string {
	return fmt.Sprintf(krSurveySeriesTemplate, alias, series)
}

// krCcybSeriesTemplate — срез v_cbr_ccyb_indicators по коду АЦН-индикатора: панель 33 читает
// кредитные гэпы (п.п.) и темпы роста кредита (%). %s по порядку: алиас серии, код ряда.
const krCcybSeriesTemplate = `SELECT date AS time, value AS %s
FROM v_cbr_ccyb_indicators
WHERE series = '%s' AND $__timeFilter(date)
ORDER BY date
`

// krCcybSeriesSQL собирает запрос АЦН-индикаторов по шаблону.
func krCcybSeriesSQL(series, alias string) string {
	return fmt.Sprintf(krCcybSeriesTemplate, alias, series)
}

// krDepositsLevelSQL — уровень депозитов населения панели 31: витрина хранит млн руб., панель
// показывает млрд (÷1000), чтобы ось не была шестизначной. Канон канала сбережений —
// «Депозиты населения» = ФЛ + ИП (KEY_RATE_CHANNELS §8.1), а не ряд физлиц.
const krDepositsLevelSQL = `SELECT date AS time, value / 1000 AS "Депозиты населения, млрд руб."
FROM v_cbr_macro
WHERE series = 'Депозиты населения' AND $__timeFilter(date)
ORDER BY date
`

// krDepositsInflowSQL — месячный приток депозитов панели 31: разность с предыдущим месяцем.
// lagInFrame с третьим аргументом NULL оставляет первую точку пустой, а не считает её как
// value − 0 (иначе на графике был бы ложный выброс). Фильтр времени — СНАРУЖИ подзапроса:
// иначе окно обрезало бы ряд и первый приток окна стал бы ложным.
const krDepositsInflowSQL = `SELECT time, chg AS "Приток за месяц, млрд руб."
FROM (
    SELECT date AS time,
           (value - lagInFrame(value, 1, NULL) OVER (ORDER BY date)) / 1000 AS chg
    FROM v_cbr_macro
    WHERE series = 'Депозиты населения'
)
WHERE $__timeFilter(time)
ORDER BY time
`

// krPPIMMAnnualSQL — годовое выражение месячного ИЦП: индекс к прошлому месяцу (100 = без
// изменения) возводится в степень 12. Длинный ряд — с 1998 года.
const krPPIMMAnnualSQL = `SELECT date AS time, pow(value / 100, 12) - 1 AS "ИЦП, м/м → годовое выражение"
FROM v_rosstat_ppi
WHERE series = 'ИЦП по ВЭД, РФ, m/m' AND $__timeFilter(date)
ORDER BY date
`

// krPPIYYAnnualSQL — официальный г/г: в витрине уже индекс к прошлому году, поэтому темп —
// value / 100 - 1, без степени. Формула другая, чем у м/м (индекс против темпа), и это
// подписано в Description панели 37.
const krPPIYYAnnualSQL = `SELECT date AS time, value / 100 - 1 AS "ИЦП, г/г (официальный)"
FROM v_rosstat_ppi
WHERE series = 'ИЦП по ВЭД, РФ, y/y' AND $__timeFilter(date)
ORDER BY date
`

// krPPISAARTemplate — срез ppi_sa: SAAR 3м уже в процентах, поэтому значение отдаётся как есть
// (никакого pow — иначе годовой темп возвёлся бы в квадрат). %s по порядку: алиас, код ряда.
const krPPISAARTemplate = `SELECT date AS time, value AS %s
FROM v_rosstat_ppi
WHERE series = '%s' AND $__timeFilter(date)
ORDER BY date
`

// krPPISAARSQL собирает запрос слоя ppi_sa по шаблону.
func krPPISAARSQL(series, alias string) string {
	return fmt.Sprintf(krPPISAARTemplate, alias, series)
}

// krFAOTemplate — индекс ФАО по месяцу наблюдения: date витрины — дата публикации (1-е число
// следующего месяца), period_date — сам месяц наблюдения. Панель читает period_date, иначе
// картинка сдвинута на месяц. %s по порядку: алиас, код ряда.
const krFAOTemplate = `SELECT period_date AS time, value AS %s
FROM v_fao_food_price
WHERE series = '%s' AND $__timeFilter(period_date)
ORDER BY period_date
`

// krFAOSQL собирает запрос индекса ФАО по шаблону.
func krFAOSQL(series, alias string) string {
	return fmt.Sprintf(krFAOTemplate, alias, series)
}

// krFedstatPrice — позиция ценовой панели: код ряда витрины (он же название товара с единицей
// измерения) и короткий алиас для легенды.
type krFedstatPrice struct {
	series string
	alias  string
}

// krFedstatPrices — шесть позиций панели 39: еда и моторное топливо, которые читаются на одной
// оси 50–760 ₽. Хлеб не взят: у позиции в 2022 сменилось название ряда, и две серии не сшиваются.
var krFedstatPrices = []krFedstatPrice{
	{series: "Говядина (кроме бескостного мяса), кг", alias: `"Говядина, ₽/кг"`},
	{series: "Куры охлажденные и мороженые, кг", alias: `"Куры, ₽/кг"`},
	{series: "Молоко питьевое цельное пастеризованное 2,5-3,2% жирности, л", alias: `"Молоко 2,5–3,2 %, ₽/л"`},
	{series: "Картофель, кг", alias: `"Картофель, ₽/кг"`},
	{series: "Яйца куриные, 10 шт.", alias: `"Яйца, ₽/10 шт."`},
	{series: "Бензин автомобильный марки АИ-92, л", alias: `"Бензин АИ-92, ₽/л"`},
}

// krFedstatPriceTemplate — уровень цены в рублях: region входит в запрос, потому что 2026 год
// публикуется в другом определении РФ и в панель не берётся (иначе — разрыв 2023–2025 и смена
// территориального определения). %s по порядку: алиас, ряд, регион.
const krFedstatPriceTemplate = `SELECT date AS time, value AS %s
FROM v_fedstat_weekly_prices
WHERE series = '%s' AND region = '%s' AND $__timeFilter(date)
ORDER BY date
`

// krFedstatPriceSQL собирает запрос средней цены по шаблону.
func krFedstatPriceSQL(p krFedstatPrice) string {
	return fmt.Sprintf(krFedstatPriceTemplate, p.alias, p.series, krFedstatRegion)
}

// krPanelDefaults — общие визуальные конвенции дашборда (выведены из UI-экспорта): палитра classic,
// легенда снизу таблицей с calcs (последнее/среднее/максимум), tooltip по одной серии, линия
// шириной 1 без заливки, градиента и соединения разрывов (spanNulls false — разрыв недельного
// ряда честно остаётся разрывом). Интерполяция — smooth: соседние месячные точки соединяются
// сглаженной линией, а не ломаной (значения при этом не сглаживаются). Панели добавляют свои
// Id/Title/Description/Span и таргеты поверх: так конвенции не разъезжаются между рядами.
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
		LineInterpolation(common.LineInterpolationSmooth).
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

// krDashedProps — пунктирная линия: серия-референс не должна читаться как ещё один равноправный
// ряд (прошлый год панели 2, М2 на панели 35).
func krDashedProps() []dashboard.DynamicConfigValue {
	return []dashboard.DynamicConfigValue{{
		Id:    "custom.lineStyle",
		Value: common.LineStyle{Dash: []float64{10, 10}, Fill: ptr(common.LineStyleFillDash)},
	}}
}

// krPastYearProps — свойства серии прошлого года панели 2: пунктир из UI-экспорта, чтобы
// календарное наложение не читалось как ещё один ряд текущего года.
func krPastYearProps() []dashboard.DynamicConfigValue {
	return krDashedProps()
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
// и инфляция» (панели 1–5), 22 «Деньги, кредит, бюджет, труд» (панели 6–10 и 35), 23 «Рыночный
// контур» (панели 11–14 и 25–28), 24 «Сигнал, консенсус, календарь» (панели 15–19) и 34 «Остальные
// каналы решения» (панели 29–33) — полный контракт спеки §5. Порядок вызовов WithRow/WithPanel
// задаёт и порядок сетки, и нумерацию панелей.
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
		WithPanel(krWeeklyAvg4Panel()).
		WithPanel(krWeeklyRisersPanel()).
		WithPanel(krInflationPanel()).
		WithPanel(krExpectationsPanel()).
		WithRow(dashboard.NewRowBuilder("Деньги, кредит, бюджет, труд").Id(firstKeyRateRowID + 1)).
		WithPanel(krBankLoansPanel()).
		WithPanel(krMoneyCreditPanel()).
		WithPanel(krCreditRatesPanel()).
		WithPanel(krBudgetPanel()).
		WithPanel(krLabourPanel()).
		WithPanel(krSberSpendingPanel()).
		WithRow(dashboard.NewRowBuilder("Рыночный контур").Id(firstKeyRateRowID + 2)).
		WithPanel(krKeyrateTrajectoryPanel()).
		WithPanel(krKeyrateSnapshotPanel()).
		WithPanel(krOfzCurvePanel()).
		WithPanel(krRuoniaPanel()).
		WithPanel(krOfzCoveragePanel()).
		WithPanel(krOfzCutoffSpreadPanel()).
		WithPanel(krOfzDebtServicePanel()).
		WithPanel(krOfzStockChangePanel()).
		WithRow(dashboard.NewRowBuilder("Сигнал, консенсус, календарь").Id(firstKeyRateRowID + 3)).
		WithPanel(krConsensusPanel()).
		WithPanel(krMpSummaryPanel()).
		WithPanel(krMpChannelsPanel()).
		WithPanel(krEventsCalendarPanel()).
		WithPanel(krNewsFeedPanel()).
		WithRow(dashboard.NewRowBuilder("Остальные каналы решения").Id(channelRowID)).
		WithPanel(krExternalFxPanel()).
		WithPanel(krExternalBopPanel()).
		WithPanel(krSavingsPanel()).
		WithPanel(krOutputGapPanel()).
		WithPanel(krAutonomousFactorsPanel()).
		WithRow(dashboard.NewRowBuilder("Цены производителей, продовольствие, средние цены").Id(pricesRowID)).
		WithPanel(krPPIPanel()).
		WithPanel(krFAOPanel()).
		WithPanel(krFedstatPricesPanel())
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
			"сдвинутый на +1 год для наложения на текущий; диапазон дат фильтрует уже сдвинутое время, "+
			"поэтому оба года видны в одном окне. Порог 4% — цель ЦБ.").
		WithTarget(timeseriesQuery("A", krWeeklyAnnualizedSQL("date", `"Текущий год, годовое выражение"`, "toYear(now())"))).
		WithTarget(timeseriesQuery("B", krWeeklyAnnualizedSQL("date_add(YEAR, 1, date)", `"Прошлый год, годовое выражение"`, "toYear(now()) - 1"))).
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
			"Базовую нельзя читать как индекс. Источники — v_rosstat_macro (ИПЦ) и v_cbr_macro (Базовый ИПЦ). " +
			"Третья серия — недельный SA-агрегат ИПЦ (v_weekly_cpi_sa, weekly_cpi:saar, value/100): это headline-агрегат, " +
			"а не core, поэтому сравнивать его с базовой как замену нельзя — на окне 2023+ в отдельные месяцы расхождение достигает " +
			"5–9 п.п. (состав корзины и годовое усиление ×13). Последняя точка — nowcast текущего месяца (partial = 1) " +
			"на дате as_of; в октябре 2026 искажён тарифной неделей 2026-10-05.").
		WithTarget(timeseriesQuery("A", krCPIAnnualizedSQL)).
		WithTarget(timeseriesQuery("B", krBaseInflationAnnualizedSQL)).
		WithTarget(timeseriesQuery("C", krWeeklySAARSQL)).
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

// krBankLoansPanel — панель 35: кредиты, выданные банками (Сбер, ВТБ), как опережающий срез
// месячных данных по М2. Сумма кредитов ЮЛ и ФЛ — gross (до резервов): customer_loans в витрине
// netto и у Сбера обрывается 2026-06, поэтому с gross не смешивается. М2 SA — пунктирный референс:
// месячный агрегат ЦБ выходит позже отчётов банков, и по панели видно, насколько кредит банков
// опережает динамику денежной массы. Значения — доли г/г, поэтому percentunit и нулевой порог.
func krBankLoansPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(35).
		Title("Кредиты банков (Сбер, ВТБ) — опережающий срез М2, г/г").
		Span(24).
		Description("Годовой темп кредитов, выданных банками: сумма кредитов ЮЛ и ФЛ (gross, до резервов) "+
			"по v_bank_loan_portfolio_forward для Сбера (РСБУ) и ВТБ (МСФО). Темп считается по сумме — "+
			"value к значению 12 месяцев назад − 1, окном по полному ряду; фильтр времени снаружи "+
			"подзапроса. Это опережающий срез кредитной составляющей М2: отчёты банков за месяц выходят "+
			"раньше денежной статистики ЦБ (Сбер за сентябрь — 09.10.2026), а М2 SA (v_cbr_macro) "+
			"показана пунктиром как референс. Оговорки: customer_loans — netto и у Сбера кончается "+
			"2026-06, поэтому не используется; базы РСБУ и МСФО разные — сравнивать темпы банков между "+
			"собой нельзя, только динамику каждого с М2. Пропуск ВТБ за 2026-01 — разрыв источника, "+
			"а не ноль. Значения — доли (Grafana печатает «11.9%»).").
		WithTarget(timeseriesQuery("A", krBankLoansYoYSQL("SBER", `"Сбер: кредиты ЮЛ+ФЛ, г/г"`))).
		WithTarget(timeseriesQuery("B", krBankLoansYoYSQL("VTBR", `"ВТБ: кредиты ЮЛ+ФЛ, г/г"`))).
		WithTarget(timeseriesQuery("C", krYoYSQL("v_cbr_macro", "М2 SA", `"М2 SA, г/г — референс"`))).
		Unit(units.PercentUnit).
		Thresholds(krZeroThresholds()).
		ThresholdsStyle(krThresholdLineStyle()).
		OverrideByQuery("C", krDashedProps())
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
			"(база 100), а не доходность: единица задана пер-серийным override'ом (short), "+
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

// krOfzCoveragePanel — панель 25: покрытие спроса на аукционах ОФЗ. Одна точка на
// аукцион (v_ofz_stress), покрытие — доля: 0.3 = размещено 30 % спроса. NULL — аукцион
// не состоялся или сделок не было; это не ноль.
func krOfzCoveragePanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(25).
		Title("Аукционы ОФЗ: покрытие спроса").
		Span(spanHalf).
		Description("Отношение размещённого объёма к спросу на аукционе ОФЗ (v_ofz_stress, " +
			"coverage_ratio; 0.3 = размещено 30 % спроса). Точки — даты аукционов, а не регулярный " +
			"ряд; NULL — аукцион не состоялся или сделок не было, это не ноль.").
		WithTarget(timeseriesQuery("A", krOfzCoverageSQL)).
		Unit(units.PercentUnit)
}

// krOfzCutoffSpreadPanel — панель 26: доходность отсечения минус ближайший тенор КБД ЦБ.
// КБД — кривая бескупонной доходности, а не сделки вторичного рынка; спред в п.п.
func krOfzCutoffSpreadPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(26).
		Title("Аукционы ОФЗ: отсечение − КБД").
		Span(spanHalf).
		Description("Доходность по цене отсечения минус доходность ближайшего тенора КБД ЦБ на " +
			"дату аукциона (v_ofz_stress, cutoff_minus_secondary_pp, п.п.). КБД — кривая " +
			"бескупонной доходности ЦБ, а не сделки вторичного рынка; положительное значение — " +
			"аукцион дороже кривой. NULL — нет одной из доходностей.").
		WithTarget(timeseriesQuery("A", krOfzCutoffSpreadSQL)).
		Unit(units.Percent)
}

// krOfzDebtServicePanel — панель 27: доля обслуживания долга в доходах федерального
// бюджета. Значение месячное и повторяется на аукционах месяца; 2022–2023 — NULL.
func krOfzDebtServicePanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(27).
		Title("Долг: обслуживание в доходах").
		Span(spanHalf).
		Description("Расходы на обслуживание долга / доходы федерального бюджета за месяц " +
			"(v_ofz_stress, debt_service_to_revenue; 0.05 = 5 %). До 2022 — бюджетная строка " +
			"(федеральный и муниципальный долг), с 2024 — обслуживание федеральных ценных бумаг; " +
			"2022–2023 — NULL (пробел источника), поэтому линия разорвана, а не равна нулю.").
		WithTarget(timeseriesQuery("A", krOfzDebtServiceSQL)).
		Unit(units.PercentUnit)
}

// krOfzStockChangePanel — панель 28: изменение номинала ОФЗ к предыдущему месяцу.
// Значение месячное, повторяется на аукционах месяца; NULL — нет одного из месяцев.
func krOfzStockChangePanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(28).
		Title("Долг: изменение запаса ОФЗ").
		Span(spanHalf).
		Description("Изменение номинала ОФЗ на 1-е число месяца к предыдущему месяцу " +
			"(v_ofz_stress, ofz_stock_change, млрд руб.): положительное — чистое наращивание " +
			"запаса, отрицательное — погашение. NULL — нет одного из месяцев (пропуски публикации " +
			"структуры), это не ноль.").
		WithTarget(timeseriesQuery("A", krOfzStockChangeSQL)).
		Unit(units.Rubles)
}

// krConsensusPanel — панель 15: консенсус макроопроса ЦБ — медианы прогнозов по ключевой ставке
// и инфляции на 2026 и 2027 плюс интервал 2027 (p10/p90). Шесть рядов одной витрины v_cbr_mo_br,
// значения — уровни в % годовых, поэтому единица percent и никаких преобразований в SQL.
// Цвета зафиксированы: цвет кодирует метрику, оттенок — горизонт и роль (синие — ключевая,
// зелёные — инфляция; светлее — 2026, насыщенный — 2027, самый светлый — границы интервала
// вокруг 2027-й медианы). Без фиксации палитра classic красила бы серии по порядку таргетов,
// и добавление новой серии меняло бы цвета уже собранных.
func krConsensusPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(15).
		Title("Консенсус макроопроса: ставка и инфляция").
		Span(spanHalf).
		Description("Консенсус макроэкономического опроса ЦБ (v_cbr_mo_br): медианы прогнозов "+
			"участников по ключевой ставке и инфляции на 2026 и 2027 годы (% годовых; одна точка — "+
			"волна опроса, а не непрерывный ряд) и интервал 2027 — 10-й и 90-й процентили прогнозов "+
			"по ставке. Медиана — центральная оценка участников, p10/p90 — разброс ответов: границы "+
			"интервала не являются ни диапазоном возможных ставок, ни обязательством ЦБ. Ставка "+
			"и инфляция — уровни в одних единицах, поэтому лежат на одной шкале.").
		WithTarget(timeseriesQuery("A", krConsensusSQL("key_rate_avg_2026_median", `"Ключевая, 2026 (медиана)"`))).
		WithTarget(timeseriesQuery("B", krConsensusSQL("key_rate_avg_2027_median", `"Ключевая, 2027 (медиана)"`))).
		WithTarget(timeseriesQuery("C", krConsensusSQL("cpi_avg_2026_median", `"Инфляция, 2026 (медиана)"`))).
		WithTarget(timeseriesQuery("D", krConsensusSQL("cpi_avg_2027_median", `"Инфляция, 2027 (медиана)"`))).
		WithTarget(timeseriesQuery("E", krConsensusSQL("key_rate_avg_2027_p10", `"Интервал 2027, p10"`))).
		WithTarget(timeseriesQuery("F", krConsensusSQL("key_rate_avg_2027_p90", `"Интервал 2027, p90"`))).
		Unit(units.Percent).
		OverrideByQuery("A", []dashboard.DynamicConfigValue{krFixedColorProp("light-blue")}).
		OverrideByQuery("B", []dashboard.DynamicConfigValue{krFixedColorProp("blue")}).
		OverrideByQuery("C", []dashboard.DynamicConfigValue{krFixedColorProp("light-green")}).
		OverrideByQuery("D", []dashboard.DynamicConfigValue{krFixedColorProp("green")}).
		OverrideByQuery("E", []dashboard.DynamicConfigValue{krFixedColorProp("super-light-blue")}).
		OverrideByQuery("F", []dashboard.DynamicConfigValue{krFixedColorProp("super-light-blue")})
}

// krMpSummaryPanel — панель 16: последние резюме ЦБ одной таблицей — решение заседания, его шаг
// и сигнал. LIMIT 3 в запросе — размер карточки, а не обрезка истории: три последних резюме
// отвечают на вопрос «что решили и каким сигналом сопроводили»; сами решения видны точками панели 1.
func krMpSummaryPanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Id(16).
		Title("Последние резюме ЦБ: решение и сигнал").
		Span(spanHalf).
		Description("Три последних резюме обсуждения ключевой ставки (v_cbr_mp_summary). Заседание " +
			"и публикация — РАЗНЫЕ даты: резюме выходит с лагом к решению, поэтому со ставкой на " +
			"дату заседания строка сопоставляется по «Заседанию», а не по дате публикации. Действие — " +
			"hold/cut/hike, ставка — принятый уровень (% годовых), шаг — изменение к предыдущему " +
			"решению в б.п. (минус — снижение), сигнал — формулировка резюме о направленности " +
			"дальнейших решений: это текст ЦБ, а не прогноз ставки.").
		Datasource(datasourceRef()).
		WithTarget(tableQuery("A", krMpSummaryTableSQL))
}

// krMpChannelsPanel — панель 17: каналы рисков последнего резюме. method = 'regex' — детерминированный
// слой разметки витрины, kind ограничен risk_pro/risk_dez: риски обоих знаков. Последнее заседание
// выбирается в SQL подзапросом max(meeting_date), LIMIT у запроса нет: строк ровно столько, сколько
// каналов рисков размечено в последнем резюме (см. krMpChannelsSQL).
func krMpChannelsPanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Id(17).
		Title("Каналы последнего резюме (риски)").
		Span(spanHalf).
		Description("Каналы рисков последнего опубликованного резюме (v_cbr_mp_summary_channels): " +
			"фрагменты текста, размеченные регексами (method = 'regex' — детерминированный слой; " +
			"LLM-разметка того же вида в витрине существует, но здесь не берётся). risk_pro — " +
			"проинфляционные риски, risk_dez — дезинфляционные: «Вид» различает знак, «Канал» — тему " +
			"(спрос, рынок труда, бюджет, курсы и т.д.). Последнее заседание берётся подзапросом " +
			"max(meeting_date). Это названные ЦБ риски, а не оценка их вероятностей.").
		Datasource(datasourceRef()).
		WithTarget(tableQuery("A", krMpChannelsSQL))
}

// krEventsCalendarPanel — панель 18: календарь ЦБ — будущие события (event_date >= today())
// категории 'cbr'. LIMIT 10 — размер карточки: десять ближайших событий. threshold — техническое
// условие сценария контура (JSON), а не решение ЦБ: подписано в Description, чтобы порог
// не прочли как прогноз ставки.
func krEventsCalendarPanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Id(18).
		Title("Календарь заседаний ЦБ").
		Span(spanHalf).
		Description("Ближайшие события календаря ЦБ (v_events_calendar, category = 'cbr'): заседания " +
			"Совета директоров по ключевой ставке, только будущие (event_date >= today()), десять " +
			"ближайших. Время — время публикации решения; у служебных строк календаря его может " +
			"не быть. Порог — техническое условие сценария контура (JSON, например минимальный шаг " +
			"ставки: >=25 б.п.), а не часть решения ЦБ и не прогноз ставки; пусто — порога нет.").
		Datasource(datasourceRef()).
		WithTarget(tableQuery("A", krEventsCalendarSQL))
}

// krNewsFeedPanel — панель 19: событийный фон — лента новостей. Отбора по теме и значимости нет:
// это фон вокруг ставки, а не календарь (календарь — панель 18); свежие сверху, 15 последних.
// date_msk — дата по московскому времени: лента не зависит от часового пояса браузера.
func krNewsFeedPanel() *table.PanelBuilder {
	return table.NewPanelBuilder().
		Id(19).
		Title("Событийный фон (лента)").
		Span(spanHalf).
		Description("Лента событий (v_news_events): дата по МСК, источник (cbr/tass/interfax/rbc) " +
			"и заголовок; свежие сверху, пятнадцать последних. Отбора по теме и значимости нет — " +
			"это фон вокруг ставки, а не календарь (заседания — панель 18); заголовки даны как " +
			"в фиде источника, один сюжет может занимать несколько строк.").
		Datasource(datasourceRef()).
		WithTarget(tableQuery("A", krNewsFeedSQL))
}

// krExternalFxPanel — панель 29: внешние условия — курс ЦБ и Brent. Единицы разные (руб. за 1 USD
// и USD/барр.), поэтому у Brent своя правая ось; «фронт-месяц, не спот» — обязательная оговорка,
// иначе settle фьючерса прочли бы как спотовую цену.
func krExternalFxPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(29).
		Title("Внешние условия: курс ЦБ и Brent").
		Span(spanHalf).
		Description("Курс ЦБ (v_cbr_macro, series = 'cbr_currency_usd') — руб. за 1 USD, левая ось; "+
			"Brent — фронт-месяц фьючерса MOEX BR (v_moex_market, series = 'brent_front', settle) — "+
			"USD/барр., правая ось. Brent — НЕ спот и не back-adjusted: на роллах возможен разрыв, "+
			"поэтому линия не интерполируется. Курс — официальный, не биржевой.").
		WithTarget(timeseriesQuery("A", krMacroValueSQL("cbr_currency_usd", `"Курс USD/RUB, руб. за 1 USD"`))).
		WithTarget(timeseriesQuery("B", krBrentSQL)).
		Unit(units.Rubles).
		OverrideByQuery("B", []dashboard.DynamicConfigValue{
			{Id: "unit", Value: units.Dollars},
			axisPlacementRightProp(),
		})
}

// krExternalBopPanel — панель 30: счёт текущих операций. Стандартная презентация ЦБ (млн USD)
// приведена к млрд USD и нарисована линией; месячная оценка (уже млрд USD) — точками, потому что
// источник публикует только последние месяцы. Смешивать единицы нельзя: пересчёт в SQL — ÷1000.
func krExternalBopPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(30).
		Title("Внешние условия: счёт текущих операций").
		Span(spanHalf).
		Description("Счёт текущих операций платёжного баланса (v_cbr_bop): линия — стандартная "+
			"презентация ЦБ (source = 'cbr_bop_standard', млн USD → млрд USD делением на 1000, "+
			"кварталы с 1994); точки — месячная оценка (source = 'cbr_bop_est', млрд USD), публикуются "+
			"только последние месяцы. Это две выгрузки одного показателя: стандартная — для сверки, "+
			"месячная — свежая оценка; ноль — значение, а не пропуск.").
		WithTarget(timeseriesQuery("A", krBopSeriesSQL("value / 1000",
			`"Счёт текущих операций (квартал), млрд USD"`, "cbr_bop_standard", "Счет текущих операций (квартал)"))).
		WithTarget(timeseriesQuery("B", krBopSeriesSQL("value",
			`"Счёт текущих операций (месяц), млрд USD"`, "cbr_bop_est", "Счет текущих операций (месяц)"))).
		Unit(units.Dollars).
		OverrideByQuery("B", pointsOnlyProps())
}

// krSavingsPanel — панель 31: сберегательное поведение — депозиты населения. Канон канала
// («Депозиты населения» = ФЛ + ИП) берётся из KEY_RATE_CHANNELS §8.1; уровень и приток — одна
// серия, поэтому две шкалы: абсолютный уровень и месячное изменение. Первая точка притока NULL
// (lagInFrame с NULL), а не value − 0.
func krSavingsPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(31).
		Title("Сбережения: депозиты населения").
		Span(spanHalf).
		Description("«Депозиты населения» (v_cbr_macro) = депозиты ФЛ + ИП — канон канала сбережений "+
			"(KEY_RATE_CHANNELS §8.1). Левая ось — уровень (млрд руб.), правая — месячный приток "+
			"(value − предыдущий месяц, млрд руб.; первая точка NULL, а не выброс). Уровни ФЛ и ИП "+
			"по отдельности с уровнем населения не смешиваются.").
		WithTarget(timeseriesQuery("A", krDepositsLevelSQL)).
		WithTarget(timeseriesQuery("B", krDepositsInflowSQL)).
		Unit(units.Rubles).
		OverrideByQuery("B", []dashboard.DynamicConfigValue{axisPlacementRightProp()})
}

// krOutputGapPanel — панель 32: разрыв выпуска/перегрев. ИБК (общий и промышленный) — балансовые
// индексы SA в пунктах, загрузка мощностей — % (правая ось). Это прокси: ЦБ числовым рядом
// разрыв выпуска не публикует, поэтому панель не выдаёт её за сам разрыв.
func krOutputGapPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(32).
		Title("Разрыв выпуска: бизнес-климат, ИБК и загрузка").
		Span(spanHalf).
		Description("ИБК Банка России (v_cbr_survey, series = 'business_climate_sa') и ИБК "+
			"промышленности ('industry_total_ibk_sa') — балансовые индексы SA в пунктах (левая ось); "+
			"загрузка производственных мощностей ('capacity_utilization_sa', %) — правая ось. "+
			"Это ПРОКСИ разрыва выпуска, а не сам разрыв: ЦБ числовым рядом его не публикует "+
			"(KEY_RATE_CHANNELS §8).").
		WithTarget(timeseriesQuery("A", krSurveySeriesSQL("business_climate_sa", `"Бизнес-климат (ИБК, SA), пункты"`))).
		WithTarget(timeseriesQuery("B", krSurveySeriesSQL("industry_total_ibk_sa", `"ИБК — промышленность (SA), пункты"`))).
		WithTarget(timeseriesQuery("C", krSurveySeriesSQL("capacity_utilization_sa", `"Загрузка мощностей (SA), %"`))).
		Unit(units.Short).
		OverrideByQuery("C", []dashboard.DynamicConfigValue{
			{Id: "unit", Value: units.Percent},
			axisPlacementRightProp(),
		})
}

// krAutonomousFactorsPanel — панель 33: автономные (немонетарные) факторы — АЦН-индикаторы ЦБ.
// Гэпы — п.п., темпы роста кредита — % (правая ось). Край ряда 2024-07/2024-10 — свойство
// публикации: после него NULL, а не ноль, и об этом сказано в Description.
func krAutonomousFactorsPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(33).
		Title("Автономные факторы: кредитные гэпы и рост кредита").
		Span(24).
		Description("АЦН-индикаторы ЦБ (v_cbr_ccyb_indicators): кредитные гэпы broad и ФЛ (п.п., "+
			"левая ось) и годовые темпы прироста кредитов ФЛ и ЮЛ в рублях (%, правая ось). Ряд "+
			"источника заканчивается 2024-07 у гэпов и 2024-10 у темпов: после — NULL, НЕ ноль. "+
			"Это отставание публикации, а не отсутствие данных в контуре.").
		WithTarget(timeseriesQuery("A", krCcybSeriesSQL("credit_gap_broad", `"Кредитный гэп (широкий), п.п."`))).
		WithTarget(timeseriesQuery("B", krCcybSeriesSQL("credit_gap_hh", `"Кредитный гэп (ФЛ), п.п."`))).
		WithTarget(timeseriesQuery("C", krCcybSeriesSQL("hh_yoy", `"Кредиты ФЛ, г/г, %"`))).
		WithTarget(timeseriesQuery("D", krCcybSeriesSQL("nfo_rub_yoy", `"Кредиты ЮЛ в рублях, г/г, %"`))).
		Unit(units.Short).
		OverrideByQuery("C", []dashboard.DynamicConfigValue{
			{Id: "unit", Value: units.Percent},
			axisPlacementRightProp(),
		}).
		OverrideByQuery("D", []dashboard.DynamicConfigValue{
			{Id: "unit", Value: units.Percent},
			axisPlacementRightProp(),
		})
}

// krWeeklyAvg4Panel — панель 36: недельные цены в годовом выражении на полной истории с
// 2022-01-10 и две 4-недельные средние. Панель 2 режет ряд по годам (календарное наложение),
// поэтому полная история живёт здесь; окно панели — 5 лет, иначе общий диапазон дашборда
// (now-3y) срезал бы 2022 год и требование «ряд с 2022» не выполнялось бы.
func krWeeklyAvg4Panel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(36).
		Title("Недельные цены: 4-недельная средняя, годовое выражение").
		Span(spanHalf).
		TimeFrom("5y").
		Description("Средний недельный индекс цен недельных товарных рядов (v_rosstat_macro, отбор — " +
			"v_series_catalog.frequency = 'W') в годовом выражении: pow(индекс/100, 52) - 1 — «если бы такая " +
			"неделя шла весь год». Это не SAAR и не сезонная корректировка. 4-нед. средняя — геометрическое " +
			"среднее четырёх последних недель, приведённое к году той же степенью 52 (эквивалент: накопленный " +
			"за 4 недели рост в степень 13); первые три недели ряда не показываются — окно по ним неполное. " +
			"История с 2022-01-10: до этой даты недельные ряды собраны из бюллетеней с другим набором позиций, " +
			"и средние несравнимы. Серия «недельный ИПЦ» — официальный агрегат «Индекс потребительских цен " +
			"(недельная оценка)», он отстаёт от товарного слоя на неделю. Порог 4 % — цель ЦБ.").
		WithTarget(timeseriesQuery("A", krWeeklyHistorySQL)).
		WithTarget(timeseriesQuery("B", krWeeklyAvg4SQL(krWeeklyGoodsIndexSQL, `"4-нед. средняя (товары)"`))).
		WithTarget(timeseriesQuery("C", krWeeklyAvg4SQL(krWeeklyAggregateIndexSQL, `"4-нед. средняя (недельный ИПЦ)"`))).
		Unit(units.PercentUnit).
		Thresholds(krFourPercentThresholds()).
		ThresholdsStyle(krThresholdLineStyle())
}

// krPPIPanel — панель 37: ИЦП в годовом выражении. Две разные формулы (индекс к прошлому месяцу
// против индекса к прошлому году) и два разных начала ряда — подписаны в Description. Порог 4 %
// не ставится: это цель по потребительской инфляции, а не по ценам производителей.
func krPPIPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(37).
		Title("ИЦП: годовое выражение").
		Span(spanHalf).
		TimeFrom("10y").
		Description("ИЦП по ВЭД (Промышленность, v_rosstat_ppi). «м/м → годовое выражение» = " +
			"pow(индекс/100, 12) - 1 с 1998: индекс к прошлому месяцу, приведённый к году. «г/г» = " +
			"индекс/100 - 1 с 2021: это уже темп к прошлому году, степень к нему не применяется. " +
			"«SAAR 3м» — сезонно скорректированный слой ppi_sa (X-13ARIMA-SEATS), «ядро» — KIPC food + " +
			"non-food; это расчёт контура, а не официальный ряд. Порога 4 % здесь нет: 4 % — цель ЦБ по " +
			"потребительской инфляции, на цены производителей она не распространяется.").
		WithTarget(timeseriesQuery("A", krPPIMMAnnualSQL)).
		WithTarget(timeseriesQuery("B", krPPIYYAnnualSQL)).
		WithTarget(timeseriesQuery("C", krPPISAARSQL("ppi_sa:total:saar3", `"ИЦП SA: SAAR 3м"`))).
		WithTarget(timeseriesQuery("D", krPPISAARSQL("ppi_sa:core:saar3", `"ИЦП SA: SAAR 3м, ядро"`))).
		Unit(units.PercentUnit)
}

// krFAOPanel — панель 38: индексы продовольственных цен ФАО. Время — месяц наблюдения
// (period_date), а не дата публикации: иначе весь ряд сдвинут на месяц вперёд.
func krFAOPanel() *timeseries.PanelBuilder {
	return krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(38).
		Title("ФАО: индексы продовольственных цен").
		Span(spanHalf).
		TimeFrom("10y").
		Description("Индексы продовольственных цен ФАО (v_fao_food_price): сводный и пять подиндексов, " +
			"номинальные, 2014-2016 = 100; real-индекса в источнике нет. По оси времени — period_date " +
			"(месяц наблюдения); date витрины — дата публикации (1-е число следующего месяца), и на " +
			"графике она сдвинула бы ряд на месяц. Это внешний фон продовольственной инфляции, а не " +
			"российские цены.").
		WithTarget(timeseriesQuery("A", krFAOSQL("Food Price Index", `"ФАО: сводный"`))).
		WithTarget(timeseriesQuery("B", krFAOSQL("Cereals", `"ФАО: зерновые"`))).
		WithTarget(timeseriesQuery("C", krFAOSQL("Dairy", `"ФАО: молочные"`))).
		WithTarget(timeseriesQuery("D", krFAOSQL("Meat", `"ФАО: мясо"`))).
		WithTarget(timeseriesQuery("E", krFAOSQL("Oils", `"ФАО: масла"`))).
		WithTarget(timeseriesQuery("F", krFAOSQL("Sugar", `"ФАО: сахар"`))).
		Unit(units.Short)
}

// krFedstatPricesPanel — панель 39: средние потребительские цены в рублях (ЕМИСС 37426). Только
// регион «Российская Федерация»: 2026 год публикуется в другом определении РФ, и без фильтра
// линия получила бы разрыв 2023–2025 и смену территории. Окно панели — 20 лет: ряд кончается
// 2022-12-26, и при общем диапазоне дашборда (now-3y) панель была бы пустой.
func krFedstatPricesPanel() *timeseries.PanelBuilder {
	builder := krPanelDefaults(timeseries.NewPanelBuilder()).
		Id(39).
		Title("Средние потребительские цены (ФЕДСТАТ), ₽").
		Span(spanHalf).
		TimeFrom("20y").
		Description("Средние потребительские цены ЕМИСС (v_fedstat_weekly_prices), рубли за единицу из " +
			"названия ряда (кг, л, 10 шт.) — это уровень цены, а не индекс: изменение за n недель = " +
			"value_t / value_(t−n) − 1. Ряд идёт 2008-01-09 … 2022-12-26: в 2023–2025 источник данных не " +
			"отдаёт, а 2026 публикуется в другом определении РФ («без учёта новых субъектов») и в панель " +
			"не взят — поэтому линия непрерывна и заканчивается 2022-12-26. Окно панели — 20 лет, чтобы " +
			"история читалась независимо от общего диапазона дашборда.").
		Unit(units.Rubles)
	for i, p := range krFedstatPrices {
		builder = builder.WithTarget(timeseriesQuery(string(rune('A'+i)), krFedstatPriceSQL(p)))
	}
	return builder
}
