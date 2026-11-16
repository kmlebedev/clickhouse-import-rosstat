package weeklysa

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// weeklySALatestVintage — фильтр последнего винтажа: строки последнего расчёта.
// Винтаж = (vintage, computed_at): отсечка входа и момент расчёта. Без фильтра
// витрины размножили бы строки по всем винтажам.
const weeklySALatestVintage = `computed_at = (SELECT max(computed_at) FROM weekly_cpi_sa FINAL)`

// weeklySAView — витрина MCP-агента над производной таблицей: последний винтаж.
// Длинная форма: одна строка на (source, series, date); source/series совпадают с
// v_series_catalog, поэтому описание ряда ищется по тому же ключу. Витрину
// публикует импортёр (и на no-op-пути), отдельный шаг не нужен. Полная история
// винтажей — v_weekly_cpi_sa_vintage, срез «как было известно на дату» —
// v_weekly_cpi_sa_asof.
var weeklySAView = util.View{
	Name:   "v_weekly_cpi_sa",
	Tables: []string{weeklySATable},
	Select: `SELECT 'weekly_cpi_sa' AS source,
       'weekly_cpi:sa_index' AS series,
       date,
       toNullable(toFloat64(sa_index)) AS value,
       'индекс (100 = без изменения за 4 недели)' AS unit,
       'M' AS frequency,
       as_of,
       toUInt8(partial) AS partial,
       vintage
FROM weekly_cpi_sa FINAL
WHERE ` + weeklySALatestVintage + `
UNION ALL
SELECT 'weekly_cpi_sa', 'weekly_cpi:sa_4w', date, toNullable(toFloat64(sa_4w)), '% за 4 недели', 'M', as_of, toUInt8(partial), vintage
FROM weekly_cpi_sa FINAL
WHERE ` + weeklySALatestVintage + `
UNION ALL
SELECT 'weekly_cpi_sa', 'weekly_cpi:saar', date, toNullable(toFloat64(saar)), '% годовых', 'M', as_of, toUInt8(partial), vintage
FROM weekly_cpi_sa FINAL
WHERE ` + weeklySALatestVintage,
	Comment: "Недельный SA-агрегат ИПЦ (производный): 4-нед. индекс, темп за 4 недели и SAAR по официальному недельному агрегату Росстата (бюллетени 2009–2021 + BI 2019+), X-13ARIMA-SEATS seats_log_cal. Витрина показывает ПОСЛЕДНИЙ винтаж (computed_at = max); полная история — v_weekly_cpi_sa_vintage, срез на дату — v_weekly_cpi_sa_asof(vintage_as_of=...). Одна строка на (source, series, date); source/series совпадают с v_series_catalog — описание, единицы и оговорки читать там. Полные месяцы идут с partial = 0; текущий неполный месяц — nowcast-строка с partial = 1 и as_of последней доступной недели. vintage — последняя неделя входа, из которой посчитан ряд. Ряд индикативный, не официальный SA ЦБ; 2022+ alone в research не значим. Сырая weekly_cpi_sa агенту не выдана; витрина читает её как DEFINER.",
	Columns: map[string]string{
		"source":    "Источник: всегда weekly_cpi_sa — производный ряд; совпадает с source в v_series_catalog",
		"series":    "Имя ряда из v_series_catalog: weekly_cpi:<sa_index|sa_4w|saar>",
		"date":      "Последний день отчётного месяца (полный месяц или текущий неполный nowcast)",
		"value":     "Значение ряда: индекс 100 = без изменения за 4 недели, % за 4 недели или % годовых — см. unit и v_series_catalog",
		"unit":      "Единица значения: индекс или %; точная формулировка — в v_series_catalog",
		"frequency": "Частота: M — месяц (ряд построен на месячной сетке, источник — недельный)",
		"as_of":     "Последняя дата 7-дневной сетки, использованная для месяца: для полного — последняя неделя внутри месяца, для nowcast — последняя доступная неделя",
		"partial":   "1 — текущий неполный месяц (nowcast, X-13 по полным месяцам + текущему), 0 — полный месяц",
		"vintage":   "Последняя неделя входа, из которой посчитан ряд (отсечка данных винтажа); момент расчёта — computed_at в v_weekly_cpi_sa_vintage",
	},
}

// weeklySAVintagesView — все винтажи недельного SA-агрегата из append-only истории:
// одна строка на (source, series, date, vintage, computed_at). Винтаж = отсечка
// входа + момент расчёта: ревизия прошлых недель с тем же последним воскресеньем
// даёт отдельный винтаж (computed_at), а не затирает прежний.
var weeklySAVintagesView = util.View{
	Name:   "v_weekly_cpi_sa_vintage",
	Tables: []string{weeklySAVintageTable},
	Select: `SELECT 'weekly_cpi_sa' AS source,
       'weekly_cpi:sa_index' AS series,
       date,
       toFloat64(sa_index) AS value,
       'индекс (100 = без изменения за 4 недели)' AS unit,
       'M' AS frequency,
       as_of,
       toUInt8(partial) AS partial,
       vintage,
       computed_at,
       published_at
FROM weekly_cpi_sa_vintage FINAL
UNION ALL
SELECT 'weekly_cpi_sa', 'weekly_cpi:sa_4w', date, toFloat64(sa_4w), '% за 4 недели', 'M', as_of, toUInt8(partial), vintage, computed_at, published_at
FROM weekly_cpi_sa_vintage FINAL
UNION ALL
SELECT 'weekly_cpi_sa', 'weekly_cpi:saar', date, toFloat64(saar), '% годовых', 'M', as_of, toUInt8(partial), vintage, computed_at, published_at
FROM weekly_cpi_sa_vintage FINAL`,
	Comment: "История винтажей недельного SA-агрегата ИПЦ: одна строка на (source, series, date, vintage, computed_at). vintage — последняя неделя входа (отсечка данных), computed_at — момент расчёта; ревизия прошлых недель с тем же последним воскресеньем даёт новый винтаж, старый не затирается. published_at — когда конвейер впервые увидел версии обоих источников (Nullable: у старых прогонов строки версии могло не быть). Последний винтаж — v_weekly_cpi_sa; срез «что было известно строго до даты» — v_weekly_cpi_sa_asof(vintage_as_of='YYYY-MM-DD'). Ряд индикативный, не официальный SA ЦБ; сырая weekly_cpi_sa агенту не выдана",
	Columns: map[string]string{
		"source":       "Источник: всегда weekly_cpi_sa — производный ряд; совпадает с source в v_series_catalog",
		"series":       "Имя ряда из v_series_catalog: weekly_cpi:<sa_index|sa_4w|saar>",
		"date":         "Последний день отчётного месяца (полный месяц или неполный nowcast того винтажа)",
		"value":        "Значение ряда в этом винтаже: индекс, % за 4 недели или % годовых — см. unit",
		"unit":         "Единица значения: индекс или %",
		"frequency":    "Частота: M — месячная сетка, построенная на недельном источнике",
		"as_of":        "Последняя дата 7-дневной сетки, использованная для месяца в этом винтаже",
		"partial":      "1 — месяц был неполным (nowcast) в этом винтаже, 0 — полный",
		"vintage":      "Последняя неделя входа (отсечка данных винтажа)",
		"computed_at":  "Момент расчёта винтажа (Europe/Moscow); различает ревизии одного последнего воскресенья",
		"published_at": "Когда версии обоих источников впервые появились в source_publications (Nullable — у старых прогонов строки версии могло не быть). Граница as-of строгая: published_at < vintage_as_of",
	},
}

// weeklySAAsOfView — параметризованная витрина: срез «что было известно строго
// до даты». Граница по published_at входа, тай-брейк (published_at, vintage,
// computed_at): два винтажа одной даты не дают произвольную строку. Колонковых
// комментариев нет: у параметризованной витрины ClickHouse нет схемы без
// аргументов (NOT_FOUND_COLUMN_IN_BLOCK), смысл колонок — в v_weekly_cpi_sa_vintage.
var weeklySAAsOfView = util.View{
	Name:   "v_weekly_cpi_sa_asof",
	Tables: []string{weeklySAVintageTable},
	Select: `SELECT s.source, s.series, s.date,
       argMax(s.value, (s.published_at, s.vintage, s.computed_at)) AS value,
       argMax(s.as_of, (s.published_at, s.vintage, s.computed_at)) AS as_of,
       argMax(s.partial, (s.published_at, s.vintage, s.computed_at)) AS partial,
       max(s.published_at) AS published_at,
       argMax(s.vintage, (s.published_at, s.vintage, s.computed_at)) AS vintage
FROM (
    SELECT 'weekly_cpi_sa' AS source, 'weekly_cpi:sa_index' AS series, date, toFloat64(sa_index) AS value,
           as_of, toUInt8(partial) AS partial, vintage, computed_at, published_at
    FROM weekly_cpi_sa_vintage FINAL
    UNION ALL
    SELECT 'weekly_cpi_sa', 'weekly_cpi:sa_4w', date, toFloat64(sa_4w), as_of, toUInt8(partial), vintage, computed_at, published_at
    FROM weekly_cpi_sa_vintage FINAL
    UNION ALL
    SELECT 'weekly_cpi_sa', 'weekly_cpi:saar', date, toFloat64(saar), as_of, toUInt8(partial), vintage, computed_at, published_at
    FROM weekly_cpi_sa_vintage FINAL
) AS s
WHERE s.published_at < {vintage_as_of:Date}
GROUP BY s.source, s.series, s.date`,
	Comment: "Срез point-in-time недельного SA-агрегата: что было известно строго до даты as_of. " +
		"Форма вызова — v_weekly_cpi_sa_asof(vintage_as_of='YYYY-MM-DD'). Граница строгая: " +
		"published_at < vintage_as_of (published_at — когда конвейер впервые увидел версии обоих " +
		"источников). Для каждого (source, series, date) берётся последняя версия по тай-брейку " +
		"(published_at, vintage, computed_at); винтажи без известной даты публикации в срез не " +
		"входят. Пустой срез — 0 строк, а не ошибка. Смысл колонок — в v_weekly_cpi_sa_vintage; " +
		"полная история — там же, последний винтаж — v_weekly_cpi_sa",
	Columns: map[string]string{},
}

// weeklySAQuarterlyView — квартальное среднее месячного SAAR недельного агрегата (спека
// 2026-11-08-cpi-quarterly-metrics §4.3): «квартал к дате» нашего трекера, а не число ЦБ. Ряд
// индикативный и построен на одном агрегате с 4-недельными окнами, поэтому квартальные значения
// расходятся с официальным SAAR ЦБ систематически (3к26 на 2026-11-08: 7,34 против 8,02; 1к26:
// 11,00 против 8,70; SA ревизуется перефитом X-13) — это методика, а не ошибка, и подгонять её
// нельзя. Витрина читает последний винтаж. max(as_of)/max(partial) в подзапросе,
// потому что alias, совпадающий с именем колонки, ClickHouse подставляет внутрь агрегата
// (max(max(partial))): ILLEGAL_AGGREGATION. full_months — сколько месяцев квартала закрыто:
// 4к26 на 2026-11-08: months = 2, full_months = 1, partial = 1, as_of = 2026-11-02; когда ноябрь
// закроется, full_months станет 3, а partial — 0. quarter_start — первый
// день отчётного квартала, читаемая метка — quarter («4к26»).
var weeklySAQuarterlyView = util.View{
	Name:   "v_weekly_cpi_quarterly",
	Tables: []string{weeklySATable},
	Select: `SELECT
    quarter_start,
    quarter,
    months,
    full_months,
    saar_q,
    max_as_of AS as_of,
    max_partial AS partial
FROM (
    SELECT
        toStartOfQuarter(date) AS quarter_start,
        concat(toString(toQuarter(quarter_start)), 'к', right(toString(toYear(quarter_start)), 2)) AS quarter,
        count() AS months,
        countIf(partial = 0) AS full_months,
        avg(saar) AS saar_q,
        max(as_of) AS max_as_of,
        max(partial) AS max_partial
    FROM weekly_cpi_sa FINAL
    WHERE ` + weeklySALatestVintage + `
    GROUP BY quarter_start
)`,
	Comment: "Квартальное среднее месячного SAAR недельного SA-агрегата ИПЦ (последний винтаж): «квартал к дате» " +
		"нашего трекера, а не число ЦБ (3к26 на 2026-11-08 — 7,34 % против 8,02 % у ДИП; 1к26 — 11,00 против " +
		"8,70; SA ревизуется перефитом X-13). Расхождение систематическое: сглаживается один агрегат на 4-недельном окне, а не " +
		"компоненты ИПЦ, поэтому квартальные значения недельной модели в сравнение с официальным " +
		"SAAR не идут и на панель «Квартальный SAAR: текущий рост и базовая» выводятся только как " +
		"точка «к дате» для незакрытого квартала. months — сколько месяцев квартала есть в ряду; " +
		"full_months — сколько из них закрыто (partial = 0); partial = 1 — в квартале есть nowcast-" +
		"месяц, as_of — последняя использованная неделя. 4к26 на 2026-11-08: months = 2, full_months = 1, " +
		"partial = 1, as_of = 2026-11-02, saar_q = 6,70 — октябрь полный + ноябрь nowcast, не квартал и не " +
		"оценка ЦБ. quarter_start — первый день отчётного квартала (2026-10-01 = IV кв. 2026), " +
		"читаемая метка — quarter («4к26»); это не каноническая привязка каталога «1-е число " +
		"следующего квартала». Перед использованием читать v_series_catalog по " +
		"(source='weekly_cpi_sa', series='weekly_cpi:saar_q'). Историю винтажей см. в v_weekly_cpi_sa_vintage",
	Columns: map[string]string{
		"quarter_start": "Первый день отчётного квартала (Date): 2026-10-01 = IV кв. 2026. Сопоставлять по quarter_start; это НЕ привязка «1-е число следующего квартала»",
		"quarter":       "Метка квартала как в публикации («4к26»): читаемая подпись к quarter_start",
		"months":        "Число месяцев квартала в ряду: 3 — закрытый квартал, меньше 3 — квартал к дате",
		"full_months":   "Сколько месяцев квартала закрыто (partial = 0): у 4к26 на 2026-11-08 — 1: октябрь полный, ноябрь nowcast",
		"saar_q":        "Квартальный SAAR недельного агрегата, % годовых: среднее месячных saar. Индикативный ряд, с официальным SAAR ЦБ не сравнивать",
		"as_of":         "Последняя дата 7-дневной сетки, использованная для месяцев квартала; у незакрытого квартала — as_of nowcast-месяца",
		"partial":       "1 — в квартале есть неполный (nowcast) месяц, 0 — все месяцы квартала закрыты",
	},
}

// weeklySARolling3mView — 3-месячный скользящий SAAR недельного агрегата (спека
// 2026-11-08-cpi-quarterly-metrics §4.4): цепочка трёх 4-недельных SA-темпов, годовой пересчёт.
// Три месячные точки — это 12 недель, поэтому степень 52/12, а не 13: в отличие от месячного
// saar (13 четырёхнедельных окон в году), здесь окно ровно вчетверо длиннее 4-недельного темпа.
// pow(exp(...)) вместо ^: в ClickHouse ^ — XOR. Окно неполное у первых двух точек ряда — там NULL
// (разрыв на графике), а не ноль; последняя точка может быть nowcast-месяцем (partial = 1),
// и панель рисует её на дате as_of, иначе метка конца месяца лежит в будущем относительно to=now.
// Витрина читает последний винтаж.
var weeklySARolling3mView = util.View{
	Name:   "v_weekly_cpi_rolling3m",
	Tables: []string{weeklySATable},
	Select: `SELECT date, as_of, partial, saar_3m
FROM (
    SELECT
        date,
        as_of,
        partial,
        if(count() OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) = 3,
           (pow(exp(sum(log(1 + sa_4w / 100)) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)), 52.0 / 12.0) - 1) * 100,
           NULL) AS saar_3m
    FROM weekly_cpi_sa FINAL
    WHERE ` + weeklySALatestVintage + `
)
ORDER BY date`,
	Comment: "3-месячный скользящий SAAR недельного SA-агрегата ИПЦ (последний винтаж): цепочка трёх месячных " +
		"4-недельных SA-темпов (12 недель) с годовым пересчётом pow(Π(1 + sa_4w/100), 52/12) − 1. " +
		"Окно скользящее: каждая точка — три последних месяца, поэтому ряд показывает разворот " +
		"раньше квартального среднего, но это не официальный SAAR ЦБ и не замена ему (сглаживается " +
		"один агрегат на 4-недельном окне). Первые две точки месяца — NULL: окно неполное, это " +
		"разрыв, а не ноль. Последняя точка может быть nowcast-месяцем (partial = 1, as_of — " +
		"последняя неделя): октябрьский всплеск 8,55 % на 2026-11-08 — тарифная неделя 05.10, а не разворот " +
		"тренда. Перед использованием читать v_series_catalog по (source='weekly_cpi_sa', " +
		"series='weekly_cpi:saar_3m'). Историю винтажей см. в v_weekly_cpi_sa_vintage",
	Columns: map[string]string{
		"date":    "Последний день месяца окна (Date): три строки подряд = 12 недель",
		"as_of":   "Последняя дата 7-дневной сетки, использованная для месяца: у nowcast-месяца — последняя доступная неделя",
		"partial": "1 — последний месяц окна неполный (nowcast), 0 — полный",
		"saar_3m": "3-месячный SAAR, % годовых: pow(Π(1 + sa_4w/100), 52/12) − 1 по трём последним месяцам. NULL — окно неполное (первые две точки ряда), а не ноль",
	},
}
