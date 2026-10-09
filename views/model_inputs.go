package views

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// modelInputsView — витрина входов DCF-модели: одна строка с последними значениями
// всех рядов (argMax по дате). Каждый блок — отдельный однострочный подзапрос:
// подзапрос без GROUP BY всегда отдаёт ровно одну строку, даже по пустой таблице,
// а toNullable даёт NULL вместо нулевого значения по умолчанию. Поэтому при пустых
// таблицах (model_runs, например, до первого прогона) витрина всё равно возвращает
// одну строку с NULL.
var modelInputsView = util.View{
	Name: "v_model_inputs",
	Tables: []string{
		"gold_prices", "cbr_currency_usd", "cbr_key_rate", "ofz_curve", "ipc_mes",
		"ipc_weeks", "macro_series", "stock_prices", "model_runs",
	},
	Select: `SELECT
    gold_moex_fix_usd, gold_date,
    usdrub, usdrub_date,
    cbr_key_rate, cbr_key_rate_date,
    ofz_1y, ofz_1y_date, ofz_3y, ofz_3y_date, ofz_5y, ofz_5y_date, ofz_10y, ofz_10y_date,
    rgbi, rgbi_date,
    ipc_mes_last, ipc_mes_last_date,
    ipc_week_ytd, ipc_week_ytd_date,
    fedfunds, fedfunds_date, dfii10, dfii10_date, dgs10, dgs10_date,
    t5yie, t5yie_date, dtwexbgs, dtwexbgs_date,
    plzl_close, plzl_date,
    last_run_id, last_nav_per_share, last_run_date
FROM (
    SELECT
        argMax(toNullable(usd), date) AS gold_moex_fix_usd,
        argMax(toNullable(date), date) AS gold_date
    FROM gold_prices FINAL
    WHERE venue = 'moex_fix_usd'
) AS gold
CROSS JOIN (
    SELECT
        argMax(toNullable(price), date) AS usdrub,
        argMax(toNullable(date), date) AS usdrub_date
    FROM cbr_currency_usd FINAL
) AS fx
CROSS JOIN (
    SELECT
        argMax(toNullable(rate), date) AS cbr_key_rate,
        argMax(toNullable(date), date) AS cbr_key_rate_date
    FROM cbr_key_rate FINAL
) AS key_rate
CROSS JOIN (
    SELECT
        argMaxIf(toNullable(yield), date, tenor = '1y') AS ofz_1y,
        argMaxIf(toNullable(date), date, tenor = '1y') AS ofz_1y_date,
        argMaxIf(toNullable(yield), date, tenor = '3y') AS ofz_3y,
        argMaxIf(toNullable(date), date, tenor = '3y') AS ofz_3y_date,
        argMaxIf(toNullable(yield), date, tenor = '5y') AS ofz_5y,
        argMaxIf(toNullable(date), date, tenor = '5y') AS ofz_5y_date,
        argMaxIf(toNullable(yield), date, tenor = '10y') AS ofz_10y,
        argMaxIf(toNullable(date), date, tenor = '10y') AS ofz_10y_date,
        argMaxIf(toNullable(yield), date, tenor = 'RGBI') AS rgbi,
        argMaxIf(toNullable(date), date, tenor = 'RGBI') AS rgbi_date
    FROM ofz_curve FINAL
) AS ofz
CROSS JOIN (
    SELECT
        argMaxIf(toNullable(percent), date, startsWith(name, 'Индексы потребительских цен на товары и услуги')) AS ipc_mes_last,
        argMaxIf(toNullable(date), date, startsWith(name, 'Индексы потребительских цен на товары и услуги')) AS ipc_mes_last_date
    FROM ipc_mes FINAL
) AS ipc_m
CROSS JOIN (
    SELECT
        if(count() > 0, avg(exp(log_ytd) - 1) * 100, NULL) AS ipc_week_ytd,
        if(count() > 0, max(last_date), NULL) AS ipc_week_ytd_date
    FROM (
        SELECT
            name,
            sum(log(toFloat64(percent) / 100)) AS log_ytd,
            max(date) AS last_date
        FROM ipc_weeks FINAL
        WHERE percent > 0
          AND toYear(date) = (SELECT toYear(max(date)) FROM ipc_weeks FINAL)
        GROUP BY name
    )
) AS ipc_w
CROSS JOIN (
    SELECT
        argMaxIf(toNullable(value), date, series = 'FEDFUNDS') AS fedfunds,
        argMaxIf(toNullable(date), date, series = 'FEDFUNDS') AS fedfunds_date,
        argMaxIf(toNullable(value), date, series = 'DFII10') AS dfii10,
        argMaxIf(toNullable(date), date, series = 'DFII10') AS dfii10_date,
        argMaxIf(toNullable(value), date, series = 'DGS10') AS dgs10,
        argMaxIf(toNullable(date), date, series = 'DGS10') AS dgs10_date,
        argMaxIf(toNullable(value), date, series = 'T5YIE') AS t5yie,
        argMaxIf(toNullable(date), date, series = 'T5YIE') AS t5yie_date,
        argMaxIf(toNullable(value), date, series = 'DTWEXBGS') AS dtwexbgs,
        argMaxIf(toNullable(date), date, series = 'DTWEXBGS') AS dtwexbgs_date
    FROM macro_series FINAL
    WHERE source = 'fred'
) AS fred
CROSS JOIN (
    SELECT
        argMax(toNullable(close), date) AS plzl_close,
        argMax(toNullable(date), date) AS plzl_date
    FROM stock_prices FINAL
    WHERE code = 'PLZL'
) AS plzl
CROSS JOIN (
    SELECT
        argMax(toNullable(run_id), run_date) AS last_run_id,
        argMax(toNullable(nav_per_share), run_date) AS last_nav_per_share,
        argMax(toNullable(run_date), run_date) AS last_run_date
    FROM model_runs FINAL
) AS model`,
	Comment: "Входы DCF-модели прогноза золота и NAV PLZL: одна строка с последними значениями рядов (argMax по дате). У каждого ряда есть дата актуальности *_date — проверять перед использованием; NULL — ряд пуст или не импортирован",
	Columns: map[string]string{
		"gold_moex_fix_usd":  "Цена золота, USD за унцию: фиксинг MOEX GOLDFIXME (руб./г) / курс ЦБ. Производная цена, не LBMA; сверка с GC=F в сессии",
		"gold_date":          "Дата последнего наблюдения gold_moex_fix_usd (дата актуальности)",
		"usdrub":             "Официальный курс ЦБ РФ, руб. за 1 USD (Float32)",
		"usdrub_date":        "Дата последнего курса usdrub (дата актуальности)",
		"cbr_key_rate":       "Ключевая ставка ЦБ РФ, % годовых (Float32)",
		"cbr_key_rate_date":  "Дата последнего значения ключевой ставки (дата актуальности)",
		"ofz_1y":             "Доходность ОФЗ, тенор 1 год, % годовых (G-curve МосБиржи)",
		"ofz_1y_date":        "Дата последнего значения ofz_1y (дата актуальности)",
		"ofz_3y":             "Доходность ОФЗ, тенор 3 года, % годовых (G-curve МосБиржи)",
		"ofz_3y_date":        "Дата последнего значения ofz_3y (дата актуальности)",
		"ofz_5y":             "Доходность ОФЗ, тенор 5 лет, % годовых (G-curve МосБиржи)",
		"ofz_5y_date":        "Дата последнего значения ofz_5y (дата актуальности)",
		"ofz_10y":            "Доходность ОФЗ, тенор 10 лет, % годовых (G-curve МосБиржи)",
		"ofz_10y_date":       "Дата последнего значения ofz_10y (дата актуальности)",
		"rgbi":               "Уровень индекса RGBI (пункты). Это не доходность: индекс полной доходности ОФЗ",
		"rgbi_date":          "Дата последнего значения rgbi (дата актуальности)",
		"ipc_mes_last":       "ИПЦ РФ на товары и услуги за последний месяц, индекс: 100 = без изменения, 103,5 = рост на 3,5% к концу предыдущего месяца (Float32)",
		"ipc_mes_last_date":  "Дата последнего значения ipc_mes_last (дата актуальности)",
		"ipc_week_ytd":       "Инфляция РФ с начала года по еженедельным ценам Росстата, %. Невзвешенное среднее YTD-изменений по товарам недельной выборки за последний год данных; ориентир, не официальный ИПЦ",
		"ipc_week_ytd_date":  "Дата последнего недельного наблюдения, вошедшего в ipc_week_ytd (дата актуальности)",
		"fedfunds":           "Effective Fed Funds Rate, % годовых (FRED FEDFUNDS)",
		"fedfunds_date":      "Дата последнего значения fedfunds (дата актуальности)",
		"dfii10":             "Реальная доходность 10-летних TIPS, % (FRED DFII10)",
		"dfii10_date":        "Дата последнего значения dfii10 (дата актуальности)",
		"dgs10":              "Номинальная доходность 10-летних казначейских облигаций США, % (FRED DGS10)",
		"dgs10_date":         "Дата последнего значения dgs10 (дата актуальности)",
		"t5yie":              "Breakeven-инфляция на 5 лет, % (FRED T5YIE)",
		"t5yie_date":         "Дата последнего значения t5yie (дата актуальности)",
		"dtwexbgs":           "Индекс широкого доллара США, индекс (FRED DTWEXBGS)",
		"dtwexbgs_date":      "Дата последнего значения dtwexbgs (дата актуальности)",
		"plzl_close":         "Цена закрытия акции Полюса (PLZL), руб. за акцию (MOEX)",
		"plzl_date":          "Дата последней цены закрытия plzl_close (дата актуальности)",
		"last_run_id":        "UUID последнего прогона модели из model_runs; NULL — прогонов ещё не было",
		"last_nav_per_share": "NAV на акцию из последнего прогона модели, руб.; NULL — прогонов ещё не было",
		"last_run_date":      "Дата и время последнего прогона модели; NULL — прогонов ещё не было",
	},
}
