package views

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// keyRateDashboardView — сводный дашборд контура ключевой ставки: одна строка на
// сигнал в long-форме v_gold_dashboard. Витрина читает только сырые таблицы: так
// её определение не зависит от порядка создания чужих витрин, а гейт util.CreateView
// проверяет ровно те таблицы, без которых запрос неисполним (спека §5).
// Шесть контуров: spot — текущая ставка; fundamental — ИПЦ, базовая SAAR-инфляция,
// инфляционные и ценовые ожидания, деловой климат; market — вменённая ставка KEYRATE
// и её ликвидность, кривая ОФЗ, RUONIA и спред; divergence — расхождение прогноза и
// рынка; forecast — последний прогноз каждого трека cbr_rate:* и его ошибка;
// calendar — ближайшее заседание Совета директоров ЦБ.
var keyRateDashboardView = util.View{
	Name: "v_key_rate_dashboard",
	Tables: []string{
		"cbr_key_rate", "cbr_indicators_cpd", "cbr_infl_exp", "rosstat_ipc_mes",
		"ofz_curve", "cbr_ruania", "macro_series", "keyrate_futures",
		"keyrate_contract", "events_calendar", "forecast_log", "weekly_cpi_sa",
	},
	// Все ветки UNION ALL приводят value к Nullable(Float64), даты — к Nullable(Date),
	// флаги — к UInt8, текст — к String, prompt_version — к UInt16: иначе типы веток
	// разъедутся и ClickHouse отвергнет определение ещё до первого чтения.
	//
	// Особенности, проверенные на ClickHouse 26.10:
	//   - алиас обязан стоять ДО FINAL (`FROM keyrate_futures AS f FINAL`); форма
	//     `keyrate_futures FINAL f` — SYNTAX_ERROR 62;
	//   - фильтр по c.last_trade стоит во внутреннем подзапросе, а argMax — снаружи:
	//     фильтр по агрегатному алиасу в том же SELECT даёт ILLEGAL_AGGREGATION 184;
	//   - пустая cbr_key_rate даёт argMax = 0, а не NULL, поэтому ставка обёрнута в
	//     nullIf(..., 0) — иначе вменённое изменение выродилось бы в settle × 100;
	//   - последний прогноз трека — row_number() OVER (PARTITION BY metric ORDER BY
	//     forecast_date DESC) = 1 поверх forecast_log FINAL;
	//   - base_ipc_saar переводит месячный темп в годовой для ОБОИХ месяцев: flag
	//     сравнивает с 4 % годовые значения, а не месячный процент, и требует, чтобы
	//     месяцы были соседними (dateDiff('month', prev_date, as_of) = 1);
	//   - ipc_yoy — произведение 12 последних индексов rosstat_ipc_mes (100 = без
	//     изменения), а не сумма процентов: exp(sum(log(percent / 100))) − 1; в окне
	//     обязаны быть ровно 12 последовательных месяцев, иначе NULL (не «12 точек за
	//     произвольный период»).
	Select: `WITH
    kr AS (
        SELECT
            toFloat64(nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0)) AS key_rate,
            (SELECT maxOrNull(date) FROM cbr_key_rate FINAL) AS as_of
    ),
    cpd AS (
        SELECT
            maxIfOrNull(saar, rn = 1) AS last_saar,
            maxIfOrNull(saar, rn = 2) AS prev_saar,
            maxIfOrNull(date, rn = 1) AS as_of,
            maxIfOrNull(date, rn = 2) AS prev_date
        FROM (
            SELECT
                (pow(1 + toFloat64(value) / 100, 12) - 1) * 100 AS saar,
                date,
                row_number() OVER (ORDER BY date DESC) AS rn
            FROM cbr_indicators_cpd FINAL
            WHERE name = 'Базовый ИПЦ'
        )
    ),
    wsa AS (
        SELECT
            argMax(saar, tuple(partial, as_of, date)) AS saar,
            argMax(as_of, tuple(partial, as_of, date)) AS weekly_as_of
        FROM weekly_cpi_sa FINAL
    ),
    ie AS (
        SELECT
            maxIfOrNull(toFloat64(value), rn = 1) AS last_value,
            maxIfOrNull(toFloat64(value), rn = 2) AS prev_value,
            maxIfOrNull(date, rn = 1) AS as_of
        FROM (
            SELECT
                value,
                date,
                row_number() OVER (ORDER BY date DESC) AS rn
            FROM cbr_infl_exp FINAL
            WHERE name = 'ожидаемая инфляция'
        )
    ),
    ipc AS (
        SELECT
            if(count() = 12 AND uniqExact(toStartOfMonth(date)) = 12,
               (exp(sum(log(toFloat64(percent) / 100))) - 1) * 100,
               CAST(NULL AS Nullable(Float64))) AS ipc_yoy,
            maxOrNull(date) AS as_of
        FROM (
            SELECT percent, date
            FROM rosstat_ipc_mes FINAL
            WHERE name = 'Индексы потребительских цен на товары и услуги по Российской Федерации'
              AND percent > 0
              AND date >= subtractMonths(
                  toStartOfMonth((SELECT max(date) FROM rosstat_ipc_mes FINAL
                                  WHERE name = 'Индексы потребительских цен на товары и услуги по Российской Федерации')),
                  11)
        )
    ),
    ofz AS (
        SELECT
            maxIfOrNull(yield, tenor = '1y') AS ofz_1y,
            maxIfOrNull(yield, tenor = '3y') AS ofz_3y,
            maxIfOrNull(yield, tenor = '5y') AS ofz_5y,
            maxIfOrNull(yield, tenor = '10y') AS ofz_10y,
            maxIfOrNull(tenor_date, tenor = '1y') AS as_1y,
            maxIfOrNull(tenor_date, tenor = '3y') AS as_3y,
            maxIfOrNull(tenor_date, tenor = '5y') AS as_5y,
            maxIfOrNull(tenor_date, tenor = '10y') AS as_10y
        FROM (
            SELECT
                tenor,
                argMax(yield, date) AS yield,
                max(date) AS tenor_date
            FROM ofz_curve FINAL
            WHERE tenor IN ('1y', '3y', '5y', '10y')
            GROUP BY tenor
        )
    ),
    ru AS (
        SELECT
            maxOrNull(toFloat64(ruo)) AS ruonia,
            maxOrNull(toDate(date)) AS as_of
        FROM (
            SELECT ruo, date
            FROM cbr_ruania FINAL
            ORDER BY date DESC
            LIMIT 1
        )
    ),
    kf AS (
        SELECT
            if(count() = 0, CAST(NULL AS Nullable(Float64)), argMax(toFloat64(settle), date)) AS settle,
            if(count() = 0, CAST(NULL AS Nullable(Float64)), argMax(toFloat64(volume), date)) AS volume,
            if(count() = 0, CAST(NULL AS Nullable(Float64)), argMax(toFloat64(openposition), date)) AS openposition,
            maxOrNull(date) AS as_of,
            maxOrNull(last_trade) AS last_trade
        FROM (
            SELECT
                f.date AS date,
                f.settle AS settle,
                f.volume AS volume,
                f.openposition AS openposition,
                c.last_trade AS last_trade
            FROM keyrate_futures AS f FINAL
            INNER JOIN keyrate_contract AS c FINAL ON f.secid = c.secid
            WHERE c.last_trade = (
                SELECT minOrNull(last_trade)
                FROM keyrate_contract FINAL
                WHERE last_trade >= today()
            )
        )
    ),
    ct AS (
        SELECT
            minOrNull(event_date) AS next_date
        FROM events_calendar FINAL
        WHERE category = 'cbr' AND event_date >= today()
    ),
    pe AS (
        SELECT
            argMaxOrNull(toFloat64(value), date) AS value,
            maxOrNull(toDate(date)) AS as_of
        FROM macro_series FINAL
        WHERE source = 'cbr_survey' AND series = 'price_expectations_sa'
    ),
    bc AS (
        SELECT
            argMaxOrNull(toFloat64(value), date) AS value,
            maxOrNull(toDate(date)) AS as_of
        FROM macro_series FINAL
        WHERE source = 'cbr_survey' AND series = 'business_climate_sa'
    )
SELECT * FROM (
    SELECT
        'spot' AS contour,
        'key_rate' AS indicator,
        kr.key_rate AS value,
        '% годовых' AS unit,
        kr.as_of AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(kr.key_rate IS NULL OR kr.as_of IS NULL, 1, kr.as_of < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM kr
    UNION ALL
    SELECT
        'fundamental' AS contour,
        'ipc_yoy' AS indicator,
        ipc.ipc_yoy AS value,
        '% г/г' AS unit,
        ipc.as_of AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        'таргет ЦБ 4 %: значение выше — проинфляционно' AS threshold,
        toUInt8(coalesce(ipc.ipc_yoy > 4, 0)) AS flag,
        toUInt8OrDefault(if(ipc.ipc_yoy IS NULL OR ipc.as_of IS NULL, 1, ipc.as_of < today() - 45), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ipc
    UNION ALL
    SELECT
        'fundamental' AS contour,
        'base_ipc_saar' AS indicator,
        cpd.last_saar AS value,
        '% г/г (SAAR)' AS unit,
        cpd.as_of AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '> 4 % два месяца подряд (SAAR; в источнике — две недели подряд; недельного базового ИПЦ в БД нет)' AS threshold,
        toUInt8(coalesce(cpd.last_saar > 4 AND cpd.prev_saar > 4 AND dateDiff('month', cpd.prev_date, cpd.as_of) = 1, 0)) AS flag,
        toUInt8OrDefault(if(cpd.as_of IS NULL, 1, cpd.as_of < today() - 45), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM cpd
    UNION ALL
    SELECT
        'fundamental' AS contour,
        'weekly_cpi_saar' AS indicator,
        wsa.saar AS value,
        '% г/г (SAAR, weekly)' AS unit,
        wsa.weekly_as_of AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '> 4 % (SAAR, weekly nowcast; 2022+ alone не значим)' AS threshold,
        toUInt8OrDefault(wsa.saar > 4, 0) AS flag,
        toUInt8OrDefault(if(wsa.weekly_as_of IS NULL, 1, wsa.weekly_as_of < today() - 14), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM wsa
    UNION ALL
    SELECT
        'fundamental' AS contour,
        'infl_exp_expected' AS indicator,
        ie.last_value AS value,
        '%' AS unit,
        ie.as_of AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        'рост к прошлой волне — проинфляционный разворот' AS threshold,
        toUInt8(coalesce(ie.last_value > ie.prev_value, 0)) AS flag,
        toUInt8OrDefault(if(ie.as_of IS NULL, 1, ie.as_of < today() - 45), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ie
    UNION ALL
    SELECT
        'fundamental' AS contour,
        'price_expectations_sa' AS indicator,
        pe.value AS value,
        'пункты' AS unit,
        pe.as_of AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(pe.as_of IS NULL, 1, pe.as_of < today() - 45), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM pe
    UNION ALL
    SELECT
        'fundamental' AS contour,
        'business_climate_sa' AS indicator,
        bc.value AS value,
        'пункты' AS unit,
        bc.as_of AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(bc.as_of IS NULL, 1, bc.as_of < today() - 45), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM bc
    UNION ALL
    SELECT
        'market' AS contour,
        'keyrate_implied_rate' AS indicator,
        kf.settle AS value,
        '% годовых' AS unit,
        kf.as_of AS as_of,
        kf.last_trade AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(kf.settle IS NULL OR kf.as_of IS NULL, 1, kf.as_of < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM kf
    UNION ALL
    SELECT
        'market' AS contour,
        'keyrate_implied_change_bp' AS indicator,
        round((kf.settle - kr.key_rate) * 100, 1) AS value,
        'б.п.' AS unit,
        kf.as_of AS as_of,
        kf.last_trade AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(kf.settle IS NULL OR kr.key_rate IS NULL OR kf.as_of IS NULL, 1, kf.as_of < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM kf
    CROSS JOIN kr
    UNION ALL
    SELECT
        'market' AS contour,
        'keyrate_volume' AS indicator,
        kf.volume AS value,
        'контрактов' AS unit,
        kf.as_of AS as_of,
        kf.last_trade AS target_date,
        '0 — сделок не было: settle не рыночное ожидание' AS threshold,
        toUInt8(coalesce(kf.volume = 0, 0)) AS flag,
        toUInt8OrDefault(if(kf.volume IS NULL OR kf.as_of IS NULL, 1, kf.as_of < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM kf
    UNION ALL
    SELECT
        'market' AS contour,
        'keyrate_openposition' AS indicator,
        kf.openposition AS value,
        'контрактов' AS unit,
        kf.as_of AS as_of,
        kf.last_trade AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(kf.openposition IS NULL OR kf.as_of IS NULL, 1, kf.as_of < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM kf
    UNION ALL
    SELECT
        'market' AS contour,
        'ofz_1y' AS indicator,
        ofz.ofz_1y AS value,
        '% годовых' AS unit,
        ofz.as_1y AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(ofz.ofz_1y IS NULL OR ofz.as_1y IS NULL, 1, ofz.as_1y < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ofz
    UNION ALL
    SELECT
        'market' AS contour,
        'ofz_3y' AS indicator,
        ofz.ofz_3y AS value,
        '% годовых' AS unit,
        ofz.as_3y AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(ofz.ofz_3y IS NULL OR ofz.as_3y IS NULL, 1, ofz.as_3y < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ofz
    UNION ALL
    SELECT
        'market' AS contour,
        'ofz_5y' AS indicator,
        ofz.ofz_5y AS value,
        '% годовых' AS unit,
        ofz.as_5y AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(ofz.ofz_5y IS NULL OR ofz.as_5y IS NULL, 1, ofz.as_5y < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ofz
    UNION ALL
    SELECT
        'market' AS contour,
        'ofz_10y' AS indicator,
        ofz.ofz_10y AS value,
        '% годовых' AS unit,
        ofz.as_10y AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(ofz.ofz_10y IS NULL OR ofz.as_10y IS NULL, 1, ofz.as_10y < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ofz
    UNION ALL
    SELECT
        'market' AS contour,
        'ofz_slope_10y_1y_bp' AS indicator,
        round((ofz.ofz_10y - ofz.ofz_1y) * 100, 1) AS value,
        'б.п.' AS unit,
        if(ofz.as_1y IS NULL OR ofz.as_10y IS NULL, CAST(NULL AS Nullable(Date)),
           least(ofz.as_1y, ofz.as_10y)) AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '< 0 — инверсия кривой' AS threshold,
        toUInt8(coalesce(ofz.ofz_10y - ofz.ofz_1y < 0, 0)) AS flag,
        toUInt8OrDefault(if(ofz.ofz_10y IS NULL OR ofz.ofz_1y IS NULL OR ofz.as_1y IS NULL OR ofz.as_10y IS NULL OR least(ofz.as_1y, ofz.as_10y) < today() - 7, 1, 0), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ofz
    UNION ALL
    SELECT
        'market' AS contour,
        'ruonia' AS indicator,
        ru.ruonia AS value,
        '% годовых' AS unit,
        ru.as_of AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(ru.ruonia IS NULL OR ru.as_of IS NULL, 1, ru.as_of < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ru
    UNION ALL
    SELECT
        'market' AS contour,
        'ruonia_spread_bp' AS indicator,
        round((ru.ruonia - kr.key_rate) * 100, 1) AS value,
        'б.п.' AS unit,
        ru.as_of AS as_of,
        CAST(NULL AS Nullable(Date)) AS target_date,
        '—' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault(if(ru.ruonia IS NULL OR kr.key_rate IS NULL OR ru.as_of IS NULL, 1, ru.as_of < today() - 7), 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ru
    CROSS JOIN kr
    UNION ALL
    SELECT
        'divergence' AS contour,
        if(d.track = 'cbr_rate:taylor', 'taylor_vs_market_bp', 'ordered_logit_vs_market_bp') AS indicator,
        if(d.target_date = d.next_date AND d.market_date = d.next_date,
           round((d.predicted - d.market) * 100, 2), CAST(NULL AS Nullable(Float64))) AS value,
        'б.п.' AS unit,
        d.as_of AS as_of,
        d.target_date AS target_date,
        '|расхождение| ≥ 25 б.п. — контуры расходятся на шаг' AS threshold,
        toUInt8(coalesce(d.target_date = d.next_date AND d.market_date = d.next_date
                         AND abs(round((d.predicted - d.market) * 100, 2)) >= 25, 0)) AS flag,
        toUInt8OrDefault(d.predicted IS NULL OR d.market IS NULL OR d.target_date IS NULL
                         OR d.next_date IS NULL OR d.market_date IS NULL
                         OR d.target_date != d.next_date OR d.market_date != d.next_date, 1) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM (
        SELECT
            t.track AS track,
            if(fl.metric = '', CAST(NULL AS Nullable(Float64)), toFloat64(fl.predicted)) AS predicted,
            if(fl.metric = '', CAST(NULL AS Nullable(Date)), toNullable(fl.target_date)) AS target_date,
            if(fl.metric = '', CAST(NULL AS Nullable(Date)), toNullable(fl.forecast_date)) AS as_of,
            kf.settle AS market,
            kf.last_trade AS market_date,
            ct.next_date AS next_date
        FROM (
            SELECT arrayJoin(['cbr_rate:taylor', 'cbr_rate:ordered_logit']) AS track
        ) AS t
        LEFT JOIN (
            SELECT metric, predicted, target_date, forecast_date
            FROM (
                SELECT
                    metric,
                    predicted,
                    target_date,
                    forecast_date,
                    row_number() OVER (PARTITION BY metric ORDER BY forecast_date DESC) AS rn
                FROM forecast_log FINAL
                WHERE metric LIKE 'cbr_rate:%'
            )
            WHERE rn = 1
        ) AS fl ON t.track = fl.metric
        CROSS JOIN kf
        CROSS JOIN ct
    ) AS d
    UNION ALL
    SELECT
        'forecast' AS contour,
        toString(fl.metric) AS indicator,
        toNullable(toFloat64(fl.predicted)) AS value,
        '% годовых' AS unit,
        toNullable(fl.forecast_date) AS as_of,
        toNullable(fl.target_date) AS target_date,
        'ошибка в б.п. — в error_bp после заседания' AS threshold,
        toUInt8(0) AS flag,
        toUInt8OrDefault((fl.target_date < today() AND fl.actual IS NULL) OR fl.forecast_date < today() - 45, 1) AS stale,
        fl.error_bp AS error_bp,
        fl.probabilities AS probabilities,
        toString(fl.model) AS model,
        fl.data_version AS data_version,
        toUInt16(fl.prompt_version) AS prompt_version
    FROM (
        SELECT
            metric,
            forecast_date,
            target_date,
            predicted,
            actual,
            error_bp,
            probabilities,
            model,
            data_version,
            prompt_version,
            row_number() OVER (PARTITION BY metric ORDER BY forecast_date DESC) AS rn
        FROM forecast_log FINAL
        WHERE metric LIKE 'cbr_rate:%'
          AND metric NOT LIKE '%:shadow'
          AND metric NOT LIKE '%:canary'
    ) AS fl
    WHERE fl.rn = 1
    UNION ALL
    SELECT
        'calendar' AS contour,
        'next_meeting' AS indicator,
        toFloat64(dateDiff('day', today(), ct.next_date)) AS value,
        'дней' AS unit,
        toNullable(today()) AS as_of,
        ct.next_date AS target_date,
        '≤ 3 календарных дня — предзаседание; T-0 — день решения' AS threshold,
        toUInt8(coalesce(dateDiff('day', today(), ct.next_date) <= 3, 0)) AS flag,
        toUInt8(ct.next_date IS NULL) AS stale,
        CAST(NULL AS Nullable(Float64)) AS error_bp,
        '' AS probabilities,
        '' AS model,
        '' AS data_version,
        toUInt16(0) AS prompt_version
    FROM ct
) ORDER BY contour, indicator`,
	Comment: "Сводный дашборд контура ключевой ставки: одна строка на сигнал в long-форме v_gold_dashboard. " +
		"Читает 11 сырых таблиц (cbr_key_rate, cbr_indicators_cpd, cbr_infl_exp, rosstat_ipc_mes, ofz_curve, " +
		"cbr_ruania, macro_series, keyrate_futures, keyrate_contract, events_calendar, forecast_log), а не другие " +
		"витрины: определение не зависит от порядка их создания, а гейт util.CreateView проверяет ровно эти таблицы. " +
		"Шесть контуров: spot — текущая ключевая ставка; fundamental — ИПЦ, базовая SAAR-инфляция, инфляционные и " +
		"ценовые ожидания, деловой климат; market — вменённая ставка KEYRATE и её ликвидность, кривая ОФЗ, RUONIA и " +
		"спред к ставке; divergence — расхождение прогноза и рынка в б.п.; forecast — последний production-прогноз " +
		"каждого трека cbr_rate:* (staged :shadow/:canary исключены и живут в v_ml_live_accuracy); calendar — " +
		"ближайшее заседание Совета директоров ЦБ. " +
		"flag — условие threshold выполнено (не «хорошо/плохо»); stale — данных нет или ряд старше естественного цикла " +
		"(daily 7 дней, monthly 45 дней). forecast_log агенту не выдан: витрина читает его как SQL SECURITY DEFINER. " +
		"KEYRATE низколиквиден (volume = 0 — settle не рыночное ожидание), ofz_curve — накопительный снимок, поэтому " +
		"stale = 1 у ОФЗ может означать разрыв импорта, а не отсутствие кривой. Создаётся импортёром key_rate_views " +
		"после первого ingest (forecast_log) и импортёров контура; запуск — make import STAT=key_rate_views, повтор идемпотентен",
	Columns: map[string]string{
		"contour":        "Контур сигнала: spot — текущая ключевая ставка; fundamental — макро-контур (ИПЦ, базовая SAAR-инфляция, инфляционные и ценовые ожидания, деловой климат); market — рыночный контур (вменённая ставка KEYRATE, кривая ОФЗ, RUONIA); divergence — расхождение прогноза и рынка; forecast — журнал прогнозов cbr_rate:*; calendar — ближайшее заседание Совета директоров ЦБ",
		"indicator":      "Код сигнала внутри контура; у forecast — metric production-трека из forecast_log (cbr_rate:taylor, cbr_rate:ordered_logit, cbr_rate:llm, будущий cbr_rate:ml); staged-метрики :shadow/:canary в forecast-контур не входят — они в v_ml_live_accuracy; у divergence — taylor_vs_market_bp или ordered_logit_vs_market_bp, у calendar — next_meeting",
		"value":          "Значение сигнала в unit; у next_meeting — сколько календарных дней осталось до заседания; NULL — данных нет (пустая таблица-источник, отсутствующий тенор ОФЗ, нет прогноза или рынка); у divergence — NULL, если target_date прогноза или дата экспирации серии не совпадают с ближайшим заседанием: число против чужой серии не показывается",
		"unit":           "Единица значения: % годовых (ставка, вменённая ставка, ОФЗ, RUONIA, прогноз), % г/г (ИПЦ), % г/г (SAAR) (базовая инфляция), % (инфляционные ожидания), пункты (мониторинг предприятий), б.п. (изменения и расхождения), контрактов (объём и открытые позиции KEYRATE), дней (до заседания)",
		"as_of":          "Дата значения: последнее наблюдение, торговый день или дата прогноза; у next_meeting — today(), потому что значение пересчитывается в момент запроса; NULL — данных нет",
		"target_date":    "Дата заседания Совета директоров ЦБ: для market — экспирация ближайшей будущей серии KEYRATE, для forecast — target_date прогноза, для divergence — target_date прогноза, для calendar — ближайшее будущее заседание; NULL у spot и fundamental",
		"threshold":      "Текст порога или условия; — (длинное тире) — числового порога нет. flag читать вместе с threshold: он значит «условие выполнено», а не «хорошо»",
		"flag":           "1 — условие threshold выполнено; 0 — не выполнено или порога нет: ipc_yoy > 4 %; base_ipc_saar > 4 % два месяца подряд (SAAR); infl_exp_expected выросла к прошлой волне; keyrate_volume = 0 (сделок не было); ofz_slope_10y_1y_bp < 0 (инверсия кривой); |divergence| ≥ 25 б.п. (только когда и target_date прогноза, и дата экспирации серии совпадают с ближайшим заседанием); next_meeting ≤ 3 дней",
		"stale":          "1 — значения нет или оно устарело по естественному циклу: daily-ряды (ставка, KEYRATE, ОФЗ, RUONIA) — старше 7 дней; monthly-ряды (ИПЦ, базовая инфляция, ожидания, мониторинг) — старше 45 дней; forecast — target_date в прошлом и actual IS NULL либо forecast_date старше 45 дней; calendar — нет будущего заседания; divergence — нет прогноза или рынка либо target_date прогноза не совпадает с ближайшим заседанием. У ofz_curve stale = 1 может означать разрыв импорта, а не отсутствие кривой",
		"error_bp":       "Ошибка прогноза в базисных пунктах из forecast_log.error_bp = (predicted − actual) × 100; NULL — прогноз ещё не свёрён с фактом или строка не forecast-контура",
		"probabilities":  "JSON распределения cut/hold/hike из forecast_log.probabilities; пустая строка — не forecast-строка",
		"model":          "Модель или трек прогноза из forecast_log.model (taylor, ordered_logit, llm, ...); пустая строка — не forecast-строка",
		"data_version":   "Версия набора данных прогноза из forecast_log.data_version; пустая строка — не forecast-строка",
		"prompt_version": "Редакция промпта LLM-трека из forecast_log.prompt_version; 0 — не forecast-строка",
	},
}

// keyRateViewsList — витрины импортёра key_rate_views в том порядке, в каком он
// их создаёт. Отдельная переменная уровня пакета, а не литерал на месте цикла:
// список нужен в двух местах — сам цикл создания и тест регистрации, который
// обязан видеть ИМЕННО то, что создаёт импортёр (прецедент — goldViewsList).
// Сейчас витрина одна; будущие срезы контура ставки (публикационный, PIT)
// добавятся сюда же.
var keyRateViewsList = []util.View{keyRateDashboardView, mlLiveAccuracyView}

type keyRateViews struct {
}

func (s *keyRateViews) Name() string {
	return "key_rate_views"
}

func (s *keyRateViews) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, v := range keyRateViewsList {
		var created bool
		if created, err = util.CreateView(ctx, conn, v); err != nil {
			return count, err
		}
		log.Infof("View %s created: %t", v.Name, created)
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &keyRateViews{})
}
