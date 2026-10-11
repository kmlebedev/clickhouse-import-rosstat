package views

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// goldDashboardIndicators — фиксированный список индикаторов дашборда верификации (статья §6).
// Список зашит в витрине, а не берётся из macro_series: индикатор, который ни разу
// не вводили, всё равно попадает в выдачу с value = NULL и stale = 1.
const goldDashboardIndicators = `'crack_ulsd_proxy', 'distillate_stocks', 'fedwatch_dec_hike', 'etf_flows_month', 'dxy'`

// goldDashboardView — дашборд верификации сценария золота: одна строка на индикатор.
// Значения живут в macro_series: 'eia' — автоматический импорт EIA API v2 (distillate_stocks,
// refinery_utilization), 'webbridge' — WebBridge-сбор (фаза 5), 'manual' — ручной ввод через ingest
// (spec §5.2). Приоритет источников eia > webbridge > manual: автоматический ряд канонический,
// а ручной ввод нужен фолбэком, если импортёр EIA временно не отработал (для distillate_stocks).
// stale = 1, если значения нет ни в одном источнике или последнее наблюдение старше 14 дней.
var goldDashboardView = util.View{
	Name:   "v_gold_dashboard",
	Tables: []string{"macro_series"},
	Select: `SELECT
    indicator,
    value,
    threshold,
    toUInt8(coalesce(multiIf(
        indicator = 'crack_ulsd_proxy', value > 50,
        indicator = 'fedwatch_dec_hike', value > 65,
        indicator = 'dxy', value > 102,
        0), 0)) AS flag,
    as_of,
    toUInt8(if(as_of IS NOT NULL AND as_of >= today() - 14, 0, 1)) AS stale
FROM (
    SELECT
        ind.indicator AS indicator,
        if(v.n > 0, v.value, NULL) AS value,
        if(v.n > 0, v.as_of, NULL) AS as_of,
        multiIf(
            ind.indicator = 'crack_ulsd_proxy', '>50 USD: медвежий сигнал (crack ULSD)',
            ind.indicator = 'fedwatch_dec_hike', '>65%: сценарное переключение; >80%: алерт',
            ind.indicator = 'dxy', '>102: порог модели (DXY)',
            ind.indicator = 'distillate_stocks', 'порога в статье §6 нет',
            ind.indicator = 'etf_flows_month', 'порога в статье §6 нет',
            '-') AS threshold
    FROM (
        SELECT arrayJoin([` + goldDashboardIndicators + `]) AS indicator
    ) AS ind
    LEFT JOIN (
        SELECT
            series AS series_name,
            coalesce(
                maxIfOrNull(n, source = 'eia'),
                maxIfOrNull(n, source = 'webbridge'),
                maxIfOrNull(n, source = 'manual'),
                toUInt64(0)
            ) AS n,
            coalesce(
                maxIfOrNull(value, source = 'eia'),
                maxIfOrNull(value, source = 'webbridge'),
                maxIfOrNull(value, source = 'manual')
            ) AS value,
            coalesce(
                maxIfOrNull(as_of, source = 'eia'),
                maxIfOrNull(as_of, source = 'webbridge'),
                maxIfOrNull(as_of, source = 'manual')
            ) AS as_of
        FROM (
            SELECT
                series,
                source,
                count() AS n,
                argMax(value, date) AS value,
                argMax(date, date) AS as_of
            FROM macro_series FINAL
            WHERE source IN ('eia', 'webbridge', 'manual')
              AND series IN (` + goldDashboardIndicators + `)
            GROUP BY series, source
        )
        GROUP BY series
    ) AS v ON ind.indicator = v.series_name
)`,
	Comment: "Дашборд верификации сценария золота (статья §6): одна строка на индикатор. Значения берутся из macro_series с приоритетом источников eia > webbridge > manual: автоматический импорт EIA API v2 канонический (distillate_stocks — запасы дистиллятов, тыс. барр.), ручной ввод — фолбэк. flag = 1 — пробит порог; stale = 1 — значения нет ни в одном источнике или последнее наблюдение старше 14 дней. Пороги: crack ULSD >50 USD (медвежий сигнал), FedWatch на декабрьское повышение: flag = 1 при >65% (сценарное переключение), >80% — уровень алерта (текст порога); DXY >102. Значения FedWatch — в процентах (65 = 65%)",
	Columns: map[string]string{
		"indicator": "Код индикатора: crack_ulsd_proxy, distillate_stocks, fedwatch_dec_hike, etf_flows_month, dxy",
		"value":     "Последнее значение индикатора из приоритетного источника (eia > webbridge > manual): для distillate_stocks — автоматический ряд EIA (тыс. барр.) с фолбэком на ручной ввод; NULL — значения нет ни в одном источнике",
		"threshold": "Пороги из статьи §6 текстом; для индикаторов без порога — 'порога в статье §6 нет'",
		"flag":      "1 — последнее значение пересекло порог из статьи §6 (crack >50, FedWatch >65 — сценарное переключение, DXY >102); 0 — не пересекло или порога нет",
		"as_of":     "Дата последнего наблюдения (Date32) в приоритетном источнике (eia > webbridge > manual); NULL — значения нет",
		"stale":     "1 — значения нет ни в одном источнике или последнее наблюдение старше 14 дней; 0 — свежее",
	},
}
