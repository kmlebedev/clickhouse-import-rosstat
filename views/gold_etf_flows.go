package views

import (
	"strings"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// wgcEtfRegions — региональная разбивка WGC Gold ETF Flows. world — мировой итог,
// north_america/europe/asia/other — регионы разбивки WGC (Дальний Восток и прочие
// рынки собраны в other). Порядок фиксирован: из него собираются и имена рядов, и
// mapping-массив витрины, и порядок описаний в каталоге-сиде.
var wgcEtfRegions = []string{"world", "north_america", "europe", "asia", "other"}

// wgcEtfMetrics — метрики набора: два потока (со знаком направления) и два стока
// (запасы и AUM на конец месяца). Единицы — в каталоге-сиде, не в имени ряда.
var wgcEtfMetrics = []string{"flow_usd_mln", "flow_tonnes", "holdings_tonnes", "aum_usd_bn"}

// wgcEtfSeriesName — код ряда в macro_series: etf_<метрика>_<регион>
// (например etf_flow_usd_mln_world). Имена не выводятся из данных при чтении:
// и IN-фильтр витрины, и каталог-сид сверяются с этой функцией.
func wgcEtfSeriesName(metric, region string) string {
	return "etf_" + metric + "_" + region
}

// wgcEtfSeriesNames — все 20 кодов рядов набора (4 метрики × 5 регионов) в порядке
// регион → метрика: так строки каталога-сида читаются по региональным блокам.
func wgcEtfSeriesNames() []string {
	names := make([]string, 0, len(wgcEtfRegions)*len(wgcEtfMetrics))

	for _, region := range wgcEtfRegions {
		for _, metric := range wgcEtfMetrics {
			names = append(names, wgcEtfSeriesName(metric, region))
		}
	}

	return names
}

// wgcEtfSeriesNamesSQL — те же 20 кодов строкой для IN (...) в SELECT витрины.
func wgcEtfSeriesNamesSQL() string {
	names := wgcEtfSeriesNames()
	quoted := make([]string, len(names))

	for i, name := range names {
		quoted[i] = "'" + name + "'"
	}

	return strings.Join(quoted, ", ")
}

// wgcEtfSeriesMapSQL — пары (series, region, metric) для ARRAY JOIN: из этого
// массива витрина узнаёт, какому (регион, метрика) принадлежит значение ряда.
// Без разметки пивот не смог бы разложить длинный формат macro_series по колонкам.
func wgcEtfSeriesMapSQL() string {
	rows := make([]string, 0, len(wgcEtfRegions)*len(wgcEtfMetrics))

	for _, region := range wgcEtfRegions {
		for _, metric := range wgcEtfMetrics {
			rows = append(rows, "('"+wgcEtfSeriesName(metric, region)+"', '"+region+"', '"+metric+"')")
		}
	}

	return strings.Join(rows, ", ")
}

// goldEtfFlowsView — пивот рядов WGC Gold ETF Flows: одна строка на (дата, регион)
// с четырьмя метриками-колонками. macro_series хранит длинный формат (одно значение
// на ряд), поэтому витрина сама раскладывает 20 рядов набора по (region, metric)
// через mapping-массив и агрегирует значения в колонки. Приоритет источников
// webbridge > manual — та же конвенция, что в v_gold_dashboard: агрегат по
// (date, series, source) внутри, выбор источника снаружи через maxIfOrNull
// (maxIf вернул бы 0 и подменил бы отсутствующий ручной ряд нулём).
var goldEtfFlowsView = util.View{
	Name:   "v_gold_etf_flows",
	Tables: []string{"macro_series"},
	Select: `SELECT
    v.date AS date,
    m.region AS region,
    maxIfOrNull(v.value, m.metric = 'flow_usd_mln') AS flow_usd_mln,
    maxIfOrNull(v.value, m.metric = 'flow_tonnes') AS flow_tonnes,
    maxIfOrNull(v.value, m.metric = 'holdings_tonnes') AS holdings_tonnes,
    maxIfOrNull(v.value, m.metric = 'aum_usd_bn') AS aum_usd_bn
FROM (
    SELECT x.1 AS series, x.2 AS region, x.3 AS metric
    FROM (SELECT arrayJoin([` + wgcEtfSeriesMapSQL() + `]) AS x)
) AS m
INNER JOIN (
    SELECT date, series,
        coalesce(maxIfOrNull(value, source = 'webbridge'), maxIfOrNull(value, source = 'manual')) AS value
    FROM macro_series FINAL
    WHERE source IN ('webbridge', 'manual') AND series IN (` + wgcEtfSeriesNamesSQL() + `)
    GROUP BY date, series
) AS v ON v.series = m.series
GROUP BY date, region
ORDER BY date DESC, region`,
	Comment: "Месячные потоки и запасы физически обеспеченных золотых ETF (WGC Gold ETF Flows, gold.org/goldhub): одна строка на (дата, регион). " +
		"Ряды набора etf_<метрика>_<регион> лежат в macro_series с source='webbridge' — сессионный сбор после месячного отчёта WGC (~10-е число), запись одним POST /v1/manual_series всеми 20 рядами; " +
		"приоритет источников webbridge > manual, ручной ввод остаётся фолбэком, если сбор не отработал. " +
		"Потоки (flow_usd_mln, flow_tonnes) — за месяц, со знаком направления (плюс приток, минус отток); запасы (holdings_tonnes, aum_usd_bn) — сток на конец месяца. " +
		"Мировой поток в USD (region='world', flow_usd_mln) — тот же показатель, что ручной etf_flows_month (source='manual', млрд USD — индикатор v_gold_dashboard), только в млн USD: не путать и не складывать. " +
		"NULL в колонке — метрика за этот месяц не введена (не 0: ноль в macro_series означал бы «потока не было»). Порядок строк — date DESC, region (детерминированная выдача). Описания рядов, единицы и связь с etf_flows_month — в v_series_catalog, source='webbridge'",
	Columns: map[string]string{
		"date":            "Последний день отчётного месяца (Date32) — дата точки ряда, как у etf_flows_month",
		"region":          "Регион разбивки WGC: world (мировой итог), north_america, europe, asia, other",
		"flow_usd_mln":    "Чистый поток за месяц, млн USD: знак — направление (плюс приток, минус отток); NULL — ряд за этот месяц не введён",
		"flow_tonnes":     "Чистый поток за месяц, тонны: знак — направление; NULL — ряд за этот месяц не введён",
		"holdings_tonnes": "Запасы (holdings) на конец месяца, тонны: сток, не поток, значение неотрицательно; NULL — ряд за этот месяц не введён",
		"aum_usd_bn":      "Активы под управлением (AUM) на конец месяца, млрд USD: сток, не поток, включает переоценку по цене золота; NULL — ряд за этот месяц не введён",
	},
}
