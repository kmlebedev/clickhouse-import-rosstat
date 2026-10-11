package views

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// goldForecastsView — консенсус аналитиков по цене золота и собственная сценарная
// сетка: одна строка на (author, published, horizon, scenario). Строки пишет
// ingest-endpoint POST /v1/gold_forecasts и сид sql/gold_forecasts_q4_2026_seed.sql.
// Числовые колонки DDL не Nullable: «поле не задано» кодируется нулём (point-прогноз
// хранит low = high = 0, банковский range — point = 0, банковские строки —
// probability = 0), витрина превращает нули в NULL, чтобы агент не читал их как цены.
var goldForecastsView = util.View{
	Name:   "v_gold_forecasts",
	Tables: []string{"gold_forecasts"},
	Select: `SELECT
    author,
    published,
    horizon,
    scenario,
    nullIf(low, 0) AS low,
    nullIf(high, 0) AS high,
    nullIf(point, 0) AS point,
    if(point > 0, point, nullIf((low + high) / 2, 0)) AS midpoint,
    nullIf(toFloat64(probability), 0) AS probability
FROM gold_forecasts FINAL
ORDER BY published DESC, author, horizon, scenario`,
	Comment: "Консенсус аналитиков по цене золота и собственная сценарная сетка (таблица gold_forecasts, сид sql/gold_forecasts_q4_2026_seed.sql, запись — POST /v1/gold_forecasts). " +
		"Одна строка на (author, published, horizon, scenario): банковские цели ('goldman','hsbc','jpm','ubs','citi') и own-сетка ('own': bull/base/bear). " +
		"Сценарий 'point' — точечная цель, 'range' — диапазон банка (low/high, point пуст), 'bull'/'base'/'bear' — вероятностные сценарии own. " +
		"Нули в колонках DDL означают «поле не задано» и здесь показаны как NULL. midpoint — единая точка сравнения: point, если он задан, иначе середина диапазона; " +
		"probability заполнена только у own-сценариев (у банков вероятности не публикуются). Порядок строк — published DESC, author, horizon, scenario. " +
		"Вероятностно-взвешенная цена = Σ(probability / 100 × midpoint) по own-сценариям Q4-2026 (25/40/35 → 4285) сверяется с xau_q_avg в v_forecast_accuracy",
	Columns: map[string]string{
		"author":      "Источник прогноза: 'goldman','hsbc','jpm','ubs','citi' — банковский консенсус; 'own' — собственная сценарная сетка",
		"published":   "Дата публикации прогноза (сид статьи 07.10.2026 — 2026-10-07 у всех строк)",
		"horizon":     "Горизонт прогноза: 'YE-2026' (конец 2026), '2027', 'Q4-2026', '12M', 'short'",
		"scenario":    "Тип строки: 'point' — точечная цель; 'range' — диапазон банка (low/high, point не задан); 'bull'/'base'/'bear' — сценарии own-сетки с вероятностями",
		"low":         "Нижняя граница диапазона, USD/oz; NULL — диапазон не задан (у point-строк low = 0 в DDL)",
		"high":        "Верхняя граница диапазона, USD/oz; NULL — диапазон не задан",
		"point":       "Точечная цель, USD/oz; NULL — задан только диапазон (у 'range' point = 0 в DDL)",
		"midpoint":    "Единая точка сравнения, USD/oz: point, если он задан, иначе середина (low + high) / 2; NULL — не задано ни то, ни другое",
		"probability": "Вероятность сценария, % (25 = 25%); заполнена только у own-сценариев, у банковских строк NULL — вероятности не публикуются",
	},
}
