package views

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// mlLiveAccuracyView — live-точность прогнозов ключевой ставки (infra/4.3):
// одна строка на прогноз из forecast_log FINAL с разбором трека и стадии.
//
// Стадия — суффикс metric: `cbr_rate:<track>` — production, `:shadow` — 2
// заседания перед canary, `:canary` — 1 заседание перед production. Базовая
// метрика `cbr_rate` без трека в витрину не входит: у неё нет трека, а live-
// оценка всегда привязана к треку. error_bp заполняет forecast_resolve;
// error_pct для ставки не используется (несопоставим между режимами).
var mlLiveAccuracyView = util.View{
	Name:   "v_ml_live_accuracy",
	Tables: []string{"forecast_log"},
	Select: `SELECT
    metric,
    splitByChar(':', metric)[2] AS track,
    multiIf(
        endsWith(metric, ':shadow'), 'shadow',
        endsWith(metric, ':canary'), 'canary',
        'production'
    ) AS stage,
    model,
    forecast_date,
    target_date,
    predicted,
    actual,
    error_bp,
    abs(error_bp) AS abs_error_bp,
    toUInt8(actual IS NOT NULL) AS is_resolved,
    dateDiff('day', target_date, today()) AS days_since_target,
    data_version,
    prompt_version
FROM forecast_log FINAL
WHERE metric LIKE 'cbr_rate:%'`,
	Comment: "Live-точность прогнозов ключевой ставки (infra/4.3): одна строка на прогноз из forecast_log FINAL. " +
		"track — второй сегмент metric (taylor/ordered_logit/ml/llm); stage — production (без суффикса), " +
		"shadow (2 заседания) или canary (1 заседание); staged-строки не публикуются и не входят в forecast-контур " +
		"v_key_rate_dashboard. error_bp = (predicted − actual) × 100, заполняет forecast_resolve после заседания; " +
		"error_pct для ставки не используется. is_resolved = 1, когда actual записан. Создаётся импортёром " +
		"key_rate_views после первого ingest (forecast_log): make import STAT=key_rate_views, повтор идемпотентен. " +
		"Сырой forecast_log агенту не выдан — витрина читает его как SQL SECURITY DEFINER",
	Columns: map[string]string{
		"metric":            "Полная метрика журнала: cbr_rate:<track>[:shadow|:canary]",
		"track":             "Трек прогноза (второй сегмент metric): taylor, ordered_logit, ml, llm",
		"stage":             "Стадия промоушена: production — опубликованный трек; shadow — 2 заседания перед canary; canary — 1 заседание перед production",
		"model":             "Модель или трек из forecast_log.model",
		"forecast_date":     "Дата, на которую сделан прогноз (день записи run'а)",
		"target_date":       "Дата заседания Совета директоров ЦБ",
		"predicted":         "Ожидаемая ставка после заседания, % годовых",
		"actual":            "Фактическая ставка после заседания из cbr_key_rate; NULL — факт ещё не записан",
		"error_bp":          "Ошибка прогноза в базисных пунктах: (predicted − actual) × 100; NULL — факт не записан",
		"abs_error_bp":      "Абсолютная ошибка прогноза в б.п.; NULL — факт не записан",
		"is_resolved":       "1 — actual записан (прогноз свёрён с фактом); 0 — ожидает резолва",
		"days_since_target": "Сколько календарных дней прошло с target_date; отрицательное значение — заседание в будущем",
		"data_version":      "Версия набора данных прогноза из forecast_log.data_version",
		"prompt_version":    "Редакция промпта LLM-трека; 0 у статистических треков",
	},
}
