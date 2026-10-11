package views

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// forecastAccuracyView — витрина точности прогнозов: forecast_log FINAL с производными
// колонками абсолютной ошибки и признаком сверки с фактом. Строки создаёт ingest
// (metric 'nav' и 'xau_q_avg' при каждом model_run); actual и error_pct заполняет
// импортёр forecast_resolve (пакет forecast/, расписание dagu), пока их нет — NULL,
// is_resolved = 0.
var forecastAccuracyView = util.View{
	Name:   "v_forecast_accuracy",
	Tables: []string{"forecast_log"},
	Select: `SELECT
    metric,
    forecast_date,
    target_date,
    predicted,
    actual,
    error_pct,
    abs(error_pct) AS abs_error_pct,
    toUInt8(actual IS NOT NULL) AS is_resolved
FROM forecast_log FINAL`,
	Comment: "Точность прогнозов из forecast_log: одна строка на прогноз (metric, дата прогноза, дата цели). error_pct = (predicted − actual) / actual × 100; " +
		"actual и error_pct заполняет импортёр forecast_resolve — окно сверки — квартал, содержащий target_date, факт — среднее gold_prices за окно по venue comex_front (фолбэк moex_fix_usd). " +
		"Строка остаётся нерезолвленной, пока target_date в будущем или в окне нет точек цены. metric 'nav' не резолвится вовсе: факта (NAV на дату) нет, actual остаётся NULL",
	Columns: map[string]string{
		"metric":        "Код прогноза: 'nav' — NAV на акцию, руб. (не резолвится: факта нет); 'xau_q_avg' — вероятностно-взвешенная цена золота на горизонт, USD/oz",
		"forecast_date": "Дата, на которую сделан прогноз (день записи run'а)",
		"target_date":   "Дата цели прогноза (конец горизонта сценария: квартал, год или дата); окно сверки факта — квартал, содержащий target_date",
		"predicted":     "Прогнозное значение метрики",
		"actual":        "Фактическое значение метрики на target_date, его считает forecast_resolve: среднее gold_prices за квартал target_date, venue comex_front → moex_fix_usd; NULL — факт ещё не записан (target_date в будущем, нет точек цены в окне или metric 'nav' — факта нет)",
		"error_pct":     "Ошибка прогноза, %: (predicted − actual) / actual × 100, считается forecast_resolve; NULL — факт не записан",
		"abs_error_pct": "Абсолютная ошибка прогноза, %; NULL — факт не записан",
		"is_resolved":   "1 — факт записан (actual не NULL), строку сверяет forecast_resolve; 0 — прогноз ещё не сверен",
	},
}
