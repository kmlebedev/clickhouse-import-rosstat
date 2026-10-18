package mlregistry

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// mlViewsList — витрины импортёра ml_registry в порядке создания. Отдельная
// переменная уровня пакета, а не литерал внутри Import: тест регистрации обязан
// видеть именно тот список, который создаёт импортёр, иначе выпавшая витрина
// оставляла бы его зелёным.
var mlViewsList = []util.View{
	mlRunsView, mlLeaderboardView, mlErrorByFoldView,
}

// mlRunsView — паспорт прогона одним рядом: конфиг, git, данные, метрики,
// артефакты. FINAL схлопывает ретраи одного run_id по ключу ReplacingMergeTree.
var mlRunsView = util.View{
	Name:   "v_ml_runs",
	Tables: []string{"ml_runs"},
	Select: `SELECT
    run_id,
    experiment,
    run_name,
    status,
    author,
    git_sha,
    git_branch,
    git_dirty,
    config_hash,
    config_json,
    data_asof,
    sample_hash,
    sample_meta_json,
    spec_path,
    metrics_json,
    artifacts_json,
    started_at,
    finished_at,
    duration_sec,
    notes
FROM ml_runs FINAL`,
	Comment: "Один прогон ML-эксперимента: конфиг, git, данные, метрики, артефакты",
	Columns: map[string]string{
		"run_id":           "UUID прогона, генерируется на старте; вместе с experiment — ключ ReplacingMergeTree",
		"experiment":       "Код эксперимента: 'cpi_nowcast', 'key_rate_model'",
		"run_name":         "Человекочитаемое имя прогона",
		"status":           "'finished' — прогон дошёл до артефактов; 'empty' — данных/фолдов нет, но прогон диагностирован; 'failed' — исключение",
		"author":           "Автор прогона: git user.name / GIT_AUTHOR_NAME",
		"git_sha":          "Полный SHA коммита, на котором выполнен прогон",
		"git_branch":       "Ветка git на момент прогона",
		"git_dirty":        "1 — рабочее дерево было грязным; 0 — чистым",
		"config_hash":      "sha256 канонического JSON конфигурации",
		"config_json":      "Полный конфиг прогона: CLI-аргументы и дефолты, включая seed",
		"data_asof":        "Дата среза данных; по ней сырьё восстанавливается из ClickHouse",
		"sample_hash":      "sha256 канонического JSON выборки",
		"sample_meta_json": "Описание выборки: rows/first/last/skipped/months, без сырых строк",
		"spec_path":        "Спека/пункт, к которому относится run",
		"metrics_json":     "Агрегированные метрики; вложенный словарь, общие ключи primary_metric, models, windows",
		"artifacts_json":   "Артефакты: [{name,path,sha256,bytes}, ...], пути относительно корня репозитория",
		"started_at":       "Время старта прогона",
		"finished_at":      "Время завершения прогона",
		"duration_sec":     "Длительность прогона, секунды",
		"notes":            "Свободные заметки прогона",
	},
}

// mlLeaderboardView — лидерборд бэктестов: число фолдов, RMSE и MAE по
// (experiment, run_id, model, window, unit). unit остаётся в ключе, чтобы
// строки с разными единицами нельзя было агрегировать между собой.
var mlLeaderboardView = util.View{
	Name:   "v_ml_leaderboard",
	Tables: []string{"ml_predictions", "ml_runs"},
	Select: `SELECT
    p.experiment,
    p.run_id,
    r.run_name,
    r.data_asof,
    r.finished_at,
    p.model,
    p.window,
    p.unit,
    count() AS folds,
    sqrt(avg(p.error * p.error)) AS rmse,
    avg(abs(p.error)) AS mae
FROM (SELECT * FROM ml_predictions FINAL) AS p
LEFT JOIN (
    SELECT run_id, experiment, run_name, data_asof, finished_at
    FROM ml_runs FINAL
) AS r ON r.experiment = p.experiment AND r.run_id = p.run_id
GROUP BY p.experiment, p.run_id, r.run_name, r.data_asof, r.finished_at,
         p.model, p.window, p.unit`,
	Comment: "Лидерборд бэктестов: RMSE/MAE и число фолдов по (experiment, run_id, model, window, unit); rmse и mae — в единицах unit, строки с разным unit сравнивать нельзя",
	Columns: map[string]string{
		"experiment":  "Код эксперимента: 'cpi_nowcast', 'key_rate_model'",
		"run_id":      "UUID прогона из паспорта ml_runs",
		"run_name":    "Человекочитаемое имя прогона из паспорта ml_runs",
		"data_asof":   "Дата среза данных прогона",
		"finished_at": "Время завершения прогона",
		"model":       "Имя модели/метода/бенчмарка",
		"window":      "Окно бэктеста: 'start=24', 'start=48, embargo=1', 'expanding', 'rolling40'",
		"unit":        "'pp' — процентные пункты (инфляция), 'bp' — базисные пункты (ставка); сравнивать строки с разным unit нельзя",
		"folds":       "Число предсказаний модели в окне",
		"rmse":        "RMSE в единицах unit; сравнивать строки с разным unit нельзя",
		"mae":         "MAE в единицах unit; сравнивать строки с разным unit нельзя",
	},
}

// mlErrorByFoldView — fold-предсказания одним рядом; error хранится в таблице
// инвариантом predicted − actual и используется как есть.
var mlErrorByFoldView = util.View{
	Name:   "v_ml_error_by_fold",
	Tables: []string{"ml_predictions"},
	Select: `SELECT
    experiment,
    run_id,
    model,
    window,
    target_date,
    predicted,
    actual,
    error,
    unit,
    meta_json,
    created_at
FROM ml_predictions FINAL`,
	Comment: "Fold-предсказания бэктеста: predicted, actual и error = predicted − actual в единицах unit",
	Columns: map[string]string{
		"experiment":  "Код эксперимента: 'cpi_nowcast', 'key_rate_model'",
		"run_id":      "UUID прогона из паспорта ml_runs",
		"model":       "Имя модели/метода/бенчмарка",
		"window":      "Окно бэктеста: 'start=24', 'start=48, embargo=1', 'expanding', 'rolling40'",
		"target_date": "Дата цели: месяц (CPI) или дата заседания (key rate)",
		"predicted":   "Предсказанное значение",
		"actual":      "Фактическое значение; не Nullable — таблица только для бэктестов с известным фактом",
		"error":       "Ошибка прогноза: predicted − actual, в единицах unit",
		"unit":        "'pp' — процентные пункты (инфляция), 'bp' — базисные пункты (ставка)",
		"meta_json":   "Дополнительные поля фолда: probs, actual_y3, start, embargo, main",
		"created_at":  "Время записи fold-предсказания",
	},
}
