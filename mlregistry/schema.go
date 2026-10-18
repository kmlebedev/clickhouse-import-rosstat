// Package mlregistry создаёт схему реестра ML-экспериментов (ml_runs — паспорт
// прогона, ml_predictions — fold-предсказания) и витрины поверх неё. Импортёр
// schema-only: внешних источников не читает, строк не возвращает.
package mlregistry

// mlRunsDdl — паспорт прогона ML-эксперимента, каноническая схема спеки §3.1.
// Одна строка — один прогон; ReplacingMergeTree по (experiment, run_id) делает
// повторную запись того же run'а (ретрай) идемпотентной.
const mlRunsDdl = `CREATE TABLE IF NOT EXISTS ml_runs (
    run_id UUID,
    experiment LowCardinality(String),   -- 'cpi_nowcast', 'key_rate_model'
    run_name String,                     -- человекочитаемое имя прогона
    status LowCardinality(String),       -- 'finished', 'empty', 'failed'
    author String,                       -- git user.name / GIT_AUTHOR_NAME
    git_sha String,
    git_branch String,
    git_dirty UInt8,                     -- 1 — рабочее дерево было грязным
    config_hash String,                  -- sha256 канонического JSON конфигурации
    config_json String,                  -- полный конфиг прогона (CLI + дефолты)
    data_asof Date,                      -- дата среза данных
    sample_hash String,                  -- sha256 канонического JSON выборки
    sample_meta_json String,             -- rows/first/last/skipped, без сырых строк
    spec_path String,                    -- спека/пункт, к которому относится run
    metrics_json String,                 -- агрегированные метрики
    artifacts_json String,               -- [{name,path,sha256,bytes}, ...]
    started_at DateTime,
    finished_at DateTime,
    duration_sec Float64,
    notes String
) ENGINE = ReplacingMergeTree
ORDER BY (experiment, run_id);`

// mlPredictionsDdl — fold-предсказания бэктестов, каноническая схема спеки §3.2.
// actual не Nullable: сюда попадают только фолды с известным фактом; unit
// ('pp' для инфляции, 'bp' для ставки) обязателен, смешивать RMSE между unit нельзя.
const mlPredictionsDdl = `CREATE TABLE IF NOT EXISTS ml_predictions (
    run_id UUID,
    experiment LowCardinality(String),
    model LowCardinality(String),        -- имя модели/метода/бенчмарка
    window LowCardinality(String),       -- 'start=24', 'start=48, embargo=1', 'expanding', 'rolling40'
    target_date Date,                    -- месяц (CPI) или дата заседания (key rate)
    predicted Float64,
    actual Float64,
    error Float64,                       -- predicted - actual, в unit
    unit LowCardinality(String),         -- 'pp' (процентные пункты), 'bp' (базисные пункты)
    meta_json String,                    -- probs/actual_y3/start/embargo/main
    created_at DateTime
) ENGINE = ReplacingMergeTree
ORDER BY (experiment, run_id, model, window, target_date);`
