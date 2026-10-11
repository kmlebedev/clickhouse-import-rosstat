-- Сид консенсуса аналитиков по золоту (gold_forecasts) и собственной сценарной сетки.
-- Источник: статья 07.10.2026, § «Сравнение с консенсусом аналитиков»; published у всех
-- строк — 2026-10-07 (дата статьи). Читается витриной v_gold_forecasts (views/gold_forecasts.go).
-- Применение: make ch-sql FILE=sql/gold_forecasts_q4_2026_seed.sql — ДО make import STAT=gold_views,
-- иначе гейт util.CreateView пропустит витрину (таблицы gold_forecasts ещё не будет).
-- DDL ниже — дословная копия goldForecastsDdl из ingest/schema.go (канонический ARCHITECTURE.md §6.2):
-- сид самодостаточен, прецедент — sql/events_calendar_q4_2026.sql.
-- Идемпотентность: ReplacingMergeTree ORDER BY (author, published, horizon, scenario) — повторный
-- прогон перезапишет строки, но версии в таблице накопятся: сид применять один раз.
--
-- Семантика строк: банковские цели — scenario='point' (точечная цель) или 'range' (диапазон
-- low/high, point не задан); own-строки — вероятностные сценарии bull/base/bear.
-- own-точки — середины диапазонов статьи; вероятностно-взвешенная цена
-- 0.25 × 4800 + 0.40 × 4300 + 0.35 × 3900 = 4285 совпадает с xau_q_avg прогона 10.10.2026
-- (forecast_log). Пустое поле («—» в таблице статьи) — 0: конвенция колонок DDL,
-- витрина v_gold_forecasts превращает 0 в NULL.

CREATE TABLE IF NOT EXISTS gold_forecasts (
    author LowCardinality(String),   -- 'goldman','jpm','hsbc','ubs','citi','own',...
    published Date,
    horizon LowCardinality(String),  -- 'Q4-2026','YE-2026','2027','12M','short'
    scenario LowCardinality(String), -- 'point','range','bull','base','bear'
    low Float64, high Float64, point Float64,
    probability Float32
) ENGINE = ReplacingMergeTree ORDER BY (author, published, horizon, scenario);

INSERT INTO gold_forecasts (author, published, horizon, scenario, low, high, point, probability) VALUES
    ('goldman', '2026-10-07', 'YE-2026', 'point',    0,    0, 4900,  0),
    ('goldman', '2026-10-07', '2027',    'range', 5400, 5600,    0,  0),
    ('hsbc',    '2026-10-07', 'YE-2026', 'point',    0,    0, 4750,  0),
    ('hsbc',    '2026-10-07', '2027',    'point',    0,    0, 5025,  0),
    ('jpm',     '2026-10-07', 'Q4-2026', 'point',    0,    0, 4500,  0),
    ('jpm',     '2026-10-07', '2027',    'point',    0,    0, 5400,  0),
    ('ubs',     '2026-10-07', '12M',     'point',    0,    0, 5200,  0),
    ('citi',    '2026-10-07', 'short',   'point',    0,    0, 4000,  0),
    ('own',     '2026-10-07', 'Q4-2026', 'bull',  4600, 5000, 4800, 25),
    ('own',     '2026-10-07', 'Q4-2026', 'base',  4000, 4600, 4300, 40),
    ('own',     '2026-10-07', 'Q4-2026', 'bear',  3750, 4050, 3900, 35);
