-- Сид обновлённой собственной сценарной сетки по золоту на Q4 2026 (31.10.2026).
-- Источник: статья «Золото: пробой $4 000 — что произошло и куда дальше» (31.10.2026),
-- § «Новый прогноз на Q4 2026». published у всех строк — 2026-10-31 (дата обновления).
-- Читается витриной v_gold_forecasts (views/gold_forecasts.go).
--
-- Контекст: сетка 07.10 (base 4 300 / bear 3 900 / bull 4 800) устарела — 30.10 COMEX
-- закрылась на 3 898,9, октябрьская средняя ~4 123. Новые точки — середины диапазонов:
--   bear 3 700–3 950 → 3 850, вероятность 45%;
--   base 3 950–4 200 → 4 050, вероятность 40%;
--   bull 4 200–4 500 → 4 300, вероятность 15%.
-- Вероятностно-взвешенная цена: 0,45×3 850 + 0,40×4 050 + 0,15×4 300 = 3 997,5 ≈ 4 000.
-- Старые строки 07.10 не трогаем: ReplacingMergeTree ORDER BY (author, published, horizon,
-- scenario) хранит историю консенсуса; новая дата — новая версия сетки.
--
-- Применение (dev): make ch-sql FILE=sql/gold_forecasts_2026_10_31_seed.sql
-- На stage сид в этой задаче НЕ применяется: stage-БД общая, запись — только по просьбе владельца.
-- Повторный прогон идемпотентен по ключу, но версии в таблице накопятся — применять один раз.
-- DDL ниже — дословная копия goldForecastsDdl из ingest/schema.go (канонический ARCHITECTURE.md §6.2):
-- сид самодостаточен, прецедент — sql/gold_forecasts_q4_2026_seed.sql.
-- Семантика: own-строки — вероятностные сценарии bull/base/bear; пустое поле — 0
-- (конвенция колонок DDL, витрина v_gold_forecasts превращает 0 в NULL).
--
-- Проверка после применения:
--   SELECT author, published, horizon, scenario, low, high, point, probability
--   FROM v_gold_forecasts WHERE published = '2026-10-31' ORDER BY scenario;
-- Ожидаем 3 строки: bear 3 700/3 950/3 850/45, base 3 950/4 200/4 050/40,
-- bull 4 200/4 500/4 300/15.

CREATE TABLE IF NOT EXISTS gold_forecasts (
    author LowCardinality(String),   -- 'goldman','jpm','hsbc','ubs','citi','own',...
    published Date,
    horizon LowCardinality(String),  -- 'Q4-2026','YE-2026','2027','12M','short'
    scenario LowCardinality(String), -- 'point','range','bull','base','bear'
    low Float64, high Float64, point Float64,
    probability Float32
) ENGINE = ReplacingMergeTree ORDER BY (author, published, horizon, scenario);

INSERT INTO gold_forecasts (author, published, horizon, scenario, low, high, point, probability) VALUES
    ('own', '2026-10-31', 'Q4-2026', 'bear', 3700, 3950, 3850, 45),
    ('own', '2026-10-31', 'Q4-2026', 'base', 3950, 4200, 4050, 40),
    ('own', '2026-10-31', 'Q4-2026', 'bull', 4200, 4500, 4300, 15);
