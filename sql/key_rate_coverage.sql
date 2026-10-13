-- key_rate_coverage.sql — read-only снимок фактического покрытия рядов контура ключевой ставки.
--
-- Назначение: ответить фактом из БД на вопрос «что уже собирается и с какой даты». Глубина многих
-- рядов задаётся файлом источника, а не константой в коде импортёра, поэтому по коду её не видно;
-- этот снимок заполняет колонку «С какой даты» таблицы §4.1 в docs/ROADMAP_KEY_RATE.md.
--
-- Запуск (пользователь default, только SELECT, ничего не меняет):
--     make ch-sql FILE=sql/key_rate_coverage.sql
--
-- Ожидаемая форма вывода: table | series | rows | min_date | max_date, отсортировано по table/series.
-- FINAL обязателен: таблицы — ReplacingMergeTree, без него вытесненные копии завысили бы count().

SELECT * FROM (
    SELECT 'cbr_key_rate' AS tbl, '' AS series, count() AS rows, min(date) AS min_date, max(date) AS max_date
    FROM cbr_key_rate FINAL
    
    UNION ALL SELECT 'cbr_ruania', '', count(), min(date), max(date) FROM cbr_ruania FINAL
    
    UNION ALL SELECT 'cbr_currency_usd', '', count(), min(date), max(date) FROM cbr_currency_usd FINAL
    
    UNION ALL SELECT 'cbr_gold', '', count(), min(date), max(date) FROM cbr_gold FINAL
    
    UNION ALL SELECT 'cbr_bank_int_rate', name, count(), min(date), max(date) FROM cbr_bank_int_rate FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_m2', name, count(), min(date), max(date) FROM cbr_m2 FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_credit_m2x', name, count(), min(date), max(date) FROM cbr_credit_m2x FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_indicators_cpd', name, count(), min(date), max(date) FROM cbr_indicators_cpd FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_infl_exp', name, count(), min(date), max(date) FROM cbr_infl_exp FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_loans_to_corporations', name, count(), min(date), max(date) FROM cbr_loans_to_corporations FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_loans_to_individuals', name, count(), min(date), max(date) FROM cbr_loans_to_individuals FINAL GROUP BY name
    
    UNION ALL SELECT 'households_b_mes', name, count(), min(date), max(date) FROM households_b_mes FINAL GROUP BY name
    
    UNION ALL SELECT 'ipc_mes', name, count(), min(date), max(date) FROM ipc_mes FINAL GROUP BY name
    
    UNION ALL SELECT 'ipc_weeks', name, count(), min(date), max(date) FROM ipc_weeks FINAL GROUP BY name
    
    UNION ALL SELECT 'vvp_kvartal', name, count(), min(date), max(date) FROM vvp_kvartal FINAL GROUP BY name
    
    UNION ALL SELECT 'salaries_mes', name, count(), min(date), max(date) FROM salaries_mes FINAL GROUP BY name
    
    UNION ALL SELECT 'minfin_fed_bud_mes', name, count(), min(date), max(date) FROM minfin_fed_bud_mes FINAL GROUP BY name
    
    UNION ALL SELECT 'minfin_fed_bud_mesyats', name, count(), min(date), max(date) FROM minfin_fed_bud_mesyats FINAL GROUP BY name
    
    UNION ALL SELECT 'ofz_curve', tenor, count(), min(date), max(date) FROM ofz_curve FINAL GROUP BY tenor
    
    UNION ALL SELECT 'events_calendar', category, count(), min(event_date), max(event_date) FROM events_calendar FINAL GROUP BY category
    
    UNION ALL SELECT 'news_events', source, count(), min(toDate(date)), max(toDate(date)) FROM news_events FINAL GROUP BY source
)
ORDER BY tbl, series;
