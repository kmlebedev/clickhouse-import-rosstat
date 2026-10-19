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
    
    UNION ALL SELECT 'cbr_int_rate_loans', name, count(), min(date), max(date) FROM cbr_int_rate_loans FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_m2', name, count(), min(date), max(date) FROM cbr_m2 FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_credit_m2x', name, count(), min(date), max(date) FROM cbr_credit_m2x FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_indicators_cpd', name, count(), min(date), max(date) FROM cbr_indicators_cpd FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_infl_exp', name, count(), min(date), max(date) FROM cbr_infl_exp FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_loans_to_corporations', name, count(), min(date), max(date) FROM cbr_loans_to_corporations FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_loans_to_individuals', name, count(), min(date), max(date) FROM cbr_loans_to_individuals FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_households_b_mes', name, count(), min(date), max(date) FROM cbr_households_b_mes FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_zcyc', tenor, count(), min(date), max(date) FROM cbr_zcyc FINAL GROUP BY tenor

    UNION ALL SELECT 'rosstat_ipc_mes', name, count(), min(date), max(date) FROM rosstat_ipc_mes FINAL GROUP BY name
    
    UNION ALL SELECT 'rosstat_ipc_weeks', name, count(), min(date), max(date) FROM rosstat_ipc_weeks FINAL GROUP BY name
    
    UNION ALL SELECT 'rosstat_ipc_weeks_bulletins', name, count(), min(date), max(date) FROM rosstat_ipc_weeks_bulletins FINAL GROUP BY name
    
    UNION ALL SELECT 'rosstat_vvp_kvartal', name, count(), min(date), max(date) FROM rosstat_vvp_kvartal FINAL GROUP BY name
    
    UNION ALL SELECT 'rosstat_salaries_mes', name, count(), min(date), max(date) FROM rosstat_salaries_mes FINAL GROUP BY name
    
    UNION ALL SELECT 'minfin_fed_bud_mes', name, count(), min(date), max(date) FROM minfin_fed_bud_mes FINAL GROUP BY name
    
    UNION ALL SELECT 'minfin_fed_bud_mesyats', name, count(), min(date), max(date) FROM minfin_fed_bud_mesyats FINAL GROUP BY name
    
    UNION ALL SELECT 'minfin_oil_gas_mes', name, count(), min(date), max(date) FROM minfin_oil_gas_mes FINAL GROUP BY name

    UNION ALL SELECT 'ofz_curve', tenor, count(), min(date), max(date) FROM ofz_curve FINAL GROUP BY tenor
    
    UNION ALL SELECT 'events_calendar', category, count(), min(event_date), max(event_date) FROM events_calendar FINAL GROUP BY category
    
    UNION ALL SELECT 'news_events', source, count(), min(toDate(date)), max(toDate(date)) FROM news_events FINAL GROUP BY source
    
    -- Потребительские расходы Сбера (спека 2026-10-15 §4.6): высокочастотные признаки nowcast'а
    -- ИПЦ. Месячный индекс разложен по name, недельный темп — по category; ключи разные, потому
    -- что у наборов пересекаются имена категорий.
    UNION ALL SELECT 'sber_consumper_spending_index', name, count(), min(date), max(date) FROM sber_consumper_spending_index FINAL GROUP BY name
    
    UNION ALL SELECT 'sber_izmenenie_trat', category, count(), min(date), max(date) FROM sber_izmenenie_trat FINAL GROUP BY category
    
    -- Платёжный баланс ЦБ (спека 2026-10-15 cbr-bop §3.3): оценка ключевых агрегатов и стандартная
    -- презентация одного и того же домена — разные единицы (млрд и млн долларов США) и разные виды
    -- периода в имени ряда, поэтому ветки отдельные, а не одна. FINAL обязателен: обе таблицы —
    -- ReplacingMergeTree, и оценка ЦБ ревизуется новым выпуском по тому же ключу (name, date).
    UNION ALL SELECT 'cbr_bop_est', name, count(), min(date), max(date) FROM cbr_bop_est FINAL GROUP BY name
    
    UNION ALL SELECT 'cbr_bop_standard', name, count(), min(date), max(date) FROM cbr_bop_standard FINAL GROUP BY name
    
    -- Приложения к бюллетеню ДИП ЦБ (спека 2026-10-15 cbr-dip-bulletin §3.6): ключ таблицы —
    -- (series, date, release_id), id выпуска входит в ключ винтажа, поэтому число точек на ряд
    -- здесь умножается на число выпусков, в которых этот ряд напечатан: снимок отвечает
    -- «сколько точек набрано всего», а не «сколько месяцев наблюдений» (ревизии одного месяца
    -- держат несколько выпусков). Число выпусков видно отдельным запросом:
    --     SELECT uniqExact(release_id) FROM cbr_dip_bulletin FINAL;
    -- FINAL обязателен: таблица — ReplacingMergeTree, и ревизия внутри выпуска (повторная
    -- публикация того же id) перезаписывает точку — без FINAL вытесненные копии завысили бы
    -- count(). min/max date при этом остаются границами наблюдённого периода.
    UNION ALL SELECT 'cbr_dip_bulletin', series, count(), min(date), max(date) FROM cbr_dip_bulletin FINAL GROUP BY series
    
    -- Диапазоны среднесрочного прогноза ЦБ («Комментарий к среднесрочному прогнозу», спека
    -- 2026-10-17 cbr-mt-forecast §3.7): ключ таблицы — (indicator, year, release_date), дата
    -- выпуска входит в ключ винтажа, поэтому число точек на показатель здесь умножается на число
    -- выпусков, в которых этот показатель напечатан: снимок отвечает «сколько точек набрано
    -- всего», а не «на сколько лет прогноз» (один и тот же год, прочитанный из двух выпусков, —
    -- две строки: следующий выпуск ревизует диапазон). min/max release_date — границы выпусков, а
    -- не горизонт прогноза: горизонт лежит в колонке year. Число выпусков видно отдельным
    -- запросом:
    --     SELECT uniqExact(release_date) FROM cbr_mt_forecast FINAL;
    -- FINAL обязателен: таблица — ReplacingMergeTree, и повторная публикация того же ключа
    -- (indicator, year, release_date) перезаписывает точку — без FINAL вытесненные копии завысили
    -- бы count().
    UNION ALL SELECT 'cbr_mt_forecast', indicator, count(), min(release_date), max(release_date) FROM cbr_mt_forecast FINAL GROUP BY indicator
)
ORDER BY tbl, series;
