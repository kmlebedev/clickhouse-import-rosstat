-- Сид месячных данных РСБУ Сбера за август 2026 (пункт 4.2 №21 ROADMAP_KEY_RATE.md,
-- калибровка pilot-маппинга формы 101, issue #98).
-- Источник: пресс-релиз Сбера «Сокращенные результаты ПАО Сбербанк по РПБУ за 8М 2026 года»,
-- опубликован 09.09.2026:
-- https://www.sberbank.com/ru/investor-relations/groupresults/rpbu/september_8month_for_investors_groupresults_sber
-- В таблице пресс-релиза: «Кредиты юридическим лицам до резервов 32 512 млрд ₽»,
-- «Кредиты физическим лицам до резервов 20 200 млрд ₽» (август 2026).
--
-- Это эталон для калибровки формы 101 на 01.09.2026 (та же отчётная дата — август).
-- customer_loans (нетто) не заводится: пресс-релиз печатает «до резервов».
-- Запуск: make ch-sql FILE=sql/sber_ras_monthly_seed_2026_08.sql
-- Повтор идемпотентен: ReplacingMergeTree(loaded_at), ключ
-- (company, metric, period, source_kind, source_url).

INSERT INTO company_financials
    (company, metric, period, period_type, source_kind, value, unit, source_url, source_page, loaded_at)
VALUES
    ('SBER', 'loans_corporations', '2026M08', 'M', 'kpi', 32512, 'млрд ₽',
     'https://www.sberbank.com/ru/investor-relations/groupresults/rpbu/september_8month_for_investors_groupresults_sber', 0, now()),
    ('SBER', 'loans_individuals', '2026M08', 'M', 'kpi', 20200, 'млрд ₽',
     'https://www.sberbank.com/ru/investor-relations/groupresults/rpbu/september_8month_for_investors_groupresults_sber', 0, now());
