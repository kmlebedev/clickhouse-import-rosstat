-- Сид месячных данных РСБУ Сбера за сентябрь 2026 (пункт 4.2 №21 ROADMAP_KEY_RATE.md).
-- Источник: пресс-релиз Сбера «Сокращенные результаты ПАО Сбербанк по РПБУ за 9М 2026 года»,
-- опубликован 09.10.2026:
-- https://www.sberbank.com/ru/investor-relations/groupresults/rpbu/sber_results_october_9month_for_investors
-- В таблице пресс-релиза: «Кредиты юридическим лицам до резервов 32 825 млрд ₽»,
-- «Кредиты физическим лицам до резервов 20 250 млрд ₽» (сентябрь 2026).
--
-- customer_loans (нетто) не заводится: пресс-релиз печатает «до резервов», а ряд
-- customer_loans — нетто; смешивать базы нельзя. Сплиты loans_corporations/loans_individuals —
-- гросс, они соответствуют подписям пресс-релиза.
--
-- Сид — временный срез до импортёра формы 101 ЦБ (issue #93). Запуск:
-- make ch-sql FILE=sql/sber_ras_monthly_seed_2026_09.sql
-- Повтор идемпотентен: ReplacingMergeTree(loaded_at), ключ
-- (company, metric, period, source_kind, source_url).

INSERT INTO company_financials
    (company, metric, period, period_type, source_kind, value, unit, source_url, source_page, loaded_at)
VALUES
    ('SBER', 'loans_corporations', '2026M09', 'M', 'kpi', 32825, 'млрд ₽',
     'https://www.sberbank.com/ru/investor-relations/groupresults/rpbu/sber_results_october_9month_for_investors', 0, now()),
    ('SBER', 'loans_individuals', '2026M09', 'M', 'kpi', 20250, 'млрд ₽',
     'https://www.sberbank.com/ru/investor-relations/groupresults/rpbu/sber_results_october_9month_for_investors', 0, now());
