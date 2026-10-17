-- Сид календаря раскрытия банковских результатов на Q4-2026 (пункт 4.2 №20 ROADMAP_KEY_RATE.md).
-- Таблица events_calendar создаётся сидом sql/events_calendar_q4_2026.sql или импортёром events_calendar.
-- Запуск: make ch-sql FILE=sql/events_calendar_bank_results_q4_2026.sql
-- Вставка идемпотентна: ReplacingMergeTree ORDER BY (event_date, category, title) сворачивает дубли.
--
-- Состав подтверждён разведкой 2026-10-17: помесячно публично раскрываются Сбер (РСБУ) и ВТБ (МСФО);
-- Т-Банк, Совкомбанк и МКБ публикуют поквартально/по полугодиям и в сид не входят.
-- date_status=confirmed — дата опубликована банком; expected — медиана фактических дат публикаций,
-- перечень использованных дат лежит в threshold.basis.

INSERT INTO events_calendar (event_date, event_time, category, title, threshold, status) VALUES
    ('2026-09-29', NULL, 'bank_results', 'ВТБ: результаты за август 2026 — кредитный портфель (подтверждённая дата)', '{"bank":"VTBR","report_month":"2026-08","metric":"customer_loans","date_status":"confirmed","source":"https://www.vtb.ru/ir/statements/results/"}', 'done'),
    ('2026-10-09', NULL, 'bank_results', 'Сбер: результаты за сентябрь 2026 — кредитный портфель (подтверждённая дата)', '{"bank":"SBER","report_month":"2026-09","metric":"customer_loans","date_status":"confirmed","source":"https://www.sberbank.com/ru/investor-relations/groupresults/rpbu/sber_results_october_9month_for_investors"}', 'done'),
    ('2026-10-30', NULL, 'bank_results', 'ВТБ: результаты за сентябрь 2026 — кредитный портфель (подтверждённая дата)', '{"bank":"VTBR","report_month":"2026-09","metric":"customer_loans","date_status":"confirmed","source":"https://www.vtb.ru/ir/calendar/"}', 'pending'),
    ('2026-11-10', NULL, 'bank_results', 'Сбер: результаты за октябрь 2026 — кредитный портфель (ожидаемая дата)', '{"bank":"SBER","report_month":"2026-10","metric":"customer_loans","date_status":"expected","rule":"9–11-е число следующего месяца; выбрано 10-е (внутри окна)","basis":"2026-08-11 (7М), 2026-09-09 (8М), 2026-10-09 (9М)"}', 'pending'),
    ('2026-11-27', NULL, 'bank_results', 'ВТБ: результаты за октябрь 2026 — кредитный портфель (ожидаемая дата)', '{"bank":"VTBR","report_month":"2026-10","metric":"customer_loans","date_status":"expected","rule":"последняя неделя ноября (24–30-е), день недели — по фактическому календарю публикаций","basis":"2025-11-28, 2024-11-28, 2023-11-24"}', 'pending'),
    ('2026-12-10', NULL, 'bank_results', 'Сбер: результаты за ноябрь 2026 — кредитный портфель (ожидаемая дата)', '{"bank":"SBER","report_month":"2026-11","metric":"customer_loans","date_status":"expected","rule":"9–11-е число следующего месяца; выбрано 10-е (внутри окна)","basis":"2026-08-11 (7М), 2026-09-09 (8М), 2026-10-09 (9М)"}', 'pending'),
    ('2026-12-25', NULL, 'bank_results', 'ВТБ: результаты за ноябрь 2026 — кредитный портфель (ожидаемая дата)', '{"bank":"VTBR","report_month":"2026-11","metric":"customer_loans","date_status":"expected","rule":"последняя неделя декабря (22–28-е), день недели — по фактическому календарю публикаций","basis":"2025-12-26, 2024-12-26, 2023-12-22"}', 'pending');
