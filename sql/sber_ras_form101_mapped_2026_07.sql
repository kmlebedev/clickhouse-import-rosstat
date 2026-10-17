-- Одноразовый backfill pilot-1 маппинга формы 101 за июль 2026 (пункт 4.2 №21,
-- issue #98). Импортёр cbr_form101 пишет ras-метрики начиная с августа (из
-- текущего выпуска формы 101); июльский raw-выпуск в таблицу не заведён,
-- поэтому его две метрики добавляются этим сидом.
--
-- Источник: форма 101 ЦБ на 01.08.2026 (июль), архив
-- https://www.cbr.ru/vfs/credit/forms/101-20260801.rar
-- Пилотные суммы по счетам (pilot-1, те же, что в cbr/form101.go):
--   кредиты ЮЛ = 45.0 (A_P=1) + 45.1 (A_P=2) +
--                451/446/442/445/448/449/447/450/453/454 (A_P=1) = 31 301,367569 млрд ₽;
--   кредиты ФЛ = 45.2 (A_P=1 + A_P=2) = 19 999,254899 млрд ₽.
-- Калибровка: пресс-релиз за 7М 2026 — 31 771 / 19 942 млрд ₽,
-- разница −1,48 % / +0,29 % (см. issue #98). 458/459 (просрочка) не прибавлены.
--
-- Запуск: make ch-sql FILE=sql/sber_ras_form101_mapped_2026_07.sql
-- Повтор идемпотентен: ReplacingMergeTree(loaded_at), ключ
-- (company, metric, period, source_kind, source_url).

INSERT INTO company_financials
    (company, metric, period, period_type, source_kind, value, unit, source_url, source_page, loaded_at)
VALUES
    ('SBER', 'loans_corporations', '2026M07', 'M', 'ras', 31301.367569, 'млрд ₽',
     'https://www.cbr.ru/vfs/credit/forms/101-20260801.rar', 0, now()),
    ('SBER', 'loans_individuals', '2026M07', 'M', 'ras', 19999.254899, 'млрд ₽',
     'https://www.cbr.ru/vfs/credit/forms/101-20260801.rar', 0, now());
