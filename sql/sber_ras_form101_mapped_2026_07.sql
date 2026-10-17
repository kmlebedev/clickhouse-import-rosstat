-- Одноразовый backfill маппинга cbr-809p-sber-1 формы 101 за июль 2026
-- (пункт 4.2 №22, issue #103). Импортёр cbr_form101 пишет ras-метрики начиная
-- с августа (из текущего выпуска формы 101); июльский raw-выпуск в таблицу не
-- заведён, поэтому его две метрики добавляются этим сидом.
--
-- Источник: форма 101 ЦБ на 01.08.2026 (июль), архив
-- https://www.cbr.ru/vfs/credit/forms/101-20260801.rar
-- Набор счетов (cbr-809p-sber-1, те же, что в cbr/form101.go):
--   кредиты ЮЛ = 45.0 (A_P=1) + активные 441–451,453,454 +
--                460–470,472,47.1,477 (прочие размещённые средства и лизинг)
--                + доля 458 (A_P=1) пропорционально текущим портфелям
--                = 31 756,324775 млрд ₽;
--   кредиты ФЛ = 45.2 (A_P=1) + доля 458 (A_P=1) = 19 963,292514 млрд ₽.
-- Резервы (пассивные 45.1/45.2), 459 (проценты), 474+ и 478 не входят.
-- Калибровка: пресс-релиз за 7М 2026 — 31 771 / 19 942 млрд ₽,
-- разница −0,05 % / +0,11 % (см. issue #103 и
-- docs/research/2026-10-17-cbr-form101-mapping-methodology.md).
--
-- Запуск: make ch-sql FILE=sql/sber_ras_form101_mapped_2026_07.sql
-- Повтор идемпотентен: ReplacingMergeTree(loaded_at), ключ
-- (company, metric, period, source_kind, source_url).

INSERT INTO company_financials
    (company, metric, period, period_type, source_kind, value, unit, source_url, source_page, loaded_at)
VALUES
    ('SBER', 'loans_corporations', '2026M07', 'M', 'ras', 31756.324775, 'млрд ₽',
     'https://www.cbr.ru/vfs/credit/forms/101-20260801.rar', 0, now()),
    ('SBER', 'loans_individuals', '2026M07', 'M', 'ras', 19963.292514, 'млрд ₽',
     'https://www.cbr.ru/vfs/credit/forms/101-20260801.rar', 0, now());
