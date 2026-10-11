-- Каталог рядов золотых резервов ЦБ (macro_series, source='imf') — витрина v_imf_macro.
-- У source='imf' нет импортёра, который вызвал бы util.UpsertSeriesCatalog, поэтому описания вносятся сидом.
-- Применение: make ch-sql FILE=sql/imf_ra_gold_catalog.sql
-- Идемпотентность: ReplacingMergeTree по (source, series) — повторный прогон перезапишет строки.
-- Значения рядов пишутся НЕ здесь, а через ingest-endpoint POST /v1/manual_series (source='imf').

INSERT INTO series_catalog (source, series, title, unit, frequency, origin, description) VALUES
('imf', 'ra_gold_pol', 'Золотые резервы: Польша (IMF IRFCL, monetary gold)', 'млн USD (рыночная стоимость)', 'M',
 'IMF IRFCL, индикатор RA_GOLD (kimi-datasource imf_irfcl_data); сессионный ввод через POST /v1/manual_series (source=''imf'')',
 'Денежное золото резервов Польши (NBP) в млн USD по рыночной стоимости (не тонны); месячная точка — первый день месяца. Дельта месяца ≈ покупки/продажи; для тонн делить на цену золота. Лаг публикации ~2 месяца. Статья Q3-2026: лидер года (+98 т YTD к августу)'),

('imf', 'ra_gold_chn', 'Золотые резервы: Китай (IMF IRFCL, monetary gold)', 'млн USD (рыночная стоимость)', 'M',
 'IMF IRFCL, индикатор RA_GOLD (kimi-datasource imf_irfcl_data); сессионный ввод через POST /v1/manual_series (source=''imf'')',
 'Денежное золото резервов Народного банка Китая в млн USD по рыночной стоимости (не тонны). Статья Q3-2026: 23-й месяц покупок подряд, ~2 387 т, 9% резервов'),

('imf', 'ra_gold_rus', 'Золотые резервы: Россия (IMF IRFCL, monetary gold)', 'млн USD (рыночная стоимость)', 'M',
 'IMF IRFCL, индикатор RA_GOLD (kimi-datasource imf_irfcl_data); сессионный ввод через POST /v1/manual_series (source=''imf'')',
 'Денежное золото резервов Банка России в млн USD по рыночной стоимости (не тонны). В 2026 году ЦБ РФ нетто-продавец (−56 т YTD к августу по WGC) — ряд позволяет видеть дельты в официальной отчётности'),

('imf', 'ra_gold_uzb', 'Золотые резервы: Узбекистан (IMF IRFCL, monetary gold)', 'млн USD (рыночная стоимость)', 'M',
 'IMF IRFCL, индикатор RA_GOLD (kimi-datasource imf_irfcl_data); сессионный ввод через POST /v1/manual_series (source=''imf'')',
 'Денежное золото резервов ЦБ Узбекистана в млн USD по рыночной стоимости (не тонны). Дельты у этого ЦБ двусторонние (активные продажи и покупки), ~90% резервов в золоте'),

('imf', 'ra_gold_kaz', 'Золотые резервы: Казахстан (IMF IRFCL, monetary gold)', 'млн USD (рыночная стоимость)', 'M',
 'IMF IRFCL, индикатор RA_GOLD (kimi-datasource imf_irfcl_data); сессионный ввод через POST /v1/manual_series (source=''imf'')',
 'Денежное золото резервов Нацбанка Казахстана в млн USD по рыночной стоимости (не тонны). Статья Q3-2026: ~+36 т YTD к августу, ~79% резервов'),

('imf', 'ra_gold_cze', 'Золотые резервы: Чехия (IMF IRFCL, monetary gold)', 'млн USD (рыночная стоимость)', 'M',
 'IMF IRFCL, индикатор RA_GOLD (kimi-datasource imf_irfcl_data); сессионный ввод через POST /v1/manual_series (source=''imf'')',
 'Денежное золото резервов Чешского национального банка в млн USD по рыночной стоимости (не тонны). Статья Q3-2026: 42-й месяц покупок подряд, многолетняя программа накопления'),

('imf', 'ra_gold_tur', 'Золотые резервы: Турция (IMF IRFCL, monetary gold)', 'млн USD (рыночная стоимость)', 'M',
 'IMF IRFCL, индикатор RA_GOLD (kimi-datasource imf_irfcl_data); сессионный ввод через POST /v1/manual_series (source=''imf'')',
 'Денежное золото резервов ЦБ Турции в млн USD по рыночной стоимости (не тонны). Статья Q3-2026: −82 т YTD — крупнейший нетто-продавец среди крупных ЦБ');
