-- Каталог рядов золотых резервов ЦБ (macro_series, source='imf') — витрина v_imf_macro.
-- У source='imf' нет импортёра, который вызвал бы util.UpsertSeriesCatalog, поэтому описания вносятся сидом.
-- Применение: make ch-sql FILE=sql/imf_ra_gold_catalog.sql
-- Идемпотентность: ReplacingMergeTree по (source, series) — повторный прогон перезапишет строки.
-- Значения ряда пишутся НЕ здесь, а через ingest-endpoint POST /v1/manual_series (source='imf').
--
-- Область описания (проверено живой пробой 2026-10-11): IRFCL через kimi-datasource отдаёт
-- золото только одним индикатором IRFCLDT1_IRFCL40_USD, и по наблюдаемым 7 странам заполнен
-- единственный ряд — Китай. POL/CZE/TUR отдают нули (нули не пишем: читались бы как «резервов
-- нет»), RUS/KAZ — индикатор отсутствует, UZB — данных нет. Резервы по остальным странам
-- (Польша — лидер покупок года в статье) ведутся отдельно: WGC/WebBridge, фаза 3
-- docs/GOLD_THESIS_DATA_ROADMAP.md.

INSERT INTO series_catalog (source, series, title, unit, frequency, origin, description) VALUES
('imf', 'ra_gold_chn', 'Золотые резервы: Китай (IMF IRFCL, monetary gold)', 'USD (индикатор IRFCLDT1_IRFCL40_USD)', 'M',
 'IMF IRFCL, индикатор RA_GOLD=IRFCLDT1_IRFCL40_USD (kimi-datasource imf_irfcl_data); сессионный ввод через POST /v1/manual_series (source=''imf'')',
 'Денежное золото резервов Народного банка Китая, значение — как отдаёт индикатор IRFCLDT1_IRFCL40_USD (USD, рыночная стоимость; не тонны); месячная точка — первый день месяца. В IRFCL через datasource это единственный заполненный ряд: POL/CZE/TUR — нули, RUS/KAZ — без индикатора, UZB — без данных (покупки ЦБ по остальным странам — отдельный канал WGC/WebBridge). Дельта месяца ≈ изменение стоимости; для тонн делить на цену золота. Лаг публикации ~2 месяца');
