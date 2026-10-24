-- Пользователь MCP-агента: только чтение витрин. Пароль подставьте вручную (<your-password>) и не коммитьте результат.
-- Запуск: clickhouse-client --multiquery < sql/mcp_kimi_reader.sql

CREATE USER IF NOT EXISTS kimi_reader IDENTIFIED BY '<your-password>' DEFAULT DATABASE default SETTINGS readonly = 1;

GRANT SELECT ON default.v_bea_pce TO kimi_reader;
GRANT SELECT ON default.v_series_catalog TO kimi_reader;
GRANT SELECT ON default.v_fred_macro TO kimi_reader;
GRANT SELECT ON default.v_bls_macro TO kimi_reader;
GRANT SELECT ON default.v_cbr_macro TO kimi_reader;
GRANT SELECT ON default.v_rosstat_macro TO kimi_reader;
-- ИЦП: сырые ряды по ВЭД, товарам/ОКПД2, веса и производный SA-слой (T5 #280).
-- Сырые rosstat_ppi_ved/product/weights и ppi_sa агенту не выданы — только витрина.
GRANT SELECT ON default.v_rosstat_ppi TO kimi_reader;
-- Недельный SA-агрегат ИПЦ (пункт key-rate/phase-4-weekly-sa-aggregate, #353):
-- производный ряд weekly_cpi_sa; сырая таблица агенту не выдана — только витрина.
GRANT SELECT ON default.v_weekly_cpi_sa TO kimi_reader;
-- ЕМИСС 37426: недельные средние потребительские цены (импортёр fedstat_weekly_prices)
GRANT SELECT ON default.v_fedstat_weekly_prices TO kimi_reader;
-- ЕМИСС макро-блоки: ИПП, розница, сельхоз, инвестиции, ИПЦ 31074 (импортёры fedstat_ipp,
-- fedstat_retail, fedstat_agri, fedstat_investment, fedstat_ipc)
GRANT SELECT ON default.v_fedstat_ipp TO kimi_reader;
GRANT SELECT ON default.v_fedstat_retail TO kimi_reader;
GRANT SELECT ON default.v_fedstat_agri TO kimi_reader;
GRANT SELECT ON default.v_fedstat_investment TO kimi_reader;
GRANT SELECT ON default.v_fedstat_ipc TO kimi_reader;
GRANT SELECT ON default.v_minfin_budget TO kimi_reader;
GRANT SELECT ON default.v_gold_prices TO kimi_reader;
GRANT SELECT ON default.v_stock_prices TO kimi_reader;
GRANT SELECT ON default.v_ofz_curve TO kimi_reader;
GRANT SELECT ON default.v_events_calendar TO kimi_reader;
GRANT SELECT ON default.v_model_inputs TO kimi_reader;
GRANT SELECT ON default.v_gold_dashboard TO kimi_reader;
GRANT SELECT ON default.v_forecast_accuracy TO kimi_reader;
GRANT SELECT ON default.v_company_financials TO kimi_reader;
GRANT SELECT ON default.v_company_metric_sources TO kimi_reader;
GRANT SELECT ON default.v_company_operating TO kimi_reader;
GRANT SELECT ON default.v_dcf_assumptions TO kimi_reader;
GRANT SELECT ON default.v_imf_macro TO kimi_reader;
GRANT SELECT ON default.v_eia_petroleum TO kimi_reader;
GRANT SELECT ON default.v_gold_forecasts TO kimi_reader;
GRANT SELECT ON default.v_gold_etf_flows TO kimi_reader;
GRANT SELECT ON default.v_cb_gold_dynamics TO kimi_reader;
GRANT SELECT ON default.v_gold_etf_flows_daily TO kimi_reader;
GRANT SELECT ON default.v_gold_etf_funds_daily TO kimi_reader;
GRANT SELECT ON default.v_news_events TO kimi_reader;
GRANT SELECT ON default.v_news_flash TO kimi_reader;
GRANT SELECT ON default.v_news_message_versions TO kimi_reader;
GRANT SELECT ON default.v_news_tags TO kimi_reader;
GRANT SELECT ON default.v_gost_vehicle_safety_certificate TO kimi_reader;
GRANT SELECT ON default.v_gost_vehicle_safety_daily TO kimi_reader;
GRANT SELECT ON default.v_keyrate_expectations TO kimi_reader;
-- Сводный дашборд контура ключевой ставки (пункт key-rate/phase-4-dashboard, #128):
-- витрина читает сырой forecast_log как DEFINER, но агенту выдан только этот срез.
GRANT SELECT ON default.v_key_rate_dashboard TO kimi_reader;
GRANT SELECT ON default.v_cbr_survey TO kimi_reader;
GRANT SELECT ON default.v_cbr_mo_br TO kimi_reader;
GRANT SELECT ON default.v_cbr_mp_summary TO kimi_reader;
GRANT SELECT ON default.v_cbr_mp_summary_blocks TO kimi_reader;
GRANT SELECT ON default.v_cbr_mp_summary_channels TO kimi_reader;
-- Контур пресс-релизов по ключевой ставке (пункт key-rate/phase-4-cbr-key-rate-press, #368):
-- три витрины — шапка релиза, абзацы и квартальные блоки; сырые таблицы агенту закрыты.
GRANT SELECT ON default.v_cbr_key_rate_press TO kimi_reader;
GRANT SELECT ON default.v_cbr_key_rate_press_blocks TO kimi_reader;
GRANT SELECT ON default.v_cbr_key_rate_press_quarters TO kimi_reader;
-- Контур стенограмм пресс-конференций по ключевой ставке (пункт
-- key-rate/phase-4-cbr-key-rate-pressconf, #370): три витрины — шапка, реплики с
-- таймкодами и полный текст; сырые таблицы агенту закрыты.
GRANT SELECT ON default.v_cbr_key_rate_pressconf TO kimi_reader;
GRANT SELECT ON default.v_cbr_key_rate_pressconf_cues TO kimi_reader;
GRANT SELECT ON default.v_cbr_key_rate_pressconf_text TO kimi_reader;
GRANT SELECT ON default.v_bank_sber_spending TO kimi_reader;
GRANT SELECT ON default.v_bank_macro TO kimi_reader;
GRANT SELECT ON default.v_cbr_bop TO kimi_reader;
GRANT SELECT ON default.v_cbr_dip_bulletin TO kimi_reader;
GRANT SELECT ON default.v_point_in_time TO kimi_reader;
GRANT SELECT ON default.v_point_in_time_asof TO kimi_reader;
GRANT SELECT ON default.v_point_in_time_version TO kimi_reader;
GRANT SELECT ON default.v_cbr_mt_forecast TO kimi_reader;
-- График публикаций ЦБ и исторические published_at (пункт key-rate/phase-4-cbr-publication-calendar, #299).
GRANT SELECT ON default.v_cbr_publication_calendar TO kimi_reader;
GRANT SELECT ON default.v_series_publication TO kimi_reader;
GRANT SELECT ON default.v_bank_loan_portfolio_forward TO kimi_reader;
GRANT SELECT ON default.v_cbr_form101 TO kimi_reader;
GRANT SELECT ON default.v_cbr_form101_loans TO kimi_reader;
GRANT SELECT ON default.v_ofz_curve_compare TO kimi_reader;
GRANT SELECT ON default.v_ofz_signal TO kimi_reader;
GRANT SELECT ON default.v_ofz_stress TO kimi_reader;
-- Макропруденциальные требования (пункт key-rate/phase-2-ccyb, #115): АЦН-индикаторы,
-- матрицы надбавок и МПЛ. Сырые cbr_ccyb_indicators/cbr_risk_weight_addons/cbr_mpl
-- агенту не выданы — только витрины.
GRANT SELECT ON default.v_cbr_ccyb_indicators TO kimi_reader;
GRANT SELECT ON default.v_cbr_risk_weight_addons TO kimi_reader;
GRANT SELECT ON default.v_cbr_mpl TO kimi_reader;
-- ФАО: индекс продовольственных цен (пункт infra/1.6, issue #225).
GRANT SELECT ON default.v_fao_food_price TO kimi_reader;

-- Реестр ML-экспериментов (infra/4.1): агенту выданы только витрины.
-- Сырые ml_runs/ml_predictions закрыты: паспорт прогона и fold-предсказания
-- читаются через v_ml_runs, v_ml_leaderboard и v_ml_error_by_fold.
GRANT SELECT ON default.v_ml_runs TO kimi_reader;
GRANT SELECT ON default.v_ml_leaderboard TO kimi_reader;
GRANT SELECT ON default.v_ml_error_by_fold TO kimi_reader;
-- Live-точность контура ставки (infra/4.3): витрина читает сырой forecast_log
-- как DEFINER, агенту выдан только срез с треком/стадией и error_bp.
GRANT SELECT ON default.v_ml_live_accuracy TO kimi_reader;

-- Task 7 (#248): публичный классификатор ОКВЭД2 и кроссворд 87 BSU-кодов на отраслевые
-- листы mp_survey_data.xlsx. Сырые rosstat_okved2 и cbr_mp_bsu_map закрыты: агенту
-- выданы витрины с названиями и публичная панель.
GRANT SELECT ON default.v_rosstat_okved2 TO kimi_reader;
GRANT SELECT ON default.v_cbr_mp_bsu_map TO kimi_reader;
GRANT SELECT ON default.v_cbr_mp_public_panel TO kimi_reader;

-- Рыночные ряды MOEX (пункт key-rate/phase-2-brent, #113): Brent-фронт-месяц.
-- Сырая moex_market агенту не выдана — только витрина.
GRANT SELECT ON default.v_moex_market TO kimi_reader;

-- Контур недвижимости (пункт real-estate/1.1): рынок ипотеки ДОМ.РФ и индекс цен первички.
-- Сырые domrf_mortgage_market, domrf_mortgage_weekly и domrf_price_index агенту не выданы —
-- только витрины с комментариями колонок.
GRANT SELECT ON default.v_domrf_mortgage TO kimi_reader;
GRANT SELECT ON default.v_domrf_price_index TO kimi_reader;
