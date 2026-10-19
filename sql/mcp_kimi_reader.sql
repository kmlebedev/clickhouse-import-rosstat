-- Пользователь MCP-агента: только чтение витрин. Пароль подставьте вручную (<your-password>) и не коммитьте результат.
-- Запуск: clickhouse-client --multiquery < sql/mcp_kimi_reader.sql

CREATE USER IF NOT EXISTS kimi_reader IDENTIFIED BY '<your-password>' DEFAULT DATABASE default SETTINGS readonly = 1;

GRANT SELECT ON default.v_bea_pce TO kimi_reader;
GRANT SELECT ON default.v_series_catalog TO kimi_reader;
GRANT SELECT ON default.v_fred_macro TO kimi_reader;
GRANT SELECT ON default.v_bls_macro TO kimi_reader;
GRANT SELECT ON default.v_cbr_macro TO kimi_reader;
GRANT SELECT ON default.v_rosstat_macro TO kimi_reader;
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
GRANT SELECT ON default.v_bank_sber_spending TO kimi_reader;
GRANT SELECT ON default.v_bank_macro TO kimi_reader;
GRANT SELECT ON default.v_cbr_bop TO kimi_reader;
GRANT SELECT ON default.v_cbr_dip_bulletin TO kimi_reader;
GRANT SELECT ON default.v_point_in_time TO kimi_reader;
GRANT SELECT ON default.v_point_in_time_asof TO kimi_reader;
GRANT SELECT ON default.v_point_in_time_version TO kimi_reader;
GRANT SELECT ON default.v_cbr_mt_forecast TO kimi_reader;
GRANT SELECT ON default.v_bank_loan_portfolio_forward TO kimi_reader;
GRANT SELECT ON default.v_cbr_form101 TO kimi_reader;
GRANT SELECT ON default.v_cbr_form101_loans TO kimi_reader;
GRANT SELECT ON default.v_ofz_curve_compare TO kimi_reader;
GRANT SELECT ON default.v_ofz_signal TO kimi_reader;
-- ФАО: индекс продовольственных цен (пункт infra/1.6, issue #225).
GRANT SELECT ON default.v_fao_food_price TO kimi_reader;

-- Реестр ML-экспериментов (infra/4.1): агенту выданы только витрины.
-- Сырые ml_runs/ml_predictions закрыты: паспорт прогона и fold-предсказания
-- читаются через v_ml_runs, v_ml_leaderboard и v_ml_error_by_fold.
GRANT SELECT ON default.v_ml_runs TO kimi_reader;
GRANT SELECT ON default.v_ml_leaderboard TO kimi_reader;
GRANT SELECT ON default.v_ml_error_by_fold TO kimi_reader;
