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
