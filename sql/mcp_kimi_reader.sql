-- Пользователь MCP-агента: только чтение витрин. Пароль подставьте вручную (<your-password>) и не коммитьте результат.
-- Запуск: clickhouse-client --multiquery < sql/mcp_kimi_reader.sql

CREATE USER IF NOT EXISTS kimi_reader IDENTIFIED BY '<your-password>' DEFAULT DATABASE default SETTINGS readonly = 1;

GRANT SELECT ON default.v_bea_pce TO kimi_reader;
GRANT SELECT ON default.v_series_catalog TO kimi_reader;
GRANT SELECT ON default.v_fred_macro TO kimi_reader;
GRANT SELECT ON default.v_bls_macro TO kimi_reader;
