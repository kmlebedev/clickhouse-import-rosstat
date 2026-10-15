### Task 4: расписание, снимок покрытия, MCP-проверки

- `dagu/cbr.yaml`: шаг `cbr_int_rate_loans` после `cbr_bank_int_rate`, `retry_policy` по
  образцу соседей.
- `sql/key_rate_coverage.sql`: `UNION ALL SELECT 'cbr_int_rate_loans', name, count(),
  min(date), max(date) FROM cbr_int_rate_loans FINAL GROUP BY name` — по образцу
  `cbr_bank_int_rate`.
- `scripts/mcp_check.py`: в блоке проверок контура ЦБ — (а) каталог содержит 16 рядов
  источника: `SELECT count() FROM v_series_catalog WHERE source='cbr' AND origin LIKE '%loans_nonfin.xlsx%'`;
  (б) сырая `cbr_int_rate_loans` закрыта для `kimi_reader` (отказ 497) — по образцу
  проверки `cbr_households_b_mes`.

