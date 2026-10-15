### Task 3: реестр, расписание, MCP

- `cbr/importers.go`: +2 записи в хвост (`cbr_bop_est`, `cbr_bop_standard` — литералы таблицы и
  `countPoints`, §8.2), порядок = порядок `dagu/cbr.yaml`.
- `cbr/importers_test.go`: +2 имени в `expectedPublicationCountPoints` (объявлений станет 13);
  счётные формулировки комментариев и тексты ошибок («одиннадцати» → «тринадцати», где речь о
  числе импортёров с префлайтом).
- `dagu/cbr.yaml`: шаги `cbr_bop_est`, `cbr_bop_standard` 17-м и 18-м, `retry_policy` по образцу.
- `sql/key_rate_coverage.sql`: +2 ветки `UNION ALL` (`GROUP BY name`).
- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_cbr_bop TO kimi_reader`.
- `scripts/mcp_check.py`: витрина `v_cbr_bop` читается и непуста (`count()` > 0);
  каталог `cbr_bop_est` — 51 ряд (`origin LIKE '%bop_est.xlsx%'`), `cbr_bop_standard` — 34
  (`origin LIKE '%bal_of_payments_standart.xlsx%'`); отказ 497 на обеих сырых таблицах.

