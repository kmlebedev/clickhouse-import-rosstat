### Task 6: живая приёмка и статус роадмапа

Требуется dev-ClickHouse (если не отвечает — попросить владельца поднять `make ch-up`,
агент его не поднимает).

1. `make all` — зелёный.
2. `go test -race -count=1 ./cbr/` — зелёный.
3. `make import STAT=cbr_int_rate_loans` — ожидание: `Imported 2432 rows`, диапазон
   2014-01-30 … 2026-08-30; spot-сверка из ClickHouse с живым xlsx (3 значения из T1).
4. Повторный `make import STAT=cbr_int_rate_loans` — no-op («версия … уже импортирована»).
5. MCP: `SELECT count() FROM v_cbr_macro` (вырос на 2 432), `SELECT count() FROM
   v_series_catalog WHERE source='cbr' AND origin LIKE '%loans_nonfin.xlsx%'` = 16,
   сырой SELECT под `kimi_reader` — отказ.
6. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА. `dagu validate dagu/cbr.yaml` — exit 0.
7. `make ch-sql FILE=sql/key_rate_coverage.sql` — новая таблица в снимке с 16 рядами.
8. `docs/ROADMAP_KEY_RATE.md`: §4.2 №8 — «✅ фаза 2, этим PR (`cbr_int_rate_loans`: 16 рядов,
   2 432 точки, 2014-01 … 2026-08, приёмка 2026-10-15)»; абзац фазы 2 в §6 — отметить
   закрытие №8. Факты приёмки — команда/числа/даты — в коммит и этот план.

