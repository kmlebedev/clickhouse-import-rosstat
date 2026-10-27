# План: опережающий nowcast-месяц `weekly_cpi_sa` (пункт `key-rate/phase-4-weekly-sa-nowcast`)

**Дата:** 2026-10-22. **Спека:** [2026-10-22-weekly-sa-nowcast-design.md](../specs/2026-10-22-weekly-sa-nowcast-design.md). **Issue:** #355. **База:** `origin/main` `27a4a29` (PR #354 merged). **Ветка:** `fix/weekly-sa-nowcast`.

## Шаги

1. **Схема** `weeklysa/schema.go`: `as_of Date`, `partial UInt8` в `CREATE TABLE`; `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` для существующей таблицы; `spec_version` остаётся `weekly-cpi-sa-2026-v1` (формула и R-скрипт не менялись; nowcast — расширение набора строк, помеченное `partial`); обновить `weeklySADdl`/`countPoints` (пин R-скрипта тот же).
2. **Месяцы** `weeklysa/grid.go`: `monthPoint{Date, Value, AsOf, Partial}`; `monthlyLastInMonth` возвращает `[]monthPoint`; `partialMonth(index, lastGridDate)`; `computeMetrics` принимает `[]monthPoint` и проставляет `AsOf`/`Partial`.
3. **Импортёр** `weeklysa/importer.go`: complete-фит (209) → исторические строки; nowcast-фит (209 + partial) → одна partial-строка; `weeklySARow` + `AsOf`/`Partial`; `insertRows` + `as_of`/`partial`; `shouldSkip` — 209 + partial; лог «209 complete + 1 nowcast».
4. **Витрина/каталог** `weeklysa/views.go`, `series_meta.go`: `as_of`/`partial` в long-форме; описание «полные месяцы + текущий неполный nowcast»; `date`/`as_of`/`partial` в комментариях.
5. **Общая таблица** `views/key_rate_views.go`: `weekly_cpi_sa` в `Tables`; CTE `wsa`; строка `fundamental/weekly_cpi_saar` (unit, as_of, threshold 4 %, stale 14 дней, flag >4). Тест `views/key_rate_dashboard_test.go` — 12 таблиц, маркер `'weekly_cpi_saar'`.
6. **Тесты** `weeklysa`: partial month включается при `lastGridDate < monthEnd` и не включается при полном месяце; complete-фит не содержит partial; nowcast-фит 210; `as_of`/`partial` в строках; parity 209 полных (существующий тест/живая проверка); `go test ./weeklysa/... ./views/...`.
7. **MCP** `scripts/mcp_check.py`: `>= 627` / 3 ряда; отдельная проверка `countIf(partial=1) <= 1`.
8. **Живой прогон**: `make import STAT=weekly_cpi_sa` — 209 + 1 nowcast, повтор no-op; `v_weekly_cpi_sa` 630 строк; `v_key_rate_dashboard` — `weekly_cpi_saar` с as_of последней недели; сравнение с `base_ipc_saar` (2026-09 5,9145, публикация 14.10.2026).
9. **Документация**: `ARCHITECTURE.md` §6.2/6.3 (колонки/строка), `README.md`, `docs/IMPORTERS.md`, `docs/ROADMAP_KEY_RATE.md` (статус), `docs/LIMITATIONS.md` (nowcast-статус).
10. **PR**: `Closes #N`; спека/план, причина (leading vs backtest), parity 209, значения nowcast, dashboard; протокол — комментарием в PR.

## Границы

Raw-таблицы, `ppi_sa`, панели 36–39 и гранты не меняются. Nowcast — `partial=1`, индикативный; 2022+ alone p=0,1896 публикуется. Оценка — 1–2 person-days.
