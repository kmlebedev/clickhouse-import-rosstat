# План: Go-импортёр `weekly_cpi_sa` (пункт `key-rate/phase-4-weekly-sa-aggregate`)

**Дата:** 2026-10-22. **Спека:** [2026-10-22-weekly-sa-aggregate-design.md](../specs/2026-10-22-weekly-sa-aggregate-design.md). **Issue:** #353. **База:** `origin/main` `e0c5930` (research #348 / PR [#350](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/350) merged). **Ветка:** `feat/key-rate-weekly-sa-aggregate`.

## Шаги

1. **Пакет `weeklysa/`** (новый, по [стандарту](../IMPORTER_PACKAGE_STANDARD.md) §14):
   - `schema.go` — DDL `weekly_cpi_sa` (колонки и `ORDER BY` из спеки §4), константы `series/method/spec_version`, `countPoints = "SELECT count() FROM weekly_cpi_sa FINAL"`, SHA-256 встроенного R-скрипта.
   - `data/weekly_cpi_sa_x13.R` — embedded: вход `date,level` (месячный 4-нед. индекс), `ts(frequency=12)`, `seats_log_cal` (`transform.function="log"`, `regression.aictest="td"`, `outlier=""`), выход `date,sa_level`.
   - `x13.go` — runner с точкой подмены в тестах. Приоритет — вынести общий мост из `ppi/x13.go` в `util` (минимальная правка `ppi`), иначе — копия с тем же `rEnvironment`/таймаутом 15 мин/`WaitDelay`; секреты в R не передаются.
   - `importer.go` — слияние `rosstat_ipc_weeks` + `rosstat_bi_cpi_weekly` (BI приоритет 2019–2021), проверка 760 точек и строгой возрастаемости; 7-дневная сетка, 4-нед. индекс 28 дней, `last_in_month` (209 месяцев); CSV → X-13 → `sa_index`/`sa_4w`/`saar`; отпечаток входов + SHA скрипта + `spec_version`; no-op через `source_publications`/`PublicationKnown`; батч `PrepareBatch → Append → Send`; публикация каталога и витрины после записи.
   - `series_meta.go` — 3 ряда `source='weekly_cpi_sa'`: `weekly_cpi:sa_index`, `weekly_cpi:sa_4w`, `weekly_cpi:saar` с `title/unit/frequency/origin/description` (оговорки 2009–2018 и 2022+).
   - `views.go` — `v_weekly_cpi_sa` (long: `source, series, date, value, unit, frequency`).
   - `importers.go` — регистрация в `chimport.Stats` (`init()`), порядок как в `dagu/ppi.yaml`.

2. **`main.go`** — blank-import нового пакета рядом с `ppi`.

3. **MCP:** `sql/mcp_kimi_reader.sql` — `GRANT SELECT ON default.v_weekly_cpi_sa TO kimi_reader`; `scripts/mcp_check.py` — проверка 3 ряда × 209 точек.

4. **DAG:** `dagu/ppi.yaml` — шаг `weekly_cpi_sa` после `ppi_sa` (четверг 14:17), `CLICKHOUSE_IMPORT_STAT=weekly_cpi_sa`, retry 3 × 300 с, тот же `R_LIBS_USER`/`seasonal`.

5. **Тесты `weeklysa/` (офлайн, без R):**
   - слияние: BI приоритетнее на 2019–2021, 760 точек, дубль даты → ошибка;
   - сетка: 14-дневный разрыв → равномерный прирост, 4-нед. индекс ровно 28 дней;
   - месячная сетка: 209 ключей 2009-05 … 2026-09, `last_in_month`;
   - CSV/разбор SA, `sa_4w`/`saar` (ручной расчёт);
   - отпечаток, no-op на stub-runner, повтор не зовёт R;
   - DDL/витрина/каталог — литералы SQL и ключи.

6. **Живой прогон:**
   - `make import STAT=weekly_cpi_sa` — 209 строк, `min(date)=2009-05-31`, `max(date)=2026-09-30`;
   - повтор — `Imported 0 rows` / no-op, `count() FINAL` не меняется;
   - parity с `docs/research/2026-10-22-weekly-core-proxy.json`, `x13_monthly_extended.full_sample`: 2012-07 **5,1813**, 2019-01 **7,8172**, 2022-03 **142,0173**, 2026-09 **5,8505** — допуск 0,01 п.п.;
   - `v_weekly_cpi_sa` — 3 × 209 строк, каталог — 3 ряда, `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

7. **Документация тем же PR:** `ARCHITECTURE.md` §6.2 (таблица) и §6.3 (витрина/грант), `README.md`, `docs/IMPORTERS.md`, `docs/ROADMAP_KEY_RATE.md` — статус пункта; `docs/GRAFANA.md` не меняется (панелей нет).

8. **Проверки:** `go test ./weeklysa/...`, `make all`, `make docs-check`, `make mcp-check`, `dagu validate dagu/ppi.yaml` — зелёные.

9. **PR:** `Closes #N` вне backticks; пункт, спека/план, источник, схема, parity-числа, команды; протокол живой приёмки — комментарием в PR (стандарт §16.1 п. 8).

## Границы

`v_rosstat_macro`, `rosstat_ipc_weeks`, `rosstat_bi_cpi_weekly`, `ppi_sa`, панели 36–39 и существующие гранты не меняются. Новые объекты — `weekly_cpi_sa`, `v_weekly_cpi_sa`, 3 записи каталога, грант, шаг DAG. Ряд производный: `series='weekly_cpi'`, `origin` — derived; 2022+ alone в research не значим (p = 0,1896), это публикуется в описании.
