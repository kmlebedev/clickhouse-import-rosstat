# Недельный SA-агрегат ИПЦ: Go-импортёр `weekly_cpi_sa` — design

**Дата:** 2026-10-22. **Пункт:** `key-rate/phase-4-weekly-sa-aggregate` (issue #353, `role:go`). **База:** research `key-rate/phase-4-weekly-core-proxy` ([#348](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/348), PR [#350](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/350), merged `e0c5930`). **Ветка:** `feat/key-rate-weekly-sa-aggregate` (worktree `.worktrees/key-rate-weekly-sa-aggregate`, база — `origin/main` `e0c5930`).
**Приёмка (кратко):** `make import STAT=weekly_cpi_sa` — 209 строк 2009-05 … 2026-09; повтор — no-op; parity с research-JSON (4 spot-месяца, допуск 0,01 п.п.); `make all`, `make docs-check`, `make mcp-check`, `dagu validate` — зелёные.

## 1. Пробел

Research #348 показал: X-13ARIMA-SEATS на месячной сетке 4-нед. накопленного индекса официального агрегата ИПЦ — единственный метод, прошедший предрегистрированный критерий as-of на длинной истории: MAE **2,67** против **3,52** у сырой 4-нед. средней, DM p = **0,0216** (n = 171, 2012-07 … 2026-09); сегмент BI 2019+ — p = **0,0433**. На 2022+ alone p = 0,1896 — это ограничение, а не отмена результата.

Сейчас ряд живёт только в Python-артефакте `scripts/models/cpi/weekly_core_proxy.py` (JSON, в ClickHouse не пишет). Нужен Go-импортёр, который воспроизводит ряд в ClickHouse и отдаёт его агенту через витрину.

## 2. Источник и правило слияния

| Источник | Что берём | Период |
|---|---|---|
| `rosstat_ipc_weeks_bulletins` | агрегатный ряд `series = 'Индекс потребительских цен (недельная оценка)'` | 2009-04-06 … 2021-12-27 (HTML-бюллетени «Оперативной информации»; в XLSX `rosstat_ipc_weeks` агрегатной строки нет) |
| `rosstat_bi_cpi_weekly` | та же серия из BI | 2019-01-09 … 2026-10-12 |

На пересечении 2019–2021 **BI приоритетен** — ровно как в `v_rosstat_macro` (иначе ряд разошёлся бы с панелью 36). Union после приоритета — **760 точек 2009-04-06 … 2026-10-12** (снимок 2026-10-22). Проверки входа: первый день 2009-04-06, **не менее 760** точек (порог, а не точное число: следующая недельная публикация добавит 761-ю и не должна валить импорт), строгая возрастаемость, положительные конечные значения. Дубли дат **внутри одного источника** — ошибка; пересечение источников 2019–2021 — штатное и разрешается BI-приоритетом. Сырые таблицы не меняются; импортёр только читает.

## 3. Алгоритм

1. Слить строки, отсортировать по дате, проверить строгую возрастаемость.
2. `g_t = ln(value/100)`, кумулятивный лог-уровень `L_t` (первая точка = 0).
3. Регулярная 7-дневная сетка от первой до последней даты; внутри интервала — равномерный прирост по дням. 4-нед. индекс: `I4_t = exp(L_t − L_{t−4}) × 100` — ровно 28 дней, первая точка +28 дней от начала.
4. Месячная сетка: `last_in_month` (последняя точка сетки в месяце), дата точки — конец месяца; месяц попадает в ряд, только если `lastGridDate >= monthEnd(month)` — текущий неполный месяц отбрасывается. Снимок 2026-10-22: **209 полных месяцев** 2009-05 … 2026-09 (порог `>= 209`, ряд растёт с каждым закрытым месяцем).
5. Вход R: CSV `date,level` (конец месяца, I4). Встроенный R-скрипт (по образцу `ppi/data/ppi_sa_x13.R`): `ts(level, start = c(year, month), frequency = 12)`, `seas(..., transform.function="log", regression.aictest="td", outlier="")`, выход `date,sa_level`.
6. `sa_index = sa_level`; `sa_4w = (sa_index/100 − 1) × 100`; `saar = ((sa_index/100)^13 − 1) × 100`.
7. Батч `PrepareBatch` → `Append` → `Send`; порядок строк детерминирован (дата).

## 4. Схема

```sql
CREATE TABLE IF NOT EXISTS weekly_cpi_sa (
    date           Date
  , series         LowCardinality(String)
  , method         LowCardinality(String)
  , spec_version   LowCardinality(String)
  , sa_index       Float64
  , sa_4w          Float64
  , saar           Float64
  , published_at   Nullable(Date)
  , loaded_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (date, series, method, spec_version);
```

- `series = 'weekly_cpi'`, `method = 'x13arima-seats'`, `spec_version = 'weekly-cpi-sa-2026-v1'`.
- `sa_index` — 4-нед. SA-индекс (100 = без изменения); `sa_4w` — % за 4 недели; `saar` — % годовых (`^13`).
- `published_at` — `first_seen` входного винтажа из `source_publications` (как в `ppi_sa`), не официальная дата Росстата.
- `taxonomy_version` не нужен: mapping нет; правка R-скрипта/формулы — bump `spec_version`, SHA скрипта пинится тестом.

## 5. Каталог, витрина, грант

- `series_catalog`, `source = 'weekly_cpi_sa'`, три ряда: `weekly_cpi:sa_index`, `weekly_cpi:sa_4w`, `weekly_cpi:saar` — `title`, `unit`, `frequency = 'M'`, `origin = 'derived: rosstat_ipc_weeks_bulletins + rosstat_bi_cpi_weekly, X-13ARIMA-SEATS seats_log_cal, 4-нед. индекс 28 дней'`, `description` с оговоркой 2009–2018 (бюллетени, grid-интерполяция) и 2022+ (p = 0,1896 — индикативно).
- Витрина `v_weekly_cpi_sa` — long-форма `(source, series, date, value, unit, frequency)` как `v_rosstat_ppi`; `source='weekly_cpi_sa'`.
- `GRANT SELECT ON default.v_weekly_cpi_sa TO kimi_reader;` — `sql/mcp_kimi_reader.sql`; проверка в `scripts/mcp_check.py` (3 ряда × 209 точек).
- `series_catalog` и витрину публикует сам импортёр (как `ppi_sa`), не отдельный шаг.

## 6. Идемпотентность

- Отпечаток: SHA-256 по (дата, значение) слитого ряда + SHA-256 встроенного R-скрипта + `spec_version`; запись в `source_publications` с `source = 'weekly_cpi_sa'`, `url = 'derived:rosstat_ipc_weeks_bulletins+rosstat_bi_cpi_weekly'` — **после** успешной записи точек и публикации каталога/витрины.
- Повтор: если версия известна и `SELECT count() FROM weekly_cpi_sa FINAL` = сохранённому числу точек — no-op без R; иначе полный прогон (как `ppi_sa`).
- Пины: `weeklysa/data/weekly_cpi_sa_x13.R` + SHA-256 в Go; правка скрипта без bump `spec_version` валит тест.

## 7. DAG

`dagu/ppi.yaml` — второй шаг после `ppi_sa` (четверг 14:17, тот же `R_LIBS_USER`/`seasonal`; `CLICKHOUSE_IMPORT_STAT=weekly_cpi_sa`, retry 3 × 300 с). Падение шага не лишает данных сырые импортёры и `ppi_sa`; повтор при неизменных входах — no-op. DAG общий с пакетом `ppi`, поэтому тест-энфорсер `ppi` проверяет только шаги с префиксом `ppi_`, а энфорсер `weeklysa` — свой шаг; это осознанное отступление от §8.3(г) стандарта для общего DAG. `Rscript`/`seasonal` отсутствуют → импорт падает с явной ошибкой; офлайн-тесты Go используют stub-runner, R в CI не нужен.

## 8. Тесты и приёмка

**Go-тесты (офлайн):** слияние (BI приоритет, дубль внутри источника — ошибка, пересечение — нет), первый день 2009-04-06 и порог `>= 760`, строгая возрастаемость, grid-интерполяция 14-дневного разрыва, 4-нед. индекс (28 дней), `last_in_month` + отбрасывание неполного месяца + непрерывность и порог `>= 209`, CSV-формат, разбор SA, `sa_4w`/`saar`, отпечаток и решение no-op, DDL/витрина/каталог.

**Живая приёмка:**
- `make import STAT=weekly_cpi_sa` — **209 строк**, `min(date)=2009-05-31`, `max(date)=2026-09-30`; повтор — `Imported 0 rows` / no-op, `count() FINAL` не меняется.
- Parity с `docs/research/2026-10-22-weekly-core-proxy.json`, `x13_monthly_extended.full_sample` (in-sample, весь ряд): 2012-07 **5,1813**, 2019-01 **7,8172**, 2022-03 **142,0173**, 2026-09 **5,8505** — допуск **0,01 п.п.** (те же входы, тот же `seats_log_cal`).
- `v_weekly_cpi_sa` — 3 × 209 строк; `v_series_catalog` — 3 ряда `source='weekly_cpi_sa'`; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
- `make all`, `make docs-check`, `dagu validate dagu/ppi.yaml` — зелёные; протокол — комментарием в PR.

## 9. Границы

- Не меняются `v_rosstat_macro`, `rosstat_ipc_weeks`, `rosstat_ipc_weeks_bulletins`, `rosstat_bi_cpi_weekly`, `ppi_sa`, панели 36–39, существующие гранты.
- Новые объекты — только `weekly_cpi_sa`, `v_weekly_cpi_sa`, 3 записи `series_catalog`, `GRANT SELECT`, шаг `dagu/ppi.yaml`.
- Ряд производный и не заменяет официальный агрегат: `series` в витрине — `weekly_cpi`, `origin` — derived.
- 2009–2018 — бюллетени с 24–51 датой/год, grid-интерполяция; 2022+ alone в research не значим (p = 0,1896) — это ограничение публикуется в `description`, а не замалчивается.
