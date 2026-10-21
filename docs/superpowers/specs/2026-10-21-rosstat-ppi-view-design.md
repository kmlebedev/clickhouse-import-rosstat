# T5: витрина `v_rosstat_ppi`, каталог и грант MCP

**Пункт:** `key-rate/phase-4-rosstat-ppi-sa` T5, issue [#280](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/280), parent [#266](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/266).
**Дата:** 21 октября 2026 г.
**Роль:** `role:go`.
**Зависимости:** T1 #276 (`rosstat_ppi_ved`), T2 #277 (`rosstat_ppi_product`, `rosstat_ppi_weights`), T4 #279 (`ppi_sa`) — закрыты/смержены.
**Спека T4:** [2026-10-21-ppi-sa-design.md](2026-10-21-ppi-sa-design.md); [план T5](2026-10-21-rosstat-ppi-view.md).

## 1. Контекст и границы

T1/T2 дали сырые ИЦП Росстата по ВЭД (`rosstat_ppi_ved`, 571 точка), товарам/ОКПД2 (`rosstat_ppi_product`, 336 512 точек) и веса по ВЭД (`rosstat_ppi_weights`, 758 точек); T4 — производный SA-слой `ppi_sa` (436 строк × 4 метрики). T5 публикует единую long-витрину `v_rosstat_ppi`, каталог метрик и грант `kimi_reader`.

**В объёме:** `v_rosstat_ppi` (`source, series, date, value, unit, frequency`), комментарии таблицы и каждой колонки, 20 новых записей `series_catalog` для метрик `ppi_sa`, `GRANT SELECT`, проверки `make mcp-check`, синхронизация `ARCHITECTURE.md`/`README.md`/`IMPORTERS.md`/`LIMITATIONS.md`/роадмапа.

**Вне объёма:** новые данные, DDL сырых таблиц, расписание (T6 #281), materialized view, PIT/винтажи.

## 2. Витрина

```sql
CREATE OR REPLACE VIEW v_rosstat_ppi DEFINER = default SQL SECURITY DEFINER AS
SELECT 'rosstat' AS source, concat('ИЦП по ВЭД, РФ, ', index_type) AS series, date,
       toNullable(toFloat64(value)) AS value, <unit по index_type> AS unit, 'M' AS frequency
FROM rosstat_ppi_ved FINAL
UNION ALL ... rosstat_ppi_product (okpd2:<код>:<тип>) ...
UNION ALL ... rosstat_ppi_weights (ved-weight:<код>:<база>, makeDate(year,1,1), unit '% (доля в структуре весов ИЦП)', 'A') ...
UNION ALL ... ppi_sa × 4 метрики (ppi_sa:<ряд>:<метрика>) ...
```

Колонки и семантика:

| Колонка | Тип | Смысл |
|---|---|---|
| `source` | String | `rosstat` — сырые ИЦП/веса; `ppi_sa` — производный SA-слой; совпадает с `v_series_catalog.source` |
| `series` | String | точное имя ряда из `v_series_catalog`: `ИЦП по ВЭД, РФ, <m/m\|ytd\|y/y\|period>`, `okpd2:<код>:<тип>`, `ved-weight:<код>:<база>`, `ppi_sa:<total\|food\|nonfood\|all\|core>:<sa\|mm_sa\|saar\|saar3>` |
| `date` | Date | дата точки; у весов — 1 января года; у `ppi_sa` — последний день отчётного месяца |
| `value` | Nullable(Float64) | значение; у `ppi_sa` NULL на краях (`mm_sa`/`saar` первого месяца, `saar3` первых двух) |
| `unit` | String | единица; для сырых — точная по `index_type`, для `ppi_sa` — индекс для `sa` и `%` для метрик; полное описание — в `v_series_catalog` |
| `frequency` | String | `M` — месяц, `A` — год (веса) |

**Ряды.** VED — 4 (по типу индекса, `concat('ИЦП по ВЭД, РФ, ', index_type)`); product — 5 308 (`okpd2:<код>:<тип>`, 1 327 кодов × 4); weights — 76 (`ved-weight:<код>:<база>`, 38 × 2); derived — 20 (5 рядов × 4 метрики). Итого **5 408 рядов**.

**Строки (проверено прототипом 2026-10-21):** 571 + 336 512 + 758 + 1 744 = **339 585**; `source='rosstat'` — 337 841, `source='ppi_sa'` — 1 744; `value IS NULL` — **20**.

**Почему `source` — `rosstat`/`ppi_sa`, а не имя таблицы.** Агент ищет описание ряда по `(source, series)` в `v_series_catalog`; каталог T1/T2 уже использует `source='rosstat'`, T4 — `source='ppi_sa'`. Совпадение ключей делает витрину и каталог одним контрактом.

**Почему `value` Nullable.** `ppi_sa` честно хранит NULL на краях; витрина не превращает их в 0. Сырые значения оборачиваются в `toNullable`, чтобы тип `UNION ALL` был `Nullable(Float64)`.

## 3. Публикация

Витрину публикует импортёр `ppi_sa` (последний писатель: сырые таблицы уже существуют, `ppi_sa` создан). Функция `publishPPIDerived` в `ppi/importer.go`:

1. `util.UpsertSeriesCatalog` — 5 базовых рядов `ppi_sa` + 20 рядов метрик витрины;
2. `util.CreateView` — `v_rosstat_ppi` с комментариями; если хотя бы одной из четырёх таблиц нет, витрина пропускается (на практике `ppi_sa` уже читает входы и падает раньше).

Вызывается и на полном прогоне (после батча), и на no-op-пути — витрина и каталог self-heal при повторном прогоне. Отдельный `views`-импортёр и шаг dagu не заводятся: T5 — витрина над уже собранными таблицами, а не новый источник.

## 4. Каталог

К базовым 5 записям `ppi_sa` (`total`, `food`, `nonfood`, `all`, `core`) добавляются **20** записей `ppi_sa:<ряд>:<метрика>`:

| Метрика | `unit` | `title` (суффикс) |
|---|---|---|
| `sa` | `индекс (100 = первый месяц ряда)` | `SA-уровень` |
| `mm_sa` | `%` | `м/м SA` |
| `saar` | `%` | `SAAR` |
| `saar3` | `%` | `3m SAAR` |

`frequency='M'`, `origin` — из базового ряда (таблица + X-13 + версии), `description` — базовое описание + смысл метрики и её NULL-края. Итог `v_series_catalog` по `source='ppi_sa'` — **25** записей; `source='rosstat'` — 5 553 (169 базовых + 5 308 товарных + 76 весовых).

## 5. Грант и MCP-проверки

- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_rosstat_ppi TO kimi_reader;`; сырые `rosstat_ppi_ved`, `rosstat_ppi_product`, `rosstat_ppi_weights`, `ppi_sa` агенту не выдаются.
- `make mcp-user` (пересоздаёт гранты) и `make mcp-check`; в `scripts/mcp_check.py` — блок PPI: витрина видна, 339 585 строк / 5 408 рядов / 20 NULL, каталог `ppi_sa` 25, spot `ppi_sa:core:saar3` 2026-08-31 = 6,0788, четыре сырые таблицы закрыты (497).
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

## 6. Комментарии

`util.View` задаёт комментарий таблицы и каждой колонки. Комментарий объясняет: `source`/`series` совпадают с каталогом; `value IS NULL` — край `ppi_sa`; полная единица/частота — в `v_series_catalog`; сырые таблицы не выданы; витрина не materialized и читает сырые таблицы как `DEFINER`.

## 7. Пример запроса

```sql
SELECT date, value
FROM v_rosstat_ppi
WHERE source = 'ppi_sa' AND series = 'ppi_sa:core:saar3'
ORDER BY date DESC
LIMIT 12;
```

Ожидаемо: последняя точка 2026-08-31, `value` = 6,0788; 12 строк — последние 12 месяцев.

## 8. Тесты и приёмка

- Go: `ppi/views_test.go` — имя/таблицы/колонки/SQL-фрагменты; `ppiSAViewSeriesMeta` — 20 записей, имена `ppi_sa:<ряд>:<метрика>`, unit/frequency; базовые 5 не изменились; view SQL содержит 4 таблицы и 6 колонок.
- `make all`, `make docs-check`, `make mcp-check` — зелёные; живой прогон `make import STAT=ppi_sa` (no-op) создаёт витрину/каталог; `make mcp-user` применяет грант.
- В PR — пример запроса и ожидаемое число строк (339 585 / 5 408 / 20).

## 9. Риски

| Риск | Митигация |
|---|---|
| Витрина читает 336 512 товарных точек при запросе без фильтра | long-форма; агенту — фильтр по `source`/`series`; в комментарии оговорка |
| `CreateView` вызывается на no-op-пути | идемпотентно; ошибка публикации не пишет строку версии |
| Расхождение имён с каталогом | имена строятся теми же `concat`-конвенциями, что T1/T2/T4; тест каталога |
| NULL в `value` | тип Nullable; комментарий и 20 NULL в MCP-проверке |
| Грант не применён | `make mcp-user`; `make mcp-check` падает, если витрина не видна |

## 10. Ссылки

- [Спека T4](2026-10-21-ppi-sa-design.md), [план T5](2026-10-21-rosstat-ppi-view.md).
- [ARCHITECTURE.md §6.3](../../../ARCHITECTURE.md), [IMPORTER_PACKAGE_STANDARD.md §10](../IMPORTER_PACKAGE_STANDARD.md).
- [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md).
