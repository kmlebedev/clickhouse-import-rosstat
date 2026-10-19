# Пакет `fao`: рефакторинг по стандарту импортёров — дизайн (2026-10-19)

**Пункт роадмапа:** `infra/1.6` — рефакторинг пакета `fao` по стандарту импортёров.
**Стандарт:** [docs/IMPORTER_PACKAGE_STANDARD.md](../IMPORTER_PACKAGE_STANDARD.md) §4, §5, §6, §8, §10, §11, §13, §15.
**Роль:** Go-инженер.

---

## 1. Границы

- Меняется только пакет `fao/` и его производные (каталог, витрина, грант, DAG, тесты, документация).
- Имя физической таблицы `fao_food_price_Indices_monthly` **сохраняется**: это контракт `scripts/cpi_nowcast.py` и `scripts/cpi_monthly.py`.
- Данные не переименовываются и не переливаются. Единственное изменение набора рядов — названное исправление дефекта: добавляется `Sugar` (441 точка).
- Пустая legacy-таблица `fao_food_price_Indices_Monthly_Real` не удаляется (destructive DDL — отдельный пункт); новый код её не создаёт.
- PIT/винтажный архив, `Float64`-миграция и lowercase-переименование таблицы — вне объёма.

## 2. Текущее состояние и дефекты

`fao/food_price_indices_mes.go` (104 строки):

| Дефект | Следствие |
|---|---|
| `package rosstat` в пакете `fao` | семантическая путаница; пакет не следует стандарту §3 |
| `colly` + `getDataUrl` | нарушение §4 (HTTP только через `util`), сетевой вызов в обход шаблона |
| `conn.Exec` в цикле | нарушение правила 2 AGENTS.md и §5.3 |
| legacy-структура | нет `util.ClickHouseImport`, батча и производных (нарушение §5.1–5.6) |
| `cols[1:6]` | **Sugar не импортируется**: 5 рядов из 6 |
| `_Monthly_Real` в DDL | пустая таблица без источника (в CSV real-блока нет) |
| нет `series_catalog` | нарушение правила 11 AGENTS.md и §10.1 |
| нет витрины/гранта | MCP-агент не видит ряд; `kimi_reader` получает 497 |
| нет фикстур/тестов/реестра | нарушение §8, §11 |
| `faoFoodPriceCSVDataUrl` не используется | мёртвая константа; старый `_sep25.csv` — 404 |
| `Name() = "fao_food_price"` ≠ таблица | нарушение §7.2 |

Живой источник (2026-10-19): страница `https://www.fao.org/worldfoodsituation/foodpricesindex/ru/` (200) → CSV `food_price_indices_data.csv?sfvrsn=…` (200, `text/csv`, 48 110 Б), 445 строк, 441 месячное наблюдение 1990-01…2026-09, 6 рядов.

## 3. Целевое устройство

### 3.1. Файлы

| Файл | Роль |
|---|---|
| `fao/food_price_indices_mes.go` | импортёр-обёртка + `util.ClickHouseImport`; `Import()` вычисляет `DataUrl` |
| `fao/food_price_indices_mes_parse.go` | разбор CSV из байтов (`BodyFunc`), без сети |
| `fao/series_meta.go` | 6 `util.SeriesMeta` |
| `fao/views.go` | витрина `v_fao_food_price` + `publishFao` |
| `fao/importers.go` | реестр из одной записи, единственный `init()` |
| `fao/testdata/` | реальные урезанные CSV и HTML |
| `fao/*_test.go` | разбор, каталог, реестр |

### 3.2. Импортёр

```go
var faoFoodPrice = util.ClickHouseImport{
    TableName: "fao_food_price_Indices_monthly",
    CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
          name LowCardinality(String)
        , date Date
        , price_index Float32
    ) ENGINE = ReplacingMergeTree ORDER BY (name, date);`},
    BodyFunc:    parseFaoFoodPrice,
    AfterImport: publishFao,
}
```

`Import()`:

1. `util.GetPage(faoUrl)` — страница.
2. `util.FindLink(page, faoLinkSelector)` — ссылка на CSV; селектор-константа `a[href*="food_price_indices_data.csv"]`.
3. Если href абсолютный — использовать как есть; если относительный — присоединить к `https://www.fao.org`.
4. `s.ClickHouseImport.Import(ctx, conn)` — полный цикл шаблона: DDL → `BodyFunc` → `AfterImport`.

Точка контроля публикации **не объявляется**: файл обновляется раз в месяц, шаг DAG — раз в месяц (§9.3). Повторный прогон идемпотентен за счёт `ReplacingMergeTree`, но не no-op. Винтажный архив/PIT — отдельный пункт.

### 3.3. Разбор

- `encoding/csv` с `Comma = ','`, `FieldsPerRecord = -1` (строки имеют 66 колонок, хвост пустой).
- Шапка ищется по якорю: первая ячейка `Date`; индексы колонок — из шапки, не магические.
- Карта стабильных имён (код, не заголовок источника):

| Колонка источника | `Series` |
|---|---|
| `Food Price Index` | `Food Price Index` |
| `Meat` | `Meat` |
| `Dairy` | `Dairy` |
| `Cereals` | `Cereals` |
| `Oils` | `Oils` |
| `Sugar` | `Sugar` |

- Дата: `time.Parse("2006-01", cell)` → первое число месяца; `AddDate(0, 1, 0)` → 1-е число месяца публикации.
- Пустая ячейка и `-` — не точка (§6.7); нечисло — ошибка с именем ряда/даты (§6.4).
- Нет шапки, нет ни одной точки, нет ссылки на странице — ошибка, не «0 rows».

### 3.4. Семантика данных

- `date` — 1-е число месяца **публикации**; `price_index` — значение за предыдущий месяц. Это же правило уже используют `cpi_nowcast.py`/`cpi_monthly.py` (as-of `date + 6`).
- `price_index` — номинальный индекс, `2014-2016 = 100`; real-индекса в CSV нет.
- `Float32` сохраняется ради совместимости; витрина отдаёт `Float64`.
- `Sugar` добавляется как названное исправление: 6 × 441 = 2646 точек вместо 5 × 441 = 2205.

### 3.5. Каталог и витрина

`series_meta.go`: 6 записей `source='fao'`, `unit='index, 2014-2016=100'`, `frequency='M'`, `origin` — CSV/колонка, `description` — дата публикации, значение за M−1, номинал, real-ряда нет, Sugar.

`views.go`:

```sql
CREATE OR REPLACE VIEW v_fao_food_price DEFINER = default SQL SECURITY DEFINER AS
SELECT 'fao' AS source,
       name AS series,
       date AS date,
       date - INTERVAL 1 MONTH AS period_date,
       toFloat64(price_index) AS value
FROM fao_food_price_Indices_monthly FINAL
```

Комментарии таблицы и каждой колонки — через `util.View`; `publishFao` = `UpsertSeriesCatalog` + `CreateView`.

### 3.6. Реестр и расписание

`fao/importers.go`:

```go
type faoImporter struct {
    chimport.ImportStat
    table       string
    countPoints string // "" — префлайта нет
}
var faoImporters = []faoImporter{
    {faoFoodPriceImporter, "fao_food_price_Indices_monthly", ""},
}
func init() { for _, imp := range faoImporters { chimport.Stats = append(chimport.Stats, imp) } }
```

`dagu/fao.yaml`: `id` и `CLICKHOUSE_IMPORT_STAT` → `fao_food_price_Indices_monthly`; расписание `47 10 5 * *` не меняется.

### 3.7. Грант и MCP

- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_fao_food_price TO kimi_reader;`
- `scripts/mcp_check.py`: витрина существует и непуста; 6 рядов, 2646 точек, `max(period_date) >= '2026-09-01'`; каталог `source='fao'` — 6; сырая таблица — 497.

## 4. Тесты и фикстуры

- `fao/testdata/food_price_indices_data.csv` — реальный урезанный CSV: шапка, пустая строка, первые 5–6 строк и последние 3–5 строк, 66 колонок сохранены.
- `fao/testdata/foodpricesindex_ru.html` — реальный фрагмент страницы со ссылкой на CSV.
- `fao/food_price_indices_mes_test.go`: 6 рядов; spot `1990-01 Food Price Index = 64.4` → `1990-02-01`; `Sugar = 87.9`; `2026-09 = 136.0` → `2026-10-01`; fail-fast (пустое тело, нет шапки, нечисло, нет ссылки).
- `fao/series_meta_test.go`: двусторонний контракт «разобрано ⊆ описано и описано ⊆ разобрано»; локальный `appendRecorder`.
- `fao/importers_test.go`: префикс `fao_`, `Name() == TableName`, уникальность/регистрация, шаг `dagu/fao.yaml` в обе стороны.

## 5. Документация

- `ARCHITECTURE.md` §2, §6.1, §6.2, §6.3, §7.
- `README.md`, `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`.
- `docs/ROADMAP_INFRA.md` — пункт `1.6` и статус.
- `docs/superpowers/plans/2026-10-19-fao-importer-refactor.md` — план и протокол.

## 6. Приёмка

```bash
make all
go test -race -count=1 ./fao/
make import STAT=fao_food_price_Indices_monthly
make mcp-check
dagu validate dagu/fao.yaml
```

Ожидаемо: `Imported 2646 rows`; `count() FINAL` = 2646; 6 рядов; `min(date)=1990-02-01`, `max(date)=2026-10-01`; `v_fao_food_price` = 2646; каталог = 6; повтор идемпотентен; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; сырая таблица — 497.

## 7. Риски

- Селектор ссылки/шапка CSV могут измениться — ошибка должна называть URL/колонку, а не давать 0 строк.
- `Sugar` меняет набор данных: это названное исправление, а не «данные не менялись».
- Mixed-case имя таблицы — legacy-исключение; переименование отдельным пунктом.
- Нет префлайта: повторный прогон — идемпотентный, не no-op.
