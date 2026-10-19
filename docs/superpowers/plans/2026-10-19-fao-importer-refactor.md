# План: рефакторинг пакета `fao` по стандарту импортёров

> **Для исполнителя:** пункт `infra/1.6`; отдельная ветка `refactor/fao-importer`, один PR. Спека — [2026-10-19-fao-importer-refactor-design.md](../specs/2026-10-19-fao-importer-refactor-design.md). WIP=1: не начинать, пока research-пункт `key-rate/phase-4-cpi-extra-channels` не смержен или владелец не разрешил stacked-ветку.

**Цель:** привести `fao/` к стандарту пакета импортёров: `util.ClickHouseImport`, батч-вставка, каталог, витрина, грант, реестр, тесты, фикстуры; исправить потерю ряда Sugar; сохранить имя таблицы и контракт моделей.

**Архитектура:** один импортёр `fao_food_price_Indices_monthly`; `Import()` вычисляет ссылку со страницы ФАО через `util.GetPage`/`util.FindLink`; `BodyFunc` разбирает CSV из байтов; `AfterImport` публикует 6 записей `series_catalog` и витрину `v_fao_food_price`.

**Технологии:** Go, `util.ClickHouseImport`, `encoding/csv`, `excelize` не нужен, ClickHouse `ReplacingMergeTree`, Dagu, MCP `kimi_reader`.

---

## Task 1. Спека и план

**Files:**
- Create: `docs/superpowers/specs/2026-10-19-fao-importer-refactor-design.md`
- Create: `docs/superpowers/plans/2026-10-19-fao-importer-refactor.md`

- [x] Спека и план созданы; коммит — первым.

---

## Task 2. Скелет пакета и реестр

**Files:**
- Create: `fao/importers.go`
- Modify: `fao/food_price_indices_mes.go`
- Test: `fao/importers_test.go`

**Step 1.** Объявить `faoImporter{table, countPoints}`, `faoImporters` с литералом `"fao_food_price_Indices_monthly"`, единственный `init()`.
**Step 2.** Переименовать пакет в `fao`; `foodPriceImporter` — обёртка с `util.ClickHouseImport`.
**Step 3.** `Name()` = `TableName` = `fao_food_price_Indices_monthly`.
**Step 4.** Тест-энфорсер: префикс `fao_`, `Name()==Table`, уникальность/регистрация, шаг `dagu/fao.yaml` в обе стороны.
**Step 5.** `go test ./fao/`.

---

## Task 3. Импортёр и разбор CSV

**Files:**
- Modify: `fao/food_price_indices_mes.go`
- Create: `fao/food_price_indices_mes_parse.go`

**Step 1.** Константы: `faoUrl`, `faoLinkSelector`, `faoFoodPriceTable`, DDL в `%s`-форме, `faoSeriesByHeader`.
**Step 2.** `Import()`: `GetPage` → `FindLink` → абсолютный/относительный href → `ClickHouseImport.Import`.
**Step 3.** `parseFaoFoodPrice(body []byte, batch driver.Batch) error`: csv с запятой, шапка по `Date`, 6 колонок, дата +1 месяц, пропуск пустых/`-`, ошибки с контекстом.
**Step 4.** Нет `colly`, `conn.Exec` в цикле, `fmt.Print*`; логи — logrus.
**Step 5.** `go test ./fao/`.

---

## Task 4. Каталог и витрина

**Files:**
- Create: `fao/series_meta.go`
- Create: `fao/views.go`
- Modify: `fao/food_price_indices_mes.go` (`AfterImport: publishFao`)

**Step 1.** 6 `util.SeriesMeta` с `source='fao'`, unit/frequency/origin/description; дата = публикация, значение за M−1, номинал, real-ряда нет.
**Step 2.** `v_fao_food_price`: `source, series, date, period_date, value`; комментарии.
**Step 3.** `publishFao` = `UpsertSeriesCatalog` + `CreateView`.
**Step 4.** Тест каталога: parsed ⊆ described и described ⊆ parsed.

---

## Task 5. Фикстуры и тесты

**Files:**
- Create: `fao/testdata/food_price_indices_data.csv`
- Create: `fao/testdata/foodpricesindex_ru.html`
- Create: `fao/food_price_indices_mes_test.go`
- Create: `fao/series_meta_test.go`
- Modify: `fao/importers_test.go`

**Step 1.** Снять реальный CSV, урезать до шапки + первых 5–6 строк + последних 3–5 (66 колонок, пустая строка сохранены).
**Step 2.** HTML-фикстура с реальной ссылкой на CSV.
**Step 3.** Тесты: 6 рядов, spot-значения, Sugar, сдвиг даты, fail-fast.
**Step 4.** Локальный `appendRecorder`; без сети, без БД.
**Step 5.** `go test -race -count=1 ./fao/`.

---

## Task 6. Расписание, грант, MCP

**Files:**
- Modify: `dagu/fao.yaml`
- Modify: `sql/mcp_kimi_reader.sql`
- Modify: `scripts/mcp_check.py`

**Step 1.** DAG: `id`/`CLICKHOUSE_IMPORT_STAT` = `fao_food_price_Indices_monthly`; расписание не менять.
**Step 2.** Грант `v_fao_food_price`.
**Step 3.** MCP-check: витрина непуста, 6 рядов/2646 точек, каталог 6, сырая таблица 497.
**Step 4.** `dagu validate dagu/fao.yaml`.

---

## Task 7. Документация и роадмап

**Files:**
- Modify: `ARCHITECTURE.md` (§2, §6.1, §6.2, §6.3, §7)
- Modify: `README.md`
- Modify: `docs/IMPORTERS.md`
- Modify: `docs/LIMITATIONS.md`
- Modify: `docs/ROADMAP_INFRA.md` (пункт `1.6`, статус)
- Create: issue `[infra/1.6] ...` (role:go)

**Step 1.** Описать таблицу, витрину, семантику даты, отсутствие real-индекса/PIT, Sugar.
**Step 2.** `make docs-check`.

---

## Task 8. Проверка и живая приёмка

```bash
make all
go test -race -count=1 ./fao/
make import STAT=fao_food_price_Indices_monthly
make mcp-check
dagu validate dagu/fao.yaml
```

**Ожидаемо:**
- `Imported 2646 rows`; `FINAL` 2646; 6 рядов; `1990-02-01 … 2026-10-01`;
- `v_fao_food_price` 2646; `v_series_catalog` source='fao' 6;
- повтор — идемпотентен (2646, не no-op);
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; сырая таблица — 497.

**Протокол живой приёмки** — комментарием в PR: команды, числа, крайние даты, повтор, mcp-check, dagu validate.

---

## Out of scope

- PIT/винтажный архив, `Float64`-миграция, lowercase-rename, удаление пустой `_Monthly_Real`.
- Починка консолидированного бюджета Минфина, ML-эксперименты, новые импортёры.
