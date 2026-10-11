# WGC-импортёры (`wgc/`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
> Диспетч — по AGENTS.md: в каждый промпт coder-субагента вписывается блок про `mcp__jetbrains__*`/`mcp__clickhouse__*`; ревью каждой задачи — `model: "haiku"`; финальное ревью ветки — на самой способной модели; проверка — `superpowers:verification-before-completion`.

**Goal:** пакет `wgc/` из двух Go-импортёров — ETF Flows (20 рядов с историей с 2003) и золотые резервы ЦБ (POL/CZE/TUR/RUS/KAZ/UZB/CHN) — из вручную скачанных xlsx goldhub в `macro_series` (`source='wgc'`), с приоритетом в `v_gold_etf_flows` и новой витриной `v_cb_gold_dynamics`.

**Architecture:** файлы лежат в `wgc/data/` (в `.gitignore`; скачивание с gold.org возможно только залогиненным браузером — боту 403, проверено); импортёры находят самый свежий файл по mtime, парсят его офлайн-тестируемыми функциями (фикстуры — реальные файлы в `wgc/testdata/`), пишут одним батчем в `macro_series` и публикуют каталог через `util.UpsertSeriesCatalog`. Прецеденты: `eia/` (импортёр пишет в `macro_series` своим `Name()`), `polyus/data/` (локальный файл), `dcf_engine` (нет файла — warn + 0 rows, dagu не краснеет).

**Tech Stack:** Go 1.27, `excelize/v2`, `clickhouse-go/v2` (`PrepareBatch → Append → Send`), logrus, dagu.

**Spec:** `docs/superpowers/specs/2026-10-11-wgc-importers-design.md` (полный layout обоих форматов, контрольные значения, границы и оговорки данных).

## Global Constraints

- Вставка только батчами: `PrepareBatch` → `Append` (err проверяется) → `Send`; построчный `Exec` в цикле запрещён.
- Никаких сетевых вызовов в `init()`; сеть в этом пункте не используется вовсе (файлы локальные).
- Ошибки не проглатывать: каждый `strconv.ParseFloat`-аналог и `batch.Append` — с проверкой `err`.
- Fail-fast: неизвестный регион ETF, отсутствующая/дублирующаяся страна из словаря — ошибка импорта, а не пропуск.
- Отсутствие файла — `log.Warn` + `Imported 0 rows`, не ошибка (dagu не краснеет до первого скачивания).
- Каждый ряд описан в каталоге (правило 11a) — через импортёр, не сидом; каждая витрина — комментарии таблицы/колонок + `GRANT SELECT` в `sql/mcp_kimi_reader.sql`.
- Логирование — logrus; даты точек — конец отчётного месяца (конвенция проекта); значения — как в файле, расхождения (RUS, Turkey*) только документируются.
- Команды, требующие ClickHouse/env, — только через `make` (Makefile подключает `~/.config/rosstat/env`); секреты не печатать.
- `make all` (gofmt, golangci-lint, vet, `go test -race`, сборка) должен быть зелёным на каждом коммите задачи.
- Ветка `feat/wgc-importers`; тесты офлайн, без БД и сети, на фикстурах `wgc/testdata/`.

## Review Focus

1. **Пустая ячейка ≠ 0**: пропуск ячейки фонда — «нет данных», а не «ноль потока»; если у всех фондов региона пусто — точка региона не пишется (иначе занижение сумм). Тесты — T1 (`TestEtfBlankCellIsNotZero`).
2. **`world` только при полных четырёх регионах** — неполный мир не пишется. Тест — T1 (там же).
3. **Выбор свежего файла по mtime, не по имени**: `Nov2026` лексикографически меньше `Oct2026`. Тесты — T2/T4 (`TestFindLatestFilePicksNewestByMtime`).
4. **Turkey net vs gross**: `Turkey*` (gross, включая банки) не должен попасть в ряд TUR — август у них 18,91 против 2,89 т. Тест — T3 (pinned).
5. **serial → дата → конец месяца**: 46295 → 2026-09-30, 46264 → 2026-08-31 (файл holdings отдаёт 30-е), 46235/46265 в заголовках Changes → 2026-08-31. Тесты — T1/T3.
6. **Приоритет витрины**: `coalesce(maxIfOrNull(...))` в порядке `wgc > webbridge > manual`; `maxIf` запрещён (0 подменил бы отсутствие). Тест — T2 (обновление `TestGoldEtfFlowsViewSourcePriority`).

---

### Task 1: ETF-парсер (`wgc/parse.go`, `wgc/etf_parse.go`)

**Files:**
- Create: `wgc/parse.go` (общие хелперы), `wgc/etf_parse.go`, `wgc/etf_parse_test.go`
- Add: `wgc/testdata/ETF_Flows_2026-10-02_1401.xlsx` (фикстура; файл уже скопирован, коммитится)

**Interfaces (производит):**
- `type seriesPoint struct { Series string; Date time.Time; Value float64 }`
- `func monthEnd(t time.Time) time.Time` — последний день месяца t, UTC 00:00
- `func cellRaw(f *excelize.File, sheet string, col, row int) (string, error)` — `GetCellValue(..., excelize.Options{RawCellValue: true})`
- `func cellFloat(v string) (float64, bool, error)` — `""` → `(0,false,nil)`; мусор → ошибка
- `func cellDate(v string) (time.Time, error)` — serial через `excelize.ExcelDateToTime(serial, false)`
- `func sheetSize(f *excelize.File, sheet string) (maxCol, maxRow int, err error)` — длины по `f.GetRows(sheet)` (значения не используются)
- `var wgcEtfRegions = []string{"world", "north_america", "europe", "asia", "other"}`; `var wgcEtfMetrics = []string{"flow_usd_mln", "flow_tonnes", "holdings_tonnes", "aum_usd_bn"}`
- `func wgcEtfSeriesName(metric, region string) string` → `etf_<metric>_<region>`
- `func parseFundSheet(f *excelize.File, sheet, metric string) ([]seriesPoint, error)` — суммы фондов по региону + `world` для метрик `flow_usd_mln | flow_tonnes | holdings_tonnes`
- `func parseChartsAum(f *excelize.File) ([]seriesPoint, error)` — блок `Area chart - monthly holdings in US$` (значения USD → /1e9)
- `func parseEtfWorkbook(f *excelize.File) ([]seriesPoint, error)` — оркестратор: `Fund flows by month`, `Demand by month`, `Holdings by month` + `parseChartsAum`

Правила разбора (спека §3.1): регион фонда — строка 4, колонки с F; имя фонда — строка 6; данные — со строки 7, кол. A = serial даты; пустая ячейка фонда — вклад пропускается; точка региона пишется, если есть ≥1 значение; `world` — только если все четыре региона за месяц присутствуют; непустой регион вне словаря → ошибка; в `Charts Data` блок находится по заголовку в строке 1 (`Area chart - monthly holdings in US$`), колонки — `Date, North America, Europe, Asia, Other` в строке 2 блока.

- [ ] **Step 1: Написать падающие тесты** (значения с допуском `|got-want| ≤ 1e-6`, если не указано иное):

```go
// wgc/etf_parse_test.go (эскиз; имена и ожидания обязательны, структура — на усмотрение исполнителя)
func TestEtfMonthlyValuesPinned(t *testing.T) {
    // fixturePath = testdata/ETF_Flows_2026-10-02_1401.xlsx; parseEtfWorkbook
    // 2026-09-30: etf_flow_usd_mln_world ≈ 9952.21564139
    //             etf_holdings_tonnes_world ≈ 4255.66981354
    //             etf_aum_usd_bn_world ≈ 574.23391418
    //             flows по регионам:  3961.93761905 / 3630.56023891 / 2255.40929756 / 104.30848587
    //             holdings по регионам: 2115.88373719 / 1521.36429276 / 541.88460453 / 76.53717906
}
func TestEtfAumRegionValuesPinned(t *testing.T) { /* 2026-09-30: 284.088446499 / 204.275334589 / 75.594060865 / 10.276072228 */ }
func TestEtfSeriesCoverage(t *testing.T) {
    // ровно 20 имён; точек у etf_flow_usd_mln_world и etf_holdings_tonnes_world ≥ 283,
    // у etf_aum_usd_bn_world ≥ 284; первые даты: 2003-03-31 (фондовые листы) и 2003-02-28 (Charts)
}
func TestEtfChartsMatchFundSums(t *testing.T) {
    // для перекрывающихся месяцев (16 шт.): |Charts - сумма фондов| ≤ 1e-6
    // по flow_usd (блок «Bar chart - monthly flows in US$»), flow_tonnes (… tonnes)
    // и holdings (блок «Area chart - monthly holdings in tonnes») для всех 4 регионов
}
func TestEtfBlankCellIsNotZero(t *testing.T) {
    // синтетический excelize.NewFile: лист «Holdings by month», 2 фонда;
    // (а) пустая ячейка одного фонда → сумма по присутствующим;
    // (б) пусто у всех фондов месяца → точки региона нет и world за этот месяц нет
}
func TestEtfUnknownRegionFails(t *testing.T) { /* регион «Mars» → err != nil */ }
func TestCellDateMonthEnd(t *testing.T) { /* 46295 → 2026-09-30; monthEnd(2026-08-30) → 2026-08-31 */ }
```

- [ ] **Step 2: Убедиться, что тесты падают:** `go test ./wgc/ -run 'TestEtf|TestCell' -v` — FAIL (`undefined: parseEtfWorkbook`).

- [ ] **Step 3: Реализовать `parse.go` и `etf_parse.go`** по интерфейсам выше. Замечания: суммы — обычным `+=` по колонкам в порядке возрастания (детерминированно); строку-заголовок листа не парсить; `world` считать после регионов; сравнивать регион строго со словарём (нормализация пробелов `strings.TrimSpace`); рантайм-сверка как в спеке §3.1: у листа `Holdings by month` колонка `D` (итог, тонны) сравнивается с суммой фондов за тот же месяц — расхождение > 1e-6 → `log.Warn` (не ошибка, запись — из суммы фондов).

- [ ] **Step 4: Прогнать тесты:** `go test ./wgc/ -race -v` — PASS. Если счётчики точек разошлись с ожиданием — проверить по фикстуре (пропуски месяцев), зафиксировать факт комментарием в тесте, значения-константы не подгонять молча.

- [ ] **Step 5: Коммит**

```bash
git add wgc/parse.go wgc/etf_parse.go wgc/etf_parse_test.go wgc/testdata/ETF_Flows_2026-10-02_1401.xlsx
git commit -m "wgc: ETF flows workbook parser with fixture tests"
```

---

### Task 2: Импортёр `wgc_etf_flows`, каталог, dagu, приоритет витрины

**Files:**
- Create: `wgc/files.go`, `wgc/files_test.go`, `wgc/etf.go`, `wgc/series_meta.go`, `wgc/series_meta_test.go`, `dagu/wgc.yaml`
- Modify: `main.go` (слепой импорт пакета), `views/gold_etf_flows.go` (приоритет), `views/gold_etf_flows_test.go`

**Interfaces:**
- Consumes: всё из Task 1.
- Produces: `const wgcSource = "wgc"`; `const defaultDataDir = "wgc/data"`; `func findLatestFile(dir, mask string) (string, error)` (пусто — файла нет); `type etfFlowsStat struct{ dir string }` c `Name() = "wgc_etf_flows"`; `func wgcEtfCatalog() []util.SeriesMeta` (20 записей).

- [ ] **Step 1: Написать падающие тесты:**

```go
// wgc/files_test.go
func TestFindLatestFilePicksNewestByMtime(t *testing.T) {
    // t.TempDir(): ETF_Flows_2026-10-02_1401.xlsx (свежее) и ETF_Flows_2026-09-01_0900.xlsx
    // os.Chtimes: «сентябрьский» файл новее по mtime → выбран он (проверка, что имя не решает)
}
func TestFindLatestFileNone(t *testing.T) { /* пустой каталог → ("", nil) */ }

// wgc/series_meta_test.go
func TestEtfCatalogDescribesEverySeries(t *testing.T) {
    // wgcEtfCatalog(): 20 записей, source == "wgc", Series == wgcEtfSeriesName(...),
    // frequency "M", Unit/Title/Origin/Description непусты, имена уникальны
}

// wgc/etf_test.go
func TestEtfImportEmptyDirIsNoop(t *testing.T) {
    // stat := etfFlowsStat{dir: t.TempDir()}; count, err := stat.Import(ctx, nil) → (0, nil)
    // (файлов нет — conn не трогается)
}

// views/gold_etf_flows_test.go — обновить TestGoldEtfFlowsViewSourcePriority:
// в каждом coalesce-блоке обязаны быть 'wgc', 'webbridge', 'manual' и порядок wgc < webbridge < manual;
// в SELECT обязано присутствовать source IN ('wgc', 'webbridge', 'manual')
```

- [ ] **Step 2: Тесты падают:** `go test ./wgc/ ./views/ -run 'TestFindLatest|TestEtfCatalog|TestEtfImport|TestGoldEtfFlowsViewSourcePriority' -v` — FAIL.

- [ ] **Step 3: Реализовать:**
  - `files.go`: `findLatestFile` — `filepath.Glob(filepath.Join(dir, mask))`, выбор по `os.Stat` mtime (свежайший), тай-брейк — имя по убыванию.
  - `etf.go`: `etfFlowsStat` — `Import`: если `findLatestFile(dir, "ETF_Flows*.xlsx")` пусто → `log.Warn` + `(0, nil)`; иначе `excelize.OpenFile` (+ `Close`), `parseEtfWorkbook`, сортировка points (series, date), один батч `INSERT INTO macro_series (source, series, date, value)`; после успешного `Send` — `util.UpsertSeriesCatalog(ctx, conn, wgcEtfCatalog())`; `log.Infof("Imported %d rows of %s (wgc ETF flows, %d series)")`. Зарегистрировать в `init()`.
  - `series_meta.go`: 20 описаний (единицы: `млн USD` / `тонны` / `млрд USD`; origin — «WGC Gold ETF Flows, файл ETF_Flows_*.xlsx, лист <лист>»; description — по образцу сида `sql/wgc_etf_flows_catalog.sql` + фраза про импортёр `wgc_etf_flows` и приоритет `wgc > webbridge > manual`).
  - `dagu/wgc.yaml`: chain, `schedule: "17 20 * * 1"`, `overlap_policy: skip`, шаг `wgc_etf_flows` (`CLICKHOUSE_IMPORT_STAT=wgc_etf_flows`, retry 3 × 300).
  - `main.go`: `_ ".../wgc"` после `views`.
  - `views/gold_etf_flows.go`: coalesce `wgc > webbridge > manual`, `source IN ('wgc','webbridge','manual')`, комментарий витрины дополняется импортёром.

- [ ] **Step 4: Тесты зелёные:** `go test ./wgc/ ./views/ -race -v` — PASS; `gofmt -l wgc views` пусто.

- [ ] **Step 5: Живая проверка (ClickHouse через `make`):** `make import STAT=wgc_etf_flows` → `Imported N rows of wgc_etf_flows (wgc ETF flows, 20 series)`; `make import STAT=gold_views` (применить новый SELECT витрины); через MCP `mcp__clickhouse__*` `SELECT * FROM v_gold_etf_flows ORDER BY date DESC LIMIT 5` → 2026-09-30: 9952.2…, 4255.67…, 574.23…; повторный импорт — те же значения (идемпотентность).

- [ ] **Step 6: Коммит**

```bash
git add wgc/ dagu/wgc.yaml main.go views/gold_etf_flows.go views/gold_etf_flows_test.go
git commit -m "wgc: wgc_etf_flows importer, catalog, dagu and view priority"
```

---

### Task 3: CB-парсеры (`wgc/cb_parse.go`)

**Files:**
- Create: `wgc/cb_parse.go`, `wgc/cb_parse_test.go`
- Add: `wgc/testdata/Changes_latest_as_of_Oct2026_IFS.xlsx`, `wgc/testdata/World_official_gold_holdings_as_of_Oct2026_IFS.xlsx` (уже скопированы)

**Interfaces (производит):**
- `type CbCountry struct { ISO, Country, Name string }` — `Country` — русское имя для витрины («Польша»), `Name` — короткое имя в файлах (`Poland, Rep. of`, `Czech Rep.`, `Turkey`, `Russian Federation`, `Kazakhstan, Rep. of`, `Uzbekistan, Rep. of`, `China, P.R.: Mainland`)
- `func CbCountries() []CbCountry` — порядок POL, CZE, TUR, RUS, KAZ, UZB, CHN (порядок фиксирован тестом)
- `func wgcCbSeriesName(metric, iso string) string` — `cb_gold_change_tonnes_<iso>` / `cb_gold_holdings_tonnes_<iso>`
- `func CbSeriesNames() []string` — 14 имён (экспорт для витрины, Task 5)
- `func parseCbChanges(f *excelize.File) ([]seriesPoint, error)` — лист `Monthly`: строки с 9, страна — кол. B по словарю (строгое равенство, `Turkey` ≠ `Turkey*`; 0 или 2 совпадения → ошибка); даты — строка 8 (serial, колонки с D; непустой не-числовой заголовок внутри диапазона → ошибка); пустые ячейки пропускаются, `0` пишется; дата = `monthEnd` месяца заголовка → `cb_gold_change_tonnes_<iso>`
- `func parseCbHoldings(f *excelize.File) ([]seriesPoint, error)` — лист `PDF`: блоки ищутся по заголовку `Tonnes` в строке заголовков (колонки блока: `Tonnes`-2 rank, -1 имя, `Tonnes`, +1 `% of reserves`, +2 `Holdings as of`); имя чистится от сноски (`\d+\)$`); страна не найдена → `log.Warn` (топ-100 меняется), дубль → ошибка; значение tonnes + `monthEnd(asof)` → `cb_gold_holdings_tonnes_<iso>`

- [ ] **Step 1: Написать падающие тесты** (допуск 1e-9 для точечных значений, 1e-6 для сумм):

```go
func TestCbChangesLastMonthPinned(t *testing.T) {
    // 2026-08-31: POL 7.7781006189542943; CZE 1.6793878884015905; KAZ 7.3512767876582492;
    //             RUS -6.2206463251531838; UZB 7.7758079064414796; CHN 20.217100556747845;
    //             TUR 2.8932138038765629 (и ≠ 18.910764828465794 — это строка Turkey*, её не берём)
}
func TestCbChangesYtd2026(t *testing.T) {
    // сумма точек с 2026-01 по 2026-08: POL 97.772267238966151; CZE 14.152996796367127;
    // KAZ 36.063396410687105; RUS -34.213554788342506; TUR -81.606776196123377;
    // UZB 48.210009019937111; CHN ≈ 80.246 (допуск 1e-3)
}
func TestCbChangesAnnualMatchesMonthly(t *testing.T) {
    // лист Annual, колонка 2026: значения равны YTD-суммам (допуск 1e-6);
    // контрольные: POL 97.772267238966151, TUR -81.606776196123377
}
func TestCbHoldingsPinned(t *testing.T) {
    // ровно 7 точек; дата у всех 2026-08-31 (monthEnd из serial 46264);
    // POL 647.98366638673758; CHN 2386.5509626450189; RUS 2270.5359086809121;
    // TUR 532.09321380387655 (сноска «Turkey5)»); UZB 438.55556592329941;
    // KAZ 377.10696556872261; CZE 85.759758638922577
}
func TestCbCountryLookupStrict(t *testing.T) { /* синтетический лист: две строки 'Turkey' → ошибка */ }
```

- [ ] **Step 2: Тесты падают:** `go test ./wgc/ -run 'TestCb' -v` — FAIL.

- [ ] **Step 3: Реализовать `cb_parse.go`** по интерфейсам. Замечания: словарь стран ищется один раз (map по имени, дубликат имени в файле → ошибка); в `Monthly` колонка C — комментарий (пропускается); в `Annual` для теста нужна колонка по году (заголовок строки 1 — «2026»), страна — колонка A — поиск в тесте по длинным именам (`Poland, Rep. of` и т. п.); строки-итоги/пустые — пропускаются по пустому имени.

- [ ] **Step 4: Тесты зелёные:** `go test ./wgc/ -race -v` — PASS.

- [ ] **Step 5: Коммит**

```bash
git add wgc/cb_parse.go wgc/cb_parse_test.go wgc/testdata/Changes_latest_as_of_Oct2026_IFS.xlsx wgc/testdata/World_official_gold_holdings_as_of_Oct2026_IFS.xlsx
git commit -m "wgc: CB reserves parsers (monthly changes + holdings) with fixture tests"
```

---

### Task 4: Импортёр `wgc_cb_reserves`

**Files:**
- Create: `wgc/cb.go`, `wgc/cb_test.go`
- Modify: `wgc/series_meta.go` (+14 записей ЦБ), `wgc/series_meta_test.go`, `dagu/wgc.yaml` (второй шаг)

**Interfaces:**
- Consumes: Task 1–3 (`parseCbChanges`, `parseCbHoldings`, `CbCountries`, `CbSeriesNames`, `findLatestFile`).
- Produces: `type cbReservesStat struct{ dir string }` c `Name() = "wgc_cb_reserves"`; `func wgcCbCatalog() []util.SeriesMeta` (14 записей).

- [ ] **Step 1: Тесты (падающие):**

```go
func TestCbCatalogDescribesEverySeries(t *testing.T) {
    // 14 записей: source "wgc", Series == CbSeriesNames(), unit «тонны», frequency "M",
    // Title: «Золотые резервы ЦБ: изменение за месяц — Польша» и «…: уровень — Польша»;
    // Description содержит оговорки: RUS — пропуски месяцев; TUR — net (строка 'Turkey', не gross 'Turkey*');
    // UZB — с 01.2018; дата — конец отчётного месяца
}
func TestCbImportEmptyDirIsNoop(t *testing.T) { /* cbReservesStat{dir: t.TempDir()}; Import(ctx, nil) → (0, nil) */ }
func TestCbSelectFilesPartial(t *testing.T) {
    // хелпер selectCbFiles(dir) → пути changes/holdings ("" если нет): только changes → holdings == ""
}
```

- [ ] **Step 2: Падают:** `go test ./wgc/ -run 'TestCb(Catalog|Import|Select)' -v` — FAIL.

- [ ] **Step 3: Реализовать `cb.go`:** `Import` — `selectCbFiles`; нет ни одного файла → `log.Warn` + `(0, nil)`; есть — открыть каждый, парсить (нет changes → warn, только уровни; нет holdings → warn, только дельты), объединить points, сортировка, один батч в `macro_series`, затем `util.UpsertSeriesCatalog(ctx, conn, wgcCbCatalog())`; лог `Imported %d rows of %s (wgc CB reserves: changes X / holdings Y)`. Регистрация в `init()`. `dagu/wgc.yaml` — второй шаг `wgc_cb_reserves`.

- [ ] **Step 4: Зелёные:** `go test ./wgc/ -race -v` — PASS.

- [ ] **Step 5: Живая проверка:** `make import STAT=wgc_cb_reserves` → дельты ≈ 7×296 минус пропуски + 7 уровней; повтор — идемпотентно.

- [ ] **Step 6: Коммит**

```bash
git add wgc/cb.go wgc/cb_test.go wgc/series_meta.go wgc/series_meta_test.go dagu/wgc.yaml
git commit -m "wgc: wgc_cb_reserves importer with catalog and dagu step"
```

---

### Task 5: Витрина `v_cb_gold_dynamics` + правило 11

**Files:**
- Create: `views/cb_gold_dynamics.go`, `views/cb_gold_dynamics_test.go`
- Modify: `views/gold_views.go` (список + doc-комментарий пакета), `sql/mcp_kimi_reader.sql` (грант), `scripts/mcp_check.py`

**Interfaces:**
- Consumes: `wgc.CbCountries()`, `wgc.CbSeriesNames()`.
- Produces: `var cbGoldDynamicsView util.View` (`Name: "v_cb_gold_dynamics"`), добавленная в `goldViewsList`.

SQL (эскиз — колонки и порядок обязательны, синтаксис сверить на живом прогоне):

```sql
SELECT
    m.iso3 AS iso3, m.country AS country,
    c.last_date AS last_change_date, c.mom_change_tonnes AS mom_change_tonnes,
    c.ytd_change_tonnes AS ytd_change_tonnes,
    h.holdings_date AS holdings_date, h.holdings_tonnes AS holdings_tonnes
FROM (SELECT x.1 AS iso3, x.2 AS country FROM (SELECT arrayJoin([('POL','Польша'),…7 пар…]) AS x)) AS m
LEFT JOIN (
    SELECT substring(series, length('cb_gold_change_tonnes_') + 1) AS iso3,
           max(d) AS last_date, argMax(v, d) AS mom_change_tonnes,
           sumIf(v, d >= toStartOfYear(max(max_d))) AS ytd_change_tonnes
    FROM (SELECT series, date AS d, value AS v, max(date) OVER (PARTITION BY series) AS max_d
          FROM macro_series FINAL WHERE source = 'wgc' AND series IN (<14 имён: CbSeriesNames()>))
    GROUP BY substring(series, length('cb_gold_change_tonnes_') + 1)
) AS c ON c.iso3 = m.iso3
LEFT JOIN (
    SELECT substring(series, length('cb_gold_holdings_tonnes_') + 1) AS iso3,
           max(date) AS holdings_date, argMax(value, date) AS holdings_tonnes
    FROM macro_series FINAL WHERE source = 'wgc' AND series IN (<те же 14>)
    GROUP BY substring(series, length('cb_gold_holdings_tonnes_') + 1)
) AS h ON h.iso3 = m.iso3
ORDER BY iso3
```

- [ ] **Step 1: Тесты (падающие):** `views/cb_gold_dynamics_test.go` по образцу `gold_etf_flows_test.go`: витрина есть в `goldViewsList`; комментарий таблицы непуст; все колонки SELECT прокомментированы (хелпер `selectedColumns`), комментариев к лишним колонкам нет; `ORDER BY iso3`; в SELECT присутствуют все 14 имён из `wgc.CbSeriesNames()` и 7 пар ISO→имя из `wgc.CbCountries()`. Прогон: `go test ./views/ -run TestCbGoldDynamics -v` — FAIL.

- [ ] **Step 2: Реализовать витрину;** зарегистрировать в `goldViewsList` и в doc-комментарии пакета; добавить `GRANT SELECT ON default.v_cb_gold_dynamics TO kimi_reader` в `sql/mcp_kimi_reader.sql`; в `scripts/mcp_check.py` добавить проверки (по образцу блока webbridge): каталог `source='wgc'` — ровно 34 записи; `v_cb_gold_dynamics` читается и содержит 7 строк; существующие проверки не менять.

- [ ] **Step 3: Тесты зелёные:** `go test ./wgc/ ./views/ -race -v` — PASS.

- [ ] **Step 4: Живая проверка:** `make import STAT=gold_views` (создать/обновить витрину) → через MCP: `SELECT * FROM v_cb_gold_dynamics` — 7 строк; контроль: POL `ytd_change_tonnes` ≈ 97.772, `holdings_tonnes` ≈ 647.984, `holdings_date` = 2026-08-31, `last_change_date` = 2026-08-31; `make mcp-user` (гранты); `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

- [ ] **Step 5: Коммит**

```bash
git add views/cb_gold_dynamics.go views/cb_gold_dynamics_test.go views/gold_views.go sql/mcp_kimi_reader.sql scripts/mcp_check.py
git commit -m "wgc: v_cb_gold_dynamics view, MCP grant and checks"
```

---

### Task 6: Документация и роадмап

**Files:**
- Modify: `ARCHITECTURE.md` (§2 — пакет `wgc/`; §6.1 — два импортёра; §6.2 — комментарий `source` (строка «Планируемые источники wgc, cme…»); §6.3 — `v_gold_etf_flows` приоритет + новая `v_cb_gold_dynamics`), `README.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`

- [ ] **Step 1:** Пройти чек-лист скила `.kimi-code/skills/sync-readme-architecture/SKILL.md` (изменены: пакет/импортёры, витрины, расписание dagu — все пункты, которые скил требует).
- [ ] **Step 2:** Роадмап: добавить пункт «Фаза 3 — follow-up: WGC-импортёры (ETF Flows и резервы ЦБ)» со статусом «в работе» (формулировка — спека §1), пункт «Фаза 3 — follow-up 2: национальные ЦБ напрямую (NBP/CNB/CBRT/ЦБ РФ/НБК/ЦБУ)» со статусом ⏳; в «Осталось» — ручная раскладка файлов на staging.
- [ ] **Step 3:** `make all` — зелёный; `git commit -m "docs: architecture, readme and roadmap for wgc importers"`.

---

## Финал (после задач)

1. Целостное ревью ветки (сильнейшая модель) → fix-loop по отчёту.
2. `verification-before-completion`: `make all`; `make import STAT=wgc_etf_flows` и `=wgc_cb_reserves` (повторно — 0 новых значений в `FINAL`); `SELECT * FROM v_gold_etf_flows ORDER BY date DESC LIMIT 5`; `SELECT * FROM v_cb_gold_dynamics`; `make mcp-check`.
3. Роадмап: статус пункта → ✅ с отметкой приёмки (команды и результаты) и `#PR` — тем же PR; один PR в конце.
