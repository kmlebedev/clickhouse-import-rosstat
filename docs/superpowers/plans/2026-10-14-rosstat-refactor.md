# Стандарт пакета импортёров + рефакторинг `rosstat/` (починка `salaries_mes` и `vvp_kvartal`) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Зафиксировать целевой вид пакета импортёров отдельным нормативным документом и привести `rosstat/` к нему, починив при этом два пустых ряда контура ключевой ставки — `salaries_mes` и `vvp_kvartal`.

**Architecture:** Один новый нормативный документ `docs/IMPORTER_PACKAGE_STANDARD.md` (ссылки из `AGENTS.md`/`ARCHITECTURE.md`/`README.md`), затем первое его применение: пакет `rosstat/` получает единый реестр с тестом-энфорсером, транспорт только через `util` (HTML-хелперы `GetPage`/`FindLink` выносятся из `cbr/links.go` в `util/links.go`), все четыре импортёра переводятся на `util.ClickHouseImport`, разбор — на сырые значения ячеек с поиском по признаку и fail-fast, таблицы получают префикс `rosstat_`, тесты — фикстуры реальных артефактов источника. Миграция данных ведётся так, что витрина `v_model_inputs` контура золота ни на шаг не остаётся без входов.

**Tech Stack:** Go 1.x, `github.com/xuri/excelize/v2` v2.11.0, `github.com/PuerkitoBio/goquery` v1.10.2, ClickHouse (`ReplacingMergeTree`, витрины `SQL SECURITY DEFINER`), `github.com/gocolly/colly/v2` (выводится из `rosstat/`), dagu (расписание), python3 + `openpyxl` (только для подготовки фикстур и как справочный инструмент).

**Spec:** `docs/superpowers/specs/2026-10-14-rosstat-refactor-design.md` (дизайн) и `docs/IMPORTER_PACKAGE_STANDARD.md` (норма; ссылки вида «стандарт §6.3» указывают на его разделы).

## Global Constraints

- Рабочая ветка `feat/rosstat-refactor`; незакоммиченная правка владельца `deploy/staging/dagu.service` **не трогается**.
- Новых зависимостей не добавлять: `goquery`, `excelize`, `x/text` уже прямые; `colly` остаётся в `go.mod` (нужен `financial/`, `customs/`, `fao/`, `minfin/`, `bank/`), но из `rosstat/` удаляется полностью.
- Вставки в ClickHouse — только батчами `PrepareBatch → Append(err!) → Send`; HTTP — только через `util.*`; логирование — только logrus (ноль `fmt.Print*` в `rosstat/`); сетевых вызовов в `init()` нет.
- DDL каждого импортёра — в `%s`-форме (`CREATE TABLE IF NOT EXISTS %s (`): шаблон исполняет `fmt.Sprintf`.
- `Name()` и таблицы — с префиксом `rosstat_`; `Name() == TableName` у всех четырёх импортёров пакета.
- Парсеры офлайн: вход `*excelize.File`, внутри — ни одного обращения к сети; значения ячеек — `excelize.Options{RawCellValue: true}`.
- Данные не меняются, кроме заявленного: `ipc_mes` меняет имена 4 рядов (остальные значения те же), `ipc_weeks` — те же ряды и точки, `salaries_mes` и `vvp_kvartal` наполняются с нуля.
- Секреты — только через `os.Getenv`; содержимое `~/.config/rosstat/env` не читать и не печатать. Команды, которым нужны ключи или ClickHouse, запускать через `make`.
- Каждая задача заканчивается gofmt-чистым кодом и зелёными `go build ./...` и `go test ./rosstat/ ./util/ ./cbr/`.

## Review Focus

Пять классов входа, которые спека подразумевает, но рутинные тесты задачи не покрывают; для каждого в задаче-владельце стоит свой тест.

1. **Источник отдаёт не XLSX, а заглушку/«Upgrade Required»** (`util.GetFile` не проверяет HTTP-статус): ожидаемое поведение — внятная ошибка с именем таблицы, а не `Imported 0 rows` и не запись мусора. Тест: пустая книга и книга без нужного листа → ошибка, 0 строк в батче (T4, T5, T6, T7).
2. **Заголовок источника меняется** (год в названии, редакция, сноска): ожидаемое — имя ряда в базе не меняется, ряд не рвётся. Тест: серия из разбора равна стабильному имени из кода (T4, T5, T6).
3. **В источнике появился новый лист или товар** (ожидаемое изменение): ожидаемое — warn и импорт остального, цепочка dagu не падает; неописанный ряд не уходит агенту молча. Тест: лист вне карты и товар вне списка → предупреждение, разбор остального продолжается (T6, T7).
4. **Ячейка без данных** (пустая или литеральный `0`, который в источнике означает «данных не имеется»): ожидаемое — не точка ряда, пропуск объяснён в `description` ряда. Тест: число точек и отсутствие нулевой зарплаты (T4).
5. **Живой контур золота на переименовании таблиц**: `v_model_inputs` читает `ipc_mes`/`ipc_weeks` и грантован агенту. Ожидаемое — на всём протяжении миграции витрина отдаёт непустые `ipc_mes_last`/`ipc_week_ytd`. Проверка: шаги T9.6–T9.8 (сверка до и после переключения витрины).

---

### Task 1: Вынос HTML-хелперов в `util/` и переключение `cbr/`

**Files:**
- Create: `util/links.go`
- Create: `util/links_test.go`
- Delete: `cbr/links.go`
- Modify: `cbr/key_rate.go`, `cbr/Infl_exp.go`, `cbr/credit_m2x.go`, `cbr/indicators_cpd.go`, `cbr/mo_br.go`, `cbr/mp_summary.go`
- Modify: `cbr/Infl_exp_test.go`, `cbr/credit_m2x_test.go`, `cbr/indicators_cpd_test.go`

**Interfaces:**
- Consumes: `util.GetFile(url string) (io.ReadCloser, error)` (существует).
- Produces: `util.GetPage(pageURL string) ([]byte, error)` и `util.FindLink(page []byte, selector string) (string, error)` — их используют все задачи про `rosstat/` (T3–T7).

- [ ] **Step 1: Прочитать эталон**

Прочитать `cbr/links.go` целиком: тела обеих функций, их комментарии и контракт (`FindLink` возвращает href как есть, пустое совпадение — ошибка; `GetPage` закрывает ридер и возвращает обе ошибки — чтения и закрытия).

- [ ] **Step 2: Написать падающий тест `util/links_test.go`**

Тесты по образцу `util/json_test.go` (там `httptest.NewServer` + `util.GetJSON`):

```go
func TestGetPageReturnsBody(t *testing.T)          // сервер отдаёт "<a href=\"/x.xlsx\">x</a>"; тело совпадает
func TestGetPageEmptyBodyFails(t *testing.T)       // сервер отдаёт 200 и 0 байт → ошибка (GetFile уже так делает)
func TestFindLinkReturnsHref(t *testing.T)         // inline-HTML, селектор `a[href$=".xlsx"]` → "/storage/x.xlsx"
func TestFindLinkMissingSelectorFails(t *testing.T) // селектор без совпадения → ошибка, в тексте есть селектор
func TestFindLinkBadHTMLFails(t *testing.T)        // битый HTML → ошибка разбора
```

- [ ] **Step 3: Запустить тест и убедиться, что он падает**

Run: `go test ./util/ -run 'TestGetPage|TestFindLink' -v`
Expected: FAIL — `undefined: util.GetPage`, `undefined: util.FindLink`.

- [ ] **Step 4: Создать `util/links.go`, перенеся тела из `cbr/links.go`**

```go
package util

func GetPage(pageURL string) ([]byte, error)
func FindLink(page []byte, selector string) (string, error)
```

Тела — дословный переезд из `cbr/links.go`; комментарии сохранить и заменить упоминание пакета на `util` (описать, что хелпер общий для пакетов импортёров, стандарт §4.1).

- [ ] **Step 5: Запустить тест и убедиться, что он проходит**

Run: `go test ./util/ -run 'TestGetPage|TestFindLink' -v`
Expected: PASS.

- [ ] **Step 6: Удалить `cbr/links.go` и переключить вызовы в `cbr/`**

`git rm cbr/links.go`; в шести файлах импортёров и трёх тестовых заменить `findLink(` → `util.FindLink(` и `getPage(` → `util.GetPage(` (импорт `util` в этих файлах уже есть; `goquery` из тестов не удалять, если он ещё используется).

- [ ] **Step 7: Прогнать тесты `cbr/` — они и есть гейт задачи**

Run: `go test ./cbr/ -count=1` и `grep -rn "findLink\|getPage" cbr/ | grep -v util.`
Expected: `ok`; grep не находит ни одного вызова без префикса `util.`.

- [ ] **Step 8: Коммит**

```bash
git add -A util/links.go util/links_test.go cbr/
git commit -m "move findLink/getPage from cbr/ to util as GetPage/FindLink"
```

---

### Task 2: Реестр `rosstat/importers.go`, префикс `rosstat_*`, тест-энфорсер

**Files:**
- Create: `rosstat/importers.go`
- Create: `rosstat/importers_test.go`
- Modify: `rosstat/ipc_mes.go`, `rosstat/ipc_weeks.go`, `rosstat/salaries_mes.go`, `rosstat/vvp_kvartal.go`, `rosstat/series_meta.go`
- Modify: `dagu/rosstat.yaml`, `views/model_inputs.go`, `sql/key_rate_coverage.sql`, `util/publications.go` (комментарий), `cbr/views.go` (комментарий)

**Interfaces:**
- Consumes: ничего из T1.
- Produces: `rosstatImporters` (список записей реестра), константы `rosstatIPCMesTable = "rosstat_ipc_mes"`, `rosstatIPCWeeksTable = "rosstat_ipc_weeks"`, `rosstatSalariesMesTable = "rosstat_salaries_mes"`, `rosstatVVPKvartalTable = "rosstat_vvp_kvartal"`; переменные импортёров `ipcMesImporter`, `ipcWeeksImporter`, `salariesMesImporter`, `vvpKvartalImporter`. Эти имена используют все последующие задачи.

- [ ] **Step 1: Переименовать константы таблиц и DDL в четырёх файлах импортёров**

В `rosstat/ipc_mes.go`: `ipcMesTable` → `rosstatIPCMesTable = "rosstat_ipc_mes"`, DDL и `ipcMesCountPoints` — на новое имя (`SELECT count() FROM rosstat_ipc_mes FINAL`). Аналогично в трёх остальных файлах (имена констант: `rosstatIPCWeeksTable`, `rosstatSalariesMesTable`, `rosstatVVPKvartalTable`; запросы чека — `rosstat_ipc_mes`/`rosstat_salaries_mes`/`rosstat_vvp_kvartal`, у `ipc_weeks` чека нет). DDL привести к `%s`-форме (`CREATE TABLE IF NOT EXISTS %s (`), убрав склейку `"CREATE TABLE IF NOT EXISTS " + ipcWeeksTable` и `ipcWeeksInsert`.

- [ ] **Step 2: Добавить именованную переменную импортёра там, где её нет**

В `rosstat/ipc_weeks.go` завести `var ipcWeeksImporter = &IpcWeeksStat{}` и **удалить** персональный `init()`; в остальных трёх файлах удалить персональные `init()`, оставив именованные переменные.

- [ ] **Step 3: Создать `rosstat/importers.go`**

Форма — по образцу `cbr/importers.go` (тип, список, один `init()`); литералы таблиц и запросов чека обязательны (стандарт §8.2), комментарий объясняет, почему литералы, а не константы:

```go
type rosstatImporter struct {
	chimport.ImportStat
	table       string
	countPoints string
}

var rosstatImporters = []rosstatImporter{
	{ipcMesImporter, "rosstat_ipc_mes", "SELECT count() FROM rosstat_ipc_mes FINAL"},
	{ipcWeeksImporter, "rosstat_ipc_weeks", ""},
	{salariesMesImporter, "rosstat_salaries_mes", "SELECT count() FROM rosstat_salaries_mes FINAL"},
	{vvpKvartalImporter, "rosstat_vvp_kvartal", "SELECT count() FROM rosstat_vvp_kvartal FINAL"},
}

func init() {
	for _, imp := range rosstatImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
```

- [ ] **Step 4: Обновить имя витрины-владельца и её `Select` в `rosstat/series_meta.go`**

`rosstatMacroView.Tables` — новые константы; `Select` — литералы `rosstat_ipc_mes`, `rosstat_ipc_weeks`, `rosstat_vvp_kvartal`, `rosstat_salaries_mes`; имя витрины `v_rosstat_macro` и `Comment`/`Columns` не меняются.

- [ ] **Step 5: Обновить расписание и внешние ссылки**

`dagu/rosstat.yaml` — четыре `id` и четыре `CLICKHOUSE_IMPORT_STAT` на новые имена (порядок шагов и расписание `17 12 * * 4` не меняются); `views/model_inputs.go` — `ipc_mes`→`rosstat_ipc_mes` и `ipc_weeks`→`rosstat_ipc_weeks` в `Tables` и во всех местах `Select` (`FROM ipc_mes FINAL`, `FROM ipc_weeks FINAL`, подзапрос года); `sql/key_rate_coverage.sql` — те же четыре строки; комментарии-примеры в `util/publications.go:30` и `cbr/views.go:68`.

- [ ] **Step 6: Создать `rosstat/importers_test.go` — энфорсер `TestRosstatImportersPrefixedAndRegistered`**

Копия структуры `cbr/importers_test.go` со своими литералами: `rosstatNamePrefix = "rosstat_"`, пустой `sharedMacroSeriesTable` (у пакета общей таблицы нет — проверка (б) выполняется как «таблица с префиксом, `Name() == table`»), ожидаемый набор точек контроля:

```go
var expectedPublicationCountPoints = map[string]string{
	"rosstat_ipc_mes":       "SELECT count() FROM rosstat_ipc_mes FINAL",
	"rosstat_salaries_mes":  "SELECT count() FROM rosstat_salaries_mes FINAL",
	"rosstat_vvp_kvartal":   "SELECT count() FROM rosstat_vvp_kvartal FINAL",
}
```

плюс инвариант числа (`число rosstat_-имён в chimport.Stats == len(rosstatImporters)`) и проверка полноты `../dagu/rosstat.yaml` в обе стороны (обратное направление — шаг `rosstat_ipc_weeks` обязан быть в расписании, у него нет объявления, которое могло бы его «защитить»).

- [ ] **Step 7: Прогнать тесты пакета**

Run: `go test ./rosstat/ -count=1 -v -run 'Importers|SeriesMeta'`
Expected: PASS (старые фикстурные тесты каталога ещё живы и должны остаться зелёными).

- [ ] **Step 8: Проверить, что старых имён в коде не осталось**

Run: `grep -rn '\bipc_mes\b\|\bipc_weeks\b\|\bsalaries_mes\b\|\bvvp_kvartal\b' --include=*.go rosstat/ views/ | grep -v rosstat_`
Expected: пусто (допустимы только строки внутри `rosstat_...`).

- [ ] **Step 9: Коммит**

```bash
git add -A rosstat/ dagu/rosstat.yaml views/model_inputs.go sql/key_rate_coverage.sql util/publications.go cbr/views.go
git commit -m "rosstat: single importer registry and rosstat_ table prefix"
```

---

### Task 3: Фикстуры `rosstat/testdata/` и базовая линия

**Files:**
- Create: `rosstat/testdata/{ipc_mes.xlsx,ipc_weeks.xlsx,salaries_mes.xlsx,vvp_kvartal.xlsx,price_page.html,salaries_page.html,accounts_page.html}`
- Delete: `rosstat/data/nedel_ipc_2023.xlsx`
- Create (git-ignored): `.superpowers/sdd/2026-10-14-rosstat-refactor/rosstat-baseline.md`

**Interfaces:**
- Produces: имена фикстур, на которые ссылаются тесты T4–T7.

- [ ] **Step 1: Скачать живые артефакты в временный каталог**

Источники (проверены 2026-10-14): страница `https://rosstat.gov.ru/statistics/price` (877 КБ; ссылки `ipc_mes_ММ-ГГГГ.xlsx` и `Nedel_ipc.xlsx`), `https://rosstat.gov.ru/labor_market_employment_salaries` (ссылка `tab1-zpl_ММ-ГГГГ.xlsx`), `https://rosstat.gov.ru/statistics/accounts` (ссылка `VVP_kvartal_s-1995-ГГГГ.xlsx`). Адреса самих XLSX брать со страниц (`grep -o 'href="[^"]*<маска>[^"]*"'`).

- [ ] **Step 2: Положить XLSX в `rosstat/testdata/`**

`ipc_mes.xlsx` (~38 КБ), `ipc_weeks.xlsx` (~173 КБ, листы `Содержание, 2022…2026`), `salaries_mes.xlsx` (~21 КБ), `vvp_kvartal.xlsx` (~115 КБ) — как есть, без урезания (форматы и листы не менять, стандарт §11.2). `Nedel_ipc.xlsx` кладётся как `ipc_weeks.xlsx`.

- [ ] **Step 3: Положить HTML-страницы**

`price_page.html` (одна страница обслуживает и `ipc_mes`, и `ipc_weeks` — разные селекторы), `salaries_page.html`, `accounts_page.html`.

- [ ] **Step 4: Удалить `rosstat/data/` и обновить ссылку в тесте**

`git rm -r rosstat/data`; в `rosstat/series_meta_test.go` путь `data/nedel_ipc_2023.xlsx` → `testdata/ipc_weeks.xlsx`.

- [ ] **Step 5: Снять базовую линию (read-only)**

Через MCP `kimi_reader` (сырые таблицы агенту закрыты — снимать по витрине) записать в `.superpowers/sdd/2026-10-14-rosstat-refactor/rosstat-baseline.md`:

```sql
SELECT series, count() AS points, toString(min(date)) AS d0, toString(max(date)) AS d1
FROM v_rosstat_macro GROUP BY series ORDER BY series;
SELECT count() AS catalog_rows FROM v_series_catalog WHERE source = 'rosstat';
```

Известное на 2026-10-14 (сверка, что снимок не разошёлся): витрина 27 407 строк / 132 ряда, `d0 = 1991-01-31`, `d1 = 2026-10-05`; каталог `source='rosstat'` — **132 строки** (4 ИПЦ + 128 товаров: `salaries_mes` и `vvp_kvartal` до `AfterImport` не доходят и своих описаний не пишут); `salaries_mes` и `vvp_kvartal` в витрину не входят вовсе.

- [ ] **Step 6: Коммит**

```bash
git add -A rosstat/testdata rosstat/series_meta_test.go && git rm -r --cached rosstat/data 2>/dev/null; git add -A rosstat/
git commit -m "rosstat: real source fixtures in testdata/, drop legacy data dir"
```

---

### Task 4: `salaries_mes` — починка разбора (сырые значения, нули, стабильное имя)

**Files:**
- Modify: `rosstat/salaries_mes.go`
- Create: `rosstat/salaries_mes_test.go`
- Modify: `rosstat/series_meta_test.go` (убрать синтетический тест `TestSalariesMesSeriesMetaCoversParsedSheet`), `rosstat/series_meta.go` (описание ряда)

**Interfaces:**
- Consumes: `util.GetPage`/`util.FindLink` (T1), фикстуру `testdata/salaries_mes.xlsx` (T3), `rosstatSalariesMesTable` (T2).
- Produces: `salariesMesImport(xlsx *excelize.File, batch driver.Batch) error`, `salariesMesSeriesName` (константа), `salariesMesLinkSelector`, тестовый харнесс `appendRecorder` (переиспользуют T5–T7).

- [ ] **Step 1: Выяснить фактическую разметку по фикстуре**

Лист `Лист1`: A1 — заголовок, строка 5 — шапка (B «В среднем за год», C «В среднем за кварталы», D–F пусты, **G…R = Январь…Декабрь**), строки 8… — данные (A = год, у поздних лет с суффиксом `2026(2)`), строка «… данных не имеется» обрывает лист. Числовой формат данных — `#,##0`, поэтому значения надо читать сырыми.

- [ ] **Step 2: Написать падающий тест `rosstat/salaries_mes_test.go`**

Общий харнесс `appendRecorder` (подмена `driver.Batch`: собирает `[]appendedRow{series, date, value}`) вынести в `rosstat/series_meta_test.go` — его используют T5–T7. Тесты:

```go
func TestSalariesMesReadsFixture(t *testing.T)        // точек 421: 418 ненулевых числовых ячеек + 3 текстовые со сноской («62 470(2)» — октябрь–декабрь 2022, значения ряда); отброшены 6 нулей января–июня 1991 и 5 пустых августа–декабря 2026; ряд один — salariesMesSeriesName
func TestSalariesMesSkipsZeroCells(t *testing.T)      // среди точек нет value == 0 (январь–июнь 1991 в источнике — «данных не имеется»)
func TestSalariesMesReadsFootnoteCells(t *testing.T)  // 2022-10 → 2022-11-01 = 62470; 2022-11 → 2022-12-01 = 63060; 2022-12 → 2023-01-01 = 88468
func TestSalariesMesSpotValues(t *testing.T)          // 1992-01 → дата 1992-02-01, значение 1; 2026-06 → дата 2026-07-01, значение 114743
func TestSalariesMesSeriesMetaCoversParsedRows(t *testing.T) // двусторонний контракт с salariesMesSeriesMeta
func TestSalariesMesMissingSheetFails(t *testing.T)   // пустая книга → ошибка, в тексте имя таблицы, в батче 0 строк
func TestSalariesMesFindLink(t *testing.T)            // util.FindLink(testdata/salaries_page.html, salariesMesLinkSelector) → href с "tab1-zpl"
```

- [ ] **Step 3: Запустить и убедиться, что падает**

Run: `go test ./rosstat/ -run TestSalariesMes -v`
Expected: FAIL — значения вроде `"103,612"`; `strconv.ParseFloat` возвращает ошибку разбора.

- [ ] **Step 4: Переписать разбор**

`xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})`; имя ряда — константа `salariesMesSeriesName` (значение — нынешний `Series` из каталога), а не `rows[0][0]`; нулевые ячейки пропускаются; лишний повторный `ParseFloat` в `salariesMesImport` убирается (парсер отдаёт уже разобранные значения). Порядок: найти якорную строку (`row[0] == salariesMesField`), считать год как `salariesMesYearStart + (i - fieldFound)`, обойти `row[6:]` по месяцам, `len(row) < 7` — конец листа. Пустой разбор — ошибка с именем таблицы (стандарт §6.4).

- [ ] **Step 5: Перевести `Import()` на `util`**

Коллектор `colly` заменить на `util.GetPage` + `util.FindLink` (селектор-константа `salariesMesLinkSelector = "a[href*=\"tab1-zpl\"]"`, домен приклеивается через `rosstatUrl`); пустая ссылка — ошибка (§4.4).

- [ ] **Step 6: Дописать оговорку в каталог**

В `salariesMesSeriesMeta[0].Description` — что нулевые ячейки источника (январь–июнь 1991) не записываются как точки.

- [ ] **Step 7: Прогнать тесты**

Run: `go test ./rosstat/ -run TestSalariesMes -v -count=1`
Expected: PASS.

- [ ] **Step 8: Коммит**

```bash
git add rosstat/salaries_mes.go rosstat/salaries_mes_test.go rosstat/series_meta.go rosstat/series_meta_test.go
git commit -m "fix salaries_mes: raw cell values, skip no-data zeros, stable series name"
```

---

### Task 5: `vvp_kvartal` — починка разбора (заголовок A2, обход с нулевой колонки, перенос года)

**Files:**
- Modify: `rosstat/vvp_kvartal.go`
- Create: `rosstat/vvp_kvartal_test.go`
- Modify: `rosstat/series_meta_test.go` (убрать `TestVvpKvartalSeriesMetaCoversParsedSheets`)

**Interfaces:**
- Consumes: `util.GetPage`/`util.FindLink` (T1), `testdata/vvp_kvartal.xlsx` (T3), `rosstatVVPKvartalTable` (T2), `appendRecorder` (T4).
- Produces: `vvpKvartalImport(xlsx *excelize.File, batch driver.Batch) error`, `vvpKvartalSeriesNames` (карта «лист → имя ряда»), `vvpKvartalLinkSelector`, `getQuarterDate(year int, quarter string) (time.Time, error)`.

- [ ] **Step 1: Выяснить фактическую разметку по фикстуре**

Листы `2, 9, 10, 12, 14`: A2 — заголовок (объединённая A2:D2), строка 3 — годы в колонках A, E, I, … (у поздних групп с суффиксом сноски `20262)`), строка 4 — подписи `I квартал`…`IV квартал` с колонки A, строка 5 — данные с колонки A, значения — полные float.

- [ ] **Step 2: Написать падающий тест `rosstat/vvp_kvartal_test.go`**

```go
func TestVvpKvartalReadsFixture(t *testing.T)          // ровно 5 рядов = карта листов, точек 302: листы "2"/"9"/"10" — по 62 (2011-I … 2026-II), листы "12"/"14" — по 58 (2012-I … 2026-II)
func TestVvpKvartalKeepsFirstGroup(t *testing.T)       // в ряду листа "2" есть точка с датой 2011-04-01 (первая группа не теряется)
func TestVvpKvartalSpotValue(t *testing.T)             // лист "2", 2011-04-01 → 13024.799539024814 (допуск 1e-6) — сырое значение, не формат "13,024.8"
func TestVvpKvartalSeriesMetaCoversParsedRows(t *testing.T) // двусторонний контракт с vvpKvartalSeriesMeta
func TestVvpKvartalMissingSheetFails(t *testing.T)     // книга без листа "9" → ошибка с именем листа и таблицы, 0 строк в батче
func TestVvpKvartalUnknownQuarterFails(t *testing.T)   // подпись "X квартал" → ошибка, НЕ panic
func TestVvpKvartalRepeatedYearLabelAdvances(t *testing.T) // лист "14" (метка последней группы повторяет "20252)") → 58 различных дат: 2025-I = 106.7618431956141 и 2026-I = 104.1602983322909 остаются разными точками, последние даты ряда — 2026-04-01 и 2026-07-01; повтор метки виден в журнале (Warn)
func TestVvpKvartalFindLink(t *testing.T)              // util.FindLink(testdata/accounts_page.html, vvpKvartalLinkSelector) → href с "VVP_kvartal"
```

- [ ] **Step 3: Запустить и убедиться, что падает**

Run: `go test ./rosstat/ -run TestVvpKvartal -v`
Expected: FAIL — `panic: runtime error: index out of range` на чтении заголовка (нынешняя строка 88 файла).

- [ ] **Step 4: Переписать разбор**

Порядок для каждого листа из `vvpKvartalSheetNames` (слайс, а не карта — порядок обхода должен быть детерминированным):

1. `rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})`; ошибка — с именем листа.
2. Найти строку подписей кварталов: первая строка, где первая ячейка начинается с `I `, `II `, `III ` или `IV ` и содержит `квартал`. Не найдена — ошибка.
3. `yearRow = labelRow - 1`, `dataRow = labelRow + 1`; выход за границы — ошибка.
4. Обход колонок `i` с **нуля**, пока ячейка `dataRow[i]` не пуста: непустая ячейка `yearRow[i]` обновляет текущий год (первые 4 символа, `strconv.Atoi`; иначе ошибка); год 0 — ошибка с номером колонки; подпись квартала — первый токен, отображение `{"I":1,"II":2,"III":3,"IV":4}`, незнакомая подпись — ошибка; значение — `ParseFloat` в сыром виде, ошибка при нечисле.
   **Год обязан возрастать.** Повтор метки прошлой группы — дефект самой выгрузки: у листа 14 в книге 09-2026 последняя группа помечена `20252)`, тогда как все четыре соседних листа того же файла говорят `20262)`. Если принять метку на веру, значения 2026-I и 2026-II запишутся датами 2025 года и **затрут** настоящие 2025-I/2025-II (в таблице `ReplacingMergeTree ORDER BY (name, date)`). Поэтому: непустая метка, равная или меньшая текущего года, выводит год как `текущий + 1` и логирует `Warn` с листом, колонкой и обеими метками; молчаливое принятие повтора запрещено. Это вывод из структуры (кварталы идут I→IV и продолжаются I следующего года), а не догадка, и он сходится с четырьмя соседними листами; когда источник исправит метку, вывод совпадёт с ней и предупреждение исчезнет.
5. Ноль точек на листе — ошибка.

`getQuarterDate(year int, quarter string) (time.Time, error)` возвращает ошибку вместо `panic`; `strings.Split(label, " ")[0]` заменяется разбором первого токена; `fmt.Printf` удаляются.

- [ ] **Step 5: Имя ряда — из карты, а не из заголовка**

`vvpKvartalSeriesNames` — карта «номер листа → стабильное имя», 1:1 с нынешними `vvpKvartalSeriesMeta[].Series` (`2 Валовой внутрений продукт`, `9 Валовой внутрений продукт`, `10 Валовой внутренний продукт`, `12 Индексы физического объема валового внутреннего продукта`, `14 Индексы - дефляторы валового внутреннего продукта`). Заголовок A2 больше не участвует в имени ряда (сноски и редакции перестают двигать имя); отсутствие ожидаемого листа ловит §6.4.

- [ ] **Step 6: Перевести `Import()` на `util`**

Коллектор `colly` → `util.GetPage` + `util.FindLink` (`vvpKvartalLinkSelector = "a[href*=\"VVP_kvartal\"]"`), пустая ссылка — ошибка.

- [ ] **Step 7: Прогнать тесты**

Run: `go test ./rosstat/ -run TestVvpKvartal -v -count=1`
Expected: PASS.

- [ ] **Step 8: Коммит**

```bash
git add rosstat/vvp_kvartal.go rosstat/vvp_kvartal_test.go rosstat/series_meta_test.go
git commit -m "fix vvp_kvartal: read title from A2, scan from column A, carry year across group"
```

---

### Task 6: `ipc_mes` — стабильные имена рядов и разбор по признаку

**Files:**
- Modify: `rosstat/ipc_mes.go`
- Create: `rosstat/ipc_mes_test.go`
- Modify: `rosstat/series_meta_test.go` (убрать `TestIpcMesSeriesMetaCoversParsedSheets`), `rosstat/series_meta.go` (4 записи каталога)

**Interfaces:**
- Consumes: `util.GetPage`/`util.FindLink` (T1), `testdata/ipc_mes.xlsx`, `testdata/price_page.html` (T3), `rosstatIPCMesTable` (T2), `appendRecorder` (T4).
- Produces: `ipcMesImport(xlsx *excelize.File, batch driver.Batch) error`, `ipcMesSeriesNames` (карта «номер листа → стабильное имя»), `ipcMesLinkSelector`.

- [ ] **Step 1: Выяснить разметку по фикстуре**

Листы `Содержание, 01, 02, 03, 04`. В листе `01`: A1 — заголовок с годами и сносками, строка 4 — годы (колонки B…, последняя заполненная — 2026), строка 5 — якорь `к концу предыдущего месяца`, строки 6…17 — `январь`…`декабрь`, строка 18 — второй якорь `к декабрю предыдущего года` и строка 19 — ещё один `декабрь` (годовой блок, его читать нельзя), дальше сноски. Формат данных — `0.00`.

- [ ] **Step 2: Написать падающий тест `rosstat/ipc_mes_test.go`**

```go
func TestIpcMesReadsFixture(t *testing.T)          // ровно 4 ряда по 429 точек (1716 всего)
func TestIpcMesStableSeriesNames(t *testing.T)     // ни одно имя ряда не содержит "1991-2026" и ")" (сноски и годы не попадают в имя)
func TestIpcMesStopsAtEndOfMonthBlock(t *testing.T) // блок месяцев обрывается на строке "к декабрю предыдущего года": у ряда "01" за 1992 год ровно 12 точек и нет точки со значением 2608.84 (годовой индекс того же декабря)
func TestIpcMesSpotValue(t *testing.T)             // лист "01", 1992-01-31 → 345.3 (год из строки 4 файла, а не из константы)
func TestIpcMesSeriesMetaCoversParsedRows(t *testing.T) // двусторонний контракт с ipcMesSeriesMeta
func TestIpcMesUnknownSheetWarns(t *testing.T)     // книга с листом "05" → предупреждение, разбор листов 01–04 продолжается
func TestIpcMesMissingSheetFails(t *testing.T)     // книга без листа "03" → ошибка с номером листа и именем таблицы
func TestIpcMesFindLink(t *testing.T)              // util.FindLink(testdata/price_page.html, ipcMesLinkSelector) → href с "ipc_mes_"
```

- [ ] **Step 3: Запустить и убедиться, что падает**

Run: `go test ./rosstat/ -run TestIpcMes -v`
Expected: FAIL — имена рядов содержат годы и сноски; тест `TestIpcMesStableSeriesNames` красный.

- [ ] **Step 4: Переписать разбор**

`ipcMesSeriesNames` — карта «номер листа → стабильное имя»: `1` → `Индексы потребительских цен на товары и услуги по Российской Федерации`, `2` → `… на продовольственные товары по Российской Федерации`, `3` → `… на непродовольственные товары по Российской Федерации`, `4` → `… на услуги по Российской Федерации` (точно эти строки — их требует двусторонний тест каталога). Обход: только листы, чьё имя парсится как число (`strconv.Atoi`); номер вне карты — `log.Warnf` и пропуск; отсутствие любого из листов 1–4 — ошибка. Внутри листа: якорная строка — `к концу предыдущего месяца`; годы читаются из строки **над** ней (колонки 1…, до первой пустой; пустая сразу — ошибка); строки месяцев — после якоря: `len(row) < 2` → конец, незнакомое имя месяца → конец блока, номер месяца из `util.MonthsToNum[strings.ToLower(row[0])]`, неперечислимые по возрастанию месяцы → ошибка. Ноль точек на листе — ошибка. Константа `ipcMesYearStart` удаляется.

- [ ] **Step 5: Обновить `Import()` и каталог**

`colly` → `util.GetPage` + `util.FindLink` (`ipcMesLinkSelector = "a[href*=\"ipc_mes_\"]"` — вместо нынешней позиционной CSS-цепочки, стандарт §6.3); в `ipcMesSeriesMeta` заменить 4 значения `Series` на новые стабильные имена и уточнить `Origin` (имя ряда задаётся кодом, а не заголовком листа).

- [ ] **Step 6: Прогнать тесты**

Run: `go test ./rosstat/ -run 'TestIpcMes' -v -count=1`
Expected: PASS.

- [ ] **Step 7: Коммит**

```bash
git add rosstat/ipc_mes.go rosstat/ipc_mes_test.go rosstat/series_meta.go rosstat/series_meta_test.go
git commit -m "ipc_mes: stable code-side series names, layout-driven parsing"
```

---

### Task 7: `ipc_weeks` — перевод на шаблон и защита разбора

**Files:**
- Modify: `rosstat/ipc_weeks.go`
- Create: `rosstat/ipc_weeks_test.go`
- Modify: `rosstat/series_meta_test.go`

**Interfaces:**
- Consumes: `util.GetPage`/`util.FindLink` (T1), `testdata/ipc_weeks.xlsx`, `testdata/price_page.html` (T3), `rosstatIPCWeeksTable` (T2), `appendRecorder` (T4).
- Produces: `ipcWeeksImport(xlsx *excelize.File, batch driver.Batch) error`, `ipcWeeksLinkSelector`; `IpcWeeksStat` больше не имеет собственных `Import`/`export`.

- [ ] **Step 1: Написать падающий тест `rosstat/ipc_weeks_test.go`**

```go
func TestIpcWeeksReadsFixture(t *testing.T)      // точек 26 611, рядов 131; живой счётчик 25 691 отличается нетто на +920: −28 (нули, записанные старым разбором из ячеек «…») + 66 (числовые ячейки двух строк «…») + 882 (числовые ячейки 19 строк товаров, отрезанных обрывом листа)
func TestIpcWeeksMetaCoversParsedRows(t *testing.T) // односторонний контракт: каждый разобранный товар описан в ipcWeeksSeriesMeta() (стандарт §11.4)
func TestIpcWeeksSpotValue(t *testing.T)         // лист "2026", товар "Говядина (кроме бескостного мяса), кг", 2026-01-12 → 100.65
func TestIpcWeeksUnknownProductWarns(t *testing.T) // товар вне ipcWeeksProducts → log warn, точка всё равно пишется
func TestIpcWeeksMissingHeaderFails(t *testing.T) // лист без строки "Наименование" → ошибка с именем таблицы
func TestIpcWeeksShortRowDoesNotPanic(t *testing.T) // строка данных короче шапки → разбор не паникует
func TestIpcWeeksFindLink(t *testing.T)          // util.FindLink(testdata/price_page.html, ipcWeeksLinkSelector) → href с "Nedel_ipc"
```

- [ ] **Step 2: Запустить и убедиться, что падает**

Run: `go test ./rosstat/ -run TestIpcWeeks -v`
Expected: FAIL — функции разбора в нынешнем виде нет как отдельной единицы (`ipcWeeksImport` не существует).

- [ ] **Step 3: Убрать собственный цикл `Import()`**

Всё тело `IpcWeeksStat.Import` (DDL, `PrepareBatch`, `Send`, `UpsertSeriesCatalog`, `CreateView`) удалить: `IpcWeeksStat` остаётся обёрткой со встроенным `util.ClickHouseImport` с полями `TableName`, `CreateTable` (в `%s`-форме), `ImportFunc: ipcWeeksImport`, `AfterImport: afterRosstatMacro(ipcWeeksSeriesMeta())`. Точки контроля публикации нет — поля `PublicationCountPoints` не заполняются (стандарт §9.3).

- [ ] **Step 4: Внести разбор и защиту**

`ipcWeeksImport` — нынешний `parseIpcWeeks` + запись в батч, с правками: `RawCellValue: true`; `fmt.Printf` удалён; `percent, _ :=` заменён проверкой ошибки; `util.MonthsToNum[...]` — с проверкой наличия ключа (иначе месяц 0 и невнятная ошибка `time.Parse` позже); `rows[fieldIdx][j+1]` — с проверкой границы (`j+1 < len(rows[fieldIdx])`), иначе паника на строке данных длиннее шапки; лист без строки `Наименование` — ошибка; пустой разбор — ошибка. Продукт вне `ipcWeeksProducts` — `log.Warnf` с именем товара и импорт (стандарт §6.6: падение недельного шага остановило бы последующие шаги цепочки).

**Граница блока данных и ячейки «нет данных».** Лист заканчивается строкой-сноской: у неё одна заполненная ячейка (`len(row) < 2`) — это и есть терминатор. Нынешнее правило «оборвать лист, если вторая ячейка строки не число» — дефект: строка товара, у которого значений нет вовсе, помечена `…` (`Полет в салоне экономического класса самолета…` в 2022, `Поездки в отдельные страны Юго-Восточной Азии…` в 2026), и обрыв на ней выбрасывает все строки ниже — 15 в 2022 и 4 в 2026. Живое подтверждение: 92 недельных ряда начинаются с 2022-01-10, ещё 22 — с 2023-01-09, то есть у товаров ниже места обрыва 2022 года в таблице нет вовсе. Ячейка со значением `…` — это «данных не имеется» (источник сам расшифровывает маркер строкой `… - данных не имеется`), такая ячейка точкой не становится, но строку и **лист не обрывает**; любой другой нечисловой текст — ошибка (§6.4). Итог: **26 611 точек, 131 ряд**; `ipcWeeksProducts` получает три недостающих товара (`Поездка на отдых в Турцию, поездка`, `Поездка на отдых на Черноморское побережье России, поездка`, `Поездки в отдельные страны Юго-Восточной Азии, поездка`) — односторонний контракт §11.4 требует описания каждого разобранного ряда.

- [ ] **Step 5: Перевести ссылку на `util`**

Коллектор `colly` → `util.GetPage` + `util.FindLink` (`ipcWeeksLinkSelector = "a[href*=\"Nedel_ipc\"]"` — вместо позиционной CSS-цепочки); в `Import()` — override шаблона: получить страницу, найти href, выставить `DataUrl = rosstatUrl + href`, вызвать `s.ClickHouseImport.Import(ctx, conn)`.

- [ ] **Step 6: Прогнать тесты**

Run: `go test ./rosstat/ -run TestIpcWeeks -v -count=1`
Expected: PASS.

- [ ] **Step 7: Коммит**

```bash
git add rosstat/ipc_weeks.go rosstat/ipc_weeks_test.go rosstat/series_meta_test.go
git commit -m "ipc_weeks: port to util.ClickHouseImport, harden parsing"
```

---

### Task 8: Каталог, витрина и проверки MCP

**Files:**
- Modify: `scripts/mcp_check.py`
- Modify: `rosstat/series_meta.go` (комментарий витрины, если состав изменился)

**Interfaces:**
- Consumes: результат T4–T7 (все четыре ряда описаны), `v_rosstat_macro`, `v_series_catalog`.
- Produces: строки проверки контура Росстата в `scripts/mcp_check.py`.

- [ ] **Step 1: Добавить проверки в `scripts/mcp_check.py`**

По образцу блоков `v_cbr_macro` и `v_bea_pce` (там же — хелперы `expect`/`text`):

```python
# Контур Росстата: витрина видна и НЕПУСТА (пустая витрина от неотработавшего
# импортёра успехом не считается), каталог описывает все 138 рядов источника,
# четыре сырые таблицы агенту закрыты.
failed += not expect("витрина v_rosstat_macro видна", "v_rosstat_macro" in tables, "list_tables default")
rosstat = text(await session.call_tool("run_query", {"query": "SELECT count() AS rows, uniqExact(series) AS series FROM v_rosstat_macro"}))
# ожидание: строк > 0 (число меняется с каждым прогоном источника), рядов 138
# (4 ИПЦ + 128 недельных + 5 ВВП + 1 зарплата)
rosstat_catalog = text(await session.call_tool("run_query", {"query": "SELECT count() AS n FROM v_series_catalog WHERE source = 'rosstat'"}))
# ожидание: n == 138
for raw in ("rosstat_ipc_mes", "rosstat_ipc_weeks", "rosstat_salaries_mes", "rosstat_vvp_kvartal"):
    denied = text(await session.call_tool("run_query", {"query": f"SELECT count() FROM {raw}"}))
    failed += not expect(f"сырая {raw} закрыта", denied_clickhouse(denied), f"отказ ClickHouse на {raw} (ожидаемо)")
```

Использовать существующие в файле хелперы проверки отказа (`497`/`ACCESS_DENIED`) — как в строке про `cbr_households_b_mes`; не изобретать новую логику.

- [ ] **Step 2: Прогнать проверку**

Run: `make mcp-check`
Expected: `ПРОВЕРКА ПРОЙДЕНА` (exit 0). До выполнения T9 число рядов может быть 132 — это ожидаемое состояние до первого живого прогона; в этом случае зафиксировать и вернуться к шагу после T9.

- [ ] **Step 3: Коммит**

```bash
git add scripts/mcp_check.py rosstat/series_meta.go
git commit -m "mcp_check: verify rosstat view, catalog and closed raw tables"
```

---

### Task 9: Живая приёмка и миграция данных

**Files:**
- Modify (по результату): `docs/ROADMAP_KEY_RATE.md` (§4.1, §6, §8) — статусы и приёмочный блок

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: запись живой приёмки, на которую ссылается роадмап.

- [ ] **Step 1: Полная проверка сборки и тестов**

Run: `make all`
Expected: `0 issues.` (gofmt + golangci-lint), `go vet` без замечаний, `go test -race ./...` — все пакеты `ok`, обе сборки. Отдельно свежим прогоном: `go test -race -count=1 ./rosstat/ ./cbr/ ./util/`.

- [ ] **Step 2: Grep-чистота пакета**

Run: `grep -rn "gocolly\|fmt\.Print" --include=*.go rosstat/`
Expected: пусто.

- [ ] **Step 3: Прогнать четыре импортёра по одному**

Run: `make import STAT=rosstat_ipc_mes`, затем `rosstat_ipc_weeks`, `rosstat_salaries_mes`, `rosstat_vvp_kvartal`.
Expected: rc=0 у всех; новые таблицы создаются **рядом** со старыми, `v_model_inputs` продолжает читать старые.

- [ ] **Step 4: Сверить состав и данные**

Run (через MCP по витрине, сырые закрыты) и записать факты: число рядов и строк по каждому из четырёх источников, крайние даты.
Ожидание: `rosstat_ipc_mes` — 4 ряда / 1716 строк (имена рядов новые, значения совпадают с базовой линией); `rosstat_ipc_weeks` — 131 ряд / 26 611 строк (прежние 128 / 25 691 объясняются: −28 нулей, записанных старым разбором из ячеек «…», +948 числовых ячеек 19 строк товаров, которые обрыв листа отрезал, и +3 товара, которых в таблице не было вовсе); `rosstat_salaries_mes` — 1 ряд, 421 точка, 1991-08-01…2026-08-01 (нулей января–июня 1991 нет; дата точки — 1-е число месяца, следующего за отчётным, поэтому июль 1991 записан на 1991-08-01, а июль 2026 — на 2026-08-01); `rosstat_vvp_kvartal` — 5 рядов, 302 точки, с 2011-04-01 по 2026-07-01 (2 квартал 2026).

- [ ] **Step 5: Spot-сверка с публикацией Росстата**

Сверить и записать: средняя номинальная зарплата за июнь 2026 (из `salaries_mes`), ВВП в текущих ценах за 1кв и 2кв 2026 (лист «2»), ИФО ВВП за те же кварталы (лист «12»). Источник сверки — открытая публикация Росстата (страница рынка труда / национальных счётов).

- [ ] **Step 6: Повтор — no-op**

Run: `make import STAT=rosstat_ipc_mes`, `rosstat_salaries_mes`, `rosstat_vvp_kvartal`
Expected: `no-op, версия … точек на месте` без скачивания; `rosstat_ipc_weeks` — идемпотентный повтор, счётчики в витрине не меняются.

- [ ] **Step 7: Переключить витрины контура золота**

Run: `make import STAT=gold_views`
Expected: `v_model_inputs` пересоздана на новых таблицах; запрос `SELECT ipc_mes_last, ipc_mes_last_date, ipc_week_ytd, ipc_week_ytd_date FROM v_model_inputs` — значения не NULL и совпадают с базовой линией.

- [ ] **Step 8: Удалить старые таблицы и осиротевшие версии**

Проверить, что старых имён нет ни в коде, ни в SQL, ни в дашбордах (`grep -rn "ipc_mes\|ipc_weeks\|salaries_mes\|vvp_kvartal" --include=*.go --include=*.sql . | grep -v rosstat_`). Затем admin-командой `make ch-sql FILE=<файл>` удалить `ipc_mes`, `ipc_weeks`, `salaries_mes`, `vvp_kvartal` и осиротевшие строки `source_publications` по четырём старым ключам. Временный SQL-файл не коммитить.
Expected: витрина `v_rosstat_macro` по-прежнему читается агентом, `v_model_inputs` не деградировала.

- [ ] **Step 9: MCP и расписание**

Run: `make mcp-user` (если требуются новые гранты — их нет) и `make mcp-check`; `dagu validate dagu/rosstat.yaml`.
Expected: `ПРОВЕРКА ПРОЙДЕНА`; exit 0 (единственное сообщение — общее предупреждение `auth.mode`).

- [ ] **Step 10: Обновить статусы роадмапа**

`docs/ROADMAP_KEY_RATE.md`: §4.1 — строки `salaries_mes`/`vvp_kvartal` больше не «пусто», с числом строк и диапазоном; §6 фаза 2 — первый шаг закрыт ссылкой на блок приёмки; §8 — строка риска про технический долг `rosstat/` обновлена. Приёмочный блок — по образцу блока фазы 3 (`cbr/`): команда → результат, плюс названные расхождения (raw-точность, изменение имён рядов `ipc_mes`, рост каталога 132 → 138).

- [ ] **Step 11: Коммит**

```bash
git add docs/ROADMAP_KEY_RATE.md
git commit -m "roadmap: phase 2 first step done — rosstat empty series fixed, package refactored"
```

---

### Task 10: Документация и ссылки на стандарт

**Files:**
- Modify: `AGENTS.md`, `ARCHITECTURE.md`, `README.md`, `docs/ROADMAP_DCF_POLYUS.md`, `docs/KEY_RATE_PUBLICATIONS.md`

**Interfaces:**
- Consumes: результат T1–T9.
- Produces: документация, синхронная с кодом.

- [ ] **Step 1: `AGENTS.md`**

В правило 7 (новые импортёры) и в «Обязательный порядок в начале сессии» добавить ссылку на `docs/IMPORTER_PACKAGE_STANDARD.md` (стандарт пакета импортёров; читать перед новым пакетом и перед рефакторингом существующего).

- [ ] **Step 2: `ARCHITECTURE.md`**

§2 — состав `rosstat/` (новые имена, реестр); §3 — ссылка на стандарт вместо разрастания шаблонов, эталон «ссылка со страницы» переводится с `colly` на `util.GetPage`/`util.FindLink`; §6.1 — строки импортёров; §6.2 — имена таблиц; §6.3 — состав `v_rosstat_macro`; §6.5; §7 — расписание и id шагов; §8 пп. 11/13 (тесты; список объявлений точек контроля); запись «ранее 2026-10-14» в шапку.

- [ ] **Step 3: `README.md`**

Строка таблицы импортёров (новые имена, ссылка на стандарт), §478 (список импортёров «ссылка со страницы»), §480 (HTTP 426), §497 (`v_model_inputs`), список тестовых пакетов (§259/§262).

- [ ] **Step 4: Остальные документы**

`docs/ROADMAP_DCF_POLYUS.md` строки 28 и 89 (имена `ipc_mes`/`ipc_weeks`), `docs/KEY_RATE_PUBLICATIONS.md` §6 (зарплаты больше не «нет обновления»).

- [ ] **Step 5: Сверить по skill `sync-readme-architecture`**

Пройти чек-лист skill `.kimi-code/skills/sync-readme-architecture/SKILL.md`: пакеты, импортёры, таблицы, витрины, env-переменные, цели Makefile, DAG-файлы, CI.

- [ ] **Step 6: Коммит**

```bash
git add AGENTS.md ARCHITECTURE.md README.md docs/
git commit -m "docs: importer package standard and rosstat rename sync"
```

---

## Self-Review

**1. Покрытие спеки.** §1.1 (стандарт) — T0 сделан документом + ссылки в T10; §1.2 (починка рядов) — T4, T5 + приёмка T9; §1.3 (рефакторинг) — T1–T3, T6–T8; Р1 — документ; Р2 — T2; Р3 — T1; Р4 — T4/T5/T6; Р5 — T4–T7; Р6 — T4 Step 4/6; Р7 — T7 Step 4; Р8 — T9 Steps 3–8; §6 приёмка — T9. Спека покрыта задачами полностью.

**2. Скан шагов.** Каждый шаг даёт ровно одно действие с проверяемым результатом; ни один не содержит «доработать по ситуации». Шаги, оставленные на выбор исполнителя (какой именно лист/колонка в фикстуре), сопровождаются точным ожиданием из разметки, снятой с живого файла.

**3. Согласованность имён.** `util.GetPage`/`util.FindLink` (T1) используются в T4–T7; `rosstat*Table` (T2) — в T2–T8; `xxxImport(xlsx, batch)` (T4, T5, T6, T7) — единая сигнатура; `appendRecorder` определён в T4 и переиспользуется T5–T7; `rosstatImporters` — T2 и T8.

**4. Review Focus.** Пять классов закрыты тестами: 1 — T4/T5/T6/T7 (пустая книга/нет листа/нет шапки → ошибка); 2 — T4/T5/T6 (`…StableSeriesNames`, spot-значения, двусторонний каталог); 3 — T6/T7 (`UnknownSheetWarns`, `UnknownProductWarns`); 4 — T4 (`SkipsZeroCells`); 5 — T9 шаги 6–8.

**5. Пропорция.** План ссылается на стандарт и спеку вместо пересказа их правил; тела функций не приводятся, кроме тех мест, где сигнатура и тесты не определяют решение (алгоритм обхода колонок с переносом года в `vvp_kvartal`, правила обхода блока месяцев в `ipc_mes`).
