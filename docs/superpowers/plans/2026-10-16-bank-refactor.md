# Рефакторинг пакета `bank/` — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Пакет `bank/` приведён к стандарту импортёров: реестр с тестом-энфорсером, `colly` и построчные вставки убраны, `domrf_mortgage` читает живой источник (понедельная цепочка `dagu/bank.yaml` оживает), ВТБ получает пофайловые версии с гейтом, `sber_finansovie_rezultaty` входит в винтажный архив, Т-Банк развязан, банковские отчётные метрики читаются агентом в компанейском слое (`company_financials`) и через `v_bank_macro`.

**Architecture:** Транспорты — только `util` (`GetPage`+goquery, `GetXlsx`/`GetFileBytes`, `GetCSV`); импортёры — на шаблоне `util.ClickHouseImport` (`ImportFunc`; собственный `Import()` — у вычисляемой ссылки ДОМ.РФ и у набора файлов ВТБ с гейтом `util.PublicationKnown`); производные — общий `afterBankMacro` (каталог + витрины `v_bank_macro`/`v_bank_sber_spending`); банковские отчётные метрики публикует внутренний импортёр `bank_company_metrics`, читающий сырые таблицы и пишущий в общую `company_financials` (компания — значение, не схема).

**Tech Stack:** Go (модуль репозитория), `github.com/PuerkitoBio/goquery`, `github.com/xuri/excelize/v2`, `clickhouse-go/v2`, `logrus`, Dagu, ClickHouse 26.10, `mcp-clickhouse` через `scripts/mcp_check.py`.

**Spec:** `docs/superpowers/specs/2026-10-16-bank-refactor-design.md` — обязательна к прочтению целиком. Ссылки «спека §N» ниже — на неё, «стандарт §N» — на `docs/IMPORTER_PACKAGE_STANDARD.md`, «ARCHITECTURE §N» — на `ARCHITECTURE.md`.

## Global Constraints

- Секреты только из окружения (правило 1 AGENTS.md); HTTP — только через `util` (правило 4); вставка — один `PrepareBatch` → `Append` с проверкой → `Send` (правило 2); ошибки не проглатывать (правило 6); логирование — logrus, `fmt.Print*` в `bank/` не остаётся (стандарт §5.6).
- Имена и таблицы **не меняются** (спека §1.4, О1): `domrf_mortgage`, `rus_vtb_group_ifrs`, `tbank_group_ifrs`, `sber_finansovie_rezultaty`, `sber_consumper_spending_index`, `sber_izmenenie_trat` + новый импортёр `bank_company_metrics`. `Name() == TableName` у шести импортёров; у `bank_company_metrics` `Name()` = `bank_company_metrics`, таблица — общая `company_financials` (литеральный allowlist владельцев, как `macro_series` у `cbr/`).
- Точки контроля публикации объявлены ровно у трёх: `domrf_mortgage`, `rus_vtb_group_ifrs`, `sber_finansovie_rezultaty`; у остальных (включая `bank_company_metrics`) — нет.
- Расписание `dagu/bank.yaml` — шесть шагов в порядке: `sber_finansovie_rezultaty`, `sber_consumper_spending_index`, `sber_izmenenie_trat`, `rus_vtb_group_ifrs`, `domrf_mortgage`, `bank_company_metrics`; шага `tbank_group_ifrs` в расписании нет (О2) — энфорсер проверяет отсутствие явно.
- `company_financials` **не пересоздаётся**; меняются копии DDL (канонический `ARCHITECTURE.md` §6.2, `views/company.go:37`, `polyus/import.go:67`, новый `bank/company_metrics.go`) и по факту — `ALTER … period_type` (задача 12). Данные `PLZL` не трогаются.
- Разбор — чистая функция без сети (стандарт §6.1); значения — сырые ячейки (`RawCellValue`, §6.2); пустой/неразобранный результат — ошибка с контекстом, не «0 строк» (§6.4–6.5); `panic` запрещён (§6.5).
- Фикстуры — урезанные живые артефакты в `bank/testdata/` (§11.1–11.3); тесты пакета офлайн. Значения-споты вписываются в тесты литералами из разведки живых файлов.
- «Данные не меняются» (§15.2): расхождения допустимы только у ДОМ.РФ (переразбор на сырые значения) и ВТБ (порядок файлов) — называются поимённо в протоколе задачи 12.
- Персональные `init()` в файлах импортёров живут до задачи 8 (реестр их забирает) — раньше этого трогать регистрацию нельзя, иначе `make import STAT=…` перестанет находить импортёр.
- Каждый коммит — после `gofmt` и зелёного адресного `go test -race`; финально — `make all` (задача 12). Команды с ClickHouse и сетью — только через `make` (AGENTS.md §«Окружение разработки»).

## Review Focus

1. **ДОМ.РФ: страница отдала не тот документ** (HTML-заглушка, JS-challenge, JSON без блока «Топ ипотечных банков») → `Import()` обязан упасть с текстом, называющим страницу и блок, и не записать ни строки; красный тест — «блока нет» (задача 1).
2. **ВТБ: список файлов «грязный»** (PDF рядом с xlsx; имена с опечаткой `jule` и хвостовым `_`; дубли ссылок) → только `.xlsx`, дедуп, сортировка по дате отчёта по возрастанию, PDF не качается (задача 4).
3. **Повторный прогон** банковской метрики не плодит строк `company_financials` и не меняет значения (ключ с `loaded_at` одной на прогон) — тест формы ключа (задача 7) и приёмка повтора (задача 12).
4. **Пропавшая/переименованная подпись листа**: у сырого импортёра отсутствие обязательного листа/якоря — ошибка (задачи 3, 5); у маппера `bank_company_metrics` отсутствующая ожидаемая подпись — `warn` и пропуск метрики, прогон не падает (задача 7).
5. **Т-Банк**: ссылка ведёт на PDF → падение до разбора с текстом, содержащим имя таблицы; шага нет в расписании, и это проверено тестом (задачи 6, 8).

---

### Task 0: Базовая линия и инвентарь (стандарт §15.1)

**Files:**
- Create: `build/bank_refactor_baseline.sql` (локальный; каталог `build/` в git не коммитится)
- Create: `.superpowers/sdd/2026-10-16-bank-refactor/bank-baseline.md` (локальный снимок)

**Interfaces:** ничего для других задач — опорная точка сверки §15.2.

- [ ] **Step 1: Проверить, что ClickHouse отвечает**

Run: `make ch-status`
Expected: «ClickHouse отвечает на порту …». Если не отвечает — доложить владельцу и остановиться (`make ch-up` ассистент не выполняет).

- [ ] **Step 2: Собрать SQL базовой линии**

Запросом к `system.tables` (database `default`) определить, какие из таблиц существуют: `domrf_mortgage`, `rus_vtb_group_ifrs`, `tbank_group_ifrs`, `sber_finansovie_rezultaty`, `sber_consumper_spending_index`, `sber_izmenenie_trat`, `company_financials`. Для каждой существующей — строка вида:

```sql
SELECT 'domrf_mortgage' AS t, count() AS rows, uniqExact(name) AS series, min(date) AS d0, max(date) AS d1 FROM domrf_mortgage FINAL;
```

Для `company_financials`: `SELECT company, count(), uniqExact((company, metric, period)) FROM company_financials FINAL GROUP BY company` (опорная точка — 347 строк `PLZL`). Отсутствующие таблицы в SQL не включаются.

- [ ] **Step 3: Прогнать и снять снимок**

Run: `make ch-sql FILE=build/bank_refactor_baseline.sql`
Перенести вывод в `.superpowers/sdd/2026-10-16-bank-refactor/bank-baseline.md` таблицей + строка «дата прогона» + список отсутствующих таблиц («импортёр ни разу не отрабатывал на этой БД»).

- [ ] **Step 4: Зафиксировать отсутствие коммита**

Коммита нет: оба артефакта локальные (стандарт §15.1). В отчёте задачи — числа базовой линии.

**Модель субагента:** `joingonka/zai-org/GLM-5.3-Flash` (мелкая задача: SQL + снимок).

---

### Task 1: `domrf_mortgage` — разведка, фикстуры, чистый разбор

**Files:**
- Modify: `bank/domrf_mortgage.go` (функции разбора; импортёр тронет задача 2)
- Create: `bank/testdata/domrf_mortgage_page.html`
- Create: `bank/testdata/domrf_mortgage.xlsx`
- Create: `bank/domrf_mortgage_test.go`
- Create: `bank/harness_test.go` (харнесс `appendRecorder` — копия по образцу `cbr/series_meta_test.go:26-64`)

**Interfaces:**
- Produces: `type domrfPoint struct{ Series string; Date time.Time; Value float64 }`; `func domrfXlsSrcFromPage(page []byte) (src string, err error)`; `func parseDomrfSheet(xlsx *excelize.File) ([]domrfPoint, error)`; `func appendDomrfPoints(batch driver.Batch, points []domrfPoint) error`; константы `domrfDataUrlPrefix = "https://xn--d1aqf.xn--p1ai"`, `domrfSheetAnchor = "Ставки предложения"`, `domrfTimeLayout` (точное значение — из разведки), `domrfPageBlockAnchor = "Топ ипотечных банков"`.
- Consumes: `util.GetPage` (есть), `domrfAnalyticsUrl` (есть в файле), харнесс записи (стандарт §11.6).

- [ ] **Step 1: Разведка живого файла (числа — в отчёт задачи)**

```bash
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
curl -skS -A "$UA" -o /tmp/domrf_page.html "https://xn--d1aqf.xn--p1ai/analytics/mortgage/"
grep -oE '"file2":\{"src":"[^"]+"' /tmp/domrf_page.html        # id1/id2 — из этого поля
curl -skSL -A "$UA" -o /tmp/domrf.xlsx "https://xn--d1aqf.xn--p1ai/api/v2/content/download/<id1>/<id2>/"
unzip -p /tmp/domrf.xlsx xl/workbook.xml | grep -oE 'name="[^"]*"'            # имя листа с «Ставки предложения»
unzip -p /tmp/domrf.xlsx xl/workbook.xml.rels | grep -oE 'sheet[0-9]+\.xml'   # сопоставить имя → sheetN
unzip -p /tmp/domrf.xlsx xl/worksheets/<sheetN>.xml | head -c 4000           # форма первых строк
```

Зафиксировать: точный `src` (он войдёт в тест литералом); имя листа-якоря; где шапка месяцев (якорь), какие строки — банки, как записана дата колонки (месяц-число или `dd-mm-yy` → `domrfTimeLayout`); первые/последние точки как spot-кандидаты; есть ли пустые/`-` ячейки (спека §6.7).

- [ ] **Step 2: Фикстуры**

Страницу сохранить как есть: `cp /tmp/domrf_page.html bank/testdata/domrf_mortgage_page.html` (127 КБ). Книга — «хирургия zip» (стандарт §11.2): оставить `[Content_Types].xml`, `_rels/.rels`, `xl/workbook.xml`, `xl/_rels/workbook.xml.rels`, `xl/sharedStrings.xml`, `xl/styles.xml` и лист-якорь с его `_rels`; удалить прочие `xl/worksheets/*` и `_rels`; проверить `go test` (открытие фикстуры). Если excelize не открывает урезанную книгу — коммитить полный файл как есть (259 КБ) и назвать это в отчёте.

- [ ] **Step 3: Падающие тесты (`bank/domrf_mortgage_test.go`; харнесс — `bank/harness_test.go`)**

```go
func TestDomrfXlsSrcFromPage(t *testing.T)        // фикстура страницы → src == "<точный из разведки>"
func TestDomrfXlsSrcFromPageNoBlock(t *testing.T) // страница без блока (минимальный []byte) → ошибка, текст называет «Топ ипотечных банков»
func TestParseDomrfSheet(t *testing.T)            // фикстура книги: len(points) == <из разведки>; 2 spot-значения (дата+значение+Series); набор Series == банки из разведки
func TestParseDomrfSheetFailFast(t *testing.T)    // книга без листа-якоря → ошибка с текстом про «Ставки предложения»
func TestAppendDomrfPoints(t *testing.T)          // appendRecorder: форма строки (Series, Date, Value); ошибка Append не проглатывается
```

Харнесс копируется из `cbr/series_meta_test.go:26-64` под именем `appendRecorder` (в пакете `bank` он ещё не заведён; чужой тестовый файл импортировать нельзя).

- [ ] **Step 4: Реализовать**

`domrfXlsSrcFromPage` — goquery над байтами страницы (`goquery.NewDocumentFromReader(bytes.NewReader(page))`, образец разбора страницы — `cbr/dip_page.go`): найти блок по `domrfPageBlockAnchor` и взять `file2.src`; не найдено — ошибка. `parseDomrfSheet` — лист по `domrfSheetAnchor` (поиск по имени, §6.3), сырые ячейки (`GetRows(sheet, excelize.Options{RawCellValue: true})`, §6.2), fail-fast. `appendDomrfPoints` — `batch.Append(Series, Date, float32(Value))` с проверкой `err` (приведение к `Float32`-колонке — как у импортёров Сбера, `bank/sber_csi.go`).

- [ ] **Step 5: Run**

Run: `go test -race -count=1 ./bank/ -run 'Domrf' -v`
Expected: PASS. Затем `go vet ./bank/`.

- [ ] **Step 6: Commit**

```bash
git add bank/domrf_mortgage.go bank/domrf_mortgage_test.go bank/harness_test.go bank/testdata/domrf_mortgage_page.html bank/testdata/domrf_mortgage.xlsx
git commit -m "bank: domrf mortgage page and sheet parsers with fixtures"
```

**Модель субагента:** `deepseek/deepseek-flash` (новые функции + фикстуры + тесты, артефакты крупные).

---

### Task 2: `domrf_mortgage` — импортёр на шаблоне, точка контроля

**Files:**
- Modify: `bank/domrf_mortgage.go` (константы, структура, `Import`, регистрация)
- Modify: `bank/domrf_mortgage_test.go` (тест DDL-формы)

**Interfaces:**
- Produces: `domrfMortgageDdl` — `%s`-форма; `domrfCountPoints = "SELECT count() FROM domrf_mortgage FINAL"` (литерал); `type DomrfMortgageStat struct{ util.ClickHouseImport }`; `func (s *DomrfMortgageStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error)`; `domrfImportFunc func(*excelize.File, driver.Batch) error`; переменная `domrfImporter *DomrfMortgageStat` (её зарегистрирует реестр задачи 8).
- Consumes: `domrfXlsSrcFromPage`, `parseDomrfSheet`, `appendDomrfPoints` (задача 1).

- [ ] **Step 1: Переписать константы и структуру**

DDL — `%s`-форма (типы не меняются): `CREATE TABLE IF NOT EXISTS %s (name LowCardinality(String), date Date, rate Float32) ENGINE = ReplacingMergeTree ORDER BY (name, date);`. Удалить: `domrfMortgageInsert`, `domrfMortgageField`, оба `fmt.Printf`, проглоченную ошибку (`value, _ :=`). **Константа `domrfMortgageTable` остаётся** — на неё ссылается `bankMacroView.Tables` в задаче 9. Структура — встроенный `util.ClickHouseImport`; своя `Name()` убирается (даёт шаблон). Собирается литералом:

```go
var domrfImporter = &DomrfMortgageStat{ClickHouseImport: util.ClickHouseImport{
	TableName: domrfMortgageTable, CreateTable: []string{domrfMortgageDdl},
	ImportFunc: domrfImportFunc, PublicationCountPoints: domrfCountPoints,
}}
```

Регистрация — персональный `init()` **сохраняется до задачи 8** (её снимет реестр), но аппендит `domrfImporter`.

- [ ] **Step 2: `Import()` — образец `cbr/mo_br.go:57-71`**

```go
func (s *DomrfMortgageStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	page, err := util.GetPage(domrfAnalyticsUrl)
	if err != nil { return count, fmt.Errorf("domrf_mortgage: %s: %w", domrfAnalyticsUrl, err) }
	src, err := domrfXlsSrcFromPage(page)
	if err != nil { return count, fmt.Errorf("domrf_mortgage: %s: %w", domrfAnalyticsUrl, err) }
	imp := s.ClickHouseImport // локальная копия: общую структуру импортёра мутировать нельзя
	imp.DataUrl = domrfDataUrlPrefix + src
	return imp.Import(ctx, conn)
}
```

- [ ] **Step 3: Тест DDL-формы**

`TestDomrfMortgageDdlShape`: `strings.Count(domrfMortgageDdl, "%s") == 1` и литерал не содержит имени таблицы.

- [ ] **Step 4: Run**

Run: `go build ./... && go test -race -count=1 ./bank/`
Expected: PASS (все тесты пакета).

- [ ] **Step 5: Commit**

```bash
git add bank/domrf_mortgage.go bank/domrf_mortgage_test.go
git commit -m "bank: domrf mortgage importer on template with publication checkpoint"
```

**Модель субагента:** `deepseek/deepseek-flash` (переписывание файла + идиом копии шаблона).
---

### Task 3: `rus_vtb_group_ifrs` — разбор книги (баланс, P&L, коэффициенты) и фикстура

**Files:**
- Modify: `bank/vtb_fin_rez.go` (функции разбора; транспорт и импортёр — задача 4)
- Create: `bank/testdata/vtb_ifrs.xlsx` (один живой файл, 42 КБ — коммитится как есть)
- Create: `bank/vtb_fin_rez_test.go`

**Interfaces:**
- Produces: `type vtbPoint struct{ Series string; Date time.Time; Value float64 }`; `func parseVtbBook(xlsx *excelize.File) ([]vtbPoint, error)`; `func appendVtbPoints(batch driver.Batch, points []vtbPoint) error`; константы `vtbBookSheet = "Ключевые балансовые показатели"`, `vtbPnlSheet = "Ключевые показатели P&L"`, `vtbRatiosSheet = "Показатели и коэффициенты"`, `vtbBalanceStop = "Итого собственные средства"`, `vtbTimeLayout` (точное значение — из разведки).
- Consumes: харнесс `appendRecorder` (задача 1).

- [ ] **Step 1: Разведка файла (фикстура + отчёт задачи)**

```bash
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
curl -skSL -A "$UA" -o bank/testdata/vtb_ifrs.xlsx "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-august-2026.xlsx"
unzip -p bank/testdata/vtb_ifrs.xlsx xl/workbook.xml | grep -oE 'name="[^"]*"'
unzip -p bank/testdata/vtb_ifrs.xlsx xl/sharedStrings.xml | grep -oE '<t[^>]*>[^<]{1,90}</t>' | sed -E 's/<[^>]+>//g' | grep -E 'Итого|Чистая прибыль|Возврат|Достаточность|Средства клиентов|Кредиты'
```

Зафиксировать: подписи, реально присутствующие на каждом из трёх листов; формат даты колонок (`vtbTimeLayout`); где обрывается баланс («Итого собственные средства»); spot-кандидаты по двум-трём листам; сколько рядов даёт разбор каждого листа. Сверить с подписями словаря спеки §3.7 — расхождения назвать в отчёте.

- [ ] **Step 2: Падающие тесты (`bank/vtb_fin_rez_test.go`)**

```go
func TestParseVtbBook(t *testing.T)         // фикстура: len(points) == <из разведки>; spot-значения по балансу, P&L и коэффициентам (серия+дата+значение); в наборе серий есть «Чистая прибыль», ROE/ROA, «Достаточность капитала»
func TestParseVtbBookFailFast(t *testing.T) // книга без листа P&L → ошибка с именем листа; без баланса → ошибка
func TestAppendVtbPoints(t *testing.T)      // appendRecorder: форма (Series, Date, Value); ошибка Append не проглатывается
```

- [ ] **Step 3: Реализовать `parseVtbBook`**

Каждый из трёх листов — своим проходом: поиск по признаку (§6.3), сырые значения (`RawCellValue`, §6.2), fail-fast при отсутствии любого из трёх листов или якоря. Балансовый лист — как прежде, до «Итого собственные средства» включительно; плюс листы P&L и коэффициентов. Старый разбор строковой таблицы (`export`, `getVtbIfrsXlsDataUrl`) пока остаётся — его удалит задача 4.

- [ ] **Step 4: Run**

Run: `go test -race -count=1 ./bank/ -run 'Vtb' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bank/vtb_fin_rez.go bank/vtb_fin_rez_test.go bank/testdata/vtb_ifrs.xlsx
git commit -m "bank: vtb ifrs book parser (balance, p&l, ratios) with live fixture"
```

**Модель субагента:** `deepseek/deepseek-flash` (разбор трёх листов + фикстура + тесты).

---

### Task 4: `rus_vtb_group_ifrs` — список ссылок со страницы и импортёр с гейтом

**Files:**
- Modify: `bank/vtb_fin_rez.go` (транспорт и импортёр; `colly` уходит целиком)
- Modify: `bank/vtb_fin_rez_test.go` (тесты страницы)
- Create: `bank/testdata/vtb_results_page.html` (фикстура страницы: как есть — 357 КБ, либо фрагмент с реальными блоками ссылок, разметка не пересобирается)

**Interfaces:**
- Produces: `type vtbLink struct{ Url string; Report time.Time }`; `func vtbIfrsXlsLinks(page []byte) ([]vtbLink, error)`; `vtbCountPoints = "SELECT count() FROM rus_vtb_group_ifrs FINAL"` (литерал); `type VtbIfrs struct{ util.ClickHouseImport }`; `func (s *VtbIfrs) Import(ctx context.Context, conn driver.Conn) (count int64, err error)`; `func vtbIfrsImportFunc(xlsx *excelize.File, batch driver.Batch) error`; `vtbImporter *VtbIfrs`; константы `vtbIfrsLinkSelector = "a[href*=\"rus-vtb-group-ifrs-as-of\"]"`, `vtbIfrsDataUrlPrefix = "https://www.vtb.ru"`.
- Consumes: `vtbIfrsResultsUrl` (есть), `parseVtbBook`/`appendVtbPoints` (задача 3), `util.GetPage`, `util.HeadFingerprint`, `util.PublicationKnown`, `util.ArchiveRestoreMode`.

- [ ] **Step 1: Падающие тесты страницы**

```go
func TestVtbIfrsXlsLinks(t *testing.T)      // фикстура: только .xlsx (pdf отброшены); дедуп; сортировка по дате отчёта по возрастанию; имена с опечаткой «jule» и хвостовым «_» разобраны; URL абсолютные
func TestVtbIfrsXlsLinksEmpty(t *testing.T) // страница без ссылок → ошибка с доменом и ожидаемым именем файла
```

- [ ] **Step 2: Реализовать `vtbIfrsXlsLinks`**

goquery над байтами страницы (образец `cbr/dip_page.go`): собрать `a[href*="rus-vtb-group-ifrs-as-of"]`, отфильтровать `.xlsx`, дедуп по href, дату отчёта вынуть регуляркой из имени (`as-of-<д>-<месяц>-<год>`; месяцы — английские, включая опечатку источника `jule`), сортировка по `Report` по возрастанию. Пустой список — ошибка. `colly`-функцию `getVtbIfrsXlsDataUrl`, старый `export` и `vtbIfrsDataInsert` удалить.

- [ ] **Step 3: Импортёр с гейтом (структура — образец `cbr/dip_bulletin.go:83`; гейт — `util.PublicationKnown`)**

Своя `Import()`: режим восстановления → ошибка с текстом (набор файлов, спека §1.3, до сети); `page, err := util.GetPage(vtbIfrsResultsUrl)`; `links, err := vtbIfrsXlsLinks(page)`; цикл по возрастанию: `fp, herr := util.HeadFingerprint(link.Url)`; если `herr == nil` — `_, known, kerr := util.PublicationKnown(ctx, conn, "rus_vtb_group_ifrs", path.Base(link.Url), fp)`, `known` → пропуск; иначе — локальная копия шаблона с `DataUrl = link.Url` и её `Import(ctx, conn)`, сумма в `count`. После цикла — производные ещё раз (если `s.AfterImport != nil`), чтобы пустая дельта не оставляла каталог/витрину неопубликованными (урок `rosstat_ipc_weeks_bulletins`, спека §3.3). `PublicationCountPoints: vtbCountPoints`; `AfterImport: afterBankMacro(bankSeriesMeta)` подключит задача 9. Персональный `init()` — до задачи 8.

- [ ] **Step 4: Run**

Run: `go build ./... && go test -race -count=1 ./bank/ -run 'Vtb'`
Expected: PASS; `grep -rn "colly" bank/` — пусто.

- [ ] **Step 5: Commit**

```bash
git add bank/vtb_fin_rez.go bank/vtb_fin_rez_test.go bank/testdata/vtb_results_page.html
git commit -m "bank: vtb ifrs importer with per-file versions gate (no colly)"
```

**Модель субагента:** `deepseek/deepseek-flash` (свой `Import` с гейтом + страница-парсер).

---

### Task 5: `sber_finansovie_rezultaty` — `ImportFunc`, листы P&L и коэффициентов

**Files:**
- Modify: `bank/sber_fin_rez.go`
- Create: `bank/testdata/sber_finansovie_rezultaty.xlsx` (живой файл 57 КБ, коммитится как есть)
- Create: `bank/sber_fin_rez_test.go`

**Interfaces:**
- Produces: `type sberFinPoint struct{ Series string; Date time.Time; Value float64 }`; `func parseSberFinBook(xlsx *excelize.File) ([]sberFinPoint, error)`; `func appendSberFinPoints(batch driver.Batch, points []sberFinPoint) error`; `func sberFinImportFunc(xlsx *excelize.File, batch driver.Batch) error`; тип `SberFinRezStat` (бывший `LoansToCorporationsStat`); литерал `sberRpbuCountPoints = "SELECT count() FROM sber_finansovie_rezultaty FINAL"` не меняется.
- Consumes: харнесс `appendRecorder`.

- [ ] **Step 1: Разведка листов + фикстура**

```bash
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
curl -skSL -A "$UA" -o bank/testdata/sber_finansovie_rezultaty.xlsx "https://www.sberbank.com/common/img/uploaded/redirected/com/investor-relations/groupresults/sber_finansovie_rezultaty_ras_2022-2024.xlsx"
unzip -p bank/testdata/sber_finansovie_rezultaty.xlsx xl/workbook.xml | grep -oE 'name="[^"]*"'
```

Зафиксировать: подписи листов «Баланс», «Отчет о прибылях и убытках», «Показатели»/«Капитал»; где «Прибыль за период», Н1.0/Н1.1/Н1.2, Рентабельность активов/капитала; **гранулярность дат** (месяц/квартал — от неё тип периода строк в маппере, задача 7); формат даты колонок; spot-кандидаты.

- [ ] **Step 2: Падающие тесты**

```go
func TestParseSberFinBook(t *testing.T)         // фикстура: len(points) == <из разведки>; spot-значения трёх листов; в наборе серий есть «Прибыль за период», Н1.0/Н1.1/Н1.2, «Рентабельность активов/капитала»
func TestParseSberFinBookFailFast(t *testing.T) // нет «Баланса» или «Отчета о прибылях и убытках» → ошибка с именем листа
func TestAppendSberFinPoints(t *testing.T)      // appendRecorder: форма (Series, Date, Value)
```

- [ ] **Step 3: Конвертировать импортёр**

`CrawFunc` → `DataUrl` (статичный `sberRpbuXlsDataUrl`) + `ImportFunc: sberFinImportFunc`; удалить `sberRpbuCraw`, пакетный `ctx` и построчный `conn.Exec`; структура — `SberFinRezStat` со встроенным `util.ClickHouseImport` (DDL `%s`-форма сохраняется); `PublicationCountPoints: sberRpbuCountPoints` (литерал не меняется). Импортёр переименовать в `sberFinRezImporter`. Персональный `init()` — до задачи 8. После конвертации импортёр попадает в винтажный архив (стандарт §9.6) — это следствие ветки `ImportFunc`.

- [ ] **Step 4: Run**

Run: `go build ./... && go test -race -count=1 ./bank/ -run 'SberFin'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bank/sber_fin_rez.go bank/sber_fin_rez_test.go bank/testdata/sber_finansovie_rezultaty.xlsx
git commit -m "bank: sber fin results on ImportFunc (archive coverage), p&l and ratios sheets"
```

**Модель субагента:** `deepseek/deepseek-flash` (конвертация + расширение разбора + фикстура + тесты).

---

### Task 6: `tbank_group_ifrs` — развязка таблицы и шаблон

**Files:**
- Modify: `bank/tbank_fin_rez.go`
- Modify: `util/clickhouse_import.go` (текст ошибки excelize: добавить имя таблицы, ~строка 116)
- Create: `bank/tbank_fin_rez_test.go` (маленький: форма DDL)

**Interfaces:**
- Produces: `tbIfrsDdl` — `%s`-форма с колонкой `balance Float64` (спека §3.5); `type TbIfrs struct{ util.ClickHouseImport }`; `func tbIfrsImportFunc(xlsx *excelize.File, batch driver.Batch) error`; `tbIfrsImporter *TbIfrs`; своя `tbIfrsTimeLayout` (значение — как у ВТБ). Точки контроля нет.
- Consumes: `tbIfrsUrl` (есть), `tbIfrsTable` (есть).

- [ ] **Step 1: Тест DDL-формы**

`TestTbIfrsDdlShape`: ровно один `%s`, в литерале `Float64`, нет `rus_vtb_group_ifrs` и нет имени `tbank_group_ifrs`.

- [ ] **Step 2: Развязать**

Свои константы: `tbIfrsTable` остаётся `tbank_group_ifrs`; `tbIfrsDdl` — своя `%s`-форма (`name LowCardinality(String), date Date, balance Float64`); удалить использования `vtbIfrsTableDdl`/`vtbIfrsDataInsert`/`vtbIfrsTimeLayout` из этого файла. Структура на шаблоне: `TableName: "tbank_group_ifrs"`, `CreateTable: []string{tbIfrsDdl}`, `DataUrl: tbIfrsUrl` (текущий PDF), `ImportFunc: tbIfrsImportFunc`. Разбор переводится на батч механически; фикстуры нет — разбор не верифицирован (отступление О4 — назвать в докблоке файла). Персональный `init()` — до задачи 8.

- [ ] **Step 3: Текст ошибки шаблона**

В `util/clickhouse_import.go` (~строка 116): `fmt.Errorf("excelize %v", err)` → `fmt.Errorf("%s: excelize %v", s.TableName, err)`.

- [ ] **Step 4: Run + Commit**

Run: `go build ./... && go test -race -count=1 ./bank/ ./util/`
```bash
git add bank/tbank_fin_rez.go bank/tbank_fin_rez_test.go util/clickhouse_import.go
git commit -m "bank: decouple tbank ifrs table from vtb, importer on template"
```

**Модель субагента:** `joingonka/zai-org/GLM-5.3-Flash` (правки двух существующих файлов, единичные артефакты малые).
---

### Task 7: `bank_company_metrics` — словарь метрик и импортёр в `company_financials`

**Files:**
- Create: `bank/company_metrics.go`
- Create: `bank/company_metrics_test.go`
- Modify: `views/company.go` (DDL-копия: `'M' = 5` в `period_type` (:37); правило разрешения (:114–120); комментарии (:124–130))
- Modify: `polyus/import.go` (DDL-копия: `'M' = 5` (:67))
- Create: `build/company_financials_period_type_m.sql` (локальный ALTER; каталог `build/` не коммитится)

**Interfaces:**
- Produces: `type bankCompanyMetric struct{ Metric string; Unit string }`; `var bankCompanyMetricMap map[string]map[string]bankCompanyMetric` (имя сырой таблицы → подпись источника → метрика); `var bankCompanySourceKind = map[string]string{"sber_finansovie_rezultaty": "ras", "rus_vtb_group_ifrs": "ifrs"}`; `var bankCompanySourceCompany` и `var bankCompanySourceUrl`; `const bankCompanyMetricsName = "bank_company_metrics"`; `func bankCompanyPeriod(d time.Time) string` («2026M08»); `type BankCompanyMetrics struct{}` с `Name()` и `Import(ctx, conn)`; `bankCompanyMetricsMeta []util.SeriesMeta` — в этом же файле (свой каталог; `bankSeriesMeta` задачи 9 его не включает).
- Consumes: фикстуры `bank/testdata/vtb_ifrs.xlsx` (задача 3) и `sber_finansovie_rezultaty.xlsx` (задача 5) — для контрактного теста словаря.

- [ ] **Step 1: Правки копий DDL и ALTER-файл**

`views/company.go:37` и `polyus/import.go:67`: `period_type Enum8('Q' = 1, 'H' = 2, 'FY' = 3, 'LTM' = 4)` → `…, 'M' = 5)`. Правило в `views/company.go` (:114–120): кортеж `(source_kind = 'ifrs', loaded_at, source_url)` → `(source_kind = 'ifrs', source_kind = 'ras', loaded_at, source_url)` во **всех** `argMax` и в комментариях правила (:48–74, :124); комментарии: `company` (:126) → добавить `'SBER','VTBR','T'`; `period_type` (:129) → добавить «M — месяц»; `source_kind` (:130) → добавить `'ras'` (РСБУ Сбера). ALTER-файл:

```sql
ALTER TABLE default.company_financials MODIFY COLUMN period_type Enum8('Q' = 1, 'H' = 2, 'FY' = 3, 'LTM' = 4, 'M' = 5);
```

(прогон — задача 12; `CREATE TABLE IF NOT EXISTS` существующую таблицу не меняет.)

- [ ] **Step 2: Словарь, карты и мета**

14 метрик спеки §3.7 (SBER — все 14, VTBR — 11: без блока капиталовых коэффициентов `capital_adequacy`/`capital_adequacy_base`/`capital_adequacy_core` — блок «Достаточность капитала» (Н20.x) ВТБ лежит ниже рамки разбора, решение контроллера; follow-up «блок коэффициентов ВТБ»). Подписи — литералами из отчётов разведки задач 3 и 5. `bankCompanyMetricsMeta`: `Source: bankCompanyMetricsName`, `Series: "<TICKER>_<metric>"`, `Frequency: "M"`, `Unit` из словаря, `origin` — таблица/лист источника, `description` — что измеряет, как читать темпы + оговорка про базы (РСБУ против МСФО: уровни сопоставимы как порядок, приросты — с осторожностью). Существующие 51 описание Сбера не трогаются.

- [ ] **Step 3: Падающие тесты (`bank/company_metrics_test.go`)**

```go
func TestBankCompanyMetricMapWellFormed(t *testing.T)   // каждая метрика ∈ словаря 14; unit ∈ {"млрд ₽","%"}; SBER покрывает 14, VTBR — 11; каждая (company, metric) описана в bankCompanyMetricsMeta; подписи уникальны внутри источника
func TestBankCompanyPeriod(t *testing.T)                // таблица: 2026-08-31 → "2026M08", 2026-01-01 → "2026M01"
func TestBankCompanyAppend(t *testing.T)                // appendRecorder: форма (company, metric, period, "M", source_kind, value, unit, source_url, 0, loadedAt); дублей ключа в одном прогоне нет
func TestBankCompanyDdlHasMonthEnum(t *testing.T)       // литерал DDL содержит "'M' = 5"
func TestBankCompanyLabelsExistInFixtures(t *testing.T) // КАЖДАЯ подпись словаря найдена в разборе своих фикстур (задачи 3/5) — контракт «словарь ↔ живые файлы» (Review Focus 4)
```

- [ ] **Step 4: `Import` — свой, без сети и без шаблона (прецедент `company_views` в `views/`)**

DDL `companyFinancials` — `CREATE TABLE IF NOT EXISTS` копией канонического §6.2 с `'M'` (образец копии — `polyus/import.go:60-75`); далее для каждой из трёх таблиц: пропуск, если таблицы нет (`SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?`); `tbank_group_ifrs` — пропуск с логом «источник не починен»; `SELECT name, date, value FROM <таблица> FINAL` (`conn.Query` → `Scan` → проверка `rows.Err()`); маппинг по карте, отсутствующая подпись — `log.Warnf` и пропуск (Review Focus 4); один батч в `company_financials` (`loadedAt` — одна `time.Now()` на прогон), `Send` с проверкой; затем `util.UpsertSeriesCatalog(ctx, conn, bankCompanyMetricsMeta)`. `count` — `batch.Rows()`. Персональный `init()` — до задачи 8.

- [ ] **Step 5: Run**

Run: `go test -race -count=1 ./bank/ -run 'BankCompany' && go test -race -count=1 ./views/ ./polyus/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add bank/company_metrics.go bank/company_metrics_test.go views/company.go polyus/import.go
git commit -m "bank: company metrics importer for SBER/VTBR into company_financials (monthly, ras/ifrs)"
```

**Модель субагента:** `deepseek/deepseek-flash` (новый файл словаря + правки копий DDL + тесты).

---

### Task 8: Реестр `bank/importers.go`, тест-энфорсер и расписание

**Files:**
- Create: `bank/importers.go`
- Create: `bank/importers_test.go`
- Modify: `dagu/bank.yaml` (состав и порядок шагов)
- Modify: `bank/domrf_mortgage.go`, `bank/vtb_fin_rez.go`, `bank/tbank_fin_rez.go`, `bank/sber_fin_rez.go`, `bank/sber_csi.go`, `bank/sber_csi_week.go`, `bank/company_metrics.go` (удалить персональные `init()`)

**Interfaces:**
- Produces: `type bankImporter struct{ chimport.ImportStat; table string; countPoints string }`; `var bankImporters = []bankImporter{…}`; `func init()`.
- Consumes: переменные-импортёры `domrfImporter`, `vtbImporter`, `tbIfrsImporter`, `sberFinRezImporter`, `sberCSIImporter`, `sberCSIWeekImporter` (имена Сбера — из `bank/sber_csi.go`/`sber_csi_week.go`), `&BankCompanyMetrics{}`.

- [ ] **Step 1: Реестр (образец `cbr/importers.go` целиком, включая докблоки)**

Семь записей-литералов в порядке `dagu/bank.yaml`; таблицы — литералами: `"sber_finansovie_rezultaty"`, `"sber_consumper_spending_index"`, `"sber_izmenenie_trat"`, `"rus_vtb_group_ifrs"`, `"domrf_mortgage"`, `"tbank_group_ifrs"`, `"company_financials"` (последняя — только у `bank_company_metrics`). `countPoints` — литералы: три запроса из спеки §3.1 у domrf/vtb/sber_fin, `""` у остальных.

- [ ] **Step 2: Удалить персональные `init()`** из семи файлов импортёров (`grep -rn "func init" bank/` — пусто, кроме `importers.go`).

- [ ] **Step 3: `dagu/bank.yaml`**

Порядок шагов: `sber_finansovie_rezultaty`, `sber_consumper_spending_index`, `sber_izmenenie_trat`, `rus_vtb_group_ifrs`, `domrf_mortgage`, `bank_company_metrics`; шаг `tbank_group_ifrs` удалить; `schedule`/`retry_policy`/`overlap_policy` не трогать; рядом с местом удалённого шага — комментарий с условием возврата Т-Банка.

- [ ] **Step 4: Тест-энфорсер `bank/importers_test.go` (образцы — `cbr/importers_test.go`, `rosstat/importers_test.go`)**

Проверки ровно по спеке §3.1: (а′) имя ∈ литерального набора из семи; (б) `table` ∈ набора ∪ `{"company_financials"}`, `table == Name()`, `company_financials` — только у `bank_company_metrics`; (в) уникальность и регистрация в `chimport.Stats`; (г) `dagu/bank.yaml` в обе стороны (шесть шагов; разбор `CLICKHOUSE_IMPORT_STAT=` регуляркой без YAML-зависимости — как `cbr/importers_test.go:82`) **плюс отдельная проверка «шага `tbank_group_ifrs` в dagu нет»**; (д) набор объявлений — литерал из трёх запросов, сверка с `Publication()` (`Source == Name()`), у остальных пусто; (¬) число импортёров пакета в `Stats` (по набору из семи) равно `len(bankImporters)`. Все ожидания — литералами (стандарт §8.5); комментарии «от какой правки защищает».

- [ ] **Step 5: Run + Commit**

Run: `go build ./... && go test -race -count=1 ./bank/ && dagu validate dagu/bank.yaml`
```bash
git add bank/importers.go bank/importers_test.go dagu/bank.yaml bank/domrf_mortgage.go bank/vtb_fin_rez.go bank/tbank_fin_rez.go bank/sber_fin_rez.go bank/sber_csi.go bank/sber_csi_week.go bank/company_metrics.go
git commit -m "bank: single importers registry with enforcer test and schedule update"
```

**Модель субагента:** `joingonka/zai-org/GLM-5.3-Flash` (проводка: реестр + шаг dagu; образцы готовы).

---

### Task 9: Каталог рядов и витрина `v_bank_macro`

**Files:**
- Modify: `bank/series_meta.go` (`domrfSeriesMeta`, `vtbSeriesMeta`, `sberFinSeriesMeta`, `bankSeriesMeta`, `afterBankMacro`)
- Modify: `bank/views.go` (`bankMacroView`; `sberSpendingView` остаётся)
- Modify: `bank/domrf_mortgage.go`, `bank/vtb_fin_rez.go`, `bank/sber_fin_rez.go`, `bank/sber_csi.go`, `bank/sber_csi_week.go` (подключение `AfterImport`)
- Create: `bank/series_meta_test.go`
- Modify: `bank/sber_csi_test.go` (не ломать существующие контракты Сбера)

**Interfaces:**
- Produces: `afterBankMacro(meta []util.SeriesMeta) func(context.Context, driver.Conn) error` (замена `afterSberMacro` — старую удалить); `bankMacroView util.View` (`Name: "v_bank_macro"`; `Tables` — пять констант: `domrfMortgageTable`, `vtbIfrsTable`, `sberRpbuDataUrlTable`, `sberCSITable`, `sberCSIWeekTable`); `bankSeriesMeta []util.SeriesMeta` (51 Сбера + domrf + vtb + sberFin).

- [ ] **Step 1: Описания рядов трёх источников**

`domrfSeriesMeta` (Series — банки из фикстуры задачи 1; unit «% ставки»; frequency `M`; origin — файл/лист; description — как читать), `vtbSeriesMeta` (серии разбора задачи 3; unit «млрд ₽» / «%»; оговорка МСФО), `sberFinSeriesMeta` (серии задачи 5; unit «млрд ₽» / «%»; оговорка РСБУ). Контракт — односторонний (§11.4).

- [ ] **Step 2: `afterBankMacro` и `bankMacroView`**

Хелпер — по образцу `afterSberMacro` (`bank/series_meta.go:37-45`): `UpsertSeriesCatalog(ctx, conn, meta)` + `CreateView` для `sberSpendingView` и `bankMacroView`. Витрина — long-форма по пяти таблицам с `toFloat64`, `Select` — литералами имён таблиц, комментарии таблицы и колонок (образец — `v_rosstat_macro`, `sberSpendingView`).

- [ ] **Step 3: Подключить производные**

У пяти импортёров — `AfterImport: afterBankMacro(bankSeriesMeta)`; в `bank/sber_csi.go`/`sber_csi_week.go` убрать использование `afterSberMacro`; собственный `Import()` ВТБ (задача 4) теперь дёргает непустой `AfterImport` и на пустой дельте — проверить, что вызывается тот же хелпер.

- [ ] **Step 4: Тесты**

```go
func TestBankSeriesMetaCoversFixtures(t *testing.T) // для каждого источника — разбор фикстуры, каждая Series описана (§11.4)
func TestBankMacroViewContract(t *testing.T)        // имя, пять Tables, пять литеральных source в Select, toFloat64
```

- [ ] **Step 5: Run + Commit**

Run: `go test -race -count=1 ./bank/`
```bash
git add bank/series_meta.go bank/views.go bank/series_meta_test.go bank/domrf_mortgage.go bank/vtb_fin_rez.go bank/sber_fin_rez.go bank/sber_csi.go bank/sber_csi_week.go bank/sber_csi_test.go
git commit -m "bank: bank-wide series catalog and v_bank_macro view"
```

**Модель субагента:** `deepseek/deepseek-flash` (крупный `series_meta.go` + правки пяти импортёров).

---

### Task 10: Грант `v_bank_macro` и проверки MCP

**Files:**
- Modify: `sql/mcp_kimi_reader.sql`
- Modify: `scripts/mcp_check.py`

**Interfaces:** исполнение — `make mcp-user` + `make mcp-check` в задаче 12.

- [ ] **Step 1: Грант** — добавить `GRANT SELECT ON default.v_bank_macro TO kimi_reader;` (после строки `v_bank_sber_spending`; пароль-плейсхолдер не трогать).

- [ ] **Step 2: `scripts/mcp_check.py` — после блока Сбера (~строка 795) добавить проверки (с комментариями «что ловят», как в файле):**
  - `v_bank_macro` непуста и по каждому из трёх новых source (`domrf_mortgage`, `rus_vtb_group_ifrs`, `sber_finansovie_rezultaty`) есть точки (образец — блок Сбера, `:758-788`);
  - каталог: `SELECT count() FROM v_series_catalog WHERE source = 'bank_company_metrics'` > 0;
  - компанейские: `SELECT count() FROM v_company_financials WHERE company IN ('SBER','VTBR')` > 0 и spot-чтение `SELECT metric, period, value FROM v_company_financials WHERE company = 'SBER' AND metric = 'total_assets' ORDER BY period DESC LIMIT 1`;
  - закрытость сырых (497): `domrf_mortgage`, `rus_vtb_group_ifrs`, `sber_finansovie_rezultaty`, `tbank_group_ifrs` (образец `:110-112` — ожидать `ACCESS_DENIED`).

- [ ] **Step 3: Run + Commit**

Run: `python3 -m py_compile scripts/mcp_check.py` (полный прогон — задача 12).
```bash
git add sql/mcp_kimi_reader.sql scripts/mcp_check.py
git commit -m "bank: grant v_bank_macro and mcp checks for bank and company views"
```

**Модель субагента:** `joingonka/zai-org/GLM-5.3-Flash` (мелкие правки по образцам внутри файла).

---

### Task 11: Документация и статусы (без ✅ — он в задаче 12)

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`, `docs/IMPORTER_PACKAGE_STANDARD.md`, `docs/ROADMAP_KEY_RATE.md`, `util/clickhouse_import.go` (комментарий ~:215)

- [ ] **Step 1: `ARCHITECTURE.md`** — по списку спеки §3.9: §2 строка `bank/` — переписать; §6.1 — добавить строки четырёх импортёров (сейчас их нет) в формате таблицы (источник, ряд, точка контроля, состояние с числом приёмки); §6.2 — `company_financials`: `'M'` в `period_type`, `'SBER','VTBR','T'` в комментарии `company`, `'ras'` в списке `source_kind`; §6.3 — `v_bank_macro` + пополнение `v_company_*`; **абзац-конвенция компанейской оси** (§6.3/§6.5): сырьё — по источнику (компания — значение, не схема), метрики эмитентов — в `company_financials`, сектор — измерение (`peer_universe`), персональных `v_<эмитент>_*` не заводим; §7 — состав цепочки `dagu/bank.yaml` (шесть шагов, Т-Банк вне); §8 п. 13 — объявлений 20 → 22, отступлений два → три (ВТБ; образец формулировки — дип-бюллетени); changelog шапки.
- [ ] **Step 2: `README.md`** — строка `bank/` в таблице импортёров; раздел долгов (533–535) — «как было/как стало»; состав архива (пересчитать: + sber_fin, + domrf, пофайловые версии ВТБ); компанейский слой (банковские метрики); счётчики.
- [ ] **Step 3: `docs/IMPORTER_PACKAGE_STANDARD.md`** — §18: строка `bank` → «✅ соответствует» с отступлениями О1–О4; §7.1 — оговорка для пакетов-агрегаторов нескольких эмитентов (`bank/`, `metals/`, `polyus/`) с литеральным набором имён; §9.3 — примечание Т-Банка (таблица заведена, пуста, вне расписания); §9.6 — пример «CrawFunc не архивируется»: `bank/sber_fin_rez.go` → `customs/vneshn_torg_countries.go`.
- [ ] **Step 4: `util/clickhouse_import.go:215`** — комментарий про `bank/sber_fin_rez.go` как CrawFunc-с-точкой-контроля: переформулировать (таких импортёров не остаётся; ветка-защита сохраняется), не ломая смысл про архив.
- [ ] **Step 5: `docs/ROADMAP_KEY_RATE.md`** — §6: новый пункт «Фаза 3.1 — рефакторинг пакета `bank/` по стандарту» (критерий приёмки; решения О1–О4; банки в `company_financials`), статус — «в работе» (✅ ставит задача 12); §8 таблица техдолга — строка про `bank/` по образцу строк `cbr/`/`rosstat/`.
- [ ] **Step 6: Порядок проверки — скил `sync-readme-architecture`** (`.kimi-code/skills/sync-readme-architecture/SKILL.md`); после — `git diff --stat` глазами.
- [ ] **Step 7: Commit**

```bash
git add ARCHITECTURE.md README.md docs/IMPORTER_PACKAGE_STANDARD.md docs/ROADMAP_KEY_RATE.md util/clickhouse_import.go
git commit -m "bank: docs — architecture, readme, standard and roadmap for bank refactor"
```

**Модель субагента:** `joingonka/zai-org/GLM-5.3-Flash` (документационные правки по образцам соседних записей; единичные Edit ≤ ~12 КБ).

---

### Task 12: Живая приёмка и протокол

**Files:**
- Modify: `docs/ROADMAP_KEY_RATE.md` (✅-статус), `docs/superpowers/plans/2026-10-16-bank-refactor.md` (протокол)

- [ ] **Step 1: `make all`** (gofmt, lint, `go vet`, `go test -race`, обе сборки), затем адресно `go test -race -count=1 ./bank/`.
- [ ] **Step 2: Схема** — `make ch-sql FILE=build/company_financials_period_type_m.sql`; проверить, что `'M'` доступно (`SELECT toTypeName(period_type) FROM company_financials LIMIT 1` или вставкой тестовой строки вручную), строки `PLZL` — 347 на месте.
- [ ] **Step 3: Прогоны** (числа, даты, повторы — в протокол): `make import STAT=domrf_mortgage` (Imported N + крайние даты; повтор — no-op с версией); `STAT=rus_vtb_group_ifrs` (первый — все файлы; повтор — «новых файлов нет»); `STAT=sber_finansovie_rezultaty` (повтор — no-op); `STAT=sber_consumper_spending_index` и `STAT=sber_izmenenie_trat` (те же `count() FINAL`); `STAT=bank_company_metrics` (число строк; повтор — то же число).
- [ ] **Step 4: Архив** — прогоны с `ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev`: объекты и строки `registry.jsonl` для domrf, sber_fin, vtb (пофайлово); состав — в протокол.
- [ ] **Step 5: Сверка с базовой линией** (§15.2) — обновлённый `build/bank_refactor_baseline.sql` через `make ch-sql`; расхождения — поимённо; ожидаемые: переразбор ДОМ.РФ, порядок файлов ВТБ; любое иное — стоп и разбор.
- [ ] **Step 6: MCP** — `make mcp-user`; `make mcp-check` → ПРОВЕРКА ПРОЙДЕНА.
- [ ] **Step 7: Чистота** — `grep -rn "colly\|fmt.Print" bank/` пусто; `grep -rn "func init()" bank/` — только `bank/importers.go`; `dagu validate dagu/bank.yaml` — exit 0.
- [ ] **Step 8: ✅-коммит до PR** — статус пункта в §6 и строка техдолга §8 → ✅; протокол (команды, числа, даты, расхождения) — в этот план-файл и в тело коммита.

```bash
git add docs/ROADMAP_KEY_RATE.md docs/superpowers/plans/2026-10-16-bank-refactor.md
git commit -m "bank: refactor accepted — live runs, archive, mcp-check"
```

- [ ] **Step 9: PR** — `gh pr create` (одна фича — один PR; описание: источник данных, схема, запуск, пример проверки) → приёмочный протокол **комментарием в этот же PR** (стандарт §16.1 п. 8) → merge решает владелец.

**Модель субагента:** `deepseek/deepseek-flash` (или исполняет ведущий агент — живая приёмка идёт с участием владельца).

---

## Self-Review (выполнено автором плана)

- **Покрытие спеки §1.2**: п.1 (реестр) → задача 8; п.2 (ДОМ.РФ) → задачи 1–2; п.3 (ВТБ) → задачи 3–4; п.4 (Сбер-фин) → задача 5; п.5 (Т-Банк) → задача 6; п.6 (каталог/витрина) → задача 9; п.7 (банки в `company_financials`) → задача 7 (+правки копий DDL); п.8 (проводка) → задачи 8, 10, 11. Отступления: О1 — литеральный набор имён в задаче 8; О2 — отсутствие шага в задаче 8 (+тест) и текст возврата в задаче 11; О3 — гейт ВТБ в задаче 4 (докблок/README — задача 11); О4 — докблок Т-Банка в задаче 6.
- **Review Focus → тесты**: 1 → задача 1 (`NoBlock`); 2 → задача 4 (`XlsLinks`); 3 → задача 7 (`Append`, форма ключа) + задача 12 (повтор); 4 → задачи 3/5 (fail-fast), задача 7 (`LabelsExistInFixtures`, warn-пропуск); 5 → задача 6 (имя таблицы в тексте ошибки) + задача 8 (отсутствие шага).
- **Согласованность типов**: `domrfPoint`/`vtbPoint`/`sberFinPoint` — каждая пара «разбор+append» только внутри своей задачи; `bankImporter{ImportStat, table, countPoints}` — задача 8; `afterBankMacro(meta)` — задача 9 (сигнатура как у `afterRosstatMacro`); `bankCompanyMetric`/`bankCompanyPeriod` — задача 7; константы таблиц (`domrfMortgageTable`, `vtbIfrsTable`, `sberRpbuDataUrlTable`, `sberCSITable`, `sberCSIWeekTable`) сохраняются — на них ссылается `bankMacroView.Tables` в задаче 9.
- **Пропорция**: ~640 строк плана на 181 строку спеки — оправдано числом артефактов (15 файлов кода/тестов/доков) и живой приёмкой; тела функций не транскрибированы, кроме двух коротких идиом (копия шаблона в задаче 2, период в задаче 7).
- **Известное упрощение**: сетевые/БД-ветки (гейт ВТБ, `bank_company_metrics.Import`, чек-точка ДОМ.РФ) офлайн-тестами не воспроизводятся — их покрывает живая приёмка задачи 12; чистые части покрыты тестами задач.

## Протокол живой приёмки

**Вердикт: ✅ приёмка пройдена (2026-10-17).** Ветка `feat/bank-refactor`, dev-ClickHouse (порт 8123,
database `default`), все команды — через `make`; числа — фактические. Полный отчёт —
`.superpowers/sdd/2026-10-16-bank-refactor/task-12-report.md`.

**Два прогона: первый нашёл дефекты, второй принял.** Первый прогон 2026-10-17 вскрыл **D1** —
`bank_company_metrics` сканировал legacy-`Float32` в `*float64` (`clickhouse [ScanRow]: (balance) converting
Float32 to *float64 is unsupported`, 0 строк вместо банковских метрик, каталог и `v_company_financials`
без метрик) — и **D2** — разбор P&L ВТБ не принимал подпись «апр'2025» (источник сокращает месяц) и выпуск
28.02.2026 без месячных колонок P&L (в шапке только накопительные `2М'2026`/`2М'2025`); приёмка была
остановлена без правок кода (BLOCKED). Дефекты починены коммитами `ebe4574` (D1) и `fb92967` (D2; добавлены
фикстуры сокращённого месяца и выпуска без P&L), после чего **шаги 3–6 пере-прогнаны** — итоговые числа ниже.

**Шаг 1 (инструменты).** `make all` — зелёный: golangci-lint `0 issues`, `go vet ./...` чисто,
`go test -race ./...` — все пакеты `ok`, обе сборки; `go test -race -count=1 ./bank/` — `ok … 5.452s`.

**Шаг 2 (схема).** `make ch-sql FILE=build/company_financials_period_type_m.sql` применён; живой тип
`period_type` — `Enum8('Q'=1,'H'=2,'FY'=3,'LTM'=4,'M'=5)`, PLZL 347/265 на месте. **Поправка фикс-волны
(2026-10-17):** запрос с `period_type = 'M'` выполнялся к **таблице**; витрины `v_company_*` при этом
остались старой версии — `CREATE VIEW` фиксирует тип колонки на момент создания, и через витрину
`period_type = 'M'` молча давал 0 (спот-проверка брала `metric/period/value` без `period_type` — пропуск
не увидела). Обязательный порядок: **ALTER → пересоздать `v_company_*`** (`make import STAT=company_views`) —
подробности и числа в «Фикс-волне финального ревью» ниже.

**Шаг 3 (прогоны).**

- `domrf_mortgage` — `Imported 15 rows`: 15 банков «Новостройки», все точки 2026-08-31; повтор — no-op
  «версия `984b5398448d06092a62b481671f02a6` уже импортирована, 15 точек на месте» (без скачивания).
- `rus_vtb_group_ifrs` — на странице 13 xlsx (и 6 pdf), **все 13 разобраны**: таблица **720 строк / 36 рядов**,
  2023-10-31 … 2026-08-31 — балансовый блок 18×22 = **396** (ровно база T0) + P&L 18×18 = **324**; повтор —
  все 13 файлов «уже импортирован — пропуск», `Imported 0 rows` без скачиваний. Выпуск 28.02.2026 точек P&L
  не даёт by design: в шапке нет месячных колонок, разбор предупреждает `log.Warnf`, баланс файла импортируется.
  Месяцев P&L — 18, потому что каждый выпуск печатает «месяц выпуска + тот же месяц годом раньше»; 10 балансовых
  месяцев без точек P&L — девять месяцев, чьих выпусков на странице нет (2023-12, 2024-09, 2024-12, 2025-03,
  2025-06, 2025-09, 2025-12, 2026-03, 2026-06), и десятый — 2026-02 (выпуск без месячных колонок).
- `sber_finansovie_rezultaty` — **`Imported 1279 rows`** (30 рядов; баланс 13×43 = **559** — ровно база T0,
  +17 рядов P&L, капитала и нормативов), 2022-12-31 … 2026-06-30; повтор — no-op «версия
  `d74f627b2d30f37d72a63d7dab83d986` уже импортирована, 1279 точек на месте». Перед первым прогоном строку
  версии снимали (`build/sber_fin_version_delete.sql`): отпечаток файла не менялся, а точек было 559 == 559,
  и смена кода разбора иначе осталась бы невидимой — санкционированное лечение (прецедент — дип-бюллетени ЦБ).
- `sber_consumper_spending_index` — `Imported 470 rows`; `sber_izmenenie_trat` — `Imported 7038 rows`
  (`count() FINAL` — 470/7038, ровно база T0).
- `bank_company_metrics` — **`Imported 829 rows`** в `company_financials`: SBER **599** (8 балансовых метрик
  × 43 месяца + net_profit/roe/roa × 42 + 3 норматива × 43) + VTBR **230** (8 балансовых × 22 + net_profit/roe/roa
  × 18); повтор — те же **829** (идемпотентно). Оценка спеки «629» не сошлась в сторону большего числа: в сырую
  таблицу ВТБ добавились ряды P&L, которые словарь метрик и читает; метрики видны в `v_company_financials`.
- `tbank_group_ifrs` — ожидаемое падение на PDF: `excelize zip: not a valid zip file`, `Imported 0 rows`, код 1;
  DDL выполнен до скачивания — пустая таблица заведена (О2/О4), в `v_bank_macro` не входит.

**Шаг 4 (винтажный архив).** `ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev` — **15 объектов и 15 строк
реестра**: `domrf_mortgage` — 1 + 1 (`984b5398448d0609_135272296`, 259 366 Б, sha256 `d9d30275…`),
`sber_finansovie_rezultaty` — 1 + 1 (`d74f627b2d30f37d_…xlsx`, 56 947 Б, sha256 `475acf53…`),
`rus_vtb_group_ifrs` — **13 + 13** пофайлово (39 652…45 485 Б). `registry.jsonl` — 357 → **372 строки**
(два этапа: архивный прогон дал 13 записей — domrf 1, sber_fin 1, ВТБ 11 — плюс дозапись двух файлов ВТБ
после фикса D2, +2, дала 15). Записи шли через документированное лечение гейта (снять строки версий и повторить прогон):
`build/t12_archive_remove_versions.sql` — для трёх источников перед архивным прогоном,
`build/t12r_archive_fix2.sql` — дозапись двух файлов ВТБ, разобранных после фикса D2 (их импортёр видел
уже архивными, а объектов не было). Повтор `domrf_mortgage` после архивного прогона — снова чистый no-op с той же версией.

**Шаг 5 (сверка с базовой линией, §15.2).** `build/bank_refactor_baseline.sql` обновлён фактическим состоянием
(пять запросов T0 — дословно; добавлены `domrf_mortgage` и `tbank_group_ifrs`; в комментарии файла — снимки T0 и T12).
Расхождения с T0 поимённо: `rus_vtb_group_ifrs` 396/18 → **720/36** (балансовый блок 396 = 18×22 без изменений,
+18 рядов и +324 точки P&L; min-дата 2023-12-31 → 2023-10-31); `sber_finansovie_rezultaty` 559/13 → **1279/30**;
`company_financials` — PLZL 347/265 без изменений, **SBER 599/599 и VTBR 230/230 (всего +829 курированных строк)**;
`domrf_mortgage` появилась — 15/15/2026-08-31; `tbank_group_ifrs` заведена пустой — 0/0;
`sber_consumper_spending_index` 470/5 и `sber_izmenenie_trat` 7038/46 — без изменений. Иных расхождений нет.

**Шаг 6 (MCP).** `make mcp-user` — ✅ (в т.ч. `GRANT SELECT ON default.v_bank_macro TO kimi_reader`).
`make mcp-check` — **95 OK, 1 FAIL** (`ПРОВАЛЕНО: 1`). Все банковские и компанейские проверки зелёные:
`v_bank_macro` — 9 522 строки (domrf 15, ВТБ 720, sber_fin 1279), каталог `bank_company_metrics` — 25 рядов,
`v_company_financials` — 829 строк SBER/VTBR, spot `SBER total_assets` за `2026M06` = 65 355,1015625,
все четыре сырые банковские таблицы закрыты агенту (497). Единственное падение — **вне `bank/`**:
`FAIL агрегатный ряд бюллетеней rosstat непуст: точек агрегатного ряда: 288, want 80`. Разбор: проверка читает
`v_rosstat_macro` (`source='rosstat'`, `series='Индекс потребительских цен (недельная оценка)'`), ряд приходит
из сырой `rosstat_ipc_weeks_bulletins` пакета `rosstat/` и держит **288 точек / 288 недель, 2015-07-06 … 2021-12-27**
(расширенная выкачка бюллетеней на dev-БД), тогда как проверка жёстко ждёт 80. Непричастность `bank/` доказана:
`git diff --stat 325ff8b..HEAD -- rosstat/` пуст, `grep -rn "недельная оценка" bank/` пуст, rosstat-блок
`scripts/mcp_check.py` в диффе ветки отсутствует. Rosstat в этой приёмке не правился — это внешнее состояние
окружения, а не дефект пункта `bank/`.

**Шаг 7 (чистота).** `grep -rn "colly\|fmt.Print" bank/` — пусто; `grep -rn "func init()" bank/` — только
`bank/importers.go:75`; `dagu validate dagu/bank.yaml` — код 0.

**Правки доков, сделанные при приёмке.** `docs/ROADMAP_INFRA.md:31` — пример `CrawFunc` → `customs/vneshn_torg_countries.go`;
`docs/IMPORTER_PACKAGE_STANDARD.md` §18 — шапка «Состояние на 2026-10-17 (срез)» и строка `bank` → «✅ соответствует»
с О1–О4; `ARCHITECTURE.md` §6.1 — починена сломанная строка `dcf_engine` (5 разделителей `|` вместо 6 — не хватало
колонки «Источник»), в строки четырёх импортёров вписаны живые числа приёмки, старый ✅ у `sber_izmenenie_trat`
помечен «(до рефакторинга)» (и подтверждён приёмкой 2026-10-17).

**Статусы.** `docs/ROADMAP_KEY_RATE.md` §6 — фаза 3.1 → ✅ «сделан этим PR» (приёмка 2026-10-17); §8 — строка
техдолга `bank/` → ✅ «устранён рефакторингом `bank/` (фаза 3.1; приёмка 2026-10-17)». Приёмка — задача 12 плана;
коммит статусов и протокола — `bank: refactor accepted — live runs, archive, mcp-check`.

**Фикс-волна финального ревью (2026-10-17).** Финальное ревью ветки нашло пропуск приёмки (**C1**): шаг 2 расширил
таблицу `company_financials`, но витрины `v_company_*` зафиксировали тип колонки на момент `CREATE VIEW` и остались
старой версии — через витрину `WHERE period_type = 'M'` молча давал 0 строк, `toString(period_type)` падал
(`Unexpected value 5 in enum`), и агент банковских метрик через витрину не видел. Лечение — `make import
STAT=company_views` (идемпотентен): пересозданы `v_company_financials`, `v_company_metric_sources`,
`v_company_operating` (лог: `View … created: true` ×3, `Imported 37 rows`). Контроль после фикса:
`count() … WHERE company = 'SBER' AND period_type = 'M'` = **599**, `toString(period_type)` по SBER/VTBR = `M`
(599/230), всего SBER+VTBR **829** — числа приёмки не изменились. В `scripts/mcp_check.py` добавлена проверка
«период 'M' читается через v_company_financials» (её отсутствие дало пропуск; при нуле detail указывает пересоздать
витрины), а проверка закрытости `tbank_group_ifrs` стала толерантной к `UNKNOWN_TABLE` (пометка «таблица появится
после ручного прогона (О2)» — иначе `make mcp-check` был бы красным на каждой БД без ручного прогона). Синхронизированы
доки: `IMPORTER_PACKAGE_STANDARD.md` §18 — строка `bank` → «✅ соответствует» (фаза 3.1; приёмка 2026-10-17), архив —
15 объектов (13 + 2 дозаписи после фикса D2); ВТБ — 11 метрик (спека §3.7 и T7 плана; было «12»); `ARCHITECTURE.md` —
«живая приёмка пройдена 2026-10-17». `make mcp-check` после фикса — **96 OK, 1 FAIL** (bank-блок зелёный, включая
новый 'M'-чек и пометку о T-Банке; единственное падение — прежнее, rosstat вне `bank/`).

**Фикс-волна 2 (по решению владельца).** Шаг `company_views` добавлен в `dagu/bank.yaml` сразу после
`bank_company_metrics`; порядок выкатки ALTER→company_views — README.
