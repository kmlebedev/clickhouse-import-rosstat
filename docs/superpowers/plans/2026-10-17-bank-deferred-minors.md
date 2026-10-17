# `bank/` — отложенные миноры финального ревью: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Закрыть миноры T1–T11, отложенные финальным ревью рефакторинга `bank/` (PR #84), не меняя архитектуру пакета: убрать мёртвый код харнесса, ужесточить разборы ДОМ.РФ/ВТБ/Сбера/Т-Банка, защитить маппер от дубля периода, исправить реестр и энфорсер dagu, синхронизировать каталог, MCP-проверки и документацию.

**Architecture:** Правки локальны и не меняют состав пакета: те же 6 таблиц, 7 записей реестра, 2 витрины, каталог, гранты и расписание. Разборы остаются чистыми функциями; защитные проверки возвращают ошибку с контекстом вместо молчаливой потери данных. Все команды — через `make`; живые прогоны — только для затронутых импортёров.

**Tech Stack:** Go, `excelize/v2`, `clickhouse-go/v2`, `logrus`, Dagu 2.18, ClickHouse 26.10, `scripts/mcp_check.py`.

**Spec:** `docs/superpowers/specs/2026-10-17-bank-deferred-minors-design.md` — обязательна к прочтению целиком. Ссылки «спека §N» ниже — на неё, «стандарт §N» — на `docs/IMPORTER_PACKAGE_STANDARD.md`.

## Global Constraints

- Секреты только из окружения; HTTP — только через `util`; запись — батчами; ошибки не проглатывать; `fmt.Print*` в `bank/` не добавлять (AGENTS.md).
- Архитектура, имена таблиц/рядов, витрины и гранты не меняются. Новых зависимостей нет.
- T0–T12 прошлого пункта не переделывать; О1–О4 и Ruling-строки прошлого леджера не пересматривать.
- Каждый минор T1–T11 закрыт или имеет Ruling; «попутных» правок нет.
- Все команды — через `make` (Makefile подключает `~/.config/rosstat/env`). ClickHouse не поднимать/не гасить.
- Модель имплементера — `deepseek/deepseek-flash` (явным `model`); ревью задач — `anthropic/haiku`; финальное ревью — `deepseek/deepseek-flash`; scoped re-review — `anthropic/haiku`.
- В каждый промпт coder-субагента вписывать блок: «У тебя есть инструменты `mcp__jetbrains__*` (GoLand MCP) и `mcp__clickhouse__*`. Используй `mcp__jetbrains__*` для проверки правок и навигации вместо grep и лишней сборки; SQL к ClickHouse — только через `mcp__clickhouse__run_query`. Если вдруг MCP-инструментов в твоём наборе нет — скажи об этом в отчёте (`DONE_WITH_CONCERNS`), не молчи».
- Каждый коммит — после `gofmt` и зелёного адресного `go test -race`; финально — `make all`.
- Правки комментариев не должны менять поведение; если правка поведения не покрыта тестом — сначала тест.

## Review Focus

1. **Источник отдал неполную/неоднозначную вёрстку** (страница ДОМ.РФ без `file2.src`, два листа ВТБ с одним якорем, непустая ячейка Т-Банка правее шапки) → разбор обязан упасть с координатой, а не молча потерять данные; тесты — Task A/B/C.
2. **Числовые крайности** («0,202», «NaN»/«Inf» в шапке и глоссарии, hex-float) → конечные числа принимаются, не-числа дают понятную ошибку, NaN/Inf не уезжают в таблицу; тесты — Task B.
3. **Дубль периода в одном прогоне маппера** (две даты одного месяца) → ошибка с метрикой и обеими датами, а не тихое схлопывание ключом; тест — Task D.
4. **Энфорсер dagu обманут комментарием или дублем `id`** → закомментированный шаг не считается шагом, дубликат `id` ловится; тесты — Task E.
5. **Документация расходится с кодом** (шесть vs семь записей, шесть vs семь шагов, §6.8, origin без пути, строка маппера в §6.1) → текст синхронизирован с фактами; проверка — Task F и чтение диффа ревьюером.

---

### Task A: T1 + T2 — харнесс bank-тестов и `domrf_mortgage`

**Files:**
- Modify: `bank/harness_test.go`
- Modify: `bank/domrf_mortgage.go`
- Test: `bank/domrf_mortgage_test.go`

**Interfaces:**
- Produces: `appendRecorder` без полей `raw`/`series`, с методами `Send() error`, `Flush() error`, `Abort() error`; `domrfImportFunc` больше не оборачивает ошибки; внутренние ошибки страницы не называют таблицу.
- Consumes: `driver.Batch`, `companyFinancialsColumnNames`, `domrfMortgageDdl`, `domrfMortgageTable` (существуют).

- [ ] **Step 1: Написать падающие тесты**

В `bank/harness_test.go` добавить:

```go
func TestAppendRecorderBatchMethodsDoNotPanic(t *testing.T) {
	rec := newAppendRecorder("name", "date", "balance")
	if err := rec.Send(); err != nil { t.Fatalf("Send: %v", err) }
	if err := rec.Flush(); err != nil { t.Fatalf("Flush: %v", err) }
	if err := rec.Abort(); err != nil { t.Fatalf("Abort: %v", err) }
}
```

В `TestDomrfXlsSrcFromPageNoBlock` добавить кейсы минимального HTML с `file2:null` и `file2.src:""`; ожидание — ошибка содержит `file2.src`, но **не** содержит `domrf_mortgage` (префикс добавляет `Import`).

В `TestParseDomrfSheetFailFast` заменить `want: "Банк"` → `"не найдена шапка таблицы"`, `want: "Ставка"` → `"не найдена подшапка"`; в цикле проверять `strings.Count(err.Error(), domrfMortgageTable) == 1`.

В `TestDomrfMortgageDdlShape` добавить:

```go
rendered := fmt.Sprintf(domrfMortgageDdl, domrfMortgageTable)
if strings.Contains(rendered, "%!") { t.Errorf("DDL не отрендерился:\n%s", rendered) }
if got := strings.Count(rendered, domrfMortgageTable); got != 1 { t.Errorf("имя таблицы в SQL %d раз, want 1:\n%s", got, rendered) }
```

- [ ] **Step 2: Прогнать тесты — убедиться, что падают**

Run: `go test -race -count=1 ./bank/ -run 'TestAppendRecorder|TestDomrf'`
Expected: FAIL — паника/`Send` не реализован, нет кейсов, двойной префикс, DDL-проверка не выполняется.

- [ ] **Step 3: Реализовать харнесс**

Удалить `raw` и `series` из `appendRecorder`; в `normalize` убрать `r.raw = append(...)` и `row := appendedRow{name: r.series}` → `row := appendedRow{}`. Добавить:

```go
func (r *appendRecorder) Send() error  { return nil }
func (r *appendRecorder) Flush() error { return nil }
func (r *appendRecorder) Abort() error { return nil }
```

Обновить докблоки: форма `(name, date, value)` несёт имя; мёртвых полей нет.

- [ ] **Step 4: Реализовать единый префикс ошибок ДОМ.РФ**

- `domrfXlsSrcFromPage`: убрать `domrf_mortgage: ` из веток «не разбирается как HTML» и «не найден блок», «нет файла» (оставить `страница %s: …`).
- `parseDomrfSheet`: добавить `domrf_mortgage: ` в ветки, где его нет (`в книге нет листа…`, `в заголовке листа нет даты…`), остальные сохранить.
- `domrfImportFunc`: вернуть `parseDomrfSheet`/`appendDomrfPoints` без обёртки.
- `Import`: оставить обёртку `domrf_mortgage: %s: %w` для страницы.

- [ ] **Step 5: Убрать датированный нарратив**

В комментариях `bank/domrf_mortgage_test.go` («разведка 2026-10-16», «127 710 Б») оставить факты (страница, лист, шапка, 15 рядов), убрать дату и размер.

- [ ] **Step 6: Прогнать тесты**

Run: `go test -race -count=1 ./bank/ -run 'TestAppendRecorder|TestDomrf'`
Expected: PASS.

- [ ] **Step 7: Коммит**

```bash
git add bank/harness_test.go bank/domrf_mortgage.go bank/domrf_mortgage_test.go
git commit -m "bank: close harness and domrf review minors (T1-T2)"
```

**Модель субагента:** `deepseek/deepseek-flash`.

---

### Task B: T3 + T4 — разбор ВТБ

**Files:**
- Modify: `bank/vtb_fin_rez.go`
- Test: `bank/vtb_fin_rez_test.go`

**Interfaces:**
- Produces:
  - `vtbRowPoints(sheet string, n int, label string, row []string, cols []vtbColumn) ([]vtbPoint, error)`;
  - `vtbSheet(xlsx *excelize.File, anchor string) (string, error)` — ошибка при >1 совпадении;
  - `vtbFloat(raw string) (float64, error)` — запятая как десятичный разделитель;
  - `vtbIfrsReportRe` с `(?i)`.
- Consumes: `vtbCell`, `vtbSeriesName`, `vtbRowHasValues`, `vtbSerialColumns`, `vtbRatioDescription` (существуют).

- [ ] **Step 1: Написать падающие тесты**

- `TestVtbFloatComma`: `vtbFloat("0,202")` → `0.202`, `nil`.
- `TestVtbSheetAmbiguous`: книга с двумя листами, содержащими `vtbBookSheet`, → ошибка, текст перечисляет оба листа.
- `TestVtbIfrsXlsLinksUppercase`: ссылка `.../rus-vtb-group-ifrs-as-of-31-August-2026.xlsx` (якорь в нижнем регистре — конвенция источника, месяц с заглавной) разбирается в дату `2026-08-31`.
- Расширить существующий тест шапки баланса: ячейка `"NaN"` в строке шапки не становится колонкой-датой и не роняет разбор.
- Расширить существующий fail-fast глоссария: определение `"NaN"` — текст, ошибки «значение — число» нет; `"0.202"` — ошибка есть.

- [ ] **Step 2: Прогнать — убедиться, что падают**

Run: `go test -race -count=1 ./bank/ -run 'TestVtb|TestParseVtb'`
Expected: FAIL на новых кейсах.

- [ ] **Step 3: Вынести общий цикл**

Добавить `vtbRowPoints` с циклом из `parseVtbBookSheet`/`parseVtbPnlSheet` (пустая ячейка — пропуск, `vtbFloat` с контекстом). В обоих проходах заменить дублированный цикл вызовом; ошибку оборачивать листом, строкой и подписью на месте вызова.

- [ ] **Step 4: Защитные проверки**

- `vtbSerialColumns`: после `ParseFloat` — `if math.IsNaN(serial) || math.IsInf(serial, 0) { continue }`.
- `vtbSheet`: собрать `matches []string`; `len(matches) > 1` → `fmt.Errorf("в книге несколько листов с якорем %q: %s", anchor, strings.Join(matches, ", "))`.
- `vtbFloat`: до `ParseFloat` — `raw = strings.ReplaceAll(strings.TrimSpace(raw), ",", ".")`; смешанный формат останется ошибкой.
- `parseVtbRatiosSheet`: `if _, err := vtbFloat(definition); err == nil { return ... }` — «NaN»/«Inf» больше не числовая таблица.
- `vtbIfrsReportRe = regexp.MustCompile(\`(?i)as-of-(\d{1,2})-([a-z]+)-(\d{4})\`)`: `(?i)` делает безрегистровым разбор месяца (`31-August-2026`); префикс href `rus-vtb-group-ifrs-as-of` остаётся конвенцией источника в нижнем регистре, старые файлы `RUS-…-As-Of-…` вне T4 — селектор ссылок не меняется.

- [ ] **Step 5: Безвременные комментарии**

Заменить «урок ревью domrf_mortgage», «найден ревью T6», ссылки на задачи ревью — объяснением причины (что именно ломалось и почему проверка нужна).

- [ ] **Step 6: Прогнать тесты**

Run: `go test -race -count=1 ./bank/ -run 'TestVtb|TestParseVtb'`
Expected: PASS.

- [ ] **Step 7: Коммит**

```bash
git add bank/vtb_fin_rez.go bank/vtb_fin_rez_test.go
git commit -m "bank: harden vtb parsing for review minors (T3-T4)"
```

**Модель субагента:** `deepseek/deepseek-flash`.

---

### Task C: T5 + T6 — Сбер РСБУ и Т-Банк

**Files:**
- Modify: `bank/sber_fin_rez.go`
- Test: `bank/sber_fin_rez_test.go`
- Modify: `bank/tbank_fin_rez.go`
- Test: `bank/tbank_fin_rez_test.go`

**Interfaces:**
- Produces: `tbIfrsPoint(rows [][]string, headerRow, dataRow, col int) (float64, time.Time, error)` — единственное место ручной индексации; ошибка на непустую ячейку правее шапки.
- Consumes: `sberFinTimeLayout`, `tbIfrsTimeLayout`, `tbIfrsTable`, `tbIfrsSheet` (существуют).

- [ ] **Step 1: Написать падающие тесты**

- Расширить тест `appendSberFinPoints`: ошибка Append содержит дату в формате `2006-01-02` (например `2026-06-30`), а не `06-30-26`. Для этого нужен рекордер, возвращающий ошибку Append: добавить в `appendRecorder` поле `appendErr error` и возвращать его из `Append` (не ломая существующие тесты).
- В `TestParseSberFinBookFailFast` заменить `want: sberFinBalanceSheet` и т.п. на литералы `"Баланс"`, `"Отчет о прибылях и убытках"`, `"Капитал"`, `"Показатели"`.
- `TestTbIfrsPointBounds`: синтетические `rows` (без фикстуры источника) — корректная точка; отсутствующая шапка; колонка за границей шапки; колонка за границей строки данных; нечисловое значение; битая дата. Каждый кейс — ошибка или значение, без паники.

- [ ] **Step 2: Прогнать — убедиться, что падают**

Run: `go test -race -count=1 ./bank/ -run 'TestAppendSberFin|TestParseSberFinBookFailFast|TestTbIfrs'`
Expected: FAIL на формате даты, литералах, отсутствующем `tbIfrsPoint`.

- [ ] **Step 3: Полировка Сбера**

- `parseSberFinBook`: `part, perr := parseSberFinSheet(...)`, `if perr != nil { return nil, fmt.Errorf("%s: %w", sberRpbuDataUrlTable, perr) }`.
- `appendSberFinPoints`: `p.Date.Format("2006-01-02")`.
- Вычитать комментарии `sber_fin_rez.go`; если конкретный грамматический дефект не найден — записать Ruling в леджер (перечень проверенных мест), не выдумывать правку.

- [ ] **Step 4: Реализовать Т-Банк**

- Добавить `tbIfrsPoint`: проверки `headerRow >= 0 && headerRow < len(rows)`, `dataRow >= 0 && dataRow < len(rows)`, `col >= 0`, `col < len(rows[headerRow])`, `col < len(rows[dataRow])`; разбор значения (запятая как тысячи, как сейчас) и даты `tbIfrsTimeLayout`; ошибки без таблицы (обёртка у вызывающего).
- В цикле: при `j+2 >= len(header)` и непустой `cell` — `return fmt.Errorf("%s: лист %q, строка %d, колонка %d: значение %q без даты в шапке", ...)`; заменить ручные индексы на `tbIfrsPoint`.
- Комментарий к `len(row) < 4 → break`: короткая строка после начала таблицы — конец таблицы (в отличие от `len(row) < 3 → continue` до якоря); поведение прежнее.
- Убрать «найден ревью T6».

- [ ] **Step 5: Прогнать тесты**

Run: `go test -race -count=1 ./bank/ -run 'TestAppendSberFin|TestParseSberFin|TestTbIfrs'`
Expected: PASS.

- [ ] **Step 6: Коммит**

```bash
git add bank/sber_fin_rez.go bank/sber_fin_rez_test.go bank/tbank_fin_rez.go bank/tbank_fin_rez_test.go
git commit -m "bank: polish sber and tbank parsing (T5-T6)"
```

**Модель субагента:** `deepseek/deepseek-flash`.

---

### Task D: T7 — `bank_company_metrics`

**Files:**
- Modify: `bank/company_metrics.go`
- Test: `bank/company_metrics_test.go`
- Modify: `bank/harness_test.go`

**Interfaces:**
- Produces:
  - `bankCompanyMapPoints(table string, points []bankCompanyPoint) (rows []bankCompanyRow, missing []bankCompanyMetricSpec, err error)`;
  - `newCompanyRecorder(cols ...string) *appendRecorder` и поле `companyForm bool`;
  - ошибка на дубль `(metric, period)`.
- Consumes: `bankCompanyMetricSpecs`, `bankCompanyMetricMap`, `bankCompanyPeriod`, `bankCompanyValue`, `companyFinancialsColumnNames` (существуют).

- [ ] **Step 1: Написать падающие тесты**

- `TestBankCompanyMapPointsDuplicatePeriod`: две точки одной метрики в одном месяце (например `2026-06-30` и `2026-06-15`) → ошибка, текст содержит метрику и период.
- `TestAppendRecorderCompanyFormExplicit`: `newAppendRecorder(companyFinancialsColumnNames...)` с 10 значениями **не** создаёт `company`-строку (ошибка формы), а `newCompanyRecorder(companyFinancialsColumnNames...)` — создаёт.
- Расширить тест missing: warn/`missing` содержит `bankCompanyMetricSpec` с `Metric`, а не подпись.

- [ ] **Step 2: Прогнать — убедиться, что падают**

Run: `go test -race -count=1 ./bank/ -run 'TestBankCompany|TestAppendRecorder'`
Expected: FAIL.

- [ ] **Step 3: Реализовать маппер**

- `bankCompanyMapPoints`: `seen := map[string]bankCompanyPoint{}` по ключу `metric + "\x00" + period`; при повторе — `fmt.Errorf("%s: метрика %s за период %s встречается дважды (даты %s и %s)", ...)`.
- `missing` — `[]bankCompanyMetricSpec`; в `Import` warn: `log.Warnf("... подписи %q (метрика %s) нет в таблице ...", spec.Label, spec.Metric)`.
- Разбить длинные однострочные записи `bankCompanyMetricSpecs` на многострочные (gofmt).
- Заменить «задача 3», «отчёты задач 3 и 5», «прогон — задача 12» на безвременные ссылки (спека §3.7, разведка живых книг, `build/company_financials_period_type_m.sql`).

- [ ] **Step 4: Явная company-форма в харнессе**

Добавить `companyForm bool`; `newCompanyRecorder` ставит его; `normalize` проверяет `r.companyForm` вместо `len(v) == len(companyFinancialsColumnNames)`. Обновить `company_metrics_test.go` на `newCompanyRecorder`.

- [ ] **Step 5: Прогнать тесты**

Run: `go test -race -count=1 ./bank/ -run 'TestBankCompany|TestAppendRecorder'`
Expected: PASS.

- [ ] **Step 6: Коммит**

```bash
git add bank/company_metrics.go bank/company_metrics_test.go bank/harness_test.go
git commit -m "bank: guard company metrics against duplicate periods (T7)"
```

**Модель субагента:** `deepseek/deepseek-flash`.

---

### Task E: T8 — реестр и энфорсер dagu

**Files:**
- Modify: `bank/importers.go`
- Test: `bank/importers_test.go`
- Modify: `cbr/importers.go`
- Modify: `rosstat/importers.go`

**Interfaces:**
- Produces:
  - `daguStats(data []byte) []string` — имена из строк `- CLICKHOUSE_IMPORT_STAT=…` (комментарии не считаются);
  - `daguStepIDs(data []byte) []string` — значения `- id: …`;
  - проверка уникальности `id` в `TestBankImportersNamedAndRegistered`.
- Consumes: `daguBankYAML`, `foreignDaguSteps`, `unscheduledTBank` (существуют).

- [ ] **Step 1: Написать падающие тесты**

```go
func TestDaguStatsIgnoreComments(t *testing.T) {
	data := []byte("- id: a\n  env:\n    - CLICKHOUSE_IMPORT_STAT=a\n# - CLICKHOUSE_IMPORT_STAT=commented\n")
	if got := daguStats(data); !slices.Equal(got, []string{"a"}) { t.Fatalf("got %v", got) }
}

func TestDaguStepIDsDuplicate(t *testing.T) {
	data := []byte("- id: a\n- id: a\n")
	if got := daguStepIDs(data); len(got) != 2 { t.Fatalf("got %v", got) }
	// проверку дубля выполняет энфорсер; тест — на реальном файле и синтетике
}
```

Добавить в `TestBankImportersNamedAndRegistered` проверку: `ids := daguStepIDs(data)`; дубль → `t.Errorf`.

- [ ] **Step 2: Прогнать — убедиться, что падают**

Run: `go test -race -count=1 ./bank/ -run 'TestDagu|TestBankImporters'`
Expected: FAIL (хелперов нет; закомментированный шаг засчитывается).

- [ ] **Step 3: Реализовать парсеры**

```go
var daguStatRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+CLICKHOUSE_IMPORT_STAT=([A-Za-z0-9_]+)[ \t]*$`)
var daguIDRef = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+id:[ \t]*([A-Za-z0-9_]+)[ \t]*$`)
```

`daguStats`/`daguStepIDs` — `FindAllStringSubmatch` + извлечение группы; комментарий «разбор регуляркой по всему файлу».

- [ ] **Step 4: Синхронные докблоки**

В трёх реестрах заменить «регистрируются … а не обёртки» на точную формулировку: регистрируется сам импортёр пакета (значение/указатель); производные публикует шаблон через `AfterImport`; у импортёров со своим `Import()` это названо. Сократить докблок `bank/importers.go`, сохранив: семь записей, литералы, три префлайта, отсутствие шага Т-Банка, `company_financials`.

- [ ] **Step 5: Прогнать тесты и validate**

Run: `go test -race -count=1 ./bank/ ./cbr/ ./rosstat/`
Run: `dagu validate dagu/bank.yaml`
Expected: PASS; exit 0 (строка `No auth.mode configured` — Ruling, не ошибка).

- [ ] **Step 6: Коммит**

```bash
git add bank/importers.go bank/importers_test.go cbr/importers.go rosstat/importers.go
git commit -m "bank: tighten registry enforcer and sync docblocks (T8)"
```

**Модель субагента:** `deepseek/deepseek-flash`.

---

### Task F: T9 + T10 + T11 — каталог, MCP, README, роадмап

**Files:**
- Modify: `bank/series_meta.go`
- Modify: `ARCHITECTURE.md` (§6.1, §7)
- Modify: `scripts/mcp_check.py`
- Modify: `README.md`
- Modify: `docs/ROADMAP_KEY_RATE.md`

**Interfaces:** новых функций нет; правки текста и имён переменных.

- [ ] **Step 1: T9 — каталог и ARCHITECTURE §6.1**

- `series_meta.go:109,218`: заменить «стандарт §6.8 неприменим» на формулировку «имена фиксирует код/список, поэтому §6.8 выполняется; новое имя источника — warn + импорт по §6.6».
- `sberFinSeriesOrigin`: указать полный путь из `sberRpbuXlsDataUrl` (включая `/common/img/uploaded/redirected/com/...`).
- `vtbRatioDescription`: удалить «(годовые — с тем же месяцем годом раньше)».
- `ARCHITECTURE.md` §6.1: добавить строку `bank_company_metrics` (пакет `bank/`, таблица `company_financials`, источник — сырые таблицы, последний шаг расписания; 25 рядов каталога).

- [ ] **Step 2: T10 — MCP-проверки**

- `scripts/mcp_check.py`: `bank_view` → `bank_view_raw`; перенести длинные `call_tool` в банковском блоке (витрина, source-цикл, каталог, company-слой).
- Run: `python3 -m py_compile scripts/mcp_check.py` — OK.

- [ ] **Step 3: T11 — README и роадмап**

- `README.md`: «тем PR» → «этим PR»; «шесть импортёров пакета» → «шесть импортёров-источников и внутренний маппер `bank_company_metrics` (семь записей реестра)»; список шагов банковской цепочки — 7 с `company_views` после `bank_company_metrics`.
- `ARCHITECTURE.md` §7 (строка расписания банка): «6 шагов» → 7 с `company_views`.
- `docs/ROADMAP_KEY_RATE.md`: в конец абзаца «Фаза 3.1» добавить: «Отложенные миноры финального ревью T1–T11 закрыты (Ruling — N), проверки: `make all` …, `dagu validate …`, живые прогоны …; PR #<N>». В «Известный мелкий долг» — строку `bank/`: отложенные миноры закрыты, Ruling-строки в PR.

- [ ] **Step 4: Проверки**

Run: `make all`
Run: `dagu validate dagu/bank.yaml`
Run: `git diff --check`
Expected: всё зелёное; exit 0.

- [ ] **Step 5: Коммит**

```bash
git add bank/series_meta.go ARCHITECTURE.md scripts/mcp_check.py README.md docs/ROADMAP_KEY_RATE.md
git commit -m "bank: sync catalog, mcp checks and docs (T9-T11)"
```

**Модель субагента:** `deepseek/deepseek-flash`.

---

## Финальная волна (контроллер)

- [ ] **Step 1:** `make all`; `go test -race -count=1 ./bank/`; `dagu validate dagu/bank.yaml`; `git diff --check`.
- [ ] **Step 2:** Живые прогоны: `make import STAT=rus_vtb_group_ifrs` (ожидаемо `Imported 0` на гейте), `make import STAT=domrf_mortgage` (15 или no-op), `make import STAT=bank_company_metrics` (829, повтор 829). Записать числа в леджер и PR.
- [ ] **Step 3:** Финальное ревью ветки (`deepseek/deepseek-flash`), фикс-волна, scoped re-review (`anthropic/haiku`).
- [ ] **Step 4:** Обновить `docs/ROADMAP_KEY_RATE.md` числами приёмки; открыть один PR с base `main`; merge не делать.
