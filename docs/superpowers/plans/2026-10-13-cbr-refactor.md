# cbr/ Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Привести пакет `cbr/` к единому стандарту (шаблон Б, батч-запись, HTTP только через `util`, logrus, «живые» тесты каталога), собрать все источники `www.cbr.ru` в папку `cbr/` и обеспечить префикс `cbr_` у всех таблиц и имён импортёров.

**Architecture:** Каждый импортёр — `util.ClickHouseImport` (XLSX → `ImportFunc`; XML/HTML → `CrawFunc`), динамический URL — в override `Import()`, каталог и витрина — через `publishedStat`/`publishSeries`. `colly` из пакета уходит; HTML-страницы тянутся `util.GetFile` и разбираются `goquery`; XML ЦБ — `util.GetXML` (с новым `CharsetReader` под windows-1251). Разбор отделён от сети (чистые функции от `[]byte`/`*excelize.File`) — на этом стоят офлайн-тесты на реальных урезанных фикстурах. Регистрация — единый реестр `cbr/importers.go` с тестом-энфорсером префиксов.

**Tech Stack:** Go (модуль `github.com/kmlebedev/clickhouse-import-rosstat`), `util.ClickHouseImport`/`GetXlsx`/`GetFile`/`GetXML`/`ParseXML`, `github.com/PuerkitoBio/goquery` (прямая зависимость), `golang.org/x/text/encoding/charmap` (прямая), `excelize/v2`, logrus; ClickHouse ReplacingMergeTree; dagu.

**Spec:** `docs/superpowers/specs/2026-10-13-cbr-refactor-design.md` (коммит `6f67036`, ветка `feat/cbr-refactor`).

## Global Constraints

- Ветка `feat/cbr-refactor`; незакоммиченная правка владельца `deploy/staging/dagu.service` — не трогать и не коммитить; перед коммитом — `git status`, в индекс только свои файлы.
- Новых зависимостей не добавлять (`goquery`, `x/text` — уже прямые в `go.mod`; `colly` остаётся в `go.mod` для legacy `financial/`, но из `cbr/` удаляется полностью).
- Вставки в ClickHouse — только батчами `PrepareBatch → Append(err!) → Send`; HTTP — только через `util.*` (в `cbr/` ноль `colly` и ноль `fmt.Print*`); логирование — logrus; сетевых вызовов в `init()` нет.
- DDL каждого импортёра — в `%s`-форме (`CREATE TABLE IF NOT EXISTS %s (`): `util.ClickHouseImport` исполняет `fmt.Sprintf` (литерал без `%s` даёт `%!(EXTRA…)` — дефект, пойманный в пункте 1.3).
- **Данные не меняются**: имена рядов, единицы, семантика дат, полный upsert истории. Переименования — только `households_b_mes` → `cbr_households_b_mes` (таблица + `Name()` + шаг DAG, см. Task 6).
- `Name()` всех импортёров — с префиксом `cbr_`; таблица — `cbr_*` либо общая `macro_series` (allowlist только для `cbr_mp_survey`).
- Разбор отделён от сети: парсеры принимают `[]byte`/`*excelize.File` и не ходят в интернет; тесты офлайн на фикстурах в `cbr/testdata/`.
- Фикстуры — реальные артефакты источников: XLSX урезаются «хирургией» zip-до нужного листа; страницы можно хранить целиком, если ≤ ~1 МБ, иначе вырезается контейнер с якорем/таблицей без изменения состава его детей; XML — как отдаёт источник.
- Ручные запуски — через `make` (env подключает Makefile); dev-ClickHouse поднят. Секретов в коде нет.
- Каждый Task заканчивается gofmt-чистым кодом, зелёными `go build ./...` и `go test ./cbr/` (где применимо), коммитом кратко по-английски.

## Review Focus

1. **Источник сменил структуру** (XML-поле, HTML-селектор, лист XLSX) — импорт обязан падать ошибкой до `Send`, а не писать «0 rows». Тесты: fail-fast в Tasks 4–5 (пустой XML/страница без таблицы/без ссылки), fail-fast-разборы в Task 6.
2. **Битые значения внутри потока** (нечисловая ставка, пустой `Record`, пустая ячейка) — точка пропускается либо ошибка, соседние не страдают. Тесты: Tasks 4–5 (пустой ячейки/записи), Task 6 (существующие пропуски пустых).
3. **Точность и даты** — значения XML обязаны совпасть с прежними до Float32 (85.4811019897461, 11422.9501953125), даты — по прежним layout'ам (`02.01.2006`, `02/01/06`, `DD.MM.YYYY`). Тесты: спот-значения в Tasks 4–5.
4. **Идемпотентность** — повторный прогон не двоит (ключи ReplacingMergeTree, полный upsert). Проверка: Task 11.
5. **Переименование households** — ни одной живой ссылки на старое имя; `DROP` только после сверок. Проверка: grep в Task 6, миграция в Task 11.

---

### Task 1: Базовая линия данных (до любого кода)

**Files:**
- Create: `<SDD-workspace>/cbr-baseline.md` (scratch-файл процесса; в git не коммитится)

**Interfaces:**
- Produces: файл-снимок, который Task 11 читает для сверки «данные не изменились».

- [ ] **Step 1: Снять снимок per-series через MCP (`kimi_reader`, витрина `v_cbr_macro`)**

```sql
SELECT series, count() AS n, min(date) AS d0, max(date) AS d1
FROM v_cbr_macro GROUP BY series ORDER BY series;
```

Плюс спот-значения и последние точки:
```sql
SELECT series, argMax(date, date) AS last_date, argMax(value, date) AS last_value
FROM v_cbr_macro GROUP BY series ORDER BY series;

SELECT series, date, value FROM v_cbr_macro
WHERE (series = 'cbr_currency_usd' AND date = '2026-10-08')
   OR (series = 'cbr_gold' AND date = '2026-10-08')
ORDER BY series;
```

Снимок — обязательный минимум: per-series `n`, `d0`, `d1`, `last_date`, `last_value`; числа из ответа переносятся в файл дословно, без округлений.

- [ ] **Step 2: Записать снимок в `<SDD-workspace>/cbr-baseline.md`**

Таблица: `series | n | d0 | d1 | последняя точка (дата, значение)`. Отдельным блоком: `count()` из `v_cbr_macro` для серии `households_b_mes` (это же число — строки старой таблицы для миграции Task 11).

**Коммита нет** — задача read-only; ревьюер проверяет файл и повторяет запрос.

---

### Task 2: `util.ParseXML` + windows-1251 в `GetXML`

**Files:**
- Modify: `util/xml.go`
- Test: `util/xml_test.go`

**Interfaces:**
- Produces: `func ParseXML(body []byte, out any) error` — разбор XML из байтов с поддержкой объявленных кодировок (`windows-1251`/`cp1251` через `charmap.Windows1251`; `utf-8`/`utf8` pass-through; прочее — ошибка), пин корня через `XMLName` сохраняется; `GetXML(url string, out any) error` — тот же контракт, внутри теперь fetch + `ParseXML`.

- [ ] **Step 1: Написать падающие тесты** (`util/xml_test.go`)

Стиль — как существующие тесты файла (httptest, где нужен fetch). Новые:
- `TestParseXMLWindows1251`: байты-фикстура с объявлением `windows-1251` и кириллицей (взять из живого CBR-ответа: `<ValCurs …><Record Date="08.10.2026" Id="R01235"><Value>85,4811</Value></Record></ValCurs>`, перекодировав в windows-1251 при подготовке) → структура с `XMLName xml.Name \`xml:"ValCurs"\`` и полем `Value` = `85,4811`.
- `TestParseXMLUnknownCharset`: объявление `koi8-r` (или любое неподдерживаемое) → ошибка с кодировкой в тексте.
- `TestParseXMLWrongRoot`: XML с другим корнем → ошибка (пин сохраняется).

- [ ] **Step 2: Прогнать — FAIL**

Run: `go test ./util/ -run TestParseXML -v` → компиляция падает (`ParseXML` не определён).

- [ ] **Step 3: Реализовать**

В `util/xml.go`: вынести тело декодера в `ParseXML(body []byte, out any) error` (тот же `xml.NewDecoder` + `Entity = xml.HTMLEntity`), добавить `decoder.CharsetReader` (switch по `strings.ToLower(charset)`); `GetXML` = fetch (как сейчас: статус, пустое тело, таймаут) → `return ParseXML(body, out)`. Обновить комментарий хелпера (кодировки ЦБ).

- [ ] **Step 4: Прогнать — PASS**

Run: `go test ./util/ -run TestParseXML -v && go test ./util/ -v` → все зелёные.

- [ ] **Step 5: Commit**

```bash
git add util/xml.go util/xml_test.go
git commit -m "util: ParseXML + windows-1251 charset for CBR XML feeds"
```

---

### Task 3: Удаление дублей и мёртвого кода

**Files:**
- Delete: `cbr/avgproc_stav.go`, `cbr/queries.go`, `cbr/purcb.go`, `financial/cbr.go`
- Modify: `financial/main.go` (убрать вызов `crawCbr` в ветке VTBR), `financial/db.go` (убрать константу `createCbrRates` и её исполнителя — найти grep'ом)

**Interfaces:**
- Produces: реестр без коллизий `Name()`; `make import STAT=cbr_currency_usd` запускает ровно один импортёр; URL заглушки `purcb.go` зафиксированы в спеке §9.

- [ ] **Step 1: Удалить файлы и следы**

```bash
git rm cbr/avgproc_stav.go cbr/queries.go cbr/purcb.go financial/cbr.go
```
Затем в `financial/main.go` удалить строку с `crawCbr(conn)`; в `financial/db.go` удалить `createCbrRates` и место, где она исполнялась (если исполнитель есть — grep `createCbrRates`); проверить, что `loadCbrRate`/`crawCbr` больше нигде не упоминаются.

- [ ] **Step 2: Проверить сборку и отсутствие следов**

Run: `grep -rn "crawCbr\|loadCbrRate\|createCbrRates\|cbrQueriesDataset" --include=*.go . | grep -v .worktrees` → пусто;
Run: `go build ./... && go test ./cbr/ ./financial/...` → ok (тесты `cbr` не сломаны; если `series_meta_test.go` ссылался на удалённое — поправить только ссылку).

- [ ] **Step 3: Commit**

```bash
git add -A cbr financial
git commit -m "cbr: remove duplicate/dead importers (avgproc_stav, queries, purcb, financial cbr scraper)"
```

---

### Task 4: XML-импортёры — `currency_usd` и `metal_gold`

**Files:**
- Modify: `cbr/currency_usd.go`, `cbr/metal_gold.go`
- Create: `cbr/testdata/currency_usd.xml`, `cbr/testdata/metal_gold.xml`, `cbr/currency_usd_test.go`, `cbr/metal_gold_test.go`

**Interfaces:**
- Consumes: `util.GetXML`, `util.ParseXML` (Task 2); `publishedStat`/`publishSeries` (`cbr/views.go`).
- Produces: `type cbrPoint struct{ date time.Time; value float64 }` (общий тип для XML/HTML-разборов — объявить здесь); `type cbrValCurs struct{ XMLName xml.Name \`xml:"ValCurs"\`; Records []struct{ Date string \`xml:"Date,attr"\`; Value string \`xml:"Value"\` } \`xml:"Record"\` }`; `type cbrMetallDoc struct{ XMLName xml.Name \`xml:"Metall"\`; Records []struct{ Code string \`xml:"Code,attr"\`; Date string \`xml:"Date,attr"\`; Buy string \`xml:"Buy"\` } \`xml:"Record"\` }`; `func parseCurrencyUSD(doc *cbrValCurs) ([]cbrPoint, error)`; `func parseMetalGold(doc *cbrMetallDoc) ([]cbrPoint, error)` (фильтр `Code == "1"` — золото); `type cbrCurrencyUSD struct{ util.ClickHouseImport }` и `type cbrGold struct{ util.ClickHouseImport }` (override `Import` ставит `DataUrl` и делегирует; `CrawFunc: cbrCurrencyUSDCraw` / `cbrGoldCraw`).

URL'ы: `https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1=%s&date_req2=%s&VAL_NM_RQ=R01235` (обе даты `02/01/2006`, `date_req1` = `17.09.2013` как глубина текущей истории); `https://www.cbr.ru/scripts/xml_metall.asp?date_req1=%s&date_req2=%s` (`DD/MM/YYYY`). Разбор: `Value`/`Buy` — замена запятой на точку, `strconv.ParseFloat(err!)`, дата — `time.Parse("02.01.2006", r.Date)`; пустая запись — пропуск; **0 записей — ошибка**. `CrawFunc` (идиом `customs`: package-level `ctx = context.TODO()`): `util.GetXML(url, &doc)` → точки → `PrepareBatch("INSERT INTO "+table)` → `Append(err!)` → `Send`.

- [ ] **Step 1: Фикстуры** — `curl --cacert russian_trusted_root_ca_pem.crt` оба URL с коротким диапазоном (08.10.2026–13.10.2026), сохранить как есть (маленькие).
- [ ] **Step 2: Падающие тесты** — `parseCurrencyUSD`/`parseMetalGold` на `util.ParseXML(фикстура, …)`: спот `2026-10-08` = `85.4811` / `11422.95` (сравнение как Float32-значение с 1e-6); число точек; fail-fast: пустой `<ValCurs/>`/`<Metall/>` → ошибка. Двусторонний каталог (идиом `checkCbrSeriesMeta`, helper добавить в `cbr/series_meta_test.go` по образцу `minfin/series_meta_test.go`: `appendRecorder` + «разбор ⊆ каталог» и «каталог ⊆ разбор»).
- [ ] **Step 3: FAIL → реализация → PASS** (`go test ./cbr/ -run 'Currency|Metal' -v`).
- [ ] **Step 4: Проверка по живому источнику** (без записи): `parseCurrencyUSD` на полном диапазоне даёт ≥ 3200 точек (с 2013-09-17); `parseMetalGold` — с глубиной текущей таблицы.
- [ ] **Step 5: Commit** — `cbr: migrate currency_usd and metal_gold to CBR XML API + tests`

---

### Task 5: HTML-транспорт — `key_rate` и три страницы-поисковика ссылок

**Files:**
- Modify: `cbr/key_rate.go`, `cbr/Infl_exp.go`, `cbr/indicators_cpd.go`, `cbr/credit_m2x.go`
- Create: `cbr/links.go` (общий хелпер), `cbr/testdata/{key_rate.html,infl_exp_page.html,aipd_page.html,credit_m2x_page.html,infl_exp.xlsx,aipd.xlsx,credit_m2x.xlsx}`, тесты `cbr/key_rate_test.go`, `cbr/Infl_exp_test.go`, `cbr/indicators_cpd_test.go`, `cbr/credit_m2x_test.go`

**Interfaces:**
- Consumes: `util.GetFile`; `cbrPoint` (Task 4); `checkCbrSeriesMeta` helper.
- Produces: `func keyRateRows(html []byte) ([]cbrPoint, error)` (goquery: `.table-wrapper .table .data tr`, пропуск строки-заголовка «Дата», запятая→точка, `02.01.2006`; 0 строк — ошибка); `func findLink(page []byte, selector string) (string, error)` в `cbr/links.go` (goquery, первая ссылка `href`, пусто — ошибка); `cbrKeyRate` — CHI + `CrawFunc` (fetch через `util.GetFile`, `io.ReadAll`, разбор, батч); `Infl_exp`/`indicators_cpd`/`credit_m2x` — их `Import()`-override заменяет colly на `util.GetFile` + `findLink` (селекторы сохраняются: `a.versions_item[href$=".xlsx"]`; позиционный селектор aipd — как в текущем коде; `a.referenceable[href$="credit_m2x.xlsx"]`), дальше — прежний CHI+`ImportFunc`.

- [ ] **Step 1: Фикстуры** — страницы целиком (если > 1 МБ — вырезать контейнер, сохранив состав детей, чтобы селекторы остались валидны); XLSX — скачать по найденным ссылкам и урезать zip-хирургией до нужного листа (`Данные для графиков` / `Лист1` / лист credit_m2x).
- [ ] **Step 2: Падающие тесты** — key_rate: 170+ строк, спот (первая/последняя дата+значение); fail-fast: страница без `.data` → ошибка. findLink: на каждой странице находит ожидаемый файл (суффикс имени); страница без якоря → ошибка. Двусторонние каталог-тесты: key_rate (1 ряд), infl_exp (2), indicators_cpd (2), credit_m2x (22) — на своих фикстурах.
- [ ] **Step 3: FAIL → реализация → PASS.**
- [ ] **Step 4: Живая проверка** (без записи): `findLink` на живых страницах находит ссылку; key_rate живой HTML парсится.
- [ ] **Step 5: Commit** — `cbr: HTML transports via util.GetFile+goquery (key_rate, link finders) + tests`

---

### Task 6: XLSX-пакет: `households` (rename), `m2`, `new_loans_corp`, `new_loans_ind`

**Files:**
- Modify: `cbr/households.go`, `cbr/m2.go`, `cbr/new_loans_corp.go`, `cbr/new_loans_ind.go`, `cbr/views.go` (SELECT → `cbr_households_b_mes`), `cbr/series_meta.go` (origin household-ряда, если упоминает имя), `cbr/series_meta_test.go` (удалить `TestHouseholdsNamesCovered` + `householdsBMesDBNames` — заменяются фикстурным двусторонним тестом), `dagu/cbr.yaml` (id шага `households_b_mes` → `cbr_households_b_mes`)
- Create: `cbr/testdata/{households_bm.xlsx,monetary_agg_SA.xlsx,new_loans_corp.xlsx,new_loans_ind.xlsx}`, тесты `cbr/households_test.go`, `cbr/m2_test.go`, `cbr/new_loans_corp_test.go`, `cbr/new_loans_ind_test.go`

**Interfaces:**
- Produces: 4 импортёра на CHI+`ImportFunc` (прежние парсеры как `xxxImport(xlsx *excelize.File, batch driver.Batch) error`); `households` — таблица и `Name()` = `cbr_households_b_mes` (DDL в `%s`-форме); парсеры без сети, с прежней семантикой (пропуск пустых ячеек сохранён).

- [ ] **Step 1: Фикстуры** — скачать 4 источника, zip-хирургией оставить лист, который читает парсер (households — «Балансы»; m2 — «Денежные агрегаты»; loans — как в коде).
- [ ] **Step 2: Падающие тесты** — двусторонний каталог на каждый (households: 63+7 имён из фикстуры ↔ каталог; m2: 1; loans: 26 и 96); спот-значения (1–2 на импортёр — зафиксировать из текущей БД-базовой линии); fail-fast: пустой лист → ошибка, не «0 rows».
- [ ] **Step 3: FAIL → реализация (конверсия на CHI+ImportFunc, `%s`-DDL, ренам households, dagu-шаг) → PASS.**
- [ ] **Step 4: Grep-контроль переименования**

Run: `grep -rn "households_b_mes" --include=*.go --include=*.yaml . | grep -v .worktrees` → только `cbr_households_b_mes`; `dagu validate dagu/cbr.yaml` → exit 0.

- [ ] **Step 5: Commit** — `cbr: batch writes + catalog tests for XLSX importers; rename households table to cbr_households_b_mes`

---

### Task 7: `ruonia` и `bank_int_rat` — чистка и тесты

**Files:**
- Modify: `cbr/ruonia.go` (удалить `fmt.Printf:23`), `cbr/bank_int_rat.go` (только если тривиальные правки стиля)
- Create: `cbr/testdata/{ruonia.xlsx,regulatory_rates.xlsx}`, `cbr/ruonia_test.go`, `cbr/bank_int_rat_test.go`

**Interfaces:**
- Produces: у обоих — двусторонние каталог-тесты на фикстурах (ruonia: 7 метрик RUONIA; bank_int_rat: 7 рядов ставок); ruonia остаётся CHI c override `Import` (динамический `DataUrl`), без `fmt.Print*`.

- [ ] **Step 1: Фикстуры** — ruonia: `https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/14315?Posted=True&FromDate=01/01/2019&ToDate=<дата>` → zip-хирургия до листа `RC`; bank_int_rat: `regulatory_rates.xlsx` → лист «в рублях».
- [ ] **Step 2: Падающие тесты → реализация → PASS.**
- [ ] **Step 3: Commit** — `cbr: ruonia/bank_int_rat cleanup and catalog tests`

---

### Task 8: Переезд `surveys/` → `cbr/`

**Files:**
- Move (переименование в пакет `cbr`): `surveys/mp_survey.go` → `cbr/mp_survey.go` (var `importer` → `mpSurveyImporter`), `surveys/parse.go` → `cbr/mp_survey_parse.go`, `surveys/mp_survey_test.go` → `cbr/mp_survey_test.go`, `surveys/parse_test.go` → `cbr/mp_survey_parse_test.go`, `surveys/testdata/mp_survey_data.xlsx` → `cbr/testdata/mp_survey_data.xlsx`
- Merge: `surveys/series_meta.go` (13 записей) → `cbr/series_meta.go`; `surveys/views.go` (`v_cbr_survey`) → `cbr/views.go`
- Delete: `surveys/` (каталог целиком), `dagu/surveys.yaml`
- Modify: `main.go` (убрать blank-import `surveys`), `dagu/cbr.yaml` (добавить последним шаг `cbr_mp_survey` из удалённого surveys.yaml — те же env/retry)

**Interfaces:**
- Produces: `cbr_mp_survey` в пакете `cbr`; `Name()`, `macro_series`, `source='cbr_survey'`, `v_cbr_survey`, грант — **без изменений**; тесты `cbr/mp_survey*` зелёные; `dagu/cbr.yaml` — 13 шагов.

- [ ] **Step 1: Перенос файлов и слияние** (`git mv`, package `surveys` → `cbr`; снять дубли имён: `so` константа `mpSurveySource` остаётся одна; конфликтов типов нет — проверить `go build`).
- [ ] **Step 2: Проверки** — `grep -rn "surveys" --include=*.go . | grep -v .worktrees` → пусто; `go test ./cbr/ -v` — все тесты, включая перенесённые, зелёные; `dagu validate dagu/cbr.yaml` → exit 0; `ls dagu/surveys.yaml` → нет.
- [ ] **Step 3: Commit** — `cbr: move cbr_mp_survey from surveys/ into the cbr package (dagu step merged)`

*(Документационные упоминания `surveys/` — в Task 10.)*

---

### Task 9: Реестр импортёров и тест-энфорсер префиксов

**Files:**
- Create: `cbr/importers.go`, `cbr/importers_test.go`
- Modify: все файлы импортёров `cbr/*.go` (убрать персональные `init()`-регистрации, выставить package-level переменные вида `var cbrKeyRateImporter = &publishedStat{ImportStat: …, meta: cbrKeyRateSeriesMeta}`)

**Interfaces:**
- Produces: `type cbrImporter struct { chimport.ImportStat; table string }`; `var cbrImporters = []cbrImporter{ {cbrKeyRateImporter, "cbr_key_rate"}, …, {mpSurveyImporter, "macro_series"} }` (13 записей); единственный `init()` в `importers.go` регистрирует все.

- [ ] **Step 1: Падающий тест** `cbr/importers_test.go`:
  - `TestCbrImportersPrefixedAndRegistered`: (а) каждый `Name()` — `cbr_`-префикс; (б) `table` — `cbr_`-префикс или `macro_series` (только `cbr_mp_survey`); (в) имена уникальны и присутствуют в `chimport.Stats`; (г) каждый `CLICKHOUSE_IMPORT_STAT=<name>` из `dagu/cbr.yaml` (простой построчный разбор файла регуляркой, без YAML-зависимостей) есть в реестре.
- [ ] **Step 2: FAIL → реализация реестра → PASS** (`go test ./cbr/ -run TestCbrImporters -v`).
- [ ] **Step 3: Полный прогон** — `go test ./cbr/ -v && go build ./...`.
- [ ] **Step 4: Commit** — `cbr: importer registry with name/table prefix enforcement test`

---

### Task 10: Документация

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`, `docs/KEY_RATE_CHANNELS.md` (§8), `sql/key_rate_coverage.sql`

**Interfaces:**
- Consumes: финальные имена/таблицы/расписание после Tasks 3–9.

- [ ] **Step 1: `ARCHITECTURE.md`** — чейнджлог (новая запись 2026-10-13: рефакторинг `cbr/` — единый стандарт, XML-транспорты, переезд `surveys/`, ренам households, удаления); §2 — строка `cbr/` переписана (все cbr.ru-источники, включая `cbr_mp_survey`; строка `surveys/` удалена; `financial/` — пометка про удаление cbr-скрапера); §3 — `util` (новые `ParseXML`/charset); §6.1 — строка `cbr_mp_survey` (пакет `cbr/`, расписание `dagu/cbr.yaml`; из перечня шаговых id — обновить); §6.3 — `v_cbr_macro` (ренам households); §6.5 — абзац про контур мониторинга (пакет `cbr/`); §7 — расписание (13 шагов в `dagu/cbr.yaml`, строка `dagu/surveys.yaml` удалена, households-шаг переименован).
- [ ] **Step 2: `README.md`** — таблица импортёров (строка `cbr`: актуальный список таблиц без `cbr_queries_*` и без `households_b_mes`; с `cbr_households_b_mes`; `cbr_mp_survey` в cbr/); прозаический абзац про контур мониторинга — пакет `cbr/`, расписание; дерево пакетов (убрать `surveys/`, обновить `cbr/`); «Имена импортёров» (заметка про общие таблицы — актуализировать); список тестовых пакетов (убрать `surveys`).
- [ ] **Step 3: `docs/KEY_RATE_CHANNELS.md` §8** — `households_b_mes` → `cbr_households_b_mes` (два места); `docs/ROADMAP_KEY_RATE.md` §4.1/§5 — имя таблицы в таблицах покрытия.
- [ ] **Step 4: `sql/key_rate_coverage.sql`** — households-строка на новое имя.
- [ ] **Step 5: Проверки и Commit**

Run: `grep -rn "households_b_mes" ARCHITECTURE.md README.md docs/ sql/ | grep -v superpowers` → только `cbr_households_b_mes` (исторические блоки приёмки не трогаем); `grep -rn "surveys/" ARCHITECTURE.md README.md | grep -v superpowers` → пусто.
```bash
git add ARCHITECTURE.md README.md docs/KEY_RATE_CHANNELS.md sql/key_rate_coverage.sql
git commit -m "docs: sync ARCHITECTURE/README/catalogs for the cbr refactor"
```

---

### Task 11: Живая приёмка и статус фазы 3

**Files:**
- Modify: `docs/ROADMAP_KEY_RATE.md`

**Interfaces:**
- Consumes: `<SDD-workspace>/cbr-baseline.md` (Task 1), все предыдущие задачи.

- [ ] **Step 1: `make all`** — зелёный (`0 issues.`, vet, тесты, сборки).
- [ ] **Step 2: Grep-чистота**

```bash
grep -rn "colly\|gocolly" --include=*.go cbr/ && echo "FAIL" || echo "OK"
grep -rnE "fmt\.Print" --include=*.go cbr/ && echo "FAIL" || echo "OK"
```

- [ ] **Step 3: Прогон всех 13 импортёров** (по одному, фиксируя `Imported N rows of <name>`):
`make import STAT=cbr_key_rate`, `…=cbr_currency_usd`, `…=cbr_gold`, `…=cbr_households_b_mes`, `…=cbr_m2`, `…=cbr_loans_to_corporations`, `…=cbr_loans_to_individuals`, `…=cbr_infl_exp`, `…=cbr_indicators_cpd`, `…=cbr_credit_m2x`, `…=cbr_bank_int_rate`, `…=cbr_ruania`, `…=cbr_mp_survey`. Повтор одного из них — идемпотентен (`v_cbr_macro`/`v_cbr_survey` счётчики не растут).
- [ ] **Step 4: Сверка с базовой линией** — тот же per-series запрос, что в Task 1: `count/min/max` и последние точки совпадают (допустимо «не меньше» для растущих дневных рядов + прежние значения на месте).
- [ ] **Step 5: Миграция households** — `SELECT count(), min(date), max(date) FROM cbr_households_b_mes` vs старое число из baseline → совпало? Тогда:
`make ch-sql FILE=sql/<временный>.sql` с `DROP TABLE households_b_mes;` (временный файл удалить, не коммитить). При расхождении — **стоп**, разбор.
- [ ] **Step 6: `make mcp-user` → `make mcp-check`** — ПРОВЕРКА ПРОЙДЕНА (включая `v_cbr_macro`, `v_cbr_survey`; сырые таблицы — ACCESS_DENIED). `dagu validate dagu/cbr.yaml` → exit 0.
- [ ] **Step 7: Статус фазы 3 в роадмапе** — `docs/ROADMAP_KEY_RATE.md`: в описании фазы 3 добавить `✅ **сделан этим PR** — приёмка ниже` и блок `#### Живая приёмка 2026-10-13 — фаза 3 (рефакторинг cbr/) — ✅ выполнен`: ссылки на спеку/план, перечень изменений (транспорты, удаления, ренам, переезд, реестр, тесты), нумерованные факты прогонов из Step 1–6 с числами.
- [ ] **Step 8: Commit**

```bash
git add docs/ROADMAP_KEY_RATE.md
git commit -m "roadmap key rate: mark phase 3 done (cbr/ refactor acceptance)"
```

---

## После задач (вне нумерации)

Финальное ревью всей ветки (самая способная модель) + `verification-before-completion`; затем один PR `feat/cbr-refactor` → `main`: источник-транспорты, единый стандарт, удаления, ренам households, переезд `cbr_mp_survey`, приёмка. Спека и план — первыми коммитами ветки (уже на месте).
