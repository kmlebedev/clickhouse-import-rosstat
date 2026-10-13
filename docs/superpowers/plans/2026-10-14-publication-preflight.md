# Publication preflight для импортёров с редким обновлением файла — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** сделать точку контроля публикации (HEAD-отпечаток версии + `source_publications`) декларативным свойством общего шаблона `util.ClickHouseImport` и включить её у десяти импортёров `cbr/` и у тех импортёров остальных пакетов, что проходят три критерия применимости.

**Architecture:** Три опциональных поля шаблона (`PublicationCountPoints`, `PublicationSource`, `AfterImport`) и полный цикл внутри `ClickHouseImport.Import`: DDL → `BeforeImport` → префлайт → точки → `AfterImport` (производные) → строка версии последней. Обёртка `publishedStat` упраздняется: публикация производных переезжает в `AfterImport`, потому что «версия последней» (ARCHITECTURE §8 п. 13) достижима только внутри шаблона. Общий механизм (`HeadFingerprint`/`PublicationKnown`/`SavePublication`) уже есть в `util/publications.go` и переиспользуется.

**Tech Stack:** Go, `util.ClickHouseImport`, `excelize/v2`, logrus; ClickHouse ReplacingMergeTree; `httptest` для HEAD-отпечатка.

**Spec:** `docs/superpowers/specs/2026-10-14-publication-preflight-design.md` (коммит этой ветки). Спека — связывающая власть, план — её аргумент.

## Global Constraints

- Ветка `feat/publication-preflight`; незакоммиченная правка владельца `deploy/staging/dagu.service` — не трогать и не коммитить; перед коммитом `git status`, в индекс только свои файлы.
- Новых зависимостей не добавлять.
- Вставки в ClickHouse — только батчами `PrepareBatch → Append(err!) → Send`; HTTP — только через `util.*`; логирование — logrus; сетевых вызовов в `init()` нет.
- Схема БД не меняется. Строка версии пишется **последней**: после `Send` точек и после `AfterImport`. Недоступный HEAD — не сбой: полный импорт без записи версии.
- `points` версии = результат запроса `PublicationCountPoints` (не `batch.Rows()`), с `FINAL` и, у владельцев общей `macro_series`, с `WHERE source = '<…>'`.
- `source_publications` агенту не выдана: `sql/mcp_kimi_reader.sql` и `scripts/mcp_check.py` не трогать.
- Каждая задача заканчивается gofmt-чистым кодом, зелёным `go build ./...` и `go test ./...`, коммитом кратко по-английски.
- Ручные команды с ClickHouse/env — только через `make` (Makefile подключает env). Секреты не читать и не печатать.

## Review Focus

1. **Порядок «точки → производные → версия»**: сбой `AfterImport` обязан оставить строку версии незаписанной, иначе следующий прогон уйдёт в no-op с устаревшим каталогом/витриной. Тесты: T1 (порядок вызовов на фейковом коннекте), T3 (приёмка).
2. **Префлайт включён только там, где уместен**: у импортёров без объявления `Import` не делает ни HEAD, ни обращения к `source_publications`. Тесты: T1, T3 (`TestCbrImportersPrefixedAndRegistered`).
3. **Расхождение числа точек**: чек сравнивает `count() FINAL` с записанным в версию — оба берутся из одного запроса импортёра, дубли ячеек (`cbr_households_b_mes` 9030 против 7350) не ломают no-op. Тесты: T1 (`TestPublicationPreflightNoop`), T3 (живая приёмка).
4. **Деградации префлайта**: недоступный HEAD → полный импорт без записи версии; сбой SELECT версии → warn и полный импорт; известная версия с несовпавшим числом → полный импорт (self-heal). Тесты: T1.
5. **Версия пишется только при непустом отпечатке** и её `name` = basename URL — иначе строка версии расходится с той, что уже лежит в БД у `cbr_mo_br` (`full.xlsx`, `4fef4f25716d388e9048149fa296039c`). Тесты: T1, T3.

---

### Task 1: `util` — точка контроля публикации в шаблоне `ClickHouseImport`

**Files:**
- Modify: `util/publications.go`, `util/clickhouse_import.go`
- Test: `util/publications_test.go` (дополнить)

**Interfaces:**
- Consumes: `HeadFingerprint`, `EnsurePublications`, `PublicationKnown`, `SavePublication`, `Publication` (`util/publications.go`); `driver.Conn` (clickhouse-go v2).
- Produces (используют T3–T4 и все прочие импортёры):
  - поля `ClickHouseImport.PublicationCountPoints string`, `.PublicationSource string`, `.AfterImport func(ctx context.Context, conn driver.Conn) error`
  - `type PublicationDecl struct { Source, Url, CountPoints string }`
  - `func (s *ClickHouseImport) PublicationKey() string` — `PublicationSource`, пусто → `TableName`
  - `func (s *ClickHouseImport) Publication() PublicationDecl` — нулевое значение, если `PublicationCountPoints == ""`; иначе `{PublicationKey(), s.DataUrl, s.PublicationCountPoints}`
  - `func PublicationPreflight(ctx context.Context, conn driver.Conn, source, url, countPoints string) (fingerprint string, noop bool, points uint64, err error)`
  - `func CountPoints(ctx context.Context, conn driver.Conn, query string) (uint64, error)`

- [ ] **Step 1: Написать падающие тесты** (`util/publications_test.go`)

Минимальный фейковый коннект (в тестовом файле): тип `fakeConn` с очередью ответов `QueryRow` и записью вызовов `Exec`; `driver.Row` — стаб со `Scan` (пишет `uint64` либо возвращает заданную ошибку), `ScanStruct`/`Err` — заглушки. Тесты:

- `TestPublicationKeyFallsBackToTableName`: `PublicationSource: ""` → ключ `TableName`; `PublicationSource: "cbr_mo_br"`, `TableName: "macro_series"` → `cbr_mo_br`.
- `TestPublicationDeclEmptyWithoutCountPoints`: без `PublicationCountPoints` `Publication()` — нулевое значение (префлайта нет).
- `TestPublicationPreflightHEADUnavailable`: `httptest.NewServer` закрыт → `PublicationPreflight` возвращает `("", false, 0, nil)` **без обращений к conn** (фейк паникует на любом вызове — этим и проверяется).
- `TestPublicationPreflightVersionUnknown`: живой `httptest`-HEAD (200 + `ETag`), `PublicationKnown` → `sql.ErrNoRows` → `noop == false`, `fingerprint` длиной 32 hex, `err == nil`.
- `TestPublicationPreflightNoop`: версия известна с `points = 42`, запрос счёта возвращает 42 → `noop == true`, `points == 42`.
- `TestPublicationPreflightSelfHeal`: версия известна с `points = 42`, счёт возвращает 41 → `noop == false`, тот же отпечаток, `err == nil`.
- `TestPublicationPreflightKnownCheckError`: SELECT версии возвращает ошибку → `noop == false`, `err == nil` (деградация в полный импорт, а не падение).
- `TestCountPoints`: запрос возвращает 7 → `7, nil`; ошибка `Scan` пробрасывается.

- [ ] **Step 2: Прогнать — FAIL**

Run: `go test ./util/ -run 'TestPublication|TestCountPoints' -v`
Expected: компиляция падает (`PublicationKey`/`PublicationPreflight`/`CountPoints` не определены).

- [ ] **Step 3: Реализовать `util/publications.go`**

`PublicationPreflight` переносится из `cbr/mo_br.go:73-104` дословно (те же `log.Warnf`/`log.Infof`, текст no-op — `"%s: no-op, версия %s уже импортирована, %d точек на месте"`), но с параметрами `source`, `url`, `countPoints` и с `CountPoints` вместо `conn.QueryRow` на месте. `CountPoints` — `conn.QueryRow(ctx, query).Scan(&points)` с обёрткой ошибки. Комментарии — в стиле файла (почему HEAD-отказ не ошибка, почему расхождение чисел лечится полным импортом).

- [ ] **Step 4: Реализовать поля и цикл в `util/clickhouse_import.go`**

Поля по интерфейсам выше; `Import` переписывается со `switch` с ранними `return` на сбор `count, err`, чтобы после полного пути могли идти `AfterImport` и запись версии:

```go
// порядок: DDL -> BeforeImport -> префлайт -> точки -> AfterImport -> версия
```

Версия пишется `util.SavePublication` с `Name: path.Base(s.DataUrl)`, `Points` из `CountPoints` — и только при непустом `fingerprint`.

- [ ] **Step 5: Прогнать — PASS**

Run: `go test ./util/ -count=1 && go build ./... && gofmt -l util/`
Expected: тесты зелёные, сборка проходит, gofmt пусто.

- [ ] **Step 6: Commit**

```bash
git add util/publications.go util/publications_test.go util/clickhouse_import.go
git commit -m "util: publication preflight as optional ClickHouseImport fields"
```

---

### Task 2: `cbr/` — упразднить обёртку `publishedStat` (без изменения поведения)

**Files:**
- Modify: `cbr/views.go`, `cbr/importers.go`
- Modify: `cbr/currency_usd.go`, `cbr/metal_gold.go`, `cbr/key_rate.go`, `cbr/ruonia.go`, `cbr/households.go`, `cbr/m2.go`, `cbr/new_loans_corp.go`, `cbr/new_loans_ind.go`, `cbr/Infl_exp.go`, `cbr/bank_int_rat.go`, `cbr/credit_m2x.go`, `cbr/indicators_cpd.go`

**Interfaces:**
- Consumes: `ClickHouseImport.AfterImport` (T1).
- Produces (использует T3): `func afterCbrMacro(meta []util.SeriesMeta) func(context.Context, driver.Conn) error` в `cbr/views.go`; импортёры пакета — конкретные типы в реестре, а не обёртка.

- [ ] **Step 1: Заменить обёртку на поле `AfterImport`**

В `cbr/views.go` удалить `type publishedStat` и `(*publishedStat).Import`, оставить `publishSeries` и добавить:

```go
// afterCbrMacro — производные импортёров пакета: каталог рядов и общая витрина
// v_cbr_macro. Публикуются шаблоном после Send точек и до записи строки версии (§8 п. 13).
func afterCbrMacro(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error { return publishSeries(ctx, conn, meta) }
}
```

В двенадцати литералах вид `&publishedStat{ImportStat: &xxxStat{ClickHouseImport: util.ClickHouseImport{…}}, meta: m}` → `&xxxStat{ClickHouseImport: util.ClickHouseImport{…, AfterImport: afterCbrMacro(m)}}`. Список (проверить `grep -n "publishedStat" cbr/*.go` — после правки совпадений быть не должно): `cbrCurrencyUSDImporter`, `cbrGoldImporter`, `cbrKeyRateImporter`, `cbrRuoniaImporter`, `householdsBMesImporter`, `cbrM2Importer`, `loansToCorporationsImporter`, `loansToIndividualsImporter`, `cbrInflExpImporter`, `cbrBankIntRateImporter`, `cbrCreditM2xImporter`, `cbrIndicatorsCpdImporter`.

Комментарий в `cbr/importers.go` про «обёртку (publishedStat публикует ряды и витрину…)» привести к факту: регистрируемый импортёр — конкретный тип пакета, производные публикует шаблон через `AfterImport`.

- [ ] **Step 2: Прогнать**

Run: `go build ./... && go test ./cbr/ -count=1 && gofmt -l cbr/ && grep -rn "publishedStat" cbr/ --include='*.go'`
Expected: сборка и тесты зелёные, gofmt пусто, `grep` — пусто. Поведение не меняется: те же `Name()`, те же таблицы, та же витрина.

- [ ] **Step 3: Commit**

```bash
git add cbr/
git commit -m "cbr: publish catalog and view via ClickHouseImport.AfterImport"
```

---

### Task 3: `cbr/` — объявление префлайта у десяти импортёров, `cbr_mo_br` на общий путь

**Files:**
- Modify: `cbr/households.go`, `cbr/m2.go`, `cbr/new_loans_corp.go`, `cbr/new_loans_ind.go`, `cbr/bank_int_rat.go`, `cbr/Infl_exp.go`, `cbr/credit_m2x.go`, `cbr/indicators_cpd.go`, `cbr/mp_survey.go`, `cbr/mo_br.go`
- Modify: `cbr/importers.go`, `cbr/importers_test.go`

**Interfaces:**
- Consumes: `PublicationCountPoints`/`PublicationSource`/`AfterImport` (T1), `afterCbrMacro` (T2), `moBrCountPoints` (`cbr/mo_br.go`).
- Produces: у каждого из десяти импортёров — объявление, видимое тестом через `Publication()`; колонка `countPoints` в `cbrImporter`.

- [ ] **Step 1: Написать падающие проверки реестра** (`cbr/importers_test.go`)

Дополнить `TestCbrImportersPrefixedAndRegistered` (или добавить `TestCbrImportersPublicationDeclared`):

- `expectedPublicationCountPoints` — карта литералов (имя → SQL) ровно для десяти имён из спеки §3.5 (`cbr_mp_survey`, `cbr_households_b_mes`, `cbr_m2`, `cbr_credit_m2x`, `cbr_loans_to_corporations`, `cbr_loans_to_individuals`, `cbr_bank_int_rate`, `cbr_indicators_cpd`, `cbr_infl_exp`, `cbr_mo_br`);
- для каждой записи реестра: объявление достаётся `imp.ImportStat.(interface{ Publication() util.PublicationDecl })`; проверяется (а) `decl.CountPoints == imp.countPoints`, (б) `(imp.countPoints != "") == (expectedPublicationCountPoints[name] != "")` и равенство строке из карты, (в) `decl.Source == name` (у `macro_series`-импортёров — `cbr_survey`/`cbr_mo_br`, у остальных совпадает с `TableName`);
- обратное направление: для каждого имени из карты в реестре есть запись (иначе карта описывала бы импортёра, которого нет).

- [ ] **Step 2: Прогнать — FAIL**

Run: `go test ./cbr/ -run TestCbrImporters -v`
Expected: FAIL — поля `countPoints` нет, объявлений нет.

- [ ] **Step 3: Объявить префлайт у восьми табличных импортёров**

В каждом файле константа рядом с таблицей и поле в литерале:

```go
// cbrM2CountPoints — количественный чек версии (§8 п. 13): число точек своего ряда.
// Литерал, как и таблица: запрос — часть утверждения «эти точки мои».
const cbrM2CountPoints = "SELECT count() FROM cbr_m2 FINAL"
```

и `PublicationCountPoints: cbrM2CountPoints` в `util.ClickHouseImport{…}`. Запросы — дословно по спеке §3.5. Для `cbr_loans_to_individuals` имя константы `loansToIndCountPoints`, для `cbr_loans_to_corporations` — `loansToCorpCountPoints` (по именам существующих констант файлов).

`cbr_mp_survey` (`cbr/mp_survey.go`): добавить `const mpSurveyCountPoints = "SELECT count() FROM macro_series FINAL WHERE source = 'cbr_survey'"`, поля `PublicationSource: mpSurveyName`, `PublicationCountPoints: mpSurveyCountPoints`; **удалить `mpSurveyStat.Import`** и заменить его производные на `AfterImport` (каталог `mpSurveySeriesMeta` + `CreateView(ctx, conn, cbrSurveyView)`).

- [ ] **Step 4: Перевести `cbr_mo_br` на общий путь**

В `cbr/mo_br.go` удалить инлайн-префлайт (§`Import`, строки с `HeadFingerprint`/`PublicationKnown`/`moBrCountPoints`) и `util.EnsurePublications`; в локальной копии выставить:

```go
local.DataUrl = url
local.PublicationSource = moBrName
local.PublicationCountPoints = moBrCountPoints
local.AfterImport = func(ctx context.Context, conn driver.Conn) error {
    if e := util.UpsertSeriesCatalog(ctx, conn, meta); e != nil {
        return e
    }
    _, e := util.CreateView(ctx, conn, vCbrMoBrView)
    return e
}
```

`local.Import` теперь сам делает префлайт (no-op возвращается как обычный успех с проверенным числом точек) и пишет версию последней. Комментарий `Import` переписать под новый порядок; убрать ставшие ненужными импорты (`time`, `log` — проверить фактическое использование).

- [ ] **Step 5: Реестр и его проверка**

В `cbr/importers.go` добавить третью колонку `countPoints string` и заполнить литералами из спеки §3.5 (у остальных импортёров — `""`); обновить комментарий реестра (зачем литералы и что они сверяются с объявлением импортёра).

- [ ] **Step 6: Прогнать — PASS**

Run: `go test ./cbr/ -count=1 && go build ./... && gofmt -l cbr/`
Expected: зелёные, сборка проходит, gofmt пусто.

- [ ] **Step 7: Commit**

```bash
git add cbr/
git commit -m "cbr: publication preflight for ten importers, mo_br on the shared path"
```

---

### Task 4: Инвентарь импортёров остальных пакетов и перевод прошедших критерии

**Files:**
- Modify: файлы подтверждённых импортёров вне `cbr/` (если такие найдутся)
- Modify: спека `docs/superpowers/specs/2026-10-14-publication-preflight-design.md` (§3.7 — фактический список)

**Interfaces:**
- Consumes: поля шаблона из T1; при публикации производных — свой `AfterImport`.

- [ ] **Step 1: Инвентарь**

Пройти по нетестовым импортёрам пакетов `bea`, `bls`, `bold`, `craw`, `customs`, `dcf`, `eia`, `fao`, `fred`, `gold`, `moex`, `news`, `rosstat`-конвейер, `minfin` и прочим, где есть `DataUrl`/`CrawFunc`. Для каждого зафиксировать три критерия спеки §3.7: (1) один URL, пригодный для HEAD; (2) содержимое меняется реже расписания шага (`dagu/*.yaml`); (3) версионный сигнал в заголовках — **проверить живым HEAD** (`curl -sI`, как в спеке §2). Результат — таблица «импортёр → проходит/нет → причина отказа».

- [ ] **Step 2: Перевести прошедших**

Для каждого прошедшего — те же поля `PublicationCountPoints` (запрос `SELECT count() FROM <таблица> FINAL`, у владельца общей таблицы — с `WHERE source = …`) и `PublicationSource`, если `Name()` ≠ `TableName`. Производные (каталог/витрина), если импортёр их публикует, переезжают в `AfterImport`. Если производных импортёр не публикует и они публикуются вне его `Import`, оставить как есть — версия всё равно пишется после `AfterImport`.

- [ ] **Step 3: Дописать факт в спеку**

В §3.7 спеки — фактический список: переведённые имена и отказы с причинами. Если прошедших нет — записать это прямым текстом с причинами (пустой результат инвентаря — результат).

- [ ] **Step 4: Прогнать**

Run: `make all`
Expected: зелёный (`0 issues.`, `go vet` без замечаний, `go test -race ./...` — все пакеты `ok`, обе сборки).

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/      # плюс конкретные файлы переведённых импортёров, без deploy/staging/dagu.service
git commit -m "publication preflight: rollout beyond cbr (inventory and declarations)"
```

---

### Task 5: Документация того же PR

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`, `docs/ROADMAP_KEY_RATE.md`

**Interfaces:**
- Consumes: факты T1–T4.
- Produces: обновлённые §3/§6.1/§6.2/§8, таблицы README и новый пункт роадмапа.

- [ ] **Step 1: `ARCHITECTURE.md`**

§3 — поля `PublicationCountPoints`/`PublicationSource`/`AfterImport` и хелперы `util.PublicationPreflight`/`CountPoints` в описании шаблона и `util`; §6.1 — строки десяти импортёров (`cbr/` — и переведённых вне него из T4) с пометкой «точка контроля публикации»; §6.2 — комментарий `source` в DDL `source_publications` (обе копии, вместе с `util/publications.go`) и перечень потребителей; §8 п. 13 — конвенция без «миграция остальных импортёров — вне объёма», с фактическим списком; чейнджлог — запись 2026-10-14.

- [ ] **Step 2: `README.md`**

Таблица импортёров `cbr` и раздел про `cbr_mo_br`/`cbr_mp_survey`: ежедневный прогон месячных XLSX — no-op без скачивания; расписание не меняется.

- [ ] **Step 3: `docs/ROADMAP_KEY_RATE.md`**

Новый пункт (follow-up §8-механизма) со статусом ✅ и блоком живой приёмки **после** T6; до приёмки — статус «код готов, приёмка — шагом ниже».

- [ ] **Step 4: Сверить по скилу**

Прогнать `sync-readme-architecture` (скилл репозитория) и исправить всё, что он назовёт: новые поля шаблона, изменившийся состав импортёров, расписание, тестовые пакеты.

- [ ] **Step 5: Commit**

```bash
git add ARCHITECTURE.md README.md docs/
git commit -m "docs: publication preflight in architecture, readme and key-rate roadmap"
```

---

### Task 6: Живая приёмка на dev-ClickHouse

**Prerequisite:** ClickHouse поднят владельцем (`make ch-up` — делает пользователь, не ассистент).

- [ ] **Step 1: Полный прогон каждого переведённого импортёра**

Run: `make import STAT=<имя>` для `cbr_mp_survey`, `cbr_households_b_mes`, `cbr_m2`, `cbr_credit_m2x`, `cbr_loans_to_corporations`, `cbr_loans_to_individuals`, `cbr_bank_int_rate`, `cbr_indicators_cpd`, `cbr_infl_exp`, `cbr_mo_br`.

Expected: `Imported N rows` (как в приёмке фазы 3) и новая строка версии на каждый источник. Админ-проверка временным SELECT через `make ch-sql` (файл затем удалить, не коммитить):

```sql
SELECT source, name, fingerprint, points FROM source_publications FINAL ORDER BY source
```

- [ ] **Step 2: Повторный прогон — no-op**

Повторить те же десять команд. Expected: `no-op, версия <fingerprint> уже импортирована, N точек на месте`; в логе нет `Get xlsx file` и `собрано … точек`; новых строк версии нет; `SELECT count() FROM v_cbr_macro` не изменился.

- [ ] **Step 3: Контрольные числа**

`cbr_households_b_mes` — `points` версии **7350** (не 9030); `cbr_mo_br` — **9348** с отпечатком `4fef4f25716d388e9048149fa296039c`; остальные — числа приёмки фазы 3 (`docs/ROADMAP_KEY_RATE.md`): `cbr_mp_survey` 2084, `cbr_m2` 309, `cbr_bank_int_rate` 413, `cbr_infl_exp` 66, `cbr_indicators_cpd` 590, `cbr_credit_m2x` 6486, `cbr_loans_to_corporations` 2392, `cbr_loans_to_individuals` 8832 (у трёх последних `points` версии = числу точек ряда после `FINAL`; если оно отличается от числа ячеек — зафиксировать факт в отчёте).

- [ ] **Step 4: Проверки контура**

Run: `make mcp-check` → **ПРОВЕРКА ПРОЙДЕНА**; `dagu validate dagu/cbr.yaml` → exit 0.

- [ ] **Step 5: Дописать приёмку и закоммитить**

Блок приёмки с фактическими командами, числами и отпечатками — в `docs/ROADMAP_KEY_RATE.md` и в спеку (§4, факты).

```bash
git add docs/ ARCHITECTURE.md README.md
git commit -m "docs: live acceptance of publication preflight on dev clickhouse"
```
