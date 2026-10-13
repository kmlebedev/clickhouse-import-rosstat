# gost_vehicle_safety_certificate (СБКТС Росстандарта) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Переписать `craw/gost.go` на REST API портала Росстандарта: импортёр `gost_vehicle_safety_certificate` с режимами `since`/`full`/`check`, расширенной схемой, витринами `v_gost_*` для MCP и расписанием раз в час.

**Architecture:** Единственный транспорт — `GET /standards/rest-api/libraries/search` с заголовком `gtn.access.appid` (из env), плоские **дневные срезы по дате публикации** (глубокие offset не нужны — портальная пагинация упирается в стену, измерено в спеке). Транспорт и разбор — чистые функции (`craw/gost_api.go`), импортёр — тонкая обвязка: DDL+миграция, обход дней (`since` — хвост от максимума таблицы, `full` — диапазон env, `check` — сверка без записи), один батч на день; витрины публикуются в начале пишущего прогона (образец `news/`).

**Tech Stack:** Go, `net/http` (+ `util.HttpClient`, `util.HttpUA`), `encoding/json`, `cenkalti/backoff/v5` (уже в go.mod), `net/http/httptest`, logrus, ClickHouse (`clickhouse-go/v2`, `PrepareBatch → Append → Send`), dagu (yaml).

**Spec:** `docs/superpowers/specs/2026-10-12-gost-sbkts-api-design.md`

## Global Constraints

- Новых внешних зависимостей нет; HTTP — через `util.HttpClient` (нац. сертификаты), UA — `util.HttpUA`; сетевых вызовов в `init()` нет.
- **Токен `gtn.access.appid` — только из env `GOST_APP_ID`**: в коде, комментариях, тестах и фикстурах значения токена нет (в legacy-версии файла он был вписан — при переписывании удаляется).
- Запись только батчами: `PrepareBatch → Append` (err проверяется) `→ Send`; ровно один батч на день; ошибки не проглатываются.
- DDL: `CREATE TABLE IF NOT EXISTS` + идемпотентные `ALTER TABLE … ADD COLUMN IF NOT EXISTS` (таблица в dev-БД уже существует и не пересоздаётся); витрины — `CREATE OR REPLACE VIEW … DEFINER = default SQL SECURITY DEFINER`, комментарии — `ALTER TABLE … MODIFY COMMENT` / `COMMENT COLUMN`.
- Канонический DDL таблицы — копия ARCHITECTURE.md §6.2 (обновляется тем же PR); `ORDER BY (mark, type, date, certificate_number)` не меняется.
- Env: `GOST_MODE` (`""`/`since`/`full`/`check`; неизвестное — ошибка до сети и БД), `GOST_FROM`/`GOST_TO` (`YYYY-MM-DD`), `GOST_SETTLE_DAYS` (int ≥ 0, дефолт 3), `GOST_APP_ID` (обязателен во всех режимах).
- Даты: `date` — дата выдачи СБКТС по МСК (epoch мс ÷ фиксированный офсет +3 ч, без перехода на летнее время); `publication_date` — день среза.
- `Name()` и имя таблицы — `gost_vehicle_safety_certificate` (контракт `CLICKHOUSE_IMPORT_STAT` и dagu).
- Тесты офлайн: сеть только `httptest`, фикстуры — `craw/testdata/*.json`; `go test ./...` без сети и БД.
- Логи — logrus по-русски; коммиты — по-английски, в духе истории.
- Расписание: `dagu/craw.yaml`, `schedule: "17 * * * *"`, `overlap_policy: skip`, retry 3 × 300, `GOST_MODE=since`.

## Review Focus

1. **`dateofissueofcertificate` бывает `null`** — документ без даты выдачи не пишется «нулевой датой», а отбрасывается со счётчиком (Task 1, `TestGostRowsFromDocsFilters`).
2. **Текстовый `query` может вернуть чужой день публикации** — клиентский фильтр по точной дате обязателен, иначе в срез дня уезжают посторонние строки (Task 1, `TestGostRowsFromDocsFilters`, `TestGostDayClientFilterCountsSkipped`).
3. **Оборванный день (`count` больше собранного)** — ошибка дня и никакой частичной записи: «половина дня» хуже пропуска, её не видно (Task 1, `TestGostDayCountMismatchFails`; Task 2 — день не пишется вообще).
4. **Легаси-строки `publication_date = 1970-01-01`** не считаются дырами в `check` (импортёр о них только предупреждает) (Task 2, `TestGostCheckDaysLegacyWarnOnly`).
5. **Пустая таблица в `since`** — не обход десяти лет: окно последних 7 дней + предупреждение «запустите full» (Task 2, `TestGostSinceDaysAnchors`).

---

### Task 1: `craw/gost_api.go` — транспорт и дневной срез

**Files:**
- Create: `craw/gost_api.go`, `craw/gost_api_test.go`, `craw/testdata/gost_day.json`

**Interfaces:**
- Consumes: `util.HttpClient` (`*http.Client`), `util.HttpUA` (константа UA), `backoff/v5`.
- Produces (нужно Task 2–3):
  - `var gostAPIURL = "https://www.gost.ru/standards/rest-api/libraries/search"` — пакетная переменная (подмена `httptest` в тестах).
  - `const gostPageSize = 900`, `const gostMaxDayCount = 10000`, `var gostAPITimeout = 120 * time.Second`, `var gostRequestGap = 200 * time.Millisecond`, `var gostBackoffPolicy = backoff.ExponentialBackOff{InitialInterval: 5 * time.Second, RandomizationFactor: 0.5, Multiplier: 2, MaxInterval: 2 * time.Minute}`.
  - `type gostAPI struct { Client *http.Client; AppID string }`; `func newGostAPI(client *http.Client, appID string) *gostAPI`.
  - `type gostDoc struct { ID int64; Fields gostFields }`; `type gostFields struct { Product, Type, NumberOfCertificate string; DateOfIssueOfCertificate *int64; EcoClass, EngineTrademark, YearOfManufacturing, NameOfManufacturer, UniqueIdentifier, DateOfPublication string }` — json-теги как у источника.
  - `type gostRow struct { Mark, Type string; Date time.Time; CertificateNumber, EcoClass, EngineTrademark string; YearOfManufacturing uint16; NameOfManufacturer, UID string; PublicationDate time.Time }`.
  - `type gostDaySlice struct { Rows []gostRow; Count, Skipped int }`.
  - `func (a *gostAPI) fetchPage(ctx context.Context, day time.Time, page, size int) (gostSearchResponse, error)`; `type gostSearchResponse struct { Count int; Data []gostDoc }`.
  - `func (a *gostAPI) day(ctx context.Context, day time.Time) (gostDaySlice, error)` — все страницы дня (`size = gostPageSize`), клиентский фильтр, ошибка на обрыв.
  - `func (a *gostAPI) dayCount(ctx context.Context, day time.Time) (int, error)` — только `count`, одним запросом `size = 1` (payload ~1 документ; в check это запрос на день, а полный диапазон чек-прогона не должен качать 800 КБ × 4000 дней).
  - Чистые: `func gostMSKDate(ms int64) time.Time`, `func gostParsePublicationDay(s string) (time.Time, bool)`, `func gostRowFromDoc(day time.Time, d gostDoc) (gostRow, bool)`, `func gostRowsFromDocs(day time.Time, docs []gostDoc) (rows []gostRow, skipped int)`, `func gostYear(raw string) uint16`.

- [ ] **Step 1: Написать падающие тесты** — `craw/gost_api_test.go` (пакет `craw`, офлайн):

```go
func TestGostMSKDate(t *testing.T)                  // 1791493200000 → 2026-10-09 (21:00 UTC = 00:00 МСК); 0 → 1970-01-01
func TestGostParsePublicationDay(t *testing.T)      // "11.10.2026 00:00:00 ZE3" → 2026-10-11, true; "мусор"/"" → false
func TestGostYear(t *testing.T)                     // "2016" → 2016; "16"/"н/д"/"" → 0
func TestGostRowsFromDocsFilters(t *testing.T)      // 4 док.: (a) свой день — row; (b) dateofpublication другого дня — skipped;
                                                    // (c) dateofissueofcertificate nil — skipped; (d) пустой numberofcertificate — skipped
func TestGostDayFetchesAllPages(t *testing.T)       // httptest: count=1000, page=1 → 900 док., page=2 → 100 док.; день прочитан целиком (Count 1000, Rows 1000)
func TestGostDayClientFilterCountsSkipped(t *testing.T) // день с 2 док. чужой даты публикации → skipped=2, Rows=0
func TestGostDayCountMismatchFails(t *testing.T)    // count=1000, page=2 пустая → ошибка «получено 900 документов из 1000»
func TestGostDayTooManyDocsFails(t *testing.T)      // count=20000 → ошибка про окно поиска (страницы 2+ не запрашиваются)
func TestGostDayPermanentStatusFails(t *testing.T)  // 401/403 → ошибка сразу, без повторов (backoff.Permanent), текст подсказывает про GOST_APP_ID
func TestGostDayRetriesServerError(t *testing.T)    // 500, затем 200 → строки получены (gostBackoffPolicy.InitialInterval = time.Millisecond в тесте)
func TestGostDayEmptyBodyFails(t *testing.T)        // 200 + пустое тело → ошибка
func TestGostDayRequestShape(t *testing.T)          // запрос day(): путь "/standards/rest-api/libraries/search", query-параметры query=DD.MM.YYYY, page, size=900, tmsp;
                                                    // заголовки gtn.access.appid и Accept; запрос dayCount(): size=1
```

- [ ] **Step 2: Прогнать — падает**

Run: `go test ./craw/ -run TestGost -v`
Expected: FAIL — `undefined: gostMSKDate` (файл ещё не создан).

- [ ] **Step 3: Снять фикстуру** `craw/testdata/gost_day.json`

Ответ живого API от 2026-10-12, обрезанный до трёх документов (реальные значения; **без токена**): `{"count":3,"data":[…]}` — документ дня публикации 11.10.2026 (`dateofissueofcertificate` = 1791493200000, Toyota/ZGE25/ТС RU А-JP.ОМ80.51298), документ другой даты публикации и документ с `dateofissueofcertificate: null`.

- [ ] **Step 4: Реализовать `craw/gost_api.go`**

Ключевые решения (тело пишется по ним):

- `fetchPage`: URL — `fmt.Sprintf("%s?query=%s&page=%d&size=%d&tmsp=%d", gostAPIURL, url.QueryEscape(day.Format("02.01.2006")), page, size, time.Now().UnixMilli())`; запрос `http.NewRequestWithContext` с пер-запросным `context.WithTimeout(ctx, gostAPITimeout)`; заголовки `gtn.access.appid: <a.AppID>`, `Accept: application/json`, `User-Agent: util.HttpUA`; **URL в тексты ошибок не попадает** (в query у вызывающих бывают ключи — конвенция `util.GetJSON`).
- Ретраи — `backoff.Retry(ctx, op, backoff.WithBackOff(&b), backoff.WithMaxTries(4))`, где `b := gostBackoffPolicy` (копия пакетной переменной: `ExponentialBackOff` не потокобезопасен, а `Retry` сам вызывает `Reset()`). Классификация: таймаут/`context.DeadlineExceeded` → `backoff.RetryAfter(30)`; 429/5xx → `backoff.RetryAfter(60)`; 401/403 → `backoff.Permanent(fmt.Errorf("http status %d — проверьте GOST_APP_ID (токен gtn.access.appid из JS-конфига страницы реестра, standarts.adminsHeaders)", code))`; прочие 4xx → `backoff.Permanent`; пустое тело/битый JSON → retryable.
- `day`: страница 1 → `count`; `count > gostMaxDayCount` → ошибка сразу; страницы `2..ceil(count/gostPageSize)` с `time.Sleep(gostRequestGap)` между запросами; `len(docs) < count` → ошибка «получено N документов из M» (никакой частичной записи — решение принимается до батча).
- `gostRowFromDoc` возвращает `false`, если нет `dateofissueofcertificate`, `numberofcertificate` пуст или день публикации не равен срезу (`gostParsePublicationDay`); `gostMSKDate` — `time.UnixMilli(ms).UTC().Add(3 * time.Hour)` с обнулением времени суток (дата в UTC-полуночи — конвенция сравнения `time.Time`).

- [ ] **Step 5: Прогнать — проходит + саботаж**

Run: `go test ./craw/ -run TestGost -v`
Expected: PASS (12/12). Саботаж: убрать клиентский фильтр по `dateofpublication` → падает `TestGostRowsFromDocsFilters`; вернуть `len(docs)` без проверки на `count` → падает `TestGostDayCountMismatchFails`.

- [ ] **Step 6: Commit**

```bash
gofmt -l craw/gost_api.go craw/gost_api_test.go
git add craw/gost_api.go craw/gost_api_test.go craw/testdata/gost_day.json
git commit -m "craw: add gost api transport with day slices"
```

---

### Task 2: `craw/gost.go` — импортёр: схема, конфиг, режимы `since`/`full`/`check`, запись

**Files:**
- Create: `craw/gost.go` (переписывается целиком — старый colly-код со встроенным токеном и построчными INSERT удаляется), `craw/gost_test.go`

**Interfaces:**
- Consumes: всё из Task 1.
- Produces (нужно Task 3–5):
  - `type gostImporter struct{}` + `func (s *gostImporter) Name() string` + `func (s *gostImporter) Import(ctx context.Context, conn driver.Conn) (int64, error)`; `init()` регистрирует импортёр.
  - `type gostConfig struct { Mode string; From, To time.Time; SettleDays int; AppID string }`; `func gostConfigFromEnv() (gostConfig, error)`.
  - `func gostSchemaStatements() []string` — CREATE + 6 × `ADD COLUMN IF NOT EXISTS`.
  - `func gostDaysBetween(from, to time.Time) ([]time.Time, error)` (включительно; больше `gostMaxRangeDays = 4000` — ошибка).
  - `func gostSinceDays(ctx context.Context, conn driver.Conn, settle int, now time.Time) (days []time.Time, tableEmpty bool, err error)`.
  - `func gostWriteDay(ctx context.Context, conn driver.Conn, rows []gostRow) (int64, error)`; `func gostRunDays(ctx context.Context, conn driver.Conn, api *gostAPI, days []time.Time) (int64, error)`.
  - `func gostCheckDays(ctx context.Context, conn driver.Conn, api *gostAPI, days []time.Time) (checked int64, err error)`.
  - `func gostLegacyRows(ctx context.Context, conn driver.Conn) (uint64, error)`.
  - `var gostClock = time.Now` (подмена «сейчас» в тестах).

**Важно:** `Import` в этой задаче витрины НЕ публикует — вызов `gostPublishViews` добавляет Task 3 (иначе функция ещё не существует и пакет не собирается).

- [ ] **Step 1: Написать падающие тесты** — `craw/gost_test.go` (пакет `craw`, офлайн: `httptest` + фейковое соединение):

```go
func TestGostConfigDefaults(t *testing.T)             // пустые env → Mode since, SettleDays 3, AppID из env
func TestGostConfigRejectsUnknownMode(t *testing.T)   // GOST_MODE=fullscan → ошибка «неизвестный режим (since|full|check)»
func TestGostConfigRejectsBadDate(t *testing.T)       // GOST_FROM=01.01.2016 → ошибка парсинга (до сети)
func TestGostConfigRejectsBadSettle(t *testing.T)     // GOST_SETTLE_DAYS=-1 / "три" → ошибка
func TestGostConfigRequiresAppID(t *testing.T)        // пустой GOST_APP_ID → ошибка с подсказкой про standarts.adminsHeaders
func TestGostConfigRejectsEmptyRange(t *testing.T)    // full: FROM > TO → ошибка
func TestGostSchemaStatementsShape(t *testing.T)      // [0] CREATE с ORDER BY (mark, type, date, certificate_number) и 10 колонками;
                                                      // [1..6] ADD COLUMN IF NOT EXISTS по ecoclass, enginetrademark, yearofmanufacturing,
                                                      // nameofmanufacturer, uid, publication_date
func TestGostDaysBetween(t *testing.T)                // 2026-06-01..2026-06-03 → 3 дня; пустой диапазон — ошибка; > 4000 дней — ошибка
func TestGostSinceDaysAnchors(t *testing.T)           // фейковый conn: (a) max(date)=2026-10-09, settle 3 → окно [2026-10-06..2026-10-11];
                                                      // (b) только publication_date; (c) оба NULL → tableEmpty=true, окно последних 7 дней
func TestGostWriteDayOneBatch(t *testing.T)           // 2 строки → один PrepareBatch, 2 Append в порядке колонок DDL, один Send
func TestGostRunDaysSkipsEmptyAndAggregatesErrors(t *testing.T) // день с count=0 — без батча; день с 500 — в ошибку, следующий день пишется; сумма строк
func TestGostCheckDaysMatchNoError(t *testing.T)      // таблица 5, API 5 → checked=1, ошибки нет
func TestGostCheckDaysReportsGap(t *testing.T)        // таблица 3, API 5, фильтр дня даёт 5 → ошибка содержит день и «3 против 5»
func TestGostCheckDaysTextNoiseNotGap(t *testing.T)   // API count=7 (текстовый шум), фильтр дня даёт 5, таблица 5 → не ошибка, отчёт «шум: 7 → 5»
func TestGostCheckDaysSkippedDayFails(t *testing.T)   // API count недоступен (500) → день в ошибке, остальные сверены
func TestGostCheckDaysLegacyWarnOnly(t *testing.T)    // легаси-строки (1970-01-01): предупреждение, дыры нет
func TestGostImportCheckDoesNotWrite(t *testing.T)    // Import(GOST_MODE=check): нет PrepareBatch и нет CREATE VIEW; Exec — только DDL таблицы
func TestGostImportSinceEmptyTableWarns(t *testing.T) // пустая таблица: окно 7 дней, warn «история не загружена», батчи пишутся
func TestGostImportUnknownModeFailsBeforeDB(t *testing.T) // GOST_MODE=bogus → у фейкового conn пустой список вызовов
```

Фейковое соединение — по образцу `newsFakeConn`/`newsFakeBatch` (`news/news_test.go:805-938`): `struct { driver.Conn; mu sync.Mutex; calls []string; maxDate, maxPub *time.Time; legacy uint64; batch *gostFakeBatch }`, `Exec`/`Query`/`PrepareBatch` записывают вызовы; строки `Query` — по виду запроса (`maxOrNull(date)` vs `publication_date` vs `count() … 1970-01-01`). Батч проверяется `gostFakeBatch.inserted(t) []gostRow` (порядок аргументов = порядок колонок DDL).

- [ ] **Step 2: Прогнать — падает** (`undefined: gostConfigFromEnv`)

- [ ] **Step 3: Реализовать `craw/gost.go`**

- `gostConfigFromEnv`: `GOST_MODE` пусто → `since`; значения — точные (`since`/`full`/`check`), иначе ошибка с перечнем; `GOST_APP_ID` — `strings.TrimSpace`, пусто → ошибка с подсказкой; `GOST_SETTLE_DAYS` — `strconv.Atoi`, `< 0` или не число → ошибка; `GOST_FROM`/`GOST_TO` — `time.Parse("2006-01-02")`, если заданы; дефолты диапазона (только `full`/`check`): FROM = 2016-01-01, TO = вчера МСК (`gostMSKDate(gostClock().UnixMilli()).AddDate(0, 0, -1)`).
- `gostSchemaStatements`: CREATE (копия канонического DDL спеки: mark, type, date, certificate_number, ecoclass, enginetrademark, yearofmanufacturing, nameofmanufacturer, uid, publication_date) + ALTER-ы `ADD COLUMN IF NOT EXISTS … AFTER …` в порядке колонок; комментарий: «копия §6.2 — копии обязаны совпадать; ALTER-ы идемпотентны, таблица в dev-БД уже существует».
- `gostSinceDays`: `SELECT maxOrNull(date), maxIfOrNull(publication_date, publication_date > '2016-01-01') FROM gost_vehicle_safety_certificate`; оба NULL → `tableEmpty=true`, окно `[вчера-6 … вчера]`; иначе `anchor = max(issue, pub)`, `from = anchor − settle`, `to = вчера` (anchor из будущего — прижать к `to`).
- `gostWriteDay`: батч `INSERT INTO gost_vehicle_safety_certificate`; порядок `Append`: mark, type, date, certificate_number, ecoclass, enginetrademark, yearofmanufacturing, nameofmanufacturer, uid, publication_date; `Abort` только в ветке ошибки `Append`; счётчик — `batch.Rows()`.
- `gostRunDays`: у каждого дня — `api.day`; ошибка дня → в список `failed` (проход продолжается — резюмируемость); `Count == 0` → `log.Infof` без батча; `Skipped > 0` → `log.Warnf`; после дня `time.Sleep(gostRequestGap)`; агрегат ошибок — `fmt.Errorf("не прочитаны дни (%d): %s", …)`.
- `gostCheckDays`: один Query счётчиков таблицы (`SELECT publication_date, uniqExact(certificate_number) FROM gost_vehicle_safety_certificate FINAL WHERE publication_date BETWEEN ? AND ? GROUP BY publication_date`) в map; цикл по дням — `api.dayCount`; `count > gostMaxDayCount` → ошибка дня; `count != table[day]` → `api.day(ctx, day)` и сравнение `len(Rows)` (различает «день не дочитан» и «текстовый шум»); строки отчёта и агрегат ошибок; сводка `проверено N дней, дыр M`.
- `Import`: конфиг → DDL → предупреждение о легаси-строках (`gostLegacyRows`; ошибка запроса фатальна) → `case "check"` — `gostCheckDays` → иначе день-лист (`since`/`full`) и `gostRunDays`; `log.Infof("Imported %d rows …")` делает main.

- [ ] **Step 4: Прогнать — проходит + саботаж**

Run: `go test ./craw/ -v`
Expected: PASS. Саботаж: убрать `AddDate(0,0,-1)` из дефолта TO → падает `TestGostConfigDefaults`; вернуть `conn.Exec` в цикле вместо батча → падает `TestGostWriteDayOneBatch`.

- [ ] **Step 5: Commit** — `git add craw/gost.go craw/gost_test.go && git commit -m "craw: rewrite gost importer on api day slices"`

---

### Task 3: Витрины `v_gost_*` — стейтменты, гранты, mcp-check

**Files:**
- Create: `craw/gost_views.go`, `craw/gost_views_test.go`
- Modify: `craw/gost.go` (вызов `gostPublishViews` в пишущих режимах до обхода дней), `sql/mcp_kimi_reader.sql` (2 GRANT), `scripts/mcp_check.py` (блок проверок)

**Interfaces:**
- Consumes: таблица `gost_vehicle_safety_certificate` (Task 2); образец стейтментов — `news/schema.go`.
- Produces:
  - `var gostViewStatements []string` — 14 стейтментов: 2 × `CREATE OR REPLACE VIEW … DEFINER = default SQL SECURITY DEFINER`, комментарий таблицы + 9 комментариев колонок у `v_gost_vehicle_safety_certificate`, комментарий таблицы + 3 комментария колонок у `v_gost_vehicle_safety_daily`.
  - `func gostPublishViews(ctx context.Context, conn driver.Conn) error` — исполняет стейтменты по порядку (образец — `news/news.go`).

- [ ] **Step 1: Написать падающие тесты** — `craw/gost_views_test.go`:

```go
func TestGostViewStatementsShape(t *testing.T)  // len == 14; [0] содержит "CREATE OR REPLACE VIEW v_gost_vehicle_safety_certificate",
                                                // "DEFINER = default SQL SECURITY DEFINER", "FROM gost_vehicle_safety_certificate FINAL",
                                                // без "uid" и без "ORDER BY"; дневная: "uniqExact(certificate_number)", "GROUP BY date, mark",
                                                // "ORDER BY date DESC, mark"; один ALTER MODIFY COMMENT и по COMMENT COLUMN на каждую колонку обеих витрин
func TestGostViewsDocumentSemantics(t *testing.T) // в текстах комментариев есть: «дата выдачи СБКТС (МСК)», «дата публикации среза»,
                                                  // «реестр СБКТС Росстандарта», «REST API», «изменения сертификата дают вторую строку»,
                                                  // «год выпуска 0 — не указан/до бэкфилла»
func TestGostImportWritesPublishViews(t *testing.T) // Import(GOST_MODE=since, пустая таблица) → в calls есть 14 стейтментов витрин ДО первого PrepareBatch
```

- [ ] **Step 2: Прогнать — падает** (`undefined: gostViewStatements`)

- [ ] **Step 3: Реализовать `craw/gost_views.go`** + дописать гранты и проверки

- Витрина строк: `SELECT date, publication_date, mark, type, certificate_number, ecoclass, enginetrademark, yearofmanufacturing, nameofmanufacturer FROM gost_vehicle_safety_certificate FINAL` — **без ORDER BY** (3.4 млн строк: сортировка на каждом запросе не нужна, комментарий велит фильтровать по `date`; `uid` в витрину не выводится).
- Дневная: `SELECT date, mark, uniqExact(certificate_number) AS certificates FROM … FINAL GROUP BY date, mark ORDER BY date DESC, mark`.
- Комментарии: таблицы и колонок, из спеки §«Витрины» (включая оговорки: amendments дают вторую строку; `certificates` — `uniqExact`, а не число строк; легаси-строки).
- `sql/mcp_kimi_reader.sql`: в конец блока `GRANT SELECT ON default.v_gost_vehicle_safety_certificate TO kimi_reader;` и `… ON default.v_gost_vehicle_safety_daily …` (одна строка на грант, как соседние).
- `scripts/mcp_check.py`: блок по образцу bold-блока — существование и читаемость обеих витрин подтверждаются запросом `SELECT count() FROM v_gost_*` (не поиском имени в `list_tables`), `SELECT count() FROM gost_vehicle_safety_certificate` → ожидание `ACCESS_DENIED` (497).

- [ ] **Step 4: Прогнать — проходит + саботаж** (убрать `uniqExact` из дневной → падает `TestGostViewStatementsShape`)

- [ ] **Step 5: Commit** — `git add craw/gost_views.go craw/gost_views_test.go craw/gost.go sql/mcp_kimi_reader.sql scripts/mcp_check.py && git commit -m "craw: publish gost views and mcp checks"`

---

### Task 4: Расписание, README, ARCHITECTURE, роадмапы

**Files:**
- Modify: `dagu/craw.yaml`, `README.md`, `ARCHITECTURE.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`, `docs/ROADMAP_DCF_POLYUS.md`

**Interfaces:**
- Consumes: env-контракт Task 2 (`GOST_MODE`/`GOST_FROM`/`GOST_TO`/`GOST_SETTLE_DAYS`/`GOST_APP_ID`), витрины Task 3.
- Produces: документация и расписание (контракт для эксплуатации).

- [ ] **Step 1: `dagu/craw.yaml`** — один шаг, `schedule: "17 * * * *"`, `overlap_policy: skip`, `run: "$ROSSTAT_IMPORT_BIN"`, `env: CLICKHOUSE_IMPORT_STAT=gost_vehicle_safety_certificate` и `GOST_MODE=since`, retry 3 × 300; комментарий-обоснование: стена портальной пагинации (почему API-транспорт), лаг публикации ~2 суток (почему хвост 3 дня), щадящая нагрузка 4–12 запросов в час, `full`/`check` — ручные (без расписания).

- [ ] **Step 2: README** — строка `craw` в таблице импортёров (источник — реестр СБКТС, три режима, витрины, расписание «раз в час», ручной `full` для истории); абзац-runbook с командами:

```bash
make import STAT=gost_vehicle_safety_certificate                        # since: хвост (штатный, этим ходит dagu)
GOST_MODE=full GOST_FROM=2016-01-01 GOST_TO=2026-10-11 make import STAT=gost_vehicle_safety_certificate
GOST_MODE=check GOST_FROM=2026-06-01 GOST_TO=2026-06-30 make import STAT=gost_vehicle_safety_certificate
```

и оговорками: полный прогон — часы, чанками по годам, резюмируемый; `GOST_APP_ID` — в `~/.config/rosstat/env` (значение из JS-конфига страницы реестра; в репозитории не хранится).

- [ ] **Step 3: ARCHITECTURE.md** — §2 (строка `craw/` — новый транспорт и режимы), §3 (заменить «Эталон краулера с двумя DDL — `craw/gost.go`» на актуальный пример `CrawFunc` — `customs/vneshn_torg_countries.go`), §5 (env-таблица + 5 строк `GOST_*`), §6.1 (строка/абзац про импортёр), §6.2 (канонический DDL — новая схема + примечание об идемпотентных ALTER-ах для существующей таблицы), §6.3 (две витрины с описанием и оговорками), §6.5 (гранты/проверки), §7 (строка календаря «раз в час»); в шапку — запись «Последнее обновление: 2026-10-12 — …».

- [ ] **Step 4: Роадмапы** — новый пункт в `docs/GOLD_THESIS_DATA_ROADMAP.md` после блока «Фаза 4 — follow-up 5»: «Фаза 4 — follow-up 6: СБКТС Росстандарта — прокси импорта авто (торговый баланс → курс рубля)», с пробелом, методом (API-срезы, режимы, витрины), ссылками на спеку/план; канал №7 в `docs/ROADMAP_DCF_POLYUS.md` §10.1 (таблица каналов) со ссылкой на пункт. Статусы ✅ — в Task 5 после живой приёмки.

- [ ] **Step 5: Проверка документов** — `grep -n "gost" README.md ARCHITECTURE.md dagu/craw.yaml` (нет упоминаний старого месячного расписания и colly-обхода), `python3 -c "import yaml,sys; yaml.safe_load(open('dagu/craw.yaml'))"`.

- [ ] **Step 6: Commit** — `git add dagu/craw.yaml README.md ARCHITECTURE.md docs/GOLD_THESIS_DATA_ROADMAP.md docs/ROADMAP_DCF_POLYUS.md && git commit -m "docs: gost hourly dags and api importer runbook"`

---

### Task 5: Живая приёмка и отметки в роадмапе

**Files:**
- Modify: `docs/GOLD_THESIS_DATA_ROADMAP.md` (статус ✅ и числа приёмки)

- [ ] **Step 1: `make all`** — gofmt, golangci-lint (0 issues), `go vet`, `go test -race`, обе сборки.

- [ ] **Step 2: Живой `since`**: `make import STAT=gost_vehicle_safety_certificate GOST_MODE=since` (нужен `GOST_APP_ID` в `~/.config/rosstat/env` — добавить вручную, значение не печатать); повтор команды — идемпотентно (в логе те же дни).

- [ ] **Step 3: Живой `full` за месяц**: `GOST_MODE=full GOST_FROM=2026-06-01 GOST_TO=2026-06-30 make import STAT=gost_vehicle_safety_certificate`; затем `GOST_MODE=check` по тому же окну — дыр нет.

- [ ] **Step 4: Сверка с порталом (2–3 окна)**: для дня выдачи D собрать набор номеров со страницы реестра (портальная пагинация `navigationalstate` c `pageSize=900`, пока дата не сменится — метод разведки из спеки) и сравнить с `SELECT certificate_number FROM gost_vehicle_safety_certificate FINAL WHERE date = D` — ожидание: портальное множество ⊆ таблицы и обратные расхождения только у соседних дней публикации; записать числа.

- [ ] **Step 5: MCP**: `make mcp-user` → гранты OK (пароль не печатается); `make mcp-check` → ПРОВЕРКА ПРОЙДЕНА (обе витрины читаются, сырая таблица 497); пример запроса «импорт авто по дням за последний месяц» через витрину.

- [ ] **Step 6: Отметки в роадмапе** — статус ✅ у follow-up 6 с командами и числами приёмки (пункт обновляется тем же PR).

- [ ] **Step 7: Полный прогон (по решению владельца)** — `GOST_MODE=full` чанками по годам (часы); после — `check` по нескольким диапазонам; результат дописать в роадмап. Минимальный объём приёмки (шаги 1–6) не блокируется полным прогоном.

- [ ] **Step 8: Commit** — `git add docs/GOLD_THESIS_DATA_ROADMAP.md && git commit -m "docs: gost importer acceptance"`
