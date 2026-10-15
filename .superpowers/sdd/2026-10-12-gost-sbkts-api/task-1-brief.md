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

