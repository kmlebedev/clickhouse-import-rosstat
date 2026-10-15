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

