### Task 2: импортёр `bold_etf_flows`

**Files:**
- Create: `bold/bold.go` (константы таблиц/рядов, DDL, два импортёра, регистрация), `bold/series_meta.go` (каталог 7 рядов), `bold/bold_test.go`

**Interfaces:**
- Consumes: всё из Task 1; `util.GetJSON(url string, out any) error`; `util.UpsertSeriesCatalog(ctx, conn, []util.SeriesMeta) error`; `driver.Conn`.
- Produces (нужно Task 3–5):
  - `const boldSource = "bold"`, `const boldFundTable = "bold_fund_snapshot"`, DDL `boldFundDdl` с ключом `ORDER BY (date, ticker, name)` (тикер не уникален — `GLD` трижды, спека §4.3).
  - `func (s *boldEtfFlows) Name() string` → `"bold_etf_flows"`; поле `apiBase string` (пусто → `boldBaseURL`) и `now func() time.Time` (nil → `time.Now`) — швы для тестов, как `apiBase` в `wgc`.
  - `func boldStoredMaxDate(ctx context.Context, conn driver.Conn) (time.Time, error)` — `SELECT maxOrNull(date) FROM macro_series FINAL WHERE source = 'bold' AND series LIKE 'etf\_bold\_%'`; `NULL` → нулевое время.
  - `func boldStoredUnits(ctx context.Context, conn driver.Conn, date time.Time) (float64, error)` — `SELECT maxOrNull(value) FROM macro_series FINAL WHERE source='bold' AND series='etf_bold_holdings_tonnes' AND date='<date>'`; нет точки → ошибка «нет запасов за <date>» (нужны для Δ).
  - `func boldNeedsFullFetch(latest time.Time, storedMax time.Time) bool` — `true`, если `storedMax.IsZero()` или `latest.Sub(storedMax).Hours() > 96` (4 календарных дня).
  - `func boldEtfFetchLatest(baseURL string) (boldRow, []boldFundRow, error)` — три запроса `units-latest`, `aum-latest`, `flows/all-latest` + `boldPathSummary`, слияние ответов в одну строку (`units`+`price` из units, `aum` из aum, `culm*` из flows; все три даты и цена обязаны совпасть — расхождение ошибка), `funds` — из среза.
  - `func boldEtfFullFetch(baseURL string) ([]boldRow, []boldFundRow, error)` — `boldEtfHistory(baseURL)` (три файла истории, слияние по дате; строка без даты в одном из файлов — ошибка) + `boldPathSummary` для `funds`; порядок строк — по дате.
  - Регистрация: `init()` — `chimport.Stats = append(chimport.Stats, &boldEtfFlows{})`.

- [ ] **Step 1: Написать падающие тесты**

`bold/bold_test.go` (пакет `bold`; фейковое соединение — по образцу `wgc/etf_test.go:234-275`):

```go
func TestBoldEtfFlowsName(t *testing.T)                    // Name() == "bold_etf_flows"
func TestBoldNeedsFullFetch(t *testing.T)                  // пусто → true; +1 день → false; +3 дня (Fri→Mon) → false; +5 дней → true
func TestBoldEtfFetchLatestMismatchedDatesFails(t *testing.T) // units-latest за 10-09, aum-latest за 10-08 → ошибка
func TestBoldEtfFullFetchUsesHistoryPaths(t *testing.T)    // httptest пинит пути: полный путь — «/gold/funds/units», «/gold/funds/aum», «/gold/flows/all» + «/gold/summary», без «-latest»
func TestBoldEtfFetchCountsRequests(t *testing.T)          // инкремент — ровно 4 запроса, полный — 4 (счётчик на сервере)
func TestBoldEtfHistoryMergesByDate(t *testing.T)          // фикстуры истории → 2 строки (08.10, 09.10), 2015-01-01 без units/aum отфильтрована в точках
func TestBoldEtfImportFullWritesAllSeries(t *testing.T)    // фейковое соединение пустое + полный путь → в батч macro_series идут 7 наборов рядов, в bold_fund_snapshot — 73 строки
func TestBoldEtfImportIncrementalWritesOneDay(t *testing.T) // conn возвращает максимум 08.10 и units 113044763 → пишутся точки за 09.10 (поток +4.233 т)
func TestBoldEtfImportNoopWhenUpToDate(t *testing.T)       // максимум == дата latest → 0 строк, ошибки нет, батч не открывается
func TestBoldEtfImportMissingUnitsAtMaxFails(t *testing.T)  // сохранённые units отсутствуют → ошибка, а не поток = 0
func TestBoldEtfImportEmptyResponseFails(t *testing.T)      // пустой JSON → ошибка
func TestBoldEtfCatalogDescribesEverySeries(t *testing.T)   // 7 записей, все поля заполнены, имена == списку, в описаниях есть «bold.report», «83 %», «не складывать», «календарные»
```

- [ ] **Step 2: Реализовать `bold/bold.go` + `bold/series_meta.go`**

- `boldEtfFlows.Import(ctx, conn)` — порядок:
  1. `stored, err := boldStoredMaxDate(ctx, conn)`; ошибка чтения → `log.Warnf` и `stored = time.Time{}` (как в `wgc`);
  2. `latest, funds, err := boldEtfFetchLatest(base)` (4 запроса); `latestDate` — дата строки; если таблица не пуста и `latestDate` не позже `stored` → `log.Infof("... нечего обновлять")`, `return 0, nil` (батч не открывается вовсе);
  3. при `boldNeedsFullFetch(latestDate, stored)` — `rows, funds, err := boldEtfFullFetch(base)` (4 запроса) и `points, err := boldETFSets(rows, nil, time.Time{})`; иначе — `prevUnits, err := boldStoredUnits(ctx, conn, stored)` (ошибка → ошибка импорта: без предыдущих унций поток не посчитать) и `points, err := boldETFSets([]boldRow{latest}, &prevUnits, stored)`;
  3a. `boldEtfFullFetch` при слиянии трёх файлов обязан отвергать «неполный день» (день есть с `units`, но без `aum`, или наоборот) и дубликат даты между файлами — ошибка с датой (Ruling 5); тест `TestBoldEtfFullFetchRejectsPartialDay`;
  4. `fundPoints, err := boldFundPoints(funds, latestDate)` — ошибка останавливает импорт до батчей (её причина — смена формы ответа `summary.json`);
  5. `conn.Exec(ctx, boldFundDdl)` и `conn.Exec(ctx, macroSeriesDdl)` — обе DDL как копии канона (спека §4.3 и §4.1; для `macro_series` копия с комментарием, как в `wgc/etf.go`), выполнить до батчей;
  6. два батча — `macro_series` (`source='bold'`, точки 7 рядов) и `bold_fund_snapshot` (73 строки): `PrepareBatch → Append (err) → Send`, каждый со своей проверкой;
  7. `util.UpsertSeriesCatalog(ctx, conn, boldETFSeriesMeta())` — только после успешных `Send`; `log.Infof("Imported %d rows of %s (bold ETF: %d точек, %d фондов)", ...)`.
- `bold/series_meta.go`: `func boldETFSeriesMeta() []util.SeriesMeta` — 7 записей, единицы и тексты из спеки §4.1/§4.4 (`frequency: "D"`, description обязан нести: «73 фонда», «≈83 % мировой величины WGC», «не складывать и не сравнивать с `etf_holdings_tonnes_world`/`etf_flow_tonnes_world`», «окна — календарные (7/30/90 дней)», «источник bold.report (ByteTree), ключей нет»).
- Регистрация импортёра в `main.go` не нужна: пакет подключается blank-import'ом в `main.go` (добавить `_ ".../bold"` в список импортов, рядом с `_ ".../wgc"`).

- [ ] **Step 3: Прогнать тесты**

Run: `go test ./bold/... -v -count=1`
Expected: PASS (все 13); саботаж-проверки: (а) `boldNeedsFullFetch` без порога 96 ч → валится `TestBoldNeedsFullFetch`; (б) чтение предыдущих унций из ответа вместо БД → `TestBoldEtfImportIncrementalWritesOneDay`; (в) отсутствие проверки дат → `TestBoldEtfFetchLatestMismatchedDatesFails`.

- [ ] **Step 4: Commit**

```bash
gofmt -l bold/
git add bold/bold.go bold/series_meta.go bold/bold_test.go main.go
git commit -m "add bold_etf_flows importer with incremental fetch"
```

---

