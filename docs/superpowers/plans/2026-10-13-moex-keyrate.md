# keyrate_futures (фьючерс на ключевую ставку MOEX KEYRATE) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Новый импортёр `keyrate_futures` в пакете `moex/`: дневные итоги торгов по всем сериям расчётного фьючерса на Индекс ключевой ставки (MOEX KEYRATE) → таблица `keyrate_futures`, паспорта серий (дата экспирации = дата заседания ЦБ) → `keyrate_contract`, витрина `v_keyrate_expectations` с вменённым изменением ставки к заседанию, каталог рядов, шаг в `dagu/moex.yaml`, грант `kimi_reader`.

**Architecture:** Разбор блоков ISS и расчёт точки старта инкремента — чистые функции (`moex/keyrate_parse.go`), импортёр и транспорт — тонкая обвязка (`moex/keyrate.go`) на существующих примитивах пакета (`getBlock`/новый `getBlocks`, `util.HttpClient`, `moexBaseUrl`, `lastDate`, `util.View`, `util.UpsertSeriesCatalog`). Список серий не хардкодится: он перечитывается из ISS каждый прогон, потому что серии добавляются и истекают каждый квартал.

**Tech Stack:** Go, `net/http/httptest`, `logrus`, ClickHouse (`clickhouse-go/v2`, `PrepareBatch → Append → Send`), MOEX ISS (JSON), dagu (yaml).

**Spec:** `docs/superpowers/specs/2026-10-13-moex-keyrate-design.md`

## Global Constraints

- Новых внешних зависимостей нет; HTTP — только через `util.HttpClient` (уже используется в `moex/`); сетевых вызовов в `init()` нет.
- Запись в ClickHouse — только батчами: `PrepareBatch → Append` (err проверяется) `→ Send`; DDL `IF NOT EXISTS`; витрина через `util.CreateView` (`CREATE OR REPLACE VIEW … DEFINER = default SQL SECURITY DEFINER` + комментарии таблицы и каждой колонки).
- DDL двух таблиц — **канонический** из `ARCHITECTURE.md` §6.2 (обновляется Task 4); копия в `moex/keyrate.go` обязана совпадать.
- **`LAST` из ISS не используется нигде**: при отсутствии сделок он равен нулю. Цена дня — `SETTLEPRICE` из блока `history`.
- Спот для вменённого изменения берётся из `cbr_key_rate` (`argMax(rate, date)`), а не из индекса `KEYRATE` (он численно дублирует ставку).
- `secid` перед подстановкой в SQL (`lastDate`) валидируется по `^[A-Z0-9]+$` — значения приходят из внешнего JSON.
- Список серий: фильтр по блоку `securities` запроса `securities.json?q=KEYRATE` — `primary_boardid == "RFUD"` и `is_traded == 1` (параметр `assetcode=` ISS игнорирует; календарные спреды отсекаются по `is_traded`).
- Логи — logrus по-русски (конвенция проекта); коммиты — по-английски, в духе истории.
- Тесты офлайн: сеть только `httptest`, фикстуры — `moex/testdata/*.json`; `go test ./...` без сети и БД.
- Расписание: шаг в `dagu/moex.yaml` (`type: chain`, ежедневно 19:13) с retry 3 × 300, как у соседних шагов; `dagu validate` обязателен.

## Review Focus

1. **`LAST` не пролез в цену** — ни в разборе, ни в витрине: цена дня только из `SETTLEPRICE` блока `history` (Task 1, 3).
2. **Серии не хардкодятся** и отсеиваются правильно: спред `KKV6KKZ6` (board `RFUD`, `is_traded=0`) не должен попасть в список (Task 1).
3. **Инкремент по каждой серии свой**: `KKG7` стартовала позже остальных (12.10 против 29.09) — общая дата старта подставила бы чужой горизонт (Task 2, 3).
4. **Пагинация `history.cursor`** не теряет вторую страницу и не зацикливается на пустой (Task 2).
5. **`secid` валидируется до SQL** (Task 3).
6. **Комментарии витрины** называют значение вменённым индикативом при низкой ликвидности, а не «прогнозом рынка» (Task 3).

---

### Task 1: Разбор блоков ISS — чистые функции

**Files:**
- Create: `moex/keyrate_parse.go`, `moex/keyrate_parse_test.go`
- Test fixture: `moex/testdata/keyrate_securities.json`, `moex/testdata/keyrate_description.json`, `moex/testdata/keyrate_history.json`

**Interfaces:**
- Consumes: `issBlock`, `columnIndex`, `rowFloat`, `errBadCell`, `moexDate` (уже в `moex/`).
- Produces (нужно Task 2–3):
  - `type keyrateContract struct { secid, seriesName string; firstTrade, lastTrade time.Time }`
  - `type keyrateDay struct { date time.Time; close, settle, waprice float64; volume, numtrades, openposition uint64 }`
  - `func parseKeyrateSecids(b issBlock) ([]string, error)` — `secid` строк, прошедших фильтр board/is_traded
  - `func parseKeyrateContract(b issBlock) (keyrateContract, error)` — из блока `description` (name/value)
  - `func parseKeyrateHistory(b issBlock) ([]keyrateDay, error)`
  - `func parseHistoryTotal(b issBlock) (int, error)` — `TOTAL` из блока `history.cursor`
  - `func validSecid(s string) bool` — `^[A-Z0-9]{4,}$`

- [ ] **Step 1: Снять фикстуры с живого ISS** (одноразово, результат кладётся в `moex/testdata/`)

```
curl -s "https://iss.moex.com/iss/securities.json?q=KEYRATE&iss.meta=off" > moex/testdata/keyrate_securities.json
curl -s "https://iss.moex.com/iss/securities/KKV6.json?iss.meta=off" > moex/testdata/keyrate_description.json
curl -s "https://iss.moex.com/iss/history/engines/futures/markets/forts/securities/KKV6.json?iss.meta=off&from=2026-09-29" > moex/testdata/keyrate_history.json
```

Фикстуры усекаются до нескольких строк вручную, но **структура блоков сохраняется целиком** (`columns` не трогать). В `keyrate_securities.json` обязаны остаться: индекс `KEYRATE`, три фьючерса `KKV6/KKZ6/KKG7`, спред `KKV6KKZ6` — иначе тест фильтра ничего не проверяет.

- [ ] **Step 2: Написать падающие тесты** — `moex/keyrate_parse_test.go`

```go
func TestParseKeyrateSecids(t *testing.T)      // фикстура → [KKV6 KKZ6 KKG7]; индекса KEYRATE и спреда KKV6KKZ6 нет
func TestParseKeyrateSecidsNoColumns(t *testing.T) // блок без primary_boardid → ошибка
func TestParseKeyrateContract(t *testing.T)    // KKV6 → seriesName "KEYRATE-10.26", firstTrade 2026-09-29, lastTrade 2026-10-23
func TestParseKeyrateContractMissingKey(t *testing.T) // нет LSTDELDATE → ошибка (молчаливый zero time недопустим)
func TestParseKeyrateHistory(t *testing.T)     // фикстура → дни с settle/close/waprice/volume/numtrades/openposition
func TestParseKeyrateHistoryIgnoresLast(t *testing.T) // в строке LAST отсутствует вовсе; проверяем, что parse не ищет колонку LAST
func TestParseHistoryTotal(t *testing.T)       // history.cursor TOTAL=10
func TestValidSecid(t *testing.T)              // "KKV6" true; "kkv6", "KK V6", "" false
```

- [ ] **Step 3: Прогнать — падает**

Run: `go test ./moex/ -run 'Keyrate|ValidSecid' -v`
Expected: FAIL — `undefined: parseKeyrateSecids` и т. д.

- [ ] **Step 4: Реализовать `moex/keyrate_parse.go`**

Разбор — по именам колонок (`columnIndex`), а не по позициям; отсутствие обязательной колонки — ошибка через `errNoColumns`. `description` — блок пар `name`/`value`, значения строковые:

```go
// parseKeyrateContract: description отдаёт пары (name, value); ищем ключи по имени.
// LSTDELDATE — дата экспирации (= дата заседания ЦБ); если пусто, берём LSTTRADE.
```

`parseKeyrateHistory` читает только `tradedate, close, settleprice, waprice, volume, numtrades, openposition`; `volume/numtrades/openposition` — целые (конвертация `uint64(rowFloat(...))` после проверки неотрицательности). Колонки `LAST` в блоке `history` нет, и код её не ищет — это и закрепляет `TestParseKeyrateHistoryIgnoresLast` (в фикстуру `last`-колонку **не добавлять**).

- [ ] **Step 5: Прогнать — зелёный**

Run: `go test ./moex/ -run 'Keyrate|ValidSecid' -v` → PASS
Run: `gofmt -l moex/ && go vet ./moex/` → пусто

---

### Task 2: Транспорт — обнаружение серий, паспорт, итоги с пагинацией

**Files:**
- Modify: `moex/moex.go` (добавить `getBlocks`, выразить через него `getBlock`), `moex/keyrate.go` (новый, транспорт пока здесь)
- Create: `moex/keyrate_test.go`

**Interfaces:**
- Consumes: `getBlocks(ctx, path) (map[string]issBlock, error)`, `parseKeyrateSecids`, `parseKeyrateContract`, `parseKeyrateHistory`, `parseHistoryTotal`.
- Produces (нужно Task 3):
  - `func fetchKeyrateSecids(ctx context.Context) ([]string, error)`
  - `func fetchKeyrateContract(ctx context.Context, secid string) (keyrateContract, error)`
  - `func fetchKeyrateHistory(ctx context.Context, secid string, from time.Time) ([]keyrateDay, error)`

- [ ] **Step 1: Написать падающие тесты** — `moex/keyrate_test.go` (httptest, подмена `moexBaseUrl`)

```go
func TestFetchKeyrateSecids(t *testing.T)        // httptest отдаёт фикстуру → [KKV6 KKZ6 KKG7]
func TestFetchKeyrateContractHTTPError(t *testing.T) // 500 → ошибка
func TestFetchKeyrateHistoryPaginates(t *testing.T)  // PAGESIZE=2, TOTAL=3: сервер отдаёт страницы по параметру start; проверяем, что сделано 2 запроса и вернулось 3 дня
func TestFetchKeyrateHistoryEmpty(t *testing.T)      // TOTAL=0 → пустой срез, без ошибки и без зацикливания
```

Восстановление `moexBaseUrl` — через `t.Cleanup` (образец — существующие тесты пакета, если подменяют; иначе `defer func(){ moexBaseUrl = old }()`).

- [ ] **Step 2: Прогнать — падает**

Run: `go test ./moex/ -run TestFetchKeyrate -v`
Expected: FAIL — `undefined: fetchKeyrateSecids`.

- [ ] **Step 3: Добавить `getBlocks` и выразить через него `getBlock`** (`moex/moex.go`)

Один HTTP-запрос отдаёт и `history`, и `history.cursor`; два вызова `getBlock` на одну страницу — лишний запрос к ISS:

```go
// getBlocks читает все блоки ответа ISS одним запросом. getBlock — обёртка над ним
// для путей, которым нужен ровно один блок.
func getBlocks(ctx context.Context, path string) (map[string]issBlock, error) { ... }
func getBlock(ctx context.Context, path, block string) (issBlock, error) { ... } // через getBlocks
```

Поведение `getBlock` не меняется (те же тексты ошибок) — существующие тесты пакета должны остаться зелёными.

- [ ] **Step 4: Реализовать транспорт** в `moex/keyrate.go`

```go
const (
	keyrateSearchPath  = "/iss/securities.json?q=KEYRATE&iss.meta=off"
	keyrateDescPath    = "/iss/securities/%s.json?iss.meta=off"
	keyrateHistoryPath = "/iss/history/engines/futures/markets/forts/securities/%s.json"
)
```

`fetchKeyrateHistory` — цикл по `start` шагом `moexPageLimit`: `path := fmt.Sprintf(keyrateHistoryPath, secid) + fmt.Sprintf("?iss.meta=off&from=%s&start=%d", from.Format(moexDate), start)`; один `getBlocks` на страницу; `history` → `parseKeyrateHistory`, `history.cursor` → `parseHistoryTotal`; выход при `len(дней) == 0` или когда собрано ≥ `TOTAL`.

- [ ] **Step 5: Прогнать — зелёный**

Run: `go test ./moex/... -v` → PASS (старые тесты пакета тоже)
Run: `gofmt -l moex/ && go vet ./moex/`

---

### Task 3: Импортёр `keyrate_futures`

**Files:**
- Modify: `moex/keyrate.go` (DDL, `Import`, каталог, витрина, `init()`)
- Modify: `moex/keyrate_test.go` (тесты чистых частей импортёра)

**Interfaces:**
- Consumes: транспорт Task 2, `lastDate`, `util.UpsertSeriesCatalog`, `util.CreateView`, `util.View`, `log`.
- Produces:
  - `const keyrateTable = "keyrate_futures"`, `const keyrateContractTable = "keyrate_contract"`
  - `func keyrateSince(c keyrateContract, stored time.Time) time.Time` — `stored+1 день`, при нулевом `stored` — `c.firstTrade`
  - `func keyrateSeriesMeta(contracts []keyrateContract) []util.SeriesMeta`
  - `type keyrateFutures struct{}` + `Name()`/`Import()` + `init()` → `chimport.Stats`

- [ ] **Step 1: Написать падающие тесты**

```go
func TestKeyrateSince(t *testing.T)          // stored=2026-10-08 → 2026-10-09; stored zero → firstTrade
func TestKeyrateSeriesMetaPerContract(t *testing.T) // 2 контракта → 2 записи: source=moex, series=secid,
                                             // все поля непусты, в Origin есть дата экспирации,
                                             // в Description — мнемоника вменённого изменения к заседанию
```

- [ ] **Step 2: Прогнать — падает** (`go test ./moex/ -run 'KeyrateSince|KeyrateSeriesMeta' -v`)

- [ ] **Step 3: DDL и витрина**

```go
const keyrateDdl = `CREATE TABLE IF NOT EXISTS keyrate_futures (
      secid LowCardinality(String)
    , date Date
    , close Float64
    , settle Float64
    , waprice Float64
    , volume UInt64
    , numtrades UInt64
    , openposition UInt64
) ENGINE = ReplacingMergeTree ORDER BY (secid, date);`

const keyrateContractDdl = `CREATE TABLE IF NOT EXISTS keyrate_contract (
      secid LowCardinality(String)
    , series_name String
    , first_trade Date
    , last_trade Date
    , updated_at DateTime
) ENGINE = ReplacingMergeTree ORDER BY (secid);`

var keyrateView = util.View{
	Name:   "v_keyrate_expectations",
	Tables: []string{keyrateTable, keyrateContractTable, "cbr_key_rate"},
	Select: `SELECT f.date AS date, f.secid AS secid, c.series_name AS series_name,
	       c.last_trade AS decision_date, f.settle AS settle, f.close AS close,
	       f.volume AS volume, f.openposition AS openposition,
	       round(f.settle - k.rate, 4) AS expected_change_pp,
	       round((f.settle - k.rate) * 100, 1) AS expected_change_bp
	FROM keyrate_futures FINAL f
	INNER JOIN keyrate_contract FINAL c ON f.secid = c.secid
	CROSS JOIN (SELECT argMax(rate, date) AS rate FROM cbr_key_rate FINAL) k`,
	Comment: "...", // см. требования к комментариям ниже
	Columns: map[string]string{ ... }, // все 10 колонок
}
```

Требования к текстам комментариев (правило 11б): в комментарии витрины — что это **вменённый рынком уровень ставки на дату заседания**, что инструмент **низколиквиден**, поэтому значение **индикативно** (не «прогноз рынка» и не цена обязательства), и что `settle` — расчётная цена дня, а `LAST` не используется; в комментарии `expected_change_bp` — что изменение считается к **последней известной** ставке `cbr_key_rate`, а нулевой объём серии делает значение нерепрезентативным; в комментарии `decision_date` — что это дата экспирации серии и она равна дате заседания ЦБ; в комментарии `secid` — что 1 п.п. = 1 000 ₽ (шаг 0,01 п.п. = 10 ₽).

- [ ] **Step 4: Реализовать `Import`**

Порядок шагов (часть контракта, не стиль):

1. `conn.Exec(keyrateDdl)`, `conn.Exec(keyrateContractDdl)` — до сети (импортёр обязан отработать на чистой базе).
2. `fetchKeyrateSecids` → для каждой валидной серии (`validSecid`) `fetchKeyrateContract` → накопить `[]keyrateContract`.
3. Паспорта — одним батчем в `keyrate_contract` (`secid, series_name, first_trade, last_trade, now()`).
4. Для каждой серии: `stored := lastDate(ctx, conn, keyrateTable, "secid = '"+c.secid+"'")` → `from := keyrateSince(c, stored)` → `fetchKeyrateHistory(ctx, c.secid, from)` → строки в батч `keyrate_futures`.
5. `Send` обоих батчей, `log.Infof("Собрано %d дней по %d сериям KEYRATE", ...)`.
6. `util.UpsertSeriesCatalog(ctx, conn, keyrateSeriesMeta(contracts))` — **после** успешного `Send`.
7. `util.CreateView(ctx, conn, keyrateView)`.

Ошибка на любой серии — не молчание: `log.Warnf` с `secid` и продолжение по остальным (серии независимы, как ленты в `news_watch`); ошибка **всех** серий — ошибка импорта.

- [ ] **Step 5: Прогнать — зелёный**

Run: `go test ./moex/... -v` → PASS
Run: `gofmt -l moex/ && go vet ./moex/ && golangci-lint run ./moex/...`

---

### Task 4: Окружение — расписание, грант, канонический DDL

**Files:**
- Modify: `dagu/moex.yaml`, `sql/mcp_kimi_reader.sql`, `ARCHITECTURE.md`

- [ ] **Step 1: Шаг в `dagu/moex.yaml`**

Добавить третьим шагом (после `ofz_curve`) по образцу соседей:

```yaml
  - id: keyrate_futures
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=keyrate_futures
    retry_policy:
      limit: 3
      interval_sec: 300
```

- [ ] **Step 2: Грант** — в `sql/mcp_kimi_reader.sql` добавить `GRANT SELECT ON default.v_keyrate_expectations TO kimi_reader;` (сырые `keyrate_futures`/`keyrate_contract` агенту не выдаются).

- [ ] **Step 3: `ARCHITECTURE.md`** — канонический DDL двух таблиц в §6.2 (дословно как в `moex/keyrate.go`), строка импортёра в §6.1 (источник, метод, расписание, где каталог), строка витрины в §6.3 с комментарием; календарь §7 — ближайшие заседания СД ЦБ уже перечислены, добавить пометку, что даты экспирации KEYRATE берутся из `cbr.ru/dkp/cal_mp/`.

- [ ] **Step 4: Проверки**

Run: `dagu validate dagu/moex.yaml` → OK
Run: `make all` → зелёный

---

### Task 5: Живая приёмка и статус пункта

- [ ] **Step 1:** `make import STAT=keyrate_futures` → в логе строки по трём сериям; повторный прогон идемпотентен (`0` новых).

- [ ] **Step 2:** витрина:

```sql
SELECT secid, date, settle, expected_change_bp FROM v_keyrate_expectations ORDER BY secid, date DESC LIMIT 10;
```

Сверить с фактом: серия ближайшего заседания (`KKV6`, экспирация 23.10.2026) при ставке 14,00% должна давать ≈ −7 б.п. (наблюдённое `settle` 13,93). Результат записать в роадмап и в приёмку пункта.

- [ ] **Step 3:** `make mcp-check` → **ПРОВЕРКА ПРОЙДЕНА** (витрина читается, `keyrate_futures` — `ACCESS_DENIED`).

- [ ] **Step 4:** `dagu validate dagu/moex.yaml` → OK; каталог: `SELECT count() FROM v_series_catalog WHERE source='moex'` вырос на число серий.

- [ ] **Step 5:** `docs/ROADMAP_KEY_RATE.md` — статус 1.1 ✅ с командой и числом строк; «Приёмка» пункта заполнена фактом, не обещанием.
