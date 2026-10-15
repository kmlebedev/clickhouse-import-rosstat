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

