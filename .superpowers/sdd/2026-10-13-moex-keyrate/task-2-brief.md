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

