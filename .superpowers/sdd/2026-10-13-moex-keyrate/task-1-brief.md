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

