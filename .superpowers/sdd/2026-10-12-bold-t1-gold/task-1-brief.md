### Task 1: `bold/` — разбор JSON, конверсии и построение точек

**Files:**
- Create: `bold/parse.go`, `bold/parse_test.go`, `bold/testdata/bold_price_latest.json`, `bold/testdata/bold_units_latest.json`, `bold/testdata/bold_aum_latest.json`, `bold/testdata/bold_flows_latest.json`, `bold/testdata/bold_summary.json`, `bold/testdata/bold_units_history.json`, `bold/testdata/bold_aum_history.json`, `bold/testdata/bold_flows_history.json`, `bold/testdata/bold_price_history.json`

**Interfaces:**
- Consumes: `wgc.seriesPoint`-подобный тип — **не импортировать `wgc`**; объявить локально `type point struct { Series string; Date time.Time; Value float64 }` в `bold/parse.go` (пакеты источников не зависят друг от друга).
- Produces (нужно Task 2 и Task 3):
  - `const boldBaseURL = "https://bold.report/api/v1/"`, пути `boldPathPrice = "gold/price"`, `boldPathPriceLatest = "gold/price-latest"`, `boldPathUnits = "gold/funds/units"`, `boldPathUnitsLatest = "gold/funds/units-latest"`, `boldPathAum = "gold/funds/aum"`, `boldPathAumLatest = "gold/funds/aum-latest"`, `boldPathFlows = "gold/flows/all"`, `boldPathFlowsLatest = "gold/flows/all-latest"`, `boldPathSummary = "gold/summary"`.
  - `type boldRow struct { Date string; Value, Units, Aum, Culm7, Culm30, Culm90, Price *float64 }` (JSON-теги `date`, `value`, `units`, `aum`, `culm7day`, `culm30day`, `culm90day`, `price`) и `type boldEnvelope struct { Version, Updated string; Data []boldRow }`.
  - `type boldFundRow struct { Ticker, Name string; Units, UnitsChange, Aum, AumChange, Price, PriceChange, Shares, SharesChange *float64 }` (`units`, `unitsChange`, `aum`, `aumChange`, `price`, `priceChange`, `shares`, `sharesChange`); `type boldFundSnapshot struct { Version, Updated string; Data []boldFundRow }`.
  - `type boldMetric int` + `const ( boldMetricPrice boldMetric = iota; boldMetricUnits; boldMetricAum; boldMetricFlows )`; `func (m boldMetric) require(r boldRow) error` — отсутствие обязательного поля метрики → ошибка с именем метрики и даты; `func (m boldMetric) present(r boldRow) bool`.
  - `func parseLatest(body []byte, metric boldMetric) (boldRow, error)`, `func parseHistory(body []byte, metric boldMetric) ([]boldRow, error)`, `func parseFundSnapshot(body []byte) ([]boldFundRow, error)`.
  - `func parseBoldDate(s string) (time.Time, error)` (layout `2006-01-02`, UTC), `func ozToTonnes(oz float64) float64`.
  - `func boldETFSets(rows []boldRow, prevUnits *float64, prevDate time.Time) ([]point, error)` — строит точки 7 рядов (см. Step 3); `func boldFundPoints(snapshot []boldFundRow, date time.Time) []boldFundPoint`, где `type boldFundPoint struct { Date time.Time; Ticker, Name string; Units, UnitsChange, Aum, AumChange, Price, PriceChange, Shares, SharesChange float64 }`.

- [ ] **Step 1: Сгенерировать фикстуры из живых ответов**

```bash
python3 - <<'PY'
import json, urllib.request, datetime
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"}
B = "https://bold.report/api/v1/"
def get(p):
    return json.load(urllib.request.urlopen(urllib.request.Request(B + p, headers=UA), timeout=40))
def dump(name, obj):
    open(f"bold/testdata/{name}", "w").write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
# «последний день» — как есть (крошечные)
for p, n in (("gold/price-latest", "bold_price_latest.json"), ("gold/funds/units-latest", "bold_units_latest.json"),
             ("gold/funds/aum-latest", "bold_aum_latest.json"), ("gold/flows/all-latest", "bold_flows_latest.json"),
             ("gold/summary", "bold_summary.json")):
    dump(n, get(p))
# история — урезаем до трёх дат: 2015-01-01 (нет units/aum), 2026-10-08, 2026-10-09
KEEP = {"2015-01-01", "2026-10-08", "2026-10-09"}
for p, n in (("gold/funds/units", "bold_units_history.json"), ("gold/funds/aum", "bold_aum_history.json"),
             ("gold/flows/all", "bold_flows_history.json"), ("gold/price", "bold_price_history.json")):
    d = get(p)
    d["data"] = [r for r in d["data"] if r.get("date") in KEEP]
    dump(n, d)
PY
wc -c bold/testdata/*.json   # ожидаем единицы КБ, summary ~15 КБ
```

- [ ] **Step 2: Написать падающие тесты**

`bold/parse_test.go` (пакет `bold`, офлайн, читает `testdata/`):

```go
func TestParseLatestPinsFacts(t *testing.T)      // 2026-10-09: units 113180843, aum 476677811738, culm7 43740, culm30 991060, culm90 5376055, price 4216.3
func TestParseLatestRequiresMetricFields(t *testing.T) // тело units-latest без "units" → ошибка, называющая метрику
func TestParseLatestRejectsBadDate(t *testing.T)  // date "09/10/2026" → ошибка
func TestParseHistorySkipsNullUnits(t *testing.T) // 2015-01-01 → Units == nil, в точки не идёт (вклад 2 строки)
func TestBoldETFSetsPinsFacts(t *testing.T)       // rows за 08.10 и 09.10 → 3520.318 т, 476.678 млрд $, 1.360/30.825/167.214 т, +4.233 т, 573.75 млн $
func TestBoldETFSetsSkipsFirstFlow(t *testing.T)  // одна строка истории → ряда etf_bold_flow_tonnes нет, остальные 5 есть
func TestBoldETFSetsUsesPrevUnitsFromDB(t *testing.T) // rows только за 09.10, prevUnits = 113044763, prevDate = 08.10 → +4.233 т
func TestBoldETFSetsErrorsOnDateMismatch(t *testing.T) // price за 10-09, units за 10-08 → ошибка
func TestParseFundSnapshotPinsFacts(t *testing.T)  // 73 фонда, у GLD units == 33552628, у всех полей есть значения
func TestOzToTonnes(t *testing.T)                 // 113180843 → 3520.318 (±1e-3), 32150.74656862 → 1.0
```

- [ ] **Step 3: Реализовать `bold/parse.go`**

- `parseBoldDate`: `time.ParseInLocation("2006-01-02", s, time.UTC)`, отказ на пустой/ином формате.
- `parseLatest`: `json.Unmarshal` в `boldEnvelope`-обёртку? — нет: в `boldRow` (файл «последнего дня» — это объект верхнего уровня, не список), затем `parseBoldDate(row.Date)` и `metric.require(row)`.
- `parseHistory`: `json.Unmarshal` в `boldEnvelope`, каждую строку — `parseBoldDate`; строки без полей метрики **оставить с `nil`** (решение о точке принимает `boldETFSets`).
- `parseFundSnapshot`: `json.Unmarshal` в `boldFundSnapshot`; `len(Data) == 0` → ошибка.
- `boldETFSets(rows, prevUnits, prevDate)`: отсортировать `rows` по дате (стабильная детерминированность), затем для каждой строки:
  - `etf_bold_holdings_tonnes` = `units/32150.74656862`, если `Units != nil`;
  - `etf_bold_aum_usd_bn` = `aum/1e9`, если `Aum != nil`;
  - окна: `culm7/30/90 / 32150.74656862`, каждое при не-nil;
  - дневной поток: `units_t − units_prev` (из предыдущей строки, а для первой строки — из `prevUnits`/`prevDate`, если `prevUnits != nil`); нет предыдущего — потока нет; `etf_bold_flow_tonnes` = Δ/32150.74656862, `etf_bold_flow_usd_mln` = Δ×`price_t`/1e6 (при не-nil `Price`; нет цены — только тоннажный поток, а `flow_usd_mln` без точки);
  - даты `rows`, `prevDate` — уже конец дня (`Date32` в 00:00 UTC), никакой нормализации не делать.
  - пустой результат → ошибка «нет точек».
- `boldFundPoints`: перевод строк среза в точки, `nil` в числах → ошибка (срез обязан быть полным; отсутствие поля — сигнал смены формы ответа).

- [ ] **Step 4: Прогнать тесты (падать → пройти)**

Run: `go test ./bold/... -run 'TestParse|TestBoldETFSets|TestOzToTonnes' -v`
Expected: сначала FAIL (`undefined: parseLatest`), после реализации — PASS (10/10); саботаж-проверки: убрать `/32150.74656862` (рост чисел), считать `null` нулём, взять окна суммой 5 дней — каждый саботаж обязан валить свой тест.

- [ ] **Step 5: Commit**

```bash
gofmt -l bold/parse.go bold/parse_test.go
git add bold/parse.go bold/parse_test.go bold/testdata/*.json
git commit -m "add bold.report json parsing and conversions"
```

---

