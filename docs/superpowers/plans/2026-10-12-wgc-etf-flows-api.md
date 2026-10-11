# WGC ETF Flows через JSON API fscdn — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Импортёр `wgc_etf_flows` наполняет те же 20 рядов `etf_<метрика>_<регион>` из JSON API `fscdn.gold.org` вместо ручного скачивания `ETF_Flows_*.xlsx`; локальная выгрузка остаётся фолбэком.

**Architecture:** Новый `util.GetJSON` (HTTP-статус проверяется, в отличие от `util.GetFile`) + `wgc/etf_api.go`, который разбирает два эндпоинта WGC в тот же тип `seriesPoint`, что и xlsx-парсер, и переиспользует существующие хелперы регионов/`world`. `wgc_etf_flows.Import` сначала идёт в API, при ошибке — в файл; общий путь записи один.

**Tech Stack:** Go, `net/http/httptest`, `encoding/json`, `logrus`, ClickHouse (`clickhouse-go/v2`, запись батчами), `dagu` (yaml-расписание).

**Spec:** `docs/superpowers/specs/2026-10-12-wgc-etf-flows-api-design.md`

## Global Constraints

- Новых внешних зависимостей не добавлять; HTTP — только через `util.HttpClient` / `util.GetFile` / `util.GetCSV` / новый `util.GetJSON`.
- Запись в ClickHouse — только батчами: `PrepareBatch` → `Append` (с проверкой `err`) → `Send`; построчный `Exec` в цикле запрещён.
- Каждый `strconv.Parse*` / `json`-разбор / `Append` — с проверенной ошибкой; ошибки не проглатывать.
- `seriesPoint`, `monthEnd`, `etfRegionsByName`, `appendRegionPoints`, `allEtfRegionsPresent`, `etfWorldValue` — существующие (см. Tasks); новой логики регионов/`world` не писать.
- `null` в JSON WGC = отсутствие данных (точки нет); `0` = значение (точка есть).
- Тесты офлайн: в `go test` нет обращений в сеть — только `httptest`.
- Сохраняются: `Name() = "wgc_etf_flows"`, `source='wgc'`, имена 20 рядов, 20 записей каталога, приоритет витрины `wgc > webbridge > manual`.
- Коммиты — по-английски, в духе истории (`add wgc etf api client`, `fix batch insert cbr_queries`).
- Все команды с окружением — через `make` (Makefile подключает `~/.config/rosstat/env`).

## Review Focus

Входы и режимы отказа, которые спека подразумевает, а тесты по умолчанию не проверят (каждая строка закреплена тестом в своей задаче):

1. **`Gold Price (rhs)` в списке рядов потоков** — это известный ценовой ряд, а не регион: пропускается по имени. Любое другое имя вне словаря регионов — ошибка, а не ряд вида `etf_flow_tonnes_antarctica` (Task 2).
2. **`null` в holdings** — отсутствие данных, не 0: точки нет, и `world` за этот месяц не появляется (Task 2).
3. **Масштаб `usd`** — в API это абсолютные доллары: забытые ÷1e6 (поток) и ÷1e9 (AUM) дают числа в миллион и миллиард раз больше без единой ошибки (Task 2).
4. **Не-200 (403/5xx) и пустое/битое тело** — ошибка с внятной причиной, уход в фолбэк; а не «Imported 0 rows» с зелёным dagu (Tasks 1, 2, 3).
5. **ms-таймстемпы → конец месяца** — дата точки не должна сдвигаться из-за локальной таймзоны (Task 2).

---

### Task 1: `util.GetJSON` — JSON-канал с проверкой HTTP-статуса

**Files:**
- Create: `util/json.go`
- Test: `util/json_test.go`

**Interfaces:**
- Consumes: `util.HttpClient`, `util`-приватный `randomUA` (тот же файл-пакет `util`).
- Produces: `func GetJSON(url string, out any) error` — используется в Task 2.

- [ ] **Step 1: Write the failing tests**

Четыре теста в `util/json_test.go` (пакет `util`, `httptest`):

```go
func TestGetJSONDecodesBody(t *testing.T)      // 200 + {"value":42} → out.Value == 42, err == nil; заголовок User-Agent непуст
func TestGetJSONNonOKStatusFails(t *testing.T) // 403 + тело "<html>blocked</html>" → err содержит "403" и "blocked"
func TestGetJSONMalformedBodyFails(t *testing.T) // 200 + "not json" → err != nil
func TestGetJSONEmptyBodyFails(t *testing.T)   // 200 + пустое тело → err != nil
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./util/... -run TestGetJSON -v`
Expected: FAIL — `undefined: GetJSON`

- [ ] **Step 3: Implement `GetJSON` in `util/json.go`**

Сигнатура: `func GetJSON(url string, out any) error`. Копия стиля `GetCSV` из `util/xls.go`: `http.NewRequest("GET", url, nil)`, `req.Header.Set("User-Agent", randomUA)`, `HttpClient.Do(req)`, `io.ReadAll(resp.Body)`; тело пустое → ошибка «пустое тело» (как в `GetFile`). Отличие: **сначала статус** — `resp.StatusCode != http.StatusOK` → `fmt.Errorf("http status %d: %s", resp.StatusCode, snippet)`, где `snippet` — до 512 байт тела, ужатое до одной строки; при пустом теле — `"(пустое тело)"`. URL в текст ошибки **не подставлять** (хелпер общий, у будущих вызывающих ключ может быть в URL — урок `eia`; контекст добавляет вызывающий). Затем `json.Unmarshal(body, out)` с проверкой ошибки.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./util/... -run TestGetJSON -v`
Expected: PASS (4/4)

- [ ] **Step 5: Commit**

```bash
gofmt -l util/json.go util/json_test.go
git add util/json.go util/json_test.go
git commit -m "add util.GetJSON with http status check"
```

---

### Task 2: `wgc/etf_api.go` — разбор JSON WGC в точки рядов

**Files:**
- Create: `wgc/etf_api.go`, `wgc/etf_api_test.go`, `wgc/testdata/etf_flows_api.json`, `wgc/testdata/etf_holdings_api.json`
- Reference (не менять): `wgc/parse.go` (`seriesPoint`, `monthEnd`), `wgc/etf_parse.go` (`etfRegionsByName`, `etfMetric*`, `appendRegionPoints`, `allEtfRegionsPresent`, `etfWorldValue`, `wgcEtfSeriesName`)

**Interfaces:**
- Consumes: `util.GetJSON` (Task 1), `wgc`-хелперы выше.
- Produces (нужно Task 3):
  - `const etfAPIBaseURL = "https://fscdn.gold.org/api/v11/charts/etfv2/revised/"`
  - `const etfAPIFlowsPath = "flows-chart2"`, `const etfAPIHoldingsPath = "holdings-chart2"`
  - `const etfAPIPriceSeries = "Gold Price (rhs)"`
  - `var etfAPIScale = map[string]float64{etfMetricFlowUsdMln: 1e6, etfMetricAumUsdBn: 1e9}`
  - `func etfPointsFromAPI(baseURL string, now time.Time) ([]seriesPoint, error)`

- [ ] **Step 1: Сгенерировать фикстуры из живых эндпоинтов**

Скрипт (сеть нужна один раз; если её нет — взять сохранённые ответы `/tmp/wgc_flows-chart2.json` и `/tmp/wgc_holdings-chart2.json`, снятые 11.10.2026, и оставить те же три строки). Оставляем ровно три даты: `2003-02-28` (там `Asia` в holdings = `null`), `2026-08-31`, `2026-09-30`; все 4 региона + `Gold Price (rhs)`, обе метрики.

```bash
python3 - <<'PY'
import json, urllib.request, datetime
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"}
B = "https://fscdn.gold.org/api/v11/charts/etfv2/revised/"
KEEP = {"2003-02-28", "2026-08-31", "2026-09-30"}
def get(p):
    return json.load(urllib.request.urlopen(urllib.request.Request(B + p, headers=UA), timeout=40))
def d(ms):
    return datetime.datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d")
f = get("flows-chart2")
series = {k: [{"name": s["name"], "data": [p for p in s["data"] if d(p[0]) in KEEP]} for s in v]
          for k, v in f["chartData"]["data"]["Monthly"]["series"].items()}
json.dump({"system": {"cached": False},
           "chartData": {"asOfDate": f["chartData"]["asOfDate"],
                         "data": {"Monthly": {"series": series}}}},
          open("wgc/testdata/etf_flows_api.json", "w"), ensure_ascii=False, separators=(",", ":"))
h = get("holdings-chart2")
m = h["chartData"]["data"]["Monthly"]
tab = {k: {"columns": m[k]["columns"], "set": [r for r in m[k]["set"] if d(r[0]) in KEEP]} for k in ("tonnes", "usd")}
json.dump({"system": {"cached": False},
           "chartData": {"asOfDate": h["chartData"]["asOfDate"], "data": {"Monthly": tab}}},
          open("wgc/testdata/etf_holdings_api.json", "w"), ensure_ascii=False, separators=(",", ":"))
PY
wc -c wgc/testdata/etf_flows_api.json wgc/testdata/etf_holdings_api.json   # ожидаем единицы КБ, не сотни
```

- [ ] **Step 2: Write the failing tests**

`wgc/etf_api_test.go` (пакет `wgc`). Общий помощник: `httptest.NewServer`, отдающий фикстуры по путям `/flows-chart2` и `/holdings-chart2` (остальное — 404); тест зовёт `etfPointsFromAPI(srv.URL+"/", time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC))`.

```go
func TestEtfPointsFromAPIRecentMonth(t *testing.T)  // ключевые закреплённые значения
func TestEtfPointsFromAPIWorldCountsExplicitZeros(t *testing.T)
func TestEtfPointsFromAPINullHoldingsGivesNoPoint(t *testing.T)
func TestEtfPointsFromAPIRejectsUnknownSeries(t *testing.T)
func TestEtfPointsFromAPINonOKStatusFails(t *testing.T)
func TestEtfPointsFromAPIEmptyDataFails(t *testing.T)
```

Закреплённые ожидания (данные источника, сверены 11.10.2026 с ClickHouse; допуск 1e-6):

| ряд | дата | ожидание |
|---|---|---|
| `etf_flow_usd_mln_asia` | 2026-09-30 | `2255.40929756` |
| `etf_flow_tonnes_asia` | 2026-09-30 | `16.18610771` |
| `etf_holdings_tonnes_asia` | 2026-09-30 | `541.88` — **источник округляет тоннажные стоки до 0,01 т**, поэтому это НЕ `541.88460453` из xlsx; см. спеку §3 |
| `etf_aum_usd_bn_asia` | 2026-09-30 | `75.594060865` |
| `etf_flow_tonnes_world` | 2026-09-30 | `67.28075855` |
| `etf_flow_tonnes_world` | 2003-02-28 | `0` (явные нули всех четырёх регионов дают точку) |

Также: ни один ряд не содержит `gold price` (регистр не важен); за `2003-02-28` у `etf_holdings_tonnes_asia`, `etf_aum_usd_bn_asia` и `etf_holdings_tonnes_world` точек **нет**, а `etf_holdings_tonnes_north_america` — есть; за `2026-09-30` `etf_flow_tonnes_world` = сумме четырёх регионов; все точки лежат **на концах месяцев** (`2026-09-30`, `2026-08-31`, `2003-02-28`) — проверка идёт и по дате, поэтому ловит и сдвиг от сырого ms-таймстемпа. Отдельно закрепи тестом полноту набора: если для метрики потоков распознано **не четыре** региона (или в ответе нет ключа `usd`/`tonnes`) — это **ошибка**, а не частичный импорт: молча половина метрики потерялась бы. `TestEtfPointsFromAPIRejectsUnknownSeries` подсовывает тело с именем ряда `Antarctica` и ждёт ошибку. `TestEtfPointsFromAPINonOKStatusFails` отдаёт 403 и ждёт ошибку с `403`. `TestEtfPointsFromAPIEmptyDataFails` отдаёт валидный конверт с пустыми `series`/`set` и ждёт ошибку.

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./wgc/... -run TestEtfPointsFromAPI -v`
Expected: FAIL — `undefined: etfPointsFromAPI`

- [ ] **Step 4: Implement `etfPointsFromAPI` in `wgc/etf_api.go`**

- Типы конверта: `system` (нужно `cached` для лога), `chartData.asOfDate`, `data.Monthly`; потоки — `series: map[string][]{name string; data [][]float64}`, есть `usd` и `tonnes`; стоки — `{columns []string; set [][]*float64}` для `tonnes` и `usd` (указатель ловит `null`).
- URL: `baseURL + path + "?break-cache=" + now.Format("02Jan06")`.
- Потоки: для каждой метрики обойти список, **пропустить элемент с `name == etfAPIPriceSeries`**, остальные имена — через `etfRegionsByName`; неизвестное имя → ошибка «ряд ... вне словаря регионов» (со списком известных). Точка: `Date: monthEnd(time.UnixMilli(int64(ms)))`, `Value: value / scale` (`etfAPIScale[metric]`, отсутствует → 1). Собирать `map[region]float64` на каждый месяц и отдавать в `appendRegionPoints` — он же добавит `world` (сумма четырёх, только когда есть все четыре) и региональные точки.
- Стоки: обойти строки `set`; `nil` → точки нет; иначе `Value /= scale`, `Date` тот же `monthEnd`.
- Обе пары (тонны+USD) складывать в один `[]seriesPoint`.
- В конце: пустой результат → ошибка «выгрузка без точек» (fail-fast); `log.Infof` с `asOfDate`, `system.cached` и числом точек.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./wgc/... -run TestEtfPointsFromAPI -v`
Expected: PASS (6/6)

- [ ] **Step 6: Commit**

```bash
gofmt -l wgc/etf_api.go wgc/etf_api_test.go
git add wgc/etf_api.go wgc/etf_api_test.go wgc/testdata/etf_flows_api.json wgc/testdata/etf_holdings_api.json
git commit -m "add wgc etf flows json api parser"
```

---

### Task 3: `wgc/etf.go` — API канонический, файл фолбэком

**Files:**
- Modify: `wgc/etf.go` (целиком: текущий `Import` разложить на три функции)
- Modify: `wgc/etf_test.go` (существующие три теста + два новых)

**Interfaces:**
- Consumes: `etfPointsFromAPI`, `etfAPIBaseURL` (Task 2); `findLatestFile`, `parseEtfWorkbook`, `defaultDataDir` (существующие).
- Produces: `etfFlowsStat.dir` (было) + **`etfFlowsStat.apiBase`** (пусто → `etfAPIBaseURL`); `var errNoEtfFile error`; `func etfLoadPoints(dir, apiBase string, now time.Time) (points []seriesPoint, fromAPI bool, file string, err error)`; `func etfPointsFromFile(dir string) ([]seriesPoint, error)`; `func writeEtfPoints(ctx context.Context, conn driver.Conn, points []seriesPoint) (int64, error)`; `func etfStoredMaxDate(ctx context.Context, conn driver.Conn) (time.Time, bool, error)`.

- [ ] **Step 1: Write the failing tests**

Обновить существующие три теста (`TestEtfImportEmptyDirIsNoop`, `TestEtfImportBrokenFileFails`, `TestEtfImportWorkbookWithoutDataFails`): они теперь обязаны задавать недоступный API — иначе `Import` уйдёт в сеть. Помощник:

```go
// apiDown — сервер, отвечающий 500: тесты, которым нужен файловый путь, не должны
// ходить в реальный fscdn (и не должны зависеть от сети вовсе).
func apiDown(t *testing.T) string
```

Существующие тесты получают `stat := etfFlowsStat{dir: dir, apiBase: apiDown(t)}` и сохраняют прежние ожидания (пустой каталог → 0 строк без ошибки; битый файл → ошибка, называющая файл; воркбук без данных → ошибка «без точек»): так они теперь закрепляют **фолбэк**.

Новые тесты:

```go
// API отвечает 500, в каталоге — реальная выгрузка: транспорт обязан уйти на файл.
func TestEtfLoadPointsFallsBackToFile(t *testing.T)
// API отвечает фикстурами, в каталоге — битый файл: файл читаться не должен,
// иначе импорт упал бы на разборе (и это доказывает, что API победил).
func TestEtfLoadPointsPrefersAPIOverFile(t *testing.T)
```

Оба зовут `etfLoadPoints` напрямую (conn не нужен): в первом ожидание `fromAPI == false` и `len(points) > 0`; во втором `fromAPI == true` и `err == nil`. Реальная выгрузка копируется из `wgc/testdata/ETF_Flows_2026-10-02_1401.xlsx` в `t.TempDir()`; битый файл — `os.WriteFile` с мусором под маской `ETF_Flows_broken.xlsx` (в том же каталоге, но проверяется победа API).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./wgc/... -run 'TestEtfLoadPoints|TestEtfImport' -v`
Expected: FAIL — `undefined: etfLoadPoints`, `unknown field apiBase`

- [ ] **Step 3: Implement the refactor in `wgc/etf.go`**

- `etfPointsFromFile(dir string) ([]seriesPoint, error)`: существующая логика `Import` (findLatestFile → OpenFile → parseEtfWorkbook); файла нет → `errNoEtfFile`; битый файл / пустой разбор → ошибки с именем файла (как сейчас).
- `etfLoadPoints(dir, apiBase string, now time.Time) (points []seriesPoint, fromAPI bool, file string, err error)`: `apiBase == ""` → `etfAPIBaseURL`; `etfPointsFromAPI`; успех API → `(points, true, "", nil)`; ошибка API → `log.Warnf("%s: API WGC недоступно (%v) — пробую локальную выгрузку", ...)` и `etfPointsFromFile`; фолбэк на файл → `(points, false, file, nil)`, где `file` — путь выбранной выгрузки (им вызывающий инструментирует лог и называет источник в guard-е устаревания); `errNoEtfFile` → warn + `(nil, false, "", nil)`; прочая ошибка файла → `(nil, false, "", err)`.
- `writeEtfPoints(ctx, conn, points) (int64, error)`: текущий код записи как есть (сортировка `(series, date)`, `conn.Exec(macroSeriesDdl)`, `PrepareBatch`, `Append` с `batch.Abort()` при ошибке, `Send`, `util.UpsertSeriesCatalog(ctx, conn, wgcEtfCatalog())` после успешного `Send`, `log.Infof("Imported %d rows of %s (wgc ETF flows, %d series)", ...)`).
- `etfStoredMaxDate(ctx, conn) (time.Time, bool, error)`: `SELECT max(date) FROM macro_series FINAL WHERE source = 'wgc' AND series LIKE 'etf\_%'`; пустая таблица (`max` → нулевая дата) → `(time.Time{}, false, nil)`.
- `Import`: `points, fromAPI, file, err := etfLoadPoints(dir, s.apiBase, time.Now())` — ветки возврата: успех API → `(points, true, "", nil)`, фолбэк на файл → `(points, false, file, nil)` с путём выбранной выгрузки в `file`, `errNoEtfFile` → `(nil, false, "", nil)`, прочая ошибка файла → `(nil, false, "", err)`; `err` → вернуть; `len(points) == 0` → `log.Warnf` + `0, nil` (соединение не трогается); если `fromAPI` — guard кэша: `etfStoredMaxDate` и при меньшем максимуме API `log.Warnf("... возможен устаревший ответ CDN: API %s < сохранённый %s")` (**не ошибка**); иначе (файловый транспорт) — `log.Infof` с путём `file` и guard с источником `"локальная выгрузка "+file`; затем `writeEtfPoints`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./wgc/... -v`
Expected: PASS — новые 2 + существующие (обновлённые) тесты `wgc`, включая xlsx-разбор `etf_parse_test.go`

- [ ] **Step 5: Commit**

```bash
gofmt -l wgc/etf.go wgc/etf_test.go
git add wgc/etf.go wgc/etf_test.go
git commit -m "wgc etf flows: use json api with xlsx fallback"
```

---

### Task 4: Объявленные контракты — каталог, витрина, расписание

**Files:**
- Modify: `wgc/series_meta.go`, `wgc/series_meta_test.go`, `views/gold_etf_flows.go`, `dagu/wgc.yaml`

**Interfaces:**
- Consumes: `etfAPIFlowsPath`, `etfAPIHoldingsPath` (Task 2); `etfCatalogImportNote`, `etfCatalogWorldSince`, `etfOrigin*` (существующие).
- Produces: обновлённые тексты каталога (их читает MCP-агент через `v_series_catalog`) — сигнатур не меняет.

- [ ] **Step 1: Обновить `wgc/series_meta.go`**

- `etfOrigin*`: происхождение — API: `fscdn.gold.org`, эндпоинт `flows-chart2` (поток USD и поток тонн) или `holdings-chart2` (запасы и AUM) с указанием метрики; фолбэк — файл `ETF_Flows_*.xlsx` (лист `Fund flows by month` / `Demand by month` / `Holdings by month` / `Charts Data`), чтобы агент понимал обе дороги.
- `etfCatalogImportNote`: канонический канал — автоматический (API, расписание `dagu/wgc.yaml` ежедневно), ручная выгрузка в `wgc/data` — фолбэк, если API недоступно; приоритет источников витрины сохранить (`wgc > webbridge > manual`).
- `etfCatalogWorldSince` → **две константы**: для потоков — «ряд начинается с 2003-02-28; до 2007-03 мировая сумма не содержит Азии — азиатских фондов ещё не было, их запасы в данных отсутствуют (`null`), а поток приходит нулём»; для запасов/AUM — прежняя формулировка про 2007-03 (Азии нет в данных).
- **Округление источника** (иначе агент прочитает разницу с прежней историей как поломку импорта): описания рядов `holdings_tonnes` (все пять регионов) обязаны нести оговорку, что тоннажные стоки источник отдаёт округлёнными **до 0,01 т** (в отличие от потоков и AUM — полной точности), и что ручная выгрузка в фолбэке даёт полную точность, поэтому расхождение ≤ 0,01 т — не дефект.

- [ ] **Step 2: Обновить ожидания `wgc/series_meta_test.go`**

`TestEtfCatalogDescribesEverySeries` сейчас пинит «origin называет лист выгрузки» и «описание `_world` содержит 2007-03» — оба утверждения устарели:
- карта `sheets` заменяется на карту метрика → эндпоинт (`etfMetricFlowUsdMln`/`etfMetricFlowTonnes` → `etfAPIFlowsPath`; `etfMetricHoldingsTonnes`/`etfMetricAumUsdBn` → `etfAPIHoldingsPath`), и проверка требует `strings.Contains(meta.Origin, endpoint)`;
- проверка начала мировых рядов становится по-метричной: потоки (`flow_*`) обязаны содержать `2003-02`, стоки/AUM (`holdings_*`, `aum_*`) — `2007-03`.

- [ ] **Step 3: Обновить комментарий витрины**

`views/gold_etf_flows.go`, `Comment`: источник рядов `source='wgc'` — импортёр `wgc_etf_flows` (ежедневно, API `fscdn.gold.org`; фолбэк — ручная выгрузка `ETF_Flows_*.xlsx` в `wgc/data`); глубина: потоки и регионы — с 2003-02-28 (в потоковых рядах нули явные, `world`-поток с 2003-02-28), запасы/AUM `world` — с 2007-03-31. Смысл `NULL` в колонке не меняется. Логику `Select` не трогать.

- [ ] **Step 4: Расписание `dagu/wgc.yaml`**

`schedule: "17 20 * * 1"` → `schedule: "17 20 * * *"`; в комментарии шапки: ETF-шаг автоматический (сеть, без ручного шага) и поэтому ежедневный — публикация ловится в день выхода; `wgc/data` — только фолбэк; выгрузки ЦБ по-прежнему попадают в `wgc/data` вручную, поэтому при наличии файлов ежедневный прогон идемпотентен — перезаписываются те же ключи и значения (в логе каждый раз `Imported N rows of wgc_cb_reserves`), а новые месяцы появляются только после очередного ручного скачивания выгрузки; `warn + 0 строк` бывает только когда файлов нет вовсе.

- [ ] **Step 5: Run tests**

Run: `make all`
Expected: зелёный (gofmt, golangci-lint 0 issues, `go vet`, `go test -race`, обе сборки)

- [ ] **Step 6: Commit**

```bash
git add wgc/series_meta.go wgc/series_meta_test.go views/gold_etf_flows.go dagu/wgc.yaml
git commit -m "wgc etf flows: catalog, view and schedule for api source"
```

---

### Task 5: Документы и приёмка

**Files:**
- Modify: `ARCHITECTURE.md` (§3, §6.1, §6.3, §7, шапка «Последнее обновление»), `README.md`, `docs/GOLD_THESIS_DATA_ROADMAP.md`
- Reference: скил `.kimi-code/skills/sync-readme-architecture/SKILL.md` — порядок синхронизации README/ARCHITECTURE

**Interfaces:**
- Consumes: всё предыдущее.
- Produces: статус пункта в роадмапе + PR.

- [ ] **Step 1: Live acceptance — импорт из API**

Run: `make import STAT=wgc_etf_flows` (дважды)
Expected: первый прогон — `Imported 5462 rows of wgc_etf_flows (wgc ETF flows, 20 series)`, в логе `asOfDate = 2026-09-30`; второй — то же число (идемпотентно).

- [ ] **Step 2: Live acceptance — сверка с прежней историей и глубина (MCP, `kimi_reader`)**

```sql
SELECT toString(date) AS date, region, flow_usd_mln, flow_tonnes, holdings_tonnes, aum_usd_bn
FROM v_gold_etf_flows WHERE date IN ('2026-08-31','2026-09-30') ORDER BY date, region;

SELECT region, toString(min(date)) AS min_d, countIf(flow_tonnes IS NOT NULL) AS n_flow_t,
       countIf(holdings_tonnes IS NOT NULL) AS n_hold
FROM v_gold_etf_flows GROUP BY region ORDER BY region;
```

Expected: потоки и AUM за 2026-08/09 совпадают со спекой до 1e-9 (`asia 2255.40929756 / 16.18610771 / 75.594060865`),
а `holdings_tonnes` — до 0,01 т (`asia` `541.88`, `world` 2026-09-30 `4255.66`: округление источника, не дефект);
глубины: `asia/europe/other/north_america` потоков 284 (с 2003-02-28),
`world` потоков 284 (с 2003-02-28), `world` стоков 235 (с 2007-03-31).

- [ ] **Step 3: Синхронизировать документы**

По скилу `sync-readme-architecture`: `ARCHITECTURE.md` — §3 (`util/` теперь и `GetJSON`), §6.1 (строка `wgc_etf_flows`: транспорт API + файловый фолбэк, `source='wgc'`, два импортёра пакета, глубины), §6.3 (`v_gold_etf_flows`: источник рядов), §7 (расписание `wgc` — ежедневно 20:17), шапка; `README.md` — что показывает про WGC-канал. Проверить, что упоминания «скачивается браузером» про ETF больше не вводят в заблуждение (для ЦБ — остаются верными).

- [ ] **Step 4: Обновить роадмап**

`docs/GOLD_THESIS_DATA_ROADMAP.md`: в разделе фазы 3 добавить подпункт «WGC ETF Flows: автоматизация через JSON API fscdn — ✅ выполнен» с тем, что именно проверено (команда + результат из Steps 1–2), и поправить формулировку «ежемесячный ввод WGC — сессионная операция» в предыдущем разделе (для ETF это больше не так).

- [ ] **Step 5: Финальная проверка ветки**

Run: `make all && make mcp-check`
Expected: `make all` зелёный; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (34 ряда `wgc`, витрина читается, скрипт не менялся)

- [ ] **Step 6: Commit и PR**

```bash
git add ARCHITECTURE.md README.md docs/GOLD_THESIS_DATA_ROADMAP.md
git commit -m "docs: wgc etf flows api source"
```

Затем — `superpowers:finishing-a-development-branch`: один PR в конце (описание: источник, схема, `CLICKHOUSE_IMPORT_STAT=wgc_etf_flows`, пример проверки `SELECT ... FROM v_gold_etf_flows`), без промежуточных PR. Владелец смотрит и решает merge.
