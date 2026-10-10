# DCF Engine Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Дать проекту первое версионированное ядро LOM-NAV: таблицы `mine_plans` / `nav_by_asset` / `price_decks`, чистое расчётное ядро (НДПИ-функция, дисконтирование LOM с хвостом закрытия, эскалация по ИПЦ, два контура ставки) и импортёр `dcf_engine`, наполняющий `nav_by_asset` по активам на трёх price deck'ах.

**Architecture:** Новый пакет `dcf/` из трёх файлов с одной ответственностью каждый: `schema.go` — канонические DDL; `model.go` — чистые функции без БД, сети и логирования (тестируются юнит-тестами на закреплённых числах); `engine.go` — тонкая обвязка `chimport.ImportStat`, которая читает `mine_plans FINAL` и `price_decks FINAL`, зовёт ядро и пишет `nav_by_asset` батчами. `model_runs` движок не пишет: `run_id` приходит из `DCF_RUN_ID`, иначе движок печатает готовое тело `POST /v1/model_run` и не пишет ничего.

**Tech Stack:** Go (версия из `go.mod`, CI — 1.27.x), `clickhouse-go/v2`, `logrus`, стандартный `testing`. Запуск — через `make` (Makefile подключает `~/.config/rosstat/env`).

**Spec:** `docs/superpowers/specs/2026-10-10-dcf-engine-core-design.md`

## Global Constraints

- **Вставка в ClickHouse только батчами**: `PrepareBatch` → `Append` → `Send`; построчный `conn.Exec` в цикле запрещён (AGENTS.md, правило 2).
- **Каждый `Append` и каждый `strconv.Parse*` — с проверкой `err`** (правило 6).
- **Никаких сетевых вызовов в `init()`** — всё внутри `Import()` (правило 3).
- **DDL — канонические из `ARCHITECTURE.md` §6.2**, дословно; схема меняется только вместе с ARCHITECTURE.md (правило 8). Изменение схемы `nav_by_asset` (колонки `deck` и `contour`) — часть этого плана; §6.2 обновляется в Task 1, **до** кода, который ею пользуется.
- **`CREATE TABLE IF NOT EXISTS`**, движок `ReplacingMergeTree`, измерения — `LowCardinality(String)`, денежные значения — `Float64` (правило 5).
- **Секреты только через `os.Getenv`**; ни паролей, ни токенов в коде и комментариях (правило 1).
- **`make all` обязан быть зелёным** (gofmt, golangci-lint, `go vet`, `go test -race`, сборка) — те же проверки гоняет CI (правило 9).
- **Витрины**: каждая — `CREATE OR REPLACE VIEW ... DEFINER = default SQL SECURITY DEFINER`; комментарии таблицы и каждой колонки через `ALTER TABLE ... MODIFY COMMENT` / `COMMENT COLUMN` (`COMMENT ON` в ClickHouse 26.10 не работает); `GRANT SELECT` пользователю `kimi_reader` в `sql/mcp_kimi_reader.sql`; сырые таблицы агенту не выдавать (правило 11).
- **Логирование — logrus** (`log.Infof`/`log.Warnf`), без `fmt.Print` в прод-коде.
- **Команды с доступом к ClickHouse — только через `make`** (`make import STAT=...`), не через прямой `go run`.
- Новый код — в новом пакете `dcf/`; в legacy `financial/` ничего не добавлять (правило 7).

## Review Focus

Случаи, которые спека подразумевает, но тесты по формулам их не покрывают. Для каждого в задаче, владеющей кодом, добавлен тест:

1. **`mine_plans` пуст** — движок обязан вернуть `0 rows` с предупреждением, а не падать: иначе `make import STAT=dcf_engine` и DAG будут красными на пустой dev-БД, где сида ещё нет. (Task 4)
2. **`DCF_RUN_ID` не задан** — движок обязан ничего не записать и напечатать готовый JSON для ingest: молчаливая запись `model_runs` нарушила бы правило «пишет только ingest-endpoint». (Task 4)
3. **`DCF_RUN_ID` задан, но не UUID** — отказ до расчёта: ClickHouse отвергнет строку с не-UUID в колонке `UUID`, но лучше упасть до половины батча. (Task 4)
4. **Три deck'а не перетирают друг друга** — ключ `(run_id, deck, asset, contour)` обязан сохранить все строки: это и есть исправляемый дефект (потеря данных, аналогичная дефекту 3 §8.1). (Task 5)
5. **Оба контура ставки сохраняются** и дают разные NPV: локальная ставка из ОФЗ выше индустриальной 5% real USD, значит её NPV ниже. (Task 3, Task 5)

---

### Task 1: Схема — исправление `nav_by_asset` в ARCHITECTURE и DDL в коде

Первое, что делается, — правка канонического DDL: правило 8 требует обновить ARCHITECTURE.md **до** кода, который новой схемой пользуется, иначе в проекте окажутся две расходящиеся формы одной таблицы.

**Files:**
- Modify: `ARCHITECTURE.md` (§6.2 — исправить DDL `nav_by_asset`; убрать `nav_by_asset`, `mine_plans`, `price_decks` из списка «не реализованы (в коде нет)»)
- Create: `dcf/schema.go`
- Create: `dcf/engine.go`
- Modify: `main.go`
- Test: `dcf/schema_test.go`

**Interfaces:**
- Consumes: `chimport.ImportStat`, `driver.Conn`.
- Produces: `dcfEngine` (реализует `chimport.ImportStat`), `Name() == "dcf_engine"`; константы `navByAssetCreateTable`, `minePlansCreateTable`, `priceDecksCreateTable`. Task 4–5 дописывают тело `Import` в `dcf/engine.go`.

- [ ] **Step 1: Исправить DDL `nav_by_asset` в `ARCHITECTURE.md` §6.2**

Заменить блок `CREATE TABLE IF NOT EXISTS nav_by_asset (...)` на форму с колонками `deck` и `contour` в ключе (spec §3.1–3.2):

```sql
CREATE TABLE IF NOT EXISTS nav_by_asset (
    run_id UUID,                       -- связка с model_runs
    deck LowCardinality(String),       -- 'spot_flat','consensus_lt','own_scenario'
    asset LowCardinality(String),
    contour LowCardinality(String),    -- 'industrial','local' — контур ставки (spec §3.2)
    npv_usd_mln Float64,
    discount_rate Float64,             -- 0.05 real USD база + страновая/стадийная надбавка
    stage_haircut Nullable(Float64)    -- construction 0.7–0.9, DFS 0.5–0.7, PEA 0.2–0.4
) ENGINE = ReplacingMergeTree ORDER BY (run_id, deck, asset, contour);
```

Рядом — одна строка комментария о причине: ключ без `deck`/`contour` схлопывал бы три deck'а и два контура в одну строку (тихая потеря, тот же класс, что дефект 3 §8.1). В строке со списком «Не реализованы (в коде нет)» удалить `nav_by_asset`, `mine_plans`, `price_decks` — их заводит этот план.

- [ ] **Step 2: Создать `dcf/schema.go` с тремя DDL**

Копия канонических из §6.2 (после правки Step 1), дословно, как того требует правило 8:

```go
package dcf

// Канонические DDL из ARCHITECTURE.md §6.2 — копировать как есть, не менять.

const minePlansCreateTable = `CREATE TABLE IF NOT EXISTS mine_plans (
    company LowCardinality(String),
    asset LowCardinality(String),
    year UInt16,
    production_koz Float64,
    grade_gpt Nullable(Float64),
    tcc Float64, aisc Float64,
    capex_sustaining Float64, capex_project Float64,
    closure_costs Float64
) ENGINE = ReplacingMergeTree ORDER BY (company, asset, year);`

const priceDecksCreateTable = `CREATE TABLE IF NOT EXISTS price_decks (
    deck LowCardinality(String),
    year UInt16,
    gold_usd Float64,
    published Date
) ENGINE = ReplacingMergeTree ORDER BY (deck, year, published);`

const navByAssetCreateTable = `CREATE TABLE IF NOT EXISTS nav_by_asset (
    run_id UUID,
    deck LowCardinality(String),
    asset LowCardinality(String),
    contour LowCardinality(String),
    npv_usd_mln Float64,
    discount_rate Float64,
    stage_haircut Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (run_id, deck, asset, contour);`
```

- [ ] **Step 3: Написать тест, что DDL и список таблиц согласованы**

```go
func TestNavByAssetKeyKeepsDecksAndContours(t *testing.T) {
    for _, want := range []string{"deck LowCardinality(String)", "contour LowCardinality(String)",
        "ORDER BY (run_id, deck, asset, contour)"} {
        if !strings.Contains(navByAssetCreateTable, want) {
            t.Errorf("nav_by_asset DDL must contain %q: without it the second deck or contour "+
                "silently overwrites the first", want)
        }
    }
}
```

- [ ] **Step 4: Запустить тест — он должен пройти**

Run: `go test -race ./dcf/...`
Expected: PASS (`ok  github.com/kmlebedev/clickhouse-import-rosstat/dcf`).

- [ ] **Step 5: Создать скелет `dcf/engine.go` и зарегистрировать импортёр**

`Import` на этом шаге создаёт таблицы и возвращает `0, nil`; логика приходит в Task 4–5.

```go
package dcf

import (
    "context"

    "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
    "github.com/kmlebedev/clickhouse-import-rosstat/chimport"
    log "github.com/sirupsen/logrus"
)

// dcfEngine — импортёр расчётного ядра DCF (Name() — значение CLICKHOUSE_IMPORT_STAT
// и имя шага в dagu). Владеет таблицами mine_plans, price_decks, nav_by_asset;
// model_runs не пишет — это делает ingest-endpoint.
type dcfEngine struct{}

func (s *dcfEngine) Name() string { return "dcf_engine" }

func (s *dcfEngine) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
    for _, ddl := range []string{minePlansCreateTable, priceDecksCreateTable, navByAssetCreateTable} {
        if err = conn.Exec(ctx, ddl); err != nil {
            return 0, err
        }
    }
    log.Infof("dcf tables ensured; core not wired yet")
    return 0, nil
}

func init() {
    chimport.Stats = append(chimport.Stats, &dcfEngine{})
}
```

Добавить в `main.go` импорт `_ "github.com/kmlebedev/clickhouse-import-rosstat/dcf"` в блок пакетных импортов (после `craw`, до `customs` — по алфавиту).

- [ ] **Step 6: Проверить сборку, тесты и создание таблиц на ClickHouse**

Run: `make all`
Expected: gofmt, golangci-lint, `go vet`, `go test -race`, сборка — зелёные.

Run: `make import STAT=dcf_engine`
Expected: `Imported 0 rows of dcf_engine`; в БД три таблицы. Проверить через MCP ClickHouse:

```sql
SELECT name FROM system.tables WHERE database = currentDatabase()
  AND name IN ('mine_plans','price_decks','nav_by_asset') ORDER BY name
```

Expected: три строки.

- [ ] **Step 7: Commit**

```bash
git add ARCHITECTURE.md main.go dcf/
git commit -m "dcf: fix nav_by_asset key and add phase 3 tables"
```

---

### Task 2: Ядро — НДПИ-функция и эскалация по ИПЦ

Две независимые чистые функции; каждая — свой цикл «тест падает → реализация → тест проходит».

**Files:**
- Create: `dcf/model.go`
- Test: `dcf/model_test.go`

**Interfaces:**
- Consumes: ничего.
- Produces: `type Params struct { NdpiBaseUSDPerOz, NdpiSurchargePct, NdpiThresholdUSD, ProfitTaxPct, WorkingCapitalDays float64 }`, константа `defaultParams Params`; `func ndpiPerOz(gold float64, p Params) float64`; `func escalate(base float64, ipc []float64, yearIndex int) float64`.

- [ ] **Step 1: Написать падающий тест НДПИ на границах порога**

```go
func TestNdpiPerOzAroundThreshold(t *testing.T) {
    p := defaultParams // threshold 1900, surcharge 0.10, base 0 (база — допущение, в тесте явно 0)
    p.NdpiBaseUSDPerOz = 0

    cases := []struct {
        name string
        gold float64
        want float64
    }{
        {"ниже порога", 1500, 0},
        {"ровно на пороге", 1900, 0},
        {"выше порога", 4000, 210},  // 0.10 × (4000 − 1900)
        {"чуть выше порога", 1900.5, 0.05},
    }
    for _, c := range cases {
        t.Run(c.name, func(t *testing.T) {
            if got := ndpiPerOz(c.gold, p); math.Abs(got-c.want) > 1e-9 {
                t.Fatalf("ndpiPerOz(%v) = %v, want %v", c.gold, got, c.want)
            }
        })
    }
}
```

- [ ] **Step 2: Запустить — тест должен не собраться**

Run: `go test -race ./dcf/... -run TestNdpiPerOzAroundThreshold`
Expected: FAIL — `undefined: defaultParams`, `undefined: ndpiPerOz`.

- [ ] **Step 3: Реализовать `Params`, `defaultParams` и `ndpiPerOz` в `dcf/model.go`**

Формула — `НДПИ = база + surcharge × max(gold − threshold, 0)` (ARCHITECTURE §6.4). Порог и ставка — поля `Params`, а не литералы: надбавка с 2025 — налоговое число, оно изменится.

- [ ] **Step 4: Запустить — тест должен пройти**

Run: `go test -race ./dcf/... -run TestNdpiPerOzAroundThreshold`
Expected: PASS.

- [ ] **Step 5: Написать падающий тест эскалации**

```go
func TestEscalateAccumulatesIPC(t *testing.T) {
    cases := []struct {
        name      string
        ipc       []float64
        yearIndex int
        want      float64
    }{
        {"нулевой ИПЦ — без изменений", []float64{0, 0, 0}, 2, 100},
        {"год 0 — база как есть", []float64{0.10, 0.10}, 0, 100},
        {"один год роста", []float64{0.10, 0.10}, 1, 110},
        {"два года накопления", []float64{0.10, 0.10}, 2, 121},
        {"индекс за пределами ряда", []float64{0.10}, 5, 110},
    }
    for _, c := range cases {
        t.Run(c.name, func(t *testing.T) {
            if got := escalate(100, c.ipc, c.yearIndex); math.Abs(got-c.want) > 1e-9 {
                t.Fatalf("escalate = %v, want %v", got, c.want)
            }
        })
    }
}
```

- [ ] **Step 6: Запустить — тест должен не собраться**

Run: `go test -race ./dcf/... -run TestEscalateAccumulatesIPC`
Expected: FAIL — `undefined: escalate`.

- [ ] **Step 7: Реализовать `escalate` в `dcf/model.go`**

Накопление: `value = base × Π_{i<yearIndex}(1+ipc_i)`; ИПЦ за пределами ряда не применяется (индекс ≥ длины ряда — множителей больше нет).

- [ ] **Step 8: Запустить оба теста**

Run: `go test -race ./dcf/...`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add dcf/model.go dcf/model_test.go
git commit -m "dcf: add NDPI function and IPC escalation"
```

---

### Task 3: Ядро — LOM-NPV с хвостом закрытия и два контура

**Files:**
- Modify: `dcf/model.go`
- Test: `dcf/model_test.go`

**Interfaces:**
- Consumes: `ndpiPerOz`, `escalate`, `Params` (Task 2).
- Produces: `type MinePlanYear struct { Year uint16; ProductionKoz, TCC, AISC, CapexSustaining, CapexProject, ClosureCosts float64 }`; `type DeckYear struct { Year uint16; GoldUSD float64 }`; `type DiscountRates struct { Industrial, Local float64 }`; `func npvLOM(plan []MinePlanYear, deck []DeckYear, rate float64, ipc []float64, p Params) float64`.

- [ ] **Step 1: Написать падающий тест NPV на закреплённом трёхлетнем плане**

Тестовые числа посчитаны вручную и закрепляются в тесте (TDD: эталон до реализации). Возьмём план без ИПЦ (`ipc` пуст), `p.ProfitTaxPct = 0`, `p.NdpiBaseUSDPerOz = 0`, золото $4 000 (НДПИ = 0.10×(4000−1900) = 210):

```
Год 1: Prod 100 koz, AISC 1000 → FCF = 100×(4000−1000) − 100×210 − 0 − 50 capex = 300000 − 21000 − 50 = 278950 (тыс. USD) = 278.95 млн
Год 2: то же, capex 0 → 279000 − 21000 = 258000 тыс. = 258.0 млн
Год 3: то же + closure −20 млн → 279.0 − 20 = 259.0 млн
NPV(rate=0): 278.95 + 258.0 + 259.0 = 795.95 млн
```

Модуль (`ndp`/`capex` — млн, `production` — koz, AISC — USD/унц; пересчёт в млн — `/1000`):

```go
func TestNpvLOMFixedPlan(t *testing.T) {
    plan := []MinePlanYear{
        {Year: 2027, ProductionKoz: 100, AISC: 1000, CapexSustaining: 50},
        {Year: 2028, ProductionKoz: 100, AISC: 1000},
        {Year: 2029, ProductionKoz: 100, AISC: 1000, ClosureCosts: -20},
    }
    deck := []DeckYear{{2027, 4000}, {2028, 4000}, {2029, 4000}}
    p := defaultParams
    p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

    if got := npvLOM(plan, deck, 0, nil, p); math.Abs(got-795.95) > 0.01 {
        t.Fatalf("npvLOM = %v, want 795.95", got)
    }
}
```

- [ ] **Step 2: Запустить — тест должен не собраться**

Run: `go test -race ./dcf/... -run TestNpvLOMFixedPlan`
Expected: FAIL — `undefined: MinePlanYear`, `undefined: npvLOM`.

- [ ] **Step 3: Реализовать типы и `npvLOM` в `dcf/model.go`**

Формула (роадмап §4.1): `FCF_t = Prod×(gold−AISC) − НДПИ×Prod − налог − capex ± ΔWC`; хвост `closure_costs` — в последнем году и **до** налога (налогово вычитаем). Год без записи в `deck` — пропуск года с `log.Warnf` (не молчаливый ноль). Единицы: результат — USD млн.

- [ ] **Step 4: Запустить — тест должен пройти**

Run: `go test -race ./dcf/... -run TestNpvLOMFixedPlan`
Expected: PASS.

- [ ] **Step 5: Написать падающие тесты: хвост закрытия и ставка**

```go
func TestNpvLOMClosureTailReducesValue(t *testing.T) {
    plan := []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000, ClosureCosts: 0}}
    withTail := plan
    withTail[0].ClosureCosts = -20
    deck := []DeckYear{{2027, 4000}}
    p := defaultParams
    p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

    without := npvLOM(plan, deck, 0, nil, p)
    with := npvLOM(withTail, deck, 0, nil, p)
    if !(with < without) {
        t.Fatalf("хвост закрытия обязан уменьшать NPV: got %v, want < %v", with, without)
    }
}

func TestNpvLOMHigherRateLowersValue(t *testing.T) {
    plan := []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000},
        {Year: 2028, ProductionKoz: 100, AISC: 1000}}
    deck := []DeckYear{{2027, 4000}, {2028, 4000}}
    p := defaultParams
    p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

    low := npvLOM(plan, deck, 0.05, nil, p)
    high := npvLOM(plan, deck, 0.20, nil, p)
    if !(high < low) {
        t.Fatalf("npvLOM не убывает по ставке: 20%% = %v, 5%% = %v", high, low)
    }
}

func TestTwoContoursGiveDifferentNPV(t *testing.T) {
    plan := []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000},
        {Year: 2028, ProductionKoz: 100, AISC: 1000}}
    deck := []DeckYear{{2027, 4000}, {2028, 4000}}
    p := defaultParams
    p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

    rates := DiscountRates{Industrial: 0.05, Local: 0.16} // локальный контур — ОФЗ + премии
    industrial := npvLOM(plan, deck, rates.Industrial, nil, p)
    local := npvLOM(plan, deck, rates.Local, nil, p)
    if !(local < industrial) {
        t.Fatalf("локальная ставка выше индустриальной — её NPV обязан быть ниже: local %v, industrial %v",
            local, industrial)
    }
}
```

- [ ] **Step 6: Запустить — тесты должны пройти (типы `DiscountRates` добавить, если ещё нет)**

Run: `go test -race ./dcf/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add dcf/model.go dcf/model_test.go
git commit -m "dcf: add LOM NPV with closure tail and two discount contours"
```

---

### Task 4: Обвязка `Import` — чтение входов, пустой `mine_plans`, `DCF_RUN_ID`

**Files:**
- Modify: `dcf/engine.go`
- Test: `dcf/engine_test.go`

**Interfaces:**
- Consumes: `npvLOM`, `DiscountRates`, `Params` (Task 2–3); таблицы из Task 1.
- Produces: `type MinePlanRecord struct { Company string; Asset string; Years []MinePlanYear }` (один актив со своим LOM-планом); `func readMinePlans(ctx, conn) ([]MinePlanRecord, error)`; `func readDecks(ctx, conn) (map[string][]DeckYear, error)`; `func checkInputs(plans []MinePlanRecord, decks map[string][]DeckYear) error`; `func resolveRunID(raw string) (uuid.UUID, error)`; `func modelRunJSON(...) string`. Task 5 вызывает их из `Import`.

- [ ] **Step 0: Определить `MinePlanRecord` в `dcf/engine.go`**

```go
// MinePlanRecord — LOM-план одного актива: строки mine_plans, сгруппированные по активу.
// Группировка нужна потому, что NPV считается на актив, а в таблице строка — год актива.
type MinePlanRecord struct {
    Company string
    Asset   string
    Years   []MinePlanYear
}
```

- [ ] **Step 1: Написать тест `resolveRunID` (Review Focus 2 и 3)**

```go
func TestResolveRunID(t *testing.T) {
    want, _ := uuid.Parse("8f14e45f-ceea-467a-9e1c-2c3b1b1c2b1c")
    if got, err := resolveRunID("8f14e45f-ceea-467a-9e1c-2c3b1b1c2b1c"); err != nil || got != want {
        t.Fatalf("валидный UUID: got %v, err %v", got, err)
    }
    if _, err := resolveRunID(""); err == nil {
        t.Fatal("пустой DCF_RUN_ID обязан давать ошибку: молчаливая генерация run_id " +
            "отвязала бы nav_by_asset от строки model_runs")
    }
    if _, err := resolveRunID("not-a-uuid"); err == nil {
        t.Fatal("не-UUID обязан отсекаться до расчёта")
    }
}
```

- [ ] **Step 2: Запустить — тест должен не собраться**

Run: `go test -race ./dcf/... -run TestResolveRunID`
Expected: FAIL — `undefined: resolveRunID`.

- [ ] **Step 3: Реализовать `resolveRunID`**

Использовать `github.com/google/uuid` (уже в зависимостях — `ingest/model_run.go` им пользуется).

- [ ] **Step 4: Запустить — тест должен пройти**

Run: `go test -race ./dcf/... -run TestResolveRunID`
Expected: PASS.

- [ ] **Step 5: Реализовать чтение входов и пустой `mine_plans` (Review Focus 1)**

`readMinePlans` — `SELECT company, asset, year, production_koz, tcc, aisc, capex_sustaining, capex_project, closure_costs FROM mine_plans FINAL ORDER BY company, asset, year` (сканировать в `[]MinePlanRecord` с полем `Company`). `readDecks` — `SELECT deck, year, gold_usd FROM price_decks FINAL`. В `Import` после `ensureTables`:

```go
    plans, err := readMinePlans(ctx, conn)
    if err != nil {
        return 0, err
    }
    if len(plans) == 0 {
        log.Warn("mine_plans пуст: сид LOM-планов — отдельный пункт роадмапа; расчёт пропущен")
        return 0, nil
    }
    decks, err := readDecks(ctx, conn)
    if err != nil {
        return 0, err
    }
    // расчёт — Task 5; пока только проверка непустых входов
    _ = decks
```

Читать значения из `Nullable`-колонок (`grade_gpt`) в этом срезе не нужно — оно не входит в формулу; тип поля держать `Nullable(Float64)` и не сканировать.

- [ ] **Step 6: Написать тест «пустой план — не ошибка» на пустом соединении**

Тест офлайн, без ClickHouse: вынести решение в чистую функцию и проверить её.

```go
func TestPlanEmptyDoesNotFail(t *testing.T) {
    if err := checkInputs(nil, nil); err != nil {
        t.Fatalf("пустой mine_plans — не ошибка: %v", err)
    }
    if err := checkInputs([]MinePlanRecord{{Company: "PLZL", Asset: "Olimpiada"}}, nil); err != nil {
        t.Fatalf("непустой план с пустыми deck'ами не должен падать на этой проверке: %v", err)
    }
}
```

`checkInputs(plans []MinePlanRecord, decks map[string][]DeckYear) error` — чистая функция, которую `Import` вызывает перед расчётом и которая решает только «план пуст → не ошибка».

- [ ] **Step 7: Запустить — тест должен не собраться, затем пройти после реализации `checkInputs`**

Run: `go test -race ./dcf/... -run TestPlanEmptyDoesNotFail`
Expected: FAIL (`undefined: checkInputs`) → после реализации PASS.

- [ ] **Step 8: Commit**

```bash
git add dcf/engine.go dcf/engine_test.go
git commit -m "dcf: read mine_plans and decks, guard empty plan and run id"
```

---

### Task 5: Запись `nav_by_asset` батчем + `price_decks` сид

**Files:**
- Modify: `dcf/engine.go`
- Test: `dcf/engine_test.go`

**Interfaces:**
- Consumes: всё из Task 1–4.
- Produces: `func navRows(runID uuid.UUID, plans []MinePlanRecord, decks map[string][]DeckYear, rates DiscountRates, p Params) ([]NavRow, error)` — чистая функция «входы → строки `nav_by_asset`»; `type NavRow struct { RunID uuid.UUID; Deck, Asset, Contour string; NPVUSDmln, DiscountRate float64; StageHaircut *float64 }`. Тестируется без БД (Review Focus 4 и 5).

- [ ] **Step 1: Написать падающий тест: три deck'а и два контура сохраняются (Review Focus 4 и 5)**

```go
func TestNavRowsKeepsAllDecksAndContours(t *testing.T) {
    plans := []MinePlanRecord{
        {Company: "PLZL", Asset: "Olimpiada", Years: []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000}}},
        {Company: "PLZL", Asset: "Blagodatnoye", Years: []MinePlanYear{{Year: 2027, ProductionKoz: 50, AISC: 1100}}},
    }
    decks := map[string][]DeckYear{
        "spot_flat":    {{2027, 4000}},
        "consensus_lt": {{2027, 3000}},
        "own_scenario": {{2027, 4600}},
    }
    rates := DiscountRates{Industrial: 0.05, Local: 0.16}
    p := defaultParams
    p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

    rows, err := navRows(uuid.New(), plans, decks, rates, p)
    if err != nil {
        t.Fatal(err)
    }
    // 2 актива × 3 deck × 2 контура = 12 строк; ни одна пара не перетёрта
    if len(rows) != 12 {
        t.Fatalf("rows = %d, want 12: ключ (run_id, deck, asset, contour) обязан различать все комбинации", len(rows))
    }
    seen := map[string]bool{}
    for _, r := range rows {
        key := r.Deck + "|" + r.Asset + "|" + r.Contour
        if seen[key] {
            t.Fatalf("дубль ключа %s — deck/contour перетираются", key)
        }
        seen[key] = true
    }
    // дешёвый deck даёт меньший NPV: consensus_lt $3 000 против spot_flat $4 000
    byKey := map[string]float64{}
    for _, r := range rows {
        byKey[r.Deck+"|"+r.Asset+"|"+r.Contour] = r.NPVUSDmln
    }
    if !(byKey["consensus_lt|Olimpiada|industrial"] < byKey["spot_flat|Olimpiada|industrial"]) {
        t.Fatal("NPV на consensus_lt обязан быть ниже, чем на spot_flat")
    }
    if !(byKey["spot_flat|Olimpiada|local"] < byKey["spot_flat|Olimpiada|industrial"]) {
        t.Fatal("локальный контур (ставка выше) обязан давать NPV ниже индустриального")
    }
}
```

- [ ] **Step 2: Запустить — тест должен не собраться**

Run: `go test -race ./dcf/... -run TestNavRowsKeepsAllDecksAndContours`
Expected: FAIL — `undefined: navRows`, `undefined: NavRow`.

- [ ] **Step 3: Реализовать `NavRow` и `navRows` в `dcf/engine.go`**

Для каждого актива, каждого deck'а и каждого контура — `npvLOM`; контуры фиксированы: `ContourIndustrial = "industrial"`, `ContourLocal = "local"`. `StageHaircut` — `nil` (стадийные haircut'ы вне среза).

- [ ] **Step 4: Запустить — тест должен пройти**

Run: `go test -race ./dcf/... -run TestNavRowsKeepsAllDecksAndContours`
Expected: PASS.

- [ ] **Step 5: Дописать `Import`: батч, `DCF_RUN_ID`, лог JSON (Review Focus 2)**

```go
    raw := os.Getenv("DCF_RUN_ID")
    if raw == "" {
        log.Info(modelRunJSON(plans, decks, rates, p))
        log.Warn("DCF_RUN_ID не задан: nav_by_asset не записан; " +
            "запись model_runs — только через ingest-endpoint (POST /v1/model_run)")
        return 0, nil
    }
    runID, err := resolveRunID(raw)
    if err != nil {
        return 0, err
    }
    rows, err := navRows(runID, plans, decks, rates, p)
    if err != nil {
        return 0, err
    }
    batch, err := conn.PrepareBatch(ctx, "INSERT INTO nav_by_asset")
    if err != nil {
        return 0, err
    }
    for _, r := range rows {
        if err = batch.Append(r.RunID, r.Deck, r.Asset, r.Contour, r.NPVUSDmln, r.DiscountRate, r.StageHaircut); err != nil {
            _ = batch.Abort()
            return 0, err
        }
    }
    if err = batch.Send(); err != nil {
        return 0, err
    }
    return int64(len(rows)), nil
```

`modelRunJSON` — чистая функция, формирующая тело `POST /v1/model_run` по контракту `ingest.ModelRun` (см. `ingest/model_run.go`): `trigger_type: "manual"`, `price_deck` по каждому deck'у, `probabilities` нормированы к 100, `gold_scenario` с `horizon`. `rates` и `p` (Params) — значения по умолчанию, задокументированные константами; они же попадают в JSON, чтобы ingest записал те же допущения в `model_runs`.

- [ ] **Step 6: Реализовать сид `price_decks` (только если таблица пуста)**

Если `readDecks` вернул пустую карту — засеять тремя deck'ами батчем: `spot_flat` — последняя цена из `gold_prices` (`SELECT argMax(usd, date) FROM gold_prices FINAL WHERE venue='moex_fix_usd'`), `consensus_lt` — $3 000, `own_scenario` — $4 600 (LT-оценки §13.4; `year` — текущий год + горизонт). Непустую таблицу не трогать.

- [ ] **Step 7: Проверить на ClickHouse: пустой план, затем фикстура**

Run: `make import STAT=dcf_engine`
Expected: `Imported 0 rows of dcf_engine` + `Warn ... mine_plans пуст` (Review Focus 1).

Затем вставить фикстуру из двух-трёх активов (SQL через MCP, временно, только для проверки) и повторить:

Run: `DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine`
Expected: `Imported N rows of dcf_engine`, где `N = активы × 3 × 2`. Проверить через MCP:

```sql
SELECT deck, contour, count() FROM nav_by_asset FINAL GROUP BY deck, contour ORDER BY deck, contour
```

Expected: шесть строк (3 deck × 2 контура).

- [ ] **Step 8: Commit**

```bash
git add dcf/engine.go dcf/engine_test.go
git commit -m "dcf: write nav_by_asset batched and seed price decks"
```

---

### Task 6: Витрина `v_dcf_assumptions` с комментариями и грантом

**Files:**
- Create: `views/dcf_assumptions.go`
- Modify: `views/gold_views.go` (добавить витрину в список `goldViews.Import`)
- Modify: `sql/mcp_kimi_reader.sql`
- Test: `views/dcf_assumptions_test.go`

**Interfaces:**
- Consumes: `nav_by_asset` (Task 1, 5); `util.View`, `util.CreateView`.
- Produces: `var dcfAssumptionsView util.View`; `GRANT SELECT ON default.v_dcf_assumptions TO kimi_reader`.

- [ ] **Step 1: Создать `views/dcf_assumptions.go` по образцу `views/forecast_accuracy.go`**

`Tables: []string{"nav_by_asset"}`, а `Select` — из spec §3.5: строка на (deck, contour, asset) плюс
`sum(npv_usd_mln) OVER (PARTITION BY run_id, deck, contour) AS nav_total_usd_mln`. Комментарий таблицы
и **каждой** колонки: что такое `contour` (`industrial` — 5% real USD + надбавки; `local` — ОФЗ + премии),
почему три deck'а и почему `stage_haircut` может быть NULL.

- [ ] **Step 2: Написать тест, что каждая колонка витрины прокомментирована**

Колонки берутся из самого `Select`, а не из списка в тесте: список в тесте пропустил бы колонку,
добавленную в `Select` позже. Тот же приём, что в `views/company_test.go`
(`TestCompanyViewColumnsAreCommented`).

```go
func TestDcfAssumptionsViewDocumentsEveryColumn(t *testing.T) {
    v := dcfAssumptionsView
    for _, column := range viewColumnsFromSelect(t, v.Select) {
        if strings.TrimSpace(v.Columns[column]) == "" {
            t.Errorf("колонка %s не описана: агент не поймёт её через MCP", column)
        }
    }
}
```

`viewColumnsFromSelect` — маленький хелпер теста: разбирает список колонок верхнего уровня из `Select`
(имена до верхнеуровневого `FROM`), отбрасывая алиасы (`AS`). Если такого хелпера в `views/` нет —
завести его в `views/dcf_assumptions_test.go` рядом с тестом.

- [ ] **Step 3: Запустить — тест должен пройти**

Run: `go test -race ./views/...`
Expected: PASS.

- [ ] **Step 4: Зарегистрировать витрину в импортёре `gold_views`**

Добавить `dcfAssumptionsView` в срез в `views/gold_views.go` (рядом с `forecastAccuracyView`).

- [ ] **Step 5: Добавить грант в `sql/mcp_kimi_reader.sql`**

Строка `GRANT SELECT ON default.v_dcf_assumptions TO kimi_reader;` — рядом с остальными `v_*`.

- [ ] **Step 6: Проверить через MCP (правило 11г)**

Run: `make import STAT=gold_views`
Expected: `View v_dcf_assumptions created: true`.

Run: `make mcp-user` (выдать грант), затем `make mcp-check`
Expected: ПРОВЕРКА ПРОЙДЕНА. Через MCP: `list_tables` видит `v_dcf_assumptions`; `run_query` к ней
читается; `SELECT count() FROM nav_by_asset` → `ACCESS_DENIED`.

- [ ] **Step 7: Commit**

```bash
git add views/dcf_assumptions.go views/dcf_assumptions_test.go views/gold_views.go sql/mcp_kimi_reader.sql
git commit -m "views: add v_dcf_assumptions with comments and grant"
```

---

### Task 7: Документация и приёмка

**Files:**
- Modify: `ARCHITECTURE.md` (§6.1 — пакет `dcf/`; §6.2 — снять три таблицы из «не реализованы»; §6.3 — витрина; §6.4 — ссылка на реализацию)
- Modify: `README.md` (структура репозитория; `make import STAT=dcf_engine`)
- Modify: `docs/ROADMAP_DCF_POLYUS.md` (§7 фаза 3 — отметить шаг 1 ✅ с командой и результатом; перечислить остаток)
- Modify: `docs/superpowers/specs/2026-10-10-dcf-engine-core-design.md` (статус: реализовано)

**Interfaces:**
- Consumes: всё выше.
- Produces: документация, синхронная с кодом.

- [ ] **Step 1: Обновить `ARCHITECTURE.md`**

§6.1: добавить строку пакета `dcf/` (что заводит, что читает). §6.2: убедиться, что три таблицы больше не в списке «не реализованы». §6.3: описать `v_dcf_assumptions`. §6.4: добавить ссылку «реализация — `dcf/`».

- [ ] **Step 2: Обновить `README.md`**

Структура репозитория: `dcf/` — расчётное ядро. В разделе проверок — пример `DCF_RUN_ID=<uuid> make import STAT=dcf_engine`.

- [ ] **Step 3: Обновить `docs/ROADMAP_DCF_POLYUS.md`**

§7, фаза 3: пометить выполненным шаг «таблицы + ядро LOM-NAV» с точной командой и результатом прогона; явно перечислить, что осталось (сид `mine_plans`, `reserves_*`/peers/haircut'ы, мост NAV → цена, NAV-матрица в Grafana).

- [ ] **Step 4: Прогнать обязательные проверки**

Run: `make all`
Expected: зелёный.

Run: `DCF_RUN_ID=<uuid> make import STAT=dcf_engine`
Expected: `Imported N rows of dcf_engine`, `N = активы × 3 × 2`.

Run: `make mcp-check`
Expected: ПРОВЕРКА ПРОЙДЕНА.

- [ ] **Step 5: Commit**

```bash
git add ARCHITECTURE.md README.md docs/ROADMAP_DCF_POLYUS.md docs/superpowers/specs/2026-10-10-dcf-engine-core-design.md
git commit -m "docs: sync DCF engine core step 1"
```

---

## Self-Review

**1. Покрытие спеки.** §3 (пакет `dcf/`, три файла) — Task 1; §3.1–3.2 (`nav_by_asset` с `deck` и `contour`) — Task 1 + тест Task 1, Task 5; §3.3 (ядро: `ndpiPerOz`, `npvLOM`, `escalate`, `Params`, `DiscountRates`) — Task 2–3; §3.4 (`Import`, пустой план, `DCF_RUN_ID`, сид deck'ов) — Task 4–5; §3.5 (витрина, комментарии, грант) — Task 6; §4 (ошибки) — Task 4 (`resolveRunID`, `checkInputs`), Task 5 (пропуск битого года в `npvLOM`); §5 (тесты) — Task 2–3, 5; §6 (приёмка) — Task 7; §7 (риски) — покрыты ограничениями и Review Focus. Пробелов нет.

**2. Скан шагов.** Каждый шаг — одно действие с проверяемым результатом. Кода в плане — только сигнатуры, тела тестов и DDL (то, что спека фиксирует дословно); тела функций не переписаны.

**3. Согласованность типов.** `MinePlanYear`, `DeckYear`, `Params`, `DiscountRates`, `navRows`/`NavRow`, `resolveRunID`, `checkInputs`, `readMinePlans`/`readDecks` названы одинаково во всех задачах, где упоминаются. `MinePlanRecord` (с `Company`, `Asset`, `Years`) вводится в Task 4 и используется в Task 5 — определение должно появиться в Task 4.

**4. Review Focus.** Пять пунктов, каждый привязан к шагу владеющей задачи: (1) пустой план — Task 4 Step 6; (2) `DCF_RUN_ID` не задан — Task 5 Step 5; (3) не-UUID — Task 4 Step 1; (4) три deck'а и два контура не перетираются — Task 5 Step 1; (5) контуры дают разные NPV — Task 3 Step 5 и Task 5 Step 1.

**5. Пропорция.** План сопоставим по длине со спекой; кодовых блоков — сигнатуры, DDL и тела тестов.
