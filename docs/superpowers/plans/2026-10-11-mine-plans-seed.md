# Сид `mine_plans` + кривая годов в `price_decks` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Наполнить `mine_plans` реальными LOM-планами активов Полюса и расширить сид `price_decks` на годы плана, чтобы `dcf_engine` считал многолетний NAV вместо нулевого.

**Architecture:** Расширяем существующий сид деков (`dcf/seed.go`) с одного текущего года на диапазон лет плана, добавляем чистый гард пересечения годов, и заводим новый импортёр `dcf_mine_plans` (пакет `dcf/`), который пишет `mine_plans` из версионированных констант + факта `databook_polyus`. Плюс отдельный документ `docs/DCF_DATA_COVERAGE.md` с картой полноты данных.

**Tech Stack:** Go 1.x, ClickHouse (clickhouse-go v2), logrus, `chimport.ImportStat`, `make` (Makefile подключает `~/.config/rosstat/env`).

**Spec:** `/Users/whitefox/.kimi-code/sessions/wd_clickhouse-import-rosstat_5f06aab4e102/session_1a0d7531-c066-4eb1-8f1d-c8ce6bc56245/agents/main/plans/thunder-luke-cage-speed.md` (утверждённый дизайн). Канонические DDL и конвенции — `ARCHITECTURE.md` §6.1/§6.2/§6.4 (источник истины), здесь не дублируются.

## Global Constraints

- Секреты только через `os.Getenv`; не печатать пароли/токены (AGENTS.md §«Жёсткие правила» 1).
- Вставка в ClickHouse только батчами: `PrepareBatch` → `Append` (err checked) → `Send`; построчный `Exec` в цикле запрещён (правило 2).
- DDL всегда `CREATE TABLE IF NOT EXISTS`, движок `ReplacingMergeTree`, измерения `LowCardinality(String)`, денежные значения `Float64` (правило 5).
- Каждый `strconv.Parse*` и `batch.Append` — с проверкой `err` (правило 6).
- Новый код — по шаблонам ARCHITECTURE.md §3, предпочтительно `util.ClickHouseImport`; пакет `financial/` — legacy, новый код туда не добавлять (правило 7).
- Логирование — logrus (`log.Infof`/`log.Warnf`), без `fmt.Print` в прод-коде (AGENTS.md §«Стиль коммитов»).
- Все команды запускать через `make` (Makefile сам подключает env); `make ch-up`/`make ch-down` не запускать.
- Единицы `mine_plans` (контракт `MinePlanYear`, `dcf/model.go:66-79`): `production_koz` — тыс. унций; `tcc`/`aisc` — USD/унц; `capex_sustaining`/`capex_project`/`closure_costs` — млн USD.
- Имена активов в `mine_plans` — те же строки, что в `databook_polyus`: `OLIMPIADA`, `BLAGODATNOYE`, `VERNINSKOYE2`, `NATALKA`, `KURANAKH`, `TITIMUKHTA`, `ZAPADNOYE`, `Sukhoi Log`.
- Коммиты — кратко, по-английски, в духе истории (`add fred importer`, `fix batch insert cbr_queries`).

## Review Focus

Пять классов входа/отказа, которые спека подразумевает, но чьи тесты распределены по задачам ниже:

1. **Год плана без цены дека** — читатель ожидает, что NAV посчитан на весь план; при отсутствии цены `npvLOM` молча пропускает год (`dcf/model.go:137-142`), и нулевой NPV неотличим от «актива нет». Гард обязан сделать пропуск видимым.
2. **Пустая `mine_plans`** — читатель ожидает предупреждение и `0 rows`, а не падение: `make import STAT=dcf_engine` и DAG не должны краснеть на пустой dev-БД.
3. **Ноль вместо отсутствующего факта** — если у актива нет факта за год, запись `production_koz = 0` даст тихо заниженный NAV; год обязан быть пропущен с warn.
4. **Смешение компанейской и per-asset метрики** — `gold_output` (релиз, весь Полюс) и `Total Dore gold output` (датапак, месторождение) — разные величины; сложение даёт молча неверный вход.
5. **Verninskoye как непрерывный ряд** — карьер законсервирован до 2028; непрерывная добыча завышает NAV актива.

---

### Task 1: Горизонт и кривая годов в сиде `price_decks`

**Files:**
- Modify: `dcf/seed.go`
- Modify: `dcf/engine.go:354-365` (вызов сида с горизонтом)
- Test: `dcf/seed_test.go`

**Interfaces:**
- Consumes: `MinePlanRecord` (`dcf/engine.go:24-28`, поля `Company`, `Asset`, `Years []MinePlanYear`), `MinePlanYear.Year uint16` (`dcf/model.go:71-79`), `seedPriceDecks` (`dcf/seed.go:78`), `deckSpotFlat`/`deckConsensusLT`/`deckOwnScenario` (`dcf/seed.go:16-18`), `assertSpotPrice` (`dcf/seed.go:48`).
- Produces: `planYears(plans []MinePlanRecord) []uint16` — отсортированный уникальный список годов планов; `seedPriceDecks(ctx context.Context, conn driver.Conn, years []uint16) error`.

**Почему меняем сигнатуру:** сид сейчас берёт год из часов (`dcf/seed.go:93`). Горизонт обязан приходить из планов — иначе годы плана и годы дека разъедутся, и это ровно дефект, ради которого задача делается. Константа `priceDeckSeedYearOffset` (`dcf/seed.go:29-35`) удаляется вместе с комментарием, который её оправдывал: он объяснял выбор «ноль», а теперь год задаётся не смещением.

- [ ] **Step 1: Write the failing test**

В `dcf/seed_test.go` добавить:

```go
// TestPlanYears — горизонт сида берётся из планов, а не из часов.
func TestPlanYears(t *testing.T) {
    plans := []MinePlanRecord{
        {Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2030}, {Year: 2028}}},
        {Company: "PLZL", Asset: "B", Years: []MinePlanYear{{Year: 2029}, {Year: 2028}}},
    }
    got := planYears(plans)
    want := []uint16{2028, 2029, 2030}
    if len(got) != len(want) {
        t.Fatalf("planYears = %v, want %v (уникальные годы, отсортированы)", got, want)
    }
    for i := range want {
        if got[i] != want[i] {
            t.Fatalf("planYears[%d] = %d, want %d", i, got[i], want[i])
        }
    }
    if got := planYears(nil); len(got) != 0 {
        t.Fatalf("пустой план: планYears = %v, want пусто", got)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dcf/ -run TestPlanYears -v`
Expected: FAIL — `undefined: planYears`.

- [ ] **Step 3: Implement `planYears` in `dcf/seed.go`**

Сигнатура: `func planYears(plans []MinePlanRecord) []uint16`. Собрать годы в `map[uint16]struct{}`, вернуть отсортированный срез. Комментарий — почему множество: два актива могут делить год, а дек обязан иметь ровно одну строку на год.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./dcf/ -run TestPlanYears -v`
Expected: PASS.

- [ ] **Step 5: Изменить `seedPriceDecks` на горизонт**

Заменить `year := uint16(time.Now().UTC().Year()) + priceDeckSeedYearOffset` (строка 93) на цикл по переданным `years`; на каждый `year` писать все три дека (три `batch.Append` на год, по одному на дек). Удалить константу `priceDeckSeedYearOffset` и её комментарий. Обновить doc-комментарий функции: год(ы) — из плана, а не из часов; следствие — `price_decks` покрывает ровно годы, на которых считается NAV.

- [ ] **Step 6: Обновить вызов в `Import`**

В `dcf/engine.go:357-360` передать горизонт: `seedPriceDecks(ctx, conn, planYears(plans))`. Планы к этому месту уже прочитаны (строка 337), порядок вызовов менять не нужно.

- [ ] **Step 7: Run the package tests**

Run: `go test ./dcf/ -v`
Expected: PASS, включая `TestPlanEmptyDoesNotFail` и `TestAssertSpotPriceRejectsNonPositive`.

- [ ] **Step 8: Commit**

```bash
git add dcf/seed.go dcf/engine.go dcf/seed_test.go
git commit -m "dcf: seed price decks across plan years"
```

---

### Task 2: Гард пересечения годов плана и дека

**Files:**
- Create: `dcf/coverage.go`
- Modify: `dcf/engine.go` (вызов гарда после сида, перед `checkInputs`)
- Test: `dcf/coverage_test.go`

**Interfaces:**
- Consumes: `MinePlanRecord`, `MinePlanYear.Year`, `DeckYear.Year` (`dcf/model.go:84-87`), `map[string][]DeckYear`.
- Produces: `uncoveredPlanYears(plans []MinePlanRecord, decks map[string][]DeckYear) []string` — строки вида `"ASSET:год"`, отсортированные; только те годы плана, у которых нет цены **ни в одном** деке. `checkYearCoverage(plans []MinePlanRecord, decks map[string][]DeckYear) error` — `log.Warnf` с перечнем, если непокрытые есть, но покрыт хотя бы один год; ошибка, если **ни один** год ни одного плана не покрыт.

**Review Focus 1** — этот тест закрывает молчаливый пропуск года.

- [ ] **Step 1: Write the failing test**

```go
// TestUncoveredPlanYears — гард делает пропуск года видимым.
func TestUncoveredPlanYears(t *testing.T) {
    plans := []MinePlanRecord{
        {Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2026}, {Year: 2027}}},
    }
    decks := map[string][]DeckYear{"spot_flat": {{Year: 2026, GoldUSD: 4000}}}

    got := uncoveredPlanYears(plans, decks)
    if len(got) != 1 || got[0] != "A:2027" {
        t.Fatalf("uncoveredPlanYears = %v, want [A:2027]", got)
    }

    full := map[string][]DeckYear{"spot_flat": {{2026, 4000}, {2027, 4100}}}
    if got := uncoveredPlanYears(plans, full); len(got) != 0 {
        t.Fatalf("полное покрытие: %v, want пусто", got)
    }
}

// TestCheckYearCoverage — полное отсутствие пересечения это ошибка, частичное — warn.
func TestCheckYearCoverage(t *testing.T) {
    plans := []MinePlanRecord{{Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2030}}}}

    if err := checkYearCoverage(plans, map[string][]DeckYear{"spot_flat": {{Year: 2026}}}); err == nil {
        t.Fatal("ни один год плана не покрыт: обязана быть ошибка, иначе непустые данные " +
            "дадут нулевой NAV с успешным логом")
    }
    if err := checkYearCoverage(plans, map[string][]DeckYear{"spot_flat": {{Year: 2030}}}); err != nil {
        t.Fatalf("покрытый год не должен быть ошибкой: %v", err)
    }
    if err := checkYearCoverage(nil, nil); err != nil {
        t.Fatalf("пустой план — не ошибка (обрабатывается раньше в Import): %v", err)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dcf/ -run 'TestUncoveredPlanYears|TestCheckYearCoverage' -v`
Expected: FAIL — `undefined: uncoveredPlanYears`.

- [ ] **Step 3: Implement `dcf/coverage.go`**

`uncoveredPlanYears`: собрать множество годов по **всем** декам (`for _, years := range decks { for _, d := range years { ... } }`), затем для каждого года каждого плана, отсутствующего в множестве, добавить `asset + ":" + strconv.Itoa(int(year))`; отсортировать `sort.Strings`.

`checkYearCoverage`: если `len(plans) == 0` → `nil`. Если непокрытых нет → `nil`. Если покрытых годов нет вовсе (непокрытых столько же, сколько всего годов плана) → `fmt.Errorf` с текстом, называющим годы плана и годы деков. Иначе → `log.Warnf` с перечнем и `nil`.

Файл начинается с комментария: почему гард вообще нужен (ссылка на `dcf/model.go:137-142` — год без цены пропускается молча), и почему warning, а не всегда error (политика warn-and-continue, `dcf/engine.go:96`).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./dcf/ -run 'TestUncoveredPlanYears|TestCheckYearCoverage' -v`
Expected: PASS.

- [ ] **Step 5: Подключить гард в `Import`**

В `dcf/engine.go` после блока сида (после строки 365) и **перед** `checkInputs` (строка 367):

```go
if err = checkYearCoverage(plans, decks); err != nil {
    return 0, err
}
```

- [ ] **Step 6: Run the package tests**

Run: `go test ./dcf/ -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add dcf/coverage.go dcf/coverage_test.go dcf/engine.go
git commit -m "dcf: guard plan years against price deck coverage"
```

---

### Task 3: Таблица констант плана — активы, горизонты, TCC, capex

**Files:**
- Create: `dcf/mine_plans_seed.go`
- Test: `dcf/mine_plans_seed_test.go`

**Interfaces:**
- Consumes: ничего из предыдущих задач.
- Produces: `type assetPlanSeed struct { Company, Asset string; MineLifeYears int; BaseYear uint16; TCC, CapexSustaining, ClosureCosts, ProjectCapex float64; ProfileStartYear uint16; ProductionProfile []float64; HaltFrom, HaltTo uint16 }`; `var polyusAssetPlans = []assetPlanSeed{...}`; `const sustainingWedgeUSDPerOz = 698.0`.

**Значения — из источников, каждый в комментарии со страницей:**

| Актив | База (2025), koz | Срок службы (MOPs) | TCC 2025, $/oz | Capex 2025, $M |
|---|---|---|---|---|
| OLIMPIADA | 926.5 | 10 | 773 | 450 |
| BLAGODATNOYE | 434.0 | 13 | 691 | 370 |
| NATALKA | 567.4 | 24 | 671 | 198 |
| VERNINSKOYE2 | 271.7 | 15 | 735 | 125 |
| KURANAKH | 329.7 | 15 | 925 | 362 |
| TITIMUKHTA | из датапака | 13 (Krasnoyarsk BU) | 773 (Krasnoyarsk BU, прокси) | — (внутри Krasnoyarsk BU) |
| ZAPADNOYE | из датапака | 15 (Aldan BU) | 671 (Natalka/Aldan BU, прокси) | — |
| Sukhoi Log | 2 300–2 800 (профиль) | профиль от `ProfileStartYear` | 735 (прокси Иркутского БУ) | 6 000 (project) |

Источники: Годовой обзор ПАО «Полюс» за 2025 (`https://polyus.com/upload/iblock/d6b/godovoy_obzor_pao_polyus_za_2025_god.pdf`) — стр. 5/31 (TCC по активам), стр. «Структура капитальных затрат» (capex по BU), стр. ~8571 (сроки службы MOPs: Олимпиада 10, Благодатное 13, Вернинское 15, Куранахское рудное поле 15, Наталка 24). Дек «Ключевые проекты роста», декабрь 2024 (`https://polyus.com/upload/iblock/f13/klyuchevye_proekty_rosta.pdf`) — слайды 16, 23, 32, 34 (Сухой Лог, Чульбаткан, Чертово Корыто). Групповые TCC $739/oz и AISC $1 437/oz за 2025 — там же, определения TCC/AISC.

**Версионирование:** в комментарии к `polyusAssetPlans` указать год отчёта и что при выходе нового обзора значения пересматриваются отдельным пунктом роадмапа, а не молча.

- [ ] **Step 1: Write the failing test**

```go
// TestPolyusAssetPlansCoverage — состав сида: 7 рудников + Сухой Лог, горизонты
// совпадают с публичными сроками службы MOPs, версии источника названы.
func TestPolyusAssetPlansCoverage(t *testing.T) {
    want := map[string]int{
        "OLIMPIADA": 10, "BLAGODATNOYE": 13, "NATALKA": 24,
        "VERNINSKOYE2": 15, "KURANAKH": 15,
    }
    got := map[string]int{}
    for _, p := range polyusAssetPlans {
        if p.Company != "PLZL" {
            t.Errorf("%s: Company = %q, want PLZL", p.Asset, p.Company)
        }
        if p.MineLifeYears <= 0 {
            t.Errorf("%s: MineLifeYears = %d, want > 0", p.Asset, p.MineLifeYears)
        }
        got[p.Asset] = p.MineLifeYears
    }
    for asset, years := range want {
        if got[asset] != years {
            t.Errorf("%s: MineLifeYears = %d, want %d (Годовой обзор 2025, сроки MOPs)", asset, got[asset], years)
        }
    }
    if len(polyusAssetPlans) != 8 {
        t.Fatalf("состав сида = %d активов, want 8 (7 рудников + Сухой Лог)", len(polyusAssetPlans))
    }
}

// TestSustainingWedgeFromReporting — клин AISC−TCC берётся из отчёта 2025,
// а не из устаревшей версии 2024 (там было 384).
func TestSustainingWedgeFromReporting(t *testing.T) {
    if sustainingWedgeUSDPerOz != 698.0 {
        t.Fatalf("sustainingWedgeUSDPerOz = %v, want 698 (AISC 1437 − TCC 739, Годовой обзор 2025)",
            sustainingWedgeUSDPerOz)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dcf/ -run 'TestPolyusAssetPlansCoverage|TestSustainingWedgeFromReporting' -v`
Expected: FAIL — `undefined: polyusAssetPlans`.

- [ ] **Step 3: Implement `dcf/mine_plans_seed.go`**

Объявить структуру и срез с точными значениями таблицы выше. На каждый актив — комментарий с источником и страницей. Для `TITIMUKHTA`/`ZAPADNOYE` явно пометить, что TCC — прокси бизнес-юнита. Для `Sukhoi Log` заполнить `ProductionProfile` значениями 10-летнего среднего 2,3–2,8 млн унц (в koz: 2300–2800), `ProfileStartYear = 2028`, `CapexSustaining = 0`, `ProjectCapex = 6000`. `VERNINSKOYE2`: `HaltFrom = 2024`, `HaltTo = 2028`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./dcf/ -run 'TestPolyusAssetPlansCoverage|TestSustainingWedgeFromReporting' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dcf/mine_plans_seed.go dcf/mine_plans_seed_test.go
git commit -m "dcf: add polyus asset plan seed constants"
```

---

### Task 4: Построение строк `mine_plans` из констант и факта

**Files:**
- Create: `dcf/mine_plans_build.go`
- Test: `dcf/mine_plans_build_test.go`

**Interfaces:**
- Consumes: `assetPlanSeed`, `polyusAssetPlans`, `sustainingWedgeUSDPerOz` (Task 3); `MinePlanYear` (`dcf/model.go:71-79`).
- Produces: `buildPlanYears(seed assetPlanSeed, baseProductionKoz float64) []MinePlanYear` — чистый построитель строк одного актива.

**Правила (из спеки §«Шаг C»):**

- Годы: от `BaseYear` (2025) до `BaseYear + MineLifeYears - 1` включительно.
- `ProductionKoz`: база (последний факт) — плато на весь срок.
- `AISC`: `TCC + sustainingWedgeUSDPerOz` (derived).
- `CapexSustaining`: `seed.CapexSustaining` в **каждом** году (split sustaining/project не публикуется — вся сумма в sustaining, `CapexProject = 0`).
- `ClosureCosts`: отрицательный хвост **только в последнем году** (как в приёмке `dcf_engine`, `docs/ROADMAP_DCF_POLYUS.md:190-192`).
- `VERNINSKOYE2`: годы в `[HaltFrom, HaltTo]` — `ProductionKoz = 0` (карьер законсервирован), но **строка присутствует**: пропуск года сдвинул бы `yearIndex` иначе, чем реальность.
- `Sukhoi Log`: годы `ProfileStartYear … ProfileStartYear + len(ProfileStartYear) - 1`, `ProductionKoz` из профиля; годы до старта не пишутся (актив ещё не производит).

**Review Focus 3** — отсутствие факта не превращается в ноль: если `base <= 0`, `buildPlanYears` возвращает `nil`, а вызывающий печатает warn и строки не пишет.

**Review Focus 5** — тест на консервацию Вернинского.

- [ ] **Step 1: Write the failing test**

```go
// TestBuildPlanYearsHorizon — число строк равно сроку службы, хвост закрытия
// только в последнем году, AISC = TCC + клин.
func TestBuildPlanYearsHorizon(t *testing.T) {
    seed := assetPlanSeed{
        Company: "PLZL", Asset: "OLIMPIADA", BaseYear: 2025, MineLifeYears: 10,
        TCC: 773, CapexSustaining: 450, ClosureCosts: -50,
    }
    years := buildPlanYears(seed, 926.5)
    if len(years) != 10 {
        t.Fatalf("len = %d, want 10 (срок службы MOPs)", len(years))
    }
    if years[0].Year != 2025 || years[9].Year != 2034 {
        t.Fatalf("годы %d..%d, want 2025..2034", years[0].Year, years[9].Year)
    }
    if years[0].AISC != 773+698.0 {
        t.Fatalf("AISC = %v, want %v (TCC + клин 2025)", years[0].AISC, 773+698.0)
    }
    for i, y := range years[:9] {
        if y.ClosureCosts != 0 {
            t.Errorf("год %d: ClosureCosts = %v, want 0 (хвост только в последнем)", i, y.ClosureCosts)
        }
    }
    if years[9].ClosureCosts != -50 {
        t.Fatalf("последний год: ClosureCosts = %v, want -50", years[9].ClosureCosts)
    }
}

// TestBuildPlanYearsHalt — консервация Вернинского даёт нулевую добычу,
// но строка остаётся: пропуск сдвинул бы нумерацию лет плана.
func TestBuildPlanYearsHalt(t *testing.T) {
    seed := assetPlanSeed{
        Company: "PLZL", Asset: "VERNINSKOYE2", BaseYear: 2025, MineLifeYears: 15,
        TCC: 735, HaltFrom: 2025, HaltTo: 2028,
    }
    years := buildPlanYears(seed, 271.7)
    if len(years) != 15 {
        t.Fatalf("len = %d, want 15: консервация не сокращает план", len(years))
    }
    for _, y := range years {
        if y.Year >= 2025 && y.Year <= 2028 && y.ProductionKoz != 0 {
            t.Errorf("год %d: ProductionKoz = %v, want 0 (карьер законсервирован)", y.Year, y.ProductionKoz)
        }
        if y.Year == 2029 && y.ProductionKoz == 0 {
            t.Errorf("год 2029: добыча должна возобновиться")
        }
    }
}

// TestBuildPlanYearsNoFact — отсутствие факта не превращается в ноль.
func TestBuildPlanYearsNoFact(t *testing.T) {
    seed := assetPlanSeed{Company: "PLZL", Asset: "X", BaseYear: 2025, MineLifeYears: 5, TCC: 700}
    if got := buildPlanYears(seed, 0); got != nil {
        t.Fatalf("нулевой факт: got %v, want nil (строки не пишем)", got)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dcf/ -run 'TestBuildPlanYears' -v`
Expected: FAIL — `undefined: buildPlanYears`.

- [ ] **Step 3: Implement `buildPlanYears` in `dcf/mine_plans_build.go`**

Сигнатура: `func buildPlanYears(seed assetPlanSeed, baseProductionKoz float64) []MinePlanYear`. Если `baseProductionKoz <= 0` → `nil`. Для Сухого Лога источник баз — `seed.ProductionProfile`, а не `base` (реализовать отдельной ветвью по непустому профилю). Комментарий — почему консервация пишется нулём, а не пропуском (нумерация `yearIndex`).
- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./dcf/ -run 'TestBuildPlanYears' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dcf/mine_plans_build.go dcf/mine_plans_build_test.go
git commit -m "dcf: build mine plan years from seed and actuals"
```

---

### Task 5: Импортёр `dcf_mine_plans` — чтение факта и запись

**Files:**
- Create: `dcf/mine_plans_import.go`
- Test: `dcf/mine_plans_import_test.go`

**Interfaces:**
- Consumes: `polyusAssetPlans` (Task 3), `buildPlanYears` (Task 4), `minePlansCreateTable` (`dcf/schema.go:7`), `chimport.ImportStat`.
- Produces: `type minePlansSeeder struct{}` с `Name() string { return "dcf_mine_plans" }` и `Import(ctx, conn) (int64, error)`; `readActualProduction(ctx, conn, asset string, year uint16) (float64, error)`.

**Факт берём из `databook_polyus`** (метрика `Total Dore gold output`, `data='ANNUAL'`), год — **из `date`, не из имени метрики** (дефект §8.1 роадмапа). `date` для `ANNUAL` — 1 декабря (`polyus/datapack.go:46-50`), поэтому год = `toYear(date)`.

**Review Focus 3 и 4** — тест, что отсутствие метрики даёт warn и пропуск, а не `0`; и что метрика читается из `databook_polyus`, а не из `company_financials`.

- [ ] **Step 1: Write the failing test**

```go
// TestMinePlansSeederName — имя шага DAG и CLICKHOUSE_IMPORT_STAT.
func TestMinePlansSeederName(t *testing.T) {
    if got := (&minePlansSeeder{}).Name(); got != "dcf_mine_plans" {
        t.Fatalf("Name() = %q, want dcf_mine_plans", got)
    }
}

// TestActualProductionQueryShape — запрос факта адресован databook_polyus и
// метрике датапака, а не релизной (Review Focus 4).
func TestActualProductionQueryShape(t *testing.T) {
    q := actualProductionSelect
    for _, want := range []string{"databook_polyus", "Total Dore gold output", "ANNUAL"} {
        if !strings.Contains(q, want) {
            t.Errorf("запрос не содержит %q: %q", want, q)
        }
    }
    if strings.Contains(q, "company_financials") || strings.Contains(q, "gold_output'") {
        t.Errorf("запрос тянет релизную метрику вместо датапаковой: %q", q)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dcf/ -run 'TestMinePlansSeeder' -v`
Expected: FAIL — `undefined: minePlansSeeder`.

- [ ] **Step 3: Implement `dcf/mine_plans_import.go`**

Константа `actualProductionSelect` — параметризованный запрос (`{asset:String}`, `{year:UInt16}` по конвенции ClickHouse-драйвера) к `databook_polyus FINAL` с `name = 'Total Dore gold output'`, `data = 'ANNUAL'`, `toYear(date) = {year:UInt16}`, `table = {asset:String}`.

`Import`: `ensureTable` (существующий DDL `minePlansCreateTable`); для каждого `polyusAssetPlans` — прочитать факт (для Сухого Лога факт не читаем: у него нет output в датапаке — берём профиль), вызвать `buildPlanYears`, собрать все строки; `log.Warnf` и пропуск актива, если строк нет; запись **одним батчем** `PrepareBatch → Append (err checked) → Send` (правило 2); `log.Infof("Imported %d rows of dcf_mine_plans", n)`.

Регистрация: `func init() { chimport.Stats = append(chimport.Stats, &minePlansSeeder{}) }`.

Порядок в DAG — `dcf_mine_plans` до `dcf_engine` (отразить в комментарии и в документации задачи 7).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./dcf/ -run 'TestMinePlansSeeder|TestActualProductionQueryShape' -v`
Expected: PASS.

- [ ] **Step 5: Run the whole package**

Run: `go test ./dcf/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dcf/mine_plans_import.go dcf/mine_plans_import_test.go
git commit -m "dcf: add mine_plans seeder importer"
```

---

### Task 6: Каталог рядов для сида

**Files:**
- Create: `dcf/series_meta.go`
- Test: `dcf/series_meta_test.go`

**Interfaces:**
- Consumes: `util.UpsertSeriesCatalog`, `util.SeriesMeta` (`util/series_catalog.go:38-46`), `polyusAssetPlans`.
- Produces: `var minePlansSeriesMeta []util.SeriesMeta` — по одной записи на актив; `func upsertMinePlansSeriesMeta(ctx, conn) error`.

**Правило 11a AGENTS.md.** Источник в каталоге — `dcf_mine_plans`; `origin` называет документ и страницу; `description` объясняет, что значение — LOM-план актива на год, как читать и что допущение помечено.

- [ ] **Step 1: Write the failing test**

```go
// TestMinePlansSeriesMetaCoversAllAssets — каждый актив сида описан в каталоге
// (правило 11a AGENTS.md), у каждой записи заполнены все поля.
func TestMinePlansSeriesMetaCoversAllAssets(t *testing.T) {
    got := map[string]bool{}
    for _, m := range minePlansSeriesMeta {
        if m.Source != "dcf_mine_plans" {
            t.Errorf("%s: Source = %q, want dcf_mine_plans", m.Series, m.Source)
        }
        for name, v := range map[string]string{
            "Title": m.Title, "Unit": m.Unit, "Frequency": m.Frequency,
            "Origin": m.Origin, "Description": m.Description,
        } {
            if v == "" {
                t.Errorf("%s: пустое поле %s", m.Series, name)
            }
        }
        got[m.Series] = true
    }
    for _, p := range polyusAssetPlans {
        if !got[p.Asset] {
            t.Errorf("актив %s не описан в minePlansSeriesMeta", p.Asset)
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dcf/ -run TestMinePlansSeriesMetaCoversAllAssets -v`
Expected: FAIL — `undefined: minePlansSeriesMeta`.

- [ ] **Step 3: Implement `dcf/series_meta.go`**

Собрать `[]util.SeriesMeta` из `polyusAssetPlans` (Series = имя актива, Source = `dcf_mine_plans`, Unit = `koz / USD per oz / USD mln`). `upsertMinePlansSeriesMeta` вызывает `util.UpsertSeriesCatalog`. Вызвать её из `Import` сида после записи строк.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./dcf/ -run TestMinePlansSeriesMetaCoversAllAssets -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dcf/series_meta.go dcf/series_meta_test.go dcf/mine_plans_import.go
git commit -m "dcf: catalog mine plan series for the agent"
```

---

### Task 7: Документ карты полноты данных

**Files:**
- Create: `docs/DCF_DATA_COVERAGE.md`
- Modify: `docs/ROADMAP_DCF_POLYUS.md` (статус пункта, §7)

**Interfaces:** нет кода.

**Это обязательный артефакт приёмки** (критерий 4). Содержание — ровно как в утверждённом дизайне §«Отдельный документ»: (1) схема зависимостей движка в Mermaid с узлами, которые **ещё не читаются** (`ipc_mes`, `ofz_curve`, `usdrub_path`) и помечены таковыми; (2) карта покрытия 8 активов × 9 столбцов со статусами `факт`/`derived`/`assumption`/`missing` и **источником со страницей** для каждого факта; (3) приоритизированный список пробелов (первым — налоговый режим РИП Сухого Лога, затем per-asset AISC, база/порог НДПИ, capex split, year-by-year профиль, closure, форма ценовой кривой); (4) раздел «чем этот документ не является».

- [ ] **Step 1: Написать `docs/DCF_DATA_COVERAGE.md`**

Никаких TBD: каждая ячейка карты покрытия заполнена по фактам этого исследования. У каждой фактической ячейки — источник и страница; у каждого допущения — обоснование. Явно сказать, что процент покрытия — доля заполненных входов, а не точность NAV.

- [ ] **Step 2: Обновить статус пункта в роадмапе**

В `docs/ROADMAP_DCF_POLYUS.md` §7 отметить выполненным шаг «сид `mine_plans`» с **точными командами и результатом** прогона (заполняется после Task 8), убрать пункт из остатка фазы 3, добавить новые пробелы (РИП Сухого Лога, параметры НДПИ, Чульбаткан/Чертово Корыто) отдельными пунктами.

- [ ] **Step 3: Проверить рендер Mermaid и ссылки**

Run: `head -40 docs/DCF_DATA_COVERAGE.md`
Expected: заголовок, Mermaid-блок открыт/закрыт, все ссылки — полные URL.

- [ ] **Step 4: Commit**

```bash
git add docs/DCF_DATA_COVERAGE.md docs/ROADMAP_DCF_POLYUS.md
git commit -m "docs: add dcf data coverage map and update roadmap"
```

---

### Task 8: Синхронизация документации и живая приёмка

**Files:**
- Modify: `ARCHITECTURE.md` (§6.1 — новый импортёр `dcf_mine_plans`; §6.2 — снять «`mine_plans` не наполняется ничем»; §6.4 — сид реализован; §6.3/§556 — актуализировать)
- Modify: `README.md`
- Modify: `docs/ROADMAP_DCF_POLYUS.md` (заполнить результаты прогона из Step 3)

**Interfaces:** нет кода.

По правилу 10 AGENTS.md и скилу `sync-readme-architecture`: изменился импортёр, схема наполнения и конвенция — значит в том же PR обновляются `ARCHITECTURE.md` и `README.md`.

- [ ] **Step 1: Обновить `ARCHITECTURE.md`**

В §6.1 добавить строку импортёра `dcf_mine_plans` (владеет `mine_plans`; читает факт из `databook_polyus`; пишет версионированные константы). В §6.2 заменить «`mine_plans` пока **не наполняется ничем**» на описание сида. В §6.4 убрать «сид `mine_plans`» из списка нереализованного, оставив остальные пункты. Обновить перечень `Name()` импортёров (§6.1, строка 181). В блоке §556 добавить `dcf_mine_plans` и замечание, что `mine_plans` по-прежнему не выдан агенту.

- [ ] **Step 2: Обновить `README.md`**

Добавить `dcf_mine_plans` в таблицу/список импортёров и порядок запуска (`dcf_mine_plans` → `dcf_engine`).

- [ ] **Step 3: Живая приёмка на dev-БД**

Run: `make env-check && make ch-status`
Expected: ClickHouse отвечает на `:8123`.

Run: `make import STAT=dcf_mine_plans`
Expected: `Imported N rows of dcf_mine_plans`, где `N` = сумма сроков службы (10+13+24+15+15+… +профиль Сухого Лога); в логе — названо, из какого отчёта взяты сроки.

Run: `DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine`
Expected: `Imported N rows of dcf_engine` = 8 активов × 3 дека × 2 контура = **48**; сид деков печатает строки на **все годы плана** (не один 2026).

- [ ] **Step 4: SQL-проверка результата**

Через MCP `clickhouse` (пользователь `kimi_reader`, только витрины):

```sql
SELECT deck, contour, count() AS n, round(sum(npv_usd_mln), 2) AS total
FROM v_dcf_assumptions
GROUP BY deck, contour ORDER BY deck, contour
```

Expected: 6 групп (`consensus_lt`/`own_scenario`/`spot_flat` × `industrial`/`local`), в каждой 8 строк; `total` **не нулевой**; для каждого дека `local` строго меньше `industrial` — это и есть регресс на одногодичную ловушку `1/(1+rate)^0`.

```sql
SELECT asset, deck, contour, npv_usd_mln, discount_rate
FROM v_dcf_assumptions
WHERE deck = 'own_scenario' ORDER BY contour, npv_usd_mln DESC
```

Expected: Сухой Лог — заметная доля суммы; `discount_rate` 0.05 / 0.16.

- [ ] **Step 5: Проверить, что агент видит результат**

Run: `make mcp-check`
Expected: **ПРОВЕРКА ПРОЙДЕНА**; `v_dcf_assumptions` читается, `SELECT count() FROM mine_plans` под `kimi_reader` даёт `ACCESS_DENIED`.

- [ ] **Step 6: Полный прогон проверок**

Run: `make all`
Expected: gofmt — чисто; golangci-lint — `0 issues`; `go vet ./...` — без замечаний; `go test -race ./...` — все пакеты `ok`; сборки `build/clickhouse-import-rosstat` и `build/ingest` — успешно.

- [ ] **Step 7: Записать результаты в роадмап и коммит**

Внести в `docs/ROADMAP_DCF_POLYUS.md` §7 фактические числа и команды из Step 3–5 (как это сделано для шага 1 фазы 3).

```bash
git add ARCHITECTURE.md README.md docs/ROADMAP_DCF_POLYUS.md
git commit -m "docs: sync architecture and roadmap with mine_plans seed"
```

---

## Открытые вопросы к пользователю (перед стартом Task 3)

Заполнено по данным разведки; менять не нужно, но два числа требуют подтверждения человека, потому что это допущения, а не публикация:

1. **`ClosureCosts`**: публичного per-asset нет. План распределяет групповой провижен пропорционально reserves. Нужно подтвердить базу распределения (reserves vs добыча).
2. **Профиль Сухого Лога**: 10-летнее среднее 2,3–2,8 млн унц — это **среднее**, не по-годичная кривая. План берёт плато по среднему; если у пользователя есть по-годичный профиль из дека, он заменяет плато.
