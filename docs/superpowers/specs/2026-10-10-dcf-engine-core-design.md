# Дизайн: DCF-движок Полюса — шаг 1: таблицы и ядро LOM-NAV

**Дата:** 2026-10-10
**Статус:** реализовано 2026-10-10 (шаг 1 фазы 3; приёмка — живой прогон на dev-БД, см. [docs/ROADMAP_DCF_POLYUS.md](../../ROADMAP_DCF_POLYUS.md) §7.1)
**Пункт роадмапа:** [docs/ROADMAP_DCF_POLYUS.md](../../ROADMAP_DCF_POLYUS.md) §7, фаза 3 «DCF-движок» — первый законченный срез
**Связанные документы:** [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.2 (DDL), §6.3 (витрины), §6.4 (конвенция DCF), §6.5 (MCP-контур); [AGENTS.md](../../../AGENTS.md) правила 2, 5, 7, 8, 9, 11

---

## 1. Задача

Фаза 3 роадмапа — самый крупный пункт: таблицы модели, sum-of-parts LOM-NAV с хвостом закрытия,
двухконтурная ставка, НДПИ-функция, стадийные haircut'ы ресурсов, мост NAV → цена, NAV-матрица.
Целиком она не помещается в один цикл и не даёт полезного результата до самого конца.

Этот дизайн берёт **первый законченный срез**: расчётное ядро LOM-NAV, которое считает NPV по
активам на существующих price deck'ах и пишет `nav_by_asset`. Результат среза — работающий,
покрытый тестами движок, который можно пересчитывать командой, а не половина незамкнутого контура.

**Вне среза** (отдельные пункты роадмапа, не делаются «попутно»):

| Не делается | Почему | Куда |
|---|---|---|
| Сид `mine_plans` из отчётности Полюса | Требует ручных допущений по годам (guidance, strip ratio, хвост LOM) и сверки с датапаком | Следующий пункт фазы 3 |
| `reserves_assets`/`reserves_dynamics`/`license_events`, стадийные haircut'ы, `v_peers_comparison` | Нужны данные по ЮГК/Селигдару (ручной ввод) | [ROADMAP_DCF_METALS.md](../../ROADMAP_DCF_METALS.md) и отдельный пункт |
| Мост NAV → цена, NAV-матрица в Grafana | Визуализация поверх готовых выходов | Отдельный пункт фазы 3 |
| `regime_states` (HMM), автоверификация `forecast_log` | Следующая фаза | Фаза 4 |
| Запись `model_runs` движком | Нарушает §5.2 роадмапа (писать может только ingest-endpoint) | Не делается никогда |

---

## 2. Что уже есть и переиспользуется

- `model_runs`, `forecast_log`, `macro_series` — DDL в `ingest/schema.go`; запись только через `cmd/ingest` (Bearer).
- Витрины `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy`, `v_company_*` — `views/`.
- Входы в БД, которые нужно читать: `mine_plans` (заводится этим срезом), `price_decks` (заводится этим срезом), `gold_prices`, `cbr_currency_usd`, `ipc_mes`, `ofz_curve`.
- Конвенция DCF — `ARCHITECTURE.md` §6.4 (rev.2): sum-of-parts, `НДПИ = база + 10% × max(gold − 1900, 0)`,
  двухконтурная ставка (5% real USD индустриальная / ОФЗ локальная), отрицательный хвост `closure_costs`,
  три price deck (`spot_flat`/`consensus_lt`/`own_scenario`), perpetual TV нет.
- Канонические DDL `mine_plans`, `nav_by_asset`, `price_decks` **уже написаны** в §6.2 и помечены как
  «не реализованы (в коде нет)».
- Шаблон импортёра: `chimport.ImportStat` (`Name()` + `Import(ctx, conn)`), регистрация в `chimport.Stats`,
  `ensureTables` по образцу `polyus/`.

---

## 3. Решение: архитектура

**Новый пакет `dcf/`** (AGENTS.md правило 7: новые импортёры — на `clickhouse-go/v2`, не в legacy
`financial/`), зарегистрированный как импортёр `dcf_engine`. Три файла, три ответственности:

```
dcf/schema.go   — DDL трёх таблиц (копия канонических из ARCHITECTURE §6.2)
dcf/model.go    — ЧИСТОЕ ядро: НДПИ, LOM-NPV, эскалация, ставки. Ни БД, ни сети, ни logrus.
dcf/engine.go   — обвязка chimport.ImportStat: ensureTables → чтение входов → ядро → батч в nav_by_asset
```

Почему так, а не в SQL-витрине: НДПИ-функция, хвост LOM и двухконтурное дисконтирование — это логика
с ветвлениями и границами, которую нельзя ни прочитать в SQL, ни проверить фикстурой без ClickHouse.
Плюс `nav_by_asset` — **таблица** (вход для будущих витрин и для `v_dcf_assumptions`), а не
представление. Ядро как чистые функции даёт юнит-тесты без БД — то, чего в проекте для расчётов нет.

Почему импортёр, а не отдельный бинарник: все импортёры проекта — `chimport.ImportStat`, их запускает
`make import STAT=...` и DAG'и `dagu`. Пересчёт NAV = обычный прогон импортёра, отдельная
инфраструктура не нужна.

### 3.1 Исправление схемы `nav_by_asset` (единственное отклонение от §6.2)

Канонический DDL §6.2 не может хранить то, что требует роадмап:

```sql
-- ARCHITECTURE §6.2 сейчас — БЕЗ измерения deck:
) ENGINE = ReplacingMergeTree ORDER BY (run_id, asset);
```

Роадмап §4.3 требует «NAV/акция по трём сценариям **× трём price deck × двум ставкам**», а §13.4 —
«P/NAV против пиров — только на одинаковом deck». Ключ без `deck` означает, что второй deck
**перетирает** первый (ReplacingMergeTree схлопывает по `(run_id, asset)`), а третий — второй. Это
ровно тот класс тихой потери данных, который в этом проекте уже был: дефект (3) §8.1
(`company_financials` схлопывала два документа за период).

**Решение — добавить `deck` в таблицу и в ключ:**

```sql
CREATE TABLE IF NOT EXISTS nav_by_asset (
    run_id UUID,                       -- связка с model_runs
    deck LowCardinality(String),       -- 'spot_flat','consensus_lt','own_scenario'
    asset LowCardinality(String),
    contour LowCardinality(String),    -- 'industrial','local' — контур ставки (§3.2)
    npv_usd_mln Float64,
    discount_rate Float64,             -- 0.05 real USD база + страновая/стадийная надбавка
    stage_haircut Nullable(Float64)    -- construction 0.7–0.9, DFS 0.5–0.7, PEA 0.2–0.4
) ENGINE = ReplacingMergeTree ORDER BY (run_id, deck, asset, contour);
```

Ключ — `(run_id, deck, asset, contour)`. Измерение контура — **строковое** (`LowCardinality(String)`),
а не числовое: `discount_rate` остаётся значением, а ключ остаётся стабильным (см. §3.2).

Правило 8 AGENTS.md: изменение схемы = сначала обновить ARCHITECTURE.md. Поэтому §6.2 обновляется
**в том же PR**, до кода.

### 3.2 Двухконтурная ставка: колонка `contour`

`nav_by_asset.discount_rate` — одно число на строку, а контуров два (индустриальный, локальный).
Если контур не выделить, второй контур перетирает первый — та же тихая потеря, что и с deck.

Решение: колонка `contour LowCardinality(String)` со значениями `industrial` / `local`, в ключе.

Альтернатива — положить `discount_rate` в `ORDER BY` — отклонена: `Float64` как ключ сортировки
хрупок (два контура с близкими ставками, разный порядок округления, смена ставки в новом прогоне
плодит строки), и «контур» перестаёт быть явным измерением, превращаясь в следствие числа. Строковое
измерение читаемо в витрине и не зависит от арифметики.

Итоговый ключ: `ORDER BY (run_id, deck, asset, contour)`. Строк `nav_by_asset` на один прогон —
`активы × 3 deck × 2 контура`.

### 3.3 Ядро расчёта (`dcf/model.go`)

Чистые функции, экспортируемые внутри пакета (нижний регистр — пакетные, тесты в том же пакете):

```go
// Params — допущения расчёта. Значения по умолчанию — экспортируемые константы,
// чтобы ingest-сторона могла положить те же числа в model_runs JSON, а тест — закрепить их.
type Params struct {
    NdpiBaseUSDPerOz     float64 // база НДПИ, USD/унц
    NdpiSurchargePct     float64 // 0.10 — надбавка с 2025 (ARCHITECTURE §6.4)
    NdpiThresholdUSD     float64 // 1900 — порог надбавки
    ProfitTaxPct         float64 // налог на прибыль, доля
    WorkingCapitalDays   float64 // ΔWC в днях выручки
}

// MinePlanYear — год life-of-mine плана актива (вход из mine_plans).
type MinePlanYear struct {
    Year            uint16
    ProductionKoz   float64
    TCC             float64 // USD/унц
    AISC            float64 // USD/унц
    CapexSustaining float64 // USD млн
    CapexProject    float64 // USD млн
    ClosureCosts    float64 // USD млн, последний год, ОТРИЦАТЕЛЬНЫЙ хвост
}

// ndpiPerOz — НДПИ на унцию, USD: база + surcharge × max(gold − threshold, 0).
func ndpiPerOz(gold float64, p Params) float64

// npvLOM — NPV актива: дисконтирование годовых FCF по ставке rate,
// хвост closure_costs в последнем году, perpetual TV НЕТ.
func npvLOM(plan []MinePlanYear, deck []DeckYear, rate float64, ipc []float64, p Params) float64

// escalate — эскалация затрат по ИПЦ: value_t = base × Π_{i<year}(1 + ipc_i).
func escalate(base float64, ipc []float64, yearIndex int) float64
```

Формула FCF (роадмап §4.1, ARCHITECTURE §6.4):

```
FCF_t = Production_t(koz) × (gold_t − AISC_t) − НДПИ_t − налог_t − capex_t ± ΔWC_t
НДПИ_t = Production_t × ndpiPerOz(gold_t)
налог_t = max(FCF_до_налога, 0) × ProfitTaxPct
в последнем году: + closure_costs (отрицательный, налогово вычитаемый — вычитается ДО налога)
```

AISC/TCC эскалируются по ИПЦ РФ (ряд `ipc_mes`) — эскалация применяется к плану, не к цене золота.
Дисконтирование — по годам от первого года плана, ставка непрерывная годовая.

### 3.4 Обвязка (`dcf/engine.go`)

`Import(ctx, conn)`:

1. `ensureTables` — идемпотентно, по образцу `polyus/`.
2. Прочитать `mine_plans FINAL`. **Пусто → вернуть `0, nil` с `log.Warn`** («сид `mine_plans` — отдельный
   пункт роадмапа»), не падать: движок обязан быть прогоняемым на dev-БД без фикстур.
3. Прочитать `price_decks FINAL` по трём декам. Если таблица пуста — засеять её константами
   (только сид: спот — из последнего `gold_prices`, `consensus_lt`/`own_scenario` — из задокументированных
   LT-оценок §13.4). Непустую таблицу не трогать.
4. Для каждого (asset × deck × контур) — `npvLOM`. Ставка контура — из `DiscountRates`
   (`industrial` = 5% real USD + надбавки, `local` = ОФЗ + премии); оба контура считаются всегда.
5. Записать `nav_by_asset` **батчем** (`PrepareBatch → Append → Send`, правило 2), каждый `Append` с проверкой `err`.
   Строка — на (asset, deck, contour): `активы × 3 × 2`.

`run_id`:

- приходит переменной окружения `DCF_RUN_ID` (UUID) — тогда строки пишутся с ним;
- отсутствует → **ничего не пишем**: считаем, логируем результат и печатаем готовый JSON тела
  `POST /v1/model_run` для ingest-endpoint. Запись `model_runs` — не дело движка (§5.2 роадмапа).

### 3.5 Витрина `v_dcf_assumptions` (`views/dcf_assumptions.go`)

Одна строка на (deck, asset, contour) из `nav_by_asset FINAL` плюс итог по run, deck и контуру:

```sql
SELECT run_id, deck, contour, asset, npv_usd_mln, discount_rate, stage_haircut,
       sum(npv_usd_mln) OVER (PARTITION BY run_id, deck, contour) AS nav_total_usd_mln
FROM nav_by_asset FINAL
```

Комментарий таблицы и **каждой** колонки (правило 11б: `ALTER TABLE ... MODIFY COMMENT` /
`COMMENT COLUMN` — `COMMENT ON` в ClickHouse 26.10 не работает). Регистрация — через `util.CreateView`
в импортёре `gold_views` или `dcf_views` (витрина создаётся, когда `nav_by_asset` уже есть).
Грант — `GRANT SELECT ON default.v_dcf_assumptions TO kimi_reader` в `sql/mcp_kimi_reader.sql` (правило 11в).
Сырая `nav_by_asset` остаётся закрытой.

---

## 4. Обработка ошибок

- Пустой `mine_plans` — не ошибка: `0` строк + `log.Warn` с указанием следующего пункта.
- Битая строка плана (отрицательная добыча, отсутствующий год) — пропуск с `log.Warnf`, не падение
  (тот же приём, что в `polyus/datapack.go` для битой ячейки).
- Ошибка чтения/вставки — возврат наверх, импортёр завершается ненулевым кодом (штатная семантика `main.go`).
- `DCF_RUN_ID` задан, но не UUID — `log.Fatal` до расчёта: писать в UUID-колонку мусор нельзя.

---

## 5. Тестирование

Ядро — table-driven тесты на **закреплённых числах**, посчитанных вручную до реализации (TDD):

| Что | Границы |
|---|---|
| `ndpiPerOz` | ниже порога (надбавки нет), ровно на пороге, выше порога |
| `npvLOM` | 3-летний план с закреплённым ответом; хвост закрытия **уменьшает** NPV; ставка выше → NPV ниже |
| `escalate` | нулевой ИПЦ (без изменений), ненулевой, накопление за несколько лет |
| двухконтурность | два вызова с разными ставками дают разные NPV и обе строки пишутся |

Интеграционно (вручную, в приёмке): `make import STAT=dcf_engine` на dev-БД с фикстурой `mine_plans`,
вставленной SQL, даёт ожидаемые строки `nav_by_asset`; пустой `mine_plans` даёт `Imported 0 rows`.

Сквозного теста с ClickHouse в пакете нет — как и у остальных импортёров (тесты ядра не требуют БД).

---

## 6. Приёмка среза

1. `make all` зелёный (gofmt, golangci-lint, `go vet`, `go test -race`, сборка) — те же проверки гоняет CI.
2. `go test -race ./dcf/...` — тесты ядра с закреплёнными числами.
3. `make import STAT=dcf_engine` на dev-БД: созданы три таблицы; с фикстурой `mine_plans` — заполнен
   `nav_by_asset` (активы × decks × контуры); с пустым `mine_plans` — `Imported 0 rows` и warn.
4. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА: `v_dcf_assumptions` видна агенту, `nav_by_asset` закрыта (`ACCESS_DENIED`).
5. ARCHITECTURE.md (§6.1 пакет, §6.2 схема и список «не реализованы», §6.3 витрина, §6.4 ссылка на
   реализацию), README.md и ROADMAP §7 обновлены **тем же PR**; в роадмапе шаг 1 фазы 3 отмечен ✅
   с командой и результатом, остаток фазы 3 перечислен явно.

---

## 7. Риски

| Риск | Митигация |
|---|---|
| Ядро нельзя проверить, пока нет реального `mine_plans` | Проверка на SQL-фикстуре в приёмке; сид — отдельный пункт, не блокирует |
| Отклонение от §6.2 (колонка `deck`) расходится с документацией | §6.2 обновляется в том же PR до кода (правило 8) |
| Движок «заодно» начнёт писать `model_runs` | Запрещено дизайном: только `DCF_RUN_ID` + лог готового JSON |
| Срез разрастётся в половину фазы 3 | Границы зафиксированы таблицей в §1; ресурсы/peers/мост/матрица — следующие пункты |
| Допущения (`Params`) «уедут» от `model_runs` | Числа по умолчанию — экспортируемые константы; ingest кладёт те же значения в JSON |
