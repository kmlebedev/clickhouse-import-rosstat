# Фаза 3 роадмапа золота: спрос, консенсус и обратная связь — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** закрыть фазу 3 роадмапа: структурированные ряды WGC ETF Flows, таблица/витрина консенсуса банков `gold_forecasts`, автозаполнение `forecast_log.actual` импортёром `forecast_resolve`.

**Архитектура:** три слоя по конвенциям репозитория: (1) `ingest/` — валидирующий endpoint `POST /v1/gold_forecasts` + DDL (копия §6.2); (2) новый пакет `forecast/` — импортёр `forecast_resolve` (читает `forecast_log FINAL`, считает факт по `gold_prices`, пишет замену одним батчем); (3) `views/` — витрины `v_gold_forecasts` и `v_gold_etf_flows` + SQL-сиды каталога/данных. Правило 11 AGENTS.md — в каждом слое.

**Стек:** Go 1.27, clickhouse-go/v2 (native), ClickHouse 26.10.1, dagu, офлайн-тесты (httptest + fake store).

**Спека:** [docs/superpowers/specs/2026-10-11-gold-phase3-demand-consensus-design.md](../specs/2026-10-11-gold-phase3-demand-consensus-design.md)

## Глобальные ограничения

- Секреты — только `os.Getenv`; в коде, комментариях и примерах не печатать.
- Вставка — только батчами `PrepareBatch → Append (err checked) → Send`; построчный `Exec` в цикле запрещён.
- DDL — `CREATE TABLE IF NOT EXISTS`; копии одного DDL обязаны совпадать (ARCHITECTURE §6.2 = код).
- Значения — `Float64`; исключение — `probability Float32` (как в каноническом DDL §6.2).
- Комментарии витрин — `ALTER TABLE ... MODIFY COMMENT` / `COMMENT COLUMN` (через `util.CreateView`).
- Команды с ключами/ClickHouse — только через `make` (Makefile подключает `~/.config/rosstat/env`).
- После кода — `make all` зелёный (gofmt, golangci-lint, `go vet`, `go test -race`, сборка).

## Review Focus

1. `POST /v1/gold_forecasts` с пустым массивом → `{"inserted":0}`, батча нет; с невалидной строкой в середине массива → 400 и **ни одной** записи (валидация всего массива до батча).
2. Резолвер при `target_date` в будущем или метрике `nav`: ничего не пишет, не падает, `Imported 0 rows` — норма, а не сбой (фильтр в SQL по `metric = 'xau_q_avg' AND target_date <= today()`).
3. Повторный прогон резолвера с неизменившимся фактом: второй батч **не** пишется (иначе ReplacingMergeTree копит версии).
4. `v_gold_etf_flows` при частично заполненном месяце (есть поток $, нет тонн/AUM): строка есть, отсутствующие метрики — NULL, не 0.
5. `v_gold_forecasts`: банковский `range` (low/high, point=0) → `midpoint` = середина, `point` = NULL; own-строка (point>0) → `midpoint` = `point`.

---

### Задача 1: ingest — `gold_forecasts` (DDL, валидация, endpoint)

**Files:**
- Modify: `ingest/schema.go`, `ingest/clickhouse.go`, `ingest/server.go`
- Create: `ingest/gold_forecasts.go`
- Test: `ingest/gold_forecasts_test.go` (новый), `ingest/server_test.go`, `ingest/clickhouse_test.go`

**Interfaces:**
- Produces: `type GoldForecast struct { Author string; Published string; Horizon string; Scenario string; Low float64; High float64; Point float64; Probability float32 }` (json-теги `author/published/horizon/scenario/low/high/point/probability`);
  `func ValidateGoldForecast(f GoldForecast) error`;
  `func (w *ClickHouseWriter) InsertGoldForecasts(ctx context.Context, forecasts []GoldForecast) error`;
  метод `InsertGoldForecasts` в интерфейсе `BatchWriter` (server.go);
  константы `goldForecastsDdl` (schema.go) и `goldForecastsInsert = "INSERT INTO gold_forecasts (author, published, horizon, scenario, low, high, point, probability)"` (clickhouse.go).
- Consumes: `parsePointDate` (дата точки, ingest/model_run.go), `sortedKeys` (детерминированный текст ошибки).

- [ ] **Step 1: Тесты (сначала падающие)**

`ingest/gold_forecasts_test.go` — таблица валидации (каждый кейс — отдельная строка): валидный `point`-прогноз; валидный `range` (low 5400, high 5600); пустой/пробельный `author`; `published` не `YYYY-MM-DD`; `published` вне Date32 (`1899-12-31`); пустой `horizon`; `scenario = "foo"`; все три числа 0; `high < low`; `probability = 101`; `low` отрицательный.

```go
func TestValidateGoldForecastErrors(t *testing.T) {
    valid := GoldForecast{Author: "goldman", Published: "2026-10-07", Horizon: "YE-2026",
        Scenario: "point", Point: 4900}
    // каждая мутация valid → ожидаемая подстрока ошибки
}
```

`ingest/server_test.go` — по образцу `TestGoldPrices*`: `TestGoldForecastsOK` (2 строки, `POST /v1/gold_forecasts` → `200 {"inserted":2}`, мок-писатель получил 2 строки), `TestGoldForecastsValidationError` (Кейс Review Focus 1: массив из валидной и невалидной строки → `400`, мок **не вызывался**), `TestGoldForecastsEmptyArray` → `{"inserted":0}`, `TestGoldForecastsUnknownFieldIsRejected` (лишнее поле — 400).
Мок `mockBatchWriter` (server_test.go) дополняется методом `InsertGoldForecasts`.

`ingest/clickhouse_test.go` — по образцу `TestInsertGoldPrices*`: `TestInsertGoldForecastsAppendsRows` (один батч `goldForecastsInsert`, поля в порядке колонок; дата — `time.Time`), `TestInsertGoldForecastsAppendErrorAborts`, `TestInsertGoldForecastsEmptySkipsBatch`.

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `go test ./ingest/ -run 'GoldForecast' -v` — FAIL (типы/методы не определены).

- [ ] **Step 3: Реализация**

- `ingest/schema.go`: константа DDL (копия §6.2 дословно; комментарий `scenario` — `-- 'point','range','bull','base','bear'`):

```sql
CREATE TABLE IF NOT EXISTS gold_forecasts (
    author LowCardinality(String),   -- 'goldman','jpm','hsbc','ubs','citi','own',...
    published Date,
    horizon LowCardinality(String),  -- 'Q4-2026','YE-2026','2027','12M','short'
    scenario LowCardinality(String), -- 'point','range','bull','base','bear'
    low Float64, high Float64, point Float64,
    probability Float32
) ENGINE = ReplacingMergeTree ORDER BY (author, published, horizon, scenario);
```

плюс в список `ensureTables` (порядок: после `goldPricesDdl`); обновить комментарий функции («четыре канонические…»).

- `ingest/gold_forecasts.go`: тип + валидация. Whitelist `validForecastScenarios = {point, range, bull, base, bear}`; ошибка по сценарию — через `sortedKeys` (детерминированный порядок). Правила: `author`/`horizon` непустые после `TrimSpace`; `published` через `parsePointDate`; числа ≥ 0, хотя бы одно > 0; если `low > 0 && high > 0` → `high >= low`; `probability` в [0,100].
- `ingest/clickhouse.go`: `InsertGoldForecasts` — пустой список → `nil` без батча; дата парсится `time.Parse(manualSeriesDateLayout, ...)` (как в `InsertGoldPrices`); один батч; `Abort` только в ветке ошибки `Append`.
- `ingest/server.go`: + `InsertGoldForecasts` в интерфейс; маршрут `s.mux.HandleFunc("POST /v1/gold_forecasts", s.handleGoldForecasts)`; handler — по образцу `handleManualSeries` (decode → валидация всех строк → writer → `writeInserted(len)`).

- [ ] **Step 4: Тесты проходят**

Run: `go test -race ./ingest/ -v` → PASS; затем `make all` → зелёный.
Ожидаемое поведение Review Focus 1 фиксируется тестами `TestGoldForecastsValidationError` (400, батча нет) и `TestGoldForecastsEmptyArray`.

- [ ] **Step 5: Commit**

```bash
git add ingest/ && git commit -m "ingest: add gold_forecasts table and POST /v1/gold_forecasts endpoint"
```

---

### Задача 2: пакет `forecast/` — импортёр `forecast_resolve` (+ dagu, бэкфилл-сид)

**Files:**
- Create: `forecast/resolve.go`, `forecast/resolve_test.go`, `dagu/forecast.yaml`, `sql/forecast_log_backfill.sql`
- Modify: `main.go` (blank-import: между `financial` и `fred`)

**Interfaces:**
- Produces: импортёр `Name() = "forecast_resolve"` (регистрация через `chimport.Stats` в `init()`);
  чистые функции: `quarterWindow(target time.Time) (time.Time, time.Time)` — `[первый день квартала, target]`;
  `pickActual(stats windowStats) (venue string, actual float64, ok bool)` — comex при `ComexCount > 0`, иначе moex при `MoexCount > 0`;
  `errorPct(predicted, actual float64) float64`;
  `needsWrite(row forecastRow, actual float64) bool` — истина при `Actual == nil` или отличающемся значении;
  типы `forecastRow{ForecastDate, TargetDate time.Time; Predicted float64; Actual *float64}`, `windowStats{ComexCount uint64; ComexAvg float64; MoexCount uint64; MoexAvg float64}`;
  узкий интерфейс стора `resolveStore { resolvable(ctx) ([]forecastRow, error); windowStats(ctx, start, end time.Time) (windowStats, error); writeResolutions(ctx, rows []forecastResolution) error }` + реализация `connStore{conn driver.Conn}`;
  `forecastResolution{ForecastDate, TargetDate time.Time; Predicted, Actual, ErrorPct float64}`.
- Consumes: `chimport` (реестр), `driver.Conn`.

- [ ] **Step 1: Тесты (сначала падающие)**

`forecast/resolve_test.go` (офлайн, фейковый стор):
- `TestQuarterWindow`: `2026-12-31 → [2026-10-01, 2026-12-31]`; `2026-07-15 → [2026-07-01, 2026-07-15]`; `2026-01-01 → [2026-01-01, 2026-01-01]`; `2026-03-31 → [2026-01-01, 2026-03-31]`; `2027-02-10 → [2027-01-01, 2027-02-10]`.
- `TestPickActual`: comex есть → comex (и при наличии moex тоже); comex нет, moex есть → moex; оба пусты → `ok = false`.
- `TestErrorPct`: `(predicted=4450, actual=4300.8) → 3.4691…`.
- `TestNeedsWrite`: `Actual == nil` → true; `*Actual == actual` → false; отличается → true.
- `TestImportWritesOnlyChanged` (Review Focus 3): фейк-стор возвращает 2 строки (у первой `Actual == nil`, у второй `Actual ==` вычисленному) → `Import` пишет батч из **1** строки, count = 1; и кейс «писать нечего» → count 0, `writeResolutions` не вызван.
- `TestResolvableQueryFilters` (Review Focus 2): тексты SQL-констант содержат `metric = 'xau_q_avg'` и `target_date <= today()` (структурная проверка, как в `views/*_test.go`).

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `go test ./forecast/ -v` — FAIL (пакет не существует).

- [ ] **Step 3: Реализация `forecast/resolve.go`**

SQL стора (по образцу текстов в `views/`; даты — параметрами):
- `resolvableSelect`:
  `SELECT forecast_date, target_date, predicted, if(actual IS NULL, 1, 0) AS actual_is_null, coalesce(actual, 0) AS actual FROM forecast_log FINAL WHERE metric = 'xau_q_avg' AND target_date <= today() ORDER BY forecast_date`
  (`if(...IS NULL...)` + `coalesce` — потому что драйвер строго типизирован и читается в `uint8`/`float64`, без Nullable-сканов);
- `windowStatsSelect` (окно `{start:Date}, {end:Date}` — `clickhouse.Named`, как в `dcf/mine_plans_import.go`):
  `SELECT countIf(venue = 'comex_front') AS comex_n, if(comex_n > 0, avgIf(usd, venue = 'comex_front'), 0) AS comex_avg, countIf(venue = 'moex_fix_usd') AS moex_n, if(moex_n > 0, avgIf(usd, venue = 'moex_fix_usd'), 0) AS moex_avg FROM gold_prices FINAL WHERE date >= {start:Date} AND date <= {end:Date}`
  (`if(…count…>0…)` — гейт по количеству: пустой `avgIf` даёт NaN, а не NULL);
- `forecastLogInsert = "INSERT INTO forecast_log (forecast_date, target_date, metric, predicted, actual, error_pct)"`, `forecastMetric = "xau_q_avg"`.

`Import`: получить `resolvable`; для каждой строки — `quarterWindow`, `windowStats`, `pickActual`; нет данных или `actual <= 0` → `log.Warnf` и пропуск; `needsWrite` ложно → пропуск; иначе в `forecastResolution` (`,` `errorPct`). Итог — `log.Infof` по каждой записи (forecast_date, target_date, venue, число точек, actual, error_pct) и один `writeResolutions`; вернуть число записанных строк. Комментарий импортёра — почему `nav` не резолвится (факта нет; `verify.md` плагина gold-nav) и почему окно именно квартал.

- [ ] **Step 4: dagu + бэкфилл-сид + регистрация**

`dagu/forecast.yaml`:

```yaml
type: chain
schedule: "23 20 * * *"
overlap_policy: skip

steps:
  - id: forecast_resolve
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=forecast_resolve
    retry_policy:
      limit: 3
      interval_sec: 300
```

`sql/forecast_log_backfill.sql` — комментарий (откуда число: статья §1, прогноз 31.07.2026, «базовый сценарий Q3 $4 300–4 600, средняя ~$4 450»; применяется вручную; требует существующей `forecast_log` — создаёт ingest) и:

```sql
INSERT INTO forecast_log (forecast_date, target_date, metric, predicted, actual, error_pct) VALUES
('2026-07-31', '2026-09-30', 'xau_q_avg', 4450, NULL, NULL);
```

`main.go` — `_ ".../forecast"` в алфавитном порядке blank-импортов.

- [ ] **Step 5: Тесты и живой прогон**

Run: `go test -race ./forecast/ -v` → PASS; `make all` → зелёный.
Live:
1. `make ch-sql FILE=sql/forecast_log_backfill.sql`
2. `make import STAT=forecast_resolve` → `Imported 1 rows of forecast_resolve`, в логе venue `comex_front`, 64 точки.
3. Повторный `make import STAT=forecast_resolve` → `Imported 0 rows` (идемпотентность; Review Focus 3).
4. MCP `run_query`: `SELECT metric, forecast_date, target_date, predicted, actual, round(error_pct, 2) AS error_pct, is_resolved FROM v_forecast_accuracy ORDER BY target_date` → Q3-строка `is_resolved = 1`, `actual ≈ 4300.8`, `error_pct ≈ 3.47`; Q4-строки — `is_resolved = 0`.

- [ ] **Step 6: Commit**

```bash
git add forecast/ dagu/forecast.yaml sql/forecast_log_backfill.sql main.go
git commit -m "forecast: add forecast_resolve importer filling forecast_log actual from gold_prices"
```

---

### Задача 3: витрины, сиды (WGC-каталог, консенсус) и гранты

**Files:**
- Create: `views/gold_forecasts.go`, `views/gold_etf_flows.go`, `views/gold_forecasts_test.go`, `views/gold_etf_flows_test.go`, `sql/wgc_etf_flows_catalog.sql`, `sql/gold_forecasts_q4_2026_seed.sql`
- Modify: `views/gold_views.go`, `views/forecast_accuracy.go`, `sql/mcp_kimi_reader.sql`

**Interfaces:**
- Produces: `var goldForecastsView util.View` (имя `v_gold_forecasts`); `var goldEtfFlowsView util.View` (имя `v_gold_etf_flows`); в `views/gold_etf_flows.go` — данные набора: `var wgcEtfRegions = []string{"world","north_america","europe","asia","other"}`, `var wgcEtfMetrics = []string{"flow_usd_mln","flow_tonnes","holdings_tonnes","aum_usd_bn"}`, `func wgcEtfSeriesName(metric, region string) string` (= `"etf_" + metric + "_" + region`), `func wgcEtfSeriesNames() []string`, `func wgcEtfSeriesMapSQL() string` (пары `('etf_flow_usd_mln_world','world','flow_usd_mln')` для `ARRAY JOIN`).
- Consumes: `util.View`/`util.CreateView`, `goldViewsList` (views/gold_views.go).

- [ ] **Step 1: Тесты (сначала падающие)**

`views/gold_forecasts_test.go`: регистрация в `goldViewsList`; непустой комментарий таблицы; полнота комментариев колонок через `selectedColumns(v.Select)`; `SELECT` содержит `FROM gold_forecasts FINAL` и `nullIf(`.
`views/gold_etf_flows_test.go`: регистрация; комментарии таблицы и всех 4 метрик-колонок; приоритет источников (`coalesce(`-блок с `source = 'webbridge'` раньше `source = 'manual'`, `maxIfOrNull`, запрет `maxIf(`); `wgcEtfSeriesNames()` — ровно 20 имён; сверка с каталог-сидом: каждое имя присутствует в файле `../sql/wgc_etf_flows_catalog.sql`.
Тест `TestForecastAccuracyViewDescribesResolution` (в `views/forecast_accuracy.go`-тесте или в `gold_forecasts_test.go`): комментарий витрины содержит `forecast_resolve`.

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `go test ./views/ -run 'GoldForecasts|GoldEtfFlows|ForecastAccuracy' -v` — FAIL.

- [ ] **Step 3: Реализация витрин**

`views/gold_forecasts.go`:

```sql
SELECT
    author, published, horizon, scenario,
    nullIf(low, 0) AS low, nullIf(high, 0) AS high, nullIf(point, 0) AS point,
    if(point > 0, point, nullIf((low + high) / 2, 0)) AS midpoint,
    nullIf(toFloat64(probability), 0) AS probability
FROM gold_forecasts FINAL
ORDER BY published DESC, author, horizon, scenario
```

`Tables: []string{"gold_forecasts"}`; комментарии таблицы и всех колонок (семантика: банковские цели и own-сетка; `range` — диапазон банка; `midpoint` — точка или середина; `probability` — только у own-сценариев).
`views/gold_etf_flows.go` — pivot-витрина (Review Focus 4/5):

```sql
SELECT
    v.date AS date,
    m.region AS region,
    maxIfOrNull(v.value, m.metric = 'flow_usd_mln')     AS flow_usd_mln,
    maxIfOrNull(v.value, m.metric = 'flow_tonnes')      AS flow_tonnes,
    maxIfOrNull(v.value, m.metric = 'holdings_tonnes')  AS holdings_tonnes,
    maxIfOrNull(v.value, m.metric = 'aum_usd_bn')       AS aum_usd_bn
FROM (
    SELECT x.1 AS series, x.2 AS region, x.3 AS metric
    FROM (SELECT arrayJoin([<wgcEtfSeriesMapSQL()>]) AS x)
) AS m
INNER JOIN (
    SELECT date, series,
        coalesce(maxIfOrNull(value, source = 'webbridge'), maxIfOrNull(value, source = 'manual')) AS value
    FROM macro_series FINAL
    WHERE source IN ('webbridge', 'manual') AND series IN (<wgcEtfSeriesNames()>)
    GROUP BY date, series
) AS v ON v.series = m.series
GROUP BY date, region
```

`Tables: []string{"macro_series"}`; комментарии таблицы и колонок (`date` — конец отчётного месяца; `region` — world/north_america/europe/asia/other; колонки — единицы и что NULL значит «метрика в этом месяце не введена»). SELECT витрин до коммита прогнать живьём через `mcp__clickhouse__run_query` (только чтение, без MANUAL_SERIES).
`views/gold_views.go`: `goldForecastsView` и `goldEtfFlowsView` — в `goldViewsList` и в doc-комментарий пакета.
`views/forecast_accuracy.go`: комментарий таблицы и колонок `actual`/`error_pct`/`is_resolved` — назвать механизм (`forecast_resolve`: окно-квартал, comex_front → moex_fix_usd; `nav` — факт не определён).

- [ ] **Step 4: Сиды и гранты**

`sql/wgc_etf_flows_catalog.sql` — 20 записей `series_catalog` (`source='webbridge'`), по шаблону: title `«WGC Gold ETF: <что>, <регион>»`, unit (`млн USD` / `тонны` / `тонны` / `млрд USD`), frequency `M`, origin `WGC Gold ETF Flows (monthly), gold.org/goldhub — сессионный сбор (WebBridge/ручной ввод), POST /v1/manual_series source='webbridge'`, description — что измеряет, знак, конец месяца, связь с `etf_flows_month` (для мир-потока в USD) и периодичность. Пример строки (остальные — по этому образцу):

```sql
('webbridge', 'etf_flow_usd_mln_world', 'WGC Gold ETF: чистый поток, мир', 'млн USD', 'M',
 'WGC Gold ETF Flows (monthly), gold.org/goldhub — сессионный сбор (WebBridge/ручной ввод), POST /v1/manual_series source=''webbridge''',
 'Чистый месячный поток физически обеспеченных золотых ETF, млн USD: знак — направление (плюс приток, минус отток). Дата — последний день отчётного месяца. Мировой итог; не путать с etf_flows_month (source=''manual'', млрд USD — индикатор v_gold_dashboard). Обновлять после месячного отчёта WGC (~10-е число), ввод одним POST всех 20 рядов'),
```

`sql/gold_forecasts_q4_2026_seed.sql` — `CREATE TABLE IF NOT EXISTS gold_forecasts (…дословная копия DDL из схемы ingest…);` + комментарий-источник (статья 07.10.2026, § «Сравнение с консенсусом аналитиков»; own-точки = середины, взвешенная 4 285 совпадает с прогоном 10.10) + INSERT 11 строк из спеки §2 (goldman YE-2026/2027, hsbc YE-2026/2027, jpm Q4-2026/2027, ubs 12M, citi short, own bull/base/bear Q4-2026 с probability 25/40/35; пустые поля — 0).
`sql/mcp_kimi_reader.sql` — `GRANT SELECT` на `v_gold_forecasts` и `v_gold_etf_flows`.

- [ ] **Step 5: Живой прогон**

1. `make import STAT=gold_views` → в логе `v_gold_forecasts created: true`, `v_gold_etf_flows created: true`; **внимание:** порядок — сид `gold_forecasts` применить до этого шага, иначе витрина пропустится (гейт `CreateView`).

Порядок шагов: `make ch-sql FILE=sql/gold_forecasts_q4_2026_seed.sql` → `make ch-sql FILE=sql/wgc_etf_flows_catalog.sql` → `make import STAT=gold_views` → `make mcp-user`.
2. MCP: `SELECT author, horizon, scenario, low, high, point, midpoint, probability FROM v_gold_forecasts ORDER BY author, horizon, scenario` — 11 строк; для `goldman/2027/range` `midpoint = 5500`, `point = NULL`; для `own/Q4-2026/bull` `midpoint = 4800` (Review Focus 5).
3. MCP: `SELECT count() FROM v_gold_etf_flows` — читается (0 строк допустимо до ввода значений).
4. MCP: `SELECT series, unit, frequency FROM v_series_catalog WHERE source = 'webbridge'` — 20 строк; `SELECT count() FROM gold_forecasts` — ACCESS_DENIED (сырая закрыта).

- [ ] **Step 6: Commit**

```bash
git add views/ sql/ && git commit -m "views: add v_gold_forecasts and v_gold_etf_flows with catalog and consensus seed"
```

---

### Задача 4: `scripts/mcp_check.py` — новые проверки

**Files:**
- Modify: `scripts/mcp_check.py`

**Interfaces:**
- Consumes: функции `text`, `expect`, `result_rows` (уже в файле).

- [ ] **Step 1: Проверки (после блока EIA, до проверки сырых таблиц)**

- `v_gold_forecasts` и `v_gold_etf_flows` видны в `list_tables`;
- `SELECT count() FROM v_gold_forecasts` → строки есть и count > 0 (detail-подсказка: «применить сид `sql/gold_forecasts_q4_2026_seed.sql`»);
- `SELECT count() FROM v_gold_etf_flows` → запрос читается (count может быть 0 — это не провал, проверяется РАЗРЕШЕНИЕ, как у `v_dcf_assumptions`);
- `SELECT count() FROM v_series_catalog WHERE source = 'webbridge'` → ровно 20 (подсказка: каталог-сид);
- `SELECT countIf(is_resolved = 1) AS resolved FROM v_forecast_accuracy` → resolved ≥ 1 (подсказка: бэкфилл + `make import STAT=forecast_resolve`);
- `SELECT count() FROM gold_forecasts` → ACCESS_DENIED / Not enough privileges (сырая закрыта).

- [ ] **Step 2: Живой прогон и коммит**

Run: `make mcp-check` → `ПРОВЕРКА ПРОЙДЕНА`.
`git add scripts/mcp_check.py && git commit -m "scripts: extend mcp-check with forecasts, ETF flows and resolution checks"`

---

### Задача 5: документация

**Files:**
- Modify: `ARCHITECTURE.md`, `README.md`

**Skill:** `sync-readme-architecture` (обязателен для этой задачи).

- [ ] **Step 1: ARCHITECTURE.md**

- §6.1: строка таблицы импортёров `forecast_resolve` (`forecast/`, статус, источник `forecast_log`+`gold_prices`, метод: окно-квартал, comex→moex, запись только при изменении) + `forecast_resolve` в перечень `Name()` + в абзац про WGC/CME: у WGC-семейства теперь есть структурные ряды (`source='webbridge'`, каталог-сид, `v_gold_etf_flows`), FedWatch — по-прежнему ручной.
- §6.2: `gold_forecasts` — из «не реализованы» в реализованные (копия DDL в `ingest/schema.go` и в сиде; `scenario` включает `'range'`); в блоке DDL обновить комментарий `scenario`; обновить упоминания «не реализовано».
- §6.3: `v_gold_forecasts`, `v_gold_etf_flows` (состав, источник, приоритет webbridge > manual); `v_forecast_accuracy` — дополнить: `actual/error_pct` заполняет `forecast_resolve`.
- §6.5: ingest — пять таблиц `EnsureTables`, endpoint `POST /v1/gold_forecasts` (контракт, валидация, ответ).
- §6.5: строка про «ряды WGC (ETF-потоки) и CME FedWatch импортёров не имеют» — уточнить: WGC — структурные ряды через сессионный канал + каталог; FedWatch — без изменений.
- Обновить строку «Последнее обновление» в шапке.

- [ ] **Step 2: README.md**

Импортёры (`forecast_resolve`), расписания (dagu), команды (`make import STAT=forecast_resolve`), витрины (2 новых), MCP-раздел (гранты/проверки), ingest-endpoint (третий POST), роадмап-раздел при необходимости.

- [ ] **Step 3: Commit**

```bash
git add ARCHITECTURE.md README.md && git commit -m "docs: sync readme and architecture with phase 3"
```

---

### Задача 6: приёмка фазы и статус роадмапа

**Files:**
- Modify: `docs/GOLD_THESIS_DATA_ROADMAP.md`

- [ ] **Step 1: Полная приёмка (repeatable, сверху вниз)**

1. `make all` — зелёный.
2. Сиды: `make ch-sql FILE=sql/gold_forecasts_q4_2026_seed.sql`, `…wgc_etf_flows_catalog.sql`, `…forecast_log_backfill.sql`; `make import STAT=gold_views`; `make mcp-user`.
3. `make import STAT=forecast_resolve` → 1 строка; повторно → 0 строк.
4. Живой ingest (перезапустить процесс с новым бинарником): `POST /v1/manual_series` c `source='webbridge'` — пять точек из статьи: `2026-06-30` `etf_flow_usd_mln_world = -8900`, `etf_aum_usd_bn_world = 526`; `2026-08-31` `etf_flow_usd_mln_world = 18000`, `etf_holdings_tonnes_world = 4189`; `2026-09-30` `etf_flow_usd_mln_world = 10000`. Затем MCP: `SELECT * FROM v_gold_etf_flows ORDER BY date DESC` → три строки; у 2026-09-30 заполнен только `flow_usd_mln` (Review Focus 4 — остальное NULL).
5. Живой `POST /v1/gold_forecasts` (2 строки, идентичные сиду — идемпотентно) → `200 {"inserted":2}`.
6. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
7. MCP-сводка: `v_forecast_accuracy` (Q3 resolved/ошибка, Q4 pending), `v_gold_forecasts` (консенсус), `v_series_catalog source='webbridge'` (20).
8. EnsureTables: `make run-ingest` на одноразовой БД (опционально: создать scratch-базу, дождаться `ingest listening`, проверить наличие `gold_forecasts` в ней, удалить базу) — либо отметить, что DDL покрыт копией в сиде и юнит-тестами.

- [ ] **Step 2: Статус роадмапа**

`docs/GOLD_THESIS_DATA_ROADMAP.md`: заголовок фазы → `✅ выполнена 2026-10-11`; блок «Реализовано» (ветка, спека) + «Результат — приёмка» с фактическими числами и командами (по образцу фаз 1–2); отметить ограничения (Q4 — резолв после 2026-12-31; `nav` — без факта; staging-шаги вручную).

- [ ] **Step 3: Commit**

```bash
git add docs/GOLD_THESIS_DATA_ROADMAP.md && git commit -m "docs: phase 3 done — ETF flows series, consensus table, forecast resolution"
```

- [ ] **Step 4: Финальные скилы**

`superpowers:requesting-code-review` (вся ветка, самая способная модель) → правки → `superpowers:verification-before-completion` → один PR (спека, план, код, SQL, docs).

---

## Самопроверка плана (выполнена при написании)

- Покрытие спеки: §1 → Задача 3 (+4), §2 → Задачи 1, 3, §3 → Задача 2, приёмка → Задача 6; правило 11 → Задачи 3, 4, 5.
- Типы согласованы: `GoldForecast` (задача 1) ↔ DDL сида (задача 3); `forecastResolution` (задача 2) ↔ колонки `forecast_log`; имена витрин совпадают в задачах 3, 4, 5.
- Review Focus: 1 → шаг тестов задачи 1; 2 → шаг тестов задачи 2; 3 → шаг тестов/прогона задачи 2; 4 → живой прогон задачи 6; 5 → живой прогон задачи 3.
