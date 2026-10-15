### Task 3: Ingest-endpoint (пакет ingest + cmd/ingest)

**Files:**
- Create: `ingest/model_run.go` (типы + валидация)
- Create: `ingest/server.go` (HTTP-сервер, хендлеры, запись в CH)
- Create: `ingest/schema.go` (EnsureTables: DDL `model_runs`, `forecast_log`)
- Create: `ingest/model_run_test.go`, `ingest/server_test.go`
- Create: `cmd/ingest/main.go`
- Modify: `Makefile` (цель `build-ingest`)

**Interfaces:**
- Consumes: `clickhouse.ParseDSN(os.Getenv("CLICKHOUSE_URL"))` как в `main.go`; канонические DDL `model_runs`, `forecast_log`, `macro_series` из ARCHITECTURE.md §6.2 (копировать как есть).
- Produces:
  - `type ModelRun struct { RunID string `json:"run_id"`; TriggerType string `json:"trigger_type"`; TriggerRef string `json:"trigger_ref"`; GoldScenario map[string]any `json:"gold_scenario"`; UsdrubPath map[string]any `json:"usdrub_path"`; PriceDeck string `json:"price_deck"`; DiscountRate float64 `json:"discount_rate"`; Wacc float64 `json:"wacc"`; NavPerShare float64 `json:"nav_per_share"`; NavBull, NavBase, NavBear float64; MarketPrice float64 `json:"market_price"`; UpsidePct float64 `json:"upside_pct"`; Comment string `json:"comment"`; Probabilities map[string]float64 `json:"probabilities"` }`
  - `func ValidateModelRun(r ModelRun) error` — price_deck ∈ {spot_flat, consensus_lt, own_scenario}; trigger_type ∈ {calendar, news, manual}; sum(probabilities) ∈ 100±0.1; run_id обязателен (клиентский, для идемпотентности — Review Focus #2).
  - `type ManualSeriesPoint struct { Series string `json:"series"`; Date string `json:"date"`; Value float64 `json:"value"` }` (source фиксирован `'manual'`; Date формата `YYYY-MM-DD`, Date32 — даты до 1970 валидны, Review Focus #3).
  - HTTP: `POST /v1/model_run`, `POST /v1/manual_series` (массив точек), оба с `Authorization: Bearer $INGEST_TOKEN`; 200 `{ "inserted": N }`, 400 с текстом ошибки валидации, 401 без/с неверным токеном.

- [ ] **Step 1: Написать failing-тесты валидации `ingest/model_run_test.go`**

Тесты: валидный run (probability bull 25/base 40/bear 35) проходит; сумма 90 → ошибка; `price_deck="wacc_guess"` → ошибка; пустой `run_id` → ошибка; `trigger_type="cron"` → ошибка.

- [ ] **Step 2: Запустить тесты — убедиться, что падают**

Run: `go test ./ingest/ -run TestValidateModelRun -v`
Expected: FAIL (функция не определена).

- [ ] **Step 3: Реализовать `ValidateModelRun` в `ingest/model_run.go`**

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./ingest/ -v`
Expected: PASS.

- [ ] **Step 5: Написать failing-тесты HTTP-слоя `ingest/server_test.go`**

`httptest.NewServer` + поддельный writer-интерфейс (интерфейс `BatchWriter { EnsureTables(ctx) error; InsertModelRun(ctx, ModelRun) error; InsertManualSeries(ctx, []ManualSeriesPoint) error }`, мок в тесте): без токена → 401; невалидный JSON → 400 и мок не вызван; сумма вероятностей 90 → 400; валидный run → 200 и мок вызван с распарсенным `run_id`; `Date="1947-01-01"` в manual_series проходит валидацию (Date32).

- [ ] **Step 6: Реализовать `ingest/server.go`, `ingest/schema.go`, `ingest/clickhouse.go`**

`EnsureTables` — три канонических DDL `IF NOT EXISTS` (`model_runs`, `forecast_log`, `macro_series`); вставки только `PrepareBatch`/`Append`/`Send`. В `InsertModelRun`: `run_id` берётся из запроса (не DEFAULT), `gold_scenario`/`usdrub_path` сериализуются в JSON-строки; из того же run'а пишутся производные строки `forecast_log` — `metric='nav'` (predicted = `nav_per_share`) и `metric='xau_q_avg'` (predicted = вероятностно-взвешенная точка из `gold_scenario`/`probabilities`), `target_date` = конец горизонта сценария, `actual`/`error_pct` = NULL.

- [ ] **Step 7: `cmd/ingest/main.go` + цель Makefile**

env: `CLICKHOUSE_URL`, `INGEST_TOKEN` (пусто → Fatal), `INGEST_ADDR` (default `:8081`). Makefile: `build-ingest: go build -o build/ingest ./cmd/ingest`.

- [ ] **Step 8: `make all` зелёный + живой прогон**

Run: `make all`; затем `make`-обёрткой запустить `build/ingest` и `curl -X POST localhost:8081/v1/model_run` с валидным JSON.
Expected: 200, строка видна в `model_runs FINAL`; повторный POST с тем же `run_id` не создаёт вторую строку.

- [ ] **Step 9: Commit**

```bash
git add ingest/ cmd/ingest/ Makefile
git commit -m "add ingest endpoint for model_runs and manual series"
```

