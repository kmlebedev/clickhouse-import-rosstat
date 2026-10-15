# Task 3 report (reconstructed by controller)

Implementer agent-6 was killed by a quota limit before writing its report.
Code state: Steps 1-7 implemented, uncommitted in the working tree.

Evidence available:
- `gofmt -l ingest cmd` — clean (run by user)
- `go test ./ingest/` — ok 0.536s (run by user)

Not done yet: Step 8 (`make all`, live curl POST /v1/model_run, duplicate run_id check), Step 9 (commit).
Reason: Go toolchain and ClickHouse are unavailable in the agent environments (org egress policy); Step 8 must be run on the user's Mac.

## Fix round 1

Review findings 1, 2, 4 fixed (finding 3 deferred per controller). Commit: see git log (`fix ingest dedupe ordering and input validation`).

### Finding 1: racy idempotency, lost derived rows
- `ingest/clickhouse.go:ClickHouseWriter` — new `mu sync.Mutex` field and `store chStore` field (seam, see finding 4).
- `ingest/clickhouse.go:InsertModelRun` — holds `w.mu` for the whole call; the `runExists` pre-check still gates the whole write; write order is now `forecast_log` first, `model_runs` last, so a `model_runs` row is the commit marker. The doc comment explains why: `ORDER BY (run_date, run_id)` in ReplacingMergeTree does not collapse duplicate POSTs because `run_date` differs.
- `ingest/clickhouse.go:connStore`, `chStore`, `chBatch` — narrow seam over `driver.Conn` (`runExists` = `count() FROM model_runs FINAL`, `prepareBatch` = `PrepareBatch`). `EnsureTables` and `InsertManualSeries` still use `w.conn` unchanged.

### Finding 2: client input errors returned as 500
- `ingest/model_run.go:ValidateModelRun` — added `uuid.Parse(run_id)` (400, "must be a UUID"), `gold_scenario == nil` (400), `scenarioTargetDate(gold_scenario)` (400 on missing/non-string/unparsable horizon), and `weightedGoldPoint(gold_scenario, probabilities)` (400 when a probability key has no block or a block has neither `point` nor `low`/`high`). It reuses the same helpers `InsertModelRun` calls.
- `ingest/clickhouse.go:scenarioTargetDate` — the year branch now uses `yearHorizonRe = ^(?:YE-)?(\d{4})$` instead of `TrimPrefix`+`Atoi`. This is a small tightening: values like `-123` now error instead of being accepted. Accepted forms are unchanged (`Q<n>-YYYY`, `YE-YYYY`, `YYYY`, `YYYY-MM-DD`).
- `ingest/server.go` is unchanged; `InsertModelRun` errors now only come from DB failures, so they stay 500.

### Finding 4: tests
- `ingest/model_run_test.go` — `validModelRun()` now sets `GoldScenario` (new helper `validGoldScenario()`), since validation requires it. New `TestValidateModelRunInputErrors` (table): bad UUID, gold_scenario nil, horizon missing, horizon not a string, horizon unparsable, probability key with no gold block, block with no point/range.
- `ingest/clickhouse_test.go` (new file):
  - `TestScenarioTargetDate` (table): Q1-2026 → 2026-03-31, Q4-2026 → 2026-12-31, YE-2026, 2026, 2026-12-31.
  - `TestScenarioTargetDateErrors` (table): missing horizon, numeric horizon, Q5-2026, free text, YE-26, -123, 2026-02-30.
  - `TestWeightedGoldPoint` (table): point wins over midpoint, midpoint when no point, midpoint mixed with point, weights divided by their sum (not by 100), plus error cases (probability key without block, block without point/range, non-object block, empty probabilities).
  - `TestInsertModelRunExistingRunSkipsAllBatches`: an existing run_id causes no batch at all.
  - `TestInsertModelRunWritesForecastLogBeforeModelRuns`: batch order is forecast_log then model_runs; checks `nav` and `xau_q_avg` rows, predicted values (12500, 4285) and target_date 2026-12-31.
  - `TestInsertModelRunForecastFailureIsRetried`: a forecast_log failure leaves no model_runs row, and the retry writes both.
  - `TestInsertModelRunConcurrentRetriesWriteOnce`: 8 concurrent POSTs with one run_id produce exactly one model_runs batch (fails without the mutex, because the fake sleeps inside `runExists` to widen the race window).

### Verification
- Go is not usable here: `/usr/local/go/bin/go` tries to download toolchain go1.27.2 and the network blocks it. Nothing was compiled and no tests were run.
- `gofmt -l` (the local gofmt binary) reports the four changed files as clean.
- Commands the user must run:
  - `go test -race ./ingest/`
  - `make all`
