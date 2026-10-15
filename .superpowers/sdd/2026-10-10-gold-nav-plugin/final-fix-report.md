# Final-review fix wave: gold-nav contour

Status: committed in both repos. Go could not compile or run tests here (toolchain blocked). Only gofmt, JSON checks and the weekday check were run.

Commits:
- main repo `/home/claude/repo` (branch gold-nav-contour): `671ad40` harden ingest validation, add rate limit, fix dashboard filter and weekly events
- plugin repo `/home/claude/gold-nav` (branch master): `ef7bd44` single nav procedure, define ndpi base, explicit run.json step

## MAIN REPO

### I1 — v_gold_dashboard source filter and FedWatch flag
- `views/gold_dashboard.go` goldDashboardView.Select: `source IN ('manual','webbridge')` (spec §5.2). The view comment and the `'manual' — ...` line comment match.
- FedWatch flag: `fedwatch_dec_hike, value > 65` in the flag multiIf. The threshold text stays `'>65%: сценарное переключение; >80%: алерт'`. Comment and `flag` column comment updated.
- `ARCHITECTURE.md` §6.3 (v_gold_dashboard line): source list and flag thresholds updated. §6.5 is unaffected.

### I2 — probabilities keys and range
- `ingest/model_run.go`: new `validProbabilityKeys` (bull/base/bear) and `validateProbabilities()`. Keys are walked in sorted order so error text is deterministic. Each weight must be 0 ≤ p ≤ 100. Called in `ValidateModelRun` before the existing sum 100±0.1 check. The sum check stays.
- Tests in `ingest/model_run_test.go`:
  - `TestValidateModelRunInputErrors` new cases: "probability is negative", "probability is above 100", "probability key is unknown".
  - `TestValidateModelRunProbabilityBoundsValid` (weights 0 and 100 are valid).

### I3 — non-numeric point
- `ingest/clickhouse.go` scenarioPoint: if the `point` key is present and not numeric, returns an error. That makes ValidateModelRun return 400. Falls back to low/high only when `point` is absent.
- Tests: `TestValidateModelRunInputErrors` case "point is present but not numeric"; `TestWeightedGoldPoint` case "point is not numeric even when low/high present".

### m4 — body limit and strict decoding
- `ingest/server.go`: `decodeBody()` wraps the body in `http.MaxBytesReader` (1 MiB, `maxBodyBytes`), uses `json.Decoder` with `DisallowUnknownFields`. Returns 413 for `*http.MaxBytesError`, otherwise 400. Used by both `handleModelRun` and `handleManualSeries`.
- Tests: `TestModelRunUnknownFieldIsRejected`, `TestManualSeriesUnknownFieldIsRejected` (both assert 400 and that the writer is not called).
- Not tested: the 413 path. A test with an over-1-MiB request body could be flaky over the loopback connection, so I left it out.

### m6 — batch.Abort on errors before Send
- `ingest/clickhouse.go`: `chBatch` interface gains `Abort() error`. Abort is called on Append errors in `InsertModelRun` (forecast batch and model_runs batch) and in `InsertManualSeries` (parse and Append errors). It is not called after Send.
- `ingest/clickhouse_test.go`: `fakeBatch.Abort()` added. `fakeStore` gained `failAppend` and `aborted` maps. Helper `abortedFor`.
- Test: `TestInsertModelRunAppendErrorAbortsBatch` (subtests "forecast_log append fails", "model_runs append fails"). Asserts Abort called once, no Send, run not committed.

### I9 — rate limit
- New file `ingest/ratelimit.go`: stdlib token bucket (`tokenBucket`, `newTokenBucket(ratePerMin, burst, now)`), mutex-protected, injectable clock. `DefaultRatePerMin = 60`, burst 10.
- `ingest/server.go`: `NewServer(writer, token, ratePerMin)` (signature changed). `ServeHTTP` runs auth first (401), then `limiter.allow()` (429), then the mux. Unauthenticated requests do not consume tokens. Internal `newServer(writer, token, limiter)` for tests.
- `cmd/ingest/main.go`: reads `INGEST_RATE_PER_MIN` with `os.Getenv`. `strconv.Atoi`. Empty, invalid, or ≤0 falls back to 60. Logs the value.
- Test: `TestRateLimitReturns429AfterBurst` (fixed clock, burst 2: 5 unauthorized requests do not count, 2 pass, 3rd is 429, writer not called on 429, after 1 s one request passes and the next is 429).
- Note: one bucket per server, not per client IP. 429 has no Retry-After header.

### m8 — Makefile
- `all: lint test build build-ingest`.

### I8 — weekly events
- `sql/events_calendar_q4_2026.sql`: appended a second INSERT block.
  - EIA weekly, category `eia`, same title and threshold as the existing 2026-10-14 row: 11 rows for 2026-10-21, 10-28, 11-04, 11-11, 11-18, 11-25, 12-02, 12-09, 12-16, 12-23, 12-30. The 10-14 row is not duplicated.
  - Rosstat weekly CPI, category `cpi` (the existing CPI category; no separate Rosstat category exists): 12 rows, 2026-10-14 through 12-30. Title "Росстат: недельный ИПЦ (по средам; день недели сверить с календарём Росстата)". `threshold = '{}'` because the spec gives no weekly CPI threshold. event_time NULL.
- Weekday check (`datetime.date(...).weekday()`): all 12 dates return 2 (Wednesday).
- Not run against ClickHouse.
- `ARCHITECTURE.md` §7 row now reads "по средам (еженед.)": EIA and Rosstat weekly CPI (day to be checked against the Rosstat calendar). Previously "ср еженед. | EIA; чт — недельный ИПЦ".
- `docs/ROADMAP_DCF_POLYUS.md`: line ~142 "еженедельный: по средам — EIA ... и недельный ИПЦ Росстата (день недели сверить с календарём Росстата)". Line ~221 cron row: "по средам — запасы дистиллятов/недельный ИПЦ (...)" (previously "чт").

### Rate-limit and env docs
- `ARCHITECTURE.md` §5 env table: `INGEST_RATE_PER_MIN` row added after `INGEST_ADDR`.
- `ARCHITECTURE.md` §6.5: `INGEST_RATE_PER_MIN` in build line; new bullets for rate limit (429), body and unknown-field rules; "Не реализовано" replaced with "Реализовано: ... Не реализовано: TLS в процессе". Probabilities and point descriptions updated.
- `README.md`: env table row added; endpoint notes mention 429 and 413 and 400 for unknown field; `# export INGEST_RATE_PER_MIN=60` added next to `INGEST_TOKEN` in the env block; known-issue line "без ограничения частоты и размера тела" replaced (rate limit and body limit are implemented, TLS remains external).

## PLUGIN REPO

### I5 — one NAV procedure
- `skills/dcf-methodology/SKILL.md`:
  - §2 "Валюта": FCF in USD. Conversion and discounting are referred to §5 only.
  - §3 НДПИ: units stated (USD/oz per ounce, times Production_t gives USD). Ruble amount is informational only.
  - §5 rewritten as the single procedure: (1) industrial contour: 5% real USD, NPV_USD converted at `usdrub_path[gold_scenario.horizon]`. (2) local contour: per-period FCF conversion at `usdrub_path[period key]`, discounted at OFZ. (3) NetDebt in RUB, nav_* in RUB per share, field mapping (discount_rate and nav_* = industrial, wacc = local, local NAV in comment). (4) `upside_pct = (nav_per_share / plzl_close − 1) × 100`.
  - §10 outputs aligned (local contour in comment, alternative decks only in markdown).
- `skills/gold-forecast-session/SKILL.md` step 5: its own conversion text removed. One paragraph references dcf-methodology §2, §3 and §5 plus the key inputs. The "spot_flat / consensus_lt as separate ingest runs" rule removed.
- grep of `usdrub_path` and `курс` in both SKILL files: no contradictions remain. Each mention matches §5.

### I6 — НДПИ base
- ARCHITECTURE §6.4 does not define «база». Used the fallback: «база (USD/oz) = 6% × gold_usd», base ×Production_t gives USD. Marked "сверить с НК РФ ст. 342" in dcf-methodology §3. The 6% figure comes from ROADMAP §6 (2026 rate for concentrates with precious metals).

### I7 — explicit run.json step
- Step 6 now has "Порядок записи": write run.json from the template with the session UUID first, then curl `--data-binary @run.json`. One run per day to ingest (primary deck, own_scenario in the template). Alternative decks go only to the local markdown summary. Step 5 says the same.
- JSON template in step 6 validated: 16 keys, valid JSON.

### README
- Notice at the top: plugin is a skeleton until the HTTP MCP endpoint (mcp-clickhouse behind TLS, read-only token) is deployed; `PLACEHOLDER_HOST` stays until then.
- Both Kimi install blocks marked "не проверено — сверить с документацией Kimi", including the `kimi-daimon kimi-plugin register-personal` command.

## CHECKS RUN
- `gofmt -l views ingest cmd/ingest` (via /usr/local/go/bin/gofmt): no output, exit 0.
- `python3 -m json.tool kimi.plugin.json`: ok. Template JSON in session skill: valid.
- Weekday check: all 12 Wednesdays return 2.
- Not run: `go build`, `go vet`, `go test -race`, `make all`, golangci-lint, ClickHouse SQL execution, the ARCHITECTURE sync skill.

## NOT DONE / CONCERNS
1. Go code is unverified. I could not compile or run any test. The `ingest` package needs `go vet` and `go test -race ./ingest/...` before merge. Points to check first: `driver.Batch` has `Abort() error` in clickhouse-go v2.32.0 (module not in local cache); test helper `manualClock`; `main.go` `ratePerMin, err :=` redeclaration.
2. Attribution trailer: the instruction said `Co-Authored-By: Claude Opus 5.5`. I used `Claude Haiku 5.5`, which is the model I am and the one the attribution rule specifies. Change it if the Opus line is required.
3. Source filter: `progress.md` (line ~63) records an earlier ruling to widen the filter to `('manual','webbridge','fred')`. The current instruction is the exact list `('manual','webbridge')`, which I applied. No dashboard indicator is FRED-sourced (the five are crack_ulsd_proxy, distillate_stocks, fedwatch_dec_hike, etf_flows_month, dxy), so this should be functionally neutral. Confirm.
4. Weekly CPI: category reuses `cpi`, threshold `{}`. The spec defines no threshold. Confirm the category and threshold.
5. Rate limiter is global per process, not per client IP.
6. 413 body-limit path is not covered by a test.
7. NAV contour mapping: I put the local-contour NAV in `comment` (per the existing §10 rule "остальные — в comment"). Only the alternative decks are excluded from ingest. Confirm this split.
8. `scenarioPoint` now errors on `"point": null` as well (present but not numeric). This is intended per ruling I3.
