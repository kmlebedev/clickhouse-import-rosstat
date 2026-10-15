# SDD ledger — plan: docs/superpowers/plans/2026-10-10-gold-nav-plugin.md

## Pre-flight scan (2026-10-10)

| # | Pair / Task | Producer → Consumer | Finding | Ruling |
|---|---|---|---|---|
| 1 | Task 2 × Task 4 | both modify `main.go` (blank imports) | No conflict: sequential, different import lines | — |
| 2 | Task 2 × Task 4 | both modify `sql/mcp_kimi_reader.sql` | No conflict: sequential GRANT appends | — |
| 3 | Task 3 → Task 4 | `model_runs` table → `v_model_inputs` | Dependency stated in plan: `gold_views` runs only after ingest first run created `model_runs`; CreateView skips views until all tables exist. Consistent | — |
| 4 | Task 3 → Task 5 | ingest contracts (`/v1/model_run`, `/v1/manual_series`, Bearer, client run_id) → skill `gold-forecast-session` JSON template | Field list in Task 3 Interfaces is the single source; Task 5 must mirror it verbatim | — |
| 5 | Task 4 → Task 5 | views `v_model_inputs`, `v_gold_dashboard`, `v_events_calendar` → skill steps 1/3 | Consistent | — |
| 6 | Task 3 self-consistency | Files list vs Step 6: Step 6 creates `ingest/clickhouse.go`, absent from Files header | Cosmetic; file is required by Step 6 text | Ruling: `ingest/clickhouse.go` is in scope for Task 3 — Step 6 explicitly names it — cost if wrong: none, file is needed regardless |
| 7 | Task 3 self-consistency | Step 6 writes `forecast_log` rows from InsertModelRun; spec/plan reviewed & amended pre-commit | Consistent (noted in plan Step 6) | — |
| 8 | Review Focus #2 vs Task 3 | client-supplied `run_id` required; server accepts it; ReplacingMergeTree collapses dupes | Validation `run_id` обязателен — matches | — |
| 9 | Tasks 2–4 vs Global Constraints | live ClickHouse needed (`make import`, seed sql, live ingest run) | ClickHouse must be up (user runs `make ch-up`); verify `make ch-status` before Task 2 | — |
| 10 | All tasks vs Global Constraints | batch-only inserts, secrets via os.Getenv, DDL canonical, make-only commands | No plan text contradicts constraints | — |

Scan complete; no blocking conflicts. One ruling recorded (row 6).

Task 1: minor (deferred): §7 phase-1 cell leads with «Исторический базлайн» instead of phase purpose (cosmetic)
Task 1: minor (deferred): §2.2 table cells text-heavy (3 claims per cell); could split «источник»/«доставка» (cosmetic)
Task 1: minor (deferred): `:57` macro_series origin parenthetical still lists EIA — slightly stale vs manual-input decision
Task 1: complete (commits b071260..619ca2e, review clean)

Ruling: Task 2 may add a minimal Makefile target `ch-sql FILE=...` to apply the seed SQL — the plan says "clickhouse-client --multiquery через make-обёртку" but no such wrapper exists; AGENTS.md forbids running ClickHouse-touching commands outside make — cost if wrong: one small extra Makefile target, trivially revertable

Task 2: minor (deferred): polyus_ir category advertised in comments but never seeded in Q4 (harmless)
Task 2: minor (deferred): two seed rows missing alignment space after category (cosmetic)
Task 2: minor (deferred): NFP Nov (2026-12-04) and WGC GDT (2026-10-30) dates are plausible but unverified — plan-mandated ambiguity, disclosed by implementer
Task 2: complete (commits 619ca2e..2d43601, review clean)

Task 3: NOTE — implementer (agent-6) was killed mid-flight by a 5-hour quota limit (403). Work left uncommitted in the tree (ingest/*.go, cmd/ingest/main.go, Makefile, go.mod). Test package passes locally. Resume agent-6 when quota resets to finish Steps 8-9 (make all, live run, commit).

Task 3: BLOCKED ON QUOTA (not a work blocker) — 2026-10-10. Controller resumed agent-6; resume rejected with the same 403 5-hour-limit error, and a fresh dispatch cannot start either. Work left in the tree, uncommitted:
  - new: ingest/{clickhouse.go,model_run.go,model_run_test.go,schema.go,server.go,server_test.go}, cmd/ingest/main.go
  - modified: Makefile, go.mod
  - verified by controller: `gofmt -l ingest cmd` clean; `go test ./ingest/` ok (0.536s)
  - NOT done: `make all`, live-run curl evidence, duplicate-run_id check, Task 3 commit, task-3-report.md, task review.
Resume point: re-dispatch implementer for Task 3 Steps 8-9 + report (or resume agent-6 once the window resets), then run the standard task review. Tasks 4-7 + final review not started. Briefs for Tasks 4 and 5 are already generated (task-4-brief.md, task-5-brief.md).

## Resume 2026-10-10 (session 2, controller on Opus, subagents on haiku per user)
Ruling: Step 8 verification (make all, live curl, dup run_id) runs on the user's Mac — Go/ClickHouse downloads are blocked by org egress policy in both agent environments; code work and reviews proceed in a cloud copy, synced back via git bundle — cost if wrong: fixes are unverified by compiler until the user runs `make all`
Task 3: review (haiku) — spec ❌ (dedupe mechanism), Needs fixes. Important: (1) racy dedupe + forecast_log lost on retry, (2) client input errors → 500, (3) plan-mandated forecast_log ORDER BY (metric, forecast_date) loses intra-day history, (4) no tests for dedupe/derived logic
Task 3: Ruling: finding 3 (forecast_log ORDER BY collapses same-day runs) — keep canonical DDL from ARCHITECTURE.md §6.2; schema changes must start in ARCHITECTURE.md (AGENTS.md rule 8); surfaced to user — cost if wrong: earlier same-day forecast silently replaced in forecast_log
Task 3: Ruling: dedupe via in-process mutex + count() pre-check, forecast_log written before model_runs (model_runs = commit marker) — spec's premise that ReplacingMergeTree collapses dup POSTs is false because run_date is in ORDER BY; ingest is single-instance — cost if wrong: duplicates if ingest ever runs multi-instance
Task 3: minor (deferred): no request body size limit (server.go)
Task 3: minor (deferred): duplicate run returns inserted:1
Task 3: minor (deferred): per-scenario probability not range-checked [0,100]
Task 3: minor (deferred): dead json.Number branch in toFloat
Task 3: minor (deferred): forecastDate built from local time, labelled UTC
Task 3: minor (deferred): Atoi accepts sign in YE-/year horizon
Task 3: minor (deferred): no WriteTimeout/IdleTimeout; startup calls without timeout ctx
Task 3: minor (deferred): import grouping in server.go
Task 3: committed original code as 03347a8 (worktree content, unchanged)
Task 3: fix round 1/5 (3 addressed, 0 open; commits 03347a8..383362b) — re-review (haiku): no compile errors found by inspection
Task 3: minor (deferred): clickhouse.go comment implies RMT collapse is immediate (needs FINAL)
Task 3: minor (deferred): scenarioPoint falls back to midpoint when `point` is non-numeric → accepted instead of 400
Task 3: minor (deferred): literal &ClickHouseWriter{} leaves store nil (constructor-only path)
Task 3: PENDING USER — Step 8 on Mac: go test -race ./ingest/, make all, live POST + duplicate run_id check. Not marked complete until evidence arrives.
Task 3: Step 8 verified on user's Mac (task3-verify.log): make all green, POST x2 → model_runs=1 (dup skipped), bad uuid 400, no token 401, forecast_log nav=12500 xau_q_avg=4285
Task 3: complete (commits 2d43601..383362b, review clean after fix round 1)
Task 4: implemented 94151d9 (haiku, cloud copy); review (haiku): spec ✅, Approved, no Critical/Important
Ruling: v_gold_dashboard source filter — spec §5.2 says manual/webbridge, brief says manual/fred; spec is binding, so the final fix wave widens it to IN ('manual','webbridge','fred') (superset keeps brief's FRED-backed dxy) — cost if wrong: none functional, one extra literal
Task 4: minor (deferred): ipc_week_ytd is an unweighted mean of per-product YTD changes (approximation, disclosed in comment)
Task 4: minor (deferred): FedWatch flag uses >80, >65 only in threshold text; unit assumed percent
Task 4: minor (deferred): v_gold_dashboard has no ORDER BY
Task 4: minor (deferred): gold_views exits 0 with info log when a source table is missing (created:false easy to miss)
Task 4: minor (deferred): extra *_date columns per series beyond spec list (brief-mandated argMax(date,date))
Task 4: PENDING USER — task4-verify.sh (make all, make import STAT=gold_views, SELECT checks)
Task 5: implemented gold-nav plugin 27e20fd (haiku, cloud /home/claude/gold-nav — to be copied to /Users/whitefox/GolandProjects/gold-nav); review: Needs fixes — (1) verify.md read non-existent ungranted v_forecast_log, (2) NAV currency unspecified
Task 5: Ruling: create canonical v_forecast_accuracy (ARCHITECTURE §6.3) over forecast_log FINAL in main repo views/, registered in gold_views, granted to kimi_reader; verify.md reads it — plan/spec had verify read forecast_log but kimi_reader cannot read raw tables — cost if wrong: one extra view to drop
Task 5: fix round 1/5 (2 addressed, 1 new — НДПИ currency §2 vs §3 contradiction; commits main 94151d9..4d5923f, plugin 27e20fd..7bbc075)
Task 5: Ruling: НДПИ base in USD (10%×max(gold_usd−1900,0) per oz), subtracted inside USD FCF; RUB amount reference-only; per-period FX = that period's usdrub_path key, single-number FX = gold_scenario.horizon key — matches ARCHITECTURE §6.4 USD formula — cost if wrong: NAV off by FX timing on НДПИ (small)
Task 5: fix round 2/5 (2 addressed, 0 open; plugin 7bbc075..4b88571)
Task 5: minor (deferred): §2/step 5 per-period RUB conversion sentence reads as applying to both contours (industrial should discount USD and convert NPV only)
Task 5: minor (deferred): "база" in НДПИ formula undefined; formula line lacks USD/oz qualifier
Task 5: minor (deferred): curl puts expanded ingest token in argv; step 6 never says to write run.json first
Task 5: minor (deferred): README workaround lacks installed-plugins path; dcf-methodology §12 omits polyus_ir and weekly CPI line
Task 5: minor (deferred): nothing fills forecast_log.actual/error_pct yet → verify shows is_resolved=0 everywhere; schema.go comment lists metrics not written
Task 5: complete (plugin 27e20fd..4b88571, main 4d5923f; review clean after 2 rounds) — PENDING USER Step 5 (kimi-daimon register-personal + Kimi Work check), PLACEHOLDER_HOST to be replaced by author
Task 6: implemented bd7a0bd (haiku); review: Needs fixes — (1) false claim that a Rosstat failure blocks v_model_inputs (3 places), (2) skill checklist item 6 (ROADMAP status) skipped
Task 6: Ruling: do ROADMAP item 6 minimally (status marks only) though the brief's Files list omits it — the project skill sync-readme-architecture requires it — cost if wrong: a few status marks to revert
Task 6: fix round 1/5 (2 addressed, 0 open; commits bd7a0bd..b56d96f)
Task 6: minor (deferred): README scripts/ described as "checks" though mcp_setup_user.py creates user+grants
Task 6: minor (deferred): §6.2 "DDL совпадает с кодом" sentence above block that includes unimplemented tables
Task 6: minor (deferred): Kimi CLI commands (kimi-daimon register-personal, /plugins install) stated as fact, unverified
Task 6: minor (deferred): README plugin <github-url> placeholder (no remote yet)
Task 6: complete (commits 4d5923f..b56d96f, review clean after 1 round)

## Final review (haiku) — With fixes
Ruling: C1 (HTTP MCP + TLS + per-user token not built, no plan task) — out of this branch's scope, surfaced to user; plugin README states it is a skeleton until the HTTP MCP is deployed — cost if wrong: one more plan task later, plugin unusable until then
Ruling: I1 v_gold_dashboard filter → IN ('manual','webbridge') per spec §5.2; 'fred' dropped (no FRED series named like the 5 indicators) — supersedes the earlier Task 4 ruling — cost if wrong: dxy from FRED would need a series alias
Ruling: I4 forecast_log keeps canonical ORDER BY (metric, forecast_date); skill records to ingest only one run per day (primary deck), alternative decks stay in the local markdown — schema change needs ARCHITECTURE-first decision by user — cost if wrong: alt-deck forecasts not tracked in forecast_log
Ruling: I5 single NAV procedure lives in dcf-methodology (industrial contour: USD FCF at 5% real, NPV converted at horizon key; local contour: RUB FCF via per-period usdrub_path at ОФЗ); session skill step 5 references it — cost if wrong: methodology text tweak
Ruling: I6 НДПИ «база» = base rate from ARCHITECTURE §6.4 if defined, else 6% of the value of extracted gold, marked «сверить с НК РФ ст. 342» — cost if wrong: NAV tax line off; flagged for owner
Ruling: I8 seed weekly EIA (Wednesdays to 2026-12-30) and weekly Rosstat CPI (Wednesdays) in events_calendar; align ARCHITECTURE §7 and roadmap weekday to Wednesday, marked to verify — cost if wrong: weekly CPI trigger off by a day
Ruling: I9 add a small stdlib in-process rate limiter to ingest (no new module deps) — spec §7 requires it — cost if wrong: trivial to tune/remove
Ruling: FedWatch flag threshold → >65 (scenario switch), >80 kept in threshold text — skill reads flag as "§6 threshold crossed" — cost if wrong: earlier flag
Final fix wave scope: I1 I2 I3 I5 I6 I7 I8 I9, FedWatch, m4 (DisallowUnknownFields+MaxBytesReader), m6 (abort batches), m8 (build-ingest in all), plugin README unverified-CLI + skeleton notice. I10 = user verification. Other minors: OK to leave per final-review triage.
Final fix wave: main 4d5923f..671ad40 (b56d96f..671ad40 = fix wave), plugin 4b88571..ef7bd44; scoped re-review (haiku): all items addressed, no Critical/Important, "ready to merge after user's verification run"
Final: parked (minor) — decodeBody accepts trailing JSON after first value; ratePerMin≤0 unguarded inside NewServer; 429 lacks Retry-After, limiter global per process; 413 path untested; Rosstat weekly CPI rows share category 'cpi' with US CPI; НДПИ 6% base assumption flagged for НК РФ ст. 342 check — Ruling: leave for a follow-up, none blocks merge or the session path — cost if wrong: small follow-up commit
Final: PENDING USER — final-verify.sh on 671ad40 (make all, go test -race ./ingest/..., reseed calendar, gold_views, view checks, plugin clone to ../gold-nav); C1 HTTP MCP deployment is out of plan scope
Final: VERIFIED on user's Mac at 671ad40 (final-verify.log): make all green incl. build-ingest; 3 views created; v_model_inputs 1 row, all blocks filled; v_gold_dashboard 5 rows stale=1; v_forecast_accuracy 2 rows; weekly cpi/eia seeded; unknown field 400; negative probability 400; plugin cloned to ../gold-nav (ef7bd44)
