# SDD ledger — plan: docs/superpowers/plans/2026-10-08-kestra-orchestration.md

Pre-flight: no shared interfaces beyond Task 1 (main.go) and Tasks 2-4 (server, no repo code); Task 5 consumes binary path and secret names from Tasks 3-4, rulings below.

Task 1: Ruling: main.go edited per plan (exit 1 on any importer error, empty filter = all, unknown name = log warning with exit 0, ParseDSN error → fixed message without DSN) — spec §3 items 1-3 plus warning from Review Focus #2 — cost if wrong: one-line revert.
Task 1: Ruling: go build failed before any change because go.sum is untracked and incomplete; ran go mod tidy — go.mod unchanged, go.sum 196→260 lines — go.sum must be committed with the change, otherwise the repo still does not build — cost if wrong: reversible.
Task 1: Found (pre-existing, not fixed): cbr/*.go call c.Visit (network) at startup before DB connect — violates AGENTS.md rule 3 — every Kestra run hits cbr.ru even with a filter — separate task.
Task 1: verified: invalid CLICKHOUSE_URL → exit 1, message "invalid CLICKHOUSE_URL"; unreachable ClickHouse → exit 1, password not in output; parseImportFilter unit check (temporary test, removed).
Task 1: NOT verified: plan steps 4.2-4.4 need a running ClickHouse (none locally). Task 1 stays open until checked on the server.

Plan rewritten for Dagu (spec approved by user 2026-10-08). Kestra is dropped; nothing was installed on any server, so nothing to undo.
Task 1: Ruling: Dagu retry_policy has constant interval only (limit + interval_sec); spec's 1/5/15 min exponential backoff is not documented, so 3 retries at 300 s — cost if wrong: longer waits on outages, retune interval_sec.
Task 1: Ruling: Telegram is sent by curl inside base.yaml handler_on.failure (Dagu has no Telegram plugin; http.request also possible) — token then appears in the curl argv for a moment — cost if wrong: switch to http.request or a script reading the secret file.
Task 2: Ruling: Dagu version — NVD: CVE-2026-33344 fixed in 2.3.1; CVE-2026-31882/31886 fixed in 2.2.4; latest stable release seen v2.18.2 — install v2.18.2 — cost if wrong: a newer release may be preferable, not a security gap.
Task 2: blocked: needs server access (host, user) to install; not started.
Task 5 (local part): Ruling: one Dagu step per DISTINCT Name() (12 steps for 14 importers), because Dagu step ids must be unique and the CLICKHOUSE_IMPORT_STAT filter runs every importer with a matching name — cost if wrong: households_b_mes step also runs the queries importer.
Task 5 (local part): Ruling: type: chain instead of default graph, because Dagu graph runs steps without depends in parallel and the plan requires sequential source calls — cost if wrong: parallel load on cbr.ru.
Task 5 (local part): Found (pre-existing): cbrQueriesDataset.Name() returns households_b_mes but writes cbr_queries_<...>; DatasetId 0 looks like a placeholder — separate fix.
Task 5 (local part): dagu/cbr.yaml written; 12 steps, unique ids, YAML parses. dagu validate NOT run (no dagu binary locally; installing it on this machine needs your OK).

Task 1: complete (dev verification 2026-10-08). Checks: unknown name → warning + exit 0; single real importer cbr_key_rate → 3275 rows, exit 0; fake fail+ok (temp copy in /tmp, not in repo) → exit 1 and both attempted; ok only → exit 0; empty filter starts importers (4 distinct in 90 s).
Task 1: Found: polyus_financial_metrics needs pdftotext (poppler), absent on this Mac; with the new exit code a run that includes it turns red. Separate task: install poppler on server or fix importer.
Task 1: Ruling: per-importer failure tested with a temporary copy and fake importers — real network failures could not be simulated (http.Transport ignores proxy env; no sudo for hosts/firewall) — cost if wrong: low, the exit path is the same code.

Task 6 (dev part): DAG files written for rosstat (ipc_mes, ipc_weeks, salaries_mes, vvp_kvartal), fao (fao_food_price), minfin (2), customs (1), craw (1), bank (6), financial (3: databook_polyus, databook_ugk, polyus_financial_metrics). All 8 DAG files pass dagu validate (rc=0); negative control with duplicate step id returns rc=1.
Task 6: Ruling: schedules are proposals per domain by most frequent source (rosstat weekly Thu, bank and financial weekly Mon, fao/minfin/customs/craw monthly) — per spec §3.4 — cost if wrong: wrong cadence, change schedule: line.
Task 6: Ruling: step ids unique per distinct Name(); domain by directory (fao dir has package rosstat). Task 6 running each DAG (plan step 3, run part) NOT done yet.

User confirmed schedules (Task 6 Ruling now confirmed).
Task 6 run (dev): sequential runs of fao, minfin, customs, craw, rosstat, bank, financial via /tmp/run-domains.sh; results in /tmp/run-domains.result, logs /tmp/dagu-<domain>.log. cbr run still in progress in parallel.
pdftotext: user reported installed, but pdftotext was not on PATH; poppler 26.10.0 keg existed unlinked; ran `brew link poppler` -> /opt/homebrew/bin/pdftotext 26.10.0 OK.

Task 6 run: fao FAILED first (rc=1): fao/food_price_indices_mes.go getDataUrl selector no longer matched FAO page (Col00 children shifted: a, img, div, div; nth-child(2) hit img). Fix: selector -> a[href*="food_price_indices_data.csv"] (verified with goquery against saved page). Ruling: one-line fix inside the fao importer, outside the original plan, required for fao DAG — cost if wrong: revert the selector line.
Task 6 run: fao direct run after fix: Imported 2205 rows of fao_food_price; table is fao_food_price_Indices_monthly (2205 rows); fao_food_price_Indices_Monthly_Real exists and is empty (pre-existing: that sheet is read only by unused exportXls).
Found: Name() "fao_food_price" is a prefix; real tables are fao_food_price_<sheet>. Spec assumed Name() == table name. Filter works by Name(), so DAG is unaffected; spec text needs a note.
Found: fao importer uses conn.Exec per row (AGENTS.md rule 2 violation, pre-existing), not changed.
Dagu fao DAG: stopped in progress; needs re-run after the fix.

Task 6 run: minfin FAILED (URL empty). Cause A: colly default UA gets HTTP 503 from minfin.gov.ru; fix: colly.UserAgent(util.HttpUA) in minfin/fedbud_mes.go and minfin/fedbud_mesyats.go. Cause B: selector nth-child(3) picked the annual _god.xlsx (sheet "год"), fix in fedbud_mes.go: a[href$="_mes.xlsx"] (sheet "месяц", verified). Ruling: both are one-line fixes outside the original plan, required for the minfin DAG — cost if wrong: revert the lines.
Task 6 run: minfin direct: minfin_fed_bud_mes 883 rows, minfin_fed_bud_mesyats 876 rows in ClickHouse. Log says "Imported 0 rows" for both — pre-existing bug: util/hd_base.go Import never sets count, so logs show 0 even on success. Not fixed; affects Review Focus #2 monitoring.
Found (same class, not fixed): colly.NewCollector() without UA in cbr/*.go (the "Visit Service Unavailable" lines at startup are likely 503 for colly UA), rosstat/ipc_mes.go, rosstat/ipc_weeks.go, rosstat/salaries_mes.go, financial/*.go, craw/gost.go (has UA? checked: uses colly.NewCollector with options). Check via run results.
Found (structural, not fixed): minfin importers and cbr indicators_cpd/ruonia compute DataUrl inside init() via network (AGENTS rule 3 violation, ARCHITECTURE §8 #6).

Row count fix: util/hd_base.go Import now returns rows := batch.Rows() taken before Send and returned only after successful Send (driver.Batch.Rows exists in clickhouse-go v2). Verified: minfin_fed_bud_mes Imported 883 (= table count), minfin_fed_bud_mesyats Imported 876 (= table count). DAG minfin re-run: rc=0.
