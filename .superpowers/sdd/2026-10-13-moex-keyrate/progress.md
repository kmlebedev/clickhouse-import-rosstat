# SDD ledger — plan: docs/superpowers/plans/2026-10-13-moex-keyrate.md

Spec: docs/superpowers/specs/2026-10-13-moex-keyrate-design.md
Branch: key-rate-forecast (base 9011194)
Workspace: .superpowers/sdd/2026-10-13-moex-keyrate

## Pre-flight scan (before Task 1)

| Pair / task | Producer → consumer | Finding |
|---|---|---|
| T1 → T2 | `parseKeyrateSecids`/`parseKeyrateContract`/`parseKeyrateHistory`/`parseHistoryTotal`/`validSecid` → `fetchKeyrate*` | consistent; names identical in both briefs |
| T2 → T3 | `fetchKeyrateSecids`/`fetchKeyrateContract`/`fetchKeyrateHistory` → `Import`; new `getBlocks` → `getBlock` | consistent |
| T3 → T4 | `keyrate_futures`/`keyrate_contract` DDL + `v_keyrate_expectations` → dagu step, grant, ARCHITECTURE §6.2 | consistent |
| T1 files | creates `moex/keyrate_parse.go`, `moex/keyrate_parse_test.go`, 3 fixtures | ok |
| T2 files | creates `moex/keyrate.go`, `moex/keyrate_test.go`; modifies `moex/moex.go` (adds `getBlocks`) | ok |
| T3 files | modifies `moex/keyrate.go`, `moex/keyrate_test.go` — files created by T2, declared in brief | ok |
| T4 files | `dagu/moex.yaml`, `sql/mcp_kimi_reader.sql`, `ARCHITECTURE.md`, `README.md` | ok |
| T5 | verification only, no files except roadmap status | ok |
| T1 self | 8 tests ↔ 6 produced functions | one test name mentions `parseKeyrateHistoryIgnoresLast` — asserts absence of a column, not a function; ok |
| T3 self | DDL ↔ view columns ↔ catalog fields | DDL has 8 columns + `keyrate_contract` 5; view selects 10 aliases incl. 2 computed; columns map must list exactly those 10 | ok |
| T2 self | `TestFetchKeyrateHistoryPaginates` uses PAGESIZE=2/TOTAL=3, but `moexPageLimit=100` is the paging step | **conflict — see Ruling 1** |

### Rulings from the scan

- **Ruling 1: pagination step stays `moexPageLimit` (100); the T2 test asserts the loop logic, not a
  real 2-row page.** The plan's `TestFetchKeyrateHistoryPaginates` describes a server with
  `PAGESIZE=2`, but the implementation advances `start` by `moexPageLimit` (the existing package
  constant, `moex/moex.go`). Rewriting the step to read `PAGESIZE` would change shared behaviour for
  the two existing importers. The test therefore feeds a fixture whose `history.cursor.TOTAL` exceeds
  the number of rows returned on the first call, so the second request is made with
  `start=100`. — Why: keeps one paging convention in the package. — Cost if wrong: one extra ISS
  request per series per run; no data loss (the loop still exits on `TOTAL` or an empty page).
- **Ruling 2: the spot in `v_keyrate_expectations` is a scalar subquery, not a `CROSS JOIN`.**
  `round(f.settle - (SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 4)`. — Why: one less join
  and no dependence on CROSS JOIN semantics; the subagent cannot validate the view against a live
  database (kimi_reader has no access to raw tables), and a scalar subquery has strictly fewer ways
  to fail. `cbr_key_rate` stays in `util.View.Tables` so the existence check still covers it. —
  Cost if wrong: identical results, only the plan text and the spec's SQL sketch differ.
- **Ruling 3: `keyrate_contract.minstep`/`stepprice` are dropped** — already reflected in the spec
  §«Схема» after a live probe showed those fields live in the forts `securities` block, not in the
  paper's `description` block. — Cost if wrong: the ruble multiplier (1 п.п. = 1 000 ₽) is documented
  in comments instead of stored; re-adding the columns later is a schema addition.

## Progress

Task 1-2: complete (commits 9b24744..4b521da, review clean — Approved, no Critical/Important)
Task 1-2: minor (deferred): `moex/keyrate_parse.go:181` — filter adds a `KK` prefix that the plan did
  not mandate (board `RFUD` + `is_traded==1` was the binding rule). Redundant today (spreads have
  `is_traded=0`), but a future KEYRATE series with another code prefix would be silently dropped.
  Ruling: deferred — the KK prefix matches the live instrument family and the risk is a code rename
  we would see as an empty import; the final review triages it. Cost if wrong: one series missing
  from the contour until noticed.
Task 1-2: minor (deferred): `secid` is not validated before being formatted into the request path
  (`moex/keyrate.go:60,80`); `validSecid` is referenced only from tests. **Carried into Task 3** as a
  required fix (the plan already puts `validSecid` before SQL).
Task 1-2: minor (deferred): `rowFloat` errors on a `nil` cell in the shared `moex/moex.go` helper; a
  `null` price in a history row would fail the whole run. Not observed live (zero-volume KKG7 parsed
  fine); Task 5's live acceptance is the check.
Task 1-2: minor (deferred): `errNoColumns` text says `tradedate/period/value` (candles-specific,
  pre-existing) and is reused by the keyrate path — misleading message only.
Task 1-2: minor (deferred): test hygiene — shadowed `want :=` (`moex/keyrate_test.go:682,708`), a
  `paths` slice appended in the httptest handler goroutine and read in the test goroutine
  (`:669-710`), case-sensitive `columnIndex` in the `LAST`-absence check
  (`moex/keyrate_parse_test.go:567`), trailing spaces in the three fixtures.
Task 1-2: note — live ISS reports `history` columns UPPERCASE (`SETTLEPRICE`) while other blocks are
  lowercase; the implementer added `keyrateColumnIndex` (EqualFold). Correct for the live source; the
  spec's lowercase spellings are the plan's shorthand, not a contract.

Task 3: fix round 1/5 opened (review 4b521da..08f2782 — spec ❌, 2 Important, 1 unresolved ⚠️).
Task 3: Ruling 4 — **catalog describes ALL discovered contracts, not only series with rows; the
  misleading comment is the defect, not the behaviour.** The plan mandates "одна запись на
  обнаруженную серию", and a described-but-empty series is the honest state (a series with zero
  trades still exists as an instrument; `KKG7` had no trades for days). — Cost if wrong: an agent
  sees a catalog entry whose fact table has no rows for that series, which is visible and harmless.
Task 3: Ruling 5 — **the empty-`cbr_key_rate` case is guarded, not only documented.** Wrap the spot
  in `nullIf(…, 0)`, making `expected_change_pp`/`expected_change_bp` `Nullable(Float64)`: NULL is an
  honest "unknown", while `argMax` returning 0 yields a nonsense ≈ +1390 б.п. that an LLM agent would
  read as a real market expectation. The key rate has never been 0, so 0 is a safe sentinel. —
  Cost if wrong: the columns become Nullable (a type change for the agent) and a genuine 0% key rate
  (never observed) would show NULL.
Task 3: fix round 1/5 (3 addressed — publication order, catalog comment per Ruling 4, empty-spot
  guard per Ruling 5 + 3 minors; 0 open; commits 08f2782..5f5b95c)
Task 3: complete (commits 4b521da..5f5b95c, review clean; 1 minor deferred)
Task 3: minor (deferred): `moex/keyrate.go:267-273` — the new all-series-failed return leaves
  `contractBatch`/`dayBatch` open (no `Abort`). Pre-existing pattern in this package
  (`ofz_curve.go`, `moex_iss.go` do the same); impact bounded — one-shot CLI, connection held until
  exit. Final review triages.
Task 3: minor (deferred): `moex/keyrate_test.go:291` `TestKeyrateViewContract` pins the view SQL with
  substring assertions rather than semantics (the chosen trade-off: the view cannot be run here).



Task 3: note (carried into Task 4) — the plan's own view SQL (`FROM keyrate_futures FINAL f`,
  plan:216) is invalid ClickHouse syntax and the spec's `CROSS JOIN` sketch was superseded by
  Ruling 2. The code's form is the authority: `FROM keyrate_futures AS f FINAL` plus
  `nullIf((SELECT argMax(rate, date) FROM cbr_key_rate FINAL), 0)`, both verified to parse by live
  read-only `DESCRIBE` (UNKNOWN_TABLE = parsed, SYNTAX_ERROR = rejected).
Task 4: fix round 1/5 (4 addressed — ARCHITECTURE gate wording, README Warnf, escaped quote
  in the documented SQL, and `scripts/mcp_check.py` extended to cover the new view and both raw
  tables; 0 open; commits 71b79f1..c60b600)
Task 4: complete (commits 5f5b95c..c60b600, review clean)
Task 4: Ruling 6 — **`scripts/mcp_check.py` is in scope for this PR.** The script carries explicit
  per-view lists, so without adding keyrate the plan's acceptance criterion ("view visible to the
  agent, raw table ACCESS_DENIED") would pass while verifying nothing. — Cost if wrong: one extra
  block in the project's smoke test.
Task 4: Ruling 7 — **the GRANT-before-view order is left alone** (R3). The project already documents
  `make mcp-user` as running after the marts are imported, and all 28 existing grants share the
  property; changing `util.CreateView`'s silent skip is project-wide behaviour. — Cost if wrong:
  `make mcp-user` on a database without the marts aborts — same as for every other grant today.
Task 4: minor (deferred): `ARCHITECTURE.md:492` — the DDL comment still says "заседания СД ЦБ, 19:00
  МСК" without naming `EXPIRATION_TIME` as its source (the README paragraph got that clarification).
Task 4: note for Task 5 — `make mcp-check` now covers keyrate, so it FAILS on a database where the
  view has not been created yet; the acceptance order must be import → `make mcp-user` → `make mcp-check`.

## Final whole-branch review (118212b..fc02369, model primary)

Verdict: **With fixes** — documentation only; no code changes required. No Critical; the live ISS/CBR
data was re-verified independently by the reviewer (fixtures match live responses byte-wise, the
acceptance numbers reproduce, DDL/view byte-identical to ARCHITECTURE).
Final round 1 (1 Important + 5 Minor, all doc text): decision split 12/10 → **13/9** (verified by
  me against the register and the rate path); KEYV6 daily volume "2–404" → 2…2232; the
  `events_calendar` coverage row mixed scopes (cbr = 2 rows, 2026-10-23…2026-12-18); the plan keeps
  the invalid view sketch; ARCHITECTURE:237 "виден агенту до первых сделок" is true only via
  `v_series_catalog`; ARCHITECTURE:492 lacks the `EXPIRATION_TIME` source.
Deferred minors triage by the reviewer: all nine acceptable to leave, each with a reason —
  including the `KK` prefix (it is in the spec, decision 3, so not a deviation).
Final round 1 re-review (fc02369..80c4b69, model haiku): all 6 findings ADDRESSED, no new
  Critical/Important. One new Minor in the fix diff: `docs/KEY_RATE_CHANNELS.md:125` — the range
  "дневной объём от 2 до 2232 лотов" now sits inside the clause that opens with "на 13.10.2026", so
  it reads as that day's volume (3 lots for KKV6) rather than the KKV6 history range.
Final: Ruling 8 — **the residual Minor is parked, not fixed.** The figure is correct and the sentence
  already says "ликвидность низкая"; only the reading is ambiguous, and the skill allows no second
  fix wave after the final review. — Cost if wrong: one slightly misleading word in a caveat sentence
  an analyst reads; a one-line wording fix if the owner wants it.
Final: Ruling 9 — **the SDD workspace is kept until PR #49 lands**, not deleted now. The owner reviews
  the PR and may send feedback that the ledger's rulings and review packages help resolve; the
  workspace is git-ignored scratch. — Cost if wrong: a few KB of ignored files to remove later.

## Owner-requested addition to PR #49 (CatBoost + publication spec)

Task 6: docs (commits e76133e, a7b7d9f) — CatBoost section in KEY_RATE_MODEL, new
  docs/KEY_RATE_PUBLICATIONS.md, roadmap items 1.6 and the ML track.
Task 6: review (e76133e) — "approve with fixes": my own arithmetic was wrong (100 б.п. при 14 % — это
  ≈7 %, а не 0,7 %) in four places across three documents, the channel count was «десять» вместо
  одиннадцати, the `cbr/` file counts were wrong (17 нетестовых файлов: colly 7, Exec/Printf 11),
  the weekly cadence was misdated, and several numbers rested on sources that do not say them.
Task 6: fix round 1/5 (A–H groups addressed — arithmetic, channel count, cadence, file counts,
  meeting counts, five citation corrections, two scope clarifications, four typos; 0 open;
  commits e76133e..a7b7d9f). Scoped re-review: all addressed, no new Critical/Important.
Task 6: Ruling 10 — **`KEY_RATE_MODEL.md:46`'s «комментарий к среднесрочному прогнозу, 05.08.2026»
  is correct as written** (the comment is published at key meetings; 05.08.2026 is the publication
  date for the 24.07.2026 meeting, same as the summary). No change. — Cost if wrong: a reader who
  expects a meeting date sees a publication date; the neighbouring registers disambiguate.
Task 6: complete (commits e76133e..a7b7d9f, review clean after one fix round)
Task 6: minor (deferred): the «и/или» phrasing for the 11 `cbr/` files is an OR-count, deliberately
  different from the old conjunction — the earlier claim (11 из 13 with all three symptoms at once)
  was wrong and the OR-count is what the code shows.
