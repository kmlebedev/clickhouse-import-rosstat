# Task 2 Report — events_calendar Q4-2026 + витрина v_events_calendar

Status: DONE
Branch: gold-nav-contour
Commit: 2d43601 `add events_calendar Q4-2026 and v_events_calendar` (7 files, +110/-1)

## What I implemented (per brief step)

### Step 1 — `sql/events_calendar_q4_2026.sql` (new)
- Canonical DDL `CREATE TABLE IF NOT EXISTS events_calendar` copied verbatim from ARCHITECTURE.md §6.2 (columns `event_date Date`, `event_time Nullable(String)`, `category LowCardinality(String)`, `title String`, `threshold String`, `status LowCardinality(String) DEFAULT 'pending'`, `ENGINE = ReplacingMergeTree ORDER BY (event_date, category, title)`).
- One `INSERT INTO events_calendar (...)` batch with 11 Q4-2026 events from ARCHITECTURE.md §7:
  EIA (2026-10-14, recurring note in title), CPI Sep (10-14), СД ЦБ (10-23), WGC GDT (~end Oct → 10-30), diesel export ban expiry (11-01), NFP Oct (11-06), CPI Oct (11-10), NFP Nov (12-04), FOMC + dot plot (12-09), CPI Nov (12-10), СД ЦБ + MOEX rebalance (12-18).
- `threshold` holds JSON strings, e.g. `'{"crack":">50","crack_bull":"<30"}'`.
- Idempotent via ReplacingMergeTree (re-seed confirmed: count stayed 11).

### Step 2 — `calendar/events.go` (new)
- `package calendar`, `init()`-registered via `chimport.Stats`, `Name() = "events_calendar"`.
- `Import()` runs the same canonical DDL, then `util.CreateView(ctx, conn, util.View{...})` for `v_events_calendar`: `Select = "SELECT * FROM events_calendar FINAL"`, `Tables = ["events_calendar"]`, table comment «Календарь событий-триггеров прогноза золота/NAV; status: pending|done|verified», per-column Russian comments for all 6 columns.
- No data seeding in Go; no network calls in `init()`; no secrets.

### Step 3 — grants + registration
- `sql/mcp_kimi_reader.sql`: added `GRANT SELECT ON default.v_events_calendar TO kimi_reader;`.
- `main.go`: added blank import `_ "github.com/kmlebedev/clickhouse-import-rosstat/calendar"` (module path verified in `go.mod`).

### Makefile (controller-ruled addition)
- Added `ch-sql` target (+ `.PHONY`): guarded by `FILE=...` required, file-exists check, `CH_BIN` presence, ClickHouse `/ping` on port 8123 first, then pipes the file via `"$(CH_BIN)" client --user "$(CH_USER)" --password "$(CH_USER_PASSWORD)" --multiquery`.
- Note: a first version without `--user/--password` failed with `Code: 516 AUTHENTICATION_FAILED` because the local dev server's `default` user requires empty-password auth on native TCP (CLICKHOUSE_PASSWORD in env is the `kimi_reader` password, port 9000). The target now defaults to `CH_USER=default` / empty password and accepts overrides.

### Step 4 — live run against ClickHouse (via make only)
- `make import STAT=events_calendar` → `View v_events_calendar created: true`, `Imported 0 rows of events_calendar` (expected: importer only does DDL+view). Re-run also OK (idempotent).
- `make ch-sql FILE=sql/events_calendar_q4_2026.sql` → clean (no output, exit 0).
- `SELECT count() FROM v_events_calendar` → **11** (≥ 10 required), verified through a temp file run via `make ch-sql`.
- Column comments verified: all 6 columns present in `system.columns` with Russian comments; table comment present.
- `make mcp-user` → all grants OK incl. `GRANT SELECT ON default.v_events_calendar TO kimi_reader`.
- MCP end-to-end (kimi_reader user): `run_query SELECT count() FROM v_events_calendar` → 11; `list_tables` shows the view with per-column COMMENT clauses and the table COMMENT.

### Step 5 — `make all`
- First run failed on `gofmt -l` (calendar/events.go); ran `gofmt -w`, then `make all` green: `golangci-lint 0 issues`, `go vet`, `go test -race ./...` all ok, linux/amd64 static build OK.

### Docs sync (rule 11e / sync-readme-architecture)
- `ARCHITECTURE.md` §2: new `calendar/` structure line; §6.3: new `v_events_calendar` view bullet.
- `README.md`: new `calendar` row in the importer table; new `make ch-sql FILE=...` row in the commands table.

## Files changed
- `sql/events_calendar_q4_2026.sql` (new)
- `calendar/events.go` (new)
- `main.go` (blank import)
- `sql/mcp_kimi_reader.sql` (GRANT)
- `Makefile` (`ch-sql` target)
- `ARCHITECTURE.md`, `README.md` (docs sync)

## Commands run and results
| Command | Result |
|---|---|
| `make ch-status` | ClickHouse отвечает на порту 8123 |
| `make import STAT=events_calendar` | View created: true; 0 rows imported (DDL+view only) |
| `make ch-sql FILE=sql/events_calendar_q4_2026.sql` | exit 0 (seed applied) |
| `SELECT count() FROM v_events_calendar` (via make ch-sql) | 11 |
| `make mcp-user` | all GRANTs OK (incl. v_events_calendar) |
| MCP `run_query`/`list_tables` (kimi_reader) | count 11, view + comments visible |
| `make all` | green (lint 0 issues, vet, test -race, build) |

## Self-review findings
- Diff read via `git show`: DDL matches §6.2 verbatim; `Name()` and blank-import path correct; no secrets; no network in `init()`; batching rule N/A (no Go insert path — data is a single SQL batch).
- Fixed during self-review: missing `gofmt`; `ch-sql` auth flags; docs sync added to the commit.

## Issues / concerns
- The `ch-sql` target changed beyond the strict minimum: it hardcodes the local dev convention that `default` has no password and accepts `CH_USER`/`CH_USER_PASSWORD` overrides. This is a Makefile/README change outside the brief file list, but required for the target to work (first no-auth form failed with AUTHENTICATION_FAILED). Documented in README and the Makefile comment.
- `threshold` JSON keys (`hot_yoy`, `cut_bp`, `hike_count`, `nfp_k`, `cb_purchase_t`, `extended`/`lifted`, `crack`) are my own naming; consumers (Task 3+/model) should read the `title` semantics, since no schema for keys exists in ARCHITECTURE.md.
- NFP Nov release date pinned to 2026-12-04 and WGC GDT to 2026-10-30 ("~end of Oct"): plausible calendar dates, not verified against official sources (offline task), flagged as invented-but-reasonable.
