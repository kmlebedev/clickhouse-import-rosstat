# Task 4 report: v_model_inputs и v_gold_dashboard

Status: DONE_WITH_CONCERNS. Commit `94151d9` on branch gold-nav-contour (parent 383362b).
Go and ClickHouse are not available in this environment, so nothing was compiled or run. gofmt is clean.

## Files
- `views/model_inputs.go`: `util.View` for `v_model_inputs` (9 source tables, 35 columns).
- `views/gold_dashboard.go`: `util.View` for `v_gold_dashboard` (Tables: `macro_series`).
- `views/gold_views.go`: importer `Name() = "gold_views"`. Its Import runs `util.CreateView` for both views. Registered in `init()` via `chimport.Stats`, same pattern as `calendar/events.go`. Package doc explains that the importer must run after the first ingest run, because `model_runs` is created by ingest.
- `main.go`: blank import of `views`.
- `sql/mcp_kimi_reader.sql`: `GRANT SELECT` for `v_model_inputs` and `v_gold_dashboard`.

Deviation: the brief listed only the two view files. The importer lives in a third file, `gold_views.go`, to keep the two views free of registration code.

`util.CreateView` already emits `CREATE OR REPLACE VIEW ... DEFINER = default SQL SECURITY DEFINER`, so no change was needed there.

## v_model_inputs columns and sources
Each block is a single-row subquery (aggregate without GROUP BY, so it always returns one row). The blocks are chained with CROSS JOIN. Values use `argMax(toNullable(x), date)` and dates use `argMax(toNullable(date), date)`, so an empty table yields NULL and never 0 or 1970-01-01.

| Column(s) | Source table and filter | Where the source column was found |
|---|---|---|
| gold_moex_fix_usd, gold_date | gold_prices FINAL, venue='moex_fix_usd' (usd, date) | gold/gold.go DDL |
| usdrub, usdrub_date | cbr_currency_usd FINAL (price Float32, date) | cbr/currency_usd.go DDL |
| cbr_key_rate, cbr_key_rate_date | cbr_key_rate FINAL (rate Float32, date) | cbr/key_rate.go DDL |
| ofz_1y/3y/5y/10y, rgbi (+ *_date) | ofz_curve FINAL, argMaxIf by tenor (yield Float64) | moex/ofz_curve.go DDL; RGBI is stored as a level under tenor 'RGBI' |
| ipc_mes_last, ipc_mes_last_date | ipc_mes FINAL, name startsWith 'Индексы потребительских цен на товары и услуги' (percent Float32) | rosstat/ipc_mes.go DDL and rosstat/series_meta.go (the name carries footnotes and years, so it is matched by prefix) |
| ipc_week_ytd, ipc_week_ytd_date | ipc_weeks FINAL (see concern 1) | rosstat/ipc_weeks.go DDL and series_meta.go |
| fedfunds, dfii10, dgs10, t5yie, dtwexbgs (+ *_date) | macro_series FINAL, source='fred', series in uppercase FRED ids | fred/fred.go (fredSeries list, source 'fred') and ingest/schema.go DDL |
| plzl_close, plzl_date | stock_prices FINAL, code='PLZL' (close Float32, date) | moex/moex_iss.go DDL |
| last_run_id, last_nav_per_share, last_run_date | model_runs FINAL (run_id, nav_per_share, run_date) | ingest/schema.go DDL, which matches ARCHITECTURE.md §6.2 |

Column names come from spec §5.1. The spec lists value columns only. The brief asks for `argMax(date, date)` per series, so every series also has a `*_date` column. These extra columns are deliberate: they give the agent an as-of date for each series, which the look-ahead and staleness checks need.

## v_gold_dashboard
Columns: `indicator, value, threshold, flag, as_of, stale`.
- The indicator list is fixed: arrayJoin over the 5 names, LEFT JOINed to `macro_series FINAL WHERE source IN ('manual','fred')` aggregated per series. The right side carries `count() AS n`, and every right-side column is guarded with `if(v.n > 0, ..., NULL)`. This makes NULL independent of the `join_use_nulls` setting, which otherwise fills misses with 0 and 1970-01-01.
- `stale = 1` when as_of is NULL or older than 14 days.
- `flag` is UInt8. crack_ulsd_proxy: value > 50. fedwatch_dec_hike: value > 80. dxy: value > 102. The other two indicators have no threshold in §6, so flag = 0.
- `threshold` is a text column from multiIf. Indicators with no threshold in §6 show 'порога в статье §6 нет'.

## Concerns and choices for review
1. **ipc_week_ytd is an approximation.** ipc_weeks has no all-goods weekly index. It holds per-product weekly indices (about 100 products, percent to the previous registration date). The view computes, for each product, the product of (p/100) over the weeks of the latest data year, minus 1, and then averages those YTD changes without weights. The product/year is taken from max(date). The column is labelled in its comment as a guide, not an official CPI. If the spec meant something else, change this block.
2. **FedWatch flag threshold.** §6 gives two levels (>65% scenario switch, >80% alert). The flag uses >80 because the roadmap's alert list names it. Values are assumed to be in percent (65 = 65%). Confirm the unit of the manual ingest.
3. **DXY source.** The brief lists source IN ('manual','fred'), so dxy is read from manual only, since no FRED series for DXY is imported. DTWEXBGS is a separate broad index and is exposed in v_model_inputs as dtwexbgs.
4. **Nullable Float32.** cbr, ipc and PLZL values stay Float32 to avoid the Float64 display noise (e.g. 123.45000457763672). The brief's Float64 rule is for new tables only, so this is consistent with it.
5. **Not done.** Per AGENTS.md rule 11 (г)-(д), the MCP check of comments, ARCHITECTURE.md §6.3 and README.md updates are not part of this task's commit scope and are still open.
6. **Unverified SQL.** The main risk is ClickHouse behaviour of `argMax(toNullable(x), d)` over an empty set returning NULL. Verify with the checks below.

## Live checks for the user (not run here)
```
make import STAT=gold_views
```
Run this only after the first ingest run, so that model_runs and macro_series exist.
```sql
SELECT count() FROM v_model_inputs;                      -- expect 1
SELECT last_run_id, last_nav_per_share, last_run_date FROM v_model_inputs; -- NULLs while model_runs is empty
SELECT toTypeName(last_run_id), toTypeName(last_run_date) FROM v_model_inputs; -- Nullable(UUID), Nullable(DateTime)
SELECT * FROM v_model_inputs FORMAT Vertical;            -- every block present, dates set where data exists
SELECT indicator, value, threshold, flag, as_of, stale FROM v_gold_dashboard; -- expect 5 rows; empty ones stale=1, value NULL
SELECT name, comment FROM system.columns WHERE table IN ('v_model_inputs','v_gold_dashboard'); -- comments present
```
Also run `make mcp-check` and list the views through the `kimi_reader` MCP session to confirm the grants.
