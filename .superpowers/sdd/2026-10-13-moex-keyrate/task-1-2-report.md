# Task 1–2 report: MOEX KEYRATE parsing + transport

Plan: `docs/superpowers/plans/2026-10-13-moex-keyrate.md` (Task 1, Task 2)
Briefs: `.superpowers/sdd/2026-10-13-moex-keyrate/task-1-brief.md`, `task-2-brief.md`
Spec: `docs/superpowers/specs/2026-10-13-moex-keyrate-design.md`
Branch: `key-rate-forecast`. Commits: `9b24744`, `4b521da` (unrelated `deploy/staging/dagu.service` modification left untouched, never staged).

Scope honoured: parsing + transport only. No DDL, no ClickHouse writes, no view, no catalog, no `init()` registration
(Task 3). No DB validation attempted (per instruction: the MCP user is read-only over views and these tasks touch no SQL;
`mcp__clickhouse__*` was not used at all).

## Step 0 — live fixtures (before any code)

Fetched with the three `curl` commands from the brief, then trimmed line-by-line (values kept byte-exact, `columns`
arrays untouched, block structure preserved):

| File | Fetched (rows) | Kept | Blocks kept |
|---|---|---|---|
| `moex/testdata/keyrate_securities.json` | 6 | 6 (all — every row is needed) | `securities` (14 cols) |
| `moex/testdata/keyrate_description.json` | 22 | 9 | `description` (7 cols) + `boards` (17 cols, 1 row) |
| `moex/testdata/keyrate_history.json` | 10 | 3 | `history` (20 cols) + `history.cursor` (3 cols, `TOTAL=10`, `PAGESIZE=100`) |

Trimming was done by selecting raw lines of the live files (`python3` script, `keyrate_description.json` keeps
`SECID, SHORTNAME, FRSTTRADE, LSTTRADE, LSTDELDATE, ASSETCODE, UNIT, EXPIRATION_TIME, GROUP`; `keyrate_history.json`
keeps 2026-09-29/30 and 2026-10-01). Nothing was hand-typed, no value invented. `keyrate_securities.json` retains the
index `KEYRATE` (board `RTSI`), the futures `KKV6/KKZ6/KKG7` (board `RFUD`, `is_traded=1`) and the spreads
`KKV6KKZ6` (`is_traded=0`) / `KKZ6KKG7` (`is_traded=null`), so the filter test is meaningful.
The 3-row `history` fixture keeps `TOTAL=10` in the cursor block, as the brief requires.

**Finding from the live fixture:** the `history` block uses **UPPERCASE** column names
(`TRADEDATE, CLOSE, SETTLEPRICE, WAPRICE, VOLUME, NUMTRADES, OPENPOSITION`), while `securities`/`description` are
lowercase (`secid`, `primary_boardid`, `is_traded`, `name`, `value`) — unlike `candles`/`zcyc` in this package, which
are lowercase. Handled by `keyrateColumnIndex` (case-insensitive, see Deviations).

## Task 1 — pure parsing (`moex/keyrate_parse.go`)

Implemented exactly the briefed interface: `keyrateContract{secid, seriesName, firstTrade, lastTrade}`,
`keyrateDay{date, close, settle, waprice, volume, numtrades, openposition}`, `parseKeyrateSecids`,
`parseKeyrateContract`, `parseKeyrateHistory`, `parseHistoryTotal`, `validSecid` (`^[A-Z0-9]{4,}$`), plus three shared
row helpers (`rowString`, `rowTraded`, `rowCount`) and `parseKeyrateDate`. Lookup by column name, never by position;
missing required column → `errNoColumns`; bad cell → `errBadCell`-style errors. `LAST` is never looked up
(review focus 1) and the fixture contains no `LAST` column.

Series filter (design decision 3): `primary_boardid == "RFUD"` **and** code prefix `KK` **and** `is_traded == 1`;
`is_traded: null` (as ISS returns for `KKZ6KKG7`) means "not traded", not a parse error. `validSecid` is *not* applied
inside `parseKeyrateSecids` — Task 3 applies it per series, as the plan says.

### TDD evidence — Task 1

RED (`go test ./moex/ -run 'Keyrate|ValidSecid' -v`):

```
# github.com/kmlebedev/clickhouse-import-rosstat/moex [github.com/kmlebedev/clickhouse-import-rosstat/moex.test]
moex/keyrate_parse_test.go:61:14: undefined: parseKeyrateSecids
moex/keyrate_parse_test.go:83:15: undefined: parseKeyrateSecids
moex/keyrate_parse_test.go:103:15: undefined: parseKeyrateSecids
moex/keyrate_parse_test.go:107:14: undefined: parseKeyrateSecids
moex/keyrate_parse_test.go:118:14: undefined: parseKeyrateContract
moex/keyrate_parse_test.go:122:10: undefined: keyrateContract
moex/keyrate_parse_test.go:141:15: undefined: parseKeyrateContract
moex/keyrate_parse_test.go:155:14: undefined: parseKeyrateContract
moex/keyrate_parse_test.go:166:14: undefined: parseKeyrateHistory
moex/keyrate_parse_test.go:170:12: undefined: keyrateDay
moex/keyrate_parse_test.go:170:12: too many errors
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/moex [build failed]
```

Intermediate RED after the first implementation (this is what forced the case-insensitive lookup — the code parsed
lowercase names per the brief, the live block is uppercase):

```
--- FAIL: TestParseKeyrateHistory (0.00s)
    keyrate_parse_test.go:168: parseKeyrateHistory: no columns tradedate/period/value in [BOARDID TRADEDATE SECID
      OPEN LOW HIGH CLOSE OPENPOSITIONVALUE VALUE VOLUME OPENPOSITION SETTLEPRICE SWAPRATE WAPRICE CHANGE QTY
      NUMTRADES SHORTNAME ASSETCODE SWAPRATE_CURR]
--- FAIL: TestParseKeyrateHistoryIgnoresLast (0.00s)  (same cause)
```

GREEN (`go test ./moex/ -run 'Keyrate|ValidSecid' -v` → all PASS; then `go test ./moex/... -v` → `ok`):

```
--- PASS: TestParseKeyrateSecids / TestParseKeyrateSecidsNoColumns / TestParseKeyrateSecidsFilters
--- PASS: TestParseKeyrateContract / TestParseKeyrateContractMissingKey / TestParseKeyrateContractEmptyExpirationDate
--- PASS: TestParseKeyrateHistory / TestParseKeyrateHistoryIgnoresLast / TestParseHistoryTotal
--- PASS: TestParseHistoryTotalNoColumns / TestValidSecid
ok  	github.com/kmlebedev/clickhouse-import-rosstat/moex	0.268s
```

## Task 2 — transport (`moex/keyrate.go`, `moex/moex.go`)

- `getBlocks(ctx, path) (map[string]issBlock, error)` added to `moex/moex.go` (same timeout / `util.HttpClient` /
  decode path as before); `getBlock` re-expressed through it, returning the identical error texts
  (`"<path>: http status %d"`, `"<path>: %w"`, `"<path>: no block %q in response"`, raw request/transport errors).
  `git diff` on `moex.go` is the pure extraction; `TestFetchCandlesPaginates`/`TestFetchYearYieldsSendsRequest` stay green.
- `moex/keyrate.go`: `fetchKeyrateSecids`, `fetchKeyrateContract`, `fetchKeyrateHistory` with the briefed path consts,
  one `getBlocks` per page, step `moexPageLimit`, plus named block consts (`history`, `history.cursor`,
  `securities`, `description`). `from` is inclusive and formatted with `moexDate`.
- `fetchKeyrateHistory` termination: empty page → stop; collected `len(days) >= TOTAL` → stop; cursor block missing →
  hard error (cannot know when to stop). No absolute page cap (mirrors the brief).

### TDD evidence — Task 2

RED (`go test ./moex/ -run TestFetchKeyrate -v`):

```
moex/keyrate_test.go:43:14: undefined: fetchKeyrateSecids
moex/keyrate_test.go:64:14: undefined: fetchKeyrateContract
moex/keyrate_test.go:87:15: undefined: fetchKeyrateContract
moex/keyrate_test.go:119:15: undefined: fetchKeyrateHistory
moex/keyrate_test.go:162:15: undefined: fetchKeyrateHistory
moex/keyrate_test.go:183:17: undefined: getBlocks
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/moex [build failed]
```

GREEN (`go test ./moex/... -race -count=1 -v`) — 24 tests, all PASS, 6 of them pre-existing:

```
--- PASS: TestFetchKeyrateSecids / TestFetchKeyrateContract / TestFetchKeyrateContractHTTPError
--- PASS: TestFetchKeyrateHistoryPaginates / TestFetchKeyrateHistoryEmpty
--- PASS: TestGetBlocksReturnsAllBlocks / TestGetBlockMissingBlock
--- PASS (pre-existing): TestSeriesMetaCoversAllSeries, TestParseCandles, TestParseCandlesErrors,
    TestFetchCandlesPaginates, TestParseYearYields, TestFetchYearYieldsSendsRequest
ok  	github.com/kmlebedev/clickhouse-import-rosstat/moex	1.625s
```

All tests are offline: the only network is `httptest` with `moexBaseUrl` swapped in `useMoexServer` and restored via
`t.Cleanup` (LIFO, server closed after restore).

## Pagination test (caller resolution 1 — step is `moexPageLimit`, not `PAGESIZE`)

`TestFetchKeyrateHistoryPaginates` serves **fewer rows than `TOTAL` on the first call**, so a second request is
provably required: page 1 = rows 0–1 (`history.cursor.TOTAL=3`, `PAGESIZE=100` — `PAGESIZE` in the response is
deliberately *not* the step), page 2 = row 2. Asserted: exactly 2 requests, `start=0` then `start=100`,
`from=2026-09-29`, path prefix `/iss/history/engines/futures/markets/forts/securities/KKV6.json`, and the 3 returned
days deep-equal the expected `keyrateDay` values. `TestFetchKeyrateHistoryEmpty` (`TOTAL=0`) asserts empty slice, nil
error, exactly 1 request; if the loop failed to stop, the fake server flips to HTTP 500 after 3 requests so the test
fails instead of hanging until the `go test` timeout.

## Live end-to-end check (one-off, deleted afterwards)

A temporary `moex/keyrate_live_tmp_test.go` ran the real transport against `iss.moex.com` (file removed before the
commit; the repository keeps no network tests):

```
secids: [KKG7 KKV6 KKZ6]                       ← index KEYRATE (RTSI) and both spreads filtered out
KKG7 KEYRATE-2.27  firstTrade 2026-10-12, lastTrade 2027-02-12, 1 day: settle 15.97, volume 0, openposition 0
KKV6 KEYRATE-10.26 firstTrade 2026-09-29, lastTrade 2026-10-23, 10 days (TOTAL=10 → single page)
KKZ6 KEYRATE-12.26 firstTrade 2026-09-29, lastTrade 2026-12-18, 10 days
```

Matches the design's live-probe facts (including the `KKG7` zero-volume `settle=15.97` artefact) and confirms the
uppercase-column handling, pagination stop condition (`10 >= 10`) and `is_traded: null` tolerance against real data.

## Files

Created: `moex/keyrate_parse.go`, `moex/keyrate_parse_test.go`, `moex/keyrate.go`, `moex/keyrate_test.go`,
`moex/testdata/keyrate_{securities,description,history}.json`.
Modified: `moex/moex.go` (getBlocks extraction only).

Checks (final, on the committed tree):

```
gofmt -l moex/         → (empty)
go vet ./moex/         → ok
golangci-lint run ./moex/... → 0 issues
go test ./moex/... -count=1 -race → ok (24 tests)
go build ./...         → ok
get_file_problems (GoLand MCP) on all 5 touched files → errors: []
```

Commits:

```
9b24744 add moex keyrate ISS block parsers with live fixtures
4b521da add moex keyrate transport: series discovery, contract, paginated history
```

## Deviations from the briefs (deliberate, all forced by live data or the caller's resolutions)

1. **Case-insensitive column lookup** (`keyrateColumnIndex`, `strings.EqualFold`) instead of plain `columnIndex`:
   the live `history` block is uppercase (`SETTLEPRICE`), `securities`/`description` lowercase. Plain `columnIndex`
   fails on the live fixture (RED output above). The existing `columnIndex` is untouched and still used by
   `candles`/`zcyc`.
2. **`TestParseKeyrateSecids` compares sets, not order.** The brief expects `[KKV6 KKZ6 KKG7]`, but live ISS returns
   the `securities` rows in a different order (`KKG7, KKV6, KKZ6` — that is what the fixture pins). ISS row order is
   not contractual: the importer processes series independently, so the test asserts the *same set* (sorted compare)
   plus the absence of `KEYRATE`, `KKV6KKZ6`, `KKZ6KKG7`. No sorting was added to the parser (would have introduced a
   third, also non-briefed, order).
3. **`LSTDELDATE` semantics reconciled**: the plan says "если пусто, берём `LSTTRADE`" while `TestParseKeyrateContractMissingKey`
   requires an error when the key is absent. Implemented as: key missing → error (no silent zero time); key present but
   empty → fall back to `LSTTRADE`. Covered by a dedicated test
   (`TestParseKeyrateContractEmptyExpirationDate`), because the fixture cannot distinguish the two columns for `KKV6`
   (`LSTTRADE == LSTDELDATE == 2026-10-23`).
4. **`fetchKeyrateContract` sets `contract.secid` from its argument** (description parsing reads `SECID` when present,
   but the identity of a series must not depend on the response body); documented in code.
5. **Extra tests beyond the briefed list** (the briefed ones exist verbatim in name and intent):
   `TestParseKeyrateSecidsFilters` (board / prefix / `is_traded` 0-vs-null / non-numeric `is_traded`),
   `TestParseKeyrateContractEmptyExpirationDate`, `TestParseHistoryTotalNoColumns`, `TestFetchKeyrateContract`
   (happy path — the brief only had the 500 case), `TestGetBlocksReturnsAllBlocks` and `TestGetBlockMissingBlock`
   (pin the error text that Task 2 must preserve). `TestGetBlocks*` live in `keyrate_test.go` to keep Task 2's file
   list intact.

## Self-review findings

- Review focus 1 (`LAST` never used): parser reads only the 7 briefed columns; `keyrateDay` has no `last` field;
  the fixture has no `LAST` column and `TestParseKeyrateHistoryIgnoresLast` additionally appends a `LAST=0` column and
  proves the result is byte-identical (guards against a future "prefer LAST" regression).
- Review focus 2 (series not hardcoded): `fetchKeyrateSecids` re-reads ISS every run; the filter test proves index and
  both spreads are dropped.
- Review focus 4 (pagination loses nothing / does not spin): see the pagination and empty tests above.
- `getBlock` behaviour byte-identical: the only change is passing the decoded map through; `moex.go` diff verified line
  by line, error texts unchanged, existing tests green.
- One leftover self-check: the temporary live test file was deleted before committing (`git status` clean apart from
  `deploy/staging/dagu.service`).

## Concerns

1. **`errNoColumns` text is candles-specific** (`"no columns tradedate/period/value in %v"`). It is the helper the
   briefs mandate reusing, and I left it as is (changing shared error text is out of scope for these two tasks), but a
   keyrate failure will log that sentence — mildly confusing for Task 3's operators. Worth a follow-up decision.
2. **`secid` order is not deterministic across ISS releases** — if the ISS `securities` ordering ever becomes part of
   someone's expectation, tests use set comparison on purpose (deviation 2).
3. **No cap on the number of pages**: a server that keeps returning non-empty pages while claiming a huge `TOTAL`
   would loop until it returns an empty page (identical exposure to `fetchCandles`); ISS behaves, and the brief
   prescribes exactly the two stop conditions, so no extra bound was invented.
4. **`PAGESIZE` in `history.cursor` is ignored** by design (step is `moexPageLimit`), per the caller's resolution 1 —
   if ISS ever lowered its hard page limit below 100, the second page would be requested with a `start` beyond the
   returned window and `TOTAL` accounting would silently drop rows. Not observable today (`PAGESIZE=100`).
5. **No database validation was performed** (per instruction) — Task 3 is the first task that can be checked against
   ClickHouse.
