# Final fix wave — whole-branch review of `feat/rosstat-refactor`

**Date:** 2026-10-14
**Branch:** `feat/rosstat-refactor`
**Commit:** `38f26fa` — `rosstat: fail on repeated year label outside Q1; review-wave fixes` (12 files, +80/−36)

All items of the fix wave applied; each is a review finding. `deploy/staging/dagu.service`
(owner's uncommitted edit) was **not touched** and remains modified/unstaged. No importers
were run, no DDL/DML issued, no files under `.superpowers/` other than this report written,
no new dependencies. `.superpowers/` is git-ignored, so this report is scratch by design.

Tooling: `mcp__jetbrains__*` (GoLand MCP) was available and used for edit checks
(`get_file_problems` on every changed Go file). `mcp__clickhouse__*` was not needed: no item
required SQL, and the item's numbers are fixture-backed (`go test`), not live.

---

## 1. Critical — repeated year label fires on any quarter (`rosstat/vvp_kvartal.go`)

**Changed.**
- `parseVvpKvartalSheet`: when `year != 0 && labelYear <= year`, the quarter label at that
  column is now read *before* the label-bounds check and the year is advanced **only** if it
  is `I`. Otherwise a fail-fast error:

  ```go
  return nil, fmt.Errorf("%s: лист %q, колонка %d: повтор метки года %d на квартале %q",
      rosstatVVPKvartalTable, sheet, i+1, labelYear, quarter)
  ```

  A missing/empty label at that column yields `quarter == ""` (also ≠ `"I"`), so it errors
  instead of silently shifting. The legitimate I-quarter case keeps the `log.Warnf` and
  `labelYear = year + 1`.
- Comment above the block rewritten: the rule (shift only on I; a repeat on II–IV is an
  error, because advancing would overwrite real values under the `(name, date)` key) plus the
  verified facts. The old comment claimed sheets `2, 9, 10, 12` carry `20262)` on the repeat
  column — wrong.

**Facts verified against the fixture** (`rosstat/testdata/vvp_kvartal.xlsx`, openpyxl dump of
the year row, non-empty cells with column index):

```
sheet 2  year row: (1,2011) (5,2012) ... (41,2021) (45,'20222)') (49,'20232)') (53,'20242)') (57,'20252)') (61,'20262)')   data: 62 cells
sheet 9  year row: (1,2011) ...                    (45,'20222)') (49,'20232)') (53,'20242)') (57,'20252)') (61,'20262)')   data: 62 cells
sheet 10 year row: (1,2011) ...                    (45,'20222)') (49,'20232)') (53,'20242)') (57,'20252)') (61,'20262)')   data: 62 cells
sheet 12 year row: (1,2012) ...                    (41,'20222)') (45,'20232)') (49,'20242)') (53,'20252)') (57,'20262)')   data: 58 cells
sheet 14 year row: (1,2012) ...                    (41,'20222)') (45,'20232)') (49,'20242)') (53,'20252)') (57,'20252)')   data: 58 cells
```

So: sheet `14` repeats `20252)` in columns 53 and 57, and column 57 is `I квартал` (label row
verified); sheets `2`/`9`/`10` are regular (`20242)` 53, `20252)` 57, `20262)` 61); sheet `12`
has `20262)` at 57. The rewritten comment states exactly this.

**Test-comment correction** (`rosstat/vvp_kvartal_test.go`, above
`TestVvpKvartalRepeatedYearLabelAdvances`): rewritten to the rule + the real fixture facts.
Test body untouched — spot values `2025-04-01 = 106.7618431956141` and
`2026-04-01 = 104.1602983322909` still asserted and passing.

**New error-branch case** in `TestVvpKvartalSheetErrorBranches` (same table-driven shape): a
sheet whose year row labels two consecutive groups with the same year (`2011` at columns 1 and
6) so the repeat column is a **II** quarter:

```go
{
    name: "повтор метки года на квартале II",
    rows: [][]any{
        {"К содержанию"},
        {title},
        {"2011", "", "", "", "", "2011"},
        {"I квартал", "II квартал", "III квартал", "IV квартал", "I квартал", "II квартал"},
        {"100", "101", "102", "103", "104", "105"},
    },
    want: "повтор метки года 2011",
},
```

### Red before the guard (recorded)

Test added first, guard not yet present:

```
$ go test ./rosstat/ -run 'TestVvpKvartalSheetErrorBranches' -count=1
time="2026-10-14T16:36:18+05:00" level=warning msg="vvpKvartal: rosstat_vvp_kvartal: лист \"2\", колонка 6: метка года \"2011\" не больше текущего 2011 — год выведен как 2012"
--- FAIL: TestVvpKvartalSheetErrorBranches (0.01s)
    --- FAIL: TestVvpKvartalSheetErrorBranches/повтор_метки_года_на_квартале_II (0.00s)
        vvp_kvartal_test.go:432: ожидалась ошибка, получено 6 точек
FAIL
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	0.636s
FAIL
```

The old code silently advanced the year on a II quarter (warn only) and produced 6 points —
exactly the silent overwrite the finding described.

### Green after the guard (recorded)

```
$ go test ./rosstat/ -run 'TestVvpKvartalSheetErrorBranches|TestVvpKvartalRepeatedYearLabelAdvances' -count=1 -v
=== RUN   TestVvpKvartalRepeatedYearLabelAdvances
--- PASS: TestVvpKvartalRepeatedYearLabelAdvances (0.02s)
=== RUN   TestVvpKvartalSheetErrorBranches
=== RUN   TestVvpKvartalSheetErrorBranches/...
=== RUN   TestVvpKvartalSheetErrorBranches/повтор_метки_года_на_квартале_II
...
--- PASS: TestVvpKvartalSheetErrorBranches (0.00s)
PASS
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	0.304s
```

The full error text was captured with a temporary throwaway test (deleted immediately after,
never staged/committed):

```
ERR=rosstat_vvp_kvartal: лист "2", колонка 6: повтор метки года 2011 на квартале "II"
```

— table, sheet, column, repeated label year and quarter, as required.

---

## 2. Important — wrong `make import` hints (`scripts/mcp_check.py`)

`Makefile` `import:` target takes **one** `STAT` (`make import STAT=bea`), so `STAT=a, STAT=b`
imports only the last, and prose with «и» breaks make.

**Changed (multi-step hints only; single-target hints left as they were):**

| Line (before) | New text |
|---|---|
| ~544 (rosstat view empty) | `импортёры контура по одному: make import STAT=<имя> для каждого из rosstat_ipc_mes, rosstat_ipc_weeks, rosstat_salaries_mes, rosstat_vvp_kvartal` |
| ~567 (rosstat catalog short) | `импортёры по одному: make import STAT=<имя> для каждого из rosstat_ipc_mes, rosstat_ipc_weeks, rosstat_salaries_mes, rosstat_vvp_kvartal (каталог перепубликовывается каждым прогоном)` |
| ~222 (wgc catalog short) | `прогнать по одному: make import STAT=wgc_etf_flows, затем make import STAT=wgc_cb_reserves` |

No comma-separated `STAT=` list and no «и» inside a command remains.

```
$ python3 -m py_compile scripts/mcp_check.py scripts/extrapolation_aisc.py
OK
```

---

## 3. Important — stale numbers in the design spec

`docs/superpowers/specs/2026-10-14-rosstat-refactor-design.md`:

- line 105: «не содержит всех **128** товаров» → «не содержит всех **131** товара».
- line 113: `rosstat_ipc_weeks` (**128 рядов, 25691 строка**) → (**131 ряд, 26 611 строк**).

Line 97 left as is (141 = 4 + 131 + 5 + 1 is correct). I scanned the whole file (123 lines) for
other figures of this class — lines 91 and 97 already say 141, and no other series/row counts
appear; nothing else needed correcting.

```
$ grep -n "128\|25691\|25 691" docs/superpowers/specs/2026-10-14-rosstat-refactor-design.md
(no matches)
```

---

## 4. Important — net vs gross in the `ipc_weeks` fixture comment

`rosstat/ipc_weeks_test.go`: the comment claimed the difference from the live 25 691 is «+948»
with «обе составляющие». Net is **+920**. Now reads:

> Точки: 26 611. От прежней таблицы ipc_weeks и числа в rosstat-baseline.md (25 691) разбор
> отличается нетто на +920, и все три составляющие разницы — исправления дефектов разбора:
> −28 … +66 … +882 …
>
> Сумма составляющих: −28 + 66 + 882 = +920 (нетто). Число 948 — не разница, а грубая сумма
> возвращённых ячеек (66 + 882): нетто меньше неё ровно на 28 исчезнувших нулей старого разбора.

All three components named; 948 explicitly labelled as the gross of recovered cells.
`docs/ROADMAP_KEY_RATE.md` was **not** touched (its −28 / +948 / +3 gross breakdown is correct).

`gofmt` required a blank `//` separator line between the bullet list and the new summary
paragraph; that is included, so `gofmt -l .` is clean.

---

## 5. Minor items

| File | Change |
|---|---|
| `views/gold_views.go` | package comment «устаревшими `ipc_*`» → «устаревшими `rosstat_*`» (the gate names the `rosstat_*` tables). |
| `docs/IMPORTER_PACKAGE_STANDARD.md` §18 | `rosstat` row: `7 / 4 файла / 4 / нет / нет / 1 / цель текущего пункта` → `13 / — / — / да / да / 6 / **✅ соответствует** (фаза 2)`. Counts verified: 13 `.go` files, 6 test files, `importers.go` + `testdata/` present, zero `gocolly` and zero `fmt.Print` in `rosstat/`. Rest of the table untouched. |
| `docs/IMPORTER_PACKAGE_STANDARD.md` §4.2 | Status point made explicit: `GetFile` (and therefore `GetXlsx`) does **not** check the HTTP status at all — unlike `GetJSON`/`GetXML` — a WAF stub arrives with any code (200/403/426) and an HTML body, so the caller sees only an `excelize` parse error with no status and no filename; hence the parser must fail fast on the first sign (empty parse / missing expected sheet). |
| `util/xls.go` | `fileTimeout` comment: `GetFile (cbr/links.go)` → `GetFile (зовут его util/links.go — GetPage — и импортёры cbr/ и rosstat/ через GetXlsx)`. Callers verified by grep (`util/links.go:37`, `util/clickhouse_import.go:84` → `GetXlsx`, used by `cbr/` and `rosstat/` importers). |
| `rosstat/ipc_mes_test.go` | Assertion no longer a bare `"05"`; it is sheet-qualified (see concern 1 below). |
| `rosstat/salaries_mes.go` | Anchor comparison `row[0] == salariesMesField` → `strings.TrimSpace(row[0]) == salariesMesField`, matching `ipc_weeks.go:175`; comment notes it is the same anchor rule (§6.3). |
| `docs/superpowers/plans/2026-10-14-rosstat-refactor.md` line 21 | «`ipc_weeks` — те же ряды и точки» → «у `ipc_weeks` тождества нет — 131 ряд и 26 611 точек вместо 128 и 25 691, а значения расходятся в третьем знаке (`RawCellValue` читает сырой float, а не форматированный текст)». Nothing else in the plan rewritten. |
| `scripts/mcp_check.py` | Comment above the catalog check: old table names `ipc_mes`/`ipc_weeks`/`vvp_kvartal`/`salaries_mes` → `rosstat_*`. |
| `scripts/extrapolation_aisc.py` | Comments `ipc_mes` → `rosstat_ipc_mes` (two places, lines ~113 and ~120). Comments only, no logic. |

---

## Verification (all at the frozen, committed state)

```
$ gofmt -l .
(empty)

$ go build ./...
OK

$ python3 -m py_compile scripts/mcp_check.py scripts/extrapolation_aisc.py
OK

$ go test ./rosstat/ ./cbr/ ./util/ -count=1
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	0.533s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/cbr	3.064s
ok  	github.com/kmlebedev/clickhouse-import-rosstat/util	0.809s

$ go test ./... -count=1
EXIT=0   # 20 packages ok, no FAIL lines

$ grep -rn "ipc_mes\|ipc_weeks" scripts/*.py | grep -v rosstat_
(no matches — nothing remains; exit 1 from grep = no lines)
```

IDE check (`mcp__jetbrains__get_file_problems`) on all changed Go files —
`rosstat/vvp_kvartal.go`, `rosstat/vvp_kvartal_test.go`, `rosstat/salaries_mes.go`,
`rosstat/ipc_mes_test.go`, `rosstat/ipc_weeks_test.go`, `util/xls.go`, `views/gold_views.go`:
no errors. `vvp_kvartal.go` reports only pre-existing project-level warnings (SQL dialect not
configured on the constant query, and a Mend.io CVE advisory for `excelize`) — both unrelated
to this wave.

---

## Concerns / deviations

1. **`ipc_mes_test.go` assertion literal.** logrus' text formatter escapes quotes inside the
   message, so the buffer contains `лист \"05\"`, not the literal `лист "05"` of the review's
   example. The assertion is therefore written as the on-buffer form
   `` strings.Contains(logged.String(), `лист \"05\"`) ``, with a comment explaining the
   escaping. Intent satisfied (sheet-qualified text, not the bare `"05"`), but the literal
   suggested in the item would not have passed. `rosstat/ipc_weeks_test.go` asserts in the
   same spirit for product names (no quotes in those names, so no escaping issue there).
2. **§18 «Состояние» cell** was also updated (`цель текущего пункта` → `✅ соответствует
   (фаза 2)`), since leaving the stale target state would have contradicted the freshly updated
   counts. This is my interpretation of "update it to today's reality"; flip back to a
   different label if the state wording should differ.
3. **Numbers 131 / 26 611 / 141 are not independently re-verified against live ClickHouse** —
   `mcp__clickhouse__run_query` access is read-only and no importer was allowed to run. They are
   backed by the passing fixture tests (`TestIpcWeeksReadsFixture` asserts 26 611 points and 131
   series; `vvp_kvartal` asserts 302 points / 5 series) and by the existing catalog counts.
   Any live re-check remains part of the acceptance run (`make mcp-check`), as the spec's §6
   already requires.
4. **Nothing from the wave was skipped.** Not touched, per instructions:
   `deploy/staging/dagu.service`, `dashboard/rosstat.json`, `grafana_dashboard_rosstat`,
   `docs/ROADMAP_KEY_RATE.md`, and the `.superpowers/` SDD report files (the dangling
   `counts_after_rerun` reference needs no fix).
