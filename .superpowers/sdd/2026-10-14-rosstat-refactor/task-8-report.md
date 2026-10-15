# Task 8 report — Каталог, витрина и проверки MCP

Commit: `c25aa07` — `mcp_check: verify rosstat view, catalog and closed raw tables`
Files: `scripts/mcp_check.py` (+72), `rosstat/series_meta.go` (1 comment line).

## 1. What changed

### `scripts/mcp_check.py` — new rosstat block (appended after the `cbr_mp_summary` block, before `return failed`)

Three check groups, in the file's existing style (`expect` / `text` / `result_rows`), reusing the
file's existing access-denied idiom verbatim (`denied = "ACCESS_DENIED" in raw or "Not enough
privileges" in raw`, as in the `cbr_households_b_mes` line) — no new helper was invented; there is no
`denied_clickhouse()` function in the file, only that inline two-way test.

1. **View visible and non-empty.** `"v_rosstat_macro" in tables` (list_tables) *and* one query
   `SELECT count() AS rows, uniqExact(series) AS series FROM v_rosstat_macro` whose parsed answer must
   satisfy `rows > 0` **and** `series > 0`. An empty view from a non-running importer is a failure
   (unlike `v_dcf_assumptions`), so both numbers are gated. Expected point count is not asserted
   (it changes with every source run); distinct series count is asserted only as `> 0` here — the
   exact catalogue size is the next check.
2. **Catalogue complete.** `SELECT count() AS n FROM v_series_catalog WHERE source = 'rosstat'` must
   equal **141** = 4 ИПЦ (`ipc_mes`) + 131 недельный товар (`ipc_weeks`) + 5 ВВП (`vvp_kvartal`) +
   1 зарплата (`salaries_mes`). The detail prints the observed number whenever it differs, plus the
   `make import STAT=…` commands that republish the catalogue.
   **Deviation from the brief, deliberate and documented:** the brief/plan say **138**
   (4 + 128 + 5 + 1). That arithmetic predates Task 7's fix-round 1 (`fc0d28f`), which returned three
   products that the old sheet-terminator rule cut off, taking `ipcWeeksProducts` from 128 to 131
   names — `rosstat/series_meta.go` has 131 entries in `ipcWeeksProducts`, no duplicates (checked), and
   `afterRosstatMacro(ipcWeeksSeriesMeta())` publishes the list wholesale regardless of parsed points
   (`rosstat/ipc_weeks.go:254`), so the post-Task-9 catalogue is 141. The commit 5d326ca fixed the
   plan's T7 expectation to 131 series but did not touch the plan's T8 line. The dispatch prompt's own
   composition rule ("four monthly CPI + 131 weekly + five VVP + one salary") sums to 141. Both numbers
   fail identically today (132 ≠ 138 and 132 ≠ 141), so nothing is loosened; if the controller rules for
   the brief's literal 138, it is a one-integer edit.
3. **Raw tables closed.** Loop over `("rosstat_ipc_mes", "ИПЦ помесячно")`, `("rosstat_ipc_weeks",
   "недельные цены")`, `("rosstat_salaries_mes", "зарплата помесячно")`, `("rosstat_vvp_kvartal",
   "ВВП поквартально")`, each with `SELECT count() FROM <table>`; the answer must carry
   `ACCESS_DENIED` / `Not enough privileges`, else the raw error text is printed. Deliberate: a
   missing table (`UNKNOWN_TABLE`, code 60) is **not** success — the table must exist *and* be closed.

### `rosstat/series_meta.go` — stale `series` column comment of `rosstatMacroView`

```
- "series": "Название ряда; совпадает с series в v_series_catalog. Для ИПЦ и ВВП название содержит сноски и годы выгрузки",
+ "series": "Название ряда; совпадает с series в v_series_catalog. Имя каждого ряда закреплено в коде и при обновлении файла-источника не меняется: у ИПЦ, ВВП и зарплат это фиксированные названия, у недельных товаров и услуг — название товара из товарного списка самого источника",
```

The old text was true pre-branch and false after Task 6 (CPI names are pinned in code now, no years or
footnotes; VVP names were already stable). The surviving meaning ("matches `series` in
`v_series_catalog`) is kept. This closes the deferred Task 6 minor item
(`progress.md`, "перенесён в T8").

## 2. `make mcp-check` — verbatim rosstat lines

```
OK   витрина v_rosstat_macro видна: list_tables default
OK   витрина v_rosstat_macro непуста: строк: 27407, рядов: 132
FAIL каталог rosstat полон: рядов rosstat: 132, want 141 — прогнать make import STAT=ipc_mes, STAT=ipc_weeks, STAT=salaries_mes и STAT=vvp_kvartal (каталог перепубликовывается каждым прогоном)
FAIL сырая rosstat_ipc_mes закрыта (ИПЦ помесячно): Query execution failed: Received ClickHouse exception, code: 60, server response: Code: 60. DB::Exception: Unknown table expression identifier 'rosstat_ipc_mes'. In scope SELECT count() FROM rosstat_i
FAIL сырая rosstat_ipc_weeks закрыта (недельные цены): Query execution failed: Received ClickHouse exception, code: 60, server response: Code: 60. DB::Exception: Unknown table expression identifier 'rosstat_ipc_weeks'. In scope SELECT count() FROM rosstat
FAIL сырая rosstat_salaries_mes закрыта (зарплата помесячно): Query execution failed: Received ClickHouse exception, code: 60, server response: Code: 60. DB::Exception: Unknown table expression identifier 'rosstat_salaries_mes'. In scope SELECT count() FROM ross
FAIL сырая rosstat_vvp_kvartal закрыта (ВВП поквартально): Query execution failed: Received ClickHouse exception, code: 60, server response: Code: 60. DB::Exception: Unknown table expression identifier 'rosstat_vvp_kvartal'. In scope SELECT count() FROM rosst
ПРОВАЛЕНО: 5
make: *** [mcp-check] Error 1
```

(The `FAIL … сырая …` lines are the script's own `raw[:200]` slice — verbatim, hence the cut mid-word.)

**Reading — exactly the expected pre-Task-9 state, no check loosened:**

- View: **27407 rows / 132 series** — the pre-branch definition over the old tables `ipc_mes` /
  `ipc_weeks` (identical to the Task 3 baseline 27407 / 132). Both rosstat view checks pass because the
  view is visible and non-empty; they will tighten in meaning after Task 9 republishes the view over
  `rosstat_*`.
- Catalogue: **132**, not 141 (today 132 = 4 `ipc_mes` + 128 old weekly entries; `salaries_mes` and
  `vvp_kvartal` never reached `AfterImport` pre-branch). One failure, printed with the observed number.
- Raw tables: all four answer **code 60 UNKNOWN_TABLE**, not `ACCESS_DENIED` — the `rosstat_*` tables do
  not exist yet; the live old table `ipc_weeks` is `ACCESS_DENIED` (code 497) for `kimi_reader`, which
  is the behaviour the four new checks will see after Task 9 creates the tables. Each is a failure, as
  intended.
- Everything else in the run stayed green; every other contour is unaffected.

## 3. Commands run

| Command | Result |
|---|---|
| `python3 -m py_compile scripts/mcp_check.py` | exit 0 (`PY_COMPILE OK`) |
| `make mcp-check` | exit 2, `ПРОВАЛЕНО: 5` (the five rosstat failures above; all pre-T9 expected) |
| `gofmt -l rosstat/series_meta.go` | no output (formatted) |
| GoLand `get_file_problems rosstat/series_meta.go` | `{"errors":[]}` |
| `go test ./rosstat/ -run 'SeriesMeta\|Importers' -count=1` | `ok …/rosstat 0.495s` |
| read-only ClickHouse checks via MCP | `v_series_catalog` by source (rosstat = 132); `v_rosstat_macro` = 27407/132; `rosstat_ipc_mes` → UNKNOWN_TABLE; `ipc_weeks` → ACCESS_DENIED 497 |

No importer was run, no DDL/DML issued (read-only `SELECT`s only, through the MCP read-only user).
`deploy/staging/dagu.service` was already modified in the working tree before this task and was left
alone / not committed. `.superpowers/` is untracked, so this report is not part of the commit.

## 4. Self-review

- **Completeness:** all three check groups added (view visible + non-empty with both gates; catalogue
  exact count; four raw tables closed), modelled on the `v_cbr_macro` / `v_bea_pce` / `cbr_mp_summary`
  blocks; the stale `series` comment rewritten.
- **Honesty:** the `make mcp-check` outcome is reported as it ran — `ПРОВАЛЕНО: 5`, exit 2 — with the
  observed numbers (27407 / 132 / 132 / four × code 60); no expectation was relaxed, no importer run.
- **Discipline:** only `scripts/mcp_check.py` and `rosstat/series_meta.go` are in the commit; the four
  importer files, the registry, `dagu/`, `deploy/staging/dagu.service`, `dashboard/rosstat.json` and the
  Grafana dashboard files are untouched.
- **Secrets:** the added code prints only query strings and ClickHouse error text; no environment
  variable, password or connection string is read or printed, and the password stays masked by the make
  target.
- **No new dependencies**, no new helper function, no new file.

## 5. Issues / concerns

1. **138 vs 141 (needs a controller ruling).** The brief's `138` is arithmetically stale (predates
   `fc0d28f`, 128 → 131 weekly products); I implemented 141, which the prompt's own breakdown implies and
   the code confirms. If the ruling is "138 literally", change the two `141` occurrences (comment + three
   comparisons/detail strings) in the catalogue block; today's behaviour is identical either way.
2. **After Task 9 the raw-table checks depend on `kimi_reader` not having SELECT on the new
   `rosstat_*` tables.** The old `ipc_weeks` is already denied (497), so the grant model looks right, but
   the new tables' grants are Task 9's to confirm — if the MCP user is granted on them, those four checks
   will correctly stay red.
3. The plan/ledger (`progress.md`, `docs/superpowers/plans/2026-10-14-rosstat-refactor.md`, T8 section)
   still says 138; I did not touch plan documents (outside this task's file list).

---

## Fix-round 1

Commit: `53e8443` — `rosstat: truthful series comment; mcp_check hints use real step names`
Files: `rosstat/series_meta.go` (1 comment line), `scripts/mcp_check.py` (2 hint strings). Logic unchanged.

### Important 1 — the `series` comment claimed weekly names come from code (corrected)

The finding is correct. `rosstat/ipc_weeks.go:198` takes the name from the sheet —
`product := strings.TrimSpace(row[0])` — while `ipcWeeksProducts` is only the description catalogue:
`ipc_weeks.go:71-73` (`ipcWeeksKnownProducts`) and `:199-201` warn and **still record** an unknown
product (`"товар %q не описан в ipcWeeksProducts — точка записана, описание товара добавить в
каталог"`). That is the deliberate one-way contract of standard §11.4, so per the ruling the text was
fixed, not the parser; no parsing code was touched.

New value of the `series` column comment (only this line changed; `source`, `date`, `value` untouched):

```
"series": "Название ряда; совпадает с series в v_series_catalog. У помесячного ИПЦ, квартального ВВП и зарплаты имя закреплено в коде и при обновлении файла-источника не меняется. У недельных рядов имя берётся из файла — это название товара или услуги из товарного списка источника; каталог описывает текущий набор товаров, а товар, которого в v_series_catalog ещё нет, записывается в витрину с предупреждением в журнале, пока его описание не добавлено в код",
```

It separates the two cases as required (CPI/VVP/salary pinned in code; weekly names taken from the
source file, catalogue covering the current product set, unknown product imported with a journal
warning), and keeps "совпадает с `series` в `v_series_catalog`" for described series.

### Minor 2 — hints named non-existent steps (fixed)

Both operator hints now name the real steps. Evidence that those are the right names:
`util/clickhouse_import.go:79` — `func (s *ClickHouseImport) Name() string { return s.TableName }`, so
`CLICKHOUSE_IMPORT_STAT` (and the `make import STAT=…` filter) takes the table name; `dagu/rosstat.yaml`
carries step ids `rosstat_ipc_mes`, `rosstat_ipc_weeks`, `rosstat_salaries_mes`, `rosstat_vvp_kvartal`.

```
-  "импортёры контура (make import STAT=ipc_mes|ipc_weeks|salaries_mes|vvp_kvartal)")
+  "импортёры контура (make import STAT=rosstat_ipc_mes, "
+  "STAT=rosstat_ipc_weeks, STAT=rosstat_salaries_mes, STAT=rosstat_vvp_kvartal)")

-  "make import STAT=ipc_mes, STAT=ipc_weeks, STAT=salaries_mes и "
-  "STAT=vvp_kvartal (каталог перепубликовывается каждым прогоном)")
+  "make import STAT=rosstat_ipc_mes, STAT=rosstat_ipc_weeks, "
+  "STAT=rosstat_salaries_mes и STAT=rosstat_vvp_kvartal "
+  "(каталог перепубликовывается каждым прогоном)")
```

The checks themselves are unchanged: expectation **141**, code 60 (`UNKNOWN_TABLE`) still distinguished
from code 497 (`ACCESS_DENIED`) and still a failure.

### Commands run

| Command | Result |
|---|---|
| `python3 -m py_compile scripts/mcp_check.py` | exit 0 (`PY_COMPILE OK`) |
| `gofmt -l rosstat` | no output (nothing unformatted) |
| `go test ./rosstat/ -run 'SeriesMeta\|Importers' -count=1` | `ok …/rosstat 0.634s` |
| GoLand `get_file_problems rosstat/series_meta.go` | `{"errors":[]}` |
| `make mcp-check` | exit 2, `ПРОВАЛЕНО: 5` — identical to round 0 (see below) |

`make mcp-check` rosstat lines, verbatim:

```
OK   витрина v_rosstat_macro видна: list_tables default
OK   витрина v_rosstat_macro непуста: строк: 27407, рядов: 132
FAIL каталог rosstat полон: рядов rosstat: 132, want 141 — прогнать make import STAT=rosstat_ipc_mes, STAT=rosstat_ipc_weeks, STAT=rosstat_salaries_mes и STAT=rosstat_vvp_kvartal (каталог перепубликовывается каждым прогоном)
FAIL сырая rosstat_ipc_mes закрыта (ИПЦ помесячно): Query execution failed: Received ClickHouse exception, code: 60, server response: Code: 60. DB::Exception: Unknown table expression identifier 'rosstat_ipc_mes'. In scope SELECT count() FROM rosstat_i
FAIL сырая rosstat_ipc_weeks закрыта (недельные цены): Query execution failed: Received ClickHouse exception, code: 60, server response: Code: 60. DB::Exception: Unknown table expression identifier 'rosstat_ipc_weeks'. In scope SELECT count() FROM rosstat
FAIL сырая rosstat_salaries_mes закрыта (зарплата помесячно): Query execution failed: Received ClickHouse exception, code: 60, server response: Code: 60. DB::Exception: Unknown table expression identifier 'rosstat_salaries_mes'. In scope SELECT count() FROM ross
FAIL сырая rosstat_vvp_kvartal закрыта (ВВП поквартально): Query execution failed: Received ClickHouse exception, code: 60, server response: Code: 60. DB::Exception: Unknown table expression identifier 'rosstat_vvp_kvartal'. In scope SELECT count() FROM rosst
ПРОВАЛЕНО: 5
```

Fact as required: exit 2, `ПРОВАЛЕНО: 5`, all five failures rosstat-only (`grep -c '^FAIL'` = 5, `grep -v
rosstat` on those = 0), the four raw tables still answer code 60 (they do not exist before Task 9's
`make import`), the live catalogue is still 132 against the expected 141, and the view still reads
27407 rows / 132 series. `make mcp-check` is expected to go green only after Task 9 step 3; nothing was
relaxed to force it.

### Note on the 138 → 141 deviation raised in round 0

The controller has since ruled in favour of 141: commit `a4ffef7` `plan+spec: rosstat catalogue grows to
141 series after ipc_weeks product recovery` (plan + spec, 6 lines) sits between `c25aa07` and this
fix-round commit, so the deviation is now sanctioned by the plan and no code change is needed.

### Self-review

- Only the requested text changed: one comment line in `rosstat/series_meta.go`, two hint strings in
  `scripts/mcp_check.py`; check logic (141, code 60 vs 497) untouched.
- `ipc_weeks` parsing, the four importer files, the registry, `dagu/`, `deploy/staging/dagu.service`
  (still pre-existing dirty, left unstaged and unmodified), `dashboard/rosstat.json` and the Grafana
  dashboard files untouched.
- No importer run, no DDL/DML; only read-only `SELECT`s via the read-only MCP user. Nothing added can
  print a credential — the new strings are static hint text and ClickHouse error text only.
- `git status` after the commit: only `deploy/staging/dagu.service` remains modified (not mine).
