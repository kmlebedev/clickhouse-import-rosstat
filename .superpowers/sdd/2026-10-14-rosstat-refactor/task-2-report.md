# Task 2 report — реестр `rosstat/importers.go`, префикс `rosstat_*`, тест-энфорсер

Status: **DONE**
Commit: `b3a10b4 rosstat: single importer registry and rosstat_ table prefix`

## What I implemented

### Step 1 — table constants + count queries
- `rosstat/ipc_mes.go`: `ipcMesTable` → `rosstatIPCMesTable = "rosstat_ipc_mes"`; `ipcMesCountPoints` →
  `"SELECT count() FROM rosstat_ipc_mes FINAL"`; DDL already `%s`-form (verified, untouched).
- `rosstat/salaries_mes.go`: `salariesMesTable` → `rosstatSalariesMesTable = "rosstat_salaries_mes"`;
  count query → `rosstat_salaries_mes`. DDL already `%s`-form.
- `rosstat/vvp_kvartal.go`: `vvpKvartalTable` → `rosstatVVPKvartalTable = "rosstat_vvp_kvartal"`;
  count query → `rosstat_vvp_kvartal`. DDL already `%s`-form.
- `rosstat/ipc_weeks.go`: **Ruling 1 followed** — only the table-name constant renamed to
  `rosstatIPCWeeksTable = "rosstat_ipc_weeks"`. `ipcWeeksDdl` (still `"CREATE TABLE IF NOT EXISTS " +`
  concatenation, now producing valid SQL for the new table), `ipcWeeksInsert`, `Name()`, `export()`,
  `Import()` untouched; the `%s` conversion and removal of `ipcWeeksInsert` are left to Task 7. The three
  identifier *uses* of the renamed constant (`ipcWeeksDdl`, `ipcWeeksInsert`, `Name()`) followed the rename,
  as Ruling 1 requires (they must compile).
- DDL/`TableName`/`PublicationCountPoints` wiring in the three template-based importers updated to the new
  constants; comments that named the sibling importers by their old names updated
  (`rosstat_salaries_mes`, `rosstat_vvp_kvartal`), including the "регистрируется init() пакета" notes, which
  now point at the registry.

### Step 2 — named importer variables, no per-file `init()`
- `rosstat/ipc_weeks.go`: added `var ipcWeeksImporter = &IpcWeeksStat{}` (with a comment saying it is not yet
  ported to `util.ClickHouseImport`), deleted its per-file `init()`.
- The other three files: named variables kept, per-file `init()` deleted.
- The now-unused `chimport` import was dropped from all four files.
- Result: exactly one `func init()` in `rosstat/` — the registry's.

### Step 3 — `rosstat/importers.go` (new)
`rosstatImporter` struct (`chimport.ImportStat` + unexported `table`/`countPoints`), the `rosstatImporters`
slice with literal table names and literal count-point SQL (order = `dagu/rosstat.yaml`), and a single
`init()` appending the slice to `chimport.Stats`. Comment explains why the registry uses literals rather than
the importer constants (a constant-to-itself comparison would pass exactly on the rename the test must catch),
that the package occupies no shared table, and why `rosstat_ipc_weeks` has no preflight.

### Steps 4–5 — owner view and external references
- `rosstat/series_meta.go`: `rosstatMacroView.Tables` → new constants; `Select` → literals
  `rosstat_ipc_mes`/`rosstat_ipc_weeks`/`rosstat_vvp_kvartal`/`rosstat_salaries_mes`; view name `v_rosstat_macro`,
  `Comment`, `Columns` unchanged; the `salariesMes` description's `(ipc_mes)` reference → `(rosstat_ipc_mes)`.
- `dagu/rosstat.yaml`: four `id`s and four `CLICKHOUSE_IMPORT_STAT` values; schedule `17 12 * * 4`, step order and
  every `retry_policy` unchanged.
- `views/model_inputs.go`: `Tables` and every `Select` mention (`FROM … FINAL` twice for weeks, the year
  subquery) moved to `rosstat_ipc_mes`/`rosstat_ipc_weeks`.
- `views/gold_views.go`: the package comment that names those tables (required by the grep gate over `views/`;
  not in the brief's file list).
- `sql/key_rate_coverage.sql`: the four lines (label and `FROM`).
- `util/publications.go` (DDL comment example) and `cbr/views.go` (prose reference to the quarterly-date
  convention) updated to the renamed table. Nothing else changed in these files.

### Step 6 — `rosstat/importers_test.go` (new), `TestRosstatImportersPrefixedAndRegistered`
Port of `cbr/importers_test.go` with own literals: `rosstatNamePrefix = "rosstat_"`, empty
`sharedMacroSeriesTable` (so check (б) is "prefixed table + `Name() == table`", with the empty-column case
reported explicitly), `expectedPublicationCountPoints` (three entries, literal SQL), `publicationDeclarer`,
`daguRosstatYAML`, `daguStatRef`. Checks:
- (а) every `Name()` starts with `rosstat_`;
- (б) table prefixed with `rosstat_` and equal to `Name()`;
- (в) names unique and present in `chimport.Stats`;
- (д) count-point column compared with the importer's own `Publication()` declaration **and** with the literal
  expected query, `Publication().Source == Name()`, no preflight for importers outside the expected set (the
  `publicationDeclarer` assertion is comma-ok because `rosstat_ipc_weeks` has no `Publication()` at all), and the
  reverse direction — the map must not name an importer absent from the registry;
- count invariant: number of `rosstat_`-prefixed names in `chimport.Stats` == `len(rosstatImporters)`;
- (г) `../dagu/rosstat.yaml` completeness in **both** directions: every scheduled step exists in the registry, and
  every registered importer has a step (this is what pins the `rosstat_ipc_weeks` step — the importer has no
  declaration of any kind to verify the schedule against, see the comment in the test).

### Step 9 — commit
`git add` of the 13 changed/new files only; `deploy/staging/dagu.service` (pre-existing uncommitted owner change)
was deliberately left unstaged and untouched, as was `dashboard/rosstat.json`.

## TDD evidence

**RED #1** — enforcer written first, registry absent:

```
$ go test ./rosstat/ -count=1 -run 'TestRosstatImportersPrefixedAndRegistered'
# github.com/kmlebedev/clickhouse-import-rosstat/rosstat [github.com/kmlebedev/clickhouse-import-rosstat/rosstat.test]
rosstat/importers_test.go:91:9: undefined: rosstatImporters
rosstat/importers_test.go:100:36: undefined: rosstatImporters
... (6 sites)
FAIL	github.com/kmlebedev/clickhouse-import-rosstat/rosstat [build failed]
```
Expected: the enforcer is written against `rosstatImporters`, which does not exist yet.

**RED #2** — after the registry and the code rename, before the schedule was updated (real assertion failures,
not a compile error):

```
$ go test ./rosstat/ -count=1 -run 'TestRosstatImportersPrefixedAndRegistered'
--- FAIL: TestRosstatImportersPrefixedAndRegistered (0.00s)
    importers_test.go:226: шаг dagu/rosstat.yaml CLICKHOUSE_IMPORT_STAT=ipc_mes не найден в реестре rosstatImporters: ...
    ... (ipc_weeks, salaries_mes, vvp_kvartal)
    importers_test.go:236: в dagu/rosstat.yaml нет шага CLICKHOUSE_IMPORT_STAT=rosstat_ipc_weeks: импортёр зарегистрирован, но по расписанию не запускается
    ... (rosstat_salaries_mes, rosstat_vvp_kvartal, rosstat_ipc_mes)
FAIL
```
This is the evidence that the test checks an *independent* statement (the schedule file), not itself.

**GREEN** — after updating `dagu/rosstat.yaml`:

```
$ go test ./rosstat/ -count=1 -run 'TestRosstatImportersPrefixedAndRegistered' -v
=== RUN   TestRosstatImportersPrefixedAndRegistered
--- PASS: TestRosstatImportersPrefixedAndRegistered (0.00s)
PASS
ok  	github.com/kmlebedev/clickhouse-import-rosstat/rosstat	0.390s
```

**Mutation check (after green)** — registry count-point literal for `rosstat_ipc_mes` changed to
`"SELECT count() FROM rosstat_ipc_mes"` (FINAL dropped):

```
$ go test ./rosstat/ -count=1 -run 'TestRosstatImportersPrefixedAndRegistered'
--- FAIL: TestRosstatImportersPrefixedAndRegistered (0.00s)
    importers_test.go:175: импортёр "rosstat_ipc_mes" объявил запрос чека "SELECT count() FROM rosstat_ipc_mes FINAL", а реестр записал "SELECT count() FROM rosstat_ipc_mes"
    importers_test.go:180: реестр записал импортёру "rosstat_ipc_mes" запрос чека "SELECT count() FROM rosstat_ipc_mes", а спека §3.5 — "... FINAL"
FAIL
```
The mutation was reverted; the committed registry carries `FINAL` again (the green run above is post-revert).

## Verification commands and results

| Command | Result |
| --- | --- |
| `go test ./rosstat/ -count=1` | `ok … rosstat 0.641s` |
| `go test ./rosstat/ -count=1 -v -run 'Importers\|SeriesMeta'` | PASS — `TestRosstatImportersPrefixedAndRegistered`, `TestIpcWeeksSeriesMetaCoversFixture`, `TestIpcMesSeriesMetaCoversParsedSheets`, `TestVvpKvartalSeriesMetaCoversParsedSheets`, `TestSalariesMesSeriesMetaCoversParsedSheet` all pass (existing fixture tests stayed green) |
| `go build ./...` | OK (exit 0) |
| `gofmt -l rosstat views util cbr` | empty (I ran `gofmt -w` on those trees; `git diff` confirms nothing outside the intended files was reformatted) |
| `grep -rn '\bipc_mes\b\|\bipc_weeks\b\|\bsalaries_mes\b\|\bvvp_kvartal\b' --include=*.go rosstat/ views/ \| grep -v rosstat_` | empty |
| `go test ./... -count=1` (wider suite, before commit) | exit 0, no failures |
| `dagu validate dagu/rosstat.yaml` | accepted (exit 0); `dagu` is installed at `/opt/homebrew/bin/dagu`. Only output was the unrelated global-config warning `No auth.mode configured — defaulting to 'builtin'` |
| `grep -rn 'func init()' --include=*.go rosstat/` | only `rosstat/importers.go:56` |
| repo-wide `grep` for the four old names (`.go`, `.sql`, `.yaml`, excluding `.worktrees/`, `dashboard/`) | no hits outside `rosstat_`-qualified lines |

## Files changed

New: `rosstat/importers.go`, `rosstat/importers_test.go`.
Modified: `rosstat/ipc_mes.go`, `rosstat/ipc_weeks.go`, `rosstat/salaries_mes.go`, `rosstat/vvp_kvartal.go`,
`rosstat/series_meta.go`, `dagu/rosstat.yaml`, `views/model_inputs.go`, `views/gold_views.go`,
`sql/key_rate_coverage.sql`, `util/publications.go`, `cbr/views.go`.
Not touched: `deploy/staging/dagu.service` (pre-existing owner change), `dashboard/rosstat.json`, docs/README
(later tasks), `sql/mcp_kimi_reader.sql` (grant is on `v_rosstat_macro`, whose name did not change — verified it
names no old table).

## Self-review findings (fixed)

- Two comment sentences in the test had bad wrapping after in-place edits (`… rosstat_vvp_kvartal) и не /
  срабатывает …`; the "Обратное направление цикл не ловит" wording, which contradicted the loop right below it) —
  rewritten.
- One `Publication()` call was duplicated inside an error branch — now computed once into `pub`.
- Comment claiming "У всех импортёров пакета `Name()` — это `TableName`" was imprecise (`ipc_weeks` overrides
  `Name()`) — corrected.
- A first attempt at the "регистрируется init() пакета" comment edits truncated the sentence
  ("… регистрируется реестром" with no object); caught on re-read and fixed to "реестром rosstat/importers.go".
- Comments/doc lines that named old table or importer names were updated so the grep gate is genuinely empty
  (including `rosstat/series_meta.go`'s `(ipc_mes)` description text and `views/gold_views.go`'s package comment);
  source *file* names such as `ipc_mes_ММ-ГГГГ.xlsx` / `Nedel_ipc.xlsx` were left alone (they are Rosstat's
  artifact names, not tables — the gate's `\b` word boundary does not match them).
- Test file inspected for self-testing shape: literals are compared against `Name()`, the importers' own
  `Publication()` and the YAML file — three independent sources; the mutation check above demonstrates it.

## Issues / concerns

- **Ruling 1 changed what I did** (explicitly, as instructed): `rosstat/ipc_weeks.go` keeps its own
  `Import()`/`export()`, so `ipcWeeksDdl` remains the string-concatenation form and `ipcWeeksInsert` was **not**
  deleted; only the table-name constant was renamed (and its three uses followed). The brief's Step 1 wording for
  `ipc_weeks` was not applied. Consequence: `rosstat_ipc_weeks` is registered in the new registry with a
  `Name()` that is currently a constant returning the new table name, but it does not go through
  `util.ClickHouseImport` until Task 7 — the registry handles that explicitly (`countPoints == ""`).
- The reverse dagu check in the enforcer is implemented over the **whole registry** (every registered importer
  must be a scheduled step), which is strictly stronger than the brief's "the `rosstat_ipc_weeks` step must be in
  the schedule"; the brief's motivating case is named in the comment and is covered by it.
- `views/gold_views.go` was edited although it is not in the brief's file list: its package comment names
  `ipc_mes`/`ipc_weeks`, and the required grep gate over `views/` cannot be empty without it (ambiguity #2's
  "comments/reference lines, update the table names" applies). Three lines, comment only.
- No ClickHouse queries were needed (MCP `clickhouse` tools unused); no DB or live-run verification was
  requested by the brief. JetBrains MCP tools were not needed either — navigation and edit checks were done with
  Read/Grep plus `go build`/`go test`/`gofmt`; nothing was silently skipped.
