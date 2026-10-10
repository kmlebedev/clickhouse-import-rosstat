# Shared Company Financials Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the MCP agent read access to Polyus financial data through a shared sector-level view, lifting ROADMAP §8.1 defects (3) and (4).

**Architecture:** One `company_financials` table keyed by `(company, metric, period, source_kind)` replaces the per-company table shape; three views sit over it (`v_company_financials` — resolved value, `v_company_metric_sources` — every value including disagreements, `v_company_operating` — the xlsx datapack). Views and table are created by a new `company_views` importer; `GRANT SELECT` is issued separately by hand via `make mcp-user`. The legacy `polyus_financial_metrics` table is frozen, left in place, and keeps feeding the Grafana dashboard.

**Tech Stack:** Go 1.x, clickhouse-go/v2 (`driver.Conn`), ClickHouse 26.10, logrus, standard `testing`.

**Spec:** [docs/superpowers/specs/2026-10-10-company-financials-views-design.md](../specs/2026-10-10-company-financials-views-design.md)

## Global Constraints

- DDL always `CREATE TABLE IF NOT EXISTS`; engine `ReplacingMergeTree`; dimensions `LowCardinality(String)`; new monetary/price values `Float64`.
- Views use `CREATE OR REPLACE VIEW ... DEFINER = default SQL SECURITY DEFINER`.
- Comments go via `ALTER TABLE ... MODIFY COMMENT` and `ALTER TABLE ... COMMENT COLUMN`; `COMMENT ON TABLE/COLUMN` does not work in ClickHouse 26.10.
- `GRANT SELECT` for `kimi_reader` lives only in `sql/mcp_kimi_reader.sql`, executed by hand — no importer issues grants.
- Insert only in batches (`PrepareBatch` → `Append` → `Send`); no per-row `conn.Exec` in a loop.
- No network calls in `init()`.
- Never swallow errors: every `strconv.Parse*` and `batch.Append` is checked.
- `source_kind` values are exactly `kpi`, `ifrs`, `datapack`, `legacy` (lowercase).
- `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, build) must pass.
- Do not modify the DDL of `polyus_financial_metrics` or `databook_polyus` — both are read by Grafana.
- No new dependency may be added (MAGN's `.xls` is explicitly out of scope).

## Review Focus

- **`source_kind` in the ordering key.** The whole point of the change is that two documents printing the same metric for the same period stop overwriting each other. If `source_kind` is absent from `ORDER BY`, or `batchDedup` still keys on `(metric, period)` only, the importer will silently drop the second document exactly as before and every test still passes.
- **Row count must not move.** Widening the key can change how many rows are inserted. The spec pins 265 rows; a different number is a finding to explain, not to accept.
- **Existing metric values must survive untouched.** `TestKPIValuesHistoryReports` and `TestExistingReportsUnchanged` pin read values. If those fail after this change, the change has broken parsing, not the schema.
- **Raw tables stay closed to the agent.** A view that is created but not granted looks identical to a working one from the repo's side. `list_tables` visibility and the `ACCESS_DENIED` on raw tables are both required.
- **Datapack rows must not silently join the release metrics.** `v_company_operating` and `v_company_financials` are separate; a metric name present in both (`gold_output`) must never be answered from the wrong one.
- **No `period = ''` rows reach the table.** A record with an empty period collides on a single key `(metric, "", source_kind)`. `guardRecords` already drops these for both report kinds (`TestGuardAppliesToIFRSRecords`, `TestGuardDropsUnassignedValues`); the risk here is that the widened key quietly changes that, so Task 1's widened key must not be applied to the record before `applyGuard` runs. `make all` covers the existing guard tests.

---

### Task 1: Add `SourceKind` to the metric record and the batch key

**Files:**
- Modify: `polyus/metrics.go` (add field to `MetricRecord`)
- Modify: `polyus/import.go` (`batchKey`, `batchDedup.add`, `batch.Append` call)
- Modify: `polyus/kpi.go:314` (set the field), `polyus/ifrs.go:121` (set the field)
- Test: `polyus/source_kind_test.go` (create)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `MetricRecord.SourceKind string`; `batchKey{Company, Metric, Period, SourceKind string}`; `func (d *batchDedup) add(record MetricRecord) bool` (signature unchanged, key widened).

- [ ] **Step 1: Write the failing test**

```go
package polyus

import "testing"

// Two documents printing the same metric for the same period must both
// survive the batch: the key carries the document kind.
func TestBatchDedupKeepsBothSourceKinds(t *testing.T) {
	var d batchDedup

	kpiRecord := MetricRecord{Company: "PLZL", Metric: "gold_output", Period: "2023FY", SourceKind: "kpi"}
	ifrsRecord := MetricRecord{Company: "PLZL", Metric: "gold_output", Period: "2023FY", SourceKind: "ifrs"}

	if !d.add(kpiRecord) {
		t.Fatal("first record with source_kind=kpi must enter the batch")
	}
	if !d.add(ifrsRecord) {
		t.Fatal("same key with source_kind=ifrs must also enter the batch: the key carries the source")
	}
	if d.duplicates != 0 {
		t.Fatalf("duplicates = %d, want 0: the two records differ by source_kind", d.duplicates)
	}
	if d.add(kpiRecord) {
		t.Fatal("a true repeat of the same source_kind must be rejected")
	}
	if d.duplicates != 1 {
		t.Fatalf("duplicates = %d, want 1", d.duplicates)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./polyus/ -run TestBatchDedupKeepsBothSourceKinds -v`
Expected: FAIL — `unknown field 'SourceKind' in struct literal of type MetricRecord`.

- [ ] **Step 3: Add the field and widen the key**

In `polyus/metrics.go` add to `MetricRecord`, after `PeriodType`:

```go
	SourceKind string `json:"source_kind"`
```

In `polyus/import.go` extend `batchKey` with `SourceKind string` and build it in `add` as `batchKey{record.Company, record.Metric, record.Period, record.SourceKind}`. Update the `batchKey` doc comment to say the key is the table's `ORDER BY` and now carries the document kind, so two documents printing the same metric for the same period no longer collapse.

- [ ] **Step 4: Set the field at both construction sites**

`polyus/kpi.go` `recordsFromLine`: add `SourceKind: "kpi",`.
`polyus/ifrs.go` `parseIFRSPage`: add `SourceKind: "ifrs",`.

- [ ] **Step 5: Run the package tests**

Run: `go test ./polyus/ -v`
Expected: PASS, including `TestBatchDedupKeepsBothSourceKinds`.

- [ ] **Step 6: Commit**

```bash
git add polyus/metrics.go polyus/import.go polyus/kpi.go polyus/ifrs.go polyus/source_kind_test.go
git commit -m "polyus: carry source kind in the batch key"
```

---

### Task 2: Point the importer at `company_financials`

**Files:**
- Modify: `polyus/import.go` (`financialMetricsTable`, `financialMetricsCreateTable`, `batch.Append` args)
- Test: `polyus/source_kind_test.go` (extend)

**Interfaces:**
- Consumes: `MetricRecord.SourceKind` from Task 1.
- Produces: table name `company_financials`; the canonical DDL string other tasks copy from.

- [ ] **Step 1: Write the failing test**

```go
// The importer writes the shared table, and the DDL carries the document
// kind in ORDER BY — without it the two documents collapse again.
func TestFinancialMetricsTargetsCompanyFinancials(t *testing.T) {
	if financialMetricsTable != "company_financials" {
		t.Fatalf("table = %q, want company_financials", financialMetricsTable)
	}
	for _, want := range []string{"company_financials", "source_kind", "ORDER BY (company, metric, period, source_kind)"} {
		if !strings.Contains(financialMetricsCreateTable, want) {
			t.Fatalf("DDL missing %q:\n%s", want, financialMetricsCreateTable)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./polyus/ -run TestFinancialMetricsTargetsCompanyFinancials -v`
Expected: FAIL — table is `polyus_financial_metrics`.

- [ ] **Step 3: Change the table name and DDL**

In `polyus/import.go` set `financialMetricsTable = "company_financials"` and replace `financialMetricsCreateTable` with the spec §3.1 DDL: `company`, `metric`, `period`, `period_type Enum8('Q'=1,'H'=2,'FY'=3,'LTM'=4)`, `source_kind LowCardinality(String)`, `value Nullable(Float64)`, `unit LowCardinality(String)`, `source_url LowCardinality(String)`, `source_page UInt16`, `loaded_at DateTime DEFAULT now()`, engine `ReplacingMergeTree(loaded_at)`, `ORDER BY (company, metric, period, source_kind)`. Keep the `%s` placeholder for the table name. Add a line to the doc comment saying `source_kind` is what keeps disagreeing documents visible, and that `legacy` marks rows moved from `polyus_financial_metrics`.

- [ ] **Step 4: Add the argument to `batch.Append`**

In `Import`, insert `record.SourceKind` between `record.PeriodType` and `record.Value`.

- [ ] **Step 5: Point the surviving raw-data accessors at the legacy table**

Update the comment above `financialMetricsTable` so it no longer claims the schema came from the legacy importer unchanged, and note that `polyus_financial_metrics` remains in place for `dashboard/finance-polyus.json` and is no longer written.

- [ ] **Step 6: Run the package tests**

Run: `go test ./polyus/ -v`
Expected: PASS.

- [ ] **Step 7: Verify the build**

Run: `make all`
Expected: exit 0; `gofmt` clean, linter clean.

- [ ] **Step 8: Commit**

```bash
git add polyus/import.go polyus/source_kind_test.go
git commit -m "polyus: write metrics into shared company_financials"
```

---

### Task 3: Create the `company_views` importer with the table and three views

**Files:**
- Create: `views/company.go`
- Test: `views/company_test.go` (create)

**Interfaces:**
- Consumes: nothing from earlier tasks (the DDL text is duplicated from Task 2's `financialMetricsCreateTable`; they must match).
- Produces: `companyFinancialsTable` (const, `"company_financials"`); `companyFinancialsCreateTable` (const DDL); `companyFinancialsView`, `companyMetricSourcesView`, `companyOperatingView` (`util.View` values); `companyViewsDDL()` returning `[]string` for table + comments; importer with `Name() = "company_views"`.

- [ ] **Step 1: Write the failing test**

```go
package views

import (
	"strings"
	"testing"
)

// Each view must name all three of its sources in the ORDER BY / resolution
// key, and the comments must reach the agent: a view without comments is
// documented nowhere the agent can look.
func TestCompanyViewsAreDocumented(t *testing.T) {
	for _, v := range []util.View{companyFinancialsView, companyMetricSourcesView, companyOperatingView} {
		if v.Comment == "" {
			t.Fatalf("view %s has no table comment", v.Name)
		}
		if v.Select == "" {
			t.Fatalf("view %s has no SELECT", v.Name)
		}
		if len(v.Columns) == 0 {
			t.Fatalf("view %s has no column comments", v.Name)
		}
	}
}

// The resolved view must prefer the audited form; the sources view must not
// apply any preference, otherwise a disagreement becomes invisible.
func TestCompanyFinancialsResolutionPrefersIFRS(t *testing.T) {
	if !strings.Contains(companyFinancialsView.Select, "'ifrs'") {
		t.Fatal("v_company_financials must reference source_kind 'ifrs' in its resolution")
	}
	for _, marker := range []string{"argMax", "row_number", "LIMIT 1 BY", "argMin"} {
		if strings.Contains(companyMetricSourcesView.Select, marker) {
			t.Fatalf("v_company_metric_sources must list every source, found %q", marker)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./views/ -run TestCompany -v`
Expected: FAIL — `undefined: companyFinancialsView`.

- [ ] **Step 3: Write `views/company.go`**

Write the three `util.View` values and the table DDL.

`companyFinancialsView.Select` — one row per `(company, metric, period)`, resolved by spec §3.4 order: `ifrs` first, then newest `loaded_at`, then `source_url` as a deterministic tie-break. Use `argMax((source_kind, value, source_url, source_page, unit, period_type), tuple(...))` or an explicit `row_number() OVER (PARTITION BY company, metric, period ORDER BY ...)` subquery — pick one and keep it; the ORDER BY inside must read `source_kind = 'ifrs' DESC, loaded_at DESC, source_url DESC`.

`companyMetricSourcesView.Select` — `SELECT company, metric, period, period_type, source_kind, value, unit, source_url, source_page, loaded_at FROM company_financials FINAL ORDER BY company, metric, period, source_kind`. No filtering, no preference.

`companyOperatingView.Select` — spec §4.2: `SELECT 'PLZL' AS company, table AS asset, name AS metric, data AS frequency, date, value FROM databook_polyus FINAL`.

`companyViewsDDL()` — the `CREATE TABLE IF NOT EXISTS company_financials ...` statement, identical to Task 2's `financialMetricsCreateTable` (same columns, same engine, same `ORDER BY`).

Comments: one `Comment` per view stating what it answers, and a `Columns` map covering every column. For `v_company_financials` the `source_kind` and `source_url` comments must say they identify which document won. For `v_company_operating` the `asset` and `frequency` comments must say the datapack is a different granularity from the release metrics and must not be mixed with them.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./views/ -run TestCompany -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add views/company.go views/company_test.go
git commit -m "views: add shared company_financials views"
```

---

### Task 4: Wire the importer: table, views, comments, series catalog

**Files:**
- Modify: `views/company.go` (add the `Import` method and `init()`)
- Test: `views/company_test.go` (extend)

**Interfaces:**
- Consumes: `util.CreateView`, `util.UpsertSeriesCatalog`, `util.SeriesMeta`; `companyViewsDDL()`.
- Produces: `Name() = "company_views"`; the importer registers itself in `chimport.Stats`.

- [ ] **Step 1: Write the failing test**

```go
func TestCompanyViewsImporterName(t *testing.T) {
	if got := (&companyViews{}).Name(); got != "company_views" {
		t.Fatalf("Name() = %q, want company_views", got)
	}
}

// The table must be created before any view reads it, and comments must
// follow the view that carries them.
func TestCompanyViewsDDLOrder(t *testing.T) {
	stmts := companyViewsDDL()
	if len(stmts) == 0 {
		t.Fatal("no DDL statements")
	}
	if !strings.Contains(stmts[0], "company_financials") {
		t.Fatalf("first statement must create the table, got %q", stmts[0])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./views/ -run TestCompanyViews -v`
Expected: FAIL — `undefined: companyViews`.

- [ ] **Step 3: Implement `Import`**

```go
type companyViews struct{}

func (s *companyViews) Name() string { return "company_views" }

func (s *companyViews) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, stmt := range companyViewsDDL() {
		if err = conn.Exec(ctx, stmt); err != nil {
			return count, err
		}
	}
	for _, v := range []util.View{companyFinancialsView, companyMetricSourcesView, companyOperatingView} {
		var created bool
		if created, err = util.CreateView(ctx, conn, v); err != nil {
			return count, err
		}
		log.Infof("View %s created: %t", v.Name, created)
	}
	if err = util.UpsertSeriesCatalog(ctx, conn, polyusSeriesMeta()); err != nil {
		return count, err
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &companyViews{})
}
```

Note: `util.CreateView` already issues the `MODIFY COMMENT` and `COMMENT COLUMN` statements from the `util.View`. Its `Tables` gate must list the view's real sources, so `companyFinancialsView.Tables` and `companyMetricSourcesView.Tables` = `[]string{"company_financials"}`, and `companyOperatingView.Tables` = `[]string{"databook_polyus"}` — the gate is what keeps `company_views` from running before those tables exist.

- [ ] **Step 4: Add `polyusSeriesMeta()`**

In `views/company.go`, return one `util.SeriesMeta` per metric the Polyus importer writes: `Source: "polyus"`, `Series` = the metric name, plus `Title`, `Unit`, `Frequency`, `Origin` (report URL/page shape), `Description` (what it measures and how to compute rates). Add `Source: "polyus_datapack"` rows for the datapack metrics, with `Origin` stating asset-level granularity.

Take the metric list from `polyus/metrics.go`'s `metrics` slice — read it and cover every `Name` except `period`. The test in Task 5 enforces coverage.

- [ ] **Step 5: Run the tests**

Run: `go test ./views/ -v`
Expected: PASS.

- [ ] **Step 6: Verify the build**

Run: `make all`
Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
git add views/company.go views/company_test.go
git commit -m "views: add company_views importer with series catalog"
```

---

### Task 5: Cover the series catalog and run it against ClickHouse

**Files:**
- Modify: `views/company_test.go` (extend)
- Modify: `views/company.go` (fill gaps the test finds)

**Interfaces:**
- Consumes: `polyusSeriesMeta()` from Task 4; the `metrics` slice in `polyus/metrics.go`.
- Produces: nothing later tasks consume.

- [ ] **Step 1: Write the failing test**

The test lives in `views/` and needs the metric names, which are unexported in `polyus/`. Add an exported accessor in `polyus/metrics.go`:

```go
// MetricNames returns the metric names the PDF parser can produce, so the
// series catalog can be checked for coverage without exporting the whole
// definition table.
func MetricNames() []string {
	names := make([]string, 0, len(metrics))
	for _, m := range metrics {
		if m.Name == "period" {
			continue
		}
		names = append(names, m.Name)
	}
	return names
}
```

Then the coverage test:

```go
func TestSeriesCatalogCoversEveryPolyusMetric(t *testing.T) {
	described := map[string]bool{}
	for _, m := range polyusSeriesMeta() {
		if m.Source == "polyus" {
			described[m.Series] = true
		}
	}
	for _, name := range polyus.MetricNames() {
		if !described[name] {
			t.Errorf("metric %q has no series_catalog entry", name)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./views/ -run TestSeriesCatalogCoversEveryPolyusMetric -v`
Expected: FAIL listing the metrics that are not yet described.

- [ ] **Step 3: Add the missing entries**

Add a `util.SeriesMeta` for each metric named in the failure output.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./views/ -run TestSeriesCatalogCoversEveryPolyusMetric -v`
Expected: PASS.

- [ ] **Step 5: Confirm no other package's tests broke**

Run: `make all`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add views/company.go views/company_test.go polyus/metrics.go
git commit -m "views: cover every polyus metric in the series catalog"
```

---

### Task 6: Grant the three views to `kimi_reader`

**Files:**
- Modify: `sql/mcp_kimi_reader.sql`
- Modify: `scripts/mcp_check.py`
- Modify: `dagu/financial.yaml` (add the `company_views` step)

**Interfaces:**
- Consumes: view names from Task 3.
- Produces: nothing later tasks consume.

- [ ] **Step 1: Add the grants**

Append to `sql/mcp_kimi_reader.sql`:

```sql
GRANT SELECT ON default.v_company_financials TO kimi_reader;
GRANT SELECT ON default.v_company_metric_sources TO kimi_reader;
GRANT SELECT ON default.v_company_operating TO kimi_reader;
```

Do not add grants for `company_financials`, `polyus_financial_metrics`, or `databook_polyus` — the agent reads views only.

- [ ] **Step 2: Extend the MCP smoke check**

In `scripts/mcp_check.py`, inside `check()`, add after the `v_bea_pce` assertion:

```python
            for view in ("v_company_financials", "v_company_metric_sources", "v_company_operating"):
                failed += not expect(f"витрина {view} видна", view in tables, "list_tables default")

            resolved = text(await session.call_tool("run_query", {"query": "SELECT metric, period, value, source_kind, source_url FROM v_company_financials WHERE metric = 'gold_output' ORDER BY period"}))
            failed += not expect("метрики Полюса читаются", "rows" in resolved and "Query execution failed" not in resolved, resolved[:200])

            raw = text(await session.call_tool("run_query", {"query": "SELECT count() FROM polyus_financial_metrics"}))
            denied = "ACCESS_DENIED" in raw or "Not enough privileges" in raw
            failed += not expect("сырые метрики Полюса закрыты", denied, "отказ ClickHouse на polyus_financial_metrics (ожидаемо)" if denied else raw[:200])
```

- [ ] **Step 3: Apply the grants and run the smoke check**

Run, in order:

```bash
make import STAT=company_views
make import STAT=polyus_financial_metrics
make mcp-user
make mcp-check
```

Expected: `mcp-user` prints `OK` for every statement including the three new grants; `mcp-check` prints `ПРОВЕРКА ПРОЙДЕНА`.

- [ ] **Step 4: Add the DAG step**

In `dagu/financial.yaml` append a third step **after** `polyus_financial_metrics`, matching the file's existing shape:

```yaml
  - id: company_views
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=company_views
    retry_policy:
      limit: 3
      interval_sec: 300
```

The order matters: the views are created over tables the earlier steps fill.

- [ ] **Step 5: Commit**

```bash
git add sql/mcp_kimi_reader.sql scripts/mcp_check.py dagu/financial.yaml
git commit -m "mcp: grant the company views to kimi_reader"
```

---

### Task 7: Acceptance — the disagreement is visible and the counts hold

**Files:**
- Modify: `docs/superpowers/specs/2026-10-10-company-financials-views-design.md` (record the acceptance result)
- Test: `polyus/source_kind_test.go` (extend)

**Interfaces:**
- Consumes: everything above.
- Produces: the written acceptance record.

- [ ] **Step 1: Write the test pinning the FY2023 disagreement**

This must be a **fixture-level** test, not one that calls `parseReport`: `parseReport` downloads the PDF over the network, and this repo's parser tests run without network access. Read the two TSV fixtures under `polyus/testdata/` for the FY2023 and FY2024 reports through the same `readTSVLines` → `parseKPILines` path the other KPI tests use, and assert each fixture yields `gold_output 2023FY`:

```go
// The FY2023 release prints 2902 for gold_output; the FY2024 release prints
// 2799 for the same period. Both are read correctly — the documents
// disagree, and the pipeline must keep both. Fixture names come from the
// existing history tests in this package.
func TestBothFixturesOfTheFY2023GoldOutputSurvive(t *testing.T) {
	cases := []struct {
		fixture string
		want    float64
	}{
		{"testdata/press_release_fy2023_p4.tsv", 2902},
		{"testdata/press_release_fy2024_p4.tsv", 2799},
	}
	for _, tc := range cases {
		lines, err := readTSVLines(tc.fixture)
		if err != nil {
			t.Fatalf("%s: %v", tc.fixture, err)
		}
		records, _, _ := parseKPILines(lines, tc.fixture, 0)
		var found bool
		for _, r := range records {
			if r.Metric == "gold_output" && r.Period == "2023FY" {
				found = true
				if r.Value != tc.want {
					t.Errorf("%s: gold_output 2023FY = %v, want %v", tc.fixture, r.Value, tc.want)
				}
			}
		}
		if !found {
			t.Errorf("%s: gold_output 2023FY not produced", tc.fixture)
		}
	}
}
```

The fixture names above are the ones the package already uses (`polyus/testdata/press_release_fy2023_p4.tsv`, `press_release_fy2024_p4.tsv`; see `TestKPIValuesHistoryReports` in `polyus/kpi_test.go`). Also assert `SourceKind`: both fixtures are KPI releases, so both records carry `"kpi"` — the point of this test is that the *values* differ and both survive, while Task 1's test covers the source-kind keying.

- [ ] **Step 2: Run it**

Run: `go test ./polyus/ -run TestBothFixturesOfTheFY2023GoldOutputSurvive -v`
Expected: PASS. If the fixtures the test names do not exist, fix the names rather than weakening the assertions — the test is worthless if it finds no fixture and passes.

- [ ] **Step 3: Run the full pipeline and record the outcome**

Run:

```bash
make all
make import STAT=company_views
make import STAT=polyus_financial_metrics
```

Record in the spec: the exact `Imported N rows ...` line, the per-period row counts, and whether `N` is 265. **If `N` differs from 265, record the discrepancy and explain it — do not adjust the number in the spec to match.**

- [ ] **Step 4: Query both views through MCP and record the output**

Run via the ClickHouse MCP tool:

```sql
SELECT metric, period, source_kind, value, source_url
FROM v_company_metric_sources
WHERE metric = 'gold_output' AND period = '2023FY'
ORDER BY value;
```

Expected: two rows, 2 799 and 2 902, with different `source_url`.

```sql
SELECT metric, period, source_kind, value, source_url
FROM v_company_financials
WHERE metric = 'gold_output' AND period = '2023FY';
```

Expected: one row whose `source_kind` and `source_url` say which document won.

Paste both outputs into the acceptance record.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-10-10-company-financials-views-design.md polyus/source_kind_test.go
git commit -m "docs: record company_financials acceptance"
```

---

### Task 8: Sync the documentation

**Files:**
- Modify: `ARCHITECTURE.md` (§6.2, §6.3, §6.5, §6.6)
- Modify: `README.md` (importer table, structure, limitations)
- Modify: `docs/ROADMAP_DCF_POLYUS.md` (§8, §8.1, phase 2)

**Interfaces:**
- Consumes: the acceptance values from Task 7.
- Produces: nothing later tasks consume.

- [ ] **Step 1: Update `ARCHITECTURE.md`**

- §6.2: add the canonical `company_financials` DDL; mark `polyus_financial_metrics` as legacy (kept for the Grafana dashboard, no longer written).
- §6.3: move the three `v_company_*` views from "не реализован" to "реализован" with their sources and the resolution rule.
- §6.5: state that rule 11 for Polyus is now satisfied — series catalog, comments, grant.
- §6.6: mark defect (3) closed by `source_kind` in the key and defect (4) closed by the views; restate what remains open (2015–2018, the `unitsMarker` limitation, the dead `scanPeriodColumns` code).

- [ ] **Step 2: Update `README.md`**

Add `company_views` to the importer table with its schedule; add `views/` to the structure listing; rewrite the three Polyus limitation bullets so the lifted ones are gone and the surviving ones are accurate.

- [ ] **Step 3: Update `docs/ROADMAP_DCF_POLYUS.md`**

Mark defects (3) and (4) closed in §8.1 and in phase 2's table; link the metals roadmap where the metals question is raised.

- [ ] **Step 4: Verify per the sync skill**

Re-read `.kimi-code/skills/sync-readme-architecture/SKILL.md` and walk its checklist. Confirm no deleted file or command is still mentioned.

- [ ] **Step 5: Run the full check**

Run: `make all`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add ARCHITECTURE.md README.md docs/ROADMAP_DCF_POLYUS.md
git commit -m "docs: sync architecture and readme for company_financials"
```
