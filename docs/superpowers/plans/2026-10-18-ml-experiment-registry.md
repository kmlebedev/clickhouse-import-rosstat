# ML Experiment Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a ClickHouse-backed experiment registry: every backtest run gets a machine-readable passport, fold-level predictions, metrics, and artifact hashes, for both `cpi_nowcast` and `key_rate_model`.

**Architecture:** A new Go importer `mlregistry` owns the DDL and three views. A Python helper `scripts/ml_experiment.py` captures git/config/data/environment metadata, writes one batch row to `ml_runs` and one batch of fold rows to `ml_predictions` via the existing HTTP transport (`ch_exec`), and falls back to a local `run.json` if ClickHouse is unavailable. Both Python scripts opt in with `ML_LOG=1`, so their default behavior stays byte-identical.

**Tech Stack:** Go 1.x (existing repo toolchain), ClickHouse (`ReplacingMergeTree`, `LowCardinality(String)`), Python 3.12 stdlib + existing `uv` environment, `unittest`/`unittest.mock`, `dagu` YAML.

**Spec:** `docs/superpowers/specs/2026-10-18-ml-experiment-registry-design.md`

## Global Constraints

- Python 3.12; no new mandatory Python dependencies; run tests with `python3 scripts/<name>_test.py`.
- ClickHouse writes only as one batch per table: `INSERT INTO <table> FORMAT JSONEachRow`; no per-row `Exec`.
- DDL: `CREATE TABLE IF NOT EXISTS`, `ReplacingMergeTree`, `LowCardinality(String)`, `Float64`; canonical copy in `ARCHITECTURE.md` §6.2 and `mlregistry/schema.go` must match.
- Views: `CREATE OR REPLACE VIEW ... DEFINER = default SQL SECURITY DEFINER`; comments via `util.CreateView`.
- `kimi_reader` gets `SELECT` on views only; raw `ml_runs`/`ml_predictions` stay closed.
- Secrets only via env; never in `config_json`, `notes`, or errors. `ch_config`/`mask` already enforce this.
- `ML_LOG` defaults off; both scripts must behave byte-identically when it is unset.
- One branch/PR; spec and plan are the first commits; final `make all`, `make mcp-check`, `make docs-check`, `make docs-build` green.

## Review Focus

- **ClickHouse unavailable:** a run must still finish, write local artifacts, and leave a `build/ml-runs/<run_id>/run.json`; no partial ClickHouse rows and no non-zero exit from logging.
- **`ML_LOG` unset:** both scripts must not call `ch_exec` at all and must keep their existing output/artifacts unchanged.
- **NaN/Inf in sample or predictions:** the helper must reject non-finite predicted/actual values and sanitize sample JSON, otherwise `JSONEachRow` or `allow_nan=False` breaks the run.
- **Retry / duplicate rows:** the same `run_id` + fold + model + window + target date must collapse under `ReplacingMergeTree`; views use `FINAL`.
- **Mixed units:** `pp` (inflation) and `bp` (rate) must never be aggregated together; `v_ml_leaderboard` keeps `unit` and tests assert both values.
- **Empty sample/calendar:** both scripts must log `status='empty'` and still return their existing exit code.

---

### Task 1: Go `mlregistry` schema, views, and importer

**Files:**
- Create: `mlregistry/schema.go`
- Create: `mlregistry/views.go`
- Create: `mlregistry/import.go`
- Create: `mlregistry/import_test.go`
- Modify: `main.go` (blank import)
- Create: `dagu/ml.yaml`

**Interfaces:**
- Consumes: `chimport.Stats`, `util.CreateView`, `driver.Conn`.
- Produces: importer name `ml_registry`; tables `ml_runs`, `ml_predictions`; views `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`; DAG step `ml_registry`.

- [ ] **Step 1: Write the failing Go contract tests**

Create `mlregistry/import_test.go` with tests that pin the contract:

```go
package mlregistry

import (
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

func TestMlRegistryName(t *testing.T) {
	if got := (&mlRegistry{}).Name(); got != "ml_registry" {
		t.Fatalf("Name() = %q, want ml_registry", got)
	}
}

func TestMlRunsDdlPinsPassport(t *testing.T) {
	for _, want := range []string{
		"run_id UUID", "experiment LowCardinality(String)",
		"config_hash String", "env_json String", "sample_hash String", "data_asof Date",
		"metrics_json String", "artifacts_json String",
		"ENGINE = ReplacingMergeTree", "ORDER BY (experiment, run_id)",
	} {
		if !strings.Contains(mlRunsDdl, want) {
			t.Errorf("ml_runs DDL must contain %q", want)
		}
	}
}

func TestMlPredictionsDdlPinsFoldKey(t *testing.T) {
	for _, want := range []string{
		"run_id UUID", "model LowCardinality(String)",
		"window LowCardinality(String)", "target_date Date",
		"predicted Float64", "actual Float64", "error Float64",
		"unit LowCardinality(String)",
		"ORDER BY (experiment, run_id, model, window, target_date)",
	} {
		if !strings.Contains(mlPredictionsDdl, want) {
			t.Errorf("ml_predictions DDL must contain %q", want)
		}
	}
}

func TestMlViewsContract(t *testing.T) {
	names := make([]string, 0, len(mlViewsList))
	for _, v := range mlViewsList {
		names = append(names, v.Name)
	}
	want := []string{"v_ml_runs", "v_ml_leaderboard", "v_ml_error_by_fold"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("views = %v, want %v", names, want)
	}
	for _, v := range mlViewsList {
		if len(v.Tables) == 0 || v.Select == "" || v.Comment == "" {
			t.Errorf("%s: Tables/Select/Comment must be set", v.Name)
		}
	}
	if !strings.Contains(mlLeaderboardView.Select, "sqrt(avg(p.error * p.error))") {
		t.Error("v_ml_leaderboard must compute RMSE from fold errors")
	}
}

func TestMlRegistryRegistered(t *testing.T) {
	for _, stat := range chimport.Stats {
		if stat.Name() == "ml_registry" {
			return
		}
	}
	t.Fatal("ml_registry is not registered in chimport.Stats")
}
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `go test ./mlregistry/`
Expected: build failure — `mlRegistry`, `mlRunsDdl`, `mlPredictionsDdl`, `mlViewsList`, `mlLeaderboardView` are undefined.

- [ ] **Step 3: Implement `mlregistry/schema.go`**

Copy the canonical DDL from spec §3.1/§3.2 verbatim into constants:

```go
package mlregistry

const mlRunsDdl = `CREATE TABLE IF NOT EXISTS ml_runs (...) ENGINE = ReplacingMergeTree ORDER BY (experiment, run_id);`
const mlPredictionsDdl = `CREATE TABLE IF NOT EXISTS ml_predictions (...) ENGINE = ReplacingMergeTree ORDER BY (experiment, run_id, model, window, target_date);`
```

The columns and comments must match spec §3.1/§3.2 exactly.

- [ ] **Step 4: Implement `mlregistry/views.go`**

Define `mlViewsList []util.View` in this order: `mlRunsView`, `mlLeaderboardView`, `mlErrorByFoldView`. Use the SQL from spec §5.1–§5.3. `mlLeaderboardView` must contain `sqrt(avg(p.error * p.error)) AS rmse` and `avg(abs(p.error)) AS mae`. Add table/column comments in Russian; `util.CreateView` applies them.

- [ ] **Step 5: Implement `mlregistry/import.go`**

```go
type mlRegistry struct{}

func (s *mlRegistry) Name() string { return "ml_registry" }

func (s *mlRegistry) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range []string{mlRunsDdl, mlPredictionsDdl} {
		if err = conn.Exec(ctx, ddl); err != nil { return 0, err }
	}
	for _, v := range mlViewsList {
		created, err := util.CreateView(ctx, conn, v)
		if err != nil { return 0, err }
		log.Infof("View %s created: %t", v.Name, created)
	}
	return 0, nil
}

func init() { chimport.Stats = append(chimport.Stats, &mlRegistry{}) }
```

Use `log "github.com/sirupsen/logrus"`.

- [ ] **Step 6: Register the importer and DAG**

Add to `main.go` imports:

```go
_ "github.com/kmlebedev/clickhouse-import-rosstat/mlregistry"
```

Create `dagu/ml.yaml`:

```yaml
type: chain
schedule: "7 6 * * *"
overlap_policy: skip

steps:
  - id: ml_registry
    run: "$ROSSTAT_IMPORT_BIN"
    env:
      - CLICKHOUSE_IMPORT_STAT=ml_registry
    retry_policy:
      limit: 3
      interval_sec: 300
```

- [ ] **Step 7: Run the tests and build**

Run: `go test ./mlregistry/`
Expected: PASS.

Run: `go build ./...`
Expected: exit 0.

Run: `dagu validate dagu/ml.yaml`
Expected: exit 0.

- [ ] **Step 8: Commit**

```bash
git add mlregistry/ main.go dagu/ml.yaml
git commit -m "mlregistry: add experiment passport schema and views"
```

---

### Task 2: Python `ml_experiment` helper and `ch_exec`

**Files:**
- Create: `scripts/ml_experiment.py`
- Create: `scripts/ml_experiment_test.py`
- Modify: `scripts/extrapolation_aisc.py` (add `ch_exec` next to `ch_query`)

**Interfaces:**
- Consumes: `extrapolation_aisc.ch_config`, `ClickHouseError`, `mask`; `CLICKHOUSE_URL`/`CH_IMPORT_PASSWORD` env.
- Produces:
  - `PredictionRow(model: str, window: str, target_date: date, predicted: float, actual: float, unit: str, meta: dict)`
  - `ExperimentRun.log_metrics(metrics: dict)`, `.log_predictions(rows: list[PredictionRow])`, `.log_artifacts(paths: list[Path])`, `.finish(status="finished", notes="") -> str`
  - `start_run(experiment, run_name, config, data_asof, sample_meta, sample, spec_path="") -> ExperimentRun`
  - `canonical_json(value)`, `sha256_text(text)`, `json_safe(value)`, `json_each_row(rows)`, `insert_sql(table, rows)`
  - `ch_exec(sql: str) -> None`

- [ ] **Step 1: Write the failing helper tests**

Create `scripts/ml_experiment_test.py` with `unittest`. Cover at least:

```python
class CanonicalJsonTest(unittest.TestCase):
    def test_order_does_not_change_hash(self):
        a = {"b": 1, "a": [2, 3]}
        b = {"a": [2, 3], "b": 1}
        self.assertEqual(canonical_json(a), canonical_json(b))
        self.assertEqual(sha256_text(canonical_json(a)), sha256_text(canonical_json(b)))


class PredictionRowTest(unittest.TestCase):
    def test_error_is_predicted_minus_actual(self):
        row = PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp")
        self.assertAlmostEqual(row.error, 0.2)

    def test_nan_rejected(self):
        with self.assertRaises(ValueError):
            PredictionRow("m", "w", date(2026, 9, 30), float("nan"), 0.5, "pp")


class JsonEachRowTest(unittest.TestCase):
    def test_dates_and_datetimes(self):
        text = json_each_row([{
            "run_id": "00000000-0000-0000-0000-000000000000",
            "d": date(2026, 10, 18),
            "t": datetime(2026, 10, 18, 12, 0),
        }])
        self.assertIn('"2026-10-18"', text)
        self.assertIn('"2026-10-18 12:00:00"', text)


class FinishTest(unittest.TestCase):
    def test_disabled_writes_nothing(self):
        run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=False)
        self.assertEqual(run.finish("finished"), "disabled")

    def test_clickhouse_failure_falls_back_to_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"ML_LOCAL_DIR": tmp}):
                run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
                with patch("ml_experiment.ch_exec", side_effect=ClickHouseError("down")):
                    self.assertEqual(run.finish("finished"), "local")
                self.assertTrue((Path(tmp) / run.run_id / "run.json").is_file())
```

Add a test that `finish` calls `ch_exec` twice when predictions exist (mock and inspect SQL starts with `INSERT INTO ml_runs`/`ml_predictions`).

- [ ] **Step 2: Run the tests and verify they fail**

Run: `python3 scripts/ml_experiment_test.py`
Expected: `ModuleNotFoundError` / missing names.

- [ ] **Step 3: Add `ch_exec` to `scripts/extrapolation_aisc.py`**

Place it immediately after `ch_query`. Reuse `ch_config`, `mask`, `ClickHouseError`, `CH_QUERY_TIMEOUT_SECONDS`. POST the SQL body; ignore the response body; raise `ClickHouseError` on non-200/OSError with the same masked diagnostics as `ch_query`.

- [ ] **Step 4: Implement `scripts/ml_experiment.py`**

Key details:

- `ROOT = Path(__file__).resolve().parent.parent`; `DEFAULT_LOCAL_DIR = ROOT / "build" / "ml-runs"`.
- `canonical_json` uses `json.dumps(..., sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)`.
- `json_safe` converts `date`/`datetime` to `isoformat()`, `Path`/`UUID` to `str`, tuples to lists, and `NaN`/`±Inf` to `None` recursively.
- `capture_git` runs `git rev-parse HEAD`, `git branch --show-current`, `git status --porcelain`, `git config user.name` with `check=False`; `dirty=1` if status is non-empty.
- `package_versions` uses `importlib.metadata.version` for `catboost`, `scikit-learn`, `pandas`, `statsmodels`, `scipy`, `numpy`; missing → `None`.
- `PredictionRow.error` validates all values finite and returns `predicted - actual`.
- `ExperimentRun.finish` builds `run_row` with `json_safe` for config/env/sample_meta/metrics/artifacts (`env_json` = `package_versions()` plus Python/OS); writes `ml_predictions` first, then the final `ml_runs` row in one `ch_exec` each, so a failed predictions insert cannot leave a `finished` passport without fold rows; on `ClickHouseError`/`OSError` writes local `run.json` + `predictions.jsonl` and logs `log.warning`.
- `finish` is idempotent; `__exit__` calls `finish("failed", ...)` when an exception escapes and `finish` was not called.
- `enabled` defaults to `os.environ.get("ML_LOG") == "1"`; `run_name` overridden by `ML_RUN_NAME`; local dir by `ML_LOCAL_DIR`.
- `log_artifacts` stores `{"name", "path", "sha256", "bytes"}`; path relative to `ROOT` when possible.

- [ ] **Step 5: Run the tests and verify they pass**

Run: `python3 scripts/ml_experiment_test.py`
Expected: PASS (all tests).

- [ ] **Step 6: Commit**

```bash
git add scripts/ml_experiment.py scripts/ml_experiment_test.py scripts/extrapolation_aisc.py
git commit -m "ml: add experiment run helper and clickhouse write transport"
```

---

### Task 3: Integrate `cpi_nowcast`

**Files:**
- Modify: `scripts/cpi_nowcast.py` (`run_summary`, `main`)
- Modify: `scripts/cpi_nowcast_test.py`

**Interfaces:**
- Consumes: `ml_experiment.start_run`, `PredictionRow`, `json_safe`.
- Produces: `run_summary(..., ml_log: Optional[bool] = None, logger: Optional[object] = None, run_name: Optional[str] = None)`; logs `experiment="cpi_nowcast"`, `unit="pp"`.

- [ ] **Step 1: Write the failing integration tests**

Add a `FakeLogger` to `cpi_nowcast_test.py` with `log_metrics`, `log_predictions`, `log_artifacts`, `finish`. Add tests using the existing fixture/monkeypatch pattern:

- `test_run_summary_logs_cpi_run`: call `run_summary(..., ml_log=True, logger=fake)` on a fixture that yields predictions; assert:
  - `fake.finished == "finished"`;
  - `len(fake.predictions) == sum(len(run.predictions) for run in runs)`;
  - `{row.unit for row in fake.predictions} == {"pp"}`;
  - windows include `start=24` and `start=48, embargo=1`;
  - `fake.metrics["primary_metric"] == "rmse_pp"`.
- `test_run_summary_without_ml_log_does_not_call_ch_exec`: patch `ml_experiment.ch_exec`, call with `ml_log=False`, assert not called.
- `test_run_summary_empty_logs_empty_status`: use the existing empty-sample fixture, assert `fake.finished == "empty"` and no predictions.

- [ ] **Step 2: Run the tests and verify they fail**

Run: `python3 scripts/cpi_nowcast_test.py`
Expected: FAIL — `run_summary` does not accept `ml_log`/`logger` or no logger calls.

- [ ] **Step 3: Implement the integration**

In `run_summary`:

- Add `ml_log`, `logger`, `run_name` parameters (keep existing positional behavior).
- After `rows, skipped = build_sample(...)` and `assert_months_purged(rows)`, create a logger if `logger is None` and `ml_log`/`ML_LOG` is enabled:
  - `experiment="cpi_nowcast"`;
  - `run_name = os.environ.get("ML_RUN_NAME") or f"cpi_nowcast__start{start}__embargo{embargo_months}"`;
  - `config={"start": start, "embargo_months": embargo_months, "seed": SEED, "models": list(MODELS), "features": feature_names(), "weekly_window_weeks": WEEKLY_WINDOW_WEEKS, "target_series": TARGET_SERIES}`;
  - `sample=[{"month": row.month.isoformat(), "y": json_safe(row.y), "features": json_safe(row.features)} for row in rows]`;
  - `sample_meta={"rows": len(rows), "first": ..., "last": ..., "skipped": len(skipped), "months": len(months)}`;
  - `spec_path="docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md"`.
- In the `if not any(run.predictions for run in runs)` branch, call `logger.finish("empty", "walk-forward не дал ни одного прогноза")` before returning.
- After `write_artifacts` / `write_nowcast_artifact`, call:
  - `logger.log_metrics(...)` with `primary_metric`, `windows`, `weights`, `sample`;
  - `logger.log_predictions([...])` for every `Prediction` in every `WindowRun`; `window=window_key(run.start, run.embargo)`, `target_date=prediction.month`, `predicted=prediction.y_pred`, `actual=prediction.y_true`, `unit="pp"`, `meta={"start": run.start, "embargo": run.embargo, "main": run.main}`;
  - `logger.log_artifacts([report_path, predictions_path, forecast_path if forecast_written else None])`;
  - `logger.finish("finished", notes)`.
- Wrap the post-sample body in `try/except Exception as exc:` to call `logger.finish("failed", f"{type(exc).__name__}: {exc}")` and re-raise. If this is too invasive, use `with logger:` only when the logger is an `ExperimentRun`; fake loggers in tests implement `__enter__/__exit__` as no-ops. Keep the existing return codes.
- Update the module docstring: "по умолчанию не пишет в ClickHouse; с `ML_LOG=1` пишет паспорт и fold-предсказания в `ml_runs`/`ml_predictions`".

- [ ] **Step 4: Run the tests and verify they pass**

Run: `python3 scripts/cpi_nowcast_test.py`
Expected: PASS (all existing + new tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/cpi_nowcast.py scripts/cpi_nowcast_test.py
git commit -m "cpi-nowcast: log experiment passport and fold predictions"
```

---

### Task 4: Integrate `key_rate_model`

**Files:**
- Modify: `scripts/key_rate_model.py` (`run_summary`, `main`)
- Modify: `scripts/key_rate_model_test.py`

**Interfaces:**
- Consumes: same helper as Task 3.
- Produces: `run_summary(..., ml_log: Optional[bool] = None, logger: Optional[object] = None, run_name: Optional[str] = None)`; logs `experiment="key_rate_model"`, `unit="bp"`.

- [ ] **Step 1: Write the failing integration tests**

Add a `FakeLogger` and tests:

- `test_run_summary_logs_key_rate_run`: with an existing fixture, `ml_log=True, logger=fake`; assert:
  - `fake.finished == "finished"`;
  - `{row.unit for row in fake.predictions} == {"bp"}`;
  - windows are `{"expanding", "rolling40"}`;
  - `fake.metrics["primary_metric"] == "rmse_bp"`;
  - at least one `row.meta["probs"]` is a list of three floats.
- `test_run_summary_without_ml_log_does_not_call_ch_exec`: patch `ml_experiment.ch_exec`, assert not called.
- `test_run_summary_empty_logs_empty_status`: use the existing empty-calendar fixture; assert `fake.finished == "empty"`.

- [ ] **Step 2: Run the tests and verify they fail**

Run: `python3 scripts/key_rate_model_test.py`
Expected: FAIL — `run_summary` does not accept the new parameters or no logger calls.

- [ ] **Step 3: Implement the integration**

In `run_summary`:

- Add `ml_log`, `logger`, `run_name` parameters.
- Before the empty-calendar early return, create a logger with `experiment="key_rate_model"`, `data_asof=today`, `sample=[]`, `sample_meta={"rows": 0}`, `config={...}`; call `logger.finish("empty", "канонических заседаний нет")`.
- After `rows` is built, create the logger if not already created:
  - `run_name = os.environ.get("ML_RUN_NAME") or f"key_rate_model__{window_label(ROLLING_WINDOW if rolling else None)}"`;
  - `config={"rolling": rolling, "min_train": MIN_TRAIN, "rolling_window": ROLLING_WINDOW, "methods": [m for m, _ in METHODS], "forecast_methods": [m for m, _ in FORECAST_METHODS]}`;
  - `sample=[{"meeting_date": row.decision.meeting_date.isoformat(), "rate_before": row.decision.rate_before, "pi_saar": row.pi_saar, "gap": row.gap} for row in rows]`;
  - `sample_meta={"rows": len(rows), "first": ..., "last": ..., "excluded": len(excluded), "missing_rate": len(missing_rate), "missing_features": len(missing_features), "fold_skips": ...}`;
  - `spec_path="docs/superpowers/specs/2026-10-15-key-rate-model-v0-design.md"`.
- Early empty-sample return: `logger.finish("empty", "выборка пуста: ...")`.
- After artifacts:
  - `metrics` grouped by `(method, window)` using `_predictions_by_group`, `_group_errors`, `_group_steps`, `rmse_bp`, `mae_bp`, `hit_rate`; include `fold_skips`.
  - `predictions`: `PredictionRow(model=prediction.method, window=prediction.window, target_date=prediction.meeting_date, predicted=prediction.step_bp, actual=prediction.actual_step_bp, unit="bp", meta={"probs": list(prediction.probs) if prediction.probs is not None else None, "actual_y3": prediction.actual_y3})`.
  - `log_artifacts([report_path, predictions_path, forecast_path if forecast_written else None])`.
  - `finish("finished", notes)`.
- Wrap the post-setup body in `try/except` to call `finish("failed", ...)` and re-raise.
- Update the docstring: "по умолчанию в ClickHouse не пишет; с `ML_LOG=1` пишет паспорт и fold-предсказания".

- [ ] **Step 4: Run the tests and verify they pass**

Run: `python3 scripts/key_rate_model_test.py`
Expected: PASS (all existing + new tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/key_rate_model.py scripts/key_rate_model_test.py
git commit -m "key-rate-model: log experiment passport and fold predictions"
```

---

### Task 5: MCP access, docs, and live acceptance

**Files:**
- Modify: `sql/mcp_kimi_reader.sql`
- Modify: `scripts/mcp_check.py`
- Modify: `ARCHITECTURE.md` (§2 package list, §6.1 importer row, §6.2 DDL, §6.3 views)
- Modify: `docs/IMPORTERS.md`, `docs/OPERATIONS.md`, `docs/CONFIGURATION.md`, `README.md`
- Modify: `docs/ROADMAP_INFRA.md` (status `4.1`)
- Test: `make all`, `make mcp-check`, `make docs-check`, `make docs-build`

**Interfaces:**
- Consumes: tables/views from Task 1, logging from Tasks 3–4.
- Produces: agent-visible views and a complete acceptance protocol.

- [ ] **Step 1: Add MCP grants**

Append to `sql/mcp_kimi_reader.sql`:

```sql
GRANT SELECT ON default.v_ml_runs TO kimi_reader;
GRANT SELECT ON default.v_ml_leaderboard TO kimi_reader;
GRANT SELECT ON default.v_ml_error_by_fold TO kimi_reader;
```

Do not grant `ml_runs`/`ml_predictions`.

- [ ] **Step 2: Extend `scripts/mcp_check.py`**

Add checks that:
- `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold` are visible via `list_tables`/`run_query`;
- `SELECT count() FROM ml_runs` and `SELECT count() FROM ml_predictions` fail with `ACCESS_DENIED`;
- `v_ml_leaderboard` returns at least one row after the live runs (or a clear informational skip when the registry is empty).

Follow the existing check style and output (`OK`/`FAIL`, final `ПРОВЕРКА ПРОЙДЕНА`).

- [ ] **Step 3: Update `ARCHITECTURE.md`**

- §2 packages: add `mlregistry/` — schema-only importer for `ml_runs`/`ml_predictions` and experiment views.
- §6.1 table: add `ml_registry` row (source: none; writes: DDL/views; schedule: `dagu/ml.yaml` daily 06:07).
- §6.2: add canonical `ml_runs`/`ml_predictions` DDL exactly as in `mlregistry/schema.go` and spec §3.
- §6.3: add the three views and their comments; note `unit` and no raw-table grants.
- Update the "Последнее обновление" line with the item and acceptance result.

- [ ] **Step 4: Update importer/operation/config docs**

- `docs/IMPORTERS.md`: add `ml_registry` row and a short section; note it is schema-only and runs before experiments.
- `docs/OPERATIONS.md`: add `make import STAT=ml_registry` and `ML_LOG=1 make cpi-nowcast` / `ML_LOG=1 make key-rate-model`; mention local fallback `build/ml-runs/`.
- `docs/CONFIGURATION.md`: document `ML_LOG`, `ML_RUN_NAME`, `ML_LOCAL_DIR`.
- `README.md`: update the importer/package list only if it already enumerates them; do not expand the landing page.

- [ ] **Step 5: Run static checks**

Run: `make all`
Expected: gofmt/lint/vet/test/build green.

Run: `go test ./mlregistry/`
Expected: PASS.

Run: `python3 scripts/ml_experiment_test.py && python3 scripts/cpi_nowcast_test.py && python3 scripts/key_rate_model_test.py`
Expected: all PASS.

Run: `make docs-check && make docs-build`
Expected: `checked N pages`, MkDocs build succeeds.

- [ ] **Step 6: Live acceptance (dev ClickHouse)**

Run:
```bash
make import STAT=ml_registry
ML_LOG=1 make cpi-nowcast
ML_LOG=1 make key-rate-model
```
Expected:
- `ml_registry` creates/ensures tables and 3 views; repeat is a no-op.
- `cpi-nowcast` writes one `ml_runs` row and `2 952`-style `ml_predictions` rows (actual count from the run); `unit='pp'`.
- `key-rate-model` writes one `ml_runs` row and fold rows; `unit='bp'`.
- `v_ml_runs`/`v_ml_leaderboard`/`v_ml_error_by_fold` show the runs; raw tables deny `kimi_reader`.

Run `ML_LOG=1 make cpi-nowcast` a second time and compare:
```sql
SELECT run_id, config_hash, sample_hash, finished_at FROM ml_runs FINAL ORDER BY finished_at DESC LIMIT 2;
SELECT model, window, count(), round(sqrt(avg(error * error)), 6) AS rmse
FROM ml_predictions FINAL
WHERE run_id = '<second-run-id>'
GROUP BY model, window ORDER BY model, window;
```
Expected: two different `run_id`, same `config_hash`/`sample_hash`; same fold counts and RMSE as the first run; artifact sha256 equal.

Run: `make mcp-check`
Expected: `ПРОВЕРКА ПРОЙДЕНА`.

- [ ] **Step 7: Update roadmap status and write the acceptance protocol**

In `docs/ROADMAP_INFRA.md`, set `4.1` to `✅ сделан этим PR` with the commands/results from Step 6 (numbers, run IDs, view counts, no-op repeat). Add the plan link back to the row. Keep the status as `In Review` semantics until merge.

- [ ] **Step 8: Commit**

```bash
git add sql/mcp_kimi_reader.sql scripts/mcp_check.py ARCHITECTURE.md docs/IMPORTERS.md docs/OPERATIONS.md docs/CONFIGURATION.md README.md docs/ROADMAP_INFRA.md
git commit -m "docs: expose ml experiment views and accept registry"
```

---

## Self-Review Notes

- Spec coverage: schema (§3) → Task 1; Go importer/views (§4–5) → Task 1; helper/transport (§6) → Task 2; cpi integration (§7.1) → Task 3; key-rate integration (§7.2) → Task 4; MCP/docs/acceptance (§9–10) → Task 5.
- Type consistency: `PredictionRow` fields are used unchanged in Tasks 3–4; table names/columns match between Task 1 and Task 2; `unit` is always `pp`/`bp`.
- Review Focus tests are placed: ClickHouse fallback (Task 2), `ML_LOG` off (Tasks 2–4), NaN/Inf (Task 2), duplicate key (Task 1 DDL + Task 3/4 run IDs), mixed units (Tasks 3–4 + Task 5 leaderboard), empty sample (Tasks 3–4).
- Deliberate omissions: MLflow, model registry, promotion, live forecast, Grafana dashboard, `cpi_monthly.py` — all named out of scope in the spec.
