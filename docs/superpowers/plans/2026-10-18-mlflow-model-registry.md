# MLflow and Model Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a self-hosted MLflow control plane (SQLite backend, local artifacts) with a model registry (`champion`/`challenger` aliases) and an idempotent export of MLflow runs into ClickHouse `ml_runs`/`ml_predictions`, without adding PostgreSQL, MySQL, MinIO or S3 to dev/staging.

**Architecture:** MLflow runs on `127.0.0.1:5000` with `--backend-store-uri sqlite:///…`, `--default-artifact-root build/mlflow/artifacts`, `--serve-artifacts`, `--workers 1`. `scripts/ml_experiment.py` keeps its ClickHouse/local behavior and optionally logs the same run into MLflow (lazy import, best-effort). Each run logs `run.json`, `predictions.jsonl` and, for `cpi_nowcast`, a CatBoost model. `scripts/mlflow_export.py` replays MLflow runs into ClickHouse idempotently under a canonical UUID derived from the MLflow run id. `scripts/mlflow_registry.py` registers model versions and moves the `challenger`/`champion` aliases.

**Tech Stack:** Python 3.12, MLflow 3.17.1 (opt-in via `uv run --with`), SQLite, MLflow REST/client, ClickHouse HTTP (`ch_exec`), Makefile, systemd (staging), `unittest`/`unittest.mock`.

**Spec:** `docs/superpowers/specs/2026-10-18-mlflow-model-registry-design.md`

## Global Constraints

- SQLite is the only backend for this item; PostgreSQL/MySQL/S3/MinIO are out of scope. MySQL is a future URI-only swap.
- MLflow server binds `127.0.0.1` only; access via SSH tunnel. No auth in this item.
- `ml_experiment.py` stays stdlib-only by default: `import mlflow` is lazy and only when `ML_LOG=1` and `MLFLOW_TRACKING_URI` is non-empty.
- A failing MLflow sink must never fail the experiment or change its exit code; ClickHouse/local fallback behavior from 4.1 is preserved.
- `run_id` in ClickHouse is a canonical UUID derived from the MLflow run id; old 4.1 local UUIDs are untouched.
- No schema change to `ml_runs`/`ml_predictions`; the canonical DDL from 4.1 stays.
- ClickHouse writes stay batched: one `INSERT … FORMAT JSONEachRow` per table via `ch_exec`.
- No secrets in `run.json`, `predictions.jsonl`, params, tags or errors.
- `MLFLOW=0` in Makefile passes an empty `MLFLOW_TRACKING_URI`, so an env-file value cannot accidentally enable MLflow.
- One branch/PR; spec and plan are the first commits; final `make all`, `make mcp-check`, `make docs-check`, `make docs-build` green.

## Review Focus

- **MLflow down / package missing:** the run must still finish, write ClickHouse or local `run.json`, and never turn into a non-zero exit from logging.
- **`ML_LOG` unset:** byte-identical behavior to 4.1; no `mlflow` import, no `ch_exec`.
- **`MLFLOW_TRACKING_URI` set but `ML_LOG=0`:** no MLflow calls.
- **Idempotent export:** exporting the same run twice produces the same rows under `ReplacingMergeTree`; views use `FINAL`.
- **Run id mapping:** 32-hex MLflow id → dashed UUID; deterministic fallback; `run.json` and prediction rows use the same id.
- **Model registry:** register sets `challenger`; promote sets `champion`; both aliases resolve to the intended version; promoting does not delete the other alias.
- **SQLite concurrency:** one worker; no long-running transaction held across artifact uploads.
- **No new services in dev:** `make mlflow-up` must not require ClickHouse, PostgreSQL, Docker or S3.

---

### Task 1: MLflow server lifecycle in Makefile (dev)

**Files:**
- Modify: `Makefile` (variables near `MCP_VERSION`/`MCP_PY`; targets; `.PHONY`)

**Interfaces:**
- Produces: `make mlflow-up`, `make mlflow-down`, `make mlflow-status`; variables `MLFLOW_VERSION`, `MLFLOW_HOME`, `MLFLOW_PORT`, `MLFLOW_TRACKING_URI`, `MLFLOW_DB`, `MLFLOW_ARTIFACTS`, `MLFLOW_PID`, `MLFLOW_LOG`, `MLFLOW`, `MLFLOW_PKG`, `MLFLOW_ENV`, `MLFLOW_HTTP_ENV`.

- [ ] **Step 1: Add variables**

```make
MLFLOW_VERSION      ?= 3.17.1
MLFLOW_HOME         ?= $(BUILD_DIR)/mlflow
MLFLOW_PORT         ?= 5000
MLFLOW_TRACKING_URI ?= http://127.0.0.1:$(MLFLOW_PORT)
MLFLOW_DB           := $(abspath $(MLFLOW_HOME)/mlflow.db)
MLFLOW_ARTIFACTS    := $(abspath $(MLFLOW_HOME)/artifacts)
MLFLOW_PID          := $(MLFLOW_HOME)/mlflow.pid
MLFLOW_LOG          := $(MLFLOW_HOME)/mlflow.log

# 1 — добавить mlflow в uv-окружение прогона и включить MLflow-приёмник.
MLFLOW ?= 0
MLFLOW_PKG    := $(if $(filter 1,$(MLFLOW)),--with "mlflow==$(MLFLOW_VERSION)",)
MLFLOW_ENV    := $(if $(filter 1,$(MLFLOW)),MLFLOW_TRACKING_URI=$(MLFLOW_TRACKING_URI),MLFLOW_TRACKING_URI=)
MLFLOW_HTTP_ENV := MLFLOW_HTTP_REQUEST_TIMEOUT=10 MLFLOW_HTTP_REQUEST_MAX_RETRIES=2
```

- [ ] **Step 2: Add `mlflow-up`**

Behavior:
- if `curl -sf http://127.0.0.1:$(MLFLOW_PORT)/health` succeeds → `MLflow уже отвечает…`;
- else `mkdir -p $(MLFLOW_HOME) $(MLFLOW_ARTIFACTS)`; `nohup uv run --with mlflow==$(MLFLOW_VERSION) --python 3.12 mlflow server --host 127.0.0.1 --port $(MLFLOW_PORT) --workers 1 --backend-store-uri sqlite:///$(MLFLOW_DB) --default-artifact-root $(MLFLOW_ARTIFACTS) --serve-artifacts >$(MLFLOW_LOG) 2>&1 &`; write `$!` to `$(MLFLOW_PID)`; loop up to 60 s on `/health`; on failure print `$(MLFLOW_LOG)` and exit 1.
- `sqlite:///` + absolute path means four slashes. Do not quote away the slashes.

- [ ] **Step 3: Add `mlflow-down` and `mlflow-status`**

- `mlflow-down`: if PID file exists and process alive → `kill`, wait up to 60 s, remove PID on success; otherwise print a message. Do not delete the DB or artifacts.
- `mlflow-status`: `curl -sf -m 2 http://127.0.0.1:$(MLFLOW_PORT)/health` → `MLflow отвечает на порту …` / `MLflow не отвечает …`.

- [ ] **Step 4: Add targets to `.PHONY` and verify**

Run: `make mlflow-up`
Expected: `MLflow запущен…`; `curl -sf http://127.0.0.1:5000/health` returns 200.

Run: `make mlflow-status`
Expected: `MLflow отвечает на порту 5000`.

Run: `make mlflow-down`
Expected: process stopped, PID file removed; `make mlflow-status` reports not responding.

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "add mlflow server lifecycle targets"
```

---

### Task 2: MLflow sink in `scripts/ml_experiment.py`

**Files:**
- Modify: `scripts/ml_experiment.py`
- Modify: `scripts/ml_experiment_test.py`
- Modify: `scripts/cpi_nowcast_test.py` (`_ML_ENV_VARS`)
- Modify: `scripts/key_rate_model_test.py` (`_ML_ENV_VARS`)

**Interfaces:**
- Consumes: env `ML_LOG`, `MLFLOW_TRACKING_URI`; `mlflow` (lazy).
- Produces:
  - `canonical_run_id(mlflow_run_id: str) -> str`
  - `flatten_metrics(metrics: dict) -> dict[str, float]`
  - `ExperimentRun.log_model(name: str, model) -> None`
  - MLflow artifacts `run.json`, `predictions.jsonl`, model under `name`.
- Existing public API (`start_run`, `PredictionRow`, `finish`, return values) is unchanged.

- [ ] **Step 1: Write failing tests with a fake `mlflow` module**

In `scripts/ml_experiment_test.py`, add a fake module installed with `unittest.mock.patch.dict(sys.modules, {"mlflow": fake})` and a `FakeMlflow` object that records:
- `set_tracking_uri(uri)`, `set_experiment(name)`, `start_run(run_name=…, tags=…)` returning an object with `info.run_id`;
- `log_text(text, artifact_file)`, `log_artifact(path)`, `log_metrics(dict)`, `log_params(dict)`, `log_tags(dict)`, `end_run(status=…)`;
- a fake `mlflow.catboost.log_model(model, name=…)`.

Cover:
- with `ML_LOG=1`, `MLFLOW_TRACKING_URI=http://mlflow:5000`, `enabled=True`: run is started, `run_id` is a UUID, `run.json`/`predictions.jsonl` logged, `end_run(FINISHED)`;
- `finish("failed", …)` → `end_run(FAILED)`;
- `MLFLOW_TRACKING_URI` empty → zero fake calls;
- `MLFLOW_TRACKING_URI` set but `ML_LOG` unset → zero fake calls;
- `start_run` raises → warning and the ClickHouse/local path still works;
- `log_text` raises → warning, `end_run(FAILED)`, `finish` still returns the ClickHouse result;
- `canonical_run_id("0123456789abcdef0123456789abcdef") == "01234567-89ab-cdef-0123-456789abcdef"`;
- `flatten_metrics` skips `None`/`NaN`, keeps finite numbers, sanitizes keys and does not emit `primary_metric` as a metric.

Add `MLFLOW_TRACKING_URI` to `_ML_ENV_VARS` in `ml_experiment_test.py`, `cpi_nowcast_test.py`, `key_rate_model_test.py`.

Run: `python3 scripts/ml_experiment_test.py -v`
Expected: FAIL (new tests) before implementation.

- [ ] **Step 2: Implement lazy MLflow start**

Add module-level helpers:

```python
def canonical_run_id(mlflow_run_id: str) -> str:
    try:
        return str(uuid.UUID(hex=mlflow_run_id))
    except (ValueError, AttributeError, TypeError):
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"mlflow:{mlflow_run_id}"))
```

`ExperimentRun.__init__` additions:

```python
self._mlflow = None
self._models = []
self._artifact_paths = []
...
self._start_mlflow()
```

`_start_mlflow`:
- read `uri = os.environ.get("MLFLOW_TRACKING_URI")`;
- return if `not self.enabled or not uri`;
- `import mlflow` in `try/except ImportError` → `log.warning(...)`, return;
- `mlflow.set_tracking_uri(uri)`, `mlflow.set_experiment(self.experiment)`;
- `run = mlflow.start_run(run_name=self.run_name, tags={...})`;
- `self.run_id = canonical_run_id(run.info.run_id)`;
- log tags (`config_hash`, `sample_hash`, `git_sha`, `git_branch`, `git_dirty`, `data_asof`, `spec_path`, `author`) and scalar params;
- `except Exception as exc`: `log.warning("mlflow: %s", exc)`, `self._mlflow = None`.

- [ ] **Step 3: Implement artifact/model/metric logging in `finish`**

Add `_log_mlflow(self, run_row, prediction_rows, status)`:
- `log_text(canonical_json(run_row), "run.json")`;
- `log_text(json_each_row(prediction_rows), "predictions.jsonl")` if any;
- `log_artifact(str(path))` for each `self._artifact_paths`;
- `mlflow.catboost.log_model(model, name=name)` for each `(name, model)`; catch per-model exceptions and warn;
- `flatten_metrics(self.metrics)` and `log_metrics`;
- `end_run(status="FINISHED" if status == "finished" else "FAILED")`.

In `finish`, call `_log_mlflow` after `run_row`/`prediction_rows` are built and before the ClickHouse insert. Catch `Exception` inside `_log_mlflow`; on exception try `end_run("FAILED")` and continue. Keep `self._result` logic unchanged.

`log_artifacts` appends the resolved `Path` to `self._artifact_paths` when the file exists; `log_model` appends to `self._models`.

- [ ] **Step 4: Update the docstring**

State that MLflow is an optional sink: `ML_LOG=1` enables the journal, `MLFLOW_TRACKING_URI` adds MLflow; a sink failure is a warning and never changes the run result.

- [ ] **Step 5: Run tests**

Run: `python3 scripts/ml_experiment_test.py -v`
Expected: PASS.

Run: `python3 scripts/cpi_nowcast_test.py -v` and `python3 scripts/key_rate_model_test.py -v` (targeted tests are acceptable while iterating; full suites before final).
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/ml_experiment.py scripts/ml_experiment_test.py scripts/cpi_nowcast_test.py scripts/key_rate_model_test.py
git commit -m "log experiment runs to mlflow"
```

---

### Task 3: Log the CatBoost model from `cpi_nowcast`

**Files:**
- Modify: `scripts/cpi_nowcast.py`
- Modify: `scripts/cpi_nowcast_test.py`

**Interfaces:**
- Consumes: `ExperimentRun.log_model(name, model)` (Task 2); `fit_catboost_direct.last_fit`.
- Produces: MLflow artifact `model/` for the full-sample CatBoost direct model.

- [ ] **Step 1: Write a failing test**

In `cpi_nowcast_test.py`, use a fake logger that records `log_model` calls (or extend the existing fake logger). Run the smallest `run_summary` path that produces a `Nowcast`; assert `log_model` was called once with `("model", <model>)` when `nowcast is not None`. If constructing a real CatBoost model is too slow, inject a stub `fit_catboost_direct.last_fit` with a sentinel model object and assert it is forwarded; restore `last_fit` in `finally`.

Run: `python3 scripts/cpi_nowcast_test.py -v`
Expected: FAIL before implementation.

- [ ] **Step 2: Implement**

After `current_nowcast(...)` and before the report/artifact block:

```python
if logger is not None and getattr(logger, "log_model", None) and nowcast is not None:
    fit = fit_catboost_direct.last_fit
    if fit is not None:
        try:
            logger.log_model("model", fit.model)
        except Exception as exc:
            log.warning("модель не залогирована: %s: %s", type(exc).__name__, exc)
```

- [ ] **Step 3: Run tests**

Run: `python3 scripts/cpi_nowcast_test.py -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add scripts/cpi_nowcast.py scripts/cpi_nowcast_test.py
git commit -m "log catboost model from cpi nowcast"
```

---

### Task 4: `scripts/mlflow_export.py`

**Files:**
- Create: `scripts/mlflow_export.py`
- Create: `scripts/mlflow_export_test.py`

**Interfaces:**
- Consumes: `mlflow` client (lazy), `ml_experiment.canonical_json`, `json_each_row`, `insert_sql`, `ch_exec`.
- Produces:
  - `canonical_run_id(mlflow_run_id) -> str`
  - `ch_datetime(value) -> str`
  - `parse_jsonl(text) -> list[dict]`
  - `build_run_row(run_json: dict, mlflow_run_id: str) -> dict`
  - `build_prediction_rows(predictions: list[dict], mlflow_run_id: str) -> list[dict]`
  - CLI `main(argv=None) -> int`.

- [ ] **Step 1: Write failing tests**

`scripts/mlflow_export_test.py` (stdlib only, no MLflow):
- `canonical_run_id` formats 32 hex and falls back deterministically for non-hex;
- `parse_jsonl` skips empty lines, raises on malformed JSON with a clear message;
- `build_run_row` overwrites `run_id`, normalizes `finished_at`/`started_at` `T` → space and strips timezone, preserves other fields;
- `build_prediction_rows` overwrites `run_id` in every row and normalizes `created_at`;
- repeated `build_*` calls produce identical dicts (idempotent row shape);
- `main(["--dry-run", …])` with a fake client does not call `ch_exec`; missing `run.json` → skip + exit 0 or 1 as specified (choose 0 for "no runs to export", 1 for malformed/CH error);
- SQL built for a fixed row contains `INSERT INTO ml_runs FORMAT JSONEachRow` and the same text on repeat.

Run: `python3 scripts/mlflow_export_test.py -v`
Expected: FAIL before implementation.

- [ ] **Step 2: Implement pure helpers**

- `canonical_run_id`: same as Task 2 (import from `ml_experiment` or duplicate? Prefer `from ml_experiment import canonical_run_id` to keep one implementation; `ml_experiment` imports `ch_exec` and stdlib only, safe).
- `ch_datetime`: accept `str`; replace `T` with space; strip `Z`/`+00:00`/`+03:00` suffix; return the first 19 chars; pass through `None`/empty.
- `build_run_row`: copy `run.json`; set `run_id`; normalize timestamps; ensure required keys exist.
- `build_prediction_rows`: same for each prediction row.

- [ ] **Step 3: Implement CLI**

```
--tracking-uri (default env MLFLOW_TRACKING_URI or http://127.0.0.1:5000)
--experiment (repeatable; default cpi_nowcast, key_rate_model)
--since YYYY-MM-DD
--limit N (default 100 per experiment)
--dry-run
```

- `import mlflow` inside `main`; `from mlflow import MlflowClient`.
- `client.get_experiment_by_name`; skip unknown with a message.
- `client.search_runs([exp_id], order_by=["attributes.start_time ASC"], max_results=limit)`.
- client-side filters: status in `FINISHED`/`FAILED`; `--since` compares `run.info.start_time` (ms) to the date; `RUNNING` → warning + skip.
- `client.download_artifacts(run_id, "run.json", tmpdir)`; `MlflowException` → warning + skip. `predictions.jsonl` → optional.
- For each run: build rows, `ch_exec(insert_sql("ml_predictions", preds))` if preds, then `ch_exec(insert_sql("ml_runs", [row]))`.
- `--dry-run`: print `experiment run_id folds` and do not call `ch_exec`.
- Summary: `exported N runs, M predictions; skipped K`.
- Exit 1 if any run failed to export; 0 otherwise.

- [ ] **Step 4: Run tests**

Run: `python3 scripts/mlflow_export_test.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/mlflow_export.py scripts/mlflow_export_test.py
git commit -m "export mlflow runs to clickhouse"
```

---

### Task 5: `scripts/mlflow_registry.py`

**Files:**
- Create: `scripts/mlflow_registry.py`
- Create: `scripts/mlflow_registry_test.py`

**Interfaces:**
- Consumes: `mlflow` client (lazy), `MlflowClient`.
- Produces:
  - `register_version(client, name, run_id) -> version`
  - `promote_version(client, name, version, alias="champion") -> None`
  - `list_models(client, name=None) -> list[dict]`
  - CLI `register|promote|list`.

- [ ] **Step 1: Write failing tests**

`scripts/mlflow_registry_test.py` with a fake client recording calls:
- `register_version` creates the registered model if absent, creates a model version with `source=f"runs:/{run_id}/model"`, sets `challenger` to the returned version;
- `promote_version` calls `set_registered_model_alias(name, "champion", version)` and does not remove `challenger`;
- `list_models` returns name/version/status/aliases/run_id; aliases from `version.aliases`, fallback to `get_model_version_by_alias`;
- CLI parses `register`, `promote`, `list` and uses `MLFLOW_TRACKING_URI` default.

Run: `python3 scripts/mlflow_registry_test.py -v`
Expected: FAIL before implementation.

- [ ] **Step 2: Implement**

- `register_version`: `client.get_registered_model(name)` in try/except; if missing, `client.create_registered_model(name)`; `version = client.create_model_version(name, source=f"runs:/{run_id}/model", run_id=run_id)`; `client.set_registered_model_alias(name, "challenger", version.version)`; return `version`.
- `promote_version`: `client.set_registered_model_alias(name, alias, version)`.
- `list_models`: `client.search_registered_models()` or `client.get_registered_model(name)`; `client.search_model_versions(f"name='{name}'")`; aliases from `version.aliases` if present, else `get_model_version_by_alias` for `champion`/`challenger`.
- CLI: `--tracking-uri`, `--name`, `--version`, `--run-id`, `--alias` (default `champion`); print concise lines; exit 1 on `MlflowException`.

- [ ] **Step 3: Run tests**

Run: `python3 scripts/mlflow_registry_test.py -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add scripts/mlflow_registry.py scripts/mlflow_registry_test.py
git commit -m "add mlflow model registry aliases"
```

---

### Task 6: Makefile targets for export, registry and MLflow runs

**Files:**
- Modify: `Makefile`

**Interfaces:**
- Produces: `make mlflow-export`, `make mlflow-register`, `make mlflow-promote`, `make mlflow-models`; `MLFLOW=1` support in `cpi-nowcast`/`key-rate-model`.

- [ ] **Step 1: Add targets**

```make
mlflow-export:
	MLFLOW_TRACKING_URI=$(MLFLOW_TRACKING_URI) uv run --with mlflow==$(MLFLOW_VERSION) --python 3.12 \
		python3 scripts/mlflow_export.py --tracking-uri $(MLFLOW_TRACKING_URI) $(ARGS)

mlflow-register:
	uv run --with mlflow==$(MLFLOW_VERSION) --python 3.12 \
		python3 scripts/mlflow_registry.py --tracking-uri $(MLFLOW_TRACKING_URI) register $(ARGS)

mlflow-promote:
	uv run --with mlflow==$(MLFLOW_VERSION) --python 3.12 \
		python3 scripts/mlflow_registry.py --tracking-uri $(MLFLOW_TRACKING_URI) promote $(ARGS)

mlflow-models:
	uv run --with mlflow==$(MLFLOW_VERSION) --python 3.12 \
		python3 scripts/mlflow_registry.py --tracking-uri $(MLFLOW_TRACKING_URI) list $(ARGS)
```

- [ ] **Step 2: Wire `MLFLOW=1` into the two run targets**

Prefix `cpi-nowcast` and `key-rate-model` recipes with `$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV)` and insert `$(MLFLOW_PKG)` into the `uv run` argument list. Add new targets to `.PHONY`.

- [ ] **Step 3: Verify**

Run: `make -n mlflow-export`
Expected: prints the command, no execution.

Run: `ML_LOG=1 MLFLOW=1 make -n cpi-nowcast`
Expected: command contains `mlflow==3.17.1` and `MLFLOW_TRACKING_URI=http://127.0.0.1:5000`.

Run: `ML_LOG=1 make -n cpi-nowcast`
Expected: command does not contain `mlflow==` and sets `MLFLOW_TRACKING_URI=`.

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "add mlflow export and registry targets"
```

---

### Task 7: Staging MLflow service

**Files:**
- Create: `deploy/staging/mlflow.service`
- Modify: `Makefile` (`deploy-staging-mlflow`, `deploy-staging`, `deploy-staging-smoke`, `upgrade-staging`, `.PHONY`)
- Modify: `deploy/staging/README.md`
- Modify: `deploy/staging/clickhouse-import-rosstat.env.example` (comment only, no secrets)

**Interfaces:**
- Produces: systemd unit `mlflow`, `make deploy-staging-mlflow`, staging `/health` smoke.

- [ ] **Step 1: Write the unit**

Use the unit from spec §6.7. Add a comment: `# MLFLOW_VERSION must match Makefile MLFLOW_VERSION`.

- [ ] **Step 2: Add deploy target**

`deploy-staging-mlflow: staging-precheck` → `scp deploy/staging/mlflow.service $(STAGING_HOST):/etc/systemd/system/mlflow.service` → `ssh … systemctl daemon-reload` → `ssh … install -d -m 755 /var/lib/mlflow /var/lib/mlflow/artifacts` → warm `uv` cache with `/root/.local/bin/uv run --with mlflow==$(MLFLOW_VERSION) --python 3.12 python -c "pass"` (ignore failure like the MCP warm-up).

- [ ] **Step 3: Wire into deploy/smoke/upgrade**

- `deploy-staging`: add `deploy-staging-mlflow` to prerequisites; `systemctl enable --now dagu mcp-clickhouse mlflow`.
- `deploy-staging-smoke`: `systemctl is-active --quiet mlflow && curl -sf -m 5 http://127.0.0.1:5000/health`.
- `upgrade-staging`: add `deploy-staging-mlflow` and restart `mlflow`.
- `.PHONY`: add `deploy-staging-mlflow`.

- [ ] **Step 4: Update staging README**

Add `/var/lib/mlflow` to the path table, a MLflow section with:
- `ssh -N -L 5000:127.0.0.1:5000 palmshell`;
- UI `http://localhost:5000`;
- `systemctl status mlflow`, `journalctl -u mlflow`;
- backup: `sqlite3 /var/lib/mlflow/mlflow.db ".backup '/root/mlflow-$(date +%F).db'"` + `tar czf /root/mlflow-artifacts-$(date +%F).tgz -C /var/lib/mlflow artifacts`;
- upgrade: backup → change `MLFLOW_VERSION` in unit and Makefile → `make deploy-staging-mlflow` → `mlflow db upgrade sqlite:////var/lib/mlflow/mlflow.db` if needed;
- no auth, localhost only.

- [ ] **Step 5: Verify locally**

Run: `systemd-analyze verify deploy/staging/mlflow.service` if available; otherwise `make -n deploy-staging-mlflow` and inspect the command.

Run: `make -n deploy-staging`
Expected: includes `deploy-staging-mlflow`.

- [ ] **Step 6: Commit**

```bash
git add deploy/staging/mlflow.service deploy/staging/README.md deploy/staging/clickhouse-import-rosstat.env.example Makefile
git commit -m "deploy mlflow to staging with local store"
```

---

### Task 8: Docs, architecture and roadmap

**Files:**
- Modify: `ARCHITECTURE.md`
- Modify: `README.md`
- Modify: `docs/OPERATIONS.md`
- Modify: `docs/CONFIGURATION.md`
- Modify: `docs/ROADMAP_INFRA.md`

**Interfaces:**
- Documents the new control plane, env vars, commands, storage paths and the backend decision.

- [ ] **Step 1: ARCHITECTURE.md**

- Add an ML-infrastructure subsection: MLflow control plane, SQLite + local artifacts, `--serve-artifacts`, flow `run → MLflow → mlflow_export → ClickHouse`.
- In §6.2/§6.3 note that `ml_runs.run_id` for MLflow-backed runs is the canonical UUID of the MLflow run; the DDL itself is unchanged.
- Update the top "Последнее обновление" line.
- Run the `sync-readme-architecture` skill checklist.

- [ ] **Step 2: README.md**

Add a short "MLflow" section: `make mlflow-up`, UI URL, `ML_LOG=1 MLFLOW=1 make cpi-nowcast`, `make mlflow-export`, `make mlflow-register/promote`, `make mlflow-down`; note SQLite/local artifacts and SSH tunnel for staging.

- [ ] **Step 3: OPERATIONS.md**

Add commands and expected outputs; note the MLflow-down fallback behavior and that export is a separate step; include idempotency check SQL:

```sql
SELECT count() FROM ml_runs FINAL;
SELECT count() FROM ml_predictions FINAL;
```

- [ ] **Step 4: CONFIGURATION.md**

Add rows for `MLFLOW`, `MLFLOW_TRACKING_URI`, `MLFLOW_HOME`, `MLFLOW_PORT`, `MLFLOW_VERSION` (make-level) and clarify that `ML_LOG=1` remains the master switch.

- [ ] **Step 5: ROADMAP_INFRA.md**

Replace the 4.2 wording "Postgres + MinIO/S3 на staging" with "SQLite + локальные артефакты (dev/staging); MySQL — опциональный апгрейд; S3 — отдельный пункт/прод". After live acceptance, set `✅ сделан этим PR` with the protocol (commands + results) and keep the acceptance criterion unchanged.

- [ ] **Step 6: Verify**

Run: `make docs-check`
Expected: exit 0.

Run: `make docs-build`
Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
git add ARCHITECTURE.md README.md docs/OPERATIONS.md docs/CONFIGURATION.md docs/ROADMAP_INFRA.md
git commit -m "document mlflow control plane"
```

---

### Task 9: Full verification and live acceptance

**Files:** none (verification only; fix bugs in place).

- [ ] **Step 1: Python suites**

```bash
python3 scripts/ml_experiment_test.py -v
python3 scripts/mlflow_export_test.py -v
python3 scripts/mlflow_registry_test.py -v
python3 scripts/cpi_nowcast_test.py -v
python3 scripts/key_rate_model_test.py -v
```

Expected: all PASS.

- [ ] **Step 2: Go and docs**

```bash
make all
make docs-check
make docs-build
```

Expected: all exit 0.

- [ ] **Step 3: Live dev acceptance**

Ask the owner to ensure ClickHouse is up (`make ch-up`); the assistant does not run it.

```bash
make mlflow-up
make mlflow-status
ML_LOG=1 MLFLOW=1 make cpi-nowcast
make mlflow-export
make mlflow-export          # second run: same FINAL counts
make mlflow-register RUN_ID=<id> NAME=cpi_nowcast_catboost_direct
make mlflow-promote NAME=cpi_nowcast_catboost_direct VERSION=1
make mlflow-models
ML_LOG=1 MLFLOW=1 make key-rate-model
make mlflow-export
make mcp-check
make mlflow-down
```

Record: MLflow UI/API run, artifact names, ClickHouse counts via `FINAL`, alias targets, repeat-export counts, `make mcp-check` result.

- [ ] **Step 4: Staging acceptance (if host reachable)**

```bash
make deploy-staging-mlflow
ssh -N -L 5000:127.0.0.1:5000 palmshell &
curl -sf http://localhost:5000/health
make upgrade-staging
```

Record: `systemctl is-active mlflow`, `/health` 200, smoke output. If the host is not reachable, report it as an unverified part and let the owner run it.

- [ ] **Step 5: PR and protocol**

- Push the branch, open one PR with `Closes #183`.
- PR body: roadmap item `infra/4.2`, backend decision, commands, schema note (no DDL change).
- Add the live acceptance protocol as a PR comment (commands + results).
- Update issue #183 and Project V2 (`Status=In Review`; `Done` only after merge).
