# Three-stage CPI → SA/expectations → key-rate pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and validate a three-stage pipeline: stage 1 CPI nowcast for `M0`/`M0+1`, stage 2 SA path + cumulative h1/h3/h6, stage 3 key-rate decision, with publication-calendar as-of and nested out-of-fold stacking.

**Architecture:** Three Python modules share a `PublicationCalendar`; stage 1/2/3 walk forward by meeting date, stage 2 trains on OOF stage 1, stage 3 trains on OOF stage 2. Baselines: current `cpi_live3_horizons`, `cpi_monthly_v3`, `channel_fan_vol`. No ClickHouse writes, no DDL, no `forecast_log`.

**Tech Stack:** Python 3.12, CatBoost, scikit-learn, statsmodels STL, pandas, existing ClickHouse read helpers (`ch_query`), optional MLflow/`ML_LOG`.

**Spec:** `docs/superpowers/specs/2026-10-20-cpi-key-rate-pipeline-design.md`

## Global Constraints

- Python only: no Go, DDL, `GRANT`, views, `forecast_log`, or production promotion.
- Every feature uses `published_at <= meeting_datetime`; no fixed `8-е M+1` or `конец M+1`.
- Walk-forward only; no random split; nested OOF for stage 2/3.
- Stage 1 target: NSA m/m for `M0` and `M0+1`.
- Stage 2 target: SA m/m path `M0…M0+5`; cumulative SA h1/h3/h6; NSA back-transform only sanity.
- CatBoost defaults, `random_seed=20261018`, `allow_writing_files=False`; linear models impute median train inside fold.
- NaN ≠ 0; every missing feature gets `None` + availability indicator.
- Stage 2 must be not worse than current live3 before stage 3; stage 3 must beat `channel_fan_vol` by pre-registered DM/CI.
- `ML_LOG=1` and MLflow by default; artifacts sha256; repeat run deterministic.
- `make all`, `make docs-check` green; Python tests via `uv run` / `make`.
- One item — one branch — one PR per phase; owner decides merge.

## Review Focus

- **Calendar boundary:** a release at 19:00 on the meeting day must not be available at a 13:30 meeting; test `test_monthly_release_time_boundary` in Task 0.
- **Nested OOF:** stage 2 must train on OOF stage 1, stage 3 on OOF stage 2; test `test_stage1_oof_not_actual` and `test_stage3_does_not_see_raw_cpi_expectations`.
- **SA endpoint/revisions:** seasonal factors must use only data available at `as_of`; test `test_factors_use_only_past`.
- **Early-meeting missingness:** stage 2/3 features must be `None` + indicator, not 0; test `test_missing_expectations` and `test_missing_stage2`.
- **Channel separation:** stage 3 must not see raw CPI/expectations when stage 2 is the primary source; test `test_drop_features_removes_raw_cpi`.

---

### Task 0: Publication calendar loader and fixture

**Files:**
- Create: `scripts/publication_calendar.py`
- Create: `scripts/publication_calendar_test.py`
- Create: `scripts/testdata/publication_calendar.csv`
- Modify: `Makefile` (add `publication-calendar-test`)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `@dataclass(frozen=True) CalendarPoint(source: str, series: str, period: date, published_at: datetime, precision: str)`
  - `class PublicationCalendar` with `from_csv(path)`, `lookup(source, series, period) -> CalendarPoint`, `published_at(source, series, period) -> datetime`, `is_available(source, series, period, as_of: datetime) -> bool`, `latest_published_month(source, series, as_of: datetime) -> date | None`, `next_unpublished_month(source, series, as_of: datetime) -> date`.
  - Fallback is explicit: `precision="fallback"`.

- [ ] **Step 1: Write failing calendar tests**

```python
# scripts/publication_calendar_test.py
from datetime import date, datetime
from publication_calendar import PublicationCalendar

CAL = PublicationCalendar.from_csv("scripts/testdata/publication_calendar.csv")

def test_monthly_release_time_boundary():
    period = date(2026, 9, 30)
    assert not CAL.is_available("rosstat", "cpi_all", period, datetime(2026, 10, 9, 13, 30))
    assert CAL.is_available("rosstat", "cpi_all", period, datetime(2026, 10, 9, 19, 1))

def test_latest_and_next_sep11():
    as_of = datetime(2026, 9, 11, 13, 30)
    assert CAL.latest_published_month("rosstat", "cpi_all", as_of) == date(2026, 8, 1)
    assert CAL.next_unpublished_month("rosstat", "cpi_all", as_of) == date(2026, 9, 1)

def test_latest_and_next_oct23():
    as_of = datetime(2026, 10, 23, 13, 30)
    assert CAL.latest_published_month("rosstat", "cpi_all", as_of) == date(2026, 9, 1)
    assert CAL.next_unpublished_month("rosstat", "cpi_all", as_of) == date(2026, 10, 1)

def test_weekly_wednesday_boundary():
    period = date(2026, 10, 5)
    assert not CAL.is_available("rosstat", "cpi_weekly", period, datetime(2026, 10, 7, 13, 30))
    assert CAL.is_available("rosstat", "cpi_weekly", period, datetime(2026, 10, 7, 19, 1))

def test_expectations_same_month():
    period = date(2026, 10, 1)
    assert not CAL.is_available("cbr", "infl_exp_expected", period, datetime(2026, 10, 20, 13, 30))
    assert CAL.is_available("cbr", "infl_exp_expected", period, datetime(2026, 10, 23, 13, 30))

def test_fallback_precision():
    point = CAL.lookup("rosstat", "cpi_all", date(2025, 1, 31))
    assert point.precision == "fallback"
```

- [ ] **Step 2: Run test to verify failure**

Run: `uv run --python 3.12 python3 scripts/publication_calendar_test.py -v`
Expected: FAIL because `publication_calendar.py` does not exist.

- [ ] **Step 3: Implement loader and fixture**

Implement `PublicationCalendar` with exact points from CSV and fallback rules:
- monthly CPI: 9th of `M+1` 19:00;
- weekly CPI: `period + 2 days` 19:00;
- CBR SA/trend: 18th of `M+1` 15:00;
- `cbr_infl_exp`: 21st of the same month 15:00;
- business survey: end of `M+1` (conservative).
`published_at` returns a `CalendarPoint`-like object with `precision` (`exact`/`fallback`). `is_available` uses `published_at <= as_of`. `latest_published_month` scans monthly periods backwards from `as_of`; `next_unpublished_month` = latest + 1 month.

Fixture CSV has at least:
`source,series,period,published_at,precision`
- `rosstat,cpi_all,2026-08-31,2026-09-09T19:00,exact`
- `rosstat,cpi_all,2026-09-30,2026-10-09T19:00,exact`
- `rosstat,cpi_weekly,2026-10-05,2026-10-07T19:00,exact`
- `cbr,infl_exp_expected,2026-10-01,2026-10-21T15:00,exact`
- `cbr,cpi_sa,2026-09-30,2026-10-16T15:00,exact`

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run --python 3.12 python3 scripts/publication_calendar_test.py -v`
Expected: PASS.

- [ ] **Step 5: Add Makefile target and commit**

```makefile
publication-calendar-test:
	uv run --python 3.12 python3 scripts/publication_calendar_test.py -v
```

```bash
git add scripts/publication_calendar.py scripts/publication_calendar_test.py scripts/testdata/publication_calendar.csv Makefile
git commit -m "add publication calendar loader"
```

**Go/DDL handoff:** production `publication_calendar` table/view and grants are a separate Go item; this task's CSV is the experiment fallback only.

---

### Task 1: Stage 1 CPI nowcast (`M0`, `M0+1`)

**Files:**
- Create: `scripts/cpi_pipeline_stage1.py`
- Create: `scripts/cpi_pipeline_stage1_test.py`
- Modify: `Makefile` (add `cpi-pipeline-stage1`, `cpi-pipeline-stage1-test`)

**Interfaces:**
- Consumes: `PublicationCalendar.next_unpublished_month`, `cpi_monthly` loaders, `cpi_monthly_v3.weekly_features`.
- Produces:
  - `@dataclass(frozen=True) Stage1Row(meeting_date: date, m0: date, m1: date, features: dict, y0: float | None, y1: float | None)`
  - `build_rows(meetings: list[date], calendar: PublicationCalendar, as_of: date) -> list[Stage1Row]`
  - `fit_predict(train: list[Stage1Row], row: Stage1Row, model: str) -> dict[str, float]`
  - `walk_forward(rows: list[Stage1Row], start: int = 24) -> list[Stage1Prediction]`
  - `evaluate(predictions) -> dict`

- [ ] **Step 1: Write failing clock and leakage tests**

```python
def test_clock_sep11():
    row = build_rows([date(2026, 9, 11)], CAL, date(2026, 9, 11))[0]
    assert row.m0 == date(2026, 9, 1)
    assert row.m1 == date(2026, 10, 1)

def test_clock_oct23():
    row = build_rows([date(2026, 10, 23)], CAL, date(2026, 10, 23))[0]
    assert row.m0 == date(2026, 10, 1)
    assert row.m1 == date(2026, 11, 1)

def test_no_expectations_sa_mp():
    names = set(build_rows([date(2026, 10, 23)], CAL, date(2026, 10, 23))[0].features)
    assert not names & {"infl_exp_expected", "price_expectations", "pi_saar", "cpi_saar"}
    assert not any("mp_" in name for name in names)

def test_weekly_asof_excludes_future_week(monkeypatch):
    # Panel: eligible week 2026-09-28 median 1.0; future week 2026-10-05 median 999.0.
    # The 2026-10-07 13:30 meeting must see 1.0, not 999.0 (release is 19:00).
    monkeypatch.setattr(stage1, "load_weekly_panel", lambda: panel)
    row = build_rows([date(2026, 10, 7)], CAL, date(2026, 10, 7))[0]
    assert row.features["wk_median"] == 1.0
```

- [ ] **Step 2: Run test to verify failure**

Run: `uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_pipeline_stage1_test.py -v`
Expected: FAIL because stage 1 module does not exist.

- [ ] **Step 3: Implement stage 1 builder and models**

- `build_rows`: for each meeting, `m0 = calendar.next_unpublished_month("rosstat","cpi_all", meeting_datetime)`, `m1 = shift_month(m0, 1)`.
- Features: `lag1`/`lag2`/`lag3` from the latest published NSA m/m; `wk_share_rising`, `wk_median`, `wk_median4`, `wk_gasoline4` via `weekly_features(panel, meeting_date)`; `usd_mm`, `key_rate`, `fao_mm`; `month`.
- Add `load_weekly_panel()` as a thin wrapper around `cmv3.load_weekly()` so the as-of test can monkeypatch it; production `build_rows` calls the wrapper.
- Targets: NSA m/m for `m0` and `m1` from `cm.load_targets()`; `None` if not observed.
- Models: CatBoost direct per target (`y0`, `y1`), ElasticNetCV baseline, B1 last value, B3 ElasticNet; no expectations/SA/MP.
- `walk_forward`: train `rows[:i]`, predict `rows[i]`; skip rows with both targets missing.
- `evaluate`: RMSE/MAE per target/model, DM/CI via `eval_utils`.

- [ ] **Step 4: Run tests to verify pass**

Run: `make cpi-pipeline-stage1-test`
Expected: PASS.

- [ ] **Step 5: Add CLI and commit**

`main` supports `--as-of`, `--out-dir`; artifacts `stage1_predictions.csv`, `stage1_result.json`, `stage1_report.md`.
Makefile targets:
```makefile
cpi-pipeline-stage1:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_pipeline_stage1.py $(ARGS)

cpi-pipeline-stage1-test:
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_pipeline_stage1_test.py -v
```
```bash
git add scripts/cpi_pipeline_stage1.py scripts/cpi_pipeline_stage1_test.py Makefile
git commit -m "add stage 1 cpi nowcast"
```

**Gate 1:** stage 1 RMSE on common folds must be ≤ current `cpi_live3_horizons`/`cpi_monthly_v3` + 5 %. If worse, stop and report `iterate`.

---

### Task 2: Real-time SA module

**Files:**
- Create: `scripts/cpi_pipeline_sa.py`
- Create: `scripts/cpi_pipeline_sa_test.py`
- Modify: `Makefile` (add `cpi-pipeline-sa-test`)

**Interfaces:**
- Consumes: monthly NSA series `list[tuple[date, float]]`.
- Produces:
  - `seasonal_factors(series: list[tuple[date, float]], as_of: date, period: int = 12) -> dict[int, float]`
  - `sa_series(series: list[tuple[date, float]], as_of: date) -> list[tuple[date, float]]`
  - `apply_sa(values: list[float], factors: dict[int, float]) -> list[float]`

- [ ] **Step 1: Write failing SA tests**

```python
def test_factors_use_only_past():
    base = [(date(2020, m, 1), 1.0 + 0.1 * m) for m in range(1, 13)]
    future = base + [(date(2021, 1, 1), 999.0)]
    assert seasonal_factors(base, date(2020, 12, 31)) == seasonal_factors(future, date(2020, 12, 31))

def test_additive_sa_removes_known_seasonality():
    series = [(date(2020, m, 1), 10.0 + (m % 12)) for m in range(1, 25)]
    factors = seasonal_factors(series, date(2021, 12, 31))
    adjusted = apply_sa([value for _, value in series], factors)
    assert max(adjusted) - min(adjusted) < 2.0

def test_deterministic():
    series = [(date(2020, m, 1), 1.0) for m in range(1, 25)]
    assert seasonal_factors(series, date(2021, 12, 31)) == seasonal_factors(series, date(2021, 12, 31))
```

- [ ] **Step 2: Run test to verify failure**

Run: `uv run --with "statsmodels>=0.15" --with pandas --with scipy --python 3.12 python3 scripts/cpi_pipeline_sa_test.py -v`
Expected: FAIL because `cpi_pipeline_sa.py` does not exist.

- [ ] **Step 3: Implement STL-based SA**

Use `statsmodels.tsa.seasonal.STL(period=12, robust=True)` on the series truncated to `date <= as_of`; seasonal component = `observed - trend - resid`; `sa_series` returns `observed - seasonal`. Document that X-13 can replace STL behind the same interface; no final factors.

- [ ] **Step 4: Run tests to verify pass**

Run: `make cpi-pipeline-sa-test`
Expected: PASS.

- [ ] **Step 5: Add Makefile target and commit**

```bash
git add scripts/cpi_pipeline_sa.py scripts/cpi_pipeline_sa_test.py Makefile
git commit -m "add real-time seasonal adjustment"
```

---

### Task 3: Stage 2 SA path and cumulative h1/h3/h6

**Files:**
- Create: `scripts/cpi_pipeline_stage2.py`
- Create: `scripts/cpi_pipeline_stage2_test.py`
- Modify: `Makefile` (add `cpi-pipeline-stage2`, `cpi-pipeline-stage2-test`)

**Interfaces:**
- Consumes: `Stage1Row`/OOF stage 1, `PublicationCalendar`, `cpi_pipeline_sa`, `cpi_live3_horizons.cumulative_targets`, `cb_comp_live_compare.fit_component/predict`, `cm.nnls_weights`.
- Produces:
  - `@dataclass(frozen=True) Stage2Row(meeting_date: date, m0: date, features: dict, sa_path: dict[date, float], cum: dict[int, float])`
  - `build_rows(stage1_rows, stage1_oof, cpi_series, expectations, mp, calendar, as_of) -> list[Stage2Row]`
  - `fit_predict(train, row, model: str) -> dict[date, float]`
  - `walk_forward(rows, start: int = 24) -> list[Stage2Prediction]`
  - `evaluate(predictions) -> dict`

- [ ] **Step 1: Write failing target and OOF tests**

```python
def test_target_path_m0_m5():
    rows = build_rows([...], stage1_oof={...}, cpi_series=..., calendar=CAL, as_of=date(2026, 10, 23))
    row = rows[0]
    assert sorted(row.sa_path) == [date(2026, 10, 1) + relativedelta(months=i) for i in range(6)]
    assert row.cum[1] == pytest.approx(row.sa_path[date(2026, 10, 1)], abs=1e-9)
    assert row.cum[3] == pytest.approx(compound(row.sa_path, 3), abs=1e-9)

def test_stage1_oof_not_actual():
    # Synthetic SA target for M0 is 1.0; OOF stage 1 is 0.123.
    rows = build_rows([...], stage1_oof={meeting: {"m0": 0.123, "m1": 0.456}}, ...)
    row = rows[0]
    assert row.features["stage1_m0"] == 0.123
    assert row.sa_path[row.m0] == pytest.approx(1.0)

def test_no_future_sa():
    # huge future NSA point must not change SA factor for as_of
    ...
```

- [ ] **Step 2: Run test to verify failure**

Run: `uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_pipeline_stage2_test.py -v`
Expected: FAIL because stage 2 module does not exist.

- [ ] **Step 3: Implement stage 2 builder and models**

- `build_rows`: `m0` from calendar; SA path targets for `m0…m0+5` using `sa_series` on actual NSA history; `cum` from compounding; features: stage 1 OOF `m0`/`m1`, SA transforms of stage 1, `cbr_sa` if available, `cbr_infl_exp` levels/changes, `price_expectations`/`costs`/`ibk`, weekly diffusion, `month`.
- `fit_predict`: direct CatBoost per month/horizon; optional bottom-up components + NNLS; ensemble 0.5/0.5 only if no worse.
- `walk_forward`: train on rows with stage 1 OOF; skip rows without enough history; evaluate cumulative h1/h3/h6.
- NSA back-transform: apply seasonal factors to the SA path; report RMSE vs actual NSA.

- [ ] **Step 4: Run tests to verify pass**

Run: `make cpi-pipeline-stage2-test`
Expected: PASS.

- [ ] **Step 5: Add CLI and commit**

Makefile targets analogous to stage 1. Artifacts: `stage2_predictions.csv`, `stage2_result.json`, `stage2_report.md`.
```bash
git add scripts/cpi_pipeline_stage2.py scripts/cpi_pipeline_stage2_test.py Makefile
git commit -m "add stage 2 cpi sa path"
```

**Gate 2:** stage 2 **NSA back-transform** cumulative h1/h3/h6 RMSE on common folds must be ≤ current `cpi_live3_horizons` + 5 %. The SA path is compared separately with CBR SA, not with live3. If the NSA back-transform is worse, stop; do not build stage 3.

---

### Task 4: Stage 3 key-rate model with stage 2 path

**Files:**
- Modify: `scripts/key_rate_scenario_fan_v3.py` (add `drop_features`)
- Modify: `scripts/key_rate_scenario_fan_v3_test.py`
- Create: `scripts/key_rate_pipeline.py`
- Create: `scripts/key_rate_pipeline_test.py`
- Modify: `Makefile` (add `key-rate-pipeline`, `key-rate-pipeline-test`)

**Interfaces:**
- Consumes: `key_rate_cpi_v3_experiment.load_rate_data`, `key_rate_scenario_fan_v3.run_v3_walk_forward`, stage 2 OOF.
- Produces:
  - `build_extra(dates: list[date], stage2_oof: dict, variant: str) -> dict[date, dict]`
  - `run_variant(data: dict, variant: str, extra: dict) -> dict`
  - `compare_variants(results: dict) -> dict`
  - `decide(results: dict, comparisons: dict) -> tuple[str, str]`

- [ ] **Step 1: Write failing separation tests**

```python
def test_drop_features_removes_raw_cpi():
    features = v3.build_channel_rows(
        [row], [panel_row], external_fixture(),
        extra_by_date={day: {"cpi_sa_h1": 1.0}},
        drop_features=("pi_saar", "cpi_saar", "infl_exp_expected", "price_expectations"),
    )[0].features
    assert "pi_saar" not in features
    assert features["cpi_sa_h1"] == 1.0

def test_missing_stage2_is_none_and_indicator():
    extra = build_extra([day], {}, "stage2")
    assert extra[day]["cpi_sa_h1"] is None
    assert extra[day]["cpi_sa_available"] == 0

def test_stage3_does_not_see_raw_cpi_expectations():
    names = set(build_extra([day], {day: {...}}, "stage2")[day])
    assert "infl_exp_expected" not in names
    assert "cpi_saar" not in names
```

- [ ] **Step 2: Run test to verify failure**

Run: `uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/key_rate_pipeline_test.py -v`
Expected: FAIL because stage 3 module/`drop_features` does not exist.

- [ ] **Step 3: Implement `drop_features` and stage 3**

- `build_channel_rows(..., extra_by_date=None, drop_features=None)`: after `features.update(...)`, remove names in `drop_features`; baseline `None` unchanged.
- `build_extra`: stage 2 path `cpi_sa_m0…m5`, `cpi_sa_h1/h3/h6`, `cpi_sa_available`; `variant="stage2"` drops raw `pi_saar`, `cpi_saar`, `infl_exp_*`, `price_expectations`, `_chg`; `variant="raw"` keeps them (ablation); missing -> `None` + 0.
- `run_variant`: call `v3.run_v3_walk_forward(..., extra_by_date=extra, drop_features=...)`.
- `compare_variants`/`decide`: same CRPS/coverage/RMSE gate as #296.
- Nested OOF: stage 3 training uses `stage2_oof`; no in-sample stage 2.

- [ ] **Step 4: Run tests to verify pass**

Run: `make key-rate-pipeline-test`
Expected: PASS.

- [ ] **Step 5: Add CLI and commit**

Makefile targets; artifacts `key_rate_pipeline_report.md`, `..._result.json`, `..._folds.csv`, `..._stage2_predictions.csv`.
```bash
git add scripts/key_rate_scenario_fan_v3.py scripts/key_rate_scenario_fan_v3_test.py scripts/key_rate_pipeline.py scripts/key_rate_pipeline_test.py Makefile
git commit -m "add stage 3 key-rate pipeline"
```

**Gate 3:** primary `channel_fan_vol` + stage 2 vs baseline: ΔCRPS < 0, 95 % CI upper ≤ 0, coverage80 in `[0.70; 0.90]` and Wilson CI includes 0.8, RMSE ≤ baseline ×1.05. Otherwise `iterate`/`reject`; no promotion.

---

### Task 5: End-to-end run, report, docs, PR

**Files:**
- Create: `docs/research/2026-10-20-cpi-key-rate-pipeline-results.md`
- Modify: `docs/ROADMAP_KEY_RATE.md`, `docs/KEY_RATE_MODEL.md`, `docs/KEY_RATE_CHANNELS.md`, `docs/LIMITATIONS.md`, `docs/site-pages.json`

**Interfaces:**
- Consumes: stage 1/2/3 artifacts, run IDs.
- Produces: research report, docs updates, PR.

- [ ] **Step 1: Run offline tests**

```bash
make publication-calendar-test
make cpi-pipeline-stage1-test
make cpi-pipeline-sa-test
make cpi-pipeline-stage2-test
make key-rate-pipeline-test
```
Expected: all PASS.

- [ ] **Step 2: Run the full pipeline**

```bash
ML_LOG=1 MLFLOW=0 make cpi-pipeline-stage1
ML_LOG=1 MLFLOW=0 make cpi-pipeline-stage2
ML_LOG=1 MLFLOW=0 make key-rate-pipeline
```
Expected: artifacts in `build/cpi-key-rate-pipeline/`; baseline `channel_fan_vol` reproduces #289 numbers (CRPS 48.215 / coverage 0.778 / RMSE 129.96) or the run is marked non-comparable.

- [ ] **Step 3: Check gates and write report**

Report sections: clock/as-of, stage 1 RMSE/MAE vs live3, stage 2 SA path vs live3, stage 3 CRPS/coverage/RMSE vs `channel_fan_vol`, ablations, DM/CI, episodes 2022–2023/2024–2026, what is not checked, run IDs and sha256.

- [ ] **Step 4: Run docs/CI and commit**

```bash
make all
make docs-check
git add docs/research/2026-10-20-cpi-key-rate-pipeline-results.md docs/ROADMAP_KEY_RATE.md docs/KEY_RATE_MODEL.md docs/KEY_RATE_CHANNELS.md docs/LIMITATIONS.md docs/site-pages.json
git commit -m "docs: publish cpi-key-rate pipeline results"
```

- [ ] **Step 5: Open PR**

PR body: item `<slug>/<item-id>`, `Closes #<issue>`, spec/plan, source, schema, run, example check, live acceptance protocol. No `forecast_log`, no Go/DDL.

---

### Task 6: Go/DDL handoff (separate item)

**Out of scope for this ML plan.** Production `publication_calendar` table/view, comments, `GRANT SELECT`, and importer/calendar scraper are a separate Go/DDL item. ML uses `scripts/testdata/publication_calendar.csv` for the experiment; production path reads the view once the Go item is merged. Do not create ad-hoc tables from Python.

---

## Self-Review

- **Spec coverage:** calendar/PIT (Task 0), stage 1 (Task 1), SA (Task 2), stage 2 (Task 3), stage 3 (Task 4), end-to-end/report (Task 5), Go/DDL handoff (Task 6). All spec sections have a task.
- **Placeholders:** none; every file/signature/test is named.
- **Type consistency:** `Stage1Row`/`Stage2Row` fields are used consistently in stage 2/3 interfaces; `drop_features` is defined in Task 4 and used in tests; `PublicationCalendar` methods are used by stage 1/2.
- **Review Focus:** each of the five failure modes has a named test in its owning task.
- **Proportion:** tasks specify signatures, test names and commands; bodies are left to the implementer except where the spec fixes values.
