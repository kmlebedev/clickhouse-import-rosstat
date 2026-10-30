# План: промоушен и live-мониторинг (`infra/4.3`, #184)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Закрыть `infra/4.3`: gate по DM-HLN + бутстрапу + робастности, shadow/canary в `forecast_log`, витрина `v_ml_live_accuracy`, проверка live-прогноза до публикации и Grafana-алерты на деградацию/свежесть.

**Architecture:** Python-скрипты (`ml_gate.py`, `ml_live_check.py`, `key_rate_forecast_log.py`) читают витрины через `ch_query`; запись live-прогноза идёт только через ingest. Go-часть расширяет разбор `metric` в `ingest/`, добавляет витрину в импортёр `key_rate_views`, грант и MCP-проверку; Grafana-алерты — provisioning YAML. Схема `forecast_log` не меняется.

**Tech Stack:** Go (repo toolchain), ClickHouse (`ReplacingMergeTree`, views), Python 3.12 stdlib + `statsmodels`/`scipy`/`arch` через `uv`, Grafana provisioning, dagu YAML.

**Spec:** `docs/superpowers/specs/2026-10-20-ml-promotion-live-monitoring-design.md`

## Global Constraints

- Один пункт — одна ветка `feat/infra-4.3` — один PR; спека и этот план — первые коммиты.
- `forecast_log` не меняет схему; стадия — суффикс `metric`.
- Запись в ClickHouse — только ingest (`POST /v1/key_rate_forecast`); Python-скрипты только читают.
- Секреты только из env; в артефакты/ошибки не попадают.
- `make all`, `make mcp-check`, `make docs-check`, `make docs-build`, Python-наборы — зелёные.
- Пороги предрегистрированы: DM p<0,05 + 95% bootstrap CI + robustness; shadow 2 + canary 1; alert rolling MAE 3 > 100 б.п.; freshness > 3 дней.

## Review Focus

- **Staged-строки не публикуются:** `v_key_rate_dashboard` их не показывает, `ml_live_check` требует именно `stage='production'`.
- **Идемпотентность:** staged-метрика — отдельный ключ `(metric, forecast_date)`; повтор writer'а и резолвера не плодит строк.
- **Gate не двигает alias:** решение — артефакт + MLflow-теги; промоушен — явное действие.
- **Пустая витрина:** `mcp_check` и `ml_live_check` дают понятную подсказку, а не ложный FAIL/OK.
- **Алерт не зависит от прод-данных для проверки:** firing-тест — временный порог на локальном стенде.

---

### Task 1: ML gate `scripts/ml_gate.py`

**Files:**
- Create: `scripts/ml_gate.py`
- Create: `scripts/ml_gate_test.py`
- Modify: `Makefile` (`ml-gate`, `ml-gate-test`, `.PHONY`)

**Interfaces:**
- Input: `--experiment`, `--champion-run`, `--challenger-run`, `--model`, `--window`, `--unit`, repeatable `--robustness-window`, `--mcs`, `--candidates`, `--out`.
- Reads: `v_ml_runs`, `v_ml_error_by_fold` via `ch_query`.
- Produces: `build/ml-gate/<challenger-run>.json`; tags on MLflow run when `MLFLOW_TRACKING_URI` set.

- [ ] **Step 1: Write failing tests** for common folds, DM direction, bootstrap CI, robustness rejection, `insufficient_folds`, `mcs` with >2 candidates, deterministic artifact, no `ch_exec`.
- [ ] **Step 2: Implement pure functions** (`load_folds`, `align`, `evaluate`, `decide`, `artifact`) with `eval_utils`; validate run IDs as UUID, quote-free SQL.
- [ ] **Step 3: Implement CLI** and `--out`; write artifact atomically; print a compact table.
- [ ] **Step 4: Add `make ml-gate` / `make ml-gate-test`** with `uv run --with "statsmodels>=0.15" --with scipy --with arch --python 3.12`; `MLFLOW_PKG` conditional.
- [ ] **Step 5: Run `python3 scripts/ml_gate_test.py -v` and `make ml-gate` on two existing runs.**

### Task 2: Live-проверка публикации `scripts/ml_live_check.py`

**Files:**
- Create: `scripts/ml_live_check.py`
- Create: `scripts/ml_live_check_test.py`
- Modify: `Makefile` (`ml-live-check`, `ml-live-check-test`)

- [ ] **Step 1: Write failing tests** for all tracks present / missing / only shadow / stale `forecast_date` / `--warn-only` / missing view.
- [ ] **Step 2: Implement** `next_meeting()` from `v_events_calendar`, row checks from `v_ml_live_accuracy`, `--require-llm`, exit codes.
- [ ] **Step 3: Add Makefile targets**; run tests and a live check.

### Task 3: Shadow/canary в writer

**Files:**
- Modify: `scripts/key_rate_forecast_log.py`
- Modify: `scripts/key_rate_forecast_log_test.py`

- [ ] **Step 1: Write failing tests** for `--stage shadow|canary`, suffix after track and after `:llm`, unknown stage, `--dry-run`, production unchanged.
- [ ] **Step 2: Implement** `stage_suffix()` and apply to `model_rows`, `llm_row`, `llm_payload_row`; CLI `--stage`.
- [ ] **Step 3: Run `python3 scripts/key_rate_forecast_log_test.py -v`.**

### Task 4: Ingest stage-суффикс и резолвер

**Files:**
- Modify: `ingest/key_rate_forecast.go`
- Modify: `ingest/key_rate_forecast_test.go`
- Modify: `forecast/resolve_test.go`

- [ ] **Step 1: Write failing tests** for `cbr_rate:taylor:shadow`, `cbr_rate:llm:canary`, rejection of `cbr_rate:shadow`, `cbr_rate:taylor:prod`, empty stage.
- [ ] **Step 2: Implement** `keyRateForecastTrack` returning `(track, stage, ok)`; validate stage only with track; keep base `cbr_rate`.
- [ ] **Step 3: Add resolver test** that staged metric and metadata survive resolution.
- [ ] **Step 4: Run `go test -race ./ingest ./forecast`.**

### Task 5: Витрина `v_ml_live_accuracy`

**Files:**
- Create: `views/ml_live_accuracy.go`
- Create: `views/ml_live_accuracy_test.go`
- Modify: `views/key_rate_views.go` (`keyRateViewsList`, forecast branch excludes staged)
- Modify: `views/key_rate_dashboard_test.go`

- [ ] **Step 1: Write failing contract tests** for name, `Tables=["forecast_log"]`, columns, stage/track parsing, comments; dashboard test asserts exclusion.
- [ ] **Step 2: Implement view** with `splitByChar`, `multiIf`/`endsWith`, `dateDiff`, `abs`.
- [ ] **Step 3: Add to `keyRateViewsList`** and update the dashboard forecast filter.
- [ ] **Step 4: Run `go test -race ./views`.**

### Task 6: Гранты, MCP, dagu, docs

**Files:**
- Modify: `sql/mcp_kimi_reader.sql`
- Modify: `scripts/mcp_check.py`
- Modify: `ARCHITECTURE.md`, `docs/IMPORTERS.md`, `docs/OPERATIONS.md`, `docs/KEY_RATE_PUBLICATIONS.md`, `README.md` (if needed)
- Modify: `docs/ROADMAP_INFRA.md`

- [ ] **Step 1: Add `GRANT SELECT`** on `v_ml_live_accuracy`; keep raw `forecast_log` closed.
- [ ] **Step 2: Extend `mcp_check`** with view readability, stage values, empty-view informational message.
- [ ] **Step 3: Document** the view/stage/writer/gate/live-check; update roadmap `4.3` status and protocol.
- [ ] **Step 4: Run `make import STAT=key_rate_views`, `make mcp-check`, `dagu validate` both files.**

### Task 7: Grafana alerts

**Files:**
- Create: `grafana/provisioning/alerting/ml-live.yaml`
- Create: `scripts/grafana_alert_check.py`
- Create: `views/ml_live_alert_test.go` (YAML contract, `gopkg.in/yaml.v3`)
- Modify: `Makefile` (`grafana-check`, `grafana-provision-alerting-staging`, `grafana-deploy-staging`)
- Modify: `docs/GRAFANA.md`

- [ ] **Step 1: Write failing YAML contract test** (rule UIDs, `v_ml_live_accuracy`, thresholds 100/3, `severity=warning`).
- [ ] **Step 2: Add provisioning YAML** with degradation + freshness rules.
- [ ] **Step 3: Add alert check script** and wire into `make grafana-check`.
- [ ] **Step 4: Add staging copy/reload target.**
- [ ] **Step 5: Live test** on local stand: rule visible; temporarily lower threshold via API, observe Firing, restore 100; record in PR protocol.

### Task 8: Full verification and PR

- [ ] `make all`
- [ ] `python3 scripts/ml_gate_test.py -v`; `python3 scripts/ml_live_check_test.py -v`; `python3 scripts/key_rate_forecast_log_test.py -v`; `python3 scripts/key_rate_model_test.py -v`; `python3 scripts/mlflow_registry_test.py -v`
- [ ] `make import STAT=key_rate_views`; `make mcp-check`
- [ ] `make ml-gate` on two runs; `make key-rate-forecast-log ARGS="--stage shadow --dry-run"`; live write; `make import STAT=forecast_resolve`; `make ml-live-check`
- [ ] `make grafana-validate`; `make grafana-check`; `make docs-check`; `make docs-build`
- [ ] `dagu validate dagu/forecast.yaml`; `dagu validate dagu/ml.yaml`
- [ ] Commit, push, open one PR with `Closes #184` outside backticks; protocol comment; Project → `In Review`; roadmap status in same PR.

## Risks

- Shadow/canary cycle (3 meetings) cannot finish in the PR; verify the mechanism on historical/test rows and say so.
- Test rows are marked `model='gate-selftest'`; destructive cleanup is owner-only.
- MCS requires `arch` and >2 candidates; pairwise path is the default.
- Staging may be unavailable; record «не проверено».
- Grafana contact point is owner-configured; the rule itself is UI-visible and Firing.
