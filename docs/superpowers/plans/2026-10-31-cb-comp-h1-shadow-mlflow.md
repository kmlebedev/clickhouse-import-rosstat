# План: P0.6a — MLflow-зеркало paper shadow `wk_mix_ex@v1`

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, issue [#462](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/462), зависит от #455. Спека — [2026-10-31-cb-comp-h1-shadow-mlflow-design.md](../specs/2026-10-31-cb-comp-h1-shadow-mlflow-design.md). Ветка `feat/cb-comp-h1-shadow-mlflow`, worktree `.worktrees/cb-comp-h1-shadow-mlflow`.

**Инварианты:** JSON — источник истины; MLflow — зеркало; **канонический backend — staging** (`ps-palmshell`/`mlflow.service`, `http://192.168.3.9:5000`; fallback — туннель `15001`), локальный `build/mlflow` — dev/офлайн; в ClickHouse не пишем; модель/пороги/JSON/`cb_comp_h1_shadow.py` не меняем; alias не создаём; `mlflow` — через `uv run --with`, не в pyproject.

## Task 1. Спека и план (первые коммиты)

- [x] `docs/superpowers/specs/2026-10-31-cb-comp-h1-shadow-mlflow-design.md`
- [x] `docs/superpowers/plans/2026-10-31-cb-comp-h1-shadow-mlflow.md`
- [x] Коммит: `docs: spec and plan for cb-comp h1 shadow mlflow mirror`

## Task 2. Логгер

- [x] `scripts/models/cb_comp/cb_comp_h1_shadow_mlflow.py`:
  - константы `EXPERIMENT=cb_comp_h1_shadow`, `VERSION=wk_mix_ex@v1`, `CHAMPION=combo3`, `SHADOW_MODELS`, `COMPONENTS`, `SHADOW_DIR`;
  - `load_payload(path)`: чтение JSON, `sha256` байтов, валидация схемы P0.6 без импорта тяжёлых модулей;
  - `build_plan(payload, path, sha)`: `run_name`, tags, params, metrics, summary;
  - `find_existing(client, experiment_id, month, sha)` — `search_runs` по `tags.month`+`tags.json_sha256`;
  - `log_path(path, tracking_uri=None, ...)`: `dry_run` — без URI; реальный прогон без URI — `MissingTrackingUriError`;
  - `resolve_tracking_uri(value)`: `--tracking-uri` → `MLFLOW_TRACKING_URI` → нет (локальный дефолт не подставляется);
  - `collect_paths(month, path, backfill)`;
  - CLI: `--month`, `--path`, `--backfill`, `--dry-run`, `--force`, `--tracking-uri`;
  - `main`: fail-fast без URI с staging-инструкцией.

## Task 3. Офлайн-тесты

- [x] `scripts/models/cb_comp/cb_comp_h1_shadow_mlflow_test.py` (unittest, **25 тестов**):
  - временный `sqlite:///<tmp>/mlflow.db`, `MlflowClient`, `chdir` в tmp, артефакты в tmp;
  - фикстуры JSON: полный 2026-10, `actual`, два месяца для backfill, битые варианты;
  - тесты: схема, идемпотентность, `--force`, dry-run без URI, missing URI, staging-like URI из env, actual/no-actual, invalid JSON, backfill, month/path mismatch.
- [x] `make cb-comp-h1-shadow-mlflow-test` — зелёный (25 OK).

## Task 4. Make, README, гайд, роадмап

- [x] Makefile: `cb-comp-h1-shadow-mlflow`, `cb-comp-h1-shadow-mlflow-test`, `.PHONY`.
  - цель требует `MLFLOW_TRACKING_URI`/`--tracking-uri`; без URI (и без `--dry-run`) печатает staging-инструкцию и падает; локальный `build/mlflow` не подставляется;
  - `MLFLOW_TRACKING_URI_EXPLICIT` фиксирует URI, заданный пользователем до дефолта.
- [x] README: строка после P0.6 — staging, туннель, обязательный URI.
- [x] Гайд `docs/guides/cb-comp-live-model-map.md`: §1.10 (staging mirror — не gate-вход), §8.1 (скрипт), §8.2 (make), §8.3 (staging run/артефакт).
- [x] `docs/ROADMAP_KEY_RATE.md`: строка P0.6a — staging run `9c536…`, experiment 1; локальный `b034…` — dev; In Review PR #464.
- [x] `docs/DEVELOPMENT.md`: офлайн-набор MLflow — staging canonical/local tests.
- [x] `docs/README`/`site-pages.json` — research-страница не добавлялась.

## Task 5. Живой прогон в staging

- [x] Проверить staging: `curl http://192.168.3.9:5000/health` → 200 (первичная проверка шла через туннель `15001`; после #486 — напрямую).
- [x] Read-only сверка staging: experiment 1, run `9c53630947894b2682bae2d0d1908762`, tags/metrics/artifacts, `shadow.json` sha `f98b1e28…`, побайтно равен локальному JSON.
- [x] `MLFLOW_TRACKING_URI=http://192.168.3.9:5000 make cb-comp-h1-shadow-mlflow ARGS="--backfill"` → `skipped 9c536…`; новых run'ов нет, staging run не перезаписан.
- [x] Не применять `--force` на staging.
- [x] Локальный dev-run `b034a74d…` (`build/mlflow`, experiment 3) — dev-артефакт, не live-приёмка.

## Task 6. Проверки и PR

- [x] `make cb-comp-h1-shadow-mlflow-test` — 25 OK.
- [x] `make docs-check`.
- [x] `make all` (Go; код Go не менялся).
- [x] `make plugin-manifests-check` — не нужен, манифесты не трогались.
- [x] `git status` — только ожидаемые файлы; `build/mlflow` не коммитится.
- [x] PR #464: staging run id/experiment, artifact sha, «локальный `b034…` — не канонический», `Closes #462`.
- [x] Project V2: `Status=In Review`, `PR`.
- [x] Не мержить.
