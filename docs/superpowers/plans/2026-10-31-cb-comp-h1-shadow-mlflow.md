# План: P0.6a — MLflow-зеркало paper shadow `wk_mix_ex@v1`

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, issue [#462](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/462), зависит от #455. Спека — [2026-10-31-cb-comp-h1-shadow-mlflow-design.md](../specs/2026-10-31-cb-comp-h1-shadow-mlflow-design.md). Ветка `feat/cb-comp-h1-shadow-mlflow`, worktree `.worktrees/cb-comp-h1-shadow-mlflow`.

**Инварианты:** JSON — источник истины; MLflow — зеркало; в ClickHouse не пишем; модель/пороги/JSON/`cb_comp_h1_shadow.py` не меняем; alias не создаём; `mlflow` — через `uv run --with`, не в pyproject.

## Task 1. Спека и план (первые коммиты)

- [ ] `docs/superpowers/specs/2026-10-31-cb-comp-h1-shadow-mlflow-design.md`
- [ ] `docs/superpowers/plans/2026-10-31-cb-comp-h1-shadow-mlflow.md`
- [ ] Коммит: `docs: spec and plan for cb-comp h1 shadow mlflow mirror`

## Task 2. Логгер

- [ ] `scripts/models/cb_comp/cb_comp_h1_shadow_mlflow.py`:
  - константы `EXPERIMENT=cb_comp_h1_shadow`, `VERSION=wk_mix_ex@v1`, `CHAMPION=combo3`, `SHADOW_MODELS`, `COMPONENTS`, `SHADOW_DIR`;
  - `load_payload(path)`: чтение JSON, `sha256` байтов, валидация схемы P0.6 без импорта тяжёлых модулей;
  - `run_spec(payload, path)`: `run_name`, tags, params;
  - `metrics(payload)`: `pred.*`, `served.*`, `weekly_features.*`, `s0.*`; `actual.*`, `error_bp`, `abs_error_bp` при наличии факта;
  - `summary(payload, path, ...)`: `run_summary.json` для `log_dict`;
  - `find_existing(client, experiment_id, month, sha)` — `search_runs` по `tags.month`+`tags.json_sha256`;
  - `log_run(payload, path, tracking_uri, force, dry_run)` — создание experiment при отсутствии, run, params/metrics/artifacts, `FINISHED`/`FAILED`;
  - `collect_paths(month, path, backfill)` — один файл, `--month`, или скан каталога;
  - CLI: `--month`, `--path`, `--backfill`, `--dry-run`, `--force`, `--tracking-uri`.
- [ ] Проверка: `python3 -m py_compile`; `get_file_problems` в GoLand.

## Task 3. Офлайн-тесты

- [ ] `scripts/models/cb_comp/cb_comp_h1_shadow_mlflow_test.py` (unittest):
  - временный `sqlite:///<tmp>/mlflow.db`, `MlflowClient`, `chdir` в tmp, артефакты в tmp;
  - фикстуры JSON: полный 2026-10, `actual`, два месяца для backfill, битые варианты;
  - тесты §7 спеки (схема, идемпотентность, `--force`, dry-run, actual/no-actual, invalid JSON, backfill, month/path mismatch).
- [ ] `make cb-comp-h1-shadow-mlflow-test` — зелёный.

## Task 4. Make, README, гайд, роадмап

- [ ] Makefile: `cb-comp-h1-shadow-mlflow`, `cb-comp-h1-shadow-mlflow-test`, обновить `.PHONY`.
  - `cb-comp-h1-shadow-mlflow`: `MLFLOW_TRACKING_URI=$(MLFLOW_TRACKING_URI) $(MLFLOW_HTTP_ENV) uv run --with "mlflow==$(MLFLOW_VERSION)" --python 3.12 python3 scripts/models/cb_comp/cb_comp_h1_shadow_mlflow.py --tracking-uri $(MLFLOW_TRACKING_URI) $(ARGS)`.
- [ ] README: строка после P0.6.
- [ ] Гайд `docs/guides/cb-comp-live-model-map.md`: §8.1 (скрипт), §8.2 (make), §8.3 (артефакт/зеркало), §1.10 (оговорка: зеркало не gate-вход).
- [ ] `docs/ROADMAP_KEY_RATE.md`: строка P0.6a в пункте `phase-4-cb-comp-88-groups-live`; статус — In Review (после PR).
- [ ] `docs/DEVELOPMENT.md`: офлайн-набор MLflow — добавить тест (раздел Python-тестов).

## Task 5. Живой backfill 2026-10

- [ ] `make cb-comp-h1-shadow-mlflow ARGS="--month 2026-10 --dry-run"` — план.
- [ ] `make cb-comp-h1-shadow-mlflow ARGS="--backfill"` — run 2026-10.
- [ ] Повторить — skip с тем же run id.
- [ ] Проверить `MlflowClient`: experiment `cb_comp_h1_shadow`, run name, tags, metrics, artifacts; записать `run_id`/`experiment_id` в PR.
- [ ] `build/mlflow` не коммитить.

## Task 6. Проверки и PR

- [ ] `make cb-comp-h1-shadow-mlflow-test`
- [ ] `make docs-check`
- [ ] `make all` (Go; если долго — CI)
- [ ] `make plugin-manifests-check` — не нужен, манифесты не трогались (зафиксировать).
- [ ] `git status` — только спека, план, скрипт, тест, Makefile, README, гайд, DEVELOPMENT, роадмап.
- [ ] PR: title `[key-rate/phase-4-cb-comp-88-groups-live] P0.6a: MLflow-зеркало paper shadow wk_mix_ex@v1`, `Closes #462`, run id/experiment, команды, idempotency, «JSON — источник истины, MLflow — зеркало; gate/пороги не менялись».
- [ ] Project V2: `Status=In Review`, `PR`.
- [ ] Не мержить.
