# P0.5: MLflow/`ML_LOG`-паспорт линии 88 групп — план

> **Пункт:** `key-rate/phase-4-cb-comp-88-groups-live` → P0.5, `role:ml`.
> **Спека:** [2026-11-01-cb-comp-p05-mlflow-design.md](../specs/2026-11-01-cb-comp-p05-mlflow-design.md).
> **Ветка:** `feat/cb-comp-88-groups-p05-mlflow`; worktree `.worktrees/cb-comp-88-p05`.

## Шаги

1. **Спека и план** — этот файл и спека первыми коммитами ветки; issue пункта
   заводится после правки владеющего роадмапа.
2. **Помощник** `scripts/models/cb_comp/cb_comp_mlog.py`:
   - `build_fold_rows(aggregated, truth, months, models, unit="pp", meta)` —
     `PredictionRow` на `(model, horizon, month)`, пропуск неконечных значений,
     `meta` с `horizon`, `version`, `normal`, `truth`, `t_live`;
   - `start(...)` — `ml_experiment.start_run`, обёрнутый в try/except
     (сбой приёмника не роняет эксперимент);
   - `finish(logger, metrics, artifacts, notes)` — best-effort `finished/failed`.
3. **`cb_comp_h1_bs_combo.py`**: в `run()` после записи артефактов — паспорт
   (`experiment=cb_comp_88_groups`, 864 fold-строки, config/sample, metrics,
   artifacts). `result.json`/`predictions.csv` не меняются.
4. **`cb_comp_live_bottomup.py`**: то же для `MODELS` (648 строк).
5. **Makefile**: `cb-comp-h1-bs-combo` и `cb-comp-live-bottomup` получают
   `@$(MLFLOW_PREFLIGHT)`, `$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV)`, `$(MLFLOW_PKG)`.
6. **Тесты**: `cb_comp_mlog_test.py` + интеграционные проверки в
   `cb_comp_h1_bs_combo_test.py`/`cb_comp_live_bottomup_test.py`; запускаются
   из существующих `-test`-целей.
7. **Живая приёмка** (входы — панель `aec91f99…`, public `47176baa…`,
   `sheet_map` `0d8a6ab5…`):
   - `ML_LOG=1 make cb-comp-h1-bs-combo ARGS="--archive <CB_COMP>"` дважды;
   - sha256 артефактов и `config_hash`/`sample_hash` совпадают, `run_id` разный;
   - MCP-SQL: `v_ml_runs`, `v_ml_leaderboard` (16 строк × 54 фолда),
     `v_ml_error_by_fold` (864 строки), сравнение fold-значений A/B — 0
     расхождений;
   - `make ml-gate ARGS="--experiment cb_comp_88_groups --champion-run <A>
     --challenger-run <B> --model combo3 --window h1 --robustness-window h3
     --robustness-window h6"`;
   - `make mlflow-export ARGS="--experiment cb_comp_88_groups --dry-run"`.
8. **Опционально**: прогон `--version 2009` для содержательной пары gate и
   живой прогон `cb-comp-live-bottomup`.
9. **Документация**: отчёт `docs/research/2026-11-01-cb-comp-p05-mlflow.md`,
   копия gate-JSON, правки гайда (§0, §1.6, §1.10, §5, §6.2, §8), роадмапа,
   README, скила `ml-engineer`, `docs/site-pages.json`.
10. **Приёмка**: `make cb-comp-h1-bs-combo-test`,
    `make cb-comp-live-bottomup-test`, `make all`, `make docs-check`;
    протокол — комментарием в PR; статус роадмапа и Project V2 — `In Review`.

## Откат

Код откатывается revert'ом коммитов; run'ы в `ml_runs`/`ml_predictions` и
MLflow остаются историей (удалять их не нужно и нельзя — это паспорта).
