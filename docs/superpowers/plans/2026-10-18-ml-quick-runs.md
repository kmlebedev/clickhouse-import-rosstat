# ML Quick Runs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить в `scripts/cpi_monthly.py` opt-in быстрый режим `--quick` (только основное окно, без robustness/фиксированного сплита) и `--jobs N` (параллельные fit'ы по процессам, `thread_count=1`), не меняя полный прогон без флагов.

**Architecture:** `--quick` сужает `window_specs` до одного окна и пропускает `fixed_split`; `--jobs` (только с `--quick`) заменяет последовательный `walk_forward` на `ProcessPoolExecutor`, где воркер считает все модели одного фолда и возвращает прогнозы/веса/диагностику. Полный путь без флагов остаётся прежним; `ML_LOG` фиксирует `quick/jobs`.

**Tech Stack:** Python 3.12, `catboost>=1.2`, `scikit-learn`, `statsmodels>=0.15`, `scipy`, `concurrent.futures.ProcessPoolExecutor`; тесты — stdlib `unittest`, офлайн.

**Spec:** `docs/superpowers/specs/2026-10-18-ml-quick-runs-design.md`

## Global Constraints

- Рабочая директория — worktree `.worktrees/ml-quick-runs`, ветка `feat/ml-quick-runs` (stacked на `feat/key-rate-ml-v2`, PR #177); главный worktree не трогать.
- Без `--quick`/`--jobs` поведение, `window_specs`, `fixed_split`, `catboost_params` и артефакты полного прогона **не меняются**.
- `--jobs` допустим только с `--quick`; `0`/`1` — последовательно, `N>1` — пул; отрицательное/нечисловое — ошибка.
- В quick-режиме CatBoost `thread_count=1`; в полном — дефолт библиотеки.
- `ML_LOG`-паспорт: `config.quick`, `config.jobs`, `run_name` с `quick`; `metrics.windows` — только main; `predictions` — только main.
- Тесты — stdlib `unittest`, офлайн, транспорт мокается; `make all` зелёный.
- Не менять модели/признаки/окна полного прогона; quick — подмножество окон, не другая модель.
- Спека/план — первые коммиты ветки; один PR в конце.

## Review Focus

1. **Полный прогон не изменился:** без флагов `window_specs` даёт 3 окна (short) / 2 (long), `fixed_split` вызывается, `catboost_params` без `thread_count`; артефакты/числа прежние.
2. **Quick только main:** ровно одно окно, `fixed_split` не вызывается, отчёт/`ML_LOG` это явно говорят.
3. **Параллелизм детерминирован:** `--jobs 1` и `--jobs N` дают одинаковые `(month, model, y_pred)`; два повтора — одинаковые sha256 артефактов; порядок сборки фиксирован.
4. **Воркер без общего состояния:** top-level picklable, возвращает прогнозы/веса/`fold_notes`/`skipped`; не полагается на `last_fit` в главном процессе.
5. **Валидация CLI:** `--jobs` без `--quick` — `parser.error`; `N<0` — ошибка; `N=0/1` — последовательно.
6. **ML_LOG:** `config.quick/jobs` в паспорте, `run_name` содержит `quick`; quick-метрики без robustness/сплита.

---

### Task 1: `--quick` — main-окно без robustness и сплита

**Files:** Modify `scripts/cpi_monthly.py`, `scripts/cpi_monthly_test.py`.

**Interfaces:**
- Consumes: текущие `window_specs`, `fixed_split`, `run_summary`, `render_report`, `main`.
- Produces:
  - `window_specs(start=..., embargo=..., quick: bool = False) -> list[tuple[int,int]]`: `quick=True` — ровно `[(start, embargo)]`.
  - `run_summary(..., quick: bool = False, jobs: int = 0)`.
  - `render_report(..., quick: bool = False)` — заголовок/строка quick; `article_split` не печатается.
  - CLI `--quick` (`store_true`), `--jobs` (`int`, default 0) с валидацией «jobs требует quick»/`jobs>=0`.
- [ ] **Step 1: Тесты (red).** `test_quick_window_specs_only_main`; `test_quick_skips_fixed_split` (мок `fixed_split` не вызван); `test_full_window_specs_unchanged` (short 3, long 2); `test_quick_report_marks_quick_and_no_split`; `test_jobs_requires_quick` (`SystemExit`); `test_negative_jobs_rejected`.
- [ ] **Step 2: Реализация.** `window_specs(..., quick)`; `run_summary` пропускает `fixed_split` при quick (`split_predictions=None`); `render_report` — quick-заголовок и «сплит не рассчитан»; `main` — флаги/валидация; `ML_LOG config.quick/jobs`, `run_name` с `quick`.
- [ ] **Step 3: Тесты/живой smoke.** `cpi_monthly_test.py` зелёный; `make cpi-monthly ARGS="--quick"` — одно окно, отчёт quick; полный без флагов — прежние окна (не запускать целиком, если дорого; проверить `--quick` на фикстурах).
- [ ] **Step 4: Commit** `cpi monthly: add quick main-window mode`.

### Task 2: `--jobs N` — параллельные фолды

**Files:** Modify `scripts/cpi_monthly.py`, `scripts/cpi_monthly_test.py`.

**Interfaces:**
- Consumes: `walk_forward`, `MODELS`, `Row`, `Prediction`, `WindowRun`, `fit_*`, `nnls_weights`.
- Produces:
  - `walk_forward_parallel(rows, models, start, embargo, jobs, skipped=None, notes=None) -> list[Prediction]` (или эквивалент, собирающий `WindowRun`): один `ProcessPoolExecutor(max_workers=min(jobs, cpu_count()))`; воркер `_run_fold_worker(train, row, model_names)` возвращает `{model: {"pred":..., "skipped":bool, "notes":..., "weights":...}}`.
  - `catboost_params`/`_fit_catboost` получают `thread_count=1` в quick (через явный параметр/контекст; полный путь — без изменений).
  - `evaluate_windows(..., quick=False, jobs=0)`; `run_summary` передаёт quick/jobs.
- [ ] **Step 1: Тесты (red).** `test_quick_parallel_matches_sequential` на фикстуре (jobs 1 vs 2/4, одинаковые предсказания); `test_quick_parallel_repeat_deterministic` (два прогона — одинаковые sha256); `test_parallel_worker_picklable` (top-level, не замыкание); `test_parallel_skipped_and_notes` (вырожденный фолд/`ConvergenceWarning` не теряются); `test_parallel_does_not_touch_full_mode` (jobs без quick — ошибка).
- [ ] **Step 2: Реализация.** `ProcessPoolExecutor` (spawn на macOS), воркер считает все модели фолда; главный собирает `Prediction` в порядке фолдов/моделей, агрегирует `skipped`/`notes`/веса; `thread_count=1` только в quick; полный путь — прежний `walk_forward`.
- [ ] **Step 3: Живой бенчмарк.** `time make cpi-monthly ARGS="--sample long --quick --jobs 8"` — ожидание ≤30 мин (≥3× против 90); short quick — аналогично; два повтора — sha256; `--jobs 1` vs `--jobs 8` — одинаковые прогнозы (на коротком окне/фикстуре).
- [ ] **Step 4: Commit** `cpi monthly: parallel quick runs`.

### Task 3: Документация и приёмка

**Files:** Modify `README.md`, `docs/DEVELOPMENT.md`, `docs/OPERATIONS.md`, `docs/LIMITATIONS.md`, `docs/ROADMAP_INFRA.md`.

- [ ] **Step 1: Документация.** README/DEVELOPMENT — `--quick`/`--jobs` и предупреждение «quick-числа не официальные»; OPERATIONS — `ML_LOG=1 make cpi-monthly ARGS="--sample long --quick --jobs 8"`; LIMITATIONS — quick не заменяет полный прогон; ROADMAP_INFRA §5 — статус `4.4` ✅ с числами бенчмарка.
- [ ] **Step 2: Приёмка.** `make all`; 4 Python-набора; `make docs-check`; быстрый long/short прогоны (jobs 8) с числами/временем; повтор (sha256); `--jobs 1` vs 8; полный без флагов — инварианты/прежние числа; `ML_LOG=1 --quick` виден в `v_ml_*`.
- [ ] **Step 3: Commit** `docs: cpi monthly quick runs`.

## Self-Review

- **Покрытие спеки:** §2.1 → Task 1; §2.2 → Task 2; §2.3/§3 → Tasks 1–2; §4 → Task 3.
- **Согласованность:** `quick`/`jobs` проходят `main → run_summary → evaluate_windows → window_specs/walk_forward_parallel → ML_LOG`; полный путь без флагов не меняется.
- **Review Focus:** каждый пункт привязан к тесту (полный режим, quick-окно/сплит, parallel==sequential, повтор, pickling, CLI).
- **Пропорция:** тела функций пишет имплементер по сигнатурам/спеке.
