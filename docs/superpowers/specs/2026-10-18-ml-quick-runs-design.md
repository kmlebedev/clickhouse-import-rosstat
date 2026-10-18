# Спека: быстрый режим ML-экспериментов (`--quick` + параллельные fit'ы)

**Дата:** 18 октября 2026 г.
**Пункт роадмапа:** `infra/4.4` — [ROADMAP_INFRA.md](../../ROADMAP_INFRA.md) §5, фаза 4; issue [#197](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/197).
**Зависит от:** `key-rate/phase-4-ml-v2` ([PR #177](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/177)) — `scripts/cpi_monthly.py`.
**Статус:** спека дизайна.

---

## 1. Зачем

Полные прогоны `cpi_monthly` стоят **46–90 минут**:

- short: 369 фолдов (24/0, 48/0, 24/1) × 8 моделей = 2 952 CatBoost-фита, ~54 мин;
- long: 494 фолда (48/0, 48/1) × 8 моделей = 3 952 фита, ~90 мин.

Для официальной приёмки это нормально, для частых экспериментов — блокер. GPU не помогает: CatBoost не поддерживает Metal/MPS (M5 GPU), а на NVIDIA-карте маленькие матрицы 150–292 × 12 и тысячи мелких fit'ов дают overhead; кроме того, `monotone_constraints` на GPU, вероятно, не поддерживаются.

Реальный рычаг — **меньше fit'ов на эксперимент** (`--quick`: только основное окно) и **параллелизм по процессам** (`--jobs N`: 8 независимых fit'ов на фолд, `thread_count=1`). Оба флага — **opt-in**: полный прогон без флагов остаётся байт-в-байт прежним.

## 2. Дизайн

### 2.1 `--quick`

- Считается **только основное окно** walk-forward: `window_specs(start, embargo, quick=True) -> [(start, embargo)]`. Robustness-окно (`48/0` для short, `48/1` для long) не считается.
- **Фиксированный сплит статьи не считается**: `fixed_split` не вызывается, `article_split` в отчёте/метриках — `null`/«не рассчитан», а не пустая таблица.
- Live nowcast остаётся (дёшево, полезно); строка «2022 исключён» и DM primary от main-окна остаются.
- Отчёт и `ML_LOG` явно помечают `quick: true`: заголовок, раздел «что не проверено», `config.quick=true`, `run_name` содержит `quick`.
- `--quick` работает с `--sample short|long`, `--start`, `--embargo-months`.

### 2.2 `--jobs N`

- Допустим **только вместе с `--quick`**; без `--quick` — ошибка argparse (`--jobs требует --quick`), чтобы не менять официальный полный путь.
- `N` — число процессов: `0`/`1` — последовательно, `N>1` — `ProcessPoolExecutor(max_workers=N)`; верхняя граница `min(N, cpu_count())`; отрицательное/нечисловое — ошибка.
- В quick-режиме CatBoost строится с `thread_count=1` (один fit — один поток), чтобы процесс-параллелизм не oversubscribe'ил CPU. `catboost_metadata` фиксирует `thread_count=1`; полный путь без флагов `thread_count` не задаёт (дефолт библиотеки).
- Единица параллелизма — **фолд** (не отдельный fit): воркер получает `train` (префикс строк), `row`, `with_mp`-варианты и считает все 8 моделей фолда; возвращает `{model: prediction}`, веса NNLS (если bottom-up), `fold_notes` (B3) и диагностику. Главный процесс собирает `Prediction`/`WindowRun` в порядке фолдов и моделей.
- Воркер — top-level picklable-функция; данные маленькие (≤292 строк), pickling дешёвый. `last_fit`/`last_weights` в воркере не нужны: метаданные модели статичны, веса возвращаются; отчёт не читает `last_fit`.
- Детерминизм: каждый fit использует `SEED` и `thread_count=1`; порядок результатов фиксирован (фолд, затем модель); `--jobs 1` и `--jobs N` обязаны дать одинаковые прогнозы/метрики. `ProcessPoolExecutor` с `spawn` на macOS — без глобального состояния.
- Полный путь (без `--quick`) не использует пул и `thread_count` не меняет.

### 2.3 Совместимость и `ML_LOG`

- Без `--quick`/`--jobs` поведение, `window_specs`, `fixed_split`, `catboost_params` и артефакты **не меняются**; официальные числа short/long остаются прежними.
- `ML_LOG=1` в quick-режиме пишет `config.quick=true`, `config.jobs=N`, `run_name` с `quick`; `metrics.windows` — только main; `predictions` — только main-фолды; `sample_meta`/artifacts — как обычно.
- Отчёт quick-режима: заголовок `(quick)`, строка `quick: только основное окно; robustness/сплит не считались`, `article_split` — «не рассчитан».

## 3. Тесты и проверки

- `test_quick_window_specs_only_main`: `window_specs(24,0,quick=True) == [(24,0)]`, `window_specs(48,0,quick=True) == [(48,0)]`; full — прежние 3/2 окна.
- `test_quick_skips_fixed_split`: `run_summary(quick=True)` не вызывает `fixed_split`; отчёт/`ML_LOG` не содержат article split.
- `test_quick_parallel_matches_sequential`: на фикстуре/синтетике `--jobs 1` и `--jobs 2/4` дают одинаковые `(month, model, y_pred)`; `thread_count=1` в обоих.
- `test_quick_parallel_repeat_deterministic`: два прогона `--quick --jobs N` — одинаковые sha256 report/CSV/JSON.
- `test_full_mode_unchanged`: без флагов `window_specs`/`fixed_split`/`catboost_params` совпадают с текущими; `--jobs` без `--quick` — ошибка; `--jobs -1` — ошибка.
- `test_quick_ml_log_config`: `ML_LOG`-config содержит `quick=true`, `jobs=N`; `metrics.windows` — только main; `run_name` содержит sample/quick.
- `make all`, `cpi_monthly_test.py`, `cpi_nowcast_test.py`, `key_rate_model_test.py`, `docs-check` — зелёные.

## 4. Приёмка

1. Бенчмарк: `time make cpi-monthly ARGS="--sample long --quick --jobs 8"` — **≥3× быстрее** полного long (90 мин), т.е. ≤30 мин; short quick — аналогично.
2. Два повтора quick-прогона — одинаковые артефакты (sha256); `--jobs 1` vs `--jobs 8` — одинаковые прогнозы.
3. Полный `make cpi-monthly` без флагов — прежние числа/артефакты (тест-инварианты + сверка с `artifacts-run2`/long-артефактами).
4. `ML_LOG=1` quick-прогон виден в `v_ml_runs`/`v_ml_leaderboard`, `config.quick/jobs` в паспорте; сырые таблицы закрыты.
5. README/DEVELOPMENT/OPERATIONS/LIMITATIONS/ROADMAP обновлены; статус `4.4` — ✅ с числами; `make all`/`make docs-check` зелёные.

## 5. Границы

- GPU/MPS и перенос на NVIDIA — вне пункта; CatBoost M5 GPU не поддерживает.
- Гиперпараметры, число итераций, число признаков, окна/фолды полного прогона не меняются; quick — это подмножество окон, не «другая модель».
- Кэш fit'ов между запусками, MLflow/промоушен (`4.2`/`4.3`), дашборды — вне пункта.
- Официальные короткие/длинные числа принимаются только полным прогоном без флагов; quick-числа — экспериментальные и помечаются.
