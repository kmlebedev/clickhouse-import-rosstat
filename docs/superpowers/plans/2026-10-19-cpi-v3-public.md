# План: Task 4 — модель v3 на публичных данных (industry MP из #239)

**Дата:** 19 октября 2026 г.
**Пункт:** `key-rate/phase-4-ml-components` (issue [#237](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/237)).
**Спека:** [2026-10-19-cpi-v3-public-design.md](../specs/2026-10-19-cpi-v3-public-design.md).
**Ветка:** `feat/cpi-v3-public`.
**Worktree:** `.worktrees/cpi-v3-public`.

## Global Constraints

- Один пункт — одна ветка — один PR; спека и план — первые коммиты.
- Только чтение ClickHouse; в БД не пишем; DDL/GRANT/Go-код не меняем.
- Industry MP — только `v_cbr_survey` (#239); официальный прогон XLSX не читает.
- `cpi_monthly_v3.py` и `cpi_monthly_industry.py` не меняются; новый модуль их переиспользует.
- Walk-forward только расширяющимся окном; никакого random split; CatBoost — дефолты, `random_seed=20261018`; никакого тюнинга по тесту.
- As-of: weekly `date + 2`; industry `available_from_survey(P) = конец P+1`; cutoff 8-е M+1.
- Purge для h>1: train-origin `j` только при `j + h − 1 < i`; `start_eff = start + h − 1`, `embargo_eff = embargo + h − 1`.
- NaN ≠ 0; CatBoost нативно, ElasticNet — медиана train; отсутствующий industry-ряд — ошибка, не NaN.
- Tests — stdlib `unittest`, офлайн, без сети/ClickHouse/XLSX.
- `make all`, `make docs-check`, `make cpi-monthly-v3-public-test` — зелёные.

## Task 1: спека, план, issue, worktree

**Files:** `docs/superpowers/specs/2026-10-19-cpi-v3-public-design.md`, `docs/superpowers/plans/2026-10-19-cpi-v3-public.md`, `docs/ROADMAP_KEY_RATE.md`.

- Создать worktree `.worktrees/cpi-v3-public`, ветку `feat/cpi-v3-public` от `origin/main`.
- Комментарий «Taking …» в #237; Project V2: `In Progress`, `Branch`, `Roadmap=key-rate`, `Phase=4`, `Item=phase-4-ml-components`.
- Зафиксировать в спеке окна, модели, гейты, decision tree **до** прогона.
- Коммит: `docs: add cpi v3 public model design and plan`.

## Task 2: загрузчик industry из #239

**Files:** `scripts/cpi_monthly_v3_public.py`, `scripts/cpi_monthly_v3_public_test.py`.

1. `CH_INDUSTRY_SLUGS` (16), `CH_INDICATORS` (3), `CH_INDUSTRY_SERIES` (48 имён).
2. `load_industry_panel_ch()`:
   - SQL: `SELECT series, date, value FROM v_cbr_survey WHERE toString(series) LIKE 'industry_%_ibk_sa' OR …`;
   - не алиасить агрегат как `series` (ClickHouse подхватывает алиас в WHERE);
   - проверить 48/48, по 297 точек, 14 256, `2002-01-01…2026-09-01`, уникальные даты, нет NULL;
   - построить `{sheet: {key: [(date, value)]}}` в форме Task 2.
3. `panel_from_xlsx(path)` — только для `--verify-xlsx`; вызвать `ci.load_industry_panel`.
4. Тесты: mapping completeness, 48 уникальных имён, missing series → ошибка, парсинг, as-of.

## Task 3: horizon-сборка

**Files:** `scripts/cpi_monthly_v3_public.py`.

1. `horizon_rows(rows, horizon)`:
   - h1 — исходные `Row` без изменений;
   - h>1 — `dataclasses.replace(row, y={code: cumulative})`, cumulative по h месяцам; обрезать последние `h−1` origin.
2. `horizon_window(start, embargo, h)` → `(start+h−1, embargo+h−1)`.
3. `assert_no_horizon_leakage(rows_h, h, start_eff, embargo_eff)`: train-origin `j` не имеет `j+h−1 ≥ i`.
4. Тесты: компаундирование h1/h2/h3, NaN, origin, purge, число фолдов.

## Task 4: признаки и модели

**Files:** `scripts/cpi_monthly_v3_public.py`.

1. `build_sample(sample)`:
   - `rows, skipped = cmv3.build_sample_v3(sample)`;
   - `panel = load_industry_panel_ch()`;
   - для каждого row — `ci.industry_features(panel, row.month, cm.cutoff_for(row.month))`;
   - `dataclasses.replace(row, features={**row.features, **extra})`.
2. `assert_no_industry_leakage(rows, panel)` — как в Task 2.
3. Наборы моделей:
   - `FULL_MODELS` (15): `v2_baseline`, `v2_bottomup`, `v3_direct`, `v3_bottomup`, `ind_direct`, `ind_bottomup`, `ind_only_bottomup`, `v3_ind_direct`, `v3_ind_bottomup`, `v3_ind_all_bottomup`, `B1`, `B2`, `B3`, `B3_weekly`, `B3_ind`;
   - `SECONDARY_MODELS` (9), `LONG_MODELS` (6), `LONG_WINDOW_MODELS` (4).
   - `B3 = cm.fit_enet_mp`, `B3_weekly = cmv3.fit_b3_weekly`, `B3_ind = ci.fit_b3_ind`.
4. Тесты: feature names, smoke CatBoost/ElasticNet/NNLS (малые iterations), bottom-up weights.

## Task 5: оценка, гейты, решение

**Files:** `scripts/cpi_monthly_v3_public.py`.

1. `evaluate_window(rows_h, models, horizon, start, embargo)`:
   - `cm.walk_forward(rows_h, models, start_eff, embargo_eff)`;
   - summary: n/RMSE/MAE/months;
   - pairs: primary 8 (2 кандидата × 4 бенчмарка) + secondary;
   - `eu.diebold_mariano(..., horizon=h)`, `eu.block_bootstrap_rmse_diff(..., 3, 2000, seed)`, MDE, Holm.
2. `stage1_gate`, `task2_gate` с допуском 1e-6.
3. `verify_xlsx(path)`: 48 SA-рядов, point-by-point max abs diff, sha256.
4. `decide(...)`: decision tree спеки §7.1; `last12` и `ex2022` диагностика.
5. Тесты: Holm, decision tree, «не хуже», industry-инкремент, гейты.

## Task 6: отчёт, JSON, MLflow, CLI

**Files:** `scripts/cpi_monthly_v3_public.py`.

1. CLI: `--sample`, `--horizons`, `--start`, `--embargo`, `--models`, `--out-dir`, `--verify-xlsx`, `--allow-gate-diff`.
2. `run_window(...)` → per-window report/JSON в `build/cpi-monthly-v3-public/<sample>__h<h>__embargo<e>/`.
3. `ML_LOG=1` — `mlx.start_run(experiment="cpi_monthly_v3_public", ...)`, metrics/pairs/gates/decision, `log_predictions(window=f"h{h}")`, artifacts; primary — weekly h1.
4. `run_all(...)` — все окна, итоговый `docs/research/2026-10-19-cpi-v3-public.md` + `.json`.
5. Отчёт: данные/сверка #239↔XLSX, окна/фолды, гейты, модель × окно, контрасты, h1/h2/h3/h6, DM/CI/MDE/Holm, last 12/ex-2022, «что не проверено», run_id/sha256, команды.

## Task 7: Makefile и документация

**Files:** `Makefile`, `README.md`, `docs/DEVELOPMENT.md`, `ARCHITECTURE.md`.

- `cpi-monthly-v3-public` / `cpi-monthly-v3-public-test` (+ `.PHONY`); зависимости `catboost>=1.2`, `scikit-learn`, `pandas`, `openpyxl`, `statsmodels>=0.15`, `scipy`, `--python 3.12`; `MLFLOW_PREFLIGHT`/`MLFLOW_ENV`.
- README — строка в таблице команд; DEVELOPMENT — детали/артефакты; ARCHITECTURE — новый ML-скрипт.
- `docs/ROADMAP_KEY_RATE.md` — абзац Task 4 после Task 3.

## Task 8: прогон и приёмка

```bash
make cpi-monthly-v3-public-test
make cpi-monthly-v3-public ARGS="--verify-only --verify-xlsx /Users/whitefox/GolandProjects/clickhouse-import-rosstat/.worktrees/cpi-public-data-audit/build/cpi-public-audit/mp_survey_data.xlsx"
ML_LOG=1 make cpi-monthly-v3-public ARGS="--all --allow-gate-diff"
make all
make docs-check
```

- Официальные числа — только полный прогон `--all`; smoke — `--models` и малые окна.
- Если Stage 1 gate расходится, но `ind_only_bottomup` совпадает с Task 2 (допуск 1e-6), это ревизия live weekly/aggregate MP (спека §8.1): текущие baseline фиксируются, официальный прогон идёт с `--allow-gate-diff`, `data_revision=documented`. Если `ind_only_bottomup` тоже расходится — стоп.
- Приёмка: h1 primary; h2/h3/h6 secondary; run_id/sha256; повтор детерминизма; CH↔XLSX exact.
- Записать результаты в отчёт/JSON, обновить роадмап.

## Task 9: PR

- `Closes #237` вне backticks; тело — пункт, источник #239, схема, окна, числа, вердикт, «что не проверено», команды, протокол.
- Project V2: `In Review`, `PR`, `Spec / Plan`; после мержа — `Done`.
- Не закрывать #76/#238.
- После мержа — синхронизация по `github-tracker` §10, если менялся роадмап.
