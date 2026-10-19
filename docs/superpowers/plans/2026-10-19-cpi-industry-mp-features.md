# План: отраслевые МП-признаки для компонентного nowcast ИПЦ (issue #235)

**Пункт:** `key-rate/phase-4-ml-components`, Task 2 (родитель [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76)).
**Issue:** [#235](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/235), `role:ml`.
**Спека:** [2026-10-19-cpi-industry-mp-features-design.md](../specs/2026-10-19-cpi-industry-mp-features-design.md).
**Ветка / worktree:** `feat/cpi-industry-mp` / `.worktrees/cpi-industry-mp`.
**Зависимость:** Task 1 [#234](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/234) — закрыт, PR [#244](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/244) смержен.

## Global Constraints

- Только чтение ClickHouse; в БД данные не пишем; DDL/GRANT/Go-код не меняем.
- `cpi_monthly.py` и `cpi_monthly_v3.py` не меняются; новые функции — в `cpi_monthly_industry.py`.
- Walk-forward только расширяющимся окном; random split запрещён; CatBoost-дефолты, `random_seed=20261018`.
- As-of отраслевой точки — конец `P+1` (как агрегатный МП); точка `M` не участвует.
- NaN ≠ 0; CatBoost нативно, ElasticNet — медиана train.
- Один PR в конце; спека и план — первые коммиты.

## Task 1: спека, план, issue

**Files:** `docs/superpowers/specs/2026-10-19-cpi-industry-mp-features-design.md`, `docs/superpowers/plans/2026-10-19-cpi-industry-mp-features.md`.

- Зафиксировать дизайн, prior-mapping, выборки, primary-контрасты и правило решения.
- Issue #235: `Taking …`, Project V2 `In Progress`, `Branch`; `Spec / Plan` — после коммита.
- Коммит: `docs: add industry MP features design and plan`.

## Task 2: value-loader и признаки

**Files:** `scripts/cpi_monthly_industry.py`.

1. Импорт `cpi_monthly as cm`, `cpi_monthly_v3 as cmv3`, `eval_utils as eu`, `ml_experiment as mlx`; из `cpi_public_panel_audit` — `INDUSTRY_SHEETS`, `MP_INDICATORS`, `MP_URL`, `resolve_mp_survey`.
2. `load_industry_panel(path, sheets=INDUSTRY_SHEETS)`:
   - `openpyxl.load_workbook(read_only=True, data_only=True)`;
   - маркеры блоков — row 3, заголовки — row 4, данные — с row 5; дата — колонка A (`datetime`/`date`/Excel-serial);
   - колонки IBK / ценовых ожиданий / издержек — по needles из `MP_INDICATORS`, SA-блок;
   - fail-fast: нет листа/маркера/колонки — `ValueError` с именем листа.
3. `INDUSTRY_SLUGS`, `COMPONENT_SHEETS`, `GENERAL_SHEETS`, `industry_feature_names(sheets)` — 96 имён.
4. `industry_features(panel, month, cutoff)` — уровень `M−1`, изменение `M−1 − M−2`, as-of `cm.available_from_survey`; отсутствие → NaN.
5. `build_sample_industry(sample)` — `cmv3.build_sample_v3(sample)` + industry-признаки; вернуть rows/skipped и метаданные панели (sha256, min/max).
6. `component_sheets(component)`, `feature_names(...)`, `assert_no_industry_leakage(...)`.
7. Тесты парсера/as-of/NaN — в Task 4.

## Task 3: модели, оценка, паспорт

**Files:** `scripts/cpi_monthly_industry.py`.

1. `fit_catboost_industry(...)` — CatBoost на выбранном наборе, `month` категориальным, NaN нативно.
2. `fit_bottomup_industry(...)` — 3 компонента + `cm.nnls_weights(train)`; `last_weights`/`last_components`.
3. `fit_enet_industry(...)` — ElasticNetCV, медиана train, all-NaN колонка → 0 после стандартизации.
4. `MODELS`: `v2_baseline`, `v2_bottomup`, `v3_direct`, `v3_bottomup`, `ind_direct`, `ind_bottomup`, `ind_only_bottomup`, `v3_ind_direct`, `v3_ind_bottomup`, `v3_ind_all_bottomup`, `B1`, `B2`, `B3`, `B3_weekly`, `B3_ind`.
5. `evaluate(...)` — RMSE/MAE, DM-HLN, блочный бутстрап (3, 2000), MDE, primary-пары.
6. `run(...)` — артефакты `build/cpi-monthly-industry/<sample>/cpi_monthly_industry_{report.md,result.json}`; `ML_LOG=1` → `mlx.start_run(experiment="cpi_monthly_industry", ...)`; JSON с `mp_survey`, `mapping`, `regression_gate`, `summary`, `pairs`, `decision`.
7. CLI: `--sample long|weekly`, `--start`, `--embargo`, `--models`, `--mp-survey`, `--out-dir`, `--allow-data-revision`.

## Task 4: офлайн-тесты

**Files:** `scripts/cpi_monthly_industry_test.py`.

1. Синтетическая книга (2 листа, геометрия публичной) — парсер находит три индикатора; нет листа/колонки — `ValueError`.
2. As-of: `M−1`/`M−2`, точка `M` не участвует, `available_from > cutoff` игнорируется.
3. NaN: отсутствующая точка/лист → NaN, не 0; строка не выбрасывается.
4. Имена: 96 уникальных, `month` последним; mapping покрывает все 16 листов; general в каждом компоненте; `all_sheets` — 96.
5. ElasticNet: медиана train, all-NaN колонка; CatBoost smoke (5 итераций).
6. Bottom-up smoke: NNLS + 3 компонента, веса суммируются к 1.
7. Старые наборы `cpi_monthly`/`cpi_monthly_v3` не меняются.

## Task 5: Makefile и live-прогоны

**Files:** `Makefile` (`.PHONY`, цели после `cpi-monthly-v3`).

```bash
make cpi-monthly-industry-test
ML_LOG=1 make cpi-monthly-industry ARGS="--sample long --start 48 --models v2_bottomup,v3_bottomup,ind_only_bottomup,v3_ind_bottomup,B1"
ML_LOG=1 make cpi-monthly-industry ARGS="--sample weekly --start 24"
```

- `uv run` с `--with openpyxl`; `MLFLOW_PREFLIGHT` и `MLFLOW_ENV` — как у `cpi-monthly-v3`.
- Полные прогоны — официальные числа; сокращённый набор моделей — только для проверки механики.
- `long` — сокращённый primary-набор (direct, `ind_bottomup`, `v3_ind_all_bottomup` — только `weekly`).
- Артефакты пишутся до проверки гейта; при расхождении Stage 1 паспорт разбирается, baseline пересчитывается текущим кодом, `--allow-data-revision` фиксирует это в JSON/отчёте.
- `ML_LOG=1` пишет только паспорт/предсказания; таблицы данных, DDL и `forecast_log` не трогаются.

## Task 6: отчёт и docs

**Files:**
- `docs/research/2026-10-19-cpi-industry-mp-features.md` — числа, DM/CI/MDE, вердикт, run_id, ограничения.
- `docs/ROADMAP_KEY_RATE.md` — Stage 2 в абзац `phase-4-ml-components`; пункт остаётся `⏳`.
- `README.md` — строка `make cpi-monthly-industry` / `-test`.
- `docs/DEVELOPMENT.md` — цель, `openpyxl`, выборки, тесты.
- `ARCHITECTURE.md` — `cpi_monthly_industry.py` в строке `scripts/`.

## Task 7: приёмка и PR

```bash
make cpi-monthly-industry-test
make all
make docs-check
```

- Регрессионный гейт Stage 1 — воспроизведён или расхождение объяснено.
- PR: `Closes #235` вне backticks; протокол приёмки — комментарием в PR.
- Project V2: `In Review` при открытии PR, `Done` — после мержа и протокола.
