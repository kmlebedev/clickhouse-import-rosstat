# План: компонентный nowcast ИПЦ на публичных данных — Stage 1

**Дата:** 19 октября 2026 г.
**Пункт:** `key-rate/phase-4-ml-components` (`docs/ROADMAP_KEY_RATE.md`, фаза 4).
**Issue:** [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76).
**Спека:** [2026-10-19-cpi-components-public-data-design.md](../specs/2026-10-19-cpi-components-public-data-design.md).
**Ветка:** `feat/key-rate-ml-components`.
**Worktree:** `.worktrees/key-rate-ml-components`.

## Global Constraints

- Только чтение ClickHouse; в БД не пишем; DDL/GRANT/Go-код не меняем.
- `scripts/cpi_monthly.py` и `scripts/cpi_nowcast.py` не меняются.
- Walk-forward только расширяющимся окном; никакого random split; start 24, embargo 0 (основное) и 1 (робастность).
- CatBoost — дефолты, `random_seed=20261018`; никакого тюнинга по test.
- As-of: недельная точка участвует, только если `date + 2 ≤ cutoff(M)`.
- Тесты — stdlib `unittest`, офлайн, без сети/БД; существующие 75 тестов остаются зелёными.
- Один PR в конце; спека и план — первые коммиты.

## Task 1: спека, план, пункт роадмапа

**Files:** `docs/superpowers/specs/2026-10-19-cpi-components-public-data-design.md`, `docs/superpowers/plans/2026-10-19-cpi-components-public-data.md`, `docs/ROADMAP_KEY_RATE.md`.

- Добавить пункт `phase-4-ml-components` со ссылкой на #76.
- Обновить issue #76: заголовок `[key-rate/phase-4-ml-components]`, labels `roadmap:key-rate`/`role:ml`, Project V2.
- Коммит: `docs: add public-data CPI components design and plan`.

## Task 2: загрузчики и недельные признаки

**Files:** `scripts/cpi_monthly_v3.py`, `scripts/cpi_monthly_v3_test.py`.

1. `WEEKLY_SQL` — `SELECT series, date, value FROM v_rosstat_macro WHERE series IN (SELECT series FROM v_series_catalog WHERE source = 'rosstat' AND frequency = 'W') ORDER BY date, series FORMAT JSON`.
2. `weekly_from_rows(rows)` — `{series: [(date, value)]}`, исключить агрегат `Индекс потребительских цен (недельная оценка)`.
3. `available_from_weekly(day) = day + 2`; `eligible_weeks(weekly, cutoff)` — группировка по дате, фильтр as-of.
4. `weekly_features(weekly, cutoff)` — до 4 последних доступных недель:
   - `wk_share_rising`, `wk_median`, `wk_median4`, `wk_gasoline4` (NaN, если бензина нет).
5. `WEEKLY_FEATURES` — порядок имён; `feature_names_v3()` = `cm.feature_names(with_mp=True)` + `WEEKLY_FEATURES`.
6. Тесты:
   - формула на синтетических неделях; компаундирование не нужно для v1-формулы (используются `percent − 100`);
   - as-of: точка `date + 2 > cutoff` не участвует;
   - ≤4 недель, NaN бензина, пустое окно;
   - `WEEKLY_FEATURES` имена и порядок.

## Task 3: выборка v3

**Files:** `scripts/cpi_monthly_v3.py`.

1. `build_sample_v3(sample="weekly"|"long")`:
   - `targets = cm.load_targets()`, `survey = cm.load_survey()`, `usd = cm.load_usd()`, `key_rate = cm.load_key_rate()`, `fao = cm.load_fao()`, `weekly = load_weekly()`.
   - Для каждого месяца M в окне: `cm.base_features` + `cm.mp_features` (v2) + `weekly_features`; `y` — 4 таргета.
   - `weekly`: 2022-01…2026-08; `long`: 2013-10…2026-08.
   - Пропуск месяца только если нет обязательных v2-признаков/таргета; недельные NaN не пропускают.
2. `assert_no_leakage_v3(rows)` — cutoff(M) < доступность собственной метки; недельная точка as-of.
3. Тесты на синтетике: окна, NaN, отсутствие утечки.

## Task 4: модели

**Files:** `scripts/cpi_monthly_v3.py`.

1. `fit_catboost_v3(train, row, target, with_weekly)` — CatBoost на `feature_names_v3()`; `month` категориальным.
2. `fit_catboost_bottomup_v3` — 3 компонентных CatBoost + NNLS (как `cm.fit_catboost_bottomup`), с недельными признаками.
3. `fit_enet_v3(train, row, with_weekly)` — ElasticNetCV на стандартизованных признаках, импутация медианой train; `fit_ar1`, `predict_last` — из `cm`.
4. `MODELS`:
   - `v2_baseline` — direct на `cm.feature_names(with_mp=True)`;
   - `v3_direct` — direct + weekly;
   - `v3_bottomup` — bottom-up + weekly;
   - `B1`, `B2`, `B3` (v2), `B3_weekly`.
5. Регрессионный гейт: `v2_baseline` на `weekly`/`long` должен совпасть с `cm` direct на тех же фолдах (в пределах округления); иначе стоп.
6. Тесты: smoke CatBoost (5 итераций), NNLS, имена признаков.

## Task 5: оценка и отчёт

**Files:** `scripts/cpi_monthly_v3.py`, `docs/research/2026-10-19-cpi-components-public-data.md`.

1. `evaluate_windows` — `cm.walk_forward` для start 24, embargo 0/1; RMSE/MAE, DM-HLN, блочный бутстрап, MDE.
2. Primary-контрасты: `v3_direct` vs `v2_baseline`, `v3_direct` vs `B3_weekly` на `weekly` h1.
3. Secondary: h2/h3/h6, `long`, `v3_bottomup`.
4. `ML_LOG=1` — паспорт run (`cb_comp_parity`-подобный формат; `ml_experiment`).
5. Отчёт: аудит (149/118/39,3 %), выборки, таблицы, вердикт, «что не проверено».
6. Обновить roadmap/Project; протокол — комментарием в PR.

## Task 6: прогон и приёмка

```bash
make env-check
make ch-status
ML_LOG=1 make cpi-monthly-v3 ARGS="--sample weekly"
ML_LOG=1 make cpi-monthly-v3 ARGS="--sample long"
make cpi-monthly-v3-test
make all
make docs-check
```

- Официальные числа — только полный прогон; `--quick` не используется.
- Приёмка: `v2_baseline` воспроизводит v2 direct; h1/CI/MDE; паспорт; отчёт; roadmap/Project.

## Task 7: PR

- `Closes #76` вне backticks; описание — пункт, источник, схема, запуск, числа, «что не проверено».
- После мержа: `Status = Done`, статус роадмапа `✅` с датой и числами.
