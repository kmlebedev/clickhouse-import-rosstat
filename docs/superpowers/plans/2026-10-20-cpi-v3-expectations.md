# План: CPI live3 h1/h3/h6 как прокси ожидаемой инфляции

**Пункт:** `key-rate/phase-4-cbr-scenario-fan`, Task 4.1 — issue [#296](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/296)
**Спека:** [2026-10-20-cpi-v3-expectations-design.md](../specs/2026-10-20-cpi-v3-expectations-design.md)
**Модель-источник:** `scripts/cb_comp_live_compare.py` — live3 (3 компонента + v3-признаки + NNLS)

## Принцип

`cb_comp_live_compare` — диагностика с фиксированным train `2010-03…2018-12` и тестом `2019-01…2024-07`. Для модели ставки это look-ahead и обрезанное покрытие. Поэтому берём **модель** live3 и оборачиваем её в walk-forward с as-of по дате заседания; готовые числа отчёта не используем.

## Task 1 — walk-forward live3 h1/h3/h6

**Files:** `scripts/cpi_live3_horizons.py`, `scripts/cpi_live3_horizons_test.py`.

- `load_rows(as_of)` — feature-вселенная: все месяцы `2013-10 … latest_origin(as_of)`, где `latest_origin` — последний `O` с `cm.cutoff_for(O) ≤ as_of`; `y` заполняется только при полном наборе четырёх меток, иначе `{}`. Для feature-only origin — `cm.base_features`/`cm.mp_features`/`v3.weekly_features` на `cutoff_for(month)`.
- `targets(rows_by_month)` — для h=1/3/6 накопленный m/m за `M−h+1…M`; ключ — целевой месяц `M = O+h`; строки без полного набора компонент пропускаются.
- `training_origins(months, origin, h)` — `O' ≤ O−h−1` (целевой конец `O'+h ≤ O−1`).
- `forecast_origin(rows_by_month, targets, origin, h)`:
  1. train = `[(rows_by_month[O'], targets[O'+h])]`;
  2. `fit_component(train, code)` для `cm.BOTTOMUP_COMPONENTS`;
  3. `predict` на `rows_by_month[origin]`;
  4. `cm.nnls_weights(train)` → взвешенный агрегат.
- `asof_origin(meeting, origins)` — последний `O` с `cm.cutoff_for(O) ≤ meeting` по feature-вселенной.
- `build_cpi_features(meetings, rows_by_month)` — `{meeting: {cpi_v3_h1,h3,h6,available,origin}}`; считает только нужные origin'ы; `available=1` только если все три не `None`.
- `assert_no_lookahead` — для каждого train-origin `O'`: `O'+h ≤ O−1`; origin не позже цели.
- `main` — `--as-of` (по умолчанию сегодня), `--meeting` (опционально), печать покрытия/последних origin'ов; `--out` JSON.

**Тесты:** no-lookahead (`O' ≤ O−h−1`; h=1 → O−2, h=3 → O−4), target convention (`O+1…O+h`), feature-only origin не в train, но в predict, as-of до/после 8-го, missing origin, deterministic aggregate, минимум 24 train-origin'а.

**Удалить/не коммитить:** прототип `scripts/cpi_v3_horizons.py` и его тест (direct-CatBoost на агрегате) — заменён live3; в PR его не включать.

## Task 2 — hook в модель ставки

**Files:** `scripts/key_rate_scenario_fan_v3.py`, `scripts/key_rate_scenario_fan_v3_test.py`.

- `build_channel_rows(rows, panel, external, extra_by_date=None)` — `features.update(extra_by_date.get(day, {}))`.
- `run_v3_walk_forward(..., external, extra_by_date=None)` — передаёт в `build_channel_rows`.
- Production-вызовы не меняются: `extra_by_date=None` даёт прежние 41 признак.
- Тесты: extra-поля попадают в `ChannelRow.features`; baseline без extra не меняется.

## Task 3 — эксперимент

**Files:** `scripts/key_rate_cpi_v3_experiment.py`, `scripts/key_rate_cpi_v3_experiment_test.py`.

- Загрузка ставки — как в `key_rate_scenario_fan_v3.run_summary`.
- CPI-признаки — `cpi_live3_horizons.build_cpi_features` (не `cpi_v3_horizons`).
- `build_extra(..., variant)`:
  - `baseline` — `None`;
  - `h1` — `cpi_v3_h1` + `cpi_v3_available`;
  - `h1h3h6` — все три + `cpi_v3_available` (primary);
  - `replace_survey` — все три + `cpi_v3_available`, survey-поля и их `_chg` = `None`, `has_expectations=0`.
- Для каждого варианта — `v3.run_v3_walk_forward(...)`; summary/compare.
- Primary: `channel_fan_vol` expanding, paired `h1h3h6` vs `baseline`.
- Sanity/ranking CPI на тех же origin'ах: `live3_bottomup` (primary), `direct_catboost`, `B1_last` (`targets[h][O−1]` — последнее наблюдённое h-окно), `B3_enet`; RMSE/MAE h1/h3/h6, DM-HLN и бутстрап для пар с общими фолдами. Ranking — диагностика, primary не переключается.
- Артефакты: `key_rate_cpi_v3_experiment_report.md`, `..._result.json`, `..._folds.csv`, `..._cpi_predictions.csv`; `ML_LOG=1` — run `key_rate_cpi_v3_expectations`.
- `_log_run`: `cpi_model=live3 bottom-up`, `cpi_target_window=O+1..O+h`, `cpi_train_rule=O'<=O-h-1`, `cpi_feature_last_origin`; `log_predictions` для primary-веера и CPI sanity.
- CLI: `--out-dir`, `--variants`, `--as-of`.

**Тесты:** варианты extra; `replace_survey` убирает survey; `decide`; отчёт содержит ranking; missing CPI → `available=0`.

## Task 4 — живой прогон

```bash
PYTHONDONTWRITEBYTECODE=1 make cpi-live3-horizons-test
PYTHONDONTWRITEBYTECODE=1 make key-rate-cpi-v3-experiment-test
ML_LOG=1 MLFLOW=0 make key-rate-cpi-v3-experiment
```

- Ожидание: 108 решений, baseline 72/56 фолдов; CPI-варианты — те же фолды с missing-индикатором early.
- Проверить: h1/h3/h6 не None после первого train-origin (~2016); `available` 0 до него; baseline CRPS 48.215 / coverage 0.778 совпадает с Task 4.
- Feature-only origin: для заседания 2026-09-11 origin 2026-08; для 2026-10-23 — origin 2026-09 (h1 — октябрь). Live-диагностика — `scripts/cpi_live3_horizons.py --as-of 2026-10-23 --out build/cpi-v3-expectations/live_2026-10-23.json`.
- Sanity/ranking CPI: h1/h3/h6 RMSE/MAE live3/direct/B1/B3 на общих origin'ах; если live3 хуже naive `last` — отметить и не делать вывод по ставке.

## Task 5 — отчёт и docs

- `docs/research/2026-10-20-cpi-v3-expectations.md`: гипотеза, as-of, live3-модель, таблицы baseline/+h1/+h1h3h6/replace, ΔCRPS/CI/DM, эпизоды, CPI sanity, вердикт, ограничения.
- Обновить `docs/KEY_RATE_MODEL.md`, `docs/KEY_RATE_CHANNELS.md` (канал ожиданий), `docs/LIMITATIONS.md`, `docs/ROADMAP_KEY_RATE.md`, `docs/site-pages.json`.
- `make all`, `make docs-check`; PR `Closes #296`, `Part of #272`.

## Границы

- Не менять `cb_comp_live_compare` и его диагностический отчёт; использовать только `fit_component`/`predict`/`cumulative_targets`/`nnls_weights`.
- Не менять `cpi_monthly_v3` и его h1-результаты.
- Не писать в `forecast_log`; Go/DDL/витрины/гранты не трогать.
- Не менять primary Task 4 без отдельного решения; эксперимент — отдельный PR.
