# План: CPI v3 h1/h3/h6 как прокси ожидаемой инфляции

**Пункт:** `key-rate/phase-4-cbr-scenario-fan`, Task 4.1 — issue [#296](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/296)
**Спека:** [2026-10-20-cpi-v3-expectations-design.md](../specs/2026-10-20-cpi-v3-expectations-design.md)

## Task 1 — CPI-прогнозы h1/h3/h6

**Files:** `scripts/cpi_v3_horizons.py`, `scripts/cpi_v3_horizons_test.py`.

- `load_rows()` — `cpi_monthly_v3.build_sample_v3("long")`; словарь по месяцам.
- `cumulative_target(rows_by_month, origin, horizon)` — compounded `cpi_all` m/m за `origin+1 … origin+h`; `None`, если хотя бы один месяц отсутствует.
- `fit_direct(train_origins, horizon)` — CatBoost на `feature_names_v3()`; target — `cumulative_target`; `month` категориальный; seed `cmv3.SEED`.
- `forecast_origins(rows_by_month, horizon, min_train=24)` — для каждого `O` train = origin'ы `M` с `M+h ≤ O−1`; прогноз строки `O`; `{O: value}`.
- `asof_forecast(meeting, origins, forecasts)` — последний `O` с `cm.cutoff_for(O) ≤ meeting`; `None`, если нет.
- `build_cpi_features(meetings, rows_by_month)` — `{meeting: {cpi_v3_h1,h3,h6,available,origin}}`.
- `assert_no_lookahead` — тест: для каждого train-origin `M+h ≤ O−1`.

**Тесты:** as-of (до/после 8-го), cumulative target, missing month, minimum train, детерминизм, отсутствие будущих месяцев в train.

## Task 2 — hook в модель ставки

**Files:** `scripts/key_rate_scenario_fan_v3.py`, `scripts/key_rate_scenario_fan_v3_test.py`.

- `build_channel_rows(rows, panel, external, extra_by_date=None)` — `features.update(extra_by_date.get(day, {}))`.
- `run_v3_walk_forward(..., external, extra_by_date=None)` — передаёт в `build_channel_rows`.
- Production-вызовы не меняются: `extra_by_date=None` даёт прежние 41 признак.
- Тест: extra-поля попадают в `ChannelRow.features`; baseline без extra не меняется.

## Task 3 — эксперимент

**Files:** `scripts/key_rate_cpi_v3_experiment.py`, `scripts/key_rate_cpi_v3_experiment_test.py`.

- Загрузка ставки — как в `key_rate_scenario_fan_v3.run_summary` (импорт функций v3).
- `build_extra(meetings, cpi_features, variant)`:
  - `baseline` — `{}` (не добавлять);
  - `h1` — `cpi_v3_h1` + `cpi_v3_available`;
  - `h1h3h6` — все три + `cpi_v3_available` (primary);
  - `replace_survey` — все три + `cpi_v3_available`, а `infl_exp_expected/observed`, `price_expectations` и их `_chg` обнуляются в `extra_by_date` (None).
- Для каждого варианта — `v3.run_v3_walk_forward(...)`; summary/compare.
- Primary: `channel_fan_vol` expanding, `augmented vs baseline` paired.
- Артефакты: `key_rate_cpi_v3_experiment_report.md`, `..._result.json`, `..._folds.csv`; `ML_LOG=1` — run `key_rate_cpi_v3_expectations`.
- CLI: `--out-dir`, `--variant` (для отладки), `--quick` (только h1; в паспорт `quick=true`).

**Тесты:** фиктивные fans/сравнения; варианты extra; `replace_survey` действительно убирает survey-поля; отсутствие CPI → `available=0`.

## Task 4 — живой прогон

```bash
make key-rate-cpi-v3-experiment-test
ML_LOG=1 MLFLOW=0 make key-rate-cpi-v3-experiment
make key-rate-cpi-v3-experiment ARGS="--variant baseline"
```

- Ожидание: 108 решений, baseline 72/56 фолдов; CPI-варианты — те же фолды с missing-индикатором early.
- Проверить: h1/h3/h6 не None после 2016; `available` 0 до 2016; baseline числа совпадают с Task 4 (CRPS 48.215 / coverage 0.778).

## Task 5 — отчёт и docs

- `docs/research/2026-10-20-cpi-v3-expectations.md`: гипотеза, as-of, таблицы baseline/+h1/+h1h3h6/replace, ΔCRPS/CI/DM, эпизоды, вердикт, ограничения.
- Обновить `docs/KEY_RATE_MODEL.md`, `docs/KEY_RATE_CHANNELS.md` (канал ожиданий), `docs/LIMITATIONS.md`, `docs/ROADMAP_KEY_RATE.md`, `docs/site-pages.json`.
- `make all`, `make docs-check`; PR `Closes #296`, `Part of #272`.

## Границы

- Не менять `cpi_monthly_v3` и его h1-результаты.
- Не писать в `forecast_log`; Go/DDL/витрины/гранты не трогать.
- Не менять primary Task 4 без отдельного решения; эксперимент — отдельный PR.
