# План: H2O AutoML как генератор challenger'а для CPI v4

**Пункт:** `key-rate/phase-4-cpi-h2o-automl` ([#502](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/502))
**Спека:** [2026-11-01-cpi-h2o-automl-design.md](../specs/2026-11-01-cpi-h2o-automl-design.md)
**Ветка:** `exp/cpi-h2o-automl`

Порядок: спека/план/роадмап → окружение → код и тесты → селектор → refit → gate → отчёт → PR.

## Task 0. Процесс

- [x] Issue #502, label `role:ml`, `roadmap:key-rate`, комментарий «Taking …».
- [x] Пункт `phase-4-cpi-h2o-automl` в `docs/ROADMAP_KEY_RATE.md` (In Progress).
- [x] Спека и этот план.
- Коммит 1: `docs: add cpi h2o automl spec and plan (#502)`.

## Task 1. Окружение

- [x] `brew install openjdk@17`; `JAVA_HOME=/opt/homebrew/opt/openjdk@17`, `java -version` → 17.0.21.
- Makefile: `JAVA_HOME ?= $(shell brew --prefix openjdk@17)`, `PATH` с `$JAVA_HOME/bin`, проверка версии 8–17 до запуска H2O.
- Пин `h2o==3.46.0.12` в `uv run --with` (sdist ~266 МБ, первый запуск дольше).

## Task 2. Код

`scripts/models/cpi/cpi_v4_h2o.py`:

1. `--stage select`: кадр `O ≤ 2015-12 − h` из `build_origin_rows` + `feature_override`; `H2OFrame` с `month` enum и NaN как NA; `fold_column` из 3 временных блоков; `H2OAutoML` по §3 спеки; запись `select.json`; второй seed.
2. `--stage refit`: чтение frozen-config; `fit_predict`-обёртки `h2o_bottomup`/`h2o_direct`; прогон через `run_walk_forward`; `evaluate_cell` против `v3_bottomup` (+ `v2_bottomup` отдельным `compare_pair`); `cell_*.json`/`predictions.csv`; паспорт `ML_LOG=1` (`cpi_v4_h2o`).
3. `--stage gate`: чтение ячеек, условия §5 спеки, `build/ml-gate/cpi_v4_h2o.{json,md}`.
4. Ленивый `import h2o`; кластер в `try/finally`; `ice_root`/`log_dir` в `build/cpi-v4-h2o/tmp`; `nthreads=1`.

`scripts/models/cpi/cpi_v4_h2o_test.py` — офлайн: кадр и типы, NaN≠0, `month` enum, `fold_column` монотонен, cut-off селектора, whitelist параметров, frozen-config round-trip, отсутствие look-ahead; H2O-смоук `skipUnless`.

`Makefile`: `cpi-v4-h2o`, `cpi-v4-h2o-test`, `MLFLOW_PREFLIGHT`.

## Task 3. Селектор

```
make cpi-v4-h2o ARGS="--stage select"
```

Проверка: `select.json` с семейством, параметрами, версиями, sha256 кадра; второй seed — то же семейство либо `selection_stable=false`.

**Факт 2026-11-01:** кадр 78 origin'ов (2009-06…2015-11); seed `20261018` → GBM (CV 0,5110), seed `20261019` → DRF (CV 0,5090), `selection_stable=false`; заморожен DRF `DRF_1` (`min_rows=1`, `ntrees=23`). XGBoost в H2O OSS 3.46 недоступен.

## Task 4. Refit

```
make cpi-v4-h2o ARGS="--stage refit --sample S_L09 --horizons 1 --quick"
ML_LOG=1 make cpi-v4-h2o ARGS="--stage refit --sample S_L09 --horizons 1,3,6"
make cpi-v4-h2o ARGS="--stage refit --sample S_L13 --horizons 1,3"
make cpi-v4-h2o ARGS="--stage refit --sample S_W22 --horizons 1,3"
```

Повтор full-прогона `S_L09` — сверка sha256 `predictions.csv`, при расхождении — max |Δpred| в отчёт.

## Task 5. Gate и отчёт

```
make cpi-v4-h2o ARGS="--stage gate"
```

- `build/ml-gate/cpi_v4_h2o.{json,md}`: решение и причина по §5.
- `docs/research/2026-11-01-cpi-h2o-automl.md` + JSON (копия `select.json`, числа, DM/CI/MDE, robustness, «что не проверено»).
- `ML_LOG=1` → паспорт; при недоступном ClickHouse — `make mlflow-export`.
- Обновить пункт роадмапа (`In Review`), `make all`, `make docs-check`.

## Task 6. PR

Один PR, `Closes #502` вне backticks; протокол приёмки — комментарием (числа, команды, что не проверено).

## Стоп-условия

- `make cpi-v4-h2o-test` красный.
- Селектор видит origin'ы после 2015-12 (тест).
- Java вне 8–17 или H2O не поднимается — фиксируем и не обходим «временным» решением.
- Gate не выполняется из-за отсутствующих ячеек → `incomplete`, а не подмена окна.
