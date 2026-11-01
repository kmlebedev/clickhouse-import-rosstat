# План: Task 17 — CatBoost vs LightGBM/XGBoost для `combo3`

> **Пункт:** `key-rate/phase-4-cb-comp-88-groups-live` (#419), Task 17 ([#501](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/501)). Спека — [2026-11-01-cb-comp-backend-compare-design.md](../specs/2026-11-01-cb-comp-backend-compare-design.md). Ветка `exp/cb-comp-88-groups-backends`, worktree `.worktrees/cb-comp-88-backends`.

## 0. Предпосылки

- `origin/main` содержит P0.5 (#485): `scripts/models/cb_comp/cb_comp_mlog.py`, experiment `cb_comp_88_groups`.
- ClickHouse поднят владельцем (`make ch-status`); архив CB_COMP доступен (`CB_COMP_ARCHIVE`/`--archive`); `build/cb-comp-groups/panel.csv` и `build/cb-comp-parity/{public_panel,sheet_map}.csv` есть или собираются целями `cb-comp-groups-panel`/`cb-comp-public-export`.
- Версии фиксируются: catboost 1.2.10, lightgbm 4.7.0, xgboost 3.4.1.

## 1. Пункт и трекер — ✅ сделано

- Sub-issue #419 → [#501](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/501), labels `roadmap:key-rate`, `role:ml`; Project V2 «clickhouse-import-rosstat — Roadmap» → `In Progress`, поля заполнены.
- `docs/ROADMAP_KEY_RATE.md`: Task 17 добавлен в пункт #419.

## 2. Спека и план — этот коммит

Файлы: `docs/superpowers/specs/2026-11-01-cb-comp-backend-compare-design.md`, `docs/superpowers/plans/2026-11-01-cb-comp-backend-compare.md`, правка роадмапа. Проверка: `make docs-check` (после кода — полный прогон).

## 3. Слой бэкендов

Файл: `scripts/models/cb_comp/cb_comp_backend_compare.py`.

1. `feature_frame(rows, names)` → `pandas.DataFrame`: числовые колонки `float64`, `month` — `CategoricalDtype(categories=range(1, 13))`; NaN сохраняются.
2. Классы `CatBoostBackend`, `LightGBMBackend`, `XGBoostBackend`: `name`, `version()`, `params(arm)`, `fit(train_rows, names, horizon)` → model, `predict(model, rows, names)` → list[float], `trees(model)`.
   - CatBoost — обёртка `bu.fit_component`/`bu.predict_component` (нулевое изменение поведения).
   - LightGBM — `LGBMRegressor` на `feature_frame`, `categorical_feature=["month"]`.
   - XGBoost — `XGBRegressor` на `feature_frame`, `enable_categorical=True`, `tree_method="hist"`.
3. Реестр `BACKENDS = {"catboost": ..., "lightgbm": ..., "xgboost": ...}` и `ARMS = {"defaults": ..., "matched": ...}`; допустимые пары — `RUNS = [(catboost, defaults), (lightgbm, defaults), (lightgbm, matched), (xgboost, defaults), (xgboost, matched)]`.

Проверка: юнит-тесты на dtypes/категорию/детерминизм (шаг 6).

## 4. Хук в `cb_comp_live_bottomup.fit_predict`

Файл: `scripts/models/cb_comp/cb_comp_live_bottomup.py`.

- Добавить параметр `regressor=None`; при `None` — текущие `bu.fit_component`/`bu.predict_component`, иначе `regressor.fit(...)`/`regressor.predict(...)`. Сигнатуры не меняются.
- Проверка: `make cb-comp-live-bottomup-test` — пины Task 3 не сдвинулись; R-B1 в новом скрипте.

## 5. Оценка, вердикт, отчёт

Файл тот же (`cb_comp_backend_compare.py`).

1. `run()`: `en.prepare` один раз; для каждой пары — `lb.fit_predict(..., regressor=backend)`; `lb.aggregate_predictions`; `combo3_<backend>` через `hc.bs_predict`/`hc.combine_mean` (OLS один раз).
2. `evaluate()`: `w5b.compare_to_cb` (ratio/class), `v4.compare_pair` (Δ/CI/DM/MDE), `v4.mcs_included` по 4 моделям, RMSE на `T_LIVE`/all67.
3. `decide()` — чистая функция по правилу спеки §6; возвращает `{backend: {decision, reasons}}`.
4. Инварианты R-B1/R-B2/R-B3 (спека §7); при R-B1/R-B3 fail — `SystemExit`; R-B2 — finding.
5. Артефакты: `build/cb-comp-backend-compare[-<version>]/{result.json,predictions.csv,timings.csv,report.md}`; копии JSON/MD — `docs/research/`.
6. Паспорт: `dc.log_run` на каждый run (5), experiment `cb_comp_88_groups`, `models=[live_cb_<backend>, combo3_<backend>]`, config/sample/metrics/artifacts по спеке §5; tags `stage=backtest`, `champion=catboost`, `challenger=<backend>`, `decision`.

Проверка: `ML_LOG=0` прогон даёт те же файлы, что `ML_LOG=1` (сравнение sha256).

## 6. Тесты

Файл: `scripts/models/cb_comp/cb_comp_backend_compare_test.py` (офлайн, без ClickHouse/архива).

- `feature_frame`: dtypes, `CategoricalDtype(1…12)`, NaN.
- Каждый бэкенд: fit/predict на 40 синтетических строках, конечные значения, повтор с тем же seed — те же прогнозы.
- `evaluate`/`compare_pair`/`mcs_included` на известных массивах; `decide()` — все три ветки + приоритет.
- `timings`-агрегаты (`mean/median/p95`).
- `mlog`-kwargs: имена run'ов, одинаковый `sample_hash` между бэкендами, config содержит backend/arm/version.
- R-B1: `fit_predict(..., regressor=CatBoostBackend())` == `lb.fit_predict(...)` на синтетике.

## 7. Makefile

- `cb-comp-backend-compare`: `@$(MLFLOW_PREFLIGHT)`, `$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "catboost==1.2.10" --with "lightgbm==4.7.0" --with "xgboost==3.4.1" --with scikit-learn --with pandas --with openpyxl --with "statsmodels>=0.15" --with scipy --with arch --with "holidays>=0.60" --python 3.12 python3 scripts/models/cb_comp/cb_comp_backend_compare.py $(ARGS)`.
- `cb-comp-backend-compare-test`: те же пакеты без mlflow, `-v`.
- Обе цели — в `.PHONY`.

## 8. Офлайн-проверки

```bash
make cb-comp-backend-compare-test
make cb-comp-live-bottomup-test
make cb-comp-h1-bs-combo-test
```

## 9. Боевой прогон и MLflow

1. `make ch-status`, `make cb-comp-public-export` (если нет `public_panel.csv`/`sheet_map.csv`), `make mlflow-preflight` (при необходимости `make mlflow-up`).
2. `ML_LOG=1 MLFLOW=1 make cb-comp-backend-compare` → 5 run'ов; зафиксировать `run_id` и URI.
3. Повтор R-B3: `ML_LOG=1 MLFLOW=1 make cb-comp-backend-compare ARGS="--backend lightgbm --arm defaults"` → те же sha256 файлов, новый `run_id`.
4. Витрины (MCP `run_query`): `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`; сверка RMSE с `result.json`.
5. `make mlflow-export ARGS="--experiment cb_comp_88_groups --dry-run"`.

## 10. Отчёт и документация

- `docs/research/2026-11-01-cb-comp-backend-compare.md` + `.json`: протокол, версии, инварианты, таблицы точности (h1/h2/h3/h6, класс, DM p, CI) и скорости, MCS, вердикт, «что не проверено», run_id/URI, sha256, воспроизведение.
- `docs/guides/cb-comp-live-model-map.md`: факт 9, реестр исследований, ledger.
- `README.md` — строка `make cb-comp-backend-compare`; `docs/site-pages.json` — спека/план/отчёт; `.kimi-code/skills/ml-engineer/SKILL.md` §6 — строка цели.
- `ARCHITECTURE.md` — только если там перечислены ML-цели/эксперименты (проверить grep'ом).

## 11. Приёмка и PR

- `make all`, `make docs-check` — зелёные; `git status` — только ожидаемые файлы.
- PR: `<slug>/<item-id>`, `Closes #501` вне backticks; протокол приёмки — комментарием; Project V2 → `In Review`, поле `PR`.
- Мерж — решение владельца; после мержа статус роадмапа `✅`, Project → `Done`.
