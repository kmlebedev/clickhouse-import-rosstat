# Task 17: CatBoost vs LightGBM/XGBoost для live-модели `combo3` — протокол

> **Пункт:** `key-rate/phase-4-cb-comp-88-groups-live`, зонтик [#419](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/419), Task 17 ([#501](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/501)), `role:ml`. Базовая спека — [2026-10-30-cb-comp-88-groups-live-design.md](2026-10-30-cb-comp-88-groups-live-design.md); карта линии — [cb-comp-live-model-map.md](../../guides/cb-comp-live-model-map.md); паспорт — P0.5 ([2026-11-01-cb-comp-p05-mlflow-design.md](2026-11-01-cb-comp-p05-mlflow-design.md)).
> **Статус:** протокол записан **до прогона**. Состав плеч, гиперпараметры, метрики и правило вердикта не меняются после того, как увидены числа.

## 1. Зачем

`live_cb` — 88 групп × 4 горизонта = 352 CatBoost на дефолтах (`CATBOOST_DEFAULTS`: 1000 итераций, lr 0,03, depth 6, seed 20261018) с frozen-окном 2010-03…2018-12. Learner в линии #419 ни разу не сравнивался с другими библиотеками: Task 3 менял структуру (EN), Task 10 — на pooled-модель, Task 12 — на VAR. Вопрос «можно ли тот же конвейер отдать LightGBM или XGBoost без потери точности и с каким выигрышем по времени обучения» не задан ни разу.

Задача отвечает на него на той же арене, что Task 9, подменяя только learner, и пишет паспорта точности и скорости в MLflow/`ml_runs` (P0.5). Промоушена нет: вердикт — research-решение, смена базы — процесс [`ml-engineer-teamlead`](../../../.kimi-code/skills/ml-engineer-teamlead/SKILL.md).

## 2. Что известно до протокола

| Факт | Значение |
|---|---|
| Арена | frozen-обучение 2010-03…2018-12 (102 мес.), тест 2019-01…2024-07, normal-54 (без 2020-04 и всего 2022) |
| Цель | `T_LIVE[h]` — накопленный SA ИПЦ без ЖКУ; истина оценки — `TARGET_FULL[h]` архива CB_COMP |
| Признаки | `bu.feature_names(mode="per_sheet")`: база (`y_lag1`, `y_lag2`, `usd_mm`, `key_rate`, `fao_mm`, `month`) + МП своего листа у 72 групп; 16 групп без МП |
| Модель-эталон | `live_cb` = агрегат 88 компонентных CatBoost по весам СПР; `combo3` = `mean(live_cb, bs_econ16, bs_rozn16)` |
| Канонические числа | `live_cb` на `TARGET_FULL` normal-54: **1,193 / 1,013 / 1,076 / 1,062** (h1/h2/h3/h6); `combo3`: **1,035 / 0,957 / 1,010 / 0,894** (Task 3, Task 9) |
| Паспорт | `ML_LOG=1` + `cb_comp_mlog`, experiment `cb_comp_88_groups`, folds `hN`, `actual=TARGET_FULL`, `t_live` в `meta_json` |
| Мощность | MDE h1–h3 ≈ 13 % на 54 normal-месяцах; «не значимо» ≠ «одинаково» |
| Версии на 2026-11-01 | `catboost==1.2.10`, `lightgbm==4.7.0`, `xgboost==3.4.1` |

## 3. Протокол сравнения

### 3.1. Плечи

| Плечо | Параметры | Кто участвует |
|---|---|---|
| `defaults` | дефолты библиотеки (CatBoost — текущие `CATBOOST_DEFAULTS`) | catboost, lightgbm, xgboost |
| `matched` | `n_estimators=1000`, `learning_rate=0.03`; глубина/листья — дефолт библиотеки | lightgbm, xgboost |

У CatBoost плечо `matched` совпадает с `defaults` (1000 итераций, lr 0,03), поэтому отдельный run для него не создаётся: CatBoost-эталон один.

**Почему два плеча.** `defaults` отвечает на вопрос «что будет, если заменить библиотеку, не трогая код», но сравнение скорости в нём смешано с разным числом деревьев (LightGBM 100 × lr 0,1; XGBoost 100 × lr 0,3; CatBoost 1000 × lr 0,03). `matched` выравнивает бюджет и отделяет «алгоритм» от «сколько деревьев строим».

### 3.2. Гиперпараметры и общие настройки

| Библиотека | `defaults` | `matched` добавляет |
|---|---|---|
| CatBoost | `loss_function="RMSE"`, `random_seed=20261018`, `verbose=False`, `allow_writing_files=False`, `cat_features=[month]` | — |
| LightGBM | `random_state=20261018`, `verbose=-1`, `deterministic=True`, `force_row_wise=True`, `categorical_feature=[month]` | `n_estimators=1000`, `learning_rate=0.03` |
| XGBoost | `random_state=20261018`, `tree_method="hist"`, `enable_categorical=True`, `verbosity=0` | `n_estimators=1000`, `learning_rate=0.03` |

- `month` (1…12) — категориальный признак во всех трёх: у CatBoost `cat_features`, у LightGBM `categorical_feature`, у XGBoost `enable_categorical=True`. Фиксированный набор категорий `1…12` для train и predict, иначе кодировки разойдутся.
- NaN **не импутируется**: все три библиотеки обрабатывают пропуски нативно (политика линии, скил §5 п. 7).
- Потоки не фиксируются (дефолт библиотеки); `os.cpu_count()` и версии пишутся в паспорт. Абсолютное время машинозависимо; сигнал — отношение внутри одного прогона на одной машине.
- Гиперпараметры не тюнятся: сетка/подбор — Task 6 (#426), отдельный пункт; здесь сравнение библиотек, а не тюнинг.

### 3.3. Один конвейер

```
cb_comp_live_en.prepare(archive, panel, public_dir)
  → specs (88), features, targets, train_months, test_months, weights, t_live, target_full, normal
  → для каждого плеча: fit_predict(..., regressor=<backend>)   # 88 × 4 = 352 fit
  → aggregate_predictions(weights) = live_cb_<backend>
  → combo3_<backend> = mean(live_cb_<backend>, bs_econ16, bs_rozn16)  # OLS из Task 9, без переобучения
  → evaluate: RMSE/ratio/class/DM/CI/MCS
```

`lb.fit_predict` получает необязательный `regressor=None`; по умолчанию — текущие `bu.fit_component`/`bu.predict_component`, поэтому числа Task 3 не меняются.

## 4. Метрики

### 4.1. Точность

Для каждого `(backend, arm)`:

- `rmse_target_full_normal_h1…h6` — RMSE `live_cb_<backend>` против `TARGET_FULL` на normal-54 (primary);
- `ratio_cb_comp_h1…h6` — отношение к CB_COMP (bootstrap ratio из `w5b.compare_to_cb`), класс §6.3 «лучше / паритет / хуже / не определено»;
- `rmse_t_live_normal_h1…h6` — справочно, против `T_LIVE`;
- `rmse_target_full_all67_h1…h6` — справочно, все 67 месяцев;
- то же для `combo3_<backend>`;
- попарно `v4.compare_pair`: ΔRMSE, 95 % блочный бутстрап-CI (block=3, n=2000, seed=20261018), DM-HLN p, MDE;
- MCS 90 % на потерях `{cb_comp, live_cb_catboost, live_cb_lightgbm, live_cb_xgboost}` для h1/h3/h6.

### 4.2. Скорость

`perf_counter` вокруг `fit` каждого компонента (и отдельно `predict`); прогревочный fit на пару `(backend, arm)` отбрасывается. Логируются:

- `fit_seconds_total`, `fit_seconds_mean`, `fit_seconds_median`, `fit_seconds_p95`, `fit_seconds_max`;
- `predict_seconds_total`, `predict_seconds_median`;
- `n_fits`, `n_fits_skipped` (< `MIN_TRAIN_ROWS`), `trees_mean` (если библиотека отдаёт), `threads`, `cpu_count`;
- построчно — `timings.csv` артефактом.

## 5. MLflow/`ml_runs`-паспорт

| Поле | Значение |
|---|---|
| `experiment` | `cb_comp_88_groups` — одно имя на линию (иначе gate/leaderboard не pairing'уют) |
| `run_name` | `live_cb_<backend>__h1-h6__normal54__target_full__<version>__<arm>` |
| `model` (folds) | `live_cb_<backend>` и `combo3_<backend>` — 2 модели × 4 горизонта × 54 месяца = 432 строки |
| `actual` | `TARGET_FULL`; `t_live` — в `meta_json` |
| `config` | `backend`, `backend_version`, `arm`, гиперпараметры, `seed`, `version`, train/test окна, `normal`, `models`, `truth`, `unit`, `spec_path`, sha256 входов (`panel`, `public_panel`, `sheet_map`, `target_xlsx`, `series2016/2009`) |
| `sample` | 4 горизонта × 54 месяца `{horizon, month, target_full, t_live}`; `sample_hash` одинаков у всех 5 run'ов |
| `metrics` | RMSE/ratio/class/DM p/CI/MDE по h1/h2/h3/h6, MCS, speed-агрегаты, инварианты R-B1/R-B2 |
| `artifacts` | `result.json`, `predictions.csv`, `timings.csv` (sha256 в паспорте) |
| `tags` | `stage=backtest`, `champion=catboost`, `challenger=<backend>`, `decision=<вердикт>`, `backend`, `arm` |

Run'ы (5): `catboost/defaults`, `lightgbm/defaults`, `lightgbm/matched`, `xgboost/defaults`, `xgboost/matched`. `ML_LOG=1` обязателен; `MLFLOW=1` — по умолчанию, с `mlflow-preflight`; URI приёмки фиксируется в отчёте. Alias/registry не используются.

## 6. Предрегистрированный вердикт

Сравнение с CatBoost внутри плеча:

- **`swap_candidate`** — challenger лучше на h1 (ΔRMSE < 0, DM-HLN p < 0,05, 95 % CI целиком < 0) **и** не хуже на h3/h6 (ни на одном из них нет значимого проигрыша: не `(ΔRMSE > 0, p < 0,05, CI > 0)`) **и** `combo3_<backend>` h1 не хуже `combo3` с margin 2 %.
- **`speed_candidate`** — точность не различима: для h1/h3/h6 95 % CI ΔRMSE лежит внутри ±5 % RMSE CatBoost соответствующего горизонта, **и** медиана `fit` ≤ 0,5× медианы CatBoost.
- иначе **`keep_catboost`**.

Если `swap_candidate` и `speed_candidate` выполняются одновременно — приоритет `swap_candidate`, в отчёте оба. Если кандидатов несколько — выбирается лучший по медиане `fit`, при равенстве — по h1 RMSE.

**Оговорка о мощности.** 54 normal-месяца дают MDE ≈ 13 % на h1: «выигрыш по точности» маловероятен, и `speed_candidate` — ожидаемый исход. Вердикт не является промоушеном и не двигает alias.

## 7. Инварианты

| Проверка | Что проверяет | Правило |
|---|---|---|
| **R-B1** | CatBoost-плечо через новый слой `regressor=` повторяет `lb.fit_predict` | агрегаты и fold-прогнозы совпадают побайтно (`predictions.csv` sha256); иначе стоп |
| **R-B2** | Пины Task 3 (`1,193/1,013/1,076/1,062`) и Task 9 (`1,035/0,957/1,010/0,894`) | ±0,002 — ✅; дрейф 0,002–0,01 — finding в отчёт; >0,01 — finding + стоп до объяснения |
| **R-B3** | Детерминизм | повтор `defaults`-плеча даёт тот же `predictions.csv` (sha256) и те же блоки `per_model`/`pairs` в `result.json`; время, `run_id` и `mlog_status` не сравниваются |
| **R-B4** | `month` как категория | train и predict используют один `CategoricalDtype(1…12)`; тест на dtype |

R-B2 — сверка абсолютного уровня: смена версии CatBoost может сдвинуть пины. R-B1/R-B3 важнее: они гарантируют, что сравнение внутренне согласовано.

## 8. Границы

- Не доказывается: паритет с CB_COMP (TOST не проходил), PIT/винтажи (их нет), live-точность (истина — `TARGET_FULL`), переносимость скорости на другую машину/потоки.
- Не делается: DDL/GRANT/витрины/Go-код, alias/registry, промоушен, shadow/canary, тюнинг гиперпараметров (Task 6), per-code МП (Task 8, no-go).
- `ML_LOG` не влияет на числа: `predictions.csv`/`result.json` при `ML_LOG=0` и `ML_LOG=1` совпадают побайтно.
- Секреты — только env; `~/.config/rosstat/env` не читается.

## 9. Проверки и критерий приёмки

1. `make cb-comp-backend-compare-test` — офлайн, зелёные; `make cb-comp-live-bottomup-test` — пины Task 3 не сдвинулись.
2. `ML_LOG=1 MLFLOW=1 make cb-comp-backend-compare`: 5 run'ов `finished`, `git_dirty=0`, R-B1/R-B2/R-B3 ✅.
3. `v_ml_runs` — 5 run'ов; `v_ml_leaderboard` — 10 строк на run (2 модели × 4 горизонта), RMSE сходится с `result.json`; `v_ml_error_by_fold` — 432 строки на run.
4. `make mlflow-export ARGS="--experiment cb_comp_88_groups --dry-run"` видит run'ы, ClickHouse не меняет.
5. Отчёт `docs/research/2026-11-01-cb-comp-backend-compare.md` + JSON: таблицы точности/скорости, MCS, вердикт, «что не проверено», run_id и URI, sha256 артефактов.
6. `make all`, `make docs-check` — зелёные; статус роадмапа и README/гайд/скил обновлены тем же PR.

## 10. Воспроизведение

```bash
make ch-status                     # ClickHouse поднят владельцем
make cb-comp-public-export         # public_panel.csv / sheet_map.csv
make cb-comp-groups-panel ARGS="--asof"   # build/cb-comp-groups/panel.csv (если нет)
make mlflow-up                     # локальный dev-сервер, если не поднят
ML_LOG=1 MLFLOW=1 make cb-comp-backend-compare
# повтор R-B3 (не перезаписывает канонический result.json/эвиденс):
ML_LOG=1 MLFLOW=1 make cb-comp-backend-compare \
  ARGS="--backend lightgbm --arm defaults --out build/cb-comp-backend-compare-repeat --no-evidence"
```
