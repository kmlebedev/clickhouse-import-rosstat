# CatBoost vs LightGBM/XGBoost для live-модели `combo3`: точность и скорость (Task 17)

> **Пункт:** `key-rate/phase-4-cb-comp-88-groups-live`, зонтик [#419](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/419), Task 17 ([#501](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/501)), `role:ml`.
> **Протокол:** [2026-11-01-cb-comp-backend-compare-design.md](../superpowers/specs/2026-11-01-cb-comp-backend-compare-design.md) (записан до прогона), [план](../superpowers/plans/2026-11-01-cb-comp-backend-compare.md).
> **Артефакт:** [2026-11-01-cb-comp-backend-compare.json](2026-11-01-cb-comp-backend-compare.json).
> **Статус:** измерено 2026-11-01; вердикт `keep_catboost`, промоушена нет.

## 1. Вопрос

`live_cb` — 88 групп × 4 горизонта = 348–352 CatBoost на дефолтах (1000 итераций, lr 0,03, depth 6, seed 20261018), frozen-окно 2010-03…2018-12. Learner в линии #419 не сравнивался с другими библиотеками ни разу. Вопрос: можно ли тот же конвейер отдать LightGBM или XGBoost без потери точности и с каким выигрышем по времени обучения.

Промоушена нет: вердикт — research-решение, смена базы — процесс [`ml-engineer-teamlead`](../../.kimi-code/skills/ml-engineer-teamlead/SKILL.md).

## 2. Протокол

| Плечо | Параметры | Участники |
|---|---|---|
| `defaults` | дефолты библиотеки (CatBoost — текущие `CATBOOST_DEFAULTS`) | catboost, lightgbm, xgboost |
| `matched` | `n_estimators=1000`, `learning_rate=0.03`, глубина — дефолт библиотеки | lightgbm, xgboost |

У CatBoost оба плеча совпадают — отдельный run не запускался. Версии: **catboost 1.2.10, lightgbm 4.7.0, xgboost 3.4.1**.

**Арена.** Frozen-обучение 2010-03…2018-12 (102 мес.), тест 2019-01…2024-07, normal-54; истина оценки — `TARGET_FULL`, справочно — `T_LIVE`. `month` (1…12) — категориальный признак во всех трёх библиотеках; NaN не импутируется. 348 fit на плечо: 4 пары «группа × горизонт» отсечены `MIN_TRAIN_ROWS = 20`.

**Вердикт (предрегистрация).**
- `swap_candidate` — challenger лучше на h1 (ΔRMSE < 0, DM-HLN p < 0,05, 95 % CI без 0), не хуже на h3/h6, `combo3_<backend>` h1 не хуже `combo3` (margin 2 %).
- `speed_candidate` — точность не различима (CI ΔRMSE в ±5 % RMSE CatBoost на h1/h3/h6) и медиана `fit` ≤ 0,5× CatBoost.
- иначе `keep_catboost`.

## 3. Инварианты

| Проверка | Результат |
|---|---|
| **R-B1** — CatBoost через новый слой `regressor=` повторяет `lb.fit_predict` | `max_abs_diff = 0,0` — ✅ |
| **R-B2** — пины Task 3/9 | `live_cb` 1,1929 / 1,0127 / 1,0758 / 1,0617 (пины 1,193 / 1,013 / 1,076 / 1,062); `combo3` 1,0348 / 0,9571 / 1,0102 / 0,8940; max |Δ| = **0,0003** — ✅ |
| **R-B3** — детерминизм повтора | Repeat `--backend lightgbm --arm defaults`: `predictions_lightgbm__defaults.csv` sha256 `24a7401f…`, `predictions_catboost__defaults.csv` `e6042de9…` — совпали побайтно; `per_model`/`pairs` равны; новые `run_id` `0035ab55…`, `7133b33c…` — ✅ |

R-B2 подтверждает: CatBoost 1.2.10 на текущих данных воспроизводит канонические числа линии — сравнение стоит на той же базе, что Task 9.

## 4. Точность

### 4.1. `live_cb` — отношение RMSE к CB_COMP на `TARGET_FULL`, normal-54

| run | h1 | h2 | h3 | h6 |
|---|---|---|---|---|
| catboost/defaults | **1,193** | **1,013** | **1,076** | **1,062** |
| lightgbm/defaults | 1,457 | 1,193 | 1,103 | 1,316 |
| lightgbm/matched | 1,571 | 1,301 | 1,133 | 1,607 |
| xgboost/defaults | 1,563 | 1,254 | 1,221 | 1,544 |
| xgboost/matched | 1,466 | 1,231 | 1,247 | 1,459 |

DM-HLN против CatBoost (ΔRMSE challenger − CatBoost, 95 % CI): h1 **+0,042…+0,060**, p = 0,0003…0,019; h6 **+0,247…+0,530**, p = 0,004…0,035. Все четыре challenger'а значимо хуже CatBoost уже на h1; на h3 различия в шуме (p = 0,24…0,70). MDE h1 — 3,4–6,4 %, то есть потеря 22–32 % по RMSE не является проблемой мощности.

### 4.2. `combo3` — отношение RMSE к CB_COMP

| run | h1 | h2 | h3 | h6 |
|---|---|---|---|---|
| catboost/defaults | **1,035** | **0,957** | **1,010** | **0,894** |
| lightgbm/defaults | 1,082 | 0,991 | 1,004 | 0,903 |
| lightgbm/matched | 1,104 | 1,019 | 1,002 | 0,949 |
| xgboost/defaults | 1,086 | 1,033 | 1,045 | 0,984 |
| xgboost/matched | 1,069 | 1,016 | 1,060 | 0,972 |

**Главная находка.** Замена learner'а бьёт по `live_cb` гораздо сильнее, чем по `combo3`: OLS-компоненты BS_ECON16/BS_ROZN16 компенсируют часть потери. На h1 `combo3` с LightGBM/XGBoost хуже CatBoost на 3,3–6,7 % (порог не-вреда 2 %), на h3 — практически паритет (0,99–1,06), на h6 — 0,90–0,98. По DM попарно `combo3` не сравнивался (в протоколе primary — `live_cb`); это точечные оценки.

## 5. Скорость

| run | fits | total, s | mean, ms | median, ms | p95, ms | trees | медиана к CatBoost |
|---|---|---|---|---|---|---|---|
| catboost/defaults | 348 | 254,7 | 732,0 | 676,9 | 1 146,8 | 1000 | 1,00 |
| lightgbm/defaults | 348 | 35,7 | 102,6 | 105,3 | 163,0 | 100 | **×0,16** |
| lightgbm/matched | 348 | 374,4 | 1 075,9 | 1 092,6 | 1 602,0 | 1000 | ×1,61 |
| xgboost/defaults | 348 | 62,5 | 179,7 | 181,9 | 284,3 | 100 | **×0,27** |
| xgboost/matched | 348 | 529,2 | 1 520,6 | 1 461,4 | 2 665,7 | 1000 | ×2,16 |

- Дефолты LightGBM/XGBoost быстрее в 3,7–6,4× по медиане fit, но это другая ёмкость: 100 деревьев × lr 0,1/0,3 против 1000 × 0,03.
- При равном бюджете (`matched`) LightGBM равен CatBoost по скорости (×1,61 — медленнее), XGBoost — в 2,2× медленнее.
- Абсолютные секунды машинозависимы: в двух прогонах на этой машине полный CatBoost занимал 170 и 255 с при одном и том же коде. Сигнал — отношение медиан внутри одного прогона (спека §3.2).

## 6. MCS 90 %

| arm | h1 | h3 | h6 |
|---|---|---|---|
| defaults | только `cb_comp` | все 4 модели | `cb_comp`, `live_cb_catboost` |
| matched | только `cb_comp` | все 4 модели | `cb_comp`, `live_cb_catboost` |

На h1 альтернативные learner'ы выпадают из 90 % MCS; на h3 набор неразличим — там ни одна модель не доказывает превосходство.

## 7. Вердикт

**`keep_catboost` для всех четырёх challenger'ов.** Причины:

| run | решение | причины |
|---|---|---|
| lightgbm/defaults | `keep_catboost` | h6 значимо хуже (p = 0,0179); `combo3` h1 +4,6 % (margin 2 %) |
| lightgbm/matched | `keep_catboost` | h6 значимо хуже (p = 0,0040); `combo3` h1 +6,7 %; медиана fit ×1,61 — не быстрее 0,5× |
| xgboost/defaults | `keep_catboost` | h6 значимо хуже (p = 0,0252); `combo3` h1 +4,9 % |
| xgboost/matched | `keep_catboost` | h6 значимо хуже (p = 0,0350); `combo3` h1 +3,3 %; медиана fit ×2,16 — не быстрее 0,5× |

`swap_candidate` не выполняется ни для одного: на h1 challenger'ы не лучше, а хуже. `speed_candidate` не выполняется: не-неполноценность точности (CI ΔRMSE в ±5 %) нарушена на h1 у всех четырёх — быстрые дефолты теряют 22–32 % RMSE по `live_cb` и 3–5 % по `combo3`.

Практический вывод: **скорость LightGBM/XGBoost на этой задаче не бесплатна**; запас по времени (3,7–6,4× на дефолтах) оплачивается значимой потерей h1. При равном бюджете выигрыша по скорости нет вовсе. `combo3` устойчивее к смене learner'а, чем `live_cb`, но и для него h1 выходит за margin не-вреда.

## 8. Что не проверено

- 54 normal-месяца: MDE h1 ≈ 3,4–6,4 %, паритет не доказывался (TOST не проходил); «не значимо» на h3 не значит «одинаково».
- PIT/винтажей нет; истина — `TARGET_FULL` до 2024-07, `T_LIVE` справочно.
- Скорость — одна машина, дефолтное число потоков; абсолютные секунды не переносятся на другой хост.
- Гиперпараметры не тюнились (Task 6, [#426](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/426)): сравнение библиотек при дефолтах и равном бюджете, а не поиск лучшей конфигурации.
- `combo3` попарно против `combo3_catboost` по DM не тестировался — только точечные отношения.
- Промоушен, alias, registry, shadow/canary — вне объёма; вердикт не меняет базу.

## 9. Паспорта MLflow/ClickHouse

Experiment `cb_comp_88_groups`, tracking URI — `http://192.168.3.9:5000` (dev-сервер из env; не staging). Все пять run'ов — `finished`, `git_dirty=1`: код закоммичен (`68b7e73`), незакоммиченными были только документы отчёта; `git_sha` в паспорте указывает на код. После `68b7e73` в ветку вошли правка распаковки ключей в цикле логирования (на числа не влияет) и флаг `--no-evidence`; повтор R-B3 на финальном коде дал побайтно те же прогнозы — дрейфа чисел нет.

| run | run_id | mlog |
|---|---|---|
| catboost/defaults | `de0a8d39-81f3-42c0-a2a1-b475ab786bee` | clickhouse |
| lightgbm/defaults | `8bcb5973-ba1b-4795-a1db-148daf68e8c6` | clickhouse |
| lightgbm/matched | `3af850ce-e57d-407f-8fb0-ae2ef177bad9` | clickhouse |
| xgboost/defaults | `52b8b729-176e-4dfe-8e3f-72d6c7515e75` | clickhouse |
| xgboost/matched | `c8098343-98e7-4f19-b399-646dbefbf00f` | clickhouse |

- `v_ml_runs`: 5 строк `finished`; `v_ml_leaderboard` — 8 строк на run (2 модели × 4 горизонта), 54 фолда, RMSE сходится с `result.json`; `v_ml_error_by_fold` — 432 строки на run.
- MLflow: 40–61 метрика на run (`fit_seconds_*`, `ratio_cb_comp_h*`, `dm_p_vs_catboost_h*`, `combo3_ratio_*`, `mcs_included_h*`, `decision_*`, `r_b1_ok`, `r_b2_ok`), artifacts `result.json`, `report.md`, `predictions_<backend>__<arm>.csv`, `timings_<backend>__<arm>.csv`, `run.json`, `predictions.jsonl`; tags `stage=backtest`, `champion=catboost`, `challenger=<backend>`, `backend`, `arm`, `decision`.
- Один прерванный run `baf9e982-…` (`FAILED`, 0 метрик) — первая попытка, остановленная на баге последовательного логирования; в расчёт не входит.

## 10. Воспроизведение

```bash
make ch-status
make cb-comp-public-export
make cb-comp-groups-panel ARGS="--asof"
make mlflow-up
ML_LOG=1 MLFLOW=1 make cb-comp-backend-compare \
  ARGS="--archive /path/to/BusinessSurveyCBRCpiForecastingSourceCode"
# повтор R-B3 (не перезаписывает канонический result.json/эвиденс):
ML_LOG=1 MLFLOW=1 make cb-comp-backend-compare \
  ARGS="--backend lightgbm --arm defaults --out build/cb-comp-backend-compare-repeat --no-evidence"
```

Артефакты: `build/cb-comp-backend-compare/{result.json,predictions.csv,timings.csv,report.md}`; копия `result.json` — [JSON](2026-11-01-cb-comp-backend-compare.json). Тесты: `make cb-comp-backend-compare-test` (15 офлайн-тестов), `make cb-comp-live-bottomup-test` (22 теста, пины Task 3 не сдвинулись).
