# P0.5: MLflow/`ML_LOG`-паспорт линии 88 групп — протокол

> **Пункт:** `key-rate/phase-4-cb-comp-88-groups-live`, зонтик [#419](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/419), P0.5, `role:ml`. Базовая спека пункта — [2026-10-30-cb-comp-88-groups-live-design.md](2026-10-30-cb-comp-88-groups-live-design.md); карта линии — [cb-comp-live-model-map.md](../../guides/cb-comp-live-model-map.md) §6.2.
> **Статус:** протокол записан **до прогона**. Схема паспорта, имена эксперимента/окон и выбор истины не меняются после того, как увидены числа.

## 1. Зачем

Линия 88 групп (`combo3`, `live_cb`, BS-модели статьи) заканчивает каждый шаг JSON-артефактом и sha256 входов, но не пишет паспорт в `ml_runs`/`ml_predictions`. `make ml-gate` читает ClickHouse (`v_ml_error_by_fold`), а не файлы, поэтому для линии gate и промоушен сейчас не запускаются: run'а нет — сравнивать нечего. P0.5 закрывает разрыв: прогон `cb_comp_h1_bs_combo` (и, при необходимости, `cb_comp_live_bottomup`) получает канонический `run_id`, `config_hash`, `sample_hash`, fold-предсказания, sha256 артефактов и MLflow-зеркало.

## 2. Что известно до протокола

- `scripts/lib/ml_experiment.py` уже умеет всё нужное: `start_run`, `PredictionRow`, `finish`, `config_hash`/`sample_hash`, артефакты sha256, MLflow best-effort; при `ML_LOG` ≠ 1 — no-op.
- Схема `ml_runs`/`ml_predictions` и витрины `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold` существуют (`mlregistry`), DDL/гранты не требуются.
- Прецедент формата — `cb_comp_parity.py`: `model=<имя>`, `window=h1…h6`, `unit=pp`, folds на normal-54.
- `ml-gate` сравнивает **один** `--model` в двух run'ах одного `--experiment`; min 12 общих фолдов в основном окне.
- `TARGET_FULL` (2009-01…2024-07) — предзарегистрированная истина оценки; `T_LIVE` считается тем же кодом, но служит live-целью и справочной истиной.
- Панели `@2010` (`aec91f99…`) и `@2009` (`7b6d3028…`), public-панель и `sheet_map` воспроизводимы; архив CB_COMP в репозиторий не входит.

## 3. Схема паспорта

| Поле | Значение |
|---|---|
| `experiment` | `cb_comp_88_groups` — одно имя на всю линию, иначе gate не pairing'ует run'ы |
| `run_name` | `combo3__h1-h6__normal54__target_full__<version>`; bottom-up — `live_cb__…`; `ML_RUN_NAME` переопределяет |
| `model` | `live_cb`, `bs_econ16`, `bs_rozn16`, `combo3`; bottom-up — `live_per_sheet`, `live_nomp`, `live_per_sheet_asof` |
| `window` | `h1`, `h2`, `h3`, `h6` — горизонт; gate вызывается с `--window h1` |
| `target_date` | месяц цели (normal-54) |
| `predicted` | прогноз модели |
| `actual` | `TARGET_FULL` |
| `error` | `predicted − actual`, `unit=pp` |
| `meta_json` | `horizon`, `version`, `normal=1`, `truth="target_full"`, `t_live` |
| `data_asof` | `2024-07-01` (`cp.TEST_END`) |

**Почему `actual = TARGET_FULL`, а не `T_LIVE`.** `ml_predictions` — одна строка на `(experiment, run_id, model, window, target_date)`; две истины в одном ключе перезаписали бы друг друга (ReplacingMergeTree). `TARGET_FULL` — истина, на которой предзарегистрированы G9/не-вред и класс W5b; `T_LIVE` остаётся в `meta_json` и в `metrics_json`.

**Почему `window = hN`.** В линии нет walk-forward-окон в смысле `ml_gate` (`start=24`, `expanding`); горизонт — единственная ось, различающая fold-наборы. Это уже принято в `cb_comp_parity`.

## 4. Что логируется

### 4.1. `cb_comp_h1_bs_combo` (обязательно)

После записи `predictions.csv`/`result.json`, без изменения самих файлов:

- folds: `CANDIDATES` × 4 горизонта × 54 normal-месяца = **864 строки**;
- `config`: `version`, `seed`, `horizons`, `primary_horizons`, train/test окна и длины, `normal`, `models`, `truth`, `unit`, `spec_path`, `inputs` (sha256 панели, public_panel, sheet_map, `TARGET_FULL`, series2016/2009);
- `sample`: 4 горизонта × 54 месяца `{horizon, month, target_full, t_live}` — `sample_hash` меняется с данными, а не с прогнозами;
- metrics: `primary_metric="rmse_pp"`, `ratios`, `g9`, `noharm`, `mcs`, `r9`;
- artifacts: `predictions.csv`, `result.json` (sha256 и размер — в паспорте).

### 4.2. `cb_comp_live_bottomup` (при необходимости)

То же: `MODELS` × 4 × 54 = **648 строк**, `config`/`sample` по тем же правилам. Живой прогон — не обязателен для приёмки P0.5.

### 4.3. Чего в паспорте нет

- Сырые данные, пути архива, ключи и токены — только sha256 входов и агрегаты.
- Модель не регистрируется (`log_model` не вызывается), alias `champion` не создаётся: линия research, registry — не про неё.
- `result.json` не меняется: повтор прогона остаётся побайтно воспроизводимым.

## 5. MLflow и ClickHouse

- `ML_LOG=1` включает приёмник; `MLFLOW=1` (по умолчанию в Makefile) подключает tracking. `MLFLOW=0` — осознанное выключение.
- Перед прогоном `mlflow_preflight.py`: недоступный API в не-TTY — стоп, `MLFLOW_ALLOW_NO_LOG=1` — явное решение.
- Прямой прогон пишет в оба приёмника одним `run_id`; `make mlflow-export`/`mlflow-backfill` — только fallback (идемпотентны).
- Канонический долгосрочный backend — staging (политика #468); локальный `build/mlflow` — dev. Код принимает любой `MLFLOW_TRACKING_URI`; адрес приёмки фиксируется в отчёте.

## 6. Гейт

`make ml-gate ARGS="--experiment cb_comp_88_groups --champion-run <A> --challenger-run <B> --model combo3 --window h1 [--robustness-window h3 --robustness-window h6]"`.

P0.5 не принимает решение о промоушене и не меняет `ml_gate.py`: проверяется, что gate **читает** run из `v_ml_error_by_fold` и пишет артефакт `build/ml-gate/<run>.json`. Пара одинаковых конфигов даёт Δ = 0, `dm_p = 1.0`, `promote = false` — это проверка проводки, а не сравнение моделей. Содержательная пара (например, `@2010` против `@2009`) — опция, её вердикт в отчёт переносится как есть и промоушеном не является.

## 7. Проверки и критерий приёмки

1. **Витрины.** Оба run'а видны в `v_ml_runs` (`status='finished'`, `git_dirty=0`), `v_ml_leaderboard` даёт 16 строк (4 модели × 4 горизонта) по 54 фолда, `v_ml_error_by_fold` — 864 строки на run; RMSE лидерборда совпадает с `result.json`.
2. **Детерминизм.** Повтор с тем же конфигом даёт те же `config_hash`/`sample_hash`, те же sha256 `predictions.csv`/`result.json`, побайтно те же fold-значения; `run_id` разный.
3. **Артефакты.** sha256 `predictions.csv`/`result.json` записаны в `ml_runs.artifacts_json`; копия паспорта и числа — в отчёте.
4. **Gate.** `make ml-gate` на двух run'ах проходит, артефакт решения существует, folds = 54.
5. **MLflow.** `make mlflow-export ARGS="--experiment cb_comp_88_groups --dry-run"` видит run'ы и ничего не пишет в ClickHouse.
6. **Тесты.** `make cb-comp-h1-bs-combo-test`, `make cb-comp-live-bottomup-test`, `make all`, `make docs-check` — зелёные.

## 8. Границы

- Без DDL, GRANT, витрин и Go-кода; `mlregistry` и `ml_gate.py` не меняются.
- `ML_LOG` не влияет на числа: `predictions.csv`/`result.json` при `ML_LOG=0` и `ML_LOG=1` совпадают побайтно (проверяется тестом и прогоном).
- Промоушен, смена базы, shadow/canary — не этот пункт.
