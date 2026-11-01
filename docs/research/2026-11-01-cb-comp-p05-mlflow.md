# P0.5 (#485): MLflow/`ML_LOG`-паспорт линии 88 групп — отчёт

> **Пункт:** `key-rate/phase-4-cb-comp-88-groups-live` → P0.5, зонтик [#419](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/419), `role:ml`. Спека — [2026-11-01-cb-comp-p05-mlflow-design.md](../superpowers/specs/2026-11-01-cb-comp-p05-mlflow-design.md), план — [2026-11-01-cb-comp-p05-mlflow.md](../superpowers/plans/2026-11-01-cb-comp-p05-mlflow.md). Ветка `feat/cb-comp-88-groups-p05-mlflow`, worktree `.worktrees/cb-comp-88-p05`.
> **Итог:** прогоны `combo3` и `live_cb` видны в `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`; повтор `@2010` даёт те же предсказания, `config_hash`/`sample_hash` и sha256 артефактов; `make ml-gate` читает run; MLflow-зеркало — staging `http://192.168.3.9:5000`, идемпотентно. Промоушен и смена базы не заявляются: gate — проверка проводки.

## 1. Что сделано

1. `scripts/models/cb_comp/cb_comp_mlog.py` — общий паспорт линии: fold-строки `(model, hN, month)`, `TARGET_FULL` как `actual`, `T_LIVE` в `meta_json`, `config_hash`/`sample_hash`, sha256 артефактов, best-effort MLflow. Без `ML_LOG=1` — no-op; сбой приёмника не меняет числа и файлы.
2. `cb_comp_h1_bs_combo.py` и `cb_comp_live_bottomup.py` пишут паспорт после своих артефактов; `result.json`/`predictions.csv` не изменились.
3. `Makefile`: цели получили `mlflow_preflight`, `MLFLOW_ENV` и `MLFLOW_PKG`; `MLFLOW=0` — осознанное выключение.
4. Тесты: `cb_comp_mlog_test.py` (10) + интеграционные в обоих `-test`-таргетах.
5. `ml-gate`, DDL, витрины и гранты не менялись; alias/registry не используются.

## 2. Приёмка

### 2.1. Паспорт и детерминизм

Входы: панель `build/cb-comp-groups/panel.csv` (`aec91f99…`), `public_panel.csv` (`47176baa…`), `sheet_map.csv` (`0d8a6ab5…`), архив CB_COMP — `/Users/whitefox/Downloads/BusinessSurveyCBRCpiForecastingSourceCode`.

| Run | backend | version | `run_id` | `config_hash` | `sample_hash` | sha256 `predictions.csv` | sha256 `result.json` |
|---|---|---|---|---|---|---|---|
| A' | staging | 2010 | `1bba8483-c367-452b-ab5b-76a330a81720` | `b3197565…` | `0e39e019…` | `e4ced16b…` | `2b6a556e…` |
| B' (повтор) | staging | 2010 | `453bfef3-3582-46e9-b72f-b3a58cb15b5b` | `b3197565…` | `0e39e019…` | `e4ced16b…` | `2b6a556e…` |
| C' | staging | 2009 | `e885eaea-a528-42c5-9ae1-63b3be914832` | `e49ecb6b…` | `3797c221…` | `b11d02d9…` | `38b1e971…` |
| D'' (bottom-up) | staging | 2010 | `b870d6dc-db64-47a6-b447-619ee9173e28` | `32cd2d9b…` | `0e39e019…` | `68378cb9…` | `6d9ff5e8…` |

Ранее те же конфиги прогонялись на локальном dev-сервере: A `9f6e6bf9…`, B `ae591fa4…`, C `a2c17a05…`, D `42de36a6…`. Они остаются в истории `ml_runs`, но приёмкой не являются. `result.json` у staging- и dev-прогонов отличается только `git_sha` (добавлен коммит `deploy/staging/mlflow.service`); `predictions.csv` и метрики совпадают. Промежуточный bottom-up `232a3ea6…` записан в staging с `git_dirty=1` (в дереве были незакоммиченные docs-правки) и оставлен как отброшенный; канонический — D'' `b870d6dc…`, `git_dirty=0`.

Повтор A'/B': одинаковые `config_hash`, `sample_hash`, sha256 `predictions.csv` (`e4ced16b76595e775f78a4f40dd6c44aca9aa1ec4b555e2e191a5ed3f94ade36`) и `result.json` (`2b6a556e2ee33ef869c1d09c85a959e6999bb53f4c8f22cab123ac07b04ee6df`), `run_id` разный. `git_dirty=0`, `status='finished'`.

### 2.2. Витрины

- `v_ml_error_by_fold`: **864 строки** на combo-run (4 модели × 4 горизонта × 54 normal-месяца), **648** на bottom-up (3 × 4 × 54); симметричная разность fold-значений A' и B' — **0**.
- `v_ml_leaderboard`: 16 строк на run (4 модели × 4 горизонта), по 54 фолда. `combo3` — 0,164327 / 0,283931 / 0,448147 / 0,868518 на h1/h2/h3/h6 (отношения к CB_COMP 1,035 / 0,957 / 1,010 / 0,894 — как в Task 9); `live_cb` — 0,189431 / 0,300424 / 0,477259 / 1,031389; `bs_econ16` — 0,166706 / 0,322351 / 0,521499 / 0,897860; `bs_rozn16` — 0,168930 / 0,322598 / 0,516526 / 0,996425.
- `v_ml_runs`: **девять** run'ов `experiment='cb_comp_88_groups'` — 4 dev (A–D) и 5 staging (A', B', C', D'' и отброшенный D'), `notes='version=…; normal=54; horizons=h1,h2,h3,h6'`, артефакты с sha256 и размером.

### 2.3. Gate читает run

`make ml-gate ARGS="--experiment cb_comp_88_groups --champion-run <A'> --challenger-run <B'> --model combo3 --window h1 --robustness-window h3 --robustness-window h6"` — артефакт `build/ml-gate/<B'>.json`; `decision='iterate'`, `reason='no_improvement'`, DM p = 1,0, folds = 54 на h1/h3/h6. Это проверка проводки, а не промоушен.

Содержательная пара `@2010` (A') против `@2009` (C'): h1 RMSE 0,164327 → 0,165240, Δ +0,000913, 95 % CI [−0,001981; +0,003903], DM p = 0,5593; h3 p = 0,91999; h6 p = 0,93575; `decision='iterate'`. Чтение совпадает с Task 16: смена версии цели не улучшает `combo3`, разница в пределах шума.

Копия обоих gate-артефактов — [2026-11-01-cb-comp-p05-mlflow-gate.json](2026-11-01-cb-comp-p05-mlflow-gate.json).

### 2.4. MLflow

Backend — staging `http://192.168.3.9:5000` (MLflow 3.17.1; `deploy/staging/mlflow.service` теперь слушает `0.0.0.0:5000`). `run_id` в MLflow совпадает с `ml_runs.run_id` (канонический UUID), `clickhouse_run_id` — тот же.

`make MLFLOW_TRACKING_URI=http://192.168.3.9:5000 mlflow-export ARGS="--experiment cb_comp_88_groups --dry-run"` видит **5 run'ов / 3888 предсказаний** — 4 канонических staging-прогона и отброшенный D' (`232a3ea6…`, `git_dirty=1`); ClickHouse не меняет. Первые dev-прогоны A–D на локальном `127.0.0.1:5000` остаются в истории как dev-дубли; приёмка — staging.

### 2.5. Тесты

`make cb-comp-h1-bs-combo-test` — 21 тест combo + 10 тестов `cb_comp_mlog`; `make cb-comp-live-bottomup-test` — 22 теста bottom-up + 10 тестов `cb_comp_mlog`. Все офлайн, без ClickHouse и MLflow.

## 3. Bottom-up

`MLFLOW_TRACKING_URI=http://192.168.3.9:5000 ML_LOG=1 make cb-comp-live-bottomup` — run `b870d6dc-db64-47a6-b447-619ee9173e28` (`git_dirty=0`), `config_hash` `32cd2d9b…`, `sample_hash` `0e39e019…`, sha256 `predictions.csv` `68378cb9…` / `result.json` `6d9ff5e8…`; **648 fold-строк** (3 модели × 4 горизонта × 54 месяца), лидерборд — 12 строк × 54 фолда. `live_per_sheet` — 0,189431 / 0,300424 / 0,477259 / 1,031389 на h1/h2/h3/h6: те же числа, что `live_cb` в combo-run (кросс-проверка паспорта); `live_nomp` — 0,212245 / 0,411916 / 0,626699 / 1,087271; `live_per_sheet_asof` — 0,189391 / 0,311443 / 0,481228 / 1,024827.

Это тот же оценщик, что `live_cb` в combo-run, но под своим именем; пара между ними требует `--champion-model`/`--challenger-model` (P1.5), поэтому gate на этой паре здесь не запускается.

## 4. Что не проверено

- **Локальный dev-сервер.** Первые прогоны A–D писали в `127.0.0.1:5000`; они остаются в истории как dev-дубли и не перезаписывались — staging-паспорта A'–D'' независимы.
- **Детерминизм `@2009`.** Повтор проверен на `@2010`; `@2009` прогнан один раз (код общий, но отдельного A/B-повтора нет).
- **Пара разных имён моделей** (`live_cb` vs `combo3`) в `ml-gate` не проверялась — это P1.5.
- **Alias/registry.** Не создавались: линия research, `champion` — термин протокола §7.8, не alias MLflow.
- **Промоушен.** P0.5 не принимает решение о смене базы; gate-артефакты — только проводка и справочное сравнение версий.

## 5. Воспроизведение

```bash
make cb-comp-h1-bs-combo-test          # 21 + 10 тестов
make cb-comp-live-bottomup-test        # 22 + 10 тестов

# входы: панель aec91f99…, public_panel 47176baa…, sheet_map 0d8a6ab5…
MLFLOW_TRACKING_URI=http://192.168.3.9:5000 \
  ML_LOG=1 make cb-comp-h1-bs-combo ARGS="--archive /Users/whitefox/Downloads/BusinessSurveyCBRCpiForecastingSourceCode"
MLFLOW_TRACKING_URI=http://192.168.3.9:5000 \
  ML_LOG=1 make cb-comp-h1-bs-combo ARGS="--version 2009 --archive /Users/whitefox/Downloads/BusinessSurveyCBRCpiForecastingSourceCode"
MLFLOW_TRACKING_URI=http://192.168.3.9:5000 \
  ML_LOG=1 make cb-comp-live-bottomup ARGS="--archive /Users/whitefox/Downloads/BusinessSurveyCBRCpiForecastingSourceCode"

make ml-gate ARGS="--experiment cb_comp_88_groups --champion-run <A'> --challenger-run <B'> --model combo3 --window h1 --robustness-window h3 --robustness-window h6"
make MLFLOW_TRACKING_URI=http://192.168.3.9:5000 mlflow-export ARGS="--experiment cb_comp_88_groups --dry-run"
```

## 6. Ссылки

- [Спека P0.5](../superpowers/specs/2026-11-01-cb-comp-p05-mlflow-design.md), [план](../superpowers/plans/2026-11-01-cb-comp-p05-mlflow.md).
- [Карта live-модели](../guides/cb-comp-live-model-map.md) §0, §1.6, §1.10, §6.2, §8.
- [Скил `ml-engineer`](../../.kimi-code/skills/ml-engineer/SKILL.md) §6–§7: паспорт, gate, MLflow.
- [P0.6a: MLflow-зеркало shadow](2026-10-31-cb-comp-h1-shadow-p06.md) — соседний контур, JSON остаётся источником истины.
