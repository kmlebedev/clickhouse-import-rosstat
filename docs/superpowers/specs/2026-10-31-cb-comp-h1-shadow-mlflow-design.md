# P0.6a: MLflow-зеркало paper shadow `wk_mix_ex@v1` — спека

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419, sub-issue [#462](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/462). Роль `ml`. Зависит от #455 (P0.6): shadow-JSON `docs/research/cb-comp-h1-shadow/<YYYY-MM>.json` уже коммитится до cutoff, начиная с 2026-10. Протокол смены базы — гайд `docs/guides/cb-comp-live-model-map.md` §7.8; пороги и модель не меняются.

**Политика backend (уточнена владельцем 2026-10-31):** канонический/постоянный backend MLflow — **staging**, не локальный `build/mlflow`. Локальный сервер `127.0.0.1:5000` — dev/офлайн; run в нём не является live-приёмкой.

## 1. Зачем

P0.6 дал ежемесячный paper shadow: JSON месяца — источник истины, MLflow его не видит. Нужно **зеркало** в MLflow, чтобы:

- история shadow-месяцев была в control plane (UI/API) рядом с другими run'ами;
- по каждому месяцу можно было найти run по `month` + `json_sha256` и сверить прогнозы/факт;
- ежемесячный ритуал «после коммита JSON — залогировать месяц» был одной make-целью;
- backfill 2026-10 закрыл первый месяц в **staging**.

Зеркало **не** является gate-входом: `ml-gate` читает ClickHouse `v_ml_error_by_fold`, а shadow-строк там нет и в этом пункте не появляется. Зеркало **не** является деплойментом и не двигает alias.

## 2. Что заморожено и не меняется

- Shadow-JSON: схема, имена полей, значения, правило коммита до cutoff. Логгер только читает файл.
- `scripts/models/cb_comp/cb_comp_h1_shadow.py`, `cb_comp_h1_weekly_ex.py`, `cb_comp_h1_weekly.py`, `cb_comp_h1_bs_combo.py`, `cb_comp_live_bottomup.py` — код модели не импортируется и не меняется (тяжёлые зависимости не нужны логгеру).
- Модель `wk_mix_ex@v1`, `combo3`, контроли, пороги §7.8 (6/12 месяцев, 0,15 п.п., 0,6 п.п., правило `wk_mix_d`) — не меняются.
- ClickHouse: скрипт не пишет и не читает; DDL/GRANT нет.
- `ml-gate`, `forecast_log`, витрины, Go-код, `site-pages.json` — не меняются.

## 3. Источник истины и backend

| Слой | Роль | Источник истины |
|---|---|---|
| `docs/research/cb-comp-h1-shadow/<YYYY-MM>.json` | paper shadow месяца | **да**; коммитится до cutoff |
| **staging** MLflow run в `cb_comp_h1_shadow` | каноническое зеркало JSON | нет; удаляем и пересоздаём из JSON |
| `build/mlflow/` (локальный `127.0.0.1:5000`) | dev/офлайн backend; **не канонический** | нет; gitignored, не коммитится |
| ClickHouse `v_ml_error_by_fold` | gate-вход для `ml-gate` | нет; требует `ML_LOG=1`/экспорт, не этот пункт |

**Staging MLflow.** Host `ps-palmshell` (ssh `palmshell`), служба `mlflow.service`, backend `sqlite:////var/lib/mlflow/mlflow.db`, артефакты `/var/lib/mlflow/artifacts`, API `0.0.0.0:5000` (служба слушает внешний интерфейс). Доступ с рабочей машины — `http://192.168.3.9:5000`; fallback — SSH-туннель:

```bash
MLFLOW_TRACKING_URI=http://192.168.3.9:5000 make cb-comp-h1-shadow-mlflow ARGS="--backfill"

# fallback
ssh -N -L 15001:127.0.0.1:5000 palmshell
MLFLOW_TRACKING_URI=http://127.0.0.1:15001 make cb-comp-h1-shadow-mlflow ARGS="--backfill"
```

Правила:

1. JSON — источник истины. Если run и JSON расходятся, прав JSON; run пересоздаётся (`--force`).
2. Run не содержит ничего, чего нет в JSON, кроме метаданных логирования (`source_path`, `logged_at`, `json_sha256`).
3. MLflow называется зеркалом/control plane, не «деплойментом»; alias `champion` не создаётся, модель не регистрируется.
4. Секреты не логируются; скрипт не читает `~/.config/rosstat/env`.
5. Канонический run — только staging. Локальный `build/mlflow` используется для dev и изолированных тестов; его run не считается live-приёмкой.

## 4. Схема run

**Experiment:** `cb_comp_h1_shadow` (создаётся логгером при отсутствии; в staging — experiment id 1).

**Run name:** `shadow__<YYYY-MM>__wk_mix_ex@v1`.

**Tags** (инвариант схемы):

| Tag | Значение |
|---|---|
| `decision` | `shadow` |
| `stage` | `paper_shadow` |
| `champion` | `combo3` |
| `challenger` | `wk_mix_ex@v1` |
| `model_version` | `wk_mix_ex@v1` |
| `source` | `json` |
| `json_sha256` | sha256 байтов shadow-JSON |
| `month` | `YYYY-MM` |
| `cutoff` | дата cutoff месяца из JSON |
| `version` | `wk_mix_ex@v1` |
| `spec` | путь спеки из JSON |
| `git_sha` | git SHA кода из JSON |
| `run_date` | дата прогона forecast из JSON |
| `resolved` | `1`, если `actual` заполнен, иначе `0` |
| `truth_primary`, `truth_secondary` | определения истины из JSON |

**Params** (только скаляры; без абсолютных локальных путей):

- `month`, `cutoff`, `run_date`, `version`, `v1_code_sha`, `git_sha`, `spec`, `unit`, `json_sha256`;
- `source_path` — путь JSON относительно корня репозитория (вне корня — basename);
- `input.<name>_sha256` — все `inputs_sha256` из JSON;
- `asof.<key>` — as-of входов; `asof.utilities_last_point.<series>`;
- `served.model`, `served.fallback_level`;
- `s0.identical`, `s0.months`.

**Metrics** (float):

- `pred.<model>` для `combo3`, `wk_mix_ex`, `wk_mix_d`, `live_cb_wk`;
- `pred.<component>` для `live_cb`, `bs_econ16`, `bs_rozn16`, `wk_agg_ex`, `wk_agg_d`;
- `served.value`;
- `weekly_features.<key>`;
- `s0.max_abs_diff.<model>`;
- при наличии `actual`: `actual.t_live_rt`, `actual.t_live`, `error_bp`, `abs_error_bp`.

**`error_bp`:** `(served.value − actual.t_live_rt) × 100`, т.е. ошибка `served`-прогноза (challenger) против primary-истины `T_LIVE_rt` в базисных пунктах (1 п.п. = 100 б.п.); `abs_error_bp = |error_bp|`. Пороги §7.8 записаны в п.п. (0,15 п.п. = 15 б.п., 0,6 п.п. = 60 б.п.) и здесь не пересчитываются. Если `actual` есть, обязателен finite `t_live_rt`; `t_live` опционален. `actual = null` — метрик `actual.*`/`error_bp` нет.

**Artifacts:**

- `shadow.json` — точные байты источника (`log_text`);
- `run_summary.json` — производная схема run'а (`log_dict`): tags/params/metrics, `errors_bp` по моделям против `actual.t_live_rt`, `source_path`, `json_sha256`, `logged_at`.

**Run status:** `FINISHED` после записи всех параметров/метрик/артефактов; при исключении — `FAILED`.

## 5. Идемпотентность и backfill

- Ключ идемпотентности: `(tags.month, tags.json_sha256)`. Перед созданием run — `MlflowClient.search_runs` по эксперименту с фильтром `tags.month = '<M>' and tags.json_sha256 = '<sha>'`.
- Найден run и нет `--force` → `skip`, печатается существующий `run_id`; новых run'ов не появляется.
- `--force` → создаётся новый run (как в `mlflow_backfill.py`); старый не удаляется и не меняется. На staging `--force` без явного решения владельца не применять.
- Изменение JSON (новый sha) → новый run; имя отображается одинаково, различаются `json_sha256` и `start_time`. Засчитывается версия JSON, закоммиченная до cutoff (правило P0.6), а не «последний run».
- **Backfill:** `--backfill` сканирует `docs/research/cb-comp-h1-shadow/` по `^\d{4}-\d{2}\.json$`, сортирует по месяцу и логирует каждый; `--month`/`--path` сужают набор. Ошибка одного файла не останавливает остальные, итоговый код 1.
- **Dry-run:** `--dry-run` валидирует и печатает план (run name, tags, метрики, артефакты), MLflow не импортируется и не вызывается; **URI не требуется**.

## 6. CLI и make

```
python3 scripts/models/cb_comp/cb_comp_h1_shadow_mlflow.py \
  [--month YYYY-MM] [--path FILE] [--backfill] [--dry-run] [--force] \
  [--tracking-uri URI]
```

- Без `--month`, `--path`, `--backfill` — ошибка.
- `--month` и `--path` вместе: месяц в JSON обязан совпасть.
- **URI обязателен для реального прогона:** `--tracking-uri` или `MLFLOW_TRACKING_URI` (staging `http://192.168.3.9:5000`; fallback — туннель `http://127.0.0.1:15001`). Локальный `http://127.0.0.1:5000` **не подставляется молча**. Без URI — ошибка с подсказкой про staging, туннель и `--tracking-uri`.
- `--dry-run` работает без URI.
- Валидация JSON: обязательные поля P0.6 (`version`, `month`, `cutoff`, `run_date`, `git_sha`, `models`, `served`, `asof`, `inputs_sha256`, `truth`), `version == wk_mix_ex@v1`, `run_date <= cutoff`, все 4 модели finite, `served` в цепочке fallback, sha256 — 64 hex. Тяжёлые модули модели не импортируются.

Make:

- `MLFLOW_TRACKING_URI=http://192.168.3.9:5000 make cb-comp-h1-shadow-mlflow ARGS="--backfill"` — месячный/backfill-прогон в staging (fallback — туннель `http://127.0.0.1:15001`);
- `make cb-comp-h1-shadow-mlflow ARGS="--month 2026-10 --dry-run"` — план без URI;
- `make cb-comp-h1-shadow-mlflow-test` — офлайн-тесты.
- Цель не подставляет локальный `build/mlflow` молча: без `MLFLOW_TRACKING_URI`/`--tracking-uri` (и без `--dry-run`) печатает staging-инструкцию и завершается с ошибкой.

## 7. Тесты (офлайн)

Временный SQLite tracking URI (`sqlite:///<tmp>/mlflow.db`), реальный `MlflowClient`, артефакты — во временном каталоге (chdir в tmp); без сети, ClickHouse и `build/mlflow`. **25 тестов.**

1. Схема: experiment/run name, tags, params, metrics, артефакты; `shadow.json` побайтно равен источнику; `json_sha256` верен.
2. Идемпотентность: второй прогон — тот же run id, число run'ов не растёт; `--force` — новый run.
3. `--dry-run` без URI: нет experiment/run, план содержит run name и sha.
4. Нет URI для реального прогона: `MissingTrackingUriError`/код 1 с сообщением про staging и `--tracking-uri`; run не создан.
5. `resolve_tracking_uri`: аргумент приоритетнее `MLFLOW_TRACKING_URI`; staging-like URI из env подхватывается; пустой env → нет URI.
6. `actual = null`: нет `actual.*`/`error_bp`; `resolved=0`.
7. `actual` есть: `error_bp`/`abs_error_bp` считаются от `served` и `t_live_rt`.
8. Невалидный JSON: нет поля, `version` не v1, `run_date > cutoff`, не finite модель, битый `served`, короткий sha256 — код 1, run не создан.
9. Backfill: два месяца в temp-каталоге → два run'а; повтор → оба skip; env-URI подхватывается.
10. `--month` + `--path` mismatch, неверный `--month` — ошибка до MLflow.

## 8. Что не доказывается

- Зеркало не добавляет статистических доказательств: это копия JSON, а не новый прогон модели.
- Run в `cb_comp_h1_shadow` не виден `ml-gate` и не заменяет `ml_runs`/`v_ml_error_by_fold`; gate-вход — отдельный пункт (`ML_LOG`/экспорт).
- Один shadow-месяц (2026-10) ничего не решает; правило §7.8 (6/12 месяцев) не меняется.
- Канонический staging run `9c53630947894b2682bae2d0d1908762` (experiment 1) подтверждает только зеркалирование; live-точность по одному месяцу не оценивается.
- Локальный dev-run `b034a74df13743f9ad7a79b53fb79054` (experiment 3, `build/mlflow`) — не канонический и в приёмку не входит.
- Артефакты MLflow не являются единственной копией: JSON коммитится в git.
- Ревизии/винтажи входов не проверяются — это свойства P0.6, не зеркала.
- Staging-MLflow проверяется через SSH-туннель; доступность сервиса/бэкапы staging — вне объёма пункта.

## 9. Приёмка

1. Спека/план — первые коммиты ветки.
2. `make cb-comp-h1-shadow-mlflow-test` — зелёный, офлайн (25 тестов).
3. `MLFLOW_TRACKING_URI=http://192.168.3.9:5000 make cb-comp-h1-shadow-mlflow ARGS="--backfill"` — staging run 2026-10 существует (`9c536…`, experiment 1), повтор — `skipped 9c536…`; staging run не перезаписывается. Первичная приёмка шла через туннель `15001`, после #486 повторена напрямую.
4. `make cb-comp-h1-shadow-mlflow ARGS="--month 2026-10 --dry-run"` — план без URI и без записи.
5. `make docs-check`, `make all` — зелёные; `git status` — только ожидаемые файлы; `build/mlflow` не коммитится.
6. README, гайд §8.1–§8.3, роадмап обновлены; статус пункта — In Review в PR.
