# Спека: MLflow и model registry (`infra/4.2`)

**Дата:** 18 октября 2026 г.
**Пункт роадмапа:** [`infra/4.2`](../ROADMAP_INFRA.md) — второй срез фазы 4 «ML-инфраструктура».
**Issue:** [#183](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/183).
**Зависимость:** `infra/4.1` (#178) принята: `ml_runs`, `ml_predictions`, витрины `v_ml_runs`/`v_ml_leaderboard`/`v_ml_error_by_fold`, `scripts/ml_experiment.py`, `ML_LOG=1`.
**Решения владельца (2026-10-18):** backend MLflow — **SQLite** (не PostgreSQL); артефакты — **локальный filesystem** для dev и staging (S3/MinIO не поднимаем); MySQL — официально поддерживаемая замена на будущее, без правок кода.
**Статус:** спека дизайна; реализация — после утверждения плана.
**Связанный ресерч:** [2026-10-18-ml-experiment-management.md](../research/2026-10-18-ml-experiment-management.md).

---

## 1. Зачем

`infra/4.1` дала машиночитаемый паспорт прогона и fold-предсказания в ClickHouse, но у истории экспериментов нет UI, сравнения run'ов и реестра моделей. `infra/4.2` добавляет control plane MLflow:

- run виден в MLflow UI: параметры, метрики, теги, артефакты;
- model registry с версиями и алиасами `champion`/`challenger`;
- run'ы из MLflow экспортируются в `ml_runs`/`ml_predictions` для Grafana/MCP;
- экспорт идемпотентен: повторный прогон не плодит строк.

ClickHouse остаётся аналитической плоскостью; MLflow — system of record для экспериментов.

---

## 2. Границы

**В объёме:**

- lifecycle MLflow-сервера для dev: `make mlflow-up/down/status`, SQLite + локальные артефакты, `--serve-artifacts`;
- MLflow-приёмник в `scripts/ml_experiment.py` (ленивый импорт, опциональность, fallback);
- логирование CatBoost-модели из `cpi_nowcast` (`mlflow.catboost.log_model`);
- `scripts/mlflow_export.py` — экспорт run'ов в ClickHouse, идемпотентный;
- `scripts/mlflow_registry.py` — регистрация версии, aliases `challenger`/`champion`, листинг;
- Makefile-цели и переменные, staging-юнит `mlflow.service`, деплой/смоук, локальные пути на staging;
- офлайн-тесты Python, `make all`, `make docs-check`, `make docs-build`, `make mcp-check`;
- документация, `docs/ROADMAP_INFRA.md`, issue #183 и Project V2.

**Вне объёма:**

- PostgreSQL, MySQL-сервис, MinIO/S3 (прод-хранилище — отдельный пункт);
- промоушен по gate DM/MCS, shadow/canary, live-мониторинг, `forecast_log`, `v_ml_live_accuracy`, алерты (`4.3`);
- Grafana-дашборд MLflow;
- аутентификация/SSO MLflow (доступ только localhost/SSH-туннель);
- регистрация моделей `key_rate_model`: методы — функции statsmodels без сохраняемого объекта; реестр в 4.2 демонстрируется на CatBoost `cpi_nowcast`, key-rate — follow-up в `4.3`;
- изменение схемы `ml_runs`/`ml_predictions` (каноническая схема 4.1 не меняется).

---

## 3. Backend store и artifact store: решение

Проверено по официальной документации MLflow 3.x и ClickHouse:

| Вариант | Подходит | Комментарий |
|---|---|---|
| SQLite | ✅ | backend по умолчанию, `sqlite:///…`; ноль новых сервисов; model registry работает (database-backed store); один писатель |
| MySQL | ✅ | `mysql+pymysql://…`; отдельный MySQL-сервис — та же категория зависимости, что PostgreSQL; нужен только при конкурентной многопользовательской записи |
| PostgreSQL | ✅ | отклонён владельцем: лишний сервис |
| MSSQL | ✅ | не рассматриваем |
| ClickHouse как backend | ❌ | MLflow поддерживает только SQLAlchemy-диалекты `sqlite`, `mysql`, `mssql`, `postgresql`; ClickHouse не транзакционный и не держит семантику миграций Alembic |
| MySQL-engine в ClickHouse | ❌ не заменяет backend | `ENGINE = MySQL` — клиент к внешнему MySQL (SELECT/INSERT), не MySQL-сервер; `RENAME`/`CREATE TABLE`/`ALTER` запрещены |

**Решение:** dev и staging — `sqlite:///…` + локальный каталог артефактов, сервер с `--serve-artifacts`. Переход на MySQL позже — смена `--backend-store-uri` и `MLFLOW_TRACKING_URI`, без правок экспорта/реестра.

**Почему `--serve-artifacts`:** клиент загружает артефакты через HTTP сервера, а не пишет в чужой filesystem; это единственный корректный режим, когда прогон и сервер — разные процессы/машины. S3/MinIO не нужен.

Источники:

- [MLflow Backend Stores](https://mlflow.org/docs/latest/self-hosting/architecture/backend-store/) — диалекты `sqlite`, `postgresql`, `mysql`, `mssql`; model registry требует database-backed store.
- [MLflow Artifact Stores](https://mlflow.org/docs/latest/self-hosting/architecture/artifact-store/) — локальный filesystem, `--default-artifact-root`, проксирование артефактов.
- [ClickHouse MySQL database engine](https://clickhouse.com/docs/en/engines/database-engines/mysql) — обмен запросами с внешним MySQL.
- [ClickHouse MySQL table engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/mysql) — `SELECT`/`INSERT` к внешнему MySQL.

---

## 4. Архитектура и поток данных

```
Python-прогон (cpi_nowcast / key_rate_model)
  │  ML_LOG=1  → паспорт + fold-предсказания
  │  MLFLOW=1  → mlflow.log_* (run, params, tags, metrics)
  ▼
MLflow tracking server (SQLite + local artifacts, 127.0.0.1:5000)
  │  run.json, predictions.jsonl, model/ — артефакты прогона
  │  model registry: cpi_nowcast_catboost_direct, aliases challenger/champion
  ▼
scripts/mlflow_export.py  (идемпотентно, ReplacingMergeTree)
  ▼
ClickHouse: ml_runs / ml_predictions → v_ml_runs / v_ml_leaderboard / v_ml_error_by_fold
  ▼
Grafana / MCP (kimi_reader)
```

**Роли:**

- MLflow — control plane: UI, сравнение run'ов, model registry, aliases.
- ClickHouse — аналитическая реплика для SQL/Grafana/MCP. Схема 4.1 не меняется.
- `run_id` в ClickHouse = канонический UUID, выведенный из MLflow run id (32 hex → UUID с дефисами). Это позволяет повторно экспортировать один и тот же run без дублей.
- Прямая запись в ClickHouse из `ml_experiment.py` сохраняется (поведение 4.1); `mlflow_export.py` закрывает разрыв, если ClickHouse был недоступен или run создан только в MLflow. Обе записи идемпотентны по `run_id`.

**Почему не наоборот:** issue и ресерч фиксируют MLflow как control plane, а ClickHouse как реплику; экспорт в ClickHouse — требование приёмки.

---

## 5. Модель данных и идентификаторы

Схема `ml_runs`/`ml_predictions` из 4.1 не меняется. Новые run'ы получают `run_id`, выведенный из MLflow run id:

```python
canonical_run_id(mlflow_run_id) -> str(uuid.UUID(hex=mlflow_run_id))
# fallback: str(uuid.uuid5(uuid.NAMESPACE_URL, f"mlflow:{mlflow_run_id}"))
```

- `run.json` (артефакт MLflow) содержит ту же строку `ml_runs`, что пишется напрямую в ClickHouse; `run_id` — уже канонический.
- `predictions.jsonl` (артефакт MLflow) содержит строки `ml_predictions` в формате JSONEachRow; `run_id` — тот же.
- Экспорт перезаписывает `run_id` в обеих структурах каноническим значением — это делает его устойчивым к старым `run.json`, где мог остаться локальный UUID.
- Старые run'ы 4.1 с локальными UUID не экспортируются и не конфликтуют.

---

## 6. Компоненты

### 6.1 MLflow-сервер (dev)

Makefile-переменные:

```make
MLFLOW_VERSION      ?= 3.17.1
MLFLOW_HOME         ?= $(BUILD_DIR)/mlflow
MLFLOW_PORT         ?= 5000
MLFLOW_TRACKING_URI ?= http://127.0.0.1:$(MLFLOW_PORT)
MLFLOW_DB           := $(abspath $(MLFLOW_HOME)/mlflow.db)
MLFLOW_ARTIFACTS    := $(abspath $(MLFLOW_HOME)/artifacts)
MLFLOW_PID          := $(MLFLOW_HOME)/mlflow.pid
MLFLOW_LOG          := $(MLFLOW_HOME)/mlflow.log
MLFLOW              ?= 0
MLFLOW_PKG          := $(if $(filter 1,$(MLFLOW)),--with "mlflow==$(MLFLOW_VERSION)",)
MLFLOW_ENV          := $(if $(filter 1,$(MLFLOW)),MLFLOW_TRACKING_URI=$(MLFLOW_TRACKING_URI),MLFLOW_TRACKING_URI=)
MLFLOW_HTTP_ENV     := MLFLOW_HTTP_REQUEST_TIMEOUT=10 MLFLOW_HTTP_REQUEST_MAX_RETRIES=2
```

`make mlflow-up` запускает:

```
uv run --with mlflow==$(MLFLOW_VERSION) --python 3.12 \
  mlflow server --host 127.0.0.1 --port $(MLFLOW_PORT) --workers 1 \
  --backend-store-uri sqlite:///$(MLFLOW_DB) \
  --default-artifact-root $(MLFLOW_ARTIFACTS) --serve-artifacts
```

в фоне (`nohup`), PID в `$(MLFLOW_PID)`, ожидание `/health` до 60 с. `sqlite:///` + абсолютный путь даёт четыре слэша. `--workers 1` — потому что SQLite допускает одного писателя.

`make mlflow-down` останавливает процесс и удаляет PID; `make mlflow-status` проверяет `/health`.

Артефакты: `build/mlflow/artifacts` (gitignored, `build/` уже в `.gitignore`). БД: `build/mlflow/mlflow.db`.

### 6.2 `scripts/ml_experiment.py` — MLflow-приёмник

Остаётся stdlib-only по умолчанию. MLflow импортируется лениво и только при `ML_LOG=1` + непустом `MLFLOW_TRACKING_URI`.

- `ExperimentRun.__init__` сохраняет `self._artifact_paths`, `self._models`, `self._mlflow = None` и вызывает `_start_mlflow()`.
- `_start_mlflow()`:
  - `uri = os.environ.get("MLFLOW_TRACKING_URI")`; если `not self.enabled or not uri` — выход;
  - `import mlflow`; `ImportError` → warning, MLflow выключен;
  - `mlflow.set_tracking_uri(uri)`, `mlflow.set_experiment(self.experiment)`, `mlflow.start_run(run_name=self.run_name, tags={...})`;
  - `self.run_id = canonical_run_id(run.info.run_id)`;
  - tags: `experiment`, `git_sha`, `git_branch`, `git_dirty`, `config_hash`, `sample_hash`, `data_asof`, `spec_path`, `author`;
  - params: скалярные ключи `config`; `config.json` — артефактом (лимит длины param);
  - любая ошибка запуска — warning, `self._mlflow = None`, прогон продолжается.
- `log_model(name, model)` — опциональный метод: `self._models.append((name, model))`.
- `log_artifacts(paths)` — дополнительно сохраняет реальные `Path` для upload.
- `finish(status, notes)` — после построения `run_row`/`prediction_rows`, **до** записи в ClickHouse:
  - `mlflow.log_text(canonical_json(run_row), "run.json")`;
  - `mlflow.log_text(json_each_row(prediction_rows), "predictions.jsonl")`;
  - `mlflow.log_artifact(str(path))` для каждого артефакта;
  - `mlflow.catboost.log_model(model, name=name)` для каждой модели;
  - `mlflow.log_metrics(flatten_metrics(self.metrics))` — только конечные числа, ключи санитизируются `[A-Za-z0-9_.-]`; `primary_metric` — tag;
  - `mlflow.end_run(status="FINISHED" if status == "finished" else "FAILED")`.
- Любая ошибка MLflow-блока — warning, попытка `end_run(status="FAILED")`, прогон не падает. Существующая запись в ClickHouse и fallback не меняются; возвращаемые значения `clickhouse`/`local`/`disabled` сохраняются.

### 6.3 `scripts/cpi_nowcast.py` — модель

После `current_nowcast(...)` (модели обучаются на полной выборке; `fit_catboost_direct.last_fit` — последний full-sample CatBoost) и при наличии метода `log_model`:

```python
if logger is not None and getattr(logger, "log_model", None) and nowcast is not None:
    fit = fit_catboost_direct.last_fit
    if fit is not None:
        logger.log_model("model", fit.model)
```

Обёрнуто в `try/except` с warning. Имя артефакта — `model` (его ждёт `mlflow_registry.py`); имя зарегистрированной модели задаёт CLI реестра. Fake logger в тестах не ломается: `getattr`.

### 6.4 `scripts/mlflow_export.py`

CLI:

```
python3 scripts/mlflow_export.py \
  --tracking-uri http://127.0.0.1:5000 \
  --experiment cpi_nowcast --experiment key_rate_model \
  --since 2026-10-18 --limit 100 --dry-run
```

- `mlflow` импортируется лениво в `main()`; чистые функции (`canonical_run_id`, `build_run_row`, `build_prediction_rows`, `parse_jsonl`, `ch_datetime`) тестируются без MLflow.
- Для каждого эксперимента: `client.get_experiment_by_name`; `client.search_runs([exp_id], order_by=["attributes.start_time ASC"], max_results=limit)`; фильтр статуса и `--since` — клиентский (FINISHED/FAILED; RUNNING пропускается с warning).
- Для каждого run: скачать `run.json` через `client.download_artifacts(run_id, "run.json", tmpdir)`; нет файла — warning + skip (run создан не нашим хелпером). `predictions.jsonl` — если есть.
- `run_id` перезаписывается каноническим UUID в `run_row` и во всех предсказаниях.
- `finished_at`/`created_at` нормализуются в `YYYY-MM-DD HH:MM:SS` (ClickHouse `DateTime`); `T` и таймзона убираются.
- Вставка: `ch_exec(insert_sql("ml_predictions", rows))`, затем `ch_exec(insert_sql("ml_runs", [row]))` — тот же порядок, что в `ml_experiment.finish`.
- `--dry-run` — только печать `experiment run_id folds` и проверка артефактов.
- Идемпотентность: те же строки при повторе; `ReplacingMergeTree` схлопывает по `(experiment, run_id)` и `(experiment, run_id, model, window, target_date)`.
- Код возврата 0 — все ок; 1 — хотя бы один run не экспортирован (нет `run.json`, ошибка ClickHouse).

### 6.5 `scripts/mlflow_registry.py`

CLI:

```
python3 scripts/mlflow_registry.py register --run-id <mlflow-run-id> --name cpi_nowcast_catboost_direct
python3 scripts/mlflow_registry.py promote  --name cpi_nowcast_catboost_direct --version 1
python3 scripts/mlflow_registry.py list     --name cpi_nowcast_catboost_direct
```

- `register`: `client.create_registered_model(name)` (если нет), `client.create_model_version(name, source=f"runs:/{run_id}/model", run_id=run_id)`, затем `client.set_registered_model_alias(name, "challenger", version.version)`. Печать `registered name vN; alias challenger -> vN`.
- `promote`: `client.set_registered_model_alias(name, "champion", version)`; печать `alias champion -> vN`.
- `list`: `client.search_registered_models()` / `search_model_versions`; таблица `name version status aliases run_id`; aliases из `version.aliases` с fallback на `client.get_model_version_by_alias`.
- `--tracking-uri` по умолчанию `MLFLOW_TRACKING_URI` или `http://127.0.0.1:5000`.
- Функции принимают `client` параметром — тесты используют fake-клиент.
- Промоушен в 4.2 ручной (gate — 4.3); скрипт не проверяет метрики.

### 6.6 Makefile-цели

- `mlflow-up`, `mlflow-down`, `mlflow-status`;
- `mlflow-export` — `uv run --with mlflow==$(MLFLOW_VERSION) --python 3.12 python3 scripts/mlflow_export.py $(ARGS)`;
- `mlflow-register`, `mlflow-promote`, `mlflow-models` — обёртки над `scripts/mlflow_registry.py` с `ARGS`;
- `cpi-nowcast` и `key-rate-model` получают `$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) …`; при `MLFLOW=0` передаётся пустой `MLFLOW_TRACKING_URI`, чтобы переменная из `~/.config/rosstat/env` не включала MLflow случайно;
- все новые цели — в `.PHONY`.

### 6.7 Staging

`deploy/staging/mlflow.service`:

```ini
[Unit]
Description=MLflow tracking server (SQLite + local artifacts)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
StateDirectory=mlflow
ExecStartPre=/bin/mkdir -p /var/lib/mlflow/artifacts
ExecStart=/root/.local/bin/uv run --with mlflow==3.17.1 --python 3.12 \
  mlflow server --host 127.0.0.1 --port 5000 --workers 1 \
  --backend-store-uri sqlite:////var/lib/mlflow/mlflow.db \
  --default-artifact-root /var/lib/mlflow/artifacts --serve-artifacts
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Версия в юните синхронизируется с `MLFLOW_VERSION` в Makefile (комментарий в файле).

Makefile:

- `deploy-staging-mlflow`: `staging-precheck` → scp юнита → `systemctl daemon-reload` → `install -d -m 755 /var/lib/mlflow /var/lib/mlflow/artifacts` → прогрев `uv run --with mlflow==$(MLFLOW_VERSION) python -c "pass"`;
- `deploy-staging-mlflow` входит в `deploy-staging` (после `deploy-staging-mcp`), в `systemctl enable --now dagu mcp-clickhouse mlflow`, в `upgrade-staging` (рестарт `mlflow`);
- `deploy-staging-smoke`: `systemctl is-active --quiet mlflow` + `curl` `/health` с ограниченным ретраем (10 попыток × 2 с: холодный старт `uv run` может не ответить с первого раза);
- `deploy/staging/README.md`: `/var/lib/mlflow`, туннель `ssh -N -L 5000:127.0.0.1:5000 palmshell`, UI, бэкап (sqlite backup + tar artifacts с общей меткой даты), отсутствие auth (только localhost);
- обновление версии: бэкап → правка версии в юните и `MLFLOW_VERSION` → `make deploy-staging-mlflow` → `systemctl stop mlflow` → `mlflow db upgrade sqlite:////var/lib/mlflow/mlflow.db` → `systemctl start mlflow` → `make deploy-staging-smoke`. `systemctl enable --now` не перезапускает уже запущенный сервис, поэтому без stop/start новая версия не вступит в силу, а `db upgrade` под живым сервером трогает SQLite, которой владеет процесс.

---

## 7. Операции и безопасность

- MLflow слушает только `127.0.0.1`; доступ — SSH-туннель. Наружу не выставляется.
- Паролей/токенов в MLflow нет; `run.json`/`predictions.jsonl` не содержат секретов (config — только CLI/дефолты; env — Python/пакеты/OS).
- Бэкап dev/staging: `sqlite3 mlflow.db ".backup ..."` + `tar` каталога артефактов; при обновлении версии — бэкап до `mlflow db upgrade`. Миграция выполняется на **остановленном** сервере (SQLite — один писатель), после чего сервис запускается заново; `systemctl enable --now` уже работающий сервис не перезапускает.
- SQLite — один писатель; `--workers 1`; конкурентные прогоны сериализуются. При росте — MySQL (URI change).

---

## 8. Тесты

- `scripts/ml_experiment_test.py` — новые тесты с fake `mlflow` в `sys.modules`: run создан, tags/params записаны, `run.json`/`predictions.jsonl`/artifacts/model залогированы, `end_run` вызван; при пустом `MLFLOW_TRACKING_URI` — ни одного вызова; при исключении MLflow — warning и продолжение; `run_id` — UUID. `MLFLOW_TRACKING_URI` добавить в `_ML_ENV_VARS` в `ml_experiment_test.py`, `cpi_nowcast_test.py`, `key_rate_model_test.py`.
- `scripts/mlflow_export_test.py` — canonical UUID, парсинг `run.json`/`predictions.jsonl`, нормализация `DateTime`, dry-run без `ch_exec`, отсутствующий `run.json` → skip, идентичная форма SQL при повторе.
- `scripts/mlflow_registry_test.py` — fake client: register → challenger, promote → champion, list.
- `scripts/cpi_nowcast_test.py`, `scripts/key_rate_model_test.py` — без регрессий.
- `make all`, `make docs-check`, `make docs-build`, `make mcp-check`.
- Секреты: assert, что в `run.json`/`predictions.jsonl` нет `password`/`token`/`secret`.

---

## 9. Живая приёмка

1. `make mlflow-up` → `make mlflow-status` = `/health` 200; UI `http://127.0.0.1:5000`.
2. `ML_LOG=1 MLFLOW=1 make cpi-nowcast` → run `cpi_nowcast` в MLflow, артефакты `run.json`, `predictions.jsonl`, `model/`.
3. `make mlflow-export` → `v_ml_runs`/`v_ml_leaderboard`/`v_ml_error_by_fold` видят run; `count() FINAL` = 1; число fold-строк совпадает.
4. Повторный `make mlflow-export` → те же counts через `FINAL`; идемпотентно.
5. `make mlflow-register RUN_ID=<id> NAME=cpi_nowcast_catboost_direct` → `challenger -> v1`; `make mlflow-promote NAME=… VERSION=1` → `champion -> v1`; `make mlflow-models` показывает оба; `get_model_version_by_alias` возвращает v1.
6. `ML_LOG=1 MLFLOW=1 make key-rate-model` → run в MLflow; экспорт → `unit='bp'`.
7. `make all`, `make mcp-check`, `make docs-check`, `make docs-build` — зелёные.
8. Staging: `make deploy-staging-mlflow`, туннель, `/health` 200, `make upgrade-staging` smoke.
9. `make mlflow-down` — dev-сервер остановлен.

---

## 10. Риски

- SQLite — один писатель; конкурентные прогоны могут дать `database is locked`; решение — сериализация или MySQL.
- MLflow недоступен — `ml_experiment` warning + ClickHouse/local fallback; экспорт — CLI-ошибка, не тихий no-op.
- Локальные артефакты — потеря хоста = потеря артефактов; бэкап ручной; S3 — отдельный пункт.
- Нет auth — только localhost/SSH-туннель.
- Версия MLflow — пин `3.17.1`; обновление через `mlflow db upgrade` с бэкапом.
- `uv run` первый старт скачивает mlflow (~200 МБ), кэш uv; staging прогрев в деплое.
- Model registry только CatBoost; key-rate — follow-up 4.3.
- `ReplacingMergeTree` без version — повторный экспорт идентичных строк безопасен; изменение `run.json` для того же run_id — follow-up.

---

## 11. Файлы

**Новые:**

- `scripts/mlflow_export.py`, `scripts/mlflow_export_test.py`;
- `scripts/mlflow_registry.py`, `scripts/mlflow_registry_test.py`;
- `deploy/staging/mlflow.service`;
- `docs/superpowers/specs/2026-10-18-mlflow-model-registry-design.md` (эта спека);
- `docs/superpowers/plans/2026-10-18-mlflow-model-registry.md` (план).

**Изменяемые:**

- `Makefile` (переменные, цели, `.PHONY`);
- `scripts/ml_experiment.py`, `scripts/ml_experiment_test.py`;
- `scripts/cpi_nowcast.py`, `scripts/cpi_nowcast_test.py`;
- `scripts/key_rate_model_test.py` (изоляция env);
- `deploy/staging/README.md`, `deploy/staging/clickhouse-import-rosstat.env.example`;
- `ARCHITECTURE.md`, `README.md`, `docs/OPERATIONS.md`, `docs/CONFIGURATION.md`;
- `docs/ROADMAP_INFRA.md`;
- issue #183 и Project V2.

---

## 12. Критерий приёмки пункта

> Run виден в MLflow UI; алиасы `champion`/`challenger` работают; экспорт run'ов в ClickHouse идемпотентен; `make all` зелёный.

Дополнительно: `make mcp-check`, `make docs-check`, `make docs-build`, staging smoke.
