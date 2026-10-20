# Эксплуатация

> Часть документации проекта; входная точка — [README.md](../README.md).

## Оркестрация (Dagu)

Расписания описаны в `dagu/*.yaml`: `type: chain`, `overlap_policy: skip`, каждый шаг запускает бинарник с `CLICKHOUSE_IMPORT_STAT=<имя>`, ретраи — 3 попытки с паузой 300 с. Путь к бинарнику (`ROSSTAT_IMPORT_BIN`) и `CLICKHOUSE_URL` задаются в `base.yaml` Dagu; секреты лежат в файлах `/etc/dagu/secrets/` с правами `0600`, в репозитории их нет. Внутри домена зависимости задаются через `depends`; между DAG зависимостей нет (см. `gold.yaml`: шаг `cbr_currency_usd` стоит внутри DAG). Отдельный `dagu/dcf.yaml` использует именно это свойство DAG-а: `type: chain` исполняет `dcf_mine_plans` строго до `dcf_engine`, и «сид раньше расчёта» — гарантия расписания, а не только порядок ручных команд (Finding 3 финального ревью). **Шага уточнённого AISC-сида в `dagu/dcf.yaml` нет** — он применяется вручную (см. «Что импортируется» и «Ограничения»); DAG содержит ровно два шага, и придумывать третий не нужно.

Запуск одного DAG вручную: `dagu start dagu/gold.yaml` (dev-окружение Dagu должно видеть `ROSSTAT_IMPORT_BIN` и `CLICKHOUSE_URL` через `base.yaml`).

## ML-эксперименты

Реестр ML-прогонов (`infra/4.1`) состоит из schema-only импортёра `ml_registry` и опциональной записи из Python-скриптов. Сначала создайте таблицы и витрины, затем запускайте эксперименты:

```bash
make import STAT=ml_registry              # ml_runs, ml_predictions, v_ml_runs/v_ml_leaderboard/v_ml_error_by_fold; повтор идемпотентен (безопасен)
ML_LOG=1 make cpi-nowcast                 # паспорт прогона + fold-предсказания (unit='pp')
ML_LOG=1 make key-rate-model              # паспорт прогона + fold-предсказания (unit='bp')
ML_LOG=1 make shock-classifier            # веса шоков: предсказания unit='weight' и unit='probability'
ML_LOG=1 make cpi-monthly                 # месячный nowcast v2, short: 155 месяцев, unit='pp'
ML_LOG=1 make cpi-monthly ARGS="--sample long"   # long-sample: 292 месяца 2002-05…2026-08, старт 48
# экспериментальный быстрый режим (числа неофициальные, см. ниже)
ML_LOG=1 make cpi-monthly ARGS="--sample short --quick --jobs 4"
ML_LOG=1 make cpi-monthly ARGS="--sample long --quick --jobs 8"
```

`ML_LOG=1` включает запись; без переменной скрипты в ClickHouse не пишут. **`ml_registry` должен отработать до первого `ML_LOG=1`-прогона** — он создаёт таблицы `ml_runs`/`ml_predictions` и витрины; в расписании он уже есть (`dagu/ml.yaml`, ежедневно 06:07 МСК). Если ClickHouse недоступен, прогон не падает: паспорт и предсказания сохраняются локально в `build/ml-runs/<run_id>/` (`run.json` + `predictions.jsonl`, каталог переопределяется `ML_LOCAL_DIR`). Имя прогона — `ML_RUN_NAME`. Чтение агентом — только через витрины `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold` (сырые таблицы закрыты, проверка — `make mcp-check`).

**Классификатор шоков ставки — ручной прогон, в Dagu его нет.** `make shock-classifier` (спека `docs/superpowers/specs/2026-10-19-key-rate-shock-classifier-design.md` §8) читает ClickHouse и пишет три локальных артефакта в каталог прогона (по умолчанию текущий; `ARGS="--out-dir DIR"`): `shock_classifier_report.md` (явные шоки, покрытие каналов, метрики классификатора, веса, «с весами / без», ограничения), `shock_classifier_weights.csv` (as-of веса и признаки) и `shock_classifier_forecast.json` (as-of срез следующего заседания). **Прямых вставок в ClickHouse нет:** `ML_LOG=1` (или `ARGS="--log"`) создаёт прогон `key_rate_shock_classifier` и пишет паспорт, метрики и два вида предсказаний (`explicit_shock`, unit `probability`; `anomaly_weight`, unit `weight`) через `scripts/lib/ml_experiment.py`; сбой журнала — warning и не меняет код возврата. При отсутствии ClickHouse/данных прогон завершается кодом 1, а артефакты прошлого прогона удаляются. `make shock-classifier ARGS="--export-fixtures scripts/testdata/shock_classifier"` обновляет офлайн-фикстуры без ClickHouse.

**Быстрый режим `cpi_monthly` (`--quick`, `--jobs N`) — экспериментальный.** `--quick` считает только основное окно walk-forward (без robustness-окон и без фиксированного сплита статьи), `--jobs N` распределяет фолды по процессам (`CatBoost thread_count=1`); `--jobs` допустим только с `--quick`, `0`/`1` — последовательно, `N>1` — пул. В паспорте `ML_LOG` видны `config_json.quick` и `config_json.jobs`, в `run_name` — сегменты `quick`/`jobsN` (например, `cpi_monthly__long__quick__jobs8__start48__embargo0`). Quick-числа **неофициальные** и не сравнимы с полными прогонами; официальные short/long — только `make cpi-monthly` без флагов, и этот полный default не меняется (`--jobs` без `--quick` отвергается).

### MLflow (control plane, `infra/4.2`)

Локальный MLflow хранит историю экспериментов и реестр моделей: SQLite `build/mlflow/mlflow.db`, артефакты `build/mlflow/artifacts`, `127.0.0.1:5000`. Сервер запускается с `--artifacts-destination build/mlflow/artifacts --serve-artifacts` и без `--default-artifact-root`: новый эксперимент получает корень `mlflow-artifacts:/`, и клиент загружает артефакты через сервер (это важно и для staging через SSH-туннель). Если БД создана раньше, со старым локальным artifact root, у default experiment в SQLite остался локальный `artifact_location` — его нужно сбросить/пересоздать (или использовать новый эксперимент), иначе upload для него не будет проксироваться. Аутентификации нет, порт наружу не публикуется; на staging доступ — только через SSH-туннель. MLflow — ручной сервис: шага в `dagu/` нет, экспорт в ClickHouse — отдельная команда.

```bash
make mlflow-up                  # поднять сервер и дождаться /health; UI http://127.0.0.1:5000
make mlflow-status              # «MLflow отвечает на порту 5000» либо «не отвечает»
ML_LOG=1 make cpi-nowcast        # MLflow по умолчанию + run.json/predictions.jsonl/model
ML_LOG=1 make key-rate-model
make mlflow-backfill ARGS="--run-id <clickhouse-uuid>"   # старый ClickHouse-run → MLflow
make mlflow-export              # MLflow → ml_runs/ml_predictions
make mlflow-export ARGS="--since 2026-10-18 --limit 20 --dry-run"   # предпросмотр без записи
make mlflow-register ARGS="--run-id <mlflow-run-id> --name cpi_nowcast_catboost_direct"
make mlflow-promote  ARGS="--name cpi_nowcast_catboost_direct --version 1"
make mlflow-models   ARGS="--name cpi_nowcast_catboost_direct"
make mlflow-down
```

Ожидаемое поведение:

- `make mlflow-up` печатает порт, PID и путь к логу `build/mlflow/mlflow.log`; повторный вызов при живом сервере второй сервер не поднимает. PID-файл проверяется по идентичности процесса (`ps -p PID -o command=` содержит `mlflow`): устаревший или чужой PID удаляется, а не считается работающим сервером. `make mlflow-down` останавливает только сервер, поднятый make (по PID-файлу), и не трогает процесс, запущенный вне make; чужой PID из файла он тоже удаляет, не убивая процесс.
- `ML_LOG=1 make cpi-nowcast` создаёт run эксперимента `cpi_nowcast` с артефактами `run.json`, `predictions.jsonl`, `model/`; MLflow включён по умолчанию (`MLFLOW=0` — осознанно выключить). Перед прогоном `mlflow_preflight.py` проверяет `/health`: если API недоступен, скрипт явно спрашивает, запускать ли без лога; в не-TTY нужен `MLFLOW_ALLOW_NO_LOG=1`. В ClickHouse при доступной БД появляются та же строка `ml_runs` и fold-строки `ml_predictions`. Полный конфиг лежит в `run.json` (`config_json`), отдельного артефакта `config.json` нет. Прямой запуск скрипта без make выставляет те же короткие HTTP-таймауты MLflow (`MLFLOW_HTTP_REQUEST_TIMEOUT=10`, `MLFLOW_HTTP_REQUEST_MAX_RETRIES=2`), что и `MLFLOW_HTTP_ENV` в Makefile.
- `make mlflow-export` печатает `exported N runs, M predictions; skipped K`. Run без `run.json` (создан не нашим хелпером) пропускается с warning и команду не роняет; битый `run.json`/`predictions.jsonl`, ошибка скачивания артефакта или вставки в ClickHouse — код 1.
- `make mlflow-export` берёт прогоны **новыми первыми** (`start_time DESC`): при упоре в `--limit` повторный экспорт не застревает на старейшем батче. Если число выбранных прогонов равно `--limit`, печатается warning, что выборка может быть обрезана — увеличьте `--limit` или сузьте `--since`.
- RUNNING-прогоны экспорт пропускает: артефактов у них ещё нет. Если прогон был жёстко убит и остался RUNNING, закройте его вручную в MLflow UI (или `mlflow.end_run`), иначе `make mlflow-export` его никогда не увидит.
- Таймстемпы в `ml_runs`/`ml_predictions` хранятся как настенное время `YYYY-MM-DD HH:MM:SS`: суффикс таймзоны (`Z`, `+03:00`) отбрасывается **без конвертации** — `10:00:00Z` и `10:00:00+03:00` дают одно и то же значение.
- `make mlflow-register` печатает `registered <name> vN; alias challenger -> vN`, `make mlflow-promote` — `alias champion -> vN`, `make mlflow-models` — таблицу `name version status aliases run_id`. Промоушен ручной: скрипт не проверяет метрики (gate — `infra/4.3`). При листинге без `--name` посторонние имена моделей вне allowlist `[A-Za-z0-9._-]` пропускаются с warning, а не ломают вывод остальных.

**Промоушен и live-мониторинг (`infra/4.3`).** Gate: `make ml-gate ARGS="--champion-run <uuid> --challenger-run <uuid> --model ordered_logit"` — общие фолды `v_ml_error_by_fold`, RMSE, DM-HLN p<0,05, 95% блочный бутстрап CI, robustness-окна (`key_rate_model` — `expanding`/`rolling40`); решение `promote|reject|iterate` — в `build/ml-gate/<challenger-run>.json` и тегах MLflow. `--mcs` добавляет 90% MCS при >2 кандидатах (`--candidate-run`). Alias не двигается автоматически: для CPI — `make mlflow-promote`, для key-rate — ручной перевод writer'а на production. Shadow/canary: `make key-rate-forecast-log ARGS="--stage shadow"` (2 заседания), затем `ARGS="--stage canary"` (1 заседание); staged-метрики `cbr_rate:<track>:shadow|canary` не публикуются. Live-витрина — `v_ml_live_accuracy` (создаётся `make import STAT=key_rate_views`), live-оценка gate — `make ml-gate ARGS="--live --live-track cbr_rate:ordered_logit"`. Перед публикацией: `make ml-live-check` (production-строки на ближайшее заседание; `ARGS="--require-llm"` — если в статье LLM-трек). График алертов — `grafana/provisioning/alerting/ml-live.yaml`.

**MLflow недоступен — прогон не падает.** `ml_experiment.py` пишет warning, а паспорт и предсказания по-прежнему идут в ClickHouse или в локальный fallback `build/ml-runs/<run_id>/`; `make mlflow-export` — отдельный ручной шаг и догоняет run'ы, когда сервер поднят. Если недоступен ClickHouse, export завершается кодом 1 — это не тихий no-op.

**`FINISHED` в MLflow не гарантирует строк в ClickHouse.** `finish()` логирует run в MLflow до вставки в ClickHouse: если вставка затем падает, MLflow-run уже закрыт `FINISHED`, а паспорт и fold-предсказания сохраняются в локальном fallback `build/ml-runs/<run_id>/` (паспорт пишется последним, поэтому fold-строки без паспорта — законное состояние сбоя). Повторный `make mlflow-export` проигрывает `run.json`/`predictions.jsonl` из артефактов и идемпотентно дописывает недостающие строки.

Проверка идемпотентности (повторный экспорт не должен менять числа):

```sql
SELECT count() FROM ml_runs FINAL;
SELECT count() FROM ml_predictions FINAL;
```

Staging: `make deploy-staging-mlflow`, туннель `ssh -N -L 5000:127.0.0.1:5000 palmshell`, UI `http://localhost:5000`; бэкап (`sqlite3 … ".backup"` + `tar` артефактов) и обновление версии (бэкап → `systemctl stop mlflow` → `mlflow db upgrade` → `systemctl start mlflow` → `make deploy-staging-smoke`) — `deploy/staging/README.md`. `make upgrade-staging` версию MLflow **не мигрирует**: он только копирует юнит и перезапускает сервис, поэтому смена версии — всегда последовательность stop → `db upgrade` → start.

## Деплой на staging

Разворачивает четыре компонента на хост `palmshell`: бинарник `clickhouse-import-rosstat`, планировщик `dagu` v2.18.2, MCP-сервер `mcp-clickhouse` 0.7.0 (HTTP, read-only) и MLflow (`127.0.0.1:5000`, SQLite + локальные артефакты). ClickHouse и Grafana на хосте не трогаются. Артефакты — `deploy/staging/`, порядок и отладка — `deploy/staging/README.md`.

```bash
# 1. Файл секретов на хосте (один раз; значения задаёт владелец). Токен: openssl rand -hex 32
ssh palmshell 'install -m 600 /dev/null /etc/clickhouse-import-rosstat.env'
#    вписать по образцу deploy/staging/clickhouse-import-rosstat.env.example

# 2. Развернуть и создать пользователя MCP
make deploy-staging
CH_SETUP_PASSWORD=<пароль default> make mcp-user-staging

# 3. Проверить end-to-end через туннель
ssh -N -L 8123:127.0.0.1:8123 palmshell &
CLICKHOUSE_PASSWORD=<пароль kimi_reader> make mcp-check MCP_CHECK_HOST=127.0.0.1 MCP_CHECK_PORT=8123
```

Переменные: `STAGING_HOST` (по умолчанию `palmshell`), `MCP_VERSION`, `DAGU_VERSION`. `deploy-staging` **падает**, если файла секретов нет, и никогда его не создаёт.

**Внимание:** после `make deploy-staging` `dagu` включается и **начинает выполнять DAG-и по расписанию** — это реальные запросы к внешним источникам. Порядок внутри цели защищает от запуска DAG-ов на неисправном бинарнике: сначала проверяются бинарь и DAG-файлы, потом сервисы включаются, потом идёт smoke.

Цель также удаляет из `/root/dagu/` DAG-файлы, снятые с репозитория: `scp` копирует, но не удаляет, а оставшийся файл продолжал бы выполняться по своему расписанию параллельно новой цепочке. Список удаляемых файлов — в Makefile, его пополняют при каждом удалении DAG из `dagu/`. Одноразовый шаг после деплоя — миграция таблицы домохозяйств (сверка `cbr_households_b_mes` со старой `households_b_mes` и удаление старой только при совпадении): см. `deploy/staging/README.md`, раздел «После деплоя».

Сервисы слушают только `127.0.0.1` (`dagu` :8080, MCP :8000): наружу ничего не публикуется, TLS/Caddy и публичный MCP-эндпоинт — следующая фаза.

## Винтажный архив

Винтажный архив — сохранённые версии скачанных файлов источников в объектном хранилище плюс реестр `registry.jsonl` (ARCHITECTURE.md §2–§3, стандарт пакета импортёров §9.6). Штатно архивация **выключена**: пустой `ARCHIVE_BACKEND` = архива нет. Версия сохраняется как объект в хранилище плюс строка реестра, и объект записывается при каждом сохранении — ключ версии выведен из отпечатка HEAD, который содержимое не покрывает, поэтому перезапись и есть условие того, что `sha256`/`size` строки описывают именно лежащие в хранилище байты.

Как получить токен Яндекса (OAuth, личная учётная запись):

1. создать OAuth-приложение ([oauth.yandex.ru](https://oauth.yandex.ru/)), отметить **оба** scope: `cloud_api:disk.write` — запись версий и `cloud_api:disk.read` — проверка наличия, чтение реестра и скачивание версий;
2. получить токен по implicit-flow: открыть `https://oauth.yandex.ru/authorize?response_type=token&client_id=<ClientID>` и подтвердить доступ — токен вернётся в адресной строке (`access_token=…`);
3. положить строку `export ARCHIVE_YANDEX_TOKEN=…` в `~/.config/rosstat/env` (dev) или в `/etc/clickhouse-import-rosstat.env` на staging (образец — `deploy/staging/clickhouse-import-rosstat.env.example`); на staging и включается сам бэкенд (`ARCHIVE_BACKEND=yandex`);
4. проверка без вывода значений: `make env-check` — строка `OK ARCHIVE_YANDEX_TOKEN задан`.

**Ловушка:** `ARCHIVE_BACKEND` — режимная переменная, и в `~/.config/rosstat/env` её дублировать нельзя (переменные файла экспортируются и перекрывают префикс команды). Бэкенд выбирается аргументом цели — так приёмка идёт на локальном хранилище, не трогая Диск:

```bash
# приёмка на локальном бэкенде: объект версии и registry.jsonl в build/archive-dev/
make import STAT=cbr_m2 ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev   # повторный прогон → no-op
# живой архив на Диске (токен берётся из окружения) и обновление git-зеркала реестра
make import STAT=cbr_m2 ARCHIVE_BACKEND=yandex ARCHIVE_YANDEX_DIR=disk:/rosstat-archive
make archive-registry ARCHIVE_BACKEND=yandex
# листинг версий и сверка хранилища с реестром (листинг умеет локальный бэкенд)
make archive-list ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev
make archive-list ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev ARCHIVE_RECONCILE=1
# восстановление ряда из архива — ручное действие, сети не касается (нужны CLICKHOUSE_URL и бэкенд)
make archive-restore STAT=rosstat_ipc_weeks_bulletins ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev
```

**Восстановление из архива** (`make archive-restore STAT=<импортёр>`) — ручное действие, в расписании dagu его нет: штатный конвейер наполняет таблицы из источников, а restore нужен, когда источник потерял версию (живой буфер Росстата вычищен, Wayback троттлит) или ряд надо наполнить на пустой базе. Режим `ARCHIVE_ACTION=restore` процесс не завершает, а переключает обычный путь импортёров: байты версии берутся из архива по паре «источник + URL» (`archive.LoadEntries` → строка реестра, самая свежая по `first_seen` → объект хранилища), sha256 полученных байтов сверяется со строкой реестра, HEAD-префлайт и повторная архивация не выполняются — **сеть источника восстановлением не вызывается вовсе** (сам архив может быть сетевым: Яндекс.Диск), источник не спрашивается. Отпечаток для строки `source_publications` берётся из строки реестра; пустой (версия под sha-ключом — HEAD при архивации был недоступен) — строка не пишется, как и в обычном пути. Границы: `CrawFunc`-импортёры (файл качает сам импортёр и в архив не попадает) в режиме дают ошибку с явным текстом, а пустой `CLICKHOUSE_IMPORT_STAT` — тоже ошибка, чтобы «восстановить всё» случайным прогоном было нельзя. Повторный restore идемпотентен: таблицы на `ReplacingMergeTree` перезаписывают те же точки, производные публикуются тем же путём, что у обычного прогона.

**`make archive-list`** печатает версии реестра по хранилищу (`ARCHIVE_ACTION=list`, ClickHouse не нужен): строки с одинаковым `sha256` помечаются дублем содержимого (один файл под sha- и fingerprint-ключом — две записи об одном содержимом). Фильтр — `ARCHIVE_LIST_SOURCE=<источник>`. С `ARCHIVE_RECONCILE=1` бэкенд с листингом (`local`) сверяется с реестром: объекты без строк печатаются, и прогон завершается кодом 1 — реконсиляция не должна выглядеть чистым зеркалом при находках; для `yandex` (листинг — остаток шага 1.2) выводится сообщение «реконсиляция недоступна для бэкенда yandex».

Коды ошибок в логе: `401` (`ErrAuth`) — токен не принят (отозван, истёк или выдан без нужного scope): действие — перевыпустить токен по шагам выше. `403` (`ErrQuota`) — **двоякий**: так Диск отвечает и при нехватке прав у токена (не отданы оба scope, либо архив заведён на `disk:/`, а токен выдан приложению с доступом к `app:/`), и при исчерпанной квоте; к `ErrQuota` сводятся ещё `413` (файл больше лимита), `423` (ресурс занят) и `507` (нет места). Причину различает фрагмент ответа Диска в тексте ошибки (`; ответ Диска: …`): если в нём про права — перевыпускать токен с обоими scope и сверять путь (`disk:/` против `app:/`), если про место — освобождать. Поэтому прежде чем идти освобождать место, прочитайте фрагмент: у `403` без прав места может быть сколько угодно. Импорт данных при любой из этих ошибок не останавливается, но версия файла в архив не сохраняется и строка версии не пишется — следующий прогон повторит полный импорт.
