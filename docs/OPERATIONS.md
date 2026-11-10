# Эксплуатация

> Часть документации проекта; входная точка — [README.md](../README.md).

## Оркестрация (Dagu)

Расписания описаны в `dagu/*.yaml`: `type: chain`, `overlap_policy: skip`, каждый шаг запускает бинарник с `CLICKHOUSE_IMPORT_STAT=<имя>`, ретраи — 3 попытки с паузой 300 с. На staging `base.yaml` — deploy-артефакт [`deploy/staging/dagu-base.yaml`](../deploy/staging/dagu-base.yaml) (ставится `make deploy-staging-dagu-base`): он задаёт `env` шагов (`ROSSTAT_IMPORT_BIN`, `CLICKHOUSE_URL`, ключи источников) и SMTP-уведомления, а секреты остаются в `/etc/clickhouse-import-rosstat.env` (права `600`, вне git). Без секции `env` шаг `run: "$ROSSTAT_IMPORT_BIN"` — пустая команда: dagu рапортует `succeeded`, импорт не выполняется (инцидент staging 2026-10-25). Внутри домена зависимости задаются через `depends`; между DAG зависимостей нет (см. `gold.yaml`: шаг `cbr_currency_usd` стоит внутри DAG). Отдельный `dagu/dcf.yaml` использует именно это свойство DAG-а: `type: chain` исполняет `dcf_mine_plans` строго до `dcf_engine`, и «сид раньше расчёта» — гарантия расписания, а не только порядок ручных команд (Finding 3 финального ревью). **Шага уточнённого AISC-сида в `dagu/dcf.yaml` нет** — он применяется вручную (см. «Что импортируется» и «Ограничения»); DAG содержит ровно два шага, и придумывать третий не нужно.

Запуск одного DAG вручную: `dagu start dagu/gold.yaml` (dev-окружение Dagu должно видеть `ROSSTAT_IMPORT_BIN` и `CLICKHOUSE_URL` через `base.yaml`).

**Уведомления о падении импорта.** `deploy/staging/dagu-base.yaml` задаёт `smtp` и `mail_on.failure: true`: после 3 ретраев шага dagu отправляет письмо с префиксом `[DAGU palmshell]` и логами на `MAIL_TO` — адрес шлюза CMC, который пересылает письмо в SMS. Проверка — `make deploy-staging-notify-test` (заведомо падающий DAG, в deploy не входит); `deploy-staging-preflight` требует непустые `SMTP_*`/`MAIL_*`, а `deploy-staging-smoke` проверяет, что шаг видит `ROSSTAT_IMPORT_BIN` и `CLICKHOUSE_URL`.

## ML-эксперименты

Разработка и тестирование моделей вынесены в [`kmlebedev/bi-ml-models`](https://github.com/kmlebedev/bi-ml-models): там модели, бэктесты, gate, MLflow-клиент, live-check и model cards. Здесь остаются данные и журнал:

- `ml_registry` (schema-only импортёр, `dagu/ml.yaml`, 06:07 МСК) создаёт `ml_runs`/`ml_predictions` и витрины `v_ml_runs`, `v_ml_leaderboard`, `v_ml_error_by_fold`; повтор идемпотентен. Импортёр должен отработать до первого прогона модели с `ML_LOG=1`.
- Запись паспорта и fold-предсказаний делает модель в `bi-ml-models` (`ML_LOG=1`); при недоступном ClickHouse — локальный fallback `build/ml-runs/<run_id>/` на стороне модели.
- `forecast_log` пишется только через ingest (`POST /v1/key_rate_forecast`, `tools/forecast_log.py` в `bi-ml-models`); факт резолвит `forecast_resolve` (`dagu/forecast.yaml`), live-точность — `v_ml_live_accuracy` (создаётся `make import STAT=key_rate_views`).
- Gate, shadow/canary, промоушен и `ml-live-check` — команды `bi-ml-models`; Grafana-алерты `ml-live-degradation`/`ml-live-freshness` остаются здесь (`grafana/provisioning/alerting/ml-live.yaml`).
- MLflow: канонический backend — staging (`deploy/staging/mlflow.service`, `make deploy-staging-mlflow`); клиентские команды и локальный dev-сервер — в `bi-ml-models`.

Запуск, паспорт run и правила gate — `bi-ml-models/README.md` и `AGENTS.md`.

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
