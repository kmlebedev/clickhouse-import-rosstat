# Разработка

> Часть документации проекта; входная точка — [README.md](../README.md).

## Команды

| Команда | Что делает |
|---|---|
| `make lint` | `gofmt -l` (падает, если есть неотформатированный код) и `golangci-lint run ./...` |
| `make test` | `go vet` и `go test -race ./...` (тесты есть у `bea`, `bls`, `bold`, `calendar`, `cbr`, `craw`, `dcf`, `eia`, `forecast`, `fred`, `gold`, `ingest`, `minfin`, `moex`, `news`, `polyus`, `rosstat`, `util`, `views`, `wgc`) |
| `make build` | статический бинарник `linux/amd64` в `build/` |
| `make run` | сборка и запуск (нужен `CLICKHOUSE_URL`) |
| `make import STAT=<имя>` | локальный прогон одного импортёра по `Name()` (собирает `build/clickhouse-import-rosstat-native`). Для расчёта NAV: `DCF_RUN_ID=$(uuidgen \| tr A-Z a-z) make import STAT=dcf_engine` |
| `make archive-registry` | обновить git-зеркало реестра винтажного архива: запускает бинарник в режиме `ARCHIVE_ACTION=registry`, тот скачивает `registry.jsonl` из хранилища в `docs/archive/registry.jsonl` (ClickHouse не нужен, подключения к базе нет). Бэкенд берётся из окружения: `make archive-registry ARCHIVE_BACKEND=yandex` (нужен `ARCHIVE_YANDEX_TOKEN`) или `... ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev`; пустой `ARCHIVE_BACKEND` — ошибка, а не пустое зеркало, и пустой скачанный реестр поверх непустого зеркала — тоже отказ (затирание разрешает `ARCHIVE_REGISTRY_ALLOW_EMPTY=1` с warn). Коммит зеркала — ручной/периодический (на staging нет git-дерева): `docs/archive/README.md` |
| `make archive-list` | напечатать версии реестра винтажного архива (`ARCHIVE_ACTION=list`, ClickHouse не нужен): «источник · имя · отпечаток(8) · размер · first_seen · url». Строки с одинаковым `sha256` помечаются «дубль содержимого», фильтр — `ARCHIVE_LIST_SOURCE=<источник>`. С `ARCHIVE_RECONCILE=1` — сверка хранилища с реестром: объекты без строк печатаются, и прогон завершается кодом 1 (нужен бэкенд с листингом, сегодня `local`; у `yandex` выводится «реконсиляция недоступна» — это остаток шага 1.2, а не находка) |
| `make archive-restore STAT=<имя>` | восстановление БД из винтажного архива (`ARCHIVE_ACTION=restore`): импортёр берёт байты версии из архива по строке реестра (`source` + `DataUrl`, самая свежая по `first_seen`, sha256 сверяется со строкой), не делает ни HEAD-префлайта, ни GET файла, ни повторной архивации — сеть источника не вызывается вовсе (сам архив может быть сетевым: yandex). Нужны `ARCHIVE_BACKEND` (например, `make archive-restore STAT=rosstat_ipc_weeks_bulletins ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev`) и `CLICKHOUSE_URL`; `STAT` обязателен — «восстановить всё» одним прогоном нельзя. Строка версии в `source_publications` пишется с отпечатком из строки реестра (пустой — не пишется) |
| `make build-ingest` | сборка ingest-endpoint (`cmd/ingest`) в `build/ingest` |
| `make run-ingest` | сборка и запуск ingest-endpoint (нужны `CLICKHOUSE_URL` и `INGEST_TOKEN`) |
| `make fmt` | `gofmt -w .` |
| `make deps` | `go mod download` и `go mod tidy` |
| `make clean` | удалить `build/` |
| `make ch-up` | запустить локальный ClickHouse (порты 8123/9000), если он ещё не отвечает |
| `make ch-down` | остановить сервер, поднятый через `make ch-up` (по PID-файлу) |
| `make ch-status` | проверить, отвечает ли сервер на `localhost:8123` |
| `make ch-sql FILE=...` | прогнать SQL-файл репозитория в локальный ClickHouse (`--multiquery`; пользователь `CH_USER`, по умолчанию `default` без пароля) |
| `make dcf-aisc` | сгенерировать сид поактивного AISC Полюса `sql/dcf_mine_plans_seed.sql` (77 строк) и CSV-артефакт контроля; в ClickHouse не пишет. Нужны env-переменные ClickHouse и живая БД (читает факт добычи из `databook_polyus`); Python 3, без зависимостей. Применение сида — вручную, отдельным шагом (см. «Что импортируется») |
| `make key-rate-model` | первый срез модели ключевой ставки (фундаментальный контур v0): выборка решений, бенчмарки B1–B3, методы №2–3 (правило Тейлора, упорядоченная логит) и walk-forward-бэктест; **в ClickHouse НЕ пишет** — артефакты локальные (`key_rate_model_report.md`, `key_rate_model_predictions.csv`, `key_rate_model_forecast.json`, в git не коммитятся). Нужны env-переменные ClickHouse и живая БД (читает `events_calendar`, `cbr_key_rate`, `cbr_indicators_cpd`, `macro_series`, `cbr_mp_summary`); зависимости Python поднимает `uv`. `ARGS=` — доп. аргументы цели, например `make key-rate-model ARGS="--export-fixtures scripts/testdata/key_rate"` (перегенерация офлайн-фикстур) |
| `make cpi-nowcast` | ML-трек контура ключевой ставки: nowcast месячного ИПЦ до публикации Росстата — выборка 2022-01…2026-09 (57 месяцев, пропусков нет), признаки недельного ИПЦ Росстата, курса USD, ключевой ставки, ОФЗ и FAO, walk-forward (старт 24 — основной, 48 — робастность, `--embargo-months`), бенчмарки B1–B3 (last value, AR(1) с HAC, ElasticNetCV) и CatBoost direct/bottom-up (веса NNLS); **в ClickHouse НЕ пишет** — артефакты локальные (`cpi_nowcast_report.md`, `cpi_nowcast_predictions.csv`, `cpi_nowcast_forecast.json`, в git не коммитятся). Нужны env-переменные ClickHouse и живая БД (читает `rosstat_ipc_mes`, `rosstat_ipc_weeks`, `cbr_currency_usd`, `cbr_key_rate`, `ofz_curve`, `fao_food_price_Indices_monthly`); зависимости Python поднимает `uv` (`catboost>=1.2`, `scikit-learn`, `pandas`, `statsmodels>=0.15`, `scipy`). `ARGS=` — доп. аргументы цели, например `make cpi-nowcast ARGS="--export-fixtures scripts/testdata/cpi_nowcast"` (перегенерация офлайн-фикстур) или `ARGS="--embargo-months 1"` |
| `make cpi-monthly` | ML-трек v2 контура ключевой ставки: месячный nowcast ИПЦ с мониторингом предприятий ЦБ (МП) — выборка 155 месяцев (2013-10…2026-08, пропусков нет), 8 моделей (4 CatBoost-варианта direct/bottom-up × с МП/без МП и бенчмарки B1–B3, B3 в двух пространствах), walk-forward (старт 24 — основной, 48 и `--embargo-months 1` — робастность), DM-HLN «с МП vs без МП», фиксированный сплит статьи 63/67, строка «2022 исключён», веса NNLS и живой nowcast; **по умолчанию в ClickHouse НЕ пишет** — артефакты локальные (`cpi_monthly_report.md`, `cpi_monthly_predictions.csv`, `cpi_monthly_forecast.json`, в git не коммитятся). Нужны env-переменные ClickHouse и живая БД (читает `rosstat_ipc_mes`, `macro_series` с `source='cbr_survey'`, `cbr_currency_usd`, `cbr_key_rate`, `fao_food_price_Indices_monthly`); зависимости Python поднимает `uv` (`catboost>=1.2`, `scikit-learn`, `pandas`, `statsmodels>=0.15`, `scipy`). `ARGS="--sample long"` — long-sample 292 месяца 2002-05…2026-08 (старт 48; `usd_mm`/`key_rate` до 2013 — NaN-контроли, месяц не пропускается); `ML_LOG=1` — паспорт и fold-предсказания в реестр `ml_runs`/`ml_predictions` (нужен отработавший `ml_registry`, см. «Эксплуатацию»). Прочие `ARGS=` — доп. аргументы цели, например `make cpi-monthly ARGS="--export-fixtures scripts/testdata/cpi_monthly"` (перегенерация офлайн-фикстур) |
| `make key-rate-llm-forecast` | экспериментальный LLM-трек контура ставки (issue #189): as-of снимок витрин, промпт v1, три OpenAI-совместимых повтора, медиана и разброс; **в ClickHouse НЕ пишет** — артефакты локальные (`key_rate_llm_forecast.json`, `key_rate_llm_forecast_report.md`, в git не коммитятся). Нужны ClickHouse env и `LLM_BASE_URL`/`LLM_API_KEY`/`LLM_MODEL`; `uv` без внешних зависимостей (`--python 3.12`). `ARGS="--as-of YYYY-MM-DD"` — срез на дату; `ARGS="--log"` — дополнительно payload `key_rate_llm_forecast_log_payload.json` для ingest #127 (прямой записи в `forecast_log` нет) |
| `make dev-check` | проверить наличие `go`, `gofmt`, `golangci-lint`, `uv`, `curl`, `clickhouse` |
| `make docs-check` | проверить манифест docs-портала и прогнать офлайн-тесты генератора (`scripts/docs_build_test.py`) |
| `make docs-build` | собрать docs-портал в `build/site` (генератор + `mkdocs build --strict`, mkdocs-material 9.7.7 через `uv`) |
| `make docs-serve` | локальный просмотр docs-портала (`mkdocs serve`, http://localhost:8000) |
| `make mcp-run` | запустить MCP-сервер `mcp-clickhouse` в stdio вручную (нужен `CLICKHOUSE_PASSWORD`) |
| `make mcp-user` | создать пользователя `kimi_reader` и выдать `GRANT SELECT` на витрины из `sql/mcp_kimi_reader.sql` (нужен `CLICKHOUSE_PASSWORD`, запущенный ClickHouse) |
| `make mcp-check` | smoke-проверка MCP: инструменты, витрины, отказ на сырые таблицы; отдельно — артефакты фаз 3 и wgc-контура: витрины `v_gold_forecasts`, `v_gold_etf_flows` и `v_cb_gold_dynamics`, каталоги `webbridge` (20 рядов) и `wgc` (34 ряда), резолв `v_forecast_accuracy` и запрет сырой `gold_forecasts` (нужен `CLICKHOUSE_PASSWORD`, запущенный ClickHouse). Адрес переопределяется: `MCP_CHECK_HOST`/`MCP_CHECK_PORT` — для проверки staging через туннель |
| `make deploy-staging` | развернуть на staging бинарник, `dagu` и `mcp-clickhouse` (см. «Деплой на staging») |
| `make deploy-staging-binary` / `-dagu` / `-mcp` | точечно: только бинарник, только dagu, только MCP |
| `make upgrade-staging` | обновить **только** dagu и mcp-clickhouse до версий из репозитория (без `apt upgrade`) |
| `make mcp-user-staging` | создать `kimi_reader` на staging (нужны `CLICKHOUSE_PASSWORD` и `CH_SETUP_PASSWORD`) |
| `make grafana-build` | сгенерировать артефакты `dashboard/<uid>.json` всех трёх дашбордов (`gold-thesis`, `key-rate-publications`, `finance-polyus`) из кода модуля `grafana/` (grafana-foundation-sdk). Детерминирована: повторный прогон не меняет артефакты |
| `make grafana-validate` | `go vet` и тесты модуля `grafana/` (состав панелей всех трёх дашбордов и ряд негативных правил `Validate`). Серверная `gcx resources validate` сюда не входит — она требует живого инстанса и кредов (рабочая форма — в комментарии цели; на стенде даёт `✔ No errors found`) |
| `make grafana-up` / `make grafana-down` | поднять/остановить локальный стенд Grafana (brew-Grafana 13.2.3; данные, логи и плагины в `$(GF_DIR)`, провижининг — `grafana/provisioning`, порт 3000). Плагин `grafana-clickhouse-datasource` 4.3.2 ставится сам; системный `brew services` не используется |
| `make grafana-test-token` | сервис-аккаунт `dashboards-cd` (роль **Editor**) на локальном стенде и свежий токен к нему: аккаунт создаётся идемпотентно (повтор — «уже есть (id N)»), токен — всегда новый (имя `deploy-<UTC-метка>-<pid>`: Grafana не принимает два токена с одним именем) и проверяется `GET /api/user` **до** печати. Печатает готовую строку `GRAFANA_TEST_TOKEN=glsa_…` для `~/.config/rosstat/env` (секрет в репозиторий/логи не попадает). Нужен запущенный стенд (иначе «стенд не отвечает: make grafana-up») и админ-креды `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD` (дефолт `admin`/`admin` — локальный одноразовый стенд) |
| `make grafana-check` | smoke задеплоенных дашбордов: по каждому uid из `DASHBOARDS` — `GET /api/dashboards/uid/<uid>` = 200 и первые панели отдают точки (SQL берётся из самого дашборда, снятого со стенда; цикл останавливается на первой ошибке). Нужны `GRAFANA_TEST_URL` и `GRAFANA_TEST_TOKEN` |
| `make grafana-deploy-test` | выгрузить все три дашборда на тест-стенд (`gcx resources push dashboards --path dashboard/<uid>.json` по списку `DASHBOARDS`) и прогнать `grafana-check`. Идемпотентна: повторный push не создаёт дубль, растёт только `version` |
| `make grafana-provision-staging` | идемпотентно разложить провижининг датасорса на staging: шаблон `deploy/staging/grafana-datasource.yaml`, пароль подставляется на хосте из env-файла, результат — `root:grafana 640`, переустановка только при отличии sha256 |
| `make grafana-deploy-staging` | staging-деплой через туннель `:3001`: проверка токена → доступность → `grafana-provision-staging` → push всех трёх дашбордов → smoke по всем трём uid. Нужен `GRAFANA_STAGING_TOKEN` |

Пункт роадмапа ведётся через GitHub issue с составным ID `<slug>/<item-id>`: шаблоны — [`.github/ISSUE_TEMPLATE/roadmap-item.yml`](../.github/ISSUE_TEMPLATE/roadmap-item.yml) и [`.github/pull_request_template.md`](../.github/pull_request_template.md); операции — скил `github-tracker` ([`.kimi-code/skills/github-tracker/SKILL.md`](../.kimi-code/skills/github-tracker/SKILL.md)); правила — [docs/ROADMAP_INDEX.md](ROADMAP_INDEX.md) и правило 12 [AGENTS.md](../AGENTS.md).

Тесты Python-скриптов идут вручную (в CI только Go-проверки, см. «Ограничения»). Наборы модели ключевой ставки, ML-треков (nowcast ИПЦ v1 и v2) — офлайн: транспорт ClickHouse и HTTP-вызов провайдера мокаются, поэтому живая БД и env-переменные не нужны; набор v2 (`scripts/cpi_monthly_test.py`) — **152 теста**; `--python 3.12` — тот же пин, что в целях `make key-rate-model`, `make cpi-nowcast`, `make cpi-monthly` и `make key-rate-llm-forecast`. Офлайн-наборы MLflow (`infra/4.2`) — stdlib-only: fake `mlflow` подменяется через `sys.modules`, сеть не используется. Офлайн-набор добычи недельных ИПЦ до 2022 (`scripts/harvest_test.py`) сети не касается вовсе и идёт системным python3:

```bash
uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_model_test.py
uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_nowcast_test.py
uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_monthly_test.py
python3 scripts/key_rate_llm_forecast_test.py
python3 scripts/ml_experiment_test.py -v
python3 scripts/mlflow_export_test.py -v
python3 scripts/mlflow_registry_test.py -v
python3 scripts/harvest_test.py
```

## Зависимости для разработки

- Go — версия из `go.mod` (сейчас `1.27`); в CI используется Go `1.27.x`;
- `golangci-lint` v2 (`brew install golangci-lint`), конфиг — `.golangci.yml`;
- `uv` (`brew install uv`): запускает `mcp-clickhouse` без установки в систему, Python `3.12` подтягивается `uv`;
- ClickHouse локально: бинарник из `PATH` или `~/.clickhouse/versions/*/clickhouse` (или `CH_BIN=...`);
- `curl` — для проверки статуса сервера;
- GoLand 2025.2+ (опционально) — MCP-сервер IDE для Kimi Code, см. «MCP GoLand (опционально)» ниже.

Порядок проверки окружения и работы:

```bash
make dev-check                       # что установлено
make all                             # lint, test, build
make env-check                       # ключи из ~/.config/rosstat/env на месте
make ch-up                           # локальный ClickHouse на 8123/9000
make import STAT=bea                 # импорт bea (ключ BEA_API_KEY из файла окружения)
make import STAT=eia                 # импорт eia (ключ EIA_API_KEY из файла окружения)
make import STAT=wgc_etf_flows       # WGC ETF Flows: JSON API fscdn.gold.org (фолбэк — ETF_Flows_*.xlsx в wgc/data)
make import STAT=wgc_cb_reserves     # WGC золотые резервы ЦБ (Changes_*.xlsx + World_official_gold_holdings_*.xlsx в wgc/data)
make import STAT=bold_gold_price     # bold.report: цена дня (JSON API без ключа; полный путь — вся история)
make import STAT=bold_etf_flows      # bold.report: 7 дневных рядов ETF-агрегата 73 фондов + пер-фондовый срез
make import STAT=gold_views          # витрины контура, включая v_gold_etf_flows_daily и v_gold_etf_funds_daily (после первого прогона bold_etf_flows)
make import STAT=dcf_mine_plans      # сид mine_plans из отчётности Полюса (идёт до движка)
make dcf-aisc                        # (опционально) уточнённый поактивный AISC → sql/dcf_mine_plans_seed.sql
make ch-sql FILE=sql/dcf_mine_plans_seed.sql   # (опционально) применить сид AISC поверх Go-сида
DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine   # расчёт NAV по засеянным планам (в шелле без uuidgen — любой UUID)
make import STAT=forecast_resolve    # сверка прогнозов журнала с фактом (нужны строки forecast_log: ingest-прогон или бэкфилл sql/forecast_log_backfill.sql)
make import STAT=news_watch          # 16 лент РФ-канала: 8 RSS (Интерфакс, РБК, ЦБ ×2, TASS, общий Смартлаб и два блога) + 8 Telegram (молния interfaxonline/ifax_go, edisclosure, newssmartlab, markettwits, russianmacro, MarketOverview, mozgovikresearch) → news_events, news_message_version, news_annotation (all по умолчанию — полный upsert окна; bootstrap-разметка нового потока — разовый NEWS_LLM_BACKLOG_LIMIT=500)
NEWS_WATCH_MODE=since make import STAT=news_watch   # штатный режим расписания: только отсутствующие позиции (ключ date/source/title)
NEWS_WATCH_MODE=check make import STAT=news_watch   # диагностика покрытия: окна источников и дыры, без записи
make import STAT=rosstat_ipc_mes            # Росстат: ИПЦ помесячно, 4 ряда (файл обновляется раз в месяц, повтор — no-op без скачивания)
make import STAT=rosstat_ipc_weeks          # Росстат: недельные индексы цен, 131 позиция (префлайта нет: файл перезаписывается в недельном темпе)
make import STAT=rosstat_salaries_mes       # Росстат: средняя номинальная зарплата (повтор — no-op без скачивания файла)
make import STAT=rosstat_vvp_kvartal        # Росстат: ВВП и дефлятор поквартально (повтор — no-op без скачивания файла)
make import STAT=rosstat_labour_force_mes   # Росстат: рынок труда — рабочая сила, занятые, безработные и три уровня (участия, занятости, безработицы), помесячно с 2017 (повтор — no-op без скачивания файла)
make ch-sql FILE=sql/gold_forecasts_q4_2026_seed.sql   # (опционально) сид консенсуса банков → v_gold_forecasts (до make import STAT=gold_views)
make ch-sql FILE=sql/wgc_etf_flows_catalog.sql         # (опционально) каталог 20 рядов WGC ETF Flows (source='webbridge')
make ch-sql FILE=sql/events_calendar_bank_results_q4_2026.sql   # даты раскрытия Сбера/ВТБ (Q4-2026)
make ch-sql FILE=sql/sber_ras_monthly_seed_2026_08.sql          # SBER РСБУ 2026-08: кредиты ЮЛ/ФЛ (эталон калибровки формы 101)
make ch-sql FILE=sql/sber_ras_form101_mapped_2026_07.sql        # SBER 2026-07: cbr-809p-sber-1 ras из формы 101 (backfill)
make ch-sql FILE=sql/sber_ras_monthly_seed_2026_09.sql          # SBER РСБУ 2026-09: кредиты ЮЛ/ФЛ (сид до импортёра формы 101)
make mcp-check                       # MCP end-to-end (CLICKHOUSE_PASSWORD из файла окружения)
make ch-down                         # остановить сервер
```

Пароль `CLICKHOUSE_PASSWORD` задаётся только в окружении текущей сессии, в файлы репозитория не записывается.

Проверка MCP в Kimi Code: откройте новую сессию в этом репозитории, командой `/mcp-config` убедитесь, что сервер `clickhouse` подключён (в версии Kimi Code, где команды `/mcp` нет), и отправьте агенту промпт:

> Проверь MCP-сервер clickhouse. 1) Через list_databases покажи базы. 2) Через list_tables для базы default покажи таблицы и комментарии колонок v_bea_pce. 3) run_query: SELECT series, unit, title FROM v_series_catalog WHERE source = 'bea' ORDER BY series — ожидаю 2 ряда (PCE_PI, PCE_PI_CORE). 4) run_query: SELECT date, value FROM v_bea_pce WHERE series = 'PCE_PI' ORDER BY date DESC LIMIT 3 — ожидаю 3 строки. 5) run_query: SELECT count() FROM macro_series — ожидаю отказ ACCESS_DENIED (доступа к сырым таблицам у агента нет). Ответь таблицей: шаг, результат, ожидание, совпало ли.

Локальный ClickHouse для проверок:

- бинарник ищется в `PATH` или в `~/.clickhouse/versions/*/clickhouse`; иначе укажите `CH_BIN=/путь/к/clickhouse`;
- данные, PID и лог лежат в `~/clickhouse-dev/.clickhouse/servers/dev/` (переменная `CH_DIR`), вне репозитория;
- запуск: `make ch-up`, затем `export CLICKHOUSE_URL="clickhouse://localhost:9000/default"`;
- импорт и проверка: `CLICKHOUSE_IMPORT_STAT=bls ./build/clickhouse-import-rosstat` (или нативный бинарник, см. «Быстрый старт»);
- остановка: `make ch-down`.

## Правила кода

Правила кода (подробно — в [AGENTS.md](../AGENTS.md) и [ARCHITECTURE.md](../ARCHITECTURE.md)):

- вставка в ClickHouse только батчами: `PrepareBatch` → `Append` → `Send`; построчный `Exec` в цикле запрещён;
- секреты только через `os.Getenv`; сетевых вызовов в `init()` не делать;
- HTTP только через `util.HttpClient` (и `util.GetXlsx`, `util.GetCSV`, `util.GetJSON` — последний проверяет HTTP-статус);
- DDL — `CREATE TABLE IF NOT EXISTS`, движок `ReplacingMergeTree`, значения новых таблиц — `Float64`;
- ошибки не проглатывать: парсинг чисел и `batch.Append` проверяются.

## CI и релизы

`.github/workflows/go.yml` на каждый push в `main` и на pull request:

- `test`: `gofmt`, `go vet`, `go test -race`;
- `lint`: `golangci-lint` v2.14.0 с конфигом `.golangci.yml`. Пакет `financial/` исключён из линта как legacy;
- `vulncheck`: `govulncheck ./...` (падает при уязвимостях, которые затрагивают код);
- `build`: `make build`, артефакт `build/`.

Все задания используют Go `1.27.x`: `govulncheck` проверяет стандартную библиотеку тулчейном, которым запущен, и на go1.27.1 находил уязвимости `net/http` и `crypto/tls`, исправленные в go1.27.2.

`.github/workflows/dashboards.yml` (`dashboards`) на каждый push в `main` и на pull request проверяет все три дашборда контура (`gold-thesis`, `key-rate-publications`, `finance-polyus`) как код: `go vet` и `go test` модуля `grafana/`, `make grafana-build`, `git diff --exit-code -- dashboard/` (все три артефакта совпадают со свежей сборкой — заодно ловит недетерминированную генерацию; легаси-экспорты сборкой не трогаются), установка пинуемой `gcx` v1.5.0 и офлайновый `gcx dev lint run` одним прогоном по всем трём файлам артефактов (два idiomatic-предупреждения генератора — `editable`-дашборд и отсутствие описаний панелей — отключены по именам, иначе был бы exit 4), затем upload всех трёх артефактов. Секретов и деплоя в workflow нет: на стенд дашборды уезжают только вручную (`make grafana-deploy-test` / `make grafana-deploy-staging`), серверная `gcx resources validate` остаётся ручной проверкой (нужны сервер, org-id и креды).

`.github/workflows/pages.yml` (`Pages`) на pull request собирает docs-портал без деплоя, а на push в `main` и `workflow_dispatch` публикует его на GitHub Pages: `python3 scripts/docs_build.py --check`, офлайн-тесты генератора, `mkdocs build -f build/mkdocs.yml --strict` (mkdocs-material 9.7.7), затем `actions/deploy-pages`. Pages включается один раз: Settings → Pages → Source: GitHub Actions. Сайт — производный артефакт: источник истины остаётся в `docs/`, спеки/планы/архив на сайт не публикуются.

`.github/workflows/release.yml` на тег `v*` запускает тесты и `goreleaser` (архив `linux/amd64`, `SHA256SUMS`, файлы `README.md` и `dagu/*.yaml`). Конфиг — `.goreleaser.yaml`; проверка: `goreleaser check`. В релиз входит только основной бинарник импорта; `cmd/ingest` собирается локально через `make build-ingest`.
