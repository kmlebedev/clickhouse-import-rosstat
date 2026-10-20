# clickhouse-import-rosstat

Go-конвейер импорта российской макроэкономической и финансовой статистики в ClickHouse. Данные используются дашбордами Grafana и аналитическими моделями (сценарный прогноз золота, DCF и NAV акций ПАО «Полюс»). Один бинарник, конфигурация только через переменные окружения.

Подробная архитектура, схемы таблиц и конвенции — в [ARCHITECTURE.md](ARCHITECTURE.md). План развития — в [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md). Реестр всех роадмапов и правила трекера — в [docs/ROADMAP_INDEX.md](docs/ROADMAP_INDEX.md). Правила для AI-ассистентов — в [AGENTS.md](AGENTS.md).

Docs-портал: **https://kmlebedev.github.io/clickhouse-import-rosstat/** — собирается из этого репозитория по манифесту `docs/site-pages.json`.

## Документация

| Документ | Что внутри |
|---|---|
| [README.md](README.md) | входная точка: быстрый старт, карта данных, ключевые команды |
| [ARCHITECTURE.md](ARCHITECTURE.md) | архитектура, канонические DDL, витрины, конвенции |
| [docs/IMPORTERS.md](docs/IMPORTERS.md) | **полные спецификации импортёров и источников** |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | переменные окружения, ключи, MCP-конфигурация |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Dagu, деплой на staging, винтажный архив |
| [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | сборка, тесты, Python-инструменты, CI и релизы |
| [docs/LIMITATIONS.md](docs/LIMITATIONS.md) | ограничения и известные проблемы |
| [docs/GRAFANA.md](docs/GRAFANA.md) | дашборды как код, стенды и приёмка |
| [docs/GOLD_CONTOUR.md](docs/GOLD_CONTOUR.md) | контур прогноза золота, ingest-endpoint, плагин |
| [docs/IMPORTER_PACKAGE_STANDARD.md](docs/IMPORTER_PACKAGE_STANDARD.md) | нормативный стандарт пакета импортёров |
| [docs/ROADMAP_INDEX.md](docs/ROADMAP_INDEX.md) | реестр роадмапов, слаги и GitHub-трекер |
| [AGENTS.md](AGENTS.md) | правила работы AI-ассистентов |

## Быстрый старт

Нужны Go 1.27, ClickHouse с нативным протоколом (порт 9000) и `CLICKHOUSE_URL`; для PDF-импортёров — `poppler-utils` (`pdftotext`): он нужен `cbr_mt_forecast` и `polyus_financial_metrics` (Debian/Ubuntu: `apt install -y poppler-utils`).

```bash
make all                                # lint, тесты, сборка
export CLICKHOUSE_URL="clickhouse://user:password@host:9000/database"
CLICKHOUSE_IMPORT_STAT=fred,gold_prices ./build/clickhouse-import-rosstat
```

`make build` собирает статический бинарник `linux/amd64` в `build/` (для сервера). Для запуска на Mac соберите нативный бинарник: `go build -o /tmp/rosstat-bin .`

Проверка результата в ClickHouse:

```sql
SELECT series, count(), max(date) FROM macro_series FINAL GROUP BY series;
SELECT venue, count(), max(date) FROM gold_prices FINAL GROUP BY venue;
```

## Что импортируется

Каждый источник — импортёр в реестре `chimport.Stats`; расписание задаёт Dagu (`dagu/`). Устройство пакета — [docs/IMPORTER_PACKAGE_STANDARD.md](docs/IMPORTER_PACKAGE_STANDARD.md).

| Домен | Что импортирует | Основные таблицы | Расписание (МСК) |
|---|---|---|---|
| [`cbr`](docs/IMPORTERS.md) | ЦБ РФ: ставка, курс, M2, золото, кредиты, ставки, ИПЦ, RUONIA, мониторинг предприятий (16 отраслевых листов), макроопрос, резюме ДКП, платёжный баланс, ДИП, среднесрочный прогноз, форма 101, кривая бескупонной доходности (КБД), макропруденциальные требования (АЦН-индикаторы, надбавки к коэффициентам риска, МПЛ), публичный кроссворд 87 BSU-кодов на отраслевые листы | `cbr_*`, `macro_series`, `cbr_mp_summary*`, `cbr_bop_*`, `cbr_dip_bulletin`, `cbr_mt_forecast`, `cbr_form101`, `cbr_zcyc`, `cbr_ccyb_indicators`, `cbr_risk_weight_addons`, `cbr_mpl`, `cbr_mp_bsu_map`; слой ДИП — витрины `v_point_in_time`, `v_point_in_time_asof` (срез `as_of='YYYY-MM-DD'`), `v_point_in_time_version`; КБД — витрины `v_ofz_curve_compare`, `v_ofz_signal`; макропруденциальные требования — витрины `v_cbr_ccyb_indicators`, `v_cbr_risk_weight_addons`, `v_cbr_mpl`; Task 7 #248 — витрины `v_cbr_mp_bsu_map`, `v_cbr_mp_public_panel` | ежедневно 09:17 |
| [`rosstat`](docs/IMPORTERS.md) | ИПЦ (месяц, неделя, до 2022), зарплаты, ВВП, рынок труда, публичный классификатор ОКВЭД2 (3 102 строки, 87/87 BSU-кодов) | `rosstat_*`; Task 7 #248 — `rosstat_okved2` и витрина `v_rosstat_okved2` | четверг 12:17 |
| [`minfin`](docs/IMPORTERS.md) | исполнение федерального и консолидированного бюджета (включая федеральный дефицит), нефтегазовые доходы и НДПИ; долговой контур ОФЗ — аукционы, запас внутреннего/внешнего долга и ОФЗ-номинал, обслуживание и факт заимствований; значения Float64 | `minfin_fed_bud_*`, `minfin_oil_gas_mes`, `minfin_ofz_auction`, `minfin_debt_stock`, `minfin_debt_service`; витрины `v_minfin_budget`, `v_ofz_stress` | 10-е 10:41 (долг/обслуживание) + четверг 11:23 (аукционы) |
| [`customs`](docs/IMPORTERS.md) | внешняя торговля по странам | `customs_vneshn_torg` | 15-е 11:41 |
| [`fao`](docs/IMPORTERS.md) | индексы продовольственных цен (6 рядов, включая Sugar) | `fao_food_price_Indices_monthly`; витрина `v_fao_food_price` | 5-е 10:47 |
| [`bank`](docs/IMPORTERS.md) | ДОМ.РФ, ВТБ, Сбер, СберИндекс, Т-Банк | `domrf_mortgage`, `rus_vtb_group_ifrs`, `sber_*`, `tbank_group_ifrs` | понедельник 09:23 |
| [`craw`](docs/IMPORTERS.md) | реестр СБКТС Росстандарта | `gost_vehicle_safety_certificate` | ежечасно :17 |
| [`edisclosure`](docs/IMPORTERS.md) | документы карточек [e-disclosure.ru](https://e-disclosure.ru/): отчётность IFRS/RAS, списки аффилированных лиц, решения собраний (PLZL, SBER, CHMF); коллектор полуавтоматический, импорт детерминированный | `edisclosure_documents`; витрина `v_edisclosure_documents` | 5-е число 21:17 (шаг `dagu/edisclosure.yaml`); сбор — вручную |
| [`fred`](docs/IMPORTERS.md), [`bls`](docs/IMPORTERS.md), [`bea`](docs/IMPORTERS.md), [`eia`](docs/IMPORTERS.md) | US macro: ставки, CPI, PCE, NFP, запасы нефти | `macro_series` | ежедневно / среда |
| [`wgc`](docs/IMPORTERS.md), [`bold`](docs/IMPORTERS.md) | золотые ETF и резервы ЦБ, цена дня | `macro_series`, `bold_fund_snapshot`, `gold_prices` | ежедневно |
| [`news`](docs/IMPORTERS.md) | 16 лент RSS/Telegram | `news_events`, `news_message_version`, `news_annotation` | каждые 30 мин |
| [`gold`](docs/IMPORTERS.md), [`moex`](docs/IMPORTERS.md) | цена золота, PLZL, ОФЗ, KEYRATE, Brent-фронт-месяц | `gold_prices`, `stock_prices`, `ofz_curve`, `keyrate_*`, `moex_market` | ежедневно 18:47/19:13 |
| [`calendar`](docs/IMPORTERS.md), [`forecast`](docs/IMPORTERS.md) | календарь событий, резолвер прогнозов | `events_calendar`, `forecast_log` | ежедневно |
| [`views`](docs/IMPORTERS.md) | витрины контура, метрик компаний, сводный дашборд ключевой ставки и live-точность (`key_rate_views`) | `v_*`, `v_key_rate_dashboard`, `v_ml_live_accuracy`, `company_financials` | понедельник / вручную |
| [`mlregistry`](docs/IMPORTERS.md) | реестр ML-экспериментов: DDL и витрины (schema-only, внешнего источника нет) | `ml_runs`, `ml_predictions`, `v_ml_*` | ежедневно 06:07 |
| [`dcf`](docs/IMPORTERS.md), [`polyus`](docs/IMPORTERS.md), [`financial`](docs/IMPORTERS.md) | LOM-планы/NAV, отчётность Полюса, legacy | `mine_plans`, `price_decks`, `nav_by_asset`, `databook_polyus`, `company_financials` | понедельник |

Документы e-disclosure.ru собираются полуавтоматически: `make edisclosure-login` сохраняет профиль Chrome (headed-окно, капчу решает владелец), `make edisclosure-collect` пишет снапшот `build/edisclosure/{manifest.json,documents.jsonl}` локально. Оператор копирует этот каталог на staging в `/root/dagu/build/edisclosure` — абсолютный путь из шага `dagu/edisclosure.yaml` (dagu запускает шаг в своём рабочем каталоге, поэтому относительный `build/edisclosure` до импортёра не доедет). Затем `make import STAT=edisclosure_documents` скачивает документы, пишет `edisclosure_documents` (ключ `(ticker, section, fileid)`, версия содержимого — `sha256`) и публикует витрину `v_edisclosure_documents`. Для приёмки `make mcp-check` этот импорт запускайте с включённым архивом (`ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=…`): проверка строки витрины требует непустой `archive_key`, а без архива он пуст. Коллектор в расписание Dagu не входит; без доставленного каталога ежемесячный шаг пропускается (`EDISCLOSURE_ALLOW_MISSING=1`), но частичный снапшот в существующем каталоге — всегда ошибка.

Полные спецификации — [docs/IMPORTERS.md](docs/IMPORTERS.md): источник, транспорт, разбор, точка контроля публикации, таблицы, расписание и живая приёмка по каждому импортёру.

## Структура репозитория

```text
main.go              точка входа и фильтр импортёров
chimport/            интерфейс ImportStat и реестр Stats
util/                HTTP, шаблон импортёра, батч-вставка
cbr/ rosstat/ ...    доменные импортёры
views/ dcf/          витрины, DCF/NAV
ingest/ cmd/ingest/  ingest-endpoint контура прогноза
dagu/                расписания DAG
grafana/ dashboard/  дашборды как код и артефакты
docs/                документация: спецификации, конфигурация, эксплуатация
scripts/             проверки, модели, генератор docs-портала
deploy/staging/      артефакты staging
```

Полное дерево с описанием пакетов — [ARCHITECTURE.md](ARCHITECTURE.md) §2.

## Конфигурация

| Переменная | Назначение |
|---|---|
| `CLICKHOUSE_URL` | DSN подключения (обязательна) |
| `CLICKHOUSE_IMPORT_STAT` | фильтр импортёров по имени; пусто — все |
| `LOG_LEVEL` | `debug`, `info`, `warn`, `error` |
| `CERT_FILES` | PEM-сертификаты нац. УЦ РФ |
| `BEA_API_KEY`, `EIA_API_KEY`, `BLS_API_KEY` | ключи источников (BLS необязателен) |
| `LLM_BASE_URL`, `LLM_API_KEY`, `MP_SUMMARY_LLM_MODEL` | LLM-разметка резюме ДКП |
| `LLM_MODEL` | модель экспериментального LLM-прогноза ключевой ставки (`make key-rate-llm-forecast`); работает с общими `LLM_BASE_URL`/`LLM_API_KEY` |
| `NEWS_LLM_PROFILE`, `NEWS_LLM_MODEL`, `GLM_LLM_*`, `NEWS_LLM_BACKLOG_LIMIT` | LLM-разметка видимого потока новостей (Telegram и RSS): профиль, модель, ключи и предел бэклога за прогон |
| `INGEST_TOKEN` | Bearer-токен ingest-endpoint |
| `INGEST_URL` | Базовый URL ingest-endpoint для `scripts/key_rate_forecast_log.py` (например, `http://127.0.0.1:8081`); обязательна вместе с `INGEST_TOKEN` |
| `EDISCLOSURE_INPUT_DIR`, `EDISCLOSURE_RECHECK`, `EDISCLOSURE_ALLOW_MISSING`, `EDISCLOSURE_OUT` | документы e-disclosure: каталог снапшота коллектора (по умолчанию `build/edisclosure`), глубокий прогон (`1`; переписывает строку при смене `sha256`/метаданных **или** при пустом `archive_key` и включённом архиве — самолечение), мягкий пропуск отсутствующего снапшота в dagu (`1`) и каталог выхода коллектора |
| `ML_LOG`, `ML_RUN_NAME`, `ML_LOCAL_DIR` | журнал ML-эксперимента: `ML_LOG=1` — мастер-выключатель записи в ClickHouse и включения MLflow-приёмника; имя прогона и каталог локального fallback (по умолчанию `build/ml-runs`) |
| `MLFLOW`, `MLFLOW_TRACKING_URI`, `MLFLOW_HOME`, `MLFLOW_PORT`, `MLFLOW_VERSION` | make-уровень MLflow: `MLFLOW=1` **по умолчанию** — добавляет `mlflow==3.17.1` в uv-окружение всех ML-целей и подставляет `MLFLOW_TRACKING_URI` (`http://127.0.0.1:5000`); `MLFLOW=0` осознанно выключает. Перед прогоном `mlflow_preflight.py` проверяет API и при недоступности спрашивает (не-TTY: `MLFLOW_ALLOW_NO_LOG=1`). `MLFLOW_HOME`/`MLFLOW_PORT`/`MLFLOW_VERSION` — каталог `build/mlflow`, порт и пин пакета |

Полный список, `ARCHIVE_*`, MCP-конфигурация и работа с ключами — [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Оркестрация (Dagu)

Расписания — `dagu/*.yaml`: `type: chain`, `overlap_policy: skip`, ретраи 3 × 300 с; путь к бинарнику (`ROSSTAT_IMPORT_BIN`) и `CLICKHOUSE_URL` — в `base.yaml`; секреты — в `/etc/dagu/secrets/`. Запуск одного DAG: `dagu start dagu/gold.yaml`.

Зависимости, staging и винтажный архив — [docs/OPERATIONS.md](docs/OPERATIONS.md).

## MLflow (ML-инфраструктура)

MLflow — control plane ML-экспериментов (`cpi_nowcast`, `key_rate_model`): UI, сравнение run'ов и реестр моделей. Хранилище — SQLite `build/mlflow/mlflow.db` и артефакты `build/mlflow/artifacts` (Postgres/MinIO не нужны); сервер запускается с `--artifacts-destination` + `--serve-artifacts` и проксирует upload, слушает только `127.0.0.1:5000`, доступ — localhost или SSH-туннель. ClickHouse остаётся аналитической репликой: run'ы экспортируются в `ml_runs`/`ml_predictions`, схема не меняется.

```bash
make mlflow-up                             # сервер и /health; UI http://127.0.0.1:5000
ML_LOG=1 make cpi-nowcast                  # MLflow по умолчанию + паспорт/fold'ы в ClickHouse
ML_LOG=1 make key-rate-model
make mlflow-backfill ARGS="--run-id <clickhouse-uuid>"   # старый ClickHouse-run → MLflow
make mlflow-export                         # MLflow → ml_runs/ml_predictions (идемпотентно)
make mlflow-register ARGS="--run-id <mlflow-run-id> --name cpi_nowcast_catboost_direct"
make mlflow-promote  ARGS="--name cpi_nowcast_catboost_direct --version 1"
make mlflow-models   ARGS="--name cpi_nowcast_catboost_direct"
make mlflow-down
```

`ML_LOG=1` — мастер-выключатель журнала; без `MLFLOW_TRACKING_URI`/`MLFLOW=1` MLflow не включается. Если MLflow остановлен, прогон не падает: паспорт и предсказания по-прежнему пишутся в ClickHouse или в локальный fallback `build/ml-runs/`, а `make mlflow-export` догоняет run'ы позже — это отдельный шаг, в DAG его нет. На staging MLflow развёрнут юнитом `deploy/staging/mlflow.service`, доступ — через туннель `ssh -N -L 5000:127.0.0.1:5000 palmshell` (операции — [docs/OPERATIONS.md](docs/OPERATIONS.md), бэкап и upgrade — `deploy/staging/README.md`). Устройство и реестр моделей — [ARCHITECTURE.md](ARCHITECTURE.md) §6.8.

## Контур прогноза золота

Витрины `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy`, `v_gold_forecasts`, `v_gold_etf_flows*`, `v_news_*`, `v_events_calendar`, `v_dcf_assumptions`, `v_ml_live_accuracy` читаются агентом через MCP (`kimi_reader`, только `v_*`). Ingest-endpoint (`cmd/ingest`) — единственный путь записи прогонов модели, ручных рядов, цен COMEX и консенсус-прогнозов; плагин `gold-nav` — отдельный репозиторий.

Журнал прогнозов ключевой ставки пишется тем же ingest: `POST /v1/key_rate_forecast` (единственный путь, MCP read-only) принимает одну строку `forecast_log` с треком в `metric` (`cbr_rate:taylor`, `cbr_rate:ordered_logit`, `cbr_rate:ml`, `cbr_rate:llm`) и метаданными публикации — распределением вероятностей, моделью, версией набора данных и `prompt_version`. Staged-прогнозы `--stage shadow|canary` получают суффикс `:shadow`/`:canary` и не публикуются. Прогноз отправляет `make key-rate-forecast-log` (`scripts/key_rate_forecast_log.py`) из локальных артефактов `key_rate_model_forecast.json` и, при наличии, `key_rate_llm_forecast.json` или готового payload #189 (`--llm-payload`); `forecast_resolve` заполняет факт из `cbr_key_rate` и ошибку в базисных пунктах (`error_bp`), сохраняя метаданные строки. Live-точность читается из `v_ml_live_accuracy`, проверка до публикации — `make ml-live-check`. Спека/план — `docs/superpowers/{specs,plans}/2026-10-18-key-rate-forecast-log*`, `2026-10-20-ml-promotion-live-monitoring*`.

Состав, эндпоинты и запуск — [docs/GOLD_CONTOUR.md](docs/GOLD_CONTOUR.md); схемы — [ARCHITECTURE.md](ARCHITECTURE.md) §6.3/§6.5.

## Разработка

| Команда | Что делает |
|---|---|
| `make all` | gofmt, lint, vet, `go test -race`, сборка |
| `make test` / `make lint` | тесты / линт |
| `make build` | бинарник `linux/amd64` в `build/` |
| `make import STAT=<имя>` | локальный прогон импортёра |
| `make cpi-nowcast` | nowcast ИПЦ; `ML_LOG=1 MLFLOW=1` включает журнал и MLflow |
| `make key-rate-model` / `make key-rate-forecast-log` | прогноз ставки (локальные артефакты; консенсус-бенчмарк №4 — годовая средняя + B4-проекция) / запись прогноза в `forecast_log` через ingest; `ML_LOG=1 MLFLOW=1` включает журнал и MLflow |
| `make key-rate-cbr-rule` / `make key-rate-cbr-rule-test` | экспериментальный трек `cbr_rule` — калиброванное правило ДДКП ЦБ: 108 заседаний, 84/68 фолдов, DM/CI, Brier/log loss против B1–B3 и `ordered_logit`; локальные артефакты `key_rate_cbr_rule_*`, **в `forecast_log` не пишет**, вердикт — `reject` (issue #268) / офлайн-тесты без ClickHouse и сети |
| `make shock-classifier` / `make shock-classifier-test` | классификатор шоков ключевой ставки: явные метки `emergency ∧ step_bp ≠ 0`, причинные as-of веса и описательный логистический слой; локальные артефакты `shock_classifier_report.md`, `shock_classifier_weights.csv`, `shock_classifier_forecast.json`; **в ClickHouse напрямую не пишет** (`ML_LOG=1` или `ARGS="--log"` — журнал через `ml_experiment`) / офлайн-тесты без ClickHouse и сети |
| `make cpi-monthly` | месячный nowcast ИПЦ с МП (ML-трек v2); долгая выборка — `ARGS="--sample long"`; журнал — `ML_LOG=1`; быстрый экспериментальный режим — `ARGS="--quick"` и `ARGS="--quick --jobs N"` (в разы быстрее; **quick-числа неофициальные**, полный прогон без флагов не меняется) |
| `make cpi-monthly-industry` / `make cpi-monthly-industry-test` | Stage 2: отраслевые МП-признаки (16 листов `mp_survey_data.xlsx`, 96 SA-признаков) в компонентном nowcast ИПЦ; aggregate vs industry на общих фолдах `long`/`weekly`, RMSE/MAE, DM-HLN, CI, MDE; в ClickHouse данные не пишет, `ML_LOG=1` — паспорт run / офлайн-тесты без ClickHouse и сети |
| `make cpi-weekly-panel` / `make cpi-weekly-panel-test` | недельная компонентная панель ИПЦ (Task 3): месячный m/m 148 товаров, реконструкция food/nonfood/services, покрытие весов и гейт bottom-up; только чтение ClickHouse, артефакты в `docs/research/` / офлайн-тесты без ClickHouse |
| `make key-rate-llm-forecast` | экспериментальный LLM-прогноз ключевой ставки: as-of снимок, три повтора, медиана/разброс, локальные артефакты; в ClickHouse не пишет; `ARGS="--log"` готовит payload для #127 |
| `make mlflow-up` / `make mlflow-down` / `make mlflow-status` | локальный MLflow-сервер (SQLite + артефакты, `127.0.0.1:5000`) |
| `make mlflow-export` | экспорт run'ов MLflow в `ml_runs`/`ml_predictions` (`ARGS` — `--since/--limit/--dry-run`) |
| `make mlflow-register` / `make mlflow-promote` / `make mlflow-models` | реестр моделей и алиасы `challenger`/`champion` (аргументы — в `ARGS`) |
| `make ml-gate` / `make ml-gate-test` | gate промоушена: DM-HLN, 95% бутстрап CI, robustness-окна, MCS при >2 кандидатах; артефакт `build/ml-gate/<run>.json`; live-режим `ARGS="--live --live-track cbr_rate:ordered_logit"` / офлайн-тесты |
| `make ml-live-check` / `make ml-live-check-test` | проверка live-прогноза до публикации: production-строки на ближайшее заседание, `forecast_date < target_date`, непустой `data_version`; `ARGS="--require-llm --warn-only"` / офлайн-тесты |
| `make mcp-check` | MCP end-to-end |
| `make cb-comp-parity` / `make cb-comp-parity-test` | паритет nowcast с CB_COMP (direct + bottom-up, архив авторов) / офлайн-тесты parity-скриптов |
| `make docs-check` / `make docs-build` | проверка и сборка docs-портала |

`make shock-classifier` пишет три локальных артефакта в каталог прогона (по умолчанию текущий, переопределяется `ARGS="--out-dir DIR"`): отчёт с разделами «Явные шоки», «Покрытие каналов», «Метрики классификатора», «Веса», «С весами / без», «Ограничения»; CSV as-of весов `meeting_date, emergency, explicit_shock, step_bp, y3, anomaly_score, weight, shock_prob, missing_count` + признаки; JSON as-of среза следующего заседания. Прямых вставок в ClickHouse нет: журнал включается только `ML_LOG=1` (или `ARGS="--log"`) и идёт через `scripts/ml_experiment.py` (`ml_runs`/`ml_predictions`). Офлайн-фикстуры — `ARGS="--export-fixtures scripts/testdata/shock_classifier"`.

Пункт роадмапа ведётся через GitHub issue с составным ID `<slug>/<item-id>`: шаблоны — [`.github/ISSUE_TEMPLATE/roadmap-item.yml`](.github/ISSUE_TEMPLATE/roadmap-item.yml) и [`.github/pull_request_template.md`](.github/pull_request_template.md); операции — скил `github-tracker` ([`.kimi-code/skills/github-tracker/SKILL.md`](.kimi-code/skills/github-tracker/SKILL.md)); правила — [docs/ROADMAP_INDEX.md](docs/ROADMAP_INDEX.md) и правило 12 [AGENTS.md](AGENTS.md). Живой проект — [clickhouse-import-rosstat — Roadmap](https://github.com/users/kmlebedev/projects/11).

### Роли и скилы

| Роль | Скил |
|---|---|
| Go-инженер | [`go-engineer`](.kimi-code/skills/go-engineer/SKILL.md) |
| Python-инженер (не-ML) | [`python-engineer`](.kimi-code/skills/python-engineer/SKILL.md) |
| ML-инженер | [`ml-engineer`](.kimi-code/skills/ml-engineer/SKILL.md) |
| Research-инженер | [`research-engineer`](.kimi-code/skills/research-engineer/SKILL.md) |

Общий роутер — [AGENTS.md](AGENTS.md); диспатч субагентов — [`subagent-dispatch`](.kimi-code/skills/subagent-dispatch/SKILL.md). Скилы не отменяют Go-стандарт для DDL/импортёров.

Полный набор целей, Python-тесты, зависимости и CI — [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

## Ограничения

Полный список ограничений и известных проблем — [docs/LIMITATIONS.md](docs/LIMITATIONS.md). Перед использованием ряда в модели прочитайте ограничения его источника: часть данных имеет задержку, ревизии или неполное покрытие.

## Grafana

Три дашборда как код — `gold-thesis`, `key-rate-publications`, `finance-polyus`. Установка, переменные стендов, цикл деплоя и приёмка — [docs/GRAFANA.md](docs/GRAFANA.md).

## CI и релизы

`go.yml` (test, lint, vulncheck, build), `dashboards.yml`, `pages.yml` (docs-портал) и `release.yml` (тег `v*`). Подробности — [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).
