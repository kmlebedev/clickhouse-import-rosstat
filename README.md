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
| [docs/publications/README.md](docs/publications/README.md) | публикационный прогон по ключевой ставке: импорт источников, модели ИПЦ и ставки, журнал, витрина, графики |
| [docs/IMPORTER_PACKAGE_STANDARD.md](docs/IMPORTER_PACKAGE_STANDARD.md) | нормативный стандарт пакета импортёров |
| [docs/ROADMAP_INDEX.md](docs/ROADMAP_INDEX.md) | реестр роадмапов, слаги и GitHub-трекер |
| [AGENTS.md](AGENTS.md) | правила работы AI-ассистентов |

## Быстрый старт

Нужны Go 1.27, ClickHouse с нативным протоколом (порт 9000) и `CLICKHOUSE_URL`; для PDF-импортёров — `poppler-utils` (`pdftotext`): он нужен `cbr_mt_forecast`, `cbr_psk` и `polyus_financial_metrics` (Debian/Ubuntu: `apt install -y poppler-utils`).

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
| [`cbr`](docs/IMPORTERS.md) | ЦБ РФ: ставка, курс, M2, золото, кредиты, средства клиентов банков (21 ряд: счета организаций, депозиты юрлиц, вклады физлиц с учётом эскроу и без, счета эскроу, средства ИП — итого и в разрезе «рубли / инвалюта», с 2012-02), ставки, ИПЦ, RUONIA, мониторинг предприятий (16 отраслевых листов), макроопрос, резюме ДКП, пресс-релизы и стенограммы пресс-конференций по ключевой ставке, платёжный баланс, ДИП, среднесрочный прогноз, форма 101, кривая бескупонной доходности (КБД), макропруденциальные требования (АЦН-индикаторы, надбавки к коэффициентам риска, МПЛ), публичный кроссворд 87 BSU-кодов на отраслевые листы, график публикаций ЦБ и исторические `published_at`, ставки по кредитам физлицам по срокам и по всем срокам (с 2026-11), ипотечная ставка РФ и среднерыночные значения ПСК (48 выпусков 2014–2026, витрина `v_cbr_psk`) | `cbr_*`, `macro_series`, `cbr_mp_summary*`, `cbr_key_rate_press*`, `cbr_key_rate_pressconf*`, `cbr_bop_*`, `cbr_dip_bulletin`, `cbr_mt_forecast`, `cbr_form101`, `cbr_zcyc`, `cbr_int_rate_loans_ind`, `cbr_mortgage_rate`, `cbr_psk`, `cbr_ccyb_indicators`, `cbr_risk_weight_addons`, `cbr_mpl`, `cbr_mp_bsu_map`, `cbr_publication_calendar`, generic `series_publication`; слой ДИП — витрины `v_point_in_time`, `v_point_in_time_asof` (срез `as_of='YYYY-MM-DD'`), `v_point_in_time_version`; КБД — витрины `v_ofz_curve_compare`, `v_ofz_signal`; макропруденциальные требования — витрины `v_cbr_ccyb_indicators`, `v_cbr_risk_weight_addons`, `v_cbr_mpl`; ПСК — витрина `v_cbr_psk`; Task 7 #248 — витрины `v_cbr_mp_bsu_map`, `v_cbr_mp_public_panel`; календарь — `v_cbr_publication_calendar`, `v_series_publication` | ежедневно 09:17 |
| [`rosstat`](docs/IMPORTERS.md) | ИПЦ (месяц, неделя, до 2022), недельный агрегат ИПЦ 2019+ из BI, зарплаты, ВВП, рынок труда, ИЦП по ВЭД (совокупный ряд, 1998+, m/m / ytd / y/y / period), ИЦП по товарам/ОКПД2 (1 327 кодов, 2021–2026, 4 типа), веса ИЦП по ВЭД (38 кодов, 2017–2026, domestic и domestic+export), веса ИПЦ (товары 2004–2026 и группы/товары 2001–2026), структура потребительских расходов и индексы ИПЦ по позициям (месячная публикация `ipc_spr`, 2025-01+), публичный классификатор ОКВЭД2 (3 102 строки, 87/87 BSU-кодов) | `rosstat_*`; Task 7 #248 — `rosstat_okved2` и витрина `v_rosstat_okved2`; `rosstat_bi_cpi_weekly` — 401 точка 2019-01-09…2026-11-02 (при недоступном BI агрегат добирается PDF-фолбэком из `central-news`); T1 #276 — `rosstat_ppi_ved`; T2 #277 — `rosstat_ppi_product` (336 512 точек 2021-01…2026-08) и `rosstat_ppi_weights` (758 точек, 2017–2026); P0.4 #479 — `rosstat_cpi_weights` (веса ИПЦ: товары 2004–2026 и группы/товары 2001–2026) и витрина `v_rosstat_cpi_weights`; P0.4a #569 — `rosstat_cpi_spr` (месячная структура расходов и индексы ИПЦ, 2025-01+) и витрина `v_rosstat_cpi_spr`; витрина `v_rosstat_ppi` (339 585 строк / 5 408 рядов)| четверг 12:17 || [`ppi`](docs/IMPORTERS.md) | Производный слой ИЦП SA: X-13ARIMA-SEATS `seats_log_cal`, 5 рядов (`total`, `food`, `nonfood`, `all`, `core`), `sa`/`mm_sa`/`saar`/`saar3`; `spec_version`/`taxonomy_version` в каждой строке; требует `Rscript` + `seasonal` | `ppi_sa`; каталог `series_catalog` (`source='ppi_sa'`, 25 рядов); витрина `v_rosstat_ppi` (339 585 строк / 5 408 рядов)| четверг 14:17 |
| [`weeklysa`](docs/IMPORTERS.md) | Производный недельный SA-агрегат ИПЦ: X-13ARIMA-SEATS `seats_log_cal` на 4-нед. индексе официального недельного агрегата (бюллетени 2009–2021 + BI 2019+), 3 метрики `sa_index`/`sa_4w`/`saar`; требует `Rscript` + `seasonal` | `weekly_cpi_sa` (210 полных + 1 nowcast 2009-05…2026-11); каталог `series_catalog` (`source='weekly_cpi_sa'`, 5 рядов: 3 месячных + `saar_q` и `saar_3m`); витрины `v_weekly_cpi_sa` (633 строки / 3 ряда), `v_weekly_cpi_quarterly` и `v_weekly_cpi_rolling3m` (квартальный SAAR и 3-месячный скользящий, #542) | четверг 14:17 |
| [`fedstat`](docs/IMPORTERS.md) | ЕМИСС: еженедельные средние потребительские цены (37426, 158 товаров, 2008–2022 + 2026) и пять макро-блоков — ИПП (43046/57806), розница (31066/31260), сельхоз (34128/59001/31062/43337), инвестиции (58089/58090/34129/40562/34475), ИПЦ 31074 (1 105 товаров) | `fedstat_weekly_prices` (56 133 точки), `fedstat_ipp` (55 968), `fedstat_retail` (4 638), `fedstat_agri` (565), `fedstat_investment` (153), `fedstat_ipc` (204 038); витрины `v_fedstat_*` | пятница 13:43 |
| [`minfin`](docs/IMPORTERS.md) | исполнение федерального и консолидированного бюджета (включая федеральный дефицит), нефтегазовые доходы и НДПИ; общий шаблон импортёра, значения Float64 | `minfin_fed_bud_*`, `minfin_oil_gas_mes` | 10-е 10:41 |
| [`fns`](docs/IMPORTERS.md) | ФНС, форма № 1-НМ: поступления налогов и сборов по видам налогов и уровням бюджета — РФ, 8 федеральных округов и 85 субъектов, месячно (47 рядов × 94 территории × 70 месяцев, 2020-01 … 2026-08; в источнике пропуски 2023-01…07, 2023-09, 2024-01, 2024-02); хранится опубликованное — накопленным итогом с начала года, помесячное считает витрина; инкремент — гейт по множеству месяцев, повтор стоит два GET | `fns_budget_revenue`; витрины `v_fns_budget_revenue` и `v_fns_budget_revenue_monthly`; каталог `source='fns_budget_revenue'` (47 рядов) | ежедневно 09:37 |
| [`minec`](docs/IMPORTERS.md) | Минэк, «Конъюнктура мировых товарных рынков»: средняя за отчётный месяц цена нефти сорта «Юралс» (104 месяца 2018-01 … 2026-10; в источнике нет 2018-06/07); импорт только новых месяцев — гейт по `max(date)`, повтор стоит один GET листинга | `macro_series` (`source='minec'`, `series='urals_avg_price'`); витрина `v_minec_macro` | ежедневно 20:43 |
| [`customs`](docs/IMPORTERS.md) | внешняя торговля по странам | `customs_vneshn_torg` | 15-е 11:41 |
| [`fao`](docs/IMPORTERS.md) | индексы продовольственных цен (6 рядов, включая Sugar) | `fao_food_price_Indices_monthly`; витрина `v_fao_food_price` | 5-е 10:47 |
| [`bank`](docs/IMPORTERS.md) | ДОМ.РФ, ВТБ, Сбер, СберИндекс, Т-Банк | `domrf_mortgage`, `rus_vtb_group_ifrs`, `sber_*`, `tbank_group_ifrs` | понедельник 09:23 |
| [`domrf`](docs/IMPORTERS.md) | рынок жилья ДОМ.РФ: месячные выдачи и ДДУ, портфель, просрочка, ставки выдачи; недельная ставка предложения по продуктам; индекс цен первички и сделки по регионам | `domrf_mortgage_market`, `domrf_mortgage_weekly`, `domrf_price_index`; витрины `v_domrf_mortgage`, `v_domrf_price_index` | понедельник 09:47 |
| [`craw`](docs/IMPORTERS.md) | реестр СБКТС Росстандарта | `gost_vehicle_safety_certificate` | ежечасно :17 |
| [`edisclosure`](docs/IMPORTERS.md) | документы карточек [e-disclosure.ru](https://e-disclosure.ru/): отчётность IFRS/RAS, списки аффилированных лиц, решения собраний (PLZL, SBER, CHMF); коллектор полуавтоматический, импорт детерминированный | `edisclosure_documents`; витрина `v_edisclosure_documents` | 5-е число 21:17 (шаг `dagu/edisclosure.yaml`); сбор — вручную |
| [`fred`](docs/IMPORTERS.md), [`bls`](docs/IMPORTERS.md), [`bea`](docs/IMPORTERS.md), [`eia`](docs/IMPORTERS.md) | US macro: ставки, CPI, PCE, NFP, запасы нефти | `macro_series` | ежедневно / среда |
| [`wgc`](docs/IMPORTERS.md), [`bold`](docs/IMPORTERS.md) | золотые ETF и резервы ЦБ, цена дня | `macro_series`, `bold_fund_snapshot`, `gold_prices` | ежедневно |
| [`news`](docs/IMPORTERS.md) | 16 лент RSS/Telegram | `news_events`, `news_message_version`, `news_annotation` | каждые 30 мин |
| [`gold`](docs/IMPORTERS.md), [`moex`](docs/IMPORTERS.md) | цена золота, PLZL, ОФЗ, KEYRATE, Brent-фронт-месяц | `gold_prices`, `stock_prices`, `ofz_curve`, `keyrate_*`, `moex_market` | ежедневно 18:47/19:13 |
| [`transaq`](docs/IMPORTERS.md) | нетто-объёмы покупок и продаж по публичным сделкам MOEX из ленты внешнего `transaq-clickhouse-exporter` (вселенная — акции IMOEX, фонды денежного рынка AKMM/LQDT/SBMM, ОФЗ TQOB и индексные фьючерсы MIX/RTS/MXI/IMOEXF; тип — `instrument_kind`); минута и день | `transaq_net_flows`; витрины `v_transaq_net_flows`, `v_transaq_net_flows_daily` | каждые 15 мин 09:00–23:45 и 23:55 ежедневно |
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
cbr/ rosstat/ ppi/ weeklysa/ ofz/ ... доменные импортёры
views/ dcf/          витрины, DCF/NAV
ingest/ cmd/ingest/  ingest-endpoint контура прогноза
dagu/                расписания DAG
grafana/ dashboard/  дашборды как код и артефакты
docs/                документация: спецификации, конфигурация, эксплуатация
scripts/lib/         транспорт ClickHouse для публикаций (clickhouse.py)
scripts/             инструменты платформы: harvest, mcp_*, docs_build, xlsx_*, grafana_*, publications/
# модели, бэктесты, gate и MLflow-клиент — в kmlebedev/bi-ml-models
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
| `LLM_MODEL` | модель экспериментального LLM-прогноза ключевой ставки (`make key-rate-llm-forecast` в `bi-ml-models`); работает с общими `LLM_BASE_URL`/`LLM_API_KEY` |
| `NEWS_LLM_PROFILE`, `NEWS_LLM_MODEL`, `GLM_LLM_*`, `NEWS_LLM_BACKLOG_LIMIT` | LLM-разметка видимого потока новостей (Telegram и RSS): профиль, модель, ключи и предел бэклога за прогон |
| `INGEST_TOKEN` | Bearer-токен ingest-endpoint |
| `INGEST_URL` | Базовый URL ingest-endpoint для `tools/forecast_log.py` в `bi-ml-models` (например, `http://127.0.0.1:8081`); обязательна вместе с `INGEST_TOKEN` |
| `EDISCLOSURE_INPUT_DIR`, `EDISCLOSURE_RECHECK`, `EDISCLOSURE_ALLOW_MISSING`, `EDISCLOSURE_OUT` | документы e-disclosure: каталог снапшота коллектора (по умолчанию `build/edisclosure`), глубокий прогон (`1`; переписывает строку при смене `sha256`/метаданных **или** при пустом `archive_key` и включённом архиве — самолечение), мягкий пропуск отсутствующего снапшота в dagu (`1`) и каталог выхода коллектора |
| `ML_LOG`, `ML_RUN_NAME`, `ML_LOCAL_DIR` | журнал ML-эксперимента в `bi-ml-models`: `ML_LOG=1` — мастер-выключатель записи в ClickHouse и включения MLflow-приёмника; имя прогона и каталог локального fallback |
| `MLFLOW_VERSION` | пин версии staging-сервера MLflow (`deploy-staging-mlflow`); клиентские `MLFLOW_TRACKING_URI`/`MLFLOW_HOME`/`MLFLOW_PORT` и `MLFLOW=1` живут в `bi-ml-models` |

Полный список, `ARCHIVE_*`, MCP-конфигурация и работа с ключами — [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Оркестрация (Dagu)

Расписания — `dagu/*.yaml`: `type: chain`, `overlap_policy: skip`, ретраи 3 × 300 с; путь к бинарнику (`ROSSTAT_IMPORT_BIN`) и `CLICKHOUSE_URL` — в `base.yaml`; секреты — в `/etc/dagu/secrets/`. Запуск одного DAG: `dagu start dagu/gold.yaml`.

Зависимости, staging и винтажный архив — [docs/OPERATIONS.md](docs/OPERATIONS.md).

## MLflow (ML-инфраструктура)

MLflow — control plane ML-экспериментов; **канонический backend — staging** на `ps-palmshell` (ssh-алиас `palmshell`): systemd `mlflow.service`, SQLite `sqlite:////var/lib/mlflow/mlflow.db`, артефакты `/var/lib/mlflow/artifacts`, API `0.0.0.0:5000`. Доступ — `MLFLOW_TRACKING_URI=http://192.168.3.9:5000`; fallback — SSH-туннель `ssh -N -L 15001:127.0.0.1:5000 palmshell` → `http://127.0.0.1:15001`. Локальный `build/mlflow` — только dev/офлайн-тесты и изолированные прогоны: он не канонический, не evidence и не называется staging/production.

Клиентские команды — `make mlflow-up/down/status`, `make mlflow-export`, `make mlflow-backfill`, `make mlflow-register/promote/models`, `make ml-gate`, `make ml-live-check` — живут в [`kmlebedev/bi-ml-models`](https://github.com/kmlebedev/bi-ml-models). Здесь остаются staging-юнит `deploy/staging/mlflow.service`, деплой `make deploy-staging-mlflow` и пин `MLFLOW_VERSION`.

ClickHouse — аналитическая реплика MLflow: run'ы экспортируются в `ml_runs`/`ml_predictions`, схема не меняется. Gate читает `v_ml_error_by_fold`, а не MLflow API. Устройство — [ARCHITECTURE.md](ARCHITECTURE.md) §6.8; операции — [docs/OPERATIONS.md](docs/OPERATIONS.md), бэкап и upgrade — `deploy/staging/README.md`.

## Контур прогноза золота

Витрины `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy`, `v_gold_forecasts`, `v_gold_etf_flows*`, `v_news_*`, `v_events_calendar`, `v_dcf_assumptions`, `v_ml_live_accuracy` читаются агентом через MCP (`kimi_reader`, только `v_*`). Ingest-endpoint (`cmd/ingest`) — единственный путь записи прогонов модели, ручных рядов, цен COMEX и консенсус-прогнозов; плагин `gold-nav` — отдельный репозиторий.

Журнал прогнозов ключевой ставки пишется тем же ingest: `POST /v1/key_rate_forecast` (единственный путь, MCP read-only) принимает одну строку `forecast_log` с треком в `metric` (`cbr_rate:taylor`, `cbr_rate:ordered_logit`, `cbr_rate:ml`, `cbr_rate:llm`) и метаданными публикации — распределением вероятностей, моделью, версией набора данных и `prompt_version`. Staged-прогнозы `--stage shadow|canary` получают суффикс `:shadow`/`:canary` и не публикуются. Прогноз отправляет `make key-rate-forecast-log` (`tools/forecast_log.py` в `bi-ml-models`) из локальных артефактов `key_rate_model_forecast.json` и, при наличии, `key_rate_llm_forecast.json` или готового payload #189 (`--llm-payload`); `forecast_resolve` заполняет факт из `cbr_key_rate` и ошибку в базисных пунктах (`error_bp`), сохраняя метаданные строки. Live-точность читается из `v_ml_live_accuracy`, проверка до публикации — `make ml-live-check` в `bi-ml-models`. Спека/план — `docs/specs/2026-10-18-key-rate-forecast-log-design.md`, `docs/plans/2026-10-18-key-rate-forecast-log.md` в том репозитории.

Состав, эндпоинты и запуск — [docs/GOLD_CONTOUR.md](docs/GOLD_CONTOUR.md); схемы — [ARCHITECTURE.md](ARCHITECTURE.md) §6.3/§6.5.

## Разработка

| Команда | Что делает |
|---|---|
| `make all` | gofmt, lint, vet, `go test -race`, сборка |
| `make test` / `make lint` | тесты / линт |
| `make build` | бинарник `linux/amd64` в `build/` |
| `make import STAT=<имя\|группа>` | локальный прогон импортёра; `STAT=key_rate_sources` — все 50 источников контура ключевой ставки одной командой |
| `make docs-check` / `make docs-build` | проверка и сборка docs-портала |
| `make mcp-check` | MCP ClickHouse end-to-end (витрины, сырые таблицы закрыты) |
| `make grafana-mcp-check` | MCP Grafana end-to-end |
| `make graphify-update` | обновление графового индекса graphify (`graphify-out/`) после влива PR в main; code-only, без LLM |
| `make deploy` / `make deploy-staging` | деплой на staging: бинарник, dagu, mcp-clickhouse, mcp-grafana, mlflow |

Модели, бэктесты, gate, MLflow-клиент и live-проверки живут в отдельном репозитории [`kmlebedev/bi-ml-models`](https://github.com/kmlebedev/bi-ml-models): `make cpi-nowcast`, `make key-rate-model`, `make key-rate-forecast-log`, `make ml-gate`, `make ml-live-check`, `make mlflow-*`, `make cb-comp-*`, `make ppi-*`, `make ppi-sa-*`. Здесь остаются ingest-endpoint `forecast_log`, схема `ml_runs`/`ml_predictions`, витрины `v_ml_*`, резолвер `forecast_resolve` и staging-сервис MLflow.

### Роли и скилы

| Роль | Скил |
|---|---|
| Go-инженер | [`go-engineer`](.kimi-code/skills/go-engineer/SKILL.md) |
| Python-инженер (не-ML) | [`python-engineer`](.kimi-code/skills/python-engineer/SKILL.md) |
| ML-инженер | [`bi-ml-models`](https://github.com/kmlebedev/bi-ml-models): README, AGENTS.md, `models/` |
| ML-инженер-тимлид | [`ml-engineer-teamlead`](https://github.com/kmlebedev/bi-ml-models/blob/main/.kimi-code/skills/ml-engineer-teamlead/SKILL.md) |
| Research-инженер | [`research-engineer`](.kimi-code/skills/research-engineer/SKILL.md) |
| Главный редактор | [`editor-in-chief`](.kimi-code/skills/editor-in-chief/SKILL.md) |

Общий роутер — [AGENTS.md](AGENTS.md); диспатч субагентов — [`subagent-dispatch`](.kimi-code/skills/subagent-dispatch/SKILL.md). Скилы не отменяют Go-стандарт для DDL/импортёров.

Инструментальный скилл [`graphify`](.kimi-code/skills/graphify/SKILL.md) — графовый индекс кодовой базы: вопросы по архитектуре/связям файлов обрабатываются запросами к `graphify-out/` (индекс обновляется `make graphify-update` после влива PR в main).

### Подключение как плагин LLM-клиента

LLM-плагин [`rosstat-mcp`](https://github.com/kmlebedev/rosstat-mcp) живёт в отдельном репозитории и даёт read-only MCP: сервер `rosstat-staging` — `https://mcp.selfinvesting.ru:8443/mcp` (витрины `v_*` staging, включая point-in-time слой) — и один скил `rosstat-sql`: как найти ряд и писать оптимальные запросы ClickHouse к этим витринам (с версии 0.4.0, `infra/5.11`; с 0.5.0 — Google OAuth + allowlist). Ролевые скилы [`.kimi-code/skills/`](.kimi-code/skills/) — для сессий, работающих в этом репозитории, и плагином не публикуются.

| Клиент | Установка |
|---|---|
| Claude Code | `claude plugin marketplace add kmlebedev/rosstat-mcp`<br>`claude plugin install rosstat-mcp@rosstat-mcp` |
| Codex | `codex plugin marketplace add kmlebedev/rosstat-mcp`<br>`codex plugin add rosstat-mcp@rosstat-mcp` |
| Kimi Code | `/plugins install https://github.com/kmlebedev/rosstat-mcp` |
| Qwen Code | `qwen extensions install kmlebedev/rosstat-mcp` |
| Cursor | `/add-plugin https://github.com/kmlebedev/rosstat-mcp` |

Доступ выдаётся по Google-аккаунту: владелец добавляет email в `ROSSTAT_MCP_ALLOWED_GOOGLE_EMAILS`, плагин 0.5.0+ входит через Google OAuth (Kimi Code 2.2.0+; остальным клиентам нужна поддержка OAuth); токен `ROSSTAT_MCP_TOKEN` и старые версии плагина больше не работают (cutover на `:8443`). Старый плагин `clickhouse-import-rosstat` (скилы + MCP, установка всего репозитория) больше не публикуется — удаление и миграция описаны в README плагина. Локальный dev-доступ к ClickHouse по-прежнему даёт [`.kimi-code/mcp.json`](.kimi-code/mcp.json) (сервер `clickhouse`, stdio); там же сервер `grafana` — read-only `mcp-grafana`. На staging `mcp-grafana` слушает только `127.0.0.1:8000` и доступен через ssh-туннель (см. [deploy/staging/README.md](deploy/staging/README.md), «Grafana MCP на staging»).

Полный набор целей, Python-тесты, зависимости и CI — [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

## Ограничения

Полный список ограничений и известных проблем — [docs/LIMITATIONS.md](docs/LIMITATIONS.md). Перед использованием ряда в модели прочитайте ограничения его источника: часть данных имеет задержку, ревизии или неполное покрытие.

## Grafana

Шесть дашбордов как код — `gold-thesis`, `key-rate-publications`, `finance-polyus`, `transaq-trades`, `urals-budget-rule`, `bond-attractiveness`. Установка, переменные стендов, цикл деплоя и приёмка — [docs/GRAFANA.md](docs/GRAFANA.md).

## CI и релизы

`go.yml` (test, lint, vulncheck, build), `dashboards.yml`, `pages.yml` (docs-портал) и `release.yml` (тег `v*`). Подробности — [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).
