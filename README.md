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

Нужны Go 1.27, ClickHouse с нативным протоколом (порт 9000) и `CLICKHOUSE_URL`.

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
| [`cbr`](docs/IMPORTERS.md) | ЦБ РФ: ставка, курс, M2, золото, кредиты, ставки, ИПЦ, RUONIA, мониторинг предприятий, макроопрос, резюме ДКП, платёжный баланс, ДИП, среднесрочный прогноз, форма 101 | `cbr_*`, `macro_series`, `cbr_mp_summary*`, `cbr_bop_*`, `cbr_dip_bulletin`, `cbr_mt_forecast`, `cbr_form101` | ежедневно 09:17 |
| [`rosstat`](docs/IMPORTERS.md) | ИПЦ (месяц, неделя, до 2022), зарплаты, ВВП, рынок труда | `rosstat_*` | четверг 12:17 |
| [`minfin`](docs/IMPORTERS.md) | исполнение федерального бюджета | `minfin_fed_bud_*` | 10-е 10:41 |
| [`customs`](docs/IMPORTERS.md) | внешняя торговля по странам | `customs_vneshn_torg` | 15-е 11:41 |
| [`fao`](docs/IMPORTERS.md) | индексы продовольственных цен | `fao_food_price` | 5-е 10:47 |
| [`bank`](docs/IMPORTERS.md) | ДОМ.РФ, ВТБ, Сбер, СберИндекс, Т-Банк | `domrf_mortgage`, `rus_vtb_group_ifrs`, `sber_*`, `tbank_group_ifrs` | понедельник 09:23 |
| [`craw`](docs/IMPORTERS.md) | реестр СБКТС Росстандарта | `gost_vehicle_safety_certificate` | ежечасно :17 |
| [`fred`](docs/IMPORTERS.md), [`bls`](docs/IMPORTERS.md), [`bea`](docs/IMPORTERS.md), [`eia`](docs/IMPORTERS.md) | US macro: ставки, CPI, PCE, NFP, запасы нефти | `macro_series` | ежедневно / среда |
| [`wgc`](docs/IMPORTERS.md), [`bold`](docs/IMPORTERS.md) | золотые ETF и резервы ЦБ, цена дня | `macro_series`, `bold_fund_snapshot`, `gold_prices` | ежедневно |
| [`news`](docs/IMPORTERS.md) | 9 лент RSS/Telegram | `news_events`, `news_message_version`, `news_annotation` | каждые 30 мин |
| [`gold`](docs/IMPORTERS.md), [`moex`](docs/IMPORTERS.md) | цена золота, PLZL, ОФЗ, KEYRATE | `gold_prices`, `stock_prices`, `ofz_curve`, `keyrate_*` | ежедневно 18:47/19:13 |
| [`calendar`](docs/IMPORTERS.md), [`forecast`](docs/IMPORTERS.md) | календарь событий, резолвер прогнозов | `events_calendar`, `forecast_log` | ежедневно |
| [`views`](docs/IMPORTERS.md) | витрины контура и метрик компаний | `v_*`, `company_financials` | понедельник / вручную |
| [`dcf`](docs/IMPORTERS.md), [`polyus`](docs/IMPORTERS.md), [`financial`](docs/IMPORTERS.md) | LOM-планы/NAV, отчётность Полюса, legacy | `mine_plans`, `price_decks`, `nav_by_asset`, `databook_polyus`, `company_financials` | понедельник |

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
| `NEWS_LLM_PROFILE`, `NEWS_LLM_MODEL`, `GLM_LLM_*` | LLM-разметка Telegram |
| `INGEST_TOKEN` | Bearer-токен ingest-endpoint |

Полный список, `ARCHIVE_*`, MCP-конфигурация и работа с ключами — [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Оркестрация (Dagu)

Расписания — `dagu/*.yaml`: `type: chain`, `overlap_policy: skip`, ретраи 3 × 300 с; путь к бинарнику (`ROSSTAT_IMPORT_BIN`) и `CLICKHOUSE_URL` — в `base.yaml`; секреты — в `/etc/dagu/secrets/`. Запуск одного DAG: `dagu start dagu/gold.yaml`.

Зависимости, staging и винтажный архив — [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Контур прогноза золота

Витрины `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy`, `v_gold_forecasts`, `v_gold_etf_flows*`, `v_news_*`, `v_events_calendar`, `v_dcf_assumptions` читаются агентом через MCP (`kimi_reader`, только `v_*`). Ingest-endpoint (`cmd/ingest`) — единственный путь записи прогонов модели, ручных рядов, цен COMEX и консенсус-прогнозов; плагин `gold-nav` — отдельный репозиторий.

Состав, эндпоинты и запуск — [docs/GOLD_CONTOUR.md](docs/GOLD_CONTOUR.md); схемы — [ARCHITECTURE.md](ARCHITECTURE.md) §6.3/§6.5.

## Разработка

| Команда | Что делает |
|---|---|
| `make all` | gofmt, lint, vet, `go test -race`, сборка |
| `make test` / `make lint` | тесты / линт |
| `make build` | бинарник `linux/amd64` в `build/` |
| `make import STAT=<имя>` | локальный прогон импортёра |
| `make mcp-check` | MCP end-to-end |
| `make docs-check` / `make docs-build` | проверка и сборка docs-портала |

Пункт роадмапа ведётся через GitHub issue с составным ID `<slug>/<item-id>`: шаблоны — [`.github/ISSUE_TEMPLATE/roadmap-item.yml`](.github/ISSUE_TEMPLATE/roadmap-item.yml) и [`.github/pull_request_template.md`](.github/pull_request_template.md); операции — скил `github-tracker` ([`.kimi-code/skills/github-tracker/SKILL.md`](.kimi-code/skills/github-tracker/SKILL.md)); правила — [docs/ROADMAP_INDEX.md](docs/ROADMAP_INDEX.md) и правило 12 [AGENTS.md](AGENTS.md). Живой проект — [clickhouse-import-rosstat — Roadmap](https://github.com/users/kmlebedev/projects/11).

Полный набор целей, Python-тесты, зависимости и CI — [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

## Ограничения

Полный список ограничений и известных проблем — [docs/LIMITATIONS.md](docs/LIMITATIONS.md). Перед использованием ряда в модели прочитайте ограничения его источника: часть данных имеет задержку, ревизии или неполное покрытие.

## Grafana

Три дашборда как код — `gold-thesis`, `key-rate-publications`, `finance-polyus`. Установка, переменные стендов, цикл деплоя и приёмка — [docs/GRAFANA.md](docs/GRAFANA.md).

## CI и релизы

`go.yml` (test, lint, vulncheck, build), `dashboards.yml`, `pages.yml` (docs-портал) и `release.yml` (тег `v*`). Подробности — [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).
