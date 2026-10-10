# clickhouse-import-rosstat

Go-конвейер импорта российской макроэкономической и финансовой статистики в ClickHouse. Данные используются дашбордами Grafana и аналитическими моделями (сценарный прогноз золота, DCF и NAV акций ПАО «Полюс»). Один бинарник, конфигурация только через переменные окружения.

Подробная архитектура, схемы таблиц и конвенции — в [ARCHITECTURE.md](ARCHITECTURE.md). План развития — в [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md). Правила для AI-ассистентов — в [AGENTS.md](AGENTS.md). Контур прогноза золота (витрины, календарь, ingest-endpoint, плагин `gold-nav`) — в разделе «Контур прогноза золота» и в [спецификации](docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md).

## Что импортируется

Каждый источник — импортёр с `init()`-регистрацией. Расписание задаёт Dagu (файлы в `dagu/`).

| Домен (пакет) | Источник | Таблицы ClickHouse | Расписание (МСК) |
|---|---|---|---|
| `cbr` | Банк России: ключевая ставка, курс USD, M2, золото, кредиты, ОФЗ-индикаторы, инфляционные ожидания | `cbr_key_rate`, `cbr_currency_usd`, `cbr_m2`, `cbr_gold`, `cbr_bank_int_rate`, `cbr_loans_to_corporations`, `cbr_loans_to_individuals`, `cbr_infl_exp`, `cbr_credit_m2x`, `cbr_indicators_cpd`, `cbr_ruania`, `households_b_mes`, `cbr_queries_*`; каталог `series_catalog`, витрина `v_cbr_macro` | ежедневно 09:17 |
| `rosstat` | Росстат: ИПЦ (месяц, неделя), зарплаты, ВВП по кварталам | `ipc_mes`, `ipc_weeks`, `salaries_mes`, `vvp_kvartal`; каталог `series_catalog`, витрина `v_rosstat_macro` | четверг 12:17 |
| `minfin` | Минфин: исполнение федерального бюджета | `minfin_fed_bud_mes`, `minfin_fed_bud_mesyats`; каталог `series_catalog`, витрина `v_minfin_budget` | 10-е число 10:41 |
| `customs` | ФТС: внешняя торговля по странам | `customs_vneshn_torg` | 15-е число 11:41 |
| `fao` | ФАО: индексы продовольственных цен | `fao_food_price` | 5-е число 10:47 |
| `bank` | Банки: индексы потребительских расходов, финрезультаты (Сбер, ВТБ, Т-Банк), ипотека ДОМ.РФ | `sber_consumper_spending_index`, `sber_izmenenie_trat`, `sber_finansovie_rezultaty`, `rus_vtb_group_ifrs`, `tbank_group_ifrs`, `domrf_mortgage` | понедельник 09:23 |
| `craw` | Росстандарт: сертификаты ГОСТ (краулер) | `gost_*` | 1-е число 11:31 |
| `fred` | FRED (CSV): DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE | `macro_series` (`source = 'fred'`), каталог `series_catalog`, витрина `v_fred_macro` | ежедневно 11:41 |
| `bls` | BLS API v2: CPI (CUUR0000SA0, CUSR0000SA0), безработица, NFP, средняя зарплата, PPI, JOLTS | `macro_series` (`source = 'bls'`), каталог `series_catalog`, витрина `v_bls_macro` | ежедневно 12:23 |
| `bea` | BEA API: индексы PCE (headline, excluding food and energy), таблица NIPA T20804 | `macro_series` (`source = 'bea'`) | ежедневно 12:37 |
| `gold` | Золото: фиксинг MOEX GOLDFIXME (₽/г) пересчитан в USD/oz по курсу ЦБ | `gold_prices` (`venue = 'moex_fix_usd'`); каталог `series_catalog`, витрина `v_gold_prices` | ежедневно 18:47 |
| `moex` | МосБиржа ISS: свечи PLZL (OHLCV), индекс RGBI, доходности G-curve ОФЗ (1y/3y/5y/10y) | `stock_prices` (`code = 'PLZL'`), `ofz_curve`; каталог `series_catalog`, витрины `v_stock_prices`, `v_ofz_curve` | ежедневно 19:13 |
| `calendar` | Календарь событий-триггеров прогноза золота/NAV (Q4-2026; сид — `sql/events_calendar_q4_2026.sql`) | `events_calendar`; витрина `v_events_calendar` | вручную (`make import STAT=events_calendar`) |
| `views` | Витрины контура прогноза: `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy` (импортёр `gold_views`) | витрины; гранты `kimi_reader` — в `sql/mcp_kimi_reader.sql` | вручную, после первого запуска ingest: `make import STAT=gold_views` |
| `views` | Метрики компаний сектора: таблица `company_financials` + витрины `v_company_financials` (разрешённое значение, одна строка на компанию/метрику/период), `v_company_metric_sources` (все версии, включая проигравшую), `v_company_operating` (операционка по активам); каталог рядов Полюса в `series_catalog` (импортёр `company_views`) | таблица `company_financials`, витрины; гранты `kimi_reader` — в `sql/mcp_kimi_reader.sql` | `dagu/financial.yaml`, понедельник 10:23 (МСК), после `polyus_financial_metrics`; вручную — `make import STAT=company_views` |
| `dcf` | Расчётное ядро sum-of-parts LOM-NAV Полюса (импортёр `dcf_engine`): НДПИ-функция, LOM-NPV с хвостом закрытия, эскалация по ИПЦ, два контура ставки. Читает `mine_plans` и `price_decks`, пишет `nav_by_asset` (активы × 3 дека × 2 контура) | `mine_plans`, `price_decks`, `nav_by_asset`; витрина `v_dcf_assumptions` (грант `kimi_reader`) | вручную: `DCF_RUN_ID=<uuid> make import STAT=dcf_engine`; DAG-файл появится вместе с сидом `mine_plans` |
| `polyus` | Отчётность ПАО «Полюс»: xlsx-датапак (операционные результаты по активам с 2007) и PDF-отчёты (KPI-пресс-релизы EN/RU + МСФО-формы EN). Разбор PDF — колоночная модель: `pdftotext -tsv`, полоса шапки по ролям строк, колонки из неё по X | `databook_polyus`; PDF-отчёты → `company_financials` (общая таблица сектора) | понедельник 10:23 |
| `financial` | Legacy: корпоративные databook'и (ЮГК), investing.com, РЖД | `databook_ugk` и др. | понедельник 10:23 |

Импортёр `gold` — временный: производная цена, не LBMA. Официальный LBMA AM/PM пока не подключён, см. «Ограничения».

Импортёр `dcf_engine` (пакет `dcf/`) считает sum-of-parts LOM-NAV по активам: читает LOM-планы из `mine_plans` и цены из `price_decks` (на пустой таблице деков сеет её сама — спот из `gold_prices` плюс два задокументированных LT-якоря), считает NPV каждого актива по трём декам и двум контурам ставки и пишет `nav_by_asset` **одним батчем**. Агенту виден только результат — витрина `v_dcf_assumptions` (ARCHITECTURE.md §6.3); сырые `mine_plans`, `price_decks` и `nav_by_asset` не выданы. Запуск:

```bash
DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine   # пишет nav_by_asset
make import STAT=dcf_engine                                      # без DCF_RUN_ID ничего не пишет
```

`run_id` — клиентский и обязателен: строки `nav_by_asset` ссылаются на `model_runs`, а туда пишет только ingest-endpoint. Без `DCF_RUN_ID` движок считает, печатает в лог готовое тело `POST /v1/model_run` (его дописывает человек и отправляет в ingest) и **не пишет ничего** — `0` строк и запись в лог. Пока `mine_plans` пуста, прогон тоже успешен: предупреждение и `Imported 0 rows` (сид LOM-планов из отчётности Полюса — отдельный пункт `docs/ROADMAP_DCF_POLYUS.md` §7, фаза 3). Строк на прогон — `активы × 3 дека × 2 контура`.

Отчётность Полюса даёт два импортёра в пакете `polyus/`: `databook_polyus` (xlsx-датапак) и `polyus_financial_metrics` (PDF-отчёты, пишет в общую таблицу `company_financials`). Текст извлекается `pdftotext -tsv`; полоса шапки финансовой таблицы собирается по ролям строк (маркер единиц, подписи периодов, подписи изменений), а подписи периодов, разбитые переносом строки («4Q» на одной строке шапки, «2019» на другой), склеиваются по X; значения раскладываются по колонкам, выведенным из шапки по X-координатам. Запуск: `make import STAT=polyus_financial_metrics` или `make import STAT=databook_polyus`; витрины над `company_financials` — `make import STAT=company_views` (в расписании — шаг после импорта метрик, см. `dagu/financial.yaml`). Ограничения этих данных — в разделе «Ограничения»; из 14 отчётов списка включены 9 (2014FY, 2019FY…2024FY, 1H2026), отключены 5 — 2015FY…2018FY (другой тип документа: MD&A и консолидированная МСФО) и 2025H2.

## Структура репозитория

```text
main.go                  точка входа: подключение к ClickHouse, фильтр импортёров, код выхода
chimport/                интерфейс ImportStat и глобальный реестр Stats
util/                    HTTP-клиент (национальные TLS-сертификаты РФ, ротация User-Agent), шаблон импортёра ClickHouseImport, батч-вставка
bank/ cbr/ craw/ customs/ fao/ minfin/ rosstat/   доменные импортёры (см. таблицу выше)
fred/                    FRED CSV → macro_series
calendar/                календарь событий-триггеров → events_calendar
views/                   витрины контура прогноза (v_model_inputs, v_gold_dashboard, v_forecast_accuracy; импортёр gold_views) и витрины метрик компаний сектора (company_financials → v_company_financials, v_company_metric_sources, v_company_operating; импортёр company_views + описания рядов Полюса в series_catalog, views/series_meta.go)
ingest/                  ingest-endpoint контура прогноза: model_runs, forecast_log, macro_series (source='manual')
cmd/ingest/              бинарник ingest (отдельный от основного импорта)
bls/                     BLS API v2 → macro_series
bea/                     BEA API (NIPA T20804) → macro_series
gold/                    MOEX GOLDFIXME + cbr_currency_usd → gold_prices
moex/                    MOEX ISS: свечи PLZL → stock_prices, RGBI + G-curve → ofz_curve
polyus/                  отчётность Полюса: xlsx-датапак → databook_polyus; PDF-отчёты (KPI EN/RU, МСФО EN) → company_financials (общая таблица сектора; читается витринами v_company_*, разбор — колоночная модель: pdftotext -tsv + полоса шапки по ролям строк + колонки по X)
dcf/                     расчётное ядро DCF: sum-of-parts LOM-NAV по активам, таблицы mine_plans, price_decks, nav_by_asset (DDL — копии канонических из ARCHITECTURE.md §6.2), чистое ядро без БД (НДПИ на унцию, LOM-NPV с хвостом закрытия, эскалация по ИПЦ, две ставки контуров); импортёр dcf_engine, витрина views/dcf_assumptions.go. model_runs не пишет
financial/               legacy-контур (database/sql, свой main), новый код туда не добавляется; остался databook_ugk (ЮГК)
dagu/                    расписания: один DAG-файл на домен, имя файла = имя DAG
sql/                     DDL и гранты вручную: пользователь kimi_reader, сид календаря Q4-2026
scripts/                 проверки MCP (mcp_setup_user.py, mcp_check.py) и тесты скриптов
deploy/staging/           артефакты деплоя на staging: юниты dagu и mcp-clickhouse, образец env
docs/                    аналитические статьи и дорожная карта
.github/workflows/       CI (go.yml) и релиз по тегу v* (release.yml)
```

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

## Конфигурация

| Переменная | Назначение |
|---|---|
| `CLICKHOUSE_URL` | DSN подключения (обязательна). Не выводится в логах |
| `CLICKHOUSE_IMPORT_STAT` | Фильтр импортёров через запятую по имени (`Name()`). Пусто — все импортёры |
| `LOG_LEVEL` | Уровень logrus: `debug`, `info`, `warn`, `error` |
| `CERT_FILES` | PEM-сертификаты через запятую (нац. УЦ РФ для gos-сайтов) |
| `BLS_API_KEY` | Регистрационный ключ BLS (необязателен; без него работает, но лимиты строже). Не выводится в логах |
| `BEA_API_KEY` | UserID BEA API (обязателен для `bea`; без него импортёр завершается ошибкой). Не выводится в логах |
| `INGEST_TOKEN` | Bearer-токен ingest-endpoint, обязателен для `cmd/ingest` (без него процесс не стартует). Не выводится в логах |
| `INGEST_ADDR` | Адрес HTTP-сервера `cmd/ingest`, по умолчанию `:8081` |
| `INGEST_RATE_PER_MIN` | Лимит запросов `cmd/ingest` в минуту (token bucket, burst 10), по умолчанию 60; превышение — `429` |

Поведение при запуске: неизвестное имя в `CLICKHOUSE_IMPORT_STAT` даёт предупреждение; ошибка любого импортёра даёт код выхода `1`, остальные импортёры при этом выполняются.

Импортёр `financial` читает `TICKER`, `FINANCIAL_DATA_DIR`, `INVESTING_EMAIL`, `INVESTING_PASSWORD`; эти переменные устаревают вместе с legacy-контуром.

`DCF_RUN_ID` — переменная окружения **разового прогона** `dcf_engine`, а не строка конфигурации источника: UUID прогона, который уходит в `nav_by_asset.run_id` и связывает строки с записью `model_runs` (её создаёт ingest-endpoint). Передавайте её прямо в команде — `DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine`; в `~/.config/rosstat/env` её хранить не нужно. Без `DCF_RUN_ID` движок считает и печатает тело `POST /v1/model_run`, но не пишет ни строки (см. «Что импортируется»).

## Оркестрация (Dagu)

Расписания описаны в `dagu/*.yaml`: `type: chain`, `overlap_policy: skip`, каждый шаг запускает бинарник с `CLICKHOUSE_IMPORT_STAT=<имя>`, ретраи — 3 попытки с паузой 300 с. Путь к бинарнику (`ROSSTAT_IMPORT_BIN`) и `CLICKHOUSE_URL` задаются в `base.yaml` Dagu; секреты лежат в файлах `/etc/dagu/secrets/` с правами `0600`, в репозитории их нет. Внутри домена зависимости задаются через `depends`; между DAG зависимостей нет (см. `gold.yaml`: шаг `cbr_currency_usd` стоит внутри DAG).

Запуск одного DAG вручную: `dagu start dagu/gold.yaml` (dev-окружение Dagu должно видеть `ROSSTAT_IMPORT_BIN` и `CLICKHOUSE_URL` через `base.yaml`).

## Контур прогноза золота

Составные части контура (подробно — ARCHITECTURE.md §6.3 и §6.5):

- витрины `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy` и календарь `v_events_calendar` — читаются агентом через MCP (пользователь `kimi_reader`, только `v_*`);
- витрины метрик компаний сектора `v_company_financials`, `v_company_metric_sources`, `v_company_operating` (импортёр `company_views`) — читаются агентом через MCP; сейчас наполнены данными Полюса (ARCHITECTURE.md §6.3);
- витрина `v_dcf_assumptions` — NPV рудников Полюса по каждому (deck, контур ставки, актив) плюс суммарный NAV рудников; пишет её импортёр `dcf_engine` (пакет `dcf/`, `DCF_RUN_ID=<uuid> make import STAT=dcf_engine`), сырая `nav_by_asset` агенту не выдана;
- ingest-endpoint (`cmd/ingest`) — единственный путь записи прогонов модели и ручных рядов;
- плагин Kimi `gold-nav` — отдельный репозиторий, сценарная сессия прогноза и DCF NAV PLZL.

### Ingest-endpoint

Запуск: `make run-ingest` (нужны `CLICKHOUSE_URL` и `INGEST_TOKEN` в `~/.config/rosstat/env`; `INGEST_ADDR` по умолчанию `:8081`). При старте создаются `model_runs`, `forecast_log`, `macro_series`, если их ещё нет. Витрины `views/` создаются отдельно: `make import STAT=gold_views` после первого запуска ingest.

| Метод и путь | Тело | Ответ |
|---|---|---|
| `POST /v1/model_run` | JSON прогона: клиентский `run_id` (UUID), `trigger_type`, `price_deck`, `probabilities`, `gold_scenario` с `horizon`, значения NAV | `200 {"inserted": 1}`; `400` при ошибке валидации |
| `POST /v1/manual_series` | JSON-массив `{"series", "date": "YYYY-MM-DD", "value"}` → `macro_series`, `source = 'manual'` | `200 {"inserted": N}`; `400` при ошибке любой точки |

Все запросы — с заголовком `Authorization: Bearer <INGEST_TOKEN>`; без него или с неверным токеном — `401`. Сверх `INGEST_RATE_PER_MIN` (по умолчанию 60 в минуту) — `429`; тело больше 1 MiB — `413`; неизвестное поле в JSON — `400`. Повторный `POST /v1/model_run` с тем же `run_id` ничего не записывает. Формат тела — в `ingest/model_run.go` (структура `ModelRun`).

### Плагин gold-nav

Плагин — отдельный репозиторий (не входит в этот проект). Локальная копия рядом: `../gold-nav`. Спецификация: [docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md](docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md).

- Kimi Code: `/plugins install <github-url>` (ссылка на репозиторий — после публикации);
- Kimi Work: зарегистрировать каталог плагина в персональном маркете (`kimi-daimon kimi-plugin register-personal <путь к gold-nav>`) и включить в «Плагины → Персональный».

Переменные окружения пользователя плагина (только имена, значения выдаёт автор): `GOLD_NAV_MCP_TOKEN` — read-only токен MCP; `GOLD_NAV_INGEST_URL` и `GOLD_NAV_INGEST_TOKEN` — только у автора. Команды плагина: `/gold-nav:session` (сценарная сессия) и `/gold-nav:verify` (сверка прогнозов с фактом).

Живая приёмка плагина в Kimi Code и Kimi Work не выполнена; см. раздел «Результат проверки» в README плагина.

## Разработка

| Команда | Что делает |
|---|---|
| `make lint` | `gofmt -l` (падает, если есть неотформатированный код) и `golangci-lint run ./...` |
| `make test` | `go vet` и `go test -race ./...` (тесты есть у `bls`, `bea`, `cbr`, `rosstat`, `minfin`, `gold`, `moex`, `polyus`) |
| `make build` | статический бинарник `linux/amd64` в `build/` |
| `make run` | сборка и запуск (нужен `CLICKHOUSE_URL`) |
| `make import STAT=<имя>` | локальный прогон одного импортёра по `Name()` (собирает `build/clickhouse-import-rosstat-native`). Для расчёта NAV: `DCF_RUN_ID=$(uuidgen \| tr A-Z a-z) make import STAT=dcf_engine` |
| `make build-ingest` | сборка ingest-endpoint (`cmd/ingest`) в `build/ingest` |
| `make run-ingest` | сборка и запуск ingest-endpoint (нужны `CLICKHOUSE_URL` и `INGEST_TOKEN`) |
| `make fmt` | `gofmt -w .` |
| `make deps` | `go mod download` и `go mod tidy` |
| `make clean` | удалить `build/` |
| `make ch-up` | запустить локальный ClickHouse (порты 8123/9000), если он ещё не отвечает |
| `make ch-down` | остановить сервер, поднятый через `make ch-up` (по PID-файлу) |
| `make ch-status` | проверить, отвечает ли сервер на `localhost:8123` |
| `make ch-sql FILE=...` | прогнать SQL-файл репозитория в локальный ClickHouse (`--multiquery`; пользователь `CH_USER`, по умолчанию `default` без пароля) |
| `make dev-check` | проверить наличие `go`, `gofmt`, `golangci-lint`, `uv`, `curl`, `clickhouse` |
| `make mcp-run` | запустить MCP-сервер `mcp-clickhouse` в stdio вручную (нужен `CLICKHOUSE_PASSWORD`) |
| `make mcp-user` | создать пользователя `kimi_reader` и выдать `GRANT SELECT` на витрины из `sql/mcp_kimi_reader.sql` (нужен `CLICKHOUSE_PASSWORD`, запущенный ClickHouse) |
| `make mcp-check` | smoke-проверка MCP: инструменты, витрины, отказ на сырые таблицы (нужен `CLICKHOUSE_PASSWORD`, запущенный ClickHouse). Адрес переопределяется: `MCP_CHECK_HOST`/`MCP_CHECK_PORT` — для проверки staging через туннель |
| `make deploy-staging` | развернуть на staging бинарник, `dagu` и `mcp-clickhouse` (см. «Деплой на staging») |
| `make deploy-staging-binary` / `-dagu` / `-mcp` | точечно: только бинарник, только dagu, только MCP |
| `make upgrade-staging` | обновить **только** dagu и mcp-clickhouse до версий из репозитория (без `apt upgrade`) |
| `make mcp-user-staging` | создать `kimi_reader` на staging (нужны `CLICKHOUSE_PASSWORD` и `CH_SETUP_PASSWORD`) |

### Деплой на staging

Разворачивает три компонента на хост `palmshell`: бинарник `clickhouse-import-rosstat`, планировщик `dagu` v2.18.2 и MCP-сервер `mcp-clickhouse` 0.7.0 (HTTP, read-only). ClickHouse и Grafana на хосте не трогаются. Артефакты — `deploy/staging/`, порядок и отладка — `deploy/staging/README.md`.

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

Сервисы слушают только `127.0.0.1` (`dagu` :8080, MCP :8000): наружу ничего не публикуется, TLS/Caddy и публичный MCP-эндпоинт — следующая фаза.

### Зависимости для разработки

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
DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine   # расчёт NAV (пока mine_plans пуста — 0 строк; в шелле без uuidgen — любой UUID)
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

### Ключи и окружение

Ключи и пароли лежат в одном файле вне репозитория: `~/.config/rosstat/env` (права `600`, каталог `700`). Формат:

```bash
export BEA_API_KEY=<your-key>
export CLICKHOUSE_URL=clickhouse://localhost:9000/default
export CLICKHOUSE_PASSWORD=<kimi_reader-password>
# export BLS_API_KEY=<your-key>
# export INGEST_TOKEN=<ingest-token>
# export INGEST_RATE_PER_MIN=60
```

- Makefile подключает файл сам (`-include`), поэтому любая `make`-цель видит ключи без `export` в shell; путь можно переопределить: `make ROSSTAT_ENV=/путь/к/файлу ...`;
- в терминале: `set -a; . ~/.config/rosstat/env; set +a`;
- проверка без вывода значений: `make env-check`;
- локальный импорт одного источника: `make import STAT=bea` (собирает бинарник под macOS в `build/clickhouse-import-rosstat-native`);
- Kimi Code: ключи для MCP задаются в `~/.kimi-code/mcp.json` (блок `env`), для остальных команд Kimi вызывает `make`, поэтому файл окружения подхватывается автоматически;
- Dagu (расписания): секреты описаны в `~/dagu-dev/base.yaml` через provider `file`, каждый ключ — отдельный файл `~/dagu-dev/secrets/<name>` (`600`). Для BEA: `bea_api_key`; при добавлении ключа нового импортёра — новый файл и строка в `secrets:`;
- файлы `*.env`, `secrets/` и ключи в коде не коммитить; `CLICKHOUSE_PASSWORD` только в файле окружения (`~/.config/rosstat/env`), в `mcp.json` его нет.

MCP для Kimi (агент читает витрины `v_*`, без записи):

- установить `uv`: `brew install uv`;
- создать пользователя и выдать права: `make mcp-user` (берёт `CLICKHOUSE_PASSWORD` из файла окружения, выполняет `sql/mcp_kimi_reader.sql`; пароль в выводе не печатается; повторный запуск не меняет уже созданного пользователя);
- записать `~/.kimi-code/mcp.json` (права `600`); пароль в файл не пишется, команда сама подгружает `~/.config/rosstat/env`:

```json
{
  "mcpServers": {
    "clickhouse": {
      "command": "bash",
      "args": ["-c", "set -a; . \"$HOME/.config/rosstat/env\"; set +a; exec uv run --with mcp-clickhouse --python 3.12 mcp-clickhouse"],
      "env": {
        "CLICKHOUSE_HOST": "localhost",
        "CLICKHOUSE_PORT": "8123",
        "CLICKHOUSE_SECURE": "false",
        "CLICKHOUSE_VERIFY": "false",
        "CLICKHOUSE_USER": "kimi_reader",
        "CLICKHOUSE_DATABASE": "default",
        "CLICKHOUSE_MCP_SERVER_TRANSPORT": "stdio"
      }
    }
  }
}
```

- проверить в Kimi командой `/mcp-config` в новой сессии: сервер `clickhouse` должен быть подключён;
- новые ряды и витрины: см. AGENTS.md, правило 11. Legacy-таблицы `cbr_*`, `rosstat`, `minfin`, `gold_prices` агенту доступны через групповые витрины `v_cbr_macro`, `v_rosstat_macro`, `v_minfin_budget`, `v_gold_prices` (создаются, когда все таблицы группы импортированы), описания рядов — в `v_series_catalog`.

MCP GoLand (опционально, ускоряет работу Kimi Code с кодом):

- зачем: агент получает инструменты IDE — ошибки и инспекции файла по индексу GoLand (`get_file_problems`), сигнатуры символов (`get_symbol_info`), семантический поиск и иерархию вызовов (`search_symbol`, `analyze_calls`), безопасный rename (`rename_refactoring`); как и когда ими пользоваться — в AGENTS.md, раздел «Инструменты GoLand (MCP `jetbrains`)». Без них агент работает обычными `Grep`/`Read`/`Edit`, настройка не обязательна;
- требование: GoLand 2025.2 или новее — MCP-сервер встроен (плагин MCP Server включён по умолчанию); npm-пакет `@jetbrains/mcp-proxy` deprecated и не нужен;
- настройка:
  1. открыть этот репозиторий в GoLand;
  2. Settings | Tools | MCP Server → включить **Enable MCP Server**;
  3. там же: **Copy HTTP Stream Config** — в буфере окажется URL вида `http://127.0.0.1:<port>/stream`;
  4. добавить запись в `~/.kimi-code/mcp.json` (URL из шага 3, порт подставить свой):

  ```json
  {
    "mcpServers": {
      "jetbrains": {
        "url": "http://127.0.0.1:<port>/stream"
      }
    }
  }
  ```

  5. начать новую сессию Kimi Code (`/new`) и проверить `/mcp-config`: сервер `jetbrains` подключён;
- порт выдаётся динамически и может смениться после перезапуска GoLand: если инструменты `jetbrains` пропали, повторите шаги 3–4;
- инструменты видны только пока GoLand запущен с открытым проектом.

Правила кода (подробно — в [AGENTS.md](AGENTS.md) и [ARCHITECTURE.md](ARCHITECTURE.md)):

- вставка в ClickHouse только батчами: `PrepareBatch` → `Append` → `Send`; построчный `Exec` в цикле запрещён;
- секреты только через `os.Getenv`; сетевых вызовов в `init()` не делать;
- HTTP только через `util.HttpClient` (и `util.GetXlsx`, `util.GetCSV`);
- DDL — `CREATE TABLE IF NOT EXISTS`, движок `ReplacingMergeTree`, значения новых таблиц — `Float64`;
- ошибки не проглатывать: парсинг чисел и `batch.Append` проверяются.

### CI и релизы

`.github/workflows/go.yml` на каждый push в `main` и на pull request:

- `test`: `gofmt`, `go vet`, `go test -race`;
- `lint`: `golangci-lint` v2.14.0 с конфигом `.golangci.yml`. Пакет `financial/` исключён из линта как legacy;
- `vulncheck`: `govulncheck ./...` (падает при уязвимостях, которые затрагивают код);
- `build`: `make build`, артефакт `build/`.

Все задания используют Go `1.27.x`: `govulncheck` проверяет стандартную библиотеку тулчейном, которым запущен, и на go1.27.1 находил уязвимости `net/http` и `crypto/tls`, исправленные в go1.27.2.

`.github/workflows/release.yml` на тег `v*` запускает тесты и `goreleaser` (архив `linux/amd64`, `SHA256SUMS`, файлы `README.md` и `dagu/*.yaml`). Конфиг — `.goreleaser.yaml`; проверка: `goreleaser check`. В релиз входит только основной бинарник импорта; `cmd/ingest` собирается локально через `make build-ingest`.

## Ограничения и известные проблемы

- **LBMA AM/PM не подключён.** Официальный LBMA JSON, Nasdaq Data Link `LBMA/GOLD` и stooq недоступны из этой сети (403 WAF, JS-проверка), FRED серии LBMA удалил. Временно используется `gold` (MOEX GOLDFIXME ÷ курс ЦБ).
- **Курс ЦБ без понедельников и новогодних праздников.** В `cbr_currency_usd` этих дат нет; `gold` берёт последний курс не позже даты и пропускает дни без курса старше 10 дней. Это дыра в источнике ЦБ.
- **Импортёры ЦБ/Росстата/ВТБ берут ссылку со страницы источника.** `cbr_infl_exp`, `cbr_credit_m2x`, `vvp_kvartal`, `salaries_mes`, `rus_vtb_group_ifrs` не хардкодят URL XLSX, а скрейпят актуальную ссылку (colly). Остальные источники ЦБ используют стабильные пути `vfs/statistics/...`.
- **`domrf_mortgage`, `sber_finansovie_rezultaty`, `tbank_group_ifrs` — ссылка задана статически.** Страницы источников закрыты JS-challenge (ServicePipe у ДОМ.РФ, TSPD у Сбера) либо не имеют листинга (CDN с UUID у Т-Банка), colly их не парсит: ссылку обновляют вручную при каждом релизе. У `tbank_group_ifrs` текущая ссылка ведёт на PDF, а не XLSX, — импорт падает.
- **Росстат может отдавать HTTP 426.** Домен `rosstat.gov.ru` периодически отклоняет запросы из-за технических работ (`Upgrade Required`), включая уже работавшие `ipc_mes`/`ipc_weeks`; это ограничение источника/сети, а не кода. Импортёры падают с диагностикой «не найдена ссылка на …».
- **`financial/`.** Legacy-контур (database/sql, свой main). Новые импортёры туда не добавляются; отчётность Полюса вынесена в `polyus/`, в `financial/` остался `databook_ugk` (ЮГК) и мёртвый код металлургов.
- **Отчётность Полюса: `unitsMarker` привязан к строке, а не к смыслу.** `polyus/columns.go` распознаёт шапку финансовой таблицы только по склеенным меткам `$млн`, `$mln`, `$million`. Отчёт, печатающий `US$ million`, оставит набор колонок пустым, и парсер вернёт **0 записей с `err == nil`** — молчаливо пустую страницу, не отличимую от «в отчёте нет метрик». (Форма `US$ million` — находка контролёра по другим релизам; у включённых релизов 2019–2024 фикстуры несут `$ million`, и оно совпадает, поэтому расширение маркера этой работой не потребовалось.) Это самое важное ограничение парсера Полюса; подробности и остальные — в [ARCHITECTURE.md](ARCHITECTURE.md) §6.6.
- **Отчётность Полюса: остальные ограничения колоночной модели.** `parsePeriod` не принимает форму `FY 2014` (год с пробелом); на МСФО-страницах допущение «первое число = отчётный период» выполняется на обеих текущих страницах, но во время прогона не проверяется, и отчёт со сравнительным столбцом первым молча поменяет периоды местами; счётчик значений вне периодных колонок (178 на приёмке релизов 2019–2024) подтверждён только живым прогоном — сквозного теста на итоговую строку нет; `pageOfLine` возвращает первую ненулевую страницу слова (корректно, т.к. строка всегда принадлежит одной странице, но тест-инварианта нет); словарь `metrics.go` содержит метку `"Earnings per share – basic (US\nDollar)"` со встроенным переводом строки, который не совпадает со склеенным текстом, — на фикстуре FY2024 записей `eps_*` нет. Перечень — ARCHITECTURE.md §6.6.
- **Отчётность Полюса: полоса шапки по ролям строк может расти вниз, а подъём вверх ограничен 8pt.** Вниз полоса продолжается, пока строки играют роль строк шапки, а роль «подпись изменения» опознаётся по словарю меток (`Y-o-Y`, `H-o-H`, `Q-o-Q`, `change`): строка с такой подписью в области данных попадёт в полосу, и её метрики не прочитаются — ни записей, ни нераспределённых значений, тот же тихий ноль, что и у ограничения с `unitsMarker`. Вверх полоса поднимается на строку кварталов («4Q 3Q 4Q» над якорем), но только если разрыв не больше `maxFragmentLiftGap = 8pt` (`polyus/columns.go`): настоящий обрывок дальше 8pt не поднимется, и «4Q 2020» с «3Q 2020» молча схлопнутся в два годовых `2020FY` — значения останутся на месте, сменится только ключ периода, и счётчик нераспределённых этого не покажет. Ловит это `TestKPIPeriodsAreUniquePerMetric`: дубликат ключа внутри страницы валит тест громко. На девяти включённых отчётах оба порога недостижимы (замерено: полосы обрываются на строке данных, настоящие подъёмы — ровно 5.40pt), но 2015–2018, включаемые следующим пунктом, несут подписи изменений в области данных. Остальные ограничения того же списка — отбор строки метрики по первому `Top` и прочее — в ARCHITECTURE.md §6.6.
- **Отчётность Полюса: 5 отчётов 2015–2018 и 2025H2 отключены.** Включены 9 отчётов (2014FY EN KPI, 2019FY…2024FY EN 4Q/FY, RU 1H2026, EN МСФО 1H2026). Причина отключения оставшихся — не выверка страниц, а тип документа: 2015–2017 — MD&A с консолидированной отчётностью, 2018FY — консолидированная МСФО-форма (у них нет шапки «Comparative financial results»), 2025H2 — англоязычный 2H25-релиз с двухшапочной таблицей. Включение 2015–2018 — следующий пункт работ. Снятые ограничения (потеря документа на ключе без измерения источника, недоступность данных агенту) — ниже.
- **Отчётность Полюса: ключ таблицы больше не теряет документ — дефект закрыт 2026-10-10.** Прежний ключ `(company, metric, period)` (таблица `polyus_financial_metrics`) не содержал измерения источника, поэтому два отчёта, печатающие один показатель за один период, конкурировали за одну строку. Среди ключей шести включённых релизов 4Q/FY за 2019–2024 таких **79, и 17 из них несли разные значения** (62 — одинаковые); побеждал первый отчёт в порядке `polyus/pages.go`, потому что импортёр отбрасывал повтор ключа внутри батча (`dedup.add`, 82 отброшенных повтора на приёмке релизов). Показательный случай: `gold_output` за `2023FY` хранился как **2 902** (релиз FY2023), а релиз FY2024 печатал ту же строку как **2 799**. Схема заменена на общую таблицу `company_financials` с ключом `(company, metric, period, source_kind, source_url)` — вид документа И сам документ; смена ключа потребовала новой таблицы, потому что `ORDER BY` — часть идентичности: `CREATE TABLE IF NOT EXISTS` уже созданную таблицу не меняет. **Проверено живым прогоном 2026-10-10:** импорт даёт 347 строк при **265 уникальных** `(company, metric, period)` и `0 duplicate keys skipped`; оба значения `gold_output 2023FY` (2 902 из релиза FY2023, 2 799 из релиза FY2024) видны в `v_company_metric_sources` с разными `source_url`, а `v_company_financials` отдаёт одно разрешённое значение (приоритет `ifrs` > `kpi`, затем `loaded_at`, затем `source_url` — детальный тай-брейк). Обе цифры всегда были прочитаны верно — спорят документы, а не парсер. Схема заложена под металлургов (`CHMF`/`MAGN`/`NLMK`), но наполняет её пока только Полюс: см. [docs/ROADMAP_DCF_METALS.md](docs/ROADMAP_DCF_METALS.md). Legacy-таблица `polyus_financial_metrics` не удалена и **больше не наполняется**: её читает дашборд `dashboard/finance-polyus.json`, перевод дашборда на `v_company_*` — отдельный шаг (в дорожной карте металлургов)
- **Отчётность Полюса: legacy-таблица `polyus_financial_metrics` заморожена.** Импортёр `polyus_financial_metrics` пишет в `company_financials` (двойной записи нет), а одноимённая таблица остаётся как есть — её нельзя переименовать или пересоздать, не сломав дашборд `dashboard/finance-polyus.json`; новых строк в ней не появляется. Поэтому цифры дашборда со временем отстают от витрин. Агент MCP эту таблицу не видит (сырая таблица, гранта нет) — и это правильно.
- **Отчётность Полюса доступна агенту MCP — ограничение снято 2026-10-10.** Витрины `v_company_financials`, `v_company_metric_sources`, `v_company_operating` созданы с комментариями и грантом `kimi_reader` (`sql/mcp_kimi_reader.sql`), ряды Полюса описаны в `series_catalog` (source `polyus`, 18 рядов; `polyus_datapack`, 19 рядов), поэтому агент читает производство, TCC, AISC, capex, выручку и операционку по активам через MCP. Сырые `databook_polyus`, `company_financials` и legacy `polyus_financial_metrics` агенту по-прежнему не выданы. Проверено живым `make mcp-check` 2026-10-10 (ПРОВЕРКА ПРОЙДЕНА). Витрины заведены общими для сектора, а не набором `v_polyus_*`.
- **Ingest-endpoint: TLS — снаружи.** Лимит частоты (`INGEST_RATE_PER_MIN`, `429`) и предел тела 1 MiB (`413`) реализованы в процессе; TLS предполагается на reverse proxy, в репозитории он не описан. Повторный `POST /v1/model_run` с тем же `run_id` отвечает `200 {"inserted": 1}`, хотя строк не пишет: ответ не отличает дубль от новой записи.
- **HTTP-режим mcp-clickhouse развёрнут на staging (2026-10-10).** Сервис `deploy/staging/mcp-clickhouse.service` слушает `127.0.0.1:8000`, требует Bearer-токен (`CLICKHOUSE_MCP_AUTH_TOKEN`, обязателен), читает витрины от пользователя `kimi_reader`. Локальная конфигурация Kimi остаётся stdio (блок «MCP для Kimi»). Наружу эндпоинт не опубликован и TLS не настроен — доступ через SSH-туннель; публикация для плагина `gold-nav` требует reverse proxy и отдельного шага.
- **Индикаторы `v_gold_dashboard` вводятся вручную.** `crack_ulsd_proxy`, `distillate_stocks`, `fedwatch_dec_hike`, `etf_flows_month`, `dxy` имеют только `manual_series`; импортёров нет. Без ввода они показываются как `stale`.
- **Сверка прогнозов не автоматизирована.** `actual` и `error_pct` в `forecast_log` не заполняются кодом; `v_forecast_accuracy` покажет `is_resolved = 0` до ручного ввода.
- **ИПЦ в `v_model_inputs` может быть пустым.** Витрина требует, чтобы существовали таблицы `ipc_mes` и `ipc_weeks`, но не проверяет их заполненность. Таблицы создаются до загрузки Росстата, поэтому при его ошибке (см. выше) витрина создаётся, а `ipc_mes_last` и `ipc_week_ytd` остаются NULL или устаревают; ориентироваться по `*_date`.
- **`nav_beta_gold` не заполняется** (поля нет во входе `ModelRun`), в таблицу пишется NULL.
- **Имена импортёров.** `Name()` у `fred` — `fred` (несколько серий, одна таблица `macro_series`), у `cbr` часть импортёров делит таблицу (`cbr_currency_usd`, `households_b_mes`).
- **Таблица `gold_prices` использует `Date32`.** Так как LBMA-ряд начинается в 1968 году.
- **Доходности G-curve (`ofz_curve`, теноры 1y/3y/5y/10y) — без ретроспективы.** Эндпоинт MOEX ISS `/iss/engines/stock/zcyc.json` отдаёт только снимок текущего дня; история накапливается с даты первого запуска импортёра. Индекс RGBI (тот же zcyc + свечи) имеет полную историю с 2010 года.

## Grafana

Установка Grafana 11.2.0 (Debian/Ubuntu, arm64):

```bash
sudo apt-get install -y adduser libfontconfig1 musl
wget https://dl.grafana.com/oss/release/grafana_11.2.0_arm64.deb
sudo dpkg -i grafana_11.2.0_arm64.deb
```

Дашборды лежат в `dashboard/` (JSON, плагин `grafana-clickhouse-datasource`).
