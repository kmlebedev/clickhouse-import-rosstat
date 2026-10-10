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
| `polyus` | Отчётность ПАО «Полюс»: xlsx-датапак (операционные результаты по активам с 2007) и PDF-отчёты (KPI-пресс-релизы EN/RU + МСФО-формы EN). Разбор PDF — колоночная модель: `pdftotext -tsv`, полоса шапки по ролям строк, колонки из неё по X | `databook_polyus`, `polyus_financial_metrics` | понедельник 10:23 |
| `financial` | Legacy: корпоративные databook'и (ЮГК), investing.com, РЖД | `databook_ugk` и др. | понедельник 10:23 |

Импортёр `gold` — временный: производная цена, не LBMA. Официальный LBMA AM/PM пока не подключён, см. «Ограничения».

Отчётность Полюса даёт два импортёра в пакете `polyus/`: `databook_polyus` (xlsx-датапак) и `polyus_financial_metrics` (PDF-отчёты). Текст извлекается `pdftotext -tsv`; полоса шапки финансовой таблицы собирается по ролям строк (маркер единиц, подписи периодов, подписи изменений), а подписи периодов, разбитые переносом строки («4Q» на одной строке шапки, «2019» на другой), склеиваются по X; значения раскладываются по колонкам, выведенным из шапки по X-координатам. Запуск: `make import STAT=polyus_financial_metrics` или `make import STAT=databook_polyus`. Ограничения этих данных — в разделе «Ограничения»; из 14 отчётов списка включены 9 (2014FY, 2019FY…2024FY, 1H2026), отключены 5 — 2015FY…2018FY (другой тип документа: MD&A и консолидированная МСФО) и 2025H2.

## Структура репозитория

```text
main.go                  точка входа: подключение к ClickHouse, фильтр импортёров, код выхода
chimport/                интерфейс ImportStat и глобальный реестр Stats
util/                    HTTP-клиент (национальные TLS-сертификаты РФ, ротация User-Agent), шаблон импортёра ClickHouseImport, батч-вставка
bank/ cbr/ craw/ customs/ fao/ minfin/ rosstat/   доменные импортёры (см. таблицу выше)
fred/                    FRED CSV → macro_series
calendar/                календарь событий-триггеров → events_calendar
views/                   витрины контура прогноза (v_model_inputs, v_gold_dashboard, v_forecast_accuracy)
ingest/                  ingest-endpoint контура прогноза: model_runs, forecast_log, macro_series (source='manual')
cmd/ingest/              бинарник ingest (отдельный от основного импорта)
bls/                     BLS API v2 → macro_series
bea/                     BEA API (NIPA T20804) → macro_series
gold/                    MOEX GOLDFIXME + cbr_currency_usd → gold_prices
moex/                    MOEX ISS: свечи PLZL → stock_prices, RGBI + G-curve → ofz_curve
polyus/                  отчётность Полюса: xlsx-датапак → databook_polyus; PDF-отчёты (KPI EN/RU, МСФО EN) → polyus_financial_metrics (разбор — колоночная модель: pdftotext -tsv + полоса шапки по ролям строк + колонки по X)
financial/               legacy-контур (database/sql, свой main), новый код туда не добавляется; остался databook_ugk (ЮГК)
dagu/                    расписания: один DAG-файл на домен, имя файла = имя DAG
sql/                     DDL и гранты вручную: пользователь kimi_reader, сид календаря Q4-2026
scripts/                 проверки MCP (mcp_setup_user.py, mcp_check.py)
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

## Оркестрация (Dagu)

Расписания описаны в `dagu/*.yaml`: `type: chain`, `overlap_policy: skip`, каждый шаг запускает бинарник с `CLICKHOUSE_IMPORT_STAT=<имя>`, ретраи — 3 попытки с паузой 300 с. Путь к бинарнику (`ROSSTAT_IMPORT_BIN`) и `CLICKHOUSE_URL` задаются в `base.yaml` Dagu; секреты лежат в файлах `/etc/dagu/secrets/` с правами `0600`, в репозитории их нет. Внутри домена зависимости задаются через `depends`; между DAG зависимостей нет (см. `gold.yaml`: шаг `cbr_currency_usd` стоит внутри DAG).

Запуск одного DAG вручную: `dagu start dagu/gold.yaml` (dev-окружение Dagu должно видеть `ROSSTAT_IMPORT_BIN` и `CLICKHOUSE_URL` через `base.yaml`).

## Контур прогноза золота

Составные части контура (подробно — ARCHITECTURE.md §6.3 и §6.5):

- витрины `v_model_inputs`, `v_gold_dashboard`, `v_forecast_accuracy` и календарь `v_events_calendar` — читаются агентом через MCP (пользователь `kimi_reader`, только `v_*`);
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
| `make import STAT=<имя>` | локальный прогон одного импортёра по `Name()` (собирает `build/clickhouse-import-rosstat-native`) |
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
| `make mcp-check` | smoke-проверка MCP: инструменты, витрины, отказ на сырые таблицы (нужен `CLICKHOUSE_PASSWORD`, запущенный ClickHouse) |

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
- **Отчётность Полюса: 5 отчётов 2015–2018 и 2025H2 отключены.** Включены 9 отчётов (2014FY EN KPI, 2019FY…2024FY EN 4Q/FY, RU 1H2026, EN МСФО 1H2026). Причина отключения оставшихся — не выверка страниц, а тип документа: 2015–2017 — MD&A с консолидированной отчётностью, 2018FY — консолидированная МСФО-форма (у них нет шапки «Comparative financial results»), 2025H2 — англоязычный 2H25-релиз с двухшапочной таблицей. Включение 2015–2018 — следующий пункт работ.
- **Отчётность Полюса: ключ таблицы не содержит источника.** `(company, metric, period)` — ключ `polyus_financial_metrics`, поэтому `revenue` и `total_revenue` — один и тот же показатель под двумя именами (оба 4 674), а `eps_basic`/`eps_diluted`/`profit_for_period` за 1H2026 приходят и из русского пресс-релиза, и из МСФО — в витрине остаётся значение **первого** отчёта в порядке списка `reports` (`polyus/pages.go`): импортёр отбрасывает повтор ключа внутри батча (`dedup.add`, в логе — `duplicate ... already in batch`), поэтому порядок отчётов в `pages.go` значим, а не порядок слияния ReplacingMergeTree. Сегодня первым идёт русский релиз, а значения обоих источников совпадают, поэтому данные не испорчены; но при расхождении источников проиграет тот, что стоит в списке позже, — молча. Колоночная модель этот дефект **не меняла**: это вопрос схемы, он решается в задаче витрины `v_polyus_*`.
- **Отчётность Полюса недоступна агенту MCP.** Витрины `v_polyus_*` и гранта `kimi_reader` нет, а сырые `databook_polyus` и `polyus_financial_metrics` агенту не выдаются (AGENTS.md, правило 11). Пока витрина не создана, данные Полюса читаются только напрямую из ClickHouse.
- **Ingest-endpoint: TLS — снаружи.** Лимит частоты (`INGEST_RATE_PER_MIN`, `429`) и предел тела 1 MiB (`413`) реализованы в процессе; TLS предполагается на reverse proxy, в репозитории он не описан. Повторный `POST /v1/model_run` с тем же `run_id` отвечает `200 {"inserted": 1}`, хотя строк не пишет: ответ не отличает дубль от новой записи.
- **HTTP-режим mcp-clickhouse для плагина не развёрнут в репозитории.** Локальная конфигурация Kimi — stdio (блок «MCP для Kimi» в разделе «Ключи и окружение»). Для плагина `gold-nav` сервер автора по HTTPS с токеном нужно поднять отдельно.
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
