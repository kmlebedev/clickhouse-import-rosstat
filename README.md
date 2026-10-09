# clickhouse-import-rosstat

Go-конвейер импорта российской макроэкономической и финансовой статистики в ClickHouse. Данные используются дашбордами Grafana и аналитическими моделями (сценарный прогноз золота, DCF и NAV акций ПАО «Полюс»). Один бинарник, конфигурация только через переменные окружения.

Подробная архитектура, схемы таблиц и конвенции — в [ARCHITECTURE.md](ARCHITECTURE.md). План развития — в [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md). Правила для AI-ассистентов — в [AGENTS.md](AGENTS.md).

## Что импортируется

Каждый источник — импортёр с `init()`-регистрацией. Расписание задаёт Dagu (файлы в `dagu/`).

| Домен (пакет) | Источник | Таблицы ClickHouse | Расписание (МСК) |
|---|---|---|---|
| `cbr` | Банк России: ключевая ставка, курс USD, M2, золото, кредиты, ОФЗ-индикаторы, инфляционные ожидания | `cbr_key_rate`, `cbr_currency_usd`, `cbr_m2`, `cbr_gold`, `cbr_bank_int_rate`, `cbr_loans_to_corporations`, `cbr_loans_to_individuals`, `cbr_infl_exp`, `cbr_credit_m2x`, `cbr_indicators_cpd`, `cbr_ruania`, `households_b_mes`, `cbr_queries_*` | ежедневно 09:17 |
| `rosstat` | Росстат: ИПЦ (месяц, неделя), зарплаты, ВВП по кварталам | `ipc_mes`, `ipc_weeks`, `salaries_mes`, `vvp_kvartal` | четверг 12:17 |
| `minfin` | Минфин: исполнение федерального бюджета | `minfin_fed_bud_mes`, `minfin_fed_bud_mesyats` | 10-е число 10:41 |
| `customs` | ФТС: внешняя торговля по странам | `customs_vneshn_torg` | 15-е число 11:41 |
| `fao` | ФАО: индексы продовольственных цен | `fao_food_price` | 5-е число 10:47 |
| `bank` | Банки: индексы потребительских расходов, финрезультаты (Сбер, ВТБ, Т-Банк), ипотека ДОМ.РФ | `sber_consumper_spending_index`, `sber_izmenenie_trat`, `sber_finansovie_rezultaty`, `rus_vtb_group_ifrs`, `tbank_group_ifrs`, `domrf_mortgage` | понедельник 09:23 |
| `craw` | Росстандарт: сертификаты ГОСТ (краулер) | `gost_*` | 1-е число 11:31 |
| `fred` | FRED (CSV): DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE | `macro_series` (`source = 'fred'`) | ежедневно 11:41 |
| `gold` | Золото: фиксинг MOEX GOLDFIXME (₽/г) пересчитан в USD/oz по курсу ЦБ | `gold_prices` (`venue = 'moex_fix_usd'`) | ежедневно 18:47 |
| `financial` | Legacy: корпоративные databook'и (ЧМФ, ММК, НЛМК, Полюс, ЮГК), investing.com, РЖД | `databook_*`, `polyus_financial_metrics` и др. | понедельник 10:23 |

Импортёр `gold` — временный: производная цена, не LBMA. Официальный LBMA AM/PM пока не подключён, см. «Ограничения».

## Структура репозитория

```text
main.go                  точка входа: подключение к ClickHouse, фильтр импортёров, код выхода
chimport/                интерфейс ImportStat и глобальный реестр Stats
util/                    HTTP-клиент (национальные TLS-сертификаты РФ, ротация User-Agent), шаблоны HdBase и ClickHouseImport, батч-вставка
bank/ cbr/ craw/ customs/ fao/ minfin/ rosstat/   доменные импортёры (см. таблицу выше)
fred/                    FRED CSV → macro_series
gold/                    MOEX GOLDFIXME + cbr_currency_usd → gold_prices
financial/               legacy-контур (database/sql, свой main), новый код туда не добавляется
dagu/                    расписания: один DAG-файл на домен, имя файла = имя DAG
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

Поведение при запуске: неизвестное имя в `CLICKHOUSE_IMPORT_STAT` даёт предупреждение; ошибка любого импортёра даёт код выхода `1`, остальные импортёры при этом выполняются.

Импортёр `financial` читает `TICKER`, `FINANCIAL_DATA_DIR`, `INVESTING_EMAIL`, `INVESTING_PASSWORD`; эти переменные устаревают вместе с legacy-контуром.

## Оркестрация (Dagu)

Расписания описаны в `dagu/*.yaml`: `type: chain`, `overlap_policy: skip`, каждый шаг запускает бинарник с `CLICKHOUSE_IMPORT_STAT=<имя>`, ретраи — 3 попытки с паузой 300 с. Путь к бинарнику (`ROSSTAT_IMPORT_BIN`) и `CLICKHOUSE_URL` задаются в `base.yaml` Dagu; секреты лежат в файлах `/etc/dagu/secrets/` с правами `0600`, в репозитории их нет. Внутри домена зависимости задаются через `depends`; между DAG зависимостей нет (см. `gold.yaml`: шаг `cbr_currency_usd` стоит внутри DAG).

Запуск одного DAG вручную: `dagu start dagu/gold.yaml` (dev-окружение Dagu должно видеть `ROSSTAT_IMPORT_BIN` и `CLICKHOUSE_URL` через `base.yaml`).

## Разработка

| Команда | Что делает |
|---|---|
| `make lint` | `gofmt -l` (падает, если есть неотформатированный код) и `golangci-lint run ./...` |
| `make test` | `go vet` и `go test -race ./...` (тестов в репозитории пока нет) |
| `make build` | статический бинарник `linux/amd64` в `build/` |
| `make run` | сборка и запуск (нужен `CLICKHOUSE_URL`) |
| `make fmt` | `gofmt -w .` |
| `make deps` | `go mod download` и `go mod tidy` |
| `make clean` | удалить `build/` |

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

Все задания используют Go `1.27.x`, а не версию из `go.mod` (`go 1.26.0`): `govulncheck` проверяет стандартную библиотеку тулчейном, которым запущен, и на go1.27.1 находил уязвимости `net/http` и `crypto/tls`, исправленные в go1.27.2.

`.github/workflows/release.yml` на тег `v*` запускает тесты и `goreleaser` (архив `linux/amd64`, `SHA256SUMS`, файлы `README.md` и `dagu/*.yaml`). Конфиг — `.goreleaser.yaml`; проверка: `goreleaser check`.

## Ограничения и известные проблемы

- **LBMA AM/PM не подключён.** Официальный LBMA JSON, Nasdaq Data Link `LBMA/GOLD` и stooq недоступны из этой сети (403 WAF, JS-проверка), FRED серии LBMA удалил. Временно используется `gold` (MOEX GOLDFIXME ÷ курс ЦБ).
- **Курс ЦБ без понедельников и новогодних праздников.** В `cbr_currency_usd` этих дат нет; `gold` берёт последний курс не позже даты и пропускает дни без курса старше 10 дней. Это дыра в источнике ЦБ.
- **`tbank_group_ifrs` не работает.** По ссылке отдаётся PDF (`application/pdf`), а импортёр открывает файл как XLSX. Импорт падает; исправление не входит в этот PR.
- **Сетевые вызовы в `init()`.** Пакеты `cbr` и `minfin` при старте обращаются к сайтам источников; любой запуск бинарника ждёт их. Это legacy-долг.
- **`financial/`.** Legacy-контур (database/sql, свой main). Новые импортёры туда не добавляются.
- **Имена импортёров.** `Name()` у `fred` — `fred` (несколько серий, одна таблица `macro_series`), у `cbr` часть импортёров делит таблицу (`cbr_currency_usd`, `households_b_mes`).
- **Таблица `gold_prices` использует `Date32`.** Так как LBMA-ряд начинается в 1968 году.

## Grafana

Установка Grafana 11.2.0 (Debian/Ubuntu, arm64):

```bash
sudo apt-get install -y adduser libfontconfig1 musl
wget https://dl.grafana.com/oss/release/grafana_11.2.0_arm64.deb
sudo dpkg -i grafana_11.2.0_arm64.deb
```

Дашборды лежат в `dashboard/` (JSON, плагин `grafana-clickhouse-datasource`).
