# ARCHITECTURE.md — clickhouse-import-rosstat

> Контекстный документ для AI-ассистентов (Kimi Code и др.). Содержит всё необходимое для написания нового кода без полного чтения репозитория: паттерны, конвенции, схемы БД, целевую архитектуру. Обновлять при каждом изменении архитектуры.
> Последнее обновление: 2026-10-10 (в `polyus/` включены шесть пресс-релизов 4Q/FY за 2019–2024: полоса шапки собирается по ролям строк, разорванные подписи периодов склеиваются по X; отключёнными остались 2015–2018 и 2025H2). Базовый коммит: `a43af65` (разделы сверены с кодом на этом коммите).

---

## 1. Что это за проект

Go-конвейер (ETL) импорта российской макроэкономической статистики и финансовых данных в **ClickHouse** для дашбордов **Grafana** и аналитических моделей (сценарный прогноз золота → DCF → NAV акции PLZL). Монолитный репозиторий: основной бинарник импорта и отдельный бинарник `cmd/ingest` (ingest-endpoint контура прогноза, §6.5); конфигурация только через env.

- Репозиторий: `github.com/kmlebedev/clickhouse-import-rosstat`
- Go 1.27, сборка: `make build` → статический linux/amd64 бинарник (CGO_ENABLED=0, ldflags `-s -w`); `make build-ingest` → `build/ingest`
- Драйвер БД: `github.com/ClickHouse/clickhouse-go/v2` (native protocol)
- Ключевые библиотеки: `excelize/v2` (XLSX), `xlsReader` (legacy XLS), `colly/v2` + `goquery` (скрейпинг), `unipdf/v3` (PDF), `cenkalti/backoff/v5` (ретраи), `logrus` (логи, `log.Infof/Errorf/Fatal`)

## 2. Структура репозитория

```
main.go            — точка входа: подключение к CH, запуск реестра импортёров
chimport/stats.go  — интерфейс ImportStat + глобальный реестр Stats
util/              — общие хелперы: HTTP-клиент (xls.go), шаблон импортёра ClickHouseImport (clickhouse_import.go), батч-импорт (db.go), каталог рядов series_catalog/v_series_catalog (series_catalog.go), создание витрин v_* с комментариями (views.go)
sql/               — SQL для ручной настройки: пользователь MCP kimi_reader и гранты на витрины (mcp_kimi_reader.sql); сид календаря Q4-2026 (events_calendar_q4_2026.sql)
scripts/           — скрипты MCP: mcp_setup_user.py (создание kimi_reader, make mcp-user), mcp_check.py (smoke-проверка, make mcp-check)
rosstat/           — Росстат: ipc_mes, ipc_weeks, vvp_kvartal, salaries_mes → series_catalog (source='rosstat'), витрина v_rosstat_macro
cbr/               — ЦБ РФ: key_rate, currency_usd, m2, ruonia (таблица cbr_ruania), metal_gold (cbr_gold), households, avgproc_stav, ... → series_catalog (source='cbr'), витрина v_cbr_macro
minfin/            — Минфин: fedbud_mes, fedbud_mesyats (исполнение федбюджета) → series_catalog (source='minfin', 'minfin_mesyats'), витрина v_minfin_budget
customs/           — ФТС: внешняя торговля по странам
fao/               — ФАО: индексы продовольственных цен
fred/              — FRED (CSV-серии US-макро: DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE) → macro_series
bls/               — BLS API v2 (CPI, безработица, NFP, зарплата, PPI, JOLTS) → macro_series (source = 'bls')
bea/               — BEA API (NIPA T20804: индексы PCE) → macro_series (source = 'bea')
gold/              — золото: MOEX GOLDFIXME (₽/г) ÷ курс ЦБ cbr_currency_usd → gold_prices (venue='moex_fix_usd'); series_catalog (source='gold'), витрина v_gold_prices
moex/              — МосБиржа ISS (анонимный REST, без ключа): свечи PLZL → stock_prices (существующая legacy-схема, Float32 не меняем), RGBI + G-curve → ofz_curve; series_catalog (source='moex'), витрины v_stock_prices, v_ofz_curve
calendar/          — календарь событий-триггеров прогноза золота/NAV: events_calendar, витрина v_events_calendar; сид Q4-2026 — sql/events_calendar_q4_2026.sql
views/             — витрины контура прогноза v_model_inputs, v_gold_dashboard, v_forecast_accuracy; импортёр gold_views.
                     Импортёр company_views: таблица company_financials (метрики компаний сектора) + витрины
                     v_company_financials, v_company_metric_sources, v_company_operating; описания рядов Полюса
                     в series_catalog (views/series_meta.go, source='polyus' и 'polyus_datapack')
ingest/            — ingest-endpoint контура прогноза: POST /v1/model_run, POST /v1/manual_series (Bearer INGEST_TOKEN); пишет model_runs, forecast_log, macro_series (source='manual')
cmd/ingest/        — main-пакет бинарника ingest (make build-ingest / make run-ingest)
dcf/               — расчётное ядро DCF: sum-of-parts LOM-NAV по активам (таблицы mine_plans,
                     price_decks, nav_by_asset). Чистое ядро — НДПИ на унцию, LOM-NPV с хвостом
                     закрытия, эскалация по ИПЦ, две ставки контуров; импортёр dcf_engine. model_runs
                     не пишет — запись прогона только через ingest-endpoint
bank/              — банки: sber_csi(+week), sber/vtb/tbank_fin_rez, domrf_mortgage
craw/              — многостраничные краулеры: gost (сертификаты Росстандарта)
polyus/            — отчётность ПАО «Полюс»: xlsx-датапак → databook_polyus; PDF-отчёты (KPI-пресс-релизы EN/RU
                     и МСФО-формы EN) → company_financials (общая таблица сектора; legacy-таблица
                     polyus_financial_metrics больше не наполняется). Разбор PDF — колоночная модель:
                     pdftotext -tsv (координаты слов) + полоса шапки по ролям строк и колонки из неё
                     по X; см. §6.6.
                     Два импортёра: databook_polyus, polyus_financial_metrics (Name() сохранён, пишет в
                     company_financials — см. §6.1 и §6.2).
                     Спеки разбора: docs/superpowers/specs/2026-10-10-polyus-column-model-design.md,
                     docs/superpowers/specs/2026-10-10-polyus-history-reports-design.md
polyus/data/       — локальная копия датапака (polyus_datapack_fy2025_new.xlsx, читается из CWD)
polyus/testdata/   — фикстуры тестов: TSV-снимки `pdftotext -tsv` (EN/RU) и xlsx-датапак с битой ячейкой.
                     Тесты работают на снимках и утилиту не вызывают
financial/         — ⚠️ legacy-контур: корпоративные databook'и (ЧМФ/MAGN/NLMK/ЮГК — только ЮГК
                     зарегистрирована как databook_ugk, остальные не в Stats), investing.com exporter,
                     РЖД. Свой main.go, своё подключение database/sql. Импортёров Полюса здесь нет
financial/data/    — локальные XLSX/XLS источники (databook'и компаний; датапак Полюса переехал в polyus/data/)
dashboard/         — экспортированные JSON-дашборды Grafana (плагин grafana-clickhouse-datasource)
docs/              — аналитические статьи (прогнозы золота)
*.crt              — национальные TLS CA РФ (для ГОСТ-шифрования gos-сайтов)
```

## 3. Ключевой паттерн: реестр импортёров

Каждый источник — файл с `init()`, который регистрирует себя. `main.go` импортирует пакеты вслепую и запускает всех (или фильтр по `CLICKHOUSE_IMPORT_STAT`).

```go
// chimport/stats.go
type ImportStat interface {
    Name() string
    Import(ctx context.Context, conn driver.Conn) (count int64, err error)
}
var Stats []ImportStat
```

**Конвенция имени:** `Name()` возвращает имя таблицы ClickHouse (`ipc_mes`, `cbr_key_rate`). Фильтр запуска: `CLICKHOUSE_IMPORT_STAT=ipc_mes,cbr_key_rate`.

### Шаблон А — ручной импортёр (скрейпинг страницы → ссылка на XLSX → парсинг)

Эталон: `rosstat/ipc_mes.go`. Скрейпер colly ищет ссылку на актуальный XLSX на странице источника → `util.GetXlsx(url)` → якорная строка в таблице → сбор `[][]string` → DDL + INSERT.

Это **предпочтительный** способ для источников, где имя файла или id в URL меняются при обновлении (`cbr_infl_exp`, `cbr_credit_m2x`, `vvp_kvartal`, `salaries_mes`, `rus_vtb_group_ifrs`). Ссылку вычислять внутри `Import()` (не в `init()`), иначе регистрация импортёра делает сетевой вызов. Если после `c.Wait()` URL пуст — вернуть понятную ошибку, а не вызывать `GetXlsx("")`.

Ограничение: страницы, закрытые JS-challenge (`domrf_mortgage` — ServicePipe; `sber_finansovie_rezultaty` — TSPD) или не имеющие листинга ссылок (`tbank_group_ifrs` — CDN с UUID), colly распарсить не может — там ссылку обновляют вручную.

### Шаблон Б — `util.ClickHouseImport` (декларативный, предпочтительный)

Единый шаблон для XLSX-источников и краулеров. Встраивается в обёртку-структуру пакета; `Import()` обёртки вычисляет `DataUrl` и при необходимости публикует ряды (`series_catalog` + витрина).

```go
type myStat struct {
    util.ClickHouseImport
}

func (s *myStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
    if s.DataUrl = myGetDataUrl(); s.DataUrl == "" {           // сеть — только здесь, не в init()
        return count, fmt.Errorf("my: не найдена ссылка на xlsx на %s", myPageUrl)
    }
    if count, err = s.ClickHouseImport.Import(ctx, conn); err != nil {
        return count, err
    }
    return count, publishMySeries(ctx, conn)                   // series_catalog + витрина
}

func init() {
    chimport.Stats = append(chimport.Stats, &myStat{ClickHouseImport: util.ClickHouseImport{
        TableName:   "minfin_fed_bud_mes",
        CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (...);`},  // несколько DDL — допустимо
        ImportFunc:  myImportFunc,   // func(xlsx *excelize.File, batch driver.Batch) error
        // CrawFunc: func(url string, conn driver.Conn) (int64, error) — альтернатива для краулеров
        // BeforeImport: func(ctx, conn) error — вычислить DataUrl внутри Import()
    }})
}
```

Поля: `TableName`, `CreateTable []string` (`%s` подставляется `TableName`), `DataUrl`, `ImportFunc` **или** `CrawFunc` (или `BeforeImport` + `ImportFunc`); ровно один режим обязателен — иначе `Import()` вернёт ошибку «no CrawFunc or ImportFunc configured».

Эталон краулера с двумя DDL — `craw/gost.go`; XLSX с `DataUrl` в рантайме — `cbr/Infl_exp.go`, `cbr/indicators_cpd.go`, `minfin/fedbud_mes.go`; подстановка даты в URL — `cbr/ruonia.go`.

### Общие хелперы `util/`

- `util.HttpClient` — единый HTTP-клиент: системные CA + `CERT_FILES` (нац. сертификаты РФ), cookie-jar с `redirect_cookie` для rosstat.gov.ru
- `util.GetXlsx(url)` / `util.GetCSV(url)` (разделитель `;`) / `util.GetFile(url)` — скачивание с ротацией User-Agent
- `util.MonthsToNum` — map русских названий месяцев (все падежи + сокращения + опечатка «авугст»)
- `util.Import(ctx, conn, ddl, insert, *[][]string)` — DDL + батч из таблицы строк (колонки: name, y, m, value)

## 4. Конвенции БД ClickHouse

- Движок: **`ReplacingMergeTree`**, ORDER BY = естественный ключ (`(name, date)`, `(code, quarter, name)`)
- Типы: `LowCardinality(String)` измерения, `Date` даты, значения — **для новых таблиц Float64** (legacy — Float32, не трогать)
- DDL всегда `CREATE TABLE IF NOT EXISTS`; идемпотентный реимпорт — норма
- ⚠️ Вставка — **только батчами**: `conn.PrepareBatch(ctx, "INSERT INTO t")` + `batch.Append(...)` + `batch.Send()`. Построчный `conn.Exec` в цикле — антипаттерн (есть в legacy, не копировать)
- ⚠️ Дашборды/запросы к ReplacingMergeTree — с `FINAL` или дедупликацией

## 5. Конфигурация (env)

| Переменная | Назначение |
|---|---|
| `CLICKHOUSE_URL` | DSN подключения (обязательная для импорта и для `cmd/ingest`) |
| `CLICKHOUSE_IMPORT_STAT` | Фильтр импортёров через запятую (пусто = все) |
| `LOG_LEVEL` | logrus-уровень (debug/info/warn/error) |
| `CERT_FILES` | Пути к PEM-сертификатам через запятую |
| `TICKER` | Маршрутизация legacy-контура financial (MOEX/VTBR/CHMF/MAGN/NLMK/Exports/ALL) |
| `INVESTING_EMAIL`, `INVESTING_PASSWORD` | ⚠️ deprecated, переезжаем на MOEX ISS / stooq |
| `BLS_API_KEY` | Регистрационный ключ BLS (необязательно, для `bls`) |
| `BEA_API_KEY` | UserID BEA API (обязательно, для `bea`) |
| `INGEST_TOKEN` | Bearer-токен ingest-endpoint (`cmd/ingest`); обязательна для него: пустое значение — выход с ошибкой. Значение только в окружении, в репозитории не хранится |
| `INGEST_ADDR` | Адрес HTTP-сервера `cmd/ingest`, по умолчанию `:8081` |
| `INGEST_RATE_PER_MIN` | Лимит запросов ingest-endpoint в минуту (token bucket, burst 10); по умолчанию 60; пустое или некорректное значение — 60. Превышение — `429` |

## 6. Целевая архитектура (дорожная карта 2026-Q4)

Контур: **Источники → ClickHouse (сырые + витрины) → MCP (read-only) → Kimi-агент → сценарный прогноз золота → DCF → NAV PLZL → model_runs**. Полная версия: [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md). Спека контура прогноза и плагина: [docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md](docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md).

Статусы в §6.1–§6.3: **реализован** — есть в коде репозитория; **не реализован** — только в плане, в коде нет.

### 6.1 Новые импортёры (по приоритету)

| Импортёр | Пакет | Статус | Источник | Метод |
|---|---|---|---|---|
| `fred` | `fred/` | реализован; `Name()` = `fred`. Исторический базлайн; первичный источник мировых рядов для прогнозной сессии — datasource (см. gold-nav) | FRED CSV API: `https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFII10` (серии: DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE) | Свой парсер `encoding/csv` (не `util.GetCSV`: разделитель `,`), `Name()` = `fred` (несколько серий, одна таблица), пропуски (пустое значение) пропускаются; UA не задаём — FRED за Imperva рвёт соединение с браузерным UA. Расписание: `dagu/fred.yaml`, ежедневно 11:41 (МСК) → `macro_series`; описания рядов → `series_catalog`; витрина `v_fred_macro` (с комментариями, грант `kimi_reader`) |
| `bls` | `bls/` | реализован; `Name()` = `bls` | BLS Public Data API v2: `POST https://api.bls.gov/publicAPI/v2/timeseries/data/` (JSON: `seriesid`, `startyear`, `endyear`, опц. `registrationkey` из `BLS_API_KEY`). Серии: CUUR0000SA0, CUSR0000SA0 (CPI), LNS14000000 (безработица), CES0000000001 (NFP), CES0500000003 (средняя зарплата), WPSFD4 (PPI final demand), JTS000000000000000JOL (JOLTS, вакансии). | Окна лет по ≤10 лет с 2006 года; все серии одним запросом. Статус ≠ `REQUEST_SUCCEEDED` — ошибка; `message` при успехе (каталог, «no data») — не ошибка; периоды M13 и не-месячные пропускаются. Тесты: `bls/bls_test.go`. Расписание: `dagu/bls.yaml`, ежедневно 12:23 (МСК) → `macro_series`; описания рядов → `series_catalog`; витрина `v_bls_macro` (с комментариями, грант `kimi_reader`) |
| `bea` | `bea/` | реализован; `Name()` = `bea` | BEA API: `GET https://apps.bea.gov/api/data?method=GetData&datasetname=NIPA&TableName=T20804&Frequency=M` (ключ `BEA_API_KEY` → параметр `UserID`, без него импорт падает с ошибкой). Строки T20804: 1 — PCE_PI (headline), 25 — PCE_PI_CORE (excluding food and energy). | Ключ маскируется в текстах ошибок (`url.Error` содержит URL). Тесты: `bea/bea_test.go`. Расписание: `dagu/bea.yaml`, ежедневно 12:37 (МСК) → `macro_series`; описания рядов → `series_catalog`; витрина `v_bea_pce` (с комментариями, грант `kimi_reader`) |
| `databook_polyus` | `polyus/` | реализован; `Name()` = `databook_polyus` | Локальный xlsx-датапак Polyus `polyus/data/polyus_datapack_fy2025_new.xlsx` (лист `Sheet1`): операционные результаты по активам с 2007 (CONSOLIDATED OPERATING RESULTS, OLIMPIADA, BLAGODATNOYE, TITIMUKHTA, VERNINSKOYE2, ALLUVIALS, KURANAKH, ZAPADNOYE, NATALKA, Sukhoi Log) | Разбор через `excelize/v2`; битая ячейка (`N/A`, текст) — пропуск строки с `log.Warnf`, импорт не падает. Тесты: `polyus/datapack_test.go`. Расписание: `dagu/financial.yaml`, понедельник 10:23 (МСК) → `databook_polyus`. Описания рядов — в `series_catalog` (source `polyus_datapack`, 19 рядов; пишет импортёр `company_views`, см. §6.3), витрина `v_company_operating` с комментариями и грантом `kimi_reader`. Отдельного набора `v_polyus_*` нет намеренно: витрина общая для сектора (§6.3) |
| `polyus_financial_metrics` | `polyus/` | реализован; `Name()` = `polyus_financial_metrics` (имя импортёра; пишет в таблицу `company_financials`). Отчётность Полюса — прямые входы DCF (производство, TCC, выручка, прибыль, capex) | PDF-отчёты ПАО «Полюс»: KPI-пресс-релизы (RU 1H2026, EN 4Q/FY 2019–2024, EN MD&A FY2014) и МСФО-формы (EN 1H2026, стр. 6–7) | Извлечение текста — `pdftotext -tsv` (координаты слов, `polyus/source.go`); разбор — колоночная модель: слова группируются в визуальные строки (`polyus/words.go`), полоса шапки собирается по ролям строк, колонки выводятся из неё по X (`polyus/columns.go`), значение относится к колонке, накрывающей его X. Два режима: `Kind: "kpi"` (таблица «метрика + строки периодов», период из шапки страницы) и `Kind: "ifrs"` (статья + два столбца периодов, период отчёта — параметр). Список отчётов — данные (`polyus/pages.go`, `[]Report`): 9 включённых, 5 с `Enabled: false` (2015–2018 и 2025H2) и причиной в комментарии. Guard `polyus/guard.go` — резерв: отбрасывает только записи с пустым `Period` и сообщает число значений вне периодных колонок (см. §6.6). Тесты: `polyus/kpi_test.go`, `ifrs_test.go`, `columns_test.go`, `words_test.go`, `periods_test.go`, `guard_test.go`. Расписание: `dagu/financial.yaml`, понедельник 10:23 (МСК) → `company_financials`; описания рядов — в `series_catalog` (source `polyus`, 18 рядов; пишет `company_views`), витрины `v_company_financials` и `v_company_metric_sources` с комментариями и грантом `kimi_reader`. `Name()` и имя таблицы разведены намеренно: имя в реестре — контракт CLI и `dagu/financial.yaml` (`financialMetricsStatName`), а таблица сменилась на общую для сектора |
| `databook_ugk` | `financial/` | реализован (legacy) | xlsx-датапак ЮГК | Остаётся в `financial/`; перенос — отдельный пункт, когда ЮГК понадобится для `peers_nav` |
| `dcf_engine` | `dcf/` | реализован; `Name()` = `dcf_engine`. Расчётное ядро sum-of-parts LOM-NAV (§6.4): **заводит** `mine_plans`, `price_decks`, `nav_by_asset` (DDL — копии §6.2 в `dcf/schema.go`); **читает** `mine_plans` (`FINAL`, порядок `company, asset, year` — группировка планов одним проходом) и `price_decks` (`FINAL`), а на пустой таблице деков сеет её сама: `spot_flat` — последняя цена `gold_prices`(`venue='moex_fix_usd'`), `consensus_lt` $3 000 и `own_scenario` $4 600 — задокументированные LT-якоря §6.4 | Чистое ядро `dcf/model.go` — `ndpiPerOz` (база + надбавка 10% с превышения порога $1 900, порог и ставка — поля `Params`, не литералы), `npvLOM` (год `t` плана: `Prod_koz×(gold−AISC) − НДПИ×Prod − налог с положительного потока − capex − ΔWC`, деньги внутри года приводятся к тыс. USD; в последнем году — ОТРИЦАТЕЛЬНЫЙ `closure_costs` налогово вычитаемым, perpetual TV нет; год плана без цены дека пропускается с `log.Warnf`, но индекс года всё равно сдвигает дисконт), `escalate` (затраты по накопленному ИПЦ; `Params.Ipc` в этом срезе `nil` — упрощение задокументировано). Ставки двух контуров — `dcf/params.go` (`industrial` 0.05, `local` 0.16). Ядро записи `navRows` — чистая, без БД и часов: `активы × 3 дека × 2 контура`, деки идут отсортированными (порядок вставки воспроизводим), `stage_haircut` всегда NULL (стадийные haircut'ы вне среза). Запись — ОДНИМ батчем `PrepareBatch → Append (err checked) → Send` (правило 2); `Abort` вызывается только в ветке `Append`, где батч ещё открыт. Пустой `mine_plans` — **не ошибка**: `log.Warn` и `Imported 0 rows`, чтобы `make import STAT=dcf_engine` и DAG не краснели на dev-БД до сида планов. `run_id` приходит ТОЛЬКО из `DCF_RUN_ID` (`os.Getenv`, не-UUID отсекается до расчёта): пустой `DCF_RUN_ID` печатает готовое тело `POST /v1/model_run` (`dcf/run_json.go`, зеркало `ingest.ModelRun` с `DisallowUnknownFields`) и **ничего не пишет** — в `model_runs` пишет только ingest-endpoint. Тесты — `dcf/*_test.go` (ядро на закреплённых числах, без БД и сети) |
| `eia` | `eia/` | не реализован; вычеркнут из Go-плана (crack — `HO=F` в сессии, запасы дистиллятов — `manual_series`), спека gold-nav §4 | EIA Weekly Petroleum Status (запасы дистиллятов, crack ULSD) | API EIA v2 (ключ в env `EIA_API_KEY`) → `macro_series` |
| `lbma_gold` | `gold/` | не реализован (LBMA недоступен с этой сети); вместо него реализован `gold`, `Name()` = `gold_prices` | ⚠️ Временно вместо LBMA: MOEX `GOLDFIXME` (борд FIXI, ₽/г, с 2024-08-05) × 31,1034768 ÷ курс `cbr_currency_usd`; производная цена, не LBMA. Курс берётся последний известный не позже даты (ЦБ не публикует понедельники и новогодние праздники; окно 10 дней). Расписание: `dagu/gold.yaml`, ежедневно 18:47 (МСК); первым шагом DAG выполняется `cbr_currency_usd`. LBMA не реализован: prices.lbma.org.uk и Nasdaq Data Link `LBMA/GOLD` отвечают 403 WAF (датасетный эндпоинт блокируется с этого IP даже с валидным ключом), stooq — JS-проверкой, FRED серии LBMA удалил (404), Yahoo — 429 | → `gold_prices` |
| `moex_iss` | `moex/` | реализован; `Name()` = `stock_prices` | MOEX ISS REST (анонимный): дневные свечи PLZL `/iss/engines/stock/markets/shares/securities/PLZL/candles.json?interval=24` (пагинация `start` шагом 100 при явном `limit=100` — без limit страницы по 500 и дубли). | Инкремент от max(date) по `code='PLZL'`; пустая таблица → с 2010-01-01. Пишет в существующую legacy-таблицу `stock_prices` (схема Float32 не меняется). Тесты: `moex/moex_test.go`. Расписание: `dagu/moex.yaml`, ежедневно 19:13 (МСК) → `stock_prices`; series_catalog (source='moex'); витрина `v_stock_prices` (с комментариями, грант `kimi_reader`) |
| `ofz_curve` | `moex/` | реализован; `Name()` = `ofz_curve` | MOEX ISS: индекс RGBI (свечи `engines/stock/markets/index/securities/RGBI/candles.json`, полная история с 2010-01-01, тенор `RGBI` — уровень индекса, не доходность) + годовые доходности G-curve `/iss/engines/stock/zcyc.json`, блок `yearyields` (period 1/3/5/10 → теноры `1y`/`3y`/`5y`/`10y`, % годовых). ⚠️ zcyc — только снимок текущего дня: история доходностей накапливается с первого запуска. | Инкремент от max(date); даты из БД нормализуются в UTC (clickhouse-go отдаёт Date с таймзоной сессии). Расписание: `dagu/moex.yaml`, ежедневно 19:13 (МСК) → `ofz_curve`; series_catalog (source='moex'); витрина `v_ofz_curve` (с комментариями, грант `kimi_reader`) |
| `mmf_aum` | `funds/` | не реализован | СЧА фондов ликвидности | парсинг → `mmf_aum` |
| `news_watch` | `news/` | не реализован | RSS Интерфакс/РБК/IR Полюса | → `news_events` |

`Name()` импортёров — то, что передаётся в `CLICKHOUSE_IMPORT_STAT`: `fred`, `bls`, `bea`, `gold_prices`, `stock_prices`, `ofz_curve`, `events_calendar`, `gold_views`, `company_views`, `databook_polyus`, `polyus_financial_metrics`, `databook_ugk`, `dcf_engine`. Пакет `gold/` выполняет `Name()` = `gold_prices`, поэтому в README и командах используется имя таблицы. Пакет `polyus/` регистрирует два импортёра, оба сохраняют свои `Name()` (`databook_polyus`, `polyus_financial_metrics`) независимо от имени таблицы, в которую пишут; пакет `views/` — тоже два импортёра (`gold_views`, `company_views`; `views/` не существует как имя импортёра). Пакет `dcf/` регистрирует один импортёр с `Name()` = `dcf_engine`: имя шага DAG и `CLICKHOUSE_IMPORT_STAT`, а таблиц у него три (`mine_plans`, `price_decks`, `nav_by_asset`) — поэтому `Name()` не совпадает ни с одной из них.

`DCF_RUN_ID` (UUID прогона, клиентский `run_id` строк `nav_by_asset`) в таблицу env выше не входит: это параметр разового прогона, а не ключ источника, и передаётся он прямо в команде (`DCF_RUN_ID=<uuid> make import STAT=dcf_engine`). Без него движок считает, печатает тело `POST /v1/model_run` и не пишет ничего.

Ряды WGC (ETF-потоки), CME FedWatch и запасы дистиллятов EIA импортёров не имеют. В контуре прогноза они вводятся вручную через `POST /v1/manual_series` (`source = 'manual'`) и читаются витриной `v_gold_dashboard` (§6.3).

### 6.2 Новые таблицы (DDL — канонические, использовать как есть)

Реализованы в коде (DDL в блоке ниже совпадает с кодом): `macro_series`, `model_runs`, `forecast_log` (создаются `ingest.EnsureTables` при старте `cmd/ingest`), `events_calendar` (`calendar/`, сид `sql/events_calendar_q4_2026.sql`), `series_catalog` (`util/`), `gold_prices` (`gold/`), `ofz_curve` (`moex/`). Таблица `stock_prices` (legacy Float32) создаётся в `moex/moex_iss.go` и в этот блок не входит.

Отдельно — таблицы отчётности Полюса (`polyus/`). Канонические DDL — в коде (`polyus/datapack.go`, `polyus/import.go`, `views/company.go`), здесь приведены для справки:

```sql
-- polyus/datapack.go — xlsx-датапак, операционные результаты по активам
CREATE TABLE IF NOT EXISTS databook_polyus (
   table LowCardinality(String),
   name LowCardinality(String),
   data LowCardinality(String),   -- ANNUAL | QUARTERLY | SEMI-ANNUAL
   date Date,
   value Float32                  -- legacy-тип, не меняем
) ENGINE = ReplacingMergeTree()
ORDER BY (table, name, data, date);

-- company_financials — общая для сектора таблица метрик компаний (polyus/import.go,
-- дублирующий DDL в views/company.go; оба обязаны совпадать). Пишет её импортёр
-- polyus_financial_metrics (PDF-отчёты) — KPI-пресс-релизы и МСФО-формы; читают
-- витрины v_company_* (§6.3) и агент через MCP. Схема заложена под металлургов
-- (CHMF/MAGN/NLMK, см. docs/ROADMAP_DCF_METALS.md), но наполняется пока только Полюсом.
-- Ключ включает измерение источника (source_kind) И сам документ (source_url):
-- source_kind различает вид документа (kpi/ifrs), но не документ — два пресс-релиза
-- (FY2023 и FY2024) несут один и тот же 'kpi' и на ключе без source_url схлопывались бы,
-- теряя второе значение (у Полюса так расходится 17 из 79 общих ключей). Поэтому в
-- ORDER BY стоят оба поля, и на 265 уникальных (company, metric, period) приходится
-- 347 строк. Значения source_kind: "kpi" (пресс-релиз), "ifrs" (аудированная форма),
-- "legacy" (строки, перенесённые из polyus_financial_metrics, — зарезервировано, код
-- их пока не пишет).
-- Период каждой строки берётся из шапки её страницы (колонка определяется по X), а не
-- из Report.Period, — но только когда шапка страницы вообще распознана (isTableHeader).
-- Если шапку не опознали (например, метка единиц не совпала с unitsMarkers, см. §6.6
-- ограничение 1), периодных колонок нет и записей по странице не будет вовсе. У
-- МСФО-страниц, где шапки нет по устройству отчёта, период приходит параметром отчёта.
-- Колоночная модель (2026-10-10) схему не меняла; включение релизов 2019–2024 (2026-10-10)
-- тоже: схема та же.
CREATE TABLE IF NOT EXISTS company_financials
(
    company     LowCardinality(String),               -- 'PLZL','UGK','SELG','CHMF','MAGN','NLMK'
    metric      LowCardinality(String),               -- 'total_revenue','tcc_per_ounce',...
    period      String,                               -- '2026H1','2024FY','2024Q4','2014H2'
    period_type Enum8('Q' = 1, 'H' = 2, 'FY' = 3, 'LTM' = 4),
    source_kind LowCardinality(String),               -- 'kpi'|'ifrs'|'legacy' — вид документа
    value       Nullable(Float64),
    unit        LowCardinality(String),
    source_url  LowCardinality(String),               -- документ-источник, часть ключа
    source_page UInt16,
    loaded_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (company, metric, period, source_kind, source_url);

-- LEGACY (не наполняется): прежняя таблица метрик Полюса. Ни переименовать, ни
-- пересоздать нельзя — её читает Grafana-дашборд dashboard/finance-polyus.json.
-- Импортёр polyus_financial_metrics пишет в company_financials, двойной записи нет;
-- перевод дашборда на витрины v_company_* — отдельный шаг (docs/ROADMAP_DCF_POLYUS.md,
-- docs/ROADMAP_DCF_METALS.md).
CREATE TABLE IF NOT EXISTS polyus_financial_metrics
(
    company LowCardinality(String),           -- 'PLZL'
    metric LowCardinality(String),
    period String,                            -- '2026H1', '2014FY', ...
    period_type Enum8('Q' = 1, 'H' = 2, 'FY' = 3, 'LTM' = 4),
    value Nullable(Float64),
    unit LowCardinality(String),
    source_url LowCardinality(String),
    source_page UInt16,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (company, metric, period)
```

Не реализованы (в коде нет): `gold_forecasts`, `news_events`, `index_weights`, `dividend_events`, `mmf_aum`, `tax_events`, `reserves_assets`, `reserves_dynamics`, `license_events`, `peers_nav`, `regime_states`.

`mine_plans`, `price_decks` и `nav_by_asset` заведены в коде пакетом `dcf/` (импортёр `dcf_engine`; DDL — копии блока выше в `dcf/schema.go`) и создаются импортёром идемпотентно. Наполнение: `nav_by_asset` пишет `dcf_engine` (при заданном `DCF_RUN_ID`), `price_decks` он же сеет на пустой таблице, а `mine_plans` пока **не наполняется ничем** — сид LOM-планов из отчётности Полюса это отдельный пункт фазы 3. Пока сида нет, движок прогоняется вхолостую: `log.Warn` и `Imported 0 rows`.

Значения `source`, используемые в коде `macro_series`: `fred`, `bls`, `bea`, `manual` (ingest). Комментарий в DDL про `eia`, `wgc`, `cme` — план, не текущее состояние.

```sql
CREATE TABLE IF NOT EXISTS macro_series (
    source LowCardinality(String),   -- 'fred','eia','wgc','cme'
    series LowCardinality(String),   -- 'DFII10','distillate_stocks',...
    date Date32,                     -- Date32, т.к. Date не покрывает даты до 1970 (CPIAUCSL с 1947, FEDFUNDS с 1954)
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);

-- Каталог рядов macro_series: заполняет util.UpsertSeriesCatalog при каждом импорте источника
CREATE TABLE IF NOT EXISTS series_catalog (
    source LowCardinality(String),
    series LowCardinality(String),
    title String,
    unit String,
    frequency LowCardinality(String),   -- 'M','Q','D','W'
    origin String,                      -- таблица/серия/строка API
    description String
) ENGINE = ReplacingMergeTree ORDER BY (source, series);

CREATE TABLE IF NOT EXISTS gold_prices (
    venue LowCardinality(String),    -- 'lbma_am','lbma_pm','spot','comex_front','moex_fix_usd' (производный, см. gold/)
    date Date32,                     -- Date32: Date не покрывает даты до 1970 (LBMA с 1968)
    usd Float64
) ENGINE = ReplacingMergeTree ORDER BY (venue, date);

CREATE TABLE IF NOT EXISTS gold_forecasts (
    author LowCardinality(String),   -- 'goldman','jpm','own'
    published Date,
    horizon LowCardinality(String),  -- 'Q4-2026','YE-2026','2027'
    scenario LowCardinality(String), -- 'bull','base','bear','point'
    low Float64, high Float64, point Float64,
    probability Float32
) ENGINE = ReplacingMergeTree ORDER BY (author, published, horizon, scenario);

CREATE TABLE IF NOT EXISTS events_calendar (
    event_date Date,
    event_time Nullable(String),
    category LowCardinality(String), -- 'fomc','cpi','nfp','eia','wgc','cbr','polyus_ir','moex_rebalance','gov_rf'
    title String,
    threshold String,                -- JSON, напр. '{"crack":">50"}'
    status LowCardinality(String) DEFAULT 'pending'  -- pending|done|verified
) ENGINE = ReplacingMergeTree ORDER BY (event_date, category, title);

CREATE TABLE IF NOT EXISTS model_runs (
    run_id UUID DEFAULT generateUUIDv4(),
    run_date DateTime,
    trigger_type LowCardinality(String),  -- 'calendar','news','manual'
    trigger_ref String,
    gold_scenario JSON,        -- сценарная сетка + вероятности
    usdrub_path JSON,
    price_deck LowCardinality(String) DEFAULT 'own_scenario',  -- 'spot_flat','consensus_lt','own_scenario'
    discount_rate Float64,     -- 0.05 real USD база + надбавки; локальная ставка — в comment/JSON
    wacc Float64,
    nav_per_share Float64,
    nav_bull Float64, nav_base Float64, nav_bear Float64,
    market_price Float64,
    upside_pct Float64,
    nav_beta_gold Nullable(Float64),   -- рычаг NAV к цене золота (эталон: EBITDA-бета ~14% на +10% Au)
    dividend_status LowCardinality(String) DEFAULT 'suspended',  -- до 2030
    comment String
) ENGINE = ReplacingMergeTree ORDER BY (run_date, run_id);

CREATE TABLE IF NOT EXISTS forecast_log (
    forecast_date Date,
    target_date Date,
    metric LowCardinality(String),   -- 'xau_q_avg','fed_decision','nav','cbr_rate'
    predicted Float64, actual Nullable(Float64),
    error_pct Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (metric, forecast_date);

-- Рыночный слой (каналы давления на цену акции)
CREATE TABLE IF NOT EXISTS news_events (
    date DateTime,
    source LowCardinality(String),
    title String,
    url String,
    sentiment Nullable(Float32)      -- -1..1, заполняет LLM-сессия
) ENGINE = ReplacingMergeTree ORDER BY (date, source, title);

CREATE TABLE IF NOT EXISTS index_weights (
    rebalance_date Date,
    index_code LowCardinality(String),   -- 'IMOEX'
    sec_code LowCardinality(String),
    weight Float32
) ENGINE = ReplacingMergeTree ORDER BY (rebalance_date, index_code, sec_code);

CREATE TABLE IF NOT EXISTS dividend_events (
    sec_code LowCardinality(String),
    period LowCardinality(String),
    board_date Date,
    amount Nullable(Float64),        -- NULL при пропуске
    yield_pct Nullable(Float64),
    status LowCardinality(String)    -- 'paid','declared','suspended'
) ENGINE = ReplacingMergeTree ORDER BY (sec_code, board_date);

CREATE TABLE IF NOT EXISTS ofz_curve (
    date Date,
    tenor LowCardinality(String),    -- '1y','3y','5y','10y','RGBI'
    yield Float64
) ENGINE = ReplacingMergeTree ORDER BY (date, tenor);

CREATE TABLE IF NOT EXISTS mmf_aum (
    date Date,
    fund LowCardinality(String),
    aum_rub_bn Float64
) ENGINE = ReplacingMergeTree ORDER BY (date, fund);

CREATE TABLE IF NOT EXISTS tax_events (
    published Date,
    topic LowCardinality(String),    -- 'ndpi_gold','profit_tax','ndfl'
    title String,
    fcf_impact_pct Nullable(Float64),
    status LowCardinality(String)    -- 'draft','adopted','active'
) ENGINE = ReplacingMergeTree ORDER BY (published, topic);

-- Ресурсная база и peers (P/NAV-сверка сектора: PLZL / ЮГК / SELG)
CREATE TABLE IF NOT EXISTS reserves_assets (
    company LowCardinality(String),  -- 'PLZL','UGK','SELG'
    asset LowCardinality(String),    -- 'Olimpiada','Blagodatnoye','Sukhoi Log','Tominskiy','Kochkarskoye','Hvoynoye'
    category LowCardinality(String), -- 'PP','MI','Inferred'
    standard LowCardinality(String), -- 'JORC','NAEN' (SELG по РФ-классификации — пересчитывать!)
    stage LowCardinality(String),    -- 'production','construction','dfs','pfs','pea','exploration'
    date Date,
    koz Float64,
    grade_gpt Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (company, asset, category, date);

CREATE TABLE IF NOT EXISTS reserves_dynamics (
    company LowCardinality(String),
    year UInt16,
    reserves_koz Float64,
    yoy_pct Nullable(Float64),
    conversion_koz Nullable(Float64),  -- ресурсы → запасы за год
    grr_spend_usd_mln Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (company, year);

CREATE TABLE IF NOT EXISTS license_events (
    company LowCardinality(String),
    asset LowCardinality(String),
    event_type LowCardinality(String),  -- 'issue','renewal','auction'
    date Date,
    cost_rub Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (company, asset, date);

CREATE TABLE IF NOT EXISTS peers_nav (
    company LowCardinality(String),
    date Date,
    peer_universe LowCardinality(String), -- 'ru','global' — P/NAV считать отдельно по контурам
    nav_per_share Nullable(Float64),   -- own-модель для PLZL; оценочная для peers
    market_price Float64,
    p_nav Nullable(Float64),
    ev_per_koz Nullable(Float64),      -- EV / запасы — ценник недр
    discount_to_leader Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (company, peer_universe, date);

-- Sum-of-parts: LOM-планы по активам (вход) и NAV по активам (выход)
CREATE TABLE IF NOT EXISTS mine_plans (
    company LowCardinality(String),
    asset LowCardinality(String),
    year UInt16,
    production_koz Float64,
    grade_gpt Nullable(Float64),
    tcc Float64, aisc Float64,
    capex_sustaining Float64, capex_project Float64,
    closure_costs Float64              -- отрицательный хвост конца LOM (рекультивация, выходные пособия)
) ENGINE = ReplacingMergeTree ORDER BY (company, asset, year);

-- Ключ обязан включать deck и contour: без них ReplacingMergeTree схлопывал бы
-- три ценовых дека и два контура ставки в одну строку — тихая потеря, тот же
-- класс дефекта, что и ключ company_financials без source_url (§8.1, дефект 3).
CREATE TABLE IF NOT EXISTS nav_by_asset (
    run_id UUID,                       -- связка с model_runs
    deck LowCardinality(String),       -- 'spot_flat','consensus_lt','own_scenario'
    asset LowCardinality(String),
    contour LowCardinality(String),    -- 'industrial','local' — контур ставки (spec §3.2)
    npv_usd_mln Float64,
    discount_rate Float64,             -- 0.05 real USD база + страновая/стадийная надбавка
    stage_haircut Nullable(Float64)    -- construction 0.7–0.9, DFS 0.5–0.7, PEA 0.2–0.4
) ENGINE = ReplacingMergeTree ORDER BY (run_id, deck, asset, contour);

-- Ценовые деки (NAV считается на всех трёх: спот / консенсус LT / собственный сценарий)
CREATE TABLE IF NOT EXISTS price_decks (
    deck LowCardinality(String),       -- 'spot_flat','consensus_lt','own_scenario'
    year UInt16,
    gold_usd Float64,
    published Date
) ENGINE = ReplacingMergeTree ORDER BY (deck, year, published);

-- Режимная детекция золота (HMM/Markov-switching): сценарные вероятности согласуются с режимом
CREATE TABLE IF NOT EXISTS regime_states (
    date Date,
    regime LowCardinality(String),     -- 'bull','bear','transition'
    probability Float32,
    model LowCardinality(String)       -- 'hmm_2state','expert'
) ENGINE = ReplacingMergeTree ORDER BY (date, model);
```

### 6.3 Витрины (views) — semantic layer для агента

- `v_model_inputs` — **реализован** (`views/model_inputs.go`): одна строка с последними значениями рядов (`argMax` по дате), у каждого ряда колонка даты актуальности `*_date`. Состав: `gold_moex_fix_usd` (MOEX-фикс, не LBMA), `usdrub`, `cbr_key_rate`, `ofz_1y/3y/5y/10y`, `rgbi`, `ipc_mes_last`, `ipc_week_ytd` (ориентир, не официальный ИПЦ), `fedfunds`, `dfii10`, `dgs10`, `t5yie`, `dtwexbgs`, `plzl_close`, `last_run_id`, `last_nav_per_share`, `last_run_date`. Crack, TCC и AISC в витрине **нет** (crack — в `v_gold_dashboard`, TCC/AISC в MCP не выводятся)
- `v_gold_dashboard` — **реализован** (`views/gold_dashboard.go`): одна строка на индикатор `crack_ulsd_proxy`, `distillate_stocks`, `fedwatch_dec_hike`, `etf_flows_month`, `dxy`; значения из `macro_series` (`source IN ('manual','webbridge')`, spec §5.2). `flag = 1` — порог пробит (crack >50, FedWatch >65 — сценарное переключение; >80 — уровень алерта в тексте порога; DXY >102); `stale = 1` — значения нет или последнее наблюдение старше 14 дней. Импортёров для этих индикаторов нет: значения только ручные, поэтому без ввода через `manual_series` они остаются `stale`
- `v_forecast_accuracy` — **реализован** (`views/forecast_accuracy.go`): строка на прогноз из `forecast_log` (`metric`, `forecast_date`, `target_date`, `error_pct`, `is_resolved`). Отдельного конкурентного ML-трека в витрине нет (различается только `metric`). `actual` и `error_pct` в коде не заполняются: сверка с фактом после `target_date` не автоматизирована
- `v_peers_comparison` — **не реализован** (в коде нет). План: P/NAV, EV/oz, дисконт к лидеру по контурам `ru`/`global`; алерт-порог — дисконт PLZL за ±1σ исторической нормы
- `v_company_financials` — **реализован** (`views/company.go`, импортёр `company_views`): метрики компаний сектора, **одна строка на `(company, metric, period)`** — разрешённое значение плюс провенанс (`source_kind`, `source_url`, `source_page`, `unit`, `period_type`). Основной вход DCF: один ответ без споров. Сейчас наполнена только Полюсом (`company = 'PLZL'`); схема заложена общей под металлургов (`docs/ROADMAP_DCF_METALS.md`). Правило разрешения: `ifrs` (аудит) > `kpi` (пресс-релиз); при равном виде документа — по свежести `loaded_at`, затем по `source_url` (детерминированный тай-брейк, чтобы выдача не «плавала» между прогонами). Внутри одного прогона импортёра все строки получают одну `loaded_at`, поэтому между двумя пресс-релизами за один период решает `source_url`, а не «свежесть документа» (спека §3.4)
- `v_company_metric_sources` — **реализован** (`views/company.go`): **все** версии значения, включая проигравшую в `v_company_financials`. Правило приоритета не применяется: витрина нужна затем, чтобы расхождение документов оставалось видимым. Проверено живым прогоном: `gold_output` за `2023FY` даёт две строки (2 799 из релиза FY2024 и 2 902 из релиза FY2023) с разными `source_url`; 347 строк при 265 различных `(company, metric, period)`
- `v_company_operating` — **реализован** (`views/company.go`): `company, asset, metric, frequency, date, value` из `databook_polyus FINAL` — операционка по активам (вход `mine_plans`). `company` — константа `'PLZL'`. Гранулярность ДРУГАЯ, чем у релизных метрик (`asset` — месторождение, `frequency` — ANNUAL/QUARTERLY/SEMI-ANNUAL); с `v_company_financials` не смешивать: совпадающие имена метрик означают здесь величину по активу, а не по компании
- `v_dcf_assumptions` — **реализован** (`views/dcf_assumptions.go`): выход DCF-движка, **одна строка на `(run_id, deck, contour, asset)`** плюс оконный итог `nav_total_usd_mln = sum(npv_usd_mln) OVER (PARTITION BY run_id, deck, contour)` — тот же итог повторяется в каждой строке группы, поэтому при выборке нескольких активов суммировать его повторно нельзя. Колонки: `run_id` (связка со строкой `model_runs` этого прогона), `deck` (`spot_flat`/`consensus_lt`/`own_scenario`), `contour` (`industrial`/`local`), `asset`, `npv_usd_mln` (NPV LOM-плана актива, млн USD, с НДПИ, налогом и ОТРИЦАТЕЛЬНЫМ хвостом закрытия — perpetual TV у рудника нет), `discount_rate` (ставка **этого** контура: 0.05 индустриальная / 0.16 локальная), `stage_haircut` (всегда NULL на этой итерации — стадийные haircut'ы вне среза; NULL значит «дисконт не применён», а не «нулевой»). Складывать `npv_usd_mln` между контурами или дека'ми **нельзя** — это две разные ставки и три разных ценовых сценария (§13.1, §13.4); сравнивать P/NAV допустимо только по одинаковому deck. `nav_total_usd_mln` — NAV **рудников**, а не NAV акции: net debt, корпоративные затраты и опционность ресурсов вне LOM в него не входят. Источник — `nav_by_asset FINAL`, которую наполняет импортёр `dcf_engine`; **сырая `nav_by_asset` агенту не выдана** (грант `kimi_reader` — только на витрину, `sql/mcp_kimi_reader.sql`), как и `mine_plans`/`price_decks`. Комментарий таблицы и каждой колонки поставлены через `ALTER TABLE ... MODIFY COMMENT` / `COMMENT COLUMN`. Пустая выдача витрины означает «`nav_by_asset` этого прогона пуста» (план актива ещё не засеян или прогон шёл без `DCF_RUN_ID`), а не «активов нет»: набор активов задаёт `mine_plans`, а не витрина
- `v_polyus_*` — **не заводится и не планируется.** Решение заказчика (спека [2026-10-10-company-financials-views-design.md](docs/superpowers/specs/2026-10-10-company-financials-views-design.md) §10): персональный набор витрин под одного эмитента повторял бы работу, гранты и комментарии столько раз, сколько компаний. Вместо него — общие `v_company_*` выше; дефект (4) ROADMAP §8.1 закрыт, ограничение «агент MCP не видит данные Полюса» снято (§6.5)
- `v_gold_attribution` — **не реализован** (в коде нет). План: GRAM-разложение движения золота (экспансия / риск / альтернативная стоимость / импульс); ошибки прогноза атрибутируются к фактору
- `v_series_catalog` — **реализован** (`util/series_catalog.go`): `series_catalog FINAL`: название, единицы, частота, происхождение и описание каждого ряда `macro_series` (ведётся импортёрами через `util.UpsertSeriesCatalog`)
- `v_bea_pce` — `macro_series FINAL WHERE source = 'bea'`: индексы PCE из BEA (`PCE_PI`, `PCE_PI_CORE`), уровни 2017=100
- `v_fred_macro` — `macro_series FINAL WHERE source = 'fred'`: ряды FRED (DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE)
- `v_bls_macro` — `macro_series FINAL WHERE source = 'bls'`: ряды BLS (CPI NSA/SA, безработица, NFP, зарплата, PPI, JOLTS)
- `v_cbr_macro` — ряды ЦБ РФ в единой форме `(source, series, date, value)`: `cbr_key_rate`, `cbr_currency_usd`, `cbr_gold`, `cbr_ruania` (7 метрик RUONIA), `cbr_m2`, `cbr_credit_m2x`, `cbr_bank_int_rate`, `cbr_indicators_cpd`, `cbr_infl_exp`, `cbr_loans_to_individuals`, `cbr_loans_to_corporations`, `households_b_mes`; единицы и сдвиги дат — в `v_series_catalog`
- `v_rosstat_macro` — ряды Росстата в той же форме: `ipc_mes`, `ipc_weeks`, `vvp_kvartal`, `salaries_mes`
- `v_minfin_budget` — исполнение бюджета: `minfin_fed_bud_mes` (source='minfin') и `minfin_fed_bud_mesyats` (source='minfin_mesyats', консолидированный бюджет; source различается, т.к. имена рядов совпадают)
- `v_gold_prices` — `gold_prices FINAL` в форме `(source, series, date, value)`: source='gold', series=venue (`moex_fix_usd` — производная цена, не LBMA)
- `v_stock_prices` — `stock_prices FINAL WHERE code='PLZL'`: дневные OHLCV акции Полюса (руб./акция); max/min — high/low дня
- `v_ofz_curve` — `ofz_curve FINAL`: доходности G-curve МосБиржи (теноры 1y/3y/5y/10y, % годовых) + уровень индекса RGBI (тенор RGBI — не доходность)
- `v_events_calendar` — **реализован** (`calendar/events.go`): `events_calendar FINAL`: календарь событий-триггеров прогноза золота/NAV (дата, категория, заголовок, пороги-триггеры в JSON, статус pending|done|verified); сид Q4-2026 — `sql/events_calendar_q4_2026.sql`

Правила витрин:
- создаются через `CREATE OR REPLACE VIEW ... DEFINER = default SQL SECURITY DEFINER AS ...` — определение может меняться, и агент читает сырые таблицы через definer, без прав на `macro_series`;
- комментарии ставятся через `ALTER TABLE v_x MODIFY COMMENT '...'` и `ALTER TABLE v_x COMMENT COLUMN col '...'`: синтаксис `COMMENT ON TABLE/COLUMN` в текущей версии ClickHouse (26.10) не поддерживается;
- `CREATE OR REPLACE` для витрин — отступление от правила «DDL всегда `IF NOT EXISTS`» (то правило относится к таблицам);
- для каждой новой витрины — `GRANT SELECT` пользователю `kimi_reader` в `sql/mcp_kimi_reader.sql`;
- витрины контура прогноза (`views/`, импортёр `gold_views`) создаются `util.CreateView` и пропускаются, пока не существуют все их исходные таблицы. `v_model_inputs` требует 9 таблиц, в том числе `ipc_mes` и `ipc_weeks` (Росстат) и `model_runs` (ingest). Гейт проверяет только наличие таблиц (`system.tables`), не заполненность: `ipc_mes` и `ipc_weeks` создаются до загрузки Росстата, поэтому при его сбое витрина создаётся, а колонки `ipc_mes_last` и `ipc_week_ytd` дают NULL или устаревшие значения (ориентироваться по `*_date`). Запускать `make import STAT=gold_views` после первого запуска ingest
- витрины метрик компаний (`views/company.go`, импортёр `company_views`) создаются тем же `util.CreateView`, но не в единственном шаге: импортёр **сначала исполняет DDL таблицы** `company_financials` сырым `conn.Exec` (`companyViewsDDL()`), и лишь затем создаёт витрины. Порядок обязателен: гейт `CreateView` считает таблицы-источники в `system.tables` и **молча пропускает** витрину (created=false, err=nil), если хоть одной нет, — на пустой БД это пропустило бы и `v_company_financials`, и `v_company_metric_sources`, потому что таблицу создаёт тот же импортёр. `v_company_operating` гейтится по `databook_polyus` и на пустой БД будет пропущена (правильно: до импорта датапака она всё равно нечитаема). Затем импортёр пишет описания рядов Полюса в `series_catalog` (18 релизных + 19 датапак = 37 строк; `util.UpsertSeriesCatalog` идемпотентен). Гранты импортёр не выдаёт — их меняет владелец БД (`make mcp-user`, `sql/mcp_kimi_reader.sql`). Запуск: `make import STAT=company_views`; шаг стоит в `dagu/financial.yaml` после `polyus_financial_metrics`
- групповые витрины legacy-источников (`v_cbr_macro`, `v_rosstat_macro`, `v_minfin_budget`, `v_gold_prices`) создаются через `util.CreateView`: витрина появляется, только когда все таблицы её группы уже импортированы, поэтому её создаёт последний импортёр группы; описания рядов группы пишутся в `series_catalog` каждым импортёром своими записями.

### 6.4 DCF-модель Полюса (ключевые допущения, rev.2 по мировой практике)

```
СТРУКТУРА: sum-of-parts, не корпоративный DCF
NAV = Σ_asset NPV(LOM-план актива из mine_plans) − NetDebt − CorpCosts + Опционность ресурсов
LOM-DCF актива: FCF_t = Production×(Gold_deck_t − AISC_t) − НДПИ − налоги − capex ± ΔWC;
  в конце LOM — хвост closure_costs (ОТРИЦАТЕЛЬНЫЙ, perpetual TV у рудника НЕТ)
НДПИ = база + 10%×max(gold−1900,0)  ← надбавка с 2025; AISC_t = AISC_base × эскалация(ИПЦ РФ)

СТАВКА (двухконтурная):
  1) индустриальная: 5% real USD + страновая/стадийная надбавка (CIM-сurvey: до 10% для рисковых
     юрисдикций) — сопоставима с глобальным P/NAV; CAPM/WACC для золотодобычи НЕ использовать
     (бета сектора ≈ 0 или отрицательная)
  2) локальная (для RU-инвестора): ОФЗ-кривая (ofz_curve) + премии — объясняет локальный дисконт
Оба результата пишутся в model_runs

PRICE DECK (обязательно 3 контура, price_decks):
  spot_flat / consensus_lt (LT-якорь системно ниже спота: Scotia $2,600 с 2029, BMO $3,000) /
  own_scenario (gold_forecasts; LT-хвост обязан сходиться к обоснованной LT-оценке)
Сценарии золота Q4-2026 (база из docs/): bull $4,600–5,000 (25%), base $4,000–4,600 (40%), bear $3,750–4,050 (35%);
вероятности согласуются с regime_states (HMM); формат сценариев — по WGC Gold Outlook

РЕСУРСЫ ВНЕ LOM: не в DCF, а опционно — EV/oz × stage_haircut (construction 0.7–0.9 / DFS 0.5–0.7 /
PEA 0.2–0.4); Сухой Лог (43 млн унц из 106,8) — отдельная строка со стадийным дисконтом

РЫНОЧНЫЙ МОСТ: дисконт к NAV декомпозируется: страновой (RU-акции ≈ −75% к EM-пирам в 2025) +
компанейский (див.спред к ОФЗ, переток ликвидности, индексные потоки, sentiment)
Peers-сверка (peer_universe='ru': PLZL/ЮГК/SELG) — относительная рамка; глобальные пиры — только
для измерения странового дисконта. Выводы run'а: NAV/акция по 3 deck × 2 ставки + NAV- и EBITDA-бета
к золоту (эталон BofA: EBITDA-бета ~14% на +10% золота)
```

**Реализация:** чистый расчёт этой конвенции — пакет `dcf/` (`ndpiPerOz`, `npvLOM`, `escalate`; порог $1 900 и надбавка 10% — поля `Params`, ставки контуров — `DiscountRates` в `dcf/params.go`), обвязка — импортёр `dcf_engine` (§6.1), выход — `nav_by_asset` и витрина `v_dcf_assumptions` (§6.3). Что из конвенции **ещё не реализовано**: сид `mine_plans` из отчётности Полюса (пока таблица пуста, движок считает 0 строк), эскалация AISC/TCC по ИПЦ (`Params.Ipc` пуст), стадийные haircut'ы ресурсов вне LOM (`stage_haircut` всегда NULL), рублёвый NAV (`usdrub_path` пуст — все деньги USD), мост NAV → цена акции и NAV-матрица в Grafana. Остаток фазы 3 перечислен в [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md) §7.

### 6.5 MCP-контур

Сервер: официальный [ClickHouse/mcp-clickhouse](https://github.com/ClickHouse/mcp-clickhouse) (PyPI `mcp-clickhouse`), версия закреплена переменной `MCP_VERSION` (сейчас `0.7.0`). Локально запускается через `uv` (Docker не требуется):

```
uv run --with mcp-clickhouse==0.7.0 --python 3.12 mcp-clickhouse
```

Конфигурация Kimi — пользовательский `~/.kimi-code/mcp.json` (права 600), блок `mcpServers.clickhouse`: `command: bash`, `args`: `-c` с подгрузкой `~/.config/rosstat/env` и затем `exec uv run --with mcp-clickhouse==0.7.0 --python 3.12 mcp-clickhouse`; `env`: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT=8123`, `CLICKHOUSE_SECURE=false`, `CLICKHOUSE_USER=kimi_reader`, `CLICKHOUSE_DATABASE=default`, `CLICKHOUSE_MCP_SERVER_TRANSPORT=stdio`. Пароль `CLICKHOUSE_PASSWORD` приходит из файла окружения и в `mcp.json` не хранится.

**HTTP-режим развёрнут на staging (реализовано 2026-10-10).** Сервис — `deploy/staging/mcp-clickhouse.service` (systemd), раскладка и порядок деплоя — `deploy/staging/README.md`. Параметры:

| Переменная | Значение | Смысл |
|---|---|---|
| `CLICKHOUSE_MCP_SERVER_TRANSPORT` | `http` | HTTP-транспорт вместо stdio |
| `CLICKHOUSE_MCP_BIND_HOST` / `_PORT` | `127.0.0.1` / `8000` | слушает только loopback; наружу не публикуется |
| `CLICKHOUSE_MCP_AUTH_TOKEN` | из `/etc/clickhouse-import-rosstat.env` | **обязателен**: без него сервер падает на старте |
| `CLICKHOUSE_USER` | `kimi_reader` | read-only, только витрины `v_*` |

`GET /health` намеренно **без** аутентификации (для проб); проверка авторизации — запросом к `POST /mcp`, который без заголовка `Authorization` обязан вернуть `401`. TLS и reverse proxy (Caddy) пока не развёрнуты: эндпоинт доступен только через SSH-туннель. Плагин `gold-nav` подключается по URL (`mcpServers.clickhouse.url`, Bearer-токен на чтение).

Инструменты сервера: `list_databases`, `list_tables` (читает `system.tables`/`system.columns`, видит только то, на что есть гранты; комментарии колонок попадают в `create_table_query`), `run_query`.

Пользователь БД `kimi_reader`: **только SELECT, только витрины `v_*`**, `readonly = 1`, `DEFAULT DATABASE default`. Сырые таблицы (`macro_series`, `gold_prices`, ...) агенту недоступны: проверено `497 ACCESS_DENIED`. Создание и гранты — `sql/mcp_kimi_reader.sql` (пароль подставляется вручную). Запись прогнозов — не через MCP, а через ingest-endpoint (`cmd/ingest`, см. ниже).

**DCF-контур (`dcf/`) правилу 11 удовлетворяет** (2026-10-10, импортёр `dcf_engine`): агент видит витрину `v_dcf_assumptions` (`GRANT SELECT` в `sql/mcp_kimi_reader.sql`, комментарии таблицы и каждой колонки — §6.3), а сырые `nav_by_asset`, `mine_plans` и `price_decks` **не выданы и не должны** — доступ даёт только витрина. Отдельного каталога рядов у контура нет намеренно: `nav_by_asset` — не ряд `macro_series`, а расчётная таблица прогонов, и описывать её в `series_catalog` значило бы завести там сущность, которой каталог не управляет. Проверено живым `make mcp-check` 2026-10-10 (ПРОВЕРКА ПРОЙДЕНА): витрина читается через `run_query`, `SELECT count() FROM nav_by_asset` под `kimi_reader` даёт `ACCESS_DENIED`.

Правило для новых источников и рядов (см. AGENTS.md, правило 11): каждый ряд описан в `series_catalog`, каждая витрина имеет комментарии таблицы и колонок и грант `kimi_reader`.

**Отчётность Полюса (`polyus/`) правилу 11 теперь удовлетворяет** (2026-10-10, импортёр `company_views`):

- **каталог рядов:** `views/series_meta.go` описывает все ряды Полюса в общем `series_catalog` — 18 релизных метрик (`source = 'polyus'`) и 19 рядов датапака (`source = 'polyus_datapack'`), 37 строк; агенту по-прежнему доступна одна точка входа `v_series_catalog` (отдельного каталога под Полюса не заводится);
- **комментарии и провенанс:** у таблицы `company_financials` и всех трёх витрин есть комментарий таблицы и каждой колонки (`ALTER TABLE ... MODIFY COMMENT` / `COMMENT COLUMN`; `COMMENT ON` в ClickHouse 26.10 не работает);
- **грант:** `GRANT SELECT` на `v_company_financials`, `v_company_metric_sources`, `v_company_operating` добавлен в `sql/mcp_kimi_reader.sql` (выдаётся вручную через `make mcp-user`).

Поэтому ограничение «агент MCP не видит данные Полюса» **снято**: производительность, TCC, AISC, capex, выручка и операционка по активам читаются агентом через `v_company_*`. Сырые таблицы `databook_polyus`, `polyus_financial_metrics` и `company_financials` агенту по-прежнему **не выданы и не должны** — это сырые таблицы; доступ даёт только витрина. Проверено живым `make mcp-check` 2026-10-10 (ПРОВЕРКА ПРОЙДЕНА): три витрины видны в `list_tables`, `run_query` к ним читается, запрос к сырым таблицам даёт `ACCESS_DENIED`. Дашборд `dashboard/finance-polyus.json` продолжает читать legacy-таблицу `polyus_financial_metrics` (она не тронута); перевод дашборда на `v_company_*` — отдельный шаг (docs/ROADMAP_DCF_METALS.md).

Отдельно от data-контура: опциональный dev-MCP `jetbrains` — встроенный MCP-сервер GoLand (2025.2+, HTTP-stream `http://127.0.0.1:<динамический порт>/stream`, запись в `~/.kimi-code/mcp.json`). Даёт агенту инструменты IDE для работы с кодом (`get_file_problems`, `get_symbol_info`, `search_symbol`, `analyze_calls`, `rename_refactoring`); правила использования и запреты — в AGENTS.md, раздел «Инструменты GoLand (MCP `jetbrains`)», настройка — в README, «MCP GoLand (опционально)». К данным ClickHouse отношения не имеет.

#### Ingest-endpoint (пакет `ingest/`, бинарник `cmd/ingest`)

**Реализован.** Единственный путь записи в `model_runs`, `forecast_log` и `macro_series` со стороны контура прогноза.

- Сборка и запуск: `make build-ingest` → `build/ingest`; `make run-ingest`. Нужны `CLICKHOUSE_URL` и `INGEST_TOKEN` (см. §5); `INGEST_ADDR` по умолчанию `:8081`; `INGEST_RATE_PER_MIN` по умолчанию 60.
- Старт: ping ClickHouse, затем `EnsureTables` — идемпотентное создание `model_runs`, `forecast_log`, `macro_series` по DDL §6.2 (копии в `ingest/schema.go`).
- Авторизация: заголовок `Authorization: Bearer <INGEST_TOKEN>`, сравнение через `subtle.ConstantTimeCompare`. Без заголовка или с неверным токеном — `401`.
- Частота: token bucket (stdlib, `ingest/ratelimit.go`), общий на сервер, burst 10, `INGEST_RATE_PER_MIN` запросов в минуту (по умолчанию 60). Проверяется после авторизации: неавторизованные запросы лимит не расходуют. Превышение — `429`.
- Тело запроса — не более 1 MiB (`http.MaxBytesReader`; больше — `413`). JSON декодируется с `DisallowUnknownFields`: неизвестное поле — `400`.
- `POST /v1/model_run` — JSON `ModelRun`: клиентский `run_id` (UUID, обязателен), `trigger_type` ∈ {`calendar`,`news`,`manual`}, `price_deck` ∈ {`spot_flat`,`consensus_lt`,`own_scenario`}, `probabilities` (ключи только `bull`/`base`/`bear`, каждый вес 0–100, сумма 100 ±0.1), `gold_scenario` (с `horizon`: `Q<n>-YYYY`, `YE-YYYY`, `YYYY` или `YYYY-MM-DD`; блоки сценариев с `point` (число, если задан) или `low`/`high`), `nav_per_share`, `nav_bull/base/bear`, `market_price`, `upside_pct`, `discount_rate`, `wacc`, `usdrub_path`, `comment`. Ответ `200 {"inserted": 1}`; `400` — ошибка валидации или JSON.
- Запись: сначала `forecast_log` (две строки: `metric = 'nav'`, `predicted = nav_per_share`; `metric = 'xau_q_avg'`, `predicted` = Σ p×point / Σ p; `forecast_date` — дата сервера, `target_date` — конец горизонта, `actual` и `error_pct` = NULL), последней — `model_runs` (`run_date` = время сервера, `nav_beta_gold` = NULL, поля в входе нет). Строка в `model_runs` — маркер фиксации прогона.
- Идемпотентность: повторный POST с тем же `run_id` ничего не пишет (проверка `count() ... FINAL` + мьютекс, один инстанс). `ReplacingMergeTree` по `(run_date, run_id)` дубли сам не схлопывает. Ответ при дубле такой же, как при записи: `200 {"inserted": 1}`.
- `POST /v1/manual_series` — JSON-массив `{"series", "date" (YYYY-MM-DD), "value"}`; пишется в `macro_series` с `source = 'manual'`. Ответ `200 {"inserted": N}`; пустой массив — `N = 0`; `400` — ошибка валидации любой точки (запись не выполняется).
- Ошибка записи в ClickHouse — `500 insert failed`, детали в логе.
- Реализовано: ограничение частоты (`429`), предел тела 1 MiB (`413`). Не реализовано: TLS в самом процессе — предполагается reverse proxy с TLS (в репозитории не описан).

#### Плагин gold-nav (отдельный репозиторий)

Плагин Kimi Code / Kimi Work лежит вне этого репозитория, sibling-каталогом `../gold-nav` (публикуется отдельно на GitHub; ссылка — после публикации). Состав: `kimi.plugin.json` (`mcpServers.clickhouse` по URL), skills `gold-forecast-session` и `dcf-methodology`, команды `/gold-nav:session` и `/gold-nav:verify`.

- Читает витрины `v_model_inputs`, `v_gold_dashboard`, `v_events_calendar`, `v_forecast_accuracy` через MCP (только `v_*`, пользователь `kimi_reader`).
- Пишет run через `POST /v1/model_run` и ручные ряды через `POST /v1/manual_series` по адресу `GOLD_NAV_INGEST_URL` с токеном `GOLD_NAV_INGEST_TOKEN`. Без токена сессия работает в режиме «только расчёт» (файл `run.json`).
- Переменные окружения плагина (имена): `GOLD_NAV_MCP_TOKEN` (выдаёт автор, read-only), `GOLD_NAV_INGEST_URL` и `GOLD_NAV_INGEST_TOKEN` (только у автора). Значения в репозиторий не кладутся.
- Не проверено: установка и живой сценарий в Kimi Code и Kimi Work, подстановка `${GOLD_NAV_MCP_TOKEN}` в манифесте (раздел «Результат проверки» в README плагина пока не заполнен). Спека: [docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md](docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md).

### 6.6 Отчётность Полюса: разбор PDF колоночной моделью и известные ограничения (пакет `polyus/`)

С 2026-10-10 извлечение текста идёт через **`pdftotext -tsv`** (координаты слов), а значения раскладываются по **колонкам, выведенным из шапки по X**, — вместо `-layout` и позиционного сопоставления. Спека: [docs/superpowers/specs/2026-10-10-polyus-column-model-design.md](docs/superpowers/specs/2026-10-10-polyus-column-model-design.md); план: [docs/superpowers/plans/2026-10-10-polyus-column-model.md](docs/superpowers/plans/2026-10-10-polyus-column-model.md). Предыдущая спецификация (`2026-10-10-polyus-parsers-design.md`) описывает перенос из legacy и остаётся историей.

Модули:

- **`polyus/source.go`** — `extractPage` вызывает `pdftotext -f N -l N -tsv -nopgbrk -enc UTF-8`; `extractPDF` извлекает страницы по отдельности и склеивает их (`joinFiles`), поэтому в одном файле может быть несколько страниц и несколько таблиц.
- **`polyus/words.go`** — `Word` (текст, `Page` из колонки `page_num`, `Left`, `Top`, ширина/высота, `Block`, `Line`), `Line` (слова одной страницы с одинаковым `Top`, по возрастанию `Left`), `parseTSV` (только строки уровня 5; строка заголовка TSV отбрасывается по имени первой колонки — в склеенном файле она повторяется в середине), `groupByLine(words, deltaTop)`. Штатный допуск — **`deltaTop = 0`**: у слов одной визуальной строки `Top` совпадает точно (допуск оставлен страховкой от дрейфа базовой линии).
- **`polyus/columns.go`** — `Column{Period, Type, Left, Right}`, `columnsFromHeader([]Line)` (вход — **полоса** шапки: у FY2024 расшифровка «(if not mentioned otherwise)» набрана мельче и уехала на 0.77pt), `Column.covers`, `widenColumns` (границы расширяются до середины промежутка между соседями: подписи выровнены по левому краю, а числа — по центру). Помощники шапки: `unitsMarker`, `isPeriodHeaderLine`, `isTableHeader`, `firstHeaderLine`, `headerBand`, `changeLabels`.
- **`polyus/kpi.go`** — `parseKPILines` / `recordsFromLine` / `parseKPIPage`: находит полосы шапок, строку обслуживает ближайшая шапка сверху, дедупликация метрик — **на полосу** (две таблицы на странице — два независимых набора), многословные числа («1 069») склеиваются по X.
- **`polyus/ifrs.go`** — `parseIFRSPage`: колоночная модель не применяется (на МСФО-странице шапки нет, ровно два столбца периодов); берётся первое число после метки, период отчётности приходит параметром.
- **`polyus/guard.go`** — резерв: отбрасывает только записи с пустым `Period` (ошибка разбора: ключ `(metric, "")` схлопнул бы весь набор в одну строку) и сообщает число значений, не попавших ни в одну периодную колонку. `polyus/import.go` печатает этот счётчик в итоговой строке и предупреждает, если он не нулевой.

**Границы шапки — по ролям строк, а не по зазору (2026-10-10).** `headerBand` собирает полосу шапки, продолжая её, пока следующая строка играет роль строки шапки (маркер единиц, подписи периодов, подписи изменений), а не пока она подходит по вертикальному зазору. Позиционный критерий (`headerContinuationGap = 2pt`) не работал на шапках, разорванных по двум строкам: в релизе 2019FY строки `Q-o-Q / Y-o-Y / 2019 / 2018 / Y-o-Y` и `otherwise) / 2019 / 2019 / 2018` отстоят друг от друга на 5.04pt и 5.28pt, и полоса шириной 2 оставляла `4Q 2019`, `3Q 2019` и `Q-o-Q` без колонок — 48 значений уходили в счётчик нераспределённых. По ролям полоса доходит до строки данных, и квартальные колонки разбираются.

**Склейка разорванных подписей периодов.** Метка периода, разбитая переносом строки («4Q» на одной строке полосы, «2019» на другой), собирается по X: слова двух строк полосы, совпадающие по горизонтали, склеиваются перед `columnsFromHeader`. Без этого `4Q` и `2019` не дали бы периода, а два квартала 2020 (`4Q 2020`, `3Q 2020`) схлопнулись бы в два одинаковых годовых `2020FY`, и три числа 710 / 771 / 2 766 встали бы под один ключ. Тест `TestKPIValuesHistoryReports` (`polyus/kpi_test.go`) закрепляет каждый квартал как проверку «колонка не слилась с соседней».

**Живая приёмка (2026-10-10, прогон контролёра через `make import STAT=polyus_financial_metrics`, не самостоятельный замер этой задачи; таблица с тех пор заменена на `company_financials` и ключ расширен — актуальная приёмка ниже):** 66 строк и итоговая строка `Imported 66 rows (11 reports skipped as disabled, 0 failed, 0 periodless records dropped, 3 duplicate keys skipped, 26 values outside period columns)`. По периодам: `2014FY` 8, `2014H1` 8, `2014H2` 8, `2025H1` 9, `2025H2` 9, `2026H1` 16 (было 39 строк: два ранее терявшихся слота периода — `2014H1` и `2025H2` — теперь разбираются). Контрольные значения `2026H1` сохранены: `gold_output` 1 287, `tcc_per_ounce` 1 069, `total_revenue` 4 674, `profit_for_period` 829, `stripping_capex` 397. RU `2025H2`: `gold_output` 1 218, `tcc_per_ounce` 814 (раньше в этом слоте стояло процентное изменение `−2`). `2014H2`: `revenue` 1 232 — настоящая цифра второго полугодия 2014; FY2013-е 2 329 в `2014H2` больше не попадает.

**Живая приёмка релизов 2019–2024 (2026-10-10, самостоятельный прогон той задачи).** Тогда таблицей была ещё `polyus_financial_metrics` с ключом `(company, metric, period)`: 265 строк, `0 duplicate keys skipped` нет — итоговая строка `Imported 265 rows of polyus_financial_metrics (5 reports skipped as disabled, 0 failed, 0 periodless records dropped, 82 duplicate keys skipped, 178 values outside period columns)`. **Эти 82 отброшенных повтора ключа и были потерей, которую закрыла смена схемы** (§6.2, §6.3): после перехода на `company_financials` и расширения ключа до документа они стали настоящими строками — актуальный прогон 2026-10-10: `make import STAT=polyus_financial_metrics` → `Imported 347 rows of company_financials (5 reports skipped as disabled, 0 failed, 0 periodless records dropped, 0 duplicate keys skipped, 178 values outside period columns)`, в `v_company_metric_sources` 347 строк при 265 различных `(company, metric, period)` и `source_kind ∈ {kpi, ifrs}`. Все шесть включённых релизов разобрались, `0 failed`. `gold_output` сверен с закреплённой таблицей по каждому периоду: 2019FY 2 841, 2018FY 2 440, 2019Q4 804, 2019Q3 753; 2020FY 2 766, 2020Q4 710, 2020Q3 771, 2019Q4 804; 2021FY 2 717, 2021Q4 684, 2021Q3 770, 2020Q4 710; 2022FY 2 541, 2021FY 2 717; 2023FY 2 902, 2022FY 2 541; 2024FY 3 002. `revenue` 2019FY = 4 005. Несовпадений нет. Невыровненным остался только «квартальный» слот релиза 2023FY: он печатает полугодия (`2023H2` 1 454 / `2023H1` 1 448) вместо кварталов, и это форма самих шести отчётов 2022–2024, а не потеря колонок. Счётчик нераспределённых вырос с 26 до 178 — это ожидаемо: в полосы шести отчётов попадают колонки-изменения без года (`Y-o-Y`, `H-o-H`), их числа и есть нераспределённые значения.


**Закрытые дефекты (ROADMAP §8.1).** Все три закрыты. (1)–(2) колоночной моделью: (а) guard больше не «отбрасывает последнюю колонку» — непериодная колонка («Изм. за год», `change`, `Y-o-Y`) занимает своё место по X, её числа не читаются, и слот `2025H2` русского релиза разбирается; (б) `2014H2` больше не несёт цифры FY2013 — `change` не становится периодной колонкой, значения кладутся по X, `revenue 2014H2 = 1 232`. `untrustworthyPeriod` удалена (её позиционная эвристика больше не нужна: колонка-изменение распознаётся по отсутствию периода в шапке). При этом `scanPeriodColumns` и тип `PeriodColumn` **остались в коде как мёртвый код**: после перехода на `columnsFromHeader` их не вызывает ни один продовый путь, только собственный тест (`polyus/kpi_test.go`); `unused`-линтер их не ловит, потому что тест обращается к функции. Это уборка для отдельной задачи, не дефект разбора.

Дефект (3) — **ключ `(company, metric, period)` без измерения источника** — **закрыт 2026-10-10 сменой схемы** (спека [2026-10-10-company-financials-views-design.md](docs/superpowers/specs/2026-10-10-company-financials-views-design.md) §3.1): ключ стал `(company, metric, period, source_kind, source_url)` в новой таблице `company_financials` (§6.2). Одного `source_kind` не хватало: он различает вид документа (`kpi`/`ifrs`), но два KPI-релиза (FY2023 и FY2024) несут один и тот же `kpi` и на ключе из четырёх полей всё равно схлопывались бы — поэтому в ключ добавлен `source_url`, признак самого документа. Масштаб дефекта был измерен на шести включённых релизах 2019–2024: среди ключей, которые они печатают, **79 встречаются более чем в одном отчёте, и 17 из них несут РАЗНЫЕ значения** (62 — одинаковые). Показательный случай — `gold_output` за `2023FY`: релиз FY2023 печатает **2 902**, релиз FY2024 — **2 799**; так же расходятся `revenue` 2023FY (5 436 против 5 237), `operating_profit` 2023FY (3 172 против 3 123) и ещё 14 ключей. Это не ошибка разбора: обе цифры прочитаны верно, спорят документы. **Проверено живым прогоном 2026-10-10:** импорт даёт 347 строк при **265 уникальных** `(company, metric, period)` и `0 duplicate keys skipped` — проигравший документ перестал исчезать и виден в `v_company_metric_sources` (`gold_output 2023FY` — две строки, 2 902 и 2 799, с разными `source_url`), а правило разрешения спора применяется только в `v_company_financials` (одна строка, 2 902). Прежнее поведение («первый отчёт в порядке `reports`») отменено: порядок отчётов в `polyus/pages.go` больше не решает исход.

Дефект (4) — **агент MCP данных Полюса не видел** — **закрыт 2026-10-10** витринами `v_company_*` с комментариями и грантом `kimi_reader`; подробности — §6.3 и §6.5. Витрина сделана общей для сектора (`company_financials`), а не набором `v_polyus_*`; перевод металлургов — `docs/ROADMAP_DCF_METALS.md`.

**Ограничения, оставленные осознанно** (проверенные, не гипотезы; не исправлялись в этой задаче):

1. **`unitsMarker` привязан к строке, а не к смыслу.** `columns.go` ищет только склеенные `$млн`, `$mln`, `$million`. Отчёт, печатающий `US$ million`, оставит набор колонок пустым, и парсер вернёт **0 записей с `err == nil`** — молчаливо пустую страницу. (Форма `US$ million` — находка контролёра по другим релизам каталога; ни один из девяти включённых отчётов её не несёт: у всех фикстур 2019–2024 напечатано `$ million`, и оно склеивается.) Ограничение остаётся для отключённых 2015–2018 и будущих отчётов: тихий нулевой результат не отличим от «в отчёте нет метрик», и включение отчёта с такой шапкой потребует сначала расширить маркер.
2. **`parsePeriod` не знает формы `FY 2014` (год с пробелом).** Проверено контролёром: `parsePeriod("FY 2014")` и `parsePeriod("FY2014")` возвращают `unknown period format`, работает только голый год — `parsePeriod("2014")` → `2014FY`. Следствие для колонки, подписанной `FY 2014` в шапке: она не распознаётся как период и попадает в набор как `Column{Period: ""}`. Своё место по X она сохраняет — колонки справа не сдвигаются, значения чужим периодам не приписываются; но **её значения не читаются вообще** и попадают в счётчик нераспределённых импортёра. Это тот же класс тихой потери, что и ограничение 1: единственный сигнал — число в итоговой строке импорта (`... values outside period columns`), а строки данных при этом просто исчезают.
3. **Допущение «первое число = отчётный период» на МСФО-страницах не проверяется.** Оно выполняется на обеих текущих страницах (проверено: стр. 6 — `2026` на x=472.54 против `2025` на 522.82; стр. 7 — 465.79 / 515.98), но во время прогона ничто это не контролирует: отчёт, напечатавший сравнительный столбец первым, молча поменяет периоды местами.
4. **Счётчик guard'а не покрыт сквозным тестом.** Направление «парсер → счётчик» закреплено тестом, а последний переход в итоговую строку импортёра требует шва на уровне `Import` и подтверждён только живым прогоном.
5. **`pageOfLine` возвращает первую ненулевую страницу слова.** Это корректно, потому что `groupByLine` гарантирует одну страницу на строку; инвариант зафиксирован комментарием и проверен пробой, но теста, сканирующего фикстуры, у него нет.
6. **Полоса шапки по ролям строк может расти вниз.** `headerBand` продолжает полосу, пока строки играют роль строк шапки (маркер единиц, подписи периодов, подписи изменений). Роль «подпись изменения» опознаётся по словарю меток (`Y-o-Y`, `H-o-H`, `Q-o-Q`, `change`), и строка с такой подписью, стоящая в области данных, попадёт в полосу; её слова уйдут в `columnsFromHeader` не колонками, а метриками такой отчёт не прочитает (тот же класс тихой потери, что и ограничение 1). На девяти включённых отчётах недостижимо: замерено, полосы обрываются на строке данных. Но 2015–2018, которые включает следующий пункт, несут подписи изменений в области данных. Записано, не исправлено.
7. **Подъём полосы вверх ограничен расстоянием `maxFragmentLiftGap = 8pt`.** Якорь полосы — строка с меткой единиц, и она не обязана быть верхней строкой шапки: у релизов 4Q/FY 2020 и 2021 строка кварталов «4Q 3Q 4Q» стоит НАД якорем (зазоры ровно 5.40pt), а голый «4Q» периодом не является (`parsePeriod` читает только «4Q 2020»). Если квартал не поднять, он не склеится со своим годом на строке ниже, и «4Q 2020» с «3Q 2020» молча станут двумя годовыми колонками `2020FY` — три разных значения под одним ключом. Подъём поэтому берётся, но только на строку, состоящую целиком из обрывков периодов **и** прижатую к якорю: разрыв не больше 8pt. Настоящий обрывок дальше 8pt от якоря не будет поднят, и его квартальные колонки потеряются — то же схлопывание «4Q/3Q» в годовые, только уже без всякого сигнала в счётчике нераспределённых (значения останутся на своих местах, сменится лишь ключ периода). Порог измерен по фикстурам: настоящие случаи — 5.40pt, ближайшая строка не-обрывок над якорем — 7.07pt, первый реалистичный разрыв между таблицами — 18.18pt и больше; запас 1.5× снизу и 2.3× сверху. Сетка на эту ошибку — `TestKPIPeriodsAreUniquePerMetric` (`polyus/kpi_test.go`): два квартала, схлопнувшись в один годовой период, дают дубликат ключа внутри полосы, и тест валит его громко — по всем метрикам, а не только по `gold_output`. Записано, не исправлено.
8. **`foundMetrics` внутри полосы отбирает по первому `Top`.** Дедупликация метрики на полосу (метка может встретиться и в сноске той же таблицы) устроена как «строка метрики с меньшим `Top` побеждает»: `foundMetrics[definition.Name]` выставляется первой строкой, давшей записи, а совпавшая позже отбрасывается. Если на странице выше настоящей строки стоит другая строка с той же меткой, побеждает она, и настоящая строка теряется молча. Воспроизведено синтетически; на закоммиченных фикстурах не встречается. Существующий комментарий описывает эту дедупликацию только как подавление повторов, а не как выбор по первому `Top`.
9. **Дефект словаря метрик (существовал до этой работы):** `metrics.go` перечисляет `"Earnings per share – basic (US\nDollar)"` со встроенным переводом строки, который не может совпасть со склеенным текстом строки; следствие — на фикстуре FY2024 `parseKPIPage` не даёт ни одной записи `eps_*`. Записано, не исправлено.
10. **Отключённые отчёты сузились до 2015–2018 (плюс 2025H2)** — после включения шести релизов 4Q/FY за 2019–2024 причина отключения у оставшихся не в выверке страниц, а в типе документа: 2015–2017 — MD&A с консолидированной отчётностью, 2018 — консолидированная МСФО-форма, у которых нет шапки «Comparative financial results», а 2025H2 — англоязычный 2H25-релиз с двухшапочной таблицей. Включение 2015–2018 — следующий пункт (спека [docs/superpowers/specs/2026-10-10-polyus-history-reports-design.md](docs/superpowers/specs/2026-10-10-polyus-history-reports-design.md), §4 «Границы»): у каждого файла свои `Kind`, `Lang` и `Pages`, и, возможно, понадобится несколько `Pages` на отчёт.

Ограничение «агент MCP не видит данные Полюса» (`v_polyus_*` и гранта `kimi_reader` не было) **снято 2026-10-10** — витрины `v_company_*` с комментариями и грантом (§6.3, §6.5).


## 7. Календарь триггеров (актуальный Q4-2026)

| Дата | Событие | Действие модели |
|---|---|---|
| по средам (еженед.) | EIA дистилляты (запасы); недельный ИПЦ Росстата (день недели сверить с календарём Росстата) | EIA — пороги: crack >$50 медведь / <$30 снято |
| 10-е ежемес. | WGC ETF-потоки + ЦБ | норма покупок ЦБ 40–60 т/мес |
| 23.10.2026 | СД ЦБ РФ (ставка 14%) | снижение → флаг перетока ликвидности |
| 01.11.2026 | Истечение запрета РФ на экспорт дизеля | продление → bear; отмена → bull |
| 10.11.2026 | CPI США за октябрь | горячий → FOMC-hike 85–90% |
| 09.12.2026 | FOMC + dot plot | 2-е повышение → пробой $4,000 → $3,750–3,800 |
| 18.12.2026 | СД ЦБ РФ + ребалансировка MOEX | база: −25 б.п. до 13,75% |

Та же информация хранится в таблице `events_calendar` (сид `sql/events_calendar_q4_2026.sql`, витрина `v_events_calendar`); агент читает календарь из витрины. Таблица выше — краткая сводка для людей: даты и пороги в сиде точнее, а WGC в сиде — одна дата 2026-10-30 (GDT), а не «10-е число ежемесячно». При расхождении верна таблица `events_calendar`, а эту сводку нужно обновить.

## 8. Правила для AI-ассистента (Kimi Code)

1. **Новый источник = новый файл** в доменном пакете + `init()`-регистрация. main.go не править (пакет уже импортирован) — если пакета нет, добавить blank-import.
2. Использовать шаблон Б (`util.ClickHouseImport`) — и для XLSX-источников, и для краулеров (`ImportFunc` / `CrawFunc`); шаблон А — только если нужна кастомная логика поиска ссылки или источник не XLSX (API/CSV/JSON).
3. Вставка только батчами (`PrepareBatch`/`Append`/`Send`). DDL `IF NOT EXISTS`. Значения новых таблиц — Float64.
4. HTTP — только через `util.HttpClient` / `util.GetXlsx` / `util.GetCSV` (там нац. сертификаты и UA).
5. **Не хардкодить секреты** (в т.ч. в комментариях и curl-примерах) — только `os.Getenv`.
6. **Не делать сетевых вызовов в `init()`** — URL вычислять внутри `Import()` (бывший legacy-баг `minfin/fedbud_mes.go` исправлен; сетевых вызовов в `init()` в репозитории не осталось).
7. Русские даты/месяцы — через `util.MonthsToNum`; форматы времени — константы рядом с импортёром.
8. Накопленные значения «с начала года» конвертировать в потоки разностями (паттерн `fedBudImport`).
9. Ошибки не проглатывать: парсинг чисел — с проверкой `err`; в `Import()` ошибка → `return count, err`.
10. Пакет `financial/` — legacy (database/sql, свой main): новый код туда не добавлять, новые корпоративные импортёры делать на clickhouse-go/v2 в новых пакетах. Парсеры Полюса уже вынесены в `polyus/` (см. §6.6); в `financial/` остаётся `databook_ugk` и мёртвый код металлургов.
11. Сборка-проверка: `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборка). Тесты есть в `bls/`, `bea/` и `polyus/` (фикстуры, без сети); для новых импортёров тесты парсера и HTTP-слоя обязательны (`httptest.Server` + подмена base URL-переменной пакета).
12. После изменения архитектуры — обновить этот файл.
