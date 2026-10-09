# ARCHITECTURE.md — clickhouse-import-rosstat

> Контекстный документ для AI-ассистентов (Kimi Code и др.). Содержит всё необходимое для написания нового кода без полного чтения репозитория: паттерны, конвенции, схемы БД, целевую архитектуру. Обновлять при каждом изменении архитектуры.
> Последнее обновление: 2026-10-10. Базовый коммит: `4d5923f` (разделы сверены с кодом на этом коммите).

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
util/              — общие хелперы: HTTP-клиент (xls.go), шаблоны HdBase/ClickHouseImport, батч-импорт (db.go), каталог рядов series_catalog/v_series_catalog (series_catalog.go), создание витрин v_* с комментариями (views.go)
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
views/             — витрины контура прогноза v_model_inputs, v_gold_dashboard, v_forecast_accuracy; импортёр gold_views (util.CreateView)
ingest/            — ingest-endpoint контура прогноза: POST /v1/model_run, POST /v1/manual_series (Bearer INGEST_TOKEN); пишет model_runs, forecast_log, macro_series (source='manual')
cmd/ingest/        — main-пакет бинарника ingest (make build-ingest / make run-ingest)
bank/              — банки: sber_csi(+week), sber/vtb/tbank_fin_rez, domrf_mortgage
craw/              — многостраничные краулеры: gost (сертификаты Росстандарта)
financial/         — ⚠️ legacy-контур: корпоративные databook'и (CHMF/MAGN/NLMK/PLZL/ЮГК),
                     investing.com exporter, РЖД. Свой main.go, своё подключение database/sql
financial/data/    — локальные XLSX/XLS источники (databook'и компаний)
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

### Шаблон Б — `util.HdBase` (декларативный, предпочтительный для новых XLSX-источников)

```go
func init() {
    s := util.HdBase{
        TableName:   "minfin_fed_bud_mes",
        DataUrl:     "...",                    // или getDataUrl() вычисляется в Import()
        CreateTable: `CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
            , date Date
            , value Float32
        ) ENGINE = ReplacingMergeTree ORDER BY (name, date);`,
        ImportFunc:  myImportFunc,             // func(xlsx *excelize.File, batch driver.Batch) error
    }
    chimport.Stats = append(chimport.Stats, &s)
}
```

### Шаблон В — `util.ClickHouseImport` (краулер или XLSX, с несколькими DDL)

Эталон: `craw/gost.go`. Поля: `TableName`, `CreateTable []string`, `DataUrl`, `CrawFunc func(url string, conn driver.Conn) error` ИЛИ `ImportFunc`. Краулер — colly с постраничной навигацией + `backoff.Retry`.

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

## 6. Целевая архитектура (дорожная карта 2026-Q4)

Контур: **Источники → ClickHouse (сырые + витрины) → MCP (read-only) → Kimi-агент → сценарный прогноз золота → DCF → NAV PLZL → model_runs**. Полная версия: [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md). Спека контура прогноза и плагина: [docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md](docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md).

Статусы в §6.1–§6.3: **реализован** — есть в коде репозитория; **не реализован** — только в плане, в коде нет.

### 6.1 Новые импортёры (по приоритету)

| Импортёр | Пакет | Статус | Источник | Метод |
|---|---|---|---|---|
| `fred` | `fred/` | реализован; `Name()` = `fred`. Исторический базлайн; первичный источник мировых рядов для прогнозной сессии — datasource (см. gold-nav) | FRED CSV API: `https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFII10` (серии: DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE) | Свой парсер `encoding/csv` (не `util.GetCSV`: разделитель `,`), `Name()` = `fred` (несколько серий, одна таблица), пропуски (пустое значение) пропускаются; UA не задаём — FRED за Imperva рвёт соединение с браузерным UA. Расписание: `dagu/fred.yaml`, ежедневно 11:41 (МСК) → `macro_series`; описания рядов → `series_catalog`; витрина `v_fred_macro` (с комментариями, грант `kimi_reader`) |
| `bls` | `bls/` | реализован; `Name()` = `bls` | BLS Public Data API v2: `POST https://api.bls.gov/publicAPI/v2/timeseries/data/` (JSON: `seriesid`, `startyear`, `endyear`, опц. `registrationkey` из `BLS_API_KEY`). Серии: CUUR0000SA0, CUSR0000SA0 (CPI), LNS14000000 (безработица), CES0000000001 (NFP), CES0500000003 (средняя зарплата), WPSFD4 (PPI final demand), JTS000000000000000JOL (JOLTS, вакансии). | Окна лет по ≤10 лет с 2006 года; все серии одним запросом. Статус ≠ `REQUEST_SUCCEEDED` — ошибка; `message` при успехе (каталог, «no data») — не ошибка; периоды M13 и не-месячные пропускаются. Тесты: `bls/bls_test.go`. Расписание: `dagu/bls.yaml`, ежедневно 12:23 (МСК) → `macro_series`; описания рядов → `series_catalog`; витрина `v_bls_macro` (с комментариями, грант `kimi_reader`) |
| `bea` | `bea/` | реализован; `Name()` = `bea` | BEA API: `GET https://apps.bea.gov/api/data?method=GetData&datasetname=NIPA&TableName=T20804&Frequency=M` (ключ `BEA_API_KEY` → параметр `UserID`, без него импорт падает с ошибкой). Строки T20804: 1 — PCE_PI (headline), 25 — PCE_PI_CORE (excluding food and energy). | Ключ маскируется в текстах ошибок (`url.Error` содержит URL). Тесты: `bea/bea_test.go`. Расписание: `dagu/bea.yaml`, ежедневно 12:37 (МСК) → `macro_series`; описания рядов → `series_catalog`; витрина `v_bea_pce` (с комментариями, грант `kimi_reader`) |
| `eia` | `eia/` | не реализован; вычеркнут из Go-плана (crack — `HO=F` в сессии, запасы дистиллятов — `manual_series`), спека gold-nav §4 | EIA Weekly Petroleum Status (запасы дистиллятов, crack ULSD) | API EIA v2 (ключ в env `EIA_API_KEY`) → `macro_series` |
| `lbma_gold` | `gold/` | не реализован (LBMA недоступен с этой сети); вместо него реализован `gold`, `Name()` = `gold_prices` | ⚠️ Временно вместо LBMA: MOEX `GOLDFIXME` (борд FIXI, ₽/г, с 2024-08-05) × 31,1034768 ÷ курс `cbr_currency_usd`; производная цена, не LBMA. Курс берётся последний известный не позже даты (ЦБ не публикует понедельники и новогодние праздники; окно 10 дней). Расписание: `dagu/gold.yaml`, ежедневно 18:47 (МСК); первым шагом DAG выполняется `cbr_currency_usd`. LBMA не реализован: prices.lbma.org.uk и Nasdaq Data Link `LBMA/GOLD` отвечают 403 WAF (датасетный эндпоинт блокируется с этого IP даже с валидным ключом), stooq — JS-проверкой, FRED серии LBMA удалил (404), Yahoo — 429 | → `gold_prices` |
| `moex_iss` | `moex/` | реализован; `Name()` = `stock_prices` | MOEX ISS REST (анонимный): дневные свечи PLZL `/iss/engines/stock/markets/shares/securities/PLZL/candles.json?interval=24` (пагинация `start` шагом 100 при явном `limit=100` — без limit страницы по 500 и дубли). | Инкремент от max(date) по `code='PLZL'`; пустая таблица → с 2010-01-01. Пишет в существующую legacy-таблицу `stock_prices` (схема Float32 не меняется). Тесты: `moex/moex_test.go`. Расписание: `dagu/moex.yaml`, ежедневно 19:13 (МСК) → `stock_prices`; series_catalog (source='moex'); витрина `v_stock_prices` (с комментариями, грант `kimi_reader`) |
| `ofz_curve` | `moex/` | реализован; `Name()` = `ofz_curve` | MOEX ISS: индекс RGBI (свечи `engines/stock/markets/index/securities/RGBI/candles.json`, полная история с 2010-01-01, тенор `RGBI` — уровень индекса, не доходность) + годовые доходности G-curve `/iss/engines/stock/zcyc.json`, блок `yearyields` (period 1/3/5/10 → теноры `1y`/`3y`/`5y`/`10y`, % годовых). ⚠️ zcyc — только снимок текущего дня: история доходностей накапливается с первого запуска. | Инкремент от max(date); даты из БД нормализуются в UTC (clickhouse-go отдаёт Date с таймзоной сессии). Расписание: `dagu/moex.yaml`, ежедневно 19:13 (МСК) → `ofz_curve`; series_catalog (source='moex'); витрина `v_ofz_curve` (с комментариями, грант `kimi_reader`) |
| `mmf_aum` | `funds/` | не реализован | СЧА фондов ликвидности | парсинг → `mmf_aum` |
| `news_watch` | `news/` | не реализован | RSS Интерфакс/РБК/IR Полюса | → `news_events` |

`Name()` импортёров — то, что передаётся в `CLICKHOUSE_IMPORT_STAT`: `fred`, `bls`, `bea`, `gold_prices`, `stock_prices`, `ofz_curve`, `events_calendar`, `gold_views`. Пакет `gold/` выполняет `Name()` = `gold_prices`, поэтому в README и командах используется имя таблицы.

Ряды WGC (ETF-потоки), CME FedWatch и запасы дистиллятов EIA импортёров не имеют. В контуре прогноза они вводятся вручную через `POST /v1/manual_series` (`source = 'manual'`) и читаются витриной `v_gold_dashboard` (§6.3).

### 6.2 Новые таблицы (DDL — канонические, использовать как есть)

Реализованы в коде (DDL в блоке ниже совпадает с кодом): `macro_series`, `model_runs`, `forecast_log` (создаются `ingest.EnsureTables` при старте `cmd/ingest`), `events_calendar` (`calendar/`, сид `sql/events_calendar_q4_2026.sql`), `series_catalog` (`util/`), `gold_prices` (`gold/`), `ofz_curve` (`moex/`). Таблица `stock_prices` (legacy Float32) создаётся в `moex/moex_iss.go` и в этот блок не входит.

Не реализованы (в коде нет): `gold_forecasts`, `news_events`, `index_weights`, `dividend_events`, `mmf_aum`, `tax_events`, `reserves_assets`, `reserves_dynamics`, `license_events`, `peers_nav`, `mine_plans`, `nav_by_asset`, `price_decks`, `regime_states`.

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

CREATE TABLE IF NOT EXISTS nav_by_asset (
    run_id UUID,                       -- связка с model_runs
    asset LowCardinality(String),
    npv_usd_mln Float64,
    discount_rate Float64,             -- 0.05 real USD база + страновая/стадийная надбавка
    stage_haircut Nullable(Float64)    -- construction 0.7–0.9, DFS 0.5–0.7, PEA 0.2–0.4
) ENGINE = ReplacingMergeTree ORDER BY (run_id, asset);

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
- `v_gold_dashboard` — **реализован** (`views/gold_dashboard.go`): одна строка на индикатор `crack_ulsd_proxy`, `distillate_stocks`, `fedwatch_dec_hike`, `etf_flows_month`, `dxy`; значения из `macro_series` (`source IN ('manual','fred')`). `flag = 1` — порог пробит (crack >50, FedWatch >80, DXY >102); `stale = 1` — значения нет или последнее наблюдение старше 14 дней. Импортёров для этих индикаторов нет: значения только ручные, поэтому без ввода через `manual_series` они остаются `stale`
- `v_forecast_accuracy` — **реализован** (`views/forecast_accuracy.go`): строка на прогноз из `forecast_log` (`metric`, `forecast_date`, `target_date`, `error_pct`, `is_resolved`). Отдельного конкурентного ML-трека в витрине нет (различается только `metric`). `actual` и `error_pct` в коде не заполняются: сверка с фактом после `target_date` не автоматизирована
- `v_peers_comparison` — **не реализован** (в коде нет). План: P/NAV, EV/oz, дисконт к лидеру по контурам `ru`/`global`; алерт-порог — дисконт PLZL за ±1σ исторической нормы
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

### 6.5 MCP-контур

Сервер: официальный [ClickHouse/mcp-clickhouse](https://github.com/ClickHouse/mcp-clickhouse) (PyPI `mcp-clickhouse`), stdio-транспорт. Локально запускается через `uv` (Docker не требуется):

```
uv run --with mcp-clickhouse --python 3.12 mcp-clickhouse
```

Конфигурация Kimi — пользовательский `~/.kimi-code/mcp.json` (права 600), блок `mcpServers.clickhouse`: `command: bash`, `args`: `-c` с подгрузкой `~/.config/rosstat/env` и затем `exec uv run --with mcp-clickhouse --python 3.12 mcp-clickhouse`; `env`: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT=8123`, `CLICKHOUSE_SECURE=false`, `CLICKHOUSE_USER=kimi_reader`, `CLICKHOUSE_DATABASE=default`, `CLICKHOUSE_MCP_SERVER_TRANSPORT=stdio`. Пароль `CLICKHOUSE_PASSWORD` приходит из файла окружения и в `mcp.json` не хранится.

Режим HTTP (для плагина gold-nav): плагин подключает mcp-clickhouse по URL (`mcpServers.clickhouse.url`, Bearer-токен на чтение `GOLD_NAV_MCP_TOKEN`). **Развёртывание HTTP-сервера на стороне автора в репозитории не описано и не проверено: конфигурации reverse proxy, TLS и выдачи токенов в коде нет — не реализовано.** Локальная конфигурация Kimi в этом разделе остаётся stdio.

Инструменты сервера: `list_databases`, `list_tables` (читает `system.tables`/`system.columns`, видит только то, на что есть гранты; комментарии колонок попадают в `create_table_query`), `run_query`.

Пользователь БД `kimi_reader`: **только SELECT, только витрины `v_*`**, `readonly = 1`, `DEFAULT DATABASE default`. Сырые таблицы (`macro_series`, `gold_prices`, ...) агенту недоступны: проверено `497 ACCESS_DENIED`. Создание и гранты — `sql/mcp_kimi_reader.sql` (пароль подставляется вручную). Запись прогнозов — не через MCP, а через ingest-endpoint (`cmd/ingest`, см. ниже).

Правило для новых источников и рядов (см. AGENTS.md, правило 11): каждый ряд описан в `series_catalog`, каждая витрина имеет комментарии таблицы и колонок и грант `kimi_reader`.

Отдельно от data-контура: опциональный dev-MCP `jetbrains` — встроенный MCP-сервер GoLand (2025.2+, HTTP-stream `http://127.0.0.1:<динамический порт>/stream`, запись в `~/.kimi-code/mcp.json`). Даёт агенту инструменты IDE для работы с кодом (`get_file_problems`, `get_symbol_info`, `search_symbol`, `analyze_calls`, `rename_refactoring`); правила использования и запреты — в AGENTS.md, раздел «Инструменты GoLand (MCP `jetbrains`)», настройка — в README, «MCP GoLand (опционально)». К данным ClickHouse отношения не имеет.

#### Ingest-endpoint (пакет `ingest/`, бинарник `cmd/ingest`)

**Реализован.** Единственный путь записи в `model_runs`, `forecast_log` и `macro_series` со стороны контура прогноза.

- Сборка и запуск: `make build-ingest` → `build/ingest`; `make run-ingest`. Нужны `CLICKHOUSE_URL` и `INGEST_TOKEN` (см. §5); `INGEST_ADDR` по умолчанию `:8081`.
- Старт: ping ClickHouse, затем `EnsureTables` — идемпотентное создание `model_runs`, `forecast_log`, `macro_series` по DDL §6.2 (копии в `ingest/schema.go`).
- Авторизация: заголовок `Authorization: Bearer <INGEST_TOKEN>`, сравнение через `subtle.ConstantTimeCompare`. Без заголовка или с неверным токеном — `401`.
- `POST /v1/model_run` — JSON `ModelRun`: клиентский `run_id` (UUID, обязателен), `trigger_type` ∈ {`calendar`,`news`,`manual`}, `price_deck` ∈ {`spot_flat`,`consensus_lt`,`own_scenario`}, `probabilities` (сумма 100 ±0.1), `gold_scenario` (с `horizon`: `Q<n>-YYYY`, `YE-YYYY`, `YYYY` или `YYYY-MM-DD`; блоки сценариев с `point` или `low`/`high`), `nav_per_share`, `nav_bull/base/bear`, `market_price`, `upside_pct`, `discount_rate`, `wacc`, `usdrub_path`, `comment`. Ответ `200 {"inserted": 1}`; `400` — ошибка валидации или JSON.
- Запись: сначала `forecast_log` (две строки: `metric = 'nav'`, `predicted = nav_per_share`; `metric = 'xau_q_avg'`, `predicted` = Σ p×point / Σ p; `forecast_date` — дата сервера, `target_date` — конец горизонта, `actual` и `error_pct` = NULL), последней — `model_runs` (`run_date` = время сервера, `nav_beta_gold` = NULL, поля в входе нет). Строка в `model_runs` — маркер фиксации прогона.
- Идемпотентность: повторный POST с тем же `run_id` ничего не пишет (проверка `count() ... FINAL` + мьютекс, один инстанс). `ReplacingMergeTree` по `(run_date, run_id)` дубли сам не схлопывает. Ответ при дубле такой же, как при записи: `200 {"inserted": 1}`.
- `POST /v1/manual_series` — JSON-массив `{"series", "date" (YYYY-MM-DD), "value"}`; пишется в `macro_series` с `source = 'manual'`. Ответ `200 {"inserted": N}`; пустой массив — `N = 0`; `400` — ошибка валидации любой точки (запись не выполняется).
- Ошибка записи в ClickHouse — `500 insert failed`, детали в логе.
- Не реализовано: ограничение частоты запросов, ограничение размера тела, TLS в самом процессе. Предполагается reverse proxy с TLS (в репозитории не описан).

#### Плагин gold-nav (отдельный репозиторий)

Плагин Kimi Code / Kimi Work лежит вне этого репозитория, sibling-каталогом `../gold-nav` (публикуется отдельно на GitHub; ссылка — после публикации). Состав: `kimi.plugin.json` (`mcpServers.clickhouse` по URL), skills `gold-forecast-session` и `dcf-methodology`, команды `/gold-nav:session` и `/gold-nav:verify`.

- Читает витрины `v_model_inputs`, `v_gold_dashboard`, `v_events_calendar`, `v_forecast_accuracy` через MCP (только `v_*`, пользователь `kimi_reader`).
- Пишет run через `POST /v1/model_run` и ручные ряды через `POST /v1/manual_series` по адресу `GOLD_NAV_INGEST_URL` с токеном `GOLD_NAV_INGEST_TOKEN`. Без токена сессия работает в режиме «только расчёт» (файл `run.json`).
- Переменные окружения плагина (имена): `GOLD_NAV_MCP_TOKEN` (выдаёт автор, read-only), `GOLD_NAV_INGEST_URL` и `GOLD_NAV_INGEST_TOKEN` (только у автора). Значения в репозиторий не кладутся.
- Не проверено: установка и живой сценарий в Kimi Code и Kimi Work, подстановка `${GOLD_NAV_MCP_TOKEN}` в манифесте (раздел «Результат проверки» в README плагина пока не заполнен). Спека: [docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md](docs/superpowers/specs/2026-10-10-gold-nav-plugin-design.md).

## 7. Календарь триггеров (актуальный Q4-2026)

| Дата | Событие | Действие модели |
|---|---|---|
| ср еженед. | EIA дистилляты; чт — недельный ИПЦ | пороги: crack >$50 медведь / <$30 снято |
| 10-е ежемес. | WGC ETF-потоки + ЦБ | норма покупок ЦБ 40–60 т/мес |
| 23.10.2026 | СД ЦБ РФ (ставка 14%) | снижение → флаг перетока ликвидности |
| 01.11.2026 | Истечение запрета РФ на экспорт дизеля | продление → bear; отмена → bull |
| 10.11.2026 | CPI США за октябрь | горячий → FOMC-hike 85–90% |
| 09.12.2026 | FOMC + dot plot | 2-е повышение → пробой $4,000 → $3,750–3,800 |
| 18.12.2026 | СД ЦБ РФ + ребалансировка MOEX | база: −25 б.п. до 13,75% |

Та же информация хранится в таблице `events_calendar` (сид `sql/events_calendar_q4_2026.sql`, витрина `v_events_calendar`); агент читает календарь из витрины. Таблица выше — краткая сводка для людей: даты и пороги в сиде точнее, а WGC в сиде — одна дата 2026-10-30 (GDT), а не «10-е число ежемесячно». При расхождении верна таблица `events_calendar`, а эту сводку нужно обновить.

## 8. Правила для AI-ассистента (Kimi Code)

1. **Новый источник = новый файл** в доменном пакете + `init()`-регистрация. main.go не править (пакет уже импортирован) — если пакета нет, добавить blank-import.
2. Использовать шаблон Б (`util.HdBase`) для XLSX-источников; шаблон В для краулеров; шаблон А — только если нужна кастомная логика поиска ссылки.
3. Вставка только батчами (`PrepareBatch`/`Append`/`Send`). DDL `IF NOT EXISTS`. Значения новых таблиц — Float64.
4. HTTP — только через `util.HttpClient` / `util.GetXlsx` / `util.GetCSV` (там нац. сертификаты и UA).
5. **Не хардкодить секреты** (в т.ч. в комментариях и curl-примерах) — только `os.Getenv`.
6. **Не делать сетевых вызовов в `init()`** — URL вычислять внутри `Import()` (бывший legacy-баг `minfin/fedbud_mes.go` исправлен; сетевых вызовов в `init()` в репозитории не осталось).
7. Русские даты/месяцы — через `util.MonthsToNum`; форматы времени — константы рядом с импортёром.
8. Накопленные значения «с начала года» конвертировать в потоки разностями (паттерн `fedBudImport`).
9. Ошибки не проглатывать: парсинг чисел — с проверкой `err`; в `Import()` ошибка → `return count, err`.
10. Пакет `financial/` — legacy (database/sql, свой main): новый код туда не добавлять, новые корпоративные импортёры делать на clickhouse-go/v2 в новых пакетах.
11. Сборка-проверка: `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборка). Тесты есть в `bls/` и `bea/`; для новых импортёров тесты парсера и HTTP-слоя обязательны (`httptest.Server` + подмена base URL-переменной пакета).
12. После изменения архитектуры — обновить этот файл.
