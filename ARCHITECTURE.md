# ARCHITECTURE.md — clickhouse-import-rosstat

> Контекстный документ для AI-ассистентов (Kimi Code и др.). Содержит всё необходимое для написания нового кода без полного чтения репозитория: паттерны, конвенции, схемы БД, целевую архитектуру. Обновлять при каждом изменении архитектуры.
> Последнее обновление: 2026-10-09. Базовый коммит: `c96fc1f`.

---

## 1. Что это за проект

Go-конвейер (ETL) импорта российской макроэкономической статистики и финансовых данных в **ClickHouse** для дашбордов **Grafana** и аналитических моделей (сценарный прогноз золота → DCF → NAV акции PLZL). Монолитный репозиторий, один бинарник, конфигурация только через env.

- Репозиторий: `github.com/kmlebedev/clickhouse-import-rosstat`
- Go 1.26, сборка: `make build` → статический linux/amd64 бинарник (CGO_ENABLED=0, ldflags `-s -w`)
- Драйвер БД: `github.com/ClickHouse/clickhouse-go/v2` (native protocol)
- Ключевые библиотеки: `excelize/v2` (XLSX), `xlsReader` (legacy XLS), `colly/v2` + `goquery` (скрейпинг), `unipdf/v3` (PDF), `cenkalti/backoff/v5` (ретраи), `logrus` (логи, `log.Infof/Errorf/Fatal`)

## 2. Структура репозитория

```
main.go            — точка входа: подключение к CH, запуск реестра импортёров
chimport/stats.go  — интерфейс ImportStat + глобальный реестр Stats
util/              — общие хелперы: HTTP-клиент (xls.go), шаблоны HdBase/ClickHouseImport, батч-импорт (db.go), каталог рядов series_catalog/v_series_catalog (series_catalog.go)
sql/               — SQL для ручной настройки: пользователь MCP kimi_reader и гранты на витрины (mcp_kimi_reader.sql)
rosstat/           — Росстат: ipc_mes, ipc_weeks, vvp_kvartal, salaries_mes
cbr/               — ЦБ РФ: key_rate, currency_usd, m2, ruonia, metal_gold, households, avgproc_stav, ...
minfin/            — Минфин: fedbud_mes, fedbud_mesyats (исполнение федбюджета)
customs/           — ФТС: внешняя торговля по странам
fao/               — ФАО: индексы продовольственных цен
fred/              — FRED (CSV-серии US-макро: DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE) → macro_series
bls/               — BLS API v2 (CPI, безработица, NFP, зарплата, PPI, JOLTS) → macro_series (source = 'bls')
bea/               — BEA API (NIPA T20804: индексы PCE) → macro_series (source = 'bea')
gold/              — золото: MOEX GOLDFIXME (₽/г) ÷ курс ЦБ cbr_currency_usd → gold_prices (venue='moex_fix_usd')
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
| `CLICKHOUSE_URL` | DSN подключения (обязательная) |
| `CLICKHOUSE_IMPORT_STAT` | Фильтр импортёров через запятую (пусто = все) |
| `LOG_LEVEL` | logrus-уровень (debug/info/warn/error) |
| `CERT_FILES` | Пути к PEM-сертификатам через запятую |
| `TICKER` | Маршрутизация legacy-контура financial (MOEX/VTBR/CHMF/MAGN/NLMK/Exports/ALL) |
| `INVESTING_EMAIL`, `INVESTING_PASSWORD` | ⚠️ deprecated, переезжаем на MOEX ISS / stooq |
| `BLS_API_KEY` | Регистрационный ключ BLS (необязательно, для `bls`) |
| `BEA_API_KEY` | UserID BEA API (обязательно, для `bea`) |

## 6. Целевая архитектура (дорожная карта 2026-Q4)

Контур: **Источники → ClickHouse (сырые + витрины) → MCP (read-only) → Kimi-агент → сценарный прогноз золота → DCF → NAV PLZL → model_runs**. Полная версия: [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md).

### 6.1 Новые импортёры (по приоритету)

| Импортёр | Пакет | Источник | Метод |
|---|---|---|---|
| `fred` | `fred/` | FRED CSV API: `https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFII10` (серии: DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE) | Свой парсер `encoding/csv` (не `util.GetCSV`: разделитель `,`), `Name()` = `fred` (несколько серий, одна таблица), пропуски (пустое значение) пропускаются; UA не задаём — FRED за Imperva рвёт соединение с браузерным UA. Расписание: `dagu/fred.yaml`, ежедневно 11:41 (МСК) → `macro_series`; описания рядов → `series_catalog`; витрина `v_fred_macro` (с комментариями, грант `kimi_reader`) |
| `bls` | `bls/` | BLS Public Data API v2: `POST https://api.bls.gov/publicAPI/v2/timeseries/data/` (JSON: `seriesid`, `startyear`, `endyear`, опц. `registrationkey` из `BLS_API_KEY`). Серии: CUUR0000SA0, CUSR0000SA0 (CPI), LNS14000000 (безработица), CES0000000001 (NFP), CES0500000003 (средняя зарплата), WPSFD4 (PPI final demand), JTS000000000000000JOL (JOLTS, вакансии). Окна лет по ≤10 лет с 2006 года; все серии одним запросом. Статус ≠ `REQUEST_SUCCEEDED` — ошибка; `message` при успехе (каталог, «no data») — не ошибка; периоды M13 и не-месячные пропускаются. Тесты: `bls/bls_test.go`. Расписание: `dagu/bls.yaml`, ежедневно 12:23 (МСК) → `macro_series`; описания рядов → `series_catalog`; витрина `v_bls_macro` (с комментариями, грант `kimi_reader`) |
| `bea` | `bea/` | BEA API: `GET https://apps.bea.gov/api/data?method=GetData&datasetname=NIPA&TableName=T20804&Frequency=M` (ключ `BEA_API_KEY` → параметр `UserID`, без него импорт падает с ошибкой). Строки T20804: 1 — PCE_PI (headline), 25 — PCE_PI_CORE (excluding food and energy). Ключ маскируется в текстах ошибок (`url.Error` содержит URL). Тесты: `bea/bea_test.go`. Расписание: `dagu/bea.yaml`, ежедневно 12:37 (МСК) → `macro_series`; описания рядов → `series_catalog`; витрина `v_bea_pce` (с комментариями, грант `kimi_reader`) |
| `eia` | `eia/` | EIA Weekly Petroleum Status (запасы дистиллятов, crack ULSD) | API EIA v2 (ключ в env `EIA_API_KEY`) → `macro_series` |
| `lbma_gold` | `gold/` | ⚠️ Временно вместо LBMA: MOEX `GOLDFIXME` (борд FIXI, ₽/г, с 2024-08-05) × 31,1034768 ÷ курс `cbr_currency_usd`; производная цена, не LBMA. Курс берётся последний известный не позже даты (ЦБ не публикует понедельники и новогодние праздники; окно 10 дней). Расписание: `dagu/gold.yaml`, ежедневно 18:47 (МСК); первым шагом DAG выполняется `cbr_currency_usd`. LBMA не реализован: prices.lbma.org.uk и Nasdaq Data Link `LBMA/GOLD` отвечают 403 WAF (датасетный эндпоинт блокируется с этого IP даже с валидным ключом), stooq — JS-проверкой, FRED серии LBMA удалил (404), Yahoo — 429 | → `gold_prices` |
| `moex_iss` | `moex/` | MOEX ISS REST (PLZL OHLCV, ОФЗ/RGBI): `https://iss.moex.com/iss/engines/stock/markets/shares/securities/PLZL/candles.json?from=...` | JSON → `stock_prices`, `ofz_curve` |
| `mmf_aum` | `funds/` | СЧА фондов ликвидности | парсинг → `mmf_aum` |
| `news_watch` | `news/` | RSS Интерфакс/РБК/IR Полюса | → `news_events` |

### 6.2 Новые таблицы (DDL — канонические, использовать как есть)

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

- `v_model_inputs` — одна строка с последними входами DCF (gold spot, USDRUB, key_rate, DFII10, crack, TCC/AISC guidance)
- `v_gold_dashboard` — дашборд верификации: crack, запасы дистиллятов, FedWatch, ETF-потоки + флаги порогов
- `v_forecast_accuracy` — скользящая точность прогнозов из `forecast_log` (включая конкурентный ML-трек)
- `v_peers_comparison` — P/NAV, EV/oz, дисконт к лидеру по контурам `ru`/`global`; алерт-порог — дисконт PLZL за ±1σ исторической нормы
- `v_gold_attribution` — GRAM-разложение движения золота (экспансия / риск / альтернативная стоимость / импульс); ошибки прогноза атрибутируются к фактору
- `v_series_catalog` — `series_catalog FINAL`: название, единицы, частота, происхождение и описание каждого ряда `macro_series` (ведётся импортёрами через `util.UpsertSeriesCatalog`)
- `v_bea_pce` — `macro_series FINAL WHERE source = 'bea'`: индексы PCE из BEA (`PCE_PI`, `PCE_PI_CORE`), уровни 2017=100
- `v_fred_macro` — `macro_series FINAL WHERE source = 'fred'`: ряды FRED (DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE)
- `v_bls_macro` — `macro_series FINAL WHERE source = 'bls'`: ряды BLS (CPI NSA/SA, безработица, NFP, зарплата, PPI, JOLTS)

Правила витрин:
- создаются через `CREATE OR REPLACE VIEW ... DEFINER = default SQL SECURITY DEFINER AS ...` — определение может меняться, и агент читает сырые таблицы через definer, без прав на `macro_series`;
- комментарии ставятся через `ALTER TABLE v_x MODIFY COMMENT '...'` и `ALTER TABLE v_x COMMENT COLUMN col '...'`: синтаксис `COMMENT ON TABLE/COLUMN` в текущей версии ClickHouse (26.10) не поддерживается;
- `CREATE OR REPLACE` для витрин — отступление от правила «DDL всегда `IF NOT EXISTS`» (то правило относится к таблицам);
- для каждой новой витрины — `GRANT SELECT` пользователю `kimi_reader` в `sql/mcp_kimi_reader.sql`.

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

Инструменты сервера: `list_databases`, `list_tables` (читает `system.tables`/`system.columns`, видит только то, на что есть гранты; комментарии колонок попадают в `create_table_query`), `run_query`.

Пользователь БД `kimi_reader`: **только SELECT, только витрины `v_*`**, `readonly = 1`, `DEFAULT DATABASE default`. Сырые таблицы (`macro_series`, `gold_prices`, ...) агенту недоступны: проверено `497 ACCESS_DENIED`. Создание и гранты — `sql/mcp_kimi_reader.sql` (пароль подставляется вручную). Запись прогнозов — не через MCP, а отдельным ingest-скриптом.

Правило для новых источников и рядов (см. AGENTS.md, правило 11): каждый ряд описан в `series_catalog`, каждая витрина имеет комментарии таблицы и колонок и грант `kimi_reader`.

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

## 8. Правила для AI-ассистента (Kimi Code)

1. **Новый источник = новый файл** в доменном пакете + `init()`-регистрация. main.go не править (пакет уже импортирован) — если пакета нет, добавить blank-import.
2. Использовать шаблон Б (`util.HdBase`) для XLSX-источников; шаблон В для краулеров; шаблон А — только если нужна кастомная логика поиска ссылки.
3. Вставка только батчами (`PrepareBatch`/`Append`/`Send`). DDL `IF NOT EXISTS`. Значения новых таблиц — Float64.
4. HTTP — только через `util.HttpClient` / `util.GetXlsx` / `util.GetCSV` (там нац. сертификаты и UA).
5. **Не хардкодить секреты** (в т.ч. в комментариях и curl-примерах) — только `os.Getenv`.
6. **Не делать сетевых вызовов в `init()`** — URL вычислять внутри `Import()` (legacy-баг в `minfin/fedbud_mes.go`, не повторять).
7. Русские даты/месяцы — через `util.MonthsToNum`; форматы времени — константы рядом с импортёром.
8. Накопленные значения «с начала года» конвертировать в потоки разностями (паттерн `fedBudImport`).
9. Ошибки не проглатывать: парсинг чисел — с проверкой `err`; в `Import()` ошибка → `return count, err`.
10. Пакет `financial/` — legacy (database/sql, свой main): новый код туда не добавлять, новые корпоративные импортёры делать на clickhouse-go/v2 в новых пакетах.
11. Сборка-проверка: `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборка). Тесты есть в `bls/` и `bea/`; для новых импортёров тесты парсера и HTTP-слоя обязательны (`httptest.Server` + подмена base URL-переменной пакета).
12. После изменения архитектуры — обновить этот файл.
