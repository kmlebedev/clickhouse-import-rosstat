# ARCHITECTURE.md — clickhouse-import-rosstat

> Контекстный документ для AI-ассистентов (Kimi Code и др.). Содержит всё необходимое для написания нового кода без полного чтения репозитория: паттерны, конвенции, схемы БД, целевую архитектуру. Обновлять при каждом изменении архитектуры.
> Последнее обновление: 2026-10-08. Базовый коммит: `c96fc1f`.

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
util/              — общие хелперы: HTTP-клиент (xls.go), шаблоны HdBase/ClickHouseImport, батч-импорт (db.go)
rosstat/           — Росстат: ipc_mes, ipc_weeks, vvp_kvartal, salaries_mes
cbr/               — ЦБ РФ: key_rate, currency_usd, m2, ruonia, metal_gold, households, avgproc_stav, ...
minfin/            — Минфин: fedbud_mes, fedbud_mesyats (исполнение федбюджета)
customs/           — ФТС: внешняя торговля по странам
fao/               — ФАО: индексы продовольственных цен
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

## 6. Целевая архитектура (дорожная карта 2026-Q4)

Контур: **Источники → ClickHouse (сырые + витрины) → MCP (read-only) → Kimi-агент → сценарный прогноз золота → DCF → NAV PLZL → model_runs**. Полная версия: [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md).

### 6.1 Новые импортёры (по приоритету)

| Импортёр | Пакет | Источник | Метод |
|---|---|---|---|
| `fred` | `fred/` | FRED CSV API: `https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFII10` (серии: DFII10, DGS10, FEDFUNDS, DTWEXBGS, CPIAUCSL, T5YIE) | Шаблон Б/В, CSV→`macro_series` |
| `eia` | `eia/` | EIA Weekly Petroleum Status (запасы дистиллятов, crack ULSD) | API EIA v2 (ключ в env `EIA_API_KEY`) → `macro_series` |
| `lbma_gold` | `gold/` | Спот XAU/USD, LBMA fix | stooq CSV / Yahoo (GC=F) → `gold_prices` |
| `moex_iss` | `moex/` | MOEX ISS REST (PLZL OHLCV, ОФЗ/RGBI): `https://iss.moex.com/iss/engines/stock/markets/shares/securities/PLZL/candles.json?from=...` | JSON → `stock_prices`, `ofz_curve` |
| `mmf_aum` | `funds/` | СЧА фондов ликвидности | парсинг → `mmf_aum` |
| `news_watch` | `news/` | RSS Интерфакс/РБК/IR Полюса | → `news_events` |

### 6.2 Новые таблицы (DDL — канонические, использовать как есть)

```sql
CREATE TABLE IF NOT EXISTS macro_series (
    source LowCardinality(String),   -- 'fred','eia','wgc','cme'
    series LowCardinality(String),   -- 'DFII10','distillate_stocks',...
    date Date,
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);

CREATE TABLE IF NOT EXISTS gold_prices (
    venue LowCardinality(String),    -- 'lbma_am','lbma_pm','spot','comex_front'
    date Date,
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
    wacc Float64,
    nav_per_share Float64,
    nav_bull Float64, nav_base Float64, nav_bear Float64,
    market_price Float64,
    upside_pct Float64,
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
```

### 6.3 Витрины (views) — semantic layer для агента

- `v_model_inputs` — одна строка с последними входами DCF (gold spot, USDRUB, key_rate, DFII10, crack, TCC/AISC guidance)
- `v_gold_dashboard` — дашборд верификации: crack, запасы дистиллятов, FedWatch, ETF-потоки + флаги порогов
- `v_forecast_accuracy` — скользящая точность прогнозов из `forecast_log`

### 6.4 DCF-модель Полюса (ключевые допущения)

```
Выручка_t = Production_t(koz) × GoldPrice_t(сценарно);  НДПИ = база + 10%×max(gold−1900,0)  ← надбавка с 2025
EBITDA_t ≈ Production × (GoldPrice − AISC_t);  AISC_t = AISC_base × эскалация(ИПЦ РФ)
FCF_t = EBITDA − налоги − capex (Сухой Лог активная фаза) ± ΔWC
NAV/акция = (Σ FCF/(1+WACC)^t + TV − NetDebt) / shares;  дивиденды приостановлены до 2030 → только FCF/NAV
Сценарии золота Q4-2026 (база из docs/): bull $4,600–5,000 (25%), base $4,000–4,600 (40%), bear $3,750–4,050 (35%)
Рыночный мост: целевая цена = NAV ± корректировки (див.спред к ОФЗ, переток из фондов ликвидности, индексные потоки, sentiment)
```

### 6.5 MCP-контур

Сервер: `mcp/clickhouse` (Docker, HTTP-транспорт, `CLICKHOUSE_MCP_AUTH_TOKEN`). Пользователь БД `kimi_reader`: **только SELECT, только витрины `v_*`**. Запись прогнозов — не через MCP, а отдельным ingest-скриптом. Все витрины документируются `COMMENT ON TABLE/COLUMN`.

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
11. Сборка-проверка: `go build ./... && go vet ./...` (тестов в репо пока нет).
12. После изменения архитектуры — обновить этот файл.
