# Дашборд «Сделки MOEX — нетто покупок и продаж»: миграция `Trades` из transaq-clickhouse-exporter — design

**Дата:** 2026-10-31. **Ветка:** `feat/grafana-transaq-trades` (worktree `.worktrees/transaq-trades-dashboard`, от `origin/main` `e992c8f`).
**Пункт:** `ofz-market/1.6` ([issue #459](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/459)), зависит от merged `ofz-market/1.5` (PR [#418](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/418)).
**Приёмка (кратко):** дашборд `transaq-trades` собран кодом (5 панелей в 2 рядах) в артефакт `dashboard/transaq-trades.json`; каждый запрос читает только витрины `v_transaq_net_flows*`, `$__dateFilter`/`$__timeFilter` в каждом таргете, `LIMIT` только на top-10; `make all`, `make grafana-validate`, `make docs-check`, `gcx dev lint run` и CI `dashboards` зелёные; живой SQL всех таргетов через MCP под `kimi_reader`; staging deploy + smoke — первые панели отдают точки; протокол — комментарием в PR. Детали — §8.

## 1. Заказ и место в роадмапах

**Заказ владельца (2026-10-31):** мигрировать UI-дашборд `Trades` из sibling-проекта `transaq-clickhouse-exporter` (`etc/grafana-dashboard-trades.json`) в текущий репозиторий по стандарту дашбордов-как-код; **дополнить скил `dashboard-migration` лучшими практиками работы с time-series данными ClickHouse** (статья ClickHouse + свежие возможности) и применить их к панелям миграции.

**Владелец роадмапа — `ofz-market`:** контур Transaq (лента сделок, витрины `v_transaq_net_flows*`) заведён и ведётся в [ROADMAP_OFZ_MARKET.md](../../ROADMAP_OFZ_MARKET.md) §5; пункт 1.6 — дашборд на витринах 1.5, данных не добавляет. `ROADMAP_INDEX.md` §1: схема item-id `ofz-market` — `1.1`–`1.6`.

**Трекер:** issue #459 с label `roadmap:ofz-market` + `role:go`; Project V2 — приватный `Treasury Autopilot — Roadmap` (number 12), поля: Track `data`, Phase `1 Data`, Priority `P1`, Item `1.6`, Depends on `ofz-market/1.5`, Spec/Plan — пути этого документа и плана.

## 2. Инвентаризация источника

Файл: `/Users/whitefox/GolandProjects/transaq-clickhouse-exporter/etc/grafana-dashboard-trades.json` (123 КБ, uid `ee5qeifx0qubke`, schemaVersion 39, `id=3`, `version=45`).
Состав: 10 элементов — 6 панелей и 4 ряда, из которых 3 ряда пустые.

| id | Тип | Заголовок | Таргеты (SQL — суть) |
|---|---|---|---|
| 14 | row | Итоги дня | — |
| 12 | bargauge | Нетто покупки в IMOEX | 2 таргета: top-10 `sum(price*quantity*lotsize)`, `board='TQBR'`, `net>0` (второй — вариант без `HAVING`, сортировка `desc`) |
| 13 | bargauge | Нетто продажи в IMOEX | top-10 `sum(...)*-1`, `board='TQBR'`, знак снимается для продаж |
| 9 | row | Volume anomalies | пусто |
| 8 | row | Volume by sec | пусто |
| 7 | row | Volume Indexes | пусто |
| 11 | timeseries | Volume trades IMOEX diff with buy and sell by interval $interval в минутах | 2 таргета: нетто по `toStartOfInterval(time, INTERVAL $interval MINUTE)`; второй — то же с `timestamp_add(day, 1, time)` (наложение прошлого дня) |
| 4 | timeseries | Volume trades IMOEX by interval $interval в минутах | 2 таргета: покупки `+sum(price*quantity)`, продажи `-sum(price*quantity)` |
| 6 | timeseries | Volume trades ETF by interval $interval в минутах | 2 таргета: то же для `sec_code in [$sec_code_etf]` |
| 5 | timeseries | Volume trades IMOEX | 2 таргета: per-trade `time`, покупки/продажи, `sum(price*quantity)` |

**Переменные.** `sec_code` — query по `transaq_trades` (multi, в SQL не используется); `interval` — custom `1,3,5,10,12,15,20,24,30,48,60,120,240,480`, default `1`; `sec_code_etf` — query по join `transaq_trades` × `transaq_securities` с `board='TQTF'` (multi + All).

**Источники и анти-паттерны.**
- Все таргеты читают **сырые** `default.transaq_trades`/`default.transaq_securities` и чужой датасорс `bdx7lxrdelxq8b` — на стенде проекта у `kimi_reader` таких грантов нет; `Validate` требует `FROM v_`.
- `LIMIT 10000` на timeseries (панели 4 и 6) — обрезка ряда вместо агрегации.
- `price * quantity` без `lotsize` — это цена × **лоты**, а не рубли; денежный объём требует `lotsize` из каталога (`transaq/views.go`).
- Дубли таргетов в панелях 12/13 (два почти одинаковых запроса с разным join-ключом) — шум конструктора.
- Переменная `$interval` в `INTERVAL ${interval} MINUTE` — ручной шаг вместо Grafana-макроса; `$sec_code_etf` — fragile-список кодов вместо `board`.
- Жёсткий диапазон `2024-12-06 06:57…16:00` — дашборд не живёт; пустые ряды; `<datasource>` в шаблонах переменных.

## 3. Живые факты: витрины и гранты

Витрины уже существуют и наполняются импортёром `transaq_net_flows` (пункт 1.5, merged):

| Витрина | Колонки |
|---|---|
| `v_transaq_net_flows` (минутная) | `minute DateTime('Europe/Moscow')`, `sec_code`, `board`, `buy_lots`, `sell_lots`, `net_lots`, `buy_value_rub`, `sell_value_rub`, `net_value_rub`, `trade_count`, `gap_suspected`, `loaded_at` |
| `v_transaq_net_flows_daily` (дневная) | `date`, `sec_code`, `board`, `buy_lots`, `sell_lots`, `net_lots`, `buy_value_rub`, `sell_value_rub`, `net_value_rub`, `trade_count`, `minutes_with_trades`, `gap_suspected`, `first_trade`, `last_trade` |

- Ключ таблицы `transaq_net_flows` — `(sec_code, board, minute)`, `ReplacingMergeTree(loaded_at)`; витрины читают `FINAL`.
- `buy_value_rub`/`sell_value_rub` — **оценка** `Σ price × лоты × lotsize`; сделки без `lotsize` в сумму не входят (комментарии колонок в `transaq/views.go`).
- `gap_suspected` — эвристика разрыва ленты (>10 минут внутри 10:00–19:00 МСК); разрыв не догоняется, нули не подставляются.
- Гранты `kimi_reader` на обе витрины — `sql/mcp_kimi_reader.sql:133-134`; сырые `transaq_*` и `transaq_net_flows` агенту не выданы. Новые гранты не нужны (шаг 4 скила — проверен).
- Локальный dev-ClickHouse: сырые `transaq_*` есть (каталоги от 24.10), таблицы `transaq_net_flows` и витрин нет — импортёр на локальной ленте ещё не прогонялся; живая приёмка 1.5 (7 362 сделки, 263 бакета) — staging. Локально витрины публикуются прогоном `make import STAT=transaq_net_flows` (на пустой ленте — no-op + `CREATE VIEW`).

## 4. Гэп-анализ «панель → витрина»

| Источник | Витрина | Ключи/фильтры | Вердикт |
|---|---|---|---|
| 12 | `v_transaq_net_flows_daily` | `board='TQBR'`, `sum(net_value_rub)>0`, top-10 | ✅ прямой маппинг (рубли вместо цена×лоты) |
| 13 | `v_transaq_net_flows_daily` | `board='TQBR'`, `sum(net_value_rub)<0`, знак снимается | ✅ прямой маппинг |
| 11 | `v_transaq_net_flows` | `board='TQBR'`, `$__timeInterval(minute)` | ✅; наложение «+1 день» — нет |
| 4 | `v_transaq_net_flows` | `board='TQBR'`, buy/sell | ✅ один SQL с двумя агрегатами |
| 6 | `v_transaq_net_flows` | `board='TQOB'` | ⚠️ ETF → ОФЗ: подписка стенда — IMOEX + ОФЗ, `TQTF` в ленте нет |
| 5 | — | — | ❌ per-trade: витрина минутная, дубль панели 4 при 1m |

Недостающих слоёв нет: все панели v1 закрываются существующими витринами. Новые витрины/таблицы/ряды не заводятся.

## 5. SQL-контракт (дословно)

Все запросы — `CHQuery` плагина `grafana-clickhouse-datasource` 4.3.2, датасорс `ch-rosstat` (`DatasourceType`/`DatasourceUID`), формат панелей 1–2 — `1`/`table`, 3–5 — `0`/`timeseries`.

### Панель 1 — «Нетто покупки в IMOEX, ₽» (bargauge)

```sql
SELECT
    sec_code AS "Инструмент",
    sum(net_value_rub) AS "Нетто покупок, ₽"
FROM v_transaq_net_flows_daily
WHERE board = 'TQBR'
  AND $__dateFilter(date)
GROUP BY sec_code
HAVING sum(net_value_rub) > 0
ORDER BY "Нетто покупок, ₽" DESC
LIMIT 10
```

### Панель 2 — «Нетто продажи в IMOEX, ₽» (bargauge)

```sql
SELECT
    sec_code AS "Инструмент",
    -sum(net_value_rub) AS "Нетто продаж, ₽"
FROM v_transaq_net_flows_daily
WHERE board = 'TQBR'
  AND $__dateFilter(date)
GROUP BY sec_code
HAVING sum(net_value_rub) < 0
ORDER BY "Нетто продаж, ₽" DESC
LIMIT 10
```

### Панель 3 — «Нетто-поток IMOEX по интервалам, ₽» (timeseries)

```sql
SELECT
    $__timeInterval(minute) AS time,
    sum(net_value_rub) AS "Нетто, ₽"
FROM v_transaq_net_flows
WHERE board = 'TQBR'
  AND $__timeFilter(minute)
GROUP BY time
ORDER BY time
```

### Панель 4 — «Покупки и продажи IMOEX по интервалам, ₽» (timeseries)

```sql
SELECT
    $__timeInterval(minute) AS time,
    sum(buy_value_rub) AS "Покупки, ₽",
    -sum(sell_value_rub) AS "Продажи, ₽"
FROM v_transaq_net_flows
WHERE board = 'TQBR'
  AND $__timeFilter(minute)
GROUP BY time
ORDER BY time
```

### Панель 5 — «Покупки и продажи ОФЗ (TQOB) по интервалам, ₽» (timeseries)

```sql
SELECT
    $__timeInterval(minute) AS time,
    sum(buy_value_rub) AS "Покупки, ₽",
    -sum(sell_value_rub) AS "Продажи, ₽"
FROM v_transaq_net_flows
WHERE board = 'TQOB'
  AND $__timeFilter(minute)
GROUP BY time
ORDER BY time
```

**Визуал.** Панели 1–2: `bargauge`, `horizontal`, `basic`, `valueMode=color`, `showUnfilled`, `reduceOptions.values=true`, единица `currencyRUB`, `decimals=0`. Панели 3–5: `timeseries`, palette-classic, легенда снизу (`table`, calcs `sum/mean/max`), tooltip `single`, `lineWidth=1`, `lineInterpolation=smooth`, `fillOpacity=0`, `gradientMode=none`, `spanNulls=false` (пропуск не соединяется), `currencyRUB`, `decimals=0`. У каждой панели — `description` на русском: рублёвая оценка через `lotsize` (не биржевой оборот), только фактические сделки ленты, разрывы не догоняются, «не сигнал и не рекомендация».

**Каркас.** Title «Сделки MOEX — нетто покупок и продаж», uid `transaq-trades`, tags `transaq`/`moex`/`ofz-market`, `refresh=5m`, `time=now-7d…now`, `timezone=browser`. Ряд 21 «Итоги периода» (панели 1–2, по 12 колонок), ряд 22 «Объёмы по интервалам» (панели 3–5, по 24).

## 6. Что осознанно не переносим

| Источник | Решение | Причина |
|---|---|---|
| Сырые `transaq_trades`/`transaq_securities` | → витрины `v_*` | у `kimi_reader` нет грантов на сырые таблицы; `Validate` требует `FROM v_` |
| `$interval` (custom) | `$__timeInterval(minute)` | адаптивный шаг Grafana; шаблонные переменные в контуре v1 не используются |
| `$sec_code` (query) | удалена | в SQL источника не участвует |
| `$sec_code_etf` (query) | `board='TQOB'` в панели 5 | список кодов хрупок; ETF (`TQTF`) в подписку стенда не входит |
| Панель 6 (ETF) | заменена панелью 5 (ОФЗ) | `TQTF` нет в ленте — панель была бы всегда пустой; ОФЗ в подписке есть |
| Панель 5 (per-trade) | не перенесена | витрина минутная; при 1m это дубль панели 4 |
| Наложение `timestamp_add(day, 1, …)` (панель 11) | не перенесено | при окне 7 дней сдвинутый ряд дублирует весь график; сравнение со вчера — сменой диапазона |
| Вторые таргеты панелей 12/13 | один SQL на панель | дубли почти одинаковых запросов |
| Пустые ряды 7/8/9 | не перенесены | панелей в них нет |
| `LIMIT 10000` на timeseries | нет `LIMIT` на графиках | обрезка ряда — анти-паттерн; агрегация в ClickHouse |
| `price * quantity` | `buy_value_rub`/`sell_value_rub` | `quantity` — лоты; рубли требуют `lotsize` |

## 7. Time-series практики ClickHouse: что применено и что нет

Нормативная памятка — скил [`dashboard-migration`](../../../.kimi-code/skills/dashboard-migration/SKILL.md), раздел «Time-series SQL: практики ClickHouse». Источники: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse), [`toStartOfInterval`](https://clickhouse.com/docs/sql-reference/functions/date-time-functions#toStartOfInterval), [`ORDER BY … WITH FILL`](https://clickhouse.com/docs/sql-reference/statements/select/order-by#fill), [`timeSeries*ToGrid`](https://clickhouse.com/docs/reference/functions/aggregate-functions/timeSeriesRateToGrid).

**Применено:**
1. **Фильтр по сырой колонке** — `$__dateFilter(date)` на Date-витрине (панели 1–2), `$__timeFilter(minute)` на DateTime-витрине (3–5), до агрегации; оборачивания колонки в функцию в `WHERE` нет.
2. **Зерно под задачу** — диапазонные top-10 читают дневную витрину (`v_transaq_net_flows_daily`), графики — минутную; сырые события в панели не сканируются.
3. **Адаптивные бакеты** — `$__timeInterval(minute)` (разворачивается в `toStartOfInterval`); календарные день/неделя в графиках не используются, поэтому UTC-выравнивание секундного шага не искажает календарный день (ловушка зафиксирована в скиле: `INTERVAL 86400 SECOND` на сервере `Asia/Yekaterinburg` даёт бакет `03:00 МСК`, `INTERVAL 1 DAY` — `00:00 МСК`).
4. **Пропуск ≠ ноль** — `WITH FILL` не применяется: отсутствие строки в витрине означает «нет сделок **или** разрыв ленты», и подстановка нуля соврала бы; `spanNulls=false`, оговорка в описании.
5. **Поток, не gauge** — `sum()` по бакету; `timeSeries*ToGrid` не нужны: витрина уже хранит минутные агрегаты, а не сырые счётчики.
6. **Агрегация в ClickHouse** — без `LIMIT` на графиках, `ORDER BY time`, разности/знаки в SQL.
7. **Проверка** — `EXPLAIN ESTIMATE` панели 3 на 7-дневном окне в приёмке.

**Не применено осознанно:** `WITH FILL`/`INTERPOLATE` (см. п. 4), `timeSeries*ToGrid` (см. п. 5), `ASOF JOIN` (нет второй серии для выравнивания), `ORDER BY`-ключ таблицы (схема 1.5 не меняется; кандидат `(board, minute, sec_code)` — только отдельным пунктом со спекой).

## 8. Реализация и приёмка

**Файлы.**
- `grafana/transaq_trades.go` — `const TransaqTradesUID = "transaq-trades"`, `NewTransaqTrades()`, SQL-константы, билдеры панелей, `ttPanelDefaults`.
- `grafana/transaq_trades_test.go` — состав, витрины, фильтры, юниты, отсутствие сырых таблиц/`WITH FILL`/шаблонов, `Validate` с чужим uid.
- `grafana/main.go` — упорядоченный слайс; `Makefile` — `DASHBOARDS`; `.github/workflows/dashboards.yml` — lint/upload.
- `dashboard/transaq-trades.json` — артефакт сборки.
- Доки: `ARCHITECTURE.md` §6.7, `docs/GRAFANA.md`, `README.md`, скил + `ROADMAP_INFRA` (очередь), `ROADMAP_OFZ_MARKET.md`, `ROADMAP_INDEX.md`.

**Приёмка.**
1. `cd grafana && go vet ./... && go test ./...`; `make grafana-build` дважды — `git diff -- dashboard/` пуст.
2. `make all`, `make grafana-validate`, `make docs-check` — зелёные.
3. `gcx dev lint run --disable uneditable-dashboard --disable panel-title-description dashboard/transaq-trades.json` — no violations.
4. `make import STAT=transaq_net_flows` публикует витрины локально; все 5 SQL — через MCP `run_query` под `kimi_reader` без ошибок (пустой результат на пустой ленте — в протокол); `EXPLAIN ESTIMATE` панели 3.
5. Staging: `make grafana-deploy-staging` (туннель :3001, owner-assisted) → smoke всех четырёх uid, первые панели `transaq-trades` отдают точки.
6. Протокол — комментарием в PR; `Closes #459` вне backticks.

## 9. Границы v1 и следствия

- **Вне v1:** ETF (`TQTF`) — панель вернётся пунктом-следствием при расширении подписки экспортёра; сравнение «вчера» отдельной серией; per-trade график; интерактивные переменные (инструмент, шаг).
- **Схема не меняется:** `transaq_net_flows` и витрины 1.5 остаются как есть; кандидат на ревизию `ORDER BY` — отдельный пункт со спекой.
- **Данные:** локальная лента пуста, поэтому финальная визуальная приёмка — staging; локально фиксируются сборка, тесты, линт, валидация SQL и детерминизм артефакта.
