# Спека: `ofz-market/1.5` — нетто-объёмы сделок MOEX (net buy/sell flows)

**Дата:** 2026-10-30
**Пункт:** `ofz-market/1.5` (issue #414)
**Роадмап:** [ROADMAP_OFZ_MARKET.md](../../ROADMAP_OFZ_MARKET.md)
**Статус:** спека готова; реализация не начата; живая приёмка ждёт рыночную подписку аккаунта Transaq
**Research:** [2026-10-30-transaq-net-flows-source.md](../../research/2026-10-30-transaq-net-flows-source.md)

## 1. Цель и границы

Минутные и дневные объёмы покупок/продаж и нетто-поток по инструментам и режимам
торгов из ленты публичных сделок `transaq_trades` (внешний стриминговый контур
`transaq-clickhouse-exporter`). Только данные и расчётные агрегаты; никаких торговых
сигналов, позиций и рекомендаций.

В scope входит:

- импортёр `transaq_net_flows` в пакете `transaq/` (реестр `transaq/importers.go`) и таблица
  `transaq_net_flows`;
- витрины `v_transaq_net_flows` (минутные) и `v_transaq_net_flows_daily` (дневные);
- новый DAG `dagu/transaq.yaml` (внутри сессии MOEX + финальный прогон);
- комментарии таблиц и колонок, `GRANT SELECT` в `sql/mcp_kimi_reader.sql`,
  проверки в `scripts/mcp_check.py`, обновление `ARCHITECTURE.md` §6.2/§6.3,
  `README.md`, `docs/IMPORTERS.md`.

Пакет — `transaq/`, а не `ofz/`: стандарт пакета §7.1 требует префикса пакета в
`Name()`, а `ofz/importers_test.go` уже энфорсит префикс `ofz_` и ровно один
импортёр; имя таблицы `transaq_net_flows` задано критерием приёмки issue. Трекер
расписания (`dagu/transaq.yaml`) называется по пакету — этого ждёт энфорсер §8.3(г).

В scope входит также **деплой внешнего экспортёра** на staging: конфигурация
подписки (режимы и список инструментов), systemd-юнит и проверки покрытия живут в
репозитории `transaq-clickhouse-exporter` (`deploy/staging/`), а не здесь; сюда
приезжает только чтение `transaq_*`.

Вне scope: подписка и обработка стакана `transaq_quotes` (L2), CVD через торговые
сессии (нетто ведётся только внутри сессии), тиковая аналитика (источник —
секундная точность), бэкфилл истории ленты задним числом (источник не отдаёт),
изменения конвейера экспортёра (схема таблиц, разбор XML, подписки).

## 2. Источники

| Источник | Что берём | Ограничение |
|---|---|---|
| `transaq_trades` | `time`, `secid`, `sec_code`, `board`, `price`, `quantity`, `buy_sell`, `trade_no` | `quantity` — **лоты**; дубли по полному ключу до background merge; разрывы после reconnect не догоняются; секундная точность |
| `transaq_securities` | `lotsize` (через связку `(secid, seccode, board)`) | снимок без `as_of`; при `lotsize = 0`/`NULL` денежный объём не считается |

Единицы фиксируются на входе: лоты и рубли. Денежный объём сделки —
`price × quantity × lotsize`; для FORTS-контрактов (`board = 'FUT'`) это оценка по
универсальной формуле, а не денежный оборот контракта — оговорка обязательна в
комментариях колонок. MOEX ISS для нетто не используется: история ISS не несёт
стороны сделки.

## 3. Расчёт

### 3.1. Дедупликация

Строки ленты читаются с дедупликацией по полному ключу
`(secid, board, sec_code, trade_no, time, buy_sell)` (`GROUP BY` / чтение с
`FINAL`-семантикой): дубли до merge идентичны по ключу, любая версия строки
пригодна. Отсутствие дедупликации — дефект приёмки.

### 3.2. Минутный бакет

`minute = toStartOfMinute(time)` (Europe/Moscow). По `(sec_code, board, minute)`:

- `buy_lots = sumIf(quantity, buy_sell = 'B')`, `sell_lots` — аналогично для `'S'`;
- `buy_value_rub = sumIf(price × quantity × lotsize, buy_sell = 'B')`, `sell_value_rub` —
  аналогично; `lotsize` — из `transaq_securities FINAL` по `(secid, seccode, board)`;
  нет соответствия или `lotsize = 0` — сделка даёт только лоты, рубли не считаются;
- `trade_count` — число сделок бакета;
- `gap_suspected` — эвристика разрыва: внутри `(sec_code, board)` есть соседние
  сделки с разрывом `> 10 минут` в интервале 10:00–19:00 МСК (`lagInFrame`/`lag`)
  — тогда все затронутые бакеты помечаются `1`; эвристика v1, в комментарии
  колонки — явное определение;
- `loaded_at` — время расчёта бакета (Europe/Moscow).

### 3.3. Нетто

Нетто не хранится, а выводится в витринах: `net_lots = buy_lots − sell_lots`,
`net_value_rub = buy_value_rub − sell_value_rub` (Int64/Float64 со знаком).
Кумулятивная дельта внутри сессии — на стороне потребителя запросом, не хранится.

### 3.4. Дневная витрина

`v_transaq_net_flows_daily` — суммы минутных бакетов по
`(sec_code, board, toDate(minute))`: buy/sell лоты и рубли, `trade_count`,
`minutes_with_trades`, `gap_suspected = max(gap_suspected)`, `first_trade`,
`last_trade`. ВАРЖИНГ: `toStartOfDay` — дата по Europe/Moscow.

### 3.5. Окно пересчёта и идемпотентность

Каждый прогон пересчитывает окно `[watermark − 15 минут, max(time) ленты]`, где
`watermark = max(minute)` из `transaq_net_flows` (первый прогон — вся доступная
лента, осторожно: полная агрегация может быть тяжёлой; при `count() > 50 млн`
строк ленты — предупреждение в логе и приёмка объёма владельцем). Повторная запись
того же бакета с новым `loaded_at` — штатная замена версии
(`ReplacingMergeTree(loaded_at)`), прогон идемпотентен по суммам лотов.

### 3.6. No-op гейт

Если `transaq_trades` не содержит строк новее `watermark − 15 минут`, прогон —
no-op с info-логом (пустая лента — штатное состояние вне сессии и без подписки;
это НЕ ошибка прогона). Пустой лентой нельзя «обнулить» уже посчитанные бакеты.

## 4. Схема

Таблица `transaq_net_flows`:

```sql
CREATE TABLE IF NOT EXISTS transaq_net_flows (
    minute          DateTime('Europe/Moscow'),
    sec_code        LowCardinality(String),
    board           LowCardinality(String),
    buy_lots        UInt64,             -- лоты, не бумаги и не рубли
    sell_lots       UInt64,
    buy_value_rub   Float64,            -- оценка: price × lots × lotsize; FORTS — приближение
    sell_value_rub  Float64,
    trade_count     UInt32,
    gap_suspected   UInt8,              -- 1: эвристика разрыва ленты, см. §3.2
    loaded_at       DateTime('Europe/Moscow')
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (sec_code, board, minute)
```

Витрины (публикует импортёр после `Send`, прецедент `ofz/views.go` +
`util.CreateView`):

- `v_transaq_net_flows` — `SELECT minute, sec_code, board, buy_lots, sell_lots,
  net_lots, buy_value_rub, sell_value_rub, net_value_rub, trade_count,
  gap_suspected, loaded_at FROM transaq_net_flows FINAL`;
- `v_transaq_net_flows_daily` — группировка минутной витрины-таблицы по дню (§3.4).

Обе — с комментариями таблицы и каждой колонки (единицы, знак нетто, оговорка
FORTS, семантика `gap_suspected`, «аналитика, не сигнал»).

## 5. Импортёр и расписание

- Реестрация: запись `&transaqNetFlows{}` в `transaqImporters`
  (`transaq/importers.go`), имя импортёра/таблицы — `transaq_net_flows`;
  запуск `make import STAT=transaq_net_flows`; blank-import пакета в `main.go`.
- Чтение — через существующее подключение проекта к ClickHouse (импортёр
  агрегирует SQL-запросом в БД, а не тянет ленту в Go построчно).
- DAG: новый `dagu/transaq.yaml`, `type: chain`,
  `overlap_policy: skip`, шаг `transaq_net_flows` (`CLICKHOUSE_IMPORT_STAT`):
  - `*/15 9-18 * * 1-5` — каждые 15 минут 09:00–18:59 МСК в будни;
  - `20 19 * * 1-5` — финальный прогон после закрытия основной сессии;
  - `retry_policy: 3 × 300 c` (прецедент `ofz_analytics.yaml`).
  Вечерняя сессия (19:00–23:50 МСК) догоняется утренним прогоном через окно
  `[watermark − 15 мин, max(time)]`; отдельного шага в 23:5x в v1 нет.

## 6. MCP и документация (правило 11)

- `GRANT SELECT ON default.v_transaq_net_flows` и `v_transaq_net_flows_daily` в
  `sql/mcp_kimi_reader.sql` (выдача вручную, `make mcp-user`); сырые `transaq_*`
  и `transaq_net_flows` агенту **не выдаются**.
- `scripts/mcp_check.py`: обе витрины читаются запросом к самой витрине (не поиском
  имени в `list_tables` — урок bold-блока), `SELECT count() FROM transaq_net_flows`
  и `FROM transaq_trades` под `kimi_reader` — `ACCESS_DENIED` (497).
- `series_catalog` не пополняется намеренно: `transaq_net_flows` — событийная
  производная таблица, а не ряд `macro_series` (прецеденты `news_events`,
  `bold_fund_snapshot`); понимание источника дают комментарии витрин.
- Документация тем же PR: `ARCHITECTURE.md` §6.2 (канонический DDL таблицы) и
  §6.3 (описание витрин), `README.md` (строка импортёра), `docs/IMPORTERS.md`
  (строка пакета `ofz`), календарь §7 при необходимости.

## 7. Тесты и приёмка

- Юнит-тесты на фикстурах ленты: сплит B/S, перевод лотов в рубли через `lotsize`,
  дедупликация по полному ключу, `gap_suspected` на синтетическом разрыве,
  идемпотентный пересчёт окна, no-op гейт.
- `make all`, `make mcp-check`, `dagu validate dagu/transaq_net_flows.yaml`,
  `make docs-check` — зелёные.
- Живая приёмка (комментарием в PR) — при включённой подписке аккаунта: витрины
  непусты и свежи, дневная агрегация за день сходится с контрольной суммой лотов по
  ленте, повтор прогона — идемпотентен. До включения подписки приёмка — на
  фикстурах; в PR явно: «живой прогон Transaq не выполнялся, внешняя зависимость
  отслеживается в issue #414».

## 8. Чего не делать

- не включать подписку `transaq_quotes` и не строить L2/OFI;
- не переносить нетто и CVD через торговые сессии;
- не заявлять PIT сверх `loaded_at` и не обещать бэкфилл разрывов ленты;
- не выдавать сырые `transaq_*`/`transaq_net_flows` агенту;
- не хардкодить конфигурацию подписки экспортёра (boards/тикеры) — это внешний
  контур владельца, проектный код читает ленту как есть.
