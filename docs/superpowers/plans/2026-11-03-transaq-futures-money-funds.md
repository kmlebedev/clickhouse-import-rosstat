# План: `ofz-market/1.7` — индексные фьючерсы и фонды денежного рынка в нетто-потоках Transaq

**Спека:** [2026-11-03-transaq-futures-money-funds-design.md](../specs/2026-11-03-transaq-futures-money-funds-design.md)
**Ветка:** `feat/ofz-market-1.7` (этот репозиторий); экспортёр — отдельная ветка и PR в `kmlebedev/transaq-clickhouse-exporter`
**Роль:** Go-инженер

Каждая задача — TDD: сначала падающий тест, затем код, затем `make all`.

## Задача 1. Экспортёр: вселенная с фьючерсами и фондами

Репозиторий `transaq-clickhouse-exporter`, ветка `feat/universe-futures-funds`.

1. Фикстура строк ISS `engines/futures/markets/forts/securities` (`SECID`, `ASSETCODE`,
   `LASTTRADEDATE`): квартальные MIX/RTS/MXI, календарный спред, истёкший контракт, чужой актив.
2. Тест чистой функции `nearestFutures(rows, assets, today, n=2)`: два ближайших неистёкших
   квартальных контракта на актив, спреды и чужие активы отброшены, порядок детерминирован.
3. Реализация функции и блока загрузки в `cmd/universe/main.go`; фиксированные коды `IMOEXF`
   и фонды `AKMM`, `SBMM`, `LQDT`.
4. `deploy/staging/exporter.export.env.in`: `EXPORT_SEC_BOARDS=TQBR,TQOB,FUT`; комментарий
   про состав вселенной.
5. `make universe-refresh`, проверить в списке 7 фьючерсов и 3 фонда; `go vet`, `go test -race`.
6. PR в экспортёр. Деплой — `make deploy-staging` после мержа по явной просьбе владельца.

## Задача 2. `ARCHITECTURE.md` до кода

1. §6.2: канонический DDL `transaq_net_flows` с `instrument_kind` и комментарием про миграцию.
2. §6.3: описание `v_transaq_net_flows*` — `instrument_kind`, формула FORTS, фонды на `TQBR`.
3. §7: строка календаря `dagu/transaq.yaml` — состав ленты.

## Задача 3. Импортёр `transaq_net_flows`

1. Тесты в `transaq/net_flows_test.go`:
   - SQL содержит ветку `board = 'FUT'` с `point_cost × 10^decimals / 100`;
   - `instrument_kind` вычисляется по правилам спеки §5 (таблица случаев на SQL-тексте или
     на выделенной Go-функции, если классификация вынесена в Go);
   - DDL содержит `instrument_kind LowCardinality(String)`, миграция — `ADD COLUMN IF NOT EXISTS`.
2. `transaq/net_flows.go`: CTE `securities_lots` → `securities_meta` (`lotsize`, `point_cost`,
   `decimals`, `shortname`); формула стоимости через `multiIf`; `instrument_kind` в `SELECT`,
   скане и `batch.Append`; миграция в схеме импортёра.
3. Живой прогон на dev-ClickHouse (`make import STAT=transaq_net_flows`) — идемпотентность.

## Задача 4. Витрины

1. `transaq/views.go`: `instrument_kind` с fallback для `''` в обеих витринах; комментарии
   таблицы и колонок (`board` теперь включает `FUT`; формула FORTS; снимок `point_cost` RTS;
   вечерний клиринг и `gap_suspected`).
2. Тест на текст витрин: колонка, fallback, комментарии без устаревшего «для FORTS умножение
   на lotsize не равно денежному обороту».

## Задача 5. Дашборд `transaq-trades`

1. `grafana/transaq_trades.go`: фильтры `board = 'TQBR'` → `instrument_kind = 'share'`,
   `board = 'TQOB'` → `instrument_kind = 'ofz'`.
2. `make grafana-build`, `make grafana-validate`; артефакт `dashboard/transaq-trades.json` в коммите.

## Задача 6. MCP и документация

1. `scripts/mcp_check.py`: чтение `instrument_kind` из обеих витрин под `kimi_reader`.
2. `README.md` — состав ленты; `docs/ROADMAP_OFZ_MARKET.md` — статус `1.7`.
3. `make all`, `make docs-check`.

## Задача 7. Живая приёмка (после деплоя экспортёра)

1. Торговый день с подпиской: строки `future` (7 кодов) и `fund` (3 кода) в
   `v_transaq_net_flows_daily`.
2. Сверка дневной оценки рублей MXZ6/MMZ6/RIZ6 с ISS VALTODAY (±5 %).
3. Топ-10 панелей IMOEX — без фондов; повтор импорта идемпотентен; длительность шага DAG.
4. `make mcp-check`; протокол — комментарием в PR; PR с `Closes #N`.
