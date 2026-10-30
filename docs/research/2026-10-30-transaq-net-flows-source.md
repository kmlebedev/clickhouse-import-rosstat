# Источник биржевых сделок Transaq → нетто-объёмы покупок/продаж: исследование (2026-10-30)

> **Объект:** sibling-репозиторий `transaq-clickhouse-exporter` (`github.com/kmlebedev/transaq-clickhouse-exporter`) и его источник `txmlconnector` (Transaq XML Connector → брокерский сервер Финам `tr1.finam.ru` → MOEX); таблицы `transaq_*` в ClickHouse проекта.
> **Задача:** решить, как регулярно собирать биржевые данные по рыночным сделкам с объёмами «нетто покупка / нетто продажа» (net buy/sell, CVD) в контуре `clickhouse-import-rosstat`, и оформить go/no-go с критерием приёмки до реализации.
> **Метод:** чтение схемы данных экспортёра (`moex-exchange-data-schema.md`, `db.go`, `process.go`, `main.go`, `etc/transaq-clickhouse-exporter.service`), документации проекта ([ROADMAP_OFZ_MARKET.md](../ROADMAP_OFZ_MARKET.md), [ARCHITECTURE.md](../../ARCHITECTURE.md) §6.3/§6.5, [IMPORTERS.md](../IMPORTERS.md)), живые пробы через MCP ClickHouse под `kimi_reader` 2026-10-30.
> **Границы:** живой прогон экспортёра не выполнялся (локально нет сессии `txmlconnector`; учётные данные Transaq — секреты и не использовались); юридические условия Transaq/Финам не разбирались; стабильность брокерского API за пределами документированной практики не проверялась.

## 1. Короткий вывод

| Компонент | Что даёт | Вердикт |
|---|---|---|
| `transaq_trades` (поток `alltrades`) | лента публичных сделок с ценой, объёмом в лотах и **стороной сделки** `buy_sell` (B/S) | **go** — единственный источник нетто-объёмов |
| Агрегация net buy/sell в этом репозитории (новый пункт `ofz-market/1.5`) | минутные/дневные бакеты buy/sell/net по (sec_code, board) из `transaq_trades` | **go** — реализация по спеке `2026-10-30-transaq-net-flows-design.md` |
| Экспортёр как код внутри репозитория | перенос/дублирование конвейера | **no-go** — он и остаётся внешним сервисом, проект только читает `transaq_*` |
| Пакетный импортёр MOEX ISS как источник нетто | дневные агрегаты по инструменту | **no-go** для нетто: ISS-история даёт NUMTRADES/VALUE/VOLUME без стороны сделки; нетто покупка/продажа из неё не выводится. ISS остаётся fallback для цены/оборота (как в `ofz_issue_analytics`) |
| `transaq_quotes` (стакан L2) | дельты книги заявок | **out of scope**: handler есть, но подписка не отправляется; для нетто-объёмов не нужен |

Главный вывод: связка «экспортёр пишет `transaq_trades` → импортёр `transaq_net_flows` агрегирует buy/sell → витрины `v_transaq_net_flows*`» закрывает требование без нового сборщика и без изменений в репозитории экспортёра. Жёсткая внешняя зависимость: у брокерского аккаунта Transaq должна быть включена подписка на рыночные данные — на приёмке 2026-10-24 (`ofz-market/1.1`) `transaq_trades` был пуст именно по этой причине.

## 2. Источник и доступ

Конвейер (по `moex-exchange-data-schema.md` §1.1):

```text
MOEX / брокерский Transaq-сервер (tr1.finam.ru:3900)
  → TXmlConnector.dll (Win32; Windows или Wine)
  → txmlconnector gRPC server (SendCommand + FetchResponseData)
  → Go client (kmlebedev/txmlconnector)
  → transaq-clickhouse-exporter
  → ClickHouse: transaq_trades, transaq_candles, transaq_quotes,
      transaq_securities, transaq_securities_info
```

| Компонент | Канал / endpoint | Формат / auth | Частота | Глубина | Единицы | Ревизии | License/terms | В репозитории |
|---|---|---|---|---|---|---|---|---|
| Публичные сделки `alltrades` | подписка XML внутри терминальной сессии; экспортёр пишет `INSERT INTO transaq_trades` | XML; auth — учётка Transaq в команде `connect` (TC_LOGIN/TC_PASSWORD, env, в документы не попадают) | поток в реальном времени в торговую сессию | с момента старта сбора; историю поток не отдаёт задним числом | price — валюта/пункты инструмента; quantity — **лоты** | `none` (событийная лента), дубли до merge | условия Transaq/Финам не разбирались | `transaq_trades` читается импортёром `ofz_issue_analytics`; сама лента пуста с приёмки 2026-10-24 |
| Минутные свечи | агрегация `quotations` в памяти + история `gethistorydata` | то же | в сессию | ограничена `EXPORT_CANDLE_COUNT` | volume — лоты | `none` | то же | `transaq_candles` (пуст на приёмке) |
| Каталог `securities` | снимок при восстановлении подписок | то же | на сессию | последний снимок | lotsize, lotdivider, point_cost, minstep | `none` (снимок без as_of) | то же | читается `ofz_issue_analytics` |
| Расширенный справочник `sec_info` | `get_securities_info` по фильтру `EXPORT_SEC_INFO_NAMES` | то же | на сессию | последнее состояние | купоны, НКД, ГО | `none` | то же | `transaq_securities_info` — 4074 строки на 2026-10-24 |
| Подключение к ClickHouse | `CLICKHOUSE_URL`, default `tcp://127.0.0.1:9000`; схема из 5 таблиц создаётся при старте | native protocol, batch INSERT / AsyncInsert | — | — | — | — | — | dev-ClickHouse проекта (`make ch-up`, TCP 9000) |

Практические правила доступа (из схемы экспортёра §5): подписки формируются из `EXPORT_SEC_BOARDS` (default `TQBR,TQCB,FUT`), `EXPORT_SEC_CODES=ALL`, `EXPORT_ALL_TRADES=ALL` — иначе сделки по конкретным инструментам не запрашиваются; команда `subscribe` может вернуть `success=true`, но брокерский аккаунт не отдать рыночные данные — проверка только по фактическим строкам и свежести.

## 3. Покрытие и качество данных

Живые пробы 2026-10-30 (MCP `clickhouse`, пользователь `kimi_reader`, read-only):

```sql
-- витрина контура ОФЗ — снимок приёмки жив, свежее обновление не прогонялось
SELECT count() AS n, max(date) AS last_date, max(updated_at) AS last_updated
FROM v_ofz_issue_analytics;
-- n = 64, last_date = 2026-10-24, last_updated = 2026-10-24 19:36:03 +03:00
```

- `kimi_reader` видит только витрины (`v_*`, 84 шт.); сырые `transaq_*` закрыты — физическое наличие таблиц подтверждается косвенно (витрина `v_ofz_issue_analytics` читается, импортёр читает `transaq_securities_info`/`transaq_securities`/`transaq_trades` при ежедневном шаге 19:47 МСК).
- Ключ `transaq_trades`: `(secid, board, sec_code, trade_no, time, buy_sell)`; `trade_no` — биржевой номер сделки, дедупликация агрегатом возможна по полному ключу; до background merge дубли возможны.
- Единицы: `quantity` — лоты, не бумаги и не рубли. Денежный объём сделки = `price × quantity × lotsize` (для облигаций lotsize обычно = 1, но правило должно идти из `transaq_securities` с as-of, не из предположения); для FORTS «рублёвое умножение» не универсально (спецификация контракта).
- Точность времени — секунды (`DD.MM.YYYY HH:MM:SS`, Europe/Moscow); миллисекундной биржевой метки в сделках нет.
- Покрытие ленты зависит от конфигурации подписки: текущий пример сервиса собирает фиксированный список тикеров TQBR; для ОФЗ по README экспортёра нужны режимы `TQCB,TQOB` (сейчас в юните их нет).
- Журнал «сбор жив» (из §7 схемы): свежие строки `transaq_trades` за 10 минут в сессию, отсутствие reconnect-шторма и роста `queue reached`, признак `TRANSAQ subscriptions restored`.

## 4. Винтажи, ревизии, PIT

- Все пять таблиц — `ReplacingMergeTree()` **без версии**, без `ingested_at` и TTL. Справочники (`transaq_securities`, `transaq_securities_info`) — последний снимок без `as_of`; связка инструментов — по составу `(secid, seccode, board)`, один тикер сам по себе ключом не является.
- Лента сделок событийная (ревизий нет), но: (а) дубли до merge, (б) разрыв сессии не догоняется — очереди в памяти, exactly-once нет, восстановление подписок пропущенный поток не восстанавливает.
- Для нетто-агрегатов PIT-дисциплина своими силами: у агрегирующей таблицы проекта будет `loaded_at` (время расчёта бакета), а as-of сырых событий — биржевое время сделки. Пересчёт бакета идемпотентен по ключу бакета; повторная агрегация после позднего дубля/merge честно обновит бакет новым `loaded_at`.
- Винтажный архив проекта (`archive/`) версии файлов источников не покрывает: поток Transaq — не файловый источник; это событийный внешний контур, прецедент — `news_events`.

## 5. Риски и режимы отказа

| Риск | Проявление | Митигация |
|---|---|---|
| Нет подписки аккаунта на рыночные данные | `subscribe` → `success=true`, `transaq_trades` пуст (наблюдалось 2026-10-24) | гейт живой приёмки — по строкам, не по успеху команды; фиксация внешней зависимости в issue `ofz-market/1.5` |
| Разрыв сессии / reconnect | потерянный сегмент ленты не восстанавливается | агрегат помечает бакет `gap_suspected`, если разрыв времени сделок внутри бакета аномален; CVD не переносим через сессию |
| Дубли до background merge | завышение объёмов при чтении без дедупа | агрегация читает `transaq_trades FINAL` либо `GROUP BY` полного ключа сделки |
| Смена лота / мультирежимный `sec_code` | ошибка в рублёвом объёме | join к `transaq_securities FINAL` по `(secid, seccode, board)` на момент бакета; конфликты режимов — явный `board` в ключе |
| Win32-зависимость коннектора | DLL только Windows/Wine; gRPC не аутентифицирует | эксплуатация — внешний контур владельца (systemd-юнит пример в `etc/`); проектный код ничего не знает о DLL |
| Секундная точность | толщина внутри секунды не различима | минутные бакеты — норма; для тиковых задач источник не подходит (зафиксировать в спеке) |
| Секреты | TC_LOGIN/TC_PASSWORD | только env; в код, документы, SQL не попадают |

## 6. Рекомендация и критерий приёмки

**Go.** Заводится пункт `ofz-market/1.5` «Нетто-объёмы сделок MOEX (net buy/sell flows)»:

- агрегирующий импортёр `transaq_net_flows` пакета `ofz/`: читает `transaq_trades` (+`transaq_securities` для лота), пишет минутные бакеты `(sec_code, board, minute)` с `buy_lots`, `sell_lots`, `buy_value_rub`, `sell_value_rub`, `net_*`, числом сделок и флагом разрыва; расписание — в торговую сессию MOEX + финальный прогон после закрытия;
- витрины `v_transaq_net_flows` (минутные) и `v_transaq_net_flows_daily` (дневные) с комментариями таблиц и колонок, `GRANT SELECT` на обе в `sql/mcp_kimi_reader.sql`; сырые `transaq_*` и `transaq_net_flows` агенту не выдаются;
- спека: [2026-10-30-transaq-net-flows-design.md](../superpowers/specs/2026-10-30-transaq-net-flows-design.md).

**Критерий приёмки:** живой прогон при включённой подписке аккаунта — витрины непусты, свежие бакеты совпадают с временем последних сделок, дневная агрегация за день сходится с контрольной суммой по лотам; повторный прогон идемпотентен; `make all`, `make mcp-check` (`ACCESS_DENIED` на сырые таблицы, витрины читаются), `dagu validate`, `make docs-check` зелёные; протокол — комментарием в PR. До включения подписки — приёмка на зафиксированных фикстурах сделок.

Оценка объёма: спека и документы — 0,5 дня; пакет с тестами — 2–3 дня; приёмка — 0,5 дня. Зависимость: работающий экспортёр с рыночной подпиской (внешняя, отслеживается в issue).

## 7. Что не проверено

- Живой прогон экспортёра с включённой подпиской: нетто-витрины и критерий приёмки по свежим сделкам — после включения подписки владельцем.
- Чтение `transaq_*` под пользователем с полным доступом (физическое состояние таблиц подтверждено только косвенно через `kimi_reader`).
- Стабильность брокерского API (`tr1.finam.ru`) и поведение под нагрузкой за пределами документированной практики.
- Юридические условия Transaq/Финам на хранение и производное использование потока сделок.
- Возможность и целесообразность сбора `transaq_quotes` (L2) — отдельный пункт, под нетто-объёмы не нужен.

## 8. Источники и команды

- Схема данных экспортёра: `/Users/whitefox/GolandProjects/transaq-clickhouse-exporter/moex-exchange-data-schema.md` (§1–§8), `db.go`, `process.go`, `main.go`, `etc/transaq-clickhouse-exporter.service`, `README.md`.
- Контур проекта: [ROADMAP_OFZ_MARKET.md](../ROADMAP_OFZ_MARKET.md) (пункты 1.1, 1.3), [ARCHITECTURE.md](../../ARCHITECTURE.md) §6.3 (витрина `v_ofz_issue_analytics`), §6.5 (MCP `kimi_reader`), [IMPORTERS.md](../IMPORTERS.md) (`ofz`, пакет `moex`), `sql/mcp_kimi_reader.sql`, `ofz/issue_analytics.go`.
- Пробы 2026-10-30 (MCP ClickHouse, `kimi_reader`): `SELECT count(), max(date), max(updated_at) FROM v_ofz_issue_analytics` → 64 / 2026-10-24 / 2026-10-24 19:36:03+03:00; `system.tables` под `kimi_reader` — только витрины (`v_*`).
