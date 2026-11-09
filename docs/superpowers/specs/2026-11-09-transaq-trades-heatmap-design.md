# Теплокарта нетто-потоков Transaq в дашборде `transaq-trades` — design

**Дата:** 2026-11-09. **Ветка:** `feat/transaq-trades-heatmap` (worktree `.worktrees/transaq-trades-heatmap`).
**Пункт:** `ofz-market/1.9` ([issue #554](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/554)), зависит от `ofz-market/1.6` (дашборд `transaq-trades`) и `ofz-market/1.7` (`instrument_kind` в витринах, merged).
**Приёмка (кратко):** в `transaq-trades` добавлена панель id 6 в ряду id 23 — Apache ECharts `treemap` на `v_transaq_net_flows_daily` + `v_ofz_issue_analytics`; каждый запрос — только `v_*`, `$__dateFilter` в таргете, `LIMIT` на теплокарте нет; `make all`, `make grafana-validate`, `make grafana-build` (детерминизм), `make docs-check`, `gcx dev lint run` и CI `dashboards` зелёные; живой SQL панели через MCP под `kimi_reader`; визуальный smoke — staging (owner-assisted); PR отревьюен `qwencloud/qwen3.8-flash`; протокол — комментарием в PR. Детали — §7.

## 1. Заказ и место в роадмапах

**Заказ владельца (2026-11-09):** добавить в `dashboard/transaq-trades.json` теплокарту нетто-покупок и нетто-продаж по референсу smart-lab. На референсе площадь плитки — размер, цвет — изменение цены; для объёмов нужно то же измерение, но по объёму: площадь — оборот, цвет — относительный перевес покупок/продаж. Желательные кластеры — акции и облигации, облигации сгруппировать по срокам 1/3/5/10 лет; использовать Apache ECharts.

**Владелец роадмапа — `ofz-market`:** контур Transaq (лента сделок, витрины `v_transaq_net_flows*`, дашборд `transaq-trades`) ведётся в [ROADMAP_OFZ_MARKET.md](../../ROADMAP_OFZ_MARKET.md) §5; пункт 1.9 — теплокарта на уже существующих витринах 1.5/1.7, данных не добавляет. `ROADMAP_INDEX.md` §1: схема item-id `ofz-market` расширена до `1.1`–`1.9`.

**Трекер:** issue #554 с labels `roadmap:ofz-market` + `role:go`; Project V2 — приватный `Treasury Autopilot — Roadmap` (number 12), поля: Roadmap `ofz-market`, Phase `1`, Item `1.9`, Priority `P1`, Depends on `ofz-market/1.6`, `ofz-market/1.7`, Spec/Plan — пути этого документа и плана.

**Дашборд — код, а не экспорт.** Источник истины — `grafana/transaq_trades.go`; `dashboard/transaq-trades.json` собирается `make grafana-build` и руками не правится (ARCHITECTURE.md §6.7). Панель добавляется в Go-билдер, артефакт перегенерируется тем же PR.

## 2. Инвентаризация источника

| Источник | Что берём | Ограничение |
|---|---|---|
| `v_transaq_net_flows_daily` | `date`, `sec_code`, `board`, `instrument_kind`, `buy_value_rub`, `sell_value_rub`, `trade_count` | дневное зерно; рублёвый объём — **оценка** `Σ price × лоты × lotsize`, не биржевой оборот; разрыв ленты не догоняется; `instrument_kind` — `share`/`fund`/`ofz`/`future`/`other` |
| `v_ofz_issue_analytics` | `sec_code`, `maturity_date` | одна строка на выпуск, режим и дату расчёта; обновляется ежедневно 19:47 МСК; покрывает ОФЗ, включая CNY-выпуски с `sec_code` вида `RU000A…` |

**Гэп-анализ «панель → витрина»:** все поля теплокарты уже есть в двух витринах. Новые таблицы, витрины, колонки и гранты не нужны. Срок ОФЗ берётся из `v_ofz_issue_analytics` — единственной доступной агенту витрины с `maturity_date`; сырые `transaq_securities_info`/`transaq_securities` агенту не выданы.

**Корпоративные облигации.** В подписке экспортёра — акции IMOEX и фонды денежного рынка (`TQBR`), ОФЗ (`TQOB`) и индексные фьючерсы (`FUT`). Доски `TQCB` (корпоративные облигации) в ленте нет; `instrument_kind` для неё был бы `other`, а срока в доступных витринах нет. Поэтому «другие облигации по срокам» в v1 — это бакеты ОФЗ; для прочих режимов остаётся кластер «Прочие» без тенора. Появление `TQCB` — отдельный пункт с витриной сроков, без выдумывания бакетов сейчас.

## 3. Живые факты: витрины, гранты, данные

- `v_transaq_net_flows_daily` — `SELECT` из `transaq_net_flows FINAL` с группировкой по `(date, sec_code, board)`; колонки `instrument_kind`, `buy_value_rub`, `sell_value_rub`, `trade_count` (см. `transaq/views.go`). На staging витрина актуальна после `ofz-market/1.7`; локальный dev-ClickHouse может отставать: до `make import STAT=transaq_net_flows` в локальной витрине нет `instrument_kind`, а лента пуста.
- `v_ofz_issue_analytics` — `ofz_issue_analytics FINAL`; колонки `date`, `sec_code`, `isin`, `board`, `maturity_date`, `days_to_maturity`, `short_name`, `calculation_status`. Локально на 2026-10-24 — 64 строки / 61 код, погашения 2027–2044; три CNY-ОФЗ лежат с `sec_code` = ISIN (`RU000A10DQA8`, `RU000A10DQB6`, `RU000A10FAK6`), остальные — `SU…RMFS…`. Группировка по `sec_code` и `max(maturity_date)` снимает дубли `board=''`/`TQOB`.
- Гранты `kimi_reader`: `sql/mcp_kimi_reader.sql` — `v_ofz_issue_analytics` (строка 109), `v_transaq_net_flows` (153), `v_transaq_net_flows_daily` (154). Новых выдач не требуется.
- Локальная лента Transaq пуста; приёмка SQL на dev фиксирует только отсутствие ошибок и 0 строк. Реальная визуальная приёмка — staging (owner-assisted): у владельца стенд с рыночной подпиской и свежими `v_transaq_net_flows_daily`.

## 4. SQL-контракт (дословно)

Формат ответа — `table` (`format=1`, `queryType=sql`), датасорс `ch-rosstat`. Запрос один; `$__dateFilter` — на сырой `Date`-колонке до агрегации.

```sql
SELECT
    multiIf(
        instrument_kind = 'share', 'Акции',
        instrument_kind = 'fund', 'Фонды денежного рынка',
        instrument_kind = 'future', 'Фьючерсы',
        instrument_kind = 'ofz' AND days_left IS NULL, 'ОФЗ (срок н/д)',
        instrument_kind = 'ofz' AND days_left <= 365, 'ОФЗ до 1 года',
        instrument_kind = 'ofz' AND days_left <= 1095, 'ОФЗ 1–3 года',
        instrument_kind = 'ofz' AND days_left <= 1825, 'ОФЗ 3–5 лет',
        instrument_kind = 'ofz' AND days_left <= 3650, 'ОФЗ 5–10 лет',
        instrument_kind = 'ofz', 'ОФЗ 10+ лет',
        'Прочие'
    ) AS "Кластер",
    "Тикер", "Покупки, ₽", "Продажи, ₽", "Оборот, ₽", "Нетто, ₽", "Сделок"
FROM (
    SELECT
        d.sec_code AS "Тикер",
        any(d.instrument_kind) AS instrument_kind,
        sum(d.buy_value_rub) AS "Покупки, ₽",
        sum(d.sell_value_rub) AS "Продажи, ₽",
        sum(d.buy_value_rub + d.sell_value_rub) AS "Оборот, ₽",
        sum(d.buy_value_rub - d.sell_value_rub) AS "Нетто, ₽",
        sum(d.trade_count) AS "Сделок",
        dateDiff('day', max(d.date), nullIf(max(m.maturity_date), toDate('1970-01-01'))) AS days_left
    FROM v_transaq_net_flows_daily AS d
    LEFT JOIN (
        SELECT sec_code, max(maturity_date) AS maturity_date
        FROM v_ofz_issue_analytics
        GROUP BY sec_code
    ) AS m ON m.sec_code = d.sec_code
    WHERE $__dateFilter(d.date)
      AND d.instrument_kind IN ('share', 'fund', 'ofz', 'future', 'other')
    GROUP BY d.sec_code
)
WHERE "Оборот, ₽" > 0
ORDER BY "Оборот, ₽" DESC
```

**Почему так.**

- `instrument_kind IN (...)` — все типы подписки; `other` оставлен как резерв для будущих режимов, но в текущей ленте его нет. Фильтр по `board` не используется: фонды на `TQBR` смешались бы с акциями.
- `LEFT JOIN` подзапроса `v_ofz_issue_analytics` — срок только для ОФЗ; для акций/фондов/фьючерсов `m.maturity_date` отсутствует. `nullIf(..., toDate('1970-01-01'))` превращает дефолт левого join в `NULL`, иначе ОФЗ без строки аналитики попала бы в «до 1 года».
- `days_left` — `dateDiff('day', max(d.date), maturity_date)`; `max(d.date)` — последняя дата инструмента в выбранном диапазоне, а не сегодняшняя. Для бакетов 1/3/5/10 лет разница в несколько дней несущественна; для точного срока в tooltip можно смотреть `maturity_date` в аналитике.
- `WHERE "Оборот, ₽" > 0` — нулевые и отсутствующие объёмы не рисуются; `WITH FILL` не применяется (пропуск ≠ ноль).
- `ORDER BY "Оборот, ₽" DESC` — внутри кластера крупные плитки идут первыми; `LIMIT` нет — график не обрезается.
- `$__dateFilter(d.date)` — макрос плагина, разворачивается в границы дашборда; алиас `d` снимает неоднозначность с `date` в подзапросе аналитики.

**Тестовый контракт.** Тест панели 6 проверяет: `FROM v_transaq_net_flows_daily` и `FROM v_ofz_issue_analytics`, ровно один `$__dateFilter(d.date)`, отсутствие `LIMIT`, `format=1`/`queryType=sql`, отсутствие сырых `transaq_*` и `WITH FILL`.

## 5. Визуал: Apache ECharts `treemap`

**Каркас.** Панель id 6, ряд id 23 «Теплокарта периода», первой в дашборде (перед «Итоги периода»), `span=24`, `height=16`. Тип — `volkovlabs-echarts-panel`, `pluginVersion` — `EChartsPluginVersion` (7.2.5), `editorMode: "code"`, `renderer: "canvas"`; `getOption` — тело JS-функции, данные только из `context.panel.data.series[0]`.

**Данные.** Поля SQL читаются по именам: `Кластер`, `Тикер`, `Покупки, ₽`, `Продажи, ₽`, `Оборот, ₽`, `Нетто, ₽`, `Сделок`. Строки группируются в два уровня:

1. кластер (top-level): `Акции`, `ОФЗ до 1 года`, `ОФЗ 1–3 года`, `ОФЗ 3–5 лет`, `ОФЗ 5–10 лет`, `ОФЗ 10+ лет`, `ОФЗ (срок н/д)`, `Фонды денежного рынка`, `Фьючерсы`, `Прочие`; порядок фиксирован, незнакомые кластеры добавляются в конец;
2. лист — `sec_code`; у ОФЗ для подписи срезается суффикс `RMFS9` (`SU26238RMFS9` → `SU26238`), в tooltip остаётся полный код.

**Площадь и цвет.**

- `value` листа = `Оборот, ₽` (`buy + sell`) — площадь показывает, где сосредоточен объём.
- `ratio = Нетто, ₽ / Оборот, ₽` — относительный перевес в диапазоне [−1, +1]. Это аналог «% изменения» референса, не зависящий от размера бумаги.
- Шкала симметричная: `scale = clamp(p95(|ratio|), 0.02, 1)`, где p95 — 95-й перцентиль по листьям; `t = sqrt(min(1, |ratio| / scale))`. Цвет — `hsl`: покупки (`ratio > 0`) — зелёный 142°, продажи — красный 0°, насыщенность 65 %, светлота `80 − 48·t` (80 % у нуля → 32 % у максимума); `ratio = 0` — серый `#ced4da`. p95 не даёт одному выбросу обесцветить остальные плитки.
- Родительский кластер — тёмно-серый (`#343a40`) с белым `upperLabel`; цвет листьев задаётся `itemStyle.color` per-item, `visualMap` не используется (иначе он перекрасил бы плитки по `value`, а не по `ratio`).

**Подписи и tooltip.**

- Лист: две строки rich-text — короткий тикер (12 px, bold) и нетто в компактных рублях (`+1,2 млрд`, `−340 млн`, `0`); белый текст с тёмной обводкой, `overflow: 'truncate'`. На маленьких плитках ECharts сам скрывает подпись.
- Кластер: `upperLabel` высотой 20 px, белый текст на полупрозрачном тёмном фоне.
- Tooltip: кластер, покупки ₽, продажи ₽, нетто ₽ и % оборота, оборот ₽, число сделок; для кластера — суммарные оборот/нетто и %.
- Внизу — `graphic`-подпись: «Цвет — перевес покупок/продаж, % оборота; площадь — оборот, ₽».

**Поведение и пустые данные.** `leafDepth: 2`, `sort: false`, `nodeClick: false`, `breadcrumb.show: false`, `roam: false`. Если серии/полей нет или все обороты нулевые — `getOption` возвращает `{}` (панель пустая, без ошибки). Пропуск срока у ОФЗ — кластер «ОФЗ (срок н/д)», а не «до 1 года». Литеральных тикеров и сумм в JS нет: все значения приходят из SQL.

**Единицы.** `Unit(units.Rubles)`, `Decimals(0)` — как у остальных панелей дашборда; форматирование в подписях/tooltip — компактное (млрд/млн/тыс. ₽).

**Описание панели** (русское, в `description`): источник — `v_transaq_net_flows_daily` и `v_ofz_issue_analytics`; площадь — оборот, цвет — относительный перевес; рублёвая оценка через `lotsize`, а не биржевой оборот; лента событийная, разрывы не догоняются; корпоративных облигаций в подписке нет; это аналитика, а не сигнал и не рекомендация.

## 6. Что осознанно не делаем

| Решение | Причина |
|---|---|
| Не читаем сырые `transaq_trades`/`transaq_securities`/`transaq_net_flows` | у `kimi_reader` нет грантов; `Validate` требует `FROM v_` |
| Не заводим новую витрину сроков / `v_transaq_securities` | срок ОФЗ уже есть в `v_ofz_issue_analytics`; новых данных панель не требует |
| Не группируем ОФЗ-АД/ПК/ИН отдельно | в ленте важен срок, а не тип купона; тип — вне объёма v1 |
| Не строим теноры для `TQCB` | корпоративных облигаций в подписке нет, срока в доступных витринах нет |
| Не используем `visualMap` | площадь и цвет — разные измерения; `visualMap` по `value` перекрасил бы плитки не по `ratio` |
| Не добавляем `LIMIT` | теплокарта — не top-N; обрезка скрыла бы инструменты |
| Не добавляем шаблонные переменные | v1 дашборда без переменных; период задаёт диапазон дашборда |
| Не добавляем стратегию/рекомендации | контур — аналитика фактических сделок |

## 7. Реализация и приёмка

**Файлы.**

- `grafana/transaq_trades.go` — `ttHeatmapSQL`, `ttHeatmapJS`, `ttHeatmapPanel()`, новый ряд 23 в `NewTransaqTrades()`.
- `grafana/transaq_trades_test.go` — 6 панелей / 3 ряда; контракт SQL панели 6; тип/версия/options ECharts; `getOption` содержит `treemap` и имена полей, не содержит SQL/`FROM v_`/литеральных тикеров; units/decimals.
- `dashboard/transaq-trades.json` — `make grafana-build`.
- Доки: этот файл, план, `docs/ROADMAP_OFZ_MARKET.md` (1.9), `docs/ROADMAP_INDEX.md` (схема), `docs/GRAFANA.md`, `ARCHITECTURE.md` §6.7.

**Приёмка.**

1. `cd grafana && go vet ./... && go test ./...`; `make grafana-build` дважды — `git diff -- dashboard/` пуст.
2. `make all`, `make grafana-validate`, `make docs-check` — зелёные.
3. `gcx dev lint run --disable uneditable-dashboard --disable panel-title-description dashboard/transaq-trades.json` — no violations.
4. `make import STAT=transaq_net_flows` локально публикует актуальные витрины (лента пуста — no-op); SQL панели 6 через MCP `run_query` под `kimi_reader` с явным диапазоном — без ошибок, 0 строк; `EXPLAIN ESTIMATE`; тенор-выражение отдельно на `v_ofz_issue_analytics` — бакеты 1/3/5/10 лет.
5. JS-харнесс на Node с синтетическим фреймом (2 кластера, покупки/продажи/ноль) — option строится, цвета/подписи/tooltip не падают; визуальный smoke — dev/staging через `mcp__grafana__get_panel_image`, если стенд с данными доступен.
6. Staging: `make grafana-deploy-staging` + smoke (owner-assisted); если туннель/токен недоступны — в PR «не проверено».
7. PR: `Closes #554` вне backticks; ревью субагентом `qwencloud/qwen3.8-flash`; findings разобраны; протокол — комментарием в PR; CI `dashboards` зелёный.

## 8. Границы v1 и следствия

- **Вне v1:** корпоративные облигации (`TQCB`) и их теноры — отдельный пункт после расширения подписки; секторная группировка акций — отдельный пункт с маппингом тикеров (литеральный маппинг в JS запрещён); абсолютное нетто в цвете — переключатель не заводим, текущий выбор — относительный перевес.
- **Схема не меняется:** `transaq_net_flows` и витрины 1.5/1.7 остаются как есть; `v_ofz_issue_analytics` — только чтение.
- **Данные:** локальная лента пуста, поэтому финальная визуальная приёмка — staging; локально фиксируются сборка, тесты, линт, валидация SQL и детерминизм артефакта.
- **Не сигнал:** панель показывает фактические сделки и их рублёвую оценку; она не является рекомендацией и не учитывает позиции/лимиты.
