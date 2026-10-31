# План: `ofz-market/1.6` — дашборд Trades как код + TS-практики в скиле

**Дата:** 2026-10-31
**Пункт:** `ofz-market/1.6` (issue [#459](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/459))
**Спека:** [2026-10-31-grafana-transaq-trades-design.md](../specs/2026-10-31-grafana-transaq-trades-design.md)
**Ветка:** `feat/grafana-transaq-trades` (worktree `.worktrees/transaq-trades-dashboard`, от `origin/main` `e992c8f`)
**Источник миграции:** `/Users/whitefox/GolandProjects/transaq-clickhouse-exporter/etc/grafana-dashboard-trades.json`

---

## 1. Цель

Перенести UI-экспорт `Trades` (sibling-проект `transaq-clickhouse-exporter`) в дашборд-как-код
текущего репозитория — 5 панелей в 2 рядах на существующих витринах `v_transaq_net_flows*` —
и зафиксировать в скиле `dashboard-migration` практики ClickHouse для time-series SQL,
применив их к панелям.

Исходное состояние (read-only проверка 2026-10-31):

| Что | Состояние |
|---|---|
| `v_transaq_net_flows`, `v_transaq_net_flows_daily` | объявлены в `transaq/views.go`, наполняются импортёром 1.5 (PR #418 merged) |
| гранты `kimi_reader` | есть на обе витрины (`sql/mcp_kimi_reader.sql:133-134`) |
| локальный dev-ClickHouse | сырые `transaq_*` есть, `transaq_net_flows` и витрин нет; лента локально пуста |
| staging | витрины наполнены (живая приёмка 1.5: 7 362 сделки, 263 бакета 10:53–10:56 МСК) |
| `grafana/`, `dashboard/` | три дашборда-кода; `dashboard/transaq-trades.json` нет |

## 2. Порядок работ

### Шаг 0. Пункт, документы, ветка

- Пункт `ofz-market/1.6` в [ROADMAP_OFZ_MARKET.md](../../ROADMAP_OFZ_MARKET.md) §4 и §9,
  схема item-id в [ROADMAP_INDEX.md](../../ROADMAP_INDEX.md) §1 → `1.1`–`1.6`.
- Issue #459 (`roadmap:ofz-market`, `role:go`), Project V2 `Treasury Autopilot` — Track `data`,
  Phase `1 Data`, Priority `P1`, Item `1.6`, Depends on `ofz-market/1.5`, Spec/Plan.
- Рабочее дерево `.worktrees/transaq-trades-dashboard`, ветка `feat/grafana-transaq-trades`.
- Коммит 1 — пункт роадмапа, реестр, спека, этот план.

### Шаг 1. TS-практики в скиле

- `.kimi-code/skills/dashboard-migration/SKILL.md`: раздел «Time-series SQL: практики ClickHouse»
  (10 правил + ссылки на статью и docs `toStartOfInterval`/`WITH FILL`/`timeSeries*ToGrid`),
  ссылка из шага 7 чек-листа, обновление очереди миграции (`transaq-trades` — мигрирован,
  следующий — `chmf.json`).
- Живые факты для правила о бакетах: `INTERVAL 86400 SECOND` vs `INTERVAL 1 DAY` на сервере
  `Asia/Yekaterinburg` (проверено MCP 2026-10-31: `03:00 МСК` vs `00:00 МСК`).

### Шаг 2. Билдер и тесты

1. `grafana/transaq_trades.go`:
   - `TransaqTradesUID = "transaq-trades"`, `NewTransaqTrades()` — title/uid/tags/refresh/time/timezone;
   - SQL-константы §5 спеки дословно; панели 1–2 — `tableQuery`, 3–5 — `timeseriesQuery`;
   - `ttPanelDefaults` по образцу `krPanelDefaults`/`fpPanelDefaults` (`spanNulls=false`);
   - `units.Rubles`, `Decimals(0)`, русские описания.
2. `grafana/transaq_trades_test.go`:
   - 5 панелей id 1–5, 2 ряда id 21–22;
   - панели 1–2 — `FROM v_transaq_net_flows_daily`, `$__dateFilter(date)` ×1, `LIMIT 10`;
   - панели 3–5 — `FROM v_transaq_net_flows`, `$__timeFilter(minute)` ×1, `$__timeInterval(minute)`,
     без `LIMIT`;
   - нет сырых `transaq_trades`/`transaq_securities`/`transaq_net_flows`, нет `WITH FILL`;
   - `board='TQBR'` (1–4), `board='TQOB'` (5); единицы `currencyRUB`, decimals 0; форматы 1/0;
   - `Validate` со своим uid проходит, с чужим — падает; `Templating` пуст.

### Шаг 3. Регистрация и артефакт

- `grafana/main.go` — `{TransaqTradesUID, NewTransaqTrades}`, help флага.
- `Makefile` — `DASHBOARDS := … transaq-trades` (последним), комментарии.
- `.github/workflows/dashboards.yml` — lint/upload/шапка.
- `make grafana-build` дважды; артефакт `dashboard/transaq-trades.json` коммитится.

### Шаг 4. Доки

- `ARCHITECTURE.md` §6.7 — «четыре дашборда», состав `transaq-trades`, ссылка на TS-раздел скила.
- `docs/GRAFANA.md` — список, дерево, деплой/smoke, приёмка.
- `README.md` — «Три дашборда» → «Четыре».
- `docs/ROADMAP_INFRA.md` §2.2 — очередь миграции (тем же PR).

### Шаг 5. Приёмка

1. `cd grafana && go vet ./... && go test ./...`; `make grafana-build` дважды — диффа нет.
2. `make all`, `make grafana-validate`, `make docs-check`.
3. `gcx dev lint run --disable uneditable-dashboard --disable panel-title-description dashboard/transaq-trades.json`.
4. `make import STAT=transaq_net_flows` (публикует витрины локально) → все 5 SQL через MCP
   `run_query` под `kimi_reader`; `EXPLAIN ESTIMATE` панели 3.
5. Staging — `make grafana-deploy-staging` (owner-assisted): все 4 uid видны, первые панели
   `transaq-trades` отдают точки.
6. Протокол — комментарием в PR; `Closes #459` вне backticks; статус роадмапа — `In Review`.

## 3. Проверки

| Что | Команда | Критерий |
|---|---|---|
| Юнит-тесты модуля | `cd grafana && go vet ./... && go test ./...` | exit 0 |
| Детерминизм | `make grafana-build` ×2 + `git diff -- dashboard/` | пусто |
| Общий контур | `make all` | exit 0 |
| Дашборды | `make grafana-validate` | exit 0 |
| Доки | `make docs-check` | exit 0 |
| Линт артефакта | `gcx dev lint run …` | «No violations found» |
| Живой SQL | MCP `run_query` под `kimi_reader` | без ошибок; пустой результат на пустой ленте — в протокол |
| Smoke | `make grafana-deploy-staging` | 4 uid, первые панели непусты |

## 4. Риски

- **Локальная лента пуста** — smoke `transaq-trades` на локальном стенде не покажет точки;
  приёмка переносится на staging, локально фиксируются сборка/тесты/линт/валидация SQL.
- **`$__timeInterval` и календарные бакеты** — секундный шаг выравнивается по UTC; при окне
  7 дней шаг внутридневной, а дневные цифры берутся из дневной витрины. Для будущих графиков
  с календарным днём — только unit-форма `toStartOfInterval` (правило 3 скила).
- **Пустая ETF-панель** — не переносится; `TQTF` нет в подписке, ОФЗ заменяет её. Расширение
  подписки — отдельный пункт-следствие.
- **Чужой uid при деплое** — `Validate` параметризован; тест ловит опечатку.
