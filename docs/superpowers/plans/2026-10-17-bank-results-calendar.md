# План реализации: календарь раскрытия топ-5 банков и форвардная витрина

**Спека:** [2026-10-17-bank-results-calendar-design.md](../specs/2026-10-17-bank-results-calendar-design.md).
**Пункт роадмапа:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №20.
**Issue:** [#86](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/86).

## Задачи

### T1. Разведка источников и сид `bank_results`

**Файлы:**
- `sql/events_calendar_bank_results_q4_2026.sql` — новый идемпотентный сид;
- `calendar/events.go` — комментарий DDL (`:26`), комментарий витрины (`:55`), комментарий колонки `category` (`:60`);
- `sql/events_calendar_q4_2026.sql` — комментарий DDL (`:8`);
- `ARCHITECTURE.md` — комментарий DDL в §6.2 (`:557`).

**Шаги:**
1. Для каждого кандидата (Сбер, ВТБ, Т-Банк, Совкомбанк, МКБ) собрать источник дат:
   - ВТБ — `https://www.vtb.ru/ir/calendar/` (машиночитаем); подтверждённая будущая дата — 30.10.2026 (3 кв./9 мес. 2026); исторические 2026 — 25.02, 26.03, 28.04, 28.05, 26.06, 28.07, 28.08, 29.09.
   - Сбер — IR-страница «Навигатор» через JS-challenge; проверить, есть ли публичный ряд помесячных пресс-релизов по РСБУ; xls-книга в БД до 2026-06, помесячной её считать нельзя.
   - Т-Банк — страница «Финансовая отчетность» через JS; видимые даты поквартальные (1П2026 — 26.08.2026, 1 кв. 2026 — 01.06.2026); помесячность подтвердить или отклонить.
   - Совкомбанк — антибот 401; МКБ — раздел инвесторам требует разведки.
2. Для каждого подтверждённого банка заполнить Q4-2026: отчётные месяцы 2026-09, 2026-10, 2026-11; `event_date` — дата публикации; `threshold` — по §3.1 спеки.
3. Для ВТБ: 30.10.2026 — `confirmed`; ноябрь/декабрь — `expected` по медиане фактических дат (28.11.2025, 26.12.2025, 28.11.2024, 26.12.2024 и т. п.), с `basis`.
4. Банки без источника — не добавлять в сид; перечислить в отчёте задачи как кандидатов.
5. Обновить три копии комментария DDL и комментарии витрины.

**Проверка:** `go test ./calendar/`; SQL-файл применяется на dev-БД через `make ch-sql FILE=...`; повторный прогон не меняет `count() FINAL`.

### T2. Витрина `v_bank_loan_portfolio_forward`

**Файлы:**
- `views/company.go` — новый `util.View` и добавление в `companyViewsDDLViews()` после `v_company_financials`;
- `views/company_test.go` — контракт витрины; при необходимости — правка `selectedColumns` под `WITH`;
- `sql/mcp_kimi_reader.sql` — `GRANT SELECT`;
- `scripts/mcp_check.py` — проверка витрины и конкретной точки.

**Шаги:**
1. SQL по §4 спеки: CTE `bank`/`cal`/`cbr`, `lagInFrame`, `JSONExtractString`, защита от non-Nullable LEFT JOIN.
2. Комментарии таблицы и каждой выданной колонки.
3. Тест: имя, `Tables`, источники в `SELECT`, `period_type='M'`, три метрики, `lagInFrame`, `JSONExtractString(threshold, 'bank')`, комментарии всех колонок.
4. `make import STAT=company_views` на dev-БД; проверить `SELECT * FROM v_bank_loan_portfolio_forward WHERE bank='SBER' ORDER BY report_month DESC LIMIT 3`.
5. MCP-проверка: `count() > 0` и последняя точка SBER `customer_loans` с `publish_date`.

### T3. Документация и статус

**Файлы:** `ARCHITECTURE.md` §6.2/§6.3/§7, `README.md`, `docs/KEY_RATE_CHANNELS.md` §3/§8, `docs/ROADMAP_KEY_RATE.md` §4.1/§4.2/§6.

**Шаги:**
1. Описать витрину и категорию `bank_results`; указать пилот SBER/VTBR и причины (JS/антибот).
2. В §4.1 добавить строку снимка; в §4.2 — gap №20; в §6 — шаг фазы 2.
3. После приёмки — статус ✅ с командами, числами и датой.

## Порядок прогона и приёмка

```bash
make all
make ch-sql FILE=sql/events_calendar_bank_results_q4_2026.sql
make ch-sql FILE=sql/events_calendar_bank_results_q4_2026.sql   # повтор — идемпотентен
make import STAT=events_calendar
make import STAT=company_views
make mcp-user
make mcp-check
dagu validate dagu/calendar.yaml
```

Проверки:
- `events_calendar`: `SELECT category, count() FROM events_calendar FINAL GROUP BY category` — `bank_results` есть, `cbr` не изменился;
- витрина: `SELECT bank, metric, report_month, value_bln_rub, mom_pct, yoy_pct, publish_date FROM v_bank_loan_portfolio_forward ORDER BY report_month DESC LIMIT 10`;
- сырые таблицы закрыты агенту (существующие проверки `mcp_check.py`).

## Ревью

1. Task-review — ревьюер не на модели, писавшей код.
2. Fix-раунды — по замечаниям.
3. Финальное ревью ветки — `superpowers:requesting-code-review`.
4. `make all` + живая приёмка + `make mcp-check` — до PR.
5. Протокол приёмки — комментарием в PR; статус ✅ в роадмапе тем же PR.
