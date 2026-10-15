### Task 2: events_calendar Q4-2026 + витрина v_events_calendar

**Files:**
- Create: `sql/events_calendar_q4_2026.sql`
- Create: `calendar/events.go`
- Modify: `main.go` (blank-import пакета calendar)
- Modify: `sql/mcp_kimi_reader.sql`

**Interfaces:**
- Consumes: `util.CreateView(ctx, conn, util.View)` из `util/views.go`; канонический DDL `events_calendar` из ARCHITECTURE.md §6.2 (копировать как есть).
- Produces: таблица `events_calendar` с сидом Q4-2026; витрина `v_events_calendar` (все колонки + комментарии); импортёр `Name() = "events_calendar"`.

- [ ] **Step 1: Написать `sql/events_calendar_q4_2026.sql`**

`CREATE TABLE IF NOT EXISTS events_calendar` (канонический DDL из ARCHITECTURE.md §6.2) + `INSERT INTO events_calendar` одним батчем с событиями из ARCHITECTURE.md §7 (EIA ср еженед. — как recurring-комментарий в title одной записью на ближайшую среду; 23.10 СД ЦБ; 01.11 запрет дизеля; 10.11 CPI; 09.12 FOMC+dot plot; 18.12 СД ЦБ + ребалансировка MOEX; 14.10 CPI сент.; 06.11 NFP; 04.12 NFP; 10.12 CPI; ~конец окт. WGC GDT). `threshold` — JSON-строки по образцу `'{"crack":">50"}'`.

- [ ] **Step 2: Написать импортёр `calendar/events.go`**

Регистрация через `init()`: `Import()` выполняет DDL `CREATE TABLE IF NOT EXISTS events_calendar` (тот же канонический), затем `util.CreateView` для `v_events_calendar` (Select = `SELECT * FROM events_calendar FINAL`, Tables = `["events_calendar"]`, комментарий таблицы «Календарь событий-триггеров прогноза золота/NAV; status: pending|done|verified», комментарии каждой колонки). Сид данных — НЕ в Go (он в sql-файле из Step 1, идемпотентный за счёт ReplacingMergeTree).

- [ ] **Step 3: Добавить грант и blank-import**

`GRANT SELECT ON default.v_events_calendar TO kimi_reader;` в `sql/mcp_kimi_reader.sql`; `_ "github.com/kmlebedev/clickhouse-import-rosstat/calendar"` в `main.go`.

- [ ] **Step 4: Прогнать против живого ClickHouse**

Run: `make import STAT=events_calendar` (ClickHouse поднят пользователем), затем `clickhouse-client --multiquery < sql/events_calendar_q4_2026.sql` через `make`-обёртку с env.
Expected: `SELECT count() FROM v_events_calendar` ≥ 10.

- [ ] **Step 5: Commit**

```bash
git add sql/events_calendar_q4_2026.sql calendar/events.go main.go sql/mcp_kimi_reader.sql
git commit -m "add events_calendar Q4-2026 and v_events_calendar"
```

