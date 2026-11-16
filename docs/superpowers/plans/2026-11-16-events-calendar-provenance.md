# План: провенанс строк календаря событий (#628)

Спека — [2026-11-16-events-calendar-provenance-design.md](../specs/2026-11-16-events-calendar-provenance-design.md).
Ветка `feat/key-rate-events-calendar-provenance`, worktree `.worktrees/events-calendar-provenance`.

## Шаг 1. Схема и миграции

`calendar/events.go`:

- `eventsDdl` += `source LowCardinality(String) DEFAULT 'manual'`, `created_at
  DateTime('Europe/Moscow') DEFAULT now()`, `retired UInt8 DEFAULT 0`.
- Константы `eventsSourceAlter`, `eventsCreatedAlter`, `eventsRetiredAlter`,
  `eventsCanonicalMark`, `eventsRetireManual` (+ guard-запросы).
- `ensureEventsProvenance(ctx, conn)`: три `ADD COLUMN IF NOT EXISTS`; `MATERIALIZE COLUMN
  created_at` только если колонки не было; однократные UPDATE пометки канонических и вывода
  рукописных `cbr`-заметок; вызов из `Import` после DDL.

## Шаг 2. Дельта-вставка

- `loadCalMPExisting` — `SELECT event_date, emergency, created_at FROM events_calendar FINAL
  WHERE category='cbr' AND title=?`.
- `calMPInsertRows(existing, meetings)` — отсутствующие (created_at=now) и смена `emergency`
  (created_at существующей).
- `calMPInsert` += `source, created_at, retired`; батч только по дельте.
- Комментарий шага dagu: «перезаписывает строки» → «дельта».

## Шаг 3. Витрина и сид

- `eventsView.Select` — явные колонки + `WHERE retired = 0`; `Columns` += `source`,
  `created_at`; комментарий таблицы — правило фильтра для as-of.
- `sql/events_calendar_q4_2026.sql`: убрать две рукописные `cbr`-строки, обновить DDL-копию.

## Шаг 4. Тесты

`calendar/events_test.go`: контракт DDL/ALTER/INSERT/витрины (source/created_at/retired,
`retired = 0`); дельта — новая дата, существующая без изменений, смена `emergency`
(сохранённый `created_at`).

## Шаг 5. Проверки и документы

- `scripts/mcp_check.py`: в `v_events_calendar` нет рукописных `cbr`-строк; 118 канонических с
  `source='canonical'`; ручные триггеры (`eia`/`bank_results`) с `source='manual'`;
  `created_at` заполнен; сырая `events_calendar` закрыта.
- `ARCHITECTURE.md` §6.2 (DDL-копия), §6.3 (`v_events_calendar`), §7 (описание контура);
  `docs/IMPORTERS.md`/`README.md` — где упоминается витрина; `docs/ROADMAP_KEY_RATE.md` §4.2.

## Шаг 6. Проверка

1. `go test ./calendar/ -run 'Events|CalMP'`.
2. `make all`, `make docs-check`.
3. `make import STAT=events_calendar` — миграция и пометки, вставок 0; повтор — 0.
4. SQL-сверка витрины (118 канонических, 0 рукописных `cbr`, `source`/`created_at`).
5. `make mcp-check`.
6. Протокол — комментарием в PR, `Closes #628`.
