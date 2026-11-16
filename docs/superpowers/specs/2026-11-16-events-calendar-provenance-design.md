# Провенанс строк календаря событий — #628

Пункт `key-rate/phase-4-events-calendar-provenance`, role:go. Контекст — дефект Ф3 бэктеста
`economist-oracle` (#622; отчёт `docs/research/2026-11-16-economist-oracle-backtest-data-gaps.md`
в PR #623).

## Дефект

В `v_events_calendar` лежат рукописные аналитические строки `category='cbr'` без даты
создания: у 2026-12-18 «СД ЦБ РФ: база — −25 б.п. до 13,75% + ребалансировка индекса MOEX
(один день)», у 2026-10-23 «СД ЦБ РФ: ставка (14%) — снижение → флаг перетока
ликвидности». Это не события календаря, а чужой прогноз/факт: в бэктесте строка подсказывает
исход будущего заседания, а отличить её от канонического расписания ЦБ нельзя — ни источника,
ни даты записи у строки нет.

Канонические строки (118 заседаний 2014-02-14 … 2027-12-17) импортёр `events_calendar` берёт
из `cbr.ru/dkp/cal_mp/`; их title ровно «СД ЦБ РФ: заседание по ключевой ставке». Остальные
категории (`eia`, `cpi`, `fomc`, `wgc`, `bank_results`, `polyus_ir`, `gov_rf`…) — ручные
триггеры из сидов `sql/events_calendar*_q4_2026.sql`; они легитимны, но тоже без провенанса.

## Схема

`events_calendar` получает три колонки (канонический DDL — `ARCHITECTURE.md` §6.2):

```sql
source LowCardinality(String) DEFAULT 'manual',   -- canonical — импортёр; manual — сид
created_at DateTime('Europe/Moscow') DEFAULT now(),
retired UInt8 DEFAULT 0                           -- 1 — выведена из календаря
```

Миграции живого стола (идемпотентны, прецедент — `ARCHITECTURE.md` §6.1/§6.2):

1. `ADD COLUMN IF NOT EXISTS` ×3.
2. Если `created_at` только что добавлена — `MATERIALIZE COLUMN created_at`: у старых строк
   DEFAULT считается при чтении, и `created_at` показывал бы время запроса; материализация
   фиксирует реальный момент бэкфилла.
Частичная миграция: если прогон прервётся между `ADD COLUMN created_at` и `MATERIALIZE COLUMN`, следующий прогон материализацию не повторит (gate — по наличию колонки); восстановление — вручную `ALTER TABLE events_calendar MATERIALIZE COLUMN created_at`.

3. Один раз пометить канонические строки: `UPDATE source='canonical' WHERE category='cbr'
   AND title='<канонический>' AND source='manual'` (после первого прогона — 0 строк).
4. Один раз вывести рукописные `cbr`-заметки: `UPDATE retired=1 WHERE category='cbr'
   AND title != '<канонический>' AND retired=0`. Категория `cbr` принадлежит импортёру;
   ручная заметка в ней — не событие. Данные не удаляются: строка остаётся в таблице с
   `retired=1` и видна только в сыром столе.

## Импортёр

`calendar/events.go`: вместо повторной вставки всех 118 заседаний каждый прогон — дельта:

- читает существующие канонические строки (`event_date`, `emergency`, `created_at`) из `FINAL`;
- вставляет отсутствующие (`source='canonical'`, `created_at=now()`, `retired=0`);
- при смене `emergency` вставляет новую версию с **сохранённым** `created_at` — дата первой
  записи не переписывается;
- повтор — `Imported 0 rows`; недоступность ЦБ по-прежнему деградация (warn), ошибка разбора —
  ошибка прогона.

## Витрина

`v_events_calendar` — явные колонки (без `retired`) и `WHERE retired = 0`:

```sql
SELECT event_date, event_time, category, title, threshold, status, emergency, source, created_at
FROM events_calendar FINAL WHERE retired = 0
```

`source` и `created_at` описаны для MCP-агента; в комментарии таблицы — правило: каноническое
расписание ЦБ читать по `title = 'СД ЦБ РФ: заседание по ключевой ставке'` или
`source='canonical'`, ручные триггеры — `source='manual'`, для as-of — `created_at <= as_of`.

## Сид

`sql/events_calendar_q4_2026.sql`: две рукописные `cbr`-строки удаляются из сида (2026-10-23,
2026-12-18); DDL-копия в шапке получает новые колонки. `events_calendar_bank_results_*.sql`
не меняется: его строки получают `source='manual'` по умолчанию.

## Инварианты

- В `v_events_calendar` нет строк `category='cbr'` с title, отличным от канонического.
- Все 118 канонических строк — `source='canonical'`, `created_at` не пустой.
- Ручные триггеры других категорий остаются видимыми с `source='manual'`.
- `emergency`, `status`, `threshold` и сами даты заседаний не меняются.
- Повторный прогон ничего не пишет.

## Приёмка

- Unit: контракт DDL/миграций/`INSERT`/витрины; тест дельты (новая дата, существующая без
  изменений, смена `emergency`).
- Dev: `make import STAT=events_calendar` — миграция, 118 помеченных канонических, 2 выведенные
  заметки, вставок 0; повтор — 0.
- SQL: `v_events_calendar` — 118 строк `category='cbr'` (все с каноническим title), 0 рукописных
  `cbr`; `source='manual'` у `eia`/`bank_results`; `created_at` у всех строк.
- `make all`, `make docs-check`, `make mcp-check` (новые проверки провенанса).
