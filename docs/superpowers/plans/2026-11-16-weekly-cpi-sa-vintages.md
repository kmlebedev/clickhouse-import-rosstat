# План: винтажи недельного SA-ИПЦ (#630)

Спека — [2026-11-16-weekly-cpi-sa-vintages-design.md](../specs/2026-11-16-weekly-cpi-sa-vintages-design.md).
Ветка `feat/key-rate-weekly-cpi-sa-vintages`, worktree `.worktrees/weekly-cpi-sa-vintages`.

## Шаг 1. Схема и миграция

`weeklysa/schema.go`:

- `weeklySADdl` += `vintage Date`, `computed_at DateTime('Europe/Moscow')`;
  `ORDER BY (date, series, method, spec_version, vintage, computed_at)`.
- `weeklySAAlters` += два `ADD COLUMN IF NOT EXISTS`.
- `weeklySAVintageBackfill` — `UPDATE vintage = as_of, computed_at = loaded_at WHERE
  vintage='1970-01-01'` + guard-запрос (однократно).
- `weeklySAFeatureVersion` → `weekly-cpi-sa-vintage-v2`.

## Шаг 2. Импортёр

`weeklysa/importer.go`:

- `weeklySARow` += `Vintage`, `ComputedAt`; `buildRows(..., vintage, computedAt)`.
- Перед `buildRows`: `vintage = lastGridDate`, `computedAt = time.Now()`.
- `INSERT` += `vintage, computed_at`.
- Миграция/бэкфилл — в начале `Import` после DDL.
- Гейт по отпечатку не меняется; счётчик `weeklySACountPoints` теперь считает все винтажи —
  при повторе он совпадает с записанным.

## Шаг 3. Витрины

`weeklysa/views.go`:

- `v_weekly_cpi_sa` — фильтр последнего винтажа + колонка `vintage`.
- `v_weekly_cpi_sa_vintage` — все винтажи.
- `v_weekly_cpi_sa_asof` — параметризованная, `published_at < {vintage_as_of:Date}`.
- `v_weekly_cpi_quarterly`/`v_weekly_cpi_rolling3m` — фильтр последнего винтажа.
- `dailyViews`? — нет; импортёр публикует все витрины в `publishWeeklyDerived`.

## Шаг 4. Гранты, проверки, документы

- `sql/mcp_kimi_reader.sql` — `GRANT SELECT` на две новые витрины.
- `scripts/mcp_check.py` — винтажи (`uniqExact(vintage, computed_at) ≥ 2`), вызов
  `v_weekly_cpi_sa_asof`, неизменность `v_weekly_cpi_sa`, сырая закрыта.
- `ARCHITECTURE.md` §6.2 (DDL), §6.3 (витрины и as-of), `docs/IMPORTERS.md`, `docs/ROADMAP_KEY_RATE.md`.

## Шаг 5. Тесты

`weeklysa/importer_test.go`: DDL/ALTER/бэкфилл/INSERT; `buildRows` с винтажом; фильтр последнего
винтажа в трёх витринах; as-of SQL (`{vintage_as_of:Date}`, `published_at <`, тай-брейк);
комментарии колонок (9).

## Шаг 6. Проверка

1. `go test ./weeklysa/ -count=1`.
2. `make all`, `make docs-check`.
3. `make import STAT=weekly_cpi_sa` — новый винтаж; повтор — 0.
4. SQL: `v_weekly_cpi_sa` 211 строк с `vintage`; `v_weekly_cpi_sa_vintage` — 2 винтажа;
   `v_weekly_cpi_sa_asof('2026-11-01')` — 0, `('2026-11-12')` — оба.
5. `make mcp-check`.
6. Протокол — комментарием в PR, `Closes #630`.
