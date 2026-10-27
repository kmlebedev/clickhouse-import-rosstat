# План: `__timeFilter` по умолчанию и weekly SA на панели 4

**Дата:** 2026-10-22. **Спека:** [2026-10-22-dashboard-timefilter-weekly-sa-design.md](../specs/2026-10-22-dashboard-timefilter-weekly-sa-design.md). **Issue:** #357. **База:** `origin/main` `8f47c0e`. **Ветка:** `feat/key-rate-dashboard-timefilter`.

## Шаги

1. **Панель 2:** `krWeeklyAnnualizedTemplate` — подзапрос `SELECT %s AS time, value` + внешний `$__timeFilter(time)`; фильтр по вычисленному времени, а не по сырому `date` (ряд B сдвинут на +1 год); комментарий шаблона обновить.
2. **Панель 4:** `krWeeklySAARSQL` (`v_weekly_cpi_sa`, `weekly_cpi:saar`, `value / 100`, время `if(partial = 1, as_of, date)`, `$__timeFilter(as_of)` — иначе nowcast с меткой 2026-10-31 выпадает из окна `to=now`); таргет `C`; `Description` — weekly headline/nowcast/тарифная оговорка и ожидаемый разброс 5–9 п.п. против core.
3. **Тест:** `TestKeyRateAllTimeseriesUseTimeFilter` — по `panelsOf`, только `p.Type == "timeseries"`, каждый `CHQuery.RawSql` содержит `$__timeFilter`; `TestKeyRateWeeklyYearOverlay` — у ряда B фильтр на `time`, не на `date`; отдельный тест панели 4 на 3 таргета/серию `weekly_cpi:saar`.
4. **Документация:** `docs/GRAFANA.md` §«Дашборд как код»; `ARCHITECTURE.md` §6.7; `.kimi-code/skills/dashboard-migration/SKILL.md` (шаг 7/конвенции); `docs/KEY_RATE_PUBLICATIONS.md` §11 (панель 2 — фильтр, панель 4 — 3 серии + оговорка).
5. **Сборка/проверка:** `make grafana-build` (детерминизм), `make grafana-validate`, `make all`, `make docs-check`; live SQL панелей 2/4 через MCP.
6. **PR:** `Closes #N`; спека/план, SQL-контракт, тест, артефакт, протокол live SQL; деплой — owner-assisted.

## Границы

Витрины/DDL/гранты не меняются; панели 36–39 по составу не меняются. Оценка — 1 person-day.
