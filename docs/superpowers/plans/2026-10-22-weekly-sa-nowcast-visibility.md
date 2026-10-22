# План: nowcast weekly SA виден в окне дашборда

**Дата:** 2026-10-22. **Спека:** [2026-10-22-weekly-sa-nowcast-visibility-design.md](../specs/2026-10-22-weekly-sa-nowcast-visibility-design.md). **Issue:** #361. **База:** `origin/main` `4412aa7`. **Ветка:** `fix/weekly-sa-nowcast-visible`.

## Шаги

1. **Спека/план** — этот файл и спека, первый коммит ветки.
2. **SQL:** `krWeeklySAARSQL` — `if(partial = 1, as_of, date) AS time`, `$__timeFilter(as_of)`; комментарий константы объясняет, почему не `date`.
3. **Описание панели 4:** ожидаемый разброс 5–9 п.п. против core, nowcast на дате `as_of`, правило чтения (headline, не core).
4. **Тест:** `TestKeyRateInflationPanelWeeklySA` — проверяет обе части SQL; общий `TestKeyRateAllTimeseriesUseTimeFilter` не меняется.
5. **Документация:** `docs/KEY_RATE_PUBLICATIONS.md` §11 (панель 4 — nowcast на `as_of`, разброс, полные месяцы), роадмап — статус пункта.
6. **Сборка/проверка:** `make grafana-build` (детерминизм), `make grafana-validate`, `make all`, `make docs-check`; live SQL при `to=now` через MCP.
7. **PR:** `Closes #361`, спека/план, SQL-контракт, тест, артефакт, live SQL; деплой — owner-assisted.

## Границы

Витрины/DDL/гранты не меняются; панели 1–3, 5–39 и правило `$__timeFilter` не затрагиваются. Оценка — 0,5 person-day.
