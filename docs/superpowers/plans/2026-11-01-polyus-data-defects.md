# План: #484 — дефекты данных Полюса (Polyus-часть)

> Спека — [2026-11-01-polyus-data-defects-design.md](../specs/2026-11-01-polyus-data-defects-design.md). Пункт `dcf-polyus/phase-3-remaining-data-defects-2026-11`, issue [#484](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/484), роль `go`. Ветка `fix/polyus-data-defects-484`, worktree `.worktrees/polyus-data-defects-484`.

## Задачи

1. **Дата отчётного года в DCF.**
   - `dcf/mine_plans_import.go`: `actualProductionSelect` → `AND date = makeDate({year:UInt16} + 1, 1, 1)`; комментарии — «годовое наблюдение — 1 января следующего года».
   - `views/company.go`: комментарий колонки `date` в `v_company_operating`.
   - `ARCHITECTURE.md` §6.1 (`dcf_mine_plans`) и §6.3 (`v_company_operating`): убрать `toYear(date)`, зафиксировать конвенцию.
   - `polyus/datapack_test.go`: тест FY2024 → `2025-01-01`, 4Q'07 → `2008-01-01`, 2H'24 → `2025-01-01`.
   - `dcf/mine_plans_import_test.go`: тест формы запроса — `makeDate`, запрет голого `toYear(date)`.

2. **Границы блоков датапака.**
   - `polyus/datapack.go`: нормализация метки, alias `VERNINSKOYE` → `VERNINSKOYE2`; неизвестная all-caps метка после первого актива — ошибка; дубли ключа `(table, name, data, date)` — ошибка.
   - `polyus/datapack_test.go`: временная копия FY2025-фикстуры с `B84 = VERNINSKOYE**`; `VERNINSKOYE2` `Ore processed` 2021-01-01 = 3277, у `TITIMUKHTA` нет строк после 2020, дублей нет. Отдельный тест: неизвестная all-caps метка → ошибка.
   - `ARCHITECTURE.md:431`, `docs/IMPORTERS.md` — alias/guard.

3. **Решения по 2H25 и 1H26.**
   - `docs/ROADMAP_DCF_POLYUS.md`: пункт `phase-3-remaining-data-defects-2026-11` — `🔶`; два follow-up (2H25, 1H26-датапак/метрики).
   - Комментарий в issue #484 с решениями и ссылками.

4. **Приёмка.**
   - `make all`, `make docs-check`, `dagu validate dagu/financial.yaml dagu/dcf.yaml`.
   - Dev: `make import STAT=databook_polyus` (8 484 строки), `make import STAT=dcf_mine_plans` (OLIMPIADA 926.5), `DCF_RUN_ID=<uuid> make import STAT=dcf_engine` (36 строк); NAV до/после в `v_dcf_assumptions`.
   - Staging: TRUNCATE `databook_polyus` + reimport; `TITIMUKHTA` без строк после 2020; `dcf_mine_plans` — 926.5; `dcf_engine` — 36 строк; `make mcp-check`.
   - Протокол — комментарием в PR.

## Порядок

- Спека и план — первые коммиты ветки.
- Задача 1 → task-review → fix-loop; задача 2 → task-review → fix-loop; задача 3 → task-review; финальное ревью ветки.
- Один PR в конце, после зелёного `make all` и приёмки.

## Модели диспатча

- Имплементеры задач 1–3 — `deepseek/deepseek-flash` (многофайловые).
- Task-reviewer — `anthropic/haiku`; fix-раунды 1–3 — `joingonka/zai-org/GLM-5.3-Flash` для одного файла, иначе deepseek.
- Финальное ревью — `deepseek/deepseek-flash`.
