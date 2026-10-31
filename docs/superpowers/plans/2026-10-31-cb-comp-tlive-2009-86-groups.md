# План Task 15 (#465): `T_LIVE@2009` — 86 групп

> Спека — [2026-10-31-cb-comp-tlive-2009-86-groups-design.md](../specs/2026-10-31-cb-comp-tlive-2009-86-groups-design.md). Ветка `exp/cb-comp-88-groups-tlive-2009-weights`, worktree `.worktrees/cb-comp-88-tlive-2009-weights`. Роль `ml`.

## Шаги

1. **Snapshot + спека/план** — этот коммит: JSON весов 2008/2009 с URL/sha256, протокол до прогона.
2. **Тесты (TDD, офлайн).** `cb_comp_groups_panel_test.py`: загрузка snapshot; `leaf_weights` 2008/2009; `leaf_fallback_start=2008-12` для `@2009` и 2010-01 для `@2010`; `group_nsa` для `1710`/`1730` в 2008-12/2009; неизменность `@2010`. Тесты без ClickHouse/сети.
3. **Код.** `cb_comp_groups_panel.py`: `VERSION_PRESETS["2009"]["leaf_fallback_start"]`, загрузка early weights, `group_nsa`/`fit_series_map`/`run` передают версионный fallback; JSON панели — `early_weights` sha. `@2010`-ветка не меняет значения.
4. **Прогон панели.** `make cb-comp-groups-panel-test`; `@2010` (регрессия); `ARGS="--version 2009"`; проверить 86 групп/0,937/`T_LIVE[1]` 2009-01; повтор для детерминизма. As-of — follow-up.
5. **Модели.** Task 2 `--version 2009 --skip-asof`, Task 3 `--version 2009`, Task 9 `--version 2009`; обновить `TASK3_PINS["2009"]` по фактическим Task 3 и `R10_PINS["2009"]` по Task 9; Task 4 `--frozen-only`. Сравнить с 84-групповой версией и `@2010`.
6. **Отчёт.** `docs/research/2026-10-31-cb-comp-tlive-2009-86-groups.md` + JSON: покрытие, T_LIVE, model RMSE/ratios, G2/G9, дельта к 84-групповой, «что не проверено».
7. **Документы.** Карта модели `@2009` 86 групп/0,937, оговорка про веса; роадмап Task 15; старые отчёты не переписывать.
8. **Приёмка.** `make all`, `make docs-check`; протокол комментарием в PR; Project V2 In Review; PR `Closes #465`.
