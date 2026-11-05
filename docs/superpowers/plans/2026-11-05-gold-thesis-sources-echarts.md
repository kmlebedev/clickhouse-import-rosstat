# План: gold-thesis/phase-4-followup-9 — групповой импорт и ECharts-панели

- Спека: [2026-11-05-gold-thesis-sources-echarts-design.md](2026-11-05-gold-thesis-sources-echarts-design.md)
- Issue: #533; ветка: `feat/gold-thesis-sources-echarts`; worktree: `.worktrees/gold-thesis-sources-echarts`

## Шаг 1. Makefile — группа импорта

`STATS_gold_thesis_sources := gold_prices,fred,eia,bls,wgc_etf_flows,wgc_cb_reserves,bold_etf_flows` + `unexport` + комментарий (состав по рядам дашборда; что не покрыто: manual/webbridge-ряды `v_gold_dashboard`, прогнозы через ingest; Brent — контур MOEX). Рецепт `import` менять не нужно.

## Шаг 2. views/cb_gold_dynamics.go — квартальные колонки

В подзапросе дельт добавить `sumIf` по кварталу последней точки (`md`) и предыдущему (`addMonths(md, -3)`): `q_change_tonnes`, `prev_q_change_tonnes`. Обновить `Columns` и комментарии таблицы. Тест `views/cb_gold_dynamics_test.go` — покрытие новых колонок (SQL-контракт).

## Шаг 3. grafana/gold_thesis.go — пять панелей

Новый ряд «Ставки, нефть и верификация» (row id 16): панели 10 (DGS10/DFII10/T5YIE, `v_fred_macro`), 11 (Золото vs Brent, `v_gold_prices` + `v_moex_market`), 14 (индикаторы верификации, `v_gold_dashboard`). В ряд «Спрос: ETF и ЦБ»: панели 12 (дневные ETF bold, `v_gold_etf_flows_daily`), 13 (ЦБ по кварталам, расширенная `v_cb_gold_dynamics`). Тип `volkovlabs-echarts-panel`, таргеты только `v_*`, данные из data frames, литеральных данных в JS нет.

## Шаг 4. Тесты grafana/

`gold_thesis_test.go`: 14 панелей, 6 рядов, id 1..14; новые заголовки; проверки типа плагина/версии и непустого getOption у панелей 10–14. `TestAllTargetsUseViews` распространяется сам.

## Шаг 5. Артефакт и документация

`make grafana-build` → `dashboard/gold-thesis.json`. `ARCHITECTURE.md`: шапка, §6.7 (состав 9→14 панелей), §6.3 (новые колонки `v_cb_gold_dynamics`). `docs/GOLD_THESIS_DATA_ROADMAP.md`: пункт phase-4-followup-9 (🔶 в работе → ✅ при приёмке).

## Проверка

1. `make grafana-build` — diff только `dashboard/gold-thesis.json`.
2. `make grafana-validate`.
3. `make all`.
4. `make import STAT=gold_thesis_sources` — живой прогон, DFII10 обновлён.
5. MCP ClickHouse: `max(date)` DFII10/DGS10/T5YIE; кварталы `v_cb_gold_dynamics` против статьи (Китай III кв ≈ +63,1 т).
6. `make grafana-deploy-test && make grafana-check`.
