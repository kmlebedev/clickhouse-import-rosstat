# План: теплокарта нетто-потоков Transaq (`ofz-market/1.9`)

**Спека:** [2026-11-09-transaq-trades-heatmap-design.md](../specs/2026-11-09-transaq-trades-heatmap-design.md).
**Пункт:** `ofz-market/1.9`, issue #554. **Ветка:** `feat/transaq-trades-heatmap`, worktree `.worktrees/transaq-trades-heatmap`.

## Шаги

1. **Доки первыми коммитами.** Роадмап 1.9, `ROADMAP_INDEX` (схема `1.1`–`1.9`), эта спека и план — отдельный docs-коммит до кода.

2. **`grafana/transaq_trades.go`.**
   - Константа `ttHeatmapSQL` — дословно спека §4; `instrument_kind IN ('share','fund','ofz','other')`, фьючерсы исключены.
   - Константа `ttHeatmapJS` — `getOption` двух ECharts `treemap` (спека §5): слева «Акции и фонды», справа «ОФЗ по срокам», у каждой карты своя шкала площади; поля по именам, `leafDepth: 2`, `sort: false`, `nodeClick: false`, `breadcrumb.show: false`, `upperLabel` с `formatter: '{b}'`, per-item `itemStyle.color` по `ratio`, `graphic`-подпись.
   - `ttHeatmapPanel()` — `echartsPanel(6, …)`, `tableQuery("A", ttHeatmapSQL)`, `Unit(units.Rubles)`, `Decimals(0)`, `Height(16)`, `Span(spanFull)`.
   - В `NewTransaqTrades()` — ряд 23 «Теплокарта периода» первым, затем панель 6, затем существующие ряды 21/22 и панели 1–5. Существующие id не меняются.

3. **`grafana/transaq_trades_test.go`.**
   - `TestTransaqTradesHasFivePanels` → шесть панелей; `transaqTradesRowTitles` → три ряда.
   - `TestTransaqTradesSQLContract` — оставить кейсы 1–5; добавить `TestTransaqTradesHeatmapSQLContract`: `FROM v_transaq_net_flows_daily`, `FROM v_ofz_issue_analytics`, один `$__dateFilter(d.date)`, `IN ('share','fund','ofz','other')` без `'future'`, нет `LIMIT`, формат `table`/`table`.
   - `TestTransaqTradesHeatmapPanel`: тип `EChartsPanelType`, версия `EChartsPluginVersion`, `editorMode=code`, `renderer=canvas`, `getOption` содержит `treemapSeries`, `'Акции и фонды'`, `'ОФЗ по срокам'` и имена полей; нет `FROM v_`, `$__dateFilter`, `$__timeFilter`, литеральных тикеров.
   - `TestTransaqTradesUnits` — панель 6 тоже `currencyRUB`/0 (ставим Unit/Decimals в билдере).
   - Существующие `TestTransaqTradesNoRawTablesAndNoFill`, `TestTransaqTradesNoTemplating`, `TestTransaqTradesValidateUID` должны проходить без изменений (кроме числа панелей в первом тесте).

4. **Сборка артефакта.** `make grafana-build`; повторный прогон — `git diff -- dashboard/` пуст.

5. **Синк доков.** `docs/GRAFANA.md` — 6 панелей в 3 рядах, две теплокарты; `ARCHITECTURE.md` §6.7 — то же и `v_ofz_issue_analytics`; `docs/ROADMAP_OFZ_MARKET.md` — статус `In Progress` → `In Review` при открытии PR.

6. **Проверка.**
   - `make all`, `make grafana-validate`, `make docs-check`.
   - `gcx dev lint run --disable uneditable-dashboard --disable panel-title-description dashboard/transaq-trades.json`.
   - `make import STAT=transaq_net_flows` (локальная лента пуста, витрины пересобираются).
   - SQL панели 6 на staging (192.168.3.9) под `kimi_reader` с явным диапазоном (`date >= '2026-11-03' AND date <= '2026-11-09'`): 104 строки, 7 кластеров (акции 44, фонды 3, ОФЗ 57 по 5 бакетам), фьючерсов нет; локально — 0 строк без ошибок; `EXPLAIN ESTIMATE`; тенор-выражение на `v_ofz_issue_analytics` — 5 бакетов.
   - Node-харнесс: синтетический фрейм + 104 реальные строки staging → два `treemap`-ряда, цвета, подписи, tooltip.

7. **Визуальный smoke.** `make grafana-up` (если стенд не поднят), `make grafana-deploy-test`; на dev данных нет — панель пустая, это ожидаемо. Для визуальной проверки — временный smoke-дашборд `transaq-trades-smoke` с 104 реальными строками staging (вне коммита), скриншот: два заголовка карт, заголовки кластеров (`Акции`, `Фонды денежного рынка`, `≤1 года`, `1–3`, …), цвета покупок/продаж. Реальная приёмка — staging (owner-assisted): `make grafana-deploy-staging`, smoke, скриншот панели; если недоступно — «не проверено» в PR.

8. **PR.** Коммиты: docs (роадмап + спека + план), код (панель + тесты), артефакт + синк доков. Push, `gh pr create`/MCP, `Closes #554` вне backticks; описание — пункт, источник, схема, запуск, проверка.

9. **Ревью.** Субагент `qwencloud/qwen3.8-flash` (`subagent-dispatch`, task-reviewer) читает diff PR, отдаёт findings с severity/file:line; findings разбираются, фиксы — отдельным коммитом, повторный scoped re-review. Итог — комментарием в PR. Финальное ревью ветки — `deepseek/deepseek-flash`.

10. **Приёмка.** CI `dashboards` зелёный; протокол живой приёмки — комментарием в PR (команды, строки, даты, staging smoke); статус роадмапа — `🔶 In Review` до мержа, `✅` после мержа и протокола (мерж — владелец).

## Риски и контроль

- **Локальная витрина без `instrument_kind`.** До `make import STAT=transaq_net_flows` SQL панели 6 на dev упадёт по неизвестной колонке; шаг 6 обязателен до MCP-проверки.
- **ECharts-ошибки не ловятся Go-тестами.** Компенсация — Node-харнесс и визуальный smoke; при недоступном стенде в PR честно «не проверено».
- **Срок ОФЗ из `v_ofz_issue_analytics`.** Если выпуск отсутствует в аналитике, он попадёт в «ОФЗ (срок н/д)», а не в «до 1 года»; это проверяется тестом SQL на `nullIf`.
- **Фьючерсы задавливают cash-карту.** На staging 52% оборота; поэтому две карты и `future` вне SQL. Если владелец захочет фьючерсы — отдельная панель/пункт, не смешивать с акциями и ОФЗ.
- **Корпоративные облигации.** В ленте их нет; теноры для `TQCB` не выдумываются — отдельный пункт после расширения подписки.
