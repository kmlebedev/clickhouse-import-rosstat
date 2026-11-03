# План: `infra/5.14` — ECharts-панели в `grafana/`

**Дата:** 2026-11-03
**Спека:** [2026-11-03-echarts-panels-design.md](../specs/2026-11-03-echarts-panels-design.md)
**Issue:** [#516](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/516)
**Ветка:** `feat/infra-5.14-echarts`; ворк-дерево `.worktrees/infra-5.14`

## Шаги

1. ✅ Роадмап: пункт 5.14 в `docs/ROADMAP_INFRA.md` §6, схема ID `5.1–5.14` в `docs/ROADMAP_INDEX.md` §1; issue #516 (labels `roadmap:infra`, `role:go`), Project V2 (`In Progress`, ветка); claim-комментарий.
2. ✅ Спека и план — первыми коммитами ветки.
3. **Dev-стенд:** установить `volkovlabs-echarts-panel` через `grafana cli --homepath /opt/homebrew/share/grafana --pluginsDir ~/grafana-dev/plugins plugins install volkovlabs-echarts-panel`; перезапустить dev-стенд (`make grafana-down && make grafana-up` — локальный стенд владельца, действие согласовано планом).
4. **Живой экспорт формата:** создать на dev-стенде через HTTP API временную дашборд-панель Business Charts с SQL контракта спеки §4, проверить рендер (PNG через image renderer), экспортировать JSON панели, зафиксировать ключи `options`; удалить временную дашборду.
5. **Makefile:** `GRAFANA_ECHARTS_PLUGIN_VERSION` + автоустановка в `grafana-up` по образцу clickhouse-datasource (Makefile:1515–1518).
6. **Код:** `grafana/echarts.go` (билдер custom-панели, ключи options по шагу 4) + панель 41 «Недельный ИПЦ — теплокарта» в `key_rate_publications.go` (ряд «Ставка и инфляция», после панели 4; SQL — спека §4 дословно).
7. **Тесты:** панель 41 — тип, id, позиция; таргет — `FROM v_rosstat_macro`, `$__dateFilter`, без `LIMIT`; детерминизм двойной сборки; `make grafana-validate`.
8. **Доки:** `ARCHITECTURE.md` §6.7 (плагин-зависимость, конвенция custom-панелей, шапка — дата/пункт).
9. **Проверки и PR:** `make grafana-build` (повторный — пустой дифф), `make grafana-validate`, `make grafana-deploy-test`, `make grafana-check`, `make docs-check`, `make all`; PR с `Closes #516`, протокол живой приёмки — комментарием в PR; staging-установка плагина — действие владельца (запись в PR).

## Примечания

- До первого коммита кода проверить живые данные ряда через MCP (762 точки, 2009-04-06…2026-10-26 — подтверждено при написании спеки).
- Candlestick на transaq не делается (нет OHLC-зерна) — см. границы спеки §2.
