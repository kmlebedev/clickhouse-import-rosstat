# Спека: ECharts-панели в `grafana/` — плагин Business Charts и конвенция custom-панелей

**Дата:** 2026-11-03
**Пункт:** `infra/5.14` ([ROADMAP_INFRA.md](../ROADMAP_INFRA.md) §6, issue [#516](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/516))
**Роль:** go
**План:** [2026-11-03-echarts-panels.md](../plans/2026-11-03-echarts-panels.md)

## 1. Контекст и цель

Дашборды репозитория собираются кодом: модуль `grafana/` на `grafana-foundation-sdk v0.0.20`, артефакты `dashboard/<uid>.json`, `make grafana-build` (ARCHITECTURE.md §6.7). Типизированные билдеры SDK покрывают core-панели (timeseries, barchart, table, bargauge); «необычные» графики Apache ECharts (calendar-теплокарта, candlestick, sankey, gauge, markLine/markArea) ими не выразить.

Цель пункта: подключить community-плагин **Business Charts** (`volkovlabs-echarts-panel`, бывший Apache ECharts Panel от Volkov Labs), дать модулю `grafana/` helper custom-панели и собрать одну пилотную панель на живой витрине. Итог — воспроизводимый путь «ECharts-панель как код» по всем конвенциям `dashboard-migration`.

## 2. Границы

- SQL импортёров, схема и гранты не меняются. Панели читают только витрины `v_*`, в каждом таргете `$__timeFilter`/`$__dateFilter`, агрегация в ClickHouse, без `LIMIT` на графиках (памятка «Time-series SQL»).
- Пилот — одна панель. Перенос существующих дашбордов на ECharts не входит.
- Установка плагина на staging — действие владельца (общий ресурс); пункт ею не блокируется, приёмка — dev- и тест-стенд.
- Candlestick-панель на transaq-данных **не делается**: витрины `v_transaq_net_flows*` содержат нетто-потоки по минутам, OHLC-зерна нет (отдельный пункт-следствие, если понадобится).

## 3. Архитектура

1. **Плагин.** `volkovlabs-echarts-panel` — панель, встраивающая полную библиотеку Apache ECharts. Пользовательский код — одна JS-функция `context → option`; данные приходят из тех же data frames ClickHouse-даatasource (`uid ch-rosstat`, тип `grafana-clickhouse-datasource`).
2. **Установка на dev-стенд.** Makefile: автоустановка в цели `grafana-up` по образцу clickhouse-datasource (Makefile:1515–1518), версия фиксируется переменной `GRAFANA_ECHARTS_PLUGIN_VERSION` рядом с `GRAFANA_PLUGIN_VERSION`. Плагин загружается при старте Grafana — dev-стенд перезапускается после установки.
3. **Helper.** `grafana/echarts.go`: билдер custom-панели (`Type = "volkovlabs-echarts-panel"`) с обязательными полями house style — заголовок, datasource, таргет(ы) `NewCHQuery`, сетка; JS-код панели — строковая опция. Точные ключи `options` берутся из живого экспорта панели установленной версии плагина (конвенция Makefile:27 — «формат подтверждён живым экспортом») и фиксируются в комментарии helper'а. Данные не вшиваются в JS: функция читает `context.panel.data`.
4. **Пилот.** Панель 41 «Недельный ИПЦ — теплокарта» в дашборде `key-rate-publications` (uid не меняется), ряд «Ставка и инфляция», после панели 4 (недельные «растущие» позиции). ECharts `calendar` + `heatmap`: неделя — ячейка, цвет — индекс. Данные — официальный недельный агрегат `Индекс потребительских цен (недельная оценка)` из `v_rosstat_macro` (762 точки, 2009-04-06…2026-10-26; ряд уже закреплён константой `krWeeklyAggregateSeries`).

## 4. Контракт пилотной панели

SQL таргета (дословно; `date` — `Date`, фильтр — макрос плагина для Date-колонок):

```sql
SELECT date AS "Дата",
       value AS "Индекс"
FROM v_rosstat_macro
WHERE series = 'Индекс потребительских цен (недельная оценка)'
  AND $__dateFilter(date)
ORDER BY date
```

JS-контракт (форма; точное тело — в коде, опции по живому экспорту):

```js
return async function (context) {
  // один ряд: time (Date→ms) + number; пары [time, value] → calendar-heatmap
  // visualMap: continuous, min/max по данным; tooltip: дата + индекс
  // calendar: диапазон лет по данным, ячейка недели, шапка лет
}
```

Инварианты: функция не содержит литеральных данных и сырых таблиц (только `v_rosstat_macro` в таргете), timezone не конвертируется (отображение — `Timezone(browser)`).

## 5. Тесты (`grafana/key_rate_publications_test.go` и/или `grafana/echarts_test.go`)

- Состав: в `key-rate-publications` появилась панель id 41, тип `volkovlabs-echarts-panel`, заголовок и положение в ряду «Ставка и инфляция».
- Таргет панели 41: `FROM v_rosstat_macro`, `$__dateFilter`, без `LIMIT`, без сырых таблиц.
- Детерминизм: повторная сборка дашборда даёт идентичный JSON.
- `make grafana-validate` зелёный (существующие негативные правила не ломаются).

## 6. Runbook

1. `make grafana-up` — ставит плагин, если отсутствует (первый раз после мержа — перезапуск dev-стенда владельцем или по запросу; плагин подхватывается при старте).
2. `make grafana-build` — артефакт `dashboard/key-rate-publications.json` обновлён; повторный прогон — пустой дифф.
3. `make grafana-validate`; `make grafana-deploy-test`; `make grafana-check` — smoke зелёный.
4. `make all`, `make docs-check` — зелёные.
5. Протокол живой приёмки — комментарием в PR (§16.1 п. 8 стандарта).

## 7. Риски

- Community-плагин вне core: пин версии в Makefile, обновление — отдельным пунктом; плагин подписан Grafana.
- Формат `options` меняется между версиями плагина: зафиксирован живым экспортом и тестом типа панели; при обновлении плагина — сверять экспорт.
- Рендер PNG (snapshot/`grafana-check`): ECharts рисует canvas — image renderer поддерживает; если smoke упадёт на рендере — фиксируется в протоколе как известное ограничение.
