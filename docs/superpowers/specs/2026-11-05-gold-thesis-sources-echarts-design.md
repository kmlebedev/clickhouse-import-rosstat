# Дизайн: групповой импорт контура gold-thesis и ECharts-панели обновления статьи 2026-11-05

- Пункт: `gold-thesis/phase-4-followup-9` (issue #533)
- Дата: 2026-11-05
- Роль: go (Makefile + views + grafana-модуль)

## Контекст

Дашборд `gold-thesis` читает девять витрин, питаемых семью импортёрами, но одной команды обновления контура нет — в отличие от ключевой ставки (`make import STAT=key_rate_sources`). Локально из-за этого протухают ряды (DFII10). Статья `docs/Золото_обновление_2026-11-05.md` опирается на таблицы и цифры, часть которых на дашборде не показана.

## Решения

### 1. Группа импорта `gold_thesis_sources`

Переменная `STATS_gold_thesis_sources` в Makefile (по образцу `STATS_key_rate_sources`, механизм `$(STATS_$(STAT))` рецепта `import` подхватывает её без правок):

```
gold_prices,fred,eia,bls,wgc_etf_flows,wgc_cb_reserves,bold_etf_flows
```

- `gold_prices` → `v_gold_prices` (панели 1–3);
- `fred` → `v_fred_macro` (DFII10; DGS10/T5YIE — новая панель 10);
- `eia` → `v_eia_petroleum` (панель 4);
- `bls` → `v_bls_macro` (панель 5);
- `wgc_etf_flows` → `v_gold_etf_flows` (панель 6);
- `wgc_cb_reserves` → `v_cb_gold_dynamics` (панель 7 и новая панель 13);
- `bold_etf_flows` → `v_gold_etf_flows_daily` / `v_gold_etf_funds_daily` (новая панель 12).

Осознанно вне группы (решение владельца):

- `moex_market` — тянет всю биржу; Brent (`v_moex_market`, `brent_front`) обновляет контур MOEX / `key_rate_sources`;
- manual/webbridge-ряды `v_gold_dashboard` (`dxy`, `crack_ulsd_proxy`, `fedwatch_dec_hike`, `etf_flows_month`) — сессионный сбор WebBridge / ручной ввод, не импортёры;
- `v_gold_forecasts` / `v_forecast_accuracy` — пишутся через ingest, не импортёры.

### 2. Кварталы в `v_cb_gold_dynamics`

Таблица «Семь стран» статьи требует III и II квартал; витрина отдаёт только MoM/YTD/уровень. Панели обязаны читать только `v_*` (тест `TestAllTargetsUseViews`), литеральные данные в JS запрещены (конвенция `grafana/echarts.go`). Расширение витрины (VIEW, обратная совместимость — добавление колонок):

- `q_change_tonnes` — сумма месячных дельт квартала последней точки страны (`toStartOfQuarter(d) = toStartOfQuarter(md)`);
- `prev_q_change_tonnes` — сумма дельт предыдущего квартала (`toStartOfQuarter(d) = toStartOfQuarter(addMonths(md, -3))`).

Оговорки: RUS — по фактическим точкам (пропуски 2025-12…2026-03), как у YTD; страна без рядов — NULL, не 0 (конвенция `if(n > 0, …, NULL)`).

### 3. Пять ECharts-панелей (Business Charts 7.2.5, хелпер `echartsPanel()`)

Существующие панели 1–9 не меняются. Новые id 10–14, новый ряд «Ставки, нефть и верификация» (row id 16, три панели по 8 колонок); панели 12–13 — в существующий ряд «Спрос: ETF и ЦБ».

| id | Панель | Источник | Покрывает из статьи |
|---|---|---|---|
| 10 | Ставочный контур США: DGS10 / DFII10 / T5YIE | `v_fred_macro`, линии, % годовых | DGS10 4,889%; T5YIE 2,39→2,30; DFII10 (октябрь −28 б.п.) |
| 11 | Золото vs Brent | `v_gold_prices` + `v_moex_market` (`brent_front`, правая ось) | Brent −8,7% / $94,62. Оговорка: фронт-месяц MOEX, не спот |
| 14 | Индикаторы верификации сценария | `v_gold_dashboard`, gauge-тройка DXY/FedWatch/crack + stale | DXY 101,26 (порог >102); FedWatch 77% (>65/>80) |
| 12 | ETF-потоки дневные, bold (73 фонда) | `v_gold_etf_flows_daily`: bar потоков + линия запасов (правая ось) | +110,3 т 02.11 (аномалия), −8,8 т 03.11, запасы 3 528→3 638. Оговорка ≈83% мирового рынка WGC |
| 13 | ЦБ: покупки по кварталам, 7 стран | расширенная `v_cb_gold_dynamics`: grouped bar II кв / III кв / янв–сен | таблица «Семь стран» (Китай III кв +63,1 и т.д.) |

Конвенции: тип `volkovlabs-echarts-panel`, `PluginVersion = EChartsPluginVersion`, данные только из таргетов ClickHouse через `context.panel.data.series`, литеральных данных в JS нет, русские алиасы колонок, описания панелей с оговорками.

## Вне объёма

- WGC Gold Demand Trends (ювелирка 313 т, слитки 268 т, итог 1 369 т, ЦБ-мир 321 т) — импортёра нет (goldhub за Cloudflare); отдельный будущий пункт (импортёр GDT или ручной ввод через ingest). Решение зафиксировано в Q1 issue #533.
- VIX/Dow/серебро статьи — новостные цифры без источника в БД, панелей не будет.

## Приёмка

- `make import STAT=gold_thesis_sources` — все семь импортёров прогоняются в одном процессе; DFII10 обновлён (сверка `max(date)` через MCP).
- `make grafana-build` меняет только `dashboard/gold-thesis.json`; `make grafana-validate` и `make all` зелёные.
- Кварталы `v_cb_gold_dynamics` сходятся со статьёй: III кв Китай ≈ +63,1 т, Польша ≈ +40,5 т, Турция ≈ +1,5 т, РФ ≈ −18,7 т.
- `make grafana-deploy-test` + `make grafana-check` — дашборд на локальном стенде, smoke зелёный.
