# Дашборд «Полюс (PLZL) — финансы и прогноз»: миграция `finance-polyus` в код + механизм миграции дашбордов — design

**Дата:** 2026-10-16. **Ветка:** `feat/grafana-polyus` (worktree `.worktrees/polyus-dashboard`, от main `1345989` — там уже стандарт PR #70).
**Пункты (регистрируются первыми коммитами этого же PR):**
1. `docs/ROADMAP_INFRA.md`, Фаза 2 «Процесс приёмки» — новый пункт «**Шаг 0: механизм миграции и добавления дашбордов**» (скил + очередь).
2. `docs/ROADMAP_DCF_POLYUS.md` — новый пункт «**Дашборд статьи PLZL**» (v1 — ✅ этим PR; остаток — ❌ следующим пунктом).

**Приёмка (кратко):** скил `.kimi-code/skills/dashboard-migration/SKILL.md` создан и применён; дашборд `finance-polyus` собран кодом (19 панелей, 4 ряда) в артефакт `dashboard/finance-polyus.json` — легаси-экспорт заменён генерацией (история — в git); все запросы — только `v_*` и проверены живьём через MCP; Makefile/CI ведут три дашборда; очередь миграции зарегистрирована; README/ARCHITECTURE/роадмапы синхронны; протокол живой приёмки — комментарием в PR. Детали — §7.

## 1. Заказ и место в роадмапах

**Заказ владельца (2026-10-16):** аналогично PR #70 — миграция `dashboard/finance-polyus.json` под общий стандарт (dashboards-as-code), устранение пробелов и снабжение дашборда **всеми графиками, на которых строится прогноз PLZL**, плюс **прогнозные точки**; «шагом ноль» — воспроизводимый механизм (скил/документ — реши сам) для добавления дашбордов других треков (банки, металлургия и т.д.) и приведения всех текущих дашбордов к изменённому стандарту.

**Решения владельца (диалог 2026-10-16):**
- v1 — **строго на существующих витринах**; новые витрины (`mine_plans`, `price_decks`, `model_runs`) и панели «план из презентаций vs наш прогноз vs факт», сценарии НАВ/цены акции — **следующим пунктом** (регистрируется этим PR).
- Прогнозные точки v1: **веер цены золота (own-сценарии)** и **точность прошлых прогнозов**; годовые планы выпуска/капекса из презентаций менеджмента — в следующий пункт.
- **Один PR** (шаг 0 первыми коммитами, затем дашборд), база — свежий main после мержа PR #70 (squash `1345989`).

**Якоря в документах:** `GOLD_THESIS_DATA_ROADMAP.md:307` («миграция легаси-дашбордов (`rosstat.json`, `chmf.json`, `finance-polyus.json`) на код»); `ROADMAP_DCF_METALS.md:50,63` (шаг 6 — `chmf.json`); `ROADMAP_DCF_POLYUS.md:96,152,163,251` («NAV-матрица в Grafana», «динамика точности — панелью», «мост NAV → цена акции» — ❌ остатки фазы 3); `ARCHITECTURE.md:998` (перевод дашборда на `v_company_*`).

## 2. Живые факты (2026-10-16, dev-ClickHouse + репо)

**Легаси `dashboard/finance-polyus.json`:** K8s-стиль `dashboard.grafana.app/v2`, `spec.title` «Finance Polyus», 11 панелей / 21 таргет / 3 ряда; чужие uid датасорсов (`bdx7lxrdelxq8b` ×17, `efgvwiwtduwaoc` ×4; `ch-rosstat` — 0); окно `now-6h`; читает 4 сырые таблицы (`cbr_gold`, `cbr_currency_usd`, `databook_polyus`, `polyus_financial_metrics`), `v_*` — 0; анти-паттерны: `LIMIT` на timeseries, `/2`-хак полугодий, вшитые в SQL прогнозные точки панели-13 (`UNION ALL SELECT toDateTime('2026-12-30') → 2500`; `gold_sold` 2500/3500/4500/5500 на 2026–2030), дубли заголовков панелей.

**Витрины и ряды (все выверены живыми запросами; все выданы `kimi_reader`):**

| Витрина | Ряды / ключи | Единицы / свежесть |
|---|---|---|
| `v_cbr_macro` | `series='cbr_gold'`, `'cbr_currency_usd'`, `'cbr_key_rate'` | ₽/г, ₽/$, % годовых; → 2026-10-13 |
| `v_gold_prices` | `source='gold'`, series `bold` (bold.report), `comex_front`, `moex_fix_usd` | $/oz; bold 2014→, остальные 2024→ (2026-10-09) |
| `v_fred_macro` | `DFII10`, `DGS10`, `FEDFUNDS`, `T5YIE` (+`CPIAUCSL`, `DTWEXBGS`) | %; → 2026-10-07/08 |
| `v_ofz_curve` | `1y/3y/5y/10y` — по 1 точке (снимок), `RGBI` — история | % / пункты |
| `v_stock_prices` | `code='PLZL'`, `close` | ₽; 4 330 точек, 2026-10-09 = 1006.80 |
| `v_company_operating` | `asset` ∈ {`CONSOLIDATED OPERATING RESULTS`, `OLIMPIADA`, `BLAGODATNOYE`, `NATALKA`, `KURANAKH`, `VERNINSKOYE2`, `ALLUVIALS`, `Sukhoi Log`, `TITIMUKHTA`, `ZAPADOWE`}; `metric` ∈ {`Total refined gold output`, `Total Dore gold output`, `Total gold produced`, `Ore mined`, `Ore processed`, `Stripping`, `Stripping ratio`, `Average grade in ore mined/processed`, `Recovery`, `Gold contained/payable in concentrate`}; `frequency` ∈ ANNUAL/SEMI-ANNUAL/QUARTERLY | из `v_series_catalog`: koz, kT, «000 m3», m3/t, g/t, %; QUARTERLY → 2022-01, SEMI-ANNUAL → 2026-01, ANNUAL → 2026-01 |
| `v_company_financials` | `metric` ∈ {`capex`, `revenue`, `adjusted_ebitda`, `gold_sold`, `tcc_per_ounce`, …}; `period` = `'YYYYH1/H2/FY/Qn'`, `period_type` Enum(Q,H,FY,LTM) | «USD million», «koz», «USD/oz»; capex H → 2026H1 |
| `v_gold_forecasts` | волна `published=2026-10-07`: `own` bull (4600–5000, mid 4800, 25%) / base (4000–4600, 4300, 40%) / bear (3750–4050, 3900, 35%); банки: citi `short` 4000, goldman `2027` range 5400–5600 (mid 5500) и `YE-2026` 4900, hsbc `2027` 5025 / `YE-2026` 4750, jpm `2027` 5400 / `Q4-2026` 4500, ubs `12M` 5200 | $/oz; вероятность — `probability` |
| `v_forecast_accuracy` | `metric='xau_q_avg'`: 2026-09-30 — 4450 vs факт 4300.80 (ошибка +3.47%); 2026-12-31 — 4285 (ждёт) | $/oz |
| `v_model_inputs` | одна строка-снимок: `last_run_id 7f3c2a10…`, `last_run_date 2026-10-10`, `last_nav_per_share 12500`, `plzl_close 1006.8`, `gold_moex_fix_usd 4126.34`, `usdrub 83.92`, `cbr_key_rate 14`, `ofz_5y 16.568`, `fedfunds 3.75`, `dfii10 2.92` (у каждой пары — `*_date`) | — |
| `v_dcf_assumptions` | **5 прогонов** × 36 строк (актив × дек × контур); **даты прогона в витрине нет**; `run_id` связан с `model_runs` (витрины нет) | млн USD |

**Ключевое следствие для v1:** «последний прогон DCF» по существующим витринам неопределим (5 UUID-прогонов без даты, связи с датой нет до витрины `model_runs`); поэтому **панель «НАВ по активам» переносится в следующий пункт** (наряду со сценариями НАВ) — это уточнение к утверждённому дизайну, зафиксировано здесь.

## 3. Шаг 0 — скил `dashboard-migration` (нормативное описание)

Файл: `.kimi-code/skills/dashboard-migration/SKILL.md` (новый проектный скил, по образцу `sync-readme-architecture`).

**Назначение:** воспроизводимый процесс для трёх режимов — (A) миграция легаси-экспорта UI в `grafana/`; (B) новый дашборд трека (банки, металлургия, …); (C) ревизия/приведение существующего дашборда к стандарту.

**Чек-лист (шаги):**
1. **Пункт роадмапа** — назвать/завести; без пункта работа не начинается.
2. **Инвентаризация источника** — для легаси: панели/таргеты/источники/юниты/анти-паттерны (по экспорту); для трека: перечень графиков прогноза и прогнозных точек от владельца.
3. **Гэп-анализ «источник → витрина»** — каждая панель → существующая `v_*` + `series`/ключи; недостающие слои не выдумывать: либо минимальный набор v1, либо отдельный пункт-следствие.
4. **Гранты** — сверить `sql/mcp_kimi_reader.sql` (панель на невыданной витрине валит staging-smoke).
5. **Маппинг «раздел статьи/трек → панели»** — таблица в спеке; для статьи со временем — её §«Иллюстрации» (образец — `KEY_RATE_PUBLICATIONS.md` §11).
6. **Спека + план** (`docs/superpowers/{specs,plans}`, первыми коммитами; SQL — дословный контракт; живая сверка серий).
7. **Реализация** — builder в `grafana/`, тесты (состав, `FROM v_`, юниты, точки/пороги, детерминизм), конвенции (чек-лист ниже).
8. **Регистрация** — ordered slice в `main.go`, `DASHBOARDS` в Makefile, списки lint/upload в CI; артефакт `dashboard/<uid>.json` коммитится.
9. **Приёмка и доки** — `make all`; живой SQL всех таргетов через MCP; `make grafana-deploy-test`/`grafana-check`; CI; протокол §16.1 п. 8 в PR; `sync-readme-architecture`; регистрация следствий и обновление очереди.

**Конвенции** (не дублировать нормативно): чек-лист со ссылками на `ARCHITECTURE.md` §6.7 и спеки `gold-thesis`/`key-rate-publications` §6 (запросы только `v_*`; `$__timeFilter(date)`; YoY окном по полному ряду; юниты `percent`/`percentunit`/`currencyRUB`/`short`; точки для разрежённых серий; пороги со `ThresholdsStyle`; русские алиасы; детерминизм; описания панелей).

**Очередь** (живой список — в пункте `ROADMAP_INFRA`, здесь фиксируется состав на 2026-10-16): `finance-polyus` ✅ (этот PR) → `chmf.json` (шаг 6 `ROADMAP_DCF_METALS`) → `rosstat.json` + корневой `grafana_dashboard_rosstat` → дашборды новых треков (банки и др.) по скилу.

## 4. Каркас дашборда

- uid / `metadata.name`: `finance-polyus`; артефакт: `dashboard/finance-polyus.json` (заменяет легаси-экспорт; прежний UI-дашборд остаётся на стенде под своим uid, новый деплой кладёт `finance-polyus`).
- title «Полюс (PLZL) — финансы и прогноз»; tags `["polyus", "dcf", "article"]`; `Refresh("1h")`; `Time("now-5y","now")`; `Timezone(browser)`.
- Ряды: id 31–34. Панели 1–19 (Span 12; панели 2, 7, 8 — Span 24). У каждой панели — `Description` (источник, единицы, где читать вероятность/оговорки).

## 5. Панели и SQL-контракт (дословно; 19 панелей)

### Ряд 31 «Золото, ставки, рынок» (панели 1–6)

**1. «Учётная цена золота ЦБ, ₽/г»** (timeseries, `currencyRUB`, decimals 2).
```sql
SELECT date AS time, value AS "Учётная цена ЦБ, ₽/г"
FROM v_cbr_macro
WHERE series = 'cbr_gold' AND $__timeFilter(date)
ORDER BY date
```

**2. «Золото, $/oz: паритет ЦБ и рынок»** (timeseries, `currencyUSD`, Span 24; 4 таргета, один юнит). A — паритет из учётной цены и курса; B/C/D — рыночные ряды (титул `bold` — «bold.report» из `v_series_catalog`).
```sql
-- A
SELECT r.date AS time, r.value / usd.value * 31.1034768 AS "ЦБ-паритет, $/oz"
FROM v_cbr_macro AS r
INNER JOIN v_cbr_macro AS usd ON usd.date = r.date AND usd.series = 'cbr_currency_usd'
WHERE r.series = 'cbr_gold' AND $__timeFilter(r.date)
ORDER BY time
-- B
SELECT date AS time, value AS "МосБиржа фикс, $/oz"
FROM v_gold_prices
WHERE source = 'gold' AND series = 'moex_fix_usd' AND $__timeFilter(date)
ORDER BY date
-- C: series = 'comex_front' («COMEX-фронт, $/oz»); D: series = 'bold' («bold.report, $/oz») — тот же шаблон
```

**3. «Курс USD/RUB»** (timeseries, `short`, decimals 4).
```sql
SELECT date AS time, value AS "USD/RUB"
FROM v_cbr_macro
WHERE series = 'cbr_currency_usd' AND $__timeFilter(date)
ORDER BY date
```

**4. «Ставка ЦБ и кривая ОФЗ (локальный контур DCF)»** (timeseries, `percent`; теноры — точки: по 1 точке).
```sql
-- A: 'cbr_key_rate' из v_cbr_macro («Ключевая ставка, %»)
SELECT date AS time, value AS "Ключевая ставка, %"
FROM v_cbr_macro
WHERE series = 'cbr_key_rate' AND $__timeFilter(date)
ORDER BY date
-- B..E
SELECT date AS time, yield AS "ОФЗ 1y, %"
FROM v_ofz_curve
WHERE tenor = '1y' AND $__timeFilter(date)
ORDER BY date
-- C/D/E: tenor = '3y'/'5y'/'10y', алиасы «ОФЗ 3y/5y/10y, %»; B..E — overrides точек (lineWidth 0, showPoints always) + фикс-цвета
```

**5. «Глобальный контур: реальная ставка и ожидания»** (timeseries, `percent`; 4 таргета — DFII10/DGS10/FEDFUNDS/T5YIE).
```sql
SELECT date AS time, value AS "DFII10, %"
FROM v_fred_macro
WHERE series = 'DFII10' AND $__timeFilter(date)
ORDER BY date
-- B/C/D: 'DGS10' («UST 10y, %»), 'FEDFUNDS' («Fed Funds, %»), 'T5YIE' («5y breakeven, %») — тот же шаблон
```

**6. «PLZL: цена акции»** (timeseries, `currencyRUB`).
```sql
SELECT date AS time, close AS "PLZL, ₽"
FROM v_stock_prices
WHERE code = 'PLZL' AND $__timeFilter(date)
ORDER BY date
```

### Ряд 32 «Операционные показатели» (панели 7–11)

**7. «Выпуск золота: год, полугодия, кварталы»** (timeseries, Span 24; 3 таргета одной метрики по частотам; юнит — `short`, единица «koz» в алиасе; кварталы заканчиваются 2022-м — это данные, не дефект).
```sql
SELECT date AS time, value AS "Год, koz"
FROM v_company_operating
WHERE company = 'PLZL' AND asset = 'CONSOLIDATED OPERATING RESULTS'
  AND metric = 'Total refined gold output' AND frequency = 'ANNUAL'
  AND $__timeFilter(date)
ORDER BY date
-- B: frequency = 'SEMI-ANNUAL' («Полугодия, koz»); C: frequency = 'QUARTERLY' («Кварталы, koz»)
```

**8. «Выпуск золота по активам (год), koz»** (timeseries, Span 24; 9 таргетов — по одному на актив; консолидированный ряд исключён).
```sql
SELECT date AS time, value AS "OLIMPIADA, koz"
FROM v_company_operating
WHERE company = 'PLZL' AND asset = 'OLIMPIADA'
  AND metric = 'Total refined gold output' AND frequency = 'ANNUAL'
  AND $__timeFilter(date)
ORDER BY date
-- остальные восемь таргетов — тот же шаблон по активам: BLAGODATNOYE, NATALKA, KURANAKH,
-- VERNINSKOYE2, ALLUVIALS, Sukhoi Log, TITIMUKHTA, ZAPADOWE
```

**9. «Руда: добыто и переработано (год), kT»** (timeseries, `short`; 2 таргета).
```sql
SELECT date AS time, value AS "Добыто руды, kT"
FROM v_company_operating
WHERE company = 'PLZL' AND asset = 'CONSOLIDATED OPERATING RESULTS'
  AND metric = 'Ore mined' AND frequency = 'ANNUAL'
  AND $__timeFilter(date)
ORDER BY date
-- B: metric = 'Ore processed' («Переработано руды, kT»)
```

**10. «Вскрыша и коэффициент вскрыши»** (timeseries, `short`; B — правая ось: m3/t против 000 m3).
```sql
-- A
SELECT date AS time, value AS "Вскрыша, тыс. м³"
FROM v_company_operating
WHERE company = 'PLZL' AND asset = 'CONSOLIDATED OPERATING RESULTS'
  AND metric = 'Stripping' AND frequency = 'ANNUAL'
  AND $__timeFilter(date)
ORDER BY date
-- B: metric = 'Stripping ratio' («Коэффициент вскрыши, м³/т») + override axisPlacement: right
```

**11. «Содержание и извлечение»** (timeseries, `short`; B — правая ось: % против g/t).
```sql
-- A
SELECT date AS time, value AS "Содержание в руде, г/т"
FROM v_company_operating
WHERE company = 'PLZL' AND asset = 'CONSOLIDATED OPERATING RESULTS'
  AND metric = 'Average grade in ore mined' AND frequency = 'ANNUAL'
  AND $__timeFilter(date)
ORDER BY date
-- B: metric = 'Recovery' («Извлечение, %») + override axisPlacement: right
```

### Ряд 33 «Финансовые показатели» (панели 12–15)

Общий приём: ось времени вычисляется из `period`; `$__timeFilter` не применяется (паттерн gold `dxy`/`crack`; окно дашборда клипает вывод) — это записано в описаниях панелей. Отображение: `value` — `Nullable(Float64)`.

**12. «Капекс, $ млн: полугодия и год»** (timeseries, `currencyUSD`; 2 таргета).
```sql
SELECT
    multiIf(
        endsWith(period, 'H1'), toDate(concat(substring(period, 1, 4), '-01-01')),
        toDate(concat(substring(period, 1, 4), '-07-01'))
    ) AS time,
    value AS "Капекс, полугодие, $ млн"
FROM v_company_financials
WHERE company = 'PLZL' AND metric = 'capex' AND period_type = 'H'
ORDER BY time
-- B: period_type = 'FY' → toDate(concat(substring(period,1,4),'-01-01')) AS time, алиас «Капекс, год, $ млн»
```

**13. «Выручка и EBITDA (год), $ млн»** (timeseries, `currencyUSD`; 2 таргета).
```sql
SELECT toDate(concat(substring(period, 1, 4), '-01-01')) AS time, value AS "Выручка, $ млн"
FROM v_company_financials
WHERE company = 'PLZL' AND metric = 'revenue' AND period_type = 'FY'
ORDER BY time
-- B: metric = 'adjusted_ebitda' («Скорр. EBITDA, $ млн»)
```

**14. «Продажи золота, koz»** (timeseries, `short`; 2 таргета).
```sql
SELECT
    multiIf(
        endsWith(period, 'H1'), toDate(concat(substring(period, 1, 4), '-01-01')),
        toDate(concat(substring(period, 1, 4), '-07-01'))
    ) AS time,
    value AS "Продажи, полугодие, koz"
FROM v_company_financials
WHERE company = 'PLZL' AND metric = 'gold_sold' AND period_type = 'H'
ORDER BY time
-- B: period_type = 'FY' (ось как в панели 12-B), алиас «Продажи, год, koz»
```

**15. «TCC на унцию, $/oz»** (timeseries, `currencyUSD`; 1 таргет; 3 точки — разрежённость подписана; AISC — вне витрин).
```sql
SELECT
    multiIf(
        endsWith(period, 'H1'), toDate(concat(substring(period, 1, 4), '-01-01')),
        toDate(concat(substring(period, 1, 4), '-07-01'))
    ) AS time,
    value AS "TCC, $/oz"
FROM v_company_financials
WHERE company = 'PLZL' AND metric = 'tcc_per_ounce' AND period_type = 'H'
ORDER BY time
-- override: lineWidth 0 + showPoints always (разрежённая серия)
```

### Ряд 34 «Прогноз и точность» (панели 16–19)

**16. «Золото, $/oz: факт и прогнозные точки (own-сценарии)»** (timeseries `currencyUSD`; A — факт линией; B/C/D — точки сценариев на дате горизонта; маппинг горизонта — по самому ярлыку, без хардкода годов: `Q#-YYYY`/`YE-YYYY` → 31 декабря года, голый `YYYY` → 31 декабря года; немэппируемые ярлыки (`12M`, `short`) отсекаются).
```sql
-- A
SELECT date AS time, value AS "COMEX-фронт, $/oz"
FROM v_gold_prices
WHERE source = 'gold' AND series = 'comex_front' AND $__timeFilter(date)
ORDER BY date
-- B
SELECT time, midpoint AS "own: bull"
FROM (
    SELECT
        multiIf(
            match(horizon, '^(Q[1-4]|YE)-[0-9]{4}$'),
                toDate(concat(substring(horizon, length(horizon) - 3, 4), '-12-31')),
            match(horizon, '^[0-9]{4}$'), toDate(concat(horizon, '-12-31')),
            NULL
        ) AS time,
        midpoint, scenario, author
    FROM v_gold_forecasts
)
WHERE author = 'own' AND scenario = 'bull' AND time IS NOT NULL
ORDER BY time
-- C/D: scenario = 'base' («own: base») и 'bear' («own: bear») — тот же шаблон;
-- B/C/D — overrides точек (lineWidth 0, showPoints always) + фикс-цвета (bull/base/bear)
```
Description: вероятности сценариев и диапазоны — панель 17; горизонты `12M` и `short` точками не рисуются (нет календарного маппинга), их значения видны в баре 17.

**17. «Прогноз цены золота: сценарии и консенсус (bar)»** (barchart через `tableQuery`; вероятность — в подписи; как gold-панель 8).
```sql
SELECT
    concat(author, ' / ', scenario, ' (', ifNull(toString(toInt32(probability)), '—'), '%)') AS "Прогноз",
    midpoint AS "Середина, $/oz",
    low,
    high
FROM v_gold_forecasts
ORDER BY midpoint
```

**18. «Последний прогон модели (table)»** (table; апсайд НЕ считается — мост NAV → цена акции в роадмапе ❌; значения показываются как записаны).
```sql
SELECT
    toString(last_run_id) AS "Прогон",
    last_run_date AS "Дата прогона",
    last_nav_per_share AS "НАВ/акция, ₽",
    plzl_close AS "Цена PLZL, ₽",
    gold_moex_fix_usd AS "Золото (fix), $/oz",
    usdrub AS "USD/RUB",
    cbr_key_rate AS "Ключевая, %",
    ofz_5y AS "ОФЗ 5y, %"
FROM v_model_inputs
```

**19. «Точность прошлых прогнозов (table)»** (table; обе строки, включая ещё не разрешённую).
```sql
SELECT
    forecast_date AS "Дата прогноза",
    target_date AS "Цель",
    predicted AS "Прогноз, $/oz",
    actual AS "Факт, $/oz",
    round(error_pct, 2) AS "Ошибка, %",
    is_resolved AS "Разрешён"
FROM v_forecast_accuracy
WHERE metric = 'xau_q_avg'
ORDER BY target_date DESC
```

## 6. Маппинг «трек прогноза → панели» (для будущей статьи PLZL)

| Трек/раздел будущей статьи | Панели |
|---|---|
| Макро-фон и входы цены | 1–5 (золото ₽/г и $/oz, курс, оба контура ставок) |
| Операционный блок (что производит Полюс) | 7–11 |
| Финансовый блок (экономика операций) | 12–15 |
| Рынок и прогноз (веер, консенсус, прогон, точность) | 6, 16–19 |
| Не покрыто v1 (следующий пункт): планы из презентаций vs наш прогноз vs факт; НАВ по активам; сценарии НАВ/цены акции; апсайд | — |

## 7. Реализация и приёмка

**Файлы:** `.kimi-code/skills/dashboard-migration/SKILL.md`; `docs/ROADMAP_INFRA.md` (новый пункт Фазы 2); `docs/ROADMAP_DCF_POLYUS.md` (новый пункт); `docs/GOLD_THESIS_DATA_ROADMAP.md` (в follow-up §4 — пометка «механизм и очередь — `ROADMAP_INFRA`; `finance-polyus` ✅ 2026-10-16»); спека+план; `grafana/finance_polyus.go` + `grafana/finance_polyus_test.go`; `grafana/main.go` (константа `FinancePolyusUID`, ordered slice +1); `Makefile` (`DASHBOARDS += finance-polyus`); `.github/workflows/dashboards.yml` (lint/upload + файл); `dashboard/finance-polyus.json` (генерация заменяет легаси-экспорт); README/ARCHITECTURE (sync-readme-architecture; в списке легаси-экспортов остаётся `rosstat.json`/`chmf.json`).

**Тесты:** 19 панелей / 4 ряда (id 31–34); все таргеты `FROM v_`; отсутствие `FINAL`; `LIMIT` — не на timeseries; юниты (панели 1/6 `currencyRUB`, 2/12–15/16 `currencyUSD`… — список фиксируется тестом); точки-оверрайды (4-B..E, 10-B, 11-B, 15, 16-B..D); «правая ось» оверрайды (10-B, 11-B); отсутствие `$__timeFilter` у панелей 12–15 (осознанно — тест фиксирует это); детерминизм; артефакты gold/key-rate не меняются.

**Приёмка (протокол — комментарием в PR, стандарт §16.1 п. 8):** `make all` и `make grafana-validate` зелёные; `make grafana-build` ×2 детерминирован (три артефакта, включая неизменные gold/key-rate); живая SQL-сверка **всех таргетов** `dashboard/finance-polyus.json` через MCP под `kimi_reader` (окно и подстановки — как в key-rate; у панелей 12–15 — полная история); `make grafana-deploy-test` + `make grafana-check` (три uid) на локальном стенде; CI зелёный; staging — owner-assisted; README/ARCHITECTURE/роадмапы синхронны; скил и очередь зарегистрированы.

**Дисциплина субагентов:** имплементеры — `deepseek/deepseek-flash` (все задачи), ревью задач — `anthropic/haiku`, финальное ревью ветки — `deepseek/deepseek-flash`; fix-раунды ≤ 5; в каждый диспатч — блок MCP из AGENTS.md.

## 8. Границы (вне объёма v1, регистрируется)

- **Следующий пункт (❌ в `ROADMAP_DCF_POLYUS`):** витрины `mine_plans`/`price_decks`/`model_runs` (даты и сценарии прогонов) → панели «план из презентаций vs наш прогноз vs факт»; НАВ по активам последнего прогона; сценарии НАВ/цены акции; апсайд (мост NAV → цена).
- **Очередь миграции** (`ROADMAP_INFRA`): `chmf.json` (шаг 6 `ROADMAP_DCF_METALS`), `rosstat.json` + корневой `grafana_dashboard_rosstat`; дашборды новых треков — по скилу.
- Сама статья PLZL (маппинг §6 — её заготовка); PNG-рендер; аннотации и переменные темплейтинга; правка легаси-таблицы `polyus_financial_metrics` (не удаляется до подтверждения работы дашборда на `v_company_*`).
