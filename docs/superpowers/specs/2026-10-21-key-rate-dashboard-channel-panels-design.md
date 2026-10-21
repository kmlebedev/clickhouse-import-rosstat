# Панели четырёх каналов без покрытия — design

**Дата:** 2026-10-21. **Пункт:** `key-rate/phase-4-dashboard-channel-panels` ([issue #328](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/328), `role:go`, P2). **Ветка:** `feat/key-rate-dashboard-channel-panels`. **Depends on:** #327 (закрыт, PR #330).
**Приёмка (кратко):** `make grafana-build` даёт детерминированный артефакт; в `dashboard/key-rate-publications.json` появились панели 29–33 по четырём каналам без покрытия; все запросы — только по витринам `v_*`; `make grafana-check` зелёный (при доступном стенде); §11 `KEY_RATE_PUBLICATIONS.md` и §8 `KEY_RATE_CHANNELS.md` обновлены тем же PR.

## 1. Пробел

Grafana-дашборд `key-rate-publications` покрывал 7 из 11 каналов решения ЦБ; без панелей оставались четыре ([KEY_RATE_CHANNELS.md](../../KEY_RATE_CHANNELS.md) §8):

| Канал | Витрина | Что нужно показать |
|---|---|---|
| Внешние условия и курс | `v_cbr_macro` (`cbr_currency_usd`), `v_moex_market` (`brent_front`), `v_cbr_bop` | курс ЦБ; Brent — **фронт-месяц, не спот**; счёт текущих операций |
| Сберегательное поведение | `v_cbr_macro` («Депозиты населения») | уровень и месячный приток; правило выбора ряда — §8.1 |
| Разрыв выпуска / перегрев | `v_cbr_survey` (`business_climate_sa`, `industry_total_ibk_sa`, `capacity_utilization_sa`) | бизнес-климат SA, ИБК, загрузка мощностей |
| Автономные (немонетарные) факторы | `v_cbr_ccyb_indicators` | кредитные гэпы и рост кредита; оговорка о крае ряда 2024-07/2024-10 |

Реестр каналов и контракт данных — #327; здесь только панели. DDL, витрины, гранты, импортёры и `v_key_rate_dashboard` не меняются.

## 2. Живые факты (MCP `kimi_reader`, 2026-10-21)

| Ряд | Витрина | Точек | Крайние даты | Единица |
|---|---|---|---|---|
| `cbr_currency_usd` | `v_cbr_macro` | 3 403 | 2013-01-10 … 2026-10-17 | руб. за 1 USD |
| `brent_front` | `v_moex_market` | 3 916 | 2009-10-01 … 2026-10-19 | USD/барр. |
| `Счет текущих операций (квартал)`, `source='cbr_bop_standard'` | `v_cbr_bop` | 130 | 1994-03-31 … 2026-06-30 | млн USD |
| `Счет текущих операций (месяц)`, `source='cbr_bop_est'` | `v_cbr_bop` | 4 | 2025-07-31 … 2026-08-31 | млрд USD |
| `Депозиты населения` | `v_cbr_macro` | 141 | 2015-01-01 … 2026-09-01 | млн руб. |
| `business_climate_sa` | `v_cbr_survey` | 297 | 2002-01-01 … 2026-09-01 | пункты |
| `industry_total_ibk_sa` | `v_cbr_survey` | 297 | 2002-01-01 … 2026-09-01 | пункты |
| `capacity_utilization_sa` | `v_cbr_survey` | 99 | 2002-04-01 … 2026-10-01 | % |
| `credit_gap_broad`, `credit_gap_hh` | `v_cbr_ccyb_indicators` | 47 | 2013-01-01 … 2024-07-01 | п.п. |
| `hh_yoy`, `nfo_rub_yoy` | `v_cbr_ccyb_indicators` | 201 | 2008-02-01 … 2024-10-01 | % |

Гранты уже выданы (`sql/mcp_kimi_reader.sql`: `v_cbr_macro`, `v_moex_market`, `v_cbr_bop`, `v_cbr_survey`, `v_cbr_ccyb_indicators`); `GRANT` не меняется. Все SQL §4 прогнаны живыми запросами; `lagInFrame(value, 1, NULL)` даёт NULL в первой точке, а не выброс.

## 3. Решение

Пять панелей в новом ряду: «внешние условия» требует двух панелей из-за разных единиц (руб./USD, USD/барр., млрд USD).

- **Ряд id 34** «Остальные каналы решения» (id 21–24 заняты; панели 25–33 — значит, ряд не может получить 25).
- Панели: **29–33**; сетка 29|30, 31|32, 33 — `Span(24)`.
- Новые панели — в конце дашборда; существующие id/ряды не переставляются.
- Разрежённая месячная оценка БОП (4 точки) — `pointsOnlyProps()`; квартальная стандартная презентация — линия.
- Правые оси: Brent (29), приток депозитов (31), загрузка мощностей (32), темпы роста кредита (33) — с unit-override.

| id | Заголовок | Серии | Unit слева / справа |
|---|---|---|---|
| 29 | Внешние условия: курс ЦБ и Brent | A `cbr_currency_usd`; B `brent_front` | `Rubles` / `Dollars` |
| 30 | Внешние условия: счёт текущих операций | A `cbr_bop_standard` ÷ 1000 (линия); B `cbr_bop_est` (точки) | `Dollars` |
| 31 | Сбережения: депозиты населения | A уровень ÷ 1000; B месячный приток ÷ 1000 | `Rubles` / `Rubles` |
| 32 | Разрыв выпуска: бизнес-климат, ИБК и загрузка | A `business_climate_sa`; B `industry_total_ibk_sa`; C `capacity_utilization_sa` | `Short` / `Percent` |
| 33 | Автономные факторы: кредитные гэпы и рост кредита | A `credit_gap_broad`; B `credit_gap_hh`; C `hh_yoy`; D `nfo_rub_yoy` | `Short` / `Percent` |

## 4. SQL — дословный контракт

```sql
-- 29 A
SELECT date AS time, value AS "Курс USD/RUB, руб. за 1 USD"
FROM v_cbr_macro
WHERE series = 'cbr_currency_usd' AND $__timeFilter(date)
ORDER BY date

-- 29 B
SELECT date AS time, value AS "Brent (фронт-месяц), USD/барр."
FROM v_moex_market
WHERE series = 'brent_front' AND $__timeFilter(date)
ORDER BY date

-- 30 A
SELECT date AS time, value / 1000 AS "Счёт текущих операций (квартал), млрд USD"
FROM v_cbr_bop
WHERE source = 'cbr_bop_standard' AND series = 'Счет текущих операций (квартал)'
  AND $__timeFilter(date)
ORDER BY date

-- 30 B
SELECT date AS time, value AS "Счёт текущих операций (месяц), млрд USD"
FROM v_cbr_bop
WHERE source = 'cbr_bop_est' AND series = 'Счет текущих операций (месяц)'
  AND $__timeFilter(date)
ORDER BY date

-- 31 A
SELECT date AS time, value / 1000 AS "Депозиты населения, млрд руб."
FROM v_cbr_macro
WHERE series = 'Депозиты населения' AND $__timeFilter(date)
ORDER BY date

-- 31 B
SELECT time, chg AS "Приток за месяц, млрд руб."
FROM (
    SELECT date AS time,
           (value - lagInFrame(value, 1, NULL) OVER (ORDER BY date)) / 1000 AS chg
    FROM v_cbr_macro
    WHERE series = 'Депозиты населения'
)
WHERE $__timeFilter(time)
ORDER BY time

-- 32 A
SELECT date AS time, value AS "Бизнес-климат (ИБК, SA), пункты"
FROM v_cbr_survey
WHERE series = 'business_climate_sa' AND $__timeFilter(date)
ORDER BY date

-- 32 B
SELECT date AS time, value AS "ИБК — промышленность (SA), пункты"
FROM v_cbr_survey
WHERE series = 'industry_total_ibk_sa' AND $__timeFilter(date)
ORDER BY date

-- 32 C
SELECT date AS time, value AS "Загрузка мощностей (SA), %"
FROM v_cbr_survey
WHERE series = 'capacity_utilization_sa' AND $__timeFilter(date)
ORDER BY date

-- 33 A
SELECT date AS time, value AS "Кредитный гэп (широкий), п.п."
FROM v_cbr_ccyb_indicators
WHERE series = 'credit_gap_broad' AND $__timeFilter(date)
ORDER BY date

-- 33 B
SELECT date AS time, value AS "Кредитный гэп (ФЛ), п.п."
FROM v_cbr_ccyb_indicators
WHERE series = 'credit_gap_hh' AND $__timeFilter(date)
ORDER BY date

-- 33 C
SELECT date AS time, value AS "Кредиты ФЛ, г/г, %"
FROM v_cbr_ccyb_indicators
WHERE series = 'hh_yoy' AND $__timeFilter(date)
ORDER BY date

-- 33 D
SELECT date AS time, value AS "Кредиты ЮЛ в рублях, г/г, %"
FROM v_cbr_ccyb_indicators
WHERE series = 'nfo_rub_yoy' AND $__timeFilter(date)
ORDER BY date
```

**Почему окно снаружи подзапроса (31 B).** `lagInFrame` должен видеть весь ряд: обрезка окна внутри подзапроса сделала бы первый приток ложным. Фильтр `$__timeFilter(time)` стоит после закрывающей скобки; третьим аргументом `lagInFrame` передан `NULL` — первая точка остаётся NULL, а не `value − 0`.

**Почему `÷ 1000` (30 A, 31 A/B).** Витрины хранят млн USD / млн руб.; панель показывает млрд — так подписи и оси читаются без шестизначных чисел, а единицы названы в алиасах и `Description`.

## 5. Оговорки в `Description` (обязательны)

- **29:** Brent — фронт-месяц фьючерса MOEX BR (`settle`), **не спот** и не back-adjusted; на роллах возможен разрыв. Курс — официальный курс ЦБ, руб. за 1 USD. Курс на левой оси, Brent на правой.
- **30:** две выгрузки одного показателя. Стандартная презентация (`cbr_bop_standard`, млн USD → млрд USD) — линия для сверки; месячная оценка (`cbr_bop_est`, млрд USD) — точки, публикуются только последние месяцы. Ноль — значение, а не пропуск.
- **31:** «Депозиты населения» = «депозиты физических лиц» + «депозиты индивидуальных предпринимателей» — канон `KEY_RATE_CHANNELS.md` §8.1. Уровень — левая ось, месячный приток — правая; обе серии в млрд руб. Уровни ФЛ и ИП по отдельности с уровнем населения не смешиваются.
- **32:** ИБК и загрузка мощностей — **прокси** разрыва выпуска, а не сам разрыв: ЦБ числовым рядом его не публикует (§8). ИБК — балансовые индексы SA в пунктах, загрузка — %.
- **33:** АЦН-индикаторы ЦБ. Кредитные гэпы (`credit_gap_broad`, `credit_gap_hh`) — п.п., темпы прироста кредитов (`hh_yoy`, `nfo_rub_yoy`) — %. Ряд источника заканчивается 2024-07 у гэпов и 2024-10 у темпов; после — NULL, **не ноль**: это отставание публикации, а не отсутствие данных в контуре.

## 6. Тесты (`grafana/key_rate_publications_test.go`)

- Состав дашборда: 28 панелей; 5 рядов с явными id `[21, 22, 23, 24, 34]` и заголовками (старая проверка вычисляла диапазон id; с рядом 34 она переписана на явные id).
- Панели по одной: `TestKeyRateChannelPanelTitlesAndUnits` (заголовки, units, число таргетов), `TestKeyRateExternalFxPanel`, `TestKeyRateExternalBopPanel`, `TestKeyRateSavingsPanel`, `TestKeyRateOutputGapPanel`, `TestKeyRateAutonomousFactorsPanel` — `FROM v_*`, `series`/`source`, `$__timeFilter`, `ORDER BY`, правые оси и unit-override; состав и id — `TestKeyRatePublicationsHasTwentyEightPanels`, `TestKeyRatePublicationsRowTitles`, `TestKeyRateSpecConventions`.
- Разрежённые серии: 30 B — `pointsOnlyProps`.
- Окно 31 B: `$__timeFilter` ровно один, снаружи подзапроса, `lagInFrame(value, 1, NULL)`.
- Оговорки в `Description`: Brent «не спот»; депозиты «ФЛ + ИП»; ccyb «2024-10» и «не ноль».
- Общие проверки (`Validate`, `FROM v_`, уникальность id) покрывают новые панели автоматически.

## 7. Документация

- `docs/KEY_RATE_PUBLICATIONS.md` §11 — 28 панелей в 5 рядах; таблица «Два артефакта» (Grafana — 11 из 11); строка «каналы без панели» заменяется на 29–33; блок «Ловушки панелей 29–33».
- `docs/KEY_RATE_CHANNELS.md` §8 — срез 23 → 28 панелей; колонка «Панель Grafana» для четырёх каналов; статусы.
- `ARCHITECTURE.md` §2 (строка `grafana/`) и §6.7 — 28 панелей в 5 рядах, новый ряд; шапка «Последнее обновление».
- `docs/GRAFANA.md`, «Дашборд как код» — актуальные 28 панелей / 5 рядов (исторические протоколы 2026-10-16 не переписываются).
- `docs/ROADMAP_KEY_RATE.md` — пункт в `In Review` со ссылкой на PR и протоколом.

## 8. Проверка

`make grafana-build` (повтор — пустой diff), `make grafana-validate`, живой SQL таргетов через MCP `kimi_reader`, `make all`, `make docs-check`; `make grafana-deploy-test` + `make grafana-check` — при доступном стенде и токене; иначе в протоколе «не проверено» (smoke смотрит первые две панели, новые доказываются тестами и MCP-SQL).

## 9. Вне объёма

Панели по `v_key_rate_dashboard` (расхождение контуров, журнал), PNG-рендер, унификация слагов каналов (#329), DDL/витрины/гранты, staging-деплой, изменение статьи.
