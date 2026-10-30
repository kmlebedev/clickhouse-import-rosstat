# `__timeFilter` по умолчанию и weekly SA на панели 4 — design

**Дата:** 2026-10-22. **Пункт:** `key-rate/phase-4-dashboard-timefilter-weekly-sa` (issue #357, `role:go`, P2). **База:** `origin/main` `8f47c0e` (PR #356 merged). **Ветка:** `feat/key-rate-dashboard-timefilter` (worktree `.worktrees/key-rate-dashboard-timefilter`).
**Приёмка (кратко):** все timeseries-таргеты содержат `$__timeFilter` (тест); панель 4 — 3 серии, включая `weekly_cpi:saar`; `make grafana-build` детерминирован, `make grafana-validate`, `make all`, `make docs-check` — зелёные.

## 1. Проблема

1. **Панель 2** «Недельные цены: годовое выражение» — единственные timeseries-таргеты без `$__timeFilter`: запрос режет год по `toYear(date)` и игнорирует выбранный диапазон дашборда. Остальные 33 панели (включая row 40 — ИЦП, ФАО, ФЕДСТАТ) фильтр уже имеют; отсутствие правила позволяет новому графику снова его забыть.
2. **Панель 4** «Инфляция: ИПЦ и базовая» показывает ИПЦ и базовую SAAR ЦБ, но не недельный SA-агрегат `weekly_cpi:saar` (`v_weekly_cpi_sa`), который с #356 отдаёт и nowcast текущего месяца (`partial=1`). Без него сравнение «опережающий weekly vs базовая ЦБ» требует отдельного запроса.

## 2. Решение

### 2.1. Панель 2

`krWeeklyAnnualizedTemplate` переводится на подзапрос: внутренний `SELECT %s AS time, value` (тот же JOIN каталога и `toYear(date) = %s`), внешний — `WHERE $__timeFilter(time)`, `GROUP BY time`. Фильтр ставится на **вычисленное** `time`, а не на сырой `date`: ряд B сдвинут на +1 год (`date_add(YEAR, 1, date)`), и фильтр по `date` выбросил бы прошлый год из окна — на живых данных при окне 2026 года это 0 точек вместо 42. Приём тот же, что у `krWeeklyAvg4Template` (`$__timeFilter(time)` поверх подзапроса). Ряды A/B по-прежнему различаются годом и сдвигом времени; фильтр только сужает источник до диапазона дашборда.

### 2.2. Панель 4

Новый таргет `C`:

```sql
SELECT if(partial = 1, as_of, date) AS time, value / 100 AS "Недельный SA-агрегат ИПЦ, SAAR"
FROM v_weekly_cpi_sa
WHERE series = 'weekly_cpi:saar' AND $__timeFilter(as_of)
ORDER BY time
```

`value` в витрине — проценты (`5,85`), панель — `percentunit`, поэтому `/100`; иначе линия была бы в 100 раз выше. Серия включает nowcast-точку `partial=1` (месяц `2026-10-31`, `as_of=2026-10-12`); в `Description` — оговорка: weekly — headline/тарифный агрегат, не core; октябрь 2026 искажён тарифной неделей 2026-10-05; последняя точка — nowcast.

Время nowcast-точки — `as_of`, а не `date`: у неполного месяца метка `date` лежит в будущем (2026-10-31 при `now` = 2026-10-22), и фильтр по `date` вырезал бы её из окна `now-3y..now` — на живом стенде ряд C кончался 2026-09, то есть опережающей оценки не было видно. Фильтр тоже по `as_of`: это дата наблюдения, по которой точка реально существует. Проверка выгрузкой Grafana: при `to=2026-10-22` запрос с `$__timeFilter(date)` отдаёт 36 точек до 2026-09, с `$__timeFilter(as_of)` — 37 точек, последняя `2026-10-12` = 13,47 %. Таргет только читает витрину `v_weekly_cpi_sa`, грант `kimi_reader` уже есть.

**Ожидаемый разброс против базовой.** Weekly SAAR — headline-агрегат на 4-нед. окне с годовым усилением ×13, поэтому отдельные месяцы расходятся с core на 5–9 п.п.: 2026-01 — 17,72 % против 9,12 % (headline annualized — 21,27 %), 2026-06 — 12,03 % против 5,16 % (headline — 10,95 %). Выбросы принадлежат полным месяцам (`partial=0`), то есть это не артефакт nowcast-строки, а состав корзины и неполная сезонная корректировка 28-дневного окна. Research #348: на 2022+ прокси не значим (DM p=0,1896), критерий пройден только на истории 2009+. Вывод для панели: сравнивать weekly с линией ИПЦ (headline), а с базовой — только как направление, не как уровень.

### 2.3. Правило

- **Тест** `TestKeyRateAllTimeseriesUseTimeFilter` в `grafana/key_rate_publications_test.go`: для каждой панели `type == timeseries` каждый `CHQuery.RawSql` обязан содержать `$__timeFilter`; иначе `make grafana-validate` падает. Это машинная норма, а не ревью-пожелание. Дополнительно `TestKeyRateWeeklyYearOverlay` фиксирует, что у ряда B фильтр стоит на `time`, а не на сыром `date`.
- **Документация:** `docs/GRAFANA.md` §«Дашборд как код» — буллет «каждый timeseries-таргет — `$__timeFilter` по своей временной колонке (`date`/`period_date`); если время вычисляется (сдвиг года, скользящее окно) — по вычисленному `time`; исключений нет, проверяется тестом»; `ARCHITECTURE.md` §6.7 — та же норма; скил `dashboard-migration` (шаг 7) — «новый график без `$__timeFilter` не проходит `make grafana-validate`».
- **Почему тест, а не хелпер:** `timeseriesQuery` не знает временную колонку (`date` vs `period_date`) и не может подставить фильтр автоматически; тест проверяет результат и не мешает осознанным исключениям (если появятся — с явным allowlist в тесте).

## 3. Приёмка

- `cd grafana && go test ./...` — `TestKeyRateAllTimeseriesUseTimeFilter` зелёный; панель 4 — 3 таргета (`A`, `B`, `C`), unit `percentunit`; панель 2 — оба ряда отдают точки в одном окне (живая проверка: A 41, B 42 при окне 2026).
- `make grafana-build` — артефакт `dashboard/key-rate-publications.json` обновлён; повтор — пустой `git diff -- dashboard/`.
- `make grafana-validate`, `make all`, `make docs-check` — зелёные.
- Live SQL панелей 2 и 4 через MCP `kimi_reader`: панель 2 A/B отдают точки в диапазоне; панель 4 C — `weekly_cpi:saar` с nowcast-точкой `2026-10-12` = 13,47 % (видна при `to=now`) и историей до 2009-05.
- `docs/KEY_RATE_PUBLICATIONS.md` §11 — панель 4: 3 серии, оговорка про nowcast/тариф; панель 2 — фильтр по диапазону.
- Деплой/smoke — owner-assisted (стенд).

## 4. Границы

`v_weekly_cpi_sa`, `v_rosstat_ppi`, `v_fao_food_price`, `v_fedstat_weekly_prices`, `v_rosstat_macro`, `v_cbr_macro` не меняются; DDL/гранты не меняются. Панели 36–39 по составу не меняются (только проверка фильтра). Юнит панели 4 остаётся `percentunit`; weekly SAAR переводится в долю в SQL.
