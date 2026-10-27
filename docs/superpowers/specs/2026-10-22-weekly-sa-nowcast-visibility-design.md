# Nowcast weekly SA виден в окне дашборда — design

**Дата:** 2026-10-22. **Пункт:** `key-rate/phase-4-dashboard-nowcast-visibility` (issue #361, `role:go`, P2, depends on #357). **База:** `origin/main` `4412aa7` (PR #358 merged). **Ветка:** `fix/weekly-sa-nowcast-visible`.

## 1. Проблема

Панель 4 «Инфляция: ИПЦ и базовая» читает `weekly_cpi:saar` с `$__timeFilter(date)`. У nowcast-строки текущего неполного месяца `date` — конец месяца (`2026-10-31`), то есть будущая дата относительно `to=now` (2026-10-22). Grafana отсекает строку, и опережающая оценка, ради которой делался #355, на графике не появляется.

Доказательство: выгрузка Grafana `now-3y..now` (`/Users/whitefox/Downloads/cpi_and_week_sa.json`) — ряд C кончается `2026-09-30`; живой SQL `$__timeFilter(date)` при `to=2026-10-22` — 36 точек, `$__timeFilter(as_of)` — 37 точек, последняя `2026-10-12` = 13,47 %.

## 2. Решение

```sql
SELECT if(partial = 1, as_of, date) AS time, value / 100 AS "Недельный SA-агрегат ИПЦ, SAAR"
FROM v_weekly_cpi_sa
WHERE series = 'weekly_cpi:saar' AND $__timeFilter(as_of)
ORDER BY time
```

- Время nowcast-точки — `as_of` (дата наблюдения): она существует на 12.10 и должна быть видна в окне, заканчивающемся «сейчас». Исторические месяцы остаются на конце месяца.
- Фильтр — по `as_of`: это дата, по которой точка реально существует; метка периода у неполного месяца ещё не наступила.
- `value / 100` не меняется: витрина в процентах, панель в `percentunit`.
- В `Description` панели 4 — ожидаемый разброс weekly SAAR против core на 2023+ 5–9 п.п. и правило чтения: weekly — headline, сравнивать с линией ИПЦ, с базовой — только направление.

## 3. Почему выбросы против базовой — ожидаемы

Weekly SAAR — headline-агрегат на 4-нед. окне с годовым усилением ×13, а не core. Крупнейшие расхождения 2023+ — полные месяцы (`partial=0`), не nowcast: 2026-01 — 17,72 % против 9,12 % (ИПЦ annualized 21,27 %, недельный индекс 2026-01-12 = 101,26), 2026-06 — 12,03 % против 5,16 % (ИПЦ 10,95 %). Research #348: на 2022+ прокси не значим (DM p=0,1896), критерий пройден только на 2009+ (p=0,0216); на окне 2023+ corr с headline 0,77 против 0,61 с core. Это фиксируется в описании панели, а не «чинится» сглаживанием: сглаживание убило бы опережающий сигнал.

## 4. Приёмка

- `TestKeyRateInflationPanelWeeklySA` — SQL содержит `if(partial = 1, as_of, date)` и `$__timeFilter(as_of)`;
- `cd grafana && go test ./...`, `make grafana-build` (повтор — тот же sha256), `make grafana-validate`, `make all`, `make docs-check` — зелёные;
- live SQL при `to=now`: 37 точек, последняя `2026-10-12` = 13,47 %;
- деплой/smoke — owner-assisted.

## 5. Границы

`v_weekly_cpi_sa`, `weekly_cpi_sa`, DDL и гранты не меняются; меняется только SQL таргета C и текст описания панели 4. Остальные панели и правило `$__timeFilter` не затрагиваются.
