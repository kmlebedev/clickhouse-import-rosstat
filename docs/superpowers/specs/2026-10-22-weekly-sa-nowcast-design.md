# Опережающий nowcast-месяц weekly SA — design

**Дата:** 2026-10-22. **Пункт:** `key-rate/phase-4-weekly-sa-nowcast` (issue #355, `role:go`). **База:** #353 / PR [#354](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/354) (импортёр `weekly_cpi_sa` в main, merge `27a4a29`). **Ветка:** `fix/weekly-sa-nowcast` (worktree `.worktrees/weekly-sa-nowcast`).
**Приёмка (кратко):** 209 полных месяцев + 1 nowcast-месяц (`partial=1`, `as_of` последней недели); parity 209 полных с research #348; строка `weekly_cpi_saar` в `v_key_rate_dashboard`; `make all`, `make docs-check`, `make mcp-check`, `dagu validate` — зелёные.

## 1. Проблема

`weeklysa/grid.go::monthlyLastInMonth` включает месяц, только если глобальная `lastGridDate >= monthEnd`. На 2026-10-22 последняя неделя агрегата — 2026-10-12 (BI) / 2026-10-19 (товарный слой), поэтому октябрь отбрасывается: таблица кончается **2026-09-30**. Это правильно для бэктеста 209 полных месяцев, но убивает смысл пункта: нужен **опережающий** прокси к `base_ipc_saar` — базовой годовой SA ИПЦ ЦБ (`v_cbr_macro`, «Базовый ИПЦ» — уже сезонно очищенный м/м; годовой темп `(1 + v/100)^12 − 1`), которая публикуется ~14-го числа следующего месяца. Сентябрьская base SAAR уже вышла 14.10.2026; без октябрьской nowcast-строки weekly SA не опережает ЦБ.

## 2. Решение

### 2.1. Схема

```sql
ALTER TABLE weekly_cpi_sa ADD COLUMN IF NOT EXISTS as_of Date DEFAULT '1970-01-01';
ALTER TABLE weekly_cpi_sa ADD COLUMN IF NOT EXISTS partial UInt8 DEFAULT 0;
```

`CREATE TABLE` для новых установок содержит те же колонки. `as_of` — последняя дата 7-дневной сетки, использованная для месяца (для полного месяца — последняя неделя внутри месяца; для nowcast — последняя доступная неделя). `partial=1` — текущий неполный месяц.

### 2.2. Месячная сетка

- `monthlyLastInMonth` возвращает `[]monthPoint{Date: monthEnd, Value, AsOf: lastGridDateInMonth, Partial: false}` — 209 полных месяцев (правило `lastGridDate >= monthEnd` сохраняется только для полных).
- `partialMonth(index, lastGridDate)` — если `lastGridDate < monthEnd` последнего месяца, возвращает `{Date: monthEnd, Value: lastIndex, AsOf: lastGridDate, Partial: true}`; иначе `nil`.

### 2.3. X-13

- **Complete fit**: 209 полных месяцев → 209 SA-точек; исторические строки пишутся из него (parity с research #348 не меняется).
- **Nowcast fit**: 209 полных + текущий неполный (210 точек) → SA для последнего месяца; в таблицу идёт **только** nowcast-строка (`partial=1`), исторические не переписываются.
- Вход R — тот же `date,level` (для partial `date=monthEnd`, порядок задаёт `ts`); `frequency=12`, `seats_log_cal`.

### 2.4. Строки и no-op

- `weeklySARow` + `AsOf`, `Partial`; `buildRows` — complete + `buildPartialRow`; `insertRows` — колонки `as_of`, `partial`.
- `count() FINAL` = 209 + (partial ? 1 : 0); `shouldSkip` сравнивает сохранённое число с текущим.
- Отпечаток входа включает последнюю неделю: новая неделя → полный прогон (nowcast пересчитывается), повтор без новой недели → no-op.

### 2.5. Витрина и общая таблица

- `v_weekly_cpi_sa` — long-форма + `as_of Date`, `partial UInt8`; комментарии: `date` — конец отчётного месяца (полный или текущий неполный), `as_of` — последняя использованная неделя, `partial` — 1 для nowcast.
- `v_key_rate_dashboard` — в `Tables` добавляется `weekly_cpi_sa`; CTE `wsa`:
  ```sql
  wsa AS (
      SELECT
          argMax(saar, tuple(partial, as_of, date)) AS saar,
          argMax(as_of, tuple(partial, as_of, date)) AS as_of,
          argMax(partial, tuple(partial, as_of, date)) AS partial
      FROM weekly_cpi_sa FINAL
  )
  ```
  Строка `fundamental/weekly_cpi_saar`, unit `% г/г (SAAR, weekly)`, `as_of = wsa.as_of`, `threshold = '> 4 % (SAAR, weekly nowcast)'`, `flag = wsa.saar > 4`, `stale = as_of < today() − 14`. Рядом остаётся `base_ipc_saar` (ЦБ, месячный, as_of 2026-09-30) — в одной таблице видно опережение: weekly nowcast 2026-10 с as_of 2026-10-12/19 против base SAAR 2026-09 с публикацией 14.10.2026.
- `series_catalog` — описание «полные месяцы + текущий неполный nowcast (partial=1)»; 2022+ alone p = 0,1896 и nowcast-статус публикуются.
- `scripts/mcp_check.py` — порог `>= 627` остаётся, 3 ряда; строка `partial=1` проверяется отдельно (`countIf(partial=1) <= 1`).

## 3. Приёмка

- `make import STAT=weekly_cpi_sa` — **209 полных + 1 nowcast** (`partial=1`, `as_of=2026-10-12` или `2026-10-19`), 210 строк; повтор — no-op, `count() FINAL` не меняется.
- Parity 209 полных с `docs/research/2026-10-22-weekly-core-proxy.json`: 2012-07 5,1813, 2019-01 7,8172, 2022-03 142,0173, 2026-09 5,8505 — допуск 0,01 п.п.
- `v_weekly_cpi_sa` — 3 × 210; `v_key_rate_dashboard` — строка `weekly_cpi_saar` с as_of последней недели и flag/stale; `base_ipc_saar` не изменился.
- `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `make all`, `make docs-check`, `dagu validate dagu/ppi.yaml` — зелёные.
- Протокол — комментарием в PR.

## 4. Границы

`v_rosstat_macro`, `rosstat_ipc_weeks_bulletins`, `rosstat_bi_cpi_weekly`, `ppi_sa`, панели 36–39, существующие гранты не меняются. Nowcast-строка — индикативная, не официальная SA ЦБ; `partial=1` и описание это фиксируют. 2009–2018 — бюллетени с 24–51 датой/год, grid-интерполяция; 2022+ alone не значим (p = 0,1896).
