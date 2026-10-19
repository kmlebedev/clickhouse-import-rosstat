# План: консенсус-бенчмарк №4 — медиана макроопроса ЦБ

**Дата:** 19 октября 2026 г.
**Пункт:** `key-rate/phase-4-consensus-benchmark` (`docs/ROADMAP_KEY_RATE.md` §6, фаза 4).
**Issue:** [#129](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/129).
**Спека:** [2026-10-19-key-rate-consensus-benchmark-design.md](../specs/2026-10-19-key-rate-consensus-benchmark-design.md).
**Ветка:** `feat/key-rate-consensus-benchmark`.

## Global Constraints

- Скрипт `scripts/key_rate_model.py` в ClickHouse **не пишет**; читает `v_cbr_mo_br` и `cbr_key_rate`; артефакты — локальные, gitignored.
- B4 **не входит** в `METHODS`, `FORECAST_METHODS`, `key_rate_model_forecast.json`, `forecast_log` и writer; основной CSV не меняется.
- Ошибки — в базисных пунктах: `(прогноз − факт) × 100`; положительная — прогноз завысил.
- Доступность волны — консервативно `wave_date + 6 дней`; для бэктеста — строго `<` события, для текущего блока — `<= today`.
- Годовая средняя — по календарным дням с fill-forward ставки; год завершён, если ряд покрывает 1 января и 31 декабря.
- Тесты — stdlib `unittest`, офлайн, транспорт мокается; без сети и БД.
- `make all` (gofmt, golangci-lint, vet, `go test -race`, сборки) и `make docs-check` — зелёные.
- Один PR в конце; спека и план — первые коммиты.

## Task 1: спека и план (этот коммит)

**Files:** `docs/superpowers/specs/2026-10-19-key-rate-consensus-benchmark-design.md`, `docs/superpowers/plans/2026-10-19-key-rate-consensus-benchmark.md`.

- Зафиксировать решения §3 спеки, границы, приёмку.
- Коммит: `docs: add consensus benchmark design and plan`.

## Task 2: парсер и загрузчик консенсуса

**Files:** `scripts/key_rate_model.py`, `scripts/key_rate_model_test.py`.

1. Константы:
   - `CONSENSUS_SOURCE = "cbr_mo_br"`, `CONSENSUS_PUBLICATION_LAG_DAYS = 6`, `CONSENSUS_MIN_DAYS_REMAINING = 30`;
   - `CONSENSUS_SQL` — `SELECT date, series, value FROM v_cbr_mo_br WHERE series LIKE 'key_rate_avg_%' ORDER BY date, series FORMAT JSON`.
2. `ConsensusForecast` (frozen dataclass): `wave_date`, `available_from`, `year`, `median`, `p10`, `p90`, `answers`.
3. `consensus_from_rows(rows)`:
   - regex `^key_rate_avg_(\d{4})_(median|p10|p90|answers)$`;
   - группировка по `(wave_date, year)`; медиана обязательна; p10/p90/answers опциональны;
   - дубль `(wave, year, stat)` — `ValueError`;
   - `answers` приводится к `int`, нецелое — `ValueError`;
   - `available_from = wave_date + 6 дней`;
   - сортировка по `(wave_date, year)`.
4. `load_consensus()`.
5. В `ch_query_from_fixtures` теста добавить ветку `if "FROM v_cbr_mo_br" in sql: return []`.
6. Тесты:
   - парсер трёх рядов + answers; `available_from` = +6;
   - отсутствие медианы — `ValueError`;
   - дубль — `ValueError`;
   - нецелый answers — `ValueError`;
   - `CONSENSUS_SQL` содержит `v_cbr_mo_br` и `FORMAT JSON`.

**Проверка:** `python3 scripts/key_rate_model_test.py` (через uv, см. команды ниже).

## Task 3: годовая средняя, hold и наблюдения

**Files:** `scripts/key_rate_model.py`, `scripts/key_rate_model_test.py`.

1. `_rate_asof(points, day)` — последняя точка `date <= day` (bisect/fill-forward); пустой ряд — `None`.
2. `annual_average_rate(points, year)` — календарные дни года; `None`, если ряд не покрывает год.
3. `sum_rates_between(points, start, end)` — сумма по календарным дням включительно.
4. `hold_forecast(points, year, available_from)` — формула спеки §3.1.
5. `consensus_observations(consensus, key_rate)` — выборка §3.1 (экс-анте + внутригодовые), `ConsensusObservation` с `year`, `wave_date`, `available_from`, `bucket`, `median`, `p10`, `p90`, `answers`, `actual`, `consensus_error_bp`, `hold_error_bp`; сортировка `(year, available_from)`.
6. `aggregate_consensus(observations, bucket)` — `n`, `consensus_rmse_bp`, `consensus_mae_bp`, `consensus_bias_bp`, `hold_rmse_bp`, `hold_mae_bp`, `hold_bias_bp`, `dm_stat`, `dm_p`; guard `n < 2` и нулевая дисперсия → `None`.
7. Тесты:
   - fill-forward и изменение ставки внутри года;
   - leap year 2024 (366 дней);
   - неполный год → `None`;
   - hold: экс-анте = ставка на дату доступности; внутригодовой = известный путь + текущая ставка;
   - бакеты и границы месяцев;
   - знак ошибки; DM на синтетике; `n=1` → `None`.

## Task 4: B4-проекция

**Files:** `scripts/key_rate_model.py`, `scripts/key_rate_model_test.py`.

1. `ConsensusPath` (frozen dataclass): `meeting_date`, `wave_date`, `available_from`, `year`, `annual_median`, `rate_before`, `implied_level`, `step_bp`, `days_remaining`.
2. `consensus_path(consensus, key_rate, meeting, rate_before)`:
   - последняя волна с медианой на `year(meeting)` и `available_from < meeting`;
   - если `available_from < 1 января Y`: `R_known = 0`, `N_rem = N_Y`, `r_implied = median_Y`;
   - иначе формула спеки §3.2;
   - guard `N_rem >= 30`, покрытие ряда, `-100 < r_implied < 100`; иначе `None`.
3. `consensus_path_predictions(rows, consensus, key_rate)` — `Prediction(method="consensus", probs=None)` для всех `rows`, где `consensus_path` не `None`.
4. `b4_metrics(predictions)` — RMSE/MAE/hit rate/QWK (qwk/hit rate — существующие функции).
5. `b4_significance(b4, predictions)` — DM на общих заседаниях против B1–B3 и методов; строки `(against, n, delta_rmse_bp, dm_stat, dm_p)`.
6. Тесты:
   - формула на синтетике (известный путь и медиана);
   - январское заседание — специальный случай;
   - guard 30 дней (декабрьское заседание пропускается);
   - нет волны → `None`;
   - `rate_before` из `Row`;
   - B4 не в `METHODS`/`FORECAST_METHODS`;
   - DM на пересечении дат.

## Task 5: текущий консенсус, сценарий удержания, отчёт и `run_summary`

**Files:** `scripts/key_rate_model.py`, `scripts/key_rate_model_test.py`.

1. `current_consensus(consensus, today)` — последняя волна `available_from <= today`; строки по годам (median/p10/p90/answers).
2. `hold_scenario_average(key_rate, today, meeting, rate_before, model_level)` — формула спеки §3.3; `None`, если нет `ordered_logit`/будущего заседания.
3. `render_consensus_section(...)` — раздел спеки §5; таблицы через существующие `_table`/`_fmt`.
4. `render_report(..., consensus: Optional[str] = None)` — вставка перед «Текущим прогнозом»; пункт в «Что не проверено».
5. `run_summary`:
   - `load_consensus()`, `consensus_observations`, `consensus_path_predictions`, `current_consensus`, `hold_scenario_average`;
   - `sample_meta` — `consensus_waves`, `consensus_pairs`, `b4_folds`;
   - строка консоли `консенсус-бенчмарк: волн N, годовых пар M, B4-фолдов K`.
6. Тесты:
   - текущая волна и граница `<= today`;
   - сценарий удержания на синтетике; отсутствие `ordered_logit` → `None`;
   - `render_report` содержит раздел и не ломается при `consensus=None`;
   - полный офлайн `run_summary` с пустым консенсусом (ветка `FROM v_cbr_mo_br` → `[]`);
   - B4 нет в JSON прогноза и в `METHODS`.

**Проверка:** офлайн-тесты, `make key-rate-model` (живой), затем числа — в протокол.

## Task 6: статья и документы

**Files:** `docs/publications/2026-10-18-key-rate.md`, `docs/publications/2026-10-18/checklist.md`, `docs/KEY_RATE_MODEL.md`, `docs/ROADMAP_KEY_RATE.md`, `docs/KEY_RATE_PUBLICATIONS.md`, `README.md`, `ARCHITECTURE.md`.

- §4.1 статьи: абзац о точности консенсуса (1–3/4–6/7–9/10–12/экс-анте; RMSE/MAE; naive hold; DM p из прогона; без превосходства без значимости) + сценарий удержания (`14,47 %` против `14,50 %`, `−3 б.п.`; B4 `−29 б.п.`); новая сноска `[^consensus_benchmark]`.
- `checklist.md`: строка «Консенсус-бенчмарк #129 — измерен».
- `KEY_RATE_MODEL.md` §6.4: измерение, статус, ограничения.
- `ROADMAP_KEY_RATE.md`: строка 244 ❌ → ✅ с датой, числами, ссылками; §4.2 №5 — отметка.
- `KEY_RATE_PUBLICATIONS.md` §6 — статус строки консенсуса.
- `README.md` — `make key-rate-model` включает консенсус-бенчмарк.
- `ARCHITECTURE.md` — описание скрипта; уточнение про +6 дней vs `source_publications`.

## Task 7: финальная проверка и PR

- `make all` — зелёный.
- `uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_model_test.py` — зелёный.
- `make key-rate-model` — отчёт с разделом; записать числа.
- `make docs-check` — зелёный.
- `make mcp-check` — если dev-ClickHouse поднят; расхождения вне пункта назвать.
- Ревью: task-review → fix-loop; финальное ревью ветки.
- PR: `Closes #129`, описание с пунктом, спекой/планом, формулами, командами и протоколом живой приёмки комментарием.
- Issue #129: `Branch`, `Spec / Plan`, `Status = In progress`; `Done` — после мержа.

## Ожидаемые числа (срез 2026-10-19, проверить живым прогоном)

- `v_cbr_mo_br`: 43 волны, 2021-05-01 … 2026-09-01, 642 ряда, 9 348 точек; завершённые годы 2021–2025.
- Годовой бенчмарк: 41 пара (экс-анте 4, 10–12: 8, 7–9: 9, 4–6: 10, 1–3: 10).

| Бакет | n | cons RMSE | cons MAE | cons bias | hold RMSE | hold MAE |
|---|---:|---:|---:|---:|---:|---:|
| экс-анте | 4 | 280,7 | 276,3 | −170,6 | 251,4 | 247,2 |
| 10–12 | 8 | 365,4 | 293,7 | −32,9 | 326,5 | 265,6 |
| 7–9 | 9 | 205,9 | 173,5 | −39,6 | 306,1 | 241,3 |
| 4–6 | 10 | 73,6 | 48,5 | −38,4 | 109,7 | 88,0 |
| 1–3 | 10 | 9,0 | 5,9 | −3,7 | 17,8 | 11,0 |

- B4 на 2026-10-23: волна 2026-09-01, медиана 14,5, `r_implied ≈ 13,7065`, шаг ≈ −29,3 б.п.
- Сценарий удержания: `ordered_logit` уровень 13,3664 → среднегодовая ≈ 14,471 %; консенсус 14,500 %; разница ≈ −2,9 б.п.
- B4-бэктест: число фолдов и DM-p — после реализации; ожидаемо ~40 заседаний 2021-05+.

## Команды

```bash
# офлайн-тесты модели
uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy \
  --python 3.12 python3 scripts/key_rate_model_test.py -v

# живой прогон (ClickHouse поднят пользователем; ключи подключает Makefile)
make key-rate-model

# обязательные проверки
make all
make docs-check
```
