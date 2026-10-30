# План: сверка SA и core из сырого ИПЦ и недельного SA с ЦБ

**Пункт:** `key-rate/phase-4-cpi-sa-reconciliation`, issue [#399](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/399), `role:research`. **Спека:** [2026-10-29-cpi-sa-reconciliation-design.md](../specs/2026-10-29-cpi-sa-reconciliation-design.md).
**Ветка:** `research/cpi-sa-reconciliation`, worktree `.worktrees/cpi-sa-reconciliation`. **Правила:** TDD, офлайн-тесты без ClickHouse, R и сети (мост X-13 подменяется); ClickHouse только читается через `lib.clickhouse.ch_query`; запуск — через `make`.

## Task 1: чистые функции и тесты

**Файлы:** `scripts/models/cpi/cpi_sa_reconciliation.py`, `cpi_sa_reconciliation_test.py`.
- [ ] `level_from_mm(months, mm)` — накопленный уровень (проверка непрерывности месяцев).
- [ ] `ex_utilities_mm(nsa, util, w)` — `(π − w·u)/(1 − w)`; `w ∈ (0,1)`, несовпадение месяцев — ошибка.
- [ ] `metrics(ours, ref, lo, hi)` — n, RMSE, MAE, bias, corr, наклон, RMSE после калибровки; месяцы пересечения; NaN пропускаются; n < 3 — ошибка.
- [ ] `decisions(results)` — решения по таблице §5 спеки, чистая функция от метрик.
- [ ] Тест на утечку as-of: изменение значений после `t` не меняет SA-значение месяца `t` (STL и мост X-13 на фейковом runner).

## Task 2: мост X-13 (R) и загрузка данных

**Файлы:** `cpi_sa_reconciliation_x13.R`, функции `run_x13`, `load_*` в скрипте.
- [ ] R-скрипт: вход CSV уровня, выход CSV (`full`, `rt` м/м, п.п.); `first_rt` как аргумент; ошибка модели → NA.
- [ ] `run_x13(levels, first_rt, runner)` — подменяемый runner (R в CI нет).
- [ ] `load_nsa`, `load_cbr_sa`, `load_cbr_core`, `load_utilities`, `load_weekly_sa4w` — SQL с `FINAL`, `FORMAT JSON`, чтение только.

## Task 3: прогон и отчёт

- [ ] `make cpi-sa-reconciliation` → `build/cpi-sa/reconciliation.json` и сводка; воспроизводимость: два прогона дают одинаковые метрики.
- [ ] Отчёт `docs/research/2026-10-29-cpi-sa-reconciliation.md` по шаблону research (вывод, данные, метрики, решения по §5, что не проверено).

## Task 4: оформление

- [ ] Цели `make cpi-sa-reconciliation(-test)` в `.PHONY`; строки в таблице команд `ml-engineer`.
- [ ] Запись в `docs/ROADMAP_KEY_RATE.md` (статус и результат), `make all`, `make docs-check`.
- [ ] Один PR с `Closes #399` вне backticks; протокол приёмки — комментарием в PR.
