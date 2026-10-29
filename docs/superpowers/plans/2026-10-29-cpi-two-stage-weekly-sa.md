# План: двухэтапный CPI — недельный SA (as-of) → Stage 1 → Stage 2

**Пункт:** `key-rate/phase-4-cpi-two-stage-weekly-sa`, issue [#401](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/401), `role:ml`. **Спека:** [2026-10-29-cpi-two-stage-weekly-sa-design.md](../specs/2026-10-29-cpi-two-stage-weekly-sa-design.md) — предрегистрация; план её не меняет.
**Ветка:** `exp/cpi-two-stage-weekly-sa`, worktree `.worktrees/cpi-two-stage-sa`. **Старт кода:** после мержа #400 (WIP = 1). **Правила:** TDD, офлайн-тесты без ClickHouse, R и сети (мост X-13 подменяется); ClickHouse только читается через `lib.clickhouse.ch_query`; запуск — через `make`.

## Порядок

```text
W0 as-of конвенция ──┬─> W1 as-of недельный X-13 ──> W2 КУ и T_SAX ─┐
                     └─> W3 таргеты, фолды ──────────────────────────┼─> W4 Stage 1 ──> W5 Stage 2 ──> W6 robustness ──> W7 отчёт
```

Стоп-условия — спека §7: W1 parity не прошёл → ничего дальше; W4 `fail` → Stage 2 не строится.

## W0: конвенция as-of и регрессионная база

**Файлы:** параметр cutoff в `cpi_v4_grid.py`/`cpi_monthly.py` (поведение по умолчанию не меняется), `cpi_two_stage_asof.py`, тесты.
- [ ] Тест: старый cutoff воспроизводит эталон #396 §7.1 (допуск 1e-6); новый (`P_k`) не видит данных позже `P_k` (недели и месяцы).
- [ ] Календарь: `P_k` = 9-е `k+1` 19:00 (`publication_calendar`); проверка фактических дат Росстата на выборке месяцев, расхождения — таблицей в отчёте.
**Done:** регрессия зелёная; тест на утечку; таблица проверки дат.

## W1: as-of перефит недельного X-13 (главная дыра)

**Файлы:** `scripts/models/cpi/weekly_sa_asof.py` (+`_test.py`, `_x13.R` — тот же скрипт, что `weeklysa/data/weekly_cpi_sa_x13.R`, SHA-256 пин).
- [ ] Тесты на синтетике: порт `buildGrid`/`fourWeekIndex`/`monthlyLastInMonth`/`partialMonth` совпадает с примерами `weeklysa/grid_test.go`.
- [ ] Parity: на полной выборке `sa_index`/`sa_4w`/`saar` совпадают с `weekly_cpi_sa` (`max|diff| ≤ 1e-6`, 209 + 1 `partial`).
- [ ] As-of ряд по `P_k` 2013+; тест на утечку (изменение будущих недель не меняет значение месяца `k`).
- [ ] Ревизии `as-of − final`, сдвиг-стабильность, критерий качества (RMSE ≤ 0,20, \|bias\| ≤ 0,05, корр. ≥ 0,95 на 2016+).
**Done:** gate W1 (спека §4) зафиксирован числами; `make cpi-weekly-sa-asof(-test)`.

## W2: КУ и таргет `T_SAX`

**Файлы:** `cpi_two_stage_targets.py` (+тест); переиспользует `level_from_mm`, `ex_utilities_mm`, мост X-13 из `cpi_sa_reconciliation.py` (#400).
- [ ] `T_SAX` окончательный и as-of (винтажная история на `P_k`); тесты формулы и утечки.
- [ ] Варианты `V0/V1/V2` входа; выбор на dev 2016–2021 по RMSE Stage 1 h1; запись выбора.
**Done:** ряды `T_SAX`, выбранный вариант, отчётная таблица dev.

## W3: таргеты, доступность, фолды

- [ ] `T_CORE`, `T_NSA`; доступность (Росстат 9-е, ЦБ 18-е `M+1`); purge по спеке §3; `common_folds`.
- [ ] Хэши `feature_sha256`/`predictions_sha256` для детерминизма.
**Done:** тесты доступности и purge.

## W4: Stage 1

**Файлы:** `cpi_two_stage_s1.py` (+тест).
- [ ] Признаки (as-of `P_k`), запрет ожиданий/SA ЦБ/МП; модели CatBoost и ElasticNetCV; baselines B1/B2/B3/`weekly_naive`/`v3_direct@P_k`.
- [ ] Walk-forward с 2016-01, min train 24, OOF с 2018-01; dev/holdout; gate S1 (спека §6.1) с Holm по двум целям.
**Done:** `s1_oof.csv`, решение `pass/partial/fail`; `make cpi-two-stage-s1(-test)`.

## W5: Stage 2

**Файлы:** `cpi_two_stage_s2.py` (+тест).
- [ ] Признаки `v3` + `s1_*` (NaN где не определён); nested OOF (тест: `s1_*` строки обучения не использует данные после `P_j`).
- [ ] Модели `v5_s1`, `v5_no_s1`; сравнения с `v3_direct@P_k`, `v4a_direct@P_k`, B3; gate S2 (спека §6.2).
**Done:** ячейки `T_SAX`/`T_CORE` × h1/h3/h6, `build/ml-gate/cpi_two_stage.{json,md}`.

## W6: robustness

- [ ] R1–R7 (спека §4 W6); знак по R1–R3 входит в решение.

## W7: отчёт, паспорт, docs

- [ ] Итоговый отчёт, `ML_LOG` паспорт, повторный прогон двух ячеек (`predictions_sha256`), «что не проверено».
- [ ] `make`-цели в `.PHONY`, таблица команд `ml-engineer`, роадмап; `make all`, `make docs-check`.
- [ ] Один PR с `Closes #401` вне backticks; протокол приёмки — комментарием в PR.
