# План: двухэтапный CPI — недельный SA (as-of) → Stage 1 → Stage 2

**Пункт:** `key-rate/phase-4-cpi-two-stage-weekly-sa`, issue [#401](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/401), `role:ml`. **Спека:** [2026-10-29-cpi-two-stage-weekly-sa-design.md](../specs/2026-10-29-cpi-two-stage-weekly-sa-design.md) — предрегистрация; план её не меняет.
**Ветка:** `exp/cpi-two-stage-weekly-sa`, worktree `.worktrees/cpi-two-stage-sa`. **Старт кода:** после мержа #400 (WIP = 1). **Правила:** TDD, офлайн-тесты без ClickHouse, R и сети (мост X-13 подменяется); ClickHouse только читается через `lib.clickhouse.ch_query`; запуск — через `make`.

## Порядок

```text
W0 as-of конвенция ──┬─> W1 as-of недельный X-13 ──> W2 КУ и T_SAX ─┐
                     ├─> W3 таргеты, фолды ──────────────────────────┼─> W4 Stage 1 ──> W5 Stage 2 ──┬─> W5b сравнение с CB_COMP ─┐
                     └─> W3b сверка данных с CB_COMP (архив) ────────┴───────────────────────────────┘                            ├─> W6 robustness ──> W7 отчёт
```

Стоп-условия — спека §7: W1 parity не прошёл → ничего дальше; W4 `fail` → Stage 2 не строится; W3b регрессионная база не воспроизвелась → W5b `incomplete`. W3b может изменить primary `w` только до W2/W4.

## W0: конвенция as-of и регрессионная база

**Файлы:** параметр cutoff в `cpi_v4_grid.py`/`cpi_monthly.py` (поведение по умолчанию не меняется), `cpi_two_stage_asof.py`, тесты.
- [x] Тест: старый cutoff воспроизводит эталон #396 §7.1 (допуск 1e-6; живой gate пройден); новый (`P_k`) не видит данных позже `P_k` (недели и месяцы) — `cpi_asof_test.py`, 15 тестов, мутационная проверка 3/3.
- [x] Календарь: `P_k` = 9-е `k+1` 19:00 согласован с `publication_calendar` (fallback-правило).
- [x] Даты публикаций Росстата: 94 месяца (декабрь 2018 … сентябрь 2026) из релизов и архивных снимков графика (2018-12 … 2021-06 — запланированные сроки); правило 9-го ошибается в обе стороны (−6…+8 дней); конвенция `publication` принимает точные даты (`release_dates`). **Не выполнено** для 2013 … ноябрь 2018 (старые релизы gks.ru дат не содержат): либо внешний источник, либо выводы по 2013–2018 условны (правило 9-го); фикстура `publication_calendar.csv` содержит ошибочную точку 2026-09-09 (факт 2026-09-11) — отдельной задачей.
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

## W3b: сверка данных CPI с датасетом CB_COMP

**Файлы:** `scripts/models/cpi/cb_comp_data_reconcile.py` (+`_test.py`); чтение `data_panel_bi_sa.fst` — R `fst` (как в `scripts/cb_comp_panel_export.R`), XLSX — openpyxl; архив — `.worktrees/cb-comp-archive/build/cb-comp/extracted/` или скачивание с проверкой md5 `797f00518c3a0fe6d4cd2c2ad22aa70a`.
- [ ] Тесты на синтетических фикстурах: накопление `TARGET_FULL` из `[1]`, normal-флаг, метрики совпадения, правило допуска `0,01` п.п.
- [ ] Регрессионная база: RMSE CB_COMP на 54 normal-месяцах = 0,158805 / 0,296655 / 0,443623 / 0,971459 (1e-6).
- [ ] Таргет, компоненты, КУ и вес `w` (спека W3b п. 2–4); решение `aligned`/`misaligned`.
**Done:** отчёт с таблицами совпадений и расхождений; решение по `w` зафиксировано до W2.

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

## W5b: сравнение точности с CB_COMP

**Файлы:** `scripts/models/cpi/cpi_two_stage_cb_comp.py` (+тест).
- [ ] Выравнивание окон и origin CB_COMP, истина `TARGET_FULL`, normal 54 / все 67; тест на воспроизведение RMSE CB_COMP.
- [ ] Варианты `v5_wf`, `v5_frozen` (train 2010-03…2018-12, один раз), ориентиры `v3_direct@P_k` и #190; h1/h3/h6 (+h2 справочно).
- [ ] DM-HLN, 95 % CI ΔRMSE, TOST δ = 10 %, Бонферрони по 3 горизонтам; классификация «лучше/паритет/хуже/не определено» (спека §6.3).
**Done:** `build/ml-gate/cpi_two_stage_cb_comp.{json,md}`, раздел отчёта.

## W6: robustness

- [ ] R1–R7 (спека §4 W6); знак по R1–R3 входит в решение.

## W7: отчёт, паспорт, docs

- [ ] Итоговый отчёт, `ML_LOG` паспорт, повторный прогон двух ячеек (`predictions_sha256`), «что не проверено».
- [ ] `make`-цели в `.PHONY`, таблица команд `ml-engineer`, роадмап; `make all`, `make docs-check`.
- [ ] Один PR с `Closes #401` вне backticks; протокол приёмки — комментарием в PR.
