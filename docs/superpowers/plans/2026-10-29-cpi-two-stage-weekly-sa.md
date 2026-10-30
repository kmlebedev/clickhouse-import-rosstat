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

**Файлы:** `scripts/models/cpi/weekly_sa_asof.py` (+`_test.py`); X-13 — продовый `weeklysa/data/weekly_cpi_sa_x13.R` (пин SHA-256 в коде).
- [x] Тесты (порт `weeklysa/grid_test.go` с теми же ожиданиями; доступность недели `date + 2`; утечка; parity; gate): 36 тестов, мутационная проверка 5/5 после дополнения теста.
- [x] Parity: на полной выборке `sa_index`/`sa_4w`/`saar` совпадают с `weekly_cpi_sa` (210 строк = 209 + 1 `partial`), `max|diff| = 0,0` ≤ 1e-6.
- [x] As-of срезы по `P_k` 2013-01 … 2026-10 (165; 1 сбой X-13 — origin 2014-08, записан); утечки нет (последняя неделя + 2 дня ≤ cutoff на всех срезах).
- [x] Ревизии первый выпуск → окончательная: RMSE 0,070 (2016+), 0,165 (2013–2015); дрейф при последующих перефитах 0,08–0,10.
- [x] Критерий качества на 2016+: RMSE 0,158, bias −0,018, корр. 0,973 → **gate W1 = pass**. Отчёт: `docs/research/2026-10-30-cpi-two-stage-w1-weekly-sa-asof.md`.
**Done:** gate W1 зафиксирован числами; `make cpi-weekly-sa-asof(-test)`; вход Stage 1 — NaN до 2016-01.

## W2: КУ и таргет `T_SAX`

**Файлы:** `scripts/models/cpi/cpi_two_stage_targets.py`, `cpi_two_stage_w2.py` (+тесты); переиспользует `ex_utilities_mm`, `x13_series`, мост X-13 из `cpi_sa_reconciliation.py` и as-of срез W1.
- [x] `T_SAX` окончательный и винтажный (`≤ O−1`); тесты формулы, утечки, ревизий (23 теста, мутационная проверка 7/7 после дополнения).
- [x] Варианты `V0/V1/V2`; протокол выбора зафиксирован в спеке до результатов (`d7d129f`), код — до запуска (`47c361d`).
- [x] Выбор на dev 2018-01…2021-12 (45 общих origin'ов): RMSE `V0` 0,2084, `V1` 0,2096, `V2` 0,2156 → **`V0`**. Отчёт: `docs/research/2026-10-30-cpi-two-stage-w2-tsax-variants.md`.
**Done:** вариант `V0` зафиксирован, ряды `T_SAX` реализованы; находка — календарь `TARIFF_DATES` расходится с данными; модель отбора не побила `weekly_naive` (риск для gate S1).

## W3: таргеты, доступность, фолды

**Файлы:** `scripts/models/cpi/cpi_two_stage_folds.py` (+тест); использует `cpi_two_stage_targets` (W2), `weekly_sa_asof.publication_cutoff` (W0/W1), таблицу релизов Росстата.
- [x] `T_CORE`, `T_NSA`, `T_SAX` (`TargetData`, загрузчики); доступность (Росстат — день релиза/9-е, ЦБ — 18-е `M+1`); purge по доступности = формуле спеки §3 (тест); `common_folds`; `make_folds`, `assert_no_lookahead`.
- [x] Отпечатки `predictions_digest`/`feature_digests`/`fold_summary`; детерминизм (два прогона побайтно одинаковы).
- [x] Документ фолдов: `docs/research/2026-10-30-cpi-two-stage-w3-targets-folds.md` (Stage 1: 94–105 фолдов на ячейку, Stage 2: 222–233).
**Done:** 32 теста, мутационная проверка 7/8 (выживший — эквивалентный); `make cpi-two-stage-folds(-test)`.

## W3b: сверка данных CPI с датасетом CB_COMP

**Файлы:** `scripts/models/cpi/cb_comp_data_reconcile.py` (+`_test.py`); чтение архива — `cb_comp_parity.resolve_archive`/`check_archive` (pandas/openpyxl).
- [x] Протокол в спеке до результатов (`d961f27`), код — до запуска (`b70d600`); 26 тестов, мутационная проверка 7/8 (выживший эквивалентен).
- [x] Регрессионная база: RMSE CB_COMP 0,158805 / 0,296655 / 0,443623 / 0,971459, накопление `TARGET_FULL`, источники — 0 расхождений.
- [x] Таргет: `T_SAX` (w = 0,10) против `TARGET_FULL[1]` RMSE 0,117; КУ — идентичен; калибровка `w` → **0,08** (RMSE 0,088, `aligned`); core — **кода нет в архиве**; групповая сверка и вес из `cpi_weights_tovar` — не выполнены. Отчёт: `docs/research/2026-10-30-cpi-two-stage-w3b-cb-comp-data.md`.
**Done:** решение `aligned` при primary w = 0,08 зафиксировано числами; W5b на истине `TARGET_FULL`, обучающая цель — `T_SAX` (w = 0,08).

## W4: Stage 1

**Файлы:** `scripts/models/cpi/cpi_two_stage_s1.py` (+тест).
- [x] Протокол (признаки, модели, baselines, детали gate) зафиксирован в спеке до результатов (`a8fab8e`); код — до запуска (`aca0806`); 49 тестов, мутационная проверка 10/10 после дополнения.
- [x] Walk-forward с 2016-01, min train 24, фолды W3; dev 2018–2021 (выбор кандидата), holdout 2022+; baselines B1/B2/B3/`weekly_naive`/`v3_direct@P_k`.
- [x] Gate S1 (спека §6.1, Holm): **`fail`** — обе цели, условия 1 и 3 не выполнены (`T_SAX`: 1,238 против `v3_direct` 1,134 и `weekly_naive` 1,151; `T_CORE`: 1,346 против `weekly_naive` 1,170). По спеке §7 Stage 2 (W5) **не строится**. Отчёт: `docs/research/2026-10-30-cpi-two-stage-w4-stage1-gate.md`.
**Done:** решение `fail` зафиксировано числами; `s1_oof.csv`, `s1_gate.json`; `make cpi-two-stage-s1(-test)`. Дальнейшие шаги (новая итерация/пересмотр постановки/закрытие) — решение владельца.

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
