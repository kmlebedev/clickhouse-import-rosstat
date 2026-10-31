# План Task 12 (#440): агрегатный VAR/BVAR на h3/h6 в комбинации `combo3`

> Спека — [2026-10-31-cb-comp-h3-h6-var-design.md](../specs/2026-10-31-cb-comp-h3-h6-var-design.md). Ветка `exp/cb-comp-88-groups-task12`, worktree `.worktrees/cb-comp-88-task12`. Роль `ml`.

## Шаги

1. **Спека, план** — первым коммитом, до кода и прогона.
2. **Тесты (TDD, офлайн)** — `scripts/models/cb_comp/cb_comp_h3_h6_var_test.py`:
   - панель: последняя ставка на конец месяца, среднее USD за месяц, уровень ожиданий, лог м/м ИПЦ; пропуск → ошибка;
   - origin/утечка: обучение по `M − h`, изменение будущего значения не меняет прогноз;
   - BIC-лаг на синтетическом VAR(1)/VAR(2); правило идентифицируемости; dummies только в train, future exog = 0;
   - прогноз и накопление: сумма первых h шагов, `(exp−1)·100`;
   - BVAR: posterior mean, детерминизм, форма и накопление;
   - комбинация `combo3_var16` — среднее, месяцы без одного прогноза исключаются;
   - G12: границы p = 0,10; h1 +0,02; улучшение только на h6; MCS-условие;
   - R12a: пины `combo3`/`live_cb`; JSON: даты/NaN.
3. **Код** — `cb_comp_h3_h6_var.py`: загрузка панели, VAR/BVAR, `combo3` пересчётом Task 9, `combo3_var16`, оценка, R12, G12, артефакты. Переиспользовать `cb_comp_live_en.prepare`, `cb_comp_live_bottomup.fit_predict/aggregate_predictions/errors_on`, `cb_comp_h1_bs_combo.load_regressors/bs_predict/combine_mean`, `compare_to_cb`, `compare_pair`, `mcs_included`.
4. **Make** — `cb-comp-h3-h6-var` / `-test`; коммит кода **до** прогона.
5. **Входы** — скопировать из `.worktrees/cb-comp-88-task9/build/` `cb-comp-groups/panel.csv` (`aec91f99…`), `cb-comp-parity/{public_panel,sheet_map}.csv` (`47176baa…`, `0d8a6ab5…`); sha256 сверить с паспортом Task 9. Архив — `--archive .worktrees/cb-comp-archive/build/cb-comp/extracted/BusinessSurveyCBRCpiForecastingSourceCode`.
6. **Прогон** — `make cb-comp-h3-h6-var ARGS="--archive …"`; R12a/b/c зелёные; G12; повтор для детерминизма (`cmp` предсказаний и JSON).
7. **Отчёт** — `docs/research/2026-10-31-cb-comp-88-groups-task12.{md,json}`: короткий вывод, R12, таблица против CB_COMP, попарно против `combo3`/`live_cb`, MCS, G12, BVAR, справочные строки, «что не проверено».
8. **Роадмап/README/site-pages** — строка Task 12 в пункте #419, `docs/site-pages.json`, README; `make all`, `make docs-check`.
9. **PR** — `Closes #440`, протокол приёмки комментарием, Project V2 → In Review.

## Риски

- **Короткий train на первых месяцах:** 2016-01…2018-07 — 30 наблюдений; правило идентифицируемости отсекает лаг 6 на первых h6-origin. Если VAR не сходится ни на одном лаге — стоп, а не «подбор» окна.
- **`pe` — уровень:** возможен unit root; BIC и Minnesota prior в BVAR — меры против взрыва; NaN/inf в прогнозе — стоп.
- **`combo3` может не воспроизвестись** из-за версии CatBoost/входов: R12a с допуском 1e-6; расхождение — стоп и разбор, не «подгонка».
- **G12 может не пройти:** это законный результат; отчёт публикует отрицательный результат, `combo3` остаётся базой.
- **MCS-условие** включено в G12 (прочтение acceptance); если combo4 выпадает из MCS — гейт не пройден, даже при p < 0,10.
