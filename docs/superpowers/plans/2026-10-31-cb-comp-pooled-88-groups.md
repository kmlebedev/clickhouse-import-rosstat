# План Task 10 (#438): pooled CatBoost на 88 группах, окно 2016+

> Спека — [2026-10-31-cb-comp-pooled-88-groups-design.md](../specs/2026-10-31-cb-comp-pooled-88-groups-design.md). Ветка `exp/cb-comp-88-groups-task10`, worktree `.worktrees/cb-comp-88-task10`. Роль `ml`.

## Шаги

1. **Спека и план** — первым коммитом, до кода и прогона.
2. **Тесты (офлайн, TDD)** — `scripts/models/cb_comp/cb_comp_pooled_test.py`:
   - схема 66 колонок: значения только у своих листов, остальные NaN; `sheet_profile`/`group_code`/`weight_share` на месте;
   - `weight_share`: доля среди групп с SA, лаг года, fallback 2016, ЖКУ исключена;
   - `annual_windows`: границы `2016-01`/`2010-03`, cutoff по `available_from_mes < cutoff_for(O_min)`, тестовые месяцы года не в train, шоки исключены;
   - no-leakage: origin строки = `t − (h−1)`, `cutoff(origin)` строго раньше доступности таргета;
   - pooled на синтетике: два фита с одним seed дают одинаковые прогнозы, NaN-таргеты пропускаются, группы без прогноза выпадают из агрегата;
   - агрегат: веса с лагом год, покрытие, нормализация;
   - G10: h1 хуже → fail; p ≥ 0,10 → fail; h3/h6 не ниже → fail; h3 ниже при p < 0,10 и h1 не хуже → pass;
   - `combo3_pooled` — равные веса, общие месяцы;
   - R10-пины и паспорт/JSON (даты сериализуются, NaN → null).
3. **Код** — `cb_comp_pooled.py`:
   - `load_inputs`/`en.prepare`, `pooled_feature_names`, `pooled_matrix`, `weight_share`;
   - `training_cutoff`, `annual_windows`, `train_rows_pooled`, `fit_predict_pooled`, `predict_pooled`;
   - `combo3` через `cb_comp_h1_bs_combo` (`load_regressors`, `bs_predict`, `combine_mean`) и `cb_comp_live_bottomup` (`fit_predict`, `aggregate_predictions`);
   - `evaluate`, `g10_check`, `r10_check`, паспорт, `predictions.csv`/`result.json`.
4. **Make** — `cb-comp-pooled` / `cb-comp-pooled-test`; коммит кода и тестов **до** прогона.
5. **Прогон** — `make cb-comp-pooled ARGS="--archive /Users/whitefox/Downloads/BusinessSurveyCBRCpiForecastingSourceCode"`; main + control; повтор для детерминизма (`cmp`), логи — в `build/cb-comp-pooled/`.
6. **Отчёт** — `docs/research/2026-10-31-cb-comp-88-groups-task10.{md,json}`: R10, таблица RMSE/отношений, G10, попарные DM/CI, MCS, контрольное окно, `combo3_pooled`, «что не проверено».
7. **Роадмап/README/site-pages** — строка Task 10 в пункте #419, README, `docs/site-pages.json`; `make all`, `make docs-check`.
8. **PR** — `Closes #438`, протокол приёмки комментарием, Project V2 → `In Review` (Project item 288934640, поля `Branch`/`Spec / Plan` заполнить после коммита).

## Ожидаемое время

- Тесты — секунды.
- Прогон: 48 pooled-моделей на 3–8 тыс. строк + пересчёт `live_cb` (352 CatBoost) и BS — ориентир 30–60 мин; при необходимости `--jobs` не вводится (детерминизм важнее).

## Риски

- `group_code` на 87 группах может переобучаться — это проверяемая гипотеза, не повод тюнить.
- Годовой фит h6 использует ~30 месяцев 2016+; проигрыш может означать нехватку данных, а не плохую идею.
- `weight_share` строго за год `M.year−1`: для main-строк 2016 и control-окна 2010–2015 это NaN (весов этих лет нет), fallback не используется.
- CatBoost-потоки могут дать небайтовый повтор — тогда `thread_count=1` и повтор обоих прогонов.
- Прогон требует ClickHouse и локальный архив CB_COMP; в CI не идёт (тесты офлайн).

## Итог Task 10

- **G10 не пройден:** `pooled_main` 1,105 / 1,037 / 1,225 / 0,955 к CB_COMP (h1/h2/h3/h6) против 1,035 / 0,957 / 1,010 / 0,894 у `combo3`; h1 хуже, h3/h6 не ниже (DM p 0,53 / 0,23 / 0,72). `combo3` остаётся базой.
- **R10a/b/c ✅, R10d буквально ❌:** взвешенное покрытие 85–87 из-за весов `803`/`930.АГ`; предсказаны все 87 групп. Гейт формально не засчитан, по числам не пройден.
- **Справочно:** `combo3_pooled` — 0,992 / 0,921 / 1,041 / 0,866, не значимо; `pooled_control` хуже main (1,159 / 1,124 / 1,342 / 1,311).
- Отчёт — `docs/research/2026-10-31-cb-comp-88-groups-task10.md`, JSON рядом.
