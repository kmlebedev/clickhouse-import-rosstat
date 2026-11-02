# План Task 11 (#439): h3 — EN по 88 группам, окно 2016+ и помесячное переобучение

> Спека — [2026-10-31-cb-comp-h3-en16-combo-design.md](../specs/2026-10-31-cb-comp-h3-en16-combo-design.md). Ветка `exp/cb-comp-88-groups-task11`, worktree `.worktrees/cb-comp-88-task11`. Роль `ml`.

## Шаги

1. **Спека и план** — первым коммитом, до кода и прогона.
2. **Тесты (TDD, офлайн)** — `scripts/models/cb_comp/cb_comp_live_en16_test.py`:
   - константы: `EN_START = 2016-01`, маржинал 0,02, пины R11a, набор кандидатов;
   - `training_rows_upto`: первая строка `2016-01 + h`, последняя `M − h`, строка M не входит, выбросы и NaN-таргеты исключены;
   - нет будущего: порча таргетов после `M − h` и признаков после origin не меняет прогноз для M;
   - EN — конфигурация B3 (alphas, l1_ratio, `TimeSeriesSplit(3)`, `random_state = SEED`), имена без `month`;
   - медиана импутации — только по train;
   - `combo4` — среднее на общих месяцах, равные веса; границы G11 (p = 0,10, маржинал ровно 0,02, MCS-членство).
3. **Код** — `cb_comp_live_en16.py`: переиспользовать `cb_comp_live_en.prepare/make_enet/enet_fit_predict/en_names/better_than_cb`, `cb_comp_live_bottomup.fit_predict/aggregate_predictions/errors_on/rmse/t_live`, `cb_comp_h1_bs_combo.BS_START/ARTICLE_OUTLIERS/load_regressors/bs_predict/combine_mean`, `cb_comp_parity/cb_comp_bottomup` для архива и строк; R11a, G11, справочные строки, артефакты.
4. **Make** — `cb-comp-live-en16` / `-test`; коммит кода **до** прогона.
5. **Прогон** — `make cb-comp-live-en16`; повтор для детерминизма (`cmp` предсказаний и JSON).
6. **Отчёт** — `docs/research/2026-10-31-cb-comp-88-groups-task11.{md,json}`: R11, таблица против CB_COMP, попарно против `combo3`, MCS, G11, не-вред, справочные строки, «что не проверено».
7. **Роадмап/README/site-pages** — строка Task 11 в пункте #419, `docs/site-pages.json`, README; `make all`, `make docs-check`.
8. **PR** — `Closes #439`, протокол приёмки комментарием, Project V2 → In Review.
