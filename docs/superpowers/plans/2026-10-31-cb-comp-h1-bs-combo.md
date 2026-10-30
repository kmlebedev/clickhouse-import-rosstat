# План Task 9 (#436): h1 — комбинация `live_cb` + BS_ECON16/BS_ROZN16

> Спека — [2026-10-31-cb-comp-h1-bs-combo-design.md](../specs/2026-10-31-cb-comp-h1-bs-combo-design.md). Ветка `exp/cb-comp-88-groups-task9`, worktree `.worktrees/cb-comp-88-task9`. Роль `ml`.

## Шаги

1. **Спека, план, разбор-основание** — первым коммитом, до кода и прогона.
2. **Тесты (TDD, офлайн)** — `scripts/models/cb_comp/cb_comp_h1_bs_combo_test.py`:
   - строки обучения BS: границы `2016-01 + h ≤ t ≤ M − h`, исключение выбросов, отсутствие строки M;
   - лаги: X(t − h) и T(t − h) берутся ровно за `t − h`;
   - OLS на синтетике восстанавливает коэффициенты;
   - процедура авторов (R9a) на синтетике совпадает с прямой реализацией ноутбука;
   - `combo3` — среднее трёх, месяц без одного прогноза в комбинацию не входит;
   - G9: граничные случаи (1,10 ровно; p = 0,05; CI с нулём).
3. **Код** — `cb_comp_h1_bs_combo.py`: R9a/R9b/R9c, BS на `T_LIVE` и `TARGET_FULL`, комбинации, оценка (§6), G9, артефакты. Переиспользовать `cb_comp_live_en.prepare`, `cb_comp_live_bottomup.fit_predict/aggregate_predictions/errors_on`, `compare_to_cb`, `compare_pair`, `mcs_included`, `better_than_cb`.
4. **Make** — `cb-comp-h1-bs-combo` / `-test`; коммит кода **до** прогона.
5. **Прогон** — `make cb-comp-h1-bs-combo`; повтор для детерминизма (`cmp` предсказаний и JSON).
6. **Отчёт** — `docs/research/2026-10-31-cb-comp-88-groups-task9.{md,json}`: R9, таблица против CB_COMP, попарно против `live_cb`, MCS, G9, не-вред, справочные строки, «что не проверено».
7. **Роадмап/README/site-pages** — строка Task 9 в пункте #419, `docs/site-pages.json`, README; `make all`, `make docs-check`.
8. **PR** — `Closes #436`, протокол приёмки комментарием, Project V2 → In Review.
