# План Task 4 (#424): недельный ИПЦ на h1 поверх `combo3`

> Спека — [2026-10-31-cb-comp-h1-weekly-design.md](../specs/2026-10-31-cb-comp-h1-weekly-design.md). Ветка `exp/cb-comp-88-groups-task4`, worktree `.worktrees/cb-comp-88-task4`. Роль `ml`. База — `combo3` из #437.

## Шаги

1. **Спека, план, роадмап** — этот коммит, до кода и прогона.
2. **Тесты маппинга/признаков (TDD, офлайн)** — `scripts/models/cb_comp/cb_comp_weekly_groups_test.py`:
   - глубочайший предок: группы уровня 3/4/5/7, лист принадлежит ровно одной группе, вложенных групп нет;
   - фикстура маппинга: 118 сопоставленных, 112 в группах, 52 группы, 36 без данных, 6 вне групп;
   - агрегация: ренормировка при пропуске позиции, вес года, `NaN` при отсутствии покрытия, `wk_g_cov` ∈ [0, 1];
   - as-of: точка после `cutoff − 2` не участвует; подмена будущих точек не меняет значение; месячная граница (последняя точка 31-го числа).
3. **Код маппинга/признаков** — `scripts/models/cb_comp/cb_comp_weekly_groups.py`:
   - `group_leaf_map`, `map_weekly_series` (переиспользует `cpi_public_panel_audit.match_mode`);
   - `weekly_group_features` (окно месяца, `date + 2 ≤ cutoff`, взвешенное геометрическое, покрытие);
   - `aggregate_features` (обёртка `cpi_weekly_aggregate.wkagg_features`), `availability_check`, `mapping_summary`, `coverage_by_year`.
4. **Тесты runner (TDD, офлайн)** — `scripts/models/cb_comp/cb_comp_h1_weekly_test.py`:
   - расписание walk-forward: train `t ≤ M−1`, шоки исключены, M не в train, минимум строк;
   - `attach_weekly` добавляет ровно `wk_g_mtd`/`wk_g_cov` и не трогает базовые;
   - OLS `wk_agg` на синтетике восстанавливает коэффициенты; NaN — медиана train;
   - `combo3`/`combo3_wk`/`wk_mix` — равные веса на общих месяцах;
   - G10/Holm: границы `p = 0,025`, CI с нулём, победа второго кандидата;
   - R10-константы и подпериоды.
5. **Код runner** — `scripts/models/cb_comp/cb_comp_h1_weekly.py`: R10/R11/R12, frozen-строки, арена 2022+ (ежемесячный expanding, `ThreadPoolExecutor` по группам, `thread_count=1`), `wk_agg`, G10, артефакты.
6. **Make** — `cb-comp-h1-weekly` / `-test` (те же `--with`, что `cb-comp-live-bottomup`), `.PHONY`; коммит кода **до** прогона.
7. **Прогон** — `make cb-comp-h1-weekly ARGS="--archive <path> --jobs 8"`; повтор в `build/cb-comp-h1-weekly-run2`; `cmp`/sha256 предсказаний и JSON. R10/R11/R12 обязаны быть зелёными.
8. **Отчёт** — `docs/research/2026-10-31-cb-comp-88-groups-task4.{md,json}` + `…-mapping.csv`: R10/R11/R12, маппинг и покрытие, окно CB_COMP (frozen), арена 2022+ (57 месяцев), DM/CI/MDE/MCS, подпериоды, G10, frozen-строки, «что не проверено».
9. **Роадмап/README/site-pages** — результат Task 4 в пункте #419, строка make-цели, страница отчёта; `make all`, `make docs-check`.
10. **PR** — `Closes #424`, протокол приёмки комментарием, Project V2 → `In Review`.

## Make-цели

- `make cb-comp-h1-weekly` — прогон (нужны ClickHouse env, локальный архив CB_COMP, `panel.csv` C0 и `build/cb-comp-parity`); `ARGS="--archive PATH --jobs 8"`.
- `make cb-comp-h1-weekly-test` — оба офлайн-теста без ClickHouse, сети и архива.

## Риски

- ~10 тыс. CatBoost-фитов; `--jobs 8` и `thread_count=1`; при >90 мин — вопрос владельцу, а не смена режима.
- Детерминизм при потоках: повтор; при расхождении — serial-rerun с записью в отчёт.
- 52/88 групп и разреженные 2019–2020: на окне CB_COMP выигрыша может не быть — это результат.
- Маппинг по словарю 2024; 6 листьев вне групп, 36 групп NaN — в отчёт.
- `late` vs `publication`: основной `late`, R12 — диагностика.
- PR #435 открыт: править только свою строку роадмапа, при конфликте — rebase.
