# План Task 4b (#446): `agg_ex` без ЖКУ и с сезонностью на h1

> Спека — [2026-10-31-cb-comp-h1-weekly-ex-utilities-design.md](../specs/2026-10-31-cb-comp-h1-weekly-ex-utilities-design.md). Ветка `exp/cb-comp-88-groups-task4b`, worktree `.worktrees/cb-comp-88-task4b`. Роль `ml`. База — `combo3`; G11 (критерий G10), primary 2023+ (45 месяцев).

## Шаги

1. **Спека, план, роадмап** — этот коммит, до кода и прогона. Роадмап: Task 4 — ✅ #444/#448; Task 4b — в работе, ветка/протокол.
2. **Тесты (TDD, офлайн)** — `scripts/models/cb_comp/cb_comp_h1_weekly_ex_test.py`:
   - маппинг ЖКУ: 6 рядов, коды 9457/9448/9458/9449/9459/9471–9474; 9416 не входит; покрытие 2024 0,8314;
   - `ЖКУ_wk` — ренормировка, `NaN` при отсутствии, as-of (`date+2`);
   - `agg_ex`: `w=0` → `agg`; `w>0` → масштаб `1/(1−w)`; `util_cov<0,5` → `NaN`;
   - OLS с месячными дамми восстанавливает коэффициенты на синтетике; без дамми — reference;
   - primary 2023+ = 45 месяцев; G11/Holm границы; R12/R13-константы.
3. **Код** — `cb_comp_h1_weekly_ex.py`: переиспользует `cb_comp_h1_weekly` (`prepare`, `weekly_inputs`, `attach_weekly`, `evaluate`, `mcs`, `holm`), `cb_comp_weekly_groups` (`weekly_group_features`), `cpi_weekly_aggregate` (`wkagg_features`); добавляет `load_base_predictions` (Task 4 `predictions.csv`, sha256/R12), `utilities_map`, `zhku_features`, `agg_ex_features`, `ols_ex_predict`, `g11_check`, R12/R13, артефакты.
4. **Make** — `cb-comp-h1-weekly-ex` / `-test` (те же `--with`, что `cb-comp-h1-weekly`); `.PHONY`; коммит кода **до** прогона.
5. **Прогон** — `make cb-comp-h1-weekly-ex ARGS="--archive …"`; повтор в `build/cb-comp-h1-weekly-ex-run2`; `cmp`/sha256. Task 4b обучает только OLS (`wk_agg_ex`), `combo3`/`combo3_wk` — из Task 4 `predictions.csv`: R12 — sha256 `0a67483f…`, RMSE 0,9698445785532128 / 0,9630314456250352 на 57 месяцах (1e-6). R13: 0,04942/0,05944/0,8314.
6. **Отчёт** — `docs/research/2026-10-31-cb-comp-88-groups-task4b.{md,json}`: маппинг ЖКУ и покрытие, `agg_ex`, primary 2023+ (45), reference 2022+ и без 2022-03, G11, подпериоды, MCS/MDE, «что не проверено».
7. **Документы** — README, `docs/site-pages.json`, роадмап; `make all`, `make docs-check`.
8. **PR** — `Closes #446`, протокол приёмки комментарием, Project V2 → `In Review`.

## Make-цели

- `make cb-comp-h1-weekly-ex` — прогон (ClickHouse env, архив CB_COMP, `panel.csv` C0, `build/cb-comp-parity`); `ARGS="--archive PATH --jobs 8"`.
- `make cb-comp-h1-weekly-ex-test` — офлайн-тесты без ClickHouse, сети и архива.

## Риски

- Нет артефакта Task 4 (`predictions.csv`) — прогон стоп; регенерация — `make cb-comp-h1-weekly` Task 4. R12/R13 не совпали — стоп и разбор, а не подмена.
- Покрытие ЖКУ 83,1 % (нет сетевого газа и ТКО) — прокси; `9416` не входит в 9470, исключён.
- Primary 45 месяцев: MDE выше, чем в Task 4; «незначимо» ≠ «эффекта нет».
- `agg_ex` — наша реконструкция; сравнение с официальным агрегатом только через `agg`.
- R12 может не совпасть, если Task 4-код изменился после merge; тогда стоп и разбор.
- Конфликт с другими PR в роадмапе — править только свои строки.

## Пост-ревью дополнение (#451)

9. **Контроли после ревью** — добавить `wk_agg_d`/`wk_agg_d_nod`/`wk_mix_d`/`seasonal_dummies_only` и `live_cb_wk`; DM-пары `agg_ex` vs `agg_d`, `mix_ex` vs `mix_d`, тарифный срез (7/12 + 2026-01), смещение по годам, тест train<M; G11 остаётся на 6 предзарегистрированных моделях.
10. **Переписать выводы** — основной вклад дамми, ЖКУ вторичен; корректный бенчмарк `live_cb_wk` 0,1864; оговорки о смене арены и правке протокола после кода.
11. **Перепрогон ×2, `make all`, `docs-check`, PR/протокол** — обновить hashes и ответ на ревью.
