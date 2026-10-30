# Task 9 (#436): h1 — комбинация `live_cb` с BS_ECON16/BS_ROZN16 — протокол

> **Пункт:** `key-rate/phase-4-cb-comp-88-groups-live`, зонтик [#419](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/419), Task 9 [#436](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/436), `role:ml`. Базовая спека пункта — [2026-10-30-cb-comp-88-groups-live-design.md](2026-10-30-cb-comp-88-groups-live-design.md) (§12 цель `T_LIVE`, §15 протокол Task 3). Основание — [разбор 2026-10-31](../../research/2026-10-31-per-code-mp-reconstruction-review.md) §6–§7.
> **Статус:** протокол записан и закоммичен **до прогона** кандидатов этого Task. Отдельная спека, а не §17 базовой, — чтобы не конфликтовать с открытым PR #435 (§16 базовой спеки).

## 1. Зачем

`live_cb` (Task 3, выбран владельцем для Task 4/5) отстаёт от CB_COMP на h1: 1,193 на `TARGET_FULL`, normal-54. Разбор 2026-10-31 показал, что публичными данными per-code МП не восстанавливается (§2–§5), гармонизация SA не помогает (§6), а в самой статье Кошкина–Крылова (2025) есть две простые модели, которые на h1 почти догоняют CB_COMP: **BS_ECON16** и **BS_ROZN16** (табл. 2: h1 0,17; на h6 BS_ROZN16 — лучшая модель статьи). Их регрессоры — публичные SA ценовые ожидания ЦБ.

Гипотеза: равновзвешенная комбинация `live_cb` с BS_ECON16 и BS_ROZN16 закрывает основную часть разрыва на h1 и не портит h3/h6.

## 2. Что известно до протокола (и поэтому не доказывается этим Task заново)

- Проба на `per_sheet` #252 (а не `live_cb`), BS обучены на `TARGET_FULL`, правило `t ≤ M − h`: h1 0,1638 (1,031 к CB_COMP), DM p = 0,026 против per-sheet; h2/h3/h6 по точке 0,95 / 0,97 / 0,87 к CB_COMP. Комбинация и окно 2016 выбраны **после** просмотра таблицы авторов.
- Регрессоры (проба до протокола, без моделей): `macro_series` `cbr_survey/price_expectations_sa` против X1 авторов — корреляция 0,9959, max |Δ| 1,66 п. (n = 187); `v_cbr_mp_public_panel` `trade_retail/price_exp` против X2 — 0,9992, 1,28 п.
- `live_cb` с базой этого Task (C0-панель, sha256 `aec91f99…`) уже прогнан в Task 3: 1,193 / 1,013 / 1,076 / 1,062.

Новое в этом Task: база — `live_cb`, обучение BS — на нашей цели `T_LIVE` (§12 базовой спеки), а не на `TARGET_FULL`. Комбинацию с `live_cb` и BS на `T_LIVE` до протокола никто не считал.

## 3. Данные и окна

- **`live_cb`** — без изменений относительно Task 3 (§15 базовой спеки, поправка §15.1): тот же код (`cb_comp_live_bottomup.fit_predict`, `with_mp=True`), входы `build/cb-comp-groups/panel.csv` (sha256 `aec91f99…`), `build/cb-comp-parity/public_panel.csv` (`47176baa…`), `sheet_map.csv` (`0d8a6ab5…`); train 2010-03…2018-12 без выбросов статьи, test 2019-01…2024-07, origin `M−(h−1)`.
- **Цель BS** — `T_LIVE[h]` (накопленный SA ИПЦ без КУ из 88 групп, `cb_comp_live_bottomup.t_live`), та же, на которой учится `live_cb`.
- **Регрессоры BS** (live-источники, ClickHouse):
  - `X_econ` — `macro_series` `source='cbr_survey'`, `series='price_expectations_sa'` (`cpi_monthly.load_survey`);
  - `X_rozn` — `v_cbr_mp_public_panel`, лист `trade_retail`, индикатор `price_exp` (тот же `public_panel.csv`, что у `live_cb`).
- **Истина оценки** — `TARGET_FULL`, normal-54 (как §15).

## 4. Модели

Для каждого h ∈ {1, 2, 3, 6} и тестового месяца M (origin `M−(h−1)`):

**BS_ECON16 / BS_ROZN16** — OLS (`sklearn.linear_model.LinearRegression`, с константой), как в ноутбуке авторов `Models/BS LM_2016/tables_code.ipynb` (`data_lag_long`, regime `lag`):

`T_LIVE[h](t) = a + b · X(t − h) + c · T_LIVE[h](t − h)`

- строки обучения t: `2016-01 + h ≤ t ≤ M − h` (таргет строки известен к origin: последний месяц накопления `t ≤ M − h`, ИПЦ за `M − h` публикуется в месяце origin); выбросы статьи исключаются по дате t (2014-12…2015-03, 2020-04, 2022-01…2022-12);
- X(t − h) — точка за месяц `t − h` (публикуется в месяце `t − h + 1` — правило as-of `date + 1` репозитория, тот же месяц, что origin для t = M);
- модель переобучается на каждом M (расширяющееся окно); строка M в обучение не входит.

**Отличие от авторов (сознательное).** В ноутбуке обучение на шаге i — `train + test.iloc[:i]`: для h ≥ 2 туда попадают строки, чей накопленный таргет заканчивается после origin (утечка). Здесь — правило `t ≤ M − h`. Для h1 оба правила совпадают.

**Кандидаты:**

| id | прогноз |
|---|---|
| `live_cb` | Task 3, без изменений (бенчмарк) |
| `bs_econ16` | BS_ECON16 на `T_LIVE` |
| `bs_rozn16` | BS_ROZN16 на `T_LIVE` |
| `combo3` | `(live_cb + bs_econ16 + bs_rozn16) / 3`, веса равные, не подбираются |

Справочно (в решении не участвуют): `combo3_tf` — то же с BS, обученными на `TARGET_FULL` (как проба §2); `bs_mean` — `(bs_econ16 + bs_rozn16) / 2`.

## 5. Проверки до гейта (R9)

- **R9a — воспроизведение статьи.** BS_ECON16 / BS_ROZN16 на рядах авторов (`series2016.xlsx`) их процедурой (`train + test.iloc[:i]`, regime `lag`) дают h1 RMSE **0,167225 / 0,169438** на normal-54 (`model_errors_and_dm_tests.xlsx`), допуск 1e-4.
- **R9b — регрессоры.** Корреляция `X_econ`/`X_rozn` с X1/X2 авторов (`series2009.xlsx`, 2009-01…2024-07) ≥ 0,99.
- **R9c — `live_cb` = Task 3.** Отношения к CB_COMP 1,193 / 1,013 / 1,076 / 1,062 (± 0,001).

Любая R9 не пройдена — прогон останавливается, гейт не считается.

## 6. Оценка

Как §15 базовой спеки, истина `TARGET_FULL`, normal-54:

1. **Против CB_COMP** — `cpi_two_stage_cb_comp.compare_to_cb` (класс W5b, отношение, 90 % CI, DM-HLN) для всех кандидатов и h ∈ {1, 2, 3, 6}.
2. **Попарно против `live_cb`** — `cpi_v4_leaderboard.compare_pair` (DM-HLN с поправкой на h, 95 % блочный бутстрап-CI ΔRMSE). «Лучше `live_cb`» — DM p < 0,05 и верхняя граница CI ΔRMSE < 0 (`cb_comp_live_en.better_than_cb`).
3. **MCS 90 %** по `live_cb`, `bs_econ16`, `bs_rozn16`, `combo3` и CB_COMP для h1/h3/h6 (`cpi_v4_leaderboard.mcs_included`).
4. Справочно — RMSE против `T_LIVE`, на всех 67 месяцах и по подпериодам 2019–2021 / 2023–2024.

## 7. Гейт G9 (предрегистрирован)

**G9 пройден**, если у `combo3` одновременно:

1. отношение RMSE к CB_COMP на h1 ≤ **1,10**;
2. `combo3` «лучше `live_cb`» на h1 (п. 6.2: DM p < 0,05 и 95 % CI ΔRMSE целиком ниже 0).

**Не-вред (условие принятия в Task 4/5, не часть G9):** отношение `combo3` к CB_COMP на h3 и h6 не выше, чем у `live_cb`, более чем на 0,02 (1,076 + 0,02 и 1,062 + 0,02). Если G9 пройден, а не-вред нет — `combo3` рекомендуется только для h1, решение за владельцем.

**Справочно:** пороги G3 (1,264 / 1,098 / 1,050) для `combo3` — проходит ли он гейт, который `live_cb` не прошёл на h6. Это не меняет решение Task 3 без владельца.

G9 не пройден — отрицательный результат публикуется, `live_cb` остаётся базой.

## 8. Детерминизм и паспорт

OLS детерминирован; `live_cb` — CatBoost с seed (как Task 3). Повтор даёт побайтно те же `predictions.csv` и `result.json`. В JSON — sha256 входов, git SHA, значения R9. `ML_LOG`/MLflow не используются (как §13/§15).

## 9. Выходы

- `scripts/models/cb_comp/cb_comp_h1_bs_combo.py` (+ `_test.py`, офлайн), `make cb-comp-h1-bs-combo` / `-test`;
- `build/cb-comp-h1-bs-combo/{predictions.csv,result.json}`; копия JSON и отчёт — `docs/research/2026-10-31-cb-comp-88-groups-task9.{md,json}`;
- разбор-основание `docs/research/2026-10-31-per-code-mp-reconstruction-review.md` — тем же PR;
- роадмап, `docs/site-pages.json`, README — тем же PR.

## 10. Что не доказывается

- **Не паритет дизайна.** BS переобучаются помесячно, `live_cb` и CB_COMP обучены один раз. Для live это честно, для #190 — нет.
- **Не PIT.** SA ценовых ожиданий — текущий винтаж ЦБ; винтажей нет. Проба на рядах авторов (винтаж 2024) дала тот же результат, но это не PIT.
- **Не walk-forward 2022+.** Критерий #419 «h1 на walk-forward 2022+» — вне этого Task.
- **Выбор после просмотра.** Модели и окно взяты из статьи после чтения её таблицы; веса равные и не подбираются, но независимым подтверждением будет только live-период после 2024-07.
