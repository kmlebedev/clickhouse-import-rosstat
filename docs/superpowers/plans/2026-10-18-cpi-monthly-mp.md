# CPI Monthly MP (ML-трек v2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Построить месячный nowcast месячного ИПЦ РФ на 155 месяцах (2013-10…2026-08) с экспериментом «с МП vs без МП» на общих фолдах: 4 CatBoost-варианта (direct/bottom-up × с/без `cbr_survey`), бенчмарки B1–B3, DM-HLN, сплит статьи 2019-01…2024-07 и текущий nowcast.

**Architecture:** Новый Python-скрипт `scripts/cpi_monthly.py` (паттерн `cpi_nowcast.py`/`key_rate_model.py`) читает сырые таблицы ClickHouse через `ch_query` из `scripts/extrapolation_aisc.py`, строит единую выборку с as-of-правилами, гоняет walk-forward по общим фолдам, пишет локальные артефакты; в ClickHouse не пишет. Общие оценочные функции — `scripts/eval_utils.py`; v1 (`scripts/cpi_nowcast.py`) не меняется.

**Tech Stack:** Python 3.12, `catboost>=1.2`, `scikit-learn` (ElasticNetCV, NNLS), `pandas`, `statsmodels>=0.15` (HAC-OLS, DM/HLN), `scipy`, `numpy` — через `uv run --with`; запуск через `make`.

**Spec:** `docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md`

## Global Constraints

- Рабочая директория — worktree `.worktrees/key-rate-ml-v2`, ветка `feat/key-rate-ml-v2`; главный worktree и другие ветки не трогать.
- Модели субагентов: имплементеры — `deepseek/deepseek-flash`; task-reviewer/scoped re-review — `anthropic/haiku`; финальное ревью ветки — `deepseek/deepseek-flash`.
- Скрипт **не пишет в ClickHouse**; транспорт — только `ch_query` из `scripts/extrapolation_aisc.py`; секреты — только из env (Makefile); живые прогоны — только через `make cpi-monthly`.
- Тесты — stdlib `unittest`, офлайн, транспорт мокается; запуск: `uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_monthly_test.py -v`.
- Зависимости — только `uv run --with` (в репозитории нет pyproject/requirements).
- `make all` зелёный на каждом коммите; коммиты краткие, по-английски.
- Спека — связывающий документ: `SEED = 20261018`, признаки, окна и as-of фиксированы **до** результатов; тюнинга нет; гиперпараметры — дефолты CatBoost или внутри train.
- README/ARCHITECTURE синхронизируются скилом `sync-readme-architecture`; статус — в `docs/ROADMAP_KEY_RATE.md` тем же PR.

## Review Focus (failure modes, обязанные быть пойманы тестами)

1. **Утечка «будущего» через МП** — точка месяца `M` не участвует на cutoff 8-го `M+1`; точка `M−1` участвует; граница «конец месяца `P+1`» включительна.
2. **Разные фолды у наборов** — «с МП» и «без МП» используют один и тот же список строк; месяц без МП не остаётся только в наборе без МП.
3. **Единицы и выравнивание** — `y = percent − 100`; `y_lag1/lag2` именно `M−1/M−2`; `usd_mm` по средним за календарные месяцы; FAO: значение за `M−1` под датой 1-го числа месяца публикации.
4. **Квартальные МП** — последняя опубликованная точка ≤ cutoff, перенос вперёд; отсутствие точки не подменяется будущей.
5. **NNLS без утечки** — веса только по обучающему окну фолда, неотрицательны, нормированы к 1; вырожденность любой компоненты пропускает фолд.
6. **Вырожденные окна** — B2/B3/CatBoost пропускают фолд со счётчиком, прогон не падает.
7. **Детерминизм** — два прогона дают одинаковые прогнозы/JSON при `SEED`.
8. **Живой nowcast** — цель = месяц за последним опубликованным ИПЦ; МП-лаг отмечается в `note`; stale-JSON чистится.

---

### Task 1: Каркас скрипта — данные, as-of, выборка, признаки, фикстуры

**Files:**
- Create: `scripts/cpi_monthly.py`
- Create: `scripts/cpi_monthly_test.py`
- Create: `scripts/testdata/cpi_monthly/*.csv` (генерируются экспортом)
- Modify: `Makefile` (цель `cpi-monthly`)
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `ch_query` (`scripts/extrapolation_aisc.py`), `date`/`datetime`, `numpy`.
- Produces (для Tasks 2–4):
  - `SEED = 20261018`; `TARGET_SERIES: dict[str,str]`, `TARGET_ALL = "cpi_all"`, `BOTTOMUP_COMPONENTS = ("cpi_food","cpi_nonfood","cpi_services")`.
  - `MP_SERIES = {"price_expectations_sa","price_expectations_sa_chg","business_climate_sa","business_climate_sa_chg","capacity_utilization_sa","headcount_expectations_sa","credit_conditions"}` (имена признаков); исходные ряды — `MP_SOURCE_SERIES`.
  - `@dataclass(frozen=True) Row: month: date; y: dict[str,float]; features: dict[str,float|int]`.
  - `row_date(value) -> date`, `row_float(value) -> float`, `_unique_dates(points, series) -> list`.
  - `targets_from_rows(rows) -> dict[str,list[tuple[date,float]]]`, `survey_from_rows(rows) -> dict[str,list[tuple[date,float]]]`, `usd_from_rows`, `key_rate_from_rows`, `fao_from_rows`.
  - `load_targets()`, `load_survey()`, `load_usd()`, `load_key_rate()`, `load_fao()`.
  - `month_of(day) -> date`, `month_end(day) -> date`, `previous_month(day) -> date`, `cutoff_for(month) -> date` (8-е `M+1`).
  - `available_from_mes(period_end) -> date`, `available_from_usd(day) -> date`, `available_from_rate(day) -> date`, `available_from_fao(day) -> date`, `available_from_survey(day) -> date` (конец `P+1`).
  - `base_features(month, targets_index, usd, key_rate, fao, cutoff) -> tuple[Optional[dict[str,float|int]], str]`.
  - `mp_features(month, survey, cutoff) -> tuple[Optional[dict[str,float]], str]`.
  - `feature_names(with_mp: bool) -> list[str]` (порядок: базовые, затем МП, `month` последним).
  - `build_sample(targets, survey, usd, key_rate, fao) -> tuple[list[Row], list[tuple[date,str]]]` (строки общие для обоих наборов; второй элемент — пропуски с причиной).
  - `assert_months_purged(rows: list[Row]) -> None`.
  - CLI: `--export-fixtures DIR`, `--start N`, `--embargo-months K`.

- [ ] **Step 1: Makefile и .gitignore**

Добавить цель рядом с `cpi-nowcast`:

```make
# ML-трек v2: месячный nowcast ИПЦ с мониторингом предприятий ЦБ (с МП vs без МП).
# Читает ClickHouse, пишет локальные артефакты; в ClickHouse НЕ пишет.
# Фикстуры: make cpi-monthly ARGS="--export-fixtures scripts/testdata/cpi_monthly"
cpi-monthly:
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_monthly.py $(ARGS)
```

В `.gitignore` — точные имена `/cpi_monthly_report.md`, `/cpi_monthly_predictions.csv`, `/cpi_monthly_forecast.json` (рядом с именами v1).

- [ ] **Step 2: Тесты разбора и календаря (red)**

В `scripts/cpi_monthly_test.py` (заглушка `ch_query`, как в `cpi_nowcast_test.py`):
- `test_two_rows_per_date_is_error`, `test_percent_minus_100`, `test_month_helpers`;
- `test_cutoff_is_eighth_of_next_month` (в т.ч. декабрь → январь);
- `test_survey_available_from_end_of_next_month` — точка 2013-09-01 доступна с 2013-10-31; на 2013-10-31 включается, на 2013-10-30 нет;
- `test_survey_month_m_not_available_at_cutoff_m_plus_1` — точка `M` не проходит `available_from_survey ≤ cutoff_for(M)`.

- [ ] **Step 3: Реализация разбора/календаря, зелёные тесты**

SQL-константы с `FINAL` и `FORMAT JSON`: таргеты, `macro_series` (`source='cbr_survey' AND series IN (...)`), `cbr_currency_usd`, `cbr_key_rate`, `fao_food_price_Indices_monthly` (`name='Food Price Index'`). Разбор — чистые функции; `_unique_dates` отказывает на дубле даты. `available_from_survey(day) = month_end(month_of(day))` для точки `P` означает конец `P+1` (реализовать через `month_end(next_month)`).

- [ ] **Step 4: Тесты выборки и признаков (red)**

- `test_target_is_percent_minus_100` — на фикстуре один месяц вручную;
- `test_lags_are_m_minus_1_and_2` — значения из фикстуры;
- `test_usd_mm_uses_monthly_averages` — ручная формула на двух месяцах;
- `test_fao_mm_uses_publication_dates` — значение за `M−1` под датой 1-го числа публикации;
- `test_quarterly_series_forward_filled` — точка Q1 держится в Q2 до публикации Q2;
- `test_quarterly_series_not_taken_after_cutoff` — будущий квартал не участвует;
- `test_missing_survey_skips_month_for_both_sets` — месяц без МП отсутствует в `build_sample`, а не только в MP-наборе;
- `test_sample_covers_155_months` — на живом полном срезе (smoke в Step 6) и на синтетике 155 месяцев: `rows[0].month == 2013-10-01`, `rows[-1].month == 2026-08-01`, длина 155, пропусков 0;
- `test_feature_names_order` — базовый набор: `y_lag1, y_lag2, usd_mm, key_rate, fao_mm, month`; MP-набор добавляет 7 имён перед `month`.

- [ ] **Step 5: Реализация выборки/признаков, зелёные тесты**

`base_features`: `y_lag1`/`y_lag2` из индекса таргета (`month_end(M−1)`, `month_end(M−2)`), проверка `available_from_mes ≤ cutoff`; `usd_mm` — средние за `M−1` и `M−2` (обе месячные точки обязательны); `key_rate` — `asof`; `fao_mm` — последняя пара доступных точек; `month`. `mp_features`: уровни `M−1`, `_chg` `M−1 − M−2`, квартальные — последняя точка с `available_from_survey ≤ cutoff`, перенос вперёд; отсутствие любой обязательной точки — причина пропуска. `build_sample`: кандидаты — месяцы 2013-10…2026-08 (`max(starts)+1` и последний месяц с МП 2026-08); строки общие.

- [ ] **Step 6: Экспорт фикстур и живой smoke**

Реализовать `--export-fixtures DIR`: окна 2013-01…2014-12 (`ipc_mes_2013_2014.csv`, `survey_2013_2014.csv`, `usd_2013_2014.csv`, `key_rate_2013_2014.csv`, `fao_2013_2014.csv`) и свежее 2026-07…2026-09 (`*_recent.csv`); имена колонок — как у SELECT. Выполнить:

```bash
make cpi-monthly ARGS="--export-fixtures scripts/testdata/cpi_monthly"
```

Проверить размеры (десятки КБ) и глазами содержимое.

Run: тестовая команда (PASS, offline). Run: `make cpi-monthly` → сводка: 155 месяцев 2013-10…2026-08, пропусков 0, диапазоны рядов. Если пропуски есть — разобрать причину, не «подкручивать» правила.

- [ ] **Step 7: Commit**

```bash
git add Makefile .gitignore scripts/cpi_monthly.py scripts/cpi_monthly_test.py scripts/testdata/cpi_monthly/
git commit -m "cpi monthly: sample, as-of rules and mp features"
```

---

### Task 2: Walk-forward, бенчмарки B1–B3, метрики, скелет отчёта

**Files:**
- Modify: `scripts/cpi_monthly.py`, `scripts/cpi_monthly_test.py`

**Interfaces:**
- Consumes: `Row`, `SEED`, `eval_utils`.
- Produces (Tasks 3–4):
  - `@dataclass(frozen=True) Prediction: month: date; model: str; y_true: float; y_pred: float`.
  - `@dataclass(frozen=True) WindowRun: start: int; embargo: int; main: bool; folds: int; predictions: list[Prediction]; skipped: dict[str,int]; notes: dict[str,dict[str,int]]`.
  - `FitPredict = Callable[[list[Row], Row], float]`.
  - `window_specs(start=24, embargo=0) -> list[tuple[int,int]]` (24/0, 48/0, 24/1; дубликаты отбрасываются).
  - `walk_forward(rows, models, start, embargo, skipped=None, notes=None) -> list[Prediction]`.
  - `evaluate_windows(rows, models, specs=None) -> list[WindowRun]`.
  - `predict_last(train,row) -> float`; `fit_ar1(train,row) -> float`.
  - `fit_enet_base(train,row)`, `fit_enet_mp(train,row)` (общая `_fit_enet(train,row,with_mp)`), `MODEL_ENET_BASE = "b3_elastic_net"`, `MODEL_ENET_MP = "b3_elastic_net_mp"`.
  - `errors`, `rmse_pp`, `mae_pp` (тонкие обёртки `eval_utils`), `last_fold_months`.
  - `render_report(...) -> str` — скелет; `write_artifacts(...)` — CSV прогнозов.

- [ ] **Step 1: Тесты harness (red)**

Синтетика 60+ месяцев: `walk_forward` обучает только на прошлом (модель-заглушка пишет `len(train)`), первый фолд при `start=24` — 25-й месяц; `embargo=1` убирает месяц перед тестом; `predict_last == row.features["y_lag1"]`; `fit_ar1` на `y = c + φ y_{M−1}` восстанавливает φ (1e-6); `fit_enet_base` детерминирован; окно короче минимума → `DegenerateWindowError` → счётчик, прогон продолжается; RMSE/MAE на ручных числах.

- [ ] **Step 2: Реализация, зелёные тесты**

B1 — `y_lag1`; B2 — OLS HAC `maxlags=3`, 11 дамми месяца, минимум 14 строк; B3 — `ElasticNetCV(alphas=np.logspace(−4,1,20), l1_ratio=[0.1,0.5,0.9], cv=TimeSeriesSplit(3), random_state=SEED)` на стандартизованных признаках (импутация медианой train), минимум 12 строк; `ConvergenceWarning` считать в `fold_notes` (`non_converged`), не вырождением. `walk_forward` ловит только `DegenerateWindowError`. Отчёт-скелет: выборка/пропуски, «модель × окно» RMSE/MAE, «последние 12 фолдов», «что не проверено»; CSV `month, window, model, y_true, y_pred`.

- [ ] **Step 3: Живой прогон и Commit**

Run: тесты; `make cpi-monthly` → строки B1/B2/B3-base/B3-mp по окнам 24/48/embargo 1, счётчики пропусков. Числа — в отчёт задачи.

```bash
git add scripts/cpi_monthly.py scripts/cpi_monthly_test.py
git commit -m "cpi monthly: walk-forward harness and benchmarks"
```

---

### Task 3: CatBoost — direct, bottom-up, 4 варианта, NNLS, метаданные

**Files:**
- Modify: `scripts/cpi_monthly.py`, `scripts/cpi_monthly_test.py`

**Interfaces:**
- Consumes: harness Task 2.
- Produces (Task 4):
  - `catboost_matrix(rows, with_mp: bool) -> np.ndarray` (`dtype=object`, числовые float + int-код месяца).
  - `monotone_constraints(with_mp: bool) -> list[int]`; `catboost_params(with_mp)`, `catboost_model(with_mp)`, `catboost_metadata(with_mp)`.
  - `fit_catboost_direct(train,row,with_mp=False) -> float`; `fit_catboost_direct_mp(train,row)`; `fit_catboost_bottomup(train,row,with_mp=False)`; `fit_catboost_bottomup_mp(train,row)`.
  - `nnls_weights(train) -> tuple[float,float,float]`.
  - `MODEL_CATBOOST_DIRECT = "catboost_direct"`, `MODEL_CATBOOST_BOTTOMUP = "catboost_bottomup"`, `MODEL_CATBOOST_DIRECT_MP = "catboost_direct_mp"`, `MODEL_CATBOOST_BOTTOMUP_MP = "catboost_bottomup_mp"`.

- [ ] **Step 1: Тесты (red)**

- `test_catboost_matrix_order_and_types` — порядок `feature_names(with_mp)`, `month` целым, числовые float;
- `test_monotone_constraints_by_name` — `+1` у `usd_mm`, `price_expectations_sa`, `price_expectations_sa_chg`; у `month` 0; в обоих наборах;
- `test_nnls_weights` — на синтетике `all = 0.4·food + 0.35·nonfood + 0.25·services` восстанавливает веса (1e-6), неотрицательны, Σ ≈ 1;
- `test_nnls_weights_use_only_train` — мутация будущего месяца не меняет веса фолда;
- `test_bottomup_is_weighted_sum` — заглушечные компонентные прогнозы, ручная сверка `Σ w·ŷ`;
- `test_catboost_deterministic` — два прогона с `SEED` дают равные числа;
- `test_catboost_metadata` — `cat_features=['month']`, `allow_writing_files=False`, monotone по именам, дефолты.

- [ ] **Step 2: Реализация, зелёные тесты, живой прогон**

CatBoost: `loss_function='RMSE'`, `random_seed=SEED`, `verbose=False`, `cat_features=['month']`, `monotone_constraints`, `allow_writing_files=False`; остальные — дефолты. Bottom-up: три компоненты, NNLS по train-окну, нормировка; любая вырожденность → `DegenerateWindowError`. Метаданные — в JSON. Четыре функции-модели с `last_fit`/`last_weights` для диагностики.

Run: тесты; `make cpi-monthly` → 4 строки CatBoost по окнам, веса NNLS, DM-пары (пока без primary-таблицы). Числа — в отчёт задачи.

- [ ] **Step 3: Commit**

```bash
git add scripts/cpi_monthly.py scripts/cpi_monthly_test.py
git commit -m "cpi monthly: catboost direct, bottom-up and mp variants"
```

---

### Task 4: DM-HLN «с МП vs без МП», сплит статьи, «2022 исключён», live nowcast, артефакты

**Files:**
- Modify: `scripts/cpi_monthly.py`, `scripts/cpi_monthly_test.py`

**Interfaces:**
- Consumes: Tasks 2–3.
- Produces:
  - `@dataclass(frozen=True) PairTest: model_a: str; model_b: str; p_value: float; diff: float; note: str`.
  - `pairwise_tests(predictions, pairs) -> list[PairTest]`; `PRIMARY_PAIRS`, `SECONDARY_PAIRS`.
  - `fixed_split(rows, models, train_end=date(2018,12,1), test_end=date(2024,7,1)) -> list[Prediction]`.
  - `metrics_without_2022(predictions) -> tuple[float,float]`.
  - `@dataclass(frozen=True) Nowcast: forecast_date; target_month; data_asof; models: dict; benchmarks: dict; weights: tuple; sample_size: int; note: str`.
  - `latest_published_month(targets)`, `nowcast_target_month(targets, today)`, `current_nowcast(rows, targets, survey, usd, key_rate, fao, today) -> tuple[Optional[Nowcast], str]`.
  - `forecast_payload(nowcast) -> dict`, `nowcast_json(nowcast) -> str`, `write_nowcast_artifact(path, nowcast) -> bool`, `clear_stale_artifacts(out_dir) -> list`.
  - `render_report(...)` — полный отчёт.

- [ ] **Step 1: Тесты (red)**

- `test_dm_sign_and_pvalue` — на синтетике известный знак/диапазон p;
- `test_pairs_use_common_months` — при пропуске фолда одной модели пара строится только по общим месяцам;
- `test_primary_pairs_are_mp_vs_base` — три пары: direct, bottomup, enet;
- `test_article_split_boundaries` — train ≤ 2018-12, test 2019-01…2024-07, 67 месяцев;
- `test_no_shock_excludes_2022` — 2022-тестовые месяцы выпадают, остальные остаются;
- `test_live_target_is_month_after_last_published` — на фикстуре сентябрь опубликован → цель октябрь;
- `test_live_mp_lag_note` — если последний МП старше `M−1`, в `note` есть пометка;
- `test_forecast_json_deterministic` — два прогона — идентичный JSON;
- `test_stale_forecast_removed` — нет цели → старый JSON удалён, отчёт/CSV записаны;
- `test_no_predictions_clears_all` — пустая выборка → все три артефакта прошлого прогона удалены.

- [ ] **Step 2: Реализация, зелёные тесты**

DM-HLN через `eval_utils.diebold_mariano` (`h=1`), блочный бутстрап (`block=3`, `n=2000`, `seed=SEED`). Primary-пары — MP vs base для direct/bottom-up/B3; secondary — MP-модели против B1 и `b3_elastic_net_mp`, direct vs bottom-up. Сплит статьи — фиксированный train/test, без walk-forward. Строка «2022 исключён» — отдельная метрика. Live nowcast — цель по данным, признаки as-of run date, модели на полной выборке; JSON по спеке §5.1; stale-cleanup как в v1.

- [ ] **Step 3: Живой прогон и Commit**

Run: тесты; `make cpi-monthly` → полный отчёт: 155 месяцев, окна, DM primary/secondary, сплит статьи, «2022 исключён», веса, live nowcast; артефакты в корне worktree. Повтор — те же числа/JSON.

```bash
git add scripts/cpi_monthly.py scripts/cpi_monthly_test.py
git commit -m "cpi monthly: mp comparison, article split and live nowcast"
```

---

### Task 5: Документация и полная приёмка

**Files:**
- Modify: `README.md`, `ARCHITECTURE.md`, `docs/ROADMAP_KEY_RATE.md`

- [ ] **Step 1: Синк документации** (скил `sync-readme-architecture`): README — цель `cpi-monthly`, команда тестов, ограничения (as-of МП приближение, отсутствие PIT, Сбер не используется, v1 не меняется); ARCHITECTURE §2 — `scripts/cpi_monthly.py`; ROADMAP — подпункт `key-rate/phase-4-ml-v2` ✅ с числами приёмки, ссылками на спеку/план и issue #123.

- [ ] **Step 2: Полная приёмка (числа — в протокол PR)**

1. `make all`;
2. `uv run --with … python3 scripts/cpi_monthly_test.py -v` (offline, счёт тестов);
3. `uv run --with … python3 scripts/cpi_nowcast_test.py` (178 OK) и `key_rate_model_test.py` (169 OK);
4. `make cpi-monthly` — 155 месяцев, окна 24/48/embargo 1, RMSE/MAE всех моделей и бенчмарков, DM p-value primary/secondary, веса, сплит статьи, live nowcast;
5. повтор `make cpi-monthly` — детерминизм;
6. `git status` — артефакты не в индексе (gitignored); в ClickHouse ничего не записано (скрипт только читает).

- [ ] **Step 3: Commit**

```bash
git add README.md ARCHITECTURE.md docs/ROADMAP_KEY_RATE.md
git commit -m "docs: cpi monthly mp — readme, architecture, roadmap"
```

---

## Self-Review (выполнен автором плана)

- **Покрытие спеки:** §2.1–2.7 → Task 1; §3.4, §4.1–4.2 → Task 2; §3.1–3.3 → Task 3; §4.3–4.5, §5 → Task 4; §6 → Task 5. §7 — границы (в ROADMAP Task 5).
- **Согласованность имён:** `Row/Prediction/WindowRun/build_sample/feature_names/fit_*` едины во всех задачах; `SEED = 20261018`; имена моделей — `catboost_direct`, `catboost_bottomup`, `catboost_direct_mp`, `catboost_bottomup_mp`, `b3_elastic_net`, `b3_elastic_net_mp`.
- **Review Focus:** каждый пункт привязан к тесту задачи (Task 1 — as-of/границы/общие строки; Task 2 — purge/embargo/вырожденные окна; Task 3 — NNLS без утечки, monotone; Task 4 — DM/сплит/live nowcast/детерминизм).
- **Пропорция:** план не воспроизводит код; тела функций пишет имплементер по сигнатурам и спеке.
