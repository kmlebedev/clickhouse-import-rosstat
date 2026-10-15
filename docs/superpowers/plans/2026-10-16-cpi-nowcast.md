# CPI Nowcast (ML-трек) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Покомпонентный nowcast месячного ИПЦ РФ (CatBoost по дефолтам) с обязательными бенчмарками (last value / AR(1) / ElasticNetCV), честным walk-forward (purging/embargo, старт 24 основной и 48 робастность), сравнением direct vs bottom-up и текущим прогнозом месяца до публикации Росстата — на данных, уже собранных в ClickHouse.

**Architecture:** Python-скрипт `scripts/cpi_nowcast.py` (паттерн `key_rate_model.py`) читает сырые таблицы через `ch_query` из `scripts/extrapolation_aisc.py`, строит месячную выборку с as-of-правилами (недельные данные Росстата + курсы + ОФЗ + FAO + ставка), гоняет walk-forward по бенчмаркам и CatBoost-моделям, пишет локальные артефакты; в ClickHouse не пишет. Общие оценочные функции выносятся в `scripts/eval_utils.py` и переиспользуются `key_rate_model.py` (без дублирования).

**Tech Stack:** Python 3.12, `catboost>=1.2` (проверено 1.2.10 на arm64), `scikit-learn` (ElasticNetCV, NNLS), `pandas`, `statsmodels>=0.15` (HAC-OLS, DM/HLN), `scipy`, `numpy` — через `uv run --with`; запуск через `make`.

**Spec:** docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md

## Global Constraints

- Рабочая директория — worktree `/Users/whitefox/GolandProjects/clickhouse-import-rosstat/.worktrees/cpi-nowcast-v0` (ветка `cpi-nowcast-v0`); главный worktree и другие worktrees не трогать.
- Модели субагентов (переопределение владельца): все имплементеры — `deepseek/deepseek-flash`; task-reviewer/scoped re-review — `anthropic/haiku`; финальное ревью ветки — `deepseek/deepseek-flash`.
- Скрипт **не пишет в ClickHouse**; транспорт — только `ch_query` из `scripts/extrapolation_aisc.py`; секреты — только из env (Makefile); команды с ClickHouse — только через `make`.
- Тесты — stdlib `unittest`, офлайн, транспорт мокается; запуск: `uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/<файл>_test.py`.
- Зависимости — только `uv run --with` (в репозитории нет pyproject/requirements).
- `make all` зелёный на каждом коммите; коммиты краткие, по-английски.
- Спека — связывающий документ: признаки, правила as-of, бенчмарки и порядок метрик фиксированы **до** результатов; тюнинга по тесту нет; гиперпараметры — дефолты или внутри train.
- README/ARCHITECTURE синхронизируются скилом `sync-readme-architecture`; статус — в `docs/ROADMAP_KEY_RATE.md` тем же PR.

## Review Focus (failure modes, обязанные быть пойманы тестами)

1. **Утечка «будущего» через правила доступности** — точка, опубликованная позже cutoff, не участвует в признаках (мутант-тест на недельных/курсовых/FAO-точках; границы «ровно в день публикации»).
2. **purging:** интервал времени признаков тестового месяца не пересекается с временем меток обучения (assert-тест на реальных данных и на синтетике).
3. **NNLS-веса:** считаются только по обучающему окну фолда (тест: веса фолда не меняются при мутации будущих месяцев), неотрицательны, нормированы.
4. **Единицы и выравнивание:** `y = percent − 100` (п.п.), lag-1 берёт именно M−1, месяц для категориального признака — число 1–12; FAO-семантика (значение за M−1 хранится под датой 1-го числа месяца публикации).
5. **Вырожденные случаи:** окно обучения без нужных классов истории (ElasticNetCV на малом окне), пустые недельные окна, отсутствие FAО/USD-глубины → фолд/месяц пропускается со счётчиком, прогон не падает.

---

### Task 1: `scripts/eval_utils.py` — вынос общих оценочных функций

**Files:**
- Create: `scripts/eval_utils.py`
- Modify: `scripts/key_rate_model.py` (функции `rmse_bp`, `mae_bp`, `diebold_mariano`, `block_bootstrap_rmse_diff` — тонкие обёртки над общим модулем)
- Modify: `scripts/key_rate_model_test.py` (только если требуется: импорт не меняется — имена остаются)

**Interfaces:**
- Consumes: ничего из предыдущих задач ветки.
- Produces (использует Task 3): `eval_utils.rmse(actuals, preds) -> float`, `mae(actuals, preds) -> float`, `diebold_mariano(err_a, err_b) -> tuple[float, float]` (HLN через statsmodels; конвенция нулевой дисперсии как в `key_rate_model`: одинаковые/нулевая дисперсия → `(0.0, 1.0)` при нулевом среднем, `(±inf, 0.0)` иначе), `block_bootstrap_rmse_diff(err_a, err_b, block, n, seed) -> tuple[float, float, float]`.

- [ ] **Step 1: Перенести функции (RED→GREEN на существующих тестах)**

Перенести реализацию один-в-один из `key_rate_model.py` в `eval_utils.py` (сохранив docstring-конвенции); в `key_rate_model.py` оставить обёртки с прежними именами и параметрами по умолчанию (`rmse_bp(actuals, preds) -> eu.rmse(...)` и т.д.), чтобы 169 тестов не менялись.

- [ ] **Step 2: Тесты и совместимость**

Run: `uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/key_rate_model_test.py -v` → все 169 OK (без правок ожиданий).
Run: `make all` → зелёный.

- [ ] **Step 3: Commit**

```bash
git add scripts/eval_utils.py scripts/key_rate_model.py
git commit -m "refactor: extract shared evaluation utils"
```

---

### Task 2: Каркас `cpi_nowcast` — цель Makefile, выборка, as-of, признаки, фикстуры

**Files:**
- Create: `scripts/cpi_nowcast.py`, `scripts/cpi_nowcast_test.py`, `scripts/testdata/cpi_nowcast/*.csv`
- Modify: `Makefile` (цель `cpi-nowcast` + `ARGS ?=`), `.gitignore` (точные имена артефактов)

**Interfaces:**
- Consumes: `ch_query` (как Task 2 ветки v0).
- Produces (используют Tasks 3–6):
  - `SEED = 20261016`; словарь `TARGET_SERIES` (4 ряда, спека §2.1); `WEEKLY_GASOLINE = "Бензин автомобильный"`; `FAO_SERIES = "Food Price Index"`.
  - загрузчики `load_targets() -> dict[str, list[tuple[date, float]]]` (значения уже `percent − 100`), `load_weekly() -> list[tuple[str, date, float]]`, `load_usd() -> list[tuple[date, float]]`, `load_key_rate()`, `load_ofz_1y()`, `load_fao() -> list[tuple[date, float]]`.
  - `cutoff_for(month: date) -> date` (8-е число M+1); правила доступности `available_from_weekly/usd/rate/ofz/fao/mes(d) -> date` (спека §2.4);
  - `@dataclass Row: month: date (первое число), y: dict[str, float], features: dict[str, float | int]`; `build_sample(targets, weekly, usd, key_rate, ofz, fao) -> tuple[list[Row], list[date]]` (второй элемент — пропущенные месяцы);
  - `feature_names() -> list[str]` (порядок §2.3: share_rising_wk, median_wk, median_wk4, gasoline_4w, usd_20d, key_rate, ofz_1y, fao_mm, y_lag1, month);
  - CLI: `--export-fixtures DIR`, `--start N`, `--embargo-months K`.

- [ ] **Step 1: Цель Makefile и экспорт фикстур**

Добавить в `Makefile` (рядом с `key-rate-model`):

```make
# ML-трек контура ставки: nowcast месячного ИПЦ до публикации Росстата (CatBoost + бенчмарки).
# Читает ClickHouse, пишет локальные артефакты; в ClickHouse НЕ пишет.
# Фикстуры: make cpi-nowcast ARGS="--export-fixtures scripts/testdata/cpi_nowcast"
cpi-nowcast:
	uv run --with "catboost>=1.2" --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/cpi_nowcast.py $(ARGS)
```

Реализовать `--export-fixtures`: `weekly_2022.csv` (name,date,percent; окно 2021-12-01…2022-05-31), `weekly_recent.csv` (2026-09-14…2026-10-05), `ipc_mes_2021_2022.csv`, `usd_2021_2022.csv`, `ofz_2021_2022.csv`, `fao_2021_2022.csv`. Выполнить экспорт; проверить размеры (каждый файл — десятки КБ) и глазами содержимое.

- [ ] **Step 2: Тесты выборки/признаков (red→green)**

В `scripts/cpi_nowcast_test.py` (мок транспорта — как в `key_rate_model_test.py`):
1. `build_sample` на фикстурах: точный состав месяцев (2022-01…2022-03 и т.п.), `y = percent − 100` (ручная сверка одного месяца по фикстуре); отсутствие FAО/глубины USD → месяц в «пропущенных»;
2. правила доступности: weekly точка `date+2` — на границе включается ровно в день публикации; мутант-тест «точка позже cutoff не участвует» (подмена значения в срезе → признак не меняется; сдвиг точки в окно → меняется);
3. `cutoff_for` = 8-е число M+1; `y_lag1` берётся из M−1 (значение пинается по фикстуре);
4. признаки: `share_rising_wk`/`median_wk`/`median_wk4` вручную на маленьком синтетическом срезе недель (посчитать руками в тесте); `gasoline_4w` — сумма 4 недель; `usd_20d` — формула; `ofz_1y` — последняя точка; `fao_mm` — % м/м с учётом семантики даты (значение за M−1 под датой 1-го числа месяца публикации);
5. purge-assert: `assert_months_purged(rows)` — интервалы признаков тестового месяца не пересекаются с временем меток обучения (на фикстуре и на реальном полном срезе — живой smoke в Step 4).

- [ ] **Step 3: Реализация**

`scripts/cpi_nowcast.py`: константы, загрузчики (SELECT … FINAL), правила §2.4, `build_sample`, `feature_names`, CLI, export-fixtures. На этом шаге основной прогон печатает сводку выборки (месяцы, пропуски) — модели добавляют Tasks 3–4.

- [ ] **Step 4: Тесты и живой smoke**

Run: тестовая команда (PASS, offline). Run: `make cpi-nowcast` → сводка: выборка ~57 месяцев (2022-01…2026-09), пропусков 0 (перечислить, если есть), диапазон недельных данных.

- [ ] **Step 5: Commit**

```bash
git add Makefile .gitignore scripts/cpi_nowcast.py scripts/cpi_nowcast_test.py scripts/testdata/cpi_nowcast/
git commit -m "cpi nowcast: sample, as-of rules and weekly features"
```

---

### Task 3: Walk-forward + бенчмарки B1–B3 + метрики + скелет отчёта

**Files:**
- Modify: `scripts/cpi_nowcast.py`, `scripts/cpi_nowcast_test.py`

**Interfaces:**
- Consumes: `Row`, `SEED`, `eval_utils` (Task 1).
- Produces (Tasks 4–6): `@dataclass Prediction(month, model, y_true, y_pred)`; `fit_predict`-сигнатура `Callable[[list[Row], Row], float]`; бенчмарки `predict_last`, `fit_ar1`, `fit_enet`; `walk_forward(rows, models, start, embargo) -> list[Prediction]`; `render_report(sample, skipped, predictions, nowcast) -> str`; метрики-обёртки через `eval_utils`; таблица «модель × окно».

- [ ] **Step 1: Тесты harness/бенчмарков (red)**

Синтетика (60+ месяцев с известной формулой): `walk_forward` обучает только на прошлом (модель-заглушка с `len(train)`), первый фолд при `start=24` — 25-й месяц; embargo=1 убирает месяц перед тестом (проверка через счётчик обучающих строк); `predict_last` = `y_{M−1}`; `fit_ar1` на синтетике `y = c + φ y_{M−1}` восстанавливает φ (1e-6); `fit_enet` детерминирован (`random_state`), при `start`-окне меньше 12 месяцев — `DegenerateWindowError`-подобный пропуск со счётчиком; метрики через `eval_utils` (RMSE в п.п. на ручных числах).

- [ ] **Step 2: Реализация, зелёные тесты, живой прогон**

Бенчмарки (B2 — OLS HAC `maxlags=3` с 11 дамми месяца; B3 — `ElasticNetCV` со сплитом внутри train, стандартизация `StandardScaler` только по train), `walk_forward`, отчёт-скелет (таблицы: выборка/пропуски, «модель × окно» RMSE/MAE; блок «последние 12 фолдов»; раздел «что не проверено»). Артефакты `cpi_nowcast_report.md`, `cpi_nowcast_predictions.csv` (в корне worktree, gitignored).

Run: тесты; `make cpi-nowcast` → строки B1–B3 по стартам 24 и 48 (embargo 0), числа — в отчёт задачи.

- [ ] **Step 3: Commit**

```bash
git add scripts/cpi_nowcast.py scripts/cpi_nowcast_test.py
git commit -m "cpi nowcast: walk-forward harness and benchmarks"
```

---

### Task 4: CatBoost — direct, покомпонентный bottom-up, NNLS-веса

**Files:**
- Modify: `scripts/cpi_nowcast.py`, `scripts/cpi_nowcast_test.py`

**Interfaces:**
- Consumes: harness Task 3.
- Produces: `fit_catboost_direct(train, row) -> float`; `fit_catboost_bottomup(train, row) -> float`; `nnls_weights(train) -> tuple[float, float, float]`; модели докладываются как `catboost_direct`, `catboost_bottomup`; метаданные модели в JSON прогона (дефолты CatBoost, monotone-конфигурация).

- [ ] **Step 1: Тесты (red)**

1. `nnls_weights`: на синтетике `all = 0.4·food + 0.35·nonfood + 0.25·services` восстанавливает веса (1e-6), неотрицательны, сумма ≈ 1; тест «веса по обучающему окну»: мутация будущего месяца не меняет веса фолда;
2. monotone-конфигурация: `CatBoostRegressor` собирается с пинутыми `monotone_constraints` (по именам признаков; тест сверяет `get_params()`), `cat_features=['month']`;
3. прямой и bottom-up прогнозы детерминированы при фиксированном `SEED` (два прогона — равные числа);
4. bottom-up = `Σ w·ŷ_j` (ручная сверка на выступе: подсунуть заглушечные компонентные прогнозы).

- [ ] **Step 2: Реализация, зелёные тесты, живой прогон**

Run: тесты; `make cpi-nowcast` → строки `catboost_direct`/`catboost_bottomup` по стартам 24/48, веса bottom-up, DM против B1–B3 и bottomup vs direct (p-value и знак), бутстрап-интервалы; числа — в отчёт задачи.

- [ ] **Step 3: Commit**

```bash
git add scripts/cpi_nowcast.py scripts/cpi_nowcast_test.py
git commit -m "cpi nowcast: CatBoost direct and bottom-up models"
```

---

### Task 5: Текущий nowcast + финальные артефакты

**Files:**
- Modify: `scripts/cpi_nowcast.py`, `scripts/cpi_nowcast_test.py`

**Interfaces:**
- Produces: `cpi_nowcast_forecast.json` (`{"forecast_date", "target_month", "data_asof", "catboost_direct", "catboost_bottomup", "bottomup_weights", "benchmarks", "sample_size", "note"}`), секция «Текущий nowcast» в отчёте; поведение «нет неопубликованного месяца» (все закрыты) — внятное сообщение, JSON не пишется, старые артефакты подчищаются.

- [ ] **Step 1: Тесты (red)**

`target_month` = последний месяц, чей `rosstat_ipc_mes` не опубликован на дату прогона (на 2026-10-16 — 2026-10-01); признаки — as-of даты прогона; детерминизм (два прогона — идентичный JSON); stale-cleanup (старый JSON при отсутствии цели удаляется); бенчмарки B1–B3 в JSON присутствуют.

- [ ] **Step 2: Реализация, зелёные тесты, живой прогон**

Run: тесты; `make cpi-nowcast` → JSON на октябрь-2026 (direct, bottom-up, веса, бенчмарки), раздел отчёта; числа — в отчёт задачи.

- [ ] **Step 3: Commit**

```bash
git add scripts/cpi_nowcast.py scripts/cpi_nowcast_test.py
git commit -m "cpi nowcast: current-month forecast artifact"
```

---

### Task 6: Документация и полная приёмка

**Files:**
- Modify: `README.md`, `ARCHITECTURE.md`, `docs/ROADMAP_KEY_RATE.md`

- [ ] **Step 1: Синк документации** (скил `sync-readme-architecture`): README — цель `cpi-nowcast`, команда тестов, ограничения (as-of v0, Сбер не используется, ансамбль отдельным пунктом); ARCHITECTURE — скрипты; ROADMAP — подпункт ML-трека ✅ с числами приёмки (после Step 2).

- [ ] **Step 2: Полная приёмка (числа — в протокол PR)**

1. `make all`; 2. `uv run --with … python3 scripts/cpi_nowcast_test.py` (offline, счёт тестов); 3. `make cpi-nowcast` — фолды 24/48, RMSE/MAE всех моделей и бенчмарков, DM p-value, веса, текущий nowcast; 4. оба набора тестов (`key_rate_model_test.py` — 169 OK после выноса `eval_utils`).

- [ ] **Step 3: Commit**

```bash
git add README.md ARCHITECTURE.md docs/ROADMAP_KEY_RATE.md
git commit -m "docs: cpi nowcast — readme, architecture, roadmap"
```

---

## Self-Review (выполнен автором плана)

- **Покрытие спеки:** §2.1–2.4→Task 2; §3.1–3.2→Task 4; §3.3, §4→Task 3; §5→Tasks 2–5; §6→Task 6; §7 — границы (в роадмапе Task 6). `eval_utils` (§4 спеки) → Task 1.
- **Согласованность имён:** `Row/Prediction/feature_names/build_sample/walk_forward/fit_*` — едины во всех задачах; `SEED=20261016`.
- **Review Focus:** каждый пункт привязан к тесту задачи (Task 2 — утечки/границы/FAO-семантика; Task 3 — purge/embargo/детерминизм; Task 4 — NNLS без утечки, monotone-конфиг).
- **Пропорция:** план не воспроизводит код; тела функций пишет имплементер по сигнатурам и спекам.
