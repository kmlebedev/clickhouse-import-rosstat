# Key Rate Model v0 (фундаментальный контур) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Первый срез контура прогноза ключевой ставки: выборка решений уровня заседания (2014+), обязательные бенчмарки, методы №2 (инерционное правило Тейлора) и №3 (упорядоченная логит MLE), walk-forward-бэктест с метриками §6.2 и тестами §6.3, отчёт и текущий прогноз на 23.10.2026 — на данных, уже собранных в ClickHouse.

**Architecture:** Python-скрипт `scripts/key_rate_model.py` (проектный паттерн `polyus_output_model.py`) читает сырые таблицы через `ch_query` из `scripts/extrapolation_aisc.py`, строит выборку заседаний с as-of-правилами публикации (v0-приближение), гоняет walk-forward по бенчмаркам и методам, пишет локальные артефакты; в ClickHouse не пишет. Предусловие — расширение Go-импортёра календаря `calendar/` на вкладки 2014–2019. Спека: [docs/superpowers/specs/2026-10-15-key-rate-model-v0-design.md](../specs/2026-10-15-key-rate-model-v0-design.md).

**Tech Stack:** Go (calendar), Python 3 + stdlib (транспорт/тесты), `statsmodels>=0.15` (OLS/HAC, OrderedModel, DM/Pesaran–Timmermann), `pandas`, `scikit-learn` (QWK/Brier/log loss), `scipy` (бутстрап/тесты); всё через `uv run --with`; запуск только через `make`.

**Spec:** docs/superpowers/specs/2026-10-15-key-rate-model-v0-design.md

## Global Constraints

- Рабочая директория всех задач — worktree `/Users/whitefox/GolandProjects/clickhouse-import-rosstat/.worktrees/key-rate-model-v0` (ветка `key-rate-model-v0`); главный worktree не трогать.
- Модели субагентов (переопределение владельца 2026-10-15): **все имплементеры — `deepseek/deepseek-flash`**; task-reviewer/scoped re-review — `anthropic/haiku`; финальное ревью ветки — `deepseek/deepseek-flash`.
- Python-скрипт **не пишет в ClickHouse**; транспорт — только `ch_query` из `scripts/extrapolation_aisc.py`; секреты — только из env (Makefile `-include ~/.config/rosstat/env` + `export`), в код и логи не попадают.
- Тесты Python — stdlib `unittest`, офлайн, транспорт подменяется `unittest.mock`; запуск: `uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy python3 scripts/key_rate_model_test.py`.
- Зависимости Python — только `uv run --with` (в репозитории нет pyproject/requirements).
- `make all` (gofmt, golangci-lint, vet, test -race, сборки) — зелёный на каждом коммите; коммиты краткие, по-английски.
- Живые прогоны и SQL к dev-ClickHouse — только через `make` (`make import`, `make ch-sql`, `make key-rate-model`); `make ch-up/down` не делать.
- Пороги, спецификации и порядок бенчмарков фиксированы спекой **до** первого прогона; тюнинга по тесту нет.
- README.md и ARCHITECTURE.md синхронизируются скилом `sync-readme-architecture`; статусы — в `docs/ROADMAP_KEY_RATE.md`.

## Review Focus (failure modes, которые обязаны быть пойманы тестами)

1. **Сдвоенные/внеочередные заседания** (2014-12-11 и 2014-12-15; окно 2022) — окно исхода не должно склеивать два решения: `rate_after` берётся на `min(d+7, next−1)`, `rate_before` — на `d−1` (Task 2).
2. **As-of утечка** — точка, доступная после даты заседания, не участвует; границы «ровно на дату» включительно/исключительно зафиксированы (Task 2).
3. **Календарь** — год вкладки не приписывается датам-не-заседаниям (доклады, пресс-конференции); год без панели/панель без вкладки пропускаются; пересечения эпох дедуплицируются; старый тест «нет дат раньше 2020» заменён осознанно (Task 1).
4. **Вырожденные окна ordered logit** — один класс в обучающем окне → метод пропускается со счётчиком, прогон не падает (Task 5).
5. **Метрики/тесты на малой выборке и нулевой дисперсии** — DM/бутстрап не дают NaN-падений; соглашение о неопределённом p зафиксировано (Task 3).

---

### Task 1: Календарь заседаний 2014–2019 (Go, `calendar/`)

**Files:**
- Modify: `calendar/events.go`
- Modify: `calendar/events_test.go`
- Modify: `docs/ROADMAP_KEY_RATE.md` (строка календаря в §4.1 — после живого прогона)

**Interfaces:**
- Produces: `parseCalMP(page []byte) ([]time.Time, error)` — контракт не меняется (отсортированные уникальные даты, 0 дат = ошибка); расширяется только множество дат (полные 118 по фикстуре).

- [ ] **Step 1: Обновить тест календаря (red)**

В `calendar/events_test.go`: пин 50 новых дат из спеки §2.3 (префикс каждой строкой), замена нижней границы «нет дат раньше 2020-01-01» на «нет дат раньше 2014-02-14», пин полного состава фикстуры (118 дат, фикстура заморожена), пин 2–3 дат докладов из старых таблиц как отсутствующих (точные даты взять из фикстуры — строки «Доклад о денежно-кредитной политике»), сохранить проверки внеочередных 2022–2023 и «незаседания 2026–2027».

- [ ] **Step 2: Убедиться, что тест падает**

Run: `cd /Users/whitefox/GolandProjects/clickhouse-import-rosstat/.worktrees/key-rate-model-v0 && go test ./calendar/ -run TestParseCalMP -v`
Expected: FAIL — в результате нет 2014–2019 дат.

- [ ] **Step 3: Реализовать разбор старой эры**

В `calendar/events.go` (аддитивно к текущему поведению, спека §2.2):
1. карта вкладок: `a.tab` с `href="#<key>"` и `data-text` вида `YYYY год` → key → year;
2. обход панелей `[data-tabs-content=<key>]`: год берётся из карты, отсутствует — панель пропускается;
3. в панели, помимо карточек `div.main-events_day` (дату без года `D месяца` дополнять годом панели), обход `table.data`: дата из первой ячейки строки (`D месяца`); если ячейка пустая — действует предыдущая дата (эффект `rowspan`); событие — из ячейки с текстом; предикат старой эры «заседание совета директоров» ∧ («по вопросам денежно-кредитной политики» ∨ «ключевой ставке»), регистронезависимо;
4. сбор в общий набор с дедупом по дате; fail-fast (0 дат) сохраняется.

- [ ] **Step 4: Тесты зелёные**

Run: `go test ./calendar/ -v && make all`
Expected: PASS, `make all` зелёный.

- [ ] **Step 5: Живой прогон и строка coverage**

Run: `make import STAT=events_calendar`, затем `make ch-sql FILE=sql/key_rate_coverage.sql`
Expected: импорт без ошибок (ожидаемо +50 дат); строка `events_calendar, cbr`: **120** строк, `min=2014-02-14`, `max=2027-12-17`. Числа — в отчёт задачи; обновить строку календаря в `docs/ROADMAP_KEY_RATE.md` §4.1 этими числами (без статуса пункта).

- [ ] **Step 6: Commit**

```bash
git add calendar/events.go calendar/events_test.go docs/ROADMAP_KEY_RATE.md
git commit -m "calendar: parse meeting tabs 2014-2019"
```

---

### Task 2: Каркас модели — цель `make key-rate-model`, выборка решений, as-of, признаки

**Files:**
- Create: `scripts/key_rate_model.py`
- Create: `scripts/key_rate_model_test.py`
- Create: `scripts/testdata/key_rate/*.csv` (фикстуры, экспорт шагом 1)
- Modify: `Makefile` (цель `key-rate-model` + `ARGS ?=`)
- Modify: `.gitignore` (артефакты §8 спеки)

**Interfaces:**
- Consumes: `ch_query` из `scripts/extrapolation_aisc.py` (сигнатура: `ch_query(sql: str) -> list[dict]`).
- Produces (точные имена — их используют Tasks 3–6):
  - `SAMPLE_TITLE = "СД ЦБ РФ: заседание по ключевой ставке"`
  - `load_meetings() -> list[date]`, `load_key_rate() -> list[tuple[date, float]]`, `load_cpd_core() -> list[tuple[date, float]]`, `load_ibk() -> list[tuple[date, float]]`, `load_mp_summary_deltas() -> list[tuple[date, int|None]]`
  - `available_from_cpd(period_end: date) -> date`, `available_from_survey(period_first: date) -> date` (обе — конец следующего месяца), `asof(points, available, asof_date) -> float|None`
  - `@dataclass Decision(meeting_date, rate_before, rate_after, step_bp, y3)`; `build_decisions(meetings, key_rate, mp_deltas) -> tuple[list[Decision], list[date], list[date]]` — второй список `excluded` («окно исхода ещё не наблюдаемо»: `min(d+7, next−1) > max(date)` ряда), третий `missing` («пропущено»: на `d−1` нет точки ряда); `run_summary` подаёт только прошедшие заседания, предстоящие печатает отдельной строкой
  - `@dataclass Row(decision, pi_saar, gap)`; `build_rows(decisions, cpd, ibk) -> tuple[list[Row], list[date]]`
  - `SEED = 20261015`; `main(argv) -> int` с `--export-fixtures DIR` и флагом `--rolling`

- [ ] **Step 1: Цель Makefile и экспорт фикстур**

В `Makefile` добавить `ARGS ?=` и цель:

```make
# Первый срез модели ключевой ставки (фундаментальный контур v0): выборка решений,
# бенчмарки, методы №2–3, walk-forward-бэктест. Читает ClickHouse, пишет локальные
# артефакты; в ClickHouse НЕ пишет. Фикстуры офлайн-тестов: make key-rate-model ARGS="--export-fixtures scripts/testdata/key_rate"
key-rate-model:
	uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_model.py $(ARGS)
```

(`--python 3.12` — требование `statsmodels>=0.15` (Python≥3.10); на системном `python3` 3.9.6 резолвер uv падает, и привязка к PATH сделала бы цель непереносимой.)

Реализовать `--export-fixtures DIR` (пишет CSV: `meetings.csv`, `key_rate_2013_2016.csv`, `cpd_core_2013_2016.csv`, `ibk_2013_2016.csv`, `mp_summary.csv`; имена = фактическое окно срезов 2013–2016, в `meetings.csv` экспорт добавляет ручную строку с уникальной датой), затем выполнить:
`make key-rate-model ARGS="--export-fixtures scripts/testdata/key_rate"` и проверить глазами состав CSV (даты, значения).

- [ ] **Step 2: Тесты выборки и as-of (red→green, TDD по каждому блоку)**

В `scripts/key_rate_model_test.py`:
1. `build_decisions` на фикстурах: состав 2014-02-14…2016-12-16; сдвоенная пара 2014-12-11 (+100 б.п.) / 2014-12-15 (+650 б.п.) — окно исхода не склеивает решения; `rate_before` берётся на `d−1`; фильтр `title` отсекает ручные строки-триггеры (в фикстуре `meetings.csv` они есть, у одной — уникальная дата 2016-12-20, чтобы эффект фильтра был наблюдаем); «пропущено» и «вне выборки» — разные списки;
2. сверка с `mp_summary.csv`: совпадение `step_bp`; подмена delta в фикстуре → функция поднимает ошибку;
3. `asof`: точка с доступностью позже даты заседания не выбирается; границы «ровно на дату» включительно; отсутствие ряда → дата в «пропущенных»;
4. `build_rows`: `pi_saar` = `((1+x/100)^12 − 1)×100` (значение проверяется руками на одной точке фикстуры `cpd_core`); `gap` — из `business_climate_sa`; формат `x = [rate_before, pi_saar, gap]`.

- [ ] **Step 3: Реализация**

`scripts/key_rate_model.py`: константы запросов (`SELECT … FINAL`), загрузчики, правила §4 спеки, сборка выборки и строк. CLI: по умолчанию — прогон бэктеста (на этом шаге печатает сводку выборки и «что» пропущено; остальные разделы добавляют Tasks 3–6).

- [ ] **Step 4: Тесты и живой smoke**

Run: `uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_model_test.py -v`
Expected: PASS, offline.
Run: `make key-rate-model`
Expected: сводка: 108 заседаний в выборке, 0 «вне выборки» (окно исхода ещё не наблюдаемо), 0 «пропущено» (нет ставки на `d−1` / нет признаков), диапазон 2014-02-14…2026-09-11; предстоящие заседания — отдельной строкой.

- [ ] **Step 5: Commit**

```bash
git add Makefile .gitignore scripts/key_rate_model.py scripts/key_rate_model_test.py scripts/testdata/key_rate/
git commit -m "key rate model: sample, as-of layer and features"
```

---

### Task 3: Ткань бэктеста — walk-forward, бенчмарки B1–B3, метрики, DM/MZ/PT, бутстрап, отчёт

**Files:**
- Modify: `scripts/key_rate_model.py`, `scripts/key_rate_model_test.py`

**Interfaces:**
- Consumes: `Row`, `Decision`, `SEED` (Task 2).
- Produces (используют Tasks 4–6):
  - `@dataclass Forecast(step_bp: float, probs: tuple[float,float,float]|None)` — `step_bp` в БАЗИСНЫХ пунктах, как и факт (`Decision.step_bp`): у бенчмарков и методов одна шкала;
  - `FitPredict = Callable[[list[Row], Row], Forecast]`; `predict_hold`, `predict_repeat`, `fit_ar1(train, row) -> Forecast` (шаг = `(pred_level − rate_before) × 100`, б.п.);
  - `DegenerateWindowError(ValueError)` — обучающее окно непригодно для оценки (мало точек, один класс у ordered-модели и т.п.); методы №2–3 поднимают его на вырожденном окне;
  - `@dataclass Prediction(meeting_date, actual_step_bp, actual_y3, method, window, step_bp, probs)`;
  - `walk_forward(rows, method, fit_predict, min_train=24, rolling=None) -> tuple[list[Prediction], int]` — второй элемент: число пропущенных фолдов (`DegenerateWindowError` пропускается со счётчиком; прочие исключения пробрасываются);
  - метрики: `rmse_bp`, `mae_bp`, `hit_rate`, `confusion3`, `qwk`, `brier_multi`, `log_loss_multi`, `reliability_bins(probs, y_true, n_bins=5)`, `diebold_mariano(err_a, err_b, h=1)`, `mincer_zarnowitz(actual, predicted)`, `pesaran_timmermann(pred_sign, actual_sign)`, `block_bootstrap_rmse_diff(err_a, err_b, block=4, n=2000, seed=SEED)`;
  - `render_report(sample, skipped, predictions, forecast, fold_skips=None) -> str` — `fold_skips` («(метод, окно)» → пропущено) печатается сводкой «фолдов/пропущено» в разделе walk-forward.

- [ ] **Step 1: Тесты метрик и walk-forward (red)**

Пины, проверяемые руками: RMSE/MAE на числах (например, ошибки {10, −10} → RMSE 10.0); hit rate и матрица 3×3 на фиксированном векторе; QWK на заранее посчитанном примере; Brier/log loss на известных вероятностях; `reliability_bins` — счётчики и доли; DM: одинаковые ошибки → статистика 0 и p ≥ 0.99 (соглашение), известная пара рядов против ручного расчёта (или сверка формы вывода); MZ на синтетике `actual = 2×predicted` → b ≈ 2 (HAC); PT: таблица попаданий по знаку; бутстрап: при `seed=SEED` результат повторяем (два прогона равны) и `err_a==err_b` → интервал включает 0; нулевая дисперсия не роняет.
Walk-forward: хронология возрастает; обучение — только прошлое (подсунуть фиктивную модель, которая считает `len(train)` — длины строго растут при `rolling=None` и равны окну при `rolling=40`); первый прогноз — 25-е заседание (min_train=24).

- [ ] **Step 2: Реализация + зелёные тесты**

`walk_forward` (expanding и rolling), бенчмарки B1–B3 (`fit_ar1` — OLS с HAC, `maxlags=4`), метрики §6.2, тесты §6.3 (DM — через statsmodels, если API отличается от ожидаемого — по формуле DM+HLN с названием пути в отчёте), бутстрап, `render_report` (разделы: выборка, пропуски, таблица «метод × окно» с RMSE/MAE/hit/QWK/Brier/log loss, DM против B1–B3 (p и знак), режимный блок 2024–2026, «что не проверено»). Артефакты: `key_rate_model_report.md`, `key_rate_model_predictions.csv` (в корне worktree).

Run: `python3 <uv-команда> scripts/key_rate_model_test.py -v`; затем `make key-rate-model`
Expected: PASS; отчёт содержит строки B1–B3 по двум окнам с числами; CSV построчный.

- [ ] **Step 3: Commit**

```bash
git add scripts/key_rate_model.py scripts/key_rate_model_test.py
git commit -m "key rate model: walk-forward harness, benchmarks and metrics"
```

---

### Task 4: Метод №2 — инерционное правило Тейлора

**Files:**
- Modify: `scripts/key_rate_model.py`, `scripts/key_rate_model_test.py`

**Interfaces:**
- Consumes: `FitPredict`, `Forecast`, `walk_forward` (Task 3).
- Produces: `fit_taylor(train, row) -> Forecast` (метод в отчёте — `"taylor"`).

- [ ] **Step 1: Тесты (red)**

Синтетические данные по формуле `i_t = c + ρ·i_{t−1} + a·π + b·gap` с известными коэффициентами → OLS на 40 точках восстанавливает их с точностью 1e-6 (свободных шумов нет); прогноз шага = `(pred_level − rate_before) × 100` (БАЗИСНЫЕ пункты, как у всех прогнозов и фактов harness'а); HAC-конфигурация пинована (`maxlags=4`, `use_correction=True`) — тест подменяет ковариацию и проверяет вызов; вырожденное окно (2 точки) → `DegenerateWindowError` с понятным текстом (walk-forward его пропускает со счётчиком).

- [ ] **Step 2: Реализация, зелёные тесты, прогон**

Добавить `fit_taylor` и строку `"taylor"` в отчёт (долгосрочные импликации: r*, α, β — отдельным блоком). Run: тесты; `make key-rate-model`.
Expected: в отчёте — строка `taylor` (оба окна) с метриками и DM; числа — в отчёт задачи.

- [ ] **Step 3: Commit**

```bash
git add scripts/key_rate_model.py scripts/key_rate_model_test.py
git commit -m "key rate model: inertial Taylor rule (method 2)"
```

---

### Task 5: Метод №3 — упорядоченная логит-модель решения

**Files:**
- Modify: `scripts/key_rate_model.py`, `scripts/key_rate_model_test.py`

**Interfaces:**
- Consumes: `FitPredict`, `Forecast`, метрики Task 3.
- Produces: `fit_ordered_logit(train, row) -> Forecast` (метод `"ordered_logit"`); вероятностные метрики также считаются для базлайна `"climatology"` (безусловные частоты классов на обучающем окне).

- [ ] **Step 1: Тесты (red)**

Синтетика с монотонной связью `gap` → класс (например, gap > порога ⇒ hike) → вероятности упорядочены по классам (P(hike) растёт по gap); вероятности суммируются к 1 (±1e-9); `E[step] = Σ P(c)·μ_c` сверяется руками; вырожденное окно (один класс) → `DegenerateWindowError`: walk-forward пропускает фолд со счётчиком, без падения; Brier/log loss на ручном примере.

- [ ] **Step 2: Реализация, зелёные тесты, прогон**

`fit_ordered_logit` (statsmodels `OrderedModel`, `distr='logit'`, регрессоры `[rate_before, pi_saar, gap]`), классы {−1, 0, +1}; в отчёт — вероятностные метрики для `ordered_logit` и `climatology`. Run: тесты; `make key-rate-model`.
Expected: строка `ordered_logit` с Brier/log loss/QWK и DM по точечному шагу; числа — в отчёт задачи.

- [ ] **Step 3: Commit**

```bash
git add scripts/key_rate_model.py scripts/key_rate_model_test.py
git commit -m "key rate model: ordered logit decision model (method 3)"
```

---

### Task 6: Текущий прогноз на ближайшее заседание + финальные артефакты

**Files:**
- Modify: `scripts/key_rate_model.py`, `scripts/key_rate_model_test.py`

**Interfaces:**
- Consumes: всё выше.
- Produces: `key_rate_model_forecast.json` (формат: `{"forecast_date", "meeting_date", "data_asof", "methods": {"taylor": {"step_bp"}, "ordered_logit": {"probs": {...}, "expected_step_bp"}}, "sample_size", "note"}`) + раздел «Текущий прогноз» в отчёте.

- [ ] **Step 1: Тесты (red)**

Ближайшее заседание = первая дата `> today` из `meetings` (на 2026-10-16 — 2026-10-23); признаки строятся as-of даты прогона; прогноз JSON детерминирован при фиксированных данных (два прогона — одинаковый JSON без полей времени); если будущих заседаний нет — внятная ошибка/пустой блок, не исключение.

- [ ] **Step 2: Реализация, зелёные тесты, прогон**

Run: тесты; `make key-rate-model`; вручную посмотреть отчёт и JSON.
Expected: `forecast.json` с вероятностями по трём классам и E[step] для 23.10.2026; числа — в отчёт задачи.

- [ ] **Step 3: Commit**

```bash
git add scripts/key_rate_model.py scripts/key_rate_model_test.py
git commit -m "key rate model: current-meeting forecast artifact"
```

---

### Task 7: Синхронизация документации и полная приёмка

**Files:**
- Modify: `README.md` (раздел скриптов: цель `key-rate-model`, команда тестов и что она требует)
- Modify: `ARCHITECTURE.md` (§2/§7 — скрипт и расписание/точки, если затрагиваются; §6 — таблица не меняется, схемы БД нет)
- Modify: `docs/ROADMAP_KEY_RATE.md` (фаза 4: подпункты — метод №1+ансамбль, байесовские priors, «inflated»-версия, ML-трек, point-in-time слой, запись в журнал + б.п./резолвер, витрина, консультант-бенчмарк №4; статус среза — ✅ после приёмки)
- Modify: `.superpowers/sdd/2026-10-15-key-rate-model-v0/progress.md` (итог)

- [ ] **Step 1: Синк документации**

Скил `sync-readme-architecture`: внести скрипт, цель и тесты в README/ARCHITECTURE; в ROADMAP_KEY_RATE — подпункты фазы 4 отдельными строками и статус этого среза (✅ с датой — по итогам Step 2).

- [ ] **Step 2: Полная приёмка (числа — в протокол PR)**

Run (в worktree, по порядку):
1. `make all` — зелёный;
2. `uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy python3 scripts/key_rate_model_test.py` — зелёный, offline;
3. `make import STAT=events_calendar` + `make ch-sql FILE=sql/key_rate_coverage.sql` — строка `events_calendar, cbr`: 120 / 2014-02-14 / 2027-12-17;
4. `make key-rate-model` — отчёт: 108 заседаний, метрики B1–B3/`taylor`/`ordered_logit` по двум окнам, DM p-value, прогноз на 23.10.2026.

- [ ] **Step 3: Commit**

```bash
git add README.md ARCHITECTURE.md docs/ROADMAP_KEY_RATE.md .superpowers/sdd/2026-10-15-key-rate-model-v0/progress.md
git commit -m "docs: key rate model v0 — readme, architecture, roadmap"
```

---

## Self-Review (выполнен автором плана)

- **Покрытие спеки:** §2→Task 1; §3–4→Task 2; §5, §7→Task 3; §6 (метод №2)→Task 4; §6 (метод №3)+вероятностные метрики→Task 5; §8→Tasks 3, 6; §9→Tasks 1, 2, 7; §10→Task 7. Границы §11 не реализуются — учтены в роадмапе (Task 7).
- **Согласованность имён:** `Decision/Row/Forecast/Prediction`, `available_from_cpd/available_from_survey`, `fit_taylor/fit_ordered_logit`, `SEED` — едины во всех задачах.
- **Review Focus:** каждый пункт привязан к тесту своей задачи (Task 1 — календарь; Task 2 — окна/утечки; Task 3 — метрики/нулевая дисперсия; Task 5 — вырожденные окна).
- **Пропорция:** план не воспроизводит код; тела функций пишет имплементер по сигнатурам и пинам.

---

## Протокол живой приёмки (2026-10-16, HEAD 8b15bdd)

Команды и результаты (повторные прогоны идемпотентны: те же числа при повторе):

1. `make all` — зелёный (gofmt, golangci-lint 0 issues, `go vet`, `go test -race`, обе сборки).
2. `uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_model_test.py` — `Ran 169 tests … OK` (offline, транспорт мокается).
3. `make import STAT=events_calendar` → `Imported 118 rows of events_calendar` (фолд-в-фолд тот же результат при повторе); `make ch-sql FILE=sql/key_rate_coverage.sql` → строка `events_calendar cbr 120 2014-02-14 2027-12-17` (118 канонических + 2 ручных строки-триггера).
4. `make key-rate-model` → выборка 108 решений (2014-02-14 … 2026-09-11); предстоящих 10; вне выборки 0; пропущено 0/0; сверка шагов с `cbr_mp_summary` 21/22; фолды 84 (расширяющееся) / 68 (скользящее 40), вырожденных 0. Expanding RMSE, б.п.: `ordered_logit` 145.06 (лучший), B1 148.35, `climatology` 149.49, `taylor` 151.70, B3 151.71, B2 171.39; DM против бенчмарков незначим (p ≈ 0.19–0.68). Прогноз на 2026-10-23: `taylor` −65.15 б.п.; `ordered_logit` P(−1)=81.65 %, P(0)=16.24 %, P(+1)=2.12 %, E[step] −63.36 б.п.
5. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА (витрины контура и Сбера видны агенту, сырые таблицы закрыты).

Протокол опубликован комментарием в PR ветки (стандарт §16.1 п. 8).
