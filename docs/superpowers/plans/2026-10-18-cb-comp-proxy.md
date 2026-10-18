# CB_COMP proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Собрать из публичных рядов приближение таргета CB_COMP (SA ИПЦ без КУ, накопленный прирост h = 1/2/3/6) и посчитать RMSE/MAE нашего месячного nowcast на окне статьи 2019-01…2024-07; сравнить агрегатные числа с опубликованными CB_COMP и честно назвать расхождение.

**Architecture:** Новый скрипт `scripts/cb_comp_proxy.py` читает `cbr_indicators_cpd`, `cbr_dip_bulletin`, `rosstat_ipc_mes` через `ch_query` из `scripts/extrapolation_aisc.py`, строит прокси-таргет, калибрует прогнозы `cpi_monthly_predictions.csv` (long-sample, PR #177) и считает метрики; в ClickHouse не пишет. Офлайн-тесты — `scripts/cb_comp_proxy_test.py` (stdlib `unittest`, транспорт мокается). Общие метрики — `scripts/eval_utils.py`, где возможно; блочный бутстрап RMSE — локальный (нужен CI одной серии).

**Tech Stack:** Python 3.12, `pandas`, `numpy`, `scipy` (NNLS), stdlib `unittest` — через `uv run --with`, запуск через `make`.

**Spec:** `docs/superpowers/specs/2026-10-18-cb-comp-proxy-design.md`

## Global Constraints

- Рабочая директория — worktree `.worktrees/cb-comp-proxy`, ветка `feat/cb-comp-proxy`; другие ветки/worktrees не трогать.
- Скрипт **не пишет в ClickHouse**; транспорт — только `ch_query`; секреты — только из env (Makefile).
- Тесты — офлайн, без сети; `ch_query` подменяется.
- Зависимости — только `uv run --with`; `make all` зелёный.
- Числа статьи — из `docs/research/2026-10-16-cb-comp-vs-cpi-nowcast.md`; margin δ = 10 % фиксирован до прогона.
- Прокси не выдаётся за строгий таргет; DM/TOST против CB_COMP не заявляются без помесячных ошибок авторов.

## Review Focus (failure modes)

1. **Утечка test в train:** кумулятивная цель train-строки не выходит за 2018-12; калибровка не видит test.
2. **Единицы:** SAAR → м/м по `((1+saar/100)^(1/12)−1)·100`; `rosstat_ipc_mes.percent−100`; таргет в п.п.
3. **Веса:** NNLS, неотрицательны, нормированы к 1, оценены только на train.
4. **Прокси-тождество:** `(π − w_u·u)/(1−w_u)`; постоянный `u` неидентифицируем после калибровки — тест на инвариантность.
5. **Исключения:** 2014-12…2015-03, 2020-04, весь 2022; наблюдение выпадает, если исключён любой месяц окна.
6. **Выравнивание h:** `cum_h(t)` = сумма `t..t+h−1`; origin — месяц nowcast.
7. **Детерминизм:** повторный прогон даёт те же JSON/числа.
8. **Отсутствие CSV nowcast:** понятная ошибка, не тихий ноль.

---

### Task 1: Спека и план

**Files:**
- Create: `docs/superpowers/specs/2026-10-18-cb-comp-proxy-design.md`
- Create: `docs/superpowers/plans/2026-10-18-cb-comp-proxy.md`

- [x] **Step 1:** Зафиксировать дизайн: данные, формула прокси, допущения, источник nowcast, калибровка, исключения, границы.
- [x] **Step 2:** Commit: `docs: cb comp proxy spec and plan`.

---

### Task 2: Каркас скрипта — данные и прокси-таргет

**Files:**
- Create: `scripts/cb_comp_proxy.py`
- Modify: `Makefile` (цель `cb-comp-proxy`)
- Modify: `.gitignore`

**Interfaces:**
- `SEED = 20261018`; `SA_CPI_SERIES = "Все товары и услуги"`; `SAAR_SERIES`; `ROSSTAT_SERIES`.
- `@dataclass(frozen=True) Point: month: date; value: float`.
- `saar_to_mm(value) -> float`; `month_shift`, `month_range`, `month_end`.
- `load_sa_cpi()`, `load_saar()`, `load_rosstat()` → `dict[str, dict[date,float]]` / list.
- `nnls_weights(rosstat, start, end) -> dict[str,float]`.
- `proxy_target(sa_cpi, weight, annual_growth) -> dict[date,float]`.
- `cumulative(points, month, h) -> Optional[float]`; `article_excluded(month)`, `window_excluded(months)`.
- CLI: `--nowcast-csv`, `--window`, `--utilities-weight`, `--utilities-annual-growth`, `--train-start/end`, `--test-start/end`, `--out-dir`.

- [ ] **Step 1: Тесты парсинга/календаря/таргета (red).**
- [ ] **Step 2: Реализация, зелёные тесты.**
- [ ] **Step 3: Commit:** `cb comp proxy: data and proxy target`.

---

### Task 3: Nowcast, калибровка, h-step, метрики

**Files:**
- Modify: `scripts/cb_comp_proxy.py`

**Interfaces:**
- `load_nowcast(path, window) -> dict[str, dict[date,float]]` (дубли — ошибка).
- `fit_direct(train_rows) -> tuple[float,float,float]` (OLS `cum_h ~ 1 + nowcast + π_ex_{t−1}`).
- `evaluate_model(...) -> ModelMetrics` (rmse/mae/n по h, с исключениями и без).
- `bootstrap_rmse_ci(errors, block=3, n=2000, seed=SEED)`.
- `render_report(results) -> str`; `write_artifacts(...)`.

- [ ] **Step 1: Тесты (red):** калибровка на синтетике восстанавливает коэффициенты; h-step сумма; исключения; RMSE на ручных числах; дубли CSV — ошибка; детерминизм.
- [ ] **Step 2: Реализация, зелёные тесты.**
- [ ] **Step 3: Commit:** `cb comp proxy: calibration, h-step and metrics`.

---

### Task 4: Живой прогон и отчёт

**Files:**
- Create: `docs/research/2026-10-18-cb-comp-proxy.md`
- Modify: `docs/ROADMAP_KEY_RATE.md` (строка прокси-пути)
- Modify: `scripts/cb_comp_proxy_test.py` (если живой прогон вскрыл дефект)

- [ ] **Step 1:** `make cb-comp-proxy ARGS="--nowcast-csv /tmp/cpi_monthly_predictions_long.csv"` — таблица RMSE/MAE по h, sensitivity, JSON/CSV.
- [ ] **Step 2:** Сверить числа с CB_COMP (0.16/0.30/0.44/0.97 и 0.20/0.39/0.59/1.05), записать отношения и вывод «не паритет».
- [ ] **Step 3:** Отчёт: данные, формула, веса, допущения, числа, бенчмарки, чувствительность, почему прокси слабый.
- [ ] **Step 4: Commit:** `docs: cb comp proxy report and roadmap`.

---

### Task 5: Приёмка

- [ ] `make all` — зелёный.
- [ ] `uv run --with pandas --with scipy --python 3.12 python3 scripts/cb_comp_proxy_test.py -v` — зелёный офлайн.
- [ ] Повторный `make cb-comp-proxy` — те же числа/JSON.
- [ ] `git status` — артефакты не в индексе; в ClickHouse ничего не записано.
- [ ] Commit/push, PR с `Refs #190`; протокол приёмки — комментарием в PR.

## Self-Review

- **Покрытие спеки:** §2–3 → Task 2; §4–6 → Task 3; §7–8 → Task 4; §9 — в отчёте.
- **Согласованность имён:** `proxy_target`, `cumulative`, `load_nowcast`, `fit_direct`, `evaluate_model` едины.
- **Review Focus:** каждый пункт привязан к тесту (утечка — train-границы; единицы — `saar_to_mm`/`percent−100`; веса — NNLS; тождество — инвариантность; исключения — границы; h — сумма; детерминизм — повтор; CSV — ошибка).
