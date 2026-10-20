# План: сценарный веер ставки с shock-aware слоем (Task 1 #272)

**Пункт:** `key-rate/phase-4-cbr-scenario-fan` — issue [#272](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/272)
**Спека:** [2026-10-20-cbr-scenario-fan-design.md](../specs/2026-10-20-cbr-scenario-fan-design.md)
**Роль:** `role:ml`; **фаза:** 4
**Ветка / worktree:** `feat/cbr-scenario-fan` / `.worktrees/cbr-scenario-fan`
**Статус:** In Progress; Task 1 — этот PR, Task 2 (CPI) — следующий PR того же пункта.

---

## 1. Цель и границы

Собрать и измерить **сценарный веер шага ставки** по 5 reduced-form сценариям КПМ с shock-aware слоем; сравнить с `fan_raw` и точечными B1–B3/`ordered_logit` по CRPS, coverage и точечным метрикам. В `forecast_log` не писать, ClickHouse не менять, Go/DDL/витрины/гранты не трогать.

**Готово, если:** офлайн-тесты зелёные, живой прогон записан числами, отчёт и роадмап обновлены, `make all`/`make docs-check` зелёные, PR Task 1 открыт с протоколом.

---

## 2. Задачи

### Task 0 — рабочее дерево и трекер (сделано)

- [x] `git fetch origin`; worktree `.worktrees/cbr-scenario-fan`, ветка `feat/cbr-scenario-fan` от `origin/main` (`c8a36d7`).
- [x] Cherry-pick `e84e42d` — §6.1 в `docs/research/2026-10-20-cbr-rule.md`.
- [x] `docs/ROADMAP_KEY_RATE.md` — строка `phase-4-cbr-scenario-fan`: `🔶`, Task 1/Task 2, `Depends on: #267, #268`, `Related: #269`, ссылки на спеку/план.
- [x] Issue #272 — тело обновлено, комментарий «Taking», evidence §6.1.
- [x] Project V2 — `Status=In Progress`, `Spec / Plan`, `Depends on=#267, #268`.

### Task 1 — спека (сделано)

- [x] `docs/superpowers/specs/2026-10-20-cbr-scenario-fan-design.md`.
- Отдельный коммит: `docs: add scenario fan design`.

### Task 2 — слой данных `scripts/key_rate_scenario_fan.py`

Файл: `scripts/key_rate_scenario_fan.py` (новый). Импорты:

```python
import eval_utils as eu
import key_rate_model as krm
import key_rate_cbr_rule as cr
import shock_classifier as sc
import ml_experiment as mlx
```

Функции:

- `load_budget()` — SQL к `minfin_fed_bud_mes`:
  ```sql
  SELECT name, date, toFloat64(value) AS value
  FROM minfin_fed_bud_mes FINAL
  WHERE name IN ('Доходы', 'Дефицит (-)/Профицит (+)')
  ```
  → `dict[str, list[(date, float)]]`.
- `available_from_budget(period_end: date) -> date` — конец M+1 (как `krm.available_from_cpd`).
- `budget_impulse(points, meeting_date) -> Optional[float]` — 12-месячные суммы, `-deficit/income*100`; пропуск/неположительный доход → `None`.
- `load_shock_panel()` — `sc.load_meetings_with_emergency()`, `sc.load_key_rate()`, `sc.load_channels()`, `sc.load_mp_summary_deltas()`, `sc.build_panel(...)`.
- `shock_arrays(panel) -> tuple[list[float], list[float]]` — `w_t`, `p_t`; `p_t=None→0`, клип `[0, 0.25]`.
- `align_rows(rows, panel) -> dict[date, PanelRow]` — проверка 1:1 по датам; расхождение → `SampleError`.
- `train_budget_z(rows, budget_by_date) -> tuple[float, float]` — медиана/IQR по train; `IQR=0` → std → 1.

**Тест:** `TestBudgetImpulse` — 12 месяцев, лаг, пропуск, отрицательный доход; `TestAlignment` — даты 1:1.

### Task 3 — сценарии

Функции:

- `Scenario` dataclass: `name`, `spec: cr.RuleSpec`, `kind` (`base|rational|budget|nml`).
- `SCENARIOS = (...)` — 5 штук, порядок фиксирован:
  `inertial`, `rational`, `hard`, `budget`, `no_market_loans`.
- `scenario_components(row, scenario, budget_z, budget_available) -> tuple[RuleComponents, dict]`:
  - `inertial`/`hard` — `cr.rule_components(row, spec)`;
  - `rational` — `e3_headline_consensus`, при `None` — `e3_headline_adaptive` + `fallback=True`;
  - `budget` — базовые компоненты, `gap + 0.30*z`; `z=0` + `fallback=True`, если импульса нет;
  - `no_market_loans` — базовые компоненты, `gap * (0.05/0.15)`.
  - Пересчёт `target`/`step_bp` — общий helper `components_with_gap(...)`.
- `scenario_steps(row, budget_z, budget_available) -> list[ScenarioStep]` — 5 шагов + метаданные (`fallback`, `gap`, `e3_mp`).

**Тесты:** `TestScenarios` — 5 имён; `hard` меняет знак/величину при отклонении E3 от цели; `rational` fallback; `budget`/`nml` меняют gap; нет NaN.

### Task 4 — шоковый слой

Функции:

- `emergency_steps(train_rows, panel_by_date) -> list[float]` — шаги прошлых `emergency=True`.
- `fan_weights(p_t, emergency_count) -> list[tuple[float, float]]` — 5 сценарных весов + emergency-ветка.
- `blend_step(step, last_step, w) -> float` — `w*step + (1-w)*last_step`.
- `build_fans(scenario_steps, emergency_steps, p_t, w_t, last_step, emergency_now) -> dict[str, list[tuple[float,float]]]`:
  - `fan_raw`, `fan_shock`, `fan_shock_weighted`, `fan_emergency_ex_post`.

**Тесты:** `TestShockLayer` — `p=0` = raw; `p=1` — только emergency; пустая ветка; `w=1` = без смешивания; первые 24 веса 1.0.

### Task 5 — метрики

В `scripts/eval_utils.py` добавить:

- `weighted_quantile(points, weights, q) -> float` — линейная интерполяция по накопленным весам.
- `crps_weighted(points, weights, actual) -> float` — формула спеки §6.2.
- `wilson_interval(successes, n, z=1.96) -> tuple[float,float]`.
- `block_bootstrap_mean_diff(loss_a, loss_b, block=4, iterations=2000, seed=...) -> BootstrapResult` — переиспользовать структуру `block_bootstrap_rmse_diff`.

В `key_rate_scenario_fan.py`:

- `fan_metrics(fan, actual) -> dict` — coverage 50/80/90, CRPS, width, probs, median.
- `aggregate(groups) -> dict` — по `(fan, window)`, эпизодам и emergency.
- `compare(fan_group, benchmark_group) -> dict` — DM-HLN по медиане и блочный бутстрап по CRPS на общих датах.

**Тесты:** `TestMetrics` — точечная масса CRPS = `|x−y|`; coverage монотонен; Wilson; бутстрап детерминирован.

### Task 6 — walk-forward и артефакты

- `run_walk_forward(rows, panel_by_date, budget_by_date) -> (predictions, fold_skips)`:
  - окна: expanding `min_train=24`, rolling40;
  - для каждого фолда: `train`, `row`; сценарии; `emergency_steps(train)`; `p_t`/`w_t`; веера; бенчмарки B1/B2/B3/`ordered_logit`/`cbr_rule` на том же `train`/`row`;
  - вырожденные бенчмарки — `bench_skips`, не роняют фолд.
- `render_report(...) -> str` — таблицы, эпизоды, вердикт, ограничения.
- `write_predictions`, `write_diagnostics`, `write_forecast` — CSV/JSON по спеке §12.
- `_log_run(...)` — `mlx.start_run(experiment="key_rate_scenario_fan", run_name="scenario_fan__rate__start24", ...)`.
- `run_summary(as_of, out_dir, rolling)` и `main(argv)` — CLI.

**Тесты:** `TestArtifacts` — детерминизм отчёта/CSV/JSON; `ML_LOG` payload.

### Task 7 — Makefile

Добавить в `.PHONY` и цели:

```make
key-rate-scenario-fan:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_scenario_fan.py $(ARGS)

key-rate-scenario-fan-test:
	uv run --with scikit-learn --with pandas --with "statsmodels>=0.15" --with scipy --python 3.12 python3 scripts/key_rate_scenario_fan_test.py -v
```

### Task 8 — офлайн-тесты

Файл: `scripts/key_rate_scenario_fan_test.py`. Покрыть 8 групп из спеки §8. Фикстуры — синтетические `CbrRow`/`PanelRow` + существующие `scripts/testdata/key_rate` и `scripts/testdata/shock_classifier`; при необходимости `--export-fixtures scripts/testdata/scenario_fan`. Тесты не ходят в ClickHouse/сеть.

### Task 9 — живой прогон и отчёт

```bash
make key-rate-scenario-fan-test
ML_LOG=1 make key-rate-scenario-fan
ML_LOG=1 make key-rate-scenario-fan          # повтор: числа/sha256 совпадают
```

- Отчёт `docs/research/2026-10-20-cbr-scenario-fan.md`: выборка, покрытие, таблица «веер × окно» (CRPS, coverage 50/80/90, ширина, Brier/log loss, RMSE/MAE медианы), DM/бутстрап против `fan_raw`/B1–B3/`ordered_logit`, эпизоды, текущий as-of прогноз, вердикт, «что не проверено».
- Паспорт: `ML_LOG=1`, run `key_rate_scenario_fan`, артефакты sha256, `git_dirty=0`.

### Task 10 — документация и трекер

- `docs/KEY_RATE_MODEL.md` — подраздел о сценарном веере и shock-aware слое.
- `docs/LIMITATIONS.md` — ограничения из спеки §10.
- `docs/site-pages.json` — research-отчёт.
- `docs/ROADMAP_KEY_RATE.md` — статус `🔶` (Task 1 done, Task 2 pending), ссылки на отчёт.
- Issue #272 — протокол приёмки комментарием; `Status=In Review` при PR; после мержа — `In Progress` (Task 2), `Done` после Task 2.
- `README.md`/`ARCHITECTURE.md` — не меняются.

### Task 11 — PR

- Один PR ветки `feat/cbr-scenario-fan`; заголовок `[key-rate/phase-4-cbr-scenario-fan] shock-aware сценарный веер ставки (Task 1)`.
- В описании: пункт, `Part of #272` (не `Closes`), источник (research §7 P1.4 + §6.1 #283), схема сценариев, запуск, пример проверки.
- Протокол живой приёмки — комментарием; `Status=In Review`, `PR` в Project.
- После мержа: Task 2 sub-issue, `Status=In Progress`; #272 не закрывается.

---

## 3. Протокол живой приёмки (заполнить числами)

```bash
make key-rate-scenario-fan-test
ML_LOG=1 make key-rate-scenario-fan
make all
make docs-check
```

Ожидаемые артефакты: `key_rate_scenario_fan_report.md`, `..._predictions.csv`, `..._diagnostics.csv`, `..._forecast.json`; run в `ml_runs` со `status=ok`; повтор — те же числа/sha256.

## 4. Что не проверено

- PIT/винтажи CPD, ВВП и Минфина; бюджетный лаг консервативный.
- 6 шоковых меток; `p_t` описательный.
- Сценарные прокси `budget`/`no_market_loans` — не QPM.
- Множественные сравнения: primary один, остальные — robustness.
- Многозаседаний путь, CPI-веер (Task 2), Phillips (#269).

## 5. Риски

| Риск | Митигация |
|---|---|
| §6.1 #283 — post-hoc на той же выборке | primary `fan_shock` предзарегистрирован; `fan_shock_weighted` — вторичная гипотеза; оговорка в отчёте |
| Мало шоковых меток | клип `p_t` 0.25; emergency-ветка только из прошлого; coverage/CRPS по эпизодам |
| Сценарные прокси произвольны | формулы и коэффициенты фиксированы в спеке §4; чувствительность — robustness |
| Coarse fan | 5–6 точек; интервалы называются coarse; CRPS честно учитывает дискретность |
| Множественные сравнения | Bonferroni/пометка; никакого выбора по тесту |
