# План: Task 3 — ядро ordered logit/ML + модель дисперсии

**Пункт:** `key-rate/phase-4-cbr-scenario-fan` — issue [#272](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/272)
**Task 3:** issue [#286](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/286)
**Спека:** [2026-10-20-cbr-scenario-fan-v2-design.md](../specs/2026-10-20-cbr-scenario-fan-v2-design.md)
**Роль:** `role:ml`; **фаза:** 4
**Ветка / worktree:** `feat/cbr-scenario-fan-v2` / `.worktrees/cbr-scenario-fan-v2`
**База:** `origin/main` (`99a87f4`, PR #284 merged)
**Статус:** In Progress.

---

## 1. Цель и границы

Заменить ядро веера с отвергнутого `cbr_rule` на `ordered_logit` (primary) / `ElasticNetCV` (challenger), оставить 5 сценариев Task 1 как **центрированные дельты** и добавить модель дисперсии остатков (`disp_global`, `disp_scale` — primary, `disp_conformal` — robustness). Проверить coverage 80 % и CRPS против Task 1/`ordered_logit`/B1–B3. В `forecast_log` не писать, ClickHouse не менять, Go/DDL/витрины/гранты не трогать.

**Готово, если:** офлайн-тесты зелёные, живой прогон записан числами, отчёт/роадмап обновлены, `make all`/`make docs-check` зелёные, PR Task 3 открыт с протоколом.

## 2. Задачи

### Task 0 — рабочее дерево и трекер (сделано)

- [x] `git fetch origin`; worktree `.worktrees/cbr-scenario-fan-v2`, ветка `feat/cbr-scenario-fan-v2` от `origin/main` (`99a87f4`).
- [x] Sub-issue #286 создан (parent #272), labels `roadmap:key-rate`, `role:ml`.
- [x] `docs/ROADMAP_KEY_RATE.md`: Task 1 `✅` merged #284, Task 2 #285, Task 3 #286 + спека/план.
- [x] Project #272: `Status=In Progress`, `Spec / Plan` = Task 1 + Task 3.

### Task 1 — спека и план (сделано)

- [x] `docs/superpowers/specs/2026-10-20-cbr-scenario-fan-v2-design.md`.
- [x] `docs/superpowers/plans/2026-10-20-cbr-scenario-fan-v2.md`.
- Отдельный коммит: `docs: add scenario fan v2 design and plan`.

### Task 2 — рефакторинг Task 1 (без изменения поведения)

- `scripts/key_rate_scenario_fan.py`: `_train_budget_stats` → `train_budget_stats`, `_budget_z` → `budget_z`; обновить вызовы.
- Task 1 тесты (`make key-rate-scenario-fan-test`, 32) остаются зелёными.

### Task 3 — ядро

Новый файл `scripts/key_rate_scenario_fan_v2.py`. Импорты:

```python
import key_rate_model as krm
import key_rate_cbr_rule as cr
import key_rate_scenario_fan as fan1
import ml_experiment as mlx
```

Функции:

- `ordered_core_predictions(rows, window)` — `krm.walk_forward(rows, "ordered_logit", krm.fit_ordered_logit, ...)`; map by date.
- `fit_enet_core(train, row)` — `ElasticNetCV` на `[rate_before, pi_saar, gap, e3_core, e3_headline_adaptive, gdp_gap, ibk_gap]`; `cv=5`, `random_state=20261020`; NaN — медиана train; `Forecast(step, None)`.
- `ml_core_predictions(rows, window)` — `krm.walk_forward(rows, "ml_core", fit_enet_core, ...)`.
- `core_residuals(predictions, day)` — `[(actual − step)]` только для `meeting_date < day`; `n < MIN_RESIDUALS=12` → skip.

**Тесты:** `TestCore` — ordered logit/ML walk-forward; остатки только прошлых фолдов; ML детерминирован.

### Task 4 — сценарные дельты

- `centered_deltas(row, impulse, budget_z)` — `fan1.scenario_steps` → `deltas = step_s − step_inertial`; `centered = deltas − mean`; `Σ=0`; вернуть `(ScenarioStep, delta)`.
- `budget_z` — `fan1.train_budget_stats`/`fan1.budget_z` по обучающему окну.
- Fallback-метки сохраняются.

**Тесты:** `TestDeltas` — 5 сценариев; `Σδ≈0`; `hard`/`budget`/`nml` меняют дельту; fallback.

### Task 5 — дисперсия

- `disp_global(residuals)` — `np.quantile` по `ALPHAS=(0.05,0.10,0.25,0.50,0.75,0.90,0.95)`.
- `disp_scale(residuals, weights, probs, current_w, current_p)` — fit `log(|e|+1) = γ0+γ1(1−w)+γ2p`; `scale_t`; `q_α = scale_t·quantile(e/scale, α)`; fallback global при `scale_t≤0`.
- `disp_conformal(residuals)` — `q_α = sorted(e)[min(n, ⌈(n+1)α⌉)−1]`.

**Тесты:** `TestDispersion` — монотонность; `scale>0`; `1−w`/`p` увеличивают ширину; conformal-индекс; fallback.

### Task 6 — веера и метрики

- `build_core_fans(core_step, deltas, quantiles)` — 5×7 точек, веса 1/35; `FanPrediction` Task 1.
- Варианты: `core_fan_global`, `core_fan_scale`, `core_fan_conformal`, `core_fan_ml`.
- Бенчмарки: `ordered_logit` point (из core predictions), B1/B2/B3 (`krm.predict_hold`/`repeat`/`fit_ar1`).
- Метрики — `fan1.fan_metrics`, `summarize_fans`, `summarize_episodes`, `compare_fan_to_benchmark`, `compare_fans`; Task 1 `fan_shock_weighted` — reference по числам (102.97/204.49).
- `decide_v2` — правило спеки §8.3.

**Тесты:** `TestFans` — точки/веса; медиана/coverage/CRPS; primary vs ablation; `Σδ=0` не сдвигает центр.

### Task 7 — артефакты, CLI, `ML_LOG`

- `key_rate_scenario_fan_v2_report.md`
- `key_rate_scenario_fan_v2_predictions.csv`
- `key_rate_scenario_fan_v2_diagnostics.csv` — core step/probs, дельты, `scale_t`, квантили.
- `key_rate_scenario_fan_v2_forecast.json` — as-of 2026-10-23.
- CLI: `--as-of`, `--out-dir`, `--rolling`.
- `_log_run` — `mlx.start_run(experiment="key_rate_scenario_fan_v2", run_name="scenario_fan_v2__rate__start24", ...)`.
- `.gitignore`: `/key_rate_scenario_fan_v2_*`.

### Task 8 — Makefile

```make
key-rate-scenario-fan-v2:
	@$(MLFLOW_PREFLIGHT)
	$(MLFLOW_ENV) $(MLFLOW_HTTP_ENV) uv run $(MLFLOW_PKG) --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_scenario_fan_v2.py $(ARGS)

key-rate-scenario-fan-v2-test:
	uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy --python 3.12 python3 scripts/key_rate_scenario_fan_v2_test.py -v
```

Добавить в `.PHONY`.

### Task 9 — тесты

`scripts/key_rate_scenario_fan_v2_test.py` — 8 групп спеки §9. Фикстуры — синтетические `CbrRow`/`PanelRow` + существующие `scripts/testdata/key_rate`, `scripts/testdata/shock_classifier`. Без ClickHouse/сети.

### Task 10 — живой прогон и отчёт

```bash
make key-rate-scenario-fan-v2-test
ML_LOG=1 MLFLOW=0 make key-rate-scenario-fan-v2
ML_LOG=1 MLFLOW=0 make key-rate-scenario-fan-v2   # повтор: числа/sha256 совпадают
```

Отчёт `docs/research/2026-10-20-cbr-scenario-fan-v2.md`:
- core (ordered logit/ML), дельты, `scale_t`, квантили;
- таблица «веер × окно»: coverage 50/80/90, CRPS, ширина, Brier/log loss, медиана RMSE/MAE;
- сравнение с Task 1 `fan_shock_weighted`, `ordered_logit`, B1–B3;
- эпизоды 2022–2023/2024–2026;
- текущий as-of прогноз;
- вердикт `promote`/`iterate`/`reject`;
- «что не проверено».

### Task 11 — документация и трекер

- `docs/KEY_RATE_MODEL.md` — подраздел «Веер v2: ядро + дисперсия».
- `docs/LIMITATIONS.md` — ограничения спеки §11.
- `docs/site-pages.json` — research-отчёт.
- `docs/ROADMAP_KEY_RATE.md` — Task 1 `✅`, Task 2 #285, Task 3 #286, результаты.
- Issue #286 — протокол приёмки комментарием; Project #272 `Status=In Review` при PR, `In Progress` после мержа (Task 2/3).
- `README.md`/`ARCHITECTURE.md` — не меняются.

### Task 12 — PR

- PR `feat/cbr-scenario-fan-v2`, заголовок `[key-rate/phase-4-cbr-scenario-fan] Task 3: ядро ordered logit/ML + модель дисперсии`.
- `Part of #272` (не `Closes`), протокол — комментарием; `Status=In Review`, `PR` в Project.

## 3. Приёмка

1. `make key-rate-scenario-fan-v2-test` — все офлайн-тесты.
2. `ML_LOG=1 MLFLOW=0 make key-rate-scenario-fan-v2` — живой прогон; 108 заседаний, 84/68; числа в отчёте; повтор детерминирован; `v_ml_runs` `status=ok`, `git_dirty=0`.
3. `make all` — зелёный.
4. `make docs-check` — зелёный.
5. `git diff` без Go/DDL/витрин/грантов/`forecast_log`.
6. PR Task 3 — `Part of #272`, протокол; `Status=In Review`.

## 4. Что не проверено

- PIT/винтажи CPD, ВВП и Минфина; бюджетный лаг консервативный.
- Ordered logit слаб в 2022; дисперсия может накрыть, но медиана смещена.
- Сценарные дельты `cbr_rule` — reduced-form прокси, не QPM.
- `disp_scale` — 2 признака; conformal — robustness.
- 2014/2015 — в train, не тестируется.
- CPI (Task 2 #285), многозаседаний путь, quantile regression/CQR — вне объёма.

## 5. Риски

| Риск | Митигация |
|---|---|
| Ordered logit смещён в 2022 | coverage/CRPS по эпизодам; вердикт `iterate` допустим |
| Сценарные дельты шумные | центрирование, ablation с `deltas=0` (core-only) |
| `disp_scale` переобучается | 2 признака, коэффициенты в отчёте, ablation `disp_global` |
| Conformal нарушает exchangeability | только robustness, не primary |
| Множественные сравнения | primary один; Bonferroni/пометка |
