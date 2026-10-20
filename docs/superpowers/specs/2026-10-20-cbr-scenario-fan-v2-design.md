# Дизайн: Task 3 — ядро ordered logit/ML + модель дисперсии

**Пункт:** `key-rate/phase-4-cbr-scenario-fan` — issue [#272](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/272)
**Task 3:** issue [#286](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/286)
**Роль:** `role:ml`; **фаза:** 4
**Спека-источник:** [2026-10-20-cbr-scenario-fan.md](../../research/2026-10-20-cbr-scenario-fan.md) §5 (следующий шаг)
**Зависимость:** #284 (Task 1, merged)
**Статус:** In Progress. В `forecast_log` не пишется.

---

## 1. Гипотеза и решение

Task 1 показал: shock-слой значимо улучшает CRPS против `fan_raw` (−16.27, p=0.012), но веер остаётся недорассеянным (coverage 80 % — 0.429) и хуже точечных бенчмарков, потому что **центр** веера — отвергнутый `cbr_rule`. Гипотеза Task 3:

> Если заменить ядро на `ordered_logit` (или ML-челленджер), оставить сценарии КПМ как центрированные отклонения и добавить модель дисперсии остатков, то веер достигнет номинального coverage 80 % (0.70–0.90) и не будет хуже Task 1/точечного `ordered_logit` по CRPS.

Гипотеза может быть **отвергнута** — это допустимый результат. Решение: `promote` / `iterate` / `reject` (§8).

## 2. Что значит «сменить ядро»

В Task 1 каждый сценарий — вариант одного правила `cbr_rule`, и шаг сценария задавал абсолютный центр. Смена ядра разделяет:

1. **Локацию** — `m_t` из `ordered_logit`/ML, модели, которая лучше предсказывает решение (ordered logit: RMSE 145.05, Brier 0.6111).
2. **Сценарии** — центрированные дельты `δ_s` (`Σδ_s = 0`): `budget`, `inertial`, `rational`, `hard`, `no_market_loans` добавляют форму/разброс, а не сдвигают центр.
3. **Остатки** — walk-forward ошибки ядра `e = факт − m`; модель дисперсии задаёт квантили `q_α(t)`, которые расширяют веер в шоках.

Веер: `point = m_t + δ_s + q_α(t)`. Это лечит недорассеяние и убирает смещение `cbr_rule` из центра.

## 3. Данные и выборка

- 108 решений `2014-02-14 … 2026-09-11`; пропусков нет.
- Walk-forward: expanding `min_train=24` — 84 фолда (primary); rolling40 — 68 фолдов.
- Строки — `CbrRow` из Task 1 (`key_rate_cbr_rule.build_cbr_rows`); шоковая панель/веса — `shock_classifier` через Task 1 (`load_shock_panel`, `shock_arrays`).
- As-of — конвенции Task 1: CPD — конец M+1; ВВП — конец квартала + 3 месяца; консенсус — волна + 6 дней; бюджет — конец M+1; PIT/винтажей нет.
- Ядро и остатки — только прошлые фолды; точка, опубликованная после заседания, не участвует (тест).

## 4. Ядро

### 4.1. Primary — `ordered_logit`

`krm.fit_ordered_logit(train, row)` на `[rate_before, pi_saar, gap]`, walk-forward; `Forecast.step_bp` — `m_t` (E[step]), `probs` — вероятности классов. Ядро предвычисляется для каждого окна один раз (`krm.walk_forward`), чтобы остатки фолда были строго прошлыми.

Почему ordered logit:
- уже конкурентоспособен: RMSE 145.05, MAE 72.08, Brier 0.6111 (Task 1/`key_rate_model`);
- даёт вероятности классов;
- не наследует смещение `cbr_rule` (RMSE 304.34).

### 4.2. ML-челленджер — `ElasticNetCV`

- Признаки: `[rate_before, pi_saar, gap, e3_core, e3_headline_adaptive, gdp_gap, ibk_gap]`.
- `ElasticNetCV(l1_ratio=[0.1, 0.5, 0.9], cv=5, random_state=20261020)` строго внутри train; NaN — медиана train.
- Возврат — `krm.Forecast(step_bp, None)`.
- CatBoost на 108 решениях **не используется**: правило [KEY_RATE_MODEL.md](../../KEY_RATE_MODEL.md) §4 — CatBoost на компонентах, не на решении.
- ML-ядро — challenger: primary-веер считается на ordered logit; ML публикуется как `core_fan_ml` (robustness). Ядро не выбирается по тесту.

## 5. Сценарные дельты

Для каждого заседания — 5 шагов Task 1 (`fan1.scenario_steps`):

```
base = steps[0].components.step_bp                 # inertial
deltas = [s.components.step_bp - base for s in steps]
mean_delta = mean(deltas)
centered = [d - mean_delta for d in deltas]        # Σ = 0
```

- `budget_z` — по обучающему окну (медиана/IQR импульса, клип ±2; `fan1.train_budget_stats`/`fan1.budget_z` после рефакторинга).
- Fallback-метки сценариев (`rational_fallback`, `budget_fallback`) сохраняются в CSV; `budget`/`rational` без признака дают дельту 0 и метку.
- Дельты — reduced-form прокси КПМ, не QPM.

## 6. Остатки и модель дисперсии

### 6.1. Остатки

Для фолда `t`:

```
e_i = actual_step_i - core_step_i          # i — прошлые walk-forward фолды ядра, i < t
n = len(e_i)
```

Если `n < MIN_RESIDUALS = 12` — фолд пропускается со счётчиком (первый expanding-фолд имеет 24 остатка, rolling40 — 40).

### 6.2. Квантильная сетка

`ALPHAS = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)`.

### 6.3. Три модели дисперсии

| Вариант | Формула | Роль |
|---|---|---|
| `disp_global` | `q_α = quantile(e_train, α)` | ablation |
| `disp_scale` | `log(|e_i|+1) = γ0 + γ1·(1−w_i) + γ2·p_i`; `scale_i = exp(γ0+γ1(1−w_i)+γ2p_i)−1`; `u_i = e_i / max(scale_i, eps)`; `q_α = scale_t·quantile(u_train, α)`; `scale_t` — по `w_t`, `p_t` текущего заседания | **primary** |
| `disp_conformal` | `q_α = sorted(e_train)[min(n, ⌈(n+1)·α⌉)−1]` | robustness |

- `w_i`, `p_i` — причинные значения Task 1; `p` клипуется 0–0.25.
- `disp_scale` — 2 признака, чтобы не переобучиться на 84 фолдах; коэффициенты публикуются.
- `scale_t ≤ 0` → fallback на `disp_global` со счётчиком.
- `disp_conformal` — конечная выборка; exchangeability временного ряда нарушается, это robustness, не гарантия.
- Вне объёма: quantile regression, CQR, Bayesian-дисперсия — следующие итерации.

## 7. Веера

Для каждого варианта:

```
point = core_step + delta_s + q_α
weight = 1 / (5 · 7)
```

| Веер | Ядро | Дисперсия | Роль |
|---|---|---|---|
| `core_fan_global` | ordered logit | `disp_global` | ablation |
| `core_fan_scale` | ordered logit | `disp_scale` | **primary** |
| `core_fan_conformal` | ordered logit | `disp_conformal` | robustness |
| `core_fan_ml` | ElasticNetCV | `disp_scale` | challenger |

`FanPrediction` Task 1: `steps`/`labels` (`"<scenario>|q<alpha>"`)/`fallbacks` выровнены; `p_shock`/`weight`/`emergency` — для CSV. С центрированными дельтами медиана сценариев ≈ 0; медиана веера — `m_t + median(q)`.

## 8. Метрики, сравнения, решение

### 8.1. Метрики

- Coverage 50/80/90 + Wilson CI; CRPS; ширина; Brier/log loss; медиана RMSE/MAE.
- DM-HLN по медиане и блочный бутстрап по CRPS (Task 1 `block_bootstrap_mean_diff`).

### 8.2. Сравнения

- Task 1 `fan_shock_weighted` — reference по числам отчёта Task 1 (CRPS 102.97, RMSE 204.49; не пересчитывается).
- `ordered_logit` point, B1 hold, B2 repeat, B3 AR(1).
- `core_fan_global` (ablation), `core_fan_ml` (challenger).

### 8.3. Правило решения

- **`promote`:** coverage 80 % в `[0.70; 0.90]` и Wilson CI включает 0.8; CRPS `core_fan_scale` значимо ниже Task 1 `fan_shock_weighted` (CI разницы ниже 0); CRPS не хуже точечного `ordered_logit` более чем на 10 %; медианный RMSE не значимо хуже `ordered_logit`.
- **`iterate`:** coverage улучшился, CRPS нет; или наоборот.
- **`reject`:** улучшения нет.
- Primary — один (`core_fan_scale`, expanding); остальные — robustness/Bonferroni; выбор по тесту запрещён.

### 8.4. Эпизоды

2022–2023, 2024–2026; 2014/2015 — в train (expanding/rolling40), не в тесте; отметить в отчёте.

## 9. Тесты

`scripts/key_rate_scenario_fan_v2_test.py` (offline, unittest):

1. **Core:** ordered logit walk-forward; остатки только прошлых фолдов; нет look-ahead.
2. **Дельты:** 5 сценариев, `Σδ=0`; `hard`/`budget`/`nml` меняют дельту; fallback-метки.
3. **disp_global:** квантили монотонны; пустой/короткий train — skip.
4. **disp_scale:** `scale>0`; `1−w`/`p` увеличивают ширину; коэффициенты конечны; fallback при `scale≤0`.
5. **disp_conformal:** индекс `⌈(n+1)α⌉`; coverage на синтетике; монотонность.
6. **Веера:** точки = core+delta+q; веса 1/35; медиана/coverage/CRPS; `core_fan_global` vs `core_fan_scale`.
7. **ML challenger:** `ElasticNetCV` только train; детерминизм; NaN-политика.
8. **Артефакты/`ML_LOG`:** CSV/JSON детерминированы; payload содержит метрики/предсказания.

## 10. Приёмка

1. `make key-rate-scenario-fan-v2-test` — офлайн-тесты.
2. `ML_LOG=1 MLFLOW=0 make key-rate-scenario-fan-v2` — 108 заседаний, 84/68; coverage/CRPS; `v_ml_runs` `status=ok`, `git_dirty=0`; повтор детерминирован.
3. `make all`, `make docs-check` — зелёные.
4. `git diff` без Go/DDL/витрин/грантов/`forecast_log`.
5. PR Task 3 — `Part of #272`, протокол комментарием.

## 11. Границы и риски

- **Ordered logit слаб в 2022** (RMSE 277.20 в #283); дисперсия может накрыть факт, но медиана останется смещённой — это результат, а не провал.
- **Сценарные дельты** из `cbr_rule` шумные; центрирование убирает уровень, не шум.
- **`disp_scale`** — 2 признака; проверка — коэффициенты и ablation `disp_global`.
- **Conformal** нарушает exchangeability временного ряда; только robustness.
- **2014/2015** — в train; не тестируется.
- **As-of** без PIT; CPD/ВВП/Минфин без винтажей.
- **Множественные сравнения** — primary один.
- **Вне объёма:** `forecast_log`, ingest, Go/DDL/витрины/гранты, CPI (Task 2, #285), многозаседаний путь, quantile regression/CQR/Bayesian.

## 12. Артефакты и CLI

- `key_rate_scenario_fan_v2_report.md`
- `key_rate_scenario_fan_v2_predictions.csv` — `meeting_date, window, fan, scenario, step_bp, weight, actual_step_bp, emergency, shock_prob, w, fallback`.
- `key_rate_scenario_fan_v2_diagnostics.csv` — core step/probs, дельты, `scale_t`, квантили.
- `key_rate_scenario_fan_v2_forecast.json` — as-of на 2026-10-23.
- CLI: `--as-of YYYY-MM-DD`, `--out-dir DIR`, `--rolling`.
- `ML_LOG=1` — run `key_rate_scenario_fan_v2`, `scenario_fan_v2__rate__start24`; метрики/предсказания/артефакты sha256; в ClickHouse не пишет, `forecast_log` не трогает.

## 13. Ссылки

- [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §6, фаза 4.
- [KEY_RATE_MODEL.md](../../KEY_RATE_MODEL.md) §4, §6.
- [2026-10-20-cbr-scenario-fan.md](../../research/2026-10-20-cbr-scenario-fan.md) §5.
- [2026-10-20-cbr-rule.md](../../research/2026-10-20-cbr-rule.md).
- [2026-10-19-key-rate-shock-classifier-design.md](2026-10-19-key-rate-shock-classifier-design.md).
