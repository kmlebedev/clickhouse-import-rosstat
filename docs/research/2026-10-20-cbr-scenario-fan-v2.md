# Веер v2: ядро ordered logit/ML + модель дисперсии (Task 3 #272)

**Пункт:** `key-rate/phase-4-cbr-scenario-fan`, Task 3 — issue [#286](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/286)
**Роль:** `role:ml`; **фаза:** 4
**Спека:** [2026-10-20-cbr-scenario-fan-v2-design.md](../superpowers/specs/2026-10-20-cbr-scenario-fan-v2-design.md)
**План:** [2026-10-20-cbr-scenario-fan-v2.md](../superpowers/plans/2026-10-20-cbr-scenario-fan-v2.md)
**Дата:** 2026-10-20; **data as-of:** 2026-10-20

**Прогоны:**
- исходный (primary `core_fan_scale`): `32fd65f9-d347-482d-946b-bfb7c41c1e34`, `git_sha=f6b7443`, `git_dirty=0`;
- **post-hoc revision (primary `core_fan_conformal`):** `e0bb8e72-ee37-4b78-b37c-d6e359d2a731`, `git_sha=f11fdd7`, `git_dirty=0`.

**Повтор revision:** числа и sha256 совпали; артефакты — report `7ae3213e…`, predictions `4e5c1e25…`, diagnostics `3d52cd15…`, forecast `d0f09524…`.
**Вердикт revision: `promote`** по primary `core_fan_conformal` (coverage 80 % **0.806** [0.700; 0.880], CRPS **60.60**, RMSE 156.10). 2022–2023 **не закрыт по CRPS**: regime-вариант поднимает coverage до 0.800, но ценой CRPS 170.65 и ширины 748.89.

---

## 0. Post-hoc revision (owner decision 2026-10-20)

Исходный Task 3 был предзарегистрирован с primary **`core_fan_scale`**: прогон `32fd65f9…` дал `promote` при coverage 0.736 [0.624; 0.824], CRPS 60.76, RMSE 155.01. По результатам владелец решил:

1. **primary → `core_fan_conformal`** (coverage 0.806, CRPS 60.60); `core_fan_global` — secondary (coverage 0.764, CRPS 60.36).
2. Для 2022–2023 добавить **regime-варианты** `core_fan_conformal_regime`/`core_fan_global_regime`: shock-режим `emergency or w<0.5`, `MIN_REGIME_RESIDUALS=6`.

Это **post-hoc**: исходный результат `core_fan_scale` сохранён, новый прогон на той же выборке не является независимым подтверждением. Выбор `conformal` основан на coverage как целевом показателе, а не на новом тесте; отчёт публикует все варианты.

## 1. Что изменено относительно Task 1

1. **Ядро** — `ordered_logit` (primary): `m_t = E[step]`, вероятности классов; ML-челленджер — `ElasticNetCV`. CatBoost на 108 решениях не используется.
2. **Сценарии КПМ** — центрированные дельты `δ_s`, `Σδ_s = 0`: добавляют форму, не сдвигают центр.
3. **Остатки и дисперсия** — `e = факт − m_t` только по прошлым фолдам; квантили 0.05…0.95; `disp_conformal` — primary; `disp_global`/`disp_scale` — ablation; `disp_conformal_regime`/`disp_global_regime` — 2022–2023 fix.
4. Веер: `point = m_t + δ_s + q_α(t)`, 5×7 = 35 точек, веса 1/35.

## 2. Данные и выборка

- 108 решений `2014-02-14 … 2026-09-11`; пропусков нет.
- Ядро: expanding 84 фолда, rolling40 — 68.
- **Веер: 72/56** — первые 12 фолдов на окно уходят на residual warm-up (`MIN_RESIDUALS=12`); `fan_fold` skip 0; `scale_fallback` 0.
- As-of: CPD — конец M+1; ВВП — конец квартала + 3 месяца; консенсус — волна + 6 дней; бюджет — конец M+1; веса/вероятность шока — walk-forward. PIT/винтажей нет.

## 3. Результаты

### 3.1. Веер × окно

| веер | окно | n | CRPS | coverage 50 | coverage 80 | coverage 90 | ширина 80 | Brier | log loss | RMSE | MAE |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `core_fan_global` | expanding | 72 | **60.358** | 0.653 | 0.764 | 0.819 | 166.06 | 0.6961 | 1.2563 | 156.53 | 73.81 |
| `core_fan_global` | rolling40 | 56 | 77.734 | 0.446 | 0.696 | 0.786 | 245.83 | 0.6593 | 1.2492 | 174.29 | 96.01 |
| `core_fan_scale` | expanding | 72 | 60.761 | 0.667 | 0.736 | 0.792 | 189.06 | 0.6895 | 2.2889 | 155.01 | 73.99 |
| `core_fan_scale` | rolling40 | 56 | 75.149 | 0.500 | 0.750 | 0.839 | 265.11 | 0.6581 | 2.1043 | 173.61 | 96.81 |
| **`core_fan_conformal`** | expanding | 72 | 60.598 | 0.681 | **0.806** | 0.833 | 203.57 | 0.7009 | 1.5923 | 156.10 | 73.27 |
| **`core_fan_conformal`** | rolling40 | 56 | 81.778 | 0.554 | **0.821** | 0.857 | 508.48 | 0.6678 | 1.7249 | 175.21 | 96.75 |
| `core_fan_conformal_regime` | expanding | 72 | 75.345 | 0.667 | 0.819 | 0.861 | 387.77 | 0.7228 | 1.6778 | 158.07 | 74.77 |
| `core_fan_conformal_regime` | rolling40 | 56 | 93.532 | 0.536 | 0.786 | 0.839 | 472.19 | 0.6732 | 1.7133 | 177.85 | 99.53 |
| `core_fan_global_regime` | expanding | 72 | 64.485 | 0.597 | 0.722 | 0.819 | 264.35 | 0.7173 | 1.6688 | 157.79 | 75.13 |
| `core_fan_global_regime` | rolling40 | 56 | 81.066 | 0.446 | 0.589 | 0.732 | 319.65 | 0.6801 | 1.7337 | 177.31 | 98.08 |
| `core_fan_ml` | expanding | 72 | 61.378 | 0.583 | 0.736 | 0.778 | 231.89 | 0.7459 | 3.0206 | **153.14** | 73.14 |
| `core_fan_ml` | rolling40 | 56 | 82.627 | 0.536 | 0.732 | 0.821 | 275.21 | 0.7249 | 1.7949 | 185.71 | 101.20 |

### 3.2. Primary `core_fan_conformal` против Task 1 и бенчмарков (expanding)

| пара | n | ΔCRPS | 95 % CI | p (бутстр.) | CRPS бенч. | RMSE веера | RMSE бенч. |
|---|---:|---:|---|---:|---:|---:|---:|
| vs Task 1 `fan_shock_weighted` | 72 | **−52.026** | [−96.868; −18.727] | <0.0001 | — | — | — |
| vs hold | 72 | −12.666 | [−23.964; −1.327] | 0.0300 | 73.264 | 156.10 | 159.78 |
| vs repeat | 72 | −11.277 | [−39.914; 10.872] | 0.3780 | 71.875 | 156.10 | 184.77 |
| vs AR(1) | 72 | **−26.512** | [−38.826; −15.383] | <0.0001 | 87.109 | 156.10 | 162.00 |
| vs `ordered_logit` point | 72 | **−16.276** | [−25.790; −7.487] | <0.0001 | 76.873 | 156.10 | 155.17 |

- Primary лучше Task 1 на **−52.03 CRPS** (CI не содержит 0), лучше точечного `ordered_logit` на **−16.28 CRPS**; RMSE 156.10 vs 155.17 (в пределах 10 %).
- `core_fan_global` — лучший CRPS (60.36), но coverage 0.764; `core_fan_ml` — лучший RMSE (153.14), coverage 0.736.

### 3.3. 2022–2023: regime-варианты

| вариант | 2022–2023 CRPS | coverage 80 | ширина 80 | 2024–2026 coverage 80 |
|---|---:|---:|---:|---:|
| `core_fan_conformal` (primary) | **136.492** | 0.650 | 269.19 | 0.909 |
| `core_fan_conformal_regime` | 170.646 | **0.800** | 748.89 | 0.773 |
| `core_fan_global_regime` | — | — | — | — |

- Regime-вариант **закрывает coverage 2022–2023** (0.800 против 0.650), но ценой CRPS 170.65 (против 136.49) и ширины 748.89 (против 269.19); на 2024–2026 coverage падает до 0.773 (было 0.909), потому что режим `w<0.5` захватывает и часть 2024–2026.
- Общий CRPS regime-варианта 75.35 против 60.60 у primary; **это не выигрыш**, а диагностика: 2022–2023 остаётся открытым по CRPS/ширине.
- `core_fan_global_regime` — хуже (CRPS 64.49, coverage 0.722).
- Вывод: простой regime-split по `w<0.5`/`emergency` слишком грубый; нужен либо более сильный core в 2022, либо отдельная модель дисперсии по волатильности/режиму, а не по одному порогу.

### 3.4. Текущий as-of прогноз (2026-10-23, информационно)

- Core `ordered_logit`: **−63.96 б.п.**; `probs` cut/hold/hike = 0.822 / 0.157 / 0.020.
- Дельты: `inertial` +1.44, `rational` −29.64, `hard` +17.92, `budget` +3.63, `no_market_loans` +6.66 б.п.
- `disp_scale` scale 30.583 (ablation); квантили веера 10/50/90: **−153.57 / −86.83 / +29.18 б.п.**
- `p_t=0.008`, `w_t=1.000`; в `forecast_log` не записывается.

## 4. Вердикт и следующий шаг

**`promote`** для primary `core_fan_conformal`: coverage 80 % **0.806** [0.700; 0.880], CRPS **60.60**, Task 1 ΔCRPS −52.03 (значимо), CRPS/RMSE не хуже `ordered_logit`. Это лучший результат трека по coverage и второй по CRPS (после `core_fan_global` 60.36).

**2022–2023 не закрыт:** regime-вариант поднимает coverage до 0.800, но CRPS 170.65 и ширина 748.89 — слишком дорого; общий CRPS 75.35. Следующий шаг — не менять primary, а сделать отдельную модель дисперсии для 2022–2023:
1. волатильность/режим (курс, ОФЗ, `usdrub_3m`) вместо одного `w<0.5`;
2. более сильный core в 2022 (ML/структурные признаки);
3. проверить `disp_conformal` с **асимметричными** хвостами: 2022 — снижения, а не повышения.

**Оговорки:** primary switched post-hoc; `disp_scale` не подтверждён (γ2<0); conformal нарушает exchangeability; 2014/2015 — в train; Task 2 CPI (#285) — отдельный PR.

## 5. Что не проверено

- PIT/винтажи CPD, ВВП и Минфина; бюджетный лаг консервативный.
- Сценарные дельты `cbr_rule` — reduced-form прокси, не QPM.
- `disp_scale` — 2 признака; regime-split — один порог; conformal — robustness, не гарантия.
- 2014/2015 — в train, не тесте.
- Множественные сравнения: primary один; остальные — robustness, p-value без поправки.
- Многозаседаний путь, CPI-веер (#285), quantile regression/CQR — вне Task 3.

## 6. Ссылки

- [ROADMAP_KEY_RATE.md](../ROADMAP_KEY_RATE.md) §6, фаза 4.
- [KEY_RATE_MODEL.md](../KEY_RATE_MODEL.md) §4, §6.
- [2026-10-20-cbr-scenario-fan.md](2026-10-20-cbr-scenario-fan.md) — Task 1.
- [2026-10-20-cbr-rule.md](2026-10-20-cbr-rule.md).
- [2026-10-19-key-rate-shock-classifier-design.md](../superpowers/specs/2026-10-19-key-rate-shock-classifier-design.md).
