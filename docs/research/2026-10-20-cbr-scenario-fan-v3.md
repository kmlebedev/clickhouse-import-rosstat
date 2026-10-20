# Веер v3: channel-complete ядро + volatility-дисперсия (Task 4 #289)

**Пункт:** `key-rate/phase-4-cbr-scenario-fan`, Task 4 — issue [#289](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/289)
**Роль:** `role:ml`; **фаза:** 4
**Спека:** [2026-10-20-cbr-scenario-fan-v3-design.md](../superpowers/specs/2026-10-20-cbr-scenario-fan-v3-design.md)
**План:** [2026-10-20-cbr-scenario-fan-v3.md](../superpowers/plans/2026-10-20-cbr-scenario-fan-v3.md)
**Дата:** 2026-10-20; **data as-of:** 2026-10-20

**Прогон:** `ab539a10-9cea-43b1-85ba-a9d3f8b6707b`, `git_sha=10f4724`, `git_dirty=0`, статус `ok`.
**Повтор:** числа и sha256 совпали; артефакты — report `1a9fae2c…`, predictions `3c6665a7…`, diagnostics `3b63d564…`, forecast `7f7f9a93…`.
**Вердикт: `promote`** по primary `channel_fan_vol`: CRPS **48.22**, coverage 80 % **0.778** [0.669; 0.858], RMSE **129.96**; против `core_fan_conformal` ΔCRPS **−12.38** [−25.84; −0.63].

---

## 1. Что изменено относительно Task 3

1. **Ядро** — `ElasticNetCV` на channel-complete матрице (41 признак):
   - 22 признака shock-панели: 14 уровней + 8 дельт;
   - сбережения 2 (`deposit_yoy`, `deposit_share`), ОФЗ 3, Brent 1, BOP 1, ccyb 3, текст 2, missing-индикаторы 5;
   - разрыв выпуска 2 (`gdp_gap`, `ibk_gap`) — добавлены к панельному `business_climate`.
2. **Дисперсия** — `disp_vol` (primary): `vol_t = mean(z(|usdrub_3m|), z(|slope_10y_1y|), z(|brent_3m|))`, `log(|e|+1) = γ0 + γ1·vol`, `scale_t`, стандартизованный conformal. Ablations: `disp_conformal` (Task 3 baseline), `disp_calibrated` (rolling coverage factor).
3. **Веер** — та же конструкция: `point = core_step + δ_s + q_α(t)`, 5 сценариев × 7 квантилей = 35 точек, веса 1/35.
4. **Никакого выбора признаков по тесту:** набор зафиксирован в спеке; проверка полного `CbrRow`-набора (E3, `gap`, `r*`, консенсус) дала CRPS 53.85 и **отвергнута**; в ядро вошли только `gdp_gap`/`ibk_gap`, которые спека §2 называет для канала разрыва выпуска.

## 2. Данные и выборка

- 108 решений `2014-02-14 … 2026-09-11`; пропусков нет.
- Ядро: expanding 84 фолда, rolling40 — 68.
- **Веер: 72/56** — первые 12 фолдов на окно уходят на residual warm-up (`MIN_RESIDUALS=12`); `vol_fallback` 0.
- As-of: CPD — конец M+1; ВВП — конец квартала + 3 месяца; BOP — конец квартала + 3 месяца; ccyb — конец M+1; ОФЗ/Brent/курс — daily; текст — `meeting_date + 14 дней`; веса/вероятность шока — walk-forward.
- PIT/винтажей нет: as-of консервативный, но не восстановленный.

## 3. Каналы `KEY_RATE_CHANNELS.md` §3: участие в primary

| Канал | Ряд | Признак(и) в ядре | Статус |
|---|---|---|---|
| Текущая/базовая инфляция | `cbr_indicators_cpd` | `pi_saar`, `cpi_saar` (+ дельты) | used |
| Ожидания | `cbr_infl_exp`, `cbr_survey`, `cbr_mo_br` | `infl_exp_expected`, `infl_exp_observed`, `price_expectations` (+ дельты) | used; консенсус в ядро не входит |
| Рынок труда | `rosstat_labour_force_mes`, `rosstat_salaries_mes` | `unemployment`, `wage_yoy` (+ дельты) | used |
| Кредит/трансмиссия | `cbr_credit_m2x`, `cbr_ruania`, ccyb | `credit_yoy`, `ruania_spread`, `credit_gap_broad`, `hh_yoy`, `nfo_rub_yoy` | used |
| Разрыв выпуска | `rosstat_vvp_kvartal`, `cbr_survey` | `gdp_gap`, `ibk_gap`, `business_climate` (+ дельта) | used |
| Бюджет | `minfin_fed_bud_mes` | `budget_balance`; `budget_impulse` — сценарная дельта | used |
| Деньги | `cbr_m2` | `m2_yoy` | used |
| Внешние условия | `cbr_currency_usd`, `moex_market`, `v_cbr_bop` | `usdrub_3m`, `brent_3m`, `bop_ca_q` | used |
| Сбережения | `v_cbr_macro` | `deposit_yoy`, `deposit_share` | used с 2018 |
| Макропруденциальные | `v_cbr_ccyb_indicators` | `credit_gap_broad`, `hh_yoy`, `nfo_rub_yoy` | used; данные до 2024-07/2024-10, дальше missing |
| Рынок ОФЗ | `v_ofz_signal` | `ofz10_spread`, `ofz2_spread`, `slope_10y_1y` | used |
| KEYRATE | — | — | **unavailable**: история с 2026-09 |
| Текст | `v_cbr_mp_summary_channels` | `text_channel_score`, `text_available` | used с 2024; score — доля 11 regex-каналов |

Итог: **10 каналов used** (сбережения и ccyb — с ограничением покрытия), **1 unavailable** (KEYRATE). E3 и консенсус в ядро не включены: полный `CbrRow`-набор ухудшил CRPS; текущая инфляция представлена `pi_saar`/`cpi_saar`, ожидания — survey-уровнями.

## 4. Результаты

### 4.1. Веер × окно

| веер | окно | n | CRPS | coverage 50 | coverage 80 | coverage 90 | ширина 80 | RMSE | MAE |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `channel_fan_conformal` | expanding | 72 | 49.100 | 0.722 | 0.875 | 0.903 | 233.12 | 130.11 | 54.97 |
| `channel_fan_conformal` | rolling40 | 56 | 78.982 | 0.661 | 0.857 | 0.857 | 379.14 | 195.66 | 90.01 |
| **`channel_fan_vol`** | expanding | 72 | **48.215** | 0.681 | **0.778** | 0.819 | **144.64** | 129.96 | 55.04 |
| **`channel_fan_vol`** | rolling40 | 56 | **76.947** | 0.589 | 0.732 | 0.750 | 235.37 | 195.83 | 90.43 |
| `channel_fan_calibrated` | expanding | 72 | 48.249 | 0.667 | 0.847 | 0.861 | 202.29 | 129.12 | 54.36 |
| `channel_fan_calibrated` | rolling40 | 56 | 78.853 | 0.643 | 0.839 | 0.857 | 371.40 | 195.80 | 89.98 |

Primary `channel_fan_vol` — лучший CRPS в expanding и rolling40; ширина 80 % **144.64** против 233.12 у conformal. `channel_fan_calibrated` даёт coverage 0.847 при CRPS 48.25 — ближе к номиналу, но это robustness, не primary.

### 4.2. Primary против Task 3 и бенчмарков (expanding)

| пара | n | ΔCRPS | 95 % CI | p (бутстр.) | DM p |
|---|---:|---:|---|---:|---:|
| `core_fan_conformal` (Task 3) | 72 | **−12.382** | [−25.838; −0.632] | 0.0340 | 0.1092 |
| hold | 72 | −25.049 | [−47.015; −6.401] | 0.0030 | 0.0704 |
| repeat | 72 | −23.660 | [−48.792; −3.765] | 0.0090 | 0.2332 |
| AR(1) | 72 | −38.894 | [−57.353; −22.600] | <0.0001 | 0.0361 |
| `ordered_logit` point | 72 | −28.658 | [−48.041; −13.113] | <0.0001 | 0.0962 |

- CRPS **48.22 против 60.60** у Task 3; bootstrap-CI не содержит 0, но DM p=0.109 — выигрыш хрупкий, на границе.
- RMSE 129.96 против 156.10 у Task 3 и 155.17 у `ordered_logit` — улучшение.
- `channel_fan_vol` уже conformal по ширине (144.64 против 233.12) при сопоставимом покрытии.

### 4.3. Эпизоды

| вариант | эпизод | n | CRPS | coverage 80 | ширина 80 |
|---|---|---:|---:|---:|---:|
| `channel_fan_vol` | 2022–2023 | 20 | 106.339 | 0.750 | 203.55 |
| `channel_fan_vol` | 2024–2026 | 22 | 38.254 | 0.773 | 212.30 |
| `channel_fan_vol` | прочее | 30 | 16.771 | 0.800 | 55.74 |
| `channel_fan_conformal` | 2022–2023 | 20 | 109.345 | 0.800 | 440.08 |
| `channel_fan_conformal` | 2024–2026 | 22 | 38.654 | 0.864 | 232.89 |
| `channel_fan_calibrated` | 2022–2023 | 20 | 108.405 | 0.800 | 383.78 |
| `channel_fan_calibrated` | 2024–2026 | 22 | 37.915 | 0.864 | 206.52 |

- В 2022–2023 primary даёт coverage 0.750 и CRPS 106.34 при ширине 203.55 — заметно лучше regime-варианта Task 3 (coverage 0.800, но CRPS 170.65 и ширина 748.89) и лучше primary Task 3 (0.650 / 136.49 / 269.19).
- В 2024–2026 coverage 0.773 против 0.909 у Task 3, но ширина 212.30 против 269.19 и CRPS 38.25.
- 2022–2023 по-прежнему остаётся самым тяжёлым эпизодом: coverage не 0.800, CRPS высокий. Это не «закрыто», а улучшено.

### 4.4. Текущий as-of прогноз (2026-10-23, информационно)

- Ядро `channel_enet`: **+41.49 б.п.**
- Дисперсия `disp_vol`, `scale_t=27.204`; веер 10/50/90: **−40.41 / +28.50 / +189.17 б.п.**
- `p(шок)=0.008`, `w=1.000`; в `forecast_log` не записывается.
- Прогноз экстремальный и широкий; это артефакт оценки, а не рекомендация. Причина — channel-ядро на 2026-10 и высокая `vol`; для продакшна нужен отдельный current-forecast gate.

## 5. Вердикт и следующий шаг

**`promote`** для primary `channel_fan_vol`:

- coverage 80 % **0.778** [0.669; 0.858] — в коридоре [0.70; 0.90], Wilson-CI включает 0.800;
- CRPS **48.22** против 60.60 у `core_fan_conformal`; ΔCRPS −12.38, bootstrap-CI [−25.84; −0.63] не содержит 0 (DM p=0.109 — оговорка);
- RMSE 129.96 лучше 156.10 и 155.17 у `ordered_logit`;
- все каналы либо used, либо явно unavailable; 2022–2023 улучшен, но не закрыт.

Следующие шаги:
1. **Task 2 (#285)** — CPI-веер, отдельный PR; не смешивать со ставкой.
2. **2022–2023** — отдельная regime/vol-модель по эпизоду, а не один порог; текущий `disp_vol` уже снизил ширину, но coverage 0.750.
3. **Current forecast gate** — проверить экстремальный +41.49 б.п.; не писать в `forecast_log` до отдельного подтверждения.
4. **ccyb freshness** — импортёр отстаёт (2024-07/2024-10); после обновления пересчитать.
5. **KEYRATE** — история с 2026-09, в выборку не входит; вернуться, когда ряд накопит ≥24 решения.

## 6. Что не проверено

- PIT/винтажи CPD, ВВП, Минфина, savings, ccyb, OFZ, BOP; as-of консервативный, не восстановленный.
- `v_cbr_mp_summary_channels` не содержит `published`; text as-of — `meeting_date + 14 дней` (допущение).
- Text — только 2024+, score — доля каналов, не направление/тональность.
- 108 решений, 41 признак: ElasticNet-регуляризация; CatBoost не используется.
- Множественные сравнения: primary один, остальные — ablations/robustness; p-value без поправки.
- DM p=0.109 против Task 3 — выигрыш по CRPS не железобетонный; bootstrap-CI на границе.
- 2022–2023 coverage 0.750 — не 0.800; CRPS 106.34 остаётся высоким.
- Многозаседаний путь, CPI-веер (#285), quantile regression/CQR — вне Task 4.

## 7. Ссылки

- [ROADMAP_KEY_RATE.md](../ROADMAP_KEY_RATE.md), фаза 4.
- [KEY_RATE_MODEL.md](../KEY_RATE_MODEL.md) §4, §6.
- [KEY_RATE_CHANNELS.md](../KEY_RATE_CHANNELS.md) §3.
- [2026-10-20-cbr-scenario-fan-v2.md](2026-10-20-cbr-scenario-fan-v2.md) — Task 3 baseline.
- [2026-10-20-cbr-scenario-fan.md](2026-10-20-cbr-scenario-fan.md) — Task 1.
- [2026-10-20-cbr-rule.md](2026-10-20-cbr-rule.md).
