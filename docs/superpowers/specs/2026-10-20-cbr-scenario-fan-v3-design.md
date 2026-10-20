# Дизайн: Task 4 — channel-complete core + dispersion recalibration

**Пункт:** `key-rate/phase-4-cbr-scenario-fan` — issue [#272](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/272)
**Task 4:** issue [#289](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/289)
**Роль:** `role:ml`; **фаза:** 4
**Зависимость:** #287 (merged), #291 (Task 3.1, revision в `main`)
**Статус:** In Progress. В `forecast_log` не пишется.

---

## 1. Гипотеза и решение

Аудит Task 3 показал: primary `core_fan_conformal` использует 3 канала полностью, 3 частично и 7 не использует. Эксперимент `enet_channel_all` (22 доступных признака shock-панели) дал RMSE **115.32** против 145.05 и CRPS **54.24** против 60.60, но coverage 80 % упал до **0.736** (у primary 0.806), а разница CRPS не значима (p=0.464).

Гипотеза Task 4:

> Если добавить в ядро все доступные каналы `KEY_RATE_CHANNELS.md` (включая savings, OFZ, Brent, BOP, ccyb и текст), а дисперсию пересчитать под channel-core (volatility-scaled conformal), то primary-веер достигнет coverage 80 % в `[0.70; 0.90]` и CRPS значимо лучше `core_fan_conformal` 60.60.

Гипотеза может быть **отвергнута** — это допустимый результат.

## 2. Карта каналов

| Канал §3 | Ряд / источник | Признак(и) | Покрытие / as-of |
|---|---|---|---|
| Текущий рост цен | `cbr_indicators_cpd` (все товары), `rosstat_ipc_mes` | `cpi_saar` | 2002+; конец M+1 |
| Устойчивая (базовая) инфляция | `cbr_indicators_cpd` (базовый) | `pi_saar`, `e3_core`, `e3_headline_adaptive` | 2002+; конец M+1 |
| Инфляционные и ценовые ожидания | `cbr_infl_exp`, `cbr_survey` (`price_expectations_sa`), `cbr_mo_br` | `infl_exp_expected/observed`, `price_expectations`, consensus CPI | 2023+ / 2013+ / волна+6д |
| Рынок труда | `rosstat_labour_force_mes`, `rosstat_salaries_mes` | `unemployment`, `wage_yoy` | 2017+ / 1991+; конец M+1 |
| Кредитование и трансмиссия | `cbr_credit_m2x`, `cbr_ruania`, `cbr_int_rate_loans`, ccyb | `credit_yoy`, `ruania_spread`, `loan_rate_spread`, `credit_gap_broad`, `hh_yoy`, `nfo_rub_yoy` | 2015+/2019+/2014+/ccyb |
| Разрыв выпуска / перегрев | `rosstat_vvp_kvartal`, `cbr_survey` `business_climate_sa` | `gdp_gap`, `ibk_gap` | 2011+/2013+ |
| Бюджетный импульс | `minfin_fed_bud_mes` | `budget_balance`, `fiscal_impulse` | 2011+; конец M+1 |
| Денежная масса | `cbr_m2` | `m2_yoy` | 2001+; конец M+1 |
| Внешние условия и курс | `cbr_currency_usd`, `moex_market` Brent, `v_cbr_bop` | `usdrub_3m`, `brent_3m`, `bop_ca_q` | 2013+/2009+/1994+ |
| Сберегательное поведение | `cbr_households_b_mes` / `v_cbr_macro` | `deposit_yoy`, `deposit_share` | 2018+; конец M+1 |
| Макропруденциальные требования | `cbr_ccyb_indicators` | `credit_gap_broad`, `hh_yoy`, `nfo_rub_yoy` | ccyb (месячно) |
| Рынок KEYRATE/ОФЗ | `cbr_zcyc` / `v_ofz_signal`; KEYRATE | `ofz10_spread`, `ofz2_spread`, `slope_10y_1y`; KEYRATE — **недоступен** (2026+, ряд короче выборки) | OFZ 2003+; daily |
| Текстовый сигнал ЦБ | `v_cbr_mp_summary_channels`, `v_news_flash` | `text_channel_score`, `text_available` | 2024+; published+1д |

**Недоступно:** KEYRATE (история с 2026-09 — меньше выборки); Urals (ряда нет). Text — только 2024+ (missing-индикатор). Всё остальное — used/partial с указанием покрытия.

## 3. Признаки и ядро

- Панель shock-classifier (22 признака: 14 уровней + 8 дельт) + новые:
  - `deposit_yoy` (г/г), `deposit_share = депозиты / (депозиты + потребссуды + ипотека)`;
  - `ofz10_spread`, `ofz2_spread`, `slope_10y_1y` (`v_ofz_signal`);
  - `brent_3m` (лог-изменение за 3 месяца);
  - `bop_ca_q` (счёт текущих операций, квартал; as-of конец квартала +3 мес.);
  - `credit_gap_broad`, `hh_yoy`, `nfo_rub_yoy` (`cbr_ccyb_indicators`; если raw-таблица недоступна — missing);
  - `text_channel_score` (число regex-каналов последнего резюме), `text_available` (2024+).
- Missing-индикаторы групп: `has_expectations`, `has_savings`, `has_ccyb`, `has_text`, `has_bop`.
- Ядро — `ElasticNetCV` (`l1_ratio=[0.1,0.5,0.9]`, `cv=5`, seed 20261020):
  - train-only медианная импутация и стандартизация; NaN не заменяется нулём до импутации;
  - walk-forward 84/68; CV только внутри train;
  - CatBoost не используется (108 решений).
- Никакого выбора признаков по тесту: полный набор фиксируется в спеке, ablations публикуются.

## 4. Дисперсия

- `disp_conformal` — baseline (Task 3).
- **`disp_vol` (primary-кандидат):**
  - `vol_i = mean(z(|usdrub_3m_i|), z(|ofz_slope_i|), z(|brent_3m_i|))` по train (медиана/IQR, клип ±2);
  - `log(|e_i|+1) = γ0 + γ1·vol_i`; `scale_i = exp(γ0+γ1·vol_i)−1`;
  - `q_α = scale_t · quantile(e_i/scale_i, α)`; `scale_t` — по `vol_t` текущего заседания;
  - `scale_t ≤ 0` → fallback `disp_conformal`.
- `disp_calibrated` — robustness: rolling coverage factor по прошлым фолдам (`k = clip(1 + 2·(0.8 − coverage_past), 0.8, 1.5)`), применяется к `disp_conformal`.
- Primary — `channel_fan_vol`; ablations `channel_fan_conformal`, `channel_fan_calibrated`.
- 2022–2023: отдельный эпизод; цель — coverage 80 % без катастрофического роста ширины (в Task 3 regime-split дал 0.800 ценой CRPS 170.65/ширины 748.89).

## 5. Веера и метрики

- `point = channel_core_step + δ_s + q_α(t)`, 5×7 = 35 точек, веса 1/35.
- Coverage 50/80/90 + Wilson CI, CRPS, ширина, Brier/log loss, медиана RMSE/MAE.
- DM-HLN по медиане и блочный бутстрап по CRPS против:
  - `core_fan_conformal` (Task 3 primary, CRPS 60.60);
  - `ordered_logit` point; B1–B3;
  - `channel_fan_conformal` (ablation), `channel_fan_calibrated` (robustness).
- Эпизоды 2022–2023, 2024–2026; 2014/2015 — в train.

## 6. Правило решения

- **`promote`:** coverage 80 % в `[0.70; 0.90]` и Wilson CI включает 0.8; CRPS `channel_fan_vol` значимо ниже `core_fan_conformal` 60.60 (95 % CI разницы ниже 0); RMSE не хуже `ordered_logit` более чем на 10 %.
- **`iterate`:** coverage улучшился, CRPS нет; или наоборот.
- **`reject`:** улучшения нет.
- Primary один (`channel_fan_vol`, expanding); остальные — robustness/Bonferroni.

## 7. Тесты

`scripts/key_rate_scenario_fan_v3_test.py` (offline):

1. **As-of:** savings/BOP/OFZ/Brent/text точка после заседания не участвует; месячный/квартальный/дневной лаг.
2. **Feature groups:** все 11 каналов отображены; `has_*`-индикаторы; KEYRATE помечен unavailable.
3. **Core:** `ElasticNetCV` только train; NaN-политика; детерминизм.
4. **disp_vol:** `scale>0`; высокий `vol` расширяет интервал; fallback при `scale≤0`.
5. **disp_calibrated:** фактор в `[0.8;1.5]`; монотонность.
6. **Веера:** точки/веса; coverage/CRPS; primary vs ablations.
7. **Артефакты/`ML_LOG`:** CSV/JSON детерминированы; payload.

## 8. Приёмка

1. `make key-rate-scenario-fan-v3-test` — офлайн-тесты.
2. `ML_LOG=1 MLFLOW=0 make key-rate-scenario-fan-v3` — 108 заседаний, 84/68, 72/56 фан-фолдов; coverage/CRPS; `v_ml_runs` `status=ok`, `git_dirty=0`; повтор детерминирован.
3. `make all`, `make docs-check` — зелёные.
4. `git diff` без Go/DDL/витрин/грантов/`forecast_log`.
5. Отчёт: таблица «канал → ряд → участие», coverage/CRPS, 2022–2023, «что не проверено».
6. PR — `Part of #272`, `Closes #289`; протокол комментарием.

## 9. Границы и риски

- **PIT/винтажей нет:** savings/ccyb/OFZ/BOP без ревизий; as-of консервативный.
- **Text 2024+** и **KEYRATE 2026+** — короткая история; missing-индикаторы, не «ноль».
- **`cbr_ccyb_indicators` raw** может быть недоступна пользователю скрипта; fallback — missing, пометить в отчёте.
- **108 решений, ~35 признаков:** ElasticNet-регуляризация; риск переобучения, ablations и коэффициенты публикуются.
- **`disp_vol`** — 1 композитный признак; в 2022 может не хватить; `disp_calibrated` — robustness.
- **Сценарные дельты** — reduced-form прокси `cbr_rule`, не QPM.
- **Вне объёма:** `forecast_log`, Go/DDL, CPI Task 2 #285, KEYRATE/Urals, полный QPM.

## 10. Ссылки

- [KEY_RATE_CHANNELS.md](../../KEY_RATE_CHANNELS.md) §3, §8.
- [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.1–4.2, §6.
- [2026-10-20-cbr-scenario-fan-v2.md](../../research/2026-10-20-cbr-scenario-fan-v2.md).
- [2026-10-20-cbr-scenario-fan.md](../../research/2026-10-20-cbr-scenario-fan.md).
- [2026-10-19-key-rate-shock-classifier-design.md](2026-10-19-key-rate-shock-classifier-design.md).
