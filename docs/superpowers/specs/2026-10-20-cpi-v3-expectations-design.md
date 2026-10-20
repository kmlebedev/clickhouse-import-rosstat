# CPI live3 h1/h3/h6 как model-based прокси ожидаемой инфляции

**Пункт:** `key-rate/phase-4-cbr-scenario-fan`, Task 4.1 — issue [#296](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/296)
**Роль:** `role:ml`; **фаза:** 4
**Дата:** 2026-10-20
**Зависит от:** #289 (Task 4, merged)
**Модель-источник:** `scripts/cb_comp_live_compare.py` — live3 (3 компонента ИПЦ + v3-признаки + NNLS)

## 1. Вопрос

Корректно ли использовать прогнозы ИПЦ модели `cb_comp_live_compare` (live3) как дополнительный вход канала «Инфляционные и ценовые ожидания» модели ставки?

**Короткий ответ до эксперимента.** Как **замену** survey-ожиданиям — нет: `cbr_infl_exp` и `price_expectations` измеряют ожидания агентов, а live3 — ожидаемый фактический рост цен. Как **дополнительный model-based прокси** — да, гипотеза допустима, но должна быть проверена walk-forward: признаки коррелированы с survey-ожиданиями и текущей инфляцией, поэтому выигрыш не гарантирован.

## 2. Что именно берём из `cb_comp_live_compare`

`cb_comp_live_compare.py` — диагностическое сравнение live3, per-sheet и CB_COMP на фиксированном train `2010-03…2018-12` и тесте `2019-01…2024-07` (54 normal-месяца). Его **готовые числа** для модели ставки не годятся: фиксированный train — это look-ahead для 2014–2018, а тест обрывается в 2024-07.

Поэтому эксперимент берёт из него **модель**, а не артефакт:

- 3 официальных компонента ИПЦ — `cpi_food`, `cpi_nonfood`, `cpi_services` (`cm.BOTTOMUP_COMPONENTS`);
- признаки — `cpi_monthly_v3.feature_names_v3()` (v2-база + 4 недельных diffusion-признака);
- `fit_component` / `predict` — CatBoost direct на компонент, дефолты, seed 20261018;
- агрегат — `cm.nnls_weights(train)` по трём компонентам;
- таргет — `cb_comp_live_compare.cumulative_targets(rows, h)`: накопленный m/m официального ИПЦ за `M−h+1 … M` (конвенция CB_COMP, не SA-архив).

`live3_sa` (SA-компоненты авторов) и `per_sheet` в этом пункте **не используются**: SA-рядов в live-витринах нет, per-sheet — отдельная модель #252.

## 3. Walk-forward адаптация live3

### 3.1. Origin и горизонт

- Для заседания `d` берётся последний origin-месяц `O` с `cm.cutoff_for(O) ≤ d` (cutoff — 8-е `O+1`).
- Прогноз h-горизонта — накопленный m/m официального ИПЦ за `O+1 … O+h` (h=1/3/6); целевой месяц `M = O+h`.
- Для h=1 это следующий месяц после origin; для h=3/6 — ожидаемый путь вперёд. Это конвенция issue #296, а не CB_COMP `M−(h−1)`: h1 не должен быть уже опубликованным месяцем.
- Origin'ы берутся из feature-вселенной, а не только из target-выборки: для живого заседания нужен месяц `O`, чья метка ещё не опубликована, но признаки уже доступны.

### 3.2. Обучение без look-ahead

- На cutoff `O` наблюдены месяцы ИПЦ до `O−1` включительно (месяц `O` публикуется в конце `O+1`).
- Train-origin'ы `O'` — те, у которых целевой конец `O'+h` уже наблюдён: `O'+h ≤ O−1`, то есть **`O' ≤ O−h−1`**. Это embargo `h` из issue.
- Для каждого train-origin `O'` строка признаков — `rows_by_month[O']`, таргет компонента — `cumulative_targets(rows,h)[O'+h]`.
- Минимум 24 train-origin'а; иначе `None`, `cpi_v3_available=0`.
- Запрещено: использовать признаки `O` в обучении при прогнозе `O`, брать origin с cutoff позже `d`, обучаться на таргете, не наблюдённом к cutoff.

### 3.3. Прогноз и агрегат

Для каждого origin `O` и горизонта `h`:

1. train = `[(rows_by_month[O'], cumulative_targets(rows,h)[O'+h])]` для `O' ≤ O−h−1`;
2. три `fit_component(train, code)`;
3. `predict` на признаках `rows_by_month[O]`;
4. `aggregate = Σ w_code · component_code`, где `w = cm.nnls_weights(train)`;
5. в артефакт — `aggregate` (п.п. накопленного роста).

Feature-only origin (строка с `y={}`) участвует в `predict`, но не в train: его таргет ещё не наблюдён. Feature-вселенная расширяется до последнего `O` с `cutoff_for(O) ≤ as_of`; target-выборка остаётся `2013-10 … 2026-08`.

### 3.4. Артефакт прогнозов

Для каждого заседания — `cpi_v3_h1`, `cpi_v3_h3`, `cpi_v3_h6`, `cpi_v3_available` (1, только если все три не `None`), `cpi_v3_origin` (дата origin — только в diagnostics).

## 4. Интеграция в модель ставки

- Базовая модель — `channel_fan_vol` из `key_rate_scenario_fan_v3.py` (41 признак, primary).
- Дополнительные признаки: `cpi_v3_h1`, `cpi_v3_h3`, `cpi_v3_h6`, `cpi_v3_available`.
- `build_channel_rows(..., extra_by_date=...)` добавляет их к `PanelRow.features`; `run_v3_walk_forward` принимает `extra_by_date`.
- Ablations:
  1. **baseline** — без CPI live3;
  2. **+h1** — только `cpi_v3_h1`;
  3. **+h1/h3/h6** — primary-гипотеза;
  4. **замена survey** — `infl_exp_*`, `price_expectations` и их дельты = `None`, вместо них live3 (проверка, что CPI-прогноз не подменяет ожидания).
- Ядро, дисперсия, веера и метрики — без изменений: `ElasticNetCV`, `disp_vol`, 35 точек, expanding/rolling40.

## 5. Метрики и критерий

- Primary: CRPS `channel_fan_vol` expanding, paired comparison augmented vs baseline.
- Полный набор: coverage 50/80/90, ширина 80, RMSE, MAE, эпизоды 2022–2023/2024–2026.
- DM-HLN и блочный бутстрап (блок 3, n=2000) по разнице CRPS.
- Sanity/ranking CPI-прогнозов на общих origin'ах: `live3_bottomup` (primary), `direct_catboost`, `B1_last` (последнее наблюдённое h-окно `O−h … O−1`), `B3_enet`; RMSE/MAE h1/h3/h6, DM-HLN и бутстрап для пар с общими фолдами. Ranking — диагностика, primary не переключается post hoc.
- **Приёмка:** coverage 80 % в [0.70; 0.90] и Wilson CI включает 0.8; ΔCRPS augmented − baseline ≤ 0, 95 % CI верх ≤ 0; RMSE не хуже baseline ×1.05; ablations и ranking опубликованы.
- Вердикт: `promote` — значимое улучшение; `iterate` — направление есть/нейтрально; `reject` — хуже baseline.

## 6. Вне объёма

- Замена primary-веера в продакшне; `forecast_log`; Go/DDL/витрины/гранты.
- Переобучение `cpi_monthly_v3` или `cb_comp_live_compare` как диагностики.
- SA-вариант live3 и per-sheet; CPI-веер #285.

## 7. Ограничения

- `cb_comp_live_compare` — диагностика с фиксированным train; здесь используется только её **модель**, обёрнутая в walk-forward. Числа исходного отчёта (2019–2024) с этим экспериментом напрямую не сравниваются.
- PIT/винтажей ИПЦ нет: as-of консервативный (cutoff 8-е M+1), ревизии не учтены.
- live3 — 3 компонента, официальный NSA-ИПЦ; SA-таргет и 88 групп CB_COMP не воспроизводятся.
- Для заседаний до первого train-origin (≈2016) CPI-признаки missing; оценка — на общих фолдах отдельно.
- Последний feature-only origin (например, 2026-09 для заседания 2026-10-23) не имеет фактического таргета: h1/h3/h6 для него — live-прогноз, а не измеренная ошибка.
- h3/h6 — прямые CatBoost-модели на 24+ train-origin'ах; early folds могут быть нестабильны.
- 108 решений, 45 признаков после добавления: ElasticNet-регуляризация.
- Множественные сравнения: primary один, ablations — robustness; p-value без поправки.
