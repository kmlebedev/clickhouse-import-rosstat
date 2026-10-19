# Компонентный nowcast ИПЦ на публичных данных: Task 4 — модель v3

**Дата:** 19 октября 2026 г.
**Пункт:** `key-rate/phase-4-ml-components`, Task 4 (issue [#237](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/237)); родитель [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76).
**Роль:** ML-инженер.
**Зависимости:** Task 2 [#235](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/235) и Task 3 [#236](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/236) закрыты; Task 6 [#239](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/239) merged (PR [#245](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/245)); Task 5 [#238](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/238) остаётся открытым и в этот пункт не входит.

---

## 1. Задача

Собрать и измерить production-v3 на публичных данных: 4 официальных компонента ИПЦ, direct и bottom-up, недельные diffusion-признаки + отраслевые МП + макро-контроли; без 87 внутренних кодов ОКВЭД2.

Ключевое отличие от Task 2: отраслевые МП читаются **сразу из ClickHouse** — 48 SA-рядов `v_cbr_survey`, импортированных в #239, а не из `mp_survey_data.xlsx`. Официальный прогон XLSX не читает. Опциональный `--verify-xlsx` — разовая сверка источника.

---

## 2. Данные

### 2.1 Таргеты

- 4 ряда `rosstat_ipc_mes`: общий ИПЦ, продовольствие, непродовольствие, услуги.
- `y = percent − 100`; единицы h1 — п.п. м/м.
- Cutoff(M) — 8-е число M+1; метка M доступна не раньше конца M+1 и в признаки не попадает.
- h2/h3/h6 — накопленный прирост за h месяцев: `100·(Π_{k=0..h−1}(1+y_{O+k}/100)−1)`, origin `O = M−(h−1)`; единицы — накопленный % за h месяцев. RMSE сравнивается только внутри горизонта.

### 2.2 Признаки

**v2-база + МП (13 колонок):** `cm.feature_names(with_mp=True)` — `y_lag1`, `y_lag2`, `usd_mm`, `key_rate`, `fao_mm`, 7 признаков `cbr_survey`, `month`. Берутся из `cmv3.build_sample_v3`, чтобы совпасть со Stage 1 пофолдово.

**Weekly (4):** `wk_share_rising`, `wk_median`, `wk_median4`, `wk_gasoline4`; 149 W-рядов `v_rosstat_macro` (`source='rosstat'`, `frequency='W'`), агрегат исключён; as-of `date + 2`; до 4 последних доступных недель; NaN допускается.

**Industry (96):** 16 листов × 3 индикатора (`ibk`, `price_exp`, `costs`) × (уровень `M−1`, изменение `M−1 − M−2`) = 96 SA-признаков. Имена — `ind_<sheet>_<indicator>_sa` и `…_sa_chg` (предрегистрация Task 2). Mapping компонент — из `cpi_monthly_industry`: food/nonfood/services + general; robustness `all` — все 16 листов.

**Источник industry — #239:**
- `v_cbr_survey` (`source='cbr_survey'`), 48 SA-рядов: 16 листов × `industry_<slug>_{ibk|price_expectations|cost_change}_sa`.
- CH-slug → лист Task 2: `total→Промышленность`, `mining→Добыча`, `manufacturing→Обработка`, `manufacturing_consumer→Обработка_потребительские`, `manufacturing_investment→Обработка_инвестиционные`, `manufacturing_intermediate→Обработка_промежуточные`, `electricity→ЭЭГП`, `water→Водоснабжение`, `agriculture→Сельское хозяйство`, `construction→Строительство`, `trade→Торговля (45+46+47)`, `trade_auto→Торговля авто (45)`, `trade_wholesale→Торговля оптовая (46)`, `trade_retail→Торговля розничная (47)`, `transport→Транспортировка и хранение`, `services→Услуги`.
- CH-индикатор → ключ Task 2: `ibk→ibk`, `price_expectations→price_exp`, `cost_change→costs`.
- Проверки: 48/48 рядов, по 297 точек, 14 256 всего, `2002-01-01…2026-09-01`, уникальные даты, нет NULL/NaN. Отсутствие ряда — ошибка.
- As-of: `available_from_survey(P) = конец P+1`; на cutoff(M) доступны M−1 и M−2; точка M не участвует.
- Сверка с публичным XLSX (sha256 `c5ad8996bda9f21715a05de3bbe98d1511fb6c160b064f1e9c1090eb501c945b`): n=14 256, 48 рядов, sum 263310.229142741, sum_sq 7312992.601352278, min −51.0462615093382, max 75.213628586654; point-by-point max abs diff = 0. Сверка — один раз, не часть модели.

### 2.3 Task 3 panel

Не используется: гейт покрытия весов **39.299 % < 50 %**; полный 88-групповой bottom-up не заявляется. Bottom-up строится на официальных компонентах `rosstat_ipc_mes` (3 компонента + NNLS), как в Stage 1/Task 2.

---

## 3. As-of/PIT

- Weekly: точка участвует, только если `date + 2 ≤ cutoff`.
- Industry: точка `P` участвует, только если `available_from_survey(P) ≤ cutoff`; уровень — строго `M−1`, изменение — `M−1 − M−2`.
- Месячная метка: `available_from_mes(month_end) = конец M+1`; для cutoff(M) собственная метка M недоступна.
- USD: `date + 1`; ключевая ставка: день вступления; FAO: `date + 6`.
- NaN ≠ 0; missing industry point → NaN, не 0; строка выборки не выбрасывается.

---

## 4. Выборки и окна (предрегистрация)

| Окно | Период | Start | Embargo | Горизонты | Модели |
|---|---|---:|---:|---|---|
| `weekly` primary | 2022-01…2026-08 | 24 | 0 | h1, h2, h3, h6 | полный набор (15) |
| `weekly` robustness | 2022-01…2026-08 | 24 | 1 | h1 | сокращённый (9) |
| `long` основной гейт | 2013-10…2026-08 | 48 | 0 | h1 | сокращённый (6) |
| `long` robustness окна | 2013-10…2026-08 | 24 | 0 | h1 | минимальный (4) |

`long` start 48 — воспроизведение Task 2 (107 фолдов); start 24 — проверка устойчивости к выбору стартового окна по issue #237. h2/h3/h6 на `long` не считаются: это зафиксировано до результатов.

---

## 5. Горизонты и purge

- Для горизонта h: `rows_h` — первые `len(rows) − h + 1` origin-строк; `y` заменяется на накопленные таргеты h по всем 4 кодам; признаки — как у origin-месяца.
- `start_eff = start + h − 1`; `embargo_eff = embargo + h − 1`.
- `cm.walk_forward(rows_h, models, start_eff, embargo_eff)`.
- Purge-инвариант: train-origin `j` не имеет `j + h − 1 ≥ i`, где `i` — test-origin. Для h1 — ровно Stage 1.
- Общие фолды: все модели одного окна используют один `rows_h`, `start_eff`, `embargo_eff`; пары — по пересечению месяцев.

---

## 6. Модели

### 6.1 Полный набор `weekly` h1

`v2_baseline` (`cm.fit_catboost_direct_mp`), `v2_bottomup` (`cm.fit_catboost_bottomup_mp`), `v3_direct` (`cmv3.fit_v3_direct`), `v3_bottomup` (`cmv3.fit_v3_bottomup`), `ind_direct`, `ind_bottomup`, `ind_only_bottomup`, `v3_ind_direct`, `v3_ind_bottomup`, `v3_ind_all_bottomup` (все из `ci`), `B1` (`cm.predict_last`), `B2` (`cm.fit_ar1`), `B3` (`cm.fit_enet_mp`), `B3_weekly` (`cmv3.fit_b3_weekly`), `B3_ind` (`ci.fit_b3_ind`).

CatBoost — дефолты `cpi_monthly`/Task 2, `random_seed=20261018`, `month` категориальным; никакого тюнинга по тесту. NaN — CatBoost нативно; ElasticNet — медиана train внутри фолда.

### 6.2 Сокращённые наборы

| Окно | Модели |
|---|---|
| `weekly` h2/h3/h6 | `v2_baseline`, `v2_bottomup`, `v3_direct`, `v3_bottomup`, `v3_ind_direct`, `v3_ind_bottomup`, `B1`, `B3`, `B3_weekly` |
| `weekly` h1, embargo 1 | те же 9 |
| `long` h1, start 48 | `v2_baseline`, `v2_bottomup`, `v3_bottomup`, `v3_ind_bottomup`, `v3_ind_direct`, `B1` |
| `long` h1, start 24 | `v2_bottomup`, `v3_bottomup`, `v3_ind_bottomup`, `B1` |

### 6.3 Bottom-up

3 компонентных CatBoost (`cpi_food`, `cpi_nonfood`, `cpi_services`) + NNLS-веса обучающего окна; агрегат `Σw·ŷ`. Веса нормируются к 1; вырожденное окно — пропуск фолда. Публичные фиксированные веса не используются в основной спецификации; это ограничение названо в отчёте.

---

## 7. Метрики и решение

- RMSE/MAE; DM-HLN с `horizon=h`; блочный бутстрап разницы RMSE (блок 3, n=2000, seed 20261018); MDE ratio.
- Holm по primary-семейству на `weekly` h1: 2 кандидата (`v3_ind_bottomup`, `v3_bottomup`) × 4 бенчмарка (`v2_bottomup`, `B1`, `B2`, `B3_weekly`) = 8 тестов.
- Диагностика: last 12 фолдов, ex-2022; выбросы/2022 остаются в основной метрике, исключений нет.
- «Значимое улучшение»: Δ RMSE < 0 и (Holm p < 0.05 или 95 % CI Δ целиком ниже 0).
- «Не хуже» на h2/h3: не (Δ > 0 и 95 % CI Δ целиком выше 0 и DM p < 0.05).

### 7.1 Decision tree (до результатов)

- A = `v3_ind_bottomup` — предпочтительный кандидат; B = `v3_bottomup` — fallback.
- `promote`: A (или B, если A не проходит) значимо лучше всех четырёх бенчмарков на `weekly` h1 **и** не хуже `v2_bottomup` на h2/h3.
- `reject`: предпочтительный кандидат значимо хуже `v2_bottomup` на h1 или значимо хуже на h2/h3; либо оба кандидата значимо хуже на h1.
- `iterate`: остальное, включая недостаток мощности и отсутствие h2/h3-гейта.
- Industry-инкремент `v3_ind_bottomup` vs `v3_bottomup` — отдельный secondary-контраст; его вывод (`iterate`/`reject`) называется отдельно.
- Незначимый результат публикуется как результат.

---

## 8. Гейты

1. **Stage 1 gate** (h1, допуск 1e-6): weekly `v2_bottomup` 0.362300, `v3_bottomup` 0.311487; long current-code `v2_bottomup` 0.691019, `v3_bottomup` 0.643146. Исторические long 0.688869/0.642997 из «грязного» паспорта не используются.
2. **Task 2 gate** (h1, допуск 1e-6): weekly `v3_ind_bottomup` 0.354359, `ind_only_bottomup` 0.384958; long `v3_ind_bottomup` 0.642664.
3. **CH↔XLSX gate**: п. 2.2; point-by-point max abs diff = 0.
4. **As-of/PIT**: п. 3; мутант-тесты.
5. **Purge**: п. 5; тест на синтетике.
6. **Данные**: п. 2.2; 48/48, 297 точек, нет NULL/дублей.

При расхождении гейта официальный прогон останавливается; `--allow-gate-diff` — только диагностика.

---

## 9. Артефакты и приёмка

- `scripts/cpi_monthly_v3_public.py` + `scripts/cpi_monthly_v3_public_test.py`; цели `make cpi-monthly-v3-public` / `-test`.
- Отчёт `docs/research/2026-10-19-cpi-v3-public.md` и JSON с числами, гейтами, run_id, sha256.
- `ML_LOG=1`: паспорт run на каждое окно; primary — `weekly` h1; fold-предсказания с `window=h1/h2/h3/h6`.
- `make cpi-monthly-v3-public-test`, `make all`, `make docs-check` — зелёные.
- README/DEVELOPMENT/ARCHITECTURE и `docs/ROADMAP_KEY_RATE.md` — тем же PR.
- PR: `Closes #237`; не закрывать #76/#238; протокол — комментарием.

---

## 10. Ограничения и что не проверено

- Task 3 panel не используется (39.299 % < 50 %); полный 88-групповой bottom-up не заявляется.
- CB_COMP напрямую не сравнивается — Task 5 (#238).
- `long` h2/h3/h6 не считаются; `long` h1 start 24 — только 4 модели.
- Публичные фиксированные веса не используются; NNLS — предрегистрированный вариант.
- Винтажей/PIT-слоя нет: as-of — правило cutoff, а не фактические даты публикаций.
- Используется SA-блок industry; raw-блок не проверяется.
- 87 внутренних кодов ОКВЭД2 отсутствуют по построению: читаются только `industry_*` из `v_cbr_survey` и публичные weekly-ряды.
