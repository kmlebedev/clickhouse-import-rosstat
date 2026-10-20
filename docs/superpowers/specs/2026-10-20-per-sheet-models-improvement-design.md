# Per-sheet модели ИПЦ: дизайн двух экспериментов (2026-10-20)

**Пункт:** follow-up к [`key-rate/phase-4-cpi-extra-channels`](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/223), issue [#223](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/223), `role:ml`.
**Роль:** ML-инженер. Go-код, DDL, витрины, гранты, запись в ClickHouse и production-промоушен не входят.
**Связанный research:** [2026-10-20-per-sheet-models-improvement.md](../../research/2026-10-20-per-sheet-models-improvement.md).
**Статус:** дизайн до реализации. Код и прогоны — после approval спеки/плана; в этом PR только документы.

---

## 1. Задача

Проверить две вещи на уже имеющихся данных:

1. **A. `cb_comp per_sheet`:** устойчив ли точечный выигрыш per-sheet к окну, какие листы/индикаторы его несут и помогает ли комбинация с `mean`. Модель — live-аппроксимация per-BSU на публичной книге ЦБ.
2. **B. `v3 industry per_sheet`:** является ли отрицательный результат Stage 2 следствием переизбытка коррелированных признаков (96) и помогает ли парсимоничный агрегат (mean/PCA).

Оба эксперимента — **negative-result friendly**: незначимый результат публикуется как результат; production-паритет не заявляется.

## 2. Гипотезы

### A. `cb_comp per_sheet`

- **A1 (устойчивость).** Точечное преимущество per-sheet над `mean` на h2/h3/h6 не устойчиво к rolling origin: на walk-forward значимого выигрыша нет.
- **A2 (атрибуция).** Сигнал несут немногие листы/индикаторы; удаление шумовых листов не ухудшает RMSE, а top-листы устойчивы по origin.
- **A3 (ансамбль).** Простое среднее `mean` + `per_sheet` (0,5/0,5) не хуже лучшей одиночной модели и устойчивее к выбору окна.
- **A4 (горизонт).** h1 требует другого информационного множества: per-sheet-признаки на h1 не помогают (или вредят), на h2–h6 — помогают.

### B. `v3 industry per_sheet`

- **B1 (парсимония).** Mean/PCA-агрегат по листам не хуже 96-признакового набора; если да, отрицательный Stage 2 — следствие размерности/коллинеарности.
- **B2 (инкремент).** Парсимоничный industry-агрегат не даёт значимого выигрыша над `v3_bottomup`; это подтверждает, что отраслевые листы не заменяют aggregate `cbr_survey`.

## 3. Общие правила

- **Данные только существующие:** архив CB_COMP (локальный, gitignored), публичная панель `v_cbr_mp_public_panel`, `v_cbr_mp_bsu_map`, `mp_survey_data.xlsx`, 4 компонента `rosstat_ipc_mes`. Новые импортёры не создаются.
- **As-of не меняется:** A — `mp_available(date, 1)`; B — `available_from_survey` (конец P+1). В A дополнительно проверяется сдвиг +1/+2 как robustness.
- **Walk-forward только расширяющимся окном**; random split и выбор окна по ошибке запрещены.
- **CatBoost — дефолты**, `random_seed=20261018`, `allow_writing_files=False`; никакого тюнинга по тесту.
- **NaN ≠ 0**; CatBoost — нативно; линейные модели (если используются) — медиана train внутри фолда.
- **Полный набор метрик:** RMSE/MAE, DM-HLN, парный блочный бутстрап (блок 3, n=2000, seed 20261018), MDE; при >5 спецификациях — MCS/SPA.
- **Multiple testing:** primary-контрасты — Holm; secondary — p показываются, вердикт по ним не выносится.
- **Паспорт run:** `ML_LOG=1`, MLflow по умолчанию; `run_id`, git SHA, config/sample hash, per-fold predictions, артефакты sha256.
- **Детерминизм:** повторный прогон с тем же конфигом — те же метрики; артефакты не перезаписываются молча.

## 4. Эксперимент A: `per-sheet-attribution`

### 4.1. Данные и окна

| Компонент | Значение |
|---|---|
| Таргет | `TARGET_FULL` из архива CB_COMP (SA ИПЦ без КУ) |
| Группы | 88 компонентных групп; 72 с BSU, 16 без МП |
| Train (минимум) | 2010-03…2018-12 (102 месяца) |
| Test | 2019-01…2024-07 (67 месяцев); 54 normal-месяца (2020-04 и 2022 исключены) |
| Origins | 12 полугодовых: 2019-01, 2019-07, …, 2024-07; expanding window, минимум 102 месяца |
| Purge/embargo | 1 месяц (robustness: 0 и 2) |
| Публичная панель | `v_cbr_mp_public_panel`: 87 кодов, 33 серии, 11 листов × 3 индикатора |
| Карта | `v_cbr_mp_bsu_map`: 87 кодов → 11 листов |
| Веса | СПР с лагом год; покрытие 0,8974; не меняются |

### 4.2. Варианты

| Имя | Признаки | Роль |
|---|---|---|
| `mean` | 6 усреднённых МП-признаков на группу (поведение #190/#222) | baseline |
| `per_sheet` | 6 × S листовых признаков (S=1–4) | главный кандидат |
| `ensemble` | 0,5 × `mean` + 0,5 × `per_sheet` | комбинация, без обучения весов |
| `per_bsu` | 6 × N BSU-кодов (только архив авторов) | историческая ссылка, 4 annual origin |
| `h1_base` | h1 без per-sheet (только базовые + `mean`) | проверка A4 |

`per_bsu` не является production-кандидатом; он нужен только как верхняя граница на архиве.

### 4.3. Walk-forward

- Для каждого origin: train = все normal-месяцы до origin минус embargo; test = origin; прогноз на h=1,2,3,6.
- Primary-набор: `mean`, `per_sheet`, `ensemble` на 12 origin × 4 h × 88 групп.
- `per_bsu` — 4 annual origin (2019-01, 2020-01, 2022-01, 2024-01) как reference.
- Fixed split статьи (2019-01…2024-07) считается вторично для сопоставимости с #222; его числа не заменяют walk-forward.

### 4.4. Атрибуция

- **SHAP (TreeSHAP)** для per_sheet-моделей на fixed split: агрегировать mean |SHAP| по листу и индикатору; top-5 листов на h1/h2/h3/h6.
- **Permutation importance** на test-месяцах (54 строки): 100 повторов, seed; агрегировать по листу/индикатору; отметить нестабильность на малой выборке.
- **Leave-one-sheet-out:** для каждой группы удалить все признаки листа, переобучить на том же train, сравнить RMSE на том же test. 88 групп × 4 h × ≤4 листа.
- **Leave-one-indicator-out:** то же для `ibk`/`costs`/`price_exp`.
- **Стабильность:** top-листы и знак ΔRMSE должны сохраняться минимум на 2/3 origin; иначе вывод — «атрибуция неустойчива».
- Атрибуция — **диагностика**, не причинный вывод; коррелированные признаки делят вклад.

### 4.5. Ансамбль и h1

- `ensemble` — фиксированные 0,5/0,5, без подбора весов; stacking (NNLS/ridge) — только как secondary, обучение строго внутри train.
- `h1_base` — h1 без per-sheet; сравнение с h1 per_sheet и h1 `mean`. Если h1_base не хуже, это подтверждает A4 и даёт практическое правило «h1 — без листовых признаков».

### 4.6. Контрасты и решение

**Primary (Holm, 3 контраста):**

1. `per_sheet` vs `mean` на walk-forward (h2/h3/h6; h1 отдельно).
2. `ensemble` vs `mean` на walk-forward.
3. `h1_base` vs `h1 per_sheet` на walk-forward.

**Secondary:** `per_sheet` vs `per_bsu` (4 annual origin), `ensemble` vs `per_sheet`, фиксированный split, embargo 0/2, `start` 102/120.

**Правило:**

- `promote`: хотя бы один primary-контраст значимо лучше (DM p < 0,05 или CI90 Δ исключает 0), нет значимой деградации, знак устойчив на ≥2/3 origin.
- `iterate`: направление положительное, но значимости/мощности нет; либо значим только secondary.
- `reject`: улучшения нет или значимая деградация.

**MDE:** считается по наблюдаемым фолдам; если MDE > 10% RMSE, вывод формулируется как «мощности нет», а не «эффекта нет».

### 4.7. Стоимость и остановка

- Primary: 12 origin × 3 варианта × 4 h × 88 групп ≈ 12 672 модели; CatBoost на ~100 строках — ориентировочно 20–60 мин.
- Атрибуция: fixed split 352 модели + leave-one-sheet-out ≈ 1 400 моделей; ~10–20 мин.
- `per_bsu` на 4 origin ≈ 1 408 моделей; ~5–10 мин.
- Бюджет: не более 2 полных прогонов A; если runtime > 2 ч, сократить origins до 6 (annual) и пометить `quick=true`; quick-числа не сравнивать с полными.
- Остановка: после A1/A2; A3/A4 запускаются только если A1 не отвергнут.

## 5. Эксперимент B: `per-sheet-parsimony`

### 5.1. Данные и окна

| Компонент | Значение |
|---|---|
| Отраслевая панель | `mp_survey_data.xlsx`, 16 листов × 3 индикатора × 297 месяцев, SA |
| Таргеты | 4 компонента `rosstat_ipc_mes`: общий ИПЦ, продовольствие, непродовольствие, услуги |
| `long` | 2013-10…2026-08, start 48, 107 фолдов |
| `weekly` | 2022-01…2026-08, start 24, 32 фолда |
| As-of | конец P+1; cutoff(M) = 8-е M+1 |
| Mapping | food/nonfood/services/general (pre-registered prior) |

### 5.2. Варианты

| Имя | Признаки | Роль |
|---|---|---|
| `v3_bottomup` | v3 без industry (baseline Stage 1) | baseline |
| `v3_ind_bottomup` | 96 industry-признаков (Stage 2) | полный набор |
| `v3_ind_mean_bottomup` | mean по всем 16 листам на индикатор (3 × 2 = 6) | парсимония-1 |
| `v3_ind_component_mean_bottomup` | mean по mapped-листам компонента + general (6) | парсимония-2 |
| `v3_ind_pca_bottomup` | PCA по 96 признакам, 3 компоненты (fit на train) | парсимония-3 |

PCA: стандартизация по train, 3 компоненты фиксируются до результата (не выбираются по тесту); если объяснённая дисперсия < 70%, добавить 5 компонент как robustness.

### 5.3. Контрасты и решение

**Primary (Holm, 2 контраста):**

1. `v3_ind_mean_bottomup` vs `v3_ind_bottomup` (mean-агрегат vs 96).
2. `v3_ind_component_mean_bottomup` vs `v3_ind_bottomup` (component-mean vs 96).

**Secondary:** `v3_ind_pca_bottomup` vs 96; каждый парсимоничный вариант vs `v3_bottomup`; `long`/`weekly`; MCS по 5 моделям.

**Правило (non-inferiority + parsimony):**

- `promote`: верхняя граница 90% CI для ΔRMSE парсимоничного варианта < +2% RMSE полного набора (заранее заданный margin) и хотя бы один вариант не хуже; модель проще (6 признаков vs 96).
- `iterate`: направление в пользу парсимонии, но CI широкий/пересекает margin.
- `reject`: парсимония хуже margin или значимо хуже.

**MDE:** считается по фолдам; при MDE > 5% вывод — «мощности нет».

### 5.4. Стоимость и остановка

- 5 моделей × 2 выборки × 4 компонента × фолды; ориентировочно 20–40 мин на выборку.
- Бюджет: не более 2 полных прогонов B; `weekly` — secondary, запускается после `long`.
- Остановка: после primary-контрастов; PCA — только если mean-агрегат не хуже.

## 6. Выходы

- `scripts/cb_comp_per_sheet_research.py` — оркестрация A: walk-forward, атрибуция, ансамбль, h1_base; использует `cb_comp_bottomup`/`cb_comp_parity`, не меняет их поведение по умолчанию.
- `scripts/cb_comp_per_sheet_research_test.py` — офлайн-тесты A (синтетика, без ClickHouse/R).
- `scripts/cpi_monthly_industry.py` — расширение B флагом `--feature-set full|mean|component_mean|pca` (default `full`, поведение Stage 2 не меняется).
- `scripts/cpi_monthly_industry_test.py` — тесты новых feature sets, PCA fit только на train, NaN.
- `make cb-comp-per-sheet-research`, `make cb-comp-per-sheet-research-test`, `make cpi-monthly-industry` (с `ARGS="--feature-set …"`).
- `build/cb-comp-per-sheet-research/`, `build/cpi-monthly-industry/<sample>/` — артефакты (gitignored).
- Отчёт `docs/research/YYYY-MM-DD-per-sheet-models-results.md` — после прогонов, не в этом PR.

## 7. Границы и приёмка

- Никаких `CREATE/INSERT/ALTER/GRANT`, Go-кода, DDL, витрин, `forecast_log`, production-промоушена.
- Нет зависимости от 87 внутренних кодов ЦБ; `per_bsu` — только архивная ссылка.
- As-of/PIT и NaN-политика проверены тестами; random split отсутствует.
- Сравнение на общих фолдах; RMSE/MAE, DM-HLN, CI, MDE, Holm; MCS при >5 спец.
- `make all`, `make docs-check` — зелёные; Python-тесты — через `uv run --with`.
- Паспорт run (`ML_LOG=1`) и артефакты sha256; повтор детерминирован.
- Незначимый результат публикуется; production-v3 не запускается без `promote`.

## 8. Риски

- Малая выборка: 54 normal-месяца; MDE может быть > 10%, вывод — «мощности нет».
- Атрибуция на коррелированных признаках нестабильна; интерпретировать как диагностику.
- Публичная панель — 33 серии, не 87; per-sheet — аппроксимация.
- Нет PIT/винтажей; as-of +1 — правило.
- PCA/mean могут потерять редкий сигнал; поэтому non-inferiority margin, а не «победитель любой ценой».
- Стоимость A: 12 672 модели; при превышении бюджета — сокращение origins с пометкой `quick`.
