# Отраслевые МП-признаки в компонентном nowcast ИПЦ (2026-10-19)

> **Объект:** пункт `key-rate/phase-4-ml-components`, Task 2 (issue [#235](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/235), родитель [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76)).
> **Задача:** проверить, дают ли 16 публичных отраслевых листов `mp_survey_data.xlsx` (IBK, ценовые ожидания, издержки) больше для компонентного nowcast ИПЦ, чем агрегатные признаки `cbr_survey`.
> **Как проверялось:** `scripts/cpi_monthly_industry.py` (цель `make cpi-monthly-industry`), офлайн-тесты `scripts/cpi_monthly_industry_test.py`; отраслевая панель — публичный xlsx, агрегатный МП и таргеты — через `cpi_monthly`/`cpi_monthly_v3`; walk-forward, CatBoost-дефолты, seed 20261018; DM-HLN и блочный бутстрап (блок 3, n=2000).
> **Спека:** [2026-10-19-cpi-industry-mp-features-design.md](../superpowers/specs/2026-10-19-cpi-industry-mp-features-design.md); **план:** [2026-10-19-cpi-industry-mp-features.md](../superpowers/plans/2026-10-19-cpi-industry-mp-features.md); **артефакты** (gitignored): `build/cpi-monthly-industry/{long,weekly}/cpi_monthly_industry_{report.md,result.json}`.

**Статус: `DONE_WITH_CONCERNS`.** Оба полных прогона выполнены; числа взяты из `build/cpi-monthly-industry/{long,weekly}/cpi_monthly_industry_result.json`, не редактировались руками. Concern — Stage 1 `long` паспорт `git_dirty=1`/другой SHA: absolute baseline не воспроизведён, current baseline пересчитан и назван.

---

## 1. Короткий ответ

**Вердикт — `iterate`.** Industry-признаки не дают значимого улучшения ни на одной выборке. На `long` (primary) `v3_ind_bottomup` практически равен `v3_bottomup` (0.642664 против 0.643146; Δ −0.000482, DM p=0.9251), `ind_only_bottomup` лучше `v2_bottomup` на 4,7 % (0.658742 против 0.691019), но незначимо (p=0.2776, CI включает 0, MDE 7,9 %). На `weekly` industry-признаки хуже bottom-up baseline (+6,3…+13,8 %, p=0.31/0.68), direct-модели немного лучше (−1,5/−6,3 %, незначимо). Mapping-all не лучше mapped. Публичные 16 листов в этой per-sheet спецификации не заменяют агрегатный `cbr_survey`; production-импорт и production-v3 на этой спецификации не обоснованы.

## 2. Данные

| Вход | Значение |
|---|---|
| Отраслевая панель | `mp_survey_data.xlsx`, 16 листов × 297 месяцев, 2002-01…2026-09 |
| Блок | SA («Сезонно скорректированные данные»), 3 индикатора: IBK (headline), ценовые ожидания, издержки |
| sha256 | `c5ad8996bda9f21715a05de3bbe98d1511fb6c160b064f1e9c1090eb501c945b` |
| Агрегатный МП | `cbr_survey`, 7 признаков v2 |
| Таргеты | 4 ряда `rosstat_ipc_mes`: общий ИПЦ, продовольствие, непродовольствие, услуги |
| Недельные признаки | 4 признака Stage 1 (`cpi_monthly_v3`) |

Отраслевые листы — **невесовые признаки**: публичного mapping «отрасль → код ИПЦ/вес» без внутренних 87 кодов ОКВЭД2 нет; весовое покрытие по ним не начисляется.

## 3. Признаки и as-of

- 16 листов × 3 индикатора × (уровень `M−1`, изменение `M−1 − M−2`) = **96 SA-признаков**.
- Имена: `ind_<slug>_<ibk|price_exp|costs>_sa` и `…_sa_chg`.
- As-of: точка `P` доступна с конца `P+1` (`cpi_monthly.available_from_survey`); cutoff(M) = 8-е `M+1`; точка `M` не участвует.
- Отсутствующая точка/лист — NaN, не 0; CatBoost — нативно, ElasticNet — медиана train (all-NaN колонка → 0 после стандартизации).
- Тесты: точка `M` не используется; точка с `available_from > cutoff` игнорируется; NaN не заменяется нулём; импутация — медиана train.

## 4. Mapping отрасль → компонент

| Группа | Листы |
|---|---|
| `food` (`cpi_food`) | Сельское хозяйство; Обработка_потребительские; Торговля розничная (47) |
| `nonfood` (`cpi_nonfood`) | Промышленность; Добыча; Обработка; Обработка_инвестиционные; Обработка_промежуточные; Строительство; Торговля авто (45) |
| `services` (`cpi_services`) | ЭЭГП; Водоснабжение; Транспортировка и хранение; Услуги |
| `general` (во все компоненты и в direct) | Торговля (45+46+47); Торговля оптовая (46) |

Это pre-registered prior о канале переноса, а не публичные веса. Robustness-вариант `all` использует все 16 листов для каждого компонента.

## 5. Выборки и модели

| Выборка | Окно | Start | Фолды | Роль |
|---|---|---:|---:|---|
| `long` | 2013-10…2026-08 | 48 | 107 | primary: отраслевая панель полна, разреженности недельных рядов нет |
| `weekly` | 2022-01…2026-08 | 24 | 32 | secondary: сопоставимость со Stage 1 |

Primary-контрасты (Holm): `v3_ind_bottomup` vs `v3_bottomup`; `ind_only_bottomup` vs `v2_bottomup`. Secondary — direct-контрасты, `v3_ind_all_bottomup` vs `v3_ind_bottomup` (чувствительность к mapping), `weekly`, `B3_ind` vs `B3`. Для `long` — сокращённый primary-набор: `v2_bottomup`, `v3_bottomup`, `ind_only_bottomup`, `v3_ind_bottomup`, `B1`; direct-модели, `ind_bottomup` и `v3_ind_all_bottomup` считаются только на `weekly` (как в Stage 1 long).

## 6. Регрессионный гейт Stage 1

| Выборка | Модель | Ожидалось | Получено | Δ | Статус |
|---|---|---:|---:|---:|---|
| `weekly` | `v2_bottomup` | 0.362300 | 0.362300 | −0.000000286 | ok |
| `weekly` | `v3_bottomup` | 0.311487 | 0.311487 | −0.000000444 | ok |
| `long` | `v2_bottomup` | 0.688869 (Stage 1) | 0.691019 | +0.002150 | differs |
| `long` | `v3_bottomup` | 0.642997 (Stage 1) | 0.643146 | +0.000149 | differs |

**Почему `long`-гейт не совпал.** Паспорт Stage 1 `long` `d1d63610-51ce-44d8-81ff-21929194d958` имеет `git_dirty=1` и SHA `1dc167d3…`; паспорт `weekly` `a70314ac-e7bb-4031-8e7f-a9f40268b6df` — SHA `9977fe4e…`. Текущий код воспроизводит `weekly`-паспорт пофолдово (сверка по `v_ml_error_by_fold`), но не `long`: это расхождение **code-state** Stage 1 long, а не рядовая ревизия данных. `long`-прогон выполнен с `--allow-data-revision`; baseline на текущем коде — в таблице, industry-контрасты считаются от него. Абсолютные числа Stage 1 `long` (0.688869 / 0.642997) остаются историческими и для гейта не используются.

## 7. `long` (primary)

Прогон `long`: 155 строк (2013-10…2026-08), start 48, 107 фолдов; паспорт `b7b04843-0fb6-4590-9f02-f8a8b5105eb6`. Сокращённый primary-набор: `v2_bottomup`, `v3_bottomup`, `ind_only_bottomup`, `v3_ind_bottomup`, `B1`.

| Модель | RMSE | MAE |
|---|---:|---:|
| `v3_bottomup` | **0.643146** | 0.236591 |
| `v3_ind_bottomup` | 0.642664 | **0.235619** |
| `ind_only_bottomup` | 0.658742 | 0.260405 |
| `v2_bottomup` | 0.691019 | 0.267211 |
| `B1` | 0.930326 | 0.379346 |

| Пара | n | Δ RMSE | 95 % CI Δ | DM p | Holm p | MDE (ratio) |
|---|---:|---:|---|---:|---:|---:|
| **`v3_ind_bottomup` vs `v3_bottomup`** | 107 | **−0.000482** | [−0.017046; +0.023638] | 0.9251 | 0.9251 | 0.0157 |
| **`ind_only_bottomup` vs `v2_bottomup`** | 107 | **−0.032277** | [−0.061275; +0.020645] | 0.2776 | 0.5551 | 0.0788 |

**Вывод по `long`:** инкремент industry поверх aggregate+weekly нулевой: `v3_ind_bottomup` 0.642664 против `v3_bottomup` 0.643146 (−0,07 %, DM p=0.9251, MDE 1,6 %) — эффект ниже порога обнаружения. Замена агрегатного МП на industry даёт направление в пользу industry: `ind_only_bottomup` 0.658742 против `v2_bottomup` 0.691019 (−4,7 %), но CI включает 0 (p=0.2776, MDE 7,9 %): мощности не хватает, значимого выигрыша нет. `B1` 0.930326 — industry-модели лучше naive, но это не отличает их от aggregate.

## 8. `weekly`

Прогон `weekly`: 56 строк (2022-01…2026-08), start 24, 32 фолда; паспорт `7c55dc2b-44c4-43db-b577-3f6905861e17`.

| Модель | RMSE | MAE |
|---|---:|---:|
| `v3_bottomup` | **0.311487** | **0.243359** |
| `v3_ind_bottomup` | 0.354359 | 0.265200 |
| `v3_ind_all_bottomup` | 0.358332 | 0.259789 |
| `v2_bottomup` | 0.362300 | 0.267082 |
| `v3_ind_direct` | 0.379299 | 0.280124 |
| `v3_direct` | 0.384900 | 0.290792 |
| `ind_only_bottomup` | 0.384958 | 0.291806 |
| `B3_weekly` | 0.390178 | 0.316048 |
| `ind_bottomup` | 0.404266 | 0.294559 |
| `ind_direct` | 0.410322 | 0.313261 |
| `v2_baseline` | 0.437790 | 0.307335 |
| `B3_ind` | 0.452118 | 0.358751 |
| `B3` | 0.472051 | 0.387375 |
| `B1` | 0.489295 | 0.371563 |
| `B2` | 0.873540 | 0.492682 |

| Пара | n | Δ RMSE | 95 % CI Δ | DM p | Holm p | MDE (ratio) |
|---|---:|---:|---|---:|---:|---:|
| **`v3_ind_bottomup` vs `v3_bottomup`** | 32 | **+0.042873** | [−0.015752; +0.121650] | 0.3059 | 0.6117 | 0.0964 |
| **`ind_only_bottomup` vs `v2_bottomup`** | 32 | **+0.022658** | [−0.066263; +0.127670] | 0.6827 | 0.6827 | 0.1329 |
| `v3_ind_direct` vs `v3_direct` | 32 | −0.005601 | [−0.112895; +0.119224] | 0.9281 | — | 0.1521 |
| `ind_direct` vs `v2_baseline` | 32 | −0.027467 | [−0.160436; +0.106233] | 0.6958 | — | 0.1872 |
| `ind_bottomup` vs `v2_bottomup` | 32 | +0.041966 | [−0.047304; +0.149284] | 0.4725 | — | 0.1494 |
| `v3_ind_all_bottomup` vs `v3_ind_bottomup` | 32 | +0.003973 | [−0.024383; +0.030522] | 0.7558 | — | 0.0405 |
| `B3_ind` vs `B3` | 32 | −0.019934 | [−0.085584; +0.042617] | 0.5454 | — | 0.0881 |

**Вывод по `weekly`:** industry-признаки не улучшают bottom-up: `v3_ind_bottomup` хуже `v3_bottomup` на +13,8 % (DM p=0.3059), `ind_only_bottomup` хуже `v2_bottomup` на +6,3 % (p=0.6827); оба CI включают 0, поэтому это «направление против», а не значимая деградация. Direct-модели немного лучше (−1,5 % и −6,3 %), но тоже незначимо. `all`-вариант не лучше mapped (+1,1 %, p=0.7558): вывод не держится на prior. Гейт Stage 1 пройден с точностью до 1e-6.

## 9. Вердикт

**`iterate`.** Ни один primary-контраст не значим: на `long` инкремент industry поверх aggregate+weekly равен нулю (Δ −0.000482, p=0.9251), замена aggregate на industry даёт −4,7 %, но CI включает 0 (p=0.2776, MDE 7,9 %); на `weekly` industry-признаки хуже bottom-up baseline (+6,3…+13,8 %). Direct-модели и `B3_ind` на `weekly` немного лучше, но незначимо; `all`-вариант не лучше mapped.

Следствия:

- Production-v3 на per-sheet industry-спецификации не запускается; Task 6 (импорт 16 листов) не получает обоснования от этого эксперимента.
- Возможный follow-up — более парсимоничный агрегат (среднее по отраслям, как `mean` в #221) и/или component-level отраслевой индекс; это отдельная гипотеза, а не проверенный здесь результат.
- Negative result публикуется: публичные 16 листов не заменяют агрегатный `cbr_survey` в компонентном nowcast ИПЦ на h1.
- **Оговорка по гейту:** absolute Stage 1 `long` числа не воспроизводятся текущим кодом (паспорт `d1d63610…` — `git_dirty=1`, SHA `1dc167d3…`); current baseline `long` — 0.691019 / 0.643146, industry-контрасты считаются от него. `weekly`-паспорт `a70314ac…` воспроизведён пофолдово.

## 10. Что не проверено

- Mapping отрасль → компонент — pre-registered prior, не публичные веса; robustness-вариант `all` назван отдельно.
- Используется SA-блок; raw-блок в этом пункте не проверялся.
- Нет винтажного архива и фактических дат публикации: as-of — правило (конец P+1), а не PIT-слой.
- CB_COMP напрямую не сравнивается; h2/h3/h6 не считаются.
- Production-импорт 16 листов в ClickHouse — отдельный Go-пункт Task 6.
- Вывод `reject`/`iterate` относится к pre-registered per-sheet спецификации; более парсимоничный агрегат (среднее по отраслям, как `mean` в #221) — возможный follow-up, а не проверенная здесь гипотеза.

## 11. Воспроизводимость

```bash
make cpi-monthly-industry-test
ML_LOG=1 make cpi-monthly-industry ARGS="--sample long --start 48"
ML_LOG=1 make cpi-monthly-industry ARGS="--sample weekly --start 24"
```

Скрипт читает ClickHouse и публичный xlsx; в ClickHouse данные не пишет. `ML_LOG=1` пишет только паспорт/предсказания в `ml_runs`/`ml_predictions`. Паспорта: `7c55dc2b-44c4-43db-b577-3f6905861e17` (`weekly`), `b7b04843-0fb6-4590-9f02-f8a8b5105eb6` (`long`).
