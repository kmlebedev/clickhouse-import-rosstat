# Архив CB_COMP (Кошкин–Крылов, 2025): код, данные и прогнозы (2026-10-18)

> **Объект:** figshare DOI [10.6084/m9.figshare.27990260.v1](https://doi.org/10.6084/m9.figshare.27990260.v1), файл `BusinessSurveyCBRCpiForecastingSourceCode.7z` (id `51053942`, 438 628 189 байт, md5 `797f00518c3a0fe6d4cd2c2ad22aa70a`). Первоисточник — А. В. Кошкин, Д. В. Крылов. «Мониторинг предприятий Банка России как фактор повышения точности прогноза инфляции» // Вестник СПбГУ. Экономика. 2025. Т. 41, вып. 3, с. 364–393.
> **Задача:** source report для пункта `key-rate/phase-4-ml-cb-comp-parity` (issue #190): разобрать архив, найти таргет «SA ИПЦ без КУ, накопленный прирост», прогнозы CB_COMP на train 2009-01…2018-12 / test 2019-01…2024-07, оценить воспроизводимость. Parity-модель не реализовывалась.
> **Как проверялось:** архив скачан и распакован; код R прочитан; `data_panel_bi_sa.fst`, `model_stats_bind.fst` и RDS прочитаны в R 4.6.1; Excel/CSV разобраны pandas/openpyxl; RMSE пересчитан из per-period прогнозов и сверен с `model_errors_and_dm_tests.xlsx` и таблицами статьи. MCP JetBrains/ClickHouse в сессии доступны; SQL к ClickHouse не требовался.
> **Артефакты:** архив и распаковка — в `build/cb-comp/` (gitignored, в коммит не входят). Сам архив в репозиторий не добавлялся.

**Статус: `DONE_WITH_CONCERNS`.** Source report готов, ключевой вывод положительный: **таргет и per-period прогнозы CB_COMP в архиве есть**, тест 2019-01…2024-07 воспроизводится по уже сохранённым прогнозам без переобучения. Concerns: публичный архив — урезанная версия, полный retrain в нашем окружении не выполнялся, есть encoding/package-риски и несогласованность части вспомогательных CSV.

---

## 1. Короткий ответ

1. **Язык и структура.** Основной код CB_COMP — R-пакет `StfCpiR` в `Models/CpiComponents`: CatBoost/elastic net/LM покомпонентно по 88 группам ИПЦ, 6 горизонтов, отдельная модель на каждую пару «группа × горизонт». Дополнительно в архиве лежат Python-ноутбуки для агрегатных моделей (RW, VAR, DFM, ARIMA, exp. smoothing, elastic net) и R-скрипт elastic net. Запуск — из `Models/CpiComponents`, порядок описан в `README.txt`.
2. **Таргет есть, это данные, а не только код.** «SA ИПЦ без КУ, накопленный прирост» лежит в:
   - `.../StfCpiR_db/raw_data_store/cpi_sa_target_history.xlsx` — 50 кодов, 2016-01…2024-07, уровни SA (100 = предыдущий месяц), включая `1 - 9470`;
   - `.../StfCpiR_db/output/cpi_sa_db_with_forecast.csv` — SA-база с прогнозом на 2025-03;
   - `Расчет ошибок вневыборочного прогноза по моделям.xlsx`, листы `TARGET` (2016-01…2024-07, флаг «учитывать») и `TARGET_FULL` (2009-01…2024-07, h1…h6);
   - `Models/Предсказания.xlsx`, лист `data` — `LN ИПЦ1/2/3/6`, из которых `TARGET_FULL` восстанавливается **точно** (max diff = 0 на 182 общих месяцах).
3. **Per-period прогнозы CB_COMP есть.** `.../out_of_sample_forecasts/catboostVCB_IBK.xlsx`: 91 строка (2017-01…2024-07), колонки `1…6` — накопленный прирост на горизонт; для теста 2019-01…2024-07 — **67 строк × 6 горизонтов, без пропусков**. Лист `StfCpiCB` в `Расчет ошибок...xlsx` содержит те же значения (max diff = 0 для h1/h2/h3/h6) и флаг включения в ошибку (54 normal-месяца).
4. **Тест 2019–2024 воспроизводится без обучения.** По `catboostVCB_IBK.xlsx` и `TARGET_FULL`, на 54 normal-месяцах: RMSE h1 = **0.158805**, h2 = **0.296655**, h3 = **0.443623**, h6 = **0.971459** — точь-в-точь `model_errors_and_dm_tests.xlsx` и таблица статьи (0.16 / 0.30 / 0.44 / 0.97). Для h4/h5 получается 0.604 / 0.788 (в таблице статьи их нет).
5. **Фолдов нет.** CB_COMP обучен один раз на фиксированном train и предсказывает фиксированный test; это не walk-forward. Фактический train — **2010-03…2018-12 (102 месяца)**, а не 2009-01: лаги/NA и исключение выбросов. Test normal — 54 месяца; test outlier — 17 дат (2014-12…2015-03 + 2020-04 + 2022), из них 13 в тестовом окне 2019–2024.
6. **Данные включены частично.** Есть производная SA-панель `data_panel_bi_sa.fst` (183 654 строки, 2009-01…2024-09): 92 группы ИПЦ, 87 кодов ОКВЭД2 × 3 индикатора МП, кредит, ставки, рыночный USD. Сырые данные МП **не включены** (`monitoring/monitoring_db/README.txt`: «Распространение оригинальных данных только по запросу»); сырой Росстат — фрагментарно (в основном 2024 г.), полной истории нет.
7. **«88 групп ОКВЭД2» — неточно как формулировка.** В архиве: **88 компонентных групп ИПЦ** (92 используемых группы минус 4 агрегата `1`, `6`, `7`, `9000`), **87 уникальных кодов ОКВЭД2** в МП-панели и **72 из 88** компонентных групп с явной BSU-привязкой. Отдельного списка «88 групп ОКВЭД2» нет; 16 компонентных групп в публичной версии идут без BSU-признаков. Это важно для интерпретации статьи.

---

## 2. Что скачано и как распаковано

Архив скачан в `build/cb-comp/` (каталог `build/` уже в `.gitignore`):

```bash
mkdir -p build/cb-comp
curl -fL --retry 5 --retry-delay 3 --connect-timeout 30 \
  -o build/cb-comp/BusinessSurveyCBRCpiForecastingSourceCode.7z \
  https://ndownloader.figshare.com/files/51053942
md5 -q build/cb-comp/BusinessSurveyCBRCpiForecastingSourceCode.7z
# 797f00518c3a0fe6d4cd2c2ad22aa70a
```

`7z`/`7zz`/`unar` в системе не было; использован `py7zr` через `uv` (систему не меняли):

```bash
uv run --with py7zr python - <<'PY'
import py7zr
p = 'build/cb-comp/BusinessSurveyCBRCpiForecastingSourceCode.7z'
with py7zr.SevenZipFile(p, 'r') as z:
    z.extractall(path='build/cb-comp/extracted')
PY
```

Распаковка — 29 с, 542 МБ на диске, 3 507 файлов (3 601 запись с каталогами), 561 388 895 байт без сжатия.

Для чтения RDS/FST установлен R (в системе его не было):

```bash
brew install r          # R 4.6.1
Rscript -e 'install.packages("fst", repos="https://cloud.r-project.org")'
```

`pyreadr` и `rdata` RDS этого архива **не читают**: внутри `model_spec` лежат R-функции и raw-объект CatBoost. Base R `readRDS()` читает; FST читается `fst::read_fst()`.

---

## 3. Структура архива, язык и зависимости

### 3.1. Верхний уровень

`BusinessSurveyCBRCpiForecastingSourceCode/`:

| Путь | Что это |
|---|---|
| `README.txt` | авторы, назначение, GPL 3 |
| `Diebold-Mariano.R` | DM-тесты по `Расчет ошибок...xlsx` |
| `Расчет ошибок вневыборочного прогноза по моделям.xlsx` | 26 листов: таблица статьи, `TARGET`, `TARGET_FULL`, прогнозы/квадраты ошибок по моделям |
| `model_errors_and_dm_tests.xlsx` | 92 строки RMSE + 40 строк DM p-value |
| `Models/CpiComponents/` | основной R-пакет `StfCpiR` + данные + обученные модели (559,5 МБ) |
| `Models/BS LM_2009/`, `BS LM_2016/` | данные/прогнозы линейных моделей BS_ECON/BS_ROZN, ноутбуки |
| `Models/Elastic net/` | X/Y train/test, R-скрипт `cv.glmnet` |
| `Models/DFM.ipynb`, `VAR.ipynb`, `AUTO ARIMA.ipynb`, `RW.ipynb`, `exponential smoothing .ipynb` | агрегатные модели на Python |
| `Models/Предсказания.xlsx` | `data` (2009-06…2024-07), `predicts`, `errors` — агрегатные прогнозы и `LN ИПЦh` |
| `Models/CpiComponents/StfCpiMpMl_весь_пайплайн.drawio.png` | схема пайплайна |

По расширениям: 3 312 `.RDS` (495 МБ), 82 `.xlsx` (28,7 МБ), 76 `.R` (0,5 МБ), 16 `.csv` (12,5 МБ), 8 `.ipynb` (0,2 МБ), 2 `.fst` (22 МБ), 1 `.png`, 1 `.xls`, 6 `.txt`.

### 3.2. R-пакет `StfCpiR`

`Models/CpiComponents/README.txt`:

- R 4.1.2, Windows 10 x64, локаль `Russian_Russia.1251`;
- пакеты: `catboost 1.2.7`, `glmnet 4.1-3`, `fst 0.9.8`, `fstcore 0.9.14`, `data.table 1.14.2`, `dplyr 1.1.4`, `openxlsx 4.2.5`, `lubridate 1.8.0`, `glue 1.6.2`, `stringr 1.4.0`, `future.apply 1.8.1`, `progressr 0.14.0`, `fastDummies 1.6.3`, `treeshap 0.2.1.1000`, `tictoc 1.0.1`;
- «Текущая версия проекта урезена»; «Распространение всех исходных данных только по запросу. Для оценки модели доступны все данные только в форме SA».

Ключевой код:

| Файл | Роль |
|---|---|
| `R/model/model_cpi_train.R` | обучение 6 версий моделей по 92 группам × 6 горизонтам; сохраняет RDS в `forecast_models/` |
| `R/model/model_cpi_evaluate.R` | читает RDS, считает прогнозы и пишет `model_stats_bind.fst` |
| `R/databases_creators/cpi_forecasts_db_create.R` | агрегирует компоненты в ИПЦ, считает ошибки, пишет `out_of_sample_forecasts/*.xlsx` |
| `R/model/model_cpi_group_data_prepare/VA03_dummy_cpi_lag_IBK.R` | формирует таргет и матрицу признаков |
| `R/model/params_list_V_get.R` | параметры CatBoost V3 (дефолты, без validation) |
| `R/model/model_fit/model_catboost_fit.R` | `catboost.train(pool_train, pool_test, params)` |
| `R/model/model_predict.R` | `catboost.predict`, `predict.glmnet`, `lm` |
| `R/common/main_reference_tables_load.R` | словари, веса, model.xlsx |
| `R/loaders/common_loaders.R` | загрузчики данных/целевого ряда |
| `R/loaders/fedstat/*.R`, `cbr/*.R`, `market_indicies/*.R` | сырые загрузчики (часть помечена `broken`) |

Ноутбуки агрегатных моделей — Python 3: `pandas`, `numpy`, `openpyxl`, `statsmodels`, `sklearn`, `pmdarima` (ARIMA). `Elastic net/Open second (model).r` — R + `glmnet`/`openxlsx`. `Diebold-Mariano.R` — R + `forecast`, `readxl`, `dplyr`, `tidyr`, `openxlsx` (этих пакетов нет в списке README).

### 3.3. Как запускается

Рабочий каталог — `Models/CpiComponents` (в `R/common/globals.R` `EXT_DATA_DIR <- "./StfCpi_External_data"`). Порядок из README:

```bash
cd Models/CpiComponents
Rscript R/model/model_cpi_train.R
Rscript R/model/model_cpi_evaluate.R
Rscript R/databases_creators/cpi_forecasts_db_create.R
```

Опционально из корня архива:

```bash
Rscript Diebold-Mariano.R
```

Скрипты не параметризованы: `last_data_date` зашит (`2024-07-01` в train, `2024-09-01` в evaluate), пути относительные, `model_files_path` и `out_of_sample_forecasts` фиксированы. Это ровно тот прогон, который породил архивные RDS/xlsx.

---

## 4. Таргет «SA ИПЦ без КУ, накопленный прирост»

### 4.1. Где лежит и что содержит

| Файл | Содержимое | Покрытие |
|---|---|---|
| `.../StfCpiR_db/raw_data_store/cpi_sa_target_history.xlsx`, лист `Россия` | 50 кодов, уровни SA (100 = предыдущий месяц), включая `1 - 9470`, `1`, `6`, `7`, `9000` | 2016-01…2024-07 |
| тот же файл, лист `Россия ПАРСИНГ` | те же 50 кодов, m/m SA | 2016-01…2024-07 |
| `.../StfCpiR_db/output/cpi_sa_db_with_forecast.csv` | `mom_sa`, `mom`, `yoy`, вес, группа, регион; историческая `mom_sa` — с 2016, прогноз — 2025-03 | 2010-01…2025-03 (36 594 строки данных) |
| `Расчет ошибок...xlsx`, лист `TARGET` | 103 месяца, h1/h2/h3/h6, флаг «Учитывать при расчете ошибки» | 2016-01…2024-07 |
| `Расчет ошибок...xlsx`, лист `TARGET_FULL` | 187 месяцев, h1…h6, накопленный прирост, % | 2009-01…2024-07 |
| `Models/Предсказания.xlsx`, лист `data` | `LN ИПЦ1`, `LN ИПЦ2`, `LN ИПЦ3`, `LN ИПЦ6` + макро/МП-признаки, `Выброс?`, `Тест?` | 2009-06…2024-07 |
| `.../data/Коммунальные услуги прогноз MoM_sa.xlsx` | коммунальные услуги (код `9470`) fact/forecast, сезонный фактор | 2010-01…2025-05 |

Таргет — **сезонно сглаженный ИПЦ без коммунальных услуг**, накопленный прирост за h месяцев:

`TARGET_h(t) = exp(Σ_{j=1..h} log(CPI_sa(t-h+j)/100)) * 100 − 100`.

В коде это `model_y = CPI#<group>#diff#l0#m1`, `frollsum(..., h)`, сдвиг на `h-1` и `shift(..., type="lead")`; затем `exp(Y)*100−100` (`cpi_forecasts_db_create.R`). `1 - 9470` — агрегат «все товары и услуги минус коммунальные услуги»; `9470` в панели отсутствует и добавляется из отдельного прогноза ЖКУ.

### 4.2. Проверка согласованности

- `TARGET_FULL` и `Предсказания.xlsx` (`exp(LN ИПЦh)*100−100`) совпадают **точно** на 182 общих месяцах (max diff = 0.0) для h1, h2, h3, h6.
- `cpi_sa_target_history.xlsx` (`1 - 9470`, кумулятивные логарифмы) и `TARGET_FULL` совпадают с точностью до округления уровней: max diff 5e-5 (h1), 9e-5 (h2), 1.4e-4 (h3), 1.6e-4 (h6).
- Для 2019-01…2024-07 таргет есть **без пропусков** для h1…h6; 54 normal-месяца после исключения 2020-04 и 2022.

**Вывод:** таргет — данные, а не только код; для строгого сравнения на 2019–2024 его можно брать из `TARGET_FULL`/`TARGET` без реконструкции.

---

## 5. Прогнозы CB_COMP: где лежат и можно ли получить per-period

### 5.1. Что обучено

`model_cpi_train.R` запускает `macro_spec_grid.xlsx` (6 спецификаций):

| model_version | model_type | MP-признаки | RDS |
|---|---|---|---|
| `catboostVCB_IBK` | CatBoost | да | 92 группы × 6 горизонтов = 552 |
| `catboostVCB_IBK_nomp` | CatBoost | нет | 552 |
| `elastic_netVEN_IBK` | glmnet | да | 552 |
| `elastic_netVEN_IBK_nomp` | glmnet | нет | 552 |
| `lmVLM_IBK` | lm | да | 552 |
| `lmVLM_IBK_nomp` | lm | нет | 552 |

Итого **3 312 RDS**. Путь: `.../StfCpiR_db/forecast_models/<model_version>/643--643004.АГ/h<1..6>/<group>.RDS`. Регион только Россия (`643->643004.АГ`); `ГУ7` (Дальневосточное ГУ) есть в панели/весах, но модели для него не обучались.

CatBoost V3 (`params_list_V_get.R`): только `loss_function=RMSE`, `eval_metric=RMSE`, `logging_level=Silent`, `thread_count=12`; остальное — дефолты CatBoost, `use_best_model=FALSE`, без validation/early stopping/тюнинга. В RDS: `fact_tree_count=1000`, `fact_feature_count=22…34`, `params$use_best_model=FALSE`. Это соответствует статье «CatBoost дефолтной конфигурации, без перебора».

### 5.2. Train/test и «фолды»

Из `macro_spec_grid.xlsx` и RDS:

- `test_dates_normal`: **2019-01…2024-07**, 54 месяца (исключены 2020-04 и 2022);
- `test_dates_outliers`: 2014-12…2015-03, 2020-04, 2022-01…2022-12 — 17 дат;
- `train_dates_fact` в RDS: **2010-03…2018-12**, 102 месяца (106 месяцев минус 4 выброса 2014-12…2015-03);
- `model_stats`: 173 даты = 102 train + 54 test_normal + 17 test_outlier; `test_normal`/`test_outlier`/`train` — ровно эти наборы.

**Фолдов/walk-forward нет.** Один фиксированный train → один фиксированный test; для каждого горизонта h обучена отдельная модель. Это не rolling refit. Формулировка статьи «псевдо-реальное время» относится к срокам публикации данных, а не к переобучению на каждом шаге.

### 5.3. Per-period прогнозы

| Файл | Что внутри | Покрытие |
|---|---|---|
| `.../out_of_sample_forecasts/catboostVCB_IBK.xlsx` | строки — дата таргета, колонки `1…6` — накопленный прирост, % | 2017-01…2024-07, 91 строка; для 2019-01…2024-07 — 67 строк, null = 0 |
| `Расчет ошибок...xlsx`, лист `StfCpiCB` | те же прогнозы h1/h2/h3/h6 + squared error + флаг `Учитывать` | 2016-01…2024-07 (прогнозы с 2017-07); `True` = 54 normal-месяца |
| `.../output/model_stats_bind.fst` | per-group/per-horizon прогнозы всех 6 моделей | 562 392 строки; до 92 групп × 6 горизонтов × 173 даты × 6 моделей (часть хвостовых дат отсутствует); 2010-03…2024-07 |
| RDS `.../forecast_models/...` | `model_stats` внутри каждого RDS | те же 173 даты на группу/горизонт |

`catboostVCB_IBK.xlsx` и лист `StfCpiCB` **совпадают побитово** для h1/h2/h3/h6 (max diff = 0) на всех общих 85 месяцах. `forecast_errors_db.csv` и `cpi_forecasts.csv` — **не** эти прогнозы: на общем таргете `1 - 9470` они совпадают между собой (max diff = 0), но отличаются от CB (max diff 0.2738); скрипта, который их пишет, в архиве нет. Для CB_COMP использовать xlsx/`StfCpiCB`, не эти CSV.

### 5.4. Воспроизведение RMSE статьи

Пересчёт по `catboostVCB_IBK.xlsx` и `TARGET_FULL`, только normal-месяцы (54):

| h | RMSE (наш пересчёт) | `model_errors_and_dm_tests.xlsx` | статья |
|---|---|---|---|
| 1 | **0.158805** | 0.158805 | 0.16 |
| 2 | **0.296655** | 0.296655 | 0.30 |
| 3 | **0.443623** | 0.443623 | 0.44 |
| 6 | **0.971459** | 0.971459 | 0.97 |

Дополнительно: h4 = 0.604, h5 = 0.788 (в таблице статьи не публиковались). DM p-value (CB с МП vs CB без МП) из `model_errors_and_dm_tests.xlsx`: h1 = **0.032952**, h2 = **0.005115**, h3 = **0.005355**, h6 = **0.132256** — совпадает со звёздами статьи (h1 `**`, h2/h3 `***`, h6 ns).

**Ответ на вопрос 3:** per-period прогнозы CB_COMP получить можно, и для теста 2019–2024 они полные — 67 месяцев × 6 горизонтов, с флагом normal/outlier. Переобучение для сравнения не требуется.

---

## 6. Какие данные включены

### 6.1. Производная SA-панель

`.../StfCpiR_db/output/data_panel_bi_sa.fst` — 183 654 строки × 9 колонок, 2009-01…2024-09 (189 месяцев), два региона: `643->643004.АГ` (120 174 строки) и `ГУ7` (63 480).

| Группировка | Строк | Состав |
|---|---:|---|
| Индекс потребительских цен | 132 288 | 92 группы × 2 региона × 189 месяцев × 4 вида (`base_index`, `diff`, `trend`, `trend_deviation`); 182 пары «код × регион», есть пропуски |
| Мониторинг предприятий | 49 110 | 87 кодов ОКВЭД2 × 3 индикатора × ~188 месяцев; только Россия |
| Кредит населению | 744 | `debt_households_wh_mortgage`, лаг публикации 2 мес.; только Россия |
| Кредитные ставки | 756 | `miacr`, `interest_rate`, `diff`, `trend`, `trend_deviation`; только Россия |
| Рыночные индексы | 756 | `usd_rub`, `base_index`/`diff`/`trend`/`trend_deviation`; только Россия |

Индикаторы МП (по 87 кодов ОКВЭД2): `Indikator biznes-klimata Banka Rossii (IBK)`, `Izmeneniye izderzhek proizvodstva (obrashcheniya): balans otvetov`, `Ozhidaniya izmeneniya tsen na gotovuyu produktsiyu (uslugi) predpriyatiy: balans otvetov`. Лаг публикации — 1 месяц; в модели МП-признаки допускаются только с лагом ≤ 1.

### 6.2. Сырые данные

- **МП:** сырых данных нет. `.../raw_data_store/monitoring/monitoring_db/README.txt`: «Распространение оригинальных данных только по запросу». В панели — производные SA-серии.
- **Росстат:** полной сырой истории нет. В `raw_data_store/other_rosstat_raw_sources/rosstat_web/` — `Prom_01-2024…Prom_08-2024.xlsx`, `Prom-june/july.xlsx`, `sales_dynamics.xlsx`, `Краткосрочные экономические показатели.xlsx`; `raw_data_store/fedstat/README.txt` пустой, сырых выгрузок Федстата нет.
- **ЦБ/рынки:** `curs_usd.xlsx`, `debt/db.csv`, `miacr.xlsx`, `ruonia.xlsx`; `market_data/` — Brent, Urals, gold, copper, CRB, RVI, VIX; `FAO_FPI.xls`; `structural_regions_indicators/` — 4 xlsx; `Данные, заполняемые в ручном режиме.xlsx`.
- **Веса:** `StfCpi_External_data/databases/weights/cpi_weights_tovar.xlsx`, листы 2016…2024; 2024 — 7 935 строк, колонки `наименование_товара_услуги`, `вес_товара`, `год`, `регион`, `код_товара_услуги`, `окато_код`. В `cpi_forecasts_db_create.R` веса сдвигаются на год (`год + 1`), последний доступный год — 2024 (`LAST_AVAILIBLE_CPI_WEIGHTS_YEAR`).
- **Словари:** `dictionary_cpi.xlsx` — 15 листов 2010…2024; в 2024: 909 строк, 692 конечных кода уровня 7; `dictionary_okato.xlsx`, `dictionary_okvedokpd2.xlsx` (2 601 КБ), `standardization_cpi/okato`.
- **Модельная спецификация:** `data/model/model.xlsx`, лист `1.0.4.2` — 114 строк, **92 used**; `macro_spec_grid.xlsx` — 6 спецификаций; `Коммунальные услуги прогноз MoM_sa.xlsx` — ЖКУ.

### 6.3. 88 групп и ОКВЭД2

- `model.xlsx` (`1.0.4.2`): 92 used-группы; 4 агрегата (`1`, `6`, `7`, `9000`) + **88 компонентных групп** — это и есть «88 групп» статьи.
- В МП-панели — **87 уникальных кодов ОКВЭД2**, ровно те же, что в BSU-колонках `model.xlsx`.
- Явная BSU-привязка: **72 из 88** компонентных групп (73 строки, если считать агрегат `6`); 16 компонент без BSU: `5600`, `24`, `25`, `7200`, `7350`, `6800`, `5400`, `8300`, `84`, `86`, `87`, `89`, `9520`, `9800`, `9850`, `9880` (табак, чай/кофе, соль/специи, игрушки, спорттовары, велосипеды, галантерея, садоводство, химчистка, фотоателье, бани, ритуальные, культура, правовые, банки, страхование). У них в RDS только CPI-лаги + кредит/ставки/USD.
- BSU-кодов на компоненту: 0 — 19 строк (4 агрегата + 15 компонент), 1 — 25, 2 — 17, 3 — 24, 4 — 7; всего 159 BSU-код-ячеек.

**Вывод:** 88 групп товаров/услуг в архиве есть; «88 групп ОКВЭД2» — нет, есть 87 кодов ОКВЭД2 и 72 компоненты с явной привязкой. Публичная версия урезана; в статье полная отраслевая привязка могла быть шире.

---

## 7. Воспроизводимость на нашем окружении

### 7.1. Что реально проверено

| Проверка | Результат |
|---|---|
| Скачивание и md5 | OK, 438 628 189 байт, md5 совпал |
| Распаковка `py7zr` | OK, 3 507 файлов, 561 МБ |
| Чтение RDS base R | OK (`readRDS`), но `pyreadr`/`rdata` падают на функциях/raw CatBoost |
| Чтение FST | OK (`fst::read_fst`), 183 654 × 9 и 562 392 × 12 |
| `TARGET_FULL` vs `Предсказания.xlsx` | max diff = 0.0 |
| `cpi_sa_target_history` vs `TARGET_FULL` | max diff ≤ 1.6e-4 (округление) |
| `catboostVCB_IBK.xlsx` vs `StfCpiCB` | max diff = 0.0 |
| RMSE h1/h2/h3/h6 | 0.158805 / 0.296655 / 0.443623 / 0.971459 — совпало |
| DM p-value | 0.032952 / 0.005115 / 0.005355 / 0.132256 — совпало |
| Полный retrain CatBoost | **не выполнялся** |

### 7.2. Оценка выполнимости

- **Строгое сравнение без retrain — выполнимо сейчас.** Нужны только `TARGET_FULL` (или `TARGET`) и `out_of_sample_forecasts/catboostVCB_IBK.xlsx`; оба файла в архиве, tarгет и прогнозы согласованы, RMSE статьи воспроизводится. Это лучший путь для #190: не зависеть от R/CatBoost/Windows.
- **Полный retrain CB_COMP — возможно, но с рисками.** Нужны R 4.1.2-совместимые пакеты, CatBoost R 1.2.7, `fst`, `data.table`, `glmnet`, `treeshap` и др. В текущем CRAN пакета `catboost` нет (проверено по `PACKAGES.gz`); авторы использовали `catboost 1.2.7` (официальный R-пакет CatBoost / GitHub), на mac arm64 + R 4.6.1 установка/совместимость не проверены. `fst` в CRAN — 0.9.8 (как у авторов); `glmnet` сейчас 5.1 (у авторов 4.1-3), `treeshap` 0.4.0 (у авторов 0.2.1.1000).
- **Encoding — главный практический блокер.** FST-панель хранит имена/строки в CP1251. На mac R 4.6.1 `names_enc2native()` не конвертирует CP1251 в UTF-8: проверка `match("группировка_по_виду_статистики", enc2native(names(d)))` даёт `NA`, а `iconv(..., from="CP1251", to="UTF-8")` — 8. Код `data.table`-фильтров сравнивает имена с UTF-8-литералами, поэтому без патча (`iconv` имён/значений) или запуска в Windows/CP1251-локали `model_cpi_train.R`/`evaluate.R` упадут на отсутствии колонок. Это не проблема данных, а проблема портативности кода.
- **Пути.** `model_stats_bind.fst` хранит `model_i` как `C:/Users/denis/...`; `model_cpi_train.R`/`evaluate.R` используют относительные пути от `Models/CpiComponents`; на mac/linux их надо сохранять. Скрипты не принимают аргументов.
- **`cpi_forecasts_db_create.R`** требует `tictoc`, `data.table`, `fst`, `openxlsx`, `glmnet` и т.д.; агрегация/ошибки уже посчитаны в архиве, поэтому запускать её для сравнения не обязательно.

### 7.3. Точные команды проверки (сокращённо)

```bash
cd build/cb-comp/extracted/BusinessSurveyCBRCpiForecastingSourceCode/Models/CpiComponents

# 1) структура одного CatBoost RDS
Rscript -e 'x <- readRDS("StfCpi_External_data/databases/StfCpiR_db/forecast_models/catboostVCB_IBK/643--643004.АГ/h1/1.RDS"); print(names(x)); print(x$model_stats$sample_type |> table()); print(range(x$train_dates_fact))'

# 2) панель и model_stats
Rscript -e 'library(fst); d <- read_fst("StfCpi_External_data/databases/StfCpiR_db/output/data_panel_bi_sa.fst"); print(dim(d)); m <- read_fst("StfCpi_External_data/databases/StfCpiR_db/output/model_stats_bind.fst"); print(dim(m)); print(table(m$model_version))'

# 3) RMSE CB_COMP по архивным прогнозам и TARGET_FULL
uv run --with pandas --with openpyxl python - <<'PY'
import pandas as pd, numpy as np
base = 'build/cb-comp/extracted/BusinessSurveyCBRCpiForecastingSourceCode'
tgt = pd.read_excel(f'{base}/Расчет ошибок вневыборочного прогноза по моделям.xlsx', sheet_name='TARGET_FULL')
tgt['дата'] = pd.to_datetime(tgt['дата']); tgt = tgt.set_index('дата')
cb = pd.read_excel(f'{base}/Models/CpiComponents/out_of_sample_forecasts/catboostVCB_IBK.xlsx')
cb['дата'] = pd.to_datetime(cb['дата'])
out = set(pd.to_datetime(['2014-12-01','2015-01-01','2015-02-01','2015-03-01','2020-04-01'] +
                         [f'2022-{i:02d}-01' for i in range(1,13)]))
cb = cb[(cb['дата'] >= '2019-01-01') & (cb['дата'] <= '2024-07-01') & (~cb['дата'].isin(out))]
for h in [1,2,3,6]:
    x = cb[['дата', str(h)]].dropna().join(tgt[h], on='дата').dropna()
    print(h, len(x), np.sqrt(np.mean((x[str(h)] - x[h]) ** 2)))
PY
```

Ожидаемый вывод: `1 54 0.158805`, `2 54 0.296655`, `3 54 0.443623`, `6 54 0.971459`.

---

## 8. Что не получилось, что отсутствует, риски для строгого паритета

1. **Публичный архив урезан.** `README.txt` прямо говорит «Текущая версия проекта урезена»; сырые МП-данные — «только по запросу». Полный производственный пайплайн (СКИФ-Аналитик, сырые отраслевые МП, полный Росстат/Федстат, винтажи) не воспроизводится. Для агрегатного сравнения это не блокер, для «полного» воспроизведения статьи — блокер.
2. **Не все 88 групп имеют BSU.** 16 компонентных групп в `model.xlsx` не имеют BSU-кодов и обучаются только на CPI-лагах + кредит/ставки/USD. Если в статье утверждается, что МП используются для всех 88 групп, публичная версия этому не соответствует; это надо оговорить в parity-отчёте.
3. **Нет 88 групп ОКВЭД2.** Есть 87 кодов ОКВЭД2 и 88 групп ИПЦ; прямой список «88 групп ОКВЭД2» отсутствует. Не выдавать одно за другое.
4. **Нет фолдов.** CB_COMP — один fixed train/test, не walk-forward. Наш nowcast — expanding walk-forward; для DM/TOST надо использовать общие normal-месяцы 2019-01…2024-07 (54) и архивные per-period прогнозы, а не переобучать CB_COMP на каждом фолде. Иначе сравнение будет не «паритетом», а разным дизайном.
5. **Фактический train — 2010-03…2018-12, не 2009-01.** Причина — лаги/NA и исключение 2014-12…2015-03. В отчёте #190 нужно писать фактический train, иначе можно неверно интерпретировать «одинаковое окно».
6. **Выбросы исключены из обучения и оценки.** 2014-12…2015-03, 2020-04, весь 2022 — `test_outlier`; в train их нет, в RMSE они не входят. Наш nowcast сейчас включает 2022; для строгого сравнения нужен тот же фильтр (и/или отдельная строка «с 2022»).
7. **Таргет — SA ИПЦ без КУ, не наш текущий таргет.** Наш nowcast прогнозирует несглаженный m/m общий ИПЦ; для parity нужен общий таргет `TARGET_FULL`. Прокси-путь (собрать SA без КУ из публичных рядов) возможен, но это не строгий таргет; в #190 уже зафиксировано.
8. **Винтажи не учитываются.** Архив — SA-панель без vintage/PIT; авторы это оговаривают. Для строгого паритета это общий ограничитель, не только наш.
9. **Несогласованные вспомогательные CSV.** `cpi_forecasts.csv` (18 542 строки, 52 группы, 2019-01…2025-03) и `forecast_errors_db.csv` (18 526 строк, 50 кодов, 2019-01…2024-07) совпадают между собой, но **не** с CB xlsx (max diff 0.2738) и не имеют генератора в архиве. Похоже на другой прогон/модель. Для CB_COMP использовать только `catboostVCB_IBK.xlsx`/`StfCpiCB`.
10. **Мелкая несогласованность EN_COMP.** RMSE из `out_of_sample_forecasts/elastic_netVEN_IBK.xlsx` (0.1719/0.2826/0.4145/0.9502) немного отличается от `model_errors_and_dm_tests.xlsx` (0.1723/0.2816/0.4088/0.9241). CB и LM совпадают точно. Для #190 это не критично (сравниваем CB), но говорить «все файлы согласованы» нельзя.
11. **R/CatBoost-воспроизводимость.** `catboost` нет в CRAN; версия 1.2.7, Windows/CP1251, абсолютные пути, `pyreadr`/`rdata` не читают RDS. Base R читает, но для `catboost.predict` нужен пакет. Полный retrain не проверен; для строгого паритета он и не нужен, если использовать архивные прогнозы.
12. **Таргет 2009-2015.** `TARGET_FULL` содержит 2009-01…2024-07, но `Предсказания.xlsx` — только с 2009-06; первые 5 месяцев есть только в `TARGET_FULL` (источник в архиве неочевиден). Для 2019–2024 это неважно; для полного train 2009–2018 — важно, если воспроизводить обучение.

---

## 9. Что это значит для #190

- **Строгий путь #190 можно делать без retrain CB_COMP:** взять `TARGET_FULL`/`TARGET` и `out_of_sample_forecasts/catboostVCB_IBK.xlsx` (или `StfCpiCB`), ограничить 2019-01…2024-07, исключить 2020-04 и 2022, сравнить с нашим nowcast на тех же 54 normal-месяцах; h1/h2/h3/h6 — основные, h4/h5 — дополнительные.
- **TOST/DM** считать на общих normal-месяцах; margin δ = 10% RMSE CB_COMP фиксировать до прогона (как в issue). Архивные CB p-value (h1 0.033, h2 0.005, h3 0.005, h6 0.132) — ориентир, но не заменяют наш тест.
- **Parity-модель не реализована** — это только source report. В отчёте #190 обязательно указать: публичный архив урезан, 16 групп без BSU, 87 кодов ОКВЭД2, фактический train 2010-03…2018-12, fixed split без фолдов, выбросы исключены.
- **Ссылки для отчёта #190:** архив — `build/cb-comp/BusinessSurveyCBRCpiForecastingSourceCode.7z` (локально, gitignored); прогнозы — `.../out_of_sample_forecasts/catboostVCB_IBK.xlsx`; таргет — `Расчет ошибок...xlsx` лист `TARGET_FULL`; per-group — `.../output/model_stats_bind.fst`.

---

## Приложение A. Ключевые пути (от корня распакованного архива)

```text
BusinessSurveyCBRCpiForecastingSourceCode/
├── README.txt
├── Diebold-Mariano.R
├── Расчет ошибок вневыборочного прогноза по моделям.xlsx
├── model_errors_and_dm_tests.xlsx
└── Models/
    ├── Предсказания.xlsx
    └── CpiComponents/
        ├── README.txt
        ├── data/
        │   ├── model/model.xlsx
        │   ├── model/macro_spec_grid.xlsx
        │   └── Коммунальные услуги прогноз MoM_sa.xlsx
        ├── R/
        │   ├── model/model_cpi_train.R
        │   ├── model/model_cpi_evaluate.R
        │   ├── model/model_cpi_group_data_prepare/VA03_dummy_cpi_lag_IBK.R
        │   ├── model/params_list_V_get.R
        │   └── databases_creators/cpi_forecasts_db_create.R
        ├── out_of_sample_forecasts/
        │   ├── catboostVCB_IBK.xlsx
        │   ├── catboostVCB_IBK_nomp.xlsx
        │   ├── elastic_netVEN_IBK.xlsx
        │   ├── elastic_netVEN_IBK_nomp.xlsx
        │   ├── lmVLM_IBK.xlsx
        │   └── lmVLM_IBK_nomp.xlsx
        └── StfCpi_External_data/
            ├── databases/weights/cpi_weights_tovar.xlsx
            ├── dictionaries/dictionary_cpi.xlsx
            └── databases/StfCpiR_db/
                ├── output/data_panel_bi_sa.fst
                ├── output/model_stats_bind.fst
                ├── output/cpi_sa_db_with_forecast.csv
                ├── output/cpi_forecasts.csv
                ├── output/forecast_errors_db.csv
                ├── raw_data_store/cpi_sa_target_history.xlsx
                └── forecast_models/<model_version>/643--643004.АГ/h<1..6>/<group>.RDS
```

## Приложение B. Сводка чисел

| Показатель | Значение |
|---|---:|
| Архив сжат / распакован | 438 628 189 B / 561 388 895 B |
| Файлов / записей | 3 507 / 3 601 |
| RDS | 3 312 (6 моделей × 92 группы × 6 горизонтов) |
| `data_panel_bi_sa.fst` | 183 654 × 9; 2009-01…2024-09 |
| `model_stats_bind.fst` | 562 392 × 12; 2010-03…2024-07 |
| Групп ИПЦ в панели | 92 (4 агрегата + 88 компонент) |
| Кодов ОКВЭД2 в МП-панели | 87 |
| Компонент с BSU | 72 из 88 |
| Test 2019-01…2024-07 | 67 месяцев; 54 normal + 13 outlier |
| Train (факт) | 102 месяца, 2010-03…2018-12 |
| CB_COMP RMSE h1/h2/h3/h6 | 0.158805 / 0.296655 / 0.443623 / 0.971459 |
| CB_COMP DM p h1/h2/h3/h6 | 0.032952 / 0.005115 / 0.005355 / 0.132256 |

---

*Отчёт фиксирует только разбор архива. Parity-модель, спека и план #190 в этой ветке не создавались.*
