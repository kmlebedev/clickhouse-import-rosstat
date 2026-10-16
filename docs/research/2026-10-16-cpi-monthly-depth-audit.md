# Аудит глубины рядов: месячный трек прогноза ИПЦ РФ (2026-10-16)

> **Объект:** dev-ClickHouse 26.10 (localhost:8123), пользователь `default`, база `default` — ряды-кандидаты для МЕСЯЧНОГО трека nowcast'а ИПЦ.
> **Задача:** оценить максимальную месячную выборку без недельных признаков — для сопоставимости с внешней работой (Кошкин–Крылов, 2025: train 2009–2018 / test 2019-07.2024); текущий недельный nowcast (`scripts/cpi_nowcast.py`) ограничен 57 месяцами (2022-01…2026-09).
> **Как проверялось:** только чтение; SQL-файлы запускались как `make ch-sql FILE=/tmp/cpi_audit/0N_*.sql` из worktree `.worktrees/cpi-nowcast-v0` (под капотом `clickhouse client --multiquery`); `make ch-up/down` не запускались, MCP-пользователь не использовался, репозиторий при аудите не изменялся.
> **Связь:** сверка [2026-10-16-cb-comp-vs-cpi-nowcast.md](2026-10-16-cb-comp-vs-cpi-nowcast.md), раздел «Пути увеличения сопоставимости»; парный документ — [2026-10-16-cpi-weekly-pre2022-probe.md](2026-10-16-cpi-weekly-pre2022-probe.md).

Дата аудита: 2026-10-16. Файлы запросов и сырых выводов — рабочие артефакты вне репозитория (все в `/tmp/cpi_audit/`; тексты SQL — в приложении A):

| Файл | Содержание |
|---|---|
| `00_tables.sql` | список таблиц БД |
| `01_describe.sql` | DESCRIBE 13 таблиц |
| `02_rosstat.sql` | rosstat_ipc_mes/weeks, salaries, labour_force, vvp |
| `03_macro.sql` | macro_series: cbr_survey по сериям, cbr_mo_br, все источники |
| `04_cbr_fao_sber.sql` | cbr_indicators_cpd/currency/key_rate/ruania/infl_exp, fao, sber, ofz_curve |
| `05_holes.sql` | дубликаты usd; пропущенные месяцы survey/usd/key_rate |
| `06_starts.sql` | старты/концы по сериям; greatest/least наборов |
| `07_coverage.sql` | покрытие окна статьи; проверка склейки 2013-10…2026-08 |
| `08_values.sql` | sanity значений (нули/NaN, первые/последние точки) |
| `09_ladder.sql` | календарная частота survey-серий; лестница стартов |

Сырые выводы: `/tmp/cpi_audit/*.out`. Полный per-series листинг cbr_mo_br (642 серии) — отдельно в `03_macro.out`, в отчёт не дублируется.

---

## 1. Главная таблица «ряд → count/min/max»

Все месячные таблицы имеют `count() == uniqExact(date)` (если не сказано иное) — т.е. дублей строк нет, кроме явно отмеченных.

| Таблица / ряд | count() | uniq dates | min(date) | max(date) |
|---|---|---|---|---|
| rosstat_ipc_mes: «ИПЦ на непродовольственные товары по РФ» | 429 | 429 | 1991-01-31 | 2026-09-30 |
| rosstat_ipc_mes: «ИПЦ на продовольственные товары по РФ» | 429 | 429 | 1991-01-31 | 2026-09-30 |
| rosstat_ipc_mes: «ИПЦ на товары и услуги по РФ» (общий) | 429 | 429 | 1991-01-31 | 2026-09-30 |
| rosstat_ipc_mes: «ИПЦ на услуги по РФ» | 429 | 429 | 1991-01-31 | 2026-09-30 |
| rosstat_ipc_weeks | 26611 | 243 | 2022-01-10 | 2026-10-05 |
| macro_series, source=`cbr_survey` (13 серий) | 2084 | — | 2002-01-01 | 2026-08-01 |
| macro_series, source=`cbr_mo_br` (642 серии) | 9348 | — | 2021-05-01 | 2026-09-01 |
| cbr_indicators_cpd: «Все товары и услуги» | 295 | 295 | 2002-02-28 | 2026-08-31 |
| cbr_indicators_cpd: «Базовый ИПЦ» | 295 | 295 | 2002-02-28 | 2026-08-31 |
| cbr_currency_usd | **6796** | **3399** | 2013-01-10 | 2026-10-13 |
| cbr_key_rate | 3278 | 3278 | 2013-09-17 | 2026-10-13 |
| cbr_ruania | 1910 | 1910 | 2019-01-09 | 2026-10-14 |
| cbr_infl_exp (бонус, не в списке) | 90 | 45 | 2023-01-14 | 2026-09-14 |
| fao_food_price_Indices_monthly: `Food Price Index` | 441 | 441 | 1990-02-01 | 2026-10-01 |
| fao_food_price_Indices_monthly: Cereals/Dairy/Meat/Oils | 441 каждый | 441 | 1990-02-01 | 2026-10-01 |
| fao_food_price_Indices_Monthly_Real | **0** (пустая) | 0 | — | — |
| sber_consumper_spending_index (5 серий: Всего, Прод., Непрод., Услуги, Общепит) | 94 каждый | 94 | 2018-12-01 | 2026-09-01 |
| rosstat_salaries_mes | 421 | 421 | 1991-08-01 | 2026-08-01 |
| rosstat_labour_force_mes (6 серий) | 115 каждый | 115 | 2017-01-31 | 2026-07-31 |
| rosstat_vvp_kvartal (5 серий) | 58–62 | = count | 2011-04-01 | 2026-07-01 |
| ofz_curve: теноры `1y`/`3y`/`5y`/`10y` | **1 каждый** | 1 | 2026-10-09 | 2026-10-09 |
| ofz_curve: тенор `RGBI` | 4252 | 4252 | 2010-01-11 | 2026-10-09 |

Проверка «count = числу месяцев между min и max» (структурное отсутствие дыр):

| Ряд | Число месяцев по календарю | count | Дыр нет? |
|---|---|---|---|
| rosstat_ipc_mes (каждая серия) | 1991-01…2026-09 = 429 | 429 | да |
| fao Food Price Index | 1990-02…2026-10 = 441 | 441 | да |
| rosstat_salaries_mes | 1991-08…2026-08 = 421 | 421 | да |
| rosstat_labour_force_mes | 2017-01…2026-07 = 115 | 115 | да |
| sber_consumper_spending_index | 2018-12…2026-09 = 94 | 94 | да |
| cbr_indicators_cpd | 2002-02…2026-08 = 295 | 295 | да |

## 2. cbr_survey — все серии источника (13)

| series | n | min(date) | max(date) | Календарные месяцы точек | Частота |
|---|---|---|---|---|---|
| business_climate | 296 | 2002-01-01 | 2026-08-01 | 1–12 | месячная |
| business_climate_sa | 296 | 2002-01-01 | 2026-08-01 | 1–12 | месячная |
| credit_conditions | 296 | 2002-01-01 | 2026-08-01 | 1–12 | месячная |
| price_expectations | 296 | 2002-01-01 | 2026-08-01 | 1–12 | месячная |
| price_expectations_sa | 296 | 2002-01-01 | 2026-08-01 | 1–12 | месячная |
| price_growth_expected | 80 | 2020-01-01 | 2026-08-01 | 1–12 | месячная |
| price_growth_expected_sa | 80 | 2020-01-01 | 2026-08-01 | 1–12 | месячная |
| capacity_utilization | 98 | 2002-04-01 | 2026-07-01 | 1,4,7,10 | квартальная |
| capacity_utilization_sa | 98 | 2002-04-01 | 2026-07-01 | 1,4,7,10 | квартальная |
| headcount_expectations | 98 | 2002-04-01 | 2026-07-01 | 1,4,7,10 | квартальная |
| headcount_expectations_sa | 98 | 2002-04-01 | 2026-07-01 | 1,4,7,10 | квартальная |
| staff_availability | 26 | 2020-04-01 | 2026-07-01 | 1,4,7,10 | квартальная |
| staff_availability_sa | 26 | 2020-04-01 | 2026-07-01 | 1,4,7,10 | квартальная |

count 296 = ровно 2002-01…2026-08 (296 месяцев), 80 = ровно 2020-01…2026-08, 98/26 согласуются с квартальной сеткой {1,4,7,10} — **дыр нет ни в одной серии**.

cbr_mo_br (сводно): 9348 строк, 642 серии, диапазон 2021-05-01…2026-09-01; это выпуски аналитических обзоров (brent_20XX_*, wage_* и т.п.), непрерывных месячных рядов среди них нет — для глубины месячного трека источник не даёт ничего глубже 2021-05.

## 3. Дыры, дубликаты, аномалии (пункт 5 задания)

Пропущенные месяцы «между min и max, кроме хвоста» (SQL: сгенерированные календарные месяцы `WHERE m NOT IN (наблюдённые)`) — результат по всем проверенным рядам **пустой**:

| Ряд | Пропущенные месяцы |
|---|---|
| macro_series cbr_survey `price_expectations` | нет |
| macro_series cbr_survey `price_expectations_sa` | нет |
| macro_series cbr_survey `business_climate` (доп.) | нет |
| cbr_currency_usd (месяц присутствует, если есть хотя бы один день) | нет |
| cbr_key_rate (доп.) | нет |

Аномалии значений (sanity):

- `cbr_currency_usd`: **2 идентичные строки на каждую дату** (6796 строк / 3399 дат; например, 2026-10-09 — `[85.4173, 85.4173]`). Это ReplacingMergeTree без `FINAL` (повторный импорт за сезон), второго ряда (EUR и т.п.) там нет. В выборках использовать `FINAL` / `GROUP BY date`. Цены валидные: min 29.9251, max 120.3785, нулей/NaN нет; первая точка 2013-01-10 = 30.4215.
- `cbr_currency_usd` — глубина **ограничена источником**: в `cbr/currency_usd.go` зафиксировано «10.01.2013 — первая дата (baseline T1, n=3397); лента глубже не отдаёт».
- `cbr_key_rate`: 2013-09-17 = 5.5 (день начала публикации ключевой ставки), min 4.25 / max 21, нулей нет.
- cbr_survey (месячные): нулей и NaN нет; диапазоны: business_climate [-21.87; 23.43], business_climate_sa [-25.73; 18.72], price_expectations [5.15; 42.68], price_expectations_sa [4.25; 42.44], credit_conditions [-55.59; 5.45]. Первые значения (2002-01) осмысленные (price_expectations = 36.4), хвост свежий (2026-08 = 19.74).
- `rosstat_ipc_mes` (общий): 1991-01 = 106.2; хвост: 2026-08 = 99.92 (м/м −0.08%), 2026-09 = 100.35 (м/м +0.35%).
- `fao_food_price_Indices_Monthly_Real` — пустая таблица (0 строк) — не использовать.
- `ofz_curve`: теноры 1y/3y/5y/10y — по одной точке 2026-10-09 (G-curve снапшот, истории нет — как и описано в ограничении `cpi_nowcast.py`); глубже всех только RGBI (4252 точки, 2010-01-11…2026-10-09).
- `cbr_ruania`: 1910 точек 2019-01-09…2026-10-14 (это ставки RUONIA — в наборы месячного трека не входит).
- `cbr_infl_exp` (инфляционные ожидания населения, infOM): 90 строк / 45 дат, 2023-01-14…2026-09-14 — слишком короткая история, для глубины нерелевантна.

---

## 4. Стартовые варианты месячного трека (пункт 2 задания)

Состав наборов по заданию:
- **S_МП** = {rosstat_ipc_mes (общий ИПЦ + 3 компоненты), cbr_survey `price_expectations` + `price_expectations_sa` + `business_climate` + `business_climate_sa`, cbr_currency_usd, cbr_key_rate, fao `Food Price Index`};
- **S_без_МП** = то же без четырёх серий cbr_survey.

Старты по сериям (месяц начала данных):

| Серия | Старт | Конец |
|---|---|---|
| rosstat_ipc_mes (все 4) | 1991-01 | 2026-09 |
| fao `Food Price Index` | 1990-02 | 2026-10 |
| cbr_survey price_expectations(_sa), business_climate(_sa) | 2002-01 | 2026-08 |
| cbr_currency_usd | 2013-01 | 2026-10 |
| **cbr_key_rate** | **2013-09** | 2026-10 |

**Позже всех в обоих наборах начинается `cbr_key_rate` (месяц 2013-09) — именно она, а не МП, связывает старт.**
МП (cbr_survey) глубину НЕ ограничивает: price_expectations(_sa) и business_climate(_sa) идут непрерывно с 2002-01.

Итог (greatest() стартов; выборка с `y_lag1` = старт + 1 месяц):

| Вариант набора | Старт данных | Старт выборки (с y_lag1) |
|---|---|---|
| S_МП (полный) | **2013-09** | **2013-10** |
| S_без_МП (полный) | **2013-09** (тот же: survey не связывает) | 2013-10 |
| S_МП / S_без_МП без `cbr_key_rate` | 2013-01 (связывает usd) | 2013-02 |
| S_МП без `key_rate` и `usd` | 2002-01 (связывает МП) | 2002-02 |
| S_без_МП без `key_rate` и `usd` | 1991-01 (связывает ИПЦ; fao глубже — 1990-02) | 1991-02 |

Дополнительно: если расширить «МП» до всех месячных серий cbr_survey (+ `price_growth_expected(_sa)`, старт 2020-01), то при наличии key_rate/usd старт не меняется (2013-09); в варианте «без key_rate и usd» старт сдвинется с 2002-01 на 2020-01. Квартальные серии МП (`capacity_utilization(_sa)` с 2002-04, `headcount_expectations(_sa)` с 2002-04, `staff_availability(_sa)` с 2020-04) в месячной сетке потребуют интерполяции.

Концы: S_МП — 2026-08 (связывает survey; ipc — 2026-09, usd/key_rate/fao — 2026-10); S_без_МП — 2026-09 (связывает ipc).

## 5. Покрытие окна статьи 2019-01 … 2024-07 (пункт 3 задания)

Окно: 67 месяцев (2019-01…2024-07 включительно).

| Набор | Месяцев доступно из 67 | Чего не хватает |
|---|---|---|
| S_МП | **67 / 67** | ничего |
| S_без_МП | **67 / 67** | ничего |

SQL-проверка «месяцы, где чего-то не хватает» по окну вернула 0 строк — пропусков нет ни по одной серии набора. Для моделей с `y_lag1`: первый месяц окна (2019-01) получает лаг из 2018-12 (доступен), т.е. окно статьи отдаётся целиком, 67 строк. Это верно и для варианта «без key_rate и usd» (его старт 2002-01/2002-02 глубже начала окна).

## 6. Объединённая месячная выборка с y_lag1 (пункт 4 задания)

- Старт = greatest(стартов) + 1 месяц = **2013-10**;
- Конец = least(концов) = **2026-08** (ограничен сводками МП; без survey-набора конец — 2026-09);
- Объём: **155 месяцев** (2013-10…2026-08), из них «полных» (все 5 компонентов набора присутствуют) — **155/155**, список месяцев с пропусками пуст;
- Для S_без_МП: 156 месяцев (2013-10…2026-09).

## 7. Контекст: недельный трек и текущее ограничение

`rosstat_ipc_weeks`: 26611 строк, 243 недельные даты, 2022-01-10…2026-10-05 — подтверждено. Именно он ограничивает текущий nowcast (`scripts/cpi_nowcast.py`) 57 месяцами (2022-01…2026-09): месячный трек без недельных признаков может быть в ~2.7 раза длиннее (155 vs 57).

## 8. Выводы для сопоставимости со статьёй

1. **Максимальный месячный трек без недельных признаков**: 155 месяцев (2013-10…2026-08) при полном S_МП; 156 месяцев (2013-10…2026-09) при S_без_МП. Против 57 месяцев у текущего недельного nowcast.
2. **Окно статьи 2019-01…2024-07 покрывается целиком (67/67)** в обоих наборах — тестовая часть статьи воспроизводима на наших данных без оговорок.
3. **Train статьи 2009–2018 (120 месяцев)**: с `key_rate`+`usd` недоступен — трек начинается с 2013-10, это только 63 месяца train. Чтобы получить train 2009–2018 полностью, нужно строить признаки без cbr_key_rate и cbr_currency_usd (они стартуют 2013-09/2013-01); тогда трек с 2002-02 (со всеми 4 месячными рядами МП) или с 1991-02 (только ИПЦ+FAO). Механизм пропуска уже есть в пайплайне: `ofz_1y` пишется NaN, и месяц не выбрасывается (аналогично можно ввести NaN для usd/key_rate до 2013).
4. Вариант с полным S_МП (4 серии МП + usd + key_rate + FAO + y_lag1 + month) даёт 155 месяцев, но train-окно статьи 2009–2018 в нём не покрывается (63 из 120 месяцев); вариант «МП без ставки/курса» полностью совместим с train 2009–2018 статьи при сохранении МП с 2002.
5. Оговорка: аудит — про глубину данных (наличие точек), as-of/даты публикаций не моделировались; для cbr_survey фактическая дата публикации внутри месяца, для ретроспективного окна это некритично, но для живого nowcast правила as-of остаются как в спеке.

---

## Приложение A. SQL (полные тексты команд)

Все файлы запускались как `make ch-sql FILE=/tmp/cpi_audit/<файл>` из `.worktrees/cpi-nowcast-v0`.

### 01_describe.sql (фрагмент — схемы)
```sql
SELECT '--- rosstat_ipc_mes' AS t; DESCRIBE TABLE rosstat_ipc_mes;
SELECT '--- macro_series' AS t; DESCRIBE TABLE macro_series;
SELECT '--- cbr_indicators_cpd' AS t; DESCRIBE TABLE cbr_indicators_cpd;
SELECT '--- cbr_currency_usd' AS t; DESCRIBE TABLE cbr_currency_usd;
SELECT '--- cbr_key_rate' AS t; DESCRIBE TABLE cbr_key_rate;
SELECT '--- fao_food_price_Indices_monthly' AS t; DESCRIBE TABLE fao_food_price_Indices_monthly;
SELECT '--- sber_consumper_spending_index' AS t; DESCRIBE TABLE sber_consumper_spending_index;
SELECT '--- cbr_ruania' AS t; DESCRIBE TABLE cbr_ruania;
SELECT '--- rosstat_salaries_mes' AS t; DESCRIBE TABLE rosstat_salaries_mes;
SELECT '--- rosstat_labour_force_mes' AS t; DESCRIBE TABLE rosstat_labour_force_mes;
SELECT '--- rosstat_vvp_kvartal' AS t; DESCRIBE TABLE rosstat_vvp_kvartal;
SELECT '--- ofz_curve' AS t; DESCRIBE TABLE ofz_curve;
SELECT '--- rosstat_ipc_weeks' AS t; DESCRIBE TABLE rosstat_ipc_weeks;
```
Результат: `rosstat_ipc_mes(name,date,percent)`, `macro_series(source,series,date,value)`, `cbr_indicators_cpd(name,date,value)`, `cbr_currency_usd(date,price)`, `cbr_key_rate(date,rate)`, `fao_food_price_Indices_monthly(name,date,price_index)`, `sber_consumper_spending_index(name,date,value)`, `cbr_ruania(date,ruo,vol,trans,minRate,maxRate,percentile25,Percentile75)`, `rosstat_salaries_mes(name,date,salary)`, `rosstat_labour_force_mes(name,date,value)`, `rosstat_vvp_kvartal(name,date,vvp)`, `ofz_curve(date,tenor,yield)`, `rosstat_ipc_weeks(name,date,percent)`.

### 02_rosstat.sql
```sql
SELECT name, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM rosstat_ipc_mes GROUP BY name ORDER BY name;
SELECT count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d FROM rosstat_ipc_weeks;
SELECT name, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM rosstat_salaries_mes GROUP BY name ORDER BY name;
SELECT name, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM rosstat_labour_force_mes GROUP BY name ORDER BY name;
SELECT name, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM rosstat_vvp_kvartal GROUP BY name ORDER BY name;
```

### 03_macro.sql
```sql
SELECT series, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM macro_series WHERE source = 'cbr_survey' GROUP BY series ORDER BY series;
SELECT series, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM macro_series WHERE source = 'cbr_mo_br' GROUP BY series ORDER BY series;
SELECT source, count() AS n, uniqExact(series) AS n_series, min(date) AS min_d, max(date) AS max_d
FROM macro_series GROUP BY source ORDER BY source;
```
(Полный листинг 642 серий cbr_mo_br — в `/tmp/cpi_audit/03_macro.out`.)

### 04_cbr_fao_sber.sql
```sql
SELECT name, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM cbr_indicators_cpd GROUP BY name ORDER BY name;
SELECT count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d FROM cbr_currency_usd;
SELECT count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d FROM cbr_key_rate;
SELECT count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d FROM cbr_ruania;
SELECT count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d FROM cbr_infl_exp;
SELECT name, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM fao_food_price_Indices_monthly GROUP BY name ORDER BY name;
SELECT name, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM fao_food_price_Indices_Monthly_Real GROUP BY name ORDER BY name;
SELECT name, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM sber_consumper_spending_index GROUP BY name ORDER BY name;
SELECT tenor, count() AS n, uniqExact(date) AS n_dates, min(date) AS min_d, max(date) AS max_d
FROM ofz_curve GROUP BY tenor ORDER BY tenor;
```

### 05_holes.sql (ключевая часть; для каждого ряда — свой блок)
```sql
-- дубликаты usd
SELECT date, count() AS n, groupArray(price) AS prices
FROM cbr_currency_usd GROUP BY date HAVING n > 1 ORDER BY date DESC LIMIT 5;

-- пропущенные месяцы (пример: price_expectations; структура повторена для
-- price_expectations_sa, business_climate, cbr_currency_usd, cbr_key_rate)
SELECT 'price_expectations' AS series, m FROM (
  SELECT addMonths(mn, arrayJoin(range(dateDiff('month', mn, mx) + 1))) AS m
  FROM (SELECT toDate(toStartOfMonth(min(date))) AS mn, toDate(toStartOfMonth(max(date))) AS mx
        FROM macro_series WHERE source='cbr_survey' AND series='price_expectations')
) WHERE m NOT IN (
  SELECT toDate(toStartOfMonth(date)) FROM macro_series WHERE source='cbr_survey' AND series='price_expectations'
) ORDER BY m;
-- для usd/key_rate диапазон без хвоста: range(dateDiff('month', mn, mx)) вместо +1
```
Результат: все блоки вернули 0 строк (пропусков нет).

### 06_starts.sql / 09_ladder.sql (greatest/least)
```sql
-- старт S_МП / S_без_МП и границы; фрагмент
SELECT
  greatest(
    toStartOfMonth((SELECT min(date) FROM rosstat_ipc_mes)),
    toStartOfMonth((SELECT min(date) FROM macro_series WHERE source='cbr_survey'
      AND series IN ('price_expectations','price_expectations_sa','business_climate','business_climate_sa'))),
    toStartOfMonth((SELECT min(date) FROM cbr_currency_usd)),
    toStartOfMonth((SELECT min(date) FROM cbr_key_rate)),
    toStartOfMonth((SELECT min(date) FROM fao_food_price_Indices_monthly WHERE name='Food Price Index'))
  ) AS smp_start, ...
```

### 07_coverage.sql (окно статьи и склейка)
```sql
WITH
  (SELECT groupUniqArray(toDate(toStartOfMonth(date))) FROM rosstat_ipc_mes) AS ipc_ms,
  (SELECT groupUniqArray(m) FROM (
      SELECT toDate(toStartOfMonth(date)) AS m FROM macro_series
      WHERE source='cbr_survey' AND series IN ('price_expectations','price_expectations_sa','business_climate','business_climate_sa')
      GROUP BY m HAVING uniqExact(series) = 4)) AS surv_ms,
  (SELECT groupUniqArray(toDate(toStartOfMonth(date))) FROM cbr_currency_usd) AS usd_ms,
  (SELECT groupUniqArray(toDate(toStartOfMonth(date))) FROM cbr_key_rate) AS kr_ms,
  (SELECT groupUniqArray(toDate(toStartOfMonth(date))) FROM fao_food_price_Indices_monthly WHERE name='Food Price Index') AS fao_ms,
  cal AS (SELECT addMonths(toDate('2019-01-01'), number) AS m FROM numbers(67))
SELECT count() AS window_months,
  countIf(has(ipc_ms,m) AND has(surv_ms,m) AND has(usd_ms,m) AND has(kr_ms,m) AND has(fao_ms,m)) AS months_S_MP,
  countIf(has(ipc_ms,m) AND has(usd_ms,m) AND has(kr_ms,m) AND has(fao_ms,m)) AS months_S_no_MP
FROM cal;
```
Результат: `67 | 67 | 67`. Склейка 2013-10…2026-08 (numbers(155)) — `155 | 155`, месяц со всеми флагами true — 155 шт.

### 08_values.sql (sanity)
```sql
SELECT series, count() AS n, min(value) AS v_min, max(value) AS v_max, round(avg(value),2) AS v_avg,
       countIf(value = 0) AS zeros, countIf(isNaN(value)) AS nans
FROM macro_series WHERE source='cbr_survey' AND series IN ('price_expectations','price_expectations_sa','business_climate','business_climate_sa','credit_conditions')
GROUP BY series ORDER BY series;
SELECT min(price) AS p_min, max(price) AS p_max, countIf(price = 0) AS zeros, countIf(isNaN(price)) AS nans FROM cbr_currency_usd;
SELECT min(rate) AS r_min, max(rate) AS r_max, countIf(rate = 0) AS zeros FROM cbr_key_rate;
-- плюс первые/последние точки survey/usd/key_rate/fao/ipc
```
Результат: survey — нулей/NaN нет; usd — min 29.9251 / max 120.3785, нулей нет; key_rate — min 4.25 / max 21, нулей нет.

## Приложение B. Точные ссылки

- Старты/концы всех серий (таблица из 06_starts.out): fao 1990-02, ipc 1991-01, salaries 1991-08, survey (business_climate/credit_conditions/price_expectations +_sa) 2002-01, cpd 2002-02, survey квартальные 2002-04, usd 2013-01, key_rate 2013-09, labour_force 2017-01, sber 2018-12, ruania 2019-01, price_growth_expected 2020-01, staff_availability 2020-04.
- Глубина cbr_currency_usd ограничена источником (комментарий в `cbr/currency_usd.go`: «10.01.2013 — первая дата; лента глубже не отдаёт»).
- `ofz_curve` (1y/3y/5y/10y = 1 точка) — известное ограничение dev-БД, зафиксировано в `scripts/cpi_nowcast.py`; для месячного трека OFZ-признак сейчас негоден (NaN), как и в недельном nowcast.
