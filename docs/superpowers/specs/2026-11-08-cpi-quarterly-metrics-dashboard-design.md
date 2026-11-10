# Квартальные метрики ЦБ по ИПЦ: ряды и панели — спека

**Дата:** 8 ноября 2026 г.
**Роль:** Go.
**Пункт:** предлагаемый `key-rate/phase-4-cpi-quarterly-metrics`; issue на момент написания не заведён — перед реализацией завести по [github-tracker](../../../.kimi-code/skills/github-tracker/SKILL.md), заголовок `[key-rate/phase-4-cpi-quarterly-metrics] Квартальные метрики ЦБ: ряды и панели`.
**Зависимости:** нет. Входы уже в БД: `cbr_dip_bulletin`, `cbr_indicators_cpd`, `weekly_cpi_sa`; витрины `v_cbr_dip_bulletin`, `v_cbr_macro`, `v_weekly_cpi_sa` — в проде.
**Связанные документы:** [KEY_RATE_PUBLICATIONS.md](../../KEY_RATE_PUBLICATIONS.md) §4.7 и §6, [KEY_RATE_CHANNELS.md](https://github.com/kmlebedev/bi-ml-models/blob/main/docs/models/key-rate-channels.md) §8, [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.3/§6.5, [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) фаза 4.

## 1. Контекст и цель

Обновление статьи по ИПЦ и траектории инфляции опирается на квартальные оценки, которые ЦБ сам называет в резюме обсуждения: текущий рост цен SAAR, базовая инфляция SAAR, диапазон устойчивого роста. На срезе 23.10.2026 это **8,0 %**, **6,2 %** и **5–6 %** за 3к26; для 4к26 официальных месячных данных ещё нет, и трекинг ведётся недельным SA-агрегатом.

**Коротко для реализации.**
- 4 производные витрины: `v_cbr_dip_quarterly`, `v_cbr_core_quarterly`, `v_weekly_cpi_quarterly`, `v_weekly_cpi_rolling3m`.
- 4 ряда каталога: `cbr_dip_bulletin:saar_q`, `cbr:Базовый ИПЦ:SAAR_q`, `weekly_cpi:saar_q`, `weekly_cpi:saar_3m`.
- Гранты `kimi_reader` на четыре витрины; сырые таблицы закрыты.
- Дашборд: новая строка `Id(45)` «Квартальные метрики ЦБ», панели **41–44**; существующие id не двигать.
- Новых таблиц, импортёров и изменений DDL нет.

Сейчас эти числа считаются вручную SQL-запросами: квартальное среднее по ДИП, квартальное среднее по базовому ИПЦ, «квартал к дате» и 3-месячное скользящее по недельному ряду. Цель — закрепить расчёт в витринах, описать ряды в каталоге и вывести на дашборд «Ключевая ставка — публикации» четыре панели, чтобы статья и её следующее обновление брали числа из одного места.

**Что делаем:** четыре производные витрины, четыре ряда каталога, гранты `kimi_reader`, одна новая строка дашборда и четыре панели.
**Чего не делаем:** новых сырых таблиц, новых импортёров, изменений DDL сырых таблиц, изменений моделей X-13 и ДИП, ML-расчётов, текста статьи.

## 2. Метрики и формулы

| Метрика | Определение | Формула | Источник |
|---|---|---|---|
| Текущий рост цен, квартальный SAAR | Среднее месячных SAAR ДИП за квартал | `avg(value)` по `saar_all_goods` за месяцы квартала | `cbr_dip_bulletin` |
| Базовая инфляция, квартальный SAAR | Среднее месячных годовых темпов из SA м/м | `avg((1 + value/100)^12 − 1) × 100` | `cbr_indicators_cpd`, `name='Базовый ИПЦ'` |
| 4к26 к дате | Годовой пересчёт одного 4-недельного SA-окна | `((1 + sa_4w/100)^13 − 1) × 100` | `weekly_cpi_sa`, `partial=1` |
| 3м SAAR недельного агрегата | Цепочка трёх 4-недельных SA-темпов, годовой пересчёт | `(Π(1 + sa_4w_i/100))^(52/12) − 1) × 100` | `weekly_cpi_sa` |
| Устойчивый рост цен | Диапазон, не точка | — | резюме ЦБ, текст |

**Правила.**
1. Квартальное среднее считается только по полным кварталам (`months = 3`); неполный квартал (например, 3к26 в выпуске 26-06 — один июль) помечается `months < 3` и в сравнение не идёт.
2. Винтажность ДИП сохраняется: у квартала есть `release` и `publication_date`; панель берёт последний выпуск, статья — выпуск на дату резюме. Одно и то же число разных выпусков — разные строки, а не перезапись.
3. 4к26 — не закрытый квартал: `partial = 1`, `as_of` — последняя использованная неделя. Не смешивать с полными кварталами и не подписывать как «оценка ЦБ».
4. У базовой инфляции винтажей в БД нет (`cbr_indicators_cpd` — текущая редакция файла); это оговаривается в описании ряда.
5. Устойчивый рост 5–6 % не получает числового ряда: это диапазон, а не точка. В таблице — текст, в графике — только справочная линия/подпись.

## 3. Ряды: что уже есть и что завести

### 3.1. Существующие входы (менять не нужно)

| Ряд | Витрина | Частота | Что даёт |
|---|---|---|---|
| `cbr_dip_bulletin` / `saar_all_goods` | `v_cbr_dip_bulletin` | M | месячный SAAR, винтажи `release`/`publication_date` |
| `cbr` / `Базовый ИПЦ` | `v_cbr_macro` | M | SA м/м базовой инфляции |
| `weekly_cpi_sa` / `weekly_cpi:sa_4w` | `v_weekly_cpi_sa` | M | 4-недельный SA-темп, `as_of`, `partial` |
| `weekly_cpi_sa` / `weekly_cpi:saar` | `v_weekly_cpi_sa` | M | месячный SAAR недельного агрегата |

### 3.2. Новые производные ряды

| # | source / series | Витрина | Частота | Единица | Что это |
|---|---|---|---|---|---|
| 1 | `cbr_dip_bulletin` / `saar_q` | `v_cbr_dip_quarterly` | Q | `% годовых` | квартальное среднее ДИП по выпуску |
| 2 | `cbr` / `Базовый ИПЦ:SAAR_q` | `v_cbr_core_quarterly` | Q | `% годовых` | квартальное среднее базовой SAAR |
| 3 | `weekly_cpi_sa` / `weekly_cpi:saar_q` | `v_weekly_cpi_quarterly` | Q | `% годовых` | квартальное среднее недельного SAAR (4к26 — `partial=1`) |
| 4 | `weekly_cpi_sa` / `weekly_cpi:saar_3m` | `v_weekly_cpi_rolling3m` | M | `% годовых` | 3-месячный скользящий SAAR |

Все четыре — **производные витрины**, не таблицы. Сырые таблицы остаются закрытыми для `kimi_reader`.

## 4. SQL витрин

Стиль — `util.View` с `Tables`, `Select`, `Comment`, `Columns`; публикация — из `AfterImport` владеющего импортёра, чтобы витрина и каталог self-heal на no-op-пути (как `weeklysa.publishWeeklyDerived`).

### 4.1. `v_cbr_dip_quarterly`

```sql
SELECT
    release,
    release_id,
    publication_date,
    toStartOfQuarter(date) AS quarter_start,
    count()                AS months,
    avg(value)             AS saar_q
FROM cbr_dip_bulletin FINAL
WHERE series = 'saar_all_goods'
GROUP BY release, release_id, publication_date, quarter_start
```

Публикуется из `cbr/views.go`: определение `cbrDipQuarterlyView` рядом с `cbrDipView`, создание — в `afterCbrDip` (тот же `AfterImport` у `cbr_dip_bulletin`). `FINAL` обязателен: таблица `ReplacingMergeTree` с ключом `(series, date, release_id)`. `months` — число месяцев в квартале у этого выпуска; `months = 3` — полный квартал.

### 4.2. `v_cbr_core_quarterly`

```sql
SELECT
    toStartOfQuarter(date)                          AS quarter_start,
    count()                                         AS months,
    avg(((1 + value / 100) ^ 12 - 1) * 100)         AS saar_q,
    max(date)                                       AS last_month
FROM cbr_indicators_cpd FINAL
WHERE name = 'Базовый ИПЦ'
GROUP BY quarter_start
```

Публикуется из `cbr/views.go` (`cbrCoreQuarterlyView`) + `cbr/indicators_cpd.go`: текущий `AfterImport` — `afterCbrMacro(cbrIndicatorsCpdSeriesMeta)`; обернуть его функцией, которая после каталога `v_cbr_macro` создаёт `v_cbr_core_quarterly` и пишет её ряд каталога. `value` — уже SA м/м в процентах; повторная сезонная корректировка не применяется. Винтажей нет — в `Comment` и каталоге написать «текущая редакция файла».

### 4.3. `v_weekly_cpi_quarterly`

```sql
SELECT
    toStartOfQuarter(date) AS quarter_start,
    count()                AS months,
    avg(saar)              AS saar_q,
    max(as_of)             AS as_of,
    max(partial)           AS partial
FROM weekly_cpi_sa FINAL
GROUP BY quarter_start
```

Публикуется из `weeklysa/importer.go` (`publishWeeklyDerived`). Для 4к26 сейчас одна строка: `months = 1`, `partial = 1`, `as_of = 2026-10-26`. Для полных кварталов `partial = 0`.

### 4.4. `v_weekly_cpi_rolling3m`

```sql
SELECT
    date,
    as_of,
    partial,
    if(count() OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) = 3,
       (exp(sum(log(1 + sa_4w / 100)) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) ^ (52.0 / 12.0) - 1) * 100,
       NULL) AS saar_3m
FROM weekly_cpi_sa FINAL
```

Публикуется оттуда же. Первые две точки — `NULL` (окно неполное), это не ноль. `date` — конец месяца; три строки = 12 недель, поэтому степень `52/12`, а не `13`.

### 4.5. Опционально: единая витрина для таблицы

Если табличная панель 42 не хочет собирать `UNION ALL` в запросе, можно добавить `v_cpi_quarterly_metrics` в длинной форме:

```sql
SELECT 'current' AS metric, quarter_start, saar_q AS value, toUInt8(months < 3) AS partial, 'cbr_dip' AS source
FROM v_cbr_dip_quarterly
UNION ALL
SELECT 'core', quarter_start, saar_q, toUInt8(months < 3), 'cbr_core' FROM v_cbr_core_quarterly
UNION ALL
SELECT 'weekly', quarter_start, saar_q, partial, 'weekly' FROM v_weekly_cpi_quarterly
```

Рекомендация: **не добавлять**, пока панель собирается одним запросом; лишняя витрина — лишний контракт. Если добавлять, то с явным `source` и комментарием, что `current` и `core` — официальные, `weekly` — наш трекер.

## 5. Каталог и гранты

### 5.1. `series_catalog`

Добавить четыре записи через `util.UpsertSeriesCatalog` из тех же `AfterImport`:

| source | series | title | unit | frequency | origin | description (кратко) |
|---|---|---|---|---|---|---|
| `cbr_dip_bulletin` | `saar_q` | Текущий рост цен: квартальный SAAR | `% годовых` | `Q` | приложения ДИП ЦБ, `stat_bulletin_*.xlsx` | среднее месячных SAAR за квартал; винтаж — `release`/`publication_date`; `months < 3` — неполный квартал |
| `cbr` | `Базовый ИПЦ:SAAR_q` | Базовая инфляция: квартальный SAAR | `% годовых` | `Q` | `indicators_cpd.xlsx`, лист «Лист1», строка «Базовый ИПЦ» | среднее `(1 + SA м/м/100)^12 − 1`; текущая редакция файла, винтажей нет |
| `weekly_cpi_sa` | `weekly_cpi:saar_q` | Недельный SA-агрегат: квартальный SAAR | `% годовых` | `Q` | `weekly_cpi_sa` | среднее месячных `saar`; 4к26 — `partial = 1`, неполный квартал |
| `weekly_cpi_sa` | `weekly_cpi:saar_3m` | Недельный SA-агрегат: 3-месячный SAAR | `% годовых` | `M` | `weekly_cpi_sa` | цепочка трёх `sa_4w`, годовой пересчёт `^(52/12)`; первые две точки `NULL` |

В описаниях обязательно оговорить: недельный ряд — **наша конструкция**, а не ряд ЦБ; за 3к26 он даёт 6,92 % против 8,02 % у ЦБ из-за сглаживания одного агрегата на 4-недельном окне. Это не ошибка и не повод подгонять.

### 5.2. Гранты

В [sql/mcp_kimi_reader.sql](../../../sql/mcp_kimi_reader.sql) добавить:

```sql
GRANT SELECT ON default.v_cbr_dip_quarterly TO kimi_reader;
GRANT SELECT ON default.v_cbr_core_quarterly TO kimi_reader;
GRANT SELECT ON default.v_weekly_cpi_quarterly TO kimi_reader;
GRANT SELECT ON default.v_weekly_cpi_rolling3m TO kimi_reader;
```

Сырые `cbr_dip_bulletin`, `cbr_indicators_cpd`, `weekly_cpi_sa` агенту не выдавать. Проверка — `make mcp-user`, `make mcp-check`; в `scripts/mcp_check.py` добавить проверку четырёх витрин (счётчик строк и последняя дата).

## 6. Панели дашборда

Дашборд — `grafana/key_rate_publications.go`, артефакт — `dashboard/key-rate-publications.json` (собирается `make grafana-build`, руками не правится). Текущие ряды: 21, 22, 23, 24, 34, 40; панели 1–39. Свободные id панелей — 41+, свободный id ряда — 45.

**Новая строка** «Квартальные метрики ЦБ», `Id(45)`, в конце дашборда (после ряда 40). Панели 41–44. Существующие id и порядок не менять.

### 6.1. Панель 41 — «Квартальный SAAR: текущий рост и базовая»

- Тип: `timeseries` (линия/точки), `Span(spanHalf)`.
- Единица: `percent`; порог 4 % — линия цели (`krFourPercentThresholds`, `line`).
- Серии:
  - A — `v_cbr_dip_quarterly`: последний `release`, `months = 3`, `quarter_start` по оси времени; подпись «Текущий рост цен, SAAR (ЦБ)».
  - B — `v_cbr_core_quarterly`: `months = 3`; подпись «Базовая инфляция, SAAR (ЦБ)».
  - C — `v_weekly_cpi_quarterly`: `partial = 1`; подпись «4к26 к дате (наш недельный)», точка, пунктир/другой цвет.
- Описание панели: формулы, «4к26 — неполный квартал, одна 4-недельная точка на 26.10; официальная базовая появится 14.11».
- Не показывать на этой панели квартальные значения недельной модели за 1к26–3к26: другая конструкция, иначе линия будет читаться как ошибка ЦБ. Они идут в таблицу 42 как справочные.

### 6.2. Панель 42 — «4к26 к дате: квартал и 3 месяца»

- Тип: `table`, `Span(spanHalf)`.
- Колонки: `Метрика`, `3к26`, `4к26 к дате`, `Δ 4к26 − 3к26`, `3м SAAR`, `Δ 3м`.
- Строки:
  - «Текущий рост цен»: 8,0 / 12,9 / +4,9 / 8,1 / +0,1.
  - «Базовая инфляция»: 6,2 / «н/д до 14.11» / «—» / «н/д» / «—».
  - «Устойчивый рост цен»: «5–6 % (диапазон)» / «н/д» / «—» / «—» / «—».
- Значения — из `v_cbr_dip_quarterly` (последний release, 3к26), `v_weekly_cpi_quarterly` (4к26, `partial=1`), `v_weekly_cpi_rolling3m` (последняя точка). Δ 3м — против 3м окна ЦБ (июль–сентябрь, 8,04 %), а не против собственного сентября недельного ряда; в описании панели дать оба: «на собственной базе недельного ряда 3м вырос с 6,91 % до 8,15 % (+1,24 п.п.)».
- 3м окно ЦБ отдельным рядом не заводим: это справочная база для Δ, считается в запросе панели из трёх месячных SA `Все товары и услуги`:
  ```sql
  SELECT (exp(sum(log(1 + value / 100))) ^ 4 - 1) * 100
  FROM v_cbr_macro
  WHERE series = 'Все товары и услуги'
    AND date IN ('2026-07-31', '2026-08-31', '2026-09-30')
  ```
  Форма запроса панели (значения не хардкодить; `NULL` → «н/д» через `ifNull(toString(...), 'н/д')`):
  ```sql
  WITH q3 AS (SELECT saar_q FROM v_cbr_dip_quarterly
              WHERE release = (SELECT max(release) FROM v_cbr_dip_quarterly)
                AND quarter_start = '2026-07-01' AND months = 3),
       q4 AS (SELECT saar_q FROM v_weekly_cpi_quarterly
              WHERE quarter_start = '2026-10-01' AND partial = 1),
       m3 AS (SELECT saar_3m FROM v_weekly_cpi_rolling3m ORDER BY date DESC LIMIT 1)
  SELECT 'Текущий рост цен' AS metric,
         (SELECT round(saar_q, 2) FROM q3) AS q3,
         (SELECT round(saar_q, 2) FROM q4) AS q4,
         (SELECT round(saar_q - (SELECT saar_q FROM q3), 1) FROM q4) AS delta,
         (SELECT round(saar_3m, 2) FROM m3) AS m3
  UNION ALL
  SELECT 'Базовая инфляция',
         (SELECT round(saar_q, 2) FROM v_cbr_core_quarterly
          WHERE quarter_start = '2026-07-01' AND months = 3),
         NULL, NULL, NULL
  UNION ALL
  SELECT 'Устойчивый рост цен', NULL, NULL, NULL, NULL
  ```
  Δ 3м в этой заготовке не показан: он требует подзапроса `m3_cbr` (окно ЦБ выше); добавить его при реализации, чтобы не плодить литералы.
- Пустые значения — строка «н/д», не ноль и не пропуск. Числа в таблице — один знак после запятой; 8,02 → 8,0, 4,89 → 4,9, 12,88 → 12,9.

### 6.3. Панель 43 — «Недельный ИПЦ: 3-месячный SAAR»

- Тип: `timeseries`, `Span(spanHalf)`.
- Серия: `v_weekly_cpi_rolling3m`, `date` по оси, `saar_3m`; последняя точка — `partial = 1`, подпись «к дате».
- Единица: `percent`; порог 4 %; описание: «12-недельное окно, цепочка трёх 4-недельных SA-темпов; не официальный SAAR ЦБ; октябрьский всплеск — тарифная неделя 05.10».
- Если точка `NULL` (первые два месяца) — разрыв, не ноль.

### 6.4. Панель 44 — «Оценки ЦБ по кварталам: ревизии»

- Тип: `table`, `Span(24)` (полная ширина; отдельной константы `spanFull` в пакете нет).
- Строки: последние 8 выпусков ДИП; колонки `Выпуск`, `Опубликован`, `1к26`, `2к26`, `3к26`, `Месяцев в квартале`.
- SQL: `v_cbr_dip_quarterly`, фильтр `quarter_start >= '2026-01-01'`, `maxIf(saar_q, quarter_start = ... AND months = 3)`; выпуски с `months < 3` показываются, но значение квартала — «—» (или отдельная колонка `months`).
- Смысл: видно, как ЦБ ревизует один и тот же квартал между выпусками (2к26: 5,02 → 5,36 → 4,89; 1к26: 8,82 → 8,74 → 8,81 → 8,71 → 8,70; 3к26: 11,59 в 26-06 — один месяц, 8,02 в 26-07 — три).
- Описание: «разные выпуски — разные редакции; сравнивать в пределах одной даты публикации».

### 6.5. Тесты дашборда

- `TestKeyRatePublicationsPanelCount` — обновить ожидаемое число панелей (+4).
- `TestKeyRatePanelIDsUnique` — новые id 41–44 не пересекаются с 1–39 и рядом 45.
- `TestKeyRateAllTargetsUseViews` — все запросы читают `v_*`.
- `TestKeyRateAllTimeseriesUseTimeFilter` — панели 41 и 43 с `$__timeFilter`.
- `TestKeyRateUnits` — панели 41/43 в `percent`, 42/44 — table.
- Новый тест `TestKeyRateQuarterlyPanelUsesLatestRelease` — запрос панели 41 содержит `release = (SELECT max(release)` и `months = 3`.
- Новый тест `TestKeyRateQuarterlyTableNoZeros` — в SQL панели 42 нет `0` для отсутствующей базовой; «н/д» формируется на стороне запроса.

## 7. Приёмка

1. `make import STAT=cbr_dip_bulletin` (no-op) и `make import STAT=weekly_cpi_sa` (no-op) создают витрины и каталог; повтор — no-op, витрины на месте.
2. Числа:
   - `v_cbr_dip_quarterly`, 26-07: Q1 **8,70**, Q2 **4,89**, Q3 **8,02**; 26-06 Q3 — **11,59**, `months = 1`.
   - `v_cbr_core_quarterly`: Q1 **6,06**, Q2 **4,57**, Q3 **6,21**.
   - `v_weekly_cpi_quarterly`: Q1 **10,80**, Q2 **6,83**, Q3 **6,92**, Q4 **12,88** (`partial = 1`, `as_of = 2026-10-26`).
   - `v_weekly_cpi_rolling3m`: последняя **8,15**; сентябрь **6,91**; первые две точки `NULL`.
3. `make all` — зелёный; `make docs-check` — зелёный; `make grafana-build` — артефакт обновлён детерминированно; `make grafana-validate` — зелёный; `make mcp-check` — четыре новые витрины видны, сырые таблицы закрыты.
4. Дашборд: панели 41–44 отрисованы, 41 показывает 8,02/6,21 и точку 12,88; 42 — +4,9 и +0,1; 43 — 8,15; 44 — строку 26-07 с 8,70/4,89/8,02.
5. В том же PR: `ARCHITECTURE.md` §6.3 (витрины), §6.5 (MCP/гранты), `README.md` (панели), `KEY_RATE_PUBLICATIONS.md` §6/§11 (панели 41–44), `dashboard/key-rate-publications.json` (артефакт сборки).

## 8. Границы и риски

- **Не смешивать официальное и наше.** Квартальные 8,0/6,2 — ЦБ; 4к26 12,9 и 3м 8,1 — наш недельный ряд. На панели 41 они разными сериями и разными подписями; в тексте статьи — с явной атрибуцией.
- **Неполный квартал — не квартал.** 3к26 в 26-06 (11,59) — один июль; 2к26 в 26-04 (2,39) — один апрель. Фильтр `months = 3`; в таблице 44 — прочерк.
- **4к26 — не закрытый квартал.** 12,9 — одна 4-недельная точка; по мере выхода ноября–декабря она изменится. Не подписывать «оценка ЦБ».
- **Базовая за октябрь — только 14.11.** До этого `v_cbr_core_quarterly` не имеет строки 4к26; панель 42 показывает «н/д», не 0.
- **Винтажи ДИП.** Один и тот же квартал в разных выпусках — разные строки; `max(release)` в панели 41 и `release` в статье могут расходиться, если статья цитирует резюме на дату. В описании панели указать, какой выпуск выбран.
- **Уровневый разрыв недельного ряда.** За 3к26 недельная модель даёт 6,92 % против 8,02 % ЦБ; за 1к26 — 10,80 % против 8,70 %. Поэтому квартальные значения недельной модели не выводятся на панель 41; 3м на панели 43 живёт своей жизнью.
- **Ряд/панель id.** Ряд 45 и панели 41–44; id не пересекаются. Порядок панелей — только в конце, существующие не двигать.
- **Границы DDL.** Новых таблиц нет; только `CREATE OR REPLACE VIEW`. Если появится соблазн материализовать — сначала спека и ARCHITECTURE §6.2, не в этом пункте.

## 9. Порядок работ

1. Issue и ветка `feat/cpi-quarterly-metrics` (WIP=1).
2. Витрины + каталог в трёх импортёрах (`cbr/dip_bulletin.go`, `cbr/indicators_cpd.go`, `weeklysa/importer.go`); тесты SQL/каталога.
3. Гранты, `scripts/mcp_check.py`, `make mcp-check`.
4. Панели 41–44, тесты дашборда, `make grafana-build`, `make grafana-validate`.
5. `ARCHITECTURE.md`, `README.md`, `KEY_RATE_PUBLICATIONS.md`, артефакт дашборда.
6. `make all`, `make docs-check`, `make mcp-check`; протокол — комментарием в PR.

## 10. Поправки при реализации (2026-11-08)

Сверка спеки с кодом и dev-БД на дату реализации дала пять расхождений; они исправлены в коде и внесены сюда, чтобы документ не расходился с артефактом.

1. **`^` — не степень.** В ClickHouse `^` — побитовый XOR: §4.2 и §4.4 в исходной записи падают с `SYNTAX_ERROR`. В витринах — `pow(x, y)` и `pow(exp(...), 52.0/12.0)`.
2. **id панелей 42–45, ряд 46.** id панели **41 занят** теплокартой «Недельный ИПЦ» (infra/5.14), поэтому панели взяли следующий свободный диапазон, а ряд — 46 (иначе его id совпал бы с панелью 45).
3. **Колонка `quarter`.** К `quarter_start` добавлена читаемая метка `quarter` («3к26») и комментарий колонки: у квартальных рядов каталога принята привязка «1-е число квартала, следующего за отчётным» (`v_rosstat_vvp_kvartal`), и без метки редактор прочитал бы 2026-07-01 как II квартал. Витрины остаются ключом по `quarter_start`.
4. **Каталог: 8 → 9 и 3 → 5.** Квартальный ряд ДИП публикуется отдельным срезом `cbrDipQuarterlySeriesMeta` (тест `TestDipSeriesMetaFields` требует `unit = "%"` и `frequency = "M"` у всех рядов месячного списка), поэтому у `cbr_dip_bulletin` в каталоге 9 рядов, у `weekly_cpi_sa` — 5. Точные равенства в `scripts/mcp_check.py` обновлены.
5. **Порог 4 % на графиках — в `percentunit`.** Значения новых панелей отдаются как `saar_q / 100`, поэтому переиспользован `krFourPercentThresholds` (шаг `0.04`), а не новый порог со значением `4`.

Дополнительно, вне спеки: шаблон `util.ClickHouseImport` не зовёт `AfterImport` на no-op-пути, поэтому у `cbr_dip_bulletin` производные публикует собственная no-op-ветка `Import()`, а у `cbr_indicators_cpd` — обёртка `Import()`; без этого на уже заполненной БД новые витрины не появились бы до следующего выпуска источника.
