# Спека: макропруденциальные требования ЦБ (`key-rate/phase-2-ccyb`, #115)

**Дата:** 2026-10-19
**Пункт:** `key-rate/phase-2-ccyb` — [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) §4.2 №12.
**Issue:** [#115](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/115).
**Роль:** Go-инженер ([go-engineer](../../../.kimi-code/skills/go-engineer/SKILL.md)).

## 1. Цель

Закрыть канал «макропруденциальные требования» контура ключевой ставки: ЦБ называет меры
макропруденциального регулирования в 18 из 22 резюме обсуждения ставки, а в БД этого канала
нет. Пункт добавляет три независимых источника:

1. **АЦН-индикаторы** — 24 месячных ряда индикаторов кредитного цикла (2008-02 … 2024-10).
2. **Надбавки к коэффициентам риска** — матрицы «код требования × код характеристики»,
   действующие и архивные (2013 … 2026).
3. **Макропруденциальные лимиты (МПЛ)** — лимиты банков и МФО по потребительским кредитам
   и ипотеке (2023Q1 … 2026Q4).

## 2. Источники и разведка (2026-10-19)

### 2.1. АЦН-индикаторы

- URL: `https://www.cbr.ru/vfs/analytics/fin_stab/indicators/ccyb_indicators_time_series.xlsx`
- Файл xlsx, 79 889 байт, `Last-Modified: 2024-12-09`, лист `Ряды данных`.
- Строка 2 — даты с колонки D: **201 месяц** `2008-02-01 … 2024-10-01`.
- 24 ряда: 13 показателей, у части есть подстроки; **3 910 непустых точек**.
- Значения в процентах (Н1.0 = 15,667 %) и п.п. (кредитные гэпы); пустая ячейка — точки нет.
- Файл не обновлялся с декабря 2024; последняя точка `2024-10-01` — свойство источника.
- Свод `ccyb_indicators.xlsx` (min/max/last) не импортируется: другие единицы (доли), не time series.

### 2.2. Надбавки к коэффициентам риска

- Landing: `https://www.cbr.ru/finstab/instruments/risk_weight_add_ons/`
- **33 xlsx-ссылки** `/Collection/Collection/File/<id>/<file>.xlsx` с подписями
  «Таблица 1/2 с ДД.ММ.ГГГГ [по ДД.ММ.ГГГГ]» / «с … по …».
- Действующие: `table1_20260904.xlsx`, `table2_20261015.xlsx`,
  `table2_20260904_20261014.xlsx`; архив — 30 файлов с 2018-10-01 по 2026-03-01,
  в старых файлах есть листы с 2013-07-01.
- Лист-матрица: `A1 = «Матрица надбавок…»`; строка 2 — назначение
  («физическим лицам» / «юридическим лицам» / «Период возникновения…»);
  строка `с`/`по` — период; строка `Код` — коды столбцов; строки `2001.i` — коды строк;
  ячейки: число, `н/п`, редко `x`/`х`/`-`.
- Полный справочник кодов — приложение 7 Указания 6960-У; машиночитаемой версии нет
  (PDF — скан). Коды хранятся как в источнике.

### 2.3. МПЛ

- Landing: `https://www.cbr.ru/finstab/instruments/makroprudencialnye-limity/`
- **10 xlsx-ссылок**: 3 файла лимитов банков, 2 файла границ банков, 3 файла лимитов МФО,
  2 файла границ МФО.
- v1 (2023-01 … 2024-03): 4 листа у банков / 2 у МФО, 2 полосы; лимиты — доли
  (`0.25` = 25 %), границы `а/б` — в `Limits_ranges_*`.
- v2 (2024-04 … 2025-06): 4 полосы, лимиты — доли, границы — в `Limits_ranges_*_0107`.
- v3 (с 2025-07): банки — `Потребительские кредиты` + `Кредиты, обеспеченные ипотекой`;
  МФО — `Потребительские займы`; лимиты в процентах; полосы текстом
  (`(50; 80]`, `более 80`, `[0; 50]`, `x`, `-`); у ипотеки есть подстроки,
  наследующие лимит и часть условий.
- Глубина 2023Q1 … 2026Q4.

## 3. Границы пункта

В объёме:

- три импортёра, три таблицы, три витрины, каталог рядов, grants, MCP-проверки;
- шаги в `dagu/cbr.yaml`, тесты на реальных фикстурах, документация.

Вне объёма (осознанно):

- **фактическое значение национальной АЦН** — рядом с индикаторами не публикуется;
  значение берётся из решений/пресс-релизов ЦБ и требует отдельного пункта;
- **НКЛ** — отдельного xlsx-канала не найдено;
- **расшифровка кодов матрицы** — приложение 7 Указания 6960-У только сканом;
- дашборды, ML-признаки, ретро-ревизии сверх архива landing.

## 4. Схема данных

### 4.1. `cbr_ccyb_indicators`

```sql
CREATE TABLE IF NOT EXISTS cbr_ccyb_indicators (
    series LowCardinality(String),
    indicator_no UInt8,
    indicator_group LowCardinality(String),
    date Date,
    value Float64,
    version UInt64
) ENGINE = ReplacingMergeTree(version) ORDER BY (series, date);
```

- `series` — стабильный код, объявленный кодом (стандарт §6.8), например `n10_capital`,
  `credits_gdp`, `nfo_rub_yoy`, `credit_gap_broad`.
- `indicator_no` — номер показателя источника (1…13); `indicator_group` — группа.
- `version` — `uint64(time.Now().UnixNano())` на прогон; по нему отличается no-op.

### 4.2. `cbr_risk_weight_addons`

```sql
CREATE TABLE IF NOT EXISTS cbr_risk_weight_addons (
    matrix LowCardinality(String),
    valid_from Date,
    valid_to Date,
    table_no LowCardinality(String),
    row_code LowCardinality(String),
    col_code LowCardinality(String),
    value Float64,
    is_applicable UInt8,
    source_file LowCardinality(String),
    source_sheet LowCardinality(String),
    source_caption String,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (matrix, valid_from, valid_to, row_code, col_code, source_file);
```

- `matrix`: `retail` (Таблица 1), `corporate` (Таблица 2), `legacy` (общая матрица до 2022).
- `valid_to = 2100-01-01` — сентинел «дальше».
- `is_applicable = 0` — `н/п`/`x`/`х`/`-`/пусто; `1` — число. `0` при `is_applicable=1` — настоящее значение.
- `source_file` в ключе: один период встречается в нескольких архивных файлах; без него
  per-file preflight и подсчёт точек файла ломаются.
- Витрина схлопывает версии разных файлов `argMax(..., version)` по
  `(matrix, valid_from, valid_to, row_code, col_code)`; `table_no` — атрибут победителя.
  Побеждает файл с более поздней датой начала действия документа.

### 4.3. `cbr_mpl`

```sql
CREATE TABLE IF NOT EXISTS cbr_mpl (
    segment LowCardinality(String),
    loan_class LowCardinality(String),
    period_from Date,
    period_to Date,
    source_file LowCardinality(String),
    source_sheet LowCardinality(String),
    row_no UInt32,
    subrow_no LowCardinality(String),
    band_idx UInt8,
    loan_kind String,
    pdn_band LowCardinality(String), pdn_min Nullable(Float64), pdn_max Nullable(Float64),
    term_band LowCardinality(String), term_min Nullable(Float64), term_max Nullable(Float64),
    down_payment_band LowCardinality(String), down_payment_min Nullable(Float64), down_payment_max Nullable(Float64),
    ltv_band LowCardinality(String), ltv_min Nullable(Float64), ltv_max Nullable(Float64),
    amount_band LowCardinality(String), amount_min Nullable(Float64), amount_max Nullable(Float64),
    limit_pct Float64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (segment, loan_class, period_from, period_to, source_file, row_no, subrow_no, band_idx);
```

- `loan_class`: v1/v2 — `consumer_with_limit` / `consumer_other`; v3 — `consumer` /
  `mortgage` / `consumer_loan`.
- `limit_pct` — всегда проценты: v1/v2 доли ×100, v3 как есть.
- `period_to = 2100-01-01` — «дальше».
- Одна строка v1/v2 — одна полоса; одна строка v3 — одно правило с набором полос.
- Файлы границ `Limits_ranges_*` читаются в память и в таблицу не пишутся.

## 5. Каталог и витрины

### 5.1. Каталог

- `cbr/ccyb_meta.go` — 24 `util.SeriesMeta` (source `cbr_ccyb_indicators`).
- `cbr/risk_weight_addons_meta.go` — 3 записи: `retail`, `corporate`, `legacy`
  (source `cbr_risk_weight_addons`, unit «п.п.», frequency `irregular`).
- `cbr/mpl_meta.go` — 7 записей: `bank_consumer`, `bank_consumer_with_limit`,
  `bank_consumer_other`, `bank_mortgage`, `mfo_consumer_with_limit`,
  `mfo_consumer_other`, `mfo_consumer_loan` (source `cbr_mpl`, unit `%`, frequency `Q`).

### 5.2. Витрины

- `v_cbr_ccyb_indicators`: `source, series, indicator_no, indicator_group, date, value`.
- `v_cbr_risk_weight_addons`: `source, matrix, series, valid_from, valid_to, table_no,
  row_code, col_code, value, is_applicable, source_file, source_caption`; `argMax` по `version`.
- `v_cbr_mpl`: `source, series (= segment || '_' || loan_class), segment, loan_class,
  period_from, period_to, row_no, subrow_no, loan_kind, <band/min/max>, limit_pct, source_file`.

Все витрины — с комментариями таблицы и каждой колонки; `GRANT SELECT` в
`sql/mcp_kimi_reader.sql`; сырые таблицы агенту не выдаются.

## 6. Разбор

### 6.1. CCyB

- `xlsx.GetRows("Ряды данных", excelize.Options{RawCellValue: true})`.
- Даты — строка 2 с колонки D; серийное число Excel → `excelize.ExcelDateToTime`;
  текстовые даты — `02.01.2006` / `2006-01-02`.
- Реестр 24 рядов объявлен кодом; группы — по строкам-заголовкам 4, 10, 18, 22, 27
  (с номером) и 30 (секция «Кредитные гэпы»); самостоятельные показатели — 8, 17, 21, 26,
  31–34; подстроки наследуют номер и группу.
- Нормализация подписи: trim, снятие `¹`, схлопывание пробелов.
- Пустая ячейка — точки нет; нечисловая непустая — ошибка с рядом и датой.
- Строка со значениями, которой нет в реестре, — ошибка (новый ряд требует описания).

### 6.2. Надбавки

- Страница: goquery, только `href` с `/Collection/Collection/File/` и `.xlsx`.
- Подпись: `document_from` — первая дата; файлы сортируются по `document_from` возрастанию
  (старые первыми), при равенстве — по basename: это порядок версий для витрины.
- Для каждого файла — локальная копия `util.ClickHouseImport` с `DataUrl`, `ImportFunc`,
  per-file `PublicationCountPoints`, `PublicationSource = cbr_risk_weight_addons`,
  `AfterImport = nil`.
- Лист `Оглавление` пропускается; для матрицы ищется строка `Код`, коды столбцов — правее;
  строка `с`/`по` даёт `valid_from`/`valid_to`; `matrix` — по строке 2; `table_no` — regex
  `Таблица №\s*(\d+)`.
- Строки: `A` соответствует `^\d{4}\.i$`; `н/п`/`x`/`х`/`-`/пусто →
  `is_applicable=0, value=0`; число → `is_applicable=1`; иное — ошибка.
- `ImportFunc` возвращает ошибку, если файл не дал ни одной строки.
- После `local.Import` — `SELECT count() FROM cbr_risk_weight_addons WHERE version = ?`
  (без `FINAL`): 0 → no-op, >0 → прибавить к счёту. Шаблон на no-op возвращает записанное
  число точек, поэтому его возврат для счёта не используется.
- Ошибки по файлам копятся `errors.Join`; остальные файлы продолжаются.

### 6.3. МПЛ

- Страница: все `href` с `/Content/Document/File/` и `.xlsx`; классификация по подписи
  (limit/ranges, bank/mfo); `document_from` — первая дата.
- Сначала ranges-файлы (`util.GetXlsx`) → map `(segment, dimension, period_from, period_to)
  -> {a,b,c,d}`; dimension — из листа `ПДН`/`Срок`; `х`/`x` → nil.
- Затем limit-файлы по возрастанию `document_from`, локальные копии шаблона с per-file
  preflight и `ImportFunc`.
- v3: листы `Потребительские кредиты`, `Кредиты, обеспеченные ипотекой`,
  `Потребительские займы`; header row 1, subheader row 2, data row 3.
- v1/v2: header row 2, subheader row 3, data row 4; полосы — между «По» и «Тип…».
- v1/v2: `loan_class`/`dimension` из имени листа; для каждой числовой полосы —
  `band_idx`, `loan_kind`, `limit_pct = число ×100` (если число ≤ 1; > 1 — warn);
  границы — из ranges map: 2 полосы → `(a,b]`, `(b,∞)`; 4 → `(a,b]`, `(b,c]`, `(c,d]`,
  `(d,∞)`; `х` → nil; `band` — нормализованная строка или исходная подпись.
- v3: `loan_class` по листу; родительская строка задаёт `row_no`, `period_from/to`,
  `loan_kind`, `limit_pct`; подстроки наследуют пустые поля; полосы парсятся из текста;
  `limit_pct` как есть; одна строка на подстроку, `band_idx=0`.
- Ошибки: нет header-строки, нет периода, нечисловой лимит, неизвестный формат — ошибка
  с файлом/листом/строкой. Неизвестная текстовая полоса v3 — warn + `band` как есть.
- После `local.Import` — та же проверка `version`.
- Каталог/витрина публикуются в конце основного `Import()` всегда (самолечение).

## 7. Точки контроля публикации и no-op

- CCyB: `PublicationCountPoints = SELECT count() FROM cbr_ccyb_indicators FINAL`,
  стабильный URL, HEAD-префлайт.
- Надбавки: базовая запись реестра — глобальный count; локальная копия на файл —
  `... WHERE source_file = '<basename>'` и `PublicationSource = cbr_risk_weight_addons`.
- МПЛ: базовая запись реестра — глобальный count; локальная копия на limit-файл —
  `... WHERE source_file = '<basename>'` и `PublicationSource = cbr_mpl`.
- Это отступление от буквы §9.1 (per-file preflight вместо одного артефакта) названо здесь;
  оно покрыто тестом реестра и протоколом приёмки.
- `version`-проверка после `local.Import` даёт честный `Imported 0 rows` на повторе.

## 8. Тесты и фикстуры

- `cbr/testdata/ccyb_indicators_time_series.xlsx` — реальный файл/срез.
- `cbr/testdata/risk_weight_addons_table1_20260904.xlsx`,
  `…table2_20261015.xlsx`, `…2018.10.01_2019.09.30.xlsx`,
  `risk_weight_addons_page.html` — реальные файлы/фрагмент.
- `cbr/testdata/mpl_banks_2025.xlsx`, `…banks_2024.xlsx`, `…banks_2023.xlsx`,
  `…mfo_2025.xlsx`, `…ranges_banks_2023.xlsx`, `…ranges_banks_2024.xlsx`,
  `mpl_page.html` — реальные файлы/фрагмент.
- Тесты: `TestCcybParse`, `TestRiskWeightAddonsPage`, `TestRiskWeightAddonsParse`,
  `TestMPLPage`, `TestMPLParse`, `TestMPLRanges`.
- Spot-проверки: CCyB — 3 910 точек/24 ряда; надбавки — `retail 2002.i×1000.i = 1.3`,
  `retail 2002.i×1008.i` not applicable, `corporate 8002.i×8002.i = 1`,
  `legacy 2004.i×1000.i = 0.6`; МПЛ — `0.25 → 25`, `более 50 → pdn_min=50`,
  `period_to=2100-01-01` для «дальше».

## 9. Приёмка

- `make all`, `make docs-check`, `dagu validate dagu/cbr.yaml`.
- `make import STAT=cbr_ccyb_indicators` — 3 910 строк, повтор `Imported 0 rows`.
- `make import STAT=cbr_risk_weight_addons` — 33 файла, повтор `Imported 0 rows`.
- `make import STAT=cbr_mpl` — 10 файлов, повтор `Imported 0 rows`.
- `make mcp-check` — три витрины читаются, каталоги 24/3/7, сырые таблицы закрыты 497.
- Протокол — комментарием в PR; `Closes #115` вне backticks.

## 10. Риски

- АЦН-файл устарел (2024-10-01) — свойство источника; зафиксировано в каталоге.
- Фактическое значение АЦН не импортируется — отдельный пункт.
- Коды матрицы без расшифровки — приложение 7 только сканом.
- 33 + 6 HEAD-запросов в день и 4 ranges-файла; при троттлинге — пауза/weekly DAG.
- Дрейф форматов МПЛ; парсер ищет якоря, v3-полоса — warn, не падение.
- Пересечение периодов матриц: `source_file` в ключе, витрина `argMax` по `version`;
  `valid_to` может отличаться — обе версии остаются как point-in-time.
