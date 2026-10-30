# План: T2 #277 — `rosstat_ppi_product` и `rosstat_ppi_weights`

**Пункт:** `key-rate/phase-4-rosstat-ppi-sa`, T2 (issue [#277](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/277))
**Umbrella:** [#266](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/266)
**Research:** `docs/research/2026-10-20-ppi-sa-methodology.md` (PR #275)
**T1 (готов):** `rosstat_ppi_ved`, PR #294, спека/план `docs/superpowers/{specs,plans}/2026-10-20-rosstat-ppi-ved*`
**Ветка:** `feat/rosstat-ppi-product`; worktree `.worktrees/rosstat-ppi-product` от `origin/main`
**Роль:** `go`

---

## 1. Решения по объёму (согласованы с владельцем)

1. **Товарные ряды — только месячные листы 2021–2026, все 4 типа индексов** (`m/m`, `ytd`, `y/y`, `period`). Годовые срезы 1998–2025 **не импортируются**: листы `1`–`2` не содержат кодов ОКПД2, лист `3` (2017–2025) частично дублирует месячные 2021–2025 и добавил бы только 2017–2020. Это ограничение фиксируется в спеке, `LIMITATIONS.md`, `IMPORTERS.md` и PR; оно не блокирует T3/T4/T5.
2. **Веса — только листы `3` и `4`** файла `vesa_2006-2026.xlsx`: 2017–2026, 38 кодов ОКВЭД2, две базы — `domestic` (внутренний рынок) и `domestic_export` (внутренний + экспорт). Листы `1`–`2` — ОКВЭД-2007, 43 кода, вне issue.
3. **Витрина `v_rosstat_ppi`, `GRANT SELECT` и MCP-проверка витрины — T5 #280**, не T2. T2 публикует только `series_catalog` (отступление от §10.2, как в T1; назвать в спеке). Витрина T5 строится над `rosstat_ppi_ved`, `rosstat_ppi_product`, `rosstat_ppi_weights` и `ppi_sa`.
4. Две таблицы в одном PR — исключение, заданное самим issue #277; каждая — отдельный импортёр, отдельная таблица, отдельный шаг dagu.

## 2. Что уже проверено по источнику (кэш research, винтаж 08-2026)

Файлы: `.worktrees/ppi-kipc-mapping/build/ppi-sa-research/raw/tov.xlsx` (24.8 МБ, 28 листов) и `vesa.xlsx` (55 КБ, 5 листов).

| Артефакт | Факт |
|---|---|
| Листы товаров с `Код товара` | `4`–`27`: 6 лет × 4 типа; `4–15` (2021–2023) — значения в строке кода, `16–27` (2024–2026) — в строке «Российская Федерация» |
| Месячный объём 08-2026 | **336 512** точек, **1 327** уникальных кодов; m/m 84 128, ytd 84 128, y/y 84 128, period 84 128 |
| Покрытие | 2021-01-31 … 2026-08-31; 2021 — 1 212 кодов, 2026 — 1 273 кода; 50 кодов только в 2021, 111 — только в 2026 |
| Имена | 44 кода меняют название между годами (например, `11.01.10.210`: «Ликеры» → «Ликероводочные изделия крепостью до 25 %»); `name` хранится по точке, а не одна на код |
| Пропуски | в месячных листах — пустые ячейки; `…`/`...` — в годовых листах (не импортируются), но парсер значения обязан их принимать |
| Лист `3` | 1 358 уникальных кодов, 10 485 точек 2017–2025, 6 кодов-дублей с разными годами/названиями — **в T2 не читается** |
| Веса, лист `3` | 38 кодов, 2017–2026, **379** точек (domestic), строка `B+C+D+E` = 100 каждый год |
| Веса, лист `4` | 38 кодов, 2017–2026, **379** точек (domestic_export), `B+C+D+E` = 100 каждый год |

> `758 = 2 × (38 × 10 − 1)`: в обеих базах у кода 38 за 2017 год источник ставит «-» — пропуск, не ноль; других пропусков в винтаже 08-2026 нет.
| Ссылки на странице цен | `a[href*="Proizvoditeli_Ind_tov_"]` → `/storage/mediabank/Proizvoditeli_Ind_tov_08-2026.xlsx`; `a[href*="vesa_"]` → `/storage/mediabank/vesa_2006-2026.xlsx` |

## 3. Схема и контракты

### 3.1. `rosstat_ppi_product`

```sql
CREATE TABLE IF NOT EXISTS rosstat_ppi_product (
      date Date
    , okpd2 LowCardinality(String)
    , name LowCardinality(String)
    , index_type LowCardinality(String)
    , value Float64
) ENGINE = ReplacingMergeTree
ORDER BY (date, okpd2, index_type);
```

- `date` — последний день отчётного месяца UTC.
- `okpd2` — код ОКПД2 из колонки «Код товара на основе ОКПД2»; имя колонки — как в research handoff (`okpd2`), а не `code`.
- `name` — название товара из строки источника, которая дала значение; у 44 кодов название может меняться по годам.
- `index_type` — `m/m` | `ytd` | `y/y` | `period`; дата + тип + код — ключ, ревизия перезаписывается.
- `value` — индекс, `%`; 100 = без изменения; пустая ячейка/`…`/`...` — пропуск, не ноль.

### 3.2. `rosstat_ppi_weights`

```sql
CREATE TABLE IF NOT EXISTS rosstat_ppi_weights (
      year UInt16
    , code LowCardinality(String)
    , name LowCardinality(String)
    , weight_type LowCardinality(String)
    , value Float64
) ENGINE = ReplacingMergeTree
ORDER BY (year, code, weight_type);
```

- `year` — 2017…2026.
- `code` — код ВЭД (ОКВЭД2), включая агрегаты `B+C+D+E`, `B`, `C`, `D`, `E` и подклассы.
- `weight_type` — `domestic` | `domestic_export`. Ключ обязан включать базу: у одного `(year, code)` два разных веса (например, 2017 `B`: 19.61 и 23.08), handoff-ключ `(year, code)` из research недостаточен — исправление фиксируется в спеке и `ARCHITECTURE.md`.
- `value` — доля, `%`; строка `B+C+D+E` = 100 в каждом году и каждой базе (проверка с допуском 0.05).

### 3.3. Каталог `series_catalog`

- Товары: одна запись на пару `(okpd2, index_type)` — **5 308** для винтажа 08-2026. `series = "okpd2:<code>:<index_type>"` (устойчивый технический ключ), `title` = последнее по дате название товара + тип, `frequency = "M"`, `unit` — по типу индекса, `source = "rosstat"`.
- Веса: одна запись на пару `(code, weight_type)` — **76**. `series = "ved-weight:<code>:<weight_type>"`, `frequency = "A"`, `unit = "% (доля в структуре весов ИЦП)"`, `source = "rosstat"`.
- Каталог строится в `AfterImport` запросом к своей таблице: `argMax(name, date)` для товаров и `argMax(name, year)` для весов. Так каталог не расходится с фактически записанными данными и переживает переименования кодов.
- Контракт T1 «`series` == `name` точки» для товаров **не выполняется**: у 1 273 кодов не может быть человекочитаемого уникального `name` в ключе, а T3 маппит по названиям. Это осознанное отличие, фиксируется в спеке.

## 4. Разбор

### 4.1. Общие правила (переиспользовать хелперы T1)

- `excelize/v2`, только `GetRows(..., RawCellValue: true)`; `ppiVedValue`, `ppiVedCell`, `ppiVedNormalizeLabel`, `ppiVedMonth`, `ppiVedYear` уже generic по смыслу — переиспользовать без переименования, чтобы не трогать T1.
- `…`, `...`, `-`, `—`, `х`, `X`, пустая ячейка — пропуск; нечисловой текст — ошибка с листом/строкой/колонкой; `NaN`/`Inf` — ошибка.
- Никакого `panic`; выход за границы среза — пустая ячейка.
- Даты — `time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)`.

### 4.2. Товарный лист

- Лист отбирается **по шапке** `Код товара` (нормализация регистра/пробелов), а не по номеру: новые годы (`28`, `29`, …) подхватятся без правки.
- Год — из заголовка `в YYYY г.` (`ppiVedYearFromTitle`).
- Тип индекса — из строки 3:
  - `к предыдущему месяцу` → `m/m`;
  - `к декабрю предыдущего года` → `ytd`;
  - `к соответствующему месяцу предыдущего года` → `y/y`;
  - `за период с начала года, в % к соответствующему периоду предыдущего года` → `period`;
  - иначе — ошибка (неизвестная разметка).
- Колонка кода — колонка шапки `Код товара` (должна быть ≥ 1); название — колонка 0; месяцы — колонки после кода, распознанные по `util.MonthsToNum`; сноски вида `октябрь1)` снимаются.
- Блок товара:
  - строка с непустыми `name` и `code` (кроме заголовка) открывает блок и, если в ней есть значения, даёт точки (формат 2021–2023);
  - следующая строка `Российская Федерация` даёт точки тому же коду (формат 2024–2026) и закрывает блок;
  - строки регионов/разделов без кода — пропускаются.
- Дубли ключа `(date, okpd2, index_type)` с разными значениями — ошибка; одинаковые значения допустимы. Это ловит и переименования строк, и смену разметки.
- Для каждого листа с `Код товара`: ≥ 1 000 кодов, 12 месяцев (2021–2025) или 8–12 (2026), ≥ 1 строка РФ для 2024+ — иначе ошибка.
- Глобальные проверки: все 4 типа дали точки; набор месяцев каждого типа непрерывен `2021-01 … max(date)` и совпадает между типами; ≥ 1 000 кодов; 0 точек — ошибка.

### 4.3. Веса

- Листы `3` и `4`; тип базы — по заголовку: содержит `и на экспорт` → `domestic_export`, иначе `domestic`.
- Шапка — строка с `Код` и ≥ 5 годами в колонках 2+; годы — regexp `(20\d{2})` (сноски `20221)` снимаются), код — колонка 1, имя — колонка 0.
- Пропуски — по общим правилам; строки без кода/`Код`/сноска `1)` — пропускаются.
- Проверки: ровно 2 базы, по 38 кодов, 10 лет 2017–2026; единственный пропуск источника — код 38 за 2017 год (`-` в обеих базах); `B+C+D+E` = 100 ± 0.05 в каждом году/базе, дубль ключа с разным значением — ошибка.

## 5. Импортёры и шаблон

Файлы: `rosstat/ppi_product.go`, `rosstat/ppi_product_parse.go`, `rosstat/ppi_weights.go`, `rosstat/ppi_weights_parse.go`.

- `PPIProductStat` / `PPIWeightsStat` встраивают `util.ClickHouseImport`; свой `Import()` вычисляет `DataUrl` со страницы цен (`util.GetPage` + `util.FindLink` + `rosstatUrl`) и **явно отказывает** в `ARCHIVE_ACTION=restore` — URL вычисляется со страницы, как у `rosstat_ppi_ved` (прецедент T1/okved2).
- `ImportFunc` — один `parse...`, затем `batch.Append` с проверкой каждой ошибки; шаблон сам делает `PrepareBatch` → `Send` и `Abort` на ошибке.
- `PublicationCountPoints`: `SELECT count() FROM rosstat_ppi_product FINAL` и `SELECT count() FROM rosstat_ppi_weights FINAL`. Оба файла обновляются реже недельного шага → HEAD-префлайт, no-op по версии, `source_publications`, винтажный архив.
- `AfterImport` — только `util.UpsertSeriesCatalog` с динамическим каталогом (см. §3.3); витрина/грант — T5.
- Регистрация: две записи в `rosstat/importers.go` (литералы имён таблиц и запросов чека), обновить комментарии «девять» → «одиннадцать».
- `rosstat/importers_test.go`: добавить оба имени в `expectedPublicationCountPoints`, обновить комментарии (семь → девять с префлайтом).
- `dagu/rosstat.yaml`: десятый шаг `rosstat_ppi_product`, одиннадцатый `rosstat_ppi_weights` в хвосте цепочки, `retry_policy: 3 × 300 s`; комментарии про хвост и T6.

## 6. Шаги работ

### Шаг 0. Захват и изоляция

1. `git fetch origin`; `git worktree add .worktrees/rosstat-ppi-product -b feat/rosstat-ppi-product origin/main`.
2. Комментарий в issue #277: «Taking `key-rate/phase-4-rosstat-ppi-sa` T2; branch `feat/rosstat-ppi-product`; worktree `.worktrees/rosstat-ppi-product`; spec/plan …».
3. Добавить #277 в Project V2 №11 (если ещё нет), `Status = In Progress`, `Branch`, `Spec / Plan`; labels: добавить `roadmap:key-rate` (сейчас только `role:go`).
4. Первый коммит ветки — спека и план:
   - `docs/superpowers/specs/2026-10-20-rosstat-ppi-product-design.md`;
   - `docs/superpowers/plans/2026-10-20-rosstat-ppi-product.md` (этот план).
   Сообщение: `docs: add rosstat PPI product and weights spec and plan`.

### Шаг 1. Парсер товаров

1. `ppiProductPoint{okpd2, name, indexType string; date time.Time; value float64}`.
2. `parsePPIProduct(xlsx) ([]ppiProductPoint, error)`:
   - перебрать листы, отобрать с шапкой `Код товара`;
   - год/тип — по §4.2; листы `1`–`3` без `Код товара` пропускаются;
   - слить точки в map по `(date, okpd2, indexType)`, конфликты — ошибка;
   - проверить покрытие §4.2, отсортировать `(date, indexType, okpd2)`.
3. Ошибки — с именем листа, строкой и колонкой; пустая книга/нет листов — ошибка.

### Шаг 2. Парсер весов

1. `ppiWeightPoint{year uint16, code, name, weightType string; value float64}`.
2. `parsePPIWeights(xlsx) ([]ppiWeightPoint, error)` по §4.3; проверить суммы и покрытие; отсортировать `(year, code, weightType)`.

### Шаг 3. Импортёры и DDL

1. DDL в `%s`-форме, константы имён таблиц, URL/селекторы, `...CountPoints`.
2. `ppiProductDataURL` / `ppiWeightsDataURL`; `Import()` с отказом restore.
3. `ppiProductImport` / `ppiWeightsImport` — Append 5 колонок.
4. `AfterImport` — каталог:
   - товары: `SELECT okpd2, index_type, argMax(name, date) ... GROUP BY okpd2, index_type ORDER BY okpd2, index_type` → 5 308 `SeriesMeta`;
   - веса: `SELECT code, weight_type, argMax(name, year) ... GROUP BY code, weight_type` → 76.
   Вынести чистую функцию построения `[]util.SeriesMeta` из строк запроса — тестировать без БД.
5. `var ppiProductImporter` / `ppiWeightsImporter`.

### Шаг 4. Реестр, dagu, тест-энфорсер

1. `rosstat/importers.go` — две записи, комментарии.
2. `rosstat/importers_test.go` — `expectedPublicationCountPoints` + комментарии.
3. `dagu/rosstat.yaml` — два шага в хвосте; `dagu validate` (если установлен).

### Шаг 5. Фикстуры

Из кэша research (абсолютные пути):

```bash
python3 scripts/xlsx_trim.py trim \
  /Users/whitefox/GolandProjects/clickhouse-import-rosstat/.worktrees/ppi-kipc-mapping/build/ppi-sa-research/raw/tov.xlsx \
  rosstat/testdata/ppi_product.xlsx \
  --keep Содержание --keep 4 --keep 5 --keep 6 --keep 7 --keep 24

python3 scripts/xlsx_trim.py trim \
  /Users/whitefox/GolandProjects/clickhouse-import-rosstat/.worktrees/ppi-kipc-mapping/build/ppi-sa-research/raw/vesa.xlsx \
  rosstat/testdata/ppi_weights.xlsx \
  --keep Содержание --keep 3 --keep 4
```

- `ppi_product.xlsx`: листы `4` (2021 m/m, старый формат), `5` (ytd), `6` (y/y), `7` (period), `24` (2026 m/m, строка РФ). Ожидаемо **68 360** точек: m/m 24 728, ytd 14 544, y/y 14 544, period 14 544; 1 323 кода; 4 959 пар для каталога.
- `ppi_weights.xlsx`: листы `3`/`4`. Ожидаемо **758** точек (379 + 379), 38 кодов, 2 базы, 2017–2026; 76 пар каталога.
- `rosstat/testdata/price_page.html` уже содержит обе ссылки; новых HTML-фикстур не нужно.

### Шаг 6. Тесты

**`rosstat/ppi_product_test.go`**
- `TestPPIProductFindLink` — селектор на `price_page.html` даёт `/storage/mediabank/Proizvoditeli_Ind_tov_08-2026.xlsx`.
- `TestPPIProductReadsFixture` — 68 360 точек, счётчики по типам, 1 323 кода, диапазон 2021-01-31…2026-08-31; spot:
  - 2021-01 m/m `02.20.11` = 101.74; 2026-01 m/m = 100.95; 2026-08 m/m = 100.11;
  - 2021-01 ytd/y/y/period `02.20.11` = 101.74 / 108.75 / 108.75;
  - код `03.12.12` есть в 2021 и отсутствует в 2026; `03.11.30` — наоборот;
  - 39 переименований 2021→2026 (например, `35.13.10.021.АГ`: «свободным тарифам» → «свободным ценам») — `name` точки сохраняет название своего года.
- `TestPPIProductEmptyCellSkipped` — известная пустая ячейка не даёт точки; `ppiVedValue` принимает `…`/`...`/`-`/`—`/`х`.
- `TestPPIProductIndexType` — таблица заголовков всех 4 типов + неизвестный → ошибка.
- Fail-fast: нет шапки `Код товара`, нет года в заголовке, нет месяцев, нечисловое значение, конфликтующий дубль, пустая книга, лист 2024+ без строки РФ.
- `TestPPIProductBatch` — 5 колонок, число строк; `TestPPIProductDdl`; `TestPPIProductRestoreRefused`; `TestPPIProductCoverageRejectsGap`; `TestPPIProductCatalog` — 4 959 серий, префикс `okpd2:`, уникальность, `argMax`-название.

**`rosstat/ppi_weights_test.go`**
- `TestPPIWeightsFindLink` — `/storage/mediabank/vesa_2006-2026.xlsx`.
- `TestPPIWeightsReadsFixture` — 758 точек, 38 кодов, 2 базы, годы 2017–2026; spot: лист 3 `B` 2017 = 19.61, 2026 = 16.27; лист 4 `B` 2017 = 23.08, 2026 = 19.56; `B+C+D+E` = 100 каждый год.
- `TestPPIWeightsType` — `domestic` / `domestic_export` по заголовку; fail-fast: нет листа, нет шапки, сумма ≠ 100, нечисловое, конфликт, пустая книга.
- `TestPPIWeightsBatch`, `TestPPIWeightsDdl`, `TestPPIWeightsRestoreRefused`, `TestPPIWeightsCatalog` — 76 серий.

**`rosstat/importers_test.go`** — энфорсер сам проверит регистрацию, dagu и префлайт; обновить ожидания.

### Шаг 7. Документация

1. `ARCHITECTURE.md`:
   - верхняя строка «Последнее обновление» — T2;
   - §2 — две таблицы и два импортёра, «девять» → «одиннадцать»;
   - §3 — строка таблицы импортёров `rosstat_ppi_product`/`rosstat_ppi_weights`; счётчик префлайта 30 → 32, «семь Росстата» → «девять»;
   - §6.1 — описание источников/покрытия/ограничений;
   - §6.2 — канонические DDL обеих таблиц рядом с `rosstat_ppi_ved`; список реализованных таблиц;
   - §7 — цепочка Росстата 9 → 11 шагов, новые шаги в хвосте;
   - §8 — `Name()`-список, счётчики.
2. `README.md` — строка `rosstat`: товары/ОКПД2 и веса, обе таблицы; витрина — T5.
3. `docs/IMPORTERS.md` — строка каталога + подробный раздел (источник, листы, форматы, пропуски, веса, no-op/архив, отступление §10.2).
4. `docs/IMPORTER_PACKAGE_STANDARD.md` — строка пакета `rosstat` (таблицы/ряды/префлайт).
5. `docs/LIMITATIONS.md` — месячные товары только с 2021, годовые 1998–2025 не импортированы; веса только 2017–2026 и только по ВЭД; ревизии перезаписываются, PIT нет; 44 кода с меняющимися названиями.
6. `docs/ROADMAP_KEY_RATE.md` — в абзаце `phase-4-rosstat-ppi-sa` зафиксировать T1/T2 done и следующий шаг T3/T4 (только по факту мержа T2; если PR ещё открыт — статус «T2 в review»).
7. `docs/superpowers/specs/...` и `plans/...` — спека/план T2.

### Шаг 8. Проверки

1. `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, обе сборки).
2. `go test -race -count=1 ./rosstat/`.
3. `make docs-check`.
4. `dagu validate dagu/rosstat.yaml` (если `dagu` установлен; иначе «не проверено»).

### Шаг 9. Живая приёмка

1. `make ch-status`; ClickHouse не поднимать — если лежит, попросить владельца.
2. Товары:
   ```bash
   ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev \
     make import STAT=rosstat_ppi_product
   ```
   - `Imported N rows`; для винтажа 08-2026 ожидаем **336 512** точек (если страница уже отдаёт 09-2026 — записать фактическое, структура та же);
   - `SELECT count(), uniqExact(okpd2), min(date), max(date) FROM rosstat_ppi_product FINAL`;
   - `SELECT index_type, count(), min(date), max(date) ... GROUP BY index_type`;
   - spot-значения из §6; повтор — no-op, `count()` не меняется;
   - `source_publications`: `Proizvoditeli_Ind_tov_08-2026.xlsx`, points = count; объект в `build/archive-dev/rosstat_ppi_product/`.
3. Веса:
   ```bash
   ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev \
     make import STAT=rosstat_ppi_weights
   ```
   - `Imported 758 rows`; 38 кодов, 2 базы, 2017–2026; spot `B` 2017 = 19.61 / 23.08; повтор — no-op; архив и `source_publications`.
4. `make mcp-check` — обновить проверки:
   - старые 169 рядов Росстата считать с исключением префиксов `okpd2:` и `ved-weight:`;
   - товарный каталог `startsWith(series,'okpd2:')` ≥ 5 308 (растёт вместе с кодами);
   - весовой каталог `startsWith(series,'ved-weight:')` = 76;
   - сырые `rosstat_ppi_product` и `rosstat_ppi_weights` закрыты для `kimi_reader`;
   - витрины `v_rosstat_ppi` в T2 нет — проверка появится в T5.
   Если ClickHouse/MCP недоступен — «не проверено», не выдавать за успех.
5. Протокол живой приёмки — комментарием в PR.

### Шаг 10. Ревью и PR

1. Ревью субагентом на другой модели (по `subagent-dispatch`), fix-loop; финальное — на сильнейшей модели.
2. Коммиты по стилю репозитория: спека/план, код, тесты/фикстуры, документация.
3. `git push -u origin feat/rosstat-ppi-product`; PR:
   - заголовок `[key-rate/phase-4-rosstat-ppi-sa] T2: импорт ИЦП по товарам/ОКПД2 и весов`;
   - `Closes #277` **вне backticks**;
   - источник, схема, два запуска, примеры проверки, ограничения (годовые срезы, T5-витрина);
   - Project `Status = In Review`, `PR = <номер>`.
4. Merge — решение владельца.

## 7. Критерии готовности

- [ ] `make import STAT=rosstat_ppi_product` — все 4 типа, 2021-01…текущий месяц, 1 327 кодов (винтаж 08-2026), без пропусков месяцев на глобальном уровне.
- [ ] `make import STAT=rosstat_ppi_weights` — 38 кодов × 2 базы × 2017–2026, суммы 100.
- [ ] Тесты на оба формата товарного листа (значения в строке кода и в строке РФ), 4 типа индексов, пустые ячейки, переименования кодов, оба листа весов.
- [ ] `series_catalog`: 5 308 товарных + 76 весовых записей; каталог строится из данных.
- [ ] `make all`, `go test -race -count=1 ./rosstat/`, `make docs-check`, `dagu validate` — зелёные.
- [ ] Повтор — no-op по версии; `source_publications` и винтажный архив у обоих.
- [ ] Документация синхронна; PR с `Closes #277` и протоколом.

## 8. Протокол живой приёмки (2026-10-20)

- `make all` — exit 0: `golangci-lint` 0 issues, `go vet ./...` чисто, `go test -race ./...` все пакеты `ok`, обе сборки. `go test -race -count=1 ./rosstat/` — `ok` (130 s в составе `make all`; без race — 5.4 s).
- `make docs-check` — exit 0: 49 страниц, 33 теста docs.
- `dagu validate dagu/rosstat.yaml` — exit 0 (предупреждение `No auth.mode configured` — информационное).
- `rosstat_ppi_product` (прямой запуск с `ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev`): **`Imported 336512 rows`**; SQL: 336 512 точек / 1 327 кодов / `2021-01-31 … 2026-08-31`; по типам `m/m/ytd/y/y/period` — по **84 128**; spot `02.20.11` m/m: 2021-01 `101.74`, 2026-01 `100.95`, 2026-08 `100.11`. Повтор — `no-op, версия d6a2cd87… уже импортирована, 336512 точек на месте`; `count()` не менялся.
- `rosstat_ppi_weights` (прямой запуск с локальным архивом): **`Imported 758 rows`**; SQL: 758 точек / 38 кодов / 2017–2026; по базам 379 + 379; spot `B`: 2017 `19.61` / `23.08`, 2026 `16.27` / `19.56`; `B+C+D+E` = 100 в каждом году. Повтор — `no-op, версия 15a83003… уже импортирована, 758 точек на месте`.
- `source_publications`: `rosstat_ppi_product` — `Proizvoditeli_Ind_tov_08-2026.xlsx`, points 336 512; `rosstat_ppi_weights` — `vesa_2006-2026.xlsx`, points 758. Локальный архив: `build/archive-dev/rosstat_ppi_product/d6a2cd87d811d462_Proizvoditeli_Ind_tov_08-2026.xlsx` (24 872 212 Б) и `build/archive-dev/rosstat_ppi_weights/15a830031c9c8fed_vesa_2006-2026.xlsx` (55 147 Б), обе строки — в `registry.jsonl`.
- Каталог: `v_series_catalog` — 5 308 товарных (`okpd2:`) + 76 весовых (`ved-weight:`) рядов; базовые 169 рядов Росстата не изменились.
- `make mcp-check` — **ПРОВЕРКА ПРОЙДЕНА**: базовый каталог rosstat 169, товарный 5 308, весовой 76, обе сырые таблицы закрыты `kimi_reader` (497), остальные проверки репозитория зелёные.
- Замечание приёмки: первый запуск весов через `make import` подхватил `ARCHIVE_BACKEND=yandex` из env-файла и получил 500 от Диска при записи `registry.jsonl`; данные при этом записались (`Imported 758 rows`), строка версии не записалась. Повторный прямой запуск с `ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev` записал версию и архив; это свойство окружения (env-файл перекрывает make-переменную), а не дефект импортёра. Для product использован тот же прямой запуск с локальным архивом.

## 9. Риски

| Риск | Что делаем |
|---|---|
| 24.8 МБ × 24 листа, 336 тыс. точек — память/время | один `PrepareBatch` на прогон (стандарт), парсинг лист за листом; живой прогон замеряет; при OOM — это дефект, а не повод менять правило батчей |
| 2026-лист содержит 8 месяцев, а 2027 добавит `28.x` | лист отбирается по шапке, год — из заголовка, число месяцев — из шапки; новых правок не требуется |
| Смена названий у 44 кодов | `name` — по точке; каталог — `argMax(name, date)`; T3 маппит по нормализованным названиям |
| Годовые срезы не импортируются | явное ограничение в спеке/LIMITATIONS/PR; 2017–2020 в code-keyed таблицу не попадают |
| `vesa_2006-2026.xlsx` переименуется в 2027 | ссылка берётся со страницы селектором `a[href*="vesa_"]`, не хардкодится |
| ClickHouse недоступен | офлайн-тесты и `make all`; живая приёмка — «не проверено» |
| Каталог 5 308 строк замедляет `AfterImport` | один батч `UpsertSeriesCatalog`; замер на живом прогоне |
