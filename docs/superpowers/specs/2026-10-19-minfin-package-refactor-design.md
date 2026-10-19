# Пакет `minfin`: полный рефакторинг по стандарту импортёров — дизайн (2026-10-19)

**Пункт роадмапа:** `key-rate/phase-2-minfin-refactor` — отдельный пункт из PR #230, выполняется до `key-rate/phase-2-ofz-debt-stress` (#170).
**Issue:** [#233](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/233).
**Стандарт:** [docs/IMPORTER_PACKAGE_STANDARD.md](../IMPORTER_PACKAGE_STANDARD.md) §4–§6, §8, §9.3, §10, §11, §13, §15–§16.
**Роль:** Go-инженер.

---

## 1. Границы

- Меняется только пакет `minfin/`, его тесты, фикстуры и производная документация.
- Имена таблиц, импортёров (`Name()`), шагов `dagu/minfin.yaml`, наборы рядов и имена рядов **сохраняются**.
- `minfin_fed_bud_mes` и `minfin_fed_bud_mesyats` переводятся с `Float32` на `Float64`; `minfin_oil_gas_mes` уже `Float64`.
- Меняется точность значений: это **названное исправление**, а не «данные не менялись» (см. §4).
- Новые импортёры долгового контура (`minfin_ofz_*`, #170), PIT/винтажный архив бюджетных файлов и добавление префлайта бюджетным импортёрам — вне объёма.

## 2. Текущее состояние и дефекты

`origin/main` после PR #230 содержит три импортёра и четыре тестовых файла.

| Файл | Дефект | Следствие |
|---|---|---|
| `fedbud_mes.go` | `colly`; `GetRows("месяц")` без `RawCellValue`; `Float32`; ручной `Import()`; `ParseFloat` проверяется после использования; `dateStr[0:3]` может паниковать | нарушение §4 и правила 4 AGENTS.md; значения округлены до одного знака; хрупкий разбор; нет общей нормализации подписи |
| `fedbud_mes.go` | нет fail-fast: нет шапки/строки/точек — молчаливый частичный импорт | §6.4, §11.5 |
| `fedbud_mesyats.go` | `Float32`; ручной `Import()`; разбор дублирует федеральный | дублирование; схема нарушает правило 5 |
| `oil_gas_mes.go` | ручной `Import()` только ради вычисляемой ссылки | шаблон используется не полностью |
| `testdata/` | нет федеральной XLSX-фикстуры | федеральный разбор проверяется только синтетикой (§11.1) |

Инвентарь: `colly` — 1 файл (`fedbud_mes.go`), `fmt.Print*` — 0 в коде (остались только закомментированные строки), ручных `Import()` — 3, тестовых файлов — 4.

## 3. Целевое устройство

### 3.1. Файлы

| Файл | Роль |
|---|---|
| `minfin/importer.go` (новый) | `minfinFileImporter`, конструктор, `BeforeImport` (ссылка со страницы), `AfterImport` (`publishFedbud`), общий DDL и Float64-миграция |
| `minfin/fedbud_parse.go` (новый) | общий разбор листа «месяц», дата, число, нормализация подписи, fail-fast |
| `minfin/fedbud_mes.go` | конфиг федерального импортёра + `fedBudImport` |
| `minfin/fedbud_mesyats.go` | конфиг консолидированного импортёра + `fedbudMesyatsImport` |
| `minfin/oil_gas_mes.go` | разбор без изменений; транспорт — `BeforeImport` |
| `minfin/series_meta.go` | `publishFedbud`; правка описаний |
| `minfin/importers.go` | реестр; без изменения формы |
| `minfin/testdata/Prilozhenie_3_dannye_109-111_mes_202610.xlsx` | реальная федеральная книга 2026/10 |
| `minfin/fedbud_mes_test.go` | ссылка, разбор, spot-значения, fail-fast |

### 3.2. Общий шаблон

```go
type minfinFileImporter struct {
    util.ClickHouseImport
    pageURL  string
    selector string
}

func newMinfinFileImporter(table, pageURL, selector string,
    createTable []string,
    importFunc func(*excelize.File, driver.Batch) error,
    meta []util.SeriesMeta, countPoints string) *minfinFileImporter
```

- `BeforeImport`: `util.GetPage(pageURL)` → `util.FindLink(page, selector)` → `DataUrl`. Абсолютный href не префиксуется. Сеть — только внутри `Import()`, не в `init()`.
- `AfterImport`: `publishFedbud(ctx, conn, meta)`.
- `ImportFunc`: общий разбор.
- `PublicationCountPoints`: у `minfin_oil_gas_mes` остаётся; у бюджетных импортёров нет — файл обновляется раз в месяц, шаг DAG тоже раз в месяц (§9.3). Это осознанное решение.
- Ручные типы `fedbudMesStat`, `fedbudMesyatsStat`, `oilGasMesStat` и их `Import()` удаляются; `Name()`/`Import()`/`Publication()` промотируются.
- Селекторы: `.document_list a[href$="_mes.xlsx"]`, `a[href*="_mesyats.xlsx"]`, `a.file_item[href*="Svedeniya_"]`.

### 3.3. Общий разбор листа «месяц»

`fedbudLayout{Table, Fields}`; `fedBudImport`/`fedbudMesyatsImport` — тонкие обёртки.

- `GetRows("месяц", excelize.Options{RawCellValue: true})`.
- Шапка — строка с `normalizeFedbudLabel(row[1]) == "Показатель"`.
- Даты колонок — из шапки, с колонки 2:
  - сериал Excel `>= 20000` → `excelize.ExcelDateToTime`, 1-е число месяца UTC;
  - текст `Jan-11` / `янв.11 г.` / `сен.26 **` → снять хвостовые `*`, первый токен, `dateReplacer` (все 12 месяцев), `time.Parse("Jan-06")`; запасные layout `2006-01-02`, `02.01.2006`.
- Подпись: `strings.Join(strings.Fields(label), " ")` + `TrimRight("* ")`; точное совпадение с полем. `Акцизы ****` → `Акцизы`; `Акцизы на ввозимые товары ****` не входит.
- Число: убрать пробелы/NBSP; `,` + `.` → запятая тысячная; только `,` → десятичная; `ParseFloat` с контекстом.
- Накопление → месячные разности; январь = накопление за январь; `valuePrev` сбрасывается на январе.
- Пустая ячейка внутри ряда — ошибка (разность через пропуск неверна); хвостовые пустые excelize обрезает.
- Fail-fast: нет листа, шапки, дат, ожидаемой строки, ноль точек у строки/всего, дубль подписи, нечисло/недата. Ошибка называет таблицу, лист, строку и колонку.
- `batch.Append` — с проверкой.

### 3.4. DDL и миграция

```sql
CREATE TABLE IF NOT EXISTS %s (
    name LowCardinality(String),
    date Date,
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (name, date);
```

Для двух бюджетных таблиц в `CreateTable` вторым стейтментом:

```sql
ALTER TABLE %s MODIFY COLUMN value Float64 SETTINGS mutations_sync = 2;
```

- Новая таблица — no-op; legacy Float32 — миграция до первого импорта; Float64 — безопасный повтор.
- `minfin_oil_gas_mes` — `migrate=false`.
- После ALTER импорт перезаписывает точки текущего выпуска точными Float64-значениями; старые строки заменяются по ключу `(name, date)`. Для детерминизма приёмка делает `OPTIMIZE ... FINAL`.
- `v_minfin_budget` сохраняет `toFloat64(value)` как явное приведение выходного типа.

### 3.5. Что не меняется

- Имена таблиц/импортёров/рядов, шаги DAG, грант `kimi_reader`.
- Разбор `oil_gas_mes.go` (только транспорт).
- `minfinBudgetView` и `publishFedbud` по существу.
- Префлайт бюджетных импортёров не появляется; у oil gas остаётся.

## 4. Данные: baseline и названное изменение

Baseline dev-ClickHouse (2026-10-19, `FINAL`):

| source | точек | рядов | min | max | тип `value` |
|---|---:|---:|---|---|---|
| `minfin` | 1076 | 6 | 2011-01-01 | 2026-09-01 | Float32 |
| `minfin_mesyats` | 884 | 5 | 2011-01-01 | 2026-08-01 | Float32 |
| `minfin_oil_gas` | 210 | 2 | 2018-01-01 | 2026-09-01 | Float64 |

Ожидаемо после миграции: те же числа и даты; тип `Float64` у всех трёх; точность:

| Точка | До | После |
|---|---|---|
| `minfin/Доходы, всего` 2011-01 | 804.9000244140625 | 804.8582643055 |
| `minfin/Акцизы` 2026-09 | 153.10000610351562 | 153.07289659012986 |
| `minfin_mesyats/Доходы, всего` 2011-01 | 1115.85498046875 | 1115.85497612064 |
| `minfin_mesyats/Акцизы` 2026-08 | 286.6877746582031 | 286.6877654519035 |

Причина: федеральный бюджет читался без `RawCellValue` (округление до 1 знака), оба бюджета хранились в `Float32`. Набор точек не меняется. В каталоге исправляется описание `привлечение`: ряд заканчивается 2026-08 (188 точек), значение за август — 0.

## 5. Тесты и фикстуры

- `minfin/testdata/Prilozhenie_3_dannye_109-111_mes_202610.xlsx` — реальная книга федерального бюджета 2026/10 (одна книга, один лист; ~216 КБ, урезание не требуется, §11.2).
- `fedbud_mes_test.go`: ссылка из `fedbud_page.html`; 1076 точек; 189/189/189/132/188/189; нет `*` в именах; spot-значения; max-даты; fail-fast.
- `fedbud_mesyats_test.go`: 884 точки и spot-значения PR #230 сохраняются; добавить проверку нормализации `Акцизы ****`.
- `series_meta_test.go`: синтетическая книга для контракта каталога; `TestFedbudMesDeficitSeries` строит полную книгу (иначе fail-fast на пропавшие строки).
- `importers_test.go`: комментарий о префлайте; `TestMinfinDDLValueFloat64`; `TestMinfinTemplateWiring`; замена oil-gas-specific wiring-теста общим.
- `oil_gas_mes_test.go`: ссылка и разбор без изменений; wiring-тест обновляется.

## 6. Документация и трекер

- `ARCHITECTURE.md` §2, §6.1, §6.2 (DDL + миграция), §6.3, §7, шапка «Последнее обновление».
- `README.md`, `docs/IMPORTERS.md`, `docs/IMPORTER_PACKAGE_STANDARD.md` §18.
- `docs/ROADMAP_KEY_RATE.md`: пункт `phase-2-minfin-refactor`, статус, порядок перед #170.
- Issue #233, Project V2, PR с `Closes #233`, протокол приёмки комментарием.

## 7. Приёмка

```bash
make all
go test -race -count=1 ./minfin/
make import STAT=minfin_fed_bud_mes
make import STAT=minfin_fed_bud_mesyats
make import STAT=minfin_oil_gas_mes
make mcp-check
dagu validate dagu/minfin.yaml
make docs-check
```

Ожидаемо: `Imported 1076 rows`, `Imported 884 rows`, oil gas — no-op (210 точек) или `Imported 210 rows` после снятия его строки `source_publications`; `v_minfin_budget` — 2170 строк, 6/5/2 ряда, `Float64`, spot-значения §4; повтор бюджетов идемпотентен; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

## 8. Риски

| Риск | Решение |
|---|---|
| `ALTER MODIFY COLUMN` повторяется каждый прогон | таблицы ~1–2 тыс. строк, `mutations_sync=2`; повтор на Float64 — no-op/дешёвая мутация |
| ReplacingMergeTree без версии: старые Float32 и новые Float64 до мержа | `OPTIMIZE ... FINAL` в приёмке; проверка `FINAL`/витрины |
| Точность выглядит как «данные поменялись» | отдельный раздел в PR/протоколе; число точек не меняется |
| Смена селектора/шапки | fail-fast с контекстом; HTML-фикстуры и `FindLink`-тест |
| Последняя ячейка шапки — текст со сноской | `parseFedbudDate` снимает `*`, все 12 месяцев, сериал и текст |
| oil gas на no-op не проходит `BeforeImport` | полный путь — тесты + опциональное снятие строки версии в dev |
| Параллельные сессии | только `.worktrees/minfin-refactor`; общий корень и соседние worktree не трогать |

## 9. Вне объёма

- Переименование таблиц/рядов/импортёров.
- Новые импортёры ОФЗ (#170).
- PIT/архив бюджетных файлов, префлайт бюджетным импортёрам (§9.3).
- Другие пакеты (`fao`, `customs`, `financial`), ML/Python.
