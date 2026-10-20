# План: T1 #276 — импортёр `rosstat_ppi_ved`

**Дата:** 2026-10-20
**Спека:** [2026-10-20-rosstat-ppi-ved-design.md](../specs/2026-10-20-rosstat-ppi-ved-design.md)
**Ветка:** `feat/rosstat-ppi-ved` (worktree `.worktrees/rosstat-ppi-ved`, от `origin/main`)
**Issue:** [#276](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/276)
**Parent:** [#266](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/266)

---

## 1. Цель

Импортёр `rosstat_ppi_ved` в пакете `rosstat/`: ИЦП Росстата по ВЭД, совокупный ряд `1323500.029.31`, четыре типа индексов, 1998-01…текущий отчётный месяц, один батч, версия в `source_publications`, винтажный архив, каталог `series_catalog`.

Витрина `v_rosstat_ppi` и грант — T5 (#280); SA — T4 (#279); товары/веса — T2 (#277); расписание/свежесть — T6 (#281).

## 2. Порядок работ

### Шаг 0. Ветка и первый коммит

- Ветка `feat/rosstat-ppi-ved` создана от `origin/main`.
- Первый коммит: эта спека и план (`docs: add rosstat PPI VED spec and plan`). Код — следующими коммитами.

### Шаг 1. Парсер (`rosstat/ppi_ved_parse.go`)

1. `ppiVedPoint{code, name, indexType string; date time.Time; value float64}`.
2. `parsePPIVed(xlsx *excelize.File) ([]ppiVedPoint, error)`:
   - перебрать `xlsx.GetSheetList()`, отобрать `^\d+\.[1-4]$`;
   - группу `1`/`2` отдать `parsePPIVedLegacySheet` (только `1.1`/`2.1`);
   - группы `≥3` — `parsePPIVedYearSheet` с годом из заголовка;
   - слить точки с приоритетом годовые > `2.1` > `1.1`, проверить дубли и непрерывность `m/m`;
   - отсортировать `(date, indexType, code)`.
3. `parsePPIVedLegacySheet`:
   - шапка — первая строка с ≥5 четырёхзначными годами (сноски снимаются);
   - маркер `К предыдущему месяцу`;
   - месяцы — `util.MonthsToNum` после `strings.ToLower` и снятия `\s*\d+\)$`;
   - после декабря — `ytd`-строка `Декабрь в % к декабрю предыдущего года`;
   - значения — сырые, пропуски по §5.1 спеки.
4. `parsePPIVedYearSheet`:
   - год из заголовка `в YYYY г.`;
   - шапка — ячейка `ОКВЭД2` после удаления пробелов;
   - месячные колонки — по названиям;
   - блок кода `1323500.029.31`; если в строке кода есть значения — RF-only (3.x–6.x), иначе следующая строка `Российская Федерация` (7.x/8.x);
   - тип по суффиксу: `.1` m/m, `.2` ytd, `.3` y/y, `.4` period;
   - fail-fast по спеке §5.3.
5. Общие хелперы: `ppiVedNormalizeLabel`, `ppiVedYear`, `ppiVedMonth`, `ppiVedValue` (NBSP, запятая, `…`/`...`/`-`/`—`/`х`/`X`, NaN/Inf).
6. `ppiVedValidateMM` — каждый месяц между min и max обязан иметь точку.

### Шаг 2. Импортёр (`rosstat/ppi_ved.go`)

1. Константы:
   - `rosstatPPIVedTable = "rosstat_ppi_ved"`;
   - `ppiVedPage = "https://rosstat.gov.ru/statistics/price"`;
   - `ppiVedLinkSelector = `a[href*="Proizvoditeli_Ind_VED_"]:not([href*="2kv-"])``;
   - `ppiVedTotalCode = "1323500.029.31"`;
   - `ppiVedCountPoints = "SELECT count() FROM rosstat_ppi_ved FINAL"`;
   - `ppiVedIndexMM/YTD/YY/Period` и четыре стабильных имени ряда.
2. DDL в `%s`-форме:
   ```sql
   CREATE TABLE IF NOT EXISTS %s (
         date Date
       , code LowCardinality(String)
       , name LowCardinality(String)
       , index_type LowCardinality(String)
       , value Float64
   ) ENGINE = ReplacingMergeTree
   ORDER BY (date, code, index_type);
   ```
3. `ppiVedDataURL()` — `util.GetPage` + `util.FindLink` + `rosstatUrl`; ошибки с текстом «ссылка не найдена на …».
4. `PPIVedStat` + override `Import()` (как `LabourForceStat`).
5. `ppiVedImport(xlsx, batch)` — `parsePPIVed`, затем `batch.Append(point.date, point.code, point.name, point.indexType, point.value)` с проверкой каждой ошибки.
6. `ppiVedImporter`:
   ```go
   var ppiVedImporter = &PPIVedStat{ClickHouseImport: util.ClickHouseImport{
       TableName:              rosstatPPIVedTable,
       CreateTable:            []string{ppiVedDdl},
       ImportFunc:             ppiVedImport,
       AfterImport:            ppiVedAfterImport,
       PublicationCountPoints: ppiVedCountPoints,
   }}
   ```
7. `ppiVedAfterImport` — только `util.UpsertSeriesCatalog(ctx, conn, ppiVedSeriesMeta)`; комментарий про T5.

### Шаг 3. Каталог (`rosstat/series_meta.go`)

1. `ppiVedSeriesMeta` — четыре записи `util.SeriesMeta`:
   - `source = rosstatSource`;
   - `series` = стабильные имена из шага 2;
   - `title`, `unit`, `frequency = "M"`, `origin`, `description` по спеке §7.
2. В `description` — покрытие по типам, дата точки (последний день месяца), «пропуск ≠ ноль», оговорка про `ytd` до 2021 (только декабрь) и `y/y`/`period` с 2021.

### Шаг 4. Реестр и dagu

1. `rosstat/importers.go`:
   - восьмая запись `{ppiVedImporter, "rosstat_ppi_ved", "SELECT count() FROM rosstat_ppi_ved FINAL"}`;
   - обновить комментарии «семь» → «восемь», список импортёров.
2. `rosstat/importers_test.go`:
   - добавить `"rosstat_ppi_ved": "SELECT count() FROM rosstat_ppi_ved FINAL"` в `expectedPublicationCountPoints`;
   - обновить комментарии (шесть → семь с префлайтом).
3. `dagu/rosstat.yaml` — восьмой шаг `rosstat_ppi_ved` в хвосте:
   ```yaml
   - id: rosstat_ppi_ved
     run: "$ROSSTAT_IMPORT_BIN"
     env:
       - CLICKHOUSE_IMPORT_STAT=rosstat_ppi_ved
     retry_policy:
       limit: 3
       interval_sec: 300
   ```

### Шаг 5. Тесты и фикстура

1. Скачать исходник в `/tmp`:
   ```bash
   curl -L -A 'Mozilla/5.0 ...' -o /tmp/Proizvoditeli_Ind_VED_08-2026.xlsx \
     https://rosstat.gov.ru/storage/mediabank/Proizvoditeli_Ind_VED_08-2026.xlsx
   ```
2. Урезать (служебный «Содержание» оставляем: на него ссылаются внутренние гиперссылки рисунков; XML листов копируется побайтово):
   ```bash
   python3 scripts/xlsx_trim.py trim /tmp/Proizvoditeli_Ind_VED_08-2026.xlsx \
     rosstat/testdata/ppi_ved.xlsx \
     --keep Содержание --keep 1.1 --keep 2.1 --keep 3.1 --keep 3.2 --keep 3.3 --keep 3.4 --keep 8.1
   ```
   `verify` подтверждает побайтовое совпадение XML листов; проверка отношений ругается на внутренние гиперссылки `#Содержание!A1` (цель — не часть архива, это ложное срабатывание скрипта). Фикстура открывается `excelize`, тесты зелёные.
3. `rosstat/ppi_ved_test.go`:
   - `TestPPIVedFindLink` — `price_page.html`, ожидается `/storage/mediabank/Proizvoditeli_Ind_VED_08-2026.xlsx`, не `2kv`; отдельная inline-страница с `2kv`-ссылкой первой проверяет сам фильтр `:not`;
   - `TestPPIVedReadsFixture` — 407 точек фикстуры (344/39/12/12), min/max, spot-значения и приоритет `2.1` над `1.1` (2013-01: 99,9 против 99,6) и годового листа над `2.1` (2021-01: 103,36 против 103,4);
   - `TestPPIVedValue` — запятая и приклеенная сноска (`98,121)` → 98,12), пропуски, нечисловой текст;
   - fail-fast: нет листа, нет кода, нет строки РФ, нечисловое значение, пустая книга; пустая строка над шапкой `1.1` не паникует; маркеры в строке кода `8.1` не подменяют строку РФ; `ppiVedAdd` отклоняет конфликт; `ppiVedValidateCoverage` отклоняет пропуск `m/m`; `Import` в `restore` возвращает ошибку;
   - `TestPPIVedImportAppends` — локальный `ppiVedBatchRecorder` (пять колонок);
   - `TestPPIVedCatalogContract` — имена каталога == `name` в точках (двусторонний);
   - `TestPPIVedDdl` — `ReplacingMergeTree`, `Float64`, ключ `(date, code, index_type)`.
   Полный винтаж 08-2026 проверяется временным тестом (571 точка: 344/91/68/68); в PR он не входит.
4. `rosstat/importers_test.go` — энфорсер сам проверит регистрацию/dagu/префлайт.

### Шаг 6. Документация

1. `ARCHITECTURE.md`:
   - §6.1 — строка `rosstat_ppi_ved` (источник, листы, покрытие, T5/T6-оговорки);
   - §6.2 — DDL и упоминание в списке реализованных таблиц;
   - §8 — счётчик объявлений точки контроля 29 → 30, «шесть Росстата» → «семь», добавить имя в список `Name()`.
2. `README.md` — строка `rosstat`: ИЦП по ВЭД, `rosstat_ppi_ved`.
3. `docs/IMPORTERS.md`:
   - строка `rosstat` в каталоге: таблица `rosstat_ppi_ved`, 4 ряда, покрытие;
   - подробный раздел пакета: импортёр, ссылка/селектор, разбор, no-op/архив, отступление §10.2 (витрина в T5).
4. `docs/ROADMAP_KEY_RATE.md` — не трогать: research-абзац в открытом PR #275; статус обновится после мержа (T5/T6).

### Шаг 7. Проверки

1. `make all` — gofmt, golangci-lint, `go vet ./...`, `go test -race ./...`, обе сборки.
2. `go test -race -count=1 ./rosstat/`.
3. `make docs-check`.
4. `dagu validate dagu/rosstat.yaml` (если `dagu` установлен).

### Шаг 8. Живая приёмка

1. `make ch-status`; если ClickHouse не запущен — попросить владельца, не делать `make ch-up`.
2. Первый прогон:
   ```bash
   ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev \
     make import STAT=rosstat_ppi_ved
   ```
3. MCP `mcp__clickhouse__run_query`:
   - `SELECT count(), min(date), max(date) FROM rosstat_ppi_ved FINAL`;
   - `SELECT index_type, count(), min(date), max(date) FROM rosstat_ppi_ved FINAL GROUP BY index_type ORDER BY index_type`;
   - spot-значения из спеки §8.
4. Повторный прогон — no-op, без скачивания, `count()` не меняется.
5. Проверить `source_publications` и объект `build/archive-dev/rosstat_ppi_ved/`.
6. `make mcp-check` — регрессия (новой проверки ИЦП в T1 нет).

### Шаг 9. Ревью и PR

1. Ревью субагентом `anthropic/haiku`, финальное — `deepseek/deepseek-flash`; fix-loop.
2. Коммиты: спека/план, код, тесты/фикстура, документация. Сообщения — по стилю репозитория.
3. `git push -u origin feat/rosstat-ppi-ved`.
4. PR:
   - заголовок `[key-rate/phase-4-rosstat-ppi-sa] T1: импорт ИЦП Росстата по ВЭД`;
   - `Closes #276` вне backticks;
   - описание: источник, схема, запуск, пример проверки, отступление §10.2;
   - Project `Status = In Review`, `PR = <номер>`;
   - протокол живой приёмки — комментарием в PR.
5. Merge — решение владельца.

## 3. Критерии готовности

- [ ] `make import STAT=rosstat_ppi_ved` — `m/m` 1998-01…2026-08 без пропусков; `ytd`/`y/y`/`period` с покрытием §3 спеки.
- [ ] `series_catalog` — 4 ряда, контракт с разбором двусторонний.
- [ ] Тесты на оба формата листов, строку РФ, `…`/пустые, перекрытие ОКВЭД→ОКВЭД2.
- [ ] `make all`, `go test -race -count=1 ./rosstat/`, `make docs-check` — зелёные.
- [ ] Повтор — no-op по версии; архив и `source_publications`.
- [ ] `dagu/rosstat.yaml` содержит шаг; T6 уточняет расписание.
- [ ] Документация синхронна; PR с протоколом.

## 5. Протокол живой приёмки (2026-10-20)

- `make all` — exit 0: `golangci-lint` 0 issues, `go vet ./...` чисто, `go test -race ./...` все пакеты `ok`, обе сборки.
- `make docs-check` — exit 0: 43 страницы, 33 теста docs.
- `dagu validate dagu/rosstat.yaml` — exit 0.
- Первый прогон: `make import STAT=rosstat_ppi_ved` — `Imported 571 rows of rosstat_ppi_ved`; MCP/`ch-sql`:
  - `m/m` 344, `1998-01-31 … 2026-08-31`;
  - `ytd` 91, `1998-12-31 … 2026-08-31`;
  - `y/y` 68 и `period` 68, `2021-01-31 … 2026-08-31`;
  - spot: 1998-01 m/m 100.8, 1998-12 ytd 119.4, 2013-01 m/m 99.9, 2021-01 m/m/ytd/y/y/period 103.36/103.36/106.69/106.69, 2026-01 m/m 98.45, 2026-08 m/m/ytd/y/y/period 100.86/107.86/106.53/103.22.
- Повтор: `no-op, версия 47c5a92ba83a6729e2cebb2e1e9887ca уже импортирована, 571 точек на месте`; `count()` — 571.
- Архив: объект `build/archive-dev/rosstat_ppi_ved/47c5a92ba83a6729_Proizvoditeli_Ind_VED_08-2026.xlsx` (6 564 513 Б), строка `registry.jsonl` с sha256 `4694a5bb…`; `source_publications` — `name=Proizvoditeli_Ind_VED_08-2026.xlsx`, `points=571`. Архив воспроизведён прямым запуском с `ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev` (make-обёртка в этой сессии переменную не пробрасывала — env-файл перекрывает; это не дефект импортёра).
- `make mcp-check` — **ПРОВЕРКА ПРОЙДЕНА** после merge `main`: каталог `rosstat` — 169 рядов, сырая `rosstat_ppi_ved` закрыта (497), агрегатный ряд бюллетеней+BI — 760 точек. До merge `main` чек ожидал 502 и падал на `rosstat_ipc_weeks_bulletins` — это был устаревший чек ветки, а не дефект T1; merge принёс обновлённое ожидание 760.
- Полный винтаж 08-2026 проверен временным тестом: 571 точка (344/91/68/68), глубины совпадают.

## 4. Риски

- `:not([href*="2kv-"])` — проверить тестом на `price_page.html`; если cascadia не поддержит, заменить на разбор всех ссылок и выбор по regex месячного имени.
- `8.1` — 10 244 строки; парсер ищет точный код и останавливается на строке РФ, не читая регионы.
- Фикстура ~1 МБ — допустимо; `xlsx_trim.py` сохраняет XML листов побайтово.
- Если ClickHouse недоступен, PR помечается «живая приёмка не проверена».
