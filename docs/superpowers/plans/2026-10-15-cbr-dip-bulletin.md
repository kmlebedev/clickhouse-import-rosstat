# План: `cbr_dip_bulletin` — сезонно сглаженные ряды приложений ДИП ЦБ и их винтажи (фаза 4, контур прогноза)

**Дата:** 15 октября 2026 г.
**Спека:** [2026-10-15-cbr-dip-bulletin-design.md](../superpowers/specs/2026-10-15-cbr-dip-bulletin-design.md).
**Пункт роадмапа:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) фаза 4, первый шаг (§4.4).
**Ветка:** `cbr-dip-bulletin`. **Исполнение:** `superpowers:subagent-driven-development` — **продовый код пишет `joingonka/zai-org/GLM-5.3-Flash`, тесты — `deepseek/deepseek-flash`** (решение владельца 2026-10-15: у GLM лимит вывода 8192, и тестовые диффы на нём не укладываются; тесты ведёт отдельный исполнитель и он же отвечает за их полноту), ревью задач — `anthropic/haiku`, fix-раунды ≤ 3 на задачу, далее эскалация контроллеру; финальное ревью ветки — `deepseek/deepseek-flash`.

---

## Критерий приёмки пункта

Зелёный `make all`; живой прогон `make import STAT=cbr_dip_bulletin` (первый — с `ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev`, затем с `ARCHIVE_BACKEND=yandex`) c записью факта (команда, число строк, число выпусков, крайние даты); повторный прогон — no-op без скачивания приложений; spot-сверка с живыми xlsx; объекты всех 49 файлов в архиве и строки в `docs/archive/registry.jsonl`; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `dagu validate dagu/cbr.yaml` — exit 0; `ARCHITECTURE.md` / `README.md` / `docs/ROADMAP_KEY_RATE.md` тем же PR; протокол приёмки — комментарием в PR (стандарт §16.1 п. 8).

**Сделано до начала задач** (контроллером): ветка `cbr-dip-bulletin`; фикстуры `cbr/testdata/dip_bulletin_26-06.xlsx` (листы `2`, `23`), `dip_bulletin_26-07.xlsx` (`2`, `20`), `dip_bulletin_2021-06.xlsx` (`3`, `64`), `dip_page.html`; инструменты `scripts/xlsx_trim.py` (trim/verify) и `scripts/xlsx_inspect.py`; спека и этот план — первыми коммитами.

## Задачи

### Task 1: разбор страницы выпусков — `cbr/dip_page.go`

- Константы пакета: адрес страницы `https://www.cbr.ru/ec_research/mb/`, CSS-селектор ссылок на приложения (`a.versions_item`, отбор по `href`, заканчивающемуся на `.xlsx`), имя атрибута даты публикации (`data-tooltip-content`), префикс домена для относительных href (стандарт §4.5, §4.4).
- Тип выпуска: `id` (из пути `/Collection/Collection/File/<id>/`, только цифры), `url`, `publicationDate` (`time.Time`, разбор «Опубликовано 14 октября 2026» — месяц через `util.MonthsToNum`), `release` (`26-07`: две цифры года из группы `data-versions-items` + номер из «№ N» текста ссылки). Порядок выпусков — как на странице (свежие первыми); сортировка по дате — за импортёром.
- Функция разбора страницы: вход — `*goquery.Document` (или HTML), выход — `[]dipRelease`; **fail-fast**: нет ни одной ссылки, ссылка без даты публикации, id не цифры, дата не разбирается, номер выпуска не найден — ошибка с именем файла-источника и ожидаемым признаком (стандарт §6.4). Дубли `id` на странице — ошибка.
- `cbr/dip_page_test.go`: разбор живой фикстуры `testdata/dip_page.html` — 49 ссылок и 49 уникальных `id` (одно имя файла повторяется дважды: `stat_bulletin_23-07.xlsx` с id `46493` и `46646`, различаются `release` — `23-07` и `23-08`); spot-проверки: `bulletin_26-07.xlsx` → id `62444`, публикация 2026-10-14, `release` = `26-07`; старейший `stat_bulletin_20-06.xlsx` → id `29360`, публикация 2020-10-14; fail-fast-ветки на мутированном HTML (без блока ссылок; ссылка без `data-tooltip-content`).

### Task 2: разбор книги — `cbr/dip_parse.go` + `cbr/dip_parse_test.go`

- Объявленный реестр рядов (спека §3.2): структура «ряд → лист-якоря → подписи колонки», 8 рядов, нормализация подписей (переносы/пробелы/кавычки/сноски/тире).
- Разбор книги: вход `*excelize.File` + мета выпуска (`releaseID`, `release`, `publicationDate`); выход — точки `{series, date, value}`; сеть и БД не трогаются (стандарт §6.1).
- Правила спек §3.4: лист по `A1`; строка шапки — первая в пределах 10 строк с любой объявленной подписью; данные со следующей строки, дата в колонке A (серийное число Excel или текст `ГГГГ-ММ-ДД`/`ДД.ММ.ГГГГ`); точка — последний день месяца; сырые ячейки (`RawCellValue`); пустая ячейка — не точка; нечисловое значение — ошибка; дубль `(series, date)` — ошибка; непустое значение в объявленных колонках ниже данных — ошибка, текст только в колонке A — норма.
- Предупреждения: нет листа; нет отдельной колонки в найденном листе — остальные колонки импортируются. Ошибки: нет ни одного объявленного листа; в найденном листе нет ни одной объявленной колонки; нулевой разбор книги.
- Тесты на трёх фикстурах: число точек и даты по каждому ряду; spot-значения (26-06 лист `23`: `unemployment_nsa` 2015-01-31 = 5.455578448855274 и `unemployment_sa` 2015-01-31 = 5.233359, 2026-06-30 = 2.239751; 26-07 лист `20`: `employment_nsa` 2017-01-31 = 59.1, `employment_sa` 2017-01-31 = 59.67240922577325; 21-06 лист `64`: `unemployment_nsa` 2015-01-31 = 5.4891410816903825, `unemployment_sa` 2015-01-31 = 5.27924038461241; SAAR 26-06: `saar_all_goods` 2021-01-31 = 4.781574875641297); **ревизия одного месяца в двух выпусках** — месяц, присутствующий и в 26-06, и в 26-07 (SAAR), обе точки лежат в результате с разными `release_id`; отсутствие листа безработицы в 26-07 даёт точки занятости и предупреждение, а не ошибку; сноска в 21-06 не ломает разбор; fail-fast-ветки на мутированных копиях фикстур (пустая книга; книга без объявленных листов; лист с переименованными колонками; нечисловое значение; дубль точки); харнесс `appendRecorder` (стандарт §11.6).
- Приёмка задачи: `go test -race -count=1 ./cbr/` — зелёный; ожидаемые числа в тестах получены из фикстур и сверены с полными живыми файлами в `/tmp/dip/` (файлы ЦБ скачаны 2026-10-15).

### Task 3: импортёр и производные — `cbr/dip_bulletin.go`, `cbr/dip_meta.go`, `cbr/views.go`, `cbr/importers.go`, `dagu/cbr.yaml`

- `cbr/dip_bulletin.go`: структура с встроенным `util.ClickHouseImport` (`TableName: cbr_dip_bulletin`, DDL спеки §3.1 в `%s`-форме, `ImportFunc` → разбор Task 2, `PublicationCountPoints: SELECT count() FROM cbr_dip_bulletin FINAL`, `AfterImport: afterCbrDip(dipSeriesMeta)`); свой `Import()` по спеке §3.5: DDL → `util.GetPage` → `dipParseReleases` → гейт `SELECT DISTINCT release_id FROM cbr_dip_bulletin FINAL` → цикл по новым выпускам в хронологическом порядке (старые первыми) с вызовом `s.ClickHouseImport.Import` на выпуск; лог на выпуск; ошибки копятся `errors.Join`; при отсутствии новых — `Imported 0 rows` и «новых выпусков нет».
- `cbr/dip_meta.go`: 8 записей каталога (спека §3.6).
- `cbr/views.go`: `cbrDipView` (`v_cbr_dip_bulletin`: `source, series, date, value, release, release_id, publication_date`, комментарии таблицы и каждой колонки) + `afterCbrDip(meta)`.
- `cbr/importers.go`: 19-я запись `{cbrDipBulletinImporter, "cbr_dip_bulletin", "SELECT count() FROM cbr_dip_bulletin FINAL"}` — литералами; порядок записей повторяет `dagu/cbr.yaml`.
- `dagu/cbr.yaml`: шаг `cbr_dip_bulletin` 19-м (в хвост), `env: CLICKHOUSE_IMPORT_STAT=cbr_dip_bulletin`, `retry_policy` как у соседей.
- Приёмка задачи: `go build ./...`; `dagu validate dagu/cbr.yaml` — exit 0; `go test -race -count=1 ./cbr/` — зелёный (реестр-энфорсер на этом шаге ещё не обновлён — обновляется в Task 4; если он падает на числе префлайтов, это ожидаемо и Task 4 закрывает).

### Task 4: тесты импортёра и тест-энфорсер реестра

- `cbr/dip_bulletin_test.go`: на фейковом `driver.Conn` — DDL выполняется до сети; гейт фильтрует известные выпуски (в цикл попадают только новые, порядок старых → новых); ошибка одного выпуска не отменяет остальные и агрегируется; пустой список выпусков со страницы — ошибка; отсутствие новых выпусков — 0 строк без скачивания.
- `cbr/importers_test.go`: в `expectedPublicationCountPoints` + `"cbr_dip_bulletin": "SELECT count() FROM cbr_dip_bulletin FINAL"`; в `keyScheduled` — `cbr_dip_bulletin` («сезонно сглаженные ряды ДИП не обновляются, а витрина `v_cbr_dip_bulletin` тихо стареет») + комментарии-счётчики (19 импортёров, 19 шагов).
- Приёмка задачи: `go test -race -count=1 ./cbr/` — зелёный.

### Task 5: MCP-контур — грант, проверки, снимок покрытия

- `sql/mcp_kimi_reader.sql`: `GRANT SELECT ON default.v_cbr_dip_bulletin TO kimi_reader;` (сырая `cbr_dip_bulletin` не выдаётся).
- `scripts/mcp_check.py`: блок `cbr_dip_bulletin` — витрина видна и непуста (`count() > 0`), каталог `source = 'cbr_dip_bulletin'` содержит 8 рядов, сырая `cbr_dip_bulletin` закрыта (отказ ClickHouse 497); стиль — как у блоков `cbr_bop_*`.
- `sql/key_rate_coverage.sql`: ветка снимка покрытия по `cbr_dip_bulletin` (`series`, число точек, крайние даты) с комментарием о винтажности.
- Приёмка задачи: `python3 -m py_compile scripts/mcp_check.py`; блок запускается в живой приёмке (контроллер).

### Task 6: документация

- `ARCHITECTURE.md`: §2 (пакет `cbr/` — 19 импортёров; строка `scripts/` — новые `xlsx_trim.py`/`xlsx_inspect.py`), §3 (упоминание многoвыпускового импортёра), §6.1 (строка `cbr_dip_bulletin`), §6.2 (канонический DDL `cbr_dip_bulletin`), §6.3 (витрина `v_cbr_dip_bulletin`), §7 (шаг в расписании), §8 п. 13 (префлайт объявлен у 19 импортёров; оговорка про гейт вместо HEAD у `cbr_dip_bulletin`).
- `README.md`: таблица импортёров (строка `cbr_dip_bulletin`), структура репозитория (`scripts/`, `cbr/`), эксплуатационные оговорки (первый прогон ≈ 4–6 мин, архив ≈ 72 МБ, повтор — no-op).
- Порядок — skill `.kimi-code/skills/sync-readme-architecture` (сверка «код ↔ документы»).
- Приёмка задачи: `make all` — зелёный.

### Task 7: живая приёмка (контроллер, не субагент)

1. `make all` (gofmt, lint, vet, `go test -race`, обе сборки).
2. `make ch-status` (ClickHouse поднимает владелец; если не отвечает — остановиться и попросить).
3. `make import STAT=cbr_dip_bulletin ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev` — первый прогон: ≈ 20–25 тыс. точек, 49 выпусков, крайние даты 2015-01-31 … 2026-09-30; проверить `ls build/archive-dev/cbr_dip_bulletin | wc -l` = 49.
4. Повтор — `Imported 0 rows`, «новых выпусков нет», без скачиваний (время прогона).
5. Spot-сверка с живыми xlsx (SA и NSA, один месяц из двух выпусков).
6. `make import STAT=cbr_dip_bulletin ARCHIVE_BACKEND=yandex` + `make archive-registry` → 49 строк в `docs/archive/registry.jsonl`.
7. `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА; `dagu validate dagu/cbr.yaml` — exit 0.
8. Записать протокол в план и в тело коммита статуса.

### Task 8: статус роадмапа и PR

- `docs/ROADMAP_KEY_RATE.md`: фаза 4 — статус пункта ✅ с командой и результатом (стандарт §13.2, §16.1).
- Один PR в конце; описание — источник, схема, запуск, пример проверки; протокол приёмки — комментарием в PR.

## Порядок диспатча

1. Task 1 и Task 2 — последовательно (Task 2 не зависит от Task 1, но обе трогают пакет `cbr/`; параллельный диспатч в один пакет не даём).
2. Task 3 → Task 4 → Task 5 → Task 6 (Task 3 зависит от 1 и 2; 4 — от 3; 5 — от 3; 6 — от всех).
3. Task 7 и Task 8 — контроллер.

## Чек-лист жёстких правил для ревью-промптов

- Вставка — одним `PrepareBatch` на выпуск (`Append` с проверкой `err`), `conn.Exec` в цикле запрещён (AGENTS.md правило 2; исключение — `conn.Exec` для DDL).
- Сеть — только `util.GetPage`/`util.GetFileBytes` внутри `Import()`; в `init()` — ничего.
- Ошибки не проглатываются; `panic` нет; индексация с проверкой.
- Fail-fast против warn — по спеке §3.4 (список обязателен к проверке в тестах).
- Логи — logrus, без `fmt.Print`.
- Реестр и `countPoints` — литералами (стандарт §8.2), тест-энфорсер обновлён.
- Дублируемость: повторный прогон ничего не пишет; тесты офлайн, без сети и БД.
