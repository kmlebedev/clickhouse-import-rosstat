# Оперативная точка инфляционных ожиданий из PDF (`cbr_infl_exp_op`) — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** забирать точку инфляционных ожиданий за новый месяц из PDF «Оперативная справка» `inFOM_ГГ-ММ.pdf` в `cbr_infl_exp` раньше, чем выйдет `Infl_exp_ГГ-ММ.xlsx`, и не делать ничего, если PDF не обновился.

**Architecture:** отдельный импортёр `cbr_infl_exp_op` (свой `Name()`, своя строка версии) пишет в общую таблицу `cbr_infl_exp` те же два ряда; собственный `Import()` ищет свежую PDF-ссылку на странице, держит гейт по `max(date)` и делегирует скачивание/архив/каталог шаблону `util.ClickHouseImport` через `BodyFunc`. Разбор PDF — чистая функция по `pdftotext -bbox`.

**Tech Stack:** Go 1.27, clickhouse-go/v2, logrus, goquery (`util.GetPage`/`util.FindLink`), внешняя утилита `pdftotext` (Poppler) — только в прод-пути, тесты офлайн на фикстуре.

**Spec:** `docs/superpowers/specs/2026-10-21-cbr-infl-exp-operational-pdf-design.md`

## Global Constraints

- Схема `cbr_infl_exp` (`name LowCardinality(String), date Date, value Float32`, ключ `(name, date)`), имена рядов, `v_cbr_macro`, `v_key_rate_dashboard`, `sql/mcp_kimi_reader.sql` — **не меняются**.
- Сеть — только через `util.GetPage`/`util.GetFile*`; `exec.CommandContext` — только для локального `pdftotext`.
- Вставка — один `PrepareBatch` на прогон через шаблон; `Append` с проверкой.
- Ошибки не проглатываются, `panic` в разборе запрещён, логирование — `log.Infof/Warnf`.
- Тесты офлайн: `pdftotext` в тестах не вызывается; фикстура — дословный вывод `-bbox`.
- Новый шаг dagu — **в хвосте** цепочки.
- Модели субагентов: код — `deepseek/deepseek-flash`; док-правки — `joingonka/zai-org/GLM-5.3-Flash`; ревью — `deepseek/deepseek-flash`.

## Review Focus

1. **Гейт.** Повторный прогон не скачивает PDF и не пишет в БД; `max(date)` сравнивается с 14-м числом месяца PDF, а не с датой публикации.
2. **Координаты.** Последняя колонка — ровно 3 значения; порядок сверху вниз = наблюдаемая / через год / через пять лет; месяц оси обязан совпасть с именем файла.
3. **Скан.** PDF без текстового слоя — ошибка с именем импортёра, а не `0 rows`.
4. **Два писателя.** `Name()` ≠ `TableName` только у `cbr_infl_exp_op`; allowlist теста реестра литеральный, с комментарием.
5. **Идемпотентность.** Повторный полный путь (после сброса версии) пишет те же строки; XLSX перезаписывает значение точным.

---

### Task 1: фикстуры

**Files:**
- Create: `cbr/testdata/infl_exp_pdf_26-10_bbox.xml` (дословный `pdftotext -bbox -enc UTF-8 -f 1 -l 1` по PDF 26-10)
- Update: `cbr/testdata/infl_exp_page.html` (снимок страницы 2026-10-21, свежая ссылка `inFOM_26-10.pdf`)

- [ ] Скачать `https://www.cbr.ru/Collection/Collection/File/62459/inFOM_26-10.pdf` во временный файл (в репозиторий не коммитить).
- [ ] `pdftotext -bbox -enc UTF-8 -f 1 -l 1 inFOM_26-10.pdf cbr/testdata/infl_exp_pdf_26-10_bbox.xml`.
- [ ] Обновить `cbr/testdata/infl_exp_page.html` текущей страницей `https://www.cbr.ru/statistics/ddkp/inflationary_expectations/`.
- [ ] Проверить: в bbox-фикстуре 102 десятичных подписи, 34 подписи оси рис. 1, в последней колонке `14,5 / 13,2 / 12,2`.

### Task 2: тесты разбора (TDD)

**Files:**
- Update: `cbr/Infl_exp_test.go`

- [ ] `TestInflExpPdfPointsFixture` — фикстура → последняя колонка `2026-10`, значения `14.5/13.2/12.2`, число колонок 34.
- [ ] `TestInflExpPdfHistoryMatchesXlsx` — 33 исторические колонки против `testdata/infl_exp.xlsx` (ряды 109/110, допуск 0.05).
- [ ] `TestInflExpPdfNoTextLayerFails`, `TestInflExpPdfColumnShapeFails` (обрезка колонки, перестановка значений, подмена месяца оси) — ошибка, приёмник пуст.
- [ ] `TestInflExpPdfNotPDFFails`, `TestInflExpPdfToolMissingFails` — сигнатура до exec и подсказка `poppler-utils`.
- [ ] `TestInflExpPdfNewer` — таблица случаев гейта.
- [ ] `TestInflExpPdfFindLink` — первая PDF-ссылка `inFOM_26-10.pdf`; xlsx-ссылка находится.
- [ ] `TestInflExpPdfAppendRows` — две строки с датой `2026-10-14`; `Append`-ошибка возвращается.
- [ ] Прогнать: `go test ./cbr/ -run 'InflExpPdf'` — красный (кода нет).

### Task 3: парсер и exec-обёртка

**Files:**
- Create: `cbr/Infl_exp_pdf.go`

- [ ] Константы: `inflExpPdfLinkSelector`, `inflExpPdfTimeout`, `inflExpPdfHeaderSize`, `inflExpPdfTool` (var), регексы слов/оси/значений, карта коротких месяцев.
- [ ] `inflExpPdfBBox(body []byte) ([]byte, error)` — сигнатура, команда, таймаут, ошибки.
- [ ] `inflExpPdfPoints(bbox []byte) ([]inflExpPdfPoint, error)` — колонки по координатам, валидации §3.2 спеки.
- [ ] `inflExpPdfNewer(month, known time.Time) bool` + нормализация к суткам.
- [ ] `inflExpPdfMonthFromHref(href string) (time.Time, error)` через `inflExpArchiveRe`.
- [ ] `inflExpPdfAppend(points, batch)` — две строки.
- [ ] Прогнать: `go test ./cbr/ -run 'InflExpPdf'` — зелёный.

### Task 4: импортёр, реестр, dagu

**Files:**
- Update: `cbr/Infl_exp.go`, `cbr/importers.go`, `dagu/cbr.yaml`

- [ ] Вынести DDL в `cbrInflExpDdl`, добавить `cbrInflExpOpName`, `cbrInflExpMaxDateQuery`.
- [ ] `type inflExpOpStat struct{ util.ClickHouseImport }` с `Name()` и `Import()` (DDL → страница → ссылка → месяц → гейт → `DataUrl` → шаблон).
- [ ] `var cbrInflExpOpImporter = &inflExpOpStat{...}` с `PublicationSource`, `PublicationCountPoints`, `BodyFunc`, `AfterImport`.
- [ ] Запись в `cbrImporters` + комментарий о втором писателе.
- [ ] Шаг `cbr_infl_exp_op` в хвосте `dagu/cbr.yaml`.
- [ ] `dagu validate dagu/cbr.yaml`.

### Task 5: тест реестра

**Files:**
- Update: `cbr/importers_test.go`

- [ ] Литеральный allowlist `cbrSharedTableOwners = {"cbr_infl_exp_op": "cbr_infl_exp"}` с комментарием; проверка (б) пропускает пары из allowlist.
- [ ] `expectedPublicationCountPoints["cbr_infl_exp_op"] = "SELECT count() FROM cbr_infl_exp FINAL"`.
- [ ] Прогнать: `go test ./cbr/ -run 'TestCbrImportersPrefixedAndRegistered'` — зелёный.

### Task 6: документация

**Files:**
- Update: `docs/IMPORTERS.md`, `ARCHITECTURE.md`, `README.md` (если перечислены импортёры/шаги)

- [ ] §6.1 — строка реестра `cbr_infl_exp_op` и оговорка про второго писателя в `cbr_infl_exp`.
- [ ] §8 п. 13 — счётчик объявивших префлайт и список `cbr/` (+1).
- [ ] §7 — число шагов `dagu/cbr.yaml` и хвост цепочки.
- [ ] `docs/IMPORTERS.md` — абзац о PDF-источнике и гейте.
- [ ] `README.md` — правка, только если счётчики/списки там есть.

### Task 7: проверки и приёмка

- [ ] `make all` — зелёный.
- [ ] `make docs-check` — зелёный.
- [ ] `make import STAT=cbr_infl_exp_op` → `Imported 2 rows`; `v_cbr_macro` за `2026-10-14` — 14.5 и 13.2.
- [ ] Повтор → `Imported 0 rows`, в логе «уже импортирован (max=2026-10-14)», PDF не скачивается.
- [ ] `make import STAT=cbr_infl_exp` → зелёный, повтор — no-op.
- [ ] `make mcp-check` — без новых провалов.
- [ ] Протокол приёмки — комментарием в PR.
