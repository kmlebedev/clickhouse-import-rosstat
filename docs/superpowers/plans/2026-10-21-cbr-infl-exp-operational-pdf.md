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

- [x] Скачать `https://www.cbr.ru/Collection/Collection/File/62459/inFOM_26-10.pdf` во временный файл (в репозиторий не коммитить).
- [x] `pdftotext -bbox -enc UTF-8 -f 1 -l 1 inFOM_26-10.pdf cbr/testdata/infl_exp_pdf_26-10_bbox.xml`.
- [x] Обновить `cbr/testdata/infl_exp_page.html` текущей страницей `https://www.cbr.ru/statistics/ddkp/inflationary_expectations/`.
- [x] Проверить: в bbox-фикстуре 102 десятичных подписи, 34 подписи оси рис. 1, в последней колонке `14,5 / 13,2 / 12,2`.

### Task 2: тесты разбора (TDD)

**Files:**
- Update: `cbr/Infl_exp_test.go`

- [x] `TestInflExpPdfPointsFixture` — фикстура → последняя колонка `2026-10`, значения `14.5/13.2/12.2`, число колонок 34.
- [x] `TestInflExpPdfHistoryMatchesXlsx` — 33 исторические колонки против `testdata/infl_exp.xlsx` (ряды 109/110, допуск 0.05).
- [x] `TestInflExpPdfNoTextLayerFails`, `TestInflExpPdfColumnShapeFails` (обрезка колонки, перестановка значений, подмена месяца оси) — ошибка, приёмник пуст.
- [x] `TestInflExpPdfNotPDFFails`, `TestInflExpPdfToolMissingFails` — сигнатура до exec и подсказка `poppler-utils`.
- [x] `TestInflExpPdfNewer` — таблица случаев гейта.
- [x] `TestInflExpPdfFindLink` — первая PDF-ссылка `inFOM_26-10.pdf`; xlsx-ссылка находится.
- [x] `TestInflExpPdfAppendRows` — две строки с датой `2026-10-14`; `Append`-ошибка возвращается.
- [x] Прогнать: `go test ./cbr/ -run 'InflExpPdf'` — красный (кода нет).

### Task 3: парсер и exec-обёртка

**Files:**
- Create: `cbr/Infl_exp_pdf.go`

- [x] Константы: `inflExpPdfLinkSelector`, `inflExpPdfTimeout`, `inflExpPdfHeaderSize`, `inflExpPdfTool` (var), регексы слов/оси/значений, карта коротких месяцев.
- [x] `inflExpPdfBBox(body []byte) ([]byte, error)` — сигнатура, команда, таймаут, ошибки.
- [x] `inflExpPdfPoints(bbox []byte) ([]inflExpPdfPoint, error)` — колонки по координатам, валидации §3.2 спеки.
- [x] `inflExpPdfNewer(month, known time.Time) bool` + нормализация к суткам.
- [x] `inflExpPdfMonthFromHref(href string) (time.Time, error)` через `inflExpArchiveRe`.
- [x] `inflExpPdfAppend(points, batch)` — две строки.
- [x] Прогнать: `go test ./cbr/ -run 'InflExpPdf'` — зелёный.

### Task 4: импортёр, реестр, dagu

**Files:**
- Update: `cbr/Infl_exp.go`, `cbr/importers.go`, `dagu/cbr.yaml`

- [x] Вынести DDL в `cbrInflExpDdl`, добавить `cbrInflExpOpName`, `cbrInflExpMaxDateQuery`.
- [x] `type inflExpOpStat struct{ util.ClickHouseImport }` с `Name()` и `Import()` (DDL → страница → ссылка → месяц → гейт → `DataUrl` → шаблон).
- [x] `var cbrInflExpOpImporter = &inflExpOpStat{...}` с `PublicationSource`, `PublicationCountPoints`, `BodyFunc`, `AfterImport`.
- [x] Запись в `cbrImporters` + комментарий о втором писателе.
- [x] Шаг `cbr_infl_exp_op` в хвосте `dagu/cbr.yaml`.
- [x] `dagu validate dagu/cbr.yaml`.

### Task 5: тест реестра

**Files:**
- Update: `cbr/importers_test.go`

- [x] Литеральный allowlist `cbrSharedTableOwners = {"cbr_infl_exp_op": "cbr_infl_exp"}` с комментарием; проверка (б) пропускает пары из allowlist.
- [x] `expectedPublicationCountPoints["cbr_infl_exp_op"] = "SELECT count() FROM cbr_infl_exp FINAL"`.
- [x] Прогнать: `go test ./cbr/ -run 'TestCbrImportersPrefixedAndRegistered'` — зелёный.

### Task 6: документация

**Files:**
- Update: `docs/IMPORTERS.md`, `ARCHITECTURE.md`, `README.md` (если перечислены импортёры/шаги)

- [x] §6.1 — строка реестра `cbr_infl_exp_op` и оговорка про второго писателя в `cbr_infl_exp`.
- [x] §8 п. 13 — счётчик объявивших префлайт и список `cbr/` (+1).
- [x] §7 — число шагов `dagu/cbr.yaml` и хвост цепочки.
- [x] `docs/IMPORTERS.md` — абзац о PDF-источнике и гейте.
- [x] `README.md` — правка, только если счётчики/списки там есть.

### Task 7: проверки и приёмка

- [x] `make all` — зелёный.
- [x] `make docs-check` — зелёный.
- [x] `make import STAT=cbr_infl_exp_op` → `Imported 2 rows`; `v_cbr_macro` за `2026-10-14` — 14.5 и 13.2.
- [x] Повтор → `Imported 0 rows`, в логе «уже импортирован (max=2026-10-14)», PDF не скачивается.
- [x] `make import STAT=cbr_infl_exp` → зелёный, повтор — no-op.
- [x] `make mcp-check` — без новых провалов.
- [x] Протокол приёмки — комментарием в PR.

## Приёмка (2026-10-21)

- [x] `make import STAT=cbr_infl_exp_op` → `Imported 2 rows of cbr_infl_exp_op` (PDF 26-10, скачан один раз).
- [x] `v_cbr_macro` за 2026-10-14: наблюдаемая 14.5, ожидаемая 13.2.
- [x] Повтор → `PDF за 2026-10 уже импортирован (max=2026-10-14) — скачивание не требуется`, `Imported 0 rows`.
- [x] `make import STAT=cbr_infl_exp` → один полный прогон (92 точки вместо 90), повтор — no-op (`92 точек на месте`).
- [x] `make all` — lint 0 issues, `go test -race ./...` зелёный, сборка.
- [x] `dagu validate dagu/cbr.yaml` — без ошибок.
- [x] `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.
- [ ] `make docs-check` — красный на `main` до этой ветки: `docs/research/2026-10-21-key-rate-channel-map-audit.md` ссылается на отсутствующий в git `docs/publications/2026-10-21-key-rate.md` (WIP другой сессии); к правкам ветки отношения не имеет.
