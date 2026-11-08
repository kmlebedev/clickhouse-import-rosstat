# План: util.Fallback + rosstat_ipc_weeks / cbr_infl_exp_op

**Спека:** [2026-11-08-util-pdf-fallback-design.md](../specs/2026-11-08-util-pdf-fallback-design.md).

## 1. `util`

- [ ] `util/fallback.go`: `FallbackDocument`, `FallbackRow`, `FallbackResult`, `Fallback`, `FallbackKnownDates`, `FallbackMissing`, `Fallback.Import`.
- [ ] `util/pdf.go`: `PDFMode`, `PDFTool`, `PDFText`.
- [ ] `util/fallback_test.go`: `FallbackMissing`, no-op (`AfterImport`), ошибка `Documents`.
- [ ] `util/pdf_test.go`: не-PDF, отсутствующий `pdftotext`.

## 2. CBR

- [ ] `cbr/Infl_exp.go`: `inflExpOpStat.fallback`, тонкий `Import`, конструктор на `util.Fallback`; удалить `cbrInflExpMaxDateQuery`, `inflExpPdfNewer`, `importPDF`.
- [ ] `cbr/Infl_exp_pdf.go`: `inflExpPdfBBox(ctx, body)` через `util.PDFText`; `inflExpPdfDocuments`, `inflExpPdfDocumentsFromPage`, `inflExpPdfRows`, `inflExpPdfRowsFromBBox`, `inflExpPdfAppendRow`.
- [ ] `cbr/Infl_exp_test.go`: новые тесты; `TestInflExpPdfNewer` удалить; `pdftotext`-тест на `util.PDFTool`.
- [ ] `make import STAT=cbr_infl_exp_op` — no-op/новая точка, каталог/витрина.

## 3. Rosstat

- [ ] `rosstat/ipc_weeks.go`: `ipcWeeksKnownDatesQuery`, `IpcWeeksStat.Import` (restore → DDL → XLSX → `util.Fallback`), политика ошибок.
- [ ] `rosstat/ipc_weeks_news.go`: URL ленты, `ipcWeeksNewsReleases`, `ipcWeeksNewsDocuments`, `ipcWeeksNewsRows`, `ipcWeeksNewsAppendRow`, `ipcWeeksNewsParse`, карта канонизации.
- [ ] `rosstat/ipc_weeks_bulletins.go`: чистая `ipcWeeksBulletinsCanonical`.
- [ ] `rosstat/series_meta.go`: `Origin`/`Description` с PDF-фолбэком.
- [ ] Фикстуры: два текста PDF, HTML-фрагмент ленты.
- [ ] `rosstat/ipc_weeks_news_test.go`: 108 точек × 2, spot-values, имена, лента, ошибки.
- [ ] `make import STAT=rosstat_ipc_weeks` — 108 строк за 2026-11-02, повтор no-op.
- [ ] `rosstat/ipc_weeks_news.go`: второй PDF-источник «О потребительских ценах на нефтепродукты» (`Kind: petrol`) — 3 марки бензина АИ-92/95/98; `ipcWeeksKnownDatesQuery` гейтит по всем трём маркам сразу, чтобы неделя без них пере-импортировалась.
- [ ] `util/fallback.go`: дедупликация документов по URL, а не по дате — у одной недели два PDF.
- [ ] `make import STAT=rosstat_ipc_weeks` — 111 строк (108 CPI + 3 бензин), повтор no-op.

## 4. Документы

- [ ] `ARCHITECTURE.md` §2, §3, §7, таблица §8, §8 п. 13.
- [ ] `docs/IMPORTER_PACKAGE_STANDARD.md` §5.
- [ ] `docs/IMPORTERS.md` (CBR + rosstat).
- [ ] `docs/LIMITATIONS.md`.
- [ ] `docs/research/2026-11-06-rosstat-source-availability.md` §5, `docs/ROADMAP_ROSSTAT.md` §3 (после живой приёмки).

## 5. Проверки

- [ ] `make all`.
- [ ] `go test -race ./util/... ./cbr/... ./rosstat/...`.
- [ ] `make mcp-check`.
- [ ] `make docs-check`.
- [ ] `git status` — только нужные файлы.
