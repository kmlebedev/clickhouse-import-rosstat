# План: PDF-фолбэк агрегата недельного ИПЦ

**Спека:** [2026-11-08-bi-cpi-weekly-pdf-fallback-design.md](../specs/2026-11-08-bi-cpi-weekly-pdf-fallback-design.md). **Пункт:** `rosstat/1.13`, issue [#546](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/546).

## 1. Трекер и ветка

- [x] issue #546 создан, `role:go`/`roadmap:rosstat`, Project V2 `In Progress`.
- [x] worktree `.worktrees/rosstat-bi-cpi-weekly-pdf-fallback`, ветка `feat/rosstat-bi-cpi-weekly-pdf-fallback` от `main`.
- [ ] спека и этот план — первый коммит.

## 2. Разбор агрегата

- [ ] `rosstat/ipc_weeks_news.go`: `ipcWeeksNewsAggregate(text) (float64, error)` — строка `(оценка)` после `Индекс потребительских цен`, первое число через `ipcWeeksNewsValue`; ошибки: нет строки, дубль, нечисло.
- [ ] `ipcWeeksNewsReleasesFor(page, context)` и `ipcWeeksCPIDocumentsFor(ctx, context)`; старые функции — обёртки с `ipcWeeksNewsContext`.
- [ ] `rosstat/ipc_weeks_news_test.go`: `TestIpcWeeksNewsAggregateFixtures` — 2026-11-02 = 99.96, 2026-10-26 = 100.08; ошибки.

## 3. Импортёр

- [ ] `rosstat/bi_cpi_weekly.go`: `biCpiWeeklyFetchPoints` (RPC → CSV → разбор) вынесен из `Import`.
- [ ] `biCpiWeeklyKnownDatesQuery`, `biCpiWeeklyPDFTextRows`, `biCpiWeeklyPDFRows`, `biCpiWeeklyPDFAppendRow`, `biCpiWeeklyFallback`.
- [ ] `Import`: fetch; при ошибке — warn + `util.Fallback`; политика ошибок как у `rosstat_ipc_weeks`; успешный BI — прежний путь без изменений.
- [ ] `errors` в импортах; реестр `rosstat/importers.go` и DDL не меняются.

## 4. Тесты

- [ ] `rosstat/bi_cpi_weekly_test.go`: `TestBICpiWeeklyPDFTextRows` (оба выпуска), `TestBICpiWeeklyPDFAppendRow`, `TestBICpiWeeklyKnownDatesQuery`, `TestBICpiWeeklyPDFRowsRejectsNonPDF`.
- [ ] `go test -race ./rosstat/... ./util/...`.

## 5. Документы

- [ ] `docs/ROADMAP_ROSSTAT.md`: пункт `1.13`, «Правило блокера», умбрелла `1.1`–`1.13`, §3.
- [ ] `docs/ROADMAP_INDEX.md`: `rosstat` → `1.1`–`1.13`.
- [ ] `ARCHITECTURE.md`: шапка, §2, §6.1 (BI), §6.3 (`v_rosstat_macro`), §7 (четверг).
- [ ] `README.md`: строка `rosstat`.
- [ ] `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`, `docs/publications/README.md` §3.1.
- [ ] `docs/research/2026-11-06-rosstat-source-availability.md`: аддендум с приёмкой.
- [ ] `dagu/rosstat.yaml`: комментарий шага BI; `dagu validate`.

## 6. Проверки

- [ ] `make all`.
- [ ] `make docs-check`.
- [ ] `make import STAT=rosstat_bi_cpi_weekly` — фолбэк, `Imported 1 rows` (2026-11-02 = 99.96), повтор — `Imported 0 rows`.
- [ ] `v_rosstat_macro` на 2026-11-02 — 112 ряда, агрегат 99.96.
- [ ] `make import STAT=weekly_cpi_sa` — 211 строк, `date=2026-11-30`, `as_of=2026-11-02`, `partial=1`.
- [ ] `make mcp-check` — без новых регрессий.
- [ ] PR с `Closes #546` вне backticks, протокол — комментарием.
