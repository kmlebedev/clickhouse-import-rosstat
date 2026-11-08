# План: PDF-фолбэк агрегата недельного ИПЦ

**Спека:** [2026-11-08-bi-cpi-weekly-pdf-fallback-design.md](../specs/2026-11-08-bi-cpi-weekly-pdf-fallback-design.md). **Пункт:** `rosstat/1.13`, issue [#546](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/546).

## 1. Трекер и ветка

- [x] issue #546 создан, `role:go`/`roadmap:rosstat`, Project V2 `In Progress` → `In Review`.
- [x] worktree `.worktrees/rosstat-bi-cpi-weekly-pdf-fallback`, ветка `feat/rosstat-bi-cpi-weekly-pdf-fallback` от `main`.
- [x] спека и этот план — первый коммит (`0675438`).

## 2. Разбор агрегата

- [x] `rosstat/ipc_weeks_news.go`: `ipcWeeksNewsAggregate`/`ipcWeeksNewsAggregateFor(text, context)` — строка `(оценка)` после `Индекс потребительских цен`, первое число через `ipcWeeksNewsValue`; ошибки: нет строки, дубль, нечисло.
- [x] `ipcWeeksNewsReleasesFor(page, context)` и `ipcWeeksCPIDocumentsFor(ctx, context)`; старые функции — обёртки с `ipcWeeksNewsContext`.
- [x] `rosstat/ipc_weeks_news_test.go`: `TestIpcWeeksNewsAggregateFixtures` — 2026-11-02 = 99.96, 2026-10-26 = 100.08; ошибки, дубль, однострочный вариант, контекст.

## 3. Импортёр

- [x] `rosstat/bi_cpi_weekly.go`: `biCpiWeeklyFetchPoints` (RPC → CSV → разбор) вынесен из `Import`.
- [x] `biCpiWeeklyKnownDatesQuery`, `biCpiWeeklyPDFTextRows`, `biCpiWeeklyPDFRows`, `biCpiWeeklyPDFAppendRow`, `biCpiWeeklyFallback`, `biCpiWeeklyFallbackOutcome`.
- [x] `Import`: fetch; при ошибке — `ctx.Err()` и warn + `util.Fallback`; политика ошибок как у `rosstat_ipc_weeks`; успешный BI — прежний путь без изменений.
- [x] `errors` в импортах; реестр `rosstat/importers.go` и DDL не меняются.

## 4. Тесты

- [x] `rosstat/bi_cpi_weekly_test.go`: `TestBICpiWeeklyPDFTextRows` (оба выпуска), `TestBICpiWeeklyPDFAppendRow`, `TestBICpiWeeklyKnownDatesQuery` (ряд + таблица + FINAL), `TestBICpiWeeklyPDFRowsRejectsNonPDF` (ошибка про сигнатуру PDF), `TestBICpiWeeklyFallbackOutcome`.
- [x] `go test -race ./rosstat/... ./util/...`; `make all`.

## 5. Документы

- [x] `docs/ROADMAP_ROSSTAT.md`: пункт `1.13`, «Правило блокера», умбрелла `1.1`–`1.13`, §3.
- [x] `docs/ROADMAP_INDEX.md`: `rosstat` → `1.1`–`1.13`.
- [x] `ARCHITECTURE.md`: шапка, §2, §6.1 (BI), §6.3 (`v_rosstat_macro`, weekly SA), §7 (четверг).
- [x] `README.md`: строки `rosstat` и `weeklysa`.
- [x] `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`, `docs/publications/README.md` §3.1.
- [x] `rosstat/series_meta.go`: `Origin`/`Description` агрегата — PDF-фолбэк, 401 точка, ревизия BI.
- [x] `weeklysa/series_meta.go`, `weeklysa/views.go`, `grafana/key_rate_publications.go`: датированные снимки 2026-11-08 вместо устаревших 6,92/8,15; пометка о ревизии X-13.
- [x] `docs/research/2026-11-06-rosstat-source-availability.md`: аддендум с приёмкой.
- [x] `dagu/rosstat.yaml`: комментарий шага BI; `dagu validate`.

## 6. Проверки

- [x] `scripts/mcp_check.py`: вместо пина 3к26 = 6,92 и rolling = 8,15 — двусторонний коридор и инвариант `saar_q(4к26)` = среднему месячных `saar` (±0,02), `partial = 1`; отрицательные значения разрешены.
- [x] `make all`, `make docs-check`, `dagu validate dagu/rosstat.yaml`, `python3 -m py_compile scripts/mcp_check.py`.
- [x] `make import STAT=rosstat_bi_cpi_weekly` — фолбэк, `Imported 1 rows` (2026-11-02 = 99.96), повтор — `Imported 0 rows`.
- [x] `v_rosstat_macro` на 2026-11-02 — 112 рядов, агрегат 99.96.
- [x] `make import STAT=weekly_cpi_sa` — 211 строк, `date=2026-11-30`, `as_of=2026-11-02`, `partial=1`.
- [x] `make mcp-check` — weekly SA OK; 5 известных чужих провалов (bootstrap-разметка, fedstat ×2, `instrument_kind` ×2).
- [x] PR [#548](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/548) с `Closes #546` вне backticks, протокол — комментарием.

## 7. Живая приёмка 2026-11-08

- BI: `dial tcp 176.211.106.40:443: connect: operation timed out`, 3 попытки; фолбэк: `ipc11_02.pdf`, `Imported 1 rows`.
- `v_rosstat_macro`: 2026-11-02 — 112 ряда (111 товарных + агрегат), 2026-10-26 — 112; агрегат 99.96.
- `weekly_cpi_sa`: 211 строк (2009-05-31…2026-11-30), `as_of=2026-11-02`, `partial=1`; ноябрь `sa_4w=0,0393 %`, `saar=0,5118 %`; октябрь полный, `saar=12,88 %`.
- `v_weekly_cpi_sa` — 633 строки / 3 ряда; `v_weekly_cpi_quarterly`: 3к26 = 7,34, 4к26 = 6,70 (`months/full_months = 2/1`, `partial=1`); `v_weekly_cpi_rolling3m`: последняя 6,44, два NULL.
- Архив не проверен: `ARCHIVE_BACKEND` в dev не настроен; `source_publications` напрямую не читается `kimi_reader`, подтверждение версии PDF косвенное (отсутствие warn о HEAD + no-op по датам).
