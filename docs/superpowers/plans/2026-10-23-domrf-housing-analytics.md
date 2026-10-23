# План: пакет `domrf/` — рынок ипотеки, недельные ставки, индекс цен

**Дата:** 2026-10-23
**Пункт:** `real-estate/1.1`
**Спека:** [2026-10-23-domrf-housing-analytics-design.md](../specs/2026-10-23-domrf-housing-analytics-design.md)
**Research:** [2026-10-23-domrf-analytics-sources.md](../research/2026-10-23-domrf-analytics-sources.md)

Ветка: `feat/real-estate-domrf`, worktree `.worktrees/real-estate-domrf`.

## Task 1. Документы (этот коммит)

- [x] research-документ `docs/research/2026-10-23-domrf-analytics-sources.md`
- [x] спека `docs/superpowers/specs/2026-10-23-domrf-housing-analytics-design.md`
- [x] план (этот файл)
- [x] `docs/ROADMAP_REAL_ESTATE.md`, строка в `docs/ROADMAP_INDEX.md`

## Task 2. Транспорт (`domrf/domrf.go`)

- [x] `domrfSite`, `domrfDataPrefix` — литералы домена.
- [x] `domrfGetPage()` — 8 попыток `util.GetPage`, детект challenge (`< 4096` && `servicepipe`), пауза 1 с, ошибка с числом попыток.
- [x] `domrfChallengePage()` — чистая функция детекта (тестируется офлайн).
- [x] `domrfPageBlocks()` — разбор `main#app` (goquery/html), карта `имя атрибута → json.RawMessage`; отсутствие `main#app` — ошибка.
- [x] `domrfPostJSON()` — `util.PostFileBytes` + `Content-Type: application/json` + разбор challenge.

## Task 3. Импортёр A (`domrf/market.go`, `domrf/market_parse.go`)

- [x] `domrfMortgageMarket` — `util.ClickHouseImport{TableName, CreateTable, BeforeImport, ImportFunc, PublicationCountPoints, AfterImport}`.
- [x] `BeforeImport`: `domrfGetPage` → блок `statistical_series` → `props.file.src`, `props.file.date` (лог) → `DataUrl`.
- [x] `domrfMarketColumns(xlsx, sheet)` — заполнение merged-диапазонов, путь колонки (dedup подряд), карта путь → индекс, 24 ожидаемых пути, проверка «ровно одна колонка».
- [x] `domrfMarketPoints` — 24 ряда, пропуск `X`/`x`/`-`/пусто, ошибка на нечисле, дата → первое число месяца.
- [x] `domrfMarketSeriesMeta` — описания 24 рядов.
- [x] DDL `domrf_mortgage_market`.

## Task 4. Импортёр B (`domrf/weekly.go`, `domrf/weekly_parse.go`)

- [x] `domrfMortgageWeekly` — свой `Import()` (нет XLSX): страница → блок `mortgage-dinamika-stavok` → батч.
- [x] `domrfWeeklyPoints`: даты по убыванию, 5 продуктов, короткий ряд обрезается, длинный — ошибка, пустое/нечисло — ошибка.
- [x] DDL `domrf_mortgage_weekly`, каталог 5 рядов.

## Task 5. Импортёр C (`domrf/price_index.go`, `domrf/price_index_parse.go`)

- [x] `domrfPriceIndex` — свой `Import()`: дешёвый POST (РФ) → `max(month_dt)` vs `max(date)` таблицы → no-op или полный POST (75×4).
- [x] Fallback: 4 POST по комнатам при 4xx/413.
- [x] `domrfPriceIndexPoints`: маппинг комнат, парсинг строковых чисел, дубли — ошибка, `deals` ≥ 0.
- [x] DDL `domrf_price_index`.

## Task 6. Производные (`series_meta.go`, `views.go`, `importers.go`)

- [x] `domrfSeriesMeta` — 24 + 5 рядов, `afterDomrfMacro` (каталог + две витрины).
- [x] `v_domrf_mortgage`, `v_domrf_price_index` с комментариями.
- [x] Реестр `importers.go` с литералами таблиц.

## Task 7. Тесты и фикстуры

- [x] `testdata/domrf_mortgage_page.html` — реальный снапшот.
- [x] `testdata/stat_series.xlsx` — реальный файл.
- [x] `testdata/price_index_mainchart_rf.json` — реальный ответ.
- [x] `market_parse_test.go`, `weekly_parse_test.go`, `price_index_parse_test.go`, `domrf_test.go` (challenge), `importers_test.go`.

## Task 8. Проводка

- [x] `main.go` — blank import `domrf`.
- [x] `dagu/domrf.yaml` (chain, `47 9 * * 1`, три шага, retry 3×300).
- [x] `sql/mcp_kimi_reader.sql` — гранты на две витрины.
- [x] `scripts/mcp_check.py` — проверки витрин.
- [x] `ARCHITECTURE.md`, `README.md`, `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`, `docs/site-pages.json`.

## Task 9. Живая приёмка (2026-10-23)

- [x] `make all`, `go test -race -count=1 ./domrf/`.
- [x] `make import STAT=domrf_mortgage_market` — `Imported 4612 rows`, 2006-01 … 2026-09; повтор — no-op по версии `2255ad1c…`.
- [x] `make import STAT=domrf_mortgage_weekly` — `Imported 1154 rows`, 2021-09-03 … 2026-10-16.
- [x] `make import STAT=domrf_price_index` — `Imported 20700 rows`; повтор — no-op (месяц 2026-09, 69 точек РФ).
- [x] `make mcp-check`, `dagu validate dagu/domrf.yaml`, `make docs-check`.
- [x] Протокол — комментарием в PR.

## Task 10. PR

- [x] PR [#374](https://github.com/kmlebedev/clickhouse-import-rosstat/pull/374): `Closes #373` вне backticks, протокол приёмки — комментарием, статус пункта в роадмапе — тем же PR.
