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

- [ ] `domrfSite`, `domrfDataPrefix` — литералы домена.
- [ ] `domrfGetPage()` — 8 попыток `util.GetPage`, детект challenge (`< 4096` && `servicepipe`), пауза 1 с, ошибка с числом попыток.
- [ ] `domrfChallengePage()` — чистая функция детекта (тестируется офлайн).
- [ ] `domrfPageBlocks()` — разбор `main#app` (goquery/html), карта `имя атрибута → json.RawMessage`; отсутствие `main#app` — ошибка.
- [ ] `domrfPostJSON()` — `util.PostFileBytes` + `Content-Type: application/json` + разбор challenge.

## Task 3. Импортёр A (`domrf/market.go`, `domrf/market_parse.go`)

- [ ] `domrfMortgageMarket` — `util.ClickHouseImport{TableName, CreateTable, BeforeImport, ImportFunc, PublicationCountPoints, AfterImport}`.
- [ ] `BeforeImport`: `domrfGetPage` → блок `statistical_series` → `props.file.src`, `props.file.date` (лог) → `DataUrl`.
- [ ] `domrfMarketColumns(xlsx, sheet)` — заполнение merged-диапазонов, путь колонки (dedup подряд), карта путь → индекс, 24 ожидаемых пути, проверка «ровно одна колонка».
- [ ] `domrfMarketPoints` — 24 ряда, пропуск `X`/`x`/`-`/пусто, ошибка на нечисле, дата → первое число месяца.
- [ ] `domrfMarketSeriesMeta` — описания 24 рядов.
- [ ] DDL `domrf_mortgage_market`.

## Task 4. Импортёр B (`domrf/weekly.go`, `domrf/weekly_parse.go`)

- [ ] `domrfMortgageWeekly` — свой `Import()` (нет XLSX): страница → блок `mortgage-dinamika-stavok` → батч.
- [ ] `domrfWeeklyPoints`: даты по убыванию, 5 продуктов, короткий ряд обрезается, длинный — ошибка, пустое/нечисло — ошибка.
- [ ] DDL `domrf_mortgage_weekly`, каталог 5 рядов.

## Task 5. Импортёр C (`domrf/price_index.go`, `domrf/price_index_parse.go`)

- [ ] `domrfPriceIndex` — свой `Import()`: дешёвый POST (РФ) → `max(month_dt)` vs `max(date)` таблицы → no-op или полный POST (75×4).
- [ ] Fallback: 4 POST по комнатам при 4xx/413.
- [ ] `domrfPriceIndexPoints`: маппинг комнат, парсинг строковых чисел, дубли — ошибка, `deals` ≥ 0.
- [ ] DDL `domrf_price_index`.

## Task 6. Производные (`series_meta.go`, `views.go`, `importers.go`)

- [ ] `domrfSeriesMeta` — 24 + 5 рядов, `afterDomrfMacro` (каталог + две витрины).
- [ ] `v_domrf_mortgage`, `v_domrf_price_index` с комментариями.
- [ ] Реестр `importers.go` с литералами таблиц.

## Task 7. Тесты и фикстуры

- [ ] `testdata/domrf_mortgage_page.html` — реальный снапшот.
- [ ] `testdata/stat_series.xlsx` — реальный файл.
- [ ] `testdata/price_index_mainchart_rf.json` — реальный ответ.
- [ ] `market_parse_test.go`, `weekly_parse_test.go`, `price_index_parse_test.go`, `domrf_test.go` (challenge), `importers_test.go`.

## Task 8. Проводка

- [ ] `main.go` — blank import `domrf`.
- [ ] `dagu/domrf.yaml` (chain, `47 9 * * 1`, три шага, retry 3×300).
- [ ] `sql/mcp_kimi_reader.sql` — гранты на две витрины.
- [ ] `scripts/mcp_check.py` — проверки витрин.
- [ ] `ARCHITECTURE.md`, `README.md`, `docs/IMPORTERS.md`, `docs/LIMITATIONS.md`, `docs/site-pages.json`.

## Task 9. Живая приёмка (2026-10-23)

- [ ] `make all`, `go test -race -count=1 ./domrf/`.
- [ ] `make import STAT=domrf_mortgage_market` — число точек и крайние даты.
- [ ] `make import STAT=domrf_mortgage_weekly` — 1 154.
- [ ] `make import STAT=domrf_price_index` — 20 700; повтор — no-op.
- [ ] `make mcp-check`, `dagu validate dagu/domrf.yaml`, `make docs-check`.
- [ ] Протокол — комментарием в PR.

## Task 10. PR

- [ ] `Closes #<issue>` вне backticks, описание по шаблону, статус пункта в роадмапе — тем же PR.
