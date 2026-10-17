### Task 1: Разбор «Табл 1» + реестр показателей + фикстуры

**Files:**
- Create: `cbr/mt_forecast_meta.go` (реестр 16 показателей + `cbrMtForecastSeriesMeta`)
- Create: `cbr/mt_forecast_parse.go` (нормализация, разбор книги и значений)
- Create: `cbr/mt_forecast_parse_test.go` (тесты разбора и каталога)
- Create: `cbr/testdata/mt_forecast_05082026.xlsx`, `cbr/testdata/mt_forecast_07052026.xlsx` (урезанные zip-хирургией)
- Create: `cbr/testdata/mt_forecast_page_05082026.html` (копия живой страницы выпуска; потребляет тест Task 2)

**Interfaces:**
- Produces (нужны Task 2): `type mtForecastPoint struct { Indicator string; Year uint16; Lo, Hi float64 }`; `func mtForecastParseTabl1(xlsx *excelize.File) ([]mtForecastPoint, error)`; `var cbrMtForecastSeriesMeta []util.SeriesMeta` (16 записей, source `cbr_mt_forecast`, frequency `A`); `func mtForecastIndicatorByLabel(label string) (mtForecastIndicator, bool)`.
- Шаблоны-эталоны: `cbr/dip_parse.go`/`dip_parse_test.go` (разбор книги + appendRecorder), `cbr/mo_br_meta.go` + `mo_br_series_meta_test.go` (реестр и каталог).

- [ ] **Step 1: Фикстуры.** Из дерева: `python3 scripts/xlsx_trim.py trim build/recon-2026-10-17/dl_commet_graph_05082026.xlsx cbr/testdata/mt_forecast_05082026.xlsx --keep 'Табл 1'` (и то же для 07052026); затем `python3 scripts/xlsx_trim.py verify build/recon-2026-10-17/dl_commet_graph_05082026.xlsx cbr/testdata/mt_forecast_05082026.xlsx --keep 'Табл 1'` → PASS (и для второй пары). Убедиться, что excelize открывает фикстуры и «Табл 1» читается. Плюс страница выпуска: `cp build/recon-2026-10-17/page_05082026.html cbr/testdata/mt_forecast_page_05082026.html` и `grep -c commet_graph_05082026.xlsx cbr/testdata/mt_forecast_page_05082026.html` ≥ 1 (страницу коммитим как есть — живая разметка).
- [ ] **Step 2: Тесты разбора (сначала падающие).** В `cbr/mt_forecast_parse_test.go`:
  - таблица спотов из спеки §3.9 (64 точки / 4 колонки у 05082026; `key_rate_avg` 2026 = (14.5, 14.6) — приклеенная сноска; `cpi_dec` 2028 = (4.0, 4.0); `gross_capital_formation` 2026 = (-3.5, -1.5); `fixed_capital_investment` 2026 = (-1.5, 0.5); `m2` 2026 = (7, 12); `bank_claims_mortgage` 2028 = (10, 15); и 48 точек у 07052026, `key_rate_avg` 2026 = (14.0, 14.5), `cpi_dec` 2026 = (4.5, 5.5); `year` и границы);
  - двусторонний контракт каталога (§11.4): каждая разобранная подпись есть в карте, каждый из 16 кодов — среди разобранных на объединении фикстур; `cbrMtForecastSeriesMeta` описывает ровно 16 кодов;
  - fail-fast: книга без листа «Табл 1»; лист без строки-показателя; шапка без годов; пустая прогнозная ячейка; `'5,0-4,0'` (lo > hi) → ошибка с адресом; неразбираемая ячейка (`'около 5'`);
  - warn-ветки: неизвестная подпись — пропуск (не ошибка); отсутствие core-строки в книге — ошибка; отсутствие не-core — предупреждение (последние два — на синтетически удалённой строке из настоящей книги через копию листа; допустимо `xlsx.SetCellValue` в тесте — разметку не менять);
  - харнесс `appendRecorder`: `mtForecastAppendPoints(release)` кладёт 64 строки для 05082026 с ожидаемым первым/последним (`sorted by row order` — зафиксировать фактический порядок).
- [ ] **Step 3: Прогнать — падает** (`go test ./cbr/ -run 'MtForecast' -v` — компиляция не проходит: функций нет).
- [ ] **Step 4: `cbr/mt_forecast_meta.go`.** Реестр `mtForecastIndicators` (16 записей: `Code`, `Label` — нормализованная подпись из спеки §3.2, `Title`, `Unit`, `Core`), `mtForecastIndicatorByLabel`, `var cbrMtForecastSeriesMeta = mtForecastMeta()` — описания из спеки §3.7 (origin: «ЦБ, „Комментарий к среднесрочному прогнозу“/„Среднесрочный прогноз“, таблица „Табл 1“, строка „…“»).
- [ ] **Step 5: `cbr/mt_forecast_parse.go`.** Слоистая нормализация и разбор строго по спеке §3.3: базовая нормализация → подпись (тире→`-`, срез хвостовых цифр) → значение (юникод-надстрочные, точка/диапазон регулярками, сноска-приклейка только у ровно 2-знаковых дробей, `lo ≤ hi`); шапка — ближайшая строка выше первой строки-показателя с ≥2 годами; факт-колонки (`факт`/`оценка`) пропускаются; конец таблицы — `^\d+ `; fail-fast и warn по §3.3.
- [ ] **Step 6: Прогнать — зелёный** (`go test -race -count=1 ./cbr/ -run 'MtForecast'`), затем весь пакет `go test -race -count=1 ./cbr/`.
- [ ] **Step 7: Commit** — `cbr: mt_forecast parse — Табл 1, meta registry, fixtures`.

