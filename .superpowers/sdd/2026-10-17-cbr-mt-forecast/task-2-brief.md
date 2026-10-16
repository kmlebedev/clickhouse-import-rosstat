### Task 2: Импортёр (индекс → гейт → цикл выпусков), витрина, регистрация, расписание

**Files:**
- Create: `cbr/mt_forecast.go` (DDL, константы, `Import()`, разбор индекса и страницы)
- Create: `cbr/mt_forecast_test.go` (индекс/страница/гейт/DDL-pin)
- Modify: `cbr/views.go` (витрина `cbrMtForecastView` + `afterCbrMtForecast`)
- Modify: `cbr/importers.go` (запись реестра), `cbr/importers_test.go` (ожидаемые наборы)
- Modify: `dagu/cbr.yaml` (шаг в хвост)

**Interfaces:**
- Consumes (из Task 1): `mtForecastParseTabl1`, `cbrMtForecastSeriesMeta`, `mtForecastPoint`.
- Produces: `cbr_mt_forecast` (таблица и `Name()`), `v_cbr_mt_forecast`, `afterCbrMtForecast([]util.SeriesMeta)`.

- [ ] **Step 1: Тесты (сначала падающие).** В `cbr/mt_forecast_test.go`:
  - `mtForecastIndexReleases` на `cbr/testdata/mp_summary_index.html`: 11 карточек «Комментарий к среднесрочному прогнозу Банка России» → 2 страничные (`release_date` 2026-05-07 с `meeting_date` 2026-04-24; 2026-08-05 с 2026-07-24) + 9 PDF-пропусков; расхождение даты карточки и слага → ошибка (проверка на копии фикстуры в памяти — дата карточки подменяется в байтах страницы, файл на диске не меняется); индекс без страничных карточек → ошибка;
  - `mtForecastPageXlsxURL` на `cbr/testdata/mt_forecast_page_05082026.html` (`util.FindLink` по `mtForecastXlsxSel`) → `/Content/Document/File/194307/commet_graph_05082026.xlsx` (отсутствие ссылки → ошибка);
  - `mtForecastNewReleases`: пустое множество известных → все (старые первыми), полное → пусто;
  - DDL-pin по прецеденту `TestMPSummaryDdlPins`: литерал `cbr_mt_forecast` внутри `mtForecastDdl` и колонки/ключ совпадают со спекой §3.1.
- [ ] **Step 2: Прогнать — падает.**
- [ ] **Step 3: `cbr/mt_forecast.go`.** Константы (`mtForecastIndexUrl`, `mtForecastCardTitle`, `mtForecastHrefMarker = "comment_"`, `mtForecastXlsxSel = a[href$=".xlsx"]`, `mtForecastDdl`, `mtForecastCountPoints = SELECT count() FROM cbr_mt_forecast FINAL`, `mtForecastKnownReleasesQuery`, `mtForecastReleaseCountQuery` с плейсхолдером); `mtForecastStat`/`Name()`; `Import()` — порядок по спеке §3.5 (DDL → `afterCbrMtForecast` → индекс → гейт → цикл: страница → `local := s.ClickHouseImport` + `DataUrl`+`ImportFunc` → поста-чек строк выпуска → `errors.Join`; no-op-сообщение со счётчиками); `mtForecastAppendPoints`. Комментарии — в стиле дипа (почему гейт, почему производные раньше, что не отслеживает подмена файла).
- [ ] **Step 4: `cbr/views.go`.** `cbrMtForecastView` (Select и комментарии таблицы/колонок — спека §3.7) + `afterCbrMtForecast(meta)` (каталог + `CreateView`; прецедент `afterCbrDip`).
- [ ] **Step 5: Регистрация + расписание.** Запись в `cbr/importers.go`: `{cbrMtForecastImporter, "cbr_mt_forecast", "SELECT count() FROM cbr_mt_forecast FINAL"}` (литералы); импортер-var с `TableName`, `CreateTable`, `PublicationCountPoints` (без `AfterImport` — производные публикует `Import()`); шаг `cbr_mt_forecast` в хвост `dagu/cbr.yaml` (retry_policy как у соседей); обновить ожидаемые наборы в `cbr/importers_test.go` (карта префлайтов, ключевые шаги, счётные комментарии — «20-я запись», «20-й шаг»).
- [ ] **Step 6: Прогнать — зелёный.** `go test -race -count=1 ./cbr/` (включая энфорсер реестра), `dagu validate dagu/cbr.yaml` (если dagu доступен; иначе зафиксировать в отчёте), `gofmt`, `go vet ./cbr/`.
- [ ] **Step 7: Commit** — `cbr: add mt_forecast importer (index, gate, per-release cycle)`.

