### Task 3: импортёр `bold_gold_price` и каталог цены

**Files:**
- Modify: `bold/bold.go` (второй импортёр), `gold/series_meta.go` (запись каталога + комментарий витрины), `gold/series_meta_test.go` (распространить проверки на новую запись)
- Create: `bold/price_test.go`

**Interfaces:**
- Consumes: Task 1 (`parseHistory`, `parseLatest`, `boldMetricPrice`, пути цены), `util.GetJSON`, `util.UpsertSeriesCatalog`.
- Produces: `func (s *boldGoldPrice) Name() string` → `"bold_gold_price"`, поля `apiBase`/`now` как в Task 2; `func boldPriceStoredMaxDate(ctx, conn) (time.Time, error)` — та же `maxOrNull(date)` по `gold_prices` c `venue = 'bold'`; в пакете `gold` — **`func BoldPriceSeriesMeta() util.SeriesMeta`** (экспортируется для пакета `bold`), и обновлённый комментарий `v_gold_prices` («Три площадки: …»).

- [ ] **Step 1: Написать падающие тесты**

```go
func TestBoldGoldPriceName(t *testing.T)                  // "bold_gold_price"
func TestBoldPriceImportFullWritesVenue(t *testing.T)     // сервер отдаёт price.json → батч gold_prices с venue="bold", 2 точки (08.10, 09.10), значение 4216.3 за 09.10
func TestBoldPriceImportIncrementalWritesOneDay(t *testing.T) // conn: максимум 08.10 → пишется только 09.10 (4216.3)
func TestBoldPriceImportNoopWhenUpToDate(t *testing.T)    // максимум == 2026-10-09 → 0 строк
func TestBoldPriceCatalogEntry(t *testing.T)              // gold.BoldPriceSeriesMeta(): source "gold", series "bold", frequency "D", unit содержит «USD», description — «метод … не раскрыт» и «comex_front»
```
В `gold/series_meta_test.go` — расширить существующую проверку полноты полей на `BoldPriceSeriesMeta()`.

- [ ] **Step 2: Реализовать**

- `boldGoldPrice.Import`: сохранённый максимум по площадке `bold`; инкремент (`boldPathPriceLatest` → `parseLatest(body, boldMetricPrice)`) или полный путь (`boldPathPrice` → `parseHistory`, пишутся точки новее сохранённого максимума — на пустой таблице весь ряд 2014-10-01 … 2026-10-09 = 3138 точек); перед записью — `conn.Exec(ctx, goldPricesDdl)`, где `goldPricesDdl` — **копия канонического DDL** `gold_prices` из `gold/gold.go` (`CREATE TABLE IF NOT EXISTS … ORDER BY (venue, date)`; копии обязаны совпадать, как у копий `macro_series`), с комментарием-ссылкой: так импортёр работает и на чистой базе, где `gold/` ещё не прогонялся;
- `boldNeedsFullFetch` переиспользуется как есть (тот же порог 4 дня);
- каталог: `util.UpsertSeriesCatalog(ctx, conn, []util.SeriesMeta{gold.BoldPriceSeriesMeta()})` после `Send`;
- в `gold/series_meta.go`: новая функция рядом с `goldSeriesMeta` + расширенный текст комментария витрины (перечислить три площадки и что `bold` — фьючерсная цена источника, не спот и не GC=F).

- [ ] **Step 3: Прогнать тесты**

Run: `go test ./bold/... ./gold/... -v -count=1`
Expected: PASS; саботаж: убрать проверку «точки новее максимума» → `TestBoldPriceImportIncrementalWritesOneDay` падает (пишутся все 2 точки); написать `venue='comex_front'` → падает `TestBoldPriceImportFullWritesVenue`.

- [ ] **Step 4: Commit**

```bash
gofmt -l bold/ gold/
git add bold/bold.go bold/price_test.go gold/series_meta.go gold/series_meta_test.go
git commit -m "add bold_gold_price importer and price catalog entry"
```

---

