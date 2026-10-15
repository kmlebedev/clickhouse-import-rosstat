### Task 4: `salaries_mes` — починка разбора (сырые значения, нули, стабильное имя)

**Files:**
- Modify: `rosstat/salaries_mes.go`
- Create: `rosstat/salaries_mes_test.go`
- Modify: `rosstat/series_meta_test.go` (убрать синтетический тест `TestSalariesMesSeriesMetaCoversParsedSheet`), `rosstat/series_meta.go` (описание ряда)

**Interfaces:**
- Consumes: `util.GetPage`/`util.FindLink` (T1), фикстуру `testdata/salaries_mes.xlsx` (T3), `rosstatSalariesMesTable` (T2).
- Produces: `salariesMesImport(xlsx *excelize.File, batch driver.Batch) error`, `salariesMesSeriesName` (константа), `salariesMesLinkSelector`, тестовый харнесс `appendRecorder` (переиспользуют T5–T7).

- [ ] **Step 1: Выяснить фактическую разметку по фикстуре**

Лист `Лист1`: A1 — заголовок, строка 5 — шапка (B «В среднем за год», C «В среднем за кварталы», D–F пусты, **G…R = Январь…Декабрь**), строки 8… — данные (A = год, у поздних лет с суффиксом `2026(2)`), строка «… данных не имеется» обрывает лист. Числовой формат данных — `#,##0`, поэтому значения надо читать сырыми.

- [ ] **Step 2: Написать падающий тест `rosstat/salaries_mes_test.go`**

Общий харнесс `appendRecorder` (подмена `driver.Batch`: собирает `[]appendedRow{series, date, value}`) вынести в `rosstat/series_meta_test.go` — его используют T5–T7. Тесты:

```go
func TestSalariesMesReadsFixture(t *testing.T)        // точек 418 (в файле 418 ненулевых числовых ячеек: 6 нулей января–июня 1991 и 5 пустых августа–декабря 2026 отброшены); ряд один — salariesMesSeriesName
func TestSalariesMesSkipsZeroCells(t *testing.T)      // среди точек нет value == 0 (январь–июнь 1991 в источнике — «данных не имеется»)
func TestSalariesMesSpotValues(t *testing.T)          // 1992-01 → дата 1992-02-01, значение 1; 2026-06 → дата 2026-07-01, значение 114743
func TestSalariesMesSeriesMetaCoversParsedRows(t *testing.T) // двусторонний контракт с salariesMesSeriesMeta
func TestSalariesMesMissingSheetFails(t *testing.T)   // пустая книга → ошибка, в тексте имя таблицы, в батче 0 строк
func TestSalariesMesFindLink(t *testing.T)            // util.FindLink(testdata/salaries_page.html, salariesMesLinkSelector) → href с "tab1-zpl"
```

- [ ] **Step 3: Запустить и убедиться, что падает**

Run: `go test ./rosstat/ -run TestSalariesMes -v`
Expected: FAIL — значения вроде `"103,612"`; `strconv.ParseFloat` возвращает ошибку разбора.

- [ ] **Step 4: Переписать разбор**

`xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})`; имя ряда — константа `salariesMesSeriesName` (значение — нынешний `Series` из каталога), а не `rows[0][0]`; нулевые ячейки пропускаются; лишний повторный `ParseFloat` в `salariesMesImport` убирается (парсер отдаёт уже разобранные значения). Порядок: найти якорную строку (`row[0] == salariesMesField`), считать год как `salariesMesYearStart + (i - fieldFound)`, обойти `row[6:]` по месяцам, `len(row) < 7` — конец листа. Пустой разбор — ошибка с именем таблицы (стандарт §6.4).

- [ ] **Step 5: Перевести `Import()` на `util`**

Коллектор `colly` заменить на `util.GetPage` + `util.FindLink` (селектор-константа `salariesMesLinkSelector = "a[href*=\"tab1-zpl\"]"`, домен приклеивается через `rosstatUrl`); пустая ссылка — ошибка (§4.4).

- [ ] **Step 6: Дописать оговорку в каталог**

В `salariesMesSeriesMeta[0].Description` — что нулевые ячейки источника (январь–июнь 1991) не записываются как точки.

- [ ] **Step 7: Прогнать тесты**

Run: `go test ./rosstat/ -run TestSalariesMes -v -count=1`
Expected: PASS.

- [ ] **Step 8: Коммит**

```bash
git add rosstat/salaries_mes.go rosstat/salaries_mes_test.go rosstat/series_meta.go rosstat/series_meta_test.go
git commit -m "fix salaries_mes: raw cell values, skip no-data zeros, stable series name"
```

---

