### Task 2: Разбор `parseLabourForce` и фикстуры

**Files:**
- Create: `rosstat/labour_force.go` (на этой задаче — только константы и разбор)
- Create: `rosstat/labour_force_test.go`
- Create: `rosstat/testdata/labour_force.xlsx`
- Create: `rosstat/testdata/labour_force_page.html`
- Modify: `rosstat/series_meta_test.go:18-53` (харнесс `appendRecorder`)

**Interfaces:**
- Consumes: ничего из предыдущих задач.
- Produces (используется в Task 3 и Task 4):
  - `rosstatLabourForceTable = "rosstat_labour_force_mes"` — const;
  - `labourForceLinkSelector = `a[href$="trud-1_15-s.xlsx"]`` — const (селектор ссылки);
  - `labourForcePage = "https://rosstat.gov.ru/labour_force"` — const;
  - `type labourForcePoint struct { series string; date time.Time; value float64 }`;
  - `func parseLabourForce(xlsx *excelize.File) ([]labourForcePoint, error)`;
  - `func labourForceLabel(raw string) string` — снимает хвостовые сноски и сжимает пробелы;
  - харнесс `appendedRow.value` — **`float64`** (было `float32`) плюс новое поле `valueType string` — имя Go-типа записанного значения.

- [ ] **Step 1: Собрать фикстуру книги (реальную, урезанную)**

```bash
curl -sS -o /tmp/trud1.xlsx "https://rosstat.gov.ru/storage/mediabank/trud-1_15-s.xlsx"
python3 -c "import openpyxl; wb=openpyxl.load_workbook('/tmp/trud1.xlsx'); print(wb.sheetnames)"
```

Урезать «хирургией» zip, сохранив листы `Содержание` и `5` целиком (разметка и значения не меняются): из `xl/workbook.xml` убрать элементы `<sheet>` лишних листов, из `xl/_rels/workbook.xml.rels` — соответствующие `<Relationship>`, из `[Content_Types].xml` — `<Override PartName="/xl/worksheets/sheetN.xml">`, файлы `xl/worksheets/sheetN.xml` удалённых листов в архив не включать. Проверить, что книга открывается:

```bash
python3 -c "import openpyxl; wb=openpyxl.load_workbook('rosstat/testdata/labour_force.xlsx', data_only=True); print(wb.sheetnames); ws=wb['5']; print(ws.max_row, ws.max_column)"
```

Ожидаемо: листы `['Содержание', '5']`, `135 7`. Если урезанная книга не открывается — положить в `testdata/` полную копию файла как есть (343 КБ) и назвать это в отчёте задачи.

- [ ] **Step 2: Собрать HTML-фикстуру страницы**

Скачать `https://rosstat.gov.ru/labour_force?print=1` (95 КБ) и сохранить фрагмент с карточкой «Занятость и безработица» — так, чтобы в фикстуре были **и** ссылка на `trud-1_15-s.xlsx`, **и** соседняя `trud_1_15-72.xlsx` (иначе селектор не проверяется). Проверка:

```bash
grep -c "trud-1_15-s.xlsx\|trud_1_15-72.xlsx" rosstat/testdata/labour_force_page.html
```

Ожидаемо: обе ссылки присутствуют.

- [ ] **Step 3: Написать падающие тесты разбора**

В `rosstat/labour_force_test.go` — тесты (имена и утверждения; код фикстуры открывается так же, как в `rosstat/salaries_mes_test.go:31`):

- `TestLabourForceReadsFixture` — `parseLabourForce` по фикстуре даёт **690 точек**, шесть различных рядов, минимальная дата `2017-01-31`, максимальная `2026-07-31`.
- `TestLabourForceSpotValues` — точные значения из источника (см. §2 спеки): `Рабочая сила` `2017-01-31` = `76489.193`; `Уровень безработицы` `2017-01-31` = `5.6`; `Рабочая сила` `2026-07-31` = `76389.482`; `Уровень занятости` `2026-07-31` = `61.5`; `Уровень безработицы` `2026-07-31` = `2.3`.
- `TestLabourForceFootnoteLabels` — `labourForceLabel("январь1)")` == `"январь"`, `labourForceLabel("октябрь1)2)")` == `"октябрь"`, `labourForceLabel("Уровень занятости, \nв %")` == `"Уровень занятости, в %"` (Review Focus 3).
- `TestLabourForceMissingSheetFails` — книга без листа «5» → ошибка, в тексте `rosstat_labour_force_mes` и `5` (отсутствие данных не означает «0 точек»).
- `TestLabourForceMissingHeaderFails` — лист «5» без строки шапки → ошибка с именем листа.
- `TestLabourForceNonNumericCellFails` — книга-копия фикстуры, где в строке месяца значение заменено текстом → ошибка разбора, значение не пропущено (Review Focus 2).
- `TestLabourForceEmptyCellIsNotPoint` — книга, где у последнего месяца нового года значения пусты → точек меньше ровно на число пустых ячеек, нулей в ряду нет (Review Focus 1).
- `TestLabourForceIgnoresTrailingFootnoteRows` — после последней строки месяца идут строки текста сносок → разбор возвращает точки и не падает (Review Focus 4).
- `TestLabourForceFindLink` — `util.FindLink(page, labourForceLinkSelector)` по HTML-фикстуре даёт `/storage/mediabank/trud-1_15-s.xlsx` (а не `trud_1_15-72.xlsx`); на странице без ссылки — ошибка (Review Focus 5).

Книги для fail-fast-веток («нет листа», «нет шапки», «нечисловая ячейка», «пустая ячейка» в конце) собираются **из копии реальной фикстуры**: `testdata/labour_force.xlsx` копируется в `t.TempDir()`, один элемент меняется через `excelize` (`DeleteSheet`, `SetCellValue`), копия сохраняется и открывается разбором. Синтетическая книга через `newTestWorkbook` (`rosstat/series_meta_test.go:73`) здесь не годится: она проверяла бы допущения парсера о разметке (сноски в подписях, перевод строки в шапке, «в том числе»), а не разметку источника (стандарт §11.1).

- [ ] **Step 4: Прогнать тесты — падают**

Run: `go test -race ./rosstat/ -run 'LabourForce' -v`
Expected: FAIL — `undefined: parseLabourForce`, `undefined: labourForceLabel`, `undefined: labourForceLinkSelector`.

- [ ] **Step 5: Расширить харнесс `appendRecorder` до float64**

В `rosstat/series_meta_test.go` поле `appendedRow.value` — `float64`, добавить рядом поле `valueType string` (имя Go-типа значения, `fmt.Sprintf("%T", v[2])`); в `Append` заменить жёсткое приведение к `float32` (строки 47-50) на разбор обоих типов:

```go
var value float64
switch v := v[2].(type) {
case float32:
	value = float64(v)
case float64:
	value = v
default:
	return fmt.Errorf("appendRecorder: значение %T, ожидалось float32 или float64", v[2])
}
```

`valueType` нужен потому, что сам `value` — уже `float64` у обоих типов, и без него тест не отличит `float32`-запись (`rosstat_salaries_mes`, legacy) от `float64`-записи новой таблицы: схема колонки иначе не проверяется ничем.

Run: `go test -race ./rosstat/`
Expected: PASS — все существующие тесты зелёные (расширение типа обратно совместимо).

- [ ] **Step 6: Реализовать разбор в `rosstat/labour_force.go`**

Константы и разбор:

```go
const (
	rosstatLabourForceTable = "rosstat_labour_force_mes"
	labourForceSheet        = "5"
	labourForcePage         = "https://rosstat.gov.ru/labour_force"
	labourForceLinkSelector = `a[href$="trud-1_15-s.xlsx"]`
)

type labourForcePoint struct {
	series string
	date   time.Time
	value  float64
}
```

Таблица рядов — пин имён и якорей шапки (шесть записей из спеки §3.3): «Рабочая сила», «Занятые» (якорь `занятые`), «Безработные» (якорь `безработные`), «Уровень участия в составе рабочей силы», «Уровень занятости», «Уровень безработицы». Якоря строки «в том числе» (`занятые`, `безработные`) лежат в **строке ниже** верхней строки шапки — искать якорь по обеим строкам найденной шапки.

Алгоритм `parseLabourForce`:
1. лист `labourForceSheet` — есть, иначе ошибка с именем таблицы и листа;
2. `xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})`;
3. строка шапки — та, где нормализованная ячейка равна `Рабочая сила`; не найдена — ошибка;
4. для каждого из шести рядов колонка ищется по нормализованной подписи в строке шапки и строке под ней, ровно одно совпадение, иначе ошибка с именем ряда;
5. ниже шапки: строка из одной ячейки-года (`2017`…`2026`) задаёт текущий год; строка, чья подпись месяца после `labourForceLabel` распознана через `util.MonthsToNum`, даёт точки по колонкам пункта 4; строка, где подпись не год и не месяц (текст сносок) — конец разбора; месяц обязан возрастать внутри года, иначе ошибка;
6. пустая ячейка — пропуск (`continue`); нечисловая — ошибка `значение ячейки %q не число` с именем таблицы, листа и строки; `NaN`/`Inf` — ошибка;
7. дата точки — последний день отчётного месяца, UTC: `time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)`;
8. пустой результат — ошибка с именем таблицы и листа.

- [ ] **Step 7: Прогнать тесты — проходят**

Run: `go test -race -count=1 ./rosstat/ -run 'LabourForce' -v`
Expected: PASS (все девять тестов Step 3).

- [ ] **Step 8: Коммит**

```bash
git add rosstat/labour_force.go rosstat/labour_force_test.go rosstat/testdata/labour_force.xlsx rosstat/testdata/labour_force_page.html rosstat/series_meta_test.go
git commit -m "add rosstat labour force parser with fixtures"
```

---

