### Task 6: `ipc_mes` — стабильные имена рядов и разбор по признаку

**Files:**
- Modify: `rosstat/ipc_mes.go`
- Create: `rosstat/ipc_mes_test.go`
- Modify: `rosstat/series_meta_test.go` (убрать `TestIpcMesSeriesMetaCoversParsedSheets`), `rosstat/series_meta.go` (4 записи каталога)

**Interfaces:**
- Consumes: `util.GetPage`/`util.FindLink` (T1), `testdata/ipc_mes.xlsx`, `testdata/price_page.html` (T3), `rosstatIPCMesTable` (T2), `appendRecorder` (T4).
- Produces: `ipcMesImport(xlsx *excelize.File, batch driver.Batch) error`, `ipcMesSeriesNames` (карта «номер листа → стабильное имя»), `ipcMesLinkSelector`.

- [ ] **Step 1: Выяснить разметку по фикстуре**

Листы `Содержание, 01, 02, 03, 04`. В листе `01`: A1 — заголовок с годами и сносками, строка 4 — годы (колонки B…, последняя заполненная — 2026), строка 5 — якорь `к концу предыдущего месяца`, строки 6…17 — `январь`…`декабрь`, строка 18 — второй якорь `к декабрю предыдущего года` и строка 19 — ещё один `декабрь` (годовой блок, его читать нельзя), дальше сноски. Формат данных — `0.00`.

- [ ] **Step 2: Написать падающий тест `rosstat/ipc_mes_test.go`**

```go
func TestIpcMesReadsFixture(t *testing.T)          // ровно 4 ряда по 429 точек (1716 всего)
func TestIpcMesStableSeriesNames(t *testing.T)     // ни одно имя ряда не содержит "1991-2026" и ")" (сноски и годы не попадают в имя)
func TestIpcMesStopsAtEndOfMonthBlock(t *testing.T) // блок месяцев обрывается на строке "к декабрю предыдущего года": у ряда "01" за 1992 год ровно 12 точек и нет точки со значением 2608.84 (годовой индекс того же декабря)
func TestIpcMesSpotValue(t *testing.T)             // лист "01", 1992-01-31 → 345.3 (год из строки 4 файла, а не из константы)
func TestIpcMesSeriesMetaCoversParsedRows(t *testing.T) // двусторонний контракт с ipcMesSeriesMeta
func TestIpcMesUnknownSheetWarns(t *testing.T)     // книга с листом "05" → предупреждение, разбор листов 01–04 продолжается
func TestIpcMesMissingSheetFails(t *testing.T)     // книга без листа "03" → ошибка с номером листа и именем таблицы
func TestIpcMesFindLink(t *testing.T)              // util.FindLink(testdata/price_page.html, ipcMesLinkSelector) → href с "ipc_mes_"
```

- [ ] **Step 3: Запустить и убедиться, что падает**

Run: `go test ./rosstat/ -run TestIpcMes -v`
Expected: FAIL — имена рядов содержат годы и сноски; тест `TestIpcMesStableSeriesNames` красный.

- [ ] **Step 4: Переписать разбор**

`ipcMesSeriesNames` — карта «номер листа → стабильное имя»: `1` → `Индексы потребительских цен на товары и услуги по Российской Федерации`, `2` → `… на продовольственные товары по Российской Федерации`, `3` → `… на непродовольственные товары по Российской Федерации`, `4` → `… на услуги по Российской Федерации` (точно эти строки — их требует двусторонний тест каталога). Обход: только листы, чьё имя парсится как число (`strconv.Atoi`); номер вне карты — `log.Warnf` и пропуск; отсутствие любого из листов 1–4 — ошибка. Внутри листа: якорная строка — `к концу предыдущего месяца`; годы читаются из строки **над** ней (колонки 1…, до первой пустой; пустая сразу — ошибка); строки месяцев — после якоря: `len(row) < 2` → конец, незнакомое имя месяца → конец блока, номер месяца из `util.MonthsToNum[strings.ToLower(row[0])]`, неперечислимые по возрастанию месяцы → ошибка. Ноль точек на листе — ошибка. Константа `ipcMesYearStart` удаляется.

- [ ] **Step 5: Обновить `Import()` и каталог**

`colly` → `util.GetPage` + `util.FindLink` (`ipcMesLinkSelector = "a[href*=\"ipc_mes_\"]"` — вместо нынешней позиционной CSS-цепочки, стандарт §6.3); в `ipcMesSeriesMeta` заменить 4 значения `Series` на новые стабильные имена и уточнить `Origin` (имя ряда задаётся кодом, а не заголовком листа).

- [ ] **Step 6: Прогнать тесты**

Run: `go test ./rosstat/ -run 'TestIpcMes' -v -count=1`
Expected: PASS.

- [ ] **Step 7: Коммит**

```bash
git add rosstat/ipc_mes.go rosstat/ipc_mes_test.go rosstat/series_meta.go rosstat/series_meta_test.go
git commit -m "ipc_mes: stable code-side series names, layout-driven parsing"
```

---

