### Task 7: `ipc_weeks` — перевод на шаблон и защита разбора

**Files:**
- Modify: `rosstat/ipc_weeks.go`
- Create: `rosstat/ipc_weeks_test.go`
- Modify: `rosstat/series_meta_test.go`

**Interfaces:**
- Consumes: `util.GetPage`/`util.FindLink` (T1), `testdata/ipc_weeks.xlsx`, `testdata/price_page.html` (T3), `rosstatIPCWeeksTable` (T2), `appendRecorder` (T4).
- Produces: `ipcWeeksImport(xlsx *excelize.File, batch driver.Batch) error`, `ipcWeeksLinkSelector`; `IpcWeeksStat` больше не имеет собственных `Import`/`export`.

- [ ] **Step 1: Написать падающий тест `rosstat/ipc_weeks_test.go`**

```go
func TestIpcWeeksReadsFixture(t *testing.T)      // точки > 3000, число рядов ≥ 100
func TestIpcWeeksMetaCoversParsedRows(t *testing.T) // односторонний контракт: каждый разобранный товар описан в ipcWeeksSeriesMeta() (стандарт §11.4)
func TestIpcWeeksSpotValue(t *testing.T)         // лист "2026", товар "Говядина (кроме бескостного мяса), кг", 2026-01-12 → 100.65
func TestIpcWeeksUnknownProductWarns(t *testing.T) // товар вне ipcWeeksProducts → log warn, точка всё равно пишется
func TestIpcWeeksMissingHeaderFails(t *testing.T) // лист без строки "Наименование" → ошибка с именем таблицы
func TestIpcWeeksShortRowDoesNotPanic(t *testing.T) // строка данных короче шапки → разбор не паникует
func TestIpcWeeksFindLink(t *testing.T)          // util.FindLink(testdata/price_page.html, ipcWeeksLinkSelector) → href с "Nedel_ipc"
```

- [ ] **Step 2: Запустить и убедиться, что падает**

Run: `go test ./rosstat/ -run TestIpcWeeks -v`
Expected: FAIL — функции разбора в нынешнем виде нет как отдельной единицы (`ipcWeeksImport` не существует).

- [ ] **Step 3: Убрать собственный цикл `Import()`**

Всё тело `IpcWeeksStat.Import` (DDL, `PrepareBatch`, `Send`, `UpsertSeriesCatalog`, `CreateView`) удалить: `IpcWeeksStat` остаётся обёрткой со встроенным `util.ClickHouseImport` с полями `TableName`, `CreateTable` (в `%s`-форме), `ImportFunc: ipcWeeksImport`, `AfterImport: afterRosstatMacro(ipcWeeksSeriesMeta())`. Точки контроля публикации нет — поля `PublicationCountPoints` не заполняются (стандарт §9.3).

- [ ] **Step 4: Внести разбор и защиту**

`ipcWeeksImport` — нынешний `parseIpcWeeks` + запись в батч, с правками: `RawCellValue: true`; `fmt.Printf` удалён; `percent, _ :=` заменён проверкой ошибки; `util.MonthsToNum[...]` — с проверкой наличия ключа (иначе месяц 0 и невнятная ошибка `time.Parse` позже); `rows[fieldIdx][j+1]` — с проверкой границы (`j+1 < len(rows[fieldIdx])`), иначе паника на строке данных длиннее шапки; лист без строки `Наименование` — ошибка; пустой разбор — ошибка. Продукт вне `ipcWeeksProducts` — `log.Warnf` с именем товара и импорт (стандарт §6.6: падение недельного шага остановило бы последующие шаги цепочки).

- [ ] **Step 5: Перевести ссылку на `util`**

Коллектор `colly` → `util.GetPage` + `util.FindLink` (`ipcWeeksLinkSelector = "a[href*=\"Nedel_ipc\"]"` — вместо позиционной CSS-цепочки); в `Import()` — override шаблона: получить страницу, найти href, выставить `DataUrl = rosstatUrl + href`, вызвать `s.ClickHouseImport.Import(ctx, conn)`.

- [ ] **Step 6: Прогнать тесты**

Run: `go test ./rosstat/ -run TestIpcWeeks -v -count=1`
Expected: PASS.

- [ ] **Step 7: Коммит**

```bash
git add rosstat/ipc_weeks.go rosstat/ipc_weeks_test.go rosstat/series_meta_test.go
git commit -m "ipc_weeks: port to util.ClickHouseImport, harden parsing"
```

---

