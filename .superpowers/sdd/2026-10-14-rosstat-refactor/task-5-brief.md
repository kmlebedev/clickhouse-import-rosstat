### Task 5: `vvp_kvartal` — починка разбора (заголовок A2, обход с нулевой колонки, перенос года)

**Files:**
- Modify: `rosstat/vvp_kvartal.go`
- Create: `rosstat/vvp_kvartal_test.go`
- Modify: `rosstat/series_meta_test.go` (убрать `TestVvpKvartalSeriesMetaCoversParsedSheets`)

**Interfaces:**
- Consumes: `util.GetPage`/`util.FindLink` (T1), `testdata/vvp_kvartal.xlsx` (T3), `rosstatVVPKvartalTable` (T2), `appendRecorder` (T4).
- Produces: `vvpKvartalImport(xlsx *excelize.File, batch driver.Batch) error`, `vvpKvartalSeriesNames` (карта «лист → имя ряда»), `vvpKvartalLinkSelector`, `getQuarterDate(year int, quarter string) (time.Time, error)`.

- [ ] **Step 1: Выяснить фактическую разметку по фикстуре**

Листы `2, 9, 10, 12, 14`: A2 — заголовок (объединённая A2:D2), строка 3 — годы в колонках A, E, I, … (у поздних групп с суффиксом сноски `20262)`), строка 4 — подписи `I квартал`…`IV квартал` с колонки A, строка 5 — данные с колонки A, значения — полные float.

- [ ] **Step 2: Написать падающий тест `rosstat/vvp_kvartal_test.go`**

```go
func TestVvpKvartalReadsFixture(t *testing.T)          // ровно 5 рядов = карта листов, точек 302: листы "2"/"9"/"10" — по 62 (2011-I … 2026-II), листы "12"/"14" — по 58 (2012-I … 2026-II)
func TestVvpKvartalKeepsFirstGroup(t *testing.T)       // в ряду листа "2" есть точка с датой 2011-04-01 (первая группа не теряется)
func TestVvpKvartalSpotValue(t *testing.T)             // лист "2", 2011-04-01 → 13024.799539024814 (допуск 1e-6) — сырое значение, не формат "13,024.8"
func TestVvpKvartalSeriesMetaCoversParsedRows(t *testing.T) // двусторонний контракт с vvpKvartalSeriesMeta
func TestVvpKvartalMissingSheetFails(t *testing.T)     // книга без листа "9" → ошибка с именем листа и таблицы, 0 строк в батче
func TestVvpKvartalUnknownQuarterFails(t *testing.T)   // подпись "X квартал" → ошибка, НЕ panic
func TestVvpKvartalFindLink(t *testing.T)              // util.FindLink(testdata/accounts_page.html, vvpKvartalLinkSelector) → href с "VVP_kvartal"
```

- [ ] **Step 3: Запустить и убедиться, что падает**

Run: `go test ./rosstat/ -run TestVvpKvartal -v`
Expected: FAIL — `panic: runtime error: index out of range` на чтении заголовка (нынешняя строка 88 файла).

- [ ] **Step 4: Переписать разбор**

Порядок для каждого листа из `vvpKvartalSheetNames` (слайс, а не карта — порядок обхода должен быть детерминированным):

1. `rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})`; ошибка — с именем листа.
2. Найти строку подписей кварталов: первая строка, где первая ячейка начинается с `I `, `II `, `III ` или `IV ` и содержит `квартал`. Не найдена — ошибка.
3. `yearRow = labelRow - 1`, `dataRow = labelRow + 1`; выход за границы — ошибка.
4. Обход колонок `i` с **нуля**, пока ячейка `dataRow[i]` не пуста: непустая ячейка `yearRow[i]` обновляет текущий год (первые 4 символа, `strconv.Atoi`; иначе ошибка); год 0 — ошибка с номером колонки; подпись квартала — первый токен, отображение `{"I":1,"II":2,"III":3,"IV":4}`, незнакомая подпись — ошибка; значение — `ParseFloat` в сыром виде, ошибка при нечисле.
5. Ноль точек на листе — ошибка.

`getQuarterDate(year int, quarter string) (time.Time, error)` возвращает ошибку вместо `panic`; `strings.Split(label, " ")[0]` заменяется разбором первого токена; `fmt.Printf` удаляются.

- [ ] **Step 5: Имя ряда — из карты, а не из заголовка**

`vvpKvartalSeriesNames` — карта «номер листа → стабильное имя», 1:1 с нынешними `vvpKvartalSeriesMeta[].Series` (`2 Валовой внутрений продукт`, `9 Валовой внутрений продукт`, `10 Валовой внутренний продукт`, `12 Индексы физического объема валового внутреннего продукта`, `14 Индексы - дефляторы валового внутреннего продукта`). Заголовок A2 больше не участвует в имени ряда (сноски и редакции перестают двигать имя); отсутствие ожидаемого листа ловит §6.4.

- [ ] **Step 6: Перевести `Import()` на `util`**

Коллектор `colly` → `util.GetPage` + `util.FindLink` (`vvpKvartalLinkSelector = "a[href*=\"VVP_kvartal\"]"`), пустая ссылка — ошибка.

- [ ] **Step 7: Прогнать тесты**

Run: `go test ./rosstat/ -run TestVvpKvartal -v -count=1`
Expected: PASS.

- [ ] **Step 8: Коммит**

```bash
git add rosstat/vvp_kvartal.go rosstat/vvp_kvartal_test.go rosstat/series_meta_test.go
git commit -m "fix vvp_kvartal: read title from A2, scan from column A, carry year across group"
```

---

