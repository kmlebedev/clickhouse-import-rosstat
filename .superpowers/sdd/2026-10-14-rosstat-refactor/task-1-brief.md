### Task 1: Вынос HTML-хелперов в `util/` и переключение `cbr/`

**Files:**
- Create: `util/links.go`
- Create: `util/links_test.go`
- Delete: `cbr/links.go`
- Modify: `cbr/key_rate.go`, `cbr/Infl_exp.go`, `cbr/credit_m2x.go`, `cbr/indicators_cpd.go`, `cbr/mo_br.go`, `cbr/mp_summary.go`
- Modify: `cbr/Infl_exp_test.go`, `cbr/credit_m2x_test.go`, `cbr/indicators_cpd_test.go`

**Interfaces:**
- Consumes: `util.GetFile(url string) (io.ReadCloser, error)` (существует).
- Produces: `util.GetPage(pageURL string) ([]byte, error)` и `util.FindLink(page []byte, selector string) (string, error)` — их используют все задачи про `rosstat/` (T3–T7).

- [ ] **Step 1: Прочитать эталон**

Прочитать `cbr/links.go` целиком: тела обеих функций, их комментарии и контракт (`FindLink` возвращает href как есть, пустое совпадение — ошибка; `GetPage` закрывает ридер и возвращает обе ошибки — чтения и закрытия).

- [ ] **Step 2: Написать падающий тест `util/links_test.go`**

Тесты по образцу `util/json_test.go` (там `httptest.NewServer` + `util.GetJSON`):

```go
func TestGetPageReturnsBody(t *testing.T)          // сервер отдаёт "<a href=\"/x.xlsx\">x</a>"; тело совпадает
func TestGetPageEmptyBodyFails(t *testing.T)       // сервер отдаёт 200 и 0 байт → ошибка (GetFile уже так делает)
func TestFindLinkReturnsHref(t *testing.T)         // inline-HTML, селектор `a[href$=".xlsx"]` → "/storage/x.xlsx"
func TestFindLinkMissingSelectorFails(t *testing.T) // селектор без совпадения → ошибка, в тексте есть селектор
func TestFindLinkBadHTMLFails(t *testing.T)        // битый HTML → ошибка разбора
```

- [ ] **Step 3: Запустить тест и убедиться, что он падает**

Run: `go test ./util/ -run 'TestGetPage|TestFindLink' -v`
Expected: FAIL — `undefined: util.GetPage`, `undefined: util.FindLink`.

- [ ] **Step 4: Создать `util/links.go`, перенеся тела из `cbr/links.go`**

```go
package util

func GetPage(pageURL string) ([]byte, error)
func FindLink(page []byte, selector string) (string, error)
```

Тела — дословный переезд из `cbr/links.go`; комментарии сохранить и заменить упоминание пакета на `util` (описать, что хелпер общий для пакетов импортёров, стандарт §4.1).

- [ ] **Step 5: Запустить тест и убедиться, что он проходит**

Run: `go test ./util/ -run 'TestGetPage|TestFindLink' -v`
Expected: PASS.

- [ ] **Step 6: Удалить `cbr/links.go` и переключить вызовы в `cbr/`**

`git rm cbr/links.go`; в шести файлах импортёров и трёх тестовых заменить `findLink(` → `util.FindLink(` и `getPage(` → `util.GetPage(` (импорт `util` в этих файлах уже есть; `goquery` из тестов не удалять, если он ещё используется).

- [ ] **Step 7: Прогнать тесты `cbr/` — они и есть гейт задачи**

Run: `go test ./cbr/ -count=1` и `grep -rn "findLink\|getPage" cbr/ | grep -v util.`
Expected: `ok`; grep не находит ни одного вызова без префикса `util.`.

- [ ] **Step 8: Коммит**

```bash
git add -A util/links.go util/links_test.go cbr/
git commit -m "move findLink/getPage from cbr/ to util as GetPage/FindLink"
```

---

