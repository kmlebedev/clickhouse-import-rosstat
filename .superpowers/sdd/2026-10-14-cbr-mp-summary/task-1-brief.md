### Task 1: `util.PostJSON` — POST JSON для LLM-вызовов

**Files:**
- Modify: `util/json.go` (рядом с `GetJSON`)
- Test: `util/json_test.go`

**Interfaces:**
- Produces (использует Task 5):
  - `var postJSONTimeout = 65 * time.Second` — переменная (не константа), тест подставляет короткую;
  - `func PostJSON(url string, headers map[string]string, request any, out any) error` — тело запроса сериализуется как JSON; HTTP — общий `HttpClient`; не-2xx — ошибка с кодом и началом тела (обрезка как `maxErrorSnippet`); пустое тело — ошибка; дедлайн `postJSONTimeout`; ключ — только в заголовке (в ошибках `url.Error` URL без секретов).

- [ ] **Step 1: Написать падающие тесты** (`httptest`): успех (200, `{...}` → структура); не-200 (500 с телом → ошибка содержит код); не-JSON тело (200, HTML → ошибка разбора); таймаут (`postJSONTimeout` = 50 ms, сервер спит → ошибка); заголовок `Authorization: Bearer test` доезжает до сервера (assert в обработчике).

- [ ] **Step 2: Прогнать — FAIL**: `go test ./util/ -run TestPostJSON -v` → `undefined: PostJSON`.

- [ ] **Step 3: Реализовать `PostJSON`** по образцу `GetJSON` (та же структура ошибок; `http.NewRequest(http.MethodPost, …)`, `bytes.NewReader(jsonBody)`, `Content-Type: application/json`; `defer resp.Body.Close()`; ошибки оборачивать без URL, когда он может нести секреты — ключ у нас в заголовке, но конвенцию сохранить).

- [ ] **Step 4: Прогнать — PASS**: `go test ./util/ -count=1 && gofmt -l util/`.

- [ ] **Step 5: Commit**: `git add util/json.go util/json_test.go && git commit -m "util: PostJSON helper for LLM calls (deadline, status check)"`.

---

