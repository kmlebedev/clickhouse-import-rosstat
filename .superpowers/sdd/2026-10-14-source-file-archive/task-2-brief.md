### Task 2: Бэкенд `YandexDisk`

**Files:**
- Create: `archive/yandex.go`
- Test: `archive/yandex_test.go`

**Interfaces:**
- Consumes: `Store`, `ErrAuth`, `ErrQuota` (Task 1).
- Produces: `NewYandex(token, root, baseURL string, hc *http.Client) *YandexDisk`; `Name() == "yandex"`; переменная `opTimeout` (2 минуты, подменяется в тестах); переменная `retrySleeps = []time.Duration{time.Second, 2 * time.Second}` (подменяется в тестах). Пустой `baseURL` → `https://cloud-api.yandex.net/v1/disk`.

- [ ] **Step 1: Написать падающий тест happy path**

`httptest`-сервер эмулирует API: `PUT /resources?path=…` → `201` (повтор → `409`), `GET /resources/upload?path=…` → `{"href":"<server>/upload-target/1","method":"PUT"}`, `PUT /upload-target/1` → `201`, `GET /resources/download?path=…` → `{"href":"<server>/download-target/1","method":"GET"}`, `GET /download-target/1` → те же байты, `GET /resources?path=…` → `200`/`404`.

Тест `TestYandexPutGetExists`: `Put` доходит; папка создаётся один раз (`409` на повторе не ломает); `Get` возвращает исходные байты; `Exists` = true до и false после «удаления».

- [ ] **Step 2: Проверить, что тест падает**

Run: `go test -run TestYandex -count=1 ./archive/` — Expected: FAIL (`undefined: NewYandex`).

- [ ] **Step 3: Реализовать `archive/yandex.go`**

- Путь на Диске = `root + "/" + key` (root вида `disk:/rosstat-archive`); параметр `path` кодируется как query-значение.
- `Put`: создать родительские папки (`PUT resources`, `201` и `409` — успех) → `resources/upload?overwrite=true` → `PUT` тела на `href`; успех `201`/`202`; `href` не логируется.
- `Get`: `resources/download` → `GET href` → тело целиком. `Exists`: `GET resources?path=…` → `200` = true, `404` = false.
- Повторы: весь шаг повторяется до 3 попыток (`retrySleeps` между ними) на статусах `429`/`503`; на `401` — обёртка `ErrAuth`, на `403`/`413`/`423`/`507` — `ErrQuota`, остальные — `fmt.Errorf("<операция> <путь на Диске>: статус %d")`. Подписанный `href` в текст ошибки не попадает.
- Дедлайн: `context.WithTimeout` на операцию (`opTimeout`).
- Авторизация: заголовок `Authorization: OAuth <token>` только на запросы к `baseURL`; на `href`-запрос — без токена.

- [ ] **Step 4: Тесты повторов и ошибок**

- `TestYandexRetriesOn429`: первый `upload`-ответ `429`, второй `200` → `Put` успешен; `retrySleeps` заменён на `{time.Millisecond}` через `t.Cleanup`.
- `TestYandexAuthError`: `401` → `errors.Is(err, ErrAuth)` и в тексте ошибки нет значения токена.
- `TestYandexQuotaError`: `507` → `errors.Is(err, ErrQuota)`.
- `TestYandexFoldersIdempotent`: второй `Put` другого файла в ту же папку не падает на `409`.

Run: `go test -race -count=1 ./archive/` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add archive/yandex.go archive/yandex_test.go
git commit -m "add yandex disk archive backend"
```

---

