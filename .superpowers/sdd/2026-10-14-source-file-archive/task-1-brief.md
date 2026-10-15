### Task 1: Пакет `archive/` — контракт, ключ, локальный бэкенд, конфигурация

**Files:**
- Create: `archive/store.go`, `archive/key.go`, `archive/local.go`, `archive/fromenv.go`
- Test: `archive/key_test.go`, `archive/local_test.go`, `archive/fromenv_test.go`

**Interfaces:**
- Consumes: ничего (первая задача пакета).
- Produces: интерфейс `Store` (`Name() string`, `Put(ctx, key string, body []byte) error`, `Get(ctx, key string) ([]byte, error)`, `Exists(ctx, key string) (bool, error)`); `ErrAuth`, `ErrQuota` (sentinel, оборачиваются через `%w`); `Key(source, basename, fingerprint string, sum [sha256.Size]byte) string`; `NewLocal(dir string) *LocalStore`; `FromEnv(hc *http.Client) (Store, error)`; константы `EnvBackend`, `EnvYandexToken`, `EnvYandexDir`, `EnvLocalDir`, `DefaultYandexDir = "disk:/rosstat-archive"`.

- [ ] **Step 1: Написать `archive/store.go`**

Комментарий интерфейса: контракт минимален под два бэкенда (Яндекс.Диск сегодня, S3 — `PutObject`/`GetObject`/`HeadObject` — потом); ключи относительные, корень навешивает бэкенд. `ErrAuth` — «токен не принят (401)», `ErrQuota` — «квота или лимит хранилища (403/413/423/507)».

- [ ] **Step 2: Написать падающие тесты ключа `archive/key_test.go`**

Тесты (таблица):
- `TestKeyFromFingerprint` — `Key("cbr_m2", "monetary_agg_SA.xlsx", "8f3c…32hex", zeroSum)` = `cbr_m2/8f3c…(16)_monetary_agg_SA.xlsx`.
- `TestKeyWithoutFingerprintUsesSHA` — пустой `fingerprint` → префикс `sha-` + первые 16 hex sha256; два разных `sum` дают разные ключи (нет схлопывания в один объект).
- `TestKeySanitizesBasename` — `/`, `\`, `:`, `*`, `?`, `"`, `<`, `>`, `|`, управляющие символы → `_`; кириллица сохраняется; имя длиннее 200 байт обрезается по границе руны; пустое после санитизации → `file`.
- `TestKeyFingerprintShorterThan16` — короткий (нештатный) отпечаток не паникует и не даёт пустой префикс.

- [ ] **Step 3: Запустить тесты — убедиться, что падают**

Run: `go test -count=1 ./archive/` — Expected: FAIL (`undefined: Key`).

- [ ] **Step 4: Реализовать `archive/key.go`**

Правило обрезки до 200 байт — по границе руны (`utf8.RuneStart`), чтобы имя не разваливалось на битых байтах; общий путь `source/` + префикс из 16 символов + `_` + имя ≤ 255 байт на компонент.

- [ ] **Step 5: Написать `archive/local.go` и его тесты**

`LocalStore{Dir}`; `Put` — `os.MkdirAll` родителя + `os.WriteFile(…, 0o644)`; `Get` — `os.ReadFile`; `Exists` — `os.Stat`, отсутствие файла = `false, nil` (не ошибка). Защита от выхода за каталог: `filepath.Join(dir, filepath.FromSlash(key))` сверяется с `filepath.Clean(dir)+separator` — ключ с `..` возвращает ошибку.

Тесты `archive/local_test.go`: round-trip Put→Get→Exists; отсутствие ключа → `false, nil`; `..`-ключ → ошибка и файл вне каталога не создан; вложенный ключ `cbr_m2/x_y.xlsx` создаёт подкаталог.

- [ ] **Step 6: Написать `archive/fromenv.go` и его тесты**

`FromEnv`: пустой `ARCHIVE_BACKEND` → `(nil, nil)`; `local` → `ARCHIVE_LOCAL_DIR` (пусто — ошибка с именем переменной); `yandex` → `ARCHIVE_YANDEX_TOKEN` обязателен (ошибка с именем переменной, **без значения**), корень из `ARCHIVE_YANDEX_DIR` или `DefaultYandexDir`; неизвестное значение → ошибка. Тесты используют `t.Setenv`.

- [ ] **Step 7: Прогнать пакет и зафиксировать результат**

Run: `gofmt -l archive/` (пусто), `go vet ./archive/`, `go test -race -count=1 ./archive/` — Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add archive/store.go archive/key.go archive/local.go archive/fromenv.go archive/key_test.go archive/local_test.go archive/fromenv_test.go
git commit -m "add archive store contract, key scheme and local backend"
```

---

