# Винтажная база: архив версий файлов источников — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** сохранять каждую увиденную конвейером версию файла источника в объектном хранилище (сегодня — личный Яндекс.Диск по REST API), с реестром `registry.jsonl` в самом хранилище и git-зеркалом, — чтобы «урезанное» обновление файла у источника и наполнение пустой ClickHouse были восстановимы.

**Architecture:** новый пакет `archive/` с интерфейсом `Store` (Put/Get/Exists), бэкендами `LocalStore` и `YandexDisk` и реестром `Registry`; интеграция — в общий шаблон `util.ClickHouseImport` (архивация после успешного импорта версии, проверка «версия в архиве» на no-op-пути). Пакеты импортёров своего кода не меняют; схема ClickHouse не меняется.

**Tech Stack:** Go 1.27 (stdlib: `net/http`, `encoding/json`, `crypto/sha256`), logrus, clickhouse-go/v2, excelize — **без новых зависимостей**.

**Spec:** `docs/superpowers/specs/2026-10-14-source-file-archive-design.md` (коммит `85552fe` этой ветки). Спека — связующая власть, план — её аргумент.

## Global Constraints

- Ветка `source-file-archive`, рабочий каталог — worktree `.worktrees/source-file-archive`. Основной каталог и ветку `rosstat-labour-force` (там незакоммиченный `deploy/staging/dagu.service` владельца) не трогать. Один PR в конце, промежуточных нет.
- **Новых зависимостей не добавлять** — только stdlib и уже используемые библиотеки. CI гоняет `govulncheck`.
- Секреты — только `os.Getenv`; значения не попадают ни в логи, ни в тексты ошибок, ни в тесты. Подписанная ссылка `href` Яндекс.Диска тоже не логируется — логируется путь на Диске.
- Схема ClickHouse не меняется: ни таблиц, ни витрин, ни грантов; `sql/mcp_kimi_reader.sql`, `scripts/mcp_check.py`, `source_publications` — не трогаются.
- Тесты офлайн: `httptest`-серверы и `t.TempDir()`, ни одного сетевого вызова. Тесты — в пакете с кодом (конвенция репозитория), комментарии по-русски, объясняют «почему».
- Комментарии и тексты — в стиле `util/publications.go`: почему так, а не что делает строка.
- Коммиты — короткие, по-английски, в духе истории (`add archive store and local backend`).
- Проверка задачи: `gofmt -l <файлы>` пуст, `go vet ./archive/ ./util/`, целевые тесты `go test -race -count=1 ./<пакет>/`; полный `make all` — перед приёмкой (Task 7).

## Review Focus

1. **Отсутствующий или пустой `registry.jsonl`** (самый первый прогон, реестр потерян/пуст): чтение не падает, `Ensure` заводит файл с первой строкой. — тест в Task 3.
2. **`409` при создании уже существующей папки** на Диске: штатная идемпотентность подготовки пути, а не ошибка. — тест в Task 2.
3. **`429`/`503` от API**: повтор, а не потеря версии файла. — тест в Task 2.
4. **`401`/`403`**: различимые `ErrAuth`/`ErrQuota`, и в тексте ошибки нет ни токена, ни подписанного `href`. — тест в Task 2.
5. **HEAD недоступен (`fingerprint == ""`)**: ключ по sha256, без пустого префикса и без коллизий разных файлов; кириллица и длинные имена санитизируются до ≤255 байт. — тесты в Task 1.
6. **Архив выключен**: шаблон не делает ни одного лишнего вызова и ни одного обращения к хранилищу. — тест в Task 4.

---

## Файловая структура

| Файл | Ответственность |
|---|---|
| `archive/store.go` (new) | Интерфейс `Store`, sentinel-ошибки `ErrAuth`/`ErrQuota` |
| `archive/key.go` (new) | `Key` + санитизация basename |
| `archive/local.go` (new) | `LocalStore` (dev/тесты) |
| `archive/yandex.go` (new) | `YandexDisk` (REST: upload/download/resources, повторы) |
| `archive/registry.go` (new) | `Entry`, `Registry` (JSONL, upsert, ленивое чтение) |
| `archive/fromenv.go` (new) | Константы имён env, `FromEnv` |
| `util/xls.go` (modify) | `GetFileBytes` |
| `util/archive.go` (new) | Синглтон хранилища, `save` (sha256→Exists→Put→Registry.Ensure), `versionExists`, test-hook |
| `util/clickhouse_import.go` (modify) | Порядок «точки → архив → производные → версия», проверка архива на no-op-пути, `importXlsBody` |
| `main.go` (modify) | `ARCHIVE_ACTION=registry` до подключения к ClickHouse |
| `Makefile` (modify) | Цель `archive-registry`, строка в `env-check` |
| `deploy/staging/clickhouse-import-rosstat.env.example` (modify) | Блок архивации |
| `docs/archive/README.md` (new), `docs/archive/registry.jsonl` (new, Task 7) | Git-зеркало реестра |
| `docs/ROADMAP_INFRA.md` (new) | Инфраструктурный роадмап, шаг 1.1 |
| `docs/IMPORTER_PACKAGE_STANDARD.md`, `AGENTS.md`, `ARCHITECTURE.md`, `README.md` (modify) | Требование винтажной базы и эксплуатация |

---

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

### Task 3: Реестр `registry.jsonl`

**Files:**
- Create: `archive/registry.go`
- Test: `archive/registry_test.go`

**Interfaces:**
- Consumes: `Store` (Task 1), `LocalStore` (Task 1).
- Produces: `RegistryKey = "registry.jsonl"`; `Entry{Source, Name, Fingerprint, SHA256 string; Size int64; URL, Key string; FirstSeen time.Time}` с JSON-тегами `source,name,fingerprint,sha256,size,url,key,first_seen`; `NewRegistry(store Store) *Registry`; `(*Registry).Ensure(ctx context.Context, e Entry) error`; `(*Registry).Entries() []Entry` (копия).

- [ ] **Step 1: Написать падающие тесты**

- `TestRegistryEnsureCreatesMissingFile` — пустое хранилище: `Ensure` создаёт `registry.jsonl` с одной строкой; формат — одна JSON-строка с `\n`, все восемь полей (Review Focus №1).
- `TestRegistryEnsureIsIdempotent` — повторный `Ensure` той же записи (тот же `Key`, тот же `SHA256`) не меняет байты файла (сравнить содержимое до/после).
- `TestRegistryEnsureReplacesChangedContent` — тот же `Key`, другой `SHA256` → строка заменена, число строк не выросло.
- `TestRegistryAppendsNewKeys` — второй ключ добавляется строкой ниже, порядок — по добавлению.
- `TestRegistryEntriesReturnsCopy` — мутация результата `Entries()` не меняет состояние реестра.

- [ ] **Step 2: Проверить, что тесты падают**

Run: `go test -run TestRegistry -count=1 ./archive/` — Expected: FAIL (`undefined: NewRegistry`).

- [ ] **Step 3: Реализовать `archive/registry.go`**

Ленивое чтение: при первом `Ensure` — `Exists`, затем `Get`; ошибка чтения (не «нет файла») — ошибка; битая строка JSONL — ошибка (реестр — источник истины, молча терять строки нельзя). Запись — `json.Marshal` каждой строки + `\n`, один `Put` всего файла; мьютекс на процесс (штатный писатель один — последовательная цепочка dagu; конкурентный ручной прогон самолечится префлайтом, спека §3.4).

- [ ] **Step 4: Прогнать пакет**

Run: `go test -race -count=1 ./archive/` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add archive/registry.go archive/registry_test.go
git commit -m "add archive registry with jsonl upsert"
```

---

### Task 4: Интеграция в шаблон `util.ClickHouseImport`

**Files:**
- Modify: `util/xls.go` (рядом с `GetFile`, после строки 153), `util/clickhouse_import.go`
- Create: `util/archive.go`, `util/archive_test.go`
- Test: `util/xls_test.go` (дополнить)

**Interfaces:**
- Consumes: `archive.Store`, `archive.Key`, `archive.NewRegistry`, `archive.Entry`, `archive.RegistryKey`, `archive.FromEnv` (Tasks 1–3); `publicationFakeConn`/`publicationFakeRow` из `util/publications_test.go` (тесты в том же пакете).
- Produces: `GetFileBytes(url string) ([]byte, error)`; в `util`: `activeArchive() *archiveRuntime` (nil — выключено), `(*archiveRuntime).save(ctx, source, url string, body []byte, fingerprint string) error`, `(*archiveRuntime).versionExists(ctx, source, url, fingerprint string) (bool, error)`, переменная `archiveRuntimeForTests *archiveRuntime` (test hook, nil у прода); порядок в `Import`: DDL → `BeforeImport` → префлайт → точки → **архив** → `AfterImport` → версия; сбой архива отменяет запись версии, но не импорт.

- [ ] **Step 1: Тест `GetFileBytes` в `util/xls_test.go`**

По образцу `TestGetFileReturnsBody`: тело читается целиком, ошибки чтения нет, `User-Agent` уходит (проверка в хендлере, `t.Error` из горутины).

Run: `go test -run TestGetFileBytes -count=1 ./util/` — Expected: FAIL (`undefined: GetFileBytes`).

- [ ] **Step 2: Реализовать `GetFileBytes` в `util/xls.go`**

`GetFile` + `io.ReadAll` + закрытие с проверкой обеих ошибок (идиом `GetPage`, `util/links.go:36-49`). Комментарий: байты нужны и разбору (`excelize.OpenReader`), и винтажному архиву.

- [ ] **Step 3: Написать `util/archive.go`**

- `type archiveRuntime struct { store archive.Store; registry *archive.Registry }`.
- `activeArchive()`: если `archiveRuntimeForTests != nil` — вернуть его; иначе `sync.Once` → `archive.FromEnv(HttpClient)`; ошибка конфигурации → `log.Errorf` один раз + nil (данные важнее, спека §3.1).
- `versionExists(ctx, source, url, fingerprint)` — `archive.Key(source, path.Base(url), fingerprint, [32]byte{})` → `store.Exists` (при непустом отпечатке sha не участвует; ключ совпадает с тем, что пишет `save`).
- `save(ctx, source, url, body, fingerprint)` — `sha256.Sum256(body)` → `archive.Key` → `Exists` (есть — не перезагружать) → `Put` → `registry.Ensure(archive.Entry{Source, Name: path.Base(url), Fingerprint, SHA256: hex, Size: int64(len(body)), URL: url, Key, FirstSeen: time.Now()})`.
- Комментарии: почему `Exists`-предпроверка, почему строка реестра дописывается и при уже лежащем объекте (самолечение реестра), почему сорсы/сайд-эффекты не идут через поля структуры.

- [ ] **Step 4: Правка `util/clickhouse_import.go`**

- Выделить `importXlsBody(ctx, body []byte, conn driver.Conn) (int64, error)` (руки `PrepareBatch` → `ImportFunc` → `Send`; без изменений логики), `ImportXls` — тонкая обёртка: `GetFileBytes` + `log.Infof("Get xlsx file %+v", dataUrl)` + `importXlsBody`.
- В `Import` путь `ImportFunc` заменяется на: `GetFileBytes` → лог → `importXlsBody`; байты остаются локальной переменной.
- После `Send`, там же в ветке `ImportFunc` (до `AfterImport`): если `activeArchive() != nil && s.PublicationCountPoints != ""` — `rt.save(…, fingerprint)`; ошибка → `log.Errorf("%s: версия %s не сохранена в архиве (%v) — строка версии не пишется, следующий прогон повторит импорт", …)` и локальный флаг `archiveFailed = true`. Ветка `CrawFunc` архива не получает (файл качает сам импортёр).
- В ветке `noop` префлайта (после `noop == true` и `fingerprint != ""`): при `activeArchive() != nil` — `versionExists`; `false` или ошибка → `log.Warnf("%s: версия %s известна, но в архиве её нет — полный импорт", …)` (ошибка — с текстом причины) и `noop = false`.
- `SavePublication` — только если `fingerprint != "" && !archiveFailed`.

- [ ] **Step 5: Тесты шаблона (`util/archive_test.go`)**

Хелпер: httptest-сервер, который на `HEAD` отдаёт версионные заголовки (как `publicationHeadServer`), на `GET` — минимальную валидную книгу (`excelize.NewFile` + `SetCellValue` + `WriteToBuffer`) и считает `GET`-запросы; `ImportFunc` пишет одну строку с `Append`. Хранилище — `archive.NewLocal(t.TempDir())` через `archiveRuntimeForTests` (`t.Cleanup` сбрасывает).

- `TestClickHouseImportArchivesVersion` — полный прогон (очередь `QueryRow`: `[sql.ErrNoRows (версия неизвестна), points]`): объект `cbr_m2/<fp16>_full.xlsx` и `registry.jsonl` в каталоге архива; строка версии записана; `Get` из LocalStore отдаёт те же байты (sha256 совпадает с реестром).
- `TestClickHouseImportArchiveKnownNoop` — очередь `[known version, matching points]`, объект на месте → no-op: `GET`-запросов ноль, батчей ноль.
- `TestClickHouseImportArchiveSelfHeal` — очередь `[known version, matching points, points]`, объект удалён → полный импорт (один `GET`), объект восстановлен, версия перезаписана (третий `QueryRow` — `CountPoints` записи версии).
- `TestClickHouseImportArchiveFailureKeepsVersionUnwritten` — store-обёртка с падающим `Put` (очередь `[sql.ErrNoRows, points]`) → `AfterImport` вызван, `PrepareBatch` в `source_publications` **не** было, `Import` вернул `nil` (Review Focus №6).
- `TestClickHouseImportArchiveOffNoCalls` — `archiveRuntimeForTests = nil`, окружение без `ARCHIVE_BACKEND` → ни одного обращения к хранилищу, поведение как до правки.

- [ ] **Step 6: Прогнать пакет**

Run: `gofmt -l util/`, `go vet ./util/`, `go test -race -count=1 ./util/` — Expected: PASS (включая все прежние тесты публикаций без изменений).

- [ ] **Step 7: Commit**

```bash
git add util/xls.go util/archive.go util/clickhouse_import.go util/xls_test.go util/archive_test.go
git commit -m "archive source file versions in import template"
```

---

### Task 5: Режим `ARCHIVE_ACTION=registry`, Makefile, staging-env, git-зеркало

**Files:**
- Modify: `main.go`, `Makefile` (цель `import` ~строка 166, блок `env-check` 156–162), `deploy/staging/clickhouse-import-rosstat.env.example`
- Create: `docs/archive/README.md`

**Interfaces:**
- Consumes: `archive.FromEnv`, `archive.RegistryKey` (Task 1/3); `util.HttpClient` (существующий).
- Produces: env-режим `ARCHIVE_ACTION=registry` (без подключения к ClickHouse); `ARCHIVE_REGISTRY_OUT` (default `docs/archive/registry.jsonl`); цель `make archive-registry`.

- [ ] **Step 1: Реализовать режим в `main.go`**

В начале `main()`, до `clickhouse.ParseDSN`: если `ARCHIVE_ACTION == "registry"` — `runArchiveRegistry(ctx)` и выход. `runArchiveRegistry`: `archive.FromEnv(util.HttpClient)`; `store == nil` → ошибка «ARCHIVE_BACKEND не задан»; `Exists(RegistryKey)` → нет файла: `log.Warnf("реестр пуст")` и запись пустого файла; есть — `Get`; `os.MkdirAll` + `os.WriteFile(out, body, 0o644)`; лог: число строк и путь. Обычный прогон импортёров — без изменений.

- [ ] **Step 2: Makefile**

```make
# Git-зеркало реестра винтажного архива (make archive-registry [ARCHIVE_BACKEND=yandex]):
# скачивает registry.jsonl из хранилища в docs/archive/; коммит — ручной/периодический.
archive-registry:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY)-native .
	ARCHIVE_ACTION=registry ./$(BUILD_DIR)/$(BINARY)-native
```

В `env-check` — строка вида остальных: `ARCHIVE_YANDEX_TOKEN задан / не задан (нужен при ARCHIVE_BACKEND=yandex)`; в `.PHONY` — `archive-registry`.

- [ ] **Step 3: Staging-env и README зеркала**

`deploy/staging/clickhouse-import-rosstat.env.example` — блок с комментариями: `ARCHIVE_BACKEND=yandex`, `ARCHIVE_YANDEX_TOKEN=REPLACE_ME` (значение — OAuth-токен приложения, scope `cloud_api:disk.write`), `ARCHIVE_YANDEX_DIR=disk:/rosstat-archive`; пометить, что токен обязателен при включённом бэкенде и что архив отключён при пустом `ARCHIVE_BACKEND`. `docs/archive/README.md` — что это зеркало реестра `registry.jsonl` из хранилища, что истина — файл на Диске, как обновлять (`make archive-registry`), что коммит периодический/ручной (на staging нет git-дерева, автокоммит невозможен), формат строки (перечень полей).

- [ ] **Step 4: Проверки**

Run: `gofmt -l main.go`, `go build ./...`, `go vet ./...`, `go test -race -count=1 ./archive/ ./util/`; `make archive-registry` без `ARCHIVE_BACKEND` → ненулевой код и понятная ошибка (проверить вручную, зафиксировать вывод); `make env-check` → строка `ARCHIVE_YANDEX_TOKEN` присутствует.

- [ ] **Step 5: Commit**

```bash
git add main.go Makefile deploy/staging/clickhouse-import-rosstat.env.example docs/archive/README.md
git commit -m "add archive registry mirror target and staging env"
```

---

### Task 6: Нормативные документы

**Files:**
- Create: `docs/ROADMAP_INFRA.md`
- Modify: `docs/IMPORTER_PACKAGE_STANDARD.md` (§2, §9, §14, §16), `AGENTS.md`, `ARCHITECTURE.md` (§2, §3, §5, §8 п. 13), `README.md` («Ключи и окружение», «Ограничения и известные проблемы»)
- Использовать скил: `sync-readme-architecture` (`.kimi-code/skills/sync-readme-architecture/SKILL.md`) — он задаёт порядок проверки синхронности README/ARCHITECTURE.

**Interfaces:**
- Consumes: поведение из Tasks 1–5 (имена env, цель make, формат реестра, семантика префлайта).
- Produces: тексты норм — ссылки на них пойдут из Task 7 и будущих пунктов.

- [ ] **Step 1: `docs/ROADMAP_INFRA.md`**

Шапка (дата, роль, правила), §1 «Зачем отдельный роадмап» (инфраструктура обслуживает все треки и не принадлежит ни одному; трековые роадмапы перечислены ссылками), §2 «Фаза 1 — Надёжность данных» таблица `| Шаг | Содержание | Критерий приёмки |`: строка **1.1** «Винтажная база: архив версий файлов источников (`archive/` + `registry.jsonl` на Яндекс.Диске, git-зеркало; покрывает 14 импортёров с точкой контроля публикации, включая `rosstat_labour_force_mes` из PR #61)» со статусом «⏳ реализуется в ветке `source-file-archive`» (Task 7 заменит на ✅ с результатами), далее названные follow-up строки-заготовки: восстановление/наполнение БД из архива, S3-бэкенд, детектор «урезанного покрытия». §3 «Приёмка и риски» — кратко скопировать порядок приёмки пункта (команда → результат) и правило «один пункт = одна ветка = один PR».

- [ ] **Step 2: Стандарт импортёров**

- §2 «Термины»: строка «**Винтажный архив** — сохранённые версии файлов источника в объектном хранилище + реестр `registry.jsonl`; вход будущего восстановления базы».
- §9: новый пункт **9.6 (ОБЯЗАТЕЛЬНО)** «Винтажная база»: версия каждого скачанного файла сохраняется в архиве общим механизмом шаблона (`ARCHIVE_BACKEND`; кода импортёра не требует); импортёр обязан качать артефакт шаблонным путём (`DataUrl`/`BeforeImport` + `ImportFunc`); `CrawFunc`, качающий файл внутри себя (пример `bank/sber_fin_rez.go`), в архив не попадает — новый источник с файлом на краулерном транспорте обязан назвать отступление в спеке (§1.5); ссылка на ARCHITECTURE §8 п. 13 за деталями.
- §14 «Чек-лист: новый пакет» — пункт: «версия файла видна в архиве и реестре (стандарт §9.6)».
- §16 «Приёмка» — пункт: «файл версии в архиве, строка в `registry.jsonl` (для импортёров с точкой контроля)».

- [ ] **Step 3: `AGENTS.md`**

В правиле «Разработка строго по дорожной карте» (пункт 1) добавить ссылку: инфраструктурные пункты — `docs/ROADMAP_INFRA.md`. Больше AGENTS.md не трогать.

- [ ] **Step 4: `ARCHITECTURE.md`**

- §2 «Структура репозитория»: строка пакета `archive/` (объектное хранилище версий файлов источников: `Store` + `LocalStore`/`YandexDisk`, реестр; S3 — будущий бэкенд).
- §3: в описание полного цикла `util.ClickHouseImport` добавить шаг «архив» между точками и производными и оговорку про сбой архива (warn + строка версии не пишется).
- §5: строки env `ARCHIVE_BACKEND`, `ARCHIVE_YANDEX_TOKEN` (формулировка секрета — по образцу `GOST_APP_ID`: только окружение, dev/staging, в ошибки не попадает), `ARCHIVE_YANDEX_DIR`, `ARCHIVE_LOCAL_DIR`, `ARCHIVE_ACTION`.
- §8 п. 13: расширенная семантика префлайта («известная версия должна быть в архиве; нет — полный импорт») и ссылка на стандарт §9.6; упомянуть, что реестр читается без ClickHouse.

- [ ] **Step 5: `README.md`**

- «Ключи и окружение»: пошагово — OAuth-приложение Яндекса (scope `cloud_api:disk.write`/`read`) → implicit-flow токен → `~/.config/rosstat/env` / staging-env; ловушка с `ARCHIVE_BACKEND` (не дублировать в dev-env, выбирать аргументом make); реальные команды приёмки из спеки §6.
- «Ограничения и известные проблемы»: что архивируется (14 импортёров с точкой контроля, XLSX-путь шаблона), что нет (краулеры, прочие пакеты, PDF, страницы), первый прогон после включения — полный ре-импорт, восстановление из архива — follow-up `ROADMAP_INFRA.md`.
- Проверить синхронность скилом `sync-readme-architecture`.

- [ ] **Step 6: Commit**

```bash
git add docs/ROADMAP_INFRA.md docs/IMPORTER_PACKAGE_STANDARD.md AGENTS.md ARCHITECTURE.md README.md
git commit -m "document vintage file archive in normative docs"
```

---

### Task 7: Живая приёмка, git-зеркало, статус ✅ (выполняет контроллер вместе с владельцем)

**Files:**
- Modify: `docs/archive/registry.jsonl` (создаётся целью), `docs/ROADMAP_INFRA.md` (статус шага 1.1)
- Никакого нового кода.

**Interfaces:**
- Consumes: всё предыдущее. **Требует поднятого dev-ClickHouse** (`make ch-status`; поднимает владелец — `make ch-up` ассистент не делает) и **токен Яндекс.Диска** от владельца в `~/.config/rosstat/env` — перед задачей спросить владельца, токен готов ли.

- [ ] **Step 1: `make all`** — зелёный (gofmt, golangci-lint, vet, `go test -race ./...`, обе сборки).

- [ ] **Step 2: Local-бэкенд, машинный цикл** — `make import STAT=cbr_m2 ARCHIVE_BACKEND=local ARCHIVE_LOCAL_DIR=build/archive-dev`: объект `cbr_m2/<fp16>_monetary_agg_SA.xlsx` и `registry.jsonl` на месте, строка версии в `source_publications`; повтор → `no-op, версия … уже импортирована`; удалить объект → прогон → warn «версия известна, но не в архиве» + восстановление.

- [ ] **Step 3: Yandex-бэкенд, живой** — `make import STAT=cbr_m2 ARCHIVE_BACKEND=yandex`: файл виден в веб-интерфейсе Диска, размер совпадает с `size` в реестре (подтверждает владелец); повтор → no-op.

- [ ] **Step 4: Git-зеркало** — `make archive-registry ARCHIVE_BACKEND=yandex` → `docs/archive/registry.jsonl` содержит запись; коммит.

- [ ] **Step 5: `make env-check`** — строка `ARCHIVE_YANDEX_TOKEN`; `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА.

- [ ] **Step 6: Статус в `docs/ROADMAP_INFRA.md`** — шаг 1.1 → ✅ с командами и результатами (число строк реестра, отпечаток, размеры), Named follow-ups остаются ⏳.

- [ ] **Step 7: Commit** — `accept vintage file archive (roadmap infra 1.1)`.

---

## Порядок исполнения

Tasks 1–3 независимы по содержанию, но Task 4 зависит от их интерфейсов (в Interfaces указано, что именно); Tasks 5–6 зависят от поведения 1–4; Task 7 — после всего. После задач — `superpowers:requesting-code-review` (финальное ревью всей ветки), затем `superpowers:verification-before-completion`, затем один PR.
