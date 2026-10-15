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

