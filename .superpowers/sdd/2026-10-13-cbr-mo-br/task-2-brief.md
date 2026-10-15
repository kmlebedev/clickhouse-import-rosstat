### Task 2: `util/publications.go` — общий механизм точек контроля публикаций

**Files:**
- Create: `util/publications.go`, `util/publications_test.go`

**Interfaces:**
- Consumes: `util.HttpClient` (`util/xls.go`), `driver.Conn` (clickhouse-go v2).
- Produces (использует T4):
  - `type Publication struct { Source, Name, Fingerprint, Url string; FirstSeen time.Time; Points uint64 }`
  - `const PublicationsTable = "source_publications"`; `const PublicationsDdl = "CREATE TABLE IF NOT EXISTS source_publications (…)"` (литерал по спеке §3.4)
  - `func EnsurePublications(ctx context.Context, conn driver.Conn) error`
  - `func HeadFingerprint(url string) (fingerprint string, err error)` — HEAD через `util.HttpClient`, `fingerprint = md5hex(basename(url) + "|" + ETag + "|" + Last-Modified + "|" + Content-Length)`; отсутствующие заголовки — пустые компоненты; не-2xx или ошибка сети — ошибка
  - `func PublicationKnown(ctx context.Context, conn driver.Conn, source, name, fingerprint string) (points uint64, known bool, err error)` — `SELECT points FROM source_publications FINAL WHERE source=? AND name=? AND fingerprint=? LIMIT 1`
  - `func SavePublication(ctx context.Context, conn driver.Conn, p Publication) error` — одна строка через `PrepareBatch → Append(err!) → Send`

- [ ] **Step 1: Написать падающие тесты отпечатка** (`util/publications_test.go`)

`httptest.NewServer` с обработчиком, отвечающим на `r.Method == http.MethodHead` выбранными заголовками (`ETag`, `Last-Modified`, `Content-Length`); тесты:
- `TestHeadFingerprintStable`: два вызова подряд — одинаковый результат, длина 32 hex;
- `TestHeadFingerprintChangesWithHeaders`: сервер A vs сервер B, отличающиеся ETag → отпечатки разные; так же для `Last-Modified` и `Content-Length` (по одному подтесту);
- `TestHeadFingerprintChangesWithName`: те же заголовки, разные URL basename (`/full.xlsx` vs `/other.xlsx`) → разные;
- `TestHeadFingerprintWithoutETag`: ETag отсутствует — работает (не ошибка) и отличается от варианта с ETag;
- `TestHeadFingerprintNon200`: HEAD отвечает 500 → ошибка.

- [ ] **Step 2: Прогнать — FAIL**

Run: `go test ./util/ -run TestHeadFingerprint -v` → компиляция падает (`HeadFingerprint` не определён).

- [ ] **Step 3: Реализовать `util/publications.go`**

По интерфейсам выше; DDL — точно по спеке §3.4 (`source LowCardinality(String), name LowCardinality(String), fingerprint String, first_seen DateTime, points UInt64, url String`, `ENGINE = ReplacingMergeTree ORDER BY (source, name, fingerprint)`). Для HEAD — `http.NewRequestWithContext`/`http.NewRequest(http.MethodHead, …)` с `util.HttpClient` и случайным UA (как `GetFile`, `util/xls.go:128-152`); md5 — `crypto/md5` (не для секретов — отпечаток версии, допустимо).

- [ ] **Step 4: Прогнать — PASS**

Run: `go test ./util/ -run TestHeadFingerprint -v && go test ./util/ -count=1 && gofmt -l util/` → зелёные, gofmt пусто.

- [ ] **Step 5: Commit**

```bash
git add util/publications.go util/publications_test.go
git commit -m "util: source_publications helpers (HEAD fingerprint, known/save)"
```

---

