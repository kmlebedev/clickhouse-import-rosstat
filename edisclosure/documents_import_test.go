package edisclosure

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
)

// Файл — офлайн-тесты Import: подменяются сеть (getFileBytes) и конфигурация
// архива (archiveFromEnv), а соединение с ClickHouse — фейковый driver.Conn.
// Ни сети, ни ClickHouse тесты не касаются.

// fakeExisting — существующая строка edisclosure_documents: ключ идентичности
// (ticker, section, fileid) и метаданные гейта. Ключ хранится отдельно, потому
// что existingDoc его не содержит, а loadExisting собирает docKey из колонок.
type fakeExisting struct {
	ticker, section, fileid string
	existingDoc
}

// fakeConn — фейковое соединение: Exec копит тексты запросов (DDL, витрина),
// Query отдаёт существующие строки, PrepareBatch — записывающий батч. QueryRow
// нужен витрине: util.CreateView спрашивает count() таблиц, прежде чем создавать
// view. Встроенные интерфейсы driver.Conn закрывают неиспользуемые методы.
type fakeConn struct {
	driver.Conn
	execs    []string
	existing []fakeExisting
	batch    *appendRecorder
}

func (c *fakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return nil
}

func (c *fakeConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return &fakeRows{rows: c.existing}, nil
}

// QueryRow отвечает витрине: одна таблица в базе — значит view можно создавать.
func (c *fakeConn) QueryRow(_ context.Context, _ string, _ ...any) driver.Row {
	return fakeRow{}
}

func (c *fakeConn) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.batch = &appendRecorder{}
	return c.batch, nil
}

// fakeRow — строка ответа count(): util.CreateView ждёт одно число.
type fakeRow struct {
	driver.Row
}

func (fakeRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return fmt.Errorf("fakeRow.Scan: приёмников %d, want 1", len(dest))
	}
	count, ok := dest[0].(*uint64)
	if !ok {
		return fmt.Errorf("fakeRow.Scan: приёмник %T, want *uint64", dest[0])
	}
	*count = 1
	return nil
}

func (fakeRow) Err() error { return nil }

// fakeRows — строки SELECT loadExisting: порядок колонок запроса —
// ticker, section, fileid, doc_type, period, publication_date, url, sha256,
// archive_key, first_seen.
type fakeRows struct {
	driver.Rows
	rows []fakeExisting
	pos  int
	err  error
}

func (r *fakeRows) Next() bool {
	if r.pos >= len(r.rows) {
		return false
	}
	r.pos++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.pos == 0 || r.pos > len(r.rows) {
		return fmt.Errorf("fakeRows.Scan: Next не вызывался")
	}
	if len(dest) != 10 {
		return fmt.Errorf("fakeRows.Scan: приёмников %d, want 10", len(dest))
	}
	row := r.rows[r.pos-1]
	for i, value := range []string{
		row.ticker, row.section, row.fileid,
		row.DocType, row.Period,
	} {
		if err := scanString(dest[i], value); err != nil {
			return err
		}
	}
	if err := scanNullTime(dest[5], row.PublicationDate); err != nil {
		return err
	}
	for i, value := range []string{
		row.URL, row.SHA256, row.ArchiveKey,
	} {
		if err := scanString(dest[6+i], value); err != nil {
			return err
		}
	}
	firstSeen, ok := dest[9].(*time.Time)
	if !ok {
		return fmt.Errorf("fakeRows.Scan: приёмник first_seen %T, want *time.Time", dest[9])
	}
	*firstSeen = row.FirstSeen
	return nil
}

func (r *fakeRows) Err() error   { return r.err }
func (r *fakeRows) Close() error { return nil }

// scanString — приёмник строковой колонки.
func scanString(dest any, value string) error {
	p, ok := dest.(*string)
	if !ok {
		return fmt.Errorf("fakeRows.Scan: приёмник %T, want *string", dest)
	}
	*p = value
	return nil
}

// scanNullTime — приёмник Nullable(Date): нулевая дата приходит как NULL.
func scanNullTime(dest any, value time.Time) error {
	p, ok := dest.(*sql.NullTime)
	if !ok {
		return fmt.Errorf("fakeRows.Scan: приёмник publication_date %T, want *sql.NullTime", dest)
	}
	if value.IsZero() {
		*p = sql.NullTime{}
		return nil
	}
	*p = sql.NullTime{Time: value, Valid: true}
	return nil
}

// appendRecorder — батч вставки: копит строки и помнит, чем закончился прогон.
type appendRecorder struct {
	driver.Batch
	rows    [][]any
	sent    bool
	aborted bool
}

func (b *appendRecorder) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}

func (b *appendRecorder) Send() error {
	b.sent = true
	return nil
}

func (b *appendRecorder) Abort() error {
	b.aborted = true
	return nil
}

func (b *appendRecorder) Rows() int { return len(b.rows) }

// TestImportOfflineWritesRowsAndArchive — полный офлайн-прогон: DDL, витрина,
// гейт по пустой таблице, шесть скачиваний, шесть строк батча и шесть версий в
// локальном архиве с реестром (спека §5.3, §5.5).
func TestImportOfflineWritesRowsAndArchive(t *testing.T) {
	t.Setenv(envInputDir, "testdata")
	t.Setenv(archive.EnvBackend, "local")
	archiveDir := t.TempDir()
	t.Setenv(archive.EnvLocalDir, archiveDir)
	t.Setenv(envRecheck, "")

	stubGetFileBytes(t, func(url string) ([]byte, error) {
		return []byte("PK\x03\x04" + url), nil
	})

	conn := &fakeConn{}
	n, err := (documentsImporter{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if n != 6 || len(conn.batch.rows) != 6 {
		t.Fatalf("n=%d rows=%d, want 6 и 6", n, len(conn.batch.rows))
	}
	if !conn.batch.sent || conn.batch.aborted {
		t.Fatalf("батч: sent=%v aborted=%v", conn.batch.sent, conn.batch.aborted)
	}
	if len(conn.execs) == 0 || conn.execs[0] != edisclosureDdl {
		t.Fatalf("первый Exec — не DDL: %q", conn.execs)
	}
	if !executed(conn, "CREATE OR REPLACE VIEW v_edisclosure_documents") {
		t.Error("витрина не создана")
	}
	if !executed(conn, "ALTER TABLE v_edisclosure_documents MODIFY COMMENT") {
		t.Error("комментарий витрины не записан")
	}

	store := archive.NewLocal(archiveDir)
	entries, err := archive.LoadEntries(context.Background(), store)
	if err != nil {
		t.Fatalf("реестр архива: %v", err)
	}
	if len(entries) != 6 {
		t.Fatalf("строк реестра %d, want 6", len(entries))
	}
	objects, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("объекты архива: %v", err)
	}
	if len(objects) != 6 {
		t.Fatalf("объектов архива %d, want 6: %v", len(objects), objects)
	}
	for _, row := range conn.batch.rows {
		key, ok := row[11].(string)
		if !ok || key == "" {
			t.Fatalf("archive_key строки = %#v, want непустой ключ", row[11])
		}
		body, err := store.Get(context.Background(), key)
		if err != nil {
			t.Errorf("объект %q не читается: %v", key, err)
			continue
		}
		if len(body) == 0 {
			t.Errorf("объект %q пуст", key)
		}
	}
	for _, e := range entries {
		if e.Source != edisclosureArchiveSource {
			t.Errorf("source реестра %q, want %q", e.Source, edisclosureArchiveSource)
		}
		if e.Fingerprint != e.SHA256 {
			t.Errorf("fingerprint %q != sha256 %q", e.Fingerprint, e.SHA256)
		}
		if e.Size <= 0 || e.URL == "" || e.Key == "" || e.FirstSeen.IsZero() {
			t.Errorf("неполная строка реестра: %+v", e)
		}
	}
}

// stubGetFileBytes подменяет скачивание на время теста и возвращает прежнюю
// функцию после него.
func stubGetFileBytes(t *testing.T, fn func(string) ([]byte, error)) {
	t.Helper()
	old := getFileBytes
	getFileBytes = fn
	t.Cleanup(func() { getFileBytes = old })
}

// stubArchiveFromEnv подменяет конфигурацию архива на время теста.
func stubArchiveFromEnv(t *testing.T, fn func(*http.Client) (archive.Store, error)) {
	t.Helper()
	old := archiveFromEnv
	archiveFromEnv = fn
	t.Cleanup(func() { archiveFromEnv = old })
}

// executed — выполнялся ли Exec, содержащий подстроку.
func executed(conn *fakeConn, substr string) bool {
	for _, query := range conn.execs {
		if strings.Contains(query, substr) {
			return true
		}
	}
	return false
}

// fixtureFirstSeen — дата первой встречи в фикстурах: проверяется, что повторная
// запись сохраняет её, а не ставит время текущего прогона.
var fixtureFirstSeen = time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)

// fixtureDocs — документы снапшота testdata: тесты гейта строят существующие
// строки из того же входа, чтобы метаданные карточки совпадали дословно.
func fixtureDocs(t *testing.T) []doc {
	t.Helper()
	_, docs, err := readInput("testdata")
	if err != nil {
		t.Fatalf("readInput(testdata): %v", err)
	}
	if len(docs) != 6 {
		t.Fatalf("в фикстуре %d документов, want 6", len(docs))
	}
	return docs
}

// existingRows — существующие строки таблицы с метаданными фикстуры: гейт
// должен считать их неизменившимися, если не задан RECHECK и версия в архиве.
func existingRows(docs []doc, sha, archiveKey string) []fakeExisting {
	rows := make([]fakeExisting, 0, len(docs))
	for _, d := range docs {
		rows = append(rows, fakeExisting{
			ticker: d.Ticker, section: d.Section, fileid: d.FileID,
			existingDoc: existingDoc{
				DocType:         d.DocType,
				Period:          d.Period,
				PublicationDate: d.PublicationDate,
				URL:             d.URL,
				SHA256:          sha,
				ArchiveKey:      archiveKey,
				FirstSeen:       fixtureFirstSeen,
			},
		})
	}
	return rows
}

// errStore — хранилище архива, у которого заданная операция падает: так
// проверяются ветки «сбой Put» и «сбой чтения реестра».
type errStore struct {
	putErr    error
	getErr    error
	exists    bool
	existsErr error
}

func (errStore) Name() string { return "fake" }

func (s errStore) Put(context.Context, string, []byte) error { return s.putErr }

func (s errStore) Get(context.Context, string) ([]byte, error) { return nil, s.getErr }

func (s errStore) Exists(context.Context, string) (bool, error) {
	return s.exists, s.existsErr
}

// TestImportNoopMakesNoCalls — второй прогон того же снапшота: все строки есть,
// метаданные совпали, версия в архиве — ни скачивания, ни батча, ни Append
// (спека §5.3: 0 скачиваний, 0 записей, лог no-op). Витрина при этом
// публикуется: её сбой не должен застревать за гейтом.
func TestImportNoopMakesNoCalls(t *testing.T) {
	t.Setenv(envInputDir, "testdata")
	t.Setenv(archive.EnvBackend, "")
	t.Setenv(envRecheck, "")

	docs := fixtureDocs(t)
	conn := &fakeConn{existing: existingRows(docs, "sha-old", "edisclosure_documents/sha-old_x.zip")}
	stubGetFileBytes(t, func(string) ([]byte, error) {
		t.Fatal("no-op прогон не должен скачивать документы")
		return nil, nil
	})

	n, err := (documentsImporter{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if n != 0 {
		t.Fatalf("Import вернул %d, want 0", n)
	}
	if conn.batch != nil {
		t.Fatal("no-op прогон открыл батч")
	}
	if len(conn.execs) == 0 || conn.execs[0] != edisclosureDdl {
		t.Fatalf("первый Exec — не DDL: %q", conn.execs)
	}
	if !executed(conn, "CREATE OR REPLACE VIEW v_edisclosure_documents") {
		t.Error("витрина не создана на no-op прогоне")
	}
}

// TestImportRecheckRewritesChangedHash — EDISCLOSURE_RECHECK=1 скачивает всё и
// переписывает строки, у которых сменился sha256, сохраняя first_seen.
func TestImportRecheckRewritesChangedHash(t *testing.T) {
	t.Setenv(envInputDir, "testdata")
	t.Setenv(archive.EnvBackend, "")
	t.Setenv(envRecheck, "1")

	docs := fixtureDocs(t)
	conn := &fakeConn{existing: existingRows(docs, "old-sha", "")}
	stubGetFileBytes(t, func(url string) ([]byte, error) {
		return []byte("PK\x03\x04" + url), nil
	})

	n, err := (documentsImporter{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if n != 6 || len(conn.batch.rows) != 6 {
		t.Fatalf("n=%d rows=%d, want 6 и 6", n, len(conn.batch.rows))
	}
	for _, row := range conn.batch.rows {
		sha, ok := row[10].(string)
		if !ok || len(sha) != 64 || sha == "old-sha" {
			t.Fatalf("sha256 строки = %#v, want новый 64-символьный хеш", row[10])
		}
		firstSeen, ok := row[12].(time.Time)
		if !ok || !firstSeen.Equal(fixtureFirstSeen) {
			t.Fatalf("first_seen = %#v, want %v", row[12], fixtureFirstSeen)
		}
	}
}

// TestImportArchiveDisabledWritesEmptyKey — без ARCHIVE_BACKEND строки пишутся
// с пустым archive_key (спека §5.5), объектов в хранилище не появляется.
func TestImportArchiveDisabledWritesEmptyKey(t *testing.T) {
	t.Setenv(envInputDir, "testdata")
	t.Setenv(archive.EnvBackend, "")
	t.Setenv(envRecheck, "")

	stubGetFileBytes(t, func(url string) ([]byte, error) {
		return []byte("PK\x03\x04" + url), nil
	})
	conn := &fakeConn{}
	n, err := (documentsImporter{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if n != 6 || len(conn.batch.rows) != 6 {
		t.Fatalf("n=%d rows=%d, want 6 и 6", n, len(conn.batch.rows))
	}
	for _, row := range conn.batch.rows {
		if key, ok := row[11].(string); !ok || key != "" {
			t.Fatalf("archive_key = %#v, want пустую строку", row[11])
		}
	}
}

// TestImportMissingInputHonoursAllowMissing — отсутствие каталога входа: без
// флага прогон падает до БД, с EDISCLOSURE_ALLOW_MISSING=1 — 0 и nil.
func TestImportMissingInputHonoursAllowMissing(t *testing.T) {
	empty := t.TempDir()
	t.Setenv(archive.EnvBackend, "")
	t.Setenv(envRecheck, "")

	t.Run("без флага ошибка", func(t *testing.T) {
		t.Setenv(envInputDir, empty)
		t.Setenv(envAllowMissing, "")
		conn := &fakeConn{}
		n, err := (documentsImporter{}).Import(context.Background(), conn)
		if err == nil {
			t.Fatal("Import без снапшота вернул nil, ожидалась ошибка")
		}
		if n != 0 {
			t.Fatalf("Import вернул %d, want 0", n)
		}
		if len(conn.execs) != 0 {
			t.Fatalf("до ошибки дошли до БД: %q", conn.execs)
		}
	})

	t.Run("с флагом пропуск", func(t *testing.T) {
		t.Setenv(envInputDir, empty)
		t.Setenv(envAllowMissing, "1")
		conn := &fakeConn{}
		n, err := (documentsImporter{}).Import(context.Background(), conn)
		if err != nil {
			t.Fatalf("Import: %v", err)
		}
		if n != 0 || conn.batch != nil {
			t.Fatalf("n=%d batch=%v, want 0 и nil", n, conn.batch)
		}
		if len(conn.execs) != 0 {
			t.Fatalf("пропущенный прогон ходил в БД: %q", conn.execs)
		}
	})
}

// TestImportAbortsBatchOnDownloadError — сбой скачивания откатывает батч:
// частичных строк в таблице быть не должно.
func TestImportAbortsBatchOnDownloadError(t *testing.T) {
	t.Setenv(envInputDir, "testdata")
	t.Setenv(archive.EnvBackend, "")
	t.Setenv(envRecheck, "")

	stubGetFileBytes(t, func(string) ([]byte, error) {
		return nil, errors.New("источник недоступен")
	})
	conn := &fakeConn{}
	n, err := (documentsImporter{}).Import(context.Background(), conn)
	if err == nil {
		t.Fatal("Import вернул nil, ожидалась ошибка скачивания")
	}
	if n != 0 {
		t.Fatalf("Import вернул %d, want 0", n)
	}
	if conn.batch == nil {
		t.Fatal("батч не был открыт")
	}
	if !conn.batch.aborted || conn.batch.sent || len(conn.batch.rows) != 0 {
		t.Fatalf("батч: aborted=%v sent=%v rows=%d", conn.batch.aborted, conn.batch.sent, len(conn.batch.rows))
	}
	if !strings.Contains(err.Error(), "скачивание") {
		t.Errorf("ошибка %v не называет скачивание", err)
	}
}

// TestImportAbortsBatchOnArchiveError — при включённом архиве сбой Put — ошибка
// прогона: строка не должна объявить archive_key без объекта в хранилище.
func TestImportAbortsBatchOnArchiveError(t *testing.T) {
	t.Setenv(envInputDir, "testdata")
	t.Setenv(archive.EnvBackend, "local")
	t.Setenv(archive.EnvLocalDir, t.TempDir())
	t.Setenv(envRecheck, "")

	stubGetFileBytes(t, func(url string) ([]byte, error) {
		return []byte("PK\x03\x04" + url), nil
	})
	stubArchiveFromEnv(t, func(*http.Client) (archive.Store, error) {
		return errStore{putErr: errors.New("диск недоступен")}, nil
	})
	conn := &fakeConn{}
	n, err := (documentsImporter{}).Import(context.Background(), conn)
	if err == nil {
		t.Fatal("Import вернул nil, ожидалась ошибка архива")
	}
	if n != 0 {
		t.Fatalf("Import вернул %d, want 0", n)
	}
	if conn.batch == nil {
		t.Fatal("батч не был открыт")
	}
	if !conn.batch.aborted || conn.batch.sent || len(conn.batch.rows) != 0 {
		t.Fatalf("батч: aborted=%v sent=%v rows=%d", conn.batch.aborted, conn.batch.sent, len(conn.batch.rows))
	}
	if !strings.Contains(err.Error(), "архив") {
		t.Errorf("ошибка %v не называет архив", err)
	}
}

// TestImportAbortsBatchOnRegistryError — Put объекта успешен, но реестр версий
// не читается: Ensure падает, батч откатывается, строка не пишется.
func TestImportAbortsBatchOnRegistryError(t *testing.T) {
	t.Setenv(envInputDir, "testdata")
	t.Setenv(archive.EnvBackend, "local")
	t.Setenv(archive.EnvLocalDir, t.TempDir())
	t.Setenv(envRecheck, "")

	stubGetFileBytes(t, func(url string) ([]byte, error) {
		return []byte("PK\x03\x04" + url), nil
	})
	stubArchiveFromEnv(t, func(*http.Client) (archive.Store, error) {
		// Exists(registry.jsonl) = true, а Get реестра падает — Ensure не может
		// прочитать состояние и обязан вернуть ошибку.
		return errStore{exists: true, getErr: errors.New("реестр недоступен")}, nil
	})
	conn := &fakeConn{}
	n, err := (documentsImporter{}).Import(context.Background(), conn)
	if err == nil {
		t.Fatal("Import вернул nil, ожидалась ошибка реестра архива")
	}
	if n != 0 {
		t.Fatalf("Import вернул %d, want 0", n)
	}
	if conn.batch == nil {
		t.Fatal("батч не был открыт")
	}
	if !conn.batch.aborted || conn.batch.sent || len(conn.batch.rows) != 0 {
		t.Fatalf("батч: aborted=%v sent=%v rows=%d", conn.batch.aborted, conn.batch.sent, len(conn.batch.rows))
	}
	if !strings.Contains(err.Error(), "реестр архива") {
		t.Errorf("ошибка %v не называет реестр архива", err)
	}
}
