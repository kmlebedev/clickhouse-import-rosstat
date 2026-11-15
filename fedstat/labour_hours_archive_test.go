package fedstat

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
)

type hoursTestConn struct {
	driver.Conn
	points   [][]any
	versions int
}

func (c *hoursTestConn) Exec(context.Context, string, ...any) error { return nil }
func (c *hoursTestConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &hoursTestRows{values: c.points, index: -1}, nil
}
func (c *hoursTestConn) QueryRow(context.Context, string, ...any) driver.Row {
	return hoursMissingRow{}
}
func (c *hoursTestConn) PrepareBatch(_ context.Context, q string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &hoursTestBatch{conn: c, publication: strings.Contains(q, "source_publications")}, nil
}

type hoursMissingRow struct{ driver.Row }

func (hoursMissingRow) Scan(...any) error { return sql.ErrNoRows }

type hoursTestRows struct {
	driver.Rows
	values [][]any
	index  int
}

func (r *hoursTestRows) Next() bool { r.index++; return r.index < len(r.values) }
func (r *hoursTestRows) Scan(dest ...any) error {
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(r.values[r.index][i]))
	}
	return nil
}
func (r *hoursTestRows) Close() error { return nil }
func (r *hoursTestRows) Err() error   { return nil }

type hoursTestBatch struct {
	driver.Batch
	conn        *hoursTestConn
	publication bool
	values      [][]any
}

func (b *hoursTestBatch) Append(v ...any) error {
	b.values = append(b.values, append([]any(nil), v...))
	return nil
}
func (b *hoursTestBatch) Rows() int { return len(b.values) }
func (b *hoursTestBatch) Send() error {
	if b.publication {
		b.conn.versions += len(b.values)
	} else {
		b.conn.points = append(b.conn.points, b.values...)
	}
	return nil
}
func (b *hoursTestBatch) Abort() error { return nil }

func TestLabourHoursArchiveFailureAndZeroDeltaRecovery(t *testing.T) {
	ctx := context.Background()
	conn := &hoursTestConn{}
	var docs []labourHoursDocument
	var points []labourHoursPoint
	for _, id := range []string{"59749", "59751", "60463"} {
		body := hoursFixture(t, id)
		p, e := parseLabourHours(body, id)
		if e != nil {
			t.Fatal(e)
		}
		docs = append(docs, labourHoursDocument{id, "https://fedstat.ru/indicator/" + id + "/download", body, uint64(len(p))})
		points = append(points, p...)
	}
	s := *fedstatLabourHoursImporter
	after := 0
	s.AfterImport = func(context.Context, driver.Conn) error { after++; return nil }
	dir := t.TempDir()
	bad := filepath.Join(dir, "not-a-directory")
	if e := os.WriteFile(bad, []byte("blocked"), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("ARCHIVE_BACKEND", "local")
	t.Setenv("ARCHIVE_LOCAL_DIR", bad)
	n, e := s.commitDocuments(ctx, conn, docs, points)
	if e == nil || n != 13 || conn.versions != 0 || after != 1 {
		t.Fatalf("archive failure: n=%d err=%v versions=%d after=%d", n, e, conn.versions, after)
	}
	// The data batch already succeeded. Retrying with an empty delta must heal the archive.
	good := filepath.Join(dir, "archive")
	t.Setenv("ARCHIVE_LOCAL_DIR", good)
	n, e = s.commitDocuments(ctx, conn, docs, points)
	if e != nil || n != 0 || conn.versions != 3 || after != 2 {
		t.Fatalf("recovery: n=%d err=%v versions=%d after=%d", n, e, conn.versions, after)
	}
	entries, e := archive.LoadEntries(ctx, archive.NewLocal(good))
	if e != nil {
		t.Fatal(e)
	}
	if len(entries) != 3 {
		t.Fatalf("archive entries=%d", len(entries))
	}
	for _, entry := range entries {
		b, e := os.ReadFile(filepath.Join(good, entry.Key))
		if e != nil {
			t.Fatal(e)
		}
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != entry.SHA256 {
			t.Fatal("archive SHA mismatch")
		}
	}
}
func TestLabourHoursRestoreFailsBeforeNetwork(t *testing.T) {
	t.Setenv("ARCHIVE_ACTION", "restore")
	if _, err := fedstatLabourHoursImporter.Import(context.Background(), nil); err == nil {
		t.Fatal("restore accepted")
	}
}
