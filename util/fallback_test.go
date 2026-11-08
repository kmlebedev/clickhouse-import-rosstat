package util

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fallbackRows — минимальный driver.Rows для тестов гейта: одна колонка date.
type fallbackRows struct {
	dates []time.Time
	next  int
}

func (r *fallbackRows) Next() bool {
	if r.next >= len(r.dates) {
		return false
	}
	r.next++
	return true
}

func (r *fallbackRows) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("fallbackRows: ожидалась одна колонка")
	}
	date, ok := dest[0].(*time.Time)
	if !ok {
		return errors.New("fallbackRows: ожидался *time.Time")
	}
	*date = r.dates[r.next-1]
	return nil
}

func (r *fallbackRows) ScanStruct(any) error {
	return errors.New("fallbackRows: ScanStruct не поддержан")
}
func (r *fallbackRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *fallbackRows) Totals(...any) error {
	return errors.New("fallbackRows: Totals не поддержан")
}
func (r *fallbackRows) Columns() []string { return []string{"date"} }
func (r *fallbackRows) Close() error      { return nil }
func (r *fallbackRows) Err() error        { return nil }

// fallbackConn — conn только с Query/Exec: DDL и гейт по датам.
type fallbackConn struct {
	driver.Conn
	dates []time.Time
	execs int
}

func (c *fallbackConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &fallbackRows{dates: c.dates}, nil
}

func (c *fallbackConn) Exec(context.Context, string, ...any) error {
	c.execs++
	return nil
}

func TestFallbackMissing(t *testing.T) {
	day := func(d int) time.Time { return time.Date(2026, time.October, d, 0, 0, 0, 0, time.UTC) }
	november := time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC)
	docs := []FallbackDocument{
		{Date: day(19), URL: "old"},
		{Date: day(26), URL: "new-first"},
		{Date: day(26), URL: "new-second"},
		{Date: day(26), URL: "new-first"},
		{Date: november, URL: "november"},
	}
	known := map[string]bool{"2026-10-19": true}

	missing := FallbackMissing(docs, known)
	if len(missing) != 3 {
		t.Fatalf("недостающих %d, ожидалось 3: %+v", len(missing), missing)
	}
	if missing[0].URL != "new-first" || missing[0].Date.Format("2006-01-02") != "2026-10-26" {
		t.Errorf("первый документ %+v, ожидался new-first/2026-10-26", missing[0])
	}
	if missing[1].URL != "new-second" || missing[1].Date.Format("2006-01-02") != "2026-10-26" {
		t.Errorf("второй документ %+v, ожидался new-second/2026-10-26", missing[1])
	}
	if missing[2].URL != "november" || missing[2].Date.Format("2006-01-02") != "2026-11-02" {
		t.Errorf("третий документ %+v, ожидался november/2026-11-02", missing[2])
	}
}

func TestFallbackImportNoopPublishes(t *testing.T) {
	date := time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC)
	published := 0
	f := &Fallback{
		Template: ClickHouseImport{
			TableName: "fallback_table",
			AfterImport: func(context.Context, driver.Conn) error {
				published++
				return nil
			},
		},
		Documents: func(context.Context) ([]FallbackDocument, error) {
			return []FallbackDocument{{Date: date, URL: "https://example.test/week.pdf"}}, nil
		},
		Parse: func(context.Context, FallbackDocument, []byte) ([]FallbackRow, error) {
			t.Fatal("Parse не должен вызываться на no-op")
			return nil, nil
		},
		KnownDatesQuery: "SELECT DISTINCT date FROM fallback_table FINAL",
		Append: func(driver.Batch, FallbackRow) error {
			t.Fatal("Append не должен вызываться на no-op")
			return nil
		},
	}
	conn := &fallbackConn{dates: []time.Time{date}}

	res, err := f.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Fallback.Import: %v", err)
	}
	if res.Documents != 1 || res.Missing != 0 || res.Imported != 0 {
		t.Errorf("результат %+v, ожидались Documents=1, Missing=0, Imported=0", res)
	}
	if !res.Published || published != 1 {
		t.Errorf("Published=%v, AfterImport вызван %d раз; ожидался один вызов", res.Published, published)
	}
	if conn.execs != 0 {
		t.Errorf("DDL выполнена %d раз, ожидалось 0", conn.execs)
	}
}

func TestFallbackImportDocumentsError(t *testing.T) {
	f := &Fallback{
		Template: ClickHouseImport{TableName: "fallback_table"},
		Documents: func(context.Context) ([]FallbackDocument, error) {
			return nil, errors.New("лента недоступна")
		},
		Parse: func(context.Context, FallbackDocument, []byte) ([]FallbackRow, error) {
			return nil, nil
		},
		KnownDatesQuery: "SELECT DISTINCT date FROM fallback_table FINAL",
		Append:          func(driver.Batch, FallbackRow) error { return nil },
	}

	res, err := f.Import(context.Background(), &fallbackConn{})
	if err == nil || !strings.Contains(err.Error(), "лента недоступна") {
		t.Fatalf("ошибка %v, ожидалась «лента недоступна»", err)
	}
	if res.Documents != 0 {
		t.Errorf("Documents=%d, ожидалось 0", res.Documents)
	}
}
