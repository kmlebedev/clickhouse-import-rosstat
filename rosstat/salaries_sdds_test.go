package rosstat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

func openSalarySDDS(t *testing.T) *excelize.File {
	t.Helper()
	book, err := excelize.OpenFile("testdata/salaries_sdds.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := book.Close(); err != nil {
			t.Error(err)
		}
	})
	return book
}

func TestSalarySDDSFixtureAndMissingOnly(t *testing.T) {
	book := openSalarySDDS(t)
	points, err := parseSalarySDDS(book)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 140 || points[0].date.Format("2006-01-02") != "2015-02-01" || points[139].date.Format("2006-01-02") != "2026-09-01" || points[139].value != 103014 {
		t.Fatalf("unexpected points: %v", points)
	}
	// All 139 overlapping months in the existing primary fixture match exactly.
	primary, err := parseSalariesMes(openSalariesMesFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	overlap := 0
	for _, p := range primary {
		known[p.date.Format("2006-01-02")] = true
		for _, q := range points {
			if p.date.Equal(q.date) {
				overlap++
				if float32(p.value) != float32(q.value) {
					t.Fatalf("mismatch %s", p.date)
				}
			}
		}
	}
	if overlap != 139 {
		t.Fatalf("overlap %d", overlap)
	}
	batch := &appendRecorder{}
	if err := importSalarySDDS(book, batch, known); err != nil {
		t.Fatal(err)
	}
	if len(batch.rows) != 1 || batch.rows[0].value != 103014 || batch.rows[0].valueType != "float32" {
		t.Fatalf("missing-only rows: %+v", batch.rows)
	}
	known["2026-09-01"] = true
	batch = &appendRecorder{}
	if err := importSalarySDDS(book, batch, known); err != nil || len(batch.rows) != 0 {
		t.Fatalf("repeat rows=%d err=%v", len(batch.rows), err)
	}
}

func TestSalarySDDSRejectsInvalidBeforeAppend(t *testing.T) {
	for _, tt := range []struct {
		name, cell string
		value      any
	}{
		{"header", "A1", "other"}, {"unit", "B6", "млн человек"}, {"caption", "A6", "Численность рабочей силы"},
		{"duplicate month", "D1", 42005}, {"not first day", "C1", 42006}, {"fractional date", "C1", 42005.5}, {"invalid date", "C1", "NaN"},
		{"nan", "EL6", "NaN"}, {"inf", "EL6", "Inf"}, {"overflow", "EL6", "1e39"}, {"negative", "EL6", -1}, {"zero", "EL6", 0}, {"interior blank", "D6", ""},
		{"extra salary", "EN6", 123},
	} {
		t.Run(tt.name, func(t *testing.T) {
			book := openSalarySDDS(t)
			if err := book.SetCellValue(salarySDDSSheet, tt.cell, tt.value); err != nil {
				t.Fatal(err)
			}
			batch := &appendRecorder{}
			if err := importSalarySDDS(book, batch, nil); err == nil {
				t.Fatal("accepted invalid workbook")
			}
			if len(batch.rows) != 0 {
				t.Fatal("partial append before validation")
			}
		})
	}
}

func TestSalarySDDSAppendError(t *testing.T) {
	want := errors.New("append failed")
	err := importSalarySDDS(openSalarySDDS(t), &appendRecorder{appendErr: want}, nil)
	if !errors.Is(err, want) {
		t.Fatalf("lost append error: %v", err)
	}
}

func TestSalarySDDSFallbackBoundary(t *testing.T) {
	read := &util.SourceReadError{URL: "source", Err: errors.New("invalid zip")}
	for _, tt := range []struct {
		err           error
		restore, want bool
	}{
		{nil, false, false}, {read, false, true}, {fmt.Errorf("wrapped: %w", read), false, true},
		{read, true, false}, {errors.New("database failure"), false, false}, {errors.New("semantic failure"), false, false},
	} {
		if got := salarySDDSFallbackAllowed(tt.err, tt.restore); got != tt.want {
			t.Fatalf("%v restore=%v: %v", tt.err, tt.restore, got)
		}
	}
	fallback := salariesMesImporter.salarySDDSImport()
	if !fallback.PublicationAlwaysRead || fallback.PublicationKey() != salarySDDSSource || fallback.DataUrl != salarySDDSURL || fallback.PublicationCountPoints == salariesMesCountPoints {
		t.Fatal("fallback publication identity mixed with primary")
	}
	if salariesMesImporter.PublicationKey() != rosstatSalariesMesTable {
		t.Fatal("mutated primary")
	}
}

func TestSalaryRestoreNeverDiscoversSource(t *testing.T) {
	t.Setenv(util.EnvArchiveAction, util.ArchiveActionRestore)
	_, err := salariesMesImporter.Import(context.Background(), nil)
	if err == nil {
		t.Fatal("expected explicit primary URL requirement")
	}
}

// Assert the test recorder still models the legacy salary batch interface.
var _ driver.Batch = (*appendRecorder)(nil)

type salaryTransport func(*http.Request) (*http.Response, error)

func (f salaryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type salaryTestRows struct {
	driver.Rows
	dates []time.Time
	index int
}

func (r *salaryTestRows) Next() bool {
	if r.index >= len(r.dates) {
		return false
	}
	r.index++
	return true
}
func (r *salaryTestRows) Scan(dest ...any) error {
	*dest[0].(*time.Time) = r.dates[r.index-1]
	return nil
}
func (r *salaryTestRows) Close() error { return nil }
func (r *salaryTestRows) Err() error   { return nil }

type salaryTestConn struct {
	driver.Conn
	batch *appendRecorder
	dates []time.Time
	dbErr error
}

func (c *salaryTestConn) Exec(context.Context, string, ...any) error { return c.dbErr }
func (c *salaryTestConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &salaryTestRows{dates: c.dates}, nil
}
func (c *salaryTestConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return c.batch, nil
}

func TestSalarySourceRouting(t *testing.T) {
	primary, err := os.ReadFile("testdata/salaries_mes.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	fallback, err := os.ReadFile("testdata/salaries_sdds.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name                                        string
		discovery, transport, invalid, semantic, db bool
		fallbackCalls                               int
		wantError                                   bool
	}{
		{name: "primary"}, {name: "discovery failure", discovery: true, fallbackCalls: 1},
		{name: "transport failure", transport: true, fallbackCalls: 1}, {name: "HTML 200", invalid: true, fallbackCalls: 1},
		{name: "semantic error", semantic: true, wantError: true}, {name: "database error", db: true, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			original := util.HttpClient
			t.Cleanup(func() { util.HttpClient = original })
			fallbackCalls := 0
			util.HttpClient = &http.Client{Transport: salaryTransport(func(r *http.Request) (*http.Response, error) {
				status := 200
				body := ""
				switch {
				case r.Method == http.MethodHead:
					status = 405
				case r.URL.String() == salarySDDSURL:
					fallbackCalls++
					body = string(fallback)
				case strings.Contains(r.URL.Path, "labor_market_employment_salaries"):
					if tt.discovery {
						body = "<html>no link</html>"
					} else {
						body = `<a href="/tab1-zpl.xlsx">salary</a>`
					}
				default:
					if tt.transport {
						return nil, errors.New("network failed")
					}
					body = string(primary)
					if tt.invalid {
						body = "<html>WAF</html>"
					}
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})}
			importer := *salariesMesImporter
			importer.AfterImport = nil
			if tt.semantic {
				importer.ImportFunc = func(*excelize.File, driver.Batch) error { return errors.New("semantic") }
			}
			conn := &salaryTestConn{batch: &appendRecorder{}}
			if tt.db {
				conn.dbErr = errors.New("database unavailable")
			}
			_, err := importer.Import(context.Background(), conn)
			if (err != nil) != tt.wantError || fallbackCalls != tt.fallbackCalls {
				t.Fatalf("err=%v fallback calls=%d", err, fallbackCalls)
			}
		})
	}
}

func TestSalarySDDSRepairsHoleWithEqualCount(t *testing.T) {
	book := openSalarySDDS(t)
	points, err := parseSalarySDDS(book)
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, p := range points {
		known[p.date.Format("2006-01-02")] = true
	}
	delete(known, "2016-03-01")
	known["2026-10-01"] = true
	if len(known) != len(points) {
		t.Fatal("test must preserve count")
	}
	batch := &appendRecorder{}
	if err := importSalarySDDS(book, batch, known); err != nil {
		t.Fatal(err)
	}
	if len(batch.rows) != 1 || batch.rows[0].date.Format("2006-01-02") != "2016-03-01" {
		t.Fatalf("hole not repaired: %v", batch.rows)
	}
}
