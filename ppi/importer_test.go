package ppi

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func testSeries(months int) ppiSeries {
	series := make(ppiSeries, len(ppiSAOrder))
	for _, name := range ppiSAOrder {
		points := make([]monthlyValue, months)
		for i := 0; i < months; i++ {
			points[i] = monthlyValue{
				Date:  time.Date(2021, time.Month(1+i), 1, 0, 0, 0, 0, time.UTC),
				Value: 100 + float64(i),
			}
		}
		series[name] = points
	}
	return series
}

func testSA(series ppiSeries) map[string][]saPoint {
	result := make(map[string][]saPoint, len(series))
	for name, points := range series {
		saPoints := make([]saPoint, len(points))
		for i, point := range points {
			saPoints[i] = saPoint{Date: point.Date, SA: 100 + float64(i)}
		}
		result[name] = saPoints
	}
	return result
}

func TestInputFingerprintStable(t *testing.T) {
	ved := []rawPoint{{Date: month(2013, 1, 31), Code: ppiSATotalCode, Value: 100}}
	product := []rawPoint{
		{Date: month(2021, 1, 31), Code: "01.1.1", Value: 100},
		{Date: month(2021, 2, 28), Code: "01.1.1", Value: 101},
	}
	first := inputFingerprint(ved, product)
	second := inputFingerprint(ved, product)
	if first != second {
		t.Fatalf("отпечаток нестабилен: %s != %s", first, second)
	}

	reordered := []rawPoint{product[1], product[0]}
	if got := inputFingerprint(ved, reordered); got != first {
		t.Fatalf("порядок product влияет на отпечаток: %s != %s", got, first)
	}

	changed := []rawPoint{
		{Date: month(2021, 1, 31), Code: "01.1.1", Value: 100},
		{Date: month(2021, 2, 28), Code: "01.1.1", Value: 102},
	}
	if got := inputFingerprint(ved, changed); got == first {
		t.Fatal("изменение значения не изменило отпечаток")
	}
}

func TestInputFingerprintPins(t *testing.T) {
	ved := []rawPoint{{Date: month(2013, 1, 31), Code: ppiSATotalCode, Value: 100}}
	product := []rawPoint{{Date: month(2021, 1, 31), Code: "01.1.1", Value: 100}}
	pins := [3]string{"x13-a", "mapping-a", "weights-a"}
	base := inputFingerprintWithPins(ved, product, pins)
	if same := inputFingerprintWithPins(ved, product, pins); same != base {
		t.Fatalf("отпечаток нестабилен при тех же пинах: %s != %s", same, base)
	}
	changed := [3]string{"x13-a", "mapping-a", "weights-b"}
	if got := inputFingerprintWithPins(ved, product, changed); got == base {
		t.Fatal("смена пина weights не изменила отпечаток: правка данных без bump версии останется no-op")
	}
}

func TestBuildRowsVersions(t *testing.T) {
	series := testSeries(3)
	published := map[string]sql.NullTime{
		"total": {Time: month(2026, 10, 1), Valid: true},
	}
	rows, err := buildRows(series, testSA(series), published)
	if err != nil {
		t.Fatalf("buildRows: %v", err)
	}
	if len(rows) != 15 {
		t.Fatalf("строк %d, want 15", len(rows))
	}
	for _, row := range rows {
		if row.Method != ppiSAMethod || row.SpecVersion != ppiSASpecVersion || row.TaxonomyVersion != ppiSATaxonomyVersion {
			t.Fatalf("строка без версий: %+v", row)
		}
	}
	if rows[0].MMSA != nil || rows[0].SAAR != nil || rows[0].SAAR3 != nil {
		t.Fatalf("первая точка без NULL-метрик: %+v", rows[0])
	}
	if rows[1].MMSA == nil || rows[1].SAAR == nil || rows[1].SAAR3 != nil {
		t.Fatalf("вторая точка: ожидались mm_sa/saar и NULL saar3: %+v", rows[1])
	}
	if rows[2].MMSA == nil || rows[2].SAAR == nil || rows[2].SAAR3 == nil {
		t.Fatalf("третья точка: ожидались все метрики: %+v", rows[2])
	}
	if !rows[0].PublishedAt.Valid {
		t.Fatal("total: published_at должен быть заполнен")
	}
	if rows[3].PublishedAt.Valid {
		t.Fatal("food: published_at должен быть NULL")
	}
}

func TestBuildRowsDateMismatch(t *testing.T) {
	series := testSeries(2)
	sa := testSA(series)
	sa["food"] = []saPoint{{Date: month(2022, 1, 1), SA: 100}, {Date: month(2022, 2, 1), SA: 101}}
	if _, err := buildRows(series, sa, nil); err == nil {
		t.Fatal("ожидалась ошибка несовпадения дат")
	}
}

func TestPPISASeriesMeta(t *testing.T) {
	meta := ppiSASeriesMeta()
	if len(meta) != len(ppiSAOrder) {
		t.Fatalf("рядов %d, want %d", len(meta), len(ppiSAOrder))
	}
	for i, item := range meta {
		if item.Source != ppiSATable || item.Series != ppiSAOrder[i] {
			t.Errorf("позиция %d: %+v", i, item)
		}
		if item.Title == "" || item.Unit == "" || item.Frequency != "M" || item.Origin == "" || item.Description == "" {
			t.Errorf("%s: неполное описание: %+v", item.Series, item)
		}
	}
}

func TestSchema(t *testing.T) {
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS ppi_sa",
		"date             Date",
		"series           LowCardinality(String)",
		"method           LowCardinality(String)",
		"spec_version     LowCardinality(String)",
		"taxonomy_version LowCardinality(String)",
		"sa               Float64",
		"mm_sa            Nullable(Float64)",
		"saar             Nullable(Float64)",
		"saar3            Nullable(Float64)",
		"published_at     Nullable(Date)",
		"loaded_at        DateTime DEFAULT now()",
		"ReplacingMergeTree(loaded_at)",
		"ORDER BY (date, series, method, spec_version, taxonomy_version)",
	} {
		if !strings.Contains(ppiSADdl, fragment) {
			t.Errorf("DDL не содержит %q", fragment)
		}
	}
	if ppiSACountPoints != "SELECT count() FROM ppi_sa FINAL" {
		t.Fatalf("countPoints = %q", ppiSACountPoints)
	}
}

func TestPublishedQueryUsesMaxOrNull(t *testing.T) {
	if !strings.Contains(ppiSAPublishedQuery, "maxOrNull") {
		t.Fatalf("published_at запрос без maxOrNull: %q", ppiSAPublishedQuery)
	}
}

func TestReadPublishedAt(t *testing.T) {
	conn := &fakeConn{row: fakeRow{value: sql.NullTime{Time: month(2026, 10, 1), Valid: true}}}
	got, err := readPublishedAt(context.Background(), conn, ppiSAVEDSource)
	if err != nil {
		t.Fatalf("readPublishedAt: %v", err)
	}
	if !got.Valid || got.Time.Format("2006-01-02") != "2026-10-01" {
		t.Fatalf("published_at = %+v", got)
	}

	conn = &fakeConn{row: fakeRow{}}
	got, err = readPublishedAt(context.Background(), conn, ppiSAProductSource)
	if err != nil {
		t.Fatalf("readPublishedAt(NULL): %v", err)
	}
	if got.Valid {
		t.Fatalf("NULL published_at прочитан как %+v", got)
	}
}

func TestInsertRows(t *testing.T) {
	mm := 1.0
	batch := &recordingBatch{}
	conn := &fakeConn{batch: batch}
	rows := []ppiSARow{
		{
			Date: month(2021, 1, 1), Series: "total", Method: ppiSAMethod,
			SpecVersion: ppiSASpecVersion, TaxonomyVersion: ppiSATaxonomyVersion,
			SA: 100, MMSA: &mm, SAAR: &mm, SAAR3: nil,
			PublishedAt: sql.NullTime{Time: month(2026, 10, 1), Valid: true},
		},
		{
			Date: month(2021, 2, 1), Series: "food", Method: ppiSAMethod,
			SpecVersion: ppiSASpecVersion, TaxonomyVersion: ppiSATaxonomyVersion,
			SA: 101,
		},
	}
	count, err := insertRows(context.Background(), conn, rows)
	if err != nil {
		t.Fatalf("insertRows: %v", err)
	}
	if count != 2 || !batch.sent || len(batch.rows) != 2 {
		t.Fatalf("count=%d sent=%v rows=%d", count, batch.sent, len(batch.rows))
	}
	first := batch.rows[0]
	if first[6] != 1.0 || first[7] != 1.0 || first[8] != nil {
		t.Fatalf("nullable-метрики первой строки: %v", first[6:9])
	}
	if _, ok := first[9].(time.Time); !ok {
		t.Fatalf("published_at первой строки = %T", first[9])
	}
	second := batch.rows[1]
	if second[6] != nil || second[7] != nil || second[8] != nil || second[9] != nil {
		t.Fatalf("NULL-поля второй строки: %v", second[6:10])
	}
}

// recordingBatch — минимальная подмена driver.Batch: собирает аргументы Append.
type recordingBatch struct {
	rows [][]any
	sent bool
}

func (b *recordingBatch) Abort() error { return nil }
func (b *recordingBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}
func (b *recordingBatch) AppendStruct(any) error        { return errors.New("not supported") }
func (b *recordingBatch) Column(int) driver.BatchColumn { return nil }
func (b *recordingBatch) Flush() error                  { return nil }
func (b *recordingBatch) Send() error                   { b.sent = true; return nil }
func (b *recordingBatch) IsSent() bool                  { return b.sent }
func (b *recordingBatch) Rows() int                     { return len(b.rows) }
func (b *recordingBatch) Columns() []column.Interface   { return nil }

// fakeConn — подмена driver.Conn: нужны только PrepareBatch и QueryRow.
type fakeConn struct {
	driver.Conn
	batch *recordingBatch
	row   driver.Row
}

func (c *fakeConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return c.batch, nil
}

func (c *fakeConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return c.row
}

// fakeRow — подмена driver.Row для readPublishedAt.
type fakeRow struct {
	value sql.NullTime
	err   error
}

func (r fakeRow) Err() error { return r.err }

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	target, ok := dest[0].(*sql.NullTime)
	if !ok {
		return errors.New("fakeRow: ожидался *sql.NullTime")
	}
	*target = r.value
	return nil
}

func (r fakeRow) ScanStruct(any) error { return errors.New("not supported") }
