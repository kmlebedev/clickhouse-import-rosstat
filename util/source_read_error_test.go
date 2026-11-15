package util

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

func TestXLSXReadErrorBoundary(t *testing.T) {
	importer := ClickHouseImport{TableName: "test", DataUrl: "source"}
	_, err := importer.importXlsBody(context.Background(), []byte("<html>not XLSX</html>"), nil)
	var sourceErr *SourceReadError
	if !errors.As(err, &sourceErr) || sourceErr.URL != "source" || errors.Unwrap(sourceErr) == nil {
		t.Fatalf("not a source read error: %v", err)
	}
	book := excelize.NewFile()
	defer func() { _ = book.Close() }()
	buf, err := book.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("semantic error")
	importer.ImportFunc = func(*excelize.File, driver.Batch) error { return want }
	_, err = importer.importXlsBody(context.Background(), buf.Bytes(), &abortFakeConn{batch: &abortFakeBatch{}})
	if !errors.Is(err, want) || errors.As(err, &sourceErr) {
		t.Fatalf("parser error classified as source: %v", err)
	}
}

func TestPublicationAlwaysReadKnownVersion(t *testing.T) {
	server := publicationHeadServer(t, publicationCbrHeaders)
	conn := &publicationFakeConn{rows: []driver.Row{
		&publicationFakeRow{points: 140}, &publicationFakeRow{points: 140}, &publicationFakeRow{points: 141},
	}}
	called := false
	importer := ClickHouseImport{TableName: "salary", DataUrl: server.URL + "/salary.xlsx", PublicationCountPoints: "SELECT count() FROM salary FINAL", PublicationAlwaysRead: true,
		CrawFunc: func(string, driver.Conn) (int64, error) { called = true; return 1, nil },
	}
	count, err := importer.Import(context.Background(), conn)
	if err != nil || !called || count != 1 || len(conn.batches) != 1 {
		t.Fatalf("known version skipped read/registration: count=%d called=%v batches=%d err=%v", count, called, len(conn.batches), err)
	}
}
