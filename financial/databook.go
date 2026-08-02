package financial

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/golang/glog"
	"github.com/xuri/excelize/v2"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

type DataBook interface {
	Import(conn *sql.DB, sheetUrl string, date time.Time, standard string, double bool) error
}

//	type FinDataBook interface {
//		Name() string
//		Import(ctx context.Context, conn driver.Conn) (count int64, err error)
//	}
type FinDataBook struct {
	name            string
	createTable     string
	insertRow       string
	dataBookPath    string
	pdfPaths        map[string]int
	tables          map[string][]string
	tableColNum     int
	pageImportFunc  func(textPath string, sourceURL string, page int) ([]MetricRecord, error)
	tableImportFunc func(f *FinDataBook, xlsx *excelize.File, batch driver.Batch) (count int64, err error)
}

func (f *FinDataBook) Name() string {
	return f.name
}

func finDataBookTableImport(f *FinDataBook, xlsx *excelize.File, batch driver.Batch) (count int64, err error) {
	for sheet, tables := range f.tables {
		fmt.Printf("Import sheet %s\n", sheet)
		rows, err := xlsx.GetRows(sheet)
		if err != nil {
			return 0, err
		}
		var table string
		var dateRowIdx int
		for i, row := range rows {
			if len(row) == 0 || row[0] == "" {
				table = ""
				continue
			}
			if slices.Contains(tables, row[f.tableColNum]) {
				table = row[f.tableColNum]
				dateRowIdx = i
				fmt.Printf("Found table %s\n", table)
				continue
			}
			if table == "" {
				continue
			}
			tableStartNum := f.tableColNum + 1
			for j, colCell := range row[tableStartNum:] {
				value, err := strconv.ParseFloat(strings.Trim(strings.ReplaceAll(colCell, " ", ""), "()"), 32)
				if err != nil {
					return count, err
				}
				if strings.HasPrefix(colCell, "(") && strings.HasSuffix(colCell, ")") {
					value = value * -1
				}
				fmt.Printf("sheet %s, table %s, name %s , date %s, value %f\n",
					sheet, table, row[f.tableColNum], rows[dateRowIdx][j+tableStartNum], value)
				if err = batch.Append(sheet, table, row[f.tableColNum], fmt.Sprintf("%s-01-01", rows[dateRowIdx][j+tableStartNum]), value); err != nil {
					return count, err
				}
				count++
			}
		}
	}
	return 0, err
}

func (f *FinDataBook) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, fmt.Sprintf(f.createTable, f.name)); err != nil {
		return 0, err
	}
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", f.name))
	if err != nil {
		return count, err
	}
	if len(f.dataBookPath) > 0 {
		xlsx, err := excelize.OpenFile(f.dataBookPath)
		if err != nil {
			return 0, err
		}
		if f.tableImportFunc == nil {
			f.tableImportFunc = finDataBookTableImport
		}
		if count, err = f.tableImportFunc(f, xlsx, batch); err != nil {
			return count, err
		}
	} else if len(f.pdfPaths) > 0 {
		for pdfUrl, page := range f.pdfPaths {
			tempDir, err := os.MkdirTemp("", fmt.Sprintf("%s-*", f.name))
			if err != nil {
				return count, fmt.Errorf("create temp dir: %v", err)
			}
			defer os.RemoveAll(tempDir)

			pdfPath := filepath.Join(tempDir, fmt.Sprintf("report%d.pdf", page))
			textPath := filepath.Join(tempDir, fmt.Sprintf("page.txt%d", page))

			if err := downloadFile(ctx, pdfUrl, pdfPath); err != nil {
				return count, fmt.Errorf("create temp dir: %v", err)
			}

			if err := extractPage(ctx, pdfPath, textPath, page); err != nil {
				return count, fmt.Errorf("create temp dir: %v", err)
			}
			records, err := f.pageImportFunc(textPath, pdfUrl, page)
			if err != nil {
				glog.Fatalf("parse KPI page: %v", err)
			}

			if len(records) == 0 {
				glog.Fatalf("no KPI records found on page %d", page)
			}
			loadedAt := time.Now()
			for _, record := range records {
				//fmt.Printf("record: %+v\n", record)
				if err = batch.Append(
					record.Company,
					record.Metric,
					record.Period,
					record.PeriodType,
					record.Value,
					record.Unit,
					record.SourceURL,
					record.SourcePage,
					loadedAt); err != nil {
					return count, err
				}
			}
		}
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

func downloadFile(ctx context.Context, url, destination string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	request.Header.Set(
		"User-Agent",
		"Mozilla/5.0 PolyusFinancialParser/1.0",
	)
	request.Header.Set("Accept", "application/pdf,*/*")

	client := &http.Client{
		Timeout: 90 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("too many redirects")
			}
			return nil
		},
	}

	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("HTTP request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))

		return fmt.Errorf(
			"unexpected HTTP status %s: %s",
			response.Status,
			strings.TrimSpace(string(body)),
		)
	}

	contentType := response.Header.Get("Content-Type")
	if contentType != "" &&
		!strings.Contains(contentType, "application/pdf") &&
		!strings.Contains(contentType, "application/octet-stream") {
		return fmt.Errorf("unexpected Content-Type: %s", contentType)
	}

	output, err := os.Create(destination)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	defer output.Close()

	written, err := io.Copy(output, response.Body)
	if err != nil {
		return fmt.Errorf("write destination: %w", err)
	}

	if written < 1024 {
		return fmt.Errorf("downloaded file is suspiciously small: %d bytes", written)
	}

	if err := validatePDFHeader(destination); err != nil {
		return err
	}

	fmt.Fprintf(
		os.Stderr,
		"downloaded %d bytes to %s\n",
		written,
		destination,
	)

	return nil
}

func validatePDFHeader(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open downloaded file: %w", err)
	}
	defer file.Close()

	header := make([]byte, 5)

	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("read PDF header: %w", err)
	}

	if string(header) != "%PDF-" {
		return fmt.Errorf(
			"downloaded file is not a PDF, header=%q",
			string(header),
		)
	}

	return nil
}

func extractPage(
	ctx context.Context,
	pdfPath string,
	textPath string,
	page int,
) error {
	if page < 1 {
		return errors.New("page must be greater than zero")
	}

	command := exec.CommandContext(
		ctx,
		"pdftotext",
		"-f", strconv.Itoa(page),
		"-l", strconv.Itoa(page),
		"-layout",
		"-nopgbrk",
		"-enc", "UTF-8",
		pdfPath,
		textPath,
	)

	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"pdftotext failed: %w: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}

	info, err := os.Stat(textPath)
	if err != nil {
		return fmt.Errorf("stat extracted text: %w", err)
	}

	if info.Size() == 0 {
		return errors.New("pdftotext produced an empty file")
	}

	return nil
}
