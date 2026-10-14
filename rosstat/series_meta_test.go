package rosstat

import (
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// appendRecorder — подмена driver.Batch для офлайн-тестов импортёров (§11.6): собирает
// вызовы Append, чтобы тест видел точки без базы. Ошибка записи не глотается: настроенная
// appendErr возвращается из Append — тест проверяет, что импортёр её вернул вызывающему,
// а не продолжил писать дальше.
type appendRecorder struct {
	rows      []appendedRow
	appendErr error
}

// appendedRow — один вызов Append: ряд, дата и значение точки.
type appendedRow struct {
	series string
	date   time.Time
	value  float32
}

var _ driver.Batch = (*appendRecorder)(nil)

func (r *appendRecorder) Append(v ...any) error {
	if r.appendErr != nil {
		return r.appendErr
	}
	if len(v) != 3 {
		return fmt.Errorf("appendRecorder: Append с %d аргументами, ожидалось 3 (ряд, дата, значение)", len(v))
	}
	series, ok := v[0].(string)
	if !ok {
		return fmt.Errorf("appendRecorder: имя ряда %T, ожидалась строка", v[0])
	}
	date, ok := v[1].(time.Time)
	if !ok {
		return fmt.Errorf("appendRecorder: дата %T, ожидалось time.Time", v[1])
	}
	value, ok := v[2].(float32)
	if !ok {
		return fmt.Errorf("appendRecorder: значение %T, ожидалось float32", v[2])
	}
	r.rows = append(r.rows, appendedRow{series: series, date: date, value: value})
	return nil
}

func (r *appendRecorder) Abort() error { return nil }

func (r *appendRecorder) AppendStruct(any) error {
	return fmt.Errorf("appendRecorder: AppendStruct не поддерживается")
}

func (r *appendRecorder) Column(int) driver.BatchColumn { return nil }

func (r *appendRecorder) Columns() []column.Interface { return nil }

func (r *appendRecorder) Flush() error { return nil }

func (r *appendRecorder) Send() error { return nil }

func (r *appendRecorder) IsSent() bool { return false }

func (r *appendRecorder) Rows() int { return len(r.rows) }

func newTestWorkbook(t *testing.T, sheets ...string) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	for i, sheet := range sheets {
		if i == 0 {
			if err := f.SetSheetName("Sheet1", sheet); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := f.NewSheet(sheet); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func setSheetRows(t *testing.T, f *excelize.File, sheet string, rows [][]any) {
	t.Helper()
	for r, row := range rows {
		for c, value := range row {
			cell, err := excelize.CoordinatesToCellName(c+1, r+1)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.SetCellValue(sheet, cell, value); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func describedSeries(t *testing.T, meta []util.SeriesMeta) map[string]bool {
	t.Helper()
	described := make(map[string]bool, len(meta))
	for _, m := range meta {
		if m.Source != rosstatSource || m.Series == "" || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Errorf("описание ряда %q заполнено не полностью", m.Series)
		}
		if described[m.Series] {
			t.Errorf("ряд %q описан дважды", m.Series)
		}
		described[m.Series] = true
	}
	return described
}

func requireParsedDescribed(t *testing.T, parsed []string, described map[string]bool) {
	t.Helper()
	seen := make(map[string]bool, len(parsed))
	for _, name := range parsed {
		if !described[name] {
			t.Errorf("ряд %q не описан в series_meta", name)
		}
		seen[name] = true
	}
	for name := range described {
		if !seen[name] {
			t.Errorf("описанный ряд %q не встретился при разборе", name)
		}
	}
}

func TestIpcWeeksSeriesMetaCoversFixture(t *testing.T) {
	xlsx, err := excelize.OpenFile("testdata/ipc_weeks.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = xlsx.Close() }()
	table, err := parseIpcWeeks(xlsx)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(*table))
	for _, row := range *table {
		names = append(names, row[0])
	}
	described := describedSeries(t, ipcWeeksSeriesMeta())
	for _, name := range names {
		if !described[name] {
			t.Errorf("ряд %q из выгрузки не описан в series_meta", name)
		}
	}
}
