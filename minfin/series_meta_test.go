package minfin

import (
	"math"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

type appendedRow struct {
	name  string
	date  time.Time
	value float64
}

type appendRecorder struct {
	driver.Batch
	rows []appendedRow
}

func (r *appendRecorder) Append(v ...any) error {
	r.rows = append(r.rows, appendedRow{name: v[0].(string), date: v[1].(time.Time), value: v[2].(float64)})
	return nil
}

func newBudgetWorkbook(t *testing.T, fields []string) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "месяц"); err != nil {
		t.Fatal(err)
	}
	rows := [][]any{{"", "Показатель", "янв.11 г.", "фев.11 г."}}
	for _, field := range fields {
		rows = append(rows, []any{"", field, "21.3", "23.3"})
	}
	for r, row := range rows {
		for c, value := range row {
			cell, err := excelize.CoordinatesToCellName(c+1, r+1)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.SetCellValue("месяц", cell, value); err != nil {
				t.Fatal(err)
			}
		}
	}
	return f
}

func describedFedbud(t *testing.T, source string, meta []util.SeriesMeta) map[string]bool {
	t.Helper()
	described := make(map[string]bool, len(meta))
	for _, m := range meta {
		if m.Source != source || m.Series == "" || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
			t.Errorf("описание ряда %q (%s) заполнено не полностью", m.Series, source)
		}
		if described[m.Series] {
			t.Errorf("ряд %q (%s) описан дважды", m.Series, source)
		}
		described[m.Series] = true
	}
	return described
}

func checkFedbudSeriesMeta(t *testing.T, source string, fields []string, meta []util.SeriesMeta, importFunc func(*excelize.File, driver.Batch) error) {
	t.Helper()
	described := describedFedbud(t, source, meta)
	for _, field := range fields {
		if !described[field] {
			t.Errorf("ряд %q не описан в series_meta (%s)", field, source)
		}
	}
	rec := &appendRecorder{}
	if err := importFunc(newBudgetWorkbook(t, fields), rec); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, row := range rec.rows {
		if !described[row.name] {
			t.Errorf("ряд %q из разбора не описан в series_meta (%s)", row.name, source)
		}
		seen[row.name] = true
	}
	for name := range described {
		if !seen[name] {
			t.Errorf("описанный ряд %q (%s) не встретился при разборе", name, source)
		}
	}
}

func TestFedbudMesSeriesMetaCoversParsedRows(t *testing.T) {
	checkFedbudSeriesMeta(t, minfinSource, fedbudMesFields, fedbudMesSeriesMeta, fedBudImport)
}

func TestFedbudMesyatsSeriesMetaCoversParsedRows(t *testing.T) {
	checkFedbudSeriesMeta(t, minfinMesyatsSource, fedbudMesyatsFields, fedbudMesyatsSeriesMeta, fedbudMesyatsImport)
}

func TestFedBudImportDifferencesCumulativeValues(t *testing.T) {
	rec := &appendRecorder{}
	if err := fedBudImport(newBudgetWorkbook(t, fedbudMesFields), rec); err != nil {
		t.Fatal(err)
	}
	jan := time.Date(2011, time.January, 1, 0, 0, 0, 0, time.UTC)
	feb := time.Date(2011, time.February, 1, 0, 0, 0, 0, time.UTC)
	for _, row := range rec.rows {
		want := 0.0
		switch row.date {
		case jan:
			want = 21.3
		case feb:
			want = 2
		default:
			t.Fatalf("неожиданная дата %s", row.date)
		}
		if math.Abs(row.value-want) > 1e-6 {
			t.Errorf("%s за %s = %v, want %v", row.name, row.date.Format("2006-01"), row.value, want)
		}
	}
}
