package rosstat

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	salarySDDSURL     = "https://rosstat.gov.ru/storage/mediabank/SDDS_labor_market.xlsx"
	salarySDDSSource  = "rosstat_salaries_mes_sdds"
	salarySDDSSheet   = "Рынок труда"
	salarySDDSCaption = "Среднемесячная номинальная начисленная заработная плата работников организаций 2"
	salarySDDSCount   = "SELECT count() FROM rosstat_salaries_mes FINAL WHERE name = '" + salariesMesSeriesName + "' AND date >= '2015-02-01'"
	salaryKnownDates  = "SELECT date FROM rosstat_salaries_mes FINAL WHERE name = '" + salariesMesSeriesName + "'"
)

func salarySDDSFallbackAllowed(err error, restore bool) bool {
	var readErr *util.SourceReadError
	return !restore && errors.As(err, &readErr)
}

func (s *SalariesMesStat) salarySDDSImport() util.ClickHouseImport {
	fallback := s.ClickHouseImport
	fallback.DataUrl = salarySDDSURL
	fallback.PublicationSource = salarySDDSSource
	fallback.PublicationAlwaysRead = true
	fallback.PublicationCountPoints = salarySDDSCount
	var known map[string]bool
	fallback.BeforeImport = func(ctx context.Context, conn driver.Conn) error {
		var err error
		known, err = util.FallbackKnownDates(ctx, conn, salaryKnownDates)
		return err
	}
	fallback.ImportFunc = func(book *excelize.File, batch driver.Batch) error {
		return importSalarySDDS(book, batch, known)
	}
	return fallback
}

func importSalarySDDS(book *excelize.File, batch driver.Batch, known map[string]bool) error {
	points, err := parseSalarySDDS(book)
	if err != nil {
		return err
	}
	for _, point := range points {
		if known[point.date.Format("2006-01-02")] {
			continue
		}
		if err := batch.Append(salariesMesSeriesName, point.date, float32(point.value)); err != nil {
			return err
		}
	}
	return nil
}

// parseSalarySDDS validates the whole salary row before any Append. Raw cells
// preserve numeric Excel dates independently of locale/display formatting.
func parseSalarySDDS(book *excelize.File) ([]salariesMesPoint, error) {
	rows, err := book.GetRows(salarySDDSSheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("salary SDDS sheet: %w", err)
	}
	if len(rows) == 0 || len(rows[0]) < 3 || strings.TrimSpace(rows[0][0]) != "Категория данных ССРД" || strings.TrimSpace(rows[0][1]) != "Единица измерения" {
		return nil, fmt.Errorf("salary SDDS: unexpected header")
	}
	var salary []string
	for _, row := range rows[1:] {
		if len(row) > 0 && strings.TrimSpace(row[0]) == salarySDDSCaption {
			if salary != nil {
				return nil, fmt.Errorf("salary SDDS: duplicate salary row")
			}
			salary = row
		}
	}
	if len(salary) < 3 || strings.TrimSpace(salary[1]) != "рубли" || len(salary) > len(rows[0]) {
		return nil, fmt.Errorf("salary SDDS: missing salary row, unexpected unit or extra values")
	}
	points := make([]salariesMesPoint, 0, len(rows[0])-2)
	expected := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	blank := false
	for col, cell := range rows[0][2:] {
		serial, err := strconv.ParseFloat(strings.TrimSpace(cell), 64)
		if err != nil || math.IsNaN(serial) || math.IsInf(serial, 0) || serial != math.Trunc(serial) || serial < 1 || serial > 2958465 {
			return nil, fmt.Errorf("salary SDDS: invalid month column %d", col+3)
		}
		date, err := excelize.ExcelDateToTime(serial, false)
		if err != nil || !date.Equal(expected) {
			return nil, fmt.Errorf("salary SDDS: nonconsecutive month column %d (expected %s)", col+3, expected.Format("2006-01-02"))
		}
		expected = expected.AddDate(0, 1, 0)
		valueCell := ""
		if col+2 < len(salary) {
			valueCell = strings.TrimSpace(salary[col+2])
		}
		if valueCell == "" {
			blank = true
			continue
		}
		if blank {
			return nil, fmt.Errorf("salary SDDS: interior missing month")
		}
		value, err := strconv.ParseFloat(valueCell, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > math.MaxFloat32 {
			return nil, fmt.Errorf("salary SDDS: invalid salary column %d", col+3)
		}
		points = append(points, salariesMesPoint{date: date.AddDate(0, 1, 0), value: value})
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("salary SDDS: no salary observations")
	}
	return points, nil
}

// The shared preflight returns before AfterImport on a known version. Refresh
// metadata on that path too; a fallback no-op reports zero inserted rows.
func importSalaryWithMetadata(ctx context.Context, conn driver.Conn, importer *util.ClickHouseImport, fallback bool) (int64, error) {
	published := false
	after := importer.AfterImport
	importer.AfterImport = func(ctx context.Context, conn driver.Conn) error {
		if after != nil {
			if err := after(ctx, conn); err != nil {
				return err
			}
		}
		published = true
		return nil
	}
	count, err := importer.Import(ctx, conn)
	if err != nil {
		return count, err
	}
	if !published {
		if after != nil {
			if err := after(ctx, conn); err != nil {
				return count, err
			}
		}
		if fallback {
			count = 0
		}
	}
	return count, nil
}
