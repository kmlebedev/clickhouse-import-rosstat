package rosstat

import (
	"bytes"
	"context"
	"database/sql"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/xuri/excelize/v2"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	ipcWeeksXlsDataUrl = "https://rosstat.gov.ru/storage/mediabank/nedel_ipc.xlsx"
	ipcWeeksTable      = "ipc_weeks"
	ipcWeeksDdl        = `CREATE TABLE IF NOT EXISTS ` + ipcWeeksTable + ` (
			  name LowCardinality(String)
			, date Date
			, percent Float32
		) ENGINE = Memory
	`
	ipcWeeksInsert     = "INSERT INTO " + ipcWeeksTable + " VALUES (?, ?, ?)"
	ipcWeeksField      = "Наименование"
	ipcWeeekDateLayout = "2006-01-02"
)

type IpcWeeksStat struct {
}

func weekStart(year, week string) time.Time {
	y, _ := strconv.Atoi(year)
	w, _ := strconv.Atoi(week)
	// Start from the middle of the year:
	t := time.Date(y, 7, 1, 0, 0, 0, 0, time.UTC)

	// Roll back to Monday:
	if wd := t.Weekday(); wd == time.Sunday {
		t = t.AddDate(0, 0, -6)
	} else {
		t = t.AddDate(0, 0, -int(wd)+1)
	}

	// Difference in weeks:
	_, isoWeek := t.ISOWeek()
	t = t.AddDate(0, 0, (w-isoWeek)*7)

	return t
}

func (s *IpcWeeksStat) Name() string {
	return "ipc_weeks"
}

func (s *IpcWeeksStat) export() (table *[][]string, err error) {
	resp, err := http.Get(ipcWeeksXlsDataUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	reader := bytes.NewReader(body)
	xlsx, err := excelize.OpenReader(reader)
	if err != nil {
		return nil, err
	}
	for _, sheet := range xlsx.GetSheetList() {
		var year int
		if year, err = strconv.Atoi(sheet); err != nil || year < 1997 {
			continue
		}
		var rows [][]string
		if rows, err = xlsx.GetRows(sheet); err != nil {
			return nil, err
		}
		fieldFound := false
		table = new([][]string)
		for _, row := range rows {
			if fieldFound {
				if len(row) < 2 {
					break
				}
				if _, err = strconv.ParseFloat(row[1], 32); err != nil {
					break
				}
				for i, cell := range row[1:] {
					*table = append(*table, []string{row[0], sheet, strconv.Itoa(i + 2), cell})
				}
			} else if row[0] == ipcWeeksField {
				fieldFound = true
			}
		}
	}
	return table, nil
}

func (s *IpcWeeksStat) Import(ctx context.Context, conn *sql.DB) (count int64, err error) {
	if _, err := conn.Exec(ipcWeeksDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		if res, err := conn.ExecContext(ctx, ipcWeeksInsert, row[0], weekStart(row[1], row[2]), row[3]); err != nil {
			return count, err
		} else {
			rows, _ := res.RowsAffected()
			count += rows
		}
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &IpcWeeksStat{})
}
