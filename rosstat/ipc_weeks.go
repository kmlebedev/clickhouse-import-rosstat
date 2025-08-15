package rosstat

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"strconv"
)

import (
	"context"
)

const (
	// ToDo update data source https://rosstat.gov.ru/statistics/price
	// Еженедельные индексы потребительских цен (тарифов) на отдельные товары и услуги по Российской
	// ipcWeeksXlsDataUrl = rosstatUrl + "/nedel_ipc.xlsx"
	ipcWeeksXlsDataUrl = rosstatUrl + "/Nedel_ipc.xlsx"
	ipcWeeksTable      = "ipc_weeks"
	ipcWeeksDdl        = `CREATE TABLE IF NOT EXISTS ` + ipcWeeksTable + ` (
			  name LowCardinality(String)
			, date Date
			, percent Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	ipcWeeksInsert = "INSERT INTO " + ipcWeeksTable
	ipcWeeksField  = "Наименование"
)

type IpcWeeksStat struct {
}

func (s *IpcWeeksStat) Name() string {
	return ipcWeeksTable
}

func (s *IpcWeeksStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(ipcWeeksXlsDataUrl); err != nil {
		return nil, err
	}
	table = new([][]string)
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

func (s *IpcWeeksStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, ipcWeeksDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	log.Infof("Export %d rows of %s", len(*table), s.Name())
	batch, err := conn.PrepareBatch(ctx, ipcWeeksInsert)
	if err != nil {
		return count, err
	}
	for _, row := range *table {
		percent, _ := strconv.ParseFloat(row[3], 32)
		if err = batch.Append(row[0], weekStart(row[1], row[2]), float32(percent)); err != nil {
			return count, err
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &IpcWeeksStat{})
}
