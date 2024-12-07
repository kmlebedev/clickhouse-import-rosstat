package rosstat

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"time"
)

const (
	// Потребительские цены https://rosstat.gov.ru/statistics/price
	// https://rosstat.gov.ru/storage/mediabank/ipc_mes_10-2024.xlsx
	ipcMesXlsDataUrl = rosstatUrl + "/ipc_mes_11-2024.xlsx"
	ipcMesTable      = "ipc_mes"
	ipcMesDdl        = `CREATE TABLE IF NOT EXISTS ` + ipcMesTable + ` (
			  name LowCardinality(String)
			, date Date
			, percent Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	ipcMesInsert     = "INSERT INTO " + ipcMesTable + " VALUES (?, ?, ?)"
	ipcMesField      = "к концу предыдущего месяца"
	ipcMesYearStart  = 1991
	ipcMesTimeLayout = "2006-01"
)

type IpcMesStat struct {
}

func (s *IpcMesStat) Name() string {
	return ipcMesTable
}

func (s *IpcMesStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(ipcMesXlsDataUrl); err != nil {
		return nil, err
	}
	table = new([][]string)
	for _, sheet := range xlsx.GetSheetList() {
		var rows [][]string
		if _, err = strconv.Atoi(sheet); err != nil {
			continue
		}
		if rows, err = xlsx.GetRows(sheet); err != nil {
			return nil, err
		}
		fieldFound := false
		mes := 0
		for _, row := range rows {
			if fieldFound {
				mes += 1
				if len(row) < 2 || mes > 12 {
					break
				}
				for i, cell := range row[1:] {
					if cell == "" {
						continue
					}
					if _, err = strconv.ParseFloat(cell, 32); err != nil {
						return nil, err
					}
					//                                name           year
					*table = append(*table, []string{rows[0][0], strconv.Itoa(ipcMesYearStart + i), fmt.Sprintf("%02d", mes), cell})
				}
			} else if row[0] == ipcMesField {
				fieldFound = true
			}
		}
	}
	return table, nil
}

func (s *IpcMesStat) Import(ctx context.Context, conn *sql.DB) (count int64, err error) {
	if _, err := conn.Exec(ipcMesDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		mes, err := time.Parse(ipcMesTimeLayout, fmt.Sprintf("%s-%s", row[1], row[2]))
		if err != nil {
			return count, err
		}
		if res, err := conn.ExecContext(ctx, ipcMesInsert, row[0], mes, row[3]); err != nil {
			return count, err
		} else {
			rows, _ := res.RowsAffected()
			count += rows
		}
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &IpcMesStat{})
}
