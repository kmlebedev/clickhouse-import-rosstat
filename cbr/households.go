package cbr

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

const (

	// Показатель сбережений сектора «Домашние хозяйства» https://www.cbr.ru/statistics/macro_itm/households/
	// https://www.cbr.ru/vfs/statistics/households/households_b.xlsx
	cbrStatsUrl              = "https://www.cbr.ru/vfs/statistics"
	householdsBMesXlsDataUrl = cbrStatsUrl + "/households/households_bm.xlsx"
	householdsBMesTable      = "households_b_mes"
	householdsBMesDdl        = `CREATE TABLE IF NOT EXISTS ` + householdsBMesTable + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	householdsBMesInsert     = "INSERT INTO " + householdsBMesTable + " VALUES (?, ?, ?)"
	householdsBMesField      = "АКТИВЫ"
	householdsBMesTimeLayout = "01-02-06"
)

type HouseholdsBMesStat struct {
}

func (s *HouseholdsBMesStat) Name() string {
	return householdsBMesTable
}

func (s *HouseholdsBMesStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(householdsBMesXlsDataUrl); err != nil {
		return nil, err
	}
	defer xlsx.Close()
	table = new([][]string)
	var rows [][]string
	if rows, err = xlsx.GetRows("Балансы"); err != nil {
		return nil, err
	}
	fieldFound := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		//fmt.Printf("name '%s'\n", row[0])
		if strings.TrimSpace(row[0]) == householdsBMesField {
			fieldFound = i - 1
			continue
		}
		if fieldFound == 0 {
			continue
		}
		if len(row) < 1 {
			break
		}
		if strings.TrimSpace(row[1]) == "" || strings.TrimSpace(row[1]) == "0" {
			continue
		}
		// Колонки с месяцами и пропуском кварталов
		for j, cell := range row[1:] {
			if cell == "" {
				continue
			}
			if j+1 >= len(rows[fieldFound]) {
				break
			}
			//fmt.Printf("name %s date %v cell %s\n", row[0], rows[fieldFound][j+1], cell)
			balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
			if _, err = strconv.ParseFloat(balance, 32); err != nil {
				return nil, err
			}
			//                                name           year
			*table = append(*table, []string{row[0], rows[fieldFound][j+1], balance})
		}
	}

	return table, nil
}

func (s *HouseholdsBMesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, householdsBMesDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		date, err := time.Parse(householdsBMesTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, householdsBMesInsert, row[0], date, row[2]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &HouseholdsBMesStat{})
}
