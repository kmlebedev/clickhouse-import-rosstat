package rosstat

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

const (
	// Рынок труда, занятость и заработная плата https://rosstat.gov.ru/labor_market_employment_salaries
	salariesMesXlsDataUrl = rosstatUrl + "/tab1-zpl_08-2024.xlsx"
	salariesMesTable      = "salaries_mes"
	salariesMesDdl        = `CREATE TABLE IF NOT EXISTS ` + salariesMesTable + ` (
			  name LowCardinality(String)
			, date Date
			, salary Float32
		) ENGINE = Memory
	`
	salariesMesInsert     = "INSERT INTO " + salariesMesTable + " VALUES (?, ?, ?)"
	salariesMesField      = "1991"
	salariesMesYearStart  = 1991
	salariesMesTimeLayout = "2006-01"
)

type SalariesMesStat struct {
}

func (s *SalariesMesStat) Name() string {
	return salariesMesTable
}

func (s *SalariesMesStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(salariesMesXlsDataUrl); err != nil {
		return nil, err
	}
	table = new([][]string)
	for _, sheet := range xlsx.GetSheetList() {
		var rows [][]string
		if rows, err = xlsx.GetRows(sheet); err != nil {
			return nil, err
		}
		fieldFound := 0
		// Строки с годами
		for i, row := range rows {
			if len(row) == 0 {
				continue
			}
			if row[0] == salariesMesField {
				fieldFound = i
			}
			if fieldFound == 0 {
				continue
			}
			if len(row) < 7 {
				break
			}
			mes := 0
			// Колонки с месяцами и пропуском кварталов
			for _, cell := range row[6:] {
				mes += 1
				if cell == "" {
					continue
				}
				fmt.Printf("year %d mes %02d cell %s\n", salariesMesYearStart+i-fieldFound, mes, cell)
				salary := strings.Split(cell, "(")[0]
				if _, err = strconv.ParseFloat(salary, 32); err != nil {
					return nil, err
				}
				//                                name           year
				*table = append(*table, []string{rows[0][0], strconv.Itoa(salariesMesYearStart + i - fieldFound), fmt.Sprintf("%02d", mes), salary})
			}
		}
	}
	return table, nil
}

func (s *SalariesMesStat) Import(ctx context.Context, conn *sql.DB) (count int64, err error) {
	if _, err := conn.Exec(salariesMesDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		mes, err := time.Parse(salariesMesTimeLayout, fmt.Sprintf("%s-%s", row[1], row[2]))
		if err != nil {
			return count, err
		}
		if res, err := conn.ExecContext(ctx, salariesMesInsert, row[0], mes, row[3]); err != nil {
			return count, err
		} else {
			rows, _ := res.RowsAffected()
			count += rows
		}
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &SalariesMesStat{})
}
