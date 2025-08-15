package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

const (
	// Сведения о размещенных и привлеченных средствах https://www.cbr.ru/statistics/bank_sector/sors/
	// Кредиты, предоставленные физическим лицам - резидентам (региональный разрез)
	// Общий объем кредитов, предоставленных физическим лицам-резидентам
	// https://www.cbr.ru/vfs/statistics/BankSector/Mortgage/02_04_New_loans_ind.xlsx
	loansToIndXlsDataUrl = cbrStatsUrl + "/BankSector/Mortgage/02_04_New_loans_ind.xlsx"
	loansToIndTable      = "cbr_loans_to_individuals"
	loansToIndDdl        = `CREATE TABLE IF NOT EXISTS ` + loansToIndTable + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	loansToIndInsert = "INSERT INTO " + loansToIndTable + " VALUES (?, ?, ?)"
	loansToIndField  = "РОССИЙСКАЯ ФЕДЕРАЦИЯ"
)

type LoansToindividualsStat struct {
}

func (s *LoansToindividualsStat) Name() string {
	return loansToIndTable
}

func (s *LoansToindividualsStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(loansToIndXlsDataUrl); err != nil {
		return nil, err
	}
	defer xlsx.Close()
	table = new([][]string)
	var rows [][]string
	if rows, err = xlsx.GetRows("итого"); err != nil {
		return nil, err
	}
	fieldFound := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		if strings.TrimSpace(row[0]) == loansToIndField {
			fieldFound = i - 1
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
			name := strings.TrimSpace(row[0])
			date := rows[fieldFound][j+1]
			balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
			fmt.Printf("name %s date %v cell %s\n", name, date, balance)
			if _, err = strconv.ParseFloat(balance, 32); err != nil {
				return nil, err
			}
			*table = append(*table, []string{name, date, balance})
		}
	}

	return table, nil
}

func (s *LoansToindividualsStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, loansToIndDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		dateArr := strings.Split(row[1], " ")
		year, _ := strconv.Atoi(dateArr[1])
		date := time.Date(year, util.MonthsToNum[strings.ToLower(dateArr[0])], 1, 0, 0, 0, 0, time.UTC)
		if err = conn.Exec(ctx, loansToIndInsert, row[0], date.AddDate(0, 1, 0), row[2]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &LoansToindividualsStat{})
}
