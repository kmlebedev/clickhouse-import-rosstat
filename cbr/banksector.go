package cbr

import (
	"context"
	"database/sql"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

const (

	// Сведения о размещенных и привлеченных средствах https://www.cbr.ru/statistics/bank_sector/sors
	// https://www.cbr.ru/vfs/statistics/households/households_b.xlsx
	loansToCorporationsXlsDataUrl = cbrStatsUrl + "/BankSector/Loans_to_corporations/01_01_A_New_loans_corp_by_activity.xlsx"
	loansToCorporationsTable      = "loans_to_corporations"
	loansToCorporationsDdl        = `CREATE TABLE IF NOT EXISTS ` + householdsBMesTable + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	loansToCorporationsInsert     = "INSERT INTO " + householdsBMesTable + " VALUES (?, ?, ?)"
	loansToCorporationsField      = "АКТИВЫ"
	loansToCorporationsTimeLayout = "01-02-06"
)

type LoansToCorporationsStat struct {
}

func (s *LoansToCorporationsStat) Name() string {
	return loansToCorporationsTable
}

func (s *LoansToCorporationsStat) export() (table *[][]string, err error) {
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

func (s *LoansToCorporationsStat) Import(ctx context.Context, conn *sql.DB) (count int64, err error) {
	if _, err := conn.Exec(householdsBMesDdl); err != nil {
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
		if res, err := conn.ExecContext(ctx, householdsBMesInsert, row[0], date, row[2]); err != nil {
			return count, err
		} else {
			rows, _ := res.RowsAffected()
			count += rows
		}
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &LoansToCorporationsStat{})
}
