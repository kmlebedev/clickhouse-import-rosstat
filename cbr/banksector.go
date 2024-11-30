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
	// Объем кредитов, предоставленных юридическим лицам
	// https://www.cbr.ru/vfs/statistics/BankSector/Loans_to_corporations/01_01_A_New_loans_corp_by_activity.xlsx
	loansToCorporationsXlsDataUrl = cbrStatsUrl + "/BankSector/Loans_to_corporations/01_01_A_New_loans_corp_by_activity.xlsx"
	loansToCorporationsTable      = "cbr_loans_to_corporations"
	loansToCorporationsDdl        = `CREATE TABLE IF NOT EXISTS ` + loansToCorporationsTable + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	loansToCorporationsInsert     = "INSERT INTO " + loansToCorporationsTable + " VALUES (?, ?, ?)"
	loansToCorporationsField      = "ВСЕГО"
	loansToCorporationsTimeLayout = "2006-01"
)

type LoansToCorporationsStat struct {
}

func (s *LoansToCorporationsStat) Name() string {
	return loansToCorporationsTable
}

func (s *LoansToCorporationsStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(loansToCorporationsXlsDataUrl); err != nil {
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
		if strings.TrimSpace(row[0]) == loansToCorporationsField {
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
			// fmt.Printf("name %s date %v cell %s\n", name, date, balance)
			if _, err = strconv.ParseFloat(balance, 32); err != nil {
				return nil, err
			}
			*table = append(*table, []string{name, date, balance})
		}
	}

	return table, nil
}

func (s *LoansToCorporationsStat) Import(ctx context.Context, conn *sql.DB) (count int64, err error) {
	if _, err := conn.Exec(loansToCorporationsDdl); err != nil {
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
		if res, err := conn.ExecContext(ctx, loansToCorporationsInsert, row[0], date.AddDate(0, 1, 0), row[2]); err != nil {
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
