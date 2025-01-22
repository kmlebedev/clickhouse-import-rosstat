package bank

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

	// https://www.sberbank.com/ru/investor-relations/groupresults/navigator/rezultaty-rpbu
	// https://www.sberbank.com/common/img/uploaded/redirected/com/investor-relations/groupresults/sber_finansovie_rezultaty_2022-2024.xlsx
	sberRpbuUrl          = "https://www.sberbank.com/common/img/uploaded/redirected/com/investor-relations/groupresults"
	sberRpbuXlsDataUrl   = sberRpbuUrl + "/sber_finansovie_rezultaty_ras_2022-2024.xlsx"
	sberRpbuDataUrlTable = "sber_finansovie_rezultaty"
	sberRpbuDataUrlDdl   = `CREATE TABLE IF NOT EXISTS ` + sberRpbuDataUrlTable + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	sberRpbuDataInsert = "INSERT INTO " + sberRpbuDataUrlTable + " VALUES (?, ?, ?)"
	sberRpbuDataField  = "Активы"
	sberRpbuTimeLayout = "01-02-06"
)

type LoansToCorporationsStat struct {
}

func (s *LoansToCorporationsStat) Name() string {
	return sberRpbuDataUrlTable
}

func (s *LoansToCorporationsStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(sberRpbuXlsDataUrl); err != nil {
		return nil, err
	}
	defer xlsx.Close()
	table = new([][]string)
	var rows [][]string
	if rows, err = xlsx.GetRows("Баланс"); err != nil {
		return nil, err
	}
	fieldFound := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		if strings.TrimSpace(row[1]) == sberRpbuDataField {
			fieldFound = i - 1
		}
		if fieldFound == 0 {
			continue
		}
		if len(row) < 3 {
			break
		}
		if strings.TrimSpace(row[2]) == "" || strings.TrimSpace(row[2]) == "0" {
			continue
		}
		// Колонки с месяцами и пропуском кварталов
		for j, cell := range row[2:] {
			if cell == "" {
				continue
			}
			if j+1 >= len(rows[fieldFound]) {
				break
			}
			// fmt.Printf("name %s date %v cell %s\n", row[1], rows[fieldFound][j+2], cell)
			balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
			if _, err = strconv.ParseFloat(balance, 32); err != nil {
				return nil, err
			}
			*table = append(*table, []string{row[1], rows[fieldFound][j+2], balance})
		}
	}

	return table, nil
}

func (s *LoansToCorporationsStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, sberRpbuDataUrlDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		date, err := time.Parse(sberRpbuTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, sberRpbuDataInsert, row[0], date, row[2]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &LoansToCorporationsStat{})
}
