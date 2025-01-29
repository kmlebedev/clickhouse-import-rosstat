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

//

const (
	// Денежно-кредитная и финансовая статистика https://www.cbr.ru/statistics/macro_itm/dkfs/
	// Сезонно скорректированный ряд денежной массы (M2)
	// https://www.cbr.ru/vfs/statistics/credit_statistics/M2-M2_SA.xlsx
	cbrM2XlsDataUrl = cbrStatsUrl + "/credit_statistics/M2-M2_SA.xlsx"
	cbrM2Table      = "cbr_m2"
	cbrM2Ddl        = `CREATE TABLE IF NOT EXISTS ` + cbrM2Table + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	cbrM2Insert     = "INSERT INTO " + cbrM2Table + " VALUES (?, ?, ?)"
	cbrM2Field      = "Date"
	cbrM2TimeLayout = "01-02-06"
)

type cbrM2Stat struct {
}

func (s *cbrM2Stat) Name() string {
	return cbrM2Table
}

func (s *cbrM2Stat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(cbrM2XlsDataUrl); err != nil {
		return nil, err
	}
	defer xlsx.Close()
	table = new([][]string)
	var rows [][]string
	if rows, err = xlsx.GetRows("M2 data"); err != nil {
		return nil, err
	}
	fieldFound := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		if strings.TrimSpace(row[0]) == cbrM2Field {
			fieldFound = i
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
			name := strings.TrimSpace(rows[fieldFound][j+1])
			date := strings.TrimSpace(row[0])
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

func (s *cbrM2Stat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, cbrM2Ddl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		date, err := time.Parse(cbrM2TimeLayout, row[1])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, cbrM2Insert, row[0], date, row[2]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &cbrM2Stat{})
}
