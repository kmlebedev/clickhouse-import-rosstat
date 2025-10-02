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
	// Денежно-кредитная и финансовая статистика https://www.cbr.ru/statistics/macro_itm/dkfs/
	// Сезонно скорректированные ряды денежных агрегатов
	// https://www.cbr.ru/vfs/statistics/credit_statistics/monetary_agg_SA.xlsx
	// https://www.cbr.ru/vfs/statistics/credit_statistics/M2-M2_SA.xlsx
	cbrM2XlsDataUrl = cbrStatsUrl + "/credit_statistics/monetary_agg_SA.xlsx"
	cbrM2Table      = "cbr_m2"
	cbrM2Ddl        = `CREATE TABLE IF NOT EXISTS ` + cbrM2Table + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	cbrM2Insert     = "INSERT INTO " + cbrM2Table + " VALUES (?, ?, ?)"
	cbrM2Field      = "Денежные агрегаты"
	cbrM2TimeLayout = "02/01/06"
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
	if rows, err = xlsx.GetRows("Денежные агрегаты"); err != nil {
		return nil, err
	}
	// Строки с годами
	for _, row := range rows[2:] {
		if len(row) == 0 {
			continue
		}
		if len(row) < 22 {
			break
		}
		if strings.TrimSpace(row[22]) == "" || strings.TrimSpace(row[22]) == "0" {
			continue
		}
		date := strings.TrimSpace(row[0])
		balance := strings.ReplaceAll(strings.TrimSpace(row[22]), ",", "")
		fmt.Printf("name %s date %v cell %s\n", rows[0][22], date, balance)
		if _, err = strconv.ParseFloat(balance, 32); err != nil {
			return nil, err
		}
		*table = append(*table, []string{rows[0][22], date, balance})
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
