package cbr

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

// Денежно-кредитная и финансовая статистика https://www.cbr.ru/statistics/macro_itm/dkfs/
// Приложение к материалу «Кредит экономике и денежная масса»
// https://www.cbr.ru/Content/Document/File/177307/credit_m2x.xlsx
var cbrСreditM2x = util.ClickHouseImport{
	TableName: "cbr_credit_m2x",
	CreateTable: `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`,
	DataUrl: "https://www.cbr.ru/Content/Document/File/177307/credit_m2x.xlsx",
	ImportFunc: func(xlsx *excelize.File, batch driver.Batch) (err error) {
		var rows [][]string
		if rows, err = xlsx.GetRows("млрд рублей"); err != nil {
			return err
		}
		// Строки с годами
		for _, row := range rows[1:] {
			if len(row) == 0 {
				continue
			}
			name := strings.TrimSpace(row[0])
			if name == "" {
				break
			}
			for j, cell := range row[1:] {
				date, err := time.Parse("01-02-06", strings.TrimSpace(rows[0][j+1]))
				if err != nil {
					return err
				}
				valueStr := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
				fmt.Printf("name %s date %v cell %s\n", name, date, valueStr)
				if value, err := strconv.ParseFloat(valueStr, 32); err != nil {
					return err
				} else {
					if err = batch.Append(name, date, value); err != nil {
						return err
					}
				}
			}
		}
		return nil
	},
}

func init() {
	chimport.Stats = append(chimport.Stats, &cbrСreditM2x)
}
