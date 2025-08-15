package cbr

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/xuri/excelize/v2"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Todo update data source https://www.cbr.ru/statistics/ddkp/aipd/
// Показатели сезонно сглаженной динамики потребительских цен
const indicatorsCpdDataUrl = "https://www.cbr.ru/Content/Document/File/108632/indicators_cpd.xlsx"

func init() {
	indicatorsCpd := hdBase{
		name:    "cbr_indicators_cpd",
		dataUrl: indicatorsCpdDataUrl,
		createTable: `CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`,
		importFunc: indicatorsCpdImport,
	}
	chimport.Stats = append(chimport.Stats, &indicatorsCpd)
}

// https://www.cbr.ru/analytics/dkp/dinamic/
func indicatorsCpdImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("Лист1")
	if err != nil {
		return err
	}
	fileds := []string{"Все товары и услуги", "Базовый ИПЦ"}
	for i, row := range rows {
		if len(row) < 1 || !slices.Contains(fileds, strings.TrimSpace(row[0])) {
			continue
		}
		for j, rowCol := range rows[i][2:] {
			//fmt.Printf("name: %s rowCol: %+v, date: %s\n", row[1], rowCol, rows[0][j+2])
			date, err := time.Parse("01/06", rows[0][j+2])
			if err != nil {
				return err
			}
			value, err := strconv.ParseFloat(rowCol, 16)
			if err != nil {
				return err
			}
			if err = batch.Append(row[0], date.AddDate(0, 0, -1), value); err != nil {
				return err
			}
		}
	}
	return nil
}
