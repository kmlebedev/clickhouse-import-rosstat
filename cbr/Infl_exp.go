package cbr

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"slices"
	"strconv"
	"time"
)

// Todo update data source https://www.cbr.ru/analytics/dkp/inflationary_expectations/
// Статистические данные
const inflExpDataUrl = "https://www.cbr.ru/Collection/Collection/File/57180/Infl_exp_25-08.xlsx"

func inflExpImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("Данные для графиков")
	if err != nil {
		return err
	}
	tableIsFoundRowNum := -1
	fields := []string{"наблюдаемая инфляция", "ожидаемая инфляция"}
	for i, row := range rows[1:] {
		if len(row) == 0 || row[0] == "" {
			continue
		}
		if row[0] == "Прямые оценки годовой инфляции: медианные  значения" {
			tableIsFoundRowNum = i
			continue
		}
		if tableIsFoundRowNum < 0 {
			continue
		}
		//fmt.Printf("row: %+v", row)
		//fmt.Printf("rowdate : %+v", rows[tableIsFoundRowNum+2])
		if slices.Contains(fields, row[0]) {
			for j, rowCol := range row[1:] {
				fmt.Printf("rowCol: %+v, date: %s\n", rowCol, rows[tableIsFoundRowNum+2][j+1])
				date, err := time.Parse("Jan-06", rows[tableIsFoundRowNum+2][j+1])
				if err != nil {
					return err
				}
				value, err := strconv.ParseFloat(rowCol, 16)
				if err != nil {
					return err
				}
				if err = batch.Append(row[0], date.AddDate(0, 0, 13), value); err != nil {
					return err
				}
			}
			continue
		}
		break
	}
	return nil
}

// https://www.cbr.ru/Collection/Collection/File/55275/Infl_exp_25-03.xlsx
func init() {
	inflExp := util.HdBase{
		TableName: "cbr_infl_exp",
		DataUrl:   inflExpDataUrl,
		CreateTable: `CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`,
		ImportFunc: inflExpImport,
	}
	chimport.Stats = append(chimport.Stats, &inflExp)
}
