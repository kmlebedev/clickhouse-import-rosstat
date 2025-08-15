package cbr

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/xuri/excelize/v2"
	"strconv"
	"time"
)

func ruoniaImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("RC")
	if err != nil {
		return err
	}
	for _, row := range rows[1:] {
		if len(row) == 0 {
			continue
		}
		fmt.Printf("row: %+v\n", row)
		date, err := time.Parse("01-02-06", row[0])
		if err != nil {
			return err
		}
		rui, _ := strconv.ParseFloat(row[1], 32)
		vol, _ := strconv.ParseFloat(row[2], 32)
		trans, _ := strconv.ParseUint(row[3], 10, 16)
		minRate, _ := strconv.ParseFloat(row[5], 32)
		maxRate, _ := strconv.ParseFloat(row[8], 32)
		p25, _ := strconv.ParseFloat(row[6], 32)
		p75, _ := strconv.ParseFloat(row[7], 32)
		if err = batch.Append(date, rui, vol, trans, minRate, maxRate, p25, p75); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	ruania := hdBase{
		name:              "cbr_ruania",
		dataUrl:           "https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/14315?Posted=True&FromDate=01/01/2019&ToDate=%s",
		dataUrlTimeFormat: true,
		createTable: `CREATE TABLE IF NOT EXISTS %s (
			  date Date
			, ruo Float32
            , vol Float32
            , trans UInt16
            , minRate Float32
            , maxRate Float32
            , percentile25 Float32
            , Percentile75 Float32
		) ENGINE = ReplacingMergeTree ORDER BY (date);`,
		importFunc: ruoniaImport,
	}
	chimport.Stats = append(chimport.Stats, &ruania)
}
