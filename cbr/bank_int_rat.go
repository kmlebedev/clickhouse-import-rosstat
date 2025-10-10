package cbr

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

// Банковские ставки от https://www.cbr.ru/statistics/bank_sector/int_rat/regulatory_rates/

const (
	cbrBankIntRateTable = "cbr_bank_int_rate"
)

var cbrBankIntRate = util.ClickHouseImport{
	TableName: cbrBankIntRateTable,
	CreateTable: `CREATE TABLE IF NOT EXISTS %s (
    		  name LowCardinality(String)
			, date Date
    		, rate Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`,
	DataUrl: "https://www.cbr.ru/vfs/statistics/pdko/int_rat/regulatory_rates.xlsx",
	ImportFunc: func(xlsx *excelize.File, batch driver.Batch) error {
		rows, err := xlsx.GetRows("в рублях")
		if err != nil {
			return err
		}
		for _, row := range rows[3:] {
			// log.Infof("row %+v", row)
			if row[0] == "" {
				continue
			}
			dateArr := strings.SplitN(strings.TrimSpace(row[0]), " ", 2)
			dateYear, err := strconv.Atoi(strings.TrimSpace(dateArr[1]))
			if err != nil {
				return err
			}
			date := time.Date(dateYear, util.MonthsToNum[strings.ToLower(dateArr[0])]+1, -1, 0, 0, 0, 0, time.UTC)
			for i, cell := range row[1:] {
				if cell == "" {
					continue
				}
				name := rows[2][i+1]
				rate, err := strconv.ParseFloat(cell, 32)
				if err != nil {
					return err
				}
				if err = batch.Append(name, date, rate); err != nil {
					log.Error(err)
					return err
				}
			}
		}
		return nil
	},
}

func init() {
	chimport.Stats = append(chimport.Stats, &cbrBankIntRate)
}
