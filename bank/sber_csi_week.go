package bank

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"golang.org/x/text/encoding/charmap"
	"strconv"
	"strings"
	"time"
)

const (
	// https://sberindex.ru/ru/dashboards/ver-izmenenie-trat-po-kategoriyam
	// https://sberindex.ru/proxy/services/node-services/v1/source/ru/csv/ver-izmenenie-trat-po-kategoriyam?representation=4&timeDivision=1
	sberCSIWeekUrl     = "https://sberindex.ru/proxy/services/node-services/v1"
	sberCSIWeekDataUrl = sberCSIUrl + "/source/ru/csv/ver-izmenenie-trat-po-kategoriyam?representation=4&timeDivision=1"
	sberCSIWeekTable   = "sber_izmenenie_trat"
	sberCSIWeekDdl     = `CREATE TABLE IF NOT EXISTS ` + sberCSIWeekTable + ` (
			  category LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (category, date);
    `
	sberCSIWeekInsert     = "INSERT INTO " + sberCSITable + " VALUES (?, ?, ?)"
	sberCSIWeekTimeLayout = "2006-01-02"
)

type SberCSIWeek struct {
}

func (s *SberCSIWeek) Name() string {
	return sberCSIWeekTable
}

func (s *SberCSIWeek) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, sberCSIWeekDdl); err != nil {
		return count, fmt.Errorf("create table: %+v", err)
	}

	records, err := util.GetCSV(sberCSIWeekDataUrl)
	if err != nil {
		return 0, err
	}
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", sberCSIWeekTable))
	for _, row := range records[1:] {
		if len(row) == 0 || len(row[2]) == 0 {
			continue
		}
		day, err := time.Parse(sberCSIWeekTimeLayout, row[0])
		if err != nil {
			return count, err
		}
		index, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 16)
		if err != nil {
			fmt.Printf("row: %+v", row)
			return count, err
		}
		decoder := charmap.Windows1251.NewDecoder()
		category, err := decoder.String(row[1])
		if err != nil {
			return count, err
		}
		fmt.Printf("name %s, date %v, idx: %v\n", category, day, index)
		if err = batch.Append(category, day, index); err != nil {
			return count, err
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &SberCSIWeek{})
}
