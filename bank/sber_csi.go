package bank

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"golang.org/x/text/encoding/charmap"
	"strconv"
	"time"
)

const (
	// https://sberindex.ru/ru/dashboards/consumper-spending-index-sa
	// https://sberindex.ru/proxy/services/node-services/v1/source/ru/csv/consumper-spending-index-sa?representation=1&timeDivision=2
	sberCSIUrl     = "https://sberindex.ru/proxy/services/node-services/v1"
	sberCSIDataUrl = sberCSIUrl + "/source/ru/csv/consumper-spending-index-sa?representation=1&timeDivision=2"
	sberCSITable   = "sber_consumper_spending_index"
	sberCSIDdl     = `CREATE TABLE IF NOT EXISTS ` + sberCSITable + ` (
			  name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
    `
	sberCSIInsert     = "INSERT INTO " + sberCSITable + " VALUES (?, ?, ?)"
	sberCSITimeLayout = "2006-01-02"
)

type SberCSI struct {
}

func (s *SberCSI) Name() string {
	return sberCSITable
}

func (s *SberCSI) export() (table *[][]string, err error) {
	var records [][]string
	if records, err = util.GetCSV(sberCSIDataUrl); err != nil {
		return nil, err
	}
	table = &records
	return table, nil
}

func (s *SberCSI) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, sberCSIDdl); err != nil {
		return count, fmt.Errorf("create table: %+v", err)
	}

	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, fmt.Errorf("export %v", err)
	}
	rows := *table
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", sberCSITable))
	for _, row := range rows[1:] {
		day, err := time.Parse(sberCSITimeLayout, row[0])
		if err != nil {
			return count, err
		}
		index, err := strconv.ParseFloat(row[2], 32)
		if err != nil {
			return count, err
		}
		decoder := charmap.Windows1251.NewDecoder()
		name, err := decoder.String(row[1])
		if err != nil {
			return count, err
		}
		fmt.Printf("name %s, date %v, idx: %v\n", name, day, index)
		if err = batch.Append(name, day, index); err != nil {
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
	chimport.Stats = append(chimport.Stats, &SberCSI{})
}
