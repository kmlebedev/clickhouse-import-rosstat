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
	// Даты публикации ежемесячных сводок в 2025 году:
	// 3 января, 7 февраля, 7 марта, 4 апреля, 2 мая, 6 июня, 4 июля, 8 августа, 5 сентября, 3 октября, 7 ноября, 5 декабря.
	// https://www.fao.org/worldfoodsituation/foodpricesindex/ru/
	// Индекса продовольственных цен ФАО https://www.fao.org/docs/worldfoodsituationlibraries/default-document-library/food_price_indices_data_nov24.xls
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
		if err = conn.Exec(ctx, sberCSIInsert, name, day, index); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &SberCSI{})
}
