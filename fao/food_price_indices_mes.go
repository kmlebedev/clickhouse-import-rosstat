package rosstat

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"slices"
	"strings"
	"time"
)

const (
	// Даты публикации ежемесячных сводок в 2025 году:
	// 3 января, 7 февраля, 7 марта, 4 апреля, 2 мая, 6 июня, 4 июля, 8 августа, 5 сентября, 3 октября, 7 ноября, 5 декабря.
	// ToDo update data source https://www.fao.org/worldfoodsituation/foodpricesindex/ru/
	// Индекса продовольственных цен ФАО https://www.fao.org/docs/worldfoodsituationlibraries/default-document-library/food_price_indices_data_f.csv
	faoUrl                 = "https://www.fao.org/docs/worldfoodsituationlibraries/default-document-library"
	faoFoodPriceCSVDataUrl = faoUrl + "/food_price_indices_data_aug25.csv"
	faoFoodPriceTable      = "fao_food_price"
	faoFoodPriceDdl        = `CREATE TABLE IF NOT EXISTS ` + faoFoodPriceTable + `_%s` + ` (
			  name LowCardinality(String)
			, date Date
			, price_index Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	faoFoodPriceInsert     = "INSERT INTO " + faoFoodPriceTable + "_%s VALUES (?, ?, ?)"
	faoFoodPriceTimeLayout = "2006-01"
)

var (
	faoFoodPriceSheet = []string{"Indices_monthly", "Indices_Monthly_Real"}
)

type FaoFoodPriceStat struct {
}

func (s *FaoFoodPriceStat) Name() string {
	return faoFoodPriceTable
}

func (s *FaoFoodPriceStat) exportXls() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(faoFoodPriceCSVDataUrl); err != nil {
		return nil, err
	}
	table = new([][]string)
	for _, sheet := range xlsx.GetSheetList() {
		if !slices.Contains(faoFoodPriceSheet, sheet) {
			continue
		}
		var rows [][]string
		if rows, err = xlsx.GetRows(sheet); err != nil {
			return nil, err
		}
		for _, row := range rows[4:] {
			*table = append(*table, []string{sheet, rows[4][1], row[0], row[1]})
		}
	}
	return table, nil
}

func (s *FaoFoodPriceStat) export() (table *[][]string, err error) {
	var records [][]string
	if records, err = util.GetCSV(faoFoodPriceCSVDataUrl); err != nil {
		return nil, err
	}
	table = new([][]string)
	fileds := strings.Split(records[2][0], ",")
	for _, row := range records[4:] {
		cols := strings.Split(row[0], ",")
		if len(cols) == 1 {
			continue
		}
		for i, cell := range cols[1:6] {
			//fmt.Printf("row field %v coll %v cell %v\n", fileds[i+1], cols[0], cell)
			*table = append(*table, []string{"Indices_monthly", fileds[i+1], cols[0], cell})
		}
	}
	return table, nil
}

func (s *FaoFoodPriceStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {

	for _, sheet := range faoFoodPriceSheet {
		if err = conn.Exec(ctx, fmt.Sprintf(faoFoodPriceDdl, sheet)); err != nil {
			return count, err
		}
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		mes, err := time.Parse(faoFoodPriceTimeLayout, row[2])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, fmt.Sprintf(faoFoodPriceInsert, row[0]), row[1], mes, row[3]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &FaoFoodPriceStat{})
}
