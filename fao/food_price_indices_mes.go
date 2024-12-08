package rosstat

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"slices"
	"time"
)

const (
	// Даты публикации ежемесячных сводок в 2024 году:
	// 5 января, 2 февраля, 8 марта, 5 апреля, 3 мая, 7 июня, 5 июля, 2 августа, 6 сентября, 4 октября, 8 ноября, 6 декабря.
	// https://www.fao.org/worldfoodsituation/foodpricesindex/ru/
	// Индекса продовольственных цен ФАО https://www.fao.org/docs/worldfoodsituationlibraries/default-document-library/food_price_indices_data_nov24.xls
	faoUrl                 = "https://www.fao.org/docs/worldfoodsituationlibraries/default-document-library"
	faoFoodPriceCSVDataUrl = faoUrl + "/food_price_indices_data_d.csv"
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
	for _, row := range records[4:] {
		for i, cell := range row[1:6] {
			*table = append(*table, []string{"Indices_monthly", records[2][i+1], row[0], cell})
		}
	}
	return table, nil
}

func (s *FaoFoodPriceStat) Import(ctx context.Context, conn *sql.DB) (count int64, err error) {

	for _, sheet := range faoFoodPriceSheet {
		if _, err := conn.Exec(fmt.Sprintf(faoFoodPriceDdl, sheet)); err != nil {
			return count, err
		}
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters

		mes, err := time.Parse(faoFoodPriceTimeLayout, row[2])
		if err != nil {
			return count, err
		}
		if res, err := conn.ExecContext(ctx, fmt.Sprintf(faoFoodPriceInsert, row[0]), row[1], mes, row[3]); err != nil {
			return count, err
		} else {
			rows, _ := res.RowsAffected()
			count += rows
		}
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &FaoFoodPriceStat{})
}
