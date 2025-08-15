package bank

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

const (

	// Ипотека Динамика ставок предложения топ-20 ипотечных банков
	// ToDo update data source https://xn--d1aqf.xn--p1ai/analytics/mortgage/
	// Скачать Динамику ставок в разрезе месяцев, xlsx
	domrfAnalyticsUrl       = "https://xn--d1aqf.xn--p1ai/upload/iblock"
	domrfMortgageXlsDataUrl = domrfAnalyticsUrl + "/098/xfjdzfswexl56n0bbwh6s22gklnqu3a5.xlsx"
	domrfMortgageTable      = "domrf_mortgage"
	domrfMortgageDdl        = `CREATE TABLE IF NOT EXISTS ` + domrfMortgageTable + ` (
			  name LowCardinality(String)
			, date Date
			, rate Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	domrfMortgageInsert     = "INSERT INTO " + domrfMortgageTable + " VALUES (?, ?, ?)"
	domrfMortgageField      = "Новостройка"
	domrfMortgageTimeLayout = "01-02-06"
)

type DomrfMortgageStat struct {
}

func (s *DomrfMortgageStat) Name() string {
	return domrfMortgageTable
}

func (s *DomrfMortgageStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(domrfMortgageXlsDataUrl); err != nil {
		return nil, err
	}
	defer xlsx.Close()
	table = new([][]string)
	var rows [][]string
	if rows, err = xlsx.GetRows(xlsx.GetSheetList()[0]); err != nil {
		return nil, err
	}
	fieldFound := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) < 2 {
			continue
		}
		name := strings.TrimSpace(row[1])
		if name == "" || name == "0" {
			continue
		}
		fmt.Printf("name '%s'\n", name)
		if name == domrfMortgageField {
			fieldFound = i - 1
		}
		if fieldFound == 0 {
			continue
		}
		if len(row) < 3 {
			break
		}
		// Колонки с месяцами и пропуском кварталов
		startColNum := 3
		for j, cell := range row[startColNum:] {
			rate := strings.TrimRight(strings.TrimSpace(cell), "%")
			if rate == "" || rate == "-" {
				continue
			}
			date := rows[fieldFound][j+startColNum]
			if date == "изм. к пред. нед." {
				continue
			}
			fmt.Printf("name %s date %v cell %s\n", name, date, cell)
			if _, err = strconv.ParseFloat(rate, 32); err != nil {
				return nil, err
			}
			//                                name           year
			*table = append(*table, []string{name, date, rate})
		}
	}

	return table, nil
}

func (s *DomrfMortgageStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	if err = conn.Exec(ctx, domrfMortgageDdl); err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, domrfMortgageInsert)
	if err != nil {
		return count, err
	}
	for _, row := range *table {
		date, err := time.Parse(domrfMortgageTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		value, _ := strconv.ParseFloat(row[2], 32)
		if err = batch.Append(row[0], date, float32(value)); err != nil {
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
	chimport.Stats = append(chimport.Stats, &DomrfMortgageStat{})
}
