package cbr

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

	// Показатель сбережений сектора «Домашние хозяйства» https://www.cbr.ru/statistics/macro_itm/households/
	// https://www.cbr.ru/vfs/statistics/households/households_b.xlsx
	cbrQueriesUrl    = "https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/%d?FromDate=%s&ToDate=%s&posted=False"
	cbrQueriesTpl    = "cbr_queries_%s"
	cbrQueriesDdlTpl = `CREATE TABLE IF NOT EXISTS ` + cbrQueriesTpl + ` (
			  name LowCardinality(String)
			, date Date
			, values Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	cbrQueriesInsertTpl  = "INSERT INTO " + cbrQueriesTpl + " VALUES (?, ?, ?)"
	cbrQueriesField      = "DT"
	cbrQueriesTimeLayout = "01-02-06"
	cbrQueriesDateLayout = "02/01/2006"
)

type cbrQueriesDataset struct {
	DatasetId   uint16
	DatasetName string
	FromDate    string
}

func (s *cbrQueriesDataset) Name() string {
	return householdsBMesTable
}

func (s *cbrQueriesDataset) getDataUrl() string {
	// https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/132934?FromDate=06%2F29%2F2024&ToDate=12%2F26%2F2024&posted=False
	return fmt.Sprintf(cbrQueriesUrl, s.DatasetId, s.FromDate, time.Now().Format(cbrQueriesDateLayout))
}

func (s *cbrQueriesDataset) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(s.getDataUrl()); err != nil {
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
		if len(row) == 0 {
			continue
		}
		fmt.Printf("name '%s'\n", row[0])
		if strings.TrimSpace(row[0]) == cbrQueriesField {
			fieldFound = i
			continue
		}
		if fieldFound == 0 {
			continue
		}
		if len(row) < 1 {
			break
		}
		date := strings.TrimSpace(row[0])
		if date == "" {
			continue
		}
		// Колонки с месяцами и пропуском кварталов
		value := strings.ReplaceAll(strings.TrimSpace(row[1]), ",", ".")
		if value == "" {
			continue
		}
		fmt.Printf("name %s date %v cell %s\n", row[0], value)
		if _, err = strconv.ParseFloat(value, 32); err != nil {
			return nil, err
		}
		//                                name           year
		*table = append(*table, []string{row[0], date, value})

	}

	return table, nil
}

func (s *cbrQueriesDataset) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, householdsBMesDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		date, err := time.Parse(householdsBMesTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, householdsBMesInsert, row[0], date, row[2]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	for _, dataSet := range []cbrQueriesDataset{{DatasetId: 0, DatasetName: "infl", FromDate: "01/01/2021"}} {
		chimport.Stats = append(chimport.Stats, &dataSet)
	}
}
