package financial

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"slices"
	"strconv"
	"strings"
	"time"
)

var toData = map[string]string{
	"20": "ANNUAL",
	"Q":  "QUARTERLY",
	"H":  "SEMI-ANNUAL",
}

// https://www.polyus.com/ru/investors/results-and-reports/
func polyusTableImport(f *FinDataBook, xlsx *excelize.File, batch driver.Batch) (count int64, err error) {
	for sheet, tables := range f.tables {
		fmt.Printf("Import sheet %s\n", sheet)
		rows, err := xlsx.GetRows(sheet)
		if err != nil {
			return 0, err
		}
		log.Infof("get rows: %d", len(rows))
		var table string
		dateRowIdx := 3
		for _, row := range rows {
			if len(row) <= f.tableColNum || row[f.tableColNum] == "" {
				continue
			}
			log.Infof("Containt not found %s\n", row[f.tableColNum])
			if slices.Contains(tables, row[f.tableColNum]) {
				table = row[f.tableColNum]
				fmt.Printf("Found table %s\n", table)
			}
			if len(row) <= f.tableColNum+1 || row[f.tableColNum+1] == "" {
				continue
			}
			tableStartNum := f.tableColNum + 4
			for key, data := range toData {
				for j, colCell := range row[tableStartNum:] {
					if !strings.Contains(rows[dateRowIdx][j+tableStartNum][0:2], key) {
						continue
					}
					log.Infof("%s = %s", rows[dateRowIdx][j+tableStartNum], colCell)
					dateStr := rows[dateRowIdx][j+tableStartNum] // 2Q'07
					dateQ, err := strconv.Atoi(dateStr[0:1])
					if err != nil {
						return count, err
					}
					var date time.Time
					switch data {
					case "ANNUAL": // 2007
						date, err = time.Parse("2006-01-02", fmt.Sprintf("%s-12-01", dateStr))
					case "QUARTERLY": // 2Q'07
						date, err = time.Parse("06-01-02", fmt.Sprintf("%s-%02d-01", dateStr[len(dateStr)-2:len(dateStr)], dateQ*3))
					case "SEMI-ANNUAL": // 2H'24
						date, err = time.Parse("06-01-02", fmt.Sprintf("%s-%02d-01", dateStr[len(dateStr)-2:len(dateStr)], dateQ*6))
					}
					if err != nil {
						return count, err
					}
					if colCell == "-" || colCell == "N/A" {
						continue
					}
					value, err := strconv.ParseFloat(strings.ReplaceAll(strings.Trim(colCell, "%()"), ",", ""), 32)
					if err != nil {
						return count, err
					}
					if strings.HasPrefix(colCell, "(") && strings.HasSuffix(colCell, ")") {
						value = value * -1
					}
					fmt.Printf("sheet %s, table %s, name %s , date %v, value %f\n",
						sheet, table, row[f.tableColNum], date, value)
					if err = batch.Append(table, row[f.tableColNum], data, date.AddDate(0, 1, 0), value); err != nil {
						return count, err
					}
					count++
				}
			}
		}
	}
	return 0, err
}

func init() {
	FinDataBookPolyus := FinDataBook{
		name:         "databook_polyus",
		dataBookPath: "financial/data/polyus_datapack_1h25.xlsx",
		tables:       map[string][]string{},
		insertRow:    "INSERT INTO %s VALUES (?, ?, ?, ?, ?)",
		createTable: `CREATE TABLE IF NOT EXISTS %s (
		   table LowCardinality(String),
 		   name LowCardinality(String),
 		   data LowCardinality(String),    
		   date Date,
           value Float32
		) ENGINE = ReplacingMergeTree()
		ORDER BY (table, name, data, date)`,
		tableColNum:     1,
		tableImportFunc: polyusTableImport,
	}

	FinDataBookPolyus.tables["Sheet1"] = []string{
		"CONSOLIDATED OPERATING RESULTS",
		"OLIMPIADA",
		"BLAGODATNOYE",
		"TITIMUKHTA",
		"VERNINSKOYE**",
		"ALLUVIALS",
		"KURANAKH",
		"ZAPADNOYE",
		"NATALKA",
		"Sukhoi Log",
	}

	chimport.Stats = append(chimport.Stats, &FinDataBookPolyus)
}
