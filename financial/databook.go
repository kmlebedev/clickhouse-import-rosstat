package financial

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
	"slices"
	"strconv"
	"strings"
	"time"
)

type DataBook interface {
	Import(conn *sql.DB, sheetUrl string, date time.Time, standard string, double bool) error
}

//	type FinDataBook interface {
//		Name() string
//		Import(ctx context.Context, conn driver.Conn) (count int64, err error)
//	}
type FinDataBook struct {
	name         string
	createTable  string
	insertRow    string
	dataBookPath string
	tables       map[string][]string
}

func (s *FinDataBook) Name() string {
	return s.name
}

func (f *FinDataBook) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, fmt.Sprintf(f.createTable, f.name)); err != nil {
		return 0, err
	}
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", f.name))
	if err != nil {
		return count, err
	}
	xlsx, err := excelize.OpenFile(f.dataBookPath)
	if err != nil {
		return 0, err
	}
	for sheet, tables := range f.tables {
		fmt.Printf("Import sheet %s\n", sheet)
		rows, err := xlsx.GetRows(sheet)
		if err != nil {
			return 0, err
		}
		var table string
		var dateRowIdx int
		for i, row := range rows {
			if len(row) == 0 || row[0] == "" {
				table = ""
				continue
			}
			if slices.Contains(tables, row[0]) {
				table = row[0]
				dateRowIdx = i
				fmt.Printf("Found table %s\n", table)
				continue
			}
			if table == "" {
				continue
			}
			for j, colCell := range row[1:] {

				value, err := strconv.ParseFloat(strings.Trim(strings.ReplaceAll(colCell, " ", ""), "()"), 32)
				if err != nil {
					return count, err
				}
				if strings.HasPrefix(colCell, "(") && strings.HasSuffix(colCell, ")") {
					value = value * -1
				}
				fmt.Printf("sheet %s, table %s, name %s , date %s, value %f\n",
					sheet, table, row[0], rows[dateRowIdx][j+1], value)
				if err = batch.Append(sheet, table, row[0], fmt.Sprintf("%s-01-01", rows[dateRowIdx][j+1]), value); err != nil {
					return count, err
				}
				count++
			}
		}
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}
