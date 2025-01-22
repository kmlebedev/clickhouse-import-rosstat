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

	// https://www.vtb.ru/ir/statements/results/
	// https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-october-2024.xlsx
	vtbIfrsUrl      = "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/"
	vtbIfrsTable    = "rus_vtb_group_ifrs"
	vtbIfrsTableDdl = `CREATE TABLE IF NOT EXISTS ` + vtbIfrsTable + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	vtbIfrsDataInsert = "INSERT INTO " + vtbIfrsTable + " VALUES (?, ?, ?)"
	vtbIfrsDataField  = "Денежные средства и краткосрочные активы"
	vtbIfrsTimeLayout = "01-02-06"
)

var (
	vtbIfrsXlsData = []string{
		"statements/results/RUS-vtb-group-ifrs-as-of-31-May-2023.xlsx",
		"statements/results/RUS-vtb-group-ifrs-as-of-31-July-2023.xlsx",
		"statements/results/RUS-vtb-group-ifrs-as-of-31-August-2023.xlsx",
		"statements/results/RUS-vtb-group-ifrs-as-of-31-October-2023.xlsx",
		"statements/results/RUS-vtb-group-ifrs-as-of-30-November-2023.xlsx",
		"statements/results/RUS-vtb-group-ifrs-as-of-29-Feb-2024_fin.xlsx",
		"financial-results/ifrs-financial-results/RUS-vtb-group-ifrs-as-of-30-Apr-2024.xlsx",
		"financial-results/ifrs-financial-results/RUS-vtb-group-ifrs-as-of-31-May-2024.xlsx",
		"financial-results/ifrs-financial-results/RUS-vtb-group-ifrs-as-of-31-July-2024.xlsx",
		"financial-results/ifrs-financial-results/RUS-vtb-group-ifrs-as-of-31-August-2024.xlsx",
		"statements/results/rus-vtb-group-ifrs-as-of-31-october-2024.xlsx",
		"statements/results/rus-vtb-group-ifrs-as-of-30-november-2024.xlsx",
		// "Financial_data_supplement_3Q2024_RUS.xls",
		// "Financial_data_supplement_2Q2024_RUS_30072024.xls",
	}
)

type VtbIfrs struct {
}

func (s *VtbIfrs) Name() string {
	return vtbIfrsTable
}

func (s *VtbIfrs) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	table = new([][]string)
	for _, xlsName := range vtbIfrsXlsData {
		if xlsx, err = util.GetXlsx(vtbIfrsUrl + xlsName); err != nil {
			return nil, fmt.Errorf("Get xlsx %s failed: %v", xlsName, err)
		}
		var rows [][]string
		if rows, err = xlsx.GetRows("Ключевые балансовые показатели"); err != nil {
			return nil, err
		}
		xlsx.Close()
		fieldFound := 0
		// Строки с годами
		for i, row := range rows {
			if len(row) == 0 {
				continue
			}
			// fmt.Printf("file %s row: %s\n", xlsName, row[0])
			if strings.TrimSpace(row[0]) == vtbIfrsDataField {
				fieldFound = i - 1
			}
			if fieldFound == 0 {
				continue
			}
			if len(row) < 2 {
				break
			}
			if strings.TrimSpace(row[1]) == "" || strings.TrimSpace(row[1]) == "0" {
				continue
			}
			// Колонки с месяцами и пропуском кварталов
			for j, cell := range row[1:] {
				if cell == "" {
					continue
				}
				if j+1 >= len(rows[fieldFound]) {
					break
				}
				if strings.HasPrefix(strings.TrimSpace(rows[fieldFound][j+1]), "изменение") {
					continue
				}
				name := strings.TrimSpace(strings.ReplaceAll(row[0], "-", ""))
				balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
				//fmt.Printf("name %s date %v balance %s\n", name, rows[fieldFound][j+1], balance)
				if _, err = strconv.ParseFloat(balance, 32); err != nil {
					return nil, err
				}

				*table = append(*table, []string{name, rows[fieldFound][j+1], balance})
			}
			if row[0] == "Итого собственные средства" {
				break
			}
		}
	}

	return table, nil
}

func (s *VtbIfrs) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, vtbIfrsTableDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		date, err := time.Parse(vtbIfrsTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, vtbIfrsDataInsert, row[0], date, row[2]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &VtbIfrs{})
}
