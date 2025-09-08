package rosstat

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

func getQuarterDate(year int, quarter string) time.Time {
	var startMonth time.Month
	switch quarter {
	case "I":
		startMonth = time.January
	case "II":
		startMonth = time.April
	case "III":
		startMonth = time.July
	case "IV":
		startMonth = time.October
	default:
		panic("Invalid quarter")
	}
	return time.Date(year, startMonth+3, 1, 0, 0, 0, 0, time.UTC)
}

const (
	// Национальные счета https://rosstat.gov.ru/statistics/accounts
	// ВВП кварталы (с 1995 г.) https://rosstat.gov.ru/storage/mediabank/VVP_kvartal_s1995-2025.xlsx
	// Валовой внутренний продукт 1) (в ценах 2021 г., млрд руб., с исключением сезонного фактора)
	vvpKvartalXlsDataUrl = rosstatUrl + "/VVP_kvartal_s1995-2025.xlsx"
	vvpKvartalTable      = "vvp_kvartal"
	vvpKvartalDdl        = `CREATE TABLE IF NOT EXISTS ` + vvpKvartalTable + ` (
			  name LowCardinality(String)
			, date Date
			, vvp Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	vvpKvartalDdlInsert = "INSERT INTO " + vvpKvartalTable + " VALUES (?, ?, ?)"
)

type vvpKvartalDdlStat struct {
}

type vvpKvartal struct {
	name string
	date time.Time
	vvp  float64
}

func (s *vvpKvartalDdlStat) Name() string {
	return vvpKvartalTable
}

func (s *vvpKvartalDdlStat) export() (table *[]vvpKvartal, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(vvpKvartalXlsDataUrl); err != nil {
		return nil, err
	}
	table = &[]vvpKvartal{}
	var rows [][]string
	// ВВП (в ценах 2021 г., млрд руб., с исключением сезонного фактора)
	for _, sheet := range []string{"2", "12", "14"} {
		if rows, err = xlsx.GetRows(sheet); err != nil {
			return nil, err
		}
		tableName := strings.Trim(strings.Split(rows[1][1], ")")[0], " 1")
		fmt.Printf("table name %s\n", tableName)
		// Строки с годами(2011) 2 и кварталами(I квартал) 3
		// Колонки со ВВП
		var year int64
		var vvp float64
		for i, cell := range rows[4][1:] {
			if cell == "" {
				break
			}
			if len(rows[2]) > i+1 && len(rows[2][i+1]) >= 4 {
				if year, err = strconv.ParseInt(rows[2][i+1][0:4], 10, 16); err != nil {
					return nil, err
				}
			}
			vvpStr := strings.ReplaceAll(strings.ReplaceAll(cell, " ", ""), ",", "")
			if vvp, err = strconv.ParseFloat(vvpStr, 32); err != nil {
				return nil, err
			}
			//fmt.Printf("%s kvartal %v+ cell %v\n", strings.Split(rows[3][i+1], " ")[0], getQuarterDate(int(year), strings.Split(rows[3][i+1], " ")[0]), vvp)
			*table = append(*table, vvpKvartal{
				tableName,
				getQuarterDate(int(year), strings.Split(rows[3][i+1], " ")[0]),
				vvp,
			})
		}
	}
	return table, nil
}

func (s *vvpKvartalDdlStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, vvpKvartalDdl); err != nil {
		return count, err
	}
	var table *[]vvpKvartal
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, r := range *table {
		if err = conn.Exec(ctx, vvpKvartalDdlInsert, r.name, r.date, r.vvp); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &vvpKvartalDdlStat{})
}
