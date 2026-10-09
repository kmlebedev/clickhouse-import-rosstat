package rosstat

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gocolly/colly/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
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
	// ВВП кварталы (с 1995 г.) — имя файла содержит диапазон лет (VVP_kvartal_s_1995-2025.xlsx)
	// и меняется при обновлении, поэтому ссылка берётся со страницы.
	// Валовой внутренний продукт 1) (в ценах 2021 г., млрд руб., с исключением сезонного фактора)
	vvpKvartalTable = "vvp_kvartal"
	vvpKvartalDdl   = `CREATE TABLE IF NOT EXISTS ` + vvpKvartalTable + ` (
				  name LowCardinality(String)
				, date Date
				, vvp Float32
			) ENGINE = ReplacingMergeTree ORDER BY (name, date);
		`
	vvpKvartalDdlInsert = "INSERT INTO " + vvpKvartalTable
)

// getVvpKvartalXlsDataUrl ищет ссылку на актуальный VVP_kvartal_*.xlsx на странице
// национальных счетов Росстата.
func getVvpKvartalXlsDataUrl() (url string) {
	c := colly.NewCollector()
	c.SetClient(util.HttpClient)
	c.OnHTML(`a[href*="VVP_kvartal"]`, func(e *colly.HTMLElement) {
		if url == "" {
			url = fmt.Sprintf("%s%s", rosstatUrl, e.Attr("href"))
			log.Infof("href url %s", url)
		}
	})
	if err := c.Visit(fmt.Sprintf("%s/statistics/accounts", rosstatUrl)); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return url
}

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

func parseVvpKvartal(xlsx *excelize.File) (table *[]vvpKvartal, err error) {
	table = &[]vvpKvartal{}
	var rows [][]string
	// ВВП (в ценах 2021 г., млрд руб., с исключением сезонного фактора)
	for _, sheet := range []string{"2", "9", "10", "12", "14"} {
		if rows, err = xlsx.GetRows(sheet); err != nil {
			return nil, err
		}
		tableName := fmt.Sprintf("%s %s", sheet, strings.Trim(strings.Split(rows[1][1], ")")[0], " 1"))
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
			fmt.Printf("%s kvartal %v+ cell %v\n", strings.Split(rows[3][i+1], " ")[0], getQuarterDate(int(year), strings.Split(rows[3][i+1], " ")[0]), vvp)
			*table = append(*table, vvpKvartal{
				tableName,
				getQuarterDate(int(year), strings.Split(rows[3][i+1], " ")[0]),
				vvp,
			})
		}
	}
	return table, nil
}

func (s *vvpKvartalDdlStat) export() (table *[]vvpKvartal, err error) {
	xlsDataUrl := getVvpKvartalXlsDataUrl()
	if xlsDataUrl == "" {
		return nil, fmt.Errorf("vvpKvartal: не найдена ссылка на VVP_kvartal_*.xlsx на %s/statistics/accounts", rosstatUrl)
	}
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(xlsDataUrl); err != nil {
		return nil, err
	}
	return parseVvpKvartal(xlsx)
}

func (s *vvpKvartalDdlStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, vvpKvartalDdl); err != nil {
		return count, err
	}
	var table *[]vvpKvartal
	if table, err = s.export(); err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, vvpKvartalDdlInsert)
	if err != nil {
		return count, err
	}
	for _, r := range *table {
		if err = batch.Append(r.name, r.date, float32(r.vvp)); err != nil {
			return count, err
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	if err = util.UpsertSeriesCatalog(ctx, conn, vvpKvartalSeriesMeta); err != nil {
		return count, err
	}
	if _, err = util.CreateView(ctx, conn, rosstatMacroView); err != nil {
		return count, err
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &vvpKvartalDdlStat{})
}
