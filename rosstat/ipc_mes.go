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
	"time"
)

const (
	// ToDo update data source Потребительские цены https://rosstat.gov.ru/statistics/price
	// Индексы потребительских цен на товары и услуги по Российской Федерации, месяцы (с 1991 г.)
	ipcMesTable = "ipc_mes"
	ipcMesDdl   = `CREATE TABLE IF NOT EXISTS ` + ipcMesTable + ` (
			  name LowCardinality(String)
			, date Date
			, percent Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	ipcMesInsert     = "INSERT INTO " + ipcMesTable + " VALUES (?, ?, ?)"
	ipcMesField      = "к концу предыдущего месяца"
	ipcMesYearStart  = 1991
	ipcMesTimeLayout = "2006-01"
)

type IpcMesStat struct {
}

func (s *IpcMesStat) getXlsDataUrl() (url string, err error) {
	c := colly.NewCollector()
	c.SetClient(util.HttpClient)
	c.OnHTML(".row > div:nth-child(3) div.toggle-section__content.toggle-section__content--open > div > div > div > div:nth-child(1) > div > div.toggle-card__main > div > div > div > div:nth-child(3) > div.document-list__item-link > a", func(e *colly.HTMLElement) {
		url = fmt.Sprintf("%s%s", rosstatUrl, e.Attr("href"))
		log.Infof("href url %s", url)
	})
	if err = c.Visit(fmt.Sprintf("%s/statistics/price", rosstatUrl)); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return url, nil
}

func (s *IpcMesStat) Name() string {
	return ipcMesTable
}

func (s *IpcMesStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	var xlsDataUrl string
	if xlsDataUrl, err = s.getXlsDataUrl(); err != nil {
		return nil, err
	}
	if xlsx, err = util.GetXlsx(xlsDataUrl); err != nil {
		return nil, err
	}
	table = new([][]string)
	for _, sheet := range xlsx.GetSheetList() {
		var rows [][]string
		if _, err = strconv.Atoi(sheet); err != nil {
			continue
		}
		if rows, err = xlsx.GetRows(sheet); err != nil {
			return nil, err
		}
		fieldFound := false
		mes := 0
		for _, row := range rows {
			if fieldFound {
				mes += 1
				if len(row) < 2 || mes > 12 {
					break
				}
				for i, cell := range row[1:] {
					if cell == "" {
						continue
					}
					if _, err = strconv.ParseFloat(cell, 32); err != nil {
						return nil, err
					}
					//                                name           year
					*table = append(*table, []string{rows[0][0], strconv.Itoa(ipcMesYearStart + i), fmt.Sprintf("%02d", mes), cell})
				}
			} else if row[0] == ipcMesField {
				fieldFound = true
			}
		}
	}
	return table, nil
}

func (s *IpcMesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, ipcMesDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		mes, err := time.Parse(ipcMesTimeLayout, fmt.Sprintf("%s-%s", row[1], row[2]))
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, ipcMesInsert, row[0], mes.AddDate(0, 1, -1), row[3]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &IpcMesStat{})
}
