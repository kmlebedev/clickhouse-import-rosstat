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

const (
	// ToDo update data source https://rosstat.gov.ru/labor_market_employment_salaries Рынок труда, занятость и заработная плата
	// Среднемесячная номинальная начисленная заработная плата работников в целом по экономике Российской Федерации в 1991-2025 гг.
	salariesMesXlsDataUrl = rosstatMediaBankUrl + "/tab1-zpl_08-2025.xlsx"
	salariesMesTable      = "salaries_mes"
	salariesMesDdl        = `CREATE TABLE IF NOT EXISTS ` + salariesMesTable + ` (
			  name LowCardinality(String)
			, date Date
			, salary Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	salariesMesInsert     = "INSERT INTO " + salariesMesTable + " VALUES (?, ?, ?)"
	salariesMesField      = "1991"
	salariesMesYearStart  = 1991
	salariesMesTimeLayout = "2006-01"
)

type SalariesMesStat struct {
}

func getIndicatorsCpdXlsDataUrl() (url string, err error) {
	c := colly.NewCollector()
	c.SetClient(util.HttpClient)
	c.OnHTML(".main > section:nth-child(2) > div > div > div > div > div > div > div.col-lg-8.order-1.order-lg-1 > div > div:nth-child(2) > div > div.toggle-section__content.toggle-section__content--open > div > div > div > div:nth-child(1) > div > div.toggle-card__main > div > div > div > div:nth-child(1) > div.document-list__item-link > a", func(e *colly.HTMLElement) {
		url = fmt.Sprintf("%s%s", rosstatUrl, e.Attr("href"))
		log.Infof("href url %s", url)
	})
	if err = c.Visit(fmt.Sprintf("%s/labor_market_employment_salaries", rosstatUrl)); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return url, nil
}

func (s *SalariesMesStat) Name() string {
	return salariesMesTable
}

func (s *SalariesMesStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(salariesMesXlsDataUrl); err != nil {
		return nil, err
	}
	table = new([][]string)
	for _, sheet := range xlsx.GetSheetList() {
		var rows [][]string
		if rows, err = xlsx.GetRows(sheet); err != nil {
			return nil, err
		}
		fieldFound := 0
		// Строки с годами
		for i, row := range rows {
			if len(row) == 0 {
				continue
			}
			if row[0] == salariesMesField {
				fieldFound = i
			}
			if fieldFound == 0 {
				continue
			}
			if len(row) < 7 {
				break
			}
			mes := 0
			// Колонки с месяцами и пропуском кварталов
			for _, cell := range row[6:] {
				mes += 1
				if cell == "" {
					continue
				}
				// fmt.Printf("year %d mes %02d cell %s\n", salariesMesYearStart+i-fieldFound, mes, cell)
				salary := strings.Split(cell, "(")[0]
				if _, err = strconv.ParseFloat(salary, 32); err != nil {
					return nil, err
				}
				//                                name           year
				*table = append(*table, []string{rows[0][0], strconv.Itoa(salariesMesYearStart + i - fieldFound), fmt.Sprintf("%02d", mes), salary})
			}
		}
	}
	return table, nil
}

func (s *SalariesMesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, salariesMesDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		mes, err := time.Parse(salariesMesTimeLayout, fmt.Sprintf("%s-%s", row[1], row[2]))
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, salariesMesInsert, row[0], mes.AddDate(0, 1, 0), row[3]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &SalariesMesStat{})
}
