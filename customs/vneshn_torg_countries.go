package customs

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gocolly/colly/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

// https://customs.gov.ru/statistic/vneshn-torg/vneshn-torg-countries

const (
	customsVneshnTorgName = "customs_vneshn_torg"
	customsVneshnTorgUrl  = "https://customs.gov.ru"
)

type CustomsVneshnTorg struct {
	util.ClickHouseImport
}

var customsVneshnTorg = util.ClickHouseImport{
	TableName: customsVneshnTorgName,
	CreateTable: `CREATE TABLE IF NOT EXISTS %s (
    		  name LowCardinality(String)
			, date Date
    		, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`,
	DataUrl:  fmt.Sprintf("%s/statistic/vneshn-torg/vneshn-torg-countries", customsVneshnTorgUrl),
	CrawFunc: CustomsVneshnTorgCraw,
}

var ctx = context.TODO()

func CustomsVneshnTorgImport(dataUrl string, conn driver.Conn) error {
	xlsx, err := util.GetXlsx(dataUrl)
	if err != nil {
		return err
	}
	defer xlsx.Close()
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", customsVneshnTorgName))
	if err != nil {
		return err
	}
	sheetName := xlsx.GetSheetList()[0]
	rows, err := xlsx.GetRows(sheetName)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("sheetName %s %d rows affected with %d", sheetName, len(rows), len(rows[3]))
	tableRowNum := 0
	for n, row := range rows {
		if len(row) == 0 || row[0] == "" || len(row) < 5 || row[1] == "" {
			continue
		}
		if tableRowNum == 0 && row[0] == "Всего" {
			tableRowNum = n
		}
		for i, cell := range row[1:] {
			if strings.TrimSpace(cell) == "" {
				continue
			}
			value, err := strconv.ParseFloat(strings.TrimSpace(cell), 16)
			if err != nil {
				log.Error(err)
				continue
			}
			var fieldN int
			if i > 2 {
				fieldN = 4
			} else {
				fieldN = 1
			}
			name := fmt.Sprintf("%s/%s", rows[tableRowNum-2][fieldN], row[0])
			dateArr := strings.Split(rows[tableRowNum-1][i+1], " ")
			if dateArr[len(dateArr)-1] == "%" {
				continue
			}
			dateYear, err := strconv.Atoi(dateArr[len(dateArr)-1])
			if err != nil {
				log.Error(err)
				continue
			}
			dataMonthArr := strings.Split(dateArr[len(dateArr)-2], "-")
			var dateMonth string
			switch len(dataMonthArr) {
			case 1:
				dateMonth = dataMonthArr[0]
			case 2:
				dateMonth = dataMonthArr[1]
			}
			date := time.Date(dateYear, util.MonthsToNum[dateMonth]+1, -1, 0, 0, 0, 0, time.UTC)
			//log.Infof("name: %s date: %v, value: %f", name, date, value)
			if err = batch.Append(name, date, value); err != nil {
				log.Error(err)
				return err
			}
		}
	}
	if err = batch.Send(); err != nil {
		return err
	}
	return nil
}

func CustomsVneshnTorgCraw(crawUrl string, conn driver.Conn) (err error) {
	c := colly.NewCollector(colly.UserAgent(util.HttpUA))
	c.SetRequestTimeout(5 * time.Second)
	var dataUrl string
	c.OnHTML("div.file-download__item-file > .file-download__item-link > a", func(e *colly.HTMLElement) {
		dataUrl = fmt.Sprintf("%s%s", customsVneshnTorgUrl, e.Attr("href"))
		log.Infof("Customs VneshnTorg: dataUrl: %s", dataUrl)
		if strings.Contains(dataUrl, "структура") {
			if err = CustomsVneshnTorgImport(dataUrl, conn); err != nil {
				log.Errorf("Import err: %v+", err)
			}
		}
	})
	c.OnHTML(".pagination__links .pagination__link:not(.active) > a", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		log.Infof("Do visit pagination: dataUrl: %s", link)
		if err = e.Request.Visit(link); err != nil {
			log.Errorf("visit err: %v+", err)
		}
	})
	log.Infof("Customs visit: dataUrl: %s", dataUrl)
	if err = c.Visit(crawUrl); err != nil {
		log.Errorf("First visit err: %v+", err)
		return err
	}
	c.Wait()
	return nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &customsVneshnTorg)
}
