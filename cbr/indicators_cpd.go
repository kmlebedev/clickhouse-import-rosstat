package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gocolly/colly/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Todo update data source https://www.cbr.ru/statistics/ddkp/aipd/
// Показатели сезонно сглаженной динамики потребительских цен
// const indicatorsCpdDataUrl = "https://www.cbr.ru/Content/Document/File/108632/indicators_cpd.xlsx"

// getIndicatorsCpdXlsDataUrl ищет ссылку на актуальный XLSX на странице
// показателей сезонно сглаженной динамики потребительских цен. Вызывается
// из Import(): сетевые вызовы в init() запрещены (ARCHITECTURE.md §8.6).
func getIndicatorsCpdXlsDataUrl() (url string) {
	c := colly.NewCollector()
	c.SetClient(util.HttpClient)
	c.OnHTML(".container-fluid > div > div:nth-child(6) > div > div.body-2.document-regular_main > div > div > a.referenceable", func(e *colly.HTMLElement) {
		url = fmt.Sprintf("%s%s", "https://www.cbr.ru", e.Attr("href"))
		log.Infof("href url %s", url)
	})
	if err := c.Visit("https://www.cbr.ru/statistics/ddkp/aipd/"); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return url
}

const indicatorsCpdUrl = "https://www.cbr.ru/statistics/ddkp/aipd/"

type indicatorsCpdStat struct {
	util.ClickHouseImport
}

func (s *indicatorsCpdStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if s.DataUrl = getIndicatorsCpdXlsDataUrl(); s.DataUrl == "" {
		return count, fmt.Errorf("indicatorsCpd: не найдена ссылка на xlsx на %s", indicatorsCpdUrl)
	}
	return s.ClickHouseImport.Import(ctx, conn)
}

func init() {
	indicatorsCpd := indicatorsCpdStat{ClickHouseImport: util.ClickHouseImport{
		TableName: "cbr_indicators_cpd",
		CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`},
		ImportFunc: indicatorsCpdImport,
	}}
	chimport.Stats = append(chimport.Stats, &publishedStat{ImportStat: &indicatorsCpd, meta: cbrIndicatorsCpdSeriesMeta})
}

// https://www.cbr.ru/analytics/dkp/dinamic/
func indicatorsCpdImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("Лист1")
	if err != nil {
		return err
	}
	fileds := []string{"Все товары и услуги", "Базовый ИПЦ"}
	for i, row := range rows {
		if len(row) < 1 || !slices.Contains(fileds, strings.TrimSpace(row[0])) {
			continue
		}
		for j, rowCol := range rows[i][2:] {
			//fmt.Printf("name: %s rowCol: %+v, date: %s\n", row[0], rowCol, rows[0][j+2])
			date, err := time.Parse("01/06", rows[0][j+2])
			if err != nil {
				return err
			}
			value, err := strconv.ParseFloat(rowCol, 64)
			if err != nil {
				return err
			}
			if err = batch.Append(row[0], date.AddDate(0, 1, -1), value); err != nil {
				return err
			}
		}
	}
	return nil
}
