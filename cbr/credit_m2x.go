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
	"strconv"
	"strings"
	"time"
)

// Денежно-кредитная и финансовая статистика https://www.cbr.ru/statistics/macro_itm/dkfs/
// Приложение к материалу «Кредит экономике и денежная масса»
const cbrCreditM2xUrl = "https://www.cbr.ru/statistics/macro_itm/dkfs/"

// getCbrCreditM2xXlsDataUrl ищет ссылку на актуальный credit_m2x.xlsx на странице
// денежно-кредитной статистики. Id файла в URL (/Content/Document/File/NNNNN/) меняется
// при обновлении, поэтому ссылку берём со страницы, а не хардкодим.
func getCbrCreditM2xXlsDataUrl() (url string) {
	c := colly.NewCollector(colly.UserAgent(util.HttpUA))
	c.SetClient(util.HttpClient)
	c.OnHTML(`a.referenceable[href$="credit_m2x.xlsx"]`, func(e *colly.HTMLElement) {
		url = fmt.Sprintf("%s%s", cbrUrl, e.Attr("href"))
		log.Infof("href url %s", url)
	})
	if err := c.Visit(cbrCreditM2xUrl); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return url
}

var cbrСreditM2x = util.ClickHouseImport{
	TableName: "cbr_credit_m2x",
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`},
	ImportFunc: func(xlsx *excelize.File, batch driver.Batch) (err error) {
		var rows [][]string
		if rows, err = xlsx.GetRows("млн рублей"); err != nil {
			return err
		}
		// Строки с годами
		for _, row := range rows[1:] {
			if len(row) == 0 {
				continue
			}
			name := strings.TrimSpace(row[0])
			if name == "" {
				break
			}
			for j, cell := range row[1:] {
				if j+1 >= len(rows[0]) {
					break
				}
				date, err := time.Parse("01-02-06", strings.TrimSpace(rows[0][j+1]))
				if err != nil {
					return err
				}
				valueStr := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
				if valueStr == "" {
					continue
				}
				if value, err := strconv.ParseFloat(valueStr, 32); err != nil {
					return err
				} else {
					if err = batch.Append(name, date, value); err != nil {
						return err
					}
				}
			}
		}
		return nil
	},
}

type cbrCreditM2xStat struct {
	util.ClickHouseImport
}

func (s *cbrCreditM2xStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if s.DataUrl = getCbrCreditM2xXlsDataUrl(); s.DataUrl == "" {
		return count, fmt.Errorf("cbrCreditM2x: не найдена ссылка на credit_m2x.xlsx на %s", cbrCreditM2xUrl)
	}
	return s.ClickHouseImport.Import(ctx, conn)
}

func init() {
	chimport.Stats = append(chimport.Stats, &publishedStat{ImportStat: &cbrCreditM2xStat{ClickHouseImport: cbrСreditM2x}, meta: cbrCreditM2xSeriesMeta})
}
