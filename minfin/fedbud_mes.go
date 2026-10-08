package minfin

import (
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

// https://minfin.gov.ru/ru/statistics/fedbud/execute?id_57=80042-kratkaya_ezhemesyachnaya_informatsiya_ob_ispolnenii_federalnogo_byudzheta_mlrd._rub._nakopleno_s_nachala_goda
// Краткая ежемесячная информация об исполнении федерального бюджета (млрд. руб., накоплено с начала года)
// Table https://minfin.gov.ru/common/upload/library/2025/08/main/Prilozhenie_3_dannye_109-111_%E2%80%94_mes.xlsx
const fedbudDataPath = "/ru/statistics/fedbud/execute"

func getFedbudMestDataUrl() (url string) {
	c := colly.NewCollector(colly.UserAgent(util.HttpUA))
	c.SetClient(util.HttpClient)
	c.OnHTML(`.document_list a[href$="_mes.xlsx"]`, func(e *colly.HTMLElement) {
		url = fmt.Sprintf("%s%s", minfinUrl, e.Attr("href"))
		log.Infof("href url %s", url)
	})
	if err := c.Visit(fmt.Sprintf("%s%s", minfinUrl, fedbudDataPath)); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return url
}

func init() {
	Fedbud := util.HdBase{
		TableName: "minfin_fed_bud_mes",
		DataUrl:   getFedbudMestDataUrl(),
		CreateTable: `CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`,
		ImportFunc: fedBudImport,
	}
	chimport.Stats = append(chimport.Stats, &Fedbud)
}

var dateReplacer = strings.NewReplacer(".", "-", "янв", "Jan", "фев", "Feb", "апр", "Apr", "июн", "Jun", "июл", "Jul", "сен", "Sep", "ноя", "Nov", "авг", "Aug", "дек", "Dec")

func fedBudImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("месяц")
	if err != nil {
		return err
	}
	tableIsFoundRowNum := -1
	var valuePrev, value float64
	fields := []string{"Доходы, всего", "Расходы, всего", "Акцизы", "Национальная оборона", "привлечение"}
	for i, row := range rows {
		if len(row) < 3 || row[1] == "" {
			continue
		}
		if row[1] == "Показатель" {
			tableIsFoundRowNum = i
			continue
		}
		if tableIsFoundRowNum < 0 {
			continue
		}
		//fmt.Printf("rowdate : %+v\n", rows[tableIsFoundRowNum])
		if slices.Contains(fields, row[1]) {
			for j, rowCol := range row[2:] {
				var date time.Time
				dateStr := rows[tableIsFoundRowNum][j+2]
				//fmt.Printf("rowCol: %+v, date: %s\n", rowCol, dateStr)
				dateArr := strings.Split(dateStr, " ")
				if len(dateArr) > 1 {
					dateStr = dateReplacer.Replace(dateArr[0])
				}
				date, err = time.Parse("Jan-06", dateStr)
				if err != nil {
					return fmt.Errorf("parse %s, err: %v", dateStr, err)
				}
				valueNew, err := strconv.ParseFloat(strings.ReplaceAll(rowCol, ",", ""), 16)
				if dateStr[0:3] == "Jan" {
					value = valueNew
				} else {
					value = valueNew - valuePrev
				}
				valuePrev = valueNew
				if err != nil {
					return err
				}
				if err = batch.Append(row[1], date, value); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
