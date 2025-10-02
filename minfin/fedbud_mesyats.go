package minfin

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Краткая ежемесячная информация об исполнении консолидированного бюджета Российской Федерации и государственных внебюджетных фондов (млрд. руб., накоплено с начала года)
// https://minfin.gov.ru/ru/document?id_4=93447-kratkaya_ezhemesyachnaya_informatsiya_ob_ispolnenii_konsolidirovannogo_byudzheta_rossiiskoi_federatsii_i_gosudarstvennykh_vnebyudzhetnykh_fondov_mlrd._rub._nakopleno_s_nachala_goda
// https://minfin.gov.ru/common/upload/library/2025/08/main/Prilozhenie_8_dannye_115-117_%E2%80%94_mesyats.xlsx
// https://minfin.gov.ru/common/upload/library/2025/10/main/Prilozhenie_8_dannye_115-117_%E2%80%94_mesyats.xlsx
func init() {
	FedbudMesyats := util.HdBase{
		TableName: "minfin_fed_bud_mesyats",
		DataUrl:   "https://minfin.gov.ru/common/upload/library/2025/10/main/Prilozhenie_8_dannye_115-117_%E2%80%94_mesyats.xlsx",
		CreateTable: `CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`,
		ImportFunc: fedbudMesyatsImport,
	}
	chimport.Stats = append(chimport.Stats, &FedbudMesyats)
}

func fedbudMesyatsImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("месяц")
	if err != nil {
		return err
	}
	tableIsFoundRowNum := -1
	var valuePrev, value float64
	fields := []string{"Доходы, всего", "Расходы, всего", "Акцизы", "Национальная оборона", "Дефицит (-)/Профицит (+)"}
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
				fmt.Printf("rowCol: %+v, date: %s\n", rowCol, dateStr)
				dateArr := strings.Split(dateStr, " ")
				if len(dateArr) > 1 {
					dateStr = dateReplacer.Replace(dateArr[0])
				}
				date, err = time.Parse("Jan-06", dateStr)
				if err != nil {
					return err
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
