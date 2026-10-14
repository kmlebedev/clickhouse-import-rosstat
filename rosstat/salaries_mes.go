package rosstat

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gocolly/colly/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

const (
	// Рынок труда, занятость и заработная плата https://rosstat.gov.ru/labor_market_employment_salaries
	// Среднемесячная номинальная начисленная заработная плата работников в целом по экономике Российской Федерации
	// Имя файла содержит месяц и год (tab1-zpl_ММ-ГГГГ.xlsx) и меняется при обновлении,
	// поэтому ссылка берётся со страницы.
	rosstatSalariesMesTable = "rosstat_salaries_mes"
	salariesMesDdl          = `CREATE TABLE IF NOT EXISTS %s (
				  name LowCardinality(String)
				, date Date
				, salary Float32
			) ENGINE = ReplacingMergeTree ORDER BY (name, date);
		`
	salariesMesField      = "1991"
	salariesMesYearStart  = 1991
	salariesMesTimeLayout = "2006-01"
	// salariesMesCountPoints — количественный чек версии (спека §3.5): число точек своего
	// ряда. Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL
	// обязателен — иначе count() считал бы несмёрженные части.
	salariesMesCountPoints = "SELECT count() FROM rosstat_salaries_mes FINAL"
)

// getSalariesMesXlsDataUrl ищет ссылку на актуальный tab1-zpl_*.xlsx на странице
// рынка труда Росстата.
func getSalariesMesXlsDataUrl() (url string) {
	c := colly.NewCollector()
	c.SetClient(util.HttpClient)
	c.OnHTML(`a[href*="tab1-zpl"]`, func(e *colly.HTMLElement) {
		if url == "" {
			url = fmt.Sprintf("%s%s", rosstatUrl, e.Attr("href"))
			log.Infof("href url %s", url)
		}
	})
	if err := c.Visit(fmt.Sprintf("%s/labor_market_employment_salaries", rosstatUrl)); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return url
}

type SalariesMesStat struct {
	util.ClickHouseImport
}

func parseSalariesMes(xlsx *excelize.File) (table *[][]string, err error) {
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

// salariesMesImport разбирает файл и пишет точки в batch: семантика прежних export() и цикла
// вставки Import сохранена дословно — имя ряда из первой ячейки листа, дата «год-месяц»
// строки, значение — зарплата, дата сдвинута на 1-е число месяца, следующего за отчётным
// (AddDate(0, 1, 0)).
func salariesMesImport(xlsx *excelize.File, batch driver.Batch) error {
	table, err := parseSalariesMes(xlsx)
	if err != nil {
		return err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		mes, err := time.Parse(salariesMesTimeLayout, fmt.Sprintf("%s-%s", row[1], row[2]))
		if err != nil {
			return err
		}
		salary, err := strconv.ParseFloat(row[3], 32)
		if err != nil {
			return err
		}
		if err = batch.Append(row[0], mes.AddDate(0, 1, 0), float32(salary)); err != nil {
			return err
		}
	}
	return nil
}

// Import берёт адрес актуального xlsx со страницы рынка труда Росстата: имя файла содержит
// месяц и год (tab1-zpl_ММ-ГГГГ.xlsx) и меняется при обновлении, а сетевое обращение в
// init() запрещено (ARCHITECTURE.md §8.6). Дальше — полный цикл шаблона: DDL → префлайт по
// HEAD (файл обновляется раз в месяц, шаг расписания — недельный) → точки → производные
// (AfterImport) → строка версии.
func (s *SalariesMesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	xlsDataUrl := getSalariesMesXlsDataUrl()
	if xlsDataUrl == "" {
		return count, fmt.Errorf("salariesMes: не найдена ссылка на tab1-zpl_*.xlsx на %s/labor_market_employment_salaries", rosstatUrl)
	}
	s.DataUrl = xlsDataUrl
	return s.ClickHouseImport.Import(ctx, conn)
}

// salariesMesImporter — среднемесячная начисленная зарплата по РФ; регистрируется реестром rosstat/importers.go.
var salariesMesImporter = &SalariesMesStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatSalariesMesTable,
	CreateTable:            []string{salariesMesDdl},
	ImportFunc:             salariesMesImport,
	AfterImport:            afterRosstatMacro(salariesMesSeriesMeta),
	PublicationCountPoints: salariesMesCountPoints,
}}
