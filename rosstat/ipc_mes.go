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
	"time"
)

const (
	// ToDo update data source Потребительские цены https://rosstat.gov.ru/statistics/price
	// Индексы потребительских цен на товары и услуги по Российской Федерации, месяцы (с 1991 г.)
	rosstatIPCMesTable = "rosstat_ipc_mes"
	ipcMesDdl          = `CREATE TABLE IF NOT EXISTS %s (
				  name LowCardinality(String)
				, date Date
				, percent Float32
			) ENGINE = ReplacingMergeTree ORDER BY (name, date);
		`
	ipcMesField      = "к концу предыдущего месяца"
	ipcMesYearStart  = 1991
	ipcMesTimeLayout = "2006-01"
	// ipcMesCountPoints — количественный чек версии (спека §3.5): число точек своего ряда.
	// Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL обязателен —
	// без него count() считает несмёрженные части и растёт после каждого повторного импорта,
	// то есть чек сравнивал бы величину, меняющуюся от прогона к прогону.
	ipcMesCountPoints = "SELECT count() FROM rosstat_ipc_mes FINAL"
)

type IpcMesStat struct {
	util.ClickHouseImport
}

// getXlsDataUrl ищет ссылку на актуальный ipc_mes_*.xlsx на странице цен Росстата и
// возвращает её как есть: пустой результат — признак «ссылки нет», и вызывающий обязан
// отличить его от рабочей ссылки сам (то же устройство у соседей — rosstat_salaries_mes, rosstat_vvp_kvartal).
// Ошибка обхода страницы только логируется: решение о сбое принимает вызывающий по пустому URL.
func (s *IpcMesStat) getXlsDataUrl() (url string) {
	c := colly.NewCollector()
	c.SetClient(util.HttpClient)
	c.OnHTML(".row > div:nth-child(3) div.toggle-section__content.toggle-section__content--open > div > div > div > div:nth-child(1) > div > div.toggle-card__main > div > div > div > div:nth-child(3) > div.document-list__item-link > a", func(e *colly.HTMLElement) {
		url = fmt.Sprintf("%s%s", rosstatUrl, e.Attr("href"))
		log.Infof("href url %s", url)
	})
	if err := c.Visit(fmt.Sprintf("%s/statistics/price", rosstatUrl)); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return url
}

func parseIpcMes(xlsx *excelize.File) (table *[][]string, err error) {
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

// ipcMesImport разбирает файл и пишет точки в batch: семантика прежних export() и цикла
// вставки Import сохранена дословно — имя ряда из первой ячейки листа, дата «год-месяц»
// строки, значение — процент, дата сдвинута на 1-е число отчётного месяца (AddDate(0, 1, -1)).
// Пустой разбор ошибкой не считается — так же вёл себя прежний Import: пустой батч
// отправлялся, и прогон возвращал ноль точек (fail-fast на пустом листе — контракт cbr, не этого ряда).
func ipcMesImport(xlsx *excelize.File, batch driver.Batch) error {
	table, err := parseIpcMes(xlsx)
	if err != nil {
		return err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		mes, err := time.Parse(ipcMesTimeLayout, fmt.Sprintf("%s-%s", row[1], row[2]))
		if err != nil {
			return err
		}
		percent, err := strconv.ParseFloat(row[3], 32)
		if err != nil {
			return err
		}
		if err = batch.Append(row[0], mes.AddDate(0, 1, -1), float32(percent)); err != nil {
			return err
		}
	}
	return nil
}

// Import берёт адрес актуального xlsx со страницы цен Росстата: имени файла в коде нет
// (ipc_mes_ММ-ГГГГ.xlsx меняется при каждом обновлении), а сетевое обращение в init()
// запрещено (ARCHITECTURE.md §8.6). Пустой URL — сразу ошибка, как у соседей (rosstat_salaries_mes,
// rosstat_vvp_kvartal): иначе прогон дошёл бы до GetXlsx("") и упал невнятно. Дальше — полный цикл
// шаблона: DDL → префлайт по HEAD (файл обновляется раз в месяц, шаг расписания — недельный)
// → точки → производные (AfterImport) → строка версии.
func (s *IpcMesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	xlsDataUrl := s.getXlsDataUrl()
	if xlsDataUrl == "" {
		return count, fmt.Errorf("ipcMes: не найдена ссылка на ipc_mes_*.xlsx на %s/statistics/price", rosstatUrl)
	}
	s.DataUrl = xlsDataUrl
	return s.ClickHouseImport.Import(ctx, conn)
}

// ipcMesImporter — помесячные ИПЦ Росстата; регистрируется реестром rosstat/importers.go.
var ipcMesImporter = &IpcMesStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatIPCMesTable,
	CreateTable:            []string{ipcMesDdl},
	ImportFunc:             ipcMesImport,
	AfterImport:            afterRosstatMacro(ipcMesSeriesMeta),
	PublicationCountPoints: ipcMesCountPoints,
}}
