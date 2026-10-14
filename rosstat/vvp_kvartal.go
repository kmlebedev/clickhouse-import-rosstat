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
	rosstatVVPKvartalTable = "rosstat_vvp_kvartal"
	vvpKvartalDdl          = `CREATE TABLE IF NOT EXISTS %s (
				  name LowCardinality(String)
				, date Date
				, vvp Float32
			) ENGINE = ReplacingMergeTree ORDER BY (name, date);
		`
	// vvpKvartalCountPoints — количественный чек версии (спека §3.5): число точек своего
	// ряда. Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL
	// обязателен — иначе count() считал бы несмёрженные части.
	vvpKvartalCountPoints = "SELECT count() FROM rosstat_vvp_kvartal FINAL"
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
	util.ClickHouseImport
}

type vvpKvartal struct {
	name string
	date time.Time
	vvp  float64
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

// vvpKvartalImport разбирает файл и пишет точки в batch: семантика прежних export() и цикла
// вставки Import сохранена дословно — имя ряда собирается из номеров листов, дата — 1-е число
// месяца, следующего за отчётным кварталом (getQuarterDate).
func vvpKvartalImport(xlsx *excelize.File, batch driver.Batch) error {
	table, err := parseVvpKvartal(xlsx)
	if err != nil {
		return err
	}
	for _, r := range *table {
		if err = batch.Append(r.name, r.date, float32(r.vvp)); err != nil {
			return err
		}
	}
	return nil
}

// Import берёт адрес актуального xlsx со страницы национальных счётов Росстата: имя файла
// содержит диапазон лет (VVP_kvartal_s-1995-ГГГГ.xlsx) и меняется при обновлении, а сетевое
// обращение в init() запрещено (ARCHITECTURE.md §8.6). Дальше — полный цикл шаблона: DDL →
// префлайт по HEAD (ряд обновляется раз в квартал, шаг расписания — недельный) → точки →
// производные (AfterImport) → строка версии.
func (s *vvpKvartalDdlStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	xlsDataUrl := getVvpKvartalXlsDataUrl()
	if xlsDataUrl == "" {
		return count, fmt.Errorf("vvpKvartal: не найдена ссылка на VVP_kvartal_*.xlsx на %s/statistics/accounts", rosstatUrl)
	}
	s.DataUrl = xlsDataUrl
	return s.ClickHouseImport.Import(ctx, conn)
}

// vvpKvartalImporter — квартальный ВВП РФ; регистрируется реестром rosstat/importers.go.
var vvpKvartalImporter = &vvpKvartalDdlStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatVVPKvartalTable,
	CreateTable:            []string{vvpKvartalDdl},
	ImportFunc:             vvpKvartalImport,
	AfterImport:            afterRosstatMacro(vvpKvartalSeriesMeta),
	PublicationCountPoints: vvpKvartalCountPoints,
}}
