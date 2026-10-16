package bank

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

const (

	// https://www.sberbank.com/ru/investor-relations/groupresults/navigator/rezultaty-rpbu
	// Страница источника отдаётся через JS-challenge (TSPD) и статичных ссылок не содержит:
	// ссылку меняем вручную при обновлении, либо нужен headless-браузер.
	// https://www.sberbank.com/common/img/uploaded/redirected/com/investor-relations/groupresults/sber_finansovie_rezultaty_2022-2024.xlsx
	sberRpbuUrl          = "https://www.sberbank.com/common/img/uploaded/redirected/com/investor-relations/groupresults"
	sberRpbuXlsDataUrl   = sberRpbuUrl + "/sber_finansovie_rezultaty_ras_2022-2024.xlsx"
	sberRpbuDataUrlTable = "sber_finansovie_rezultaty"
	sberRpbuDataUrlDdl   = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	sberRpbuDataInsert = "INSERT INTO " + sberRpbuDataUrlTable + " VALUES (?, ?, ?)"
	sberRpbuDataField  = "Активы"
	sberRpbuTimeLayout = "01-02-06"
	// sberRpbuCountPoints — количественный чек версии (спека §3.5): число точек своего ряда.
	// Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL обязателен —
	// без него count() считает несмёрженные части и растёт после каждого повторного импорта.
	sberRpbuCountPoints = "SELECT count() FROM sber_finansovie_rezultaty FINAL"
)

// ctx — фон для вставок: импорт не привязан к запросу пользователя, а CrawFunc получает
// только conn (идиом cbr/currency_usd.go, customs/vneshn_torg_countries.go).
var ctx = context.TODO()

// LoansToCorporationsStat — месячные финрезультаты Сбера по РСБУ (лист «Баланс»). Ссылка на
// файл статична и обновляется вручную (страница источника закрыта JS-challenge), а содержимое
// меняется реже недельного расписания шага, поэтому у артефакта есть точка контроля публикации.
type LoansToCorporationsStat struct {
	util.ClickHouseImport
}

// sberRpbuExport скачивает xlsx по url (адрес приходит от шаблона через CrawFunc) и разбирает
// лист «Баланс»: семантика прежнего export() сохранена дословно.
func sberRpbuExport(dataUrl string) (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(dataUrl); err != nil {
		return nil, err
	}
	defer func() { _ = xlsx.Close() }()
	table = new([][]string)
	var rows [][]string
	if rows, err = xlsx.GetRows("Баланс"); err != nil {
		return nil, err
	}
	fieldFound := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		if strings.TrimSpace(row[1]) == sberRpbuDataField {
			fieldFound = i - 1
		}
		if fieldFound == 0 {
			continue
		}
		if len(row) < 3 {
			break
		}
		if strings.TrimSpace(row[2]) == "" || strings.TrimSpace(row[2]) == "0" {
			continue
		}
		// Колонки с месяцами и пропуском кварталов
		for j, cell := range row[2:] {
			if cell == "" {
				continue
			}
			if j+1 >= len(rows[fieldFound]) {
				break
			}
			// fmt.Printf("name %s date %v cell %s\n", row[1], rows[fieldFound][j+2], cell)
			balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
			if _, err = strconv.ParseFloat(balance, 32); err != nil {
				return nil, err
			}
			*table = append(*table, []string{row[1], rows[fieldFound][j+2], balance})
		}
	}

	return table, nil
}

// sberRpbuCraw — полный путь точек: скачивание, разбор и построчные INSERT-ы (прежний цикл
// Import не изменён; DDL и точку контроля публикации выполняет шаблон).
func sberRpbuCraw(dataUrl string, conn driver.Conn) (count int64, err error) {
	var table *[][]string
	if table, err = sberRpbuExport(dataUrl); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		date, err := time.Parse(sberRpbuTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, sberRpbuDataInsert, row[0], date, row[2]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// sberRpbuImporter — финрезультаты Сбера; регистрируется init() пакета.
var sberRpbuImporter = &LoansToCorporationsStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              sberRpbuDataUrlTable,
	CreateTable:            []string{sberRpbuDataUrlDdl},
	DataUrl:                sberRpbuXlsDataUrl,
	CrawFunc:               sberRpbuCraw,
	PublicationCountPoints: sberRpbuCountPoints,
}}

func init() {
	chimport.Stats = append(chimport.Stats, sberRpbuImporter)
}
