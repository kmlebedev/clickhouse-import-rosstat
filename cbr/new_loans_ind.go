package cbr

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	// Сведения о размещенных и привлеченных средствах https://www.cbr.ru/statistics/bank_sector/sors/
	// Кредиты, предоставленные физическим лицам - резидентам (региональный разрез)
	// Общий объем кредитов, предоставленных физическим лицам-резидентам
	// https://www.cbr.ru/vfs/statistics/BankSector/Mortgage/02_04_New_loans_ind.xlsx
	loansToIndXlsDataUrl = cbrStatsUrl + "/BankSector/Mortgage/02_04_New_loans_ind.xlsx"
	loansToIndTable      = "cbr_loans_to_individuals"
	loansToIndDdl        = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	loansToIndSheet = "итого"
	loansToIndField = "РОССИЙСКАЯ ФЕДЕРАЦИЯ"
	// loansToIndCountPoints — количественный чек версии (спека §3.5): число точек
	// своего ряда. Литерал, как и таблица: запрос — часть утверждения «эти точки
	// мои». FINAL обязателен — иначе count() считал бы несмёрженные части.
	loansToIndCountPoints = "SELECT count() FROM cbr_loans_to_individuals FINAL"
)

type LoansToindividualsStat struct {
	util.ClickHouseImport
}

// cbrLoansToIndImport — разбор листа «итого» файла 02_04_New_loans_ind.xlsx: имя ряда —
// регион или федеральный округ, значение — объём новых кредитов физлицам, млн руб. Лист по
// структуре совпадает с файлом юрлиц, поэтому разбор общий (cbrLoansImport).
func cbrLoansToIndImport(xlsx *excelize.File, batch driver.Batch) error {
	return cbrLoansImport(xlsx, batch, loansToIndTable, loansToIndSheet, loansToIndField)
}

// loansToIndividualsImporter — объём новых кредитов физлицам (региональный разрез);
// регистрируется реестром cbr/importers.go.
var loansToIndividualsImporter = &LoansToindividualsStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              loansToIndTable,
	CreateTable:            []string{loansToIndDdl},
	DataUrl:                loansToIndXlsDataUrl,
	ImportFunc:             cbrLoansToIndImport,
	AfterImport:            afterCbrMacro(cbrLoansToIndSeriesMeta),
	PublicationCountPoints: loansToIndCountPoints,
}}
