package cbr

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

// Банковские ставки от https://www.cbr.ru/statistics/bank_sector/int_rat/regulatory_rates/

const (
	cbrBankIntRateTable = "cbr_bank_int_rate"
	cbrBankIntRateSheet = "в рублях"
	// cbrBankIntRateCountPoints — количественный чек версии (спека §3.5): число
	// точек своего ряда. Литерал, как и таблица: запрос — часть утверждения «эти
	// точки мои». FINAL обязателен — иначе count() считал бы несмёрженные части.
	cbrBankIntRateCountPoints = "SELECT count() FROM cbr_bank_int_rate FINAL"
)

// cbrBankIntRate — ставки по вкладам и кредитам: таблица, DDL и разбор листа «в рублях»,
// производные (каталог рядов и витрина v_cbr_macro) публикует шаблон через AfterImport.
// Пустой/короткий лист и ноль точек — ошибка (fail-fast), иначе «0 rows» выглядели бы
// успешным прогоном, а семь рядов по срокам вклада остались бы без обновления.
var cbrBankIntRate = util.ClickHouseImport{
	TableName: cbrBankIntRateTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, rate Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`},
	DataUrl: "https://www.cbr.ru/vfs/statistics/pdko/int_rat/regulatory_rates.xlsx",
	ImportFunc: func(xlsx *excelize.File, batch driver.Batch) error {
		rows, err := xlsx.GetRows(cbrBankIntRateSheet)
		if err != nil {
			return err
		}
		// Короткий лист — сразу ошибка: rows[3:] ниже паникует на срезе короче трёх строк
		// (заголовок файла), а паника в импортёре — не «0 rows», а упавший шаг DAG без
		// понятного сообщения.
		if len(rows) < 3 {
			return fmt.Errorf("%s: в листе «%s» нет строк данных", cbrBankIntRateTable, cbrBankIntRateSheet)
		}
		count := 0
		for _, row := range rows[3:] {
			if row[0] == "" {
				continue
			}
			dateArr := strings.SplitN(strings.TrimSpace(row[0]), " ", 2)
			dateYear, err := strconv.Atoi(strings.TrimSpace(dateArr[1]))
			if err != nil {
				return err
			}
			date := time.Date(dateYear, util.MonthsToNum[strings.ToLower(dateArr[0])]+1, -1, 0, 0, 0, 0, time.UTC)
			for i, cell := range row[1:] {
				if cell == "" {
					continue
				}
				name := rows[2][i+1]
				rate, err := strconv.ParseFloat(cell, 32)
				if err != nil {
					return err
				}
				if err = batch.Append(name, date, rate); err != nil {
					log.Error(err)
					return err
				}
				count++
			}
		}
		if count == 0 {
			return fmt.Errorf("%s: разбор листа «%s» не дал ни одной точки", cbrBankIntRateTable, cbrBankIntRateSheet)
		}
		return nil
	},
	AfterImport:            afterCbrMacro(cbrBankIntRateSeriesMeta),
	PublicationCountPoints: cbrBankIntRateCountPoints,
}

// cbrBankIntRateImporter — ставки по вкладам и кредитам; регистрируется реестром
// cbr/importers.go.
var cbrBankIntRateImporter = &cbrBankIntRate
