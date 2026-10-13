package cbr

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	// Показатель сбережений сектора «Домашние хозяйства» https://www.cbr.ru/statistics/macro_itm/households/
	// https://www.cbr.ru/vfs/statistics/households/households_b.xlsx
	cbrUrl                   = "https://www.cbr.ru"
	cbrStatsUrl              = "https://www.cbr.ru/vfs/statistics"
	householdsBMesXlsDataUrl = cbrStatsUrl + "/households/households_bm.xlsx"
	householdsBMesTable      = "cbr_households_b_mes"
	householdsBMesDdl        = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	householdsBMesSheet      = "Балансы"
	householdsBMesField      = "АКТИВЫ"
	householdsBMesTimeLayout = "01-02-06"
)

type HouseholdsBMesStat struct {
	util.ClickHouseImport
}

// householdsBMesImport разбирает лист «Балансы» и пишет точки в batch: name — название
// строки файла как есть, date — первое число отчётного месяца, balance — значение.
// Семантика прежнего export() сохранена: строки до раздела «АКТИВЫ» пропускаются, строки
// с пустой или нулевой первой ячейкой (подстроки подразделов) пропускаются целиком, пустые
// ячейки внутри строки пропускаются по одной. Пустой лист/отсутствие раздела/ноль точек —
// ошибка (fail-fast): молчаливый прогон сотрёт ряд нулём строк.
func householdsBMesImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows(householdsBMesSheet)
	if err != nil {
		return err
	}
	headerRow, fieldFound := 0, false
	count := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		if i > 0 && strings.TrimSpace(row[0]) == householdsBMesField {
			headerRow, fieldFound = i-1, true
			continue
		}
		if !fieldFound {
			continue
		}
		// строка из одной ячейки: заголовок колонок не прочитан, дальше смысла нет
		if len(row) < 2 {
			break
		}
		if strings.TrimSpace(row[1]) == "" || strings.TrimSpace(row[1]) == "0" {
			continue
		}
		// Колонки с месяцами и пропуском кварталов
		for j, cell := range row[1:] {
			if cell == "" {
				continue
			}
			if j+1 >= len(rows[headerRow]) {
				break
			}
			date, err := time.Parse(householdsBMesTimeLayout, strings.TrimSpace(rows[headerRow][j+1]))
			if err != nil {
				return fmt.Errorf("%s: дата колонки %q: %w", householdsBMesTable, rows[headerRow][j+1], err)
			}
			balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
			value, err := strconv.ParseFloat(balance, 32)
			if err != nil {
				return fmt.Errorf("%s: строка %q, дата %s: значение %q: %w",
					householdsBMesTable, row[0], date.Format("2006-01-02"), balance, err)
			}
			if err = batch.Append(row[0], date, value); err != nil {
				return err
			}
			count++
		}
	}
	if !fieldFound {
		return fmt.Errorf("%s: в листе «%s» не найдена строка «%s» с датами колонок", householdsBMesTable, householdsBMesSheet, householdsBMesField)
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор листа «%s» не дал ни одной точки", householdsBMesTable, householdsBMesSheet)
	}
	return nil
}

// householdsBMesImporter — балансы сектора «Домашние хозяйства»; регистрируется
// реестром cbr/importers.go.
var householdsBMesImporter = &publishedStat{
	ImportStat: &HouseholdsBMesStat{ClickHouseImport: util.ClickHouseImport{
		TableName:   householdsBMesTable,
		CreateTable: []string{householdsBMesDdl},
		DataUrl:     householdsBMesXlsDataUrl,
		ImportFunc:  householdsBMesImport,
	}},
	meta: householdsBMesSeriesMeta,
}
