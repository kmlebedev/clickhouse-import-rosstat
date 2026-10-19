package fao

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// faoSeriesByHeader — стабильные имена рядов из кода (§6.8). Заголовки источника
// используются только для поиска колонки; смена подписи даёт ошибку, а не новый ряд.
var faoSeriesByHeader = []struct {
	Header string
	Series string
}{
	{"Food Price Index", "Food Price Index"},
	{"Meat", "Meat"},
	{"Dairy", "Dairy"},
	{"Cereals", "Cereals"},
	{"Oils", "Oils"},
	{"Sugar", "Sugar"},
}

// parseFaoFoodPrice разбирает CSV ФАО из байтов. Сеть сюда не попадает: байты
// качает шаблон util.ClickHouseImport и он же сохраняет их в винтажный архив.
//
// Семантика: в CSV дата — месяц наблюдения (YYYY-MM); в таблицу она пишется
// первым числом следующего месяца, то есть датой публикации. Так ряд читают
// cpi_nowcast.py/cpi_monthly.py (as-of date + 6 дней).
func parseFaoFoodPrice(body []byte, batch driver.Batch) error {
	reader := csv.NewReader(bytes.NewReader(body))
	reader.Comma = ','
	reader.FieldsPerRecord = -1 // хвост строки — пустые колонки до 66 полей
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("%s: csv: %w", faoFoodPriceTable, err)
	}
	if len(records) == 0 {
		return fmt.Errorf("%s: пустой CSV", faoFoodPriceTable)
	}

	headerRow := -1
	for i, row := range records {
		if len(row) > 0 && strings.TrimSpace(row[0]) == "Date" {
			headerRow = i
			break
		}
	}
	if headerRow < 0 {
		return fmt.Errorf("%s: не найдена шапка с первым столбцом Date", faoFoodPriceTable)
	}

	columns := make(map[string]int, len(faoSeriesByHeader))
	for _, series := range faoSeriesByHeader {
		idx := -1
		for i, cell := range records[headerRow] {
			if strings.TrimSpace(cell) == series.Header {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("%s: в шапке нет колонки %q", faoFoodPriceTable, series.Header)
		}
		columns[series.Series] = idx
	}

	count := 0
	for _, row := range records[headerRow+1:] {
		if len(row) == 0 {
			continue
		}
		dateStr := strings.TrimSpace(row[0])
		if dateStr == "" {
			continue
		}
		period, err := time.Parse("2006-01", dateStr)
		if err != nil {
			return fmt.Errorf("%s: дата %q: %w", faoFoodPriceTable, dateStr, err)
		}
		date := period.AddDate(0, 1, 0)
		for _, series := range faoSeriesByHeader {
			idx := columns[series.Series]
			if idx >= len(row) {
				continue
			}
			cell := strings.TrimSpace(row[idx])
			if cell == "" || cell == "-" {
				continue
			}
			value, err := strconv.ParseFloat(cell, 64)
			if err != nil {
				return fmt.Errorf("%s: %s %s: значение %q не число: %w", faoFoodPriceTable, series.Series, dateStr, cell, err)
			}
			if err = batch.Append(series.Series, date, value); err != nil {
				return err
			}
			count++
		}
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор не дал ни одной точки", faoFoodPriceTable)
	}
	return nil
}
