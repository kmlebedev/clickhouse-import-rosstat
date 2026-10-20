package minfin

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

const debtServiceBudgetSeries = "Обслуживание государственного и муниципального долга"

var debtServiceBudgetLayout = fedbudLayout{
	Table:  debtServiceTable,
	Fields: []string{debtServiceBudgetSeries},
}

func debtServiceBudgetImport(xlsx *excelize.File, batch driver.Batch) error {
	return importFedbud(xlsx, batch, debtServiceBudgetLayout)
}

var indicatorsYearRe = regexp.MustCompile(`в\s+(\d{4})\s+году`)

var monthByName = map[string]time.Month{
	"январь": time.January, "февраль": time.February, "март": time.March,
	"апрель": time.April, "май": time.May, "июнь": time.June,
	"июль": time.July, "август": time.August, "сентябрь": time.September,
	"октябрь": time.October, "ноябрь": time.November, "декабрь": time.December,
}

// parseIndicatorsBudget разбирает «Основные показатели исполнения федерального
// бюджета … в части государственных внутренних заимствований»: строки — месяцы,
// колонки — привлечение, погашение, чистое привлечение и обслуживание. Значения
// источника в млн руб., таблица хранит млрд руб. (÷1000). Подстроки «в том числе:»
// и виды обязательств пропускаются: они детализируют месяц, а не добавляют месяц.
func parseIndicatorsBudget(xlsx *excelize.File, batch driver.Batch) error {
	sheet := xlsx.GetSheetList()[0]
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист %q: %w", debtServiceTable, sheet, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("%s: лист %q пуст", debtServiceTable, sheet)
	}

	year := 0
	for _, row := range rows[:min(3, len(rows))] {
		for _, cell := range row {
			if match := indicatorsYearRe.FindStringSubmatch(cell); match != nil {
				year, _ = strconv.Atoi(match[1])
				break
			}
		}
		if year != 0 {
			break
		}
	}
	if year == 0 {
		return fmt.Errorf("%s: лист %q: в заголовке не найден год «в <год> году»", debtServiceTable, sheet)
	}

	headerRow, columns := -1, map[string]int{}
	required := map[string]string{
		"привлечение":           "Привлечение",
		"погашение":             "Погашение основного долга",
		"чистоепривлечение":     "Чистое привлечение",
		"расходынаобслуживание": "Расходы на обслуживание",
	}
	for i, row := range rows {
		candidate := map[string]int{}
		for j, cell := range row {
			normalized := normalizeOfzHeader(cell)
			for key, series := range required {
				if strings.HasPrefix(normalized, key) {
					candidate[series] = j
				}
			}
		}
		if len(candidate) == len(required) {
			headerRow, columns = i, candidate
			break
		}
	}
	if headerRow < 0 {
		return fmt.Errorf("%s: лист %q: не найдена шапка колонок заимствований", debtServiceTable, sheet)
	}

	points := 0
	for _, row := range rows[headerRow+1:] {
		if len(row) == 0 {
			continue
		}
		month, ok := monthByName[strings.ToLower(strings.TrimSpace(row[0]))]
		if !ok {
			continue
		}
		for series, col := range columns {
			if col >= len(row) {
				continue
			}
			value, present, err := parseOfzNumber(row[col])
			if err != nil {
				return fmt.Errorf("%s: лист %q, %s %d, %s: %w", debtServiceTable, sheet, month, year, series, err)
			}
			if !present {
				continue
			}
			date := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
			if err = batch.Append(series, date, value/1000); err != nil {
				return fmt.Errorf("%s: лист %q, %s %d, %s: запись: %w", debtServiceTable, sheet, month, year, series, err)
			}
			points++
		}
	}
	if points == 0 {
		return fmt.Errorf("%s: лист %q: разбор не дал ни одной точки заимствований", debtServiceTable, sheet)
	}
	return nil
}
