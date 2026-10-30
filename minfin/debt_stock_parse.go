package minfin

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shakinm/xlsReader/xls"
	"github.com/shakinm/xlsReader/xls/structure"
	"github.com/xuri/excelize/v2"
)

const (
	debtStockInternalTotal      = "Внутренний долг, всего"
	debtStockInternalGuarantees = "Внутренний долг, в т.ч. гарантии"
	debtStockOfzTotal           = "ОФЗ, номинал"
	debtStockExternalTotal      = "Внешний долг, всего"
	debtStockExternalGuarantees = "Внешний долг, в т.ч. гарантии"

	debtStockOfzPk = "ОФЗ-ПК"
	debtStockOfzPd = "ОФЗ-ПД"
	debtStockOfzAd = "ОФЗ-АД"
	debtStockOfzIn = "ОФЗ-ИН"
	debtStockOfzN  = "ОФЗ-н"
)

// ofzTypeByHeader — нормализованная подпись колонки → имя ряда. Нормализация
// снимает пробелы и дефисы: «ОФЗ-ПК» → «офзпк».
var ofzTypeByHeader = map[string]string{
	"офзпк": debtStockOfzPk,
	"офзпд": debtStockOfzPd,
	"офзад": debtStockOfzAd,
	"офзин": debtStockOfzIn,
	"офзн":  debtStockOfzN,
}

// parseInternalDebtXls разбирает legacy .xls внутреннего долга: листы-годы,
// строка «всего» и строка гарантий, даты месяцев — Excel-сериалы или текст.
// Пустая ячейка будущего месяца — не точка.
func parseInternalDebtXls(body []byte, batch driver.Batch) error {
	wb, err := xls.OpenReader(bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%s: открытие внутреннего .xls: %w", debtStockTable, err)
	}
	points := 0
	for _, sheet := range wb.GetSheets() {
		rows := sheet.GetRows()
		dateRow, headerRow := -1, -1
		for i, row := range rows {
			if i >= len(rows)-1 {
				continue
			}
			cells := row.GetCols()
			if len(cells) < 3 {
				continue
			}
			if strings.Contains(normalizeOfzHeader(cells[2].GetString()), "посостояниюна") {
				headerRow, dateRow = i, i+1
				break
			}
		}
		if headerRow < 0 || dateRow < 0 {
			return fmt.Errorf("%s: лист %q: не найдена шапка «по состоянию на»", debtStockTable, sheet.GetName())
		}
		dates, err := debtDatesFromXlsCells(rows[dateRow].GetCols())
		if err != nil {
			return fmt.Errorf("%s: лист %q: %w", debtStockTable, sheet.GetName(), err)
		}
		if len(dates) == 0 {
			return fmt.Errorf("%s: лист %q: нет дат в шапке", debtStockTable, sheet.GetName())
		}
		totalRow, guaranteeRow := -1, -1
		for i, row := range rows[headerRow+1:] {
			cells := row.GetCols()
			if len(cells) == 0 {
				continue
			}
			label := strings.ToLower(strings.TrimSpace(cells[0].GetString()))
			switch {
			case strings.Contains(label, "внутренн") && strings.Contains(label, "всего"):
				totalRow = headerRow + 1 + i
			case strings.Contains(label, "государственные гарантии"):
				guaranteeRow = headerRow + 1 + i
			}
		}
		if totalRow < 0 {
			return fmt.Errorf("%s: лист %q: не найдена строка «внутренний долг — всего»", debtStockTable, sheet.GetName())
		}
		if guaranteeRow < 0 {
			return fmt.Errorf("%s: лист %q: не найдена строка государственных гарантий", debtStockTable, sheet.GetName())
		}
		for col, date := range dates {
			total, totalOK, err := xlsCellNumber(rows[totalRow].GetCols(), col)
			if err != nil {
				return fmt.Errorf("%s: лист %q, всего, колонка %d: %w", debtStockTable, sheet.GetName(), col+1, err)
			}
			guarantees, guaranteeOK, err := xlsCellNumber(rows[guaranteeRow].GetCols(), col)
			if err != nil {
				return fmt.Errorf("%s: лист %q, гарантии, колонка %d: %w", debtStockTable, sheet.GetName(), col+1, err)
			}
			if totalOK {
				if err = batch.Append(debtStockInternalTotal, date, total); err != nil {
					return fmt.Errorf("%s: лист %q, всего, колонка %d: запись: %w", debtStockTable, sheet.GetName(), col+1, err)
				}
				points++
			}
			if guaranteeOK {
				if err = batch.Append(debtStockInternalGuarantees, date, guarantees); err != nil {
					return fmt.Errorf("%s: лист %q, гарантии, колонка %d: запись: %w", debtStockTable, sheet.GetName(), col+1, err)
				}
				points++
			}
		}
	}
	if points == 0 {
		return fmt.Errorf("%s: разбор внутреннего .xls не дал ни одной точки", debtStockTable)
	}
	return nil
}

func debtDatesFromXlsCells(cells []structure.CellData) (map[int]time.Time, error) {
	dates := make(map[int]time.Time, len(cells))
	for col := 2; col < len(cells); col++ {
		raw := strings.TrimSpace(cells[col].GetString())
		if raw == "" {
			continue
		}
		date, err := parseOfzDate(raw)
		if err != nil {
			return nil, fmt.Errorf("колонка %d: дата %q: %w", col+1, raw, err)
		}
		dates[col] = date
	}
	return dates, nil
}

// xlsCellNumber читает числовую ячейку строки; пустая и «-» — не точка.
func xlsCellNumber(cells []structure.CellData, col int) (float64, bool, error) {
	if col >= len(cells) {
		return 0, false, nil
	}
	return parseOfzNumber(cells[col].GetString())
}

// parseExternalDebtXlsx разбирает GVD_*.xlsx: помесячные строки с 2011 года,
// колонка 1 — дата, колонка 2 — всего, колонка 3 — гарантии (млн долл. США).
func parseExternalDebtXlsx(xlsx *excelize.File, batch driver.Batch) error {
	sheet := xlsx.GetSheetList()[0]
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист %q: %w", debtStockTable, sheet, err)
	}
	points := 0
	for _, row := range rows {
		if len(row) < 3 {
			continue
		}
		rawDate := strings.TrimSpace(row[1])
		if rawDate == "" || strings.Contains(strings.ToLower(rawDate), "состоянию") {
			continue
		}
		date, err := parseOfzDate(rawDate)
		if err != nil {
			continue
		}
		total, ok, err := parseOfzNumber(row[2])
		if err != nil {
			return fmt.Errorf("%s: лист %q, дата %s, всего: %w", debtStockTable, sheet, date.Format("2006-01-02"), err)
		}
		if !ok {
			continue
		}
		if err = batch.Append(debtStockExternalTotal, date, total); err != nil {
			return fmt.Errorf("%s: лист %q, дата %s, всего: запись: %w", debtStockTable, sheet, date.Format("2006-01-02"), err)
		}
		if guarantees, ok, err := parseOfzNumber(row[3]); err != nil {
			return fmt.Errorf("%s: лист %q, дата %s, гарантии: %w", debtStockTable, sheet, date.Format("2006-01-02"), err)
		} else if ok {
			if err = batch.Append(debtStockExternalGuarantees, date, guarantees); err != nil {
				return fmt.Errorf("%s: лист %q, дата %s, гарантии: запись: %w", debtStockTable, sheet, date.Format("2006-01-02"), err)
			}
		}
		points++
	}
	if points == 0 {
		return fmt.Errorf("%s: разбор внешнего .xlsx не дал ни одной точки", debtStockTable)
	}
	return nil
}

// parseOfzStructure2022Xls разбирает годовой .xls 2022 года: шапка по видам
// обязательств, строки — месяцы 2022-01-01 … 2023-01-01. «Итого» содержит ГСО,
// поэтому сумма ОФЗ считается по колонкам типов, а не по «Итого».
func parseOfzStructure2022Xls(body []byte, batch driver.Batch) error {
	wb, err := xls.OpenReader(bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%s: открытие структуры 2022: %w", debtStockTable, err)
	}
	sheets := wb.GetSheets()
	if len(sheets) == 0 {
		return fmt.Errorf("%s: структура 2022: нет листов", debtStockTable)
	}
	rows := sheets[0].GetRows()
	headerRow := -1
	typeCols := map[int]string{}
	for i, row := range rows {
		candidate := map[int]string{}
		for j, cell := range row.GetCols() {
			if series, ok := ofzTypeByHeader[normalizeOfzHeader(cell.GetString())]; ok {
				candidate[j] = series
			}
		}
		if len(candidate) >= 4 {
			headerRow, typeCols = i, candidate
			break
		}
	}
	if headerRow < 0 {
		return fmt.Errorf("%s: структура 2022: не найдена шапка видов ОФЗ", debtStockTable)
	}
	points := 0
	for _, row := range rows[headerRow+1:] {
		cells := row.GetCols()
		if len(cells) < 2 {
			continue
		}
		date, err := parseOfzDate(cells[1].GetString())
		if err != nil {
			continue
		}
		total := 0.0
		for col, series := range typeCols {
			if col >= len(cells) {
				continue
			}
			value, ok, err := parseOfzNumber(cells[col].GetString())
			if err != nil {
				return fmt.Errorf("%s: структура 2022, %s, %s: %w", debtStockTable, date.Format("2006-01-02"), series, err)
			}
			if !ok {
				continue
			}
			total += value
			if err = batch.Append(series, date, value); err != nil {
				return fmt.Errorf("%s: структура 2022, %s, %s: запись: %w", debtStockTable, date.Format("2006-01-02"), series, err)
			}
		}
		if total == 0 {
			continue
		}
		if err = batch.Append(debtStockOfzTotal, date, total); err != nil {
			return fmt.Errorf("%s: структура 2022, %s, ОФЗ: запись: %w", debtStockTable, date.Format("2006-01-02"), err)
		}
		points++
	}
	if points == 0 {
		return fmt.Errorf("%s: структура 2022 не дала ни одной точки", debtStockTable)
	}
	return nil
}

// parseOfzStructureMonthXlsx разбирает debtDynamicsMonth_*.xlsx: шапка
// «Виды государственных ценных бумаг», две колонки «По состоянию на»; берётся
// последняя (конец месяца), строки ОФЗ суммируются без ГСО.
func parseOfzStructureMonthXlsx(xlsx *excelize.File, batch driver.Batch) error {
	sheet := xlsx.GetSheetList()[0]
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист %q: %w", debtStockTable, sheet, err)
	}
	headerRow, endCol := -1, -1
	for i, row := range rows {
		for _, cell := range row {
			if normalizeOfzHeader(cell) == "видыгосударственныхценныхбумаг" {
				headerRow = i
				break
			}
		}
		if headerRow >= 0 {
			break
		}
	}
	if headerRow < 0 {
		return fmt.Errorf("%s: лист %q: не найдена шапка «Виды государственных ценных бумаг»", debtStockTable, sheet)
	}
	for j, cell := range rows[headerRow] {
		if strings.Contains(normalizeOfzHeader(cell), "посостояниюна") {
			endCol = j
		}
	}
	if endCol < 0 {
		return fmt.Errorf("%s: лист %q: не найдена колонка «По состоянию на»", debtStockTable, sheet)
	}
	date, err := headerDate(rows[headerRow][endCol])
	if err != nil {
		return fmt.Errorf("%s: лист %q: дата шапки %q: %w", debtStockTable, sheet, rows[headerRow][endCol], err)
	}

	points := 0
	total := 0.0
	for _, row := range rows[headerRow+1:] {
		if len(row) <= endCol {
			continue
		}
		series, ok := ofzTypeByHeader[normalizeOfzHeader(row[0])]
		if !ok {
			continue
		}
		value, ok, err := parseOfzNumber(row[endCol])
		if err != nil {
			return fmt.Errorf("%s: лист %q, %s, %s: %w", debtStockTable, sheet, series, date.Format("2006-01-02"), err)
		}
		if !ok {
			continue
		}
		total += value
		if err = batch.Append(series, date, value); err != nil {
			return fmt.Errorf("%s: лист %q, %s, %s: запись: %w", debtStockTable, sheet, series, date.Format("2006-01-02"), err)
		}
		points++
	}
	if points == 0 {
		return fmt.Errorf("%s: лист %q: разбор не дал ни одной точки ОФЗ", debtStockTable, sheet)
	}
	if err = batch.Append(debtStockOfzTotal, date, total); err != nil {
		return fmt.Errorf("%s: лист %q, %s, ОФЗ: запись: %w", debtStockTable, sheet, date.Format("2006-01-02"), err)
	}
	return nil
}

func headerDate(raw string) (time.Time, error) {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return time.Time{}, fmt.Errorf("пустая дата")
	}
	return parseOfzDate(fields[len(fields)-1])
}

// parseOfzStructureMonthURL — шаблон файла месячной структуры: имя содержит
// первое число месяца публикации (оно же конец предыдущего отчётного месяца),
// каталог library/<год>/<месяц>/main/ совпадает с этим месяцем.
func parseOfzStructureMonthURL(year, month int) string {
	return fmt.Sprintf("https://minfin.gov.ru/common/upload/library/%04d/%02d/main/debtDynamicsMonth_01.%02d.%04d.xlsx",
		year, month, month, year)
}
