package rosstat

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

const (
	// cpiSPRMinRows — минимальное число позиций на лист-месяц (в источнике 745 в 2025
	// и 778 в 2026; порог 700 ловит смену макета и не режет новые позиции).
	cpiSPRMinRows = 700
	// cpiSPRSumMin/cpiSPRSumMax — сумма весов листа: в файле все уровни (товары,
	// группы, агрегаты), поэтому ~987–989, а не 100. Выход за диапазон — смена макета.
	cpiSPRSumMin = 900.0
	cpiSPRSumMax = 1100.0
	// cpiSPRIndexMax — верхняя граница индекса (проценты): защита от попадания
	// в колонку индекса нечисловых/переставленных значений.
	cpiSPRIndexMax = 1000.0

	cpiSPRFile = "ipc_spr"
)

// cpiSPRPoint — одна строка листа-месяца.
type cpiSPRPoint struct {
	year, month uint16
	code, name  string
	weight      float64
	indexPrev   *float64
	indexYTD    *float64
	indexYOY    *float64
}

type cpiSPRSheetPoints struct {
	sheet  string
	year   uint16
	month  uint16
	points []cpiSPRPoint
}

// cpiSPRMonthNames — месяцы в заголовке листа («… за сентябрь 2025 г.»). В родительном
// падеже Росстат месяцы не пишет, но формы добавлены: заголовок — единственный
// надёжный источник даты (лист сентября 2025 назван `0922025`).
var cpiSPRMonthNames = map[string]uint16{
	"январь": 1, "января": 1,
	"февраль": 2, "февраля": 2,
	"март": 3, "марта": 3,
	"апрель": 4, "апреля": 4,
	"май": 5, "мая": 5,
	"июнь": 6, "июня": 6,
	"июль": 7, "июля": 7,
	"август": 8, "августа": 8,
	"сентябрь": 9, "сентября": 9,
	"октябрь": 10, "октября": 10,
	"ноябрь": 11, "ноября": 11,
	"декабрь": 12, "декабря": 12,
}

var (
	cpiSPRTitleDate = regexp.MustCompile(`(?i)за\s+([а-яё]+)\s+(\d{4})`)
	cpiSPRSheetName = regexp.MustCompile(`^(\d{2})(\d{4})$`)
)

// cpiSPRSheetMeta достаёт месяц и год листа: сначала из заголовка выпуска,
// затем из имени листа `MMYYYY`. Второй путь — фолбэк: в книге 2026-11-13
// лист сентября 2025 назван `0922025`, и по имени он не разбирается.
func cpiSPRSheetMeta(rows [][]string, sheet string) (year, month uint16, ok bool) {
	limit := len(rows)
	if limit > 8 {
		limit = 8
	}
	for i := 0; i < limit; i++ {
		for _, cell := range rows[i] {
			match := cpiSPRTitleDate.FindStringSubmatch(strings.ToLower(cell))
			if match == nil {
				continue
			}
			number, known := cpiSPRMonthNames[match[1]]
			if !known {
				continue
			}
			parsed, err := strconv.ParseUint(match[2], 10, 16)
			if err != nil {
				continue
			}
			return uint16(parsed), number, true
		}
	}
	if match := cpiSPRSheetName.FindStringSubmatch(sheet); match != nil {
		number, errMonth := strconv.ParseUint(match[1], 10, 8)
		parsed, errYear := strconv.ParseUint(match[2], 10, 16)
		if errMonth == nil && errYear == nil && number >= 1 && number <= 12 {
			return uint16(parsed), uint16(number), true
		}
	}
	return 0, 0, false
}

// cpiSPRColumns — индексы колонок листа.
type cpiSPRColumns struct {
	header, name, code, weight, prev, ytd, yoy int
}

// cpiSPRColumnsFor ищет шапку по подписям: в книге она занимает две строки
// (наименование/код/структура, ниже — три подписи индексов). Возвращает шапку
// и номер первой строки данных.
func cpiSPRColumnsFor(formattedRows [][]string) (cpiSPRColumns, int, bool) {
	limit := len(formattedRows)
	if limit > 12 {
		limit = 12
	}
	for i := 0; i < limit; i++ {
		columns := cpiSPRColumns{header: i, name: -1, code: -1, weight: -1, prev: -1, ytd: -1, yoy: -1}
		for j, cell := range formattedRows[i] {
			label := strings.ToLower(strings.Join(strings.Fields(cell), " "))
			switch {
			case strings.Contains(label, "наименование"):
				columns.name = j
			case strings.Contains(label, "локальный код"):
				columns.code = j
			case strings.Contains(label, "структура потребительских"):
				columns.weight = j
			}
		}
		if columns.name < 0 || columns.code < 0 || columns.weight < 0 {
			continue
		}
		if i+1 < len(formattedRows) {
			for j, cell := range formattedRows[i+1] {
				label := strings.ToLower(strings.Join(strings.Fields(cell), " "))
				switch {
				case strings.Contains(label, "предыдущему месяцу"):
					columns.prev = j
				case strings.Contains(label, "декабрю"):
					columns.ytd = j
				case strings.Contains(label, "соответствующему"):
					columns.yoy = j
				}
			}
		}
		if columns.prev < 0 || columns.ytd < 0 || columns.yoy < 0 {
			continue
		}
		return columns, i + 2, true
	}
	return cpiSPRColumns{}, 0, false
}

// cpiSPRCell безопасно читает ячейку: GetRows обрезает хвостовые пустые ячейки,
// и короткая строка — норма источника, а не паника.
func cpiSPRCell(row []string, index int) string {
	if index < 0 || index >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[index])
}

// cpiSPRValue разбирает число публикации: запятая или точка, пробелы-разделители.
// «-»/«…»/пустая ячейка — пропуск (индекса нет), нечисловой текст — ошибка.
func cpiSPRValue(cell string) (float64, bool, error) {
	text := strings.TrimSpace(strings.ReplaceAll(cell, "\u00a0", ""))
	switch text {
	case "", "-", "—", "–", "…", "...", "х", "Х":
		return 0, false, nil
	}
	text = strings.ReplaceAll(text, " ", "")
	text = strings.ReplaceAll(text, ",", ".")
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, false, fmt.Errorf("не число: %q", cell)
	}
	return value, true, nil
}

// cpiSPRSum — сумма весов листа (все уровни файла).
func cpiSPRSum(points []cpiSPRPoint) float64 {
	sum := 0.0
	for _, point := range points {
		sum += point.weight
	}
	return sum
}

// parseCPISPRSheet разбирает лист-месяц.
func parseCPISPRSheet(rows, formattedRows [][]string, sheet string, year, month uint16) ([]cpiSPRPoint, error) {
	columns, firstRow, ok := cpiSPRColumnsFor(formattedRows)
	if !ok {
		return nil, fmt.Errorf("%s: лист %s: шапка с колонками кода, структуры и индексов не найдена", cpiSPRFile, sheet)
	}
	points := make([]cpiSPRPoint, 0, len(rows))
	seen := make(map[string]cpiSPRPoint, len(rows))
	for i := firstRow; i < len(rows); i++ {
		row, formatted := rows[i], formattedRows[i]
		name := cpiSPRCell(formatted, columns.name)
		code := cpiSPRCell(formatted, columns.code)
		if name == "" && code == "" {
			continue
		}
		weightCell := cpiSPRCell(row, columns.weight)
		if weightCell == "" {
			weightCell = cpiSPRCell(formatted, columns.weight)
		}
		weight, hasWeight, err := cpiSPRValue(weightCell)
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s, строка %d: вес: %w", cpiSPRFile, sheet, i+1, err)
		}
		if !hasWeight {
			// Сноски и служебные строки без веса — не точки.
			continue
		}
		if code == "" || name == "" {
			continue
		}
		if weight <= 0 || weight > 100 {
			return nil, fmt.Errorf("%s: лист %s, строка %d: вес %v вне (0; 100]", cpiSPRFile, sheet, i+1, weight)
		}
		point := cpiSPRPoint{year: year, month: month, code: code, name: name, weight: weight}
		if point.indexPrev, err = cpiSPRIndex(row, formatted, columns.prev, sheet, i+1); err != nil {
			return nil, err
		}
		if point.indexYTD, err = cpiSPRIndex(row, formatted, columns.ytd, sheet, i+1); err != nil {
			return nil, err
		}
		if point.indexYOY, err = cpiSPRIndex(row, formatted, columns.yoy, sheet, i+1); err != nil {
			return nil, err
		}
		if previous, exists := seen[code]; exists {
			if previous.weight != weight || previous.name != name {
				return nil, fmt.Errorf("%s: лист %s, код %s встречается дважды с разными значениями", cpiSPRFile, sheet, code)
			}
			continue
		}
		seen[code] = point
		points = append(points, point)
	}
	if len(points) < cpiSPRMinRows {
		return nil, fmt.Errorf("%s: лист %s: позиций %d, ожидалось не меньше %d", cpiSPRFile, sheet, len(points), cpiSPRMinRows)
	}
	if sum := cpiSPRSum(points); sum < cpiSPRSumMin || sum > cpiSPRSumMax {
		return nil, fmt.Errorf("%s: лист %s: сумма весов %.3f вне [%.0f; %.0f] — макет изменился", cpiSPRFile, sheet, sum, cpiSPRSumMin, cpiSPRSumMax)
	}
	return points, nil
}

// cpiSPRIndex читает индекс: пустая ячейка и «-» — NULL (у 9–11 строк на лист
// индекса нет), нечисловой текст — ошибка.
func cpiSPRIndex(row, formatted []string, index int, sheet string, rowNumber int) (*float64, error) {
	cell := cpiSPRCell(row, index)
	if cell == "" {
		cell = cpiSPRCell(formatted, index)
	}
	value, ok, err := cpiSPRValue(cell)
	if err != nil {
		return nil, fmt.Errorf("%s: лист %s, строка %d: индекс: %w", cpiSPRFile, sheet, rowNumber, err)
	}
	if !ok {
		return nil, nil
	}
	if value <= 0 || value >= cpiSPRIndexMax {
		return nil, fmt.Errorf("%s: лист %s, строка %d: индекс %v вне (0; %v)", cpiSPRFile, sheet, rowNumber, value, cpiSPRIndexMax)
	}
	return &value, nil
}

// parseCPISPR разбирает книгу целиком: все листы-месяцы, проверка непрерывности
// месяцев в окне и сортировка по (год, месяц, код).
func parseCPISPR(xlsx *excelize.File) ([]cpiSPRPoint, error) {
	var sheets []cpiSPRSheetPoints
	for _, sheet := range xlsx.GetSheetList() {
		// «Содержание» тоже содержит даты выпусков («… за январь 2025 г.»), поэтому
		// отсекается по имени: иначе разбор потребует у оглавления шапку весов.
		if strings.Contains(strings.ToLower(sheet), "содерж") {
			continue
		}
		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", cpiSPRFile, sheet, err)
		}
		year, month, ok := cpiSPRSheetMeta(rows, sheet)
		if !ok {
			// «Содержание» и прочие листы без даты выпуска.
			continue
		}
		formattedRows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: false})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", cpiSPRFile, sheet, err)
		}
		points, err := parseCPISPRSheet(rows, formattedRows, sheet, year, month)
		if err != nil {
			return nil, err
		}
		sheets = append(sheets, cpiSPRSheetPoints{sheet: sheet, year: year, month: month, points: points})
	}
	if len(sheets) < 2 {
		return nil, fmt.Errorf("%s: листов-месяцев %d, ожидалось не меньше 2", cpiSPRFile, len(sheets))
	}
	sort.Slice(sheets, func(i, j int) bool {
		if sheets[i].year != sheets[j].year {
			return sheets[i].year < sheets[j].year
		}
		return sheets[i].month < sheets[j].month
	})
	for i := 0; i < len(sheets); i++ {
		if i == 0 {
			continue
		}
		prev, cur := sheets[i-1], sheets[i]
		expected := prev.month + 1
		expectedYear := prev.year
		if expected > 12 {
			expected, expectedYear = 1, prev.year+1
		}
		if cur.year != expectedYear || cur.month != expected {
			return nil, fmt.Errorf("%s: пропуск месяца: после %02d.%d идёт %02d.%d", cpiSPRFile, prev.month, prev.year, cur.month, cur.year)
		}
	}
	result := make([]cpiSPRPoint, 0, len(sheets)*cpiSPRMinRows)
	for _, sheet := range sheets {
		result = append(result, sheet.points...)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].year != result[j].year {
			return result[i].year < result[j].year
		}
		if result[i].month != result[j].month {
			return result[i].month < result[j].month
		}
		return result[i].code < result[j].code
	})
	return result, nil
}
