package fedstat

// Разбор BIFF .xls из ЕМИСС. Источник отдаёт лист «Данные» с четырьмя
// раскладками, проверенными на живой выгрузке 2026-10-21:
//
//   matrix            — строки-измерения, колонки-периоды (57806, 31074, 31066,
//                       31260, 34128, 58089, 58090);
//   long              — периоды в колонке (43046);
//   year_columns      — колонки-годы (34129, 40562, 34475, 31062, 43337);
//   activity_columns  — виды деятельности в шапке, год в подписи колонок (59001).
//
// Разбор отделён от сети (стандарт §6.1): ошибка разметки — ошибка, а не пустой
// результат; неизвестный период с числом — ошибка, чтобы новый вид периода не
// терялся молча. Числовые ячейки читаются как float64, пустые пропускаются.

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shakinm/xlsReader/xls"
	"github.com/shakinm/xlsReader/xls/structure"
)

type fedstatCell struct {
	text    string
	number  float64
	numeric bool
}

type fedstatRecord struct {
	labels     []string // подписи измерений в порядке конфигурации
	activity   string   // для activity_columns — вид деятельности из шапки
	period     string   // подпись периода источника
	periodType string   // 'M' | 'Q' | 'CUM' | 'Y'
	date       time.Time
	value      float64
}

// fedstatLayout — раскладка листа «Данные».
type fedstatLayout string

const (
	fedstatLayoutMatrix          fedstatLayout = "matrix"
	fedstatLayoutLong            fedstatLayout = "long"
	fedstatLayoutYearColumns     fedstatLayout = "year_columns"
	fedstatLayoutActivityColumns fedstatLayout = "activity_columns"
)

func fedstatReadSheet(data []byte) ([][]fedstatCell, error) {
	wb, err := xls.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("открытие .xls: %w", err)
	}
	for _, sheet := range wb.GetSheets() {
		if strings.TrimSpace(sheet.GetName()) != "Данные" {
			continue
		}
		rows := sheet.GetRows()
		out := make([][]fedstatCell, 0, len(rows))
		for _, row := range rows {
			cells := row.GetCols()
			converted := make([]fedstatCell, len(cells))
			for i, cell := range cells {
				converted[i] = fedstatCellFrom(cell)
			}
			out = append(out, converted)
		}
		return out, nil
	}
	return nil, fmt.Errorf("в книге нет листа «Данные»")
}

func fedstatCellFrom(cell structure.CellData) fedstatCell {
	typ := cell.GetType()
	switch {
	case strings.HasSuffix(typ, "Blank"):
		return fedstatCell{}
	case strings.HasSuffix(typ, "Number"), strings.HasSuffix(typ, "Rk"):
		return fedstatCell{number: cell.GetFloat64(), numeric: true, text: cell.GetString()}
	}
	text := strings.TrimSpace(cell.GetString())
	if number, err := fedstatParseNumber(text); err == nil {
		return fedstatCell{text: text, number: number, numeric: true}
	}
	return fedstatCell{text: text}
}

// fedstatParseNumber — число из текстовой ячейки: запятая как десятичный
// разделитель, пробелы-разделители тысяч. Пустая строка — не число.
func fedstatParseNumber(raw string) (float64, error) {
	normalized := strings.TrimSpace(strings.ReplaceAll(raw, ",", "."))
	normalized = strings.ReplaceAll(normalized, " ", "")
	normalized = strings.ReplaceAll(normalized, "\u00a0", "")
	if normalized == "" {
		return 0, fmt.Errorf("пусто")
	}
	value, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func fedstatCellAt(row []fedstatCell, index int) fedstatCell {
	if index < 0 || index >= len(row) {
		return fedstatCell{}
	}
	return row[index]
}

func fedstatRowText(row []fedstatCell, index int) string {
	return strings.TrimSpace(fedstatCellAt(row, index).text)
}

func fedstatRowNumber(row []fedstatCell, index int) (float64, bool) {
	cell := fedstatCellAt(row, index)
	return cell.number, cell.numeric
}

func fedstatMaxCols(rows [][]fedstatCell) int {
	max := 0
	for _, row := range rows {
		if len(row) > max {
			max = len(row)
		}
	}
	return max
}

var (
	fedstatMonths = map[string]time.Month{
		"январь": time.January, "февраль": time.February, "март": time.March,
		"апрель": time.April, "май": time.May, "июнь": time.June,
		"июль": time.July, "август": time.August, "сентябрь": time.September,
		"октябрь": time.October, "ноябрь": time.November, "декабрь": time.December,
	}
	fedstatQuarters = map[string]time.Month{
		"i квартал": time.January, "ii квартал": time.April,
		"iii квартал": time.July, "iv квартал": time.October,
	}
)

// fedstatPeriod — подпись периода -> дата начала, тип и стабильная подпись.
func fedstatPeriod(label string, year int) (period string, periodType string, date time.Time, ok bool) {
	normalized := strings.ToLower(strings.Join(strings.Fields(label), " "))
	if normalized == "" {
		return "", "", time.Time{}, false
	}
	if month, found := fedstatMonths[normalized]; found {
		return strings.TrimSpace(label), "M", time.Date(year, month, 1, 0, 0, 0, 0, time.UTC), true
	}
	if month, found := fedstatQuarters[normalized]; found {
		return strings.TrimSpace(label), "Q", time.Date(year, month, 1, 0, 0, 0, 0, time.UTC), true
	}
	if normalized == "значение показателя за год" {
		return "значение показателя за год", "Y", time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC), true
	}
	if dash := strings.Index(normalized, "-"); dash > 0 {
		from, to := normalized[:dash], normalized[dash+1:]
		if _, okFrom := fedstatMonths[from]; okFrom {
			if _, okTo := fedstatMonths[to]; okTo {
				return strings.TrimSpace(label), "CUM", time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC), true
			}
		}
	}
	return "", "", time.Time{}, false
}

// fedstatMatrixHeader — первая строка с узнаваемыми периодами. Возвращает
// индекс строки, минимальную колонку периода и подписи периода по колонкам.
func fedstatMatrixHeader(rows [][]fedstatCell, year int) (int, int, []string, error) {
	limit := len(rows)
	if limit > 15 {
		limit = 15
	}
	for r := 0; r < limit; r++ {
		row := rows[r]
		first := -1
		labels := make([]string, len(row))
		recognized := 0
		for c := 0; c < len(row); c++ {
			label := fedstatRowText(row, c)
			if label == "" {
				continue
			}
			if _, _, _, ok := fedstatPeriod(label, year); !ok {
				continue
			}
			if first < 0 {
				first = c
			}
			labels[c] = label
			recognized++
		}
		if recognized > 0 && first >= 0 {
			return r, first, labels, nil
		}
	}
	return 0, 0, nil, fmt.Errorf("шапка периодов не найдена")
}

func fedstatParseMatrix(rows [][]fedstatCell, year, labelCount int) ([]fedstatRecord, error) {
	headerRow, firstPeriod, _, err := fedstatMatrixHeader(rows, year)
	if err != nil {
		return nil, err
	}
	if firstPeriod != labelCount {
		return nil, fmt.Errorf("колонок измерений %d, ожидалось %d", firstPeriod, labelCount)
	}
	header := rows[headerRow]
	records := make([]fedstatRecord, 0, len(rows))
	for r := headerRow + 1; r < len(rows); r++ {
		row := rows[r]
		labels := make([]string, labelCount)
		for i := 0; i < labelCount; i++ {
			labels[i] = fedstatRowText(row, i)
		}
		if !fedstatAnyLabel(labels) {
			continue
		}
		for c := firstPeriod; c < len(row); c++ {
			value, numeric := fedstatRowNumber(row, c)
			if !numeric {
				continue
			}
			label := fedstatRowText(header, c)
			period, periodType, date, ok := fedstatPeriod(label, year)
			if !ok {
				return nil, fmt.Errorf("строка %d, колонка %d: период %q не распознан", r+1, c+1, label)
			}
			records = append(records, fedstatRecord{
				labels: append([]string(nil), labels...), period: period,
				periodType: periodType, date: date, value: value,
			})
		}
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("разбор дал 0 точек")
	}
	return records, nil
}

func fedstatAnyLabel(labels []string) bool {
	for _, label := range labels {
		if strings.TrimSpace(label) != "" {
			return true
		}
	}
	return false
}

func fedstatParseLong(rows [][]fedstatCell, year, labelCount int) ([]fedstatRecord, error) {
	maxCols := fedstatMaxCols(rows)
	periodCol := -1
	for c := 0; c < maxCols; c++ {
		hits := 0
		for _, row := range rows {
			if _, _, _, ok := fedstatPeriod(fedstatRowText(row, c), year); ok {
				hits++
			}
		}
		if hits >= 3 {
			periodCol = c
			break
		}
	}
	if periodCol < 0 {
		return nil, fmt.Errorf("колонка периодов не найдена")
	}
	valueCol := maxCols - 1
	if valueCol == periodCol {
		return nil, fmt.Errorf("колонка значения совпала с колонкой периода")
	}
	labelCols := make([]int, 0, maxCols-2)
	for c := 0; c < maxCols; c++ {
		if c != periodCol && c != valueCol {
			labelCols = append(labelCols, c)
		}
	}
	if len(labelCols) < labelCount {
		return nil, fmt.Errorf("колонок измерений %d, ожидалось %d", len(labelCols), labelCount)
	}
	records := make([]fedstatRecord, 0, len(rows))
	for r, row := range rows {
		period, periodType, date, ok := fedstatPeriod(fedstatRowText(row, periodCol), year)
		if !ok {
			continue
		}
		value, numeric := fedstatRowNumber(row, valueCol)
		if !numeric {
			continue
		}
		labels := make([]string, labelCount)
		for i := 0; i < labelCount; i++ {
			labels[i] = fedstatRowText(row, labelCols[i])
		}
		if !fedstatAnyLabel(labels) {
			return nil, fmt.Errorf("строка %d: подписи измерений пусты", r+1)
		}
		records = append(records, fedstatRecord{labels: labels, period: period, periodType: periodType, date: date, value: value})
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("разбор дал 0 точек")
	}
	return records, nil
}

func fedstatParseYearColumns(rows [][]fedstatCell, labelCount int) ([]fedstatRecord, error) {
	yearRow, firstYear, yearCols, err := fedstatYearHeader(rows)
	if err != nil {
		return nil, err
	}
	if firstYear != labelCount {
		return nil, fmt.Errorf("колонок измерений %d, ожидалось %d", firstYear, labelCount)
	}
	dataStart := yearRow + 1
	if dataStart < len(rows) && !fedstatHasYearNumber(rows[dataStart], yearCols) {
		dataStart++
	}
	records := make([]fedstatRecord, 0, len(rows))
	for r := dataStart; r < len(rows); r++ {
		row := rows[r]
		labels := make([]string, labelCount)
		for i := 0; i < labelCount; i++ {
			labels[i] = fedstatRowText(row, i)
		}
		if !fedstatAnyLabel(labels) {
			continue
		}
		for _, col := range yearCols {
			value, numeric := fedstatRowNumber(row, col.column)
			if !numeric {
				continue
			}
			records = append(records, fedstatRecord{
				labels: labels, period: "значение показателя за год", periodType: "Y",
				date: time.Date(col.year, time.January, 1, 0, 0, 0, 0, time.UTC), value: value,
			})
		}
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("разбор дал 0 точек")
	}
	return records, nil
}

type fedstatYearColumn struct {
	column int
	year   int
}

func fedstatYearHeader(rows [][]fedstatCell) (int, int, []fedstatYearColumn, error) {
	bestRow, bestFirst := -1, -1
	var best []fedstatYearColumn
	for r := 0; r < len(rows) && r < 8; r++ {
		var columns []fedstatYearColumn
		for c := 0; c < len(rows[r]); c++ {
			value, numeric := fedstatRowNumber(rows[r], c)
			if !numeric || value < 1900 || value > 2100 {
				continue
			}
			columns = append(columns, fedstatYearColumn{column: c, year: int(value)})
		}
		if len(columns) > len(best) {
			bestRow, bestFirst, best = r, columns[0].column, columns
		}
	}
	if bestRow < 0 || len(best) == 0 {
		return 0, 0, nil, fmt.Errorf("шапка годов не найдена")
	}
	return bestRow, bestFirst, best, nil
}

func fedstatHasYearNumber(row []fedstatCell, columns []fedstatYearColumn) bool {
	for _, col := range columns {
		if _, numeric := fedstatRowNumber(row, col.column); numeric {
			return true
		}
	}
	return false
}

func fedstatParseActivityColumns(rows [][]fedstatCell, year, labelCount int) ([]fedstatRecord, error) {
	headerRow, firstActivity := -1, -1
	for r := 0; r < len(rows) && r < 10; r++ {
		row := rows[r]
		if len(row) <= labelCount {
			continue
		}
		texts, numbers := 0, 0
		first := -1
		for c := labelCount; c < len(row); c++ {
			if row[c].numeric {
				numbers++
			}
			if text := fedstatRowText(row, c); text != "" {
				texts++
				if first < 0 {
					first = c
				}
			}
		}
		if texts >= 2 && numbers == 0 && first >= 0 {
			headerRow, firstActivity = r, first
			break
		}
	}
	if headerRow < 0 {
		return nil, fmt.Errorf("шапка видов деятельности не найдена")
	}
	dataStart := headerRow + 1
	for dataStart < len(rows) && fedstatIsYearRow(rows[dataStart], labelCount, firstActivity) {
		dataStart++
	}
	header := rows[headerRow]
	records := make([]fedstatRecord, 0, len(rows))
	for r := dataStart; r < len(rows); r++ {
		row := rows[r]
		labels := make([]string, labelCount)
		for i := 0; i < labelCount; i++ {
			labels[i] = fedstatRowText(row, i)
		}
		if !fedstatAnyLabel(labels) {
			continue
		}
		for c := firstActivity; c < len(row); c++ {
			value, numeric := fedstatRowNumber(row, c)
			if !numeric {
				continue
			}
			activity := fedstatRowText(header, c)
			if activity == "" {
				return nil, fmt.Errorf("строка %d, колонка %d: вид деятельности пуст", r+1, c+1)
			}
			records = append(records, fedstatRecord{
				labels: labels, activity: activity, period: "значение показателя за год", periodType: "Y",
				date: time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC), value: value,
			})
		}
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("разбор дал 0 точек")
	}
	return records, nil
}

func fedstatIsYearRow(row []fedstatCell, labelCount, firstActivity int) bool {
	for i := 0; i < labelCount; i++ {
		if fedstatRowText(row, i) != "" {
			return false
		}
	}
	for c := firstActivity; c < len(row); c++ {
		value, numeric := fedstatRowNumber(row, c)
		if !numeric || value < 1900 || value > 2100 {
			return false
		}
	}
	return true
}

func fedstatParseExcel(data []byte, layout fedstatLayout, year, labelCount int) ([]fedstatRecord, error) {
	rows, err := fedstatReadSheet(data)
	if err != nil {
		return nil, err
	}
	switch layout {
	case fedstatLayoutMatrix:
		return fedstatParseMatrix(rows, year, labelCount)
	case fedstatLayoutLong:
		return fedstatParseLong(rows, year, labelCount)
	case fedstatLayoutYearColumns:
		return fedstatParseYearColumns(rows, labelCount)
	case fedstatLayoutActivityColumns:
		return fedstatParseActivityColumns(rows, year, labelCount)
	default:
		return nil, fmt.Errorf("неизвестная раскладка %q", layout)
	}
}
