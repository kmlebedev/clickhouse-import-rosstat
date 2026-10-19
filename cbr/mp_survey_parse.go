package cbr

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// surveyPoint — одна точка ряда мониторинга предприятий ЦБ: имя ряда в macro_series,
// отчётная дата (для месячных рядов — 1-е число отчётного месяца, для квартальных —
// 1-е число месяца, следующего за отчётным кварталом) и значение.
type surveyPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

// Геометрия листов mp_survey_data.xlsx: строка 3 — маркеры блоков «Исходные данные» и
// «Сезонно скорректированные данные», строка 4 — заголовки колонок, данные — с 5-й строки.
const (
	markerRow = 3 // 1-based номер строки маркеров блоков
	headerRow = 4 // 1-based номер строки заголовков колонок
	dataRow   = 5 // 1-based номер первой строки данных

	rawBlockMarker = "Исходные данные"
	saBlockMarker  = "Сезонно скорректированные данные"

	// monthlySheet — агрегатный лист с месячными рядами (даты в первом столбце — серийные
	// номера Excel). Отраслевые листы тоже месячные: их список — mpSurveyIndustrySheets,
	// проверка — isMonthlySurveySheet. Остальные читаемые листы книги квартальные
	// (метки «1к02»).
	monthlySheet = "Экономика всего"

	// Пороги fail-fast: меньше — значит, лист не той книги или изменил структуру.
	minDatedRows     = 200
	minQuarterLabels = 20
)

// quarterRe — метка отчётного квартала в первом столбце квартального листа («1к02»:
// квартал 1, 2002 год; «к» кириллическая).
var quarterRe = regexp.MustCompile(`^[1-4]к([0-9]{2})$`)

// parseSurvey читает книгу мониторинга предприятий ЦБ (mp_survey_data.xlsx) и возвращает
// точки по определениям defs. Числа и даты читаются только в raw-виде (RawCellValue):
// форматированные строки теряют точность («67.02» вместо 67.024800401256897) и
// превращают дату в текст. Листы кэшируются по имени, разбор завершается полностью до
// возврата — при ошибке не отдаётся ни одной точки, поэтому импортёр не отправит
// неполный батч.
func parseSurvey(xlsx *excelize.File, defs []seriesDef) ([]surveyPoint, error) {
	var points []surveyPoint
	for _, sheet := range sheetOrder(defs) {
		data, err := readSheet(xlsx, sheet)
		if err != nil {
			return nil, err
		}
		for _, def := range defs {
			if def.Sheet != sheet {
				continue
			}
			col, err := findColumn(data, def)
			if err != nil {
				return nil, err
			}
			pts, err := extractPoints(xlsx, data, def, col)
			if err != nil {
				return nil, err
			}
			points = append(points, pts...)
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("не извлечено ни одной точки: проверьте структуру книги")
	}
	return points, nil
}

// sheetOrder — имена листов в порядке первого появления в defs (без повторов).
func sheetOrder(defs []seriesDef) []string {
	seen := make(map[string]bool, len(defs))
	var order []string
	for _, def := range defs {
		if !seen[def.Sheet] {
			seen[def.Sheet] = true
			order = append(order, def.Sheet)
		}
	}
	return order
}

// sheetData — разобранная структура листа: заголовки колонок, границы блоков и строки с
// датами (для месячного листа — серийные номера Excel, для квартальных — метки кварталов).
type sheetData struct {
	name    string
	header  []string
	rawFrom int // первый индекс колонки блока «Исходные данные»
	saFrom  int // первый индекс колонки блока «Сезонно скорректированные данные»
	dated   []datedRow
}

// datedRow — строка листа с отчётной датой: номер строки (1-based) и дата.
type datedRow struct {
	row  int
	date time.Time
}

// readSheet валидирует лист и собирает его структуру; порядок проверок — существование
// листа → маркеры блоков → порог строк с датами. Все ошибки содержат имя листа.
func readSheet(xlsx *excelize.File, sheet string) (*sheetData, error) {
	idx, err := xlsx.GetSheetIndex(sheet)
	if err != nil {
		return nil, fmt.Errorf("лист %q: %w", sheet, err)
	}
	if idx < 0 {
		return nil, fmt.Errorf("лист %q не найден в книге", sheet)
	}
	rows, err := xlsx.GetRows(sheet)
	if err != nil {
		return nil, fmt.Errorf("лист %q: чтение строк: %w", sheet, err)
	}
	if len(rows) < headerRow {
		return nil, fmt.Errorf("лист %q: строк %d, нет строки маркеров (%d) и заголовков (%d)", sheet, len(rows), markerRow, headerRow)
	}

	markers := rows[markerRow-1]
	rawFrom, saFrom := -1, -1
	for i, cell := range markers {
		switch cell {
		case rawBlockMarker:
			if rawFrom < 0 {
				rawFrom = i
			}
		case saBlockMarker:
			if saFrom < 0 {
				saFrom = i
			}
		}
	}
	switch {
	case rawFrom < 0:
		return nil, fmt.Errorf("лист %q: в строке %d нет маркера %q", sheet, markerRow, rawBlockMarker)
	case saFrom < 0:
		return nil, fmt.Errorf("лист %q: в строке %d нет маркера %q", sheet, markerRow, saBlockMarker)
	case saFrom <= rawFrom:
		return nil, fmt.Errorf("лист %q: маркер %q (%d) не правее маркера %q (%d)", sheet, saBlockMarker, saFrom, rawBlockMarker, rawFrom)
	}

	header := rows[headerRow-1]

	data := &sheetData{name: sheet, header: header, rawFrom: rawFrom, saFrom: saFrom}
	if isMonthlySurveySheet(sheet) {
		data.dated, err = monthlyRows(xlsx, sheet, rows)
		if err != nil {
			return nil, err
		}
		if len(data.dated) < minDatedRows {
			return nil, fmt.Errorf("лист %q: слишком мало строк с датами (%d, ожидалось не меньше %d)", sheet, len(data.dated), minDatedRows)
		}
		return data, nil
	}

	data.dated = quarterlyRows(rows)
	if len(data.dated) < minQuarterLabels {
		return nil, fmt.Errorf("лист %q: слишком мало кварталов (%d, ожидалось не меньше %d)", sheet, len(data.dated), minQuarterLabels)
	}
	return data, nil
}

// monthlyRows — строки месячного листа, у которых первый столбец является серийным
// номером даты Excel. Пустые строки и сноски (текст вместо числа) пропускаются.
func monthlyRows(xlsx *excelize.File, sheet string, rows [][]string) ([]datedRow, error) {
	var dated []datedRow
	for r := dataRow; r <= len(rows); r++ {
		cell := "A" + strconv.Itoa(r)
		raw, err := xlsx.GetCellValue(sheet, cell, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("лист %q: %s: %w", sheet, cell, err)
		}
		if raw == "" {
			continue
		}
		serial, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			continue // сноска под данными
		}
		date, err := excelize.ExcelDateToTime(serial, false)
		if err != nil {
			return nil, fmt.Errorf("лист %q: %s: серийный номер %q не является датой: %w", sheet, cell, raw, err)
		}
		dated = append(dated, datedRow{row: r, date: date})
	}
	return dated, nil
}

// quarterlyRows — строки квартального листа: метка квартала из первого столбца
// («2к26» → 2026-07-01, месяц, следующий за отчётным кварталом).
func quarterlyRows(rows [][]string) []datedRow {
	var dated []datedRow
	for i := dataRow - 1; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			continue
		}
		m := quarterRe.FindStringSubmatch(rows[i][0])
		if m == nil {
			continue
		}
		quarter := int(m[0][0] - '0')   // метка начинается с ASCII-цифры квартала
		year, err := strconv.Atoi(m[1]) // группа ([0-9]{2}) — две цифры года
		if err != nil {
			continue // недостижимо: шаблон гарантирует две цифры
		}
		dated = append(dated, datedRow{
			row: i + 1,
			// Дата — 1-е число месяца, следующего за отчётным кварталом; месяц 13
			// (4-й квартал) нормализуется в январь следующего года.
			date: time.Date(2000+year, time.Month(3*quarter+1), 1, 0, 0, 0, 0, time.UTC),
		})
	}
	return dated
}

// findColumn — индекс колонки ряда в его блоке данных: ровно одно совпадение подстроки
// def.Header, ни одно из def.Exclude не должно встречаться в заголовке.
func findColumn(data *sheetData, def seriesDef) (int, error) {
	from, to := data.rawFrom, data.saFrom
	block := "исходных данных"
	if def.SA {
		from, to = data.saFrom, len(data.header)
		block = "сезонно скорректированных данных"
	}
	// Строка заголовков может быть короче, чем указывают маркеры (тогда блок пуст —
	// ниже это превратится в «колонка не найдена»).
	to = min(to, len(data.header))
	from = min(from, len(data.header))
	var matched []int
	for i := from; i < to; i++ {
		if !strings.Contains(data.header[i], def.Header) {
			continue
		}
		excluded := false
		for _, ex := range def.Exclude {
			if strings.Contains(data.header[i], ex) {
				excluded = true
				break
			}
		}
		if !excluded {
			matched = append(matched, i)
		}
	}
	switch len(matched) {
	case 0:
		return 0, fmt.Errorf("ряд %s: колонка %q не найдена в блоке %s листа %q", def.Name, def.Header, block, data.name)
	case 1:
		return matched[0], nil
	default:
		return 0, fmt.Errorf("ряд %s: колонка %q неоднозначна в блоке %s листа %q: совпало колонок %d (первые: %s)",
			def.Name, def.Header, block, data.name, len(matched), strings.Join(columnLetters(matched[:min(len(matched), 3)]), ", "))
	}
}

// columnLetters — буквенные имена колонок по их 0-based индексам (для сообщений об ошибке).
func columnLetters(cols []int) []string {
	out := make([]string, 0, len(cols))
	for _, col := range cols {
		name, err := excelize.ColumnNumberToName(col + 1)
		if err != nil {
			name = strconv.Itoa(col + 1)
		}
		out = append(out, name)
	}
	return out
}

// extractPoints — значения ряда по строкам с датами. Пустая ячейка означает отсутствие
// точки (пропуск), непустая нечисловая — ошибку разбора.
func extractPoints(xlsx *excelize.File, data *sheetData, def seriesDef, col int) ([]surveyPoint, error) {
	colName, err := excelize.ColumnNumberToName(col + 1)
	if err != nil {
		return nil, fmt.Errorf("ряд %s: колонка %d: %w", def.Name, col+1, err)
	}
	points := make([]surveyPoint, 0, len(data.dated))
	for _, row := range data.dated {
		cell := colName + strconv.Itoa(row.row)
		raw, err := xlsx.GetCellValue(data.name, cell, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("ряд %s: ячейка %s листа %q: %w", def.Name, cell, data.name, err)
		}
		if raw == "" {
			continue
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("ряд %s: ячейка %s листа %q: значение %q не является числом", def.Name, cell, data.name, raw)
		}
		points = append(points, surveyPoint{Series: def.Name, Date: row.date, Value: value})
	}
	return points, nil
}
