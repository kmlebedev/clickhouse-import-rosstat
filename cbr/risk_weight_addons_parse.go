package cbr

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// riskAddonPoint — ячейка матрицы надбавок: (вид матрицы, период, код строки,
// код столбца) → значение. isApplicable=0 — источник написал «н/п»; value=0 при
// isApplicable=1 — настоящее значение.
type riskAddonPoint struct {
	matrix        string
	validFrom     time.Time
	validTo       time.Time
	tableNo       string
	rowCode       string
	colCode       string
	value         float64
	isApplicable  uint8
	sourceFile    string
	sourceSheet   string
	sourceCaption string
}

const (
	// riskAddonMatrixTitle — маркер листа-матрицы: «Оглавление» и служебные
	// листы не начинаются с этой строки.
	riskAddonMatrixTitle = "Матрица надбавок"
)

var (
	riskAddonRowCodeRe = regexp.MustCompile(`^\d{4}\.i$`)
	riskAddonTableNoRe = regexp.MustCompile(`Таблица\s*№\s*(\d+)`)
)

// cbrRiskWeightAddonsParseBook — разбор книги надбавок. На входе открытая книга
// и файл страницы; внутри нет сети и БД (стандарт §6.1). Лист «Оглавление»
// пропускается; каждый лист-матрица даёт плоский набор ячеек.
func cbrRiskWeightAddonsParseBook(xlsx *excelize.File, file riskAddonFile) ([]riskAddonPoint, error) {
	var points []riskAddonPoint
	for _, sheet := range xlsx.GetSheetList() {
		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %q: %w", cbrRiskWeightAddonsTable, sheet, err)
		}
		if len(rows) == 0 || !strings.HasPrefix(strings.TrimSpace(ccybCell(rows[0], 0)), riskAddonMatrixTitle) {
			continue
		}
		sheetPoints, err := cbrRiskWeightAddonsParseSheet(rows, file, sheet)
		if err != nil {
			return nil, err
		}
		points = append(points, sheetPoints...)
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: файл %s не дал ни одной ячейки матрицы", cbrRiskWeightAddonsTable, file.Name)
	}
	return points, nil
}

// cbrRiskWeightAddonsParseSheet разбирает один лист-матрицу. Вёрстка дрейфует
// (с 2022 года — Таблицы 1/2, до — общая матрица, с 2013 по 2026), поэтому
// якоря ищутся по подписям «с», «по», «Код», а не по номерам строк.
func cbrRiskWeightAddonsParseSheet(rows [][]string, file riskAddonFile, sheet string) ([]riskAddonPoint, error) {
	validFrom, validTo, err := riskAddonPeriod(rows)
	if err != nil {
		return nil, fmt.Errorf("%s: лист %q: %w", cbrRiskWeightAddonsTable, sheet, err)
	}
	matrix := riskAddonMatrix(rows)
	tableNo := riskAddonTableNo(rows)

	header := -1
	for i, row := range rows {
		if strings.TrimSpace(ccybCell(row, 0)) == "Код" {
			header = i
			break
		}
	}
	if header < 0 {
		return nil, fmt.Errorf("%s: лист %q: не найдена строка «Код»", cbrRiskWeightAddonsTable, sheet)
	}
	var colCodes []string
	for _, cell := range rows[header][1:] {
		code := strings.TrimSpace(cell)
		if code == "" {
			break
		}
		colCodes = append(colCodes, code)
	}
	if len(colCodes) == 0 {
		return nil, fmt.Errorf("%s: лист %q: в строке «Код» нет кодов столбцов", cbrRiskWeightAddonsTable, sheet)
	}

	var points []riskAddonPoint
	for i := header + 1; i < len(rows); i++ {
		rowCode := strings.TrimSpace(ccybCell(rows[i], 0))
		if rowCode == "" {
			break
		}
		if !riskAddonRowCodeRe.MatchString(rowCode) {
			// После матрицы могут идти сноски и примечания; первая строка не
			// с кодом строки — конец матрицы, а не ошибка.
			break
		}
		for j, colCode := range colCodes {
			raw := strings.TrimSpace(ccybCell(rows[i], j+1))
			value, applicable, err := riskAddonCellValue(raw)
			if err != nil {
				return nil, fmt.Errorf("%s: лист %q, строка %d, столбец %s: %w",
					cbrRiskWeightAddonsTable, sheet, i+1, colCode, err)
			}
			points = append(points, riskAddonPoint{
				matrix:        matrix,
				validFrom:     validFrom,
				validTo:       validTo,
				tableNo:       tableNo,
				rowCode:       rowCode,
				colCode:       colCode,
				value:         value,
				isApplicable:  applicable,
				sourceFile:    file.Name,
				sourceSheet:   sheet,
				sourceCaption: file.Caption,
			})
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: лист %q: матрица не дала ни одной ячейки", cbrRiskWeightAddonsTable, sheet)
	}
	return points, nil
}

// riskAddonPeriod — период из строки «с <дата> [по <дата>]». Пустой «по» —
// «дальше»: сентинел 2100-01-01, чтобы дата оставалась обычным Date и входила
// в ключ ReplacingMergeTree.
func riskAddonPeriod(rows [][]string) (from, to time.Time, err error) {
	for _, row := range rows {
		if strings.TrimSpace(ccybCell(row, 0)) != "с" {
			continue
		}
		rawFrom := strings.TrimSpace(ccybCell(row, 1))
		if rawFrom == "" {
			rawFrom = strings.TrimSpace(ccybCell(row, 2))
		}
		from, ok := ccybExcelDate(rawFrom)
		if !ok {
			return time.Time{}, time.Time{}, fmt.Errorf("начало периода %q не дата", rawFrom)
		}
		to = mustRiskAddonOpenEnded()
		for j := 1; j+1 < len(row); j++ {
			if strings.TrimSpace(row[j]) != "по" {
				continue
			}
			rawTo := strings.TrimSpace(ccybCell(row, j+1))
			if rawTo == "" {
				break
			}
			parsed, ok := ccybExcelDate(rawTo)
			if !ok {
				return time.Time{}, time.Time{}, fmt.Errorf("конец периода %q не дата", rawTo)
			}
			to = parsed
			break
		}
		return from, to, nil
	}
	return time.Time{}, time.Time{}, fmt.Errorf("не найдена строка периода «с»")
}

func mustRiskAddonOpenEnded() time.Time {
	return time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
}

// riskAddonMatrix — вид матрицы по строке 2: «физическим лицам» — Таблица 1,
// «юридическим лицам» — Таблица 2, иначе старая общая матрица (legacy).
func riskAddonMatrix(rows [][]string) string {
	var title string
	for i := 0; i < len(rows) && i < 3; i++ {
		title += " " + strings.Join(rows[i], " ")
	}
	switch {
	case strings.Contains(title, "физическим лицам"):
		return "retail"
	case strings.Contains(title, "юридическим лицам"):
		return "corporate"
	default:
		return "legacy"
	}
}

// riskAddonTableNo — «Таблица № 15» из первой строки, если она есть; у старой
// общей матрицы номера в листе нет (он только в «Оглавлении»).
func riskAddonTableNo(rows [][]string) string {
	if len(rows) == 0 {
		return ""
	}
	for _, cell := range rows[0] {
		if m := riskAddonTableNoRe.FindStringSubmatch(cell); m != nil {
			return "Таблица № " + m[1]
		}
	}
	return ""
}

// riskAddonCellValue — значение ячейки матрицы. «н/п», x/х, дефис и пусто —
// «не применяется» (0/0); число — значение; иная строка — ошибка.
func riskAddonCellValue(raw string) (value float64, applicable uint8, err error) {
	raw = strings.TrimSpace(raw)
	switch strings.ToLower(raw) {
	case "", "н/п", "x", "х", "-":
		return 0, 0, nil
	}
	v, convErr := strconv.ParseFloat(strings.ReplaceAll(raw, ",", "."), 64)
	if convErr != nil {
		return 0, 0, fmt.Errorf("значение %q не число", raw)
	}
	return v, 1, nil
}
