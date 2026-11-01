package rosstat

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/xuri/excelize/v2"
)

const (
	// Имена файлов источника — часть ключа строки таблицы: по ним различаются
	// товарный файл (листья) и файл групп (группы и товары).
	cpiWeightsFileItems  = "CPR_tov_RF"
	cpiWeightsFileGroups = "CPR_Gr-Tov-RF"

	// Минимальное число позиций на лист: товарный лист 2016+ содержит 500+ кодов,
	// групповой — группы и товары. Порог ловит смену макета, а не состав года.
	cpiWeightsItemsMinRows  = 400
	cpiWeightsGroupsMinRows = 50

	// Сумма товарного листа (без итоговой строки «Все товары и услуги») — 100 %.
	cpiWeightsSumTarget    = 100.0
	cpiWeightsSumTolerance = 0.5
	cpiWeightsSumMin       = 90.0

	// Варианты листа 2022: публикация разбита на «до 01.04» и «с 01.04».
	cpiWeightsVariantBefore = "before-2022-04-01"
	cpiWeightsVariantFrom   = "from-2022-04-01"

	// Горизонт, ниже которого файл не может быть актуальным: 2024 — конец архива
	// CB_COMP, с которого модель начала зависеть от фолбэка.
	cpiWeightsMinHorizonYear = 2024
)

var (
	// Код и название в одной ячейке: «10 Мясопродукты», «930.АГ Услуги…».
	cpiWeightsCodeName = regexp.MustCompile(`^\s*(\d+(?:[.\-][\p{L}\p{N}]+)*)\s+(.+)$`)
	cpiWeightsYear     = regexp.MustCompile(`(19|20)\d{2}`)
	cpiWeightsSlug     = regexp.MustCompile(`[^0-9a-zа-яё]+`)
	cpiWeightsWideGap  = regexp.MustCompile(`\s{2,}|\t`)
)

// cpiWeightPoint — точка таблицы: год, вариант листа, файл, уровень, код, название, вес (%).
type cpiWeightPoint struct {
	year    uint16
	variant string
	file    string
	level   uint8
	code    string
	name    string
	weight  float64
}

type cpiWeightKey struct {
	year    uint16
	variant string
	file    string
	code    string
}

// cpiWeightColumns — найденные колонки шапки. code и name совпадают, когда
// код и название лежат в одной ячейке.
type cpiWeightColumns struct {
	header   int
	code     int
	name     int
	weight   int
	level    int
	combined bool
}

// cpiWeightSheetMeta разбирает имя листа: год и остаток подписи.
func cpiWeightSheetMeta(sheet string) (uint16, string, bool) {
	match := cpiWeightsYear.FindString(sheet)
	if match == "" {
		return 0, "", false
	}
	year, err := strconv.Atoi(match)
	if err != nil || year < 2000 || year > 2100 {
		return 0, "", false
	}
	return uint16(year), strings.TrimSpace(strings.Replace(sheet, match, "", 1)), true
}

// cpiWeightVariant превращает остаток подписи листа в вариант. Для единственного
// листа года вариант пуст; при двух листах 2022 различаются «до» и «с 01.04».
func cpiWeightVariant(suffix string) string {
	text := strings.ToLower(strings.Join(strings.Fields(suffix), " "))
	if text == "" {
		return ""
	}
	hasDate := strings.Contains(text, "01.04") || strings.Contains(text, "01 04")
	hasBefore := strings.Contains(text, "до")
	hasAfter := strings.Contains(text, "с 01") || strings.Contains(text, "после") || strings.Contains(text, "from")
	if hasDate && hasBefore {
		return cpiWeightsVariantBefore
	}
	if hasDate && hasAfter {
		return cpiWeightsVariantFrom
	}
	slug := strings.Trim(cpiWeightsSlug.ReplaceAllString(text, "-"), "-")
	if slug == "" {
		return "variant"
	}
	return slug
}

// cpiWeightColumnsFor ищет шапку по подписям колонок. Поддерживаются оба макета
// Q5: «код | название | СПР» и «код  название | СПР» (код и название в одной
// ячейке). Возвращает false, если лист не похож на таблицу весов.
func cpiWeightColumnsFor(rows [][]string) (cpiWeightColumns, bool) {
	limit := len(rows)
	if limit > 40 {
		limit = 40
	}
	for i := 0; i < limit; i++ {
		columns := cpiWeightColumns{header: i, code: -1, name: -1, weight: -1, level: -1}
		for j, cell := range rows[i] {
			label := strings.ToLower(strings.Join(strings.Fields(cell), " "))
			if label == "" {
				continue
			}
			if columns.weight < 0 && (strings.Contains(label, "спр") || strings.Contains(label, "вес") || strings.Contains(label, "удельн")) {
				columns.weight = j
			}
			if columns.level < 0 && strings.Contains(label, "уров") {
				columns.level = j
			}
			if strings.Contains(label, "код") {
				if columns.code < 0 {
					columns.code = j
				}
				if strings.Contains(label, "наимен") {
					columns.combined = true
					if columns.name < 0 {
						columns.name = j
					}
				}
			}
			if columns.name < 0 && strings.Contains(label, "наимен") {
				columns.name = j
			}
		}
		if columns.weight < 0 && columns.name >= 0 {
			// Макет 2024-винтажа: «N п/п | Код товара | Наименование | вес» —
			// у колонки веса нет подписи «СПР», она сразу после названия.
			columns.weight = columns.name + 1
		}
		if columns.weight < 0 {
			continue
		}
		if columns.code < 0 && columns.name < 0 && columns.weight > 0 {
			// Макет «код  название | СПР»: одна колонка до веса.
			columns.code, columns.name, columns.combined = 0, 0, true
		}
		// Лист без кода (групповой файл 2016–2024) — это не ошибка: у него есть
		// название и вес, а код в источнике не публикуется. Такие строки получают
		// синтетический ключ в parseCPIWeightsSheet.
		if columns.name >= 0 || columns.code >= 0 {
			return columns, true
		}
	}
	return cpiWeightColumns{}, false
}

// cpiWeightLooksLikeCode — первая часть составной ячейки похожа на код.
func cpiWeightLooksLikeCode(text string) bool {
	if text == "" || text[0] < '0' || text[0] > '9' {
		return false
	}
	for _, r := range text {
		if unicode.IsDigit(r) || unicode.IsLetter(r) || r == '.' || r == '-' {
			continue
		}
		return false
	}
	return true
}

// cpiWeightSplitCodeName делит составную ячейку на код и название.
func cpiWeightSplitCodeName(cell string) (string, string) {
	text := strings.TrimSpace(cell)
	if text == "" {
		return "", ""
	}
	if match := cpiWeightsCodeName.FindStringSubmatch(text); match != nil {
		return strings.TrimSpace(match[1]), strings.TrimSpace(match[2])
	}
	if parts := cpiWeightsWideGap.Split(text, 2); len(parts) == 2 {
		code := strings.TrimSpace(parts[0])
		if cpiWeightLooksLikeCode(code) {
			return code, strings.TrimSpace(parts[1])
		}
	}
	return "", text
}

// cpiWeightCell безопасно читает ячейку: GetRows обрезает хвостовые пустые
// ячейки, и короткая строка — норма источника, а не паника.
func cpiWeightCell(row []string, index int) string {
	if index < 0 || index >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[index])
}

// cpiWeightValue разбирает вес: запятая или точка, пробелы-разделители,
// «-»/«…»/пустая ячейка — пропуск, нечисловой текст — ошибка.
func cpiWeightValue(cell string) (float64, bool, error) {
	text := strings.TrimSpace(strings.ReplaceAll(cell, "\u00a0", ""))
	switch text {
	case "", "-", "—", "–", "…", "...", "х", "Х":
		return 0, false, nil
	}
	text = strings.ReplaceAll(text, " ", "")
	text = strings.ReplaceAll(text, ",", ".")
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, false, fmt.Errorf("вес %q не число: %w", cell, err)
	}
	return value, true, nil
}

// cpiWeightIsTotal — итоговая строка «Все товары и услуги» (код 1 или подпись):
// в сумму товарного листа она не входит, иначе лист дал бы 200 %.
func cpiWeightIsTotal(point cpiWeightPoint) bool {
	if point.code == "1" {
		return true
	}
	name := strings.ToLower(strings.TrimSpace(point.name))
	return strings.HasPrefix(name, "все товары")
}

// cpiWeightsNameCode — синтетический ключ строки без кода: нормализованное
// название с префиксом name:. Префикс отделяет такие строки от кодов Росстата
// и позволяет каталогу их пропускать.
func cpiWeightsNameCode(name string) string {
	text := strings.ToLower(strings.Join(strings.Fields(strings.ReplaceAll(name, "\n", " ")), " "))
	return "name:" + text
}

// cpiWeightLevel разбирает колонку уровня, если она есть.
func cpiWeightLevel(cell string, rowNumber int) (uint8, error) {
	text := strings.TrimSpace(cell)
	if text == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(text)
	if err != nil || value < 0 || value > 255 {
		return 0, fmt.Errorf("строка %d: уровень %q не целое 0…255", rowNumber, cell)
	}
	return uint8(value), nil
}

// parseCPIWeightsSheet разбирает один лист-год.
func parseCPIWeightsSheet(rows, formattedRows [][]string, sheet, file string, year uint16) ([]cpiWeightPoint, error) {
	columns, ok := cpiWeightColumnsFor(formattedRows)
	if !ok {
		return nil, fmt.Errorf("%s: лист %s: шапка с колонками кода и СПР не найдена", file, sheet)
	}
	points := make([]cpiWeightPoint, 0, len(rows))
	seen := make(map[string]cpiWeightPoint, len(rows))
	skipped := 0
	for i := columns.header + 1; i < len(rows); i++ {
		row := rows[i]
		formatted := formattedRows[i]
		weightCell := cpiWeightCell(row, columns.weight)
		if weightCell == "" {
			weightCell = cpiWeightCell(formatted, columns.weight)
		}
		weight, ok, err := cpiWeightValue(weightCell)
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s, строка %d: %w", file, sheet, i+1, err)
		}
		if !ok {
			continue
		}
		var code, name string
		switch {
		case columns.combined:
			code, name = cpiWeightSplitCodeName(cpiWeightCell(formatted, columns.name))
		case columns.code < 0:
			// Групповой файл 2016–2024: кода в источнике нет. Синтетический ключ
			// `name:<нормализованное название>` держит строку в ключе таблицы и явно
			// говорит, что это не код Росстата; модель читает только товарный файл.
			name = cpiWeightCell(formatted, columns.name)
			code = cpiWeightsNameCode(name)
		default:
			code = cpiWeightCell(formatted, columns.code)
			name = cpiWeightCell(formatted, columns.name)
			if code == "" && name != "" {
				code, name = cpiWeightSplitCodeName(name)
			}
		}
		if code == "" || name == "" {
			// Итоговые и служебные строки без кода/названия не точки ряда.
			skipped++
			continue
		}
		if strings.EqualFold(code, "код") || strings.HasPrefix(strings.ToLower(name), "наименование") {
			continue
		}
		level, err := cpiWeightLevel(cpiWeightCell(formatted, columns.level), i+1)
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s, строка %d: %w", file, sheet, i+1, err)
		}
		point := cpiWeightPoint{year: year, file: file, level: level, code: code, name: name, weight: weight}
		if previous, exists := seen[code]; exists {
			if previous.weight != weight {
				return nil, fmt.Errorf("%s: лист %s, код %s встречается дважды с весами %v и %v",
					file, sheet, code, previous.weight, weight)
			}
			continue
		}
		seen[code] = point
		points = append(points, point)
	}
	if len(points) < cpiWeightsMinRowsFor(file) {
		return nil, fmt.Errorf("%s: лист %s: позиций %d, ожидалось не меньше %d", file, sheet, len(points), cpiWeightsMinRowsFor(file))
	}
	if skipped > len(points)/10 {
		return nil, fmt.Errorf("%s: лист %s: пропущено %d строк из %d — макет изменился", file, sheet, skipped, len(points)+skipped)
	}
	if err := cpiWeightsNormalizeScale(file, sheet, points); err != nil {
		return nil, err
	}
	if err := cpiWeightsValidateSheetSum(file, sheet, points); err != nil {
		return nil, err
	}
	return points, nil
}

func cpiWeightsMinRowsFor(file string) int {
	if file == cpiWeightsFileGroups {
		return cpiWeightsGroupsMinRows
	}
	return cpiWeightsItemsMinRows
}

// cpiWeightsNormalizeScale приводит лист к процентам. Винтажи источника
// публикуют вес и в долях (сумма ≈ 1), и в процентах (сумма ≈ 100): 2024-винтаж
// — доли, 2025/2026 — проценты. Определение по сумме без итоговой строки;
// масштаб применяется ко всем точкам листа, включая итоговую.
func cpiWeightsNormalizeScale(file, sheet string, points []cpiWeightPoint) error {
	sum := 0.0
	for _, point := range points {
		if !cpiWeightIsTotal(point) {
			sum += point.weight
		}
	}
	switch {
	case sum > 0 && sum <= 2:
		for i := range points {
			points[i].weight *= 100
		}
	case file == cpiWeightsFileItems && (sum < cpiWeightsSumMin || sum > cpiWeightsSumTarget+cpiWeightsSumTolerance):
		return fmt.Errorf("%s: лист %s: сумма весов %.6f не похожа ни на доли, ни на проценты", file, sheet, sum)
	}
	return nil
}

// cpiWeightsValidateSheetSum проверяет сумму листа без итоговой строки. Для
// товарного файла сумма обязана быть 100 ± 0,5 в каждом году. Для группового
// файла: если он содержит только верхний уровень, сумма тоже 100; файл со всеми
// уровнями даёт сумму больше 100 — тогда проверка не применяется (уровни
// пересекаются, нормировать нечем).
//
// 2025-винтаж товарного файла не дотягивает до 100 на вес объединённой группы
// «Воздушный и железнодорожный транспорт» (0,859 %): в нём нет отдельных строк
// 9260/9290. Это свойство публикации, а не ошибка разбора, поэтому нижняя
// граница — 90 %, а гэп называется в отчёте cb-comp-weights-check.
func cpiWeightsValidateSheetSum(file, sheet string, points []cpiWeightPoint) error {
	sum := 0.0
	for _, point := range points {
		if !cpiWeightIsTotal(point) {
			sum += point.weight
		}
	}
	if file == cpiWeightsFileItems {
		if sum < cpiWeightsSumMin || sum > cpiWeightsSumTarget+cpiWeightsSumTolerance {
			return fmt.Errorf("%s: лист %s: сумма весов %.6f, ожидалось %.1f…%.1f", file, sheet, sum, cpiWeightsSumMin, cpiWeightsSumTarget+cpiWeightsSumTolerance)
		}
		return nil
	}
	if sum < cpiWeightsSumMin {
		return fmt.Errorf("%s: лист %s: сумма весов %.6f, ожидалось не меньше %.1f", file, sheet, sum, cpiWeightsSumMin)
	}
	return nil
}

// parseCPIWeights разбирает книгу целиком. Вариант листа назначается после
// чтения: у года с одним листом он пуст, у 2022 с двумя — before/from.
func parseCPIWeights(xlsx *excelize.File, file string) ([]cpiWeightPoint, error) {
	type sheetPoints struct {
		sheet  string
		year   uint16
		suffix string
		points []cpiWeightPoint
	}
	var sheets []sheetPoints
	for _, sheet := range xlsx.GetSheetList() {
		year, suffix, ok := cpiWeightSheetMeta(sheet)
		if !ok {
			continue
		}
		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", file, sheet, err)
		}
		formattedRows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: false})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", file, sheet, err)
		}
		points, err := parseCPIWeightsSheet(rows, formattedRows, sheet, file, year)
		if err != nil {
			return nil, err
		}
		sheets = append(sheets, sheetPoints{sheet: sheet, year: year, suffix: suffix, points: points})
	}
	if len(sheets) == 0 {
		return nil, fmt.Errorf("%s: ни одного листа-года не найдено", file)
	}

	byYear := make(map[uint16][]int, len(sheets))
	for index, sheet := range sheets {
		byYear[sheet.year] = append(byYear[sheet.year], index)
	}
	variants := make(map[int]string, len(sheets))
	for year, indexes := range byYear {
		if len(indexes) == 1 {
			variants[indexes[0]] = ""
			continue
		}
		for _, index := range indexes {
			variant := cpiWeightVariant(sheets[index].suffix)
			if variant == "" {
				return nil, fmt.Errorf("%s: у года %d несколько листов, но подпись %q не различает вариант", file, year, sheets[index].sheet)
			}
			variants[index] = variant
		}
	}

	merged := make(map[cpiWeightKey]cpiWeightPoint)
	for index, sheet := range sheets {
		variant := variants[index]
		for _, point := range sheet.points {
			point.variant = variant
			key := cpiWeightKey{year: point.year, variant: point.variant, file: point.file, code: point.code}
			if previous, exists := merged[key]; exists {
				if previous.weight != point.weight || previous.name != point.name {
					return nil, fmt.Errorf("%s: ключ (%d, %q, %s) встречается дважды с разными значениями",
						file, key.year, key.variant, key.code)
				}
				continue
			}
			merged[key] = point
		}
	}

	result := make([]cpiWeightPoint, 0, len(merged))
	for _, point := range merged {
		result = append(result, point)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].year != result[j].year {
			return result[i].year < result[j].year
		}
		if result[i].variant != result[j].variant {
			return result[i].variant < result[j].variant
		}
		return result[i].code < result[j].code
	})
	if err := cpiWeightsValidateCoverage(result, file); err != nil {
		return nil, err
	}
	return result, nil
}

// cpiWeightsValidateCoverage проверяет непрерывность лет и горизонт. Порядок
// кодов и строк — по sort в parseCPIWeights, поэтому проверка детерминирована.
func cpiWeightsValidateCoverage(points []cpiWeightPoint, file string) error {
	if len(points) == 0 {
		return fmt.Errorf("%s: разбор дал 0 точек", file)
	}
	years := make(map[uint16]bool, len(points))
	for _, point := range points {
		years[point.year] = true
	}
	minYear, maxYear := uint16(9999), uint16(0)
	for year := range years {
		if year < minYear {
			minYear = year
		}
		if year > maxYear {
			maxYear = year
		}
	}
	if maxYear < cpiWeightsMinHorizonYear {
		return fmt.Errorf("%s: последний год %d, ожидался не раньше %d", file, maxYear, cpiWeightsMinHorizonYear)
	}
	for year := minYear; year <= maxYear; year++ {
		if !years[year] {
			return fmt.Errorf("%s: нет листа за %d (годы %d–%d должны идти без пропусков)", file, year, minYear, maxYear)
		}
	}
	return nil
}
