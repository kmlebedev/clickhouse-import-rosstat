package rosstat

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	// ppiVedTotalCode — совокупный ряд «Промышленность»: строка «Российская Федерация»
	// на листах 7.x/8.x и единственная строка итога на остальных листах.
	ppiVedTotalCode = "1323500.029.31"

	ppiVedIndexMM     = "m/m"
	ppiVedIndexYTD    = "ytd"
	ppiVedIndexYY     = "y/y"
	ppiVedIndexPeriod = "period"

	// Имена рядов объявлены кодом, а не берутся из подписи строки источника: подпись
	// содержит редакцию («на основе ОКВЭД2») и меняется от выпуска к выпуску (§6.8).
	ppiVedSeriesMM     = "ИЦП по ВЭД, РФ, m/m"
	ppiVedSeriesYTD    = "ИЦП по ВЭД, РФ, ytd"
	ppiVedSeriesYY     = "ИЦП по ВЭД, РФ, y/y"
	ppiVedSeriesPeriod = "ИЦП по ВЭД, РФ, period"
)

var (
	// ppiVedSheetRe — листы с индексами: 1.1/2.1 — legacy-группы, 3.x–8.x — годовые.
	// Группа 1/2 читается только по .1: остальные листы — разделы B/C/D/E, они вне T1.
	ppiVedSheetRe = regexp.MustCompile(`^(\d+)\.([1-4])$`)
	// ppiVedFootnoteRe — хвостовые сноски источника: «январь1)», «20241)2)».
	ppiVedFootnoteRe = regexp.MustCompile(`(?:\s*\d+\))+$`)
	// ppiVedValueFootnoteRe — сноска, приклеенная к числу: «98,121)», «103,361)2)».
	// Снимается ровно одна цифра на группу: цифры перед «)» — часть десятичной дроби,
	// и жадное \d+ срезало бы «12» из «98,12».
	ppiVedValueFootnoteRe = regexp.MustCompile(`(?:\d\))+$`)
	// ppiVedYearPrefixRe — год в подписи шапки; сноска после года уже снята.
	ppiVedYearPrefixRe = regexp.MustCompile(`^((?:19|20)\d{2})`)
	// ppiVedYearTitleRe — год в заголовке годового листа: «… в 2026 г.1)».
	ppiVedYearTitleRe = regexp.MustCompile(`(?i)в\s+((?:19|20)\d{2})\s*г`)
)

// ppiVedKey — ключ точки: дата (последний день месяца) и тип индекса. Код в T1 один,
// поэтому в ключе слияния он не участвует; в таблице ключ включает code.
type ppiVedKey struct {
	date      time.Time
	indexType string
}

type ppiVedPoint struct {
	code      string
	name      string
	indexType string
	date      time.Time
	value     float64
}

// ppiVedCell — ячейка строки по индексу; выход за границу среза — пустая ячейка, а не
// паника (§6.5): GetRows обрезает хвостовые пустые ячейки.
func ppiVedCell(row []string, col int) string {
	if col < 0 || col >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[col])
}

// ppiVedNormalizeLabel сжимает пробелы (включая NBSP и переводы строк) и снимает
// хвостовые сноски источника. Подписи месяцев в листах приходят как «январь1)».
func ppiVedNormalizeLabel(raw string) string {
	label := strings.Join(strings.Fields(raw), " ")
	label = ppiVedFootnoteRe.ReplaceAllString(label, "")
	return strings.TrimSpace(label)
}

// ppiVedMonth разрешает подпись месяца через общий словарь пакета util.
func ppiVedMonth(raw string) (time.Month, bool) {
	label := strings.ToLower(ppiVedNormalizeLabel(raw))
	month, ok := util.MonthsToNum[label]
	return month, ok
}

// ppiVedYear разбирает год из подписи шапки. «20221)» → 2022; «2024 г.» → 2024.
func ppiVedYear(raw string) (int, bool) {
	text := strings.TrimSpace(raw)
	match := ppiVedYearPrefixRe.FindStringSubmatch(text)
	if match == nil {
		return 0, false
	}
	year, err := strconv.Atoi(match[1])
	if err != nil || year < 1990 || year > 2100 {
		return 0, false
	}
	return year, true
}

// ppiVedYearFromTitle берёт год годового листа из заголовка «… в 2026 г.». Номер
// листа не используется: в 2027 появится 9.x, и код не должен требовать правки.
func ppiVedYearFromTitle(rows [][]string) (int, error) {
	limit := 3
	if len(rows) < limit {
		limit = len(rows)
	}
	for i := 0; i < limit; i++ {
		line := strings.Join(rows[i], " ")
		match := ppiVedYearTitleRe.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		year, err := strconv.Atoi(match[1])
		if err != nil {
			continue
		}
		return year, nil
	}
	return 0, fmt.Errorf("год «в YYYY г.» не найден в заголовке листа")
}

// ppiVedValue разбирает значение ячейки. Пустая ячейка и маркеры пропуска источника
// («…», «...», «-», «—», «х», «X») — не точка; нечисловой текст — ошибка (§6.4).
// NBSP и пробелы внутри числа снимаются, запятая принимается как десятичный разделитель.
func ppiVedValue(raw string) (float64, bool, error) {
	text := strings.TrimSpace(raw)
	text = strings.ReplaceAll(text, "\u00a0", "")
	text = strings.ReplaceAll(text, " ", "")
	// Сноска источника может стоять прямо на числе: «98,121)». Снимается отдельным
	// правилом, которое сохраняет цифры десятичной дроби; иначе значение с запятой и
	// сноской не разберётся.
	text = ppiVedValueFootnoteRe.ReplaceAllString(text, "")
	if text == "" {
		return 0, false, nil
	}
	switch text {
	case "...", "…", "-", "—", "х", "X":
		return 0, false, nil
	}
	value, err := strconv.ParseFloat(strings.ReplaceAll(text, ",", "."), 64)
	if err != nil {
		return 0, false, fmt.Errorf("значение ячейки %q не число: %w", raw, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false, fmt.Errorf("значение ячейки %q не конечное число", raw)
	}
	return value, true, nil
}

// ppiVedLastDay — последний день отчётного месяца в UTC.
func ppiVedLastDay(year int, month time.Month) time.Time {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)
}

func ppiVedSeriesName(indexType string) string {
	switch indexType {
	case ppiVedIndexMM:
		return ppiVedSeriesMM
	case ppiVedIndexYTD:
		return ppiVedSeriesYTD
	case ppiVedIndexYY:
		return ppiVedSeriesYY
	case ppiVedIndexPeriod:
		return ppiVedSeriesPeriod
	default:
		return indexType
	}
}

func ppiVedIndexOrder(indexType string) int {
	switch indexType {
	case ppiVedIndexMM:
		return 0
	case ppiVedIndexYTD:
		return 1
	case ppiVedIndexYY:
		return 2
	case ppiVedIndexPeriod:
		return 3
	default:
		return 4
	}
}

// ppiVedAdd добавляет точку в карту источника и падает на дубле с другим значением:
// два разных числа по одному ключу означают, что разметка листа изменилась.
func ppiVedAdd(points map[ppiVedKey]float64, key ppiVedKey, value float64, context string) error {
	if previous, ok := points[key]; ok {
		if previous != value {
			return fmt.Errorf("%s: ключ (%s, %s) встречается дважды со значениями %v и %v",
				context, key.date.Format("2006-01-02"), key.indexType, previous, value)
		}
		return nil
	}
	points[key] = value
	return nil
}

// ppiVedRowHasValue — есть ли в строке хотя бы одна непустая ячейка в месячных
// колонках. Нужно, чтобы отличить RF-only листы 3.x–6.x (значения в строке кода) от
// листов 7.x–8.x (значения в следующей строке «Российская Федерация»). Нечисловая
// ячейка тоже считается заполненной: тогда ошибку назовёт разбор значения, а не
// сообщение «нет строки РФ».
func ppiVedRowHasValue(row []string, monthCols map[int]time.Month) bool {
	for col := range monthCols {
		if ppiVedCell(row, col) != "" {
			return true
		}
	}
	return false
}

// parsePPIVedLegacySheet читает листы 1.1 и 2.1: блок «К предыдущему месяцу» и
// декабрьскую строку «Декабрь в % к декабрю предыдущего года» (ytd только за декабрь).
func parsePPIVedLegacySheet(rows [][]string, sheet string) (map[ppiVedKey]float64, error) {
	header := -1
	for i, row := range rows {
		years := 0
		for _, cell := range row[1:] {
			if _, ok := ppiVedYear(cell); ok {
				years++
			}
		}
		if years >= 5 {
			header = i
			break
		}
	}
	if header < 0 {
		return nil, fmt.Errorf("%s: лист %s: строка годов не найдена", rosstatPPIVedTable, sheet)
	}

	years := make([]int, 0, len(rows[header])-1)
	for col := 1; col < len(rows[header]); col++ {
		year, ok := ppiVedYear(ppiVedCell(rows[header], col))
		if !ok {
			years = append(years, 0)
			continue
		}
		years = append(years, year)
	}

	marker := -1
	for i := header + 1; i < len(rows); i++ {
		label := strings.ToLower(ppiVedNormalizeLabel(ppiVedCell(rows[i], 0)))
		if strings.Contains(label, "к предыдущему месяцу") {
			marker = i
			break
		}
	}
	if marker < 0 {
		return nil, fmt.Errorf("%s: лист %s: маркер «К предыдущему месяцу» не найден", rosstatPPIVedTable, sheet)
	}

	points := make(map[ppiVedKey]float64)
	ytdFound := false
	for i := marker + 1; i < len(rows); i++ {
		row := rows[i]
		label := ppiVedNormalizeLabel(ppiVedCell(row, 0))
		lower := strings.ToLower(label)
		if strings.HasPrefix(lower, "декабрь в %") {
			ytdFound = true
			for col, year := range years {
				if year == 0 {
					continue
				}
				value, ok, err := ppiVedValue(ppiVedCell(row, col+1))
				if err != nil {
					return nil, fmt.Errorf("%s: лист %s, строка %d, год %d: %w", rosstatPPIVedTable, sheet, i+1, year, err)
				}
				if !ok {
					continue
				}
				key := ppiVedKey{date: ppiVedLastDay(year, time.December), indexType: ppiVedIndexYTD}
				if err := ppiVedAdd(points, key, value, fmt.Sprintf("%s: лист %s, строка %d", rosstatPPIVedTable, sheet, i+1)); err != nil {
					return nil, err
				}
			}
			break
		}

		month, ok := ppiVedMonth(label)
		if !ok {
			// До декабрьской строки неизвестная подпись с числом — потерянный месяц,
			// а не текст сносок: молча пропустить его значит отдать дыру в ряду.
			for col, year := range years {
				if year == 0 {
					continue
				}
				if _, filled, err := ppiVedValue(ppiVedCell(row, col+1)); err != nil {
					return nil, fmt.Errorf("%s: лист %s, строка %d, год %d: %w", rosstatPPIVedTable, sheet, i+1, year, err)
				} else if filled {
					return nil, fmt.Errorf("%s: лист %s, строка %d: подпись %q не распознана, а значение заполнено",
						rosstatPPIVedTable, sheet, i+1, label)
				}
			}
			continue
		}

		for col, year := range years {
			if year == 0 {
				continue
			}
			value, ok, err := ppiVedValue(ppiVedCell(row, col+1))
			if err != nil {
				return nil, fmt.Errorf("%s: лист %s, строка %d, год %d: %w", rosstatPPIVedTable, sheet, i+1, year, err)
			}
			if !ok {
				continue
			}
			key := ppiVedKey{date: ppiVedLastDay(year, month), indexType: ppiVedIndexMM}
			if err := ppiVedAdd(points, key, value, fmt.Sprintf("%s: лист %s, строка %d", rosstatPPIVedTable, sheet, i+1)); err != nil {
				return nil, err
			}
		}
	}
	if !ytdFound {
		return nil, fmt.Errorf("%s: лист %s: строка «Декабрь в %% к декабрю предыдущего года» не найдена", rosstatPPIVedTable, sheet)
	}
	return points, nil
}

// parsePPIVedYearSheet читает годовой лист 3.x–8.x. Тип индекса задаёт суффикс:
// .1 m/m, .2 ytd, .3 y/y, .4 period. На 3.x–6.x значения лежат в строке кода
// (только РФ), на 7.x–8.x — в следующей строке «Российская Федерация».
func parsePPIVedYearSheet(rows [][]string, sheet string) (map[ppiVedKey]float64, error) {
	var indexType string
	switch {
	case strings.HasSuffix(sheet, ".1"):
		indexType = ppiVedIndexMM
	case strings.HasSuffix(sheet, ".2"):
		indexType = ppiVedIndexYTD
	case strings.HasSuffix(sheet, ".3"):
		indexType = ppiVedIndexYY
	case strings.HasSuffix(sheet, ".4"):
		indexType = ppiVedIndexPeriod
	default:
		return nil, fmt.Errorf("%s: лист %s: неизвестный тип индекса", rosstatPPIVedTable, sheet)
	}

	year, err := ppiVedYearFromTitle(rows)
	if err != nil {
		return nil, fmt.Errorf("%s: лист %s: %w", rosstatPPIVedTable, sheet, err)
	}

	header := -1
	codeCol := -1
	for i, row := range rows {
		for col, cell := range row {
			if strings.EqualFold(strings.Join(strings.Fields(cell), ""), "ОКВЭД2") {
				header = i
				codeCol = col
				break
			}
		}
		if header >= 0 {
			break
		}
	}
	if header < 0 {
		return nil, fmt.Errorf("%s: лист %s: шапка «ОКВЭД2» не найдена", rosstatPPIVedTable, sheet)
	}

	monthCols := make(map[int]time.Month)
	for col, cell := range rows[header] {
		if col == codeCol {
			continue
		}
		if month, ok := ppiVedMonth(cell); ok {
			monthCols[col] = month
		}
	}
	if len(monthCols) == 0 {
		return nil, fmt.Errorf("%s: лист %s: месячные колонки не найдены", rosstatPPIVedTable, sheet)
	}

	codeRow := -1
	for i := header + 1; i < len(rows); i++ {
		if ppiVedCell(rows[i], codeCol) == ppiVedTotalCode {
			codeRow = i
			break
		}
	}
	if codeRow < 0 {
		return nil, fmt.Errorf("%s: лист %s: код %s не найден", rosstatPPIVedTable, sheet, ppiVedTotalCode)
	}

	valuesRow := codeRow
	if !ppiVedRowHasValue(rows[codeRow], monthCols) {
		found := false
		for i := codeRow + 1; i < len(rows); i++ {
			if ppiVedCell(rows[i], codeCol) != "" {
				break
			}
			if strings.EqualFold(ppiVedNormalizeLabel(ppiVedCell(rows[i], 0)), "российская федерация") {
				valuesRow = i
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%s: лист %s: у кода %s нет строки «Российская Федерация»", rosstatPPIVedTable, sheet, ppiVedTotalCode)
		}
	}

	points := make(map[ppiVedKey]float64)
	for col, month := range monthCols {
		value, ok, err := ppiVedValue(ppiVedCell(rows[valuesRow], col))
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s, строка %d, колонка %d: %w", rosstatPPIVedTable, sheet, valuesRow+1, col+1, err)
		}
		if !ok {
			continue
		}
		key := ppiVedKey{date: ppiVedLastDay(year, month), indexType: indexType}
		if err := ppiVedAdd(points, key, value, fmt.Sprintf("%s: лист %s, строка %d", rosstatPPIVedTable, sheet, valuesRow+1)); err != nil {
			return nil, err
		}
	}
	return points, nil
}

// parsePPIVed собирает четыре типа индексов из 1.1/2.1 и годовых листов. Перекрытия
// разрешаются приоритетом: годовые > 2.1 > 1.1. Приоритет нужен из-за смены
// ОКВЭД→ОКВЭД2 и перекрытия 2013–2016 и 2021–2025: без него один и тот же месяц
// попал бы в таблицу дважды (последняя запись выиграла бы по порядку листов).
func parsePPIVed(xlsx *excelize.File) ([]ppiVedPoint, error) {
	legacy1 := make(map[ppiVedKey]float64)
	legacy2 := make(map[ppiVedKey]float64)
	years := make(map[ppiVedKey]float64)
	foundLegacy1, foundLegacy2, foundYear := false, false, false

	for _, sheet := range xlsx.GetSheetList() {
		match := ppiVedSheetRe.FindStringSubmatch(sheet)
		if match == nil {
			continue
		}
		group, err := strconv.Atoi(match[1])
		if err != nil {
			continue
		}
		suffix := match[2]
		if group < 3 && suffix != "1" {
			continue
		}

		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", rosstatPPIVedTable, sheet, err)
		}

		switch group {
		case 1:
			points, err := parsePPIVedLegacySheet(rows, sheet)
			if err != nil {
				return nil, err
			}
			legacy1 = points
			foundLegacy1 = true
		case 2:
			points, err := parsePPIVedLegacySheet(rows, sheet)
			if err != nil {
				return nil, err
			}
			legacy2 = points
			foundLegacy2 = true
		default:
			points, err := parsePPIVedYearSheet(rows, sheet)
			if err != nil {
				return nil, err
			}
			for key, value := range points {
				if err := ppiVedAdd(years, key, value, fmt.Sprintf("%s: лист %s", rosstatPPIVedTable, sheet)); err != nil {
					return nil, err
				}
			}
			foundYear = true
		}
	}

	if !foundLegacy1 {
		return nil, fmt.Errorf("%s: лист 1.1 не найден", rosstatPPIVedTable)
	}
	if !foundLegacy2 {
		return nil, fmt.Errorf("%s: лист 2.1 не найден", rosstatPPIVedTable)
	}
	if !foundYear {
		return nil, fmt.Errorf("%s: годовые листы 3.x–8.x не найдены", rosstatPPIVedTable)
	}

	merged := make(map[ppiVedKey]float64, len(legacy1)+len(legacy2)+len(years))
	for key, value := range legacy1 {
		merged[key] = value
	}
	for key, value := range legacy2 {
		merged[key] = value
	}
	for key, value := range years {
		merged[key] = value
	}

	points := make([]ppiVedPoint, 0, len(merged))
	for key, value := range merged {
		points = append(points, ppiVedPoint{
			code:      ppiVedTotalCode,
			name:      ppiVedSeriesName(key.indexType),
			indexType: key.indexType,
			date:      key.date,
			value:     value,
		})
	}
	sort.Slice(points, func(i, j int) bool {
		if !points[i].date.Equal(points[j].date) {
			return points[i].date.Before(points[j].date)
		}
		if points[i].indexType != points[j].indexType {
			return ppiVedIndexOrder(points[i].indexType) < ppiVedIndexOrder(points[j].indexType)
		}
		return points[i].code < points[j].code
	})

	if err := ppiVedValidateCoverage(points); err != nil {
		return nil, err
	}
	return points, nil
}

// ppiVedValidateCoverage проверяет полноту рядов: каждый из четырёх типов обязан дать
// хотя бы одну точку, а m/m — быть непрерывным на всей глубине 1998-01…max. У ytd до
// 2021 г. источник даёт только декабрь, а y/y/period начинаются с 2021; их покрытие
// задано источником, поэтому помесячная непрерывность для них не требуется. Пропуск
// месяца в m/m означает дефект разбора: неполный ряд в витрине выглядит полным.
func ppiVedValidateCoverage(points []ppiVedPoint) error {
	mmDates := make(map[time.Time]bool)
	counts := make(map[string]int)
	var minMM, maxMM time.Time
	for _, point := range points {
		counts[point.indexType]++
		if point.indexType != ppiVedIndexMM {
			continue
		}
		mmDates[point.date] = true
		if minMM.IsZero() || point.date.Before(minMM) {
			minMM = point.date
		}
		if maxMM.IsZero() || point.date.After(maxMM) {
			maxMM = point.date
		}
	}
	for _, indexType := range []string{ppiVedIndexMM, ppiVedIndexYTD, ppiVedIndexYY, ppiVedIndexPeriod} {
		if counts[indexType] == 0 {
			return fmt.Errorf("%s: тип индекса %s не дал ни одной точки", rosstatPPIVedTable, indexType)
		}
	}
	if minMM.IsZero() {
		return fmt.Errorf("%s: разбор дал 0 точек", rosstatPPIVedTable)
	}

	// Непрерывность проверяется только для m/m — главного ряда и единственного, который
	// обязан быть полным на всей глубине 1998-01…max. У ytd до 2021 г. источник даёт
	// только декабрь, а y/y/period начинаются с 2021; их покрытие задано источником,
	// и требовать помесячной непрерывности на урезанной фикстуре нельзя.
	year, month := minMM.Year(), minMM.Month()
	for {
		date := ppiVedLastDay(year, month)
		if date.After(maxMM) {
			return nil
		}
		if !mmDates[date] {
			return fmt.Errorf("%s: %s: пропущен месяц %s", rosstatPPIVedTable, ppiVedIndexMM, date.Format("2006-01"))
		}
		month++
		if month > time.December {
			month = time.January
			year++
		}
	}
}
