package rosstat

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	// ppiProductCodeHeader — колонка кода в месячных листах товаров. Годовые
	// листы 1–2 её не содержат, у листа 3 она есть, но месячных колонок нет:
	// такие листы распознаются как annual и пропускаются (решение T2).
	ppiProductCodeHeader = "Код товара"
	ppiProductMinCodes   = 1000
	ppiProductMinMonths  = 8
	ppiProductStartYear  = 2021
)

// ppiProductKey — ключ слияния точек. В таблице тот же ключ плюс name/value;
// name не входит в ключ: у 44 кодов название меняется между годами.
type ppiProductKey struct {
	date      time.Time
	okpd2     string
	indexType string
}

type ppiProductPoint struct {
	okpd2     string
	name      string
	indexType string
	date      time.Time
	value     float64
}

// ppiProductHeader ищет шапку месячного листа по колонке «Код товара».
// Возвращает annual=true для годовых листов: у них есть «Код товара» (лист 3),
// но нет месячных колонок. Листы без шапки (1–2) дают found=false.
func ppiProductHeader(rows [][]string) (header, codeCol int, monthCols map[int]time.Month, annual, found bool, err error) {
	headerLabel := strings.ToLower(strings.Join(strings.Fields(ppiProductCodeHeader), ""))
	for i, row := range rows {
		col := -1
		for j, cell := range row {
			label := strings.ToLower(strings.Join(strings.Fields(cell), ""))
			if strings.Contains(label, headerLabel) {
				col = j
				break
			}
		}
		if col < 0 {
			continue
		}
		header, codeCol = i, col
		if codeCol < 1 {
			return 0, 0, nil, false, true, fmt.Errorf("колонка «%s» на позиции 1: нет колонки названия", ppiProductCodeHeader)
		}

		monthCols = make(map[int]time.Month)
		seenMonths := make(map[time.Month]int, 12)
		for k := codeCol + 1; k < len(row); k++ {
			month, ok := ppiVedMonth(row[k])
			if !ok {
				continue
			}
			if prev, dup := seenMonths[month]; dup {
				return 0, 0, nil, false, true, fmt.Errorf("месяц %s встречается в колонках %d и %d", month, prev+1, k+1)
			}
			seenMonths[month] = k
			monthCols[k] = month
		}
		if len(monthCols) >= ppiProductMinMonths {
			return header, codeCol, monthCols, false, true, nil
		}

		years := 0
		for _, cell := range row {
			if _, ok := ppiVedYear(cell); ok {
				years++
			}
		}
		if years >= 5 {
			return header, codeCol, nil, true, true, nil
		}
		return 0, 0, nil, false, true, fmt.Errorf("лист с «%s» не содержит ни месячных колонок, ни годовых", ppiProductCodeHeader)
	}
	return 0, 0, nil, false, false, nil
}

// ppiProductIndexType читает тип индекса из подписи под заголовком листа.
// Порядок веток важен: period содержит «к соответствующему периоду», а не
// «месяцу», поэтому не пересекается с y/y; m/m и ytd идут отдельными ветками.
func ppiProductIndexType(rows [][]string) (string, error) {
	limit := 4
	if len(rows) < limit {
		limit = len(rows)
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parts = append(parts, strings.Join(rows[i], " "))
	}
	text := strings.ToLower(strings.Join(strings.Fields(strings.Join(parts, " ")), " "))
	switch {
	case strings.Contains(text, "за период с начала года") && strings.Contains(text, "к соответствующему периоду"):
		return ppiVedIndexPeriod, nil
	case strings.Contains(text, "к предыдущему месяцу"):
		return ppiVedIndexMM, nil
	case strings.Contains(text, "к декабрю предыдущего года"):
		return ppiVedIndexYTD, nil
	case strings.Contains(text, "к соответствующему месяцу предыдущего года"):
		return ppiVedIndexYY, nil
	default:
		return "", fmt.Errorf("тип индекса не распознан в заголовке листа: %q", text)
	}
}

// addPPIProductRow переносит значения одной строки в карту точек. Пустая
// ячейка и маркеры пропуска — не точка. Дубль ключа с другим значением —
// ошибка: это означает, что разметка листа изменилась.
func addPPIProductRow(points map[ppiProductKey]float64, names map[ppiProductKey]string, row []string, rowNumber int, sheet string, year int, code, name, indexType string, monthCols map[int]time.Month) error {
	for col, month := range monthCols {
		value, ok, err := ppiVedValue(ppiVedCell(row, col))
		if err != nil {
			return fmt.Errorf("лист %s, строка %d, колонка %d: %w", sheet, rowNumber, col+1, err)
		}
		if !ok {
			continue
		}
		key := ppiProductKey{date: ppiVedLastDay(year, month), okpd2: code, indexType: indexType}
		if previous, exists := points[key]; exists {
			if previous != value {
				return fmt.Errorf("лист %s, строка %d: ключ (%s, %s, %s) встречается дважды со значениями %v и %v",
					sheet, rowNumber, key.date.Format("2006-01-02"), code, indexType, previous, value)
			}
			continue
		}
		points[key] = value
		names[key] = name
	}
	return nil
}

// parsePPIProductSheet разбирает один месячный лист. rows — сырые значения
// (RawCellValue), formattedRows — те же строки в отображаемом виде: колонка
// кода читается из formattedRows, потому что короткие коды (10.12, 15.2)
// хранятся числовыми ячейками, и сырое значение даёт 10.119999999999999.
// Формат 2021–2023: значения в строке кода. Формат 2024–2026: строка кода
// пустая, значения — в следующей строке «Российская Федерация»; код-строка
// всегда открывает блок, поэтому оба формата читаются одним проходом.
func parsePPIProductSheet(rows, formattedRows [][]string, sheet string, header, codeCol int, monthCols map[int]time.Month) ([]ppiProductPoint, error) {
	year, err := ppiVedYearFromTitle(rows)
	if err != nil {
		return nil, fmt.Errorf("лист %s: %w", sheet, err)
	}
	indexType, err := ppiProductIndexType(rows)
	if err != nil {
		return nil, fmt.Errorf("лист %s: %w", sheet, err)
	}

	points := make(map[ppiProductKey]float64)
	names := make(map[ppiProductKey]string)
	codes := make(map[string]bool)
	russiaRows := 0
	currentCode, currentName := "", ""

	for i := header + 1; i < len(rows); i++ {
		row := rows[i]
		name := strings.TrimSpace(ppiVedCell(row, 0))
		code := ""
		if i < len(formattedRows) {
			code = strings.TrimSpace(ppiVedCell(formattedRows[i], codeCol))
		}

		if name != "" && code != "" &&
			!strings.Contains(strings.ToLower(strings.Join(strings.Fields(code), "")), strings.ToLower(strings.Join(strings.Fields(ppiProductCodeHeader), ""))) {
			currentCode, currentName = code, name
			codes[code] = true
			if err := addPPIProductRow(points, names, row, i+1, sheet, year, currentCode, currentName, indexType, monthCols); err != nil {
				return nil, err
			}
			continue
		}

		if strings.EqualFold(ppiVedNormalizeLabel(name), "российская федерация") {
			if currentCode == "" {
				return nil, fmt.Errorf("лист %s, строка %d: строка «Российская Федерация» без кода товара", sheet, i+1)
			}
			russiaRows++
			if err := addPPIProductRow(points, names, row, i+1, sheet, year, currentCode, currentName, indexType, monthCols); err != nil {
				return nil, err
			}
			currentCode, currentName = "", ""
			continue
		}
		// Регионы, разделы и служебные строки без кода — не точки.
	}

	if len(codes) < ppiProductMinCodes {
		return nil, fmt.Errorf("лист %s: кодов %d, ожидалось не меньше %d", sheet, len(codes), ppiProductMinCodes)
	}
	if year >= 2024 && russiaRows == 0 {
		return nil, fmt.Errorf("лист %s: для %d г. не найдено ни одной строки «Российская Федерация»", sheet, year)
	}

	result := make([]ppiProductPoint, 0, len(points))
	for key, value := range points {
		result = append(result, ppiProductPoint{
			okpd2:     key.okpd2,
			name:      names[key],
			indexType: key.indexType,
			date:      key.date,
			value:     value,
		})
	}
	return result, nil
}

// parsePPIProduct собирает все месячные листы, проверяет конфликты между
// листами, непрерывность месяцев и полноту кодов. Годовые листы 1–3
// пропускаются: в T2 импортируются только месячные 2021–2026.
func parsePPIProduct(xlsx *excelize.File) ([]ppiProductPoint, error) {
	return parsePPIProductWithCoverage(xlsx, true)
}

// parsePPIProductWithCoverage — общий разбор. requireContinuous=false нужен
// тестам на урезанной фикстуре, где намеренно нет листов 2022–2025: проверка
// непрерывности покрытия остаётся обязательной для боевого пути.
func parsePPIProductWithCoverage(xlsx *excelize.File, requireContinuous bool) ([]ppiProductPoint, error) {
	merged := make(map[ppiProductKey]ppiProductPoint)
	found := false

	for _, sheet := range xlsx.GetSheetList() {
		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", rosstatPPIProductTable, sheet, err)
		}
		header, codeCol, monthCols, annual, hasHeader, err := ppiProductHeader(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", rosstatPPIProductTable, sheet, err)
		}
		if !hasHeader || annual {
			continue
		}
		formattedRows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: false})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", rosstatPPIProductTable, sheet, err)
		}
		found = true
		points, err := parsePPIProductSheet(rows, formattedRows, sheet, header, codeCol, monthCols)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", rosstatPPIProductTable, err)
		}
		for _, point := range points {
			key := ppiProductKey{date: point.date, okpd2: point.okpd2, indexType: point.indexType}
			if previous, ok := merged[key]; ok {
				if previous.value != point.value {
					return nil, fmt.Errorf("%s: ключ (%s, %s, %s) встречается в разных листах со значениями %v и %v",
						rosstatPPIProductTable, key.date.Format("2006-01-02"), point.okpd2, point.indexType, previous.value, point.value)
				}
				continue
			}
			merged[key] = point
		}
	}
	if !found {
		return nil, fmt.Errorf("%s: месячные листы с «%s» не найдены", rosstatPPIProductTable, ppiProductCodeHeader)
	}

	result := make([]ppiProductPoint, 0, len(merged))
	for _, point := range merged {
		result = append(result, point)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].date.Equal(result[j].date) {
			return result[i].date.Before(result[j].date)
		}
		if result[i].indexType != result[j].indexType {
			return ppiVedIndexOrder(result[i].indexType) < ppiVedIndexOrder(result[j].indexType)
		}
		return result[i].okpd2 < result[j].okpd2
	})

	if requireContinuous {
		if err := ppiProductValidateCoverage(result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ppiProductValidateCoverage проверяет глобальную полноту: все четыре типа
// индексов дали точки, каждый тип покрывает месяцы 2021-01…max без пропусков,
// кодов не меньше порога. Непрерывность по каждому коду не требуется: разные
// товары появляются и исчезают в разные годы — это свойство источника.
func ppiProductValidateCoverage(points []ppiProductPoint) error {
	if len(points) == 0 {
		return fmt.Errorf("%s: разбор дал 0 точек", rosstatPPIProductTable)
	}
	counts := make(map[string]int, 4)
	codes := make(map[string]bool, 1400)
	months := make(map[string]map[time.Time]bool, 4)
	var maxDate time.Time
	for _, point := range points {
		counts[point.indexType]++
		codes[point.okpd2] = true
		if months[point.indexType] == nil {
			months[point.indexType] = make(map[time.Time]bool)
		}
		months[point.indexType][point.date] = true
		if point.date.After(maxDate) {
			maxDate = point.date
		}
	}
	for _, indexType := range []string{ppiVedIndexMM, ppiVedIndexYTD, ppiVedIndexYY, ppiVedIndexPeriod} {
		if counts[indexType] == 0 {
			return fmt.Errorf("%s: тип индекса %s не дал ни одной точки", rosstatPPIProductTable, indexType)
		}
	}
	if len(codes) < ppiProductMinCodes {
		return fmt.Errorf("%s: кодов %d, ожидалось не меньше %d", rosstatPPIProductTable, len(codes), ppiProductMinCodes)
	}
	start := time.Date(ppiProductStartYear, time.January, 31, 0, 0, 0, 0, time.UTC)
	if maxDate.Before(start) {
		return fmt.Errorf("%s: max(date) %s раньше %s", rosstatPPIProductTable, maxDate.Format("2006-01-02"), start.Format("2006-01"))
	}
	for _, indexType := range []string{ppiVedIndexMM, ppiVedIndexYTD, ppiVedIndexYY, ppiVedIndexPeriod} {
		year, month := ppiProductStartYear, time.January
		for {
			date := ppiVedLastDay(year, month)
			if date.After(maxDate) {
				break
			}
			if !months[indexType][date] {
				return fmt.Errorf("%s: %s: пропущен месяц %s", rosstatPPIProductTable, indexType, date.Format("2006-01"))
			}
			month++
			if month > time.December {
				month = time.January
				year++
			}
		}
	}
	return nil
}

func ppiProductSeriesKey(okpd2, indexType string) string {
	return "okpd2:" + okpd2 + ":" + indexType
}

func ppiProductIndexTitle(indexType string) string {
	switch indexType {
	case ppiVedIndexMM:
		return "м/м"
	case ppiVedIndexYTD:
		return "с начала года"
	case ppiVedIndexYY:
		return "г/г"
	case ppiVedIndexPeriod:
		return "период к прошлому году"
	default:
		return indexType
	}
}

func ppiProductIndexUnit(indexType string) string {
	switch indexType {
	case ppiVedIndexMM:
		return "индекс, % к предыдущему месяцу (100 = без изменения)"
	case ppiVedIndexYTD:
		return "индекс, % к декабрю предыдущего года (100 = без изменения)"
	case ppiVedIndexYY:
		return "индекс, % к соответствующему месяцу предыдущего года (100 = без изменения)"
	case ppiVedIndexPeriod:
		return "индекс, % к соответствующему периоду предыдущего года (100 = без изменения)"
	default:
		return "индекс, % (100 = без изменения)"
	}
}

// ppiProductSeriesMeta строит каталог товарных рядов. Одна запись на пару
// (okpd2, index_type): у 1 273 кодов нельзя использовать человекочитаемое
// name как ключ, а название из источника меняется между годами.
func ppiProductSeriesMeta(rows []ppiProductCatalogRow) []util.SeriesMeta {
	metas := make([]util.SeriesMeta, 0, len(rows))
	for _, row := range rows {
		metas = append(metas, util.SeriesMeta{
			Source:    rosstatSource,
			Series:    ppiProductSeriesKey(row.okpd2, row.indexType),
			Title:     fmt.Sprintf("%s — %s", row.name, ppiProductIndexTitle(row.indexType)),
			Unit:      ppiProductIndexUnit(row.indexType),
			Frequency: "M",
			Origin:    fmt.Sprintf("rosstat.gov.ru/statistics/price, файл Proizvoditeli_Ind_tov_*.xlsx, листы 4–27, код ОКПД2 %s", row.okpd2),
			Description: "Индекс цен производителей по товару/товарной группе ОКПД2: 100 — цены не изменились. " +
				"Дата точки — последний день отчётного месяца; месячный ряд с 2021-01, ревизии перезаписываются по ключу (date, okpd2, index_type). " +
				"Пустая ячейка и «…»/«...» — пропуск источника, а не ноль. Название — из последнего по дате выпуска; у части кодов название менялось между годами.",
		})
	}
	return metas
}
