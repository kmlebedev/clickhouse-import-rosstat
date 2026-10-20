package rosstat

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	ppiWeightDomestic       = "domestic"
	ppiWeightDomesticExport = "domestic_export"

	ppiWeightStartYear = 2017
	ppiWeightEndYear   = 2026
	ppiWeightCodes     = 38
	// ppiWeightTotalCode — строка «Производство промышленных товаров»:
	// единственная, чья сумма обязана быть 100 в каждом году и каждой базе.
	ppiWeightTotalCode = "B+C+D+E"
)

type ppiWeightKey struct {
	year       uint16
	code       string
	weightType string
}

type ppiWeightPoint struct {
	year       uint16
	code       string
	name       string
	weightType string
	value      float64
}

// ppiWeightSheetKind определяет базу весов по заголовку листа. Листы 1–2
// (ОКВЭД-2007) тоже попадают в domestic/domestic_export, но отсекаются по
// годам шапки: в T2 читаются только 2017+.
func ppiWeightSheetKind(rows [][]string) (string, bool) {
	limit := 2
	if len(rows) < limit {
		limit = len(rows)
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parts = append(parts, strings.Join(rows[i], " "))
	}
	text := strings.ToLower(strings.Join(strings.Fields(strings.Join(parts, " ")), " "))
	if strings.Contains(text, "и на экспорт") {
		return ppiWeightDomesticExport, true
	}
	if strings.Contains(text, "внутреннем рынке") {
		return ppiWeightDomestic, true
	}
	return "", false
}

// ppiWeightHeader ищет строку с кодом и годами. Годы берутся из подписей
// колонок 2+, сноски вида «20221)» снимаются ppiVedYear.
func ppiWeightHeader(rows [][]string) (header int, yearCols map[int]int, found bool, err error) {
	for i, row := range rows {
		years := make(map[int]int)
		for col := 2; col < len(row); col++ {
			year, ok := ppiVedYear(ppiVedCell(row, col))
			if !ok {
				continue
			}
			years[col] = year
		}
		if len(years) < 5 {
			continue
		}
		seen := make(map[int]int, len(years))
		for col, year := range years {
			if prev, dup := seen[year]; dup {
				return 0, nil, true, fmt.Errorf("год %d встречается в колонках %d и %d", year, prev+1, col+1)
			}
			seen[year] = col
		}
		return i, years, true, nil
	}
	return 0, nil, false, nil
}

func addPPIWeight(points map[ppiWeightKey]float64, names map[ppiWeightKey]string, key ppiWeightKey, name string, value float64, sheet string, rowNumber int) error {
	if previous, exists := points[key]; exists {
		if previous != value {
			return fmt.Errorf("лист %s, строка %d: ключ (%d, %s, %s) встречается дважды со значениями %v и %v",
				sheet, rowNumber, key.year, key.code, key.weightType, previous, value)
		}
		return nil
	}
	points[key] = value
	names[key] = name
	return nil
}

func parsePPIWeightsSheet(rows [][]string, sheet, weightType string, header int, yearCols map[int]int) ([]ppiWeightPoint, error) {
	points := make(map[ppiWeightKey]float64)
	names := make(map[ppiWeightKey]string)
	codes := make(map[string]bool)

	for i := header + 1; i < len(rows); i++ {
		row := rows[i]
		name := strings.TrimSpace(ppiVedCell(row, 0))
		code := strings.TrimSpace(ppiVedCell(row, 1))
		if code == "" || strings.EqualFold(code, "Код") || strings.HasPrefix(name, "1)") {
			continue
		}
		codes[code] = true
		for col, year := range yearCols {
			if year < ppiWeightStartYear {
				continue
			}
			value, ok, err := ppiVedValue(ppiVedCell(row, col))
			if err != nil {
				return nil, fmt.Errorf("лист %s, строка %d, колонка %d: %w", sheet, i+1, col+1, err)
			}
			if !ok {
				continue
			}
			key := ppiWeightKey{year: uint16(year), code: code, weightType: weightType}
			if err := addPPIWeight(points, names, key, name, value, sheet, i+1); err != nil {
				return nil, err
			}
		}
	}
	if len(codes) < ppiWeightCodes {
		return nil, fmt.Errorf("лист %s: кодов %d, ожидалось %d", sheet, len(codes), ppiWeightCodes)
	}

	result := make([]ppiWeightPoint, 0, len(points))
	for key, value := range points {
		result = append(result, ppiWeightPoint{
			year:       key.year,
			code:       key.code,
			name:       names[key],
			weightType: key.weightType,
			value:      value,
		})
	}
	return result, nil
}

// parsePPIWeights читает листы 3–4 (2017–2026): domestic и domestic+export.
// Листы 1–2 — ОКВЭД-2007, 2006–2016 — пропускаются по годам шапки.
func parsePPIWeights(xlsx *excelize.File) ([]ppiWeightPoint, error) {
	merged := make(map[ppiWeightKey]ppiWeightPoint)
	foundTypes := make(map[string]bool, 2)

	for _, sheet := range xlsx.GetSheetList() {
		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", rosstatPPIWeightsTable, sheet, err)
		}
		weightType, ok := ppiWeightSheetKind(rows)
		if !ok {
			continue
		}
		header, yearCols, found, err := ppiWeightHeader(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: лист %s: %w", rosstatPPIWeightsTable, sheet, err)
		}
		if !found {
			continue
		}
		hasCurrent := false
		for _, year := range yearCols {
			if year >= ppiWeightStartYear {
				hasCurrent = true
				break
			}
		}
		if !hasCurrent {
			continue
		}
		foundTypes[weightType] = true
		points, err := parsePPIWeightsSheet(rows, sheet, weightType, header, yearCols)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", rosstatPPIWeightsTable, err)
		}
		for _, point := range points {
			key := ppiWeightKey{year: point.year, code: point.code, weightType: point.weightType}
			if previous, exists := merged[key]; exists {
				if previous.value != point.value {
					return nil, fmt.Errorf("%s: ключ (%d, %s, %s) встречается в разных листах со значениями %v и %v",
						rosstatPPIWeightsTable, key.year, key.code, key.weightType, previous.value, point.value)
				}
				continue
			}
			merged[key] = point
		}
	}
	for _, weightType := range []string{ppiWeightDomestic, ppiWeightDomesticExport} {
		if !foundTypes[weightType] {
			return nil, fmt.Errorf("%s: лист с базой %s не найден", rosstatPPIWeightsTable, weightType)
		}
	}

	result := make([]ppiWeightPoint, 0, len(merged))
	for _, point := range merged {
		result = append(result, point)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].year != result[j].year {
			return result[i].year < result[j].year
		}
		if result[i].code != result[j].code {
			return result[i].code < result[j].code
		}
		return result[i].weightType < result[j].weightType
	})

	if err := ppiWeightsValidateCoverage(result); err != nil {
		return nil, err
	}
	return result, nil
}

// ppiWeightsValidateCoverage проверяет полноту весов: ровно две базы, по
// 38 кодов, годы 2017–2026 без пропусков, строка B+C+D+E = 100 ± 0.05.
// Полнота всех пар (код, год) не требуется: в источнике у кода 38 за 2017 год
// стоит «-» в обеих базах (пропуск, не ноль), поэтому 758 точек, а не 760.
// Это единственный пропуск винтажа 08-2026; он закреплён тестом
// TestPPIWeightsKnownGap, чтобы смена публикации была видна.
func ppiWeightsValidateCoverage(points []ppiWeightPoint) error {
	if len(points) == 0 {
		return fmt.Errorf("%s: разбор дал 0 точек", rosstatPPIWeightsTable)
	}
	codes := make(map[string]map[string]bool, 2)
	years := make(map[string]map[uint16]bool, 2)
	totals := make(map[ppiWeightKey]float64)
	for _, point := range points {
		if codes[point.weightType] == nil {
			codes[point.weightType] = make(map[string]bool)
			years[point.weightType] = make(map[uint16]bool)
		}
		codes[point.weightType][point.code] = true
		years[point.weightType][point.year] = true
		if point.code == ppiWeightTotalCode {
			totals[ppiWeightKey{year: point.year, code: point.code, weightType: point.weightType}] = point.value
		}
	}
	for _, weightType := range []string{ppiWeightDomestic, ppiWeightDomesticExport} {
		if len(codes[weightType]) != ppiWeightCodes {
			return fmt.Errorf("%s: база %s: кодов %d, ожидалось %d", rosstatPPIWeightsTable, weightType, len(codes[weightType]), ppiWeightCodes)
		}
		for year := ppiWeightStartYear; year <= ppiWeightEndYear; year++ {
			if !years[weightType][uint16(year)] {
				return fmt.Errorf("%s: база %s: нет точек за %d", rosstatPPIWeightsTable, weightType, year)
			}
			key := ppiWeightKey{year: uint16(year), code: ppiWeightTotalCode, weightType: weightType}
			total, ok := totals[key]
			if !ok {
				return fmt.Errorf("%s: база %s: нет строки %s за %d", rosstatPPIWeightsTable, weightType, ppiWeightTotalCode, year)
			}
			if math.Abs(total-100) > 0.05 {
				return fmt.Errorf("%s: база %s: %s за %d = %v, ожидалось 100", rosstatPPIWeightsTable, weightType, ppiWeightTotalCode, year, total)
			}
		}
	}
	return nil
}

func ppiWeightSeriesKey(code, weightType string) string {
	return "ved-weight:" + code + ":" + weightType
}

func ppiWeightTypeTitle(weightType string) string {
	switch weightType {
	case ppiWeightDomestic:
		return "внутренний рынок"
	case ppiWeightDomesticExport:
		return "внутренний рынок и экспорт"
	default:
		return weightType
	}
}

// ppiWeightsSeriesMeta строит каталог весов: одна запись на пару
// (код ВЭД, база). Веса — годовые, не месячные ряды.
func ppiWeightsSeriesMeta(rows []ppiWeightCatalogRow) []util.SeriesMeta {
	metas := make([]util.SeriesMeta, 0, len(rows))
	for _, row := range rows {
		metas = append(metas, util.SeriesMeta{
			Source:    rosstatSource,
			Series:    ppiWeightSeriesKey(row.code, row.weightType),
			Title:     fmt.Sprintf("%s — вес, %s", row.name, ppiWeightTypeTitle(row.weightType)),
			Unit:      "% (доля в структуре весов ИЦП)",
			Frequency: "A",
			Origin:    fmt.Sprintf("rosstat.gov.ru/statistics/price, файл vesa_*.xlsx, листы 3–4, код ВЭД %s", row.code),
			Description: "Годовой вес вида деятельности в структуре весов ИЦП Росстата, 2017–2026. " +
				"domestic — товары для реализации на внутреннем рынке; domestic_export — внутренний рынок и экспорт. " +
				"Сумма по строке B+C+D+E = 100 в каждом году и каждой базе. Дата — год, а не месяц; ревизия перезаписывается по ключу (year, code, weight_type).",
		})
	}
	return metas
}
