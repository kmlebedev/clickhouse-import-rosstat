package wgc

import (
	"fmt"
	"math"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// Раскладка выгрузки ETF (ETF_Flows_*.xlsx) и имена рядов ETF-блока.
const (
	etfFlowsSheet    = "Fund flows by month"
	etfDemandSheet   = "Demand by month"
	etfHoldingsSheet = "Holdings by month"
	etfChartsSheet   = "Charts Data"

	etfMetricFlowUsdMln     = "flow_usd_mln"
	etfMetricFlowTonnes     = "flow_tonnes"
	etfMetricHoldingsTonnes = "holdings_tonnes"
	etfMetricAumUsdBn       = "aum_usd_bn"

	etfWorldRegion        = "world"
	etfRegionNorthAmerica = "north_america"
	etfRegionEurope       = "europe"
	etfRegionAsia         = "asia"
	etfRegionOther        = "other"

	// etfFundsFirstCol — колонка F: в A дата, в B..E справочные итоги листа (цена, унции,
	// тонны, стоимость), фонды идут с F.
	etfFundsFirstCol = 6
	// etfRegionRow — строка 4 листа фондов: регион фонда (строка 6 — имя фонда).
	etfRegionRow = 4
	// etfFirstDataRow — данные фондов начинаются со строки 7 (1..6 — шапка).
	etfFirstDataRow = 7
	// etfTotalCol — колонка D: готовый итог листа в тоннах, используется только для
	// рантайм-сверки со суммой фондов.
	etfTotalCol = 4
	// etfChartsFirstDataRow — данные блока «Charts Data» идут со строки 3: строка 1 — заголовок
	// блока, строка 2 — заголовки колонок.
	etfChartsFirstDataRow   = 3
	etfChartsBlockHeaderRow = 1
	etfChartsColumnsRow     = 2
	// etfDateHeader — заголовок колонки дат внутри блока «Charts Data».
	etfDateHeader = "Date"
	// etfAumChartsHeader — блок листа «Charts Data» с историей AUM в долларах.
	etfAumChartsHeader = "Area chart - monthly holdings in US$"

	// etfTotalTolerance — порог рантайм-сверки итога колонки D с суммой фондов.
	etfTotalTolerance = 1e-6
)

var (
	wgcEtfRegions = []string{etfWorldRegion, etfRegionNorthAmerica, etfRegionEurope, etfRegionAsia, etfRegionOther}
	wgcEtfMetrics = []string{etfMetricFlowUsdMln, etfMetricFlowTonnes, etfMetricHoldingsTonnes, etfMetricAumUsdBn}

	// etfRegionsByName — регион фонда в выгрузке → имя региона в ряде.
	etfRegionsByName = map[string]string{
		"North America": etfRegionNorthAmerica,
		"Europe":        etfRegionEurope,
		"Asia":          etfRegionAsia,
		"Other":         etfRegionOther,
	}

	// chartMetricScale — делитель значений блока «Charts Data»: блоки со знаком US$ хранят
	// абсолютные доллары, а ряд AUM — в млрд. Метрики нет в словаре — значения как в файле.
	chartMetricScale = map[string]float64{etfMetricAumUsdBn: 1e9}
)

// wgcEtfSeriesName — имя ряда ETF по метрике и региону.
func wgcEtfSeriesName(metric, region string) string {
	return "etf_" + metric + "_" + region
}

// regionColumn — колонка листа с распознанным регионом.
type regionColumn struct {
	col    int
	region string
}

// parseEtfWorkbook разбирает выгрузку ETF целиком: три листа фондов и блок AUM листа
// «Charts Data» (остальные блоки Charts — справочные и в импорт не входят).
func parseEtfWorkbook(f *excelize.File) ([]seriesPoint, error) {
	sheets := []struct {
		sheet  string
		metric string
	}{
		{etfFlowsSheet, etfMetricFlowUsdMln},
		{etfDemandSheet, etfMetricFlowTonnes},
		{etfHoldingsSheet, etfMetricHoldingsTonnes},
	}
	var points []seriesPoint
	for _, s := range sheets {
		sheetPoints, err := parseFundSheet(f, s.sheet, s.metric)
		if err != nil {
			return nil, err
		}
		points = append(points, sheetPoints...)
	}
	aum, err := parseChartsBlock(f, etfAumChartsHeader, etfMetricAumUsdBn)
	if err != nil {
		return nil, err
	}
	return append(points, aum...), nil
}

// parseFundSheet собирает месячные суммы фондов по регионам одного листа фондов (метрика
// metric); world — сумма четырёх регионов за месяцы, где есть значения у всех четырёх.
func parseFundSheet(f *excelize.File, sheet, metric string) ([]seriesPoint, error) {
	maxCol, maxRow, err := sheetSize(f, sheet)
	if err != nil {
		return nil, fmt.Errorf("лист %s: размеры: %w", sheet, err)
	}
	if maxCol < etfFundsFirstCol || maxRow < etfFirstDataRow {
		return nil, fmt.Errorf("лист %s: нет колонок фондов (с %d) или строк данных (с %d)", sheet, etfFundsFirstCol, etfFirstDataRow)
	}
	cols, err := fundRegionColumns(f, sheet, maxCol, maxRow)
	if err != nil {
		return nil, err
	}

	var points []seriesPoint
	for row := etfFirstDataRow; row <= maxRow; row++ {
		date, ok, err := rowMonth(f, sheet, 1, row)
		if err != nil {
			return nil, err
		}
		if !ok {
			// Хвост листа: пустые строки и подписи вроде «Column : fund_flow»; строка без даты
			// допустима только пока значения фондов в ней не появились.
			if err := checkSkippedRowStaysEmpty(f, sheet, 1, row, cols); err != nil {
				return nil, err
			}
			continue
		}
		sums := make(map[string]float64, len(wgcEtfRegions))
		total := 0.0
		for _, c := range cols { // колонки по возрастанию → детерминированный итог
			raw, err := cellRaw(f, sheet, c.col, row)
			if err != nil {
				return nil, fmt.Errorf("лист %s: колонка %d строка %d: %w", sheet, c.col, row, err)
			}
			value, ok, err := cellFloat(raw)
			if err != nil {
				return nil, fmt.Errorf("лист %s: колонка %d строка %d: %w", sheet, c.col, row, err)
			}
			if !ok {
				continue // пустая ячейка фонда — вклада нет, но и нулём она не считается
			}
			sums[c.region] += value
			total += value
		}
		points = appendRegionPoints(points, metric, date, sums)
		if sheet == etfHoldingsSheet {
			if err := checkHoldingsTotal(f, row, date, total); err != nil {
				return nil, err
			}
		}
	}
	return points, nil
}

// fundRegionColumns размечает колонки фондов по строке 4: F.. до последней колонки листа.
// Непустой регион вне словаря — ошибка, а колонка без региона допустима только пока в ней нет
// данных: молча выбросить фонд нельзя (fail-fast).
func fundRegionColumns(f *excelize.File, sheet string, maxCol, maxRow int) ([]regionColumn, error) {
	cols := make([]regionColumn, 0, maxCol-etfFundsFirstCol+1)
	for col := etfFundsFirstCol; col <= maxCol; col++ {
		raw, err := cellRaw(f, sheet, col, etfRegionRow)
		if err != nil {
			return nil, fmt.Errorf("лист %s: регион колонки %d: %w", sheet, col, err)
		}
		name := strings.TrimSpace(raw)
		if name == "" {
			if err := checkColumnStaysEmpty(f, sheet, col, maxRow); err != nil {
				return nil, err
			}
			continue
		}
		region, ok := etfRegionsByName[name]
		if !ok {
			return nil, fmt.Errorf("лист %s: колонка %d: регион фонда %q вне словаря регионов", sheet, col, name)
		}
		cols = append(cols, regionColumn{col: col, region: region})
	}
	return cols, nil
}

// parseChartsBlock читает месячный блок листа «Charts Data», найденный по заголовку в строке 1:
// колонка дат (Date), четыре колонки регионов и следующая колонка блока (Gold Price / Total),
// которая в импорт не входит. metric задаёт имена рядов, chartMetricScale — делитель значений.
func parseChartsBlock(f *excelize.File, header, metric string) ([]seriesPoint, error) {
	maxCol, maxRow, err := sheetSize(f, etfChartsSheet)
	if err != nil {
		return nil, fmt.Errorf("лист %s: размеры: %w", etfChartsSheet, err)
	}
	dateCol, err := chartsBlockColumn(f, header, maxCol)
	if err != nil {
		return nil, err
	}
	cols, err := chartsRegionColumns(f, dateCol)
	if err != nil {
		return nil, err
	}
	scale := chartMetricScale[metric]
	if scale == 0 {
		scale = 1
	}

	var points []seriesPoint
	for row := etfChartsFirstDataRow; row <= maxRow; row++ {
		date, ok, err := rowMonth(f, etfChartsSheet, dateCol, row)
		if err != nil {
			return nil, err
		}
		if !ok {
			// Блоки «Charts Data» стоят рядом и разной длины: строка без даты допустима только
			// пока в колонках регионов этого блока нет значений.
			if err := checkSkippedRowStaysEmpty(f, etfChartsSheet, dateCol, row, cols); err != nil {
				return nil, err
			}
			continue
		}
		sums := make(map[string]float64, len(wgcEtfRegions))
		for _, c := range cols {
			raw, err := cellRaw(f, etfChartsSheet, c.col, row)
			if err != nil {
				return nil, fmt.Errorf("лист %s: колонка %d строка %d: %w", etfChartsSheet, c.col, row, err)
			}
			value, ok, err := cellFloat(raw)
			if err != nil {
				return nil, fmt.Errorf("лист %s: колонка %d строка %d: %w", etfChartsSheet, c.col, row, err)
			}
			if !ok {
				continue
			}
			sums[c.region] = value / scale
		}
		points = appendRegionPoints(points, metric, date, sums)
	}
	return points, nil
}

// chartsBlockColumn находит колонку блока «Charts Data» по заголовку в строке 1 и проверяет, что
// под ним стоит колонка дат: при обновлении выгрузки блоки дрейфуют, а читать не тот блок молча
// нельзя.
func chartsBlockColumn(f *excelize.File, header string, maxCol int) (int, error) {
	for col := 1; col <= maxCol; col++ {
		raw, err := cellRaw(f, etfChartsSheet, col, etfChartsBlockHeaderRow)
		if err != nil {
			return 0, fmt.Errorf("лист %s: заголовок блока в колонке %d: %w", etfChartsSheet, col, err)
		}
		if strings.TrimSpace(raw) != header {
			continue
		}
		dateHeader, err := cellRaw(f, etfChartsSheet, col, etfChartsColumnsRow)
		if err != nil {
			return 0, fmt.Errorf("лист %s: заголовок колонки дат %d: %w", etfChartsSheet, col, err)
		}
		if strings.TrimSpace(dateHeader) != etfDateHeader {
			return 0, fmt.Errorf("лист %s: блок %q: в строке %d колонка %d = %q, ожидалось %q",
				etfChartsSheet, header, etfChartsColumnsRow, col, dateHeader, etfDateHeader)
		}
		return col, nil
	}
	return 0, fmt.Errorf("лист %s: блок %q не найден в строке %d", etfChartsSheet, header, etfChartsBlockHeaderRow)
}

// chartsRegionColumns — четыре колонки регионов блока сразу за колонкой дат.
func chartsRegionColumns(f *excelize.File, dateCol int) ([]regionColumn, error) {
	cols := make([]regionColumn, 0, len(wgcEtfRegions)-1)
	for i := 1; i < len(wgcEtfRegions); i++ {
		col := dateCol + i
		raw, err := cellRaw(f, etfChartsSheet, col, etfChartsColumnsRow)
		if err != nil {
			return nil, fmt.Errorf("лист %s: заголовок колонки %d: %w", etfChartsSheet, col, err)
		}
		name := strings.TrimSpace(raw)
		region, ok := etfRegionsByName[name]
		if !ok {
			return nil, fmt.Errorf("лист %s: колонка %d: заголовок региона %q вне словаря регионов", etfChartsSheet, col, name)
		}
		cols = append(cols, regionColumn{col: col, region: region})
	}
	return cols, nil
}

// rowMonth читает дату строки (колонка col) и нормализует её к концу месяца; ok=false — колонка
// дат пуста, то есть строка данных не началась.
func rowMonth(f *excelize.File, sheet string, col, row int) (date time.Time, ok bool, err error) {
	raw, err := cellRaw(f, sheet, col, row)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("лист %s: дата строки %d: %w", sheet, row, err)
	}
	if raw == "" {
		return time.Time{}, false, nil
	}
	date, err = cellDate(raw)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("лист %s: дата строки %d: %w", sheet, row, err)
	}
	return monthEnd(date), true, nil
}

// checkSkippedRowStaysEmpty — страховка от тихой потери значений: строка без даты пропускается
// только когда в колонках данных пусто (в хвосте листов есть пустые строки и подписи). Если
// значение есть, а даты нет — это дефект выгрузки, и терять его молча нельзя.
func checkSkippedRowStaysEmpty(f *excelize.File, sheet string, dateCol, row int, cols []regionColumn) error {
	for _, c := range cols {
		raw, err := cellRaw(f, sheet, c.col, row)
		if err != nil {
			return fmt.Errorf("лист %s: колонка %d строка %d: %w", sheet, c.col, row, err)
		}
		if raw == "" {
			continue
		}
		ref, err := excelize.CoordinatesToCellName(c.col, row)
		if err != nil {
			return err
		}
		return fmt.Errorf("лист %s: строка %d без даты (колонка %d пуста), но ячейка %s = %q: значение потерялось бы",
			sheet, row, dateCol, ref, raw)
	}
	return nil
}

// checkColumnStaysEmpty — страховка от тихой потери колонки: заголовок региона в строке 4 может
// дрейфовать от релиза к релизу, и колонка без региона допустима только пока в ней нет данных,
// иначе фонд выпал бы из сумм молча.
func checkColumnStaysEmpty(f *excelize.File, sheet string, col, maxRow int) error {
	for row := etfFirstDataRow; row <= maxRow; row++ {
		raw, err := cellRaw(f, sheet, col, row)
		if err != nil {
			return fmt.Errorf("лист %s: колонка %d строка %d: %w", sheet, col, row, err)
		}
		if raw == "" {
			continue
		}
		ref, err := excelize.CoordinatesToCellName(col, row)
		if err != nil {
			return err
		}
		return fmt.Errorf("лист %s: колонка %d без региона в строке %d, но ячейка %s = %q: регион фонда не распознан",
			sheet, col, etfRegionRow, ref, raw)
	}
	return nil
}

// appendRegionPoints добавляет точки регионов и world. world пишется только когда за месяц есть
// значения всех четырёх регионов: сумма по неполному набору занижала бы итог.
func appendRegionPoints(points []seriesPoint, metric string, date time.Time, sums map[string]float64) []seriesPoint {
	if allEtfRegionsPresent(sums) {
		points = append(points, seriesPoint{
			Series: wgcEtfSeriesName(metric, etfWorldRegion),
			Date:   date,
			Value:  etfWorldValue(sums),
		})
	}
	for _, region := range wgcEtfRegions {
		if region == etfWorldRegion {
			continue
		}
		value, ok := sums[region]
		if !ok {
			continue // у региона за месяц нет ни одного значения — точки нет
		}
		points = append(points, seriesPoint{Series: wgcEtfSeriesName(metric, region), Date: date, Value: value})
	}
	return points
}

// allEtfRegionsPresent — есть ли за месяц значения у всех четырёх регионов.
func allEtfRegionsPresent(sums map[string]float64) bool {
	for _, region := range wgcEtfRegions {
		if region == etfWorldRegion {
			continue
		}
		if _, ok := sums[region]; !ok {
			return false
		}
	}
	return true
}

// etfWorldValue — сумма четырёх регионов в фиксированном порядке (детерминированный итог).
func etfWorldValue(sums map[string]float64) float64 {
	var total float64
	for _, region := range wgcEtfRegions {
		if region == etfWorldRegion {
			continue
		}
		total += sums[region]
	}
	return total
}

// checkHoldingsTotal сверяет готовый итог листа «Holdings by month» (колонка D, тонны) с суммой
// фондов за тот же месяц. Расхождение — сигнал дрейфа выгрузки, но не ошибка: точкой ряда
// остаётся сумма фондов, поэтому только warn.
func checkHoldingsTotal(f *excelize.File, row int, date time.Time, fundsTotal float64) error {
	raw, err := cellRaw(f, etfHoldingsSheet, etfTotalCol, row)
	if err != nil {
		return fmt.Errorf("лист %s: итог колонки D строки %d: %w", etfHoldingsSheet, row, err)
	}
	total, ok, err := cellFloat(raw)
	if err != nil {
		return fmt.Errorf("лист %s: итог колонки D строки %d: %w", etfHoldingsSheet, row, err)
	}
	if ok && math.Abs(total-fundsTotal) > etfTotalTolerance {
		log.Warnf("wgc: лист %s за %s: итог колонки D = %v т, сумма фондов = %v т (расхождение %v т)",
			etfHoldingsSheet, date.Format("2006-01"), total, fundsTotal, total-fundsTotal)
	}
	return nil
}
