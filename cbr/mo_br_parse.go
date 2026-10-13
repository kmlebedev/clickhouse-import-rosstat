package cbr

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// moBrPoint — одна точка ряда макроэкономического опроса ЦБ (full.xlsx): имя ряда в
// macro_series (Series), дата волны опроса (Date — 1-е число месяца волны) и значение
// прогноза или статистики (Value).
type moBrPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

const (
	// moBrNavigationMarker — подстрока имени навигационного листа полной книги ЦБ
	// (в живом файле — «Навигация (Navigation) » с хвостовым пробелом, поэтому сравнение
	// по вхождению). Наличие такого листа означает полную книгу: все 17 листов реестра
	// обязаны быть на месте.
	moBrNavigationMarker = "Навигация"

	// Индексы колонок листа (0-based): B — метки блоков статистик, D — прогнозные годы.
	moBrLabelCol = 1
	moBrYearCol  = 3

	// moBrMinHeaderDates — сколько дат должно быть в строке, чтобы считать её шапкой волн
	// (у одиночных дат-годов в колонке D эта проверка не срабатывает).
	moBrMinHeaderDates = 3

	// Окно правдоподобных лет даты Excel: и дата волны, и год прогноза обязаны в него
	// попадать. Значения-прогнозы (0–100) при переводе дают 1900 год и датами не считаются —
	// это и отсеивает их при поиске шапки и колонок-волн.
	moBrMinYear = 2000
	moBrMaxYear = 2100

	// moBrDash — маркер отсутствующего значения волны в файле.
	moBrDash = "-"
)

// moBrWave — колонка-волна листа: индекс колонки (0-based) и дата волны из шапки.
type moBrWave struct {
	col  int
	date time.Time
}

// parseMoBr читает книгу макроэкономического опроса ЦБ (full.xlsx) и возвращает точки рядов
// и их описания для series_catalog. Функция чистая: на вход — открытая книга, без сети и БД.
//
// Числа и даты читаются только в raw-виде (RawCellValue): форматированные строки теряют
// точность (14.599246575342466 вместо 14.5) и превращают даты в текст. Обход — по
// GetSheetList (порядок книги), а не по реестру: лист с «Навигация» в имени пропускается,
// лист реестра 17 разбирается, любой другой лист — ошибка (структура источника изменилась).
// Если навигационный лист был (полная книга ЦБ), все 17 листов реестра обязаны присутствовать;
// в сокращённой книге (например, фикстура) их отсутствие не ошибка.
//
// Разбор fail-fast: ошибка на любом листе или отсутствие точек — возврат без частичных данных,
// поэтому импортёр не отправит неполный батч.
func parseMoBr(xlsx *excelize.File) ([]moBrPoint, []util.SeriesMeta, error) {
	var (
		points []moBrPoint
		metas  []util.SeriesMeta
	)
	present := make(map[string]bool, len(moBrIndicators))
	full := false
	for _, sheet := range xlsx.GetSheetList() {
		if strings.Contains(sheet, moBrNavigationMarker) {
			full = true
			continue
		}
		ind, ok := moBrIndicatorBySheet(sheet)
		if !ok {
			return nil, nil, fmt.Errorf("неизвестный лист-показатель %q: структура книги изменилась, дополните реестр moBrIndicators", sheet)
		}
		present[sheet] = true
		pts, sheetMetas, err := parseMoBrSheet(xlsx, ind)
		if err != nil {
			return nil, nil, err
		}
		points = append(points, pts...)
		metas = append(metas, sheetMetas...)
	}
	if full {
		var missing []string
		for _, ind := range moBrIndicators {
			if !present[ind.Sheet] {
				missing = append(missing, fmt.Sprintf("«%s»", ind.Sheet))
			}
		}
		if len(missing) > 0 {
			return nil, nil, fmt.Errorf("полная книга (навигационный лист есть): нет %d листов-показателей из %d — %s",
				len(missing), len(moBrIndicators), strings.Join(missing, ", "))
		}
	}
	if len(points) == 0 {
		return nil, nil, fmt.Errorf("не извлечено ни одной точки: проверьте структуру книги")
	}
	return points, metas, nil
}

// parseMoBrSheet разбирает один лист-показатель: находит шапку волн (строка с датами) и
// колонки-волны, затем идёт по строкам, отслеживая текущий блок статистики (метка колонки B)
// и прогнозный год (дата колонки D; у листов 16–17 годов нет). Пустые ячейки и «-» —
// пропуск точки. Возвращает точки и описания каталога для комбинаций (год, статистика),
// у которых оказалась хотя бы одна точка, — поэтому множества рядов точек и каталога
// совпадают по построению.
func parseMoBrSheet(xlsx *excelize.File, ind moBrIndicator) ([]moBrPoint, []util.SeriesMeta, error) {
	rows, err := xlsx.GetRows(ind.Sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, nil, fmt.Errorf("лист %q: чтение строк: %w", ind.Sheet, err)
	}
	headerRow, waves, err := moBrWaveHeader(rows, ind)
	if err != nil {
		return nil, nil, err
	}

	var (
		points []moBrPoint
		metas  []util.SeriesMeta
	)
	described := make(map[string]bool)
	blocks, stat, year := 0, "", ""
	for r := headerRow + 1; r <= len(rows); r++ {
		if label := strings.TrimSpace(moBrCell(rows, r, moBrLabelCol)); label != "" {
			// Новая метка блока: известная статистика — разбираем, неизвестная
			// («Среднее», 3-й и 1-й квартили) — вне объёма, блок молча пропускается.
			stat, _ = moBrStatSuffix[label]
			if stat != "" {
				blocks++
			}
			year = "" // год не переносится в следующий блок
		}
		if stat == "" {
			continue
		}
		if !ind.NoYears {
			// Год — дата в колонке D. Пустая ячейка оставляет год предыдущей строки
			// (перенос объединённой ячейки); пока года нет, строка не разбирается.
			if raw := strings.TrimSpace(moBrCell(rows, r, moBrYearCol)); raw != "" {
				date, ok := moBrExcelDate(raw)
				if !ok {
					return nil, nil, fmt.Errorf("лист %q: строка %d, колонка D: значение %q не является датой прогнозного года", ind.Sheet, r, raw)
				}
				year = strconv.Itoa(date.Year())
			}
			if year == "" {
				continue
			}
		}
		series := moBrSeriesName(ind.Base, year, stat)
		for _, wave := range waves {
			raw := strings.TrimSpace(moBrCell(rows, r, wave.col))
			if raw == "" || raw == moBrDash {
				continue // «-» и пусто — отсутствие точки, соседние значения не страдают
			}
			value, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("лист %q: ячейка %s: значение %q не является числом",
					ind.Sheet, moBrCellName(wave.col, r), raw)
			}
			if !described[series] {
				described[series] = true
				metas = append(metas, moBrSeriesMeta(ind, year, stat))
			}
			points = append(points, moBrPoint{Series: series, Date: wave.date, Value: value})
		}
	}
	if blocks == 0 {
		return nil, nil, fmt.Errorf("лист %q: не найдено ни одного блока статистик из 6 в колонке B (ожидались метки: %s)",
			ind.Sheet, strings.Join(moBrStatLabels(), ", "))
	}
	if len(points) == 0 {
		return nil, nil, fmt.Errorf("лист %q: 0 точек после разбора (все значения пустые или «-»)", ind.Sheet)
	}
	return points, metas, nil
}

// moBrWaveHeader находит шапку волн — первую строку листа, в которой не меньше
// moBrMinHeaderDates ячеек являются датами Excel, — и колонки-волны (по возрастанию).
// На годовых листах колонка D отведена под прогнозные годы и волной не считается.
func moBrWaveHeader(rows [][]string, ind moBrIndicator) (int, []moBrWave, error) {
	for i, row := range rows {
		var waves []moBrWave
		for c, raw := range row {
			if c == moBrYearCol && !ind.NoYears {
				continue
			}
			date, ok := moBrExcelDate(strings.TrimSpace(raw))
			if !ok {
				continue
			}
			waves = append(waves, moBrWave{col: c, date: date})
		}
		if len(waves) >= moBrMinHeaderDates {
			return i + 1, waves, nil
		}
	}
	return 0, nil, fmt.Errorf("лист %q: не найдена шапка волн (строка с %d и более датами)", ind.Sheet, moBrMinHeaderDates)
}

// moBrExcelDate переводит raw-значение ячейки в дату Excel. Датой считается только значение
// в окне лет moBrMinYear…moBrMaxYear: значения-прогнозы (0–100) дают 1900 год, поэтому
// отличаются от дат волн и прогнозных годов без отдельного признака формата.
func moBrExcelDate(raw string) (time.Time, bool) {
	serial, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return time.Time{}, false
	}
	date, err := excelize.ExcelDateToTime(serial, false)
	if err != nil {
		return time.Time{}, false
	}
	if date.Year() < moBrMinYear || date.Year() > moBrMaxYear {
		return time.Time{}, false
	}
	return date, true
}

// moBrCell — значение ячейки листа (строка 1-based, колонка 0-based); пустая строка, если
// ячейки нет: GetRows обрезает хвостовые пустые ячейки строки.
func moBrCell(rows [][]string, row, col int) string {
	if row < 1 || row > len(rows) {
		return ""
	}
	cells := rows[row-1]
	if col < 0 || col >= len(cells) {
		return ""
	}
	return cells[col]
}

// moBrCellName — адрес ячейки (строка 1-based, колонка 0-based) для сообщений об ошибке.
func moBrCellName(col, row int) string {
	name, err := excelize.CoordinatesToCellName(col+1, row)
	if err != nil {
		return fmt.Sprintf("колонка %d, строка %d", col+1, row)
	}
	return name
}
