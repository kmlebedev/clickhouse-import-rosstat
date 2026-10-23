package domrf

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

const (
	// domrfMarketSheetAnchor — фрагмент имени листа книги. Лист ищется по имени, а не по номеру:
	// добавленный источником лист сдвинул бы номер.
	domrfMarketSheetAnchor = "общая таблица"
	// domrfMarketHeaderAnchor — начало подписи первой группы шапки. Строка шапки ищется по
	// признаку, а не по номеру: у книги над шапкой заголовок и пустая строка, и их число может
	// измениться.
	domrfMarketHeaderAnchor = "Количество выданных кредитов"
	// domrfMarketHeaderRows — сколько строк занимает шапка (merged-диапазоны B3:E4, N3:AA3,
	// AH3:AL3): путь колонки собирается из трёх строк.
	domrfMarketHeaderRows = 3
)

// domrfMarketNoData — литералы «нет данных» источника: X/x/х (латиница и кириллица) и дефис.
// Пустая ячейка — тоже «нет данных». Ноль — настоящее значение и точкой становится.
var domrfMarketNoData = map[string]bool{"": true, "x": true, "х": true, "-": true, "—": true, "–": true}

// domrfMarketSeries — ряд набора A: имя из кода и путь шапки. Путь — подписи строк шапки после
// заполнения merged-диапазонов; сравнение идёт по нормализованному виду (регистр, пробелы).
type domrfMarketSeries struct {
	Series string
	Path   []string
	// Percent — колонка хранит долю, а источник форматирует её процентом («0.10%» = 0.001).
	// У остальных рядов знак «%» в ячейке — ошибка: он означал бы смену формата источника.
	Percent bool
}

// domrfMarketSeriesList — 24 ряда в порядке колонок источника. Имена объявляет код (стандарт
// §6.8), каталог (domrf/series_meta.go) читает тот же список.
//
// Блок задолженности даёт три разных итога, и пути у них различаются уровнем:
//   - portfolio_total — верхний «всего» блока (баланс банков + приобретённые права + ИЦБ + ДОМ.РФ);
//   - portfolio_bank — «всего» внутри группы «на балансе банков»;
//   - portfolio_bank_total — «Банковский портфель» (баланс + приобретённые права).
var domrfMarketSeriesList = []domrfMarketSeries{
	{Series: "issued_count_rub", Path: []string{"Количество выданных кредитов, единиц", "в рублях"}},
	{Series: "issued_count_fx", Path: []string{"Количество выданных кредитов, единиц", "в валюте"}},
	{Series: "issued_count_total", Path: []string{"Количество выданных кредитов, единиц", "всего"}},
	{Series: "issued_count_ddu", Path: []string{"Количество выданных кредитов, единиц", "в т.ч. ДДУ"}},
	{Series: "issued_volume_rub", Path: []string{"Объем выданных кредитов, млн руб.", "в рублях"}},
	{Series: "issued_volume_fx", Path: []string{"Объем выданных кредитов, млн руб.", "в валюте"}},
	{Series: "issued_volume_total", Path: []string{"Объем выданных кредитов, млн руб.", "всего"}},
	{Series: "issued_volume_ddu", Path: []string{"Объем выданных кредитов, млн руб.", "в т.ч. ДДУ"}},
	{Series: "avg_loan_size", Path: []string{"Средний размер кредита, тыс. руб.", "в рублях"}},
	{Series: "portfolio_total", Path: []string{"Задолженность по выданным кредитам, млн руб.", "всего"}},
	{Series: "portfolio_bank", Path: []string{"Задолженность по выданным кредитам, млн руб.", "на балансе банков", "всего"}},
	{Series: "portfolio_acquired", Path: []string{"Задолженность по выданным кредитам, млн руб.", "приобретенные права требований", "в рублях"}},
	{Series: "portfolio_bank_total", Path: []string{"Задолженность по выданным кредитам, млн руб.", "Банковский портфель"}},
	{Series: "portfolio_ddu", Path: []string{"Задолженность по выданным кредитам, млн руб.", "в т.ч. ДДУ"}},
	{Series: "portfolio_mbs", Path: []string{"Задолженность по выданным кредитам, млн руб.", "ИЦБ (вне баланса)"}},
	{Series: "portfolio_domrf", Path: []string{"Задолженность по выданным кредитам, млн руб.", "ДОМ.РФ"}},
	{Series: "overdue_total", Path: []string{"Задолженность по выданным кредитам, млн руб.", "в том числе просроченная", "всего"}},
	{Series: "overdue_ddu", Path: []string{"Задолженность по выданным кредитам, млн руб.", "в том числе просроченная", "в т.ч. ДДУ"}},
	{Series: "overdue_share_total", Path: []string{"Доля просроченной задолженности в общем объеме задолженности на балансе банков по выданным кредитам, %", "всего"}, Percent: true},
	{Series: "overdue_share_ddu", Path: []string{"Доля просроченной задолженности в общем объеме задолженности на балансе банков по выданным кредитам, %", "в т.ч. ДДУ"}, Percent: true},
	{Series: "rate_month", Path: []string{"Средневзвешенная ставка, %", "в рублях", "в течение месяца"}},
	{Series: "rate_ytd", Path: []string{"Средневзвешенная ставка, %", "в рублях", "с начала года"}},
	{Series: "rate_primary", Path: []string{"Средневзвешенная ставка, %", "в рублях", "на первичном рынке (мес.)"}},
	{Series: "rate_secondary", Path: []string{"Средневзвешенная ставка, %", "в рублях", "на вторичном рынке (мес.)"}},
}

// domrfMarketPoint — точка набора A: ряд, первое число отчётного месяца и значение в единице
// ряда (единицы разные — см. series_catalog).
type domrfMarketPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

// domrfMarketKey нормализует подпись шапки: регистр не значим, неразрывный пробел — пробел,
// повторы пробелов схлопнуты. Типографика источника (двойной пробел в «ИЦБ  (вне баланса)»)
// не должна делать колонку «новой».
func domrfMarketKey(parts []string) string {
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		field := strings.Join(strings.Fields(strings.ToLower(strings.ReplaceAll(part, "\u00a0", " "))), " ")
		if field == "" {
			continue
		}
		normalized = append(normalized, field)
	}
	return strings.Join(normalized, " | ")
}

// domrfMarketHeaderPath собирает путь колонки из строк шапки: подписи строк 3–5, повторы подряд
// схлопываются (вертикальный merge даёт одну и ту же подпись в двух строках). Так «на балансе
// банков / всего» и верхний «всего» перестают быть одним путём.
func domrfMarketHeaderPath(labels []string) []string {
	path := make([]string, 0, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		if len(path) > 0 && path[len(path)-1] == label {
			continue
		}
		path = append(path, label)
	}
	return path
}

// domrfMarketHeaderFill заполняет merged-диапазоны шапки значением левой верхней ячейки: Excel
// хранит подпись группы только в первой колонке, а остальные колонки группы пусты. Без заполнения
// путь колонки «в рублях» потерял бы группу и совпал с одноимённой колонкой другой группы.
func domrfMarketHeaderFill(rows [][]string, merges []excelize.MergeCell, headerRow int) {
	for i := range merges {
		merge := &merges[i]
		value := strings.TrimSpace(merge.GetCellValue())
		if value == "" {
			continue
		}
		x1, y1, err := excelize.CellNameToCoordinates(merge.GetStartAxis())
		if err != nil {
			continue
		}
		x2, y2, err := excelize.CellNameToCoordinates(merge.GetEndAxis())
		if err != nil {
			continue
		}
		for row := max(y1, headerRow); row <= min(y2, headerRow+domrfMarketHeaderRows-1); row++ {
			for col := x1; col <= x2; col++ {
				if row-1 < len(rows) && col-1 < len(rows[row-1]) && strings.TrimSpace(rows[row-1][col-1]) == "" {
					rows[row-1][col-1] = value
				}
			}
		}
	}
}

// domrfMarketPad выравнивает строки листа по самой широкой: excelize отдаёт каждую строку своей
// длины (пустые хвостовые ячейки обрезаны), а разбор адресует колонки по индексу. Без выравнивания
// последняя группа шапки («Средневзвешенная ставка») выпадала бы из карты путей, а короткая строка
// данных молча теряла бы значения.
func domrfMarketPad(rows [][]string) {
	width := 0
	for _, row := range rows {
		if len(row) > width {
			width = len(row)
		}
	}
	for i, row := range rows {
		if len(row) == width {
			continue
		}
		padded := make([]string, width)
		copy(padded, row)
		rows[i] = padded
	}
}

// domrfMarketColumns отдаёт индекс колонки для каждого ряда. Ожидаемый путь обязан найти ровно
// одну колонку: дубль означал бы, что шапка изменилась и под прежним именем читалась бы чужая
// колонка, а пропуск — что источник переименовал группу. Оба случая валят прогон (стандарт §6.4).
func domrfMarketColumns(header [][]string) (map[string]int, error) {
	byPath := make(map[string][]int, len(header[0]))
	for col := range header[0] {
		labels := make([]string, 0, len(header))
		for row := 0; row < len(header); row++ {
			labels = append(labels, header[row][col])
		}
		key := domrfMarketKey(domrfMarketHeaderPath(labels))
		byPath[key] = append(byPath[key], col)
	}
	columns := make(map[string]int, len(domrfMarketSeriesList))
	for _, series := range domrfMarketSeriesList {
		key := domrfMarketKey(series.Path)
		found := byPath[key]
		switch len(found) {
		case 1:
			columns[series.Series] = found[0]
		case 0:
			return nil, fmt.Errorf("%s: колонка %q в шапке не найдена (источник изменил подписи)", series.Series, key)
		default:
			return nil, fmt.Errorf("%s: колонка %q встречается %d раза (шапка неоднозначна)", series.Series, key, len(found))
		}
	}
	return columns, nil
}

// domrfMarketIsSerial — строка состоит только из цифр: серийное число Excel всегда целое, а
// «09.2026» (месяц-год) и «01.09.2026» содержат разделители и разбираются layouts, а не как serial.
func domrfMarketIsSerial(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// domrfMarketDate разбирает дату отчётного месяца: excelize отдаёт её форматированной строкой,
// вид зависит от числового формата ячейки источника (дата, дата-время, ISO). Неизвестный вид —
// ошибка, а не пропуск строки: пропуск молча сократил бы историю.
func domrfMarketDate(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	// Форматированная дата у книги зависит от локали формата ячейки (в источнике — m/d/yy),
	// поэтому дата читается сырым серийным числом Excel и конвертируется; текстовые layouts
	// остаются для ячеек, где дата записана строкой.
	if serial, err := strconv.ParseFloat(value, 64); err == nil && domrfMarketIsSerial(value) {
		date, err := excelize.ExcelDateToTime(serial, false)
		if err != nil {
			return time.Time{}, fmt.Errorf("серийная дата %q: %w", value, err)
		}
		return time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, time.UTC), nil
	}
	layouts := []string{
		"02.01.2006", "02.01.06", "02-01-2006", "02-01-06",
		"2006-01-02", "2006-01-02 15:04:05", "2006-01-02T15:04:05Z",
		"01.2006", "2006-01", "02.01.2006 15:04:05",
	}
	for _, layout := range layouts {
		if date, err := time.Parse(layout, value); err == nil {
			return time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, time.UTC), nil
		}
	}
	return time.Time{}, fmt.Errorf("дата %q не разобрана", value)
}

// domrfMarketNormalizeNumber снимает разделители разрядов и оставляет десятичную точку. Книга
// смешивает форматы: «1,020,743» (запятая — тысячи), «56,341» (тоже тысячи), «4 466,28» и «14,9»
// (запятая — десятичная). Однозначного правила «запятая всегда X» нет, поэтому:
// несколько запятых — тысячи; одна запятая и три цифры после неё при ненулевом целом — тоже тысячи;
// иначе — десятичная. «0,125» остаётся десятичной: доля меньше единицы не бывает тысячами.
func domrfMarketNormalizeNumber(value string) string {
	s := strings.NewReplacer(" ", "", "\u00a0", "", "\u202f", "").Replace(value)
	commas, dots := strings.Count(s, ","), strings.Count(s, ".")
	switch {
	case commas > 1:
		s = strings.ReplaceAll(s, ",", "")
	case commas == 1 && dots == 0:
		parts := strings.SplitN(s, ",", 2)
		if len(parts[1]) == 3 && parts[0] != "" && parts[0] != "0" && parts[0] != "-0" {
			s = strings.ReplaceAll(s, ",", "")
		} else {
			s = strings.ReplaceAll(s, ",", ".")
		}
	case commas == 1 && dots > 0:
		s = strings.ReplaceAll(s, ",", "")
	case dots > 1:
		s = strings.ReplaceAll(s, ".", "")
	}
	return s
}

// domrfMarketNumber разбирает числовую ячейку: пробелы (в том числе неразрывные) — разделители
// разрядов, запятая — десятичный разделитель. «Нет данных» отличает отсутствие точки от нуля;
// нечисловой текст — ошибка, а не пропуск (иначе смена формата источника выглядела бы дырой).
func domrfMarketNumber(value string, percent bool) (number float64, ok bool, err error) {
	trimmed := strings.TrimSpace(strings.ToLower(strings.ReplaceAll(value, "\u00a0", " ")))
	if domrfMarketNoData[trimmed] {
		return 0, false, nil
	}
	// Процентный формат Excel: ячейка хранит долю, а отображается «0.10%». Делим на 100, чтобы
	// доля осталась долей; в не-долевом ряду знак «%» — смена формата источника и ошибка.
	percentSign := strings.HasSuffix(trimmed, "%")
	if percentSign && !percent {
		return 0, false, fmt.Errorf("значение %q в процентном формате, ряд не доля", value)
	}
	trimmed = strings.TrimSuffix(trimmed, "%")
	number, err = strconv.ParseFloat(domrfMarketNormalizeNumber(trimmed), 64)
	if err != nil {
		return 0, false, fmt.Errorf("значение %q не число: %w", value, err)
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, false, fmt.Errorf("значение %q не конечно", value)
	}
	if percentSign {
		number /= 100
	}
	return number, true, nil
}

// domrfMarketPoints разбирает книгу «Общая таблица по РФ» в точки. Чистая функция (§6.1):
// файл приходит байтами из шаблона, сеть и БД не участвуют.
func domrfMarketPoints(xlsx *excelize.File) ([]domrfMarketPoint, error) {
	sheet, err := domrfMarketSheet(xlsx)
	if err != nil {
		return nil, err
	}
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("лист %s: %w", sheet, err)
	}
	// GetRows обрезает пустые хвостовые ячейки у каждой строки отдельно: у шапки последняя
	// группа («Средневзвешенная ставка») короче строки данных, и без выравнивания колонки 34+
	// выпали бы из карты путей, а у данных — молча пропустили бы значения.
	domrfMarketPad(rows)
	headerRow, err := domrfMarketHeaderRow(rows)
	if err != nil {
		return nil, fmt.Errorf("лист %s: %w", sheet, err)
	}
	merges, err := xlsx.GetMergeCells(sheet)
	if err != nil {
		return nil, fmt.Errorf("лист %s: merged-диапазоны: %w", sheet, err)
	}
	domrfMarketHeaderFill(rows, merges, headerRow)
	header := rows[headerRow-1 : headerRow-1+domrfMarketHeaderRows]
	columns, err := domrfMarketColumns(header)
	if err != nil {
		return nil, fmt.Errorf("лист %s: %w", sheet, err)
	}
	points := make([]domrfMarketPoint, 0, len(rows)*len(domrfMarketSeriesList)/2)
	perSeries := make(map[string]int, len(domrfMarketSeriesList))
	for row := headerRow - 1 + domrfMarketHeaderRows; row < len(rows); row++ {
		first := strings.TrimSpace(rows[row][0])
		if first == "" {
			continue
		}
		// Сноска под таблицей («*Данные до 01.01.2023 — …») стоит в колонке даты. Пропускаем
		// только строки с явным маркером сноски: любая другая неразобранная дата — ошибка.
		if strings.HasPrefix(first, "*") {
			continue
		}
		date, err := domrfMarketDate(first)
		if err != nil {
			return nil, fmt.Errorf("лист %s, строка %d: %w", sheet, row+1, err)
		}
		for _, series := range domrfMarketSeriesList {
			col := columns[series.Series]
			if col >= len(rows[row]) {
				continue
			}
			number, ok, err := domrfMarketNumber(rows[row][col], series.Percent)
			if err != nil {
				return nil, fmt.Errorf("лист %s, строка %d, %s: %w", sheet, row+1, series.Series, err)
			}
			if !ok {
				continue
			}
			points = append(points, domrfMarketPoint{Series: series.Series, Date: date, Value: number})
			perSeries[series.Series]++
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("лист %s: ни одной точки", sheet)
	}
	for _, series := range domrfMarketSeriesList {
		if perSeries[series.Series] == 0 {
			return nil, fmt.Errorf("лист %s: ряд %s без точек", sheet, series.Series)
		}
	}
	return points, nil
}

// domrfMarketSheet ищет лист по фрагменту имени. У книги он единственный, но поиск по признаку
// переживёт добавление второго листа источником.
func domrfMarketSheet(xlsx *excelize.File) (string, error) {
	for _, sheet := range xlsx.GetSheetList() {
		if strings.Contains(strings.ToLower(sheet), domrfMarketSheetAnchor) {
			return sheet, nil
		}
	}
	return "", fmt.Errorf("лист с %q не найден (листы: %s)", domrfMarketSheetAnchor, strings.Join(xlsx.GetSheetList(), ", "))
}

// domrfMarketHeaderRow ищет первую строку шапки по якорю первой группы.
func domrfMarketHeaderRow(rows [][]string) (int, error) {
	const searchRows = 10
	for row := 0; row < len(rows) && row < searchRows; row++ {
		for _, cell := range rows[row] {
			if strings.Contains(strings.ToLower(cell), strings.ToLower(domrfMarketHeaderAnchor)) {
				return row + 1, nil
			}
		}
	}
	return 0, fmt.Errorf("шапка с %q не найдена в первых %d строках", domrfMarketHeaderAnchor, searchRows)
}
