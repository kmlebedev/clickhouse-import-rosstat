package cbr

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// Разбор листа «Табл 1» книги «Комментария к среднесрочному прогнозу» ЦБ (таблица cbr_mt_forecast):
// поиск таблицы по якорям и чтение точек. Качает книгу выпуска и пишет точки импортёр
// cbr/mt_forecast.go; здесь ни сети, ни БД (стандарт §6.1) — на входе открытая книга, на выходе
// плоский список точек, коды которых объявлены реестром cbr/mt_forecast_meta.go (стандарт §6.8).
//
// Вёрстка «Табл 1» дрейфует между выпусками (спека §2, §3.3): горизонт прогноза — 3–4 года
// (07052026: 2026–2028, 05082026: 2026–2029), число листов книги — 24–25, тире в ячейках — и
// дефис, и en dash, отрицательные границы — в скобках, а номер сноски источник печатает вплотную
// к последней цифре значения («14,5-14,61» = «14,5–14,6» + сноска 1). Поэтому лист ищется по
// имени, первая строка-показатель и шапка годов — по признакам (спека §3.3), а не по номерам
// строк: номер шапки плавает вместе с числом прогнозных колонок.
const (
	// mtForecastTable — таблица импортёра: её называют сообщения разбора и предупреждения (стандарт
	// §6.4). Имя то же, что у реестра cbr/importers.go (там — литералом, стандарт §8.2).
	mtForecastTable = "cbr_mt_forecast"

	// mtForecastSheet — лист книги с таблицей (спека §2). Имя листа — якорь: у книги 24–25 листов, и
	// по номеру лист не ищется; смена имени («Табл 1» → «Табл. 1») обязана упасть с именем листа, а
	// не вернуть ноль точек.
	mtForecastSheet = "Табл 1"

	// mtForecastSheetsInError — сколько имён листов книги входит в сообщение «нет листа «Табл 1»»: в
	// книге 24–25 листов, и полный список превратил бы ошибку в свалку.
	mtForecastSheetsInError = 5

	// mtForecastHeaderYears — сколько ячеек шапки должны парситься как год, чтобы строка считалась
	// шапкой годов (спека §3.3). Одного мало: год может случайно оказаться в строке-заголовке или в
	// подписи факт-колонки.
	mtForecastHeaderYears = 2

	// mtForecastLabelColumn — колонка подписей строк: в таблице это A (спека §2), от неё вправо идут
	// колонки данных. Индекс нулевой — как у excelize.
	mtForecastLabelColumn = 0
)

var (
	// mtForecastEndRe — конец таблицы: строка-сноска начинается с номера («1 С учетом…», спека §3.3).
	// Ниже таблицы лежат сноски 1–3 в том же первом столбце, что подписи показателей, и без якоря
	// каждая из них дала бы предупреждение на каждый выпуск.
	mtForecastEndRe = regexp.MustCompile(`^\d+ `)

	// mtForecastPointRe — точечный прогноз: «4,0», «-4,9»; скобки у отрицательного значения форма
	// допускает — источник печатает в скобках отрицательные границы диапазонов; разделитель дроби —
	// запятая источника или точка (спека §3.3).
	mtForecastPointRe = regexp.MustCompile(`^\(?(-?\d+(?:[,.]\d+)?)\)?$`)

	// mtForecastRangeRe — диапазон: обе границы в той же форме, тире — дефис источника, en dash
	// («7–12») или em dash, скобки у отрицательных границ (спека §3.3).
	mtForecastRangeRe = regexp.MustCompile(`^\(?(-?\d+(?:[,.]\d+)?)\)?\s*[-–—]\s*\(?(-?\d+(?:[,.]\d+)?)\)?$`)

	// mtForecastFootnoteRe — форма «номер сноски приклеен к значению»: ровно два знака после
	// разделителя, последний — номер сноски (спека §3.3). Допущение названо в спеке: границы в
	// таблице печатаются с ≤1 знаком после запятой, поэтому «14,61» — это «14,6» + сноска 1.
	// Защита от искажения здесь — наблюдение формата источника и спот-тесты фикстур: проверка lo ≤ hi
	// ловит инверсию границ, а сдвиг обеих (истинное «4,25» → 4.2 при lo == hi) не видит.
	mtForecastFootnoteRe = regexp.MustCompile(`^-?\d+[,.]\d{2}$`)
)

// mtForecastSpaceReplacer — типографика источника, не влияющая на смысл (спека §3.3, слой 1):
// пробелы-двойники (NBSP U+00A0, EN SPACE U+2002, узкий U+2009 и hair U+200A, узкий неразрывный
// U+202F) и переводы строк → пробел, soft hyphen U+00AD удаляется. '2025\n(факт)' → '2025 (факт)'.
var mtForecastSpaceReplacer = strings.NewReplacer(
	"\u00a0", " ", "\u2002", " ", "\u2009", " ", "\u200a", " ", "\u202f", " ",
	"\r", " ", "\n", " ", "\t", " ",
	"\u00ad", "",
)

// mtForecastDashReplacer — оба тире источника (en dash U+2013, em dash U+2014) → ASCII-дефис
// (спека §3.3, слой 2): подписи подпунктов источник печатает en dash'ем («– домашних хозяйств»),
// а в карте реестра они записаны дефисом.
var mtForecastDashReplacer = strings.NewReplacer("–", "-", "—", "-")

// mtForecastPoint — точка разбора: код показателя, прогнозный год и границы диапазона. Выпуск в
// точку не входит: meeting_date, release_date и url добавляет импортёр при записи строки таблицы —
// они свойства выпуска, а не измерения (спека §3.1). У точечного прогноза lo == hi.
type mtForecastPoint struct {
	Indicator string
	Year      uint16
	Lo, Hi    float64
}

// mtForecastColumn — прогнозная колонка листа: индекс в строке (0-based; A — 0) и год из шапки.
type mtForecastColumn struct {
	Index int
	Year  uint16
}

// mtForecastRelease — выпуск источника: дата опорного заседания, дата публикации «Комментария» —
// ключ винтажа (спека §3.1, §3.4) — и адрес страницы выпуска (провенанс). Разбор книги о выпуске
// не знает; выпуск собирает импортёр из индексной страницы (mtForecastIndexReleases,
// cbr/mt_forecast.go) и передаёт в mtForecastAppendPoints. PDF — признак карточки без страницы и
// машиночитаемого файла (9 из 11 выпусков на 2026-10-17, спека §2): такой выпуск пропускается со
// счётчиком, MeetingDate у него не разобран, и в цикл импорта он не попадает.
type mtForecastRelease struct {
	MeetingDate time.Time
	ReleaseDate time.Time
	URL         string
	PDF         bool
}

// mtForecastContext — общая часть сообщений разбора: таблица и лист. Выпуск в сообщение не входит
// (разбор книги о нём не знает), но адрес ячейки («Табл 1!C10») в ошибках обязателен (спека §3.3):
// по нему ячейку находят в книге глазами.
func mtForecastContext(sheet string) string {
	return fmt.Sprintf("%s, лист «%s»", mtForecastTable, sheet)
}

// mtForecastNormalizeCell приводит ячейку к сравнимому виду (спека §3.3, слой 1): пробелы-двойники
// и переводы строк — пробел, soft hyphen удаляется, серии пробелов схлопываются, края обрезаются.
// Цифры не трогаются: приклеенный номер сноски снимает слой подписи, а не базовая нормализация.
func mtForecastNormalizeCell(raw string) string {
	return strings.Join(strings.Fields(mtForecastSpaceReplacer.Replace(raw)), " ")
}

// mtForecastNormalizeLabel — подпись строки (спека §3.3, слой 2): к базовой нормализации
// добавляются оба тире → дефис и срезание хвостовых цифр — номера сноски («…валюте3»; PDF-версия
// печатает его и через пробел — «…валюте 3», поэтому за цифрами снимается и пробел, иначе подпись
// не совпала бы с картой и строка выпала бы молча). Именно эта форма сравнивается с картой реестра,
// и только она: «почти совпало» означало бы чужую метрику под знакомым кодом.
func mtForecastNormalizeLabel(raw string) string {
	return strings.TrimSpace(strings.TrimRight(mtForecastDashReplacer.Replace(mtForecastNormalizeCell(raw)), "0123456789"))
}

// mtForecastStripSuperscripts снимает юникод-надстрочные цифры (¹ U+00B9, ² U+00B2, ³ U+00B3 и
// U+2070–U+2079) в любой позиции (спека §3.3): в PDF-версии таблицы номер сноски печатается
// надстрочным («14,5-14,6¹»), в xlsx он приезжает обычной цифрой — обе формы разбор читает одинаково.
func mtForecastStripSuperscripts(raw string) string {
	var out strings.Builder
	out.Grow(len(raw))
	for _, symbol := range raw {
		if symbol == '\u00b9' || symbol == '\u00b2' || symbol == '\u00b3' || (symbol >= '\u2070' && symbol <= '\u2079') {
			continue
		}
		out.WriteRune(symbol)
	}
	return out.String()
}

// mtForecastCell — ячейка строки по индексу колонки: GetRows обрезает хвостовые пустые ячейки, и
// короткая строка — норма; отсутствующая ячейка равна пустой (стандарт §6.5, §6.7).
func mtForecastCell(row []string, column int) string {
	if column < 0 || column >= len(row) {
		return ""
	}
	return row[column]
}

// mtForecastCellAddress — адрес ячейки так, как его видит человек в Excel («Табл 1!C10», спека
// §3.3): ошибки называют адрес, а не «колонка 3, строка 10». Индексы колонки и строки — нулевые;
// CoordinatesToCellName может отказать, и тогда адрес собирается числами (стандарт §6.5).
func mtForecastCellAddress(sheet string, column, row int) string {
	name, err := excelize.CoordinatesToCellName(column+1, row+1)
	if err != nil {
		return fmt.Sprintf("%s!колонка %d, строка %d", sheet, column+1, row+1)
	}
	return sheet + "!" + name
}

// mtForecastSheetName — лист книги с «Табл 1» (спека §3.3). Лист ищется по имени, а не по номеру:
// у книги 24–25 листов и порядок плавает. Ошибка называет листы книги — подменённая или
// переименованная книга должна быть видна сразу, а не по «0 points» (стандарт §6.4).
func mtForecastSheetName(xlsx *excelize.File) (string, error) {
	sheets := xlsx.GetSheetList()
	for _, sheet := range sheets {
		if sheet == mtForecastSheet {
			return sheet, nil
		}
	}
	return "", fmt.Errorf("%s: в книге нет листа «%s» (листы книги: %s) — источник сменил имя листа или разбирается чужая книга",
		mtForecastTable, mtForecastSheet, mtForecastSheetList(sheets))
}

// mtForecastSheetList — имена листов книги для сообщения: первые mtForecastSheetsInError,
// остальные — счётчиком.
func mtForecastSheetList(sheets []string) string {
	quoted := make([]string, 0, mtForecastSheetsInError)
	rest := 0
	for _, sheet := range sheets {
		if len(quoted) >= mtForecastSheetsInError {
			rest++
			continue
		}
		quoted = append(quoted, "«"+sheet+"»")
	}
	if len(quoted) == 0 {
		return "(пусто)"
	}
	text := strings.Join(quoted, ", ")
	if rest > 0 {
		text += fmt.Sprintf(", …(+%d)", rest)
	}
	return text
}

// mtForecastYear — год ячейки шапки: ровно число в границах 1900–2100 (спека §3.3). Год в шапке —
// числовая ячейка («2026»), факт-колонка — текст с пометкой («2025 (факт)»), поэтому ParseFloat
// различает их сам; дробное, «NaN» и текст годами не считаются.
func mtForecastYear(cell string) (uint16, bool) {
	value, err := strconv.ParseFloat(cell, 64)
	if err != nil || value != math.Trunc(value) || value < 1900 || value > 2100 {
		return 0, false
	}
	return uint16(value), true
}

// mtForecastFirstIndicatorRow — первая (при обходе сверху) строка, чья нормализованная подпись есть
// в реестре показателей: якорь всей таблицы (спека §3.3, стандарт §6.3). -1 — такой строки нет.
func mtForecastFirstIndicatorRow(rows [][]string) int {
	for i, row := range rows {
		if _, ok := mtForecastIndicatorByLabel(mtForecastNormalizeLabel(mtForecastCell(row, mtForecastLabelColumn))); ok {
			return i
		}
	}
	return -1
}

// mtForecastHeaderRow — шапка годов: ближайшая непустая строка выше первой строки-показателя с не
// менее чем mtForecastHeaderYears ячейками-годами (спека §3.3). Поиск идёт вверх от якоря: заголовок
// «Табл. 1», название таблицы и «Источник: Банк России.» стоят выше и при поиске «где-то выше»
// привязали бы шапку к себе. -1 — шапки нет.
func mtForecastHeaderRow(rows [][]string, firstIndicator int) int {
	for i := firstIndicator - 1; i >= 0; i-- {
		years := 0
		for j := mtForecastLabelColumn + 1; j < len(rows[i]); j++ {
			if _, ok := mtForecastYear(mtForecastNormalizeCell(rows[i][j])); ok {
				years++
			}
		}
		if years >= mtForecastHeaderYears {
			return i
		}
	}
	return -1
}

// mtForecastColumns — прогнозные колонки шапки: ячейка-год → колонка прогноза, ячейка с пометкой
// «факт»/«оценка» (по нижнему регистру) → факт-колонка и пропускается, пустая ячейка и всё
// остальное — ошибка (спека §3.3): не год и не помеченная факт-колонка значит, что разметка
// изменилась, и молча истолковать такую колонку значило бы записать значения не того года. Дубль
// года — тоже ошибка: две колонки одного года дали бы две точки (показатель, год) в одной книге.
// Колонки возвращаются по возрастанию года — этим порядком разбор читает строку.
func mtForecastColumns(sheet string, headerRow int, header []string) ([]mtForecastColumn, error) {
	columns := make([]mtForecastColumn, 0, len(header))
	seen := make(map[uint16]bool, len(header))
	for j := mtForecastLabelColumn + 1; j < len(header); j++ {
		cell := mtForecastNormalizeCell(header[j])
		year, ok := mtForecastYear(cell)
		if ok {
			if seen[year] {
				return nil, fmt.Errorf("ячейка %s: год %d повторяется — две колонки одного года дали бы две точки одного показателя за год",
					mtForecastCellAddress(sheet, j, headerRow), year)
			}
			seen[year] = true
			columns = append(columns, mtForecastColumn{Index: j, Year: year})
			continue
		}
		if strings.Contains(strings.ToLower(cell), "факт") || strings.Contains(strings.ToLower(cell), "оценка") {
			// Факт-колонка: факты живут в своих рядах (cbr_key_rate, rosstat_*), а у колонки дрейфует
			// пометка («2025 (факт)», в PDF — «(факт / оценка)») — спека §2.
			continue
		}
		return nil, fmt.Errorf("ячейка %s: шапка %q — ни год, ни факт-колонка (пометки «факт»/«оценка»): разбор не знает, что это за колонка",
			mtForecastCellAddress(sheet, j, headerRow), cell)
	}
	sort.SliceStable(columns, func(i, j int) bool { return columns[i].Year < columns[j].Year })
	return columns, nil
}

// mtForecastNumber — число границы: десятичная запятая источника → точка, приклеенный номер сноски
// у ровно 2-знаковой дроби отбрасывается (спека §3.3). Форму числа к этому моменту фиксируют
// регулярки разбора, поэтому от ParseFloat нужна только проверка ошибки (правило 6 AGENTS.md).
func mtForecastNumber(text string) (float64, error) {
	if mtForecastFootnoteRe.MatchString(text) {
		text = text[:len(text)-1]
	}
	value, err := strconv.ParseFloat(strings.ReplaceAll(text, ",", "."), 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", text, err)
	}
	return value, nil
}

// mtForecastBounds — границы прогноза из ячейки (спека §3.3): точка, диапазон, скобочные
// отрицательные, приклеенный номер сноски; точка даёт lo == hi. Незнакомая форма — ошибка, а не
// пропуск: молча пропущенная ячейка оставила бы (показатель, год) без прогноза, а неверно
// истолкованная — записала бы в таблицу чужое число. Обратный порядок границ — тоже ошибка: так
// видна инверсия (сноска снята не с той границы), но не сдвиг обеих — от него защищают наблюдение
// формата источника и спот-тесты фикстур (комментарий mtForecastFootnoteRe).
func mtForecastBounds(cell string) (float64, float64, error) {
	// Надстрочные цифры снимаются после базовой нормализации, а не до: в PDF-версии сноска печатается
	// надстрочной, и «14,5-14,6¹» — то же значение, что «14,5-14,6». Проверка пустоты — после обеих
	// операций: ячейку из одной надстрочной цифры (или из пробелов-двойников) источник напечатал, а
	// значения в ней нет.
	value := mtForecastNormalizeCell(mtForecastStripSuperscripts(mtForecastNormalizeCell(cell)))
	if value == "" {
		return 0, 0, fmt.Errorf("пустая прогнозная ячейка: значение, где ожидалось число (стандарт §6.4)")
	}

	if matches := mtForecastRangeRe.FindStringSubmatch(value); matches != nil {
		lo, err := mtForecastNumber(matches[1])
		if err != nil {
			return 0, 0, err
		}
		hi, err := mtForecastNumber(matches[2])
		if err != nil {
			return 0, 0, err
		}
		if lo > hi {
			return 0, 0, fmt.Errorf("границы диапазона %q в обратном порядке: lo %v > hi %v — значение искажено", value, lo, hi)
		}
		return lo, hi, nil
	}
	if matches := mtForecastPointRe.FindStringSubmatch(value); matches != nil {
		point, err := mtForecastNumber(matches[1])
		if err != nil {
			return 0, 0, err
		}
		return point, point, nil
	}
	return 0, 0, fmt.Errorf("значение %q не точка и не диапазон: форма ячейки незнакома (спека §3.3)", value)
}

// mtForecastMissing — показатели реестра, строк которых не нашлось в таблице: core и остальные
// отдельно. Решение спеки §3.3: пропажа core — ошибка (ради них пункт заведён), пропажа
// остальных — предупреждение с перечнем кодов (полнота набора важна, но не блокирует цепочку dagu).
func mtForecastMissing(seen map[string]bool) (core, rest []string) {
	for _, indicator := range mtForecastIndicators {
		if seen[indicator.Code] {
			continue
		}
		if indicator.Core {
			core = append(core, indicator.Code)
			continue
		}
		rest = append(rest, indicator.Code)
	}
	return core, rest
}

// mtForecastParseTabl1 разбирает лист «Табл 1» открытой книги: находит первую строку-показатель
// (якорь), выше неё — шапку годов, читает строки показателей до строки-сноски и отдаёт плоский
// список точек в порядке обхода — строки сверху вниз, внутри строки прогнозные годы по
// возрастанию (спека §3.3).
//
// Fail-fast (стандарт §6.4, спека §3.3): нет листа «Табл 1»; нет ни одной строки-показателя; выше
// неё нет шапки с годами; колонка шапки ни год, ни факт-колонка; дубль года; пустая или
// неразбираемая прогнозная ячейка; границы в обратном порядке; ни одной точки после разбора;
// отсутствие core-показателя. Предупреждение и продолжение (§6.6, §12.3): незнакомая подпись
// строки (новые строки источника — ожидаемое изменение) и пропажа не-core показателя.
//
// Ни сети, ни БД (стандарт §6.1): книга приходит открытой, значения читаются сырыми
// (RawCellValue, §6.2) — форматированная строка превратила бы «10,5» в текст формата и потеряла бы
// точность факт-ячеек.
func mtForecastParseTabl1(xlsx *excelize.File) ([]mtForecastPoint, error) {
	sheet, err := mtForecastSheetName(xlsx)
	if err != nil {
		return nil, err
	}
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("%s: чтение листа: %w", mtForecastContext(sheet), err)
	}
	first := mtForecastFirstIndicatorRow(rows)
	if first < 0 {
		return nil, fmt.Errorf("%s: ни одной строки-показателя: ни одна строка не несёт объявленной подписи реестра (строк в листе: %d) — таблица не то, чем кажется, или подписи источника переписаны",
			mtForecastContext(sheet), len(rows))
	}
	header := mtForecastHeaderRow(rows, first)
	if header < 0 {
		return nil, fmt.Errorf("%s: выше первой строки-показателя (строка %d) нет шапки с годами (ячеек-годов не менее %d) — разбор не знает прогнозных колонок",
			mtForecastContext(sheet), first+1, mtForecastHeaderYears)
	}
	columns, err := mtForecastColumns(sheet, header, rows[header])
	if err != nil {
		return nil, fmt.Errorf("%s, строка %d: %w", mtForecastContext(sheet), header+1, err)
	}

	seen := make(map[string]bool, len(mtForecastIndicators))
	var points []mtForecastPoint
	for i := first; i < len(rows); i++ {
		// Подпись строки — только из колонки A: остальные ячейки строки несут значения, и подпись,
		// случайно совпавшая с объявленной в колонке значения, привязала бы точки к чужой строке.
		label := mtForecastNormalizeLabel(mtForecastCell(rows[i], mtForecastLabelColumn))
		if label == "" {
			// Пустая строка (R24 между таблицей и сносками) — не строка таблицы.
			continue
		}
		if mtForecastEndRe.MatchString(label) {
			// Сноски источника (R25–R27): ниже таблицы, подписи у них нет — таблица кончилась.
			break
		}
		indicator, ok := mtForecastIndicatorByLabel(label)
		if !ok {
			log.Warnf("%s, строка %d: подпись %q не объявлена реестром показателей — строка пропущена, остальные разобраны (стандарт §6.6)",
				mtForecastContext(sheet), i+1, label)
			continue
		}
		seen[indicator.Code] = true
		for _, column := range columns {
			lo, hi, err := mtForecastBounds(mtForecastCell(rows[i], column.Index))
			if err != nil {
				return nil, fmt.Errorf("%s, ячейка %s (год %d): %w",
					mtForecastContext(sheet), mtForecastCellAddress(sheet, column.Index, i), column.Year, err)
			}
			points = append(points, mtForecastPoint{Indicator: indicator.Code, Year: column.Year, Lo: lo, Hi: hi})
		}
	}

	missingCore, missingRest := mtForecastMissing(seen)
	if len(missingCore) > 0 {
		return nil, fmt.Errorf("%s: нет строк core-показателей %s — ради них импортёр и заведён (спека §3.2): эти ряды обязаны быть в каждом выпуске",
			mtForecastContext(sheet), strings.Join(missingCore, ", "))
	}
	if len(missingRest) > 0 {
		log.Warnf("%s: строки показателей %s не найдены — подписи источника изменились? Ряды пропущены, остальные разобраны",
			mtForecastContext(sheet), strings.Join(missingRest, ", "))
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: разбор не дал ни одной точки — ни один объявленный показатель не разобран",
			mtForecastContext(sheet))
	}
	return points, nil
}

// mtForecastAppendPoints — ImportFunc одного выпуска: разбор открытой книги и запись точек (спека
// §3.5). Строка — колонки DDL (спека §3.1) в порядке: meeting_date, release_date, url, indicator,
// year, lo, hi. Разбор идёт целиком до цикла Append: при ошибке не записывается ни одной строки —
// частично записанный выпуск хуже неписанного (стандарт §6.4). Сбой Append не проглатывается:
// ошибка называет выпуск и точку, иначе строка терялась бы молча (стандарт §5.5).
func mtForecastAppendPoints(release mtForecastRelease) func(xlsx *excelize.File, batch driver.Batch) error {
	return func(xlsx *excelize.File, batch driver.Batch) error {
		points, err := mtForecastParseTabl1(xlsx)
		if err != nil {
			return err
		}
		for _, point := range points {
			if err := batch.Append(release.MeetingDate, release.ReleaseDate, release.URL,
				point.Indicator, point.Year, point.Lo, point.Hi); err != nil {
				return fmt.Errorf("%s: выпуск %s: точка (%s, %d): %w",
					mtForecastTable, release.ReleaseDate.Format("2006-01-02"), point.Indicator, point.Year, err)
			}
		}
		return nil
	}
}
