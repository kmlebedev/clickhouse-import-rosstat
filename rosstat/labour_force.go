package rosstat

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	// Рынок труда Росстата: рабочая сила, занятые и безработные (тыс. чел.) и три уровня (%)
	// https://rosstat.gov.ru/labour_force. Импортёр появится в следующей задаче; здесь — только
	// адресация и разбор.
	rosstatLabourForceTable = "rosstat_labour_force_mes"
	// labourForceSheet — лист «5» книги «Численность и состав рабочей силы в возрасте 15 лет и
	// старше по Российской Федерации». Лист ищется по имени (§6.3): номер листа в книге
	// сдвинулся бы от любого добавленного листа, а имя — часть разметки выгрузки.
	labourForceSheet = "5"
	// labourForcePage — страница источника. Ссылка на файл берётся с неё, а не собирается в
	// коде: имя файла стабильное, но адрес хранения Росстат менял.
	labourForcePage = "https://rosstat.gov.ru/labour_force"
	// labourForceLinkSelector — селектор ссылки на xlsx: константа рядом с импортёром (§4.5).
	// Дефис в имени файла — не опечатка разбора: у соседних выгрузок той же карточки стоит
	// подчёркивание (trud_1_15-72.xlsx), и общая маска по «trud» вернула бы не тот файл.
	labourForceLinkSelector = `a[href$="trud-1_15-s.xlsx"]`

	// Имена шести рядов — стабильные идентификаторы в каталоге (§6.8). Из заголовка листа они не
	// читаются: заголовок несёт годы и редакцию («…в возрасте 15 лет и старше…») и меняется при
	// каждом обновлении файла, тогда как ряд обязан остаться тем же.
	labourForceSeriesLabourForce   = "Рабочая сила"
	labourForceSeriesEmployed      = "Занятые"
	labourForceSeriesUnemployed    = "Безработные"
	labourForceSeriesParticipation = "Уровень участия в составе рабочей силы"
	labourForceSeriesEmployment    = "Уровень занятости"
	labourForceSeriesUnemployment  = "Уровень безработицы"
)

// labourForcePoint — точка ряда: имя ряда объявлено кодом (§6.8), дата — последний день
// отчётного месяца (значение относится к отчётному месяцу и лежит внутри него), значение —
// сырое значение ячейки (§6.2).
type labourForcePoint struct {
	series string
	date   time.Time
	value  float64
}

// labourForceSeries — пин ряда: стабильное имя (§6.8) и якорь его колонки в шапке (§6.3).
// Якоря «занятые» и «безработные» лежат строкой ниже верхней строки шапки: ячейка C5 «в том
// числе» раскрывается подписями C6/D6, поэтому колонка ищется по обеим строкам шапки.
type labourForceSeries struct {
	name   string
	anchor string
}

// labourForceSeriesPins — шесть рядов выгрузки и их якоря в шапке листа.
var labourForceSeriesPins = []labourForceSeries{
	{name: labourForceSeriesLabourForce, anchor: "Рабочая сила"},
	{name: labourForceSeriesEmployed, anchor: "занятые"},
	{name: labourForceSeriesUnemployed, anchor: "безработные"},
	{name: labourForceSeriesParticipation, anchor: "Уровень участия в составе рабочей силы, в %"},
	{name: labourForceSeriesEmployment, anchor: "Уровень занятости, в %"},
	{name: labourForceSeriesUnemployment, anchor: "Уровень безработицы, в %"},
}

// labourForceColumn — колонка ряда в шапке: имя ряда и индекс колонки (0-based).
type labourForceColumn struct {
	series string
	col    int
}

// labourForceFootnoteRe — хвостовые сноски источника на подписи: «январь1)», «октябрь1)2)».
// С октября 2022 г. источник ставит на каждый месяц обе сноски сразу, поэтому правило снимает
// группу сносок целиком, а не одну.
var labourForceFootnoteRe = regexp.MustCompile(`(?:\s*\d+\))+$`)

// labourForceLabel приводит подпись ячейки к сравнимому виду: сжимает все пробельные
// последовательности в один пробел (внутри ячейки «Уровень занятости, \nв %» — перевод строки) и
// снимает хвостовые сноски источника. util.MonthsToNum сноски не понимает (util/xls.go:25) и по
// «январь1)» месяц не находит, поэтому снятие сноски — часть разбора, а не косметика.
func labourForceLabel(raw string) string {
	label := strings.Join(strings.Fields(raw), " ")
	label = labourForceFootnoteRe.ReplaceAllString(label, "")
	return strings.TrimSpace(label)
}

// labourForceNormalize — форма подписи для сравнения с якорем: регистр заголовка источник менял
// от редакции к редакции, а подпись месяца приходит со сноской.
func labourForceNormalize(raw string) string {
	return strings.ToLower(labourForceLabel(raw))
}

// labourForceCell — ячейка строки по индексу колонки; выход за границу среза отдаёт пустую
// ячейку, а не панику (§6.5): GetRows обрезает хвостовые пустые ячейки, и короткая строка —
// обычное дело (у последнего месяца нового года данные ещё пусты).
func labourForceCell(row []string, col int) string {
	if col < 0 || col >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[col])
}

// labourForceHeaderRow ищет строку шапки по признаку (§6.3): первая строка, где встречается
// якорь первого ряда. Номер строки в разметке не пишется — сноски и вставленные строки сдвигают
// лист. -1 — шапки нет.
func labourForceHeaderRow(rows [][]string) int {
	anchor := labourForceNormalize(labourForceSeriesPins[0].anchor)
	for i, row := range rows {
		for _, cell := range row {
			if labourForceNormalize(cell) == anchor {
				return i
			}
		}
	}
	return -1
}

// labourForceColumns находит колонку каждого из шести рядов по нормализованному якорю в строке
// шапки и строке под ней и возвращает также последнюю строку шапки. Якорь обязан найтись ровно
// один раз (§6.4): два совпадения означают, что разметка изменилась, и выбрать из них одну —
// значит молча читать не тот показатель.
func labourForceColumns(rows [][]string, header int) ([]labourForceColumn, int, error) {
	columns := make([]labourForceColumn, 0, len(labourForceSeriesPins))
	headerEnd := header
	for _, pin := range labourForceSeriesPins {
		anchor := labourForceNormalize(pin.anchor)
		col := -1
		for i := header; i < len(rows) && i <= header+1; i++ {
			for c, cell := range rows[i] {
				if labourForceNormalize(cell) != anchor {
					continue
				}
				if col >= 0 && col != c {
					return nil, 0, fmt.Errorf("%s: лист %q: якорь %q найден дважды (колонки %d и %d)",
						rosstatLabourForceTable, labourForceSheet, pin.anchor, col+1, c+1)
				}
				col = c
				if i > headerEnd {
					headerEnd = i
				}
			}
		}
		if col < 0 {
			return nil, 0, fmt.Errorf("%s: лист %q: у ряда %q не найдена колонка (якорь %q) в строках шапки %d–%d",
				rosstatLabourForceTable, labourForceSheet, pin.name, pin.anchor, header+1, header+2)
		}
		columns = append(columns, labourForceColumn{series: pin.name, col: col})
	}
	return columns, headerEnd, nil
}

// labourForceYear — год строки-секции: подпись — единственная непустая ячейка строки и это
// четырёхзначное число («2017»…«2026»). Год переносится вперёд из найденной строки (§6.3), а не
// отсчитывается от первой: строк-секций в листе столько, сколько лет в выгрузке.
func labourForceYear(row []string) (int, bool) {
	label := labourForceCell(row, 0)
	if len(label) != 4 {
		return 0, false
	}
	for i := 1; i < len(row); i++ {
		if strings.TrimSpace(row[i]) != "" {
			return 0, false
		}
	}
	year, err := strconv.Atoi(label)
	if err != nil {
		return 0, false
	}
	return year, true
}

// labourForceValue разбирает ячейку значения. Ячейка приходит сырым текстом (§6.2): числовой
// формат выгрузки — представление, а не данные. NaN и бесконечность, которые принимает
// ParseFloat, отбрасываются, как у соседних рядов пакета: такой мусор уехал бы в витрину точкой
// ряда (§6.4).
func labourForceValue(cell string) (float64, error) {
	value, err := strconv.ParseFloat(cell, 64)
	if err != nil {
		return 0, fmt.Errorf("значение ячейки %q не число: %w", cell, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение ячейки %q не конечное число", cell)
	}
	return value, nil
}

// parseLabourForce читает лист «5» книги и отдаёт плоскую таблицу точек шести рядов. Разметка:
// строка шапки с якорем «Рабочая сила» (и строка под ней с «занятые»/«безработные»), ниже —
// строки-секции годов («2017»…«2026») и строки месяцев со сносками. Строка, чья подпись — не год
// и не месяц, замыкает таблицу: под данными листа лежит текст сносок источника, а не значения.
// Пустая ячейка значением не является и точкой не становится (§6.7) — у последних месяцев
// нового года данных ещё нет; нечисловая ячейка, наоборот, ошибка (§6.4), а не пропуск.
func parseLabourForce(xlsx *excelize.File) ([]labourForcePoint, error) {
	sheetFound := false
	for _, sheet := range xlsx.GetSheetList() {
		if sheet == labourForceSheet {
			sheetFound = true
			break
		}
	}
	if !sheetFound {
		return nil, fmt.Errorf("%s: в книге нет листа %q", rosstatLabourForceTable, labourForceSheet)
	}
	// Сырые значения ячеек (§6.2): форматированная строка отдала бы значение по формату ячейки.
	rows, err := xlsx.GetRows(labourForceSheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("%s: лист %q: %w", rosstatLabourForceTable, labourForceSheet, err)
	}
	header := labourForceHeaderRow(rows)
	if header < 0 {
		return nil, fmt.Errorf("%s: лист %q: строка шапки «%s» не найдена",
			rosstatLabourForceTable, labourForceSheet, labourForceSeriesPins[0].anchor)
	}
	columns, headerEnd, err := labourForceColumns(rows, header)
	if err != nil {
		return nil, err
	}
	var points []labourForcePoint
	year := 0
	month := time.Month(0)
	for i := headerEnd + 1; i < len(rows); i++ {
		row := rows[i]
		label := labourForceLabel(labourForceCell(row, 0))
		if label == "" {
			continue
		}
		if sectionYear, ok := labourForceYear(row); ok {
			year, month = sectionYear, 0
			continue
		}
		sectionMonth, ok := util.MonthsToNum[strings.ToLower(label)]
		if !ok {
			// Подпись не год и не месяц — текст сносок под таблицей: данные кончились.
			break
		}
		if year == 0 {
			return nil, fmt.Errorf("%s: лист %q, строка %d: месяц %q до строки-года",
				rosstatLabourForceTable, labourForceSheet, i+1, label)
		}
		if sectionMonth <= month {
			return nil, fmt.Errorf("%s: лист %q, строка %d: месяц %q идёт после месяца %q — месяцы обязаны идти по возрастанию",
				rosstatLabourForceTable, labourForceSheet, i+1, label, month)
		}
		month = sectionMonth
		for _, column := range columns {
			cell := labourForceCell(row, column.col)
			if cell == "" {
				// Пустая ячейка — не точка (§6.7): пропуск означает отсутствие значения, а не нуль.
				continue
			}
			value, err := labourForceValue(cell)
			if err != nil {
				return nil, fmt.Errorf("%s: лист %q, строка %d, колонка %d: %w",
					rosstatLabourForceTable, labourForceSheet, i+1, column.col+1, err)
			}
			points = append(points, labourForcePoint{
				series: column.series,
				// Последний день отчётного месяца, UTC: time.Date нормализует месяц 13 в январь
				// следующего года, а день 0 — в последний день предыдущего месяца.
				date:  time.Date(year, sectionMonth+1, 0, 0, 0, 0, 0, time.UTC),
				value: value,
			})
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: лист %q: разбор дал 0 точек", rosstatLabourForceTable, labourForceSheet)
	}
	return points, nil
}
