package cbr

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// mplRangeKey — период и измерение границ: один и тот же период встречается
// в листах ПДН и Срок, поэтому измерение — часть ключа.
type mplRangeKey struct {
	segment   string
	dimension string
	from      time.Time
	to        time.Time
}

// mplRange — границы а/б/в/г. nil — источник написал «х» (границы нет).
type mplRange struct {
	a, b, c, d *float64
}

type mplRanges map[mplRangeKey]mplRange

// cbrMPLParseRanges — разбор файла границ. В v1 границ две (а, б), в v2 —
// четыре (а, б, в, г); парсер читает столько, сколько объявлено в шапке,
// и не привязывается к числу колонок.
func cbrMPLParseRanges(xlsx *excelize.File, file mplFile) (mplRanges, error) {
	out := make(mplRanges)
	for _, sheet := range xlsx.GetSheetList() {
		dimension := ""
		switch {
		case strings.Contains(sheet, "ПДН"):
			dimension = "pdn"
		case strings.Contains(strings.ToLower(sheet), "срок"):
			dimension = "term"
		default:
			continue
		}
		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s: файл %s, лист %q: %w", cbrMPLTable, file.Name, sheet, err)
		}
		sub, fromCol, toCol, boundaryCols, err := mplTableHeader(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: файл %s, лист %q: %w", cbrMPLTable, file.Name, sheet, err)
		}
		count := 0
		for i := sub + 1; i < len(rows); i++ {
			row := rows[i]
			rowNo := strings.TrimSpace(ccybCell(row, 0))
			if rowNo == "" {
				break
			}
			if _, convErr := strconv.Atoi(rowNo); convErr != nil {
				break
			}
			from, ok := ccybExcelDate(strings.TrimSpace(ccybCell(row, fromCol)))
			if !ok {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: начало периода не дата",
					cbrMPLTable, file.Name, sheet, i+1)
			}
			to := mustRiskAddonOpenEnded()
			if rawTo := strings.TrimSpace(ccybCell(row, toCol)); rawTo != "" {
				parsed, ok := ccybExcelDate(rawTo)
				if !ok {
					return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: конец периода не дата",
						cbrMPLTable, file.Name, sheet, i+1)
				}
				to = parsed
			}
			bounds := make([]*float64, 0, len(boundaryCols))
			for _, col := range boundaryCols {
				v, boundErr := mplBoundary(ccybCell(row, col))
				if boundErr != nil {
					return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d, колонка %d: %w",
						cbrMPLTable, file.Name, sheet, i+1, col+1, boundErr)
				}
				bounds = append(bounds, v)
			}
			r := mplRange{}
			if len(bounds) > 0 {
				r.a = bounds[0]
			}
			if len(bounds) > 1 {
				r.b = bounds[1]
			}
			if len(bounds) > 2 {
				r.c = bounds[2]
			}
			if len(bounds) > 3 {
				r.d = bounds[3]
			}
			out[mplRangeKey{segment: file.Segment, dimension: dimension, from: from, to: to}] = r
			count++
		}
		if count == 0 {
			return nil, fmt.Errorf("%s: файл %s, лист %q: границы не разобраны",
				cbrMPLTable, file.Name, sheet)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: файл %s не дал ни одной строки границ", cbrMPLTable, file.Name)
	}
	return out, nil
}

// mplTableHeader — строка подзаголовка «С/По» и колонки: начало периода,
// конец периода и границы после «По». Ищется по признаку, а не по номеру
// строки: в v1/v2 шапка на 2–3-й строке, в v3 — на 2-й.
func mplTableHeader(rows [][]string) (sub, fromCol, toCol int, boundaryCols []int, err error) {
	for i, row := range rows {
		from, to := -1, -1
		for j, cell := range row {
			switch strings.TrimSpace(cell) {
			case "С":
				if from < 0 {
					from = j
				}
			case "По":
				if to < 0 {
					to = j
				}
			}
		}
		if from >= 0 && to >= 0 {
			sub, fromCol, toCol = i, from, to
			for j := to + 1; j < len(row); j++ {
				if strings.TrimSpace(row[j]) != "" {
					boundaryCols = append(boundaryCols, j)
				}
			}
			return sub, fromCol, toCol, boundaryCols, nil
		}
	}
	return 0, 0, 0, nil, fmt.Errorf("не найдена строка «С/По»")
}

func mplBoundary(raw string) (*float64, error) {
	raw = strings.TrimSpace(raw)
	switch strings.ToLower(raw) {
	case "", "x", "х", "-":
		return nil, nil
	}
	v, err := strconv.ParseFloat(strings.ReplaceAll(raw, ",", "."), 64)
	if err != nil {
		return nil, fmt.Errorf("граница %q не число", raw)
	}
	return &v, nil
}
