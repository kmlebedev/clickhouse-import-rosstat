package cbr

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// mplPoint — нормализованная строка макропруденциального лимита. Одна строка
// v1/v2 — одна полоса с одним лимитом; одна строка v3 — правило с набором
// полос (ПДН + срок + взнос/LTV/сумма).
type mplPoint struct {
	segment         string
	loanClass       string
	periodFrom      time.Time
	periodTo        time.Time
	sourceFile      string
	sourceSheet     string
	rowNo           uint32
	subrowNo        string
	bandIdx         uint8
	loanKind        string
	pdnBand         string
	pdnMin          *float64
	pdnMax          *float64
	termBand        string
	termMin         *float64
	termMax         *float64
	downPaymentBand string
	downPaymentMin  *float64
	downPaymentMax  *float64
	ltvBand         string
	ltvMin          *float64
	ltvMax          *float64
	amountBand      string
	amountMin       *float64
	amountMax       *float64
	limitPct        float64
}

// mplBand — подпись и числовые границы полосы. nil — границы нет («x»/«-»/
// «более» без верхней границы).
type mplBand struct {
	label string
	min   *float64
	max   *float64
}

var (
	mplMoreThanRe = regexp.MustCompile(`^(?:более|свыше)\s+(-?\d+(?:[.,]\d+)?)$`)
	mplLessThanRe = regexp.MustCompile(`^до\s+(-?\d+(?:[.,]\d+)?)$`)
	mplIntervalRe = regexp.MustCompile(`^([\[(])\s*(-?\d+(?:[.,]\d+)?)\s*;\s*(-?\d+(?:[.,]\d+)?)\s*([\])])$`)
)

// cbrMPLParseBook — разбор книги лимитов. На входе открытая книга, файл
// страницы и карта границ старых форматов; внутри нет сети и БД (§6.1).
func cbrMPLParseBook(xlsx *excelize.File, file mplFile, ranges mplRanges) ([]mplPoint, error) {
	sheets := xlsx.GetSheetList()
	v3 := mplIsV3(sheets)
	var points []mplPoint
	for _, sheet := range sheets {
		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return nil, fmt.Errorf("%s: файл %s, лист %q: %w", cbrMPLTable, file.Name, sheet, err)
		}
		if !mplHasPeriodHeader(rows) {
			continue
		}
		var sheetPoints []mplPoint
		if v3 {
			sheetPoints, err = mplParseV3Sheet(rows, file, sheet)
		} else {
			sheetPoints, err = mplParseLegacySheet(rows, file, sheet, ranges)
		}
		if err != nil {
			return nil, err
		}
		points = append(points, sheetPoints...)
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: файл %s не дал ни одной строки лимитов", cbrMPLTable, file.Name)
	}
	return points, nil
}

func mplIsV3(sheets []string) bool {
	for _, sheet := range sheets {
		if strings.Contains(sheet, "Потребительские кредиты") ||
			strings.Contains(sheet, "Кредиты, обеспеченные ипотекой") ||
			strings.Contains(sheet, "Потребительские займы") {
			return true
		}
	}
	return false
}

func mplHasPeriodHeader(rows [][]string) bool {
	for _, row := range rows {
		for _, cell := range row {
			if strings.Contains(strings.ToLower(mplNorm(cell)), "период, в котором") {
				return true
			}
		}
	}
	return false
}

// mplParseV3Sheet — текущий формат (с 2025-07): строки-правила с набором полос.
// Родительская строка задаёт row_no, период, вид кредита, лимит и полосы;
// подстроки наследуют пустые поля и переопределяют непустые.
func mplParseV3Sheet(rows [][]string, file mplFile, sheet string) ([]mplPoint, error) {
	loanClass, err := mplV3LoanClass(sheet)
	if err != nil {
		return nil, fmt.Errorf("%s: файл %s, лист %q: %w", cbrMPLTable, file.Name, sheet, err)
	}
	header, sub, fromCol, toCol, err := mplLimitHeader(rows)
	if err != nil {
		return nil, fmt.Errorf("%s: файл %s, лист %q: %w", cbrMPLTable, file.Name, sheet, err)
	}
	rowNoCol := mplFindCol(rows[header], "номер", "строки")
	subrowCol := mplFindCol(rows[header], "номер", "подстроки")
	loanKindCol := mplFindCol(rows[header], "вид")
	limitCol := mplFindCol(rows[header], "макропруденциальный", "лимит")
	if rowNoCol < 0 || subrowCol < 0 || loanKindCol < 0 || limitCol < 0 {
		return nil, fmt.Errorf("%s: файл %s, лист %q: не найдены колонки номера строки/подстроки, вида или лимита",
			cbrMPLTable, file.Name, sheet)
	}
	if fromCol < 0 || toCol < 0 {
		return nil, fmt.Errorf("%s: файл %s, лист %q: не найдены колонки периода",
			cbrMPLTable, file.Name, sheet)
	}
	pdnCol := mplFindCol(rows[header], "пдн")
	termCol := mplFindCol(rows[header], "срок")
	downCol := mplFindAnyCol(rows[header], "первоначальн", "взнос")
	ltvCol := mplFindAnyCol(rows[header], "соотношен", "задолжен")
	amountCol := mplFindAnyCol(rows[header], "сумм")
	if pdnCol < 0 || termCol < 0 {
		return nil, fmt.Errorf("%s: файл %s, лист %q: не найдены колонки ПДН и срока",
			cbrMPLTable, file.Name, sheet)
	}
	switch loanClass {
	case "mortgage":
		if downCol < 0 || ltvCol < 0 {
			return nil, fmt.Errorf("%s: файл %s, лист %q: не найдены колонки первоначального взноса/LTV",
				cbrMPLTable, file.Name, sheet)
		}
	case "consumer_loan":
		if amountCol < 0 {
			return nil, fmt.Errorf("%s: файл %s, лист %q: не найдена колонка суммы займа",
				cbrMPLTable, file.Name, sheet)
		}
	}

	var points []mplPoint
	var state, parent mplV3State
	for i := sub + 1; i < len(rows); i++ {
		row := rows[i]
		rowNoRaw := strings.TrimSpace(ccybCell(row, rowNoCol))
		subrow := strings.TrimSpace(ccybCell(row, subrowCol))
		if rowNoRaw == "" && subrow == "" {
			break
		}
		isParent := rowNoRaw != ""
		var pointState mplV3State
		if isParent {
			rowNo, convErr := strconv.ParseUint(rowNoRaw, 10, 32)
			if convErr != nil {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: номер строки %q не число",
					cbrMPLTable, file.Name, sheet, i+1, rowNoRaw)
			}
			state = mplV3State{rowNo: uint32(rowNo)}
			rawFrom := strings.TrimSpace(ccybCell(row, fromCol))
			if rawFrom == "" {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: нет начала периода",
					cbrMPLTable, file.Name, sheet, i+1)
			}
			from, ok := ccybExcelDate(rawFrom)
			if !ok {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: начало периода не дата",
					cbrMPLTable, file.Name, sheet, i+1)
			}
			state.from = from
			if rawTo := strings.TrimSpace(ccybCell(row, toCol)); rawTo != "" {
				to, ok := ccybExcelDate(rawTo)
				if !ok {
					return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: конец периода не дата",
						cbrMPLTable, file.Name, sheet, i+1)
				}
				state.to = to
			} else {
				state.to = mustRiskAddonOpenEnded()
			}
			if err = mplV3Override(&state, row, loanKindCol, limitCol, pdnCol, termCol, downCol, ltvCol, amountCol); err != nil {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: %w",
					cbrMPLTable, file.Name, sheet, i+1, err)
			}
			if !state.hasLimit {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: нет лимита",
					cbrMPLTable, file.Name, sheet, i+1)
			}
			parent = state
			pointState = state
		} else {
			if parent.rowNo == 0 {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: подстрока без родительской строки",
					cbrMPLTable, file.Name, sheet, i+1)
			}
			// Подстрока наследует условия родителя, а не предыдущей подстроки:
			// pointState начинается со снимка parent, непустые ячейки переопределяют.
			pointState = parent
			if err = mplV3Override(&pointState, row, loanKindCol, limitCol, pdnCol, termCol, downCol, ltvCol, amountCol); err != nil {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: %w",
					cbrMPLTable, file.Name, sheet, i+1, err)
			}
			if !pointState.hasLimit {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: нет лимита",
					cbrMPLTable, file.Name, sheet, i+1)
			}
		}

		// Неизвестная полоса уже залогирована в mplParseBandText: подпись
		// сохраняется как в источнике, числовые границы остаются nil.
		pdn, _ := mplParseBandText(pointState.pdn, file, sheet, i+1)
		term, _ := mplParseBandText(pointState.term, file, sheet, i+1)
		down, _ := mplParseBandText(pointState.downPayment, file, sheet, i+1)
		ltv, _ := mplParseBandText(pointState.ltv, file, sheet, i+1)
		amount, _ := mplParseBandText(pointState.amount, file, sheet, i+1)
		points = append(points, mplPoint{
			segment:         file.Segment,
			loanClass:       loanClass,
			periodFrom:      pointState.from,
			periodTo:        pointState.to,
			sourceFile:      file.Name,
			sourceSheet:     sheet,
			rowNo:           pointState.rowNo,
			subrowNo:        subrow,
			bandIdx:         0,
			loanKind:        pointState.loanKind,
			pdnBand:         pdn.label,
			pdnMin:          pdn.min,
			pdnMax:          pdn.max,
			termBand:        term.label,
			termMin:         term.min,
			termMax:         term.max,
			downPaymentBand: down.label,
			downPaymentMin:  down.min,
			downPaymentMax:  down.max,
			ltvBand:         ltv.label,
			ltvMin:          ltv.min,
			ltvMax:          ltv.max,
			amountBand:      amount.label,
			amountMin:       amount.min,
			amountMax:       amount.max,
			limitPct:        pointState.limit,
		})
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: файл %s, лист %q: лимиты не разобраны",
			cbrMPLTable, file.Name, sheet)
	}
	return points, nil
}

type mplV3State struct {
	rowNo       uint32
	from, to    time.Time
	loanKind    string
	limit       float64
	hasLimit    bool
	pdn         string
	term        string
	downPayment string
	ltv         string
	amount      string
}

func mplV3Override(state *mplV3State, row []string, loanKindCol, limitCol, pdnCol, termCol, downCol, ltvCol, amountCol int) error {
	if loanKindCol >= 0 {
		if v := strings.TrimSpace(ccybCell(row, loanKindCol)); v != "" {
			state.loanKind = v
		}
	}
	if limitCol >= 0 {
		if v := strings.TrimSpace(ccybCell(row, limitCol)); v != "" {
			parsed, err := mplParseFloat(v)
			if err != nil {
				return fmt.Errorf("лимит: %w", err)
			}
			state.limit = parsed
			state.hasLimit = true
		}
	}
	set := func(col int, dst *string) {
		if col < 0 {
			return
		}
		if v := strings.TrimSpace(ccybCell(row, col)); v != "" {
			*dst = v
		}
	}
	set(pdnCol, &state.pdn)
	set(termCol, &state.term)
	set(downCol, &state.downPayment)
	set(ltvCol, &state.ltv)
	set(amountCol, &state.amount)
	return nil
}

// mplParseLegacySheet — v1/v2 (2023-01 … 2025-06): каждая полоса — отдельный
// лимит; границы полос берутся из Limits_ranges_*. Значения лимитов в источнике
// доли (0.25 = 25 %), поэтому нормализуются в проценты.
func mplParseLegacySheet(rows [][]string, file mplFile, sheet string, ranges mplRanges) ([]mplPoint, error) {
	loanClass, dimension, err := mplLegacyClass(sheet)
	if err != nil {
		return nil, fmt.Errorf("%s: файл %s, лист %q: %w", cbrMPLTable, file.Name, sheet, err)
	}
	header, sub, fromCol, toCol, err := mplLimitHeader(rows)
	if err != nil {
		return nil, fmt.Errorf("%s: файл %s, лист %q: %w", cbrMPLTable, file.Name, sheet, err)
	}
	limitTypeCol := mplFindCol(rows[header], "тип", "макропруденциального", "лимита")
	if limitTypeCol < 0 {
		limitTypeCol = mplLastNonEmptyCol(rows[header])
	}
	if limitTypeCol < 0 {
		return nil, fmt.Errorf("%s: файл %s, лист %q: не найдена колонка типа лимита",
			cbrMPLTable, file.Name, sheet)
	}
	var bandCols []int
	for j := toCol + 1; j < limitTypeCol; j++ {
		if strings.TrimSpace(ccybCell(rows[sub], j)) != "" {
			bandCols = append(bandCols, j)
		}
	}
	if len(bandCols) == 0 {
		return nil, fmt.Errorf("%s: файл %s, лист %q: нет колонок полос",
			cbrMPLTable, file.Name, sheet)
	}

	var points []mplPoint
	for i := sub + 1; i < len(rows); i++ {
		row := rows[i]
		rowNoRaw := strings.TrimSpace(ccybCell(row, 0))
		if rowNoRaw == "" {
			break
		}
		rowNo, convErr := strconv.ParseUint(rowNoRaw, 10, 32)
		if convErr != nil {
			break
		}
		from, ok := ccybExcelDate(strings.TrimSpace(ccybCell(row, fromCol)))
		if !ok {
			return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: начало периода не дата",
				cbrMPLTable, file.Name, sheet, i+1)
		}
		to := mustRiskAddonOpenEnded()
		if raw := strings.TrimSpace(ccybCell(row, toCol)); raw != "" {
			parsed, ok := ccybExcelDate(raw)
			if !ok {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: конец периода не дата",
					cbrMPLTable, file.Name, sheet, i+1)
			}
			to = parsed
		}
		loanKind := strings.TrimSpace(ccybCell(row, limitTypeCol))
		for idx, col := range bandCols {
			raw := strings.TrimSpace(ccybCell(row, col))
			if raw == "" || strings.EqualFold(raw, "x") || raw == "х" || raw == "-" {
				continue
			}
			limit, parseErr := mplParseFloat(raw)
			if parseErr != nil {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d, колонка %d: %w",
					cbrMPLTable, file.Name, sheet, i+1, col+1, parseErr)
			}
			if limit <= 1 {
				limit *= 100
			} else {
				log.Warnf("%s: файл %s, лист %q, строка %d: лимит %v > 1 — считаю уже процентами",
					cbrMPLTable, file.Name, sheet, i+1, limit)
			}
			band, bandErr := mplRangeBand(ranges, file.Segment, dimension, from, to, idx,
				len(bandCols), strings.TrimSpace(ccybCell(rows[sub], col)))
			if bandErr != nil {
				return nil, fmt.Errorf("%s: файл %s, лист %q, строка %d: %w",
					cbrMPLTable, file.Name, sheet, i+1, bandErr)
			}
			point := mplPoint{
				segment:     file.Segment,
				loanClass:   loanClass,
				periodFrom:  from,
				periodTo:    to,
				sourceFile:  file.Name,
				sourceSheet: sheet,
				rowNo:       uint32(rowNo),
				bandIdx:     uint8(idx),
				loanKind:    loanKind,
				limitPct:    limit,
			}
			switch dimension {
			case "pdn":
				point.pdnBand, point.pdnMin, point.pdnMax = band.label, band.min, band.max
			case "term":
				point.termBand, point.termMin, point.termMax = band.label, band.min, band.max
			}
			points = append(points, point)
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: файл %s, лист %q: лимиты не разобраны",
			cbrMPLTable, file.Name, sheet)
	}
	return points, nil
}

// mplLimitHeader — строка заголовка колонок и строка «С/По». Заголовок ищется
// по признаку «Период, в котором», а не по номеру строки: в v3 это строка 1,
// в v1/v2 — 2.
func mplLimitHeader(rows [][]string) (header, sub, fromCol, toCol int, err error) {
	header = -1
	for i, row := range rows {
		for _, cell := range row {
			if strings.Contains(strings.ToLower(mplNorm(cell)), "период, в котором") {
				header = i
				break
			}
		}
		if header >= 0 {
			break
		}
	}
	sub, fromCol, toCol, _, err = mplTableHeader(rows)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if header < 0 {
		header = sub - 1
	}
	if header < 0 {
		return 0, 0, 0, 0, fmt.Errorf("не найдена строка заголовка колонок")
	}
	return header, sub, fromCol, toCol, nil
}

func mplV3LoanClass(sheet string) (string, error) {
	switch {
	case strings.Contains(sheet, "Потребительские кредиты"):
		return "consumer", nil
	case strings.Contains(sheet, "Кредиты, обеспеченные ипотекой"):
		return "mortgage", nil
	case strings.Contains(sheet, "Потребительские займы"):
		return "consumer_loan", nil
	default:
		return "", fmt.Errorf("неизвестный лист формата v3: %q", sheet)
	}
}

func mplLegacyClass(sheet string) (loanClass, dimension string, err error) {
	switch {
	case strings.Contains(sheet, "С лимитом кредитования"):
		loanClass = "consumer_with_limit"
	case strings.Contains(sheet, "Иные кредиты"):
		loanClass = "consumer_other"
	default:
		return "", "", fmt.Errorf("неизвестный лист формата v1/v2: %q", sheet)
	}
	switch {
	case strings.Contains(sheet, "ПДН"):
		dimension = "pdn"
	case strings.Contains(strings.ToLower(sheet), "срок"):
		dimension = "term"
	default:
		return "", "", fmt.Errorf("неизвестное измерение листа: %q", sheet)
	}
	return loanClass, dimension, nil
}

func mplFindCol(row []string, parts ...string) int {
	for j, cell := range row {
		cell = strings.ToLower(mplNorm(cell))
		ok := true
		for _, part := range parts {
			if !strings.Contains(cell, strings.ToLower(part)) {
				ok = false
				break
			}
		}
		if ok {
			return j
		}
	}
	return -1
}

// mplFindAnyCol — колонка, в заголовке которой есть хотя бы одна из основ:
// падежи источника («Первоначальный взнос» / «первоначального взноса»,
// «Соотношение задолженности» / «соотношения долга») не должны ломать поиск.
func mplFindAnyCol(row []string, parts ...string) int {
	for j, cell := range row {
		cell = strings.ToLower(mplNorm(cell))
		for _, part := range parts {
			if strings.Contains(cell, strings.ToLower(part)) {
				return j
			}
		}
	}
	return -1
}

func mplLastNonEmptyCol(row []string) int {
	for j := len(row) - 1; j >= 0; j-- {
		if strings.TrimSpace(row[j]) != "" {
			return j
		}
	}
	return -1
}

func mplNorm(s string) string {
	s = strings.ReplaceAll(s, "\u00a0", " ")
	return strings.Join(strings.Fields(s), " ")
}

func mplParseFloat(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	v, err := strconv.ParseFloat(strings.ReplaceAll(raw, ",", "."), 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число", raw)
	}
	return v, nil
}

// mplParseBandText — текстовая полоса v3: «(50; 80]», «[0; 50]», «более 80»,
// «x»/«х»/«-». Неизвестная подпись сохраняется как есть, числовые границы nil:
// это дрейф формата, а не повод валить весь прогон (стандарт §6.6).
func mplParseBandText(raw string, file mplFile, sheet string, row int) (mplBand, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || strings.EqualFold(s, "x") || s == "х" || s == "-" {
		return mplBand{}, true
	}
	lower := strings.ToLower(s)
	if m := mplMoreThanRe.FindStringSubmatch(lower); m != nil {
		v, err := mplParseFloat(m[1])
		if err == nil {
			return mplBand{label: s, min: &v}, true
		}
	}
	if m := mplLessThanRe.FindStringSubmatch(lower); m != nil {
		v, err := mplParseFloat(m[1])
		if err == nil {
			return mplBand{label: s, max: &v}, true
		}
	}
	if m := mplIntervalRe.FindStringSubmatch(s); m != nil {
		lo, errLo := mplParseFloat(m[2])
		hi, errHi := mplParseFloat(m[3])
		if errLo == nil && errHi == nil {
			return mplBand{label: s, min: &lo, max: &hi}, true
		}
	}
	if v, err := mplParseFloat(s); err == nil {
		return mplBand{label: s, min: &v, max: &v}, true
	}
	log.Warnf("%s: файл %s, лист %q, строка %d: полоса %q не распознана — границы nil",
		cbrMPLTable, file.Name, sheet, row, s)
	return mplBand{label: s}, false
}

// mplRangeBand — полоса v1/v2 из карты границ: 2 полосы → (a,b], (b,∞);
// 4 полосы → (a,b], (b,c], (c,d], (d,∞). Отсутствие периода в карте — ошибка:
// без границ старый формат не читается.
func mplRangeBand(ranges mplRanges, segment, dimension string, from, to time.Time,
	idx, n int, fallback string) (mplBand, error) {
	key := mplRangeKey{segment: segment, dimension: dimension, from: from, to: to}
	r, ok := ranges[key]
	if !ok {
		return mplBand{}, fmt.Errorf("нет границ %s/%s за %s..%s",
			segment, dimension, from.Format("2006-01-02"), to.Format("2006-01-02"))
	}
	var min, max *float64
	switch n {
	case 2:
		switch idx {
		case 0:
			min, max = r.a, r.b
		case 1:
			min, max = r.b, nil
		default:
			return mplBand{}, fmt.Errorf("полоса %d при 2 колонках", idx)
		}
	case 4:
		switch idx {
		case 0:
			min, max = r.a, r.b
		case 1:
			min, max = r.b, r.c
		case 2:
			min, max = r.c, r.d
		case 3:
			min, max = r.d, nil
		default:
			return mplBand{}, fmt.Errorf("полоса %d при 4 колонках", idx)
		}
	default:
		return mplBand{}, fmt.Errorf("неизвестное число полос: %d", n)
	}
	return mplBand{label: mplBandLabel(min, max, fallback), min: min, max: max}, nil
}

func mplBandLabel(min, max *float64, fallback string) string {
	switch {
	case min == nil && max == nil:
		return fallback
	case min == nil:
		return "до " + mplFormatFloat(*max)
	case max == nil:
		return "более " + mplFormatFloat(*min)
	default:
		return "(" + mplFormatFloat(*min) + "; " + mplFormatFloat(*max) + "]"
	}
}

func mplFormatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// mplNullable — *float64 → nil/float64 для driver.Batch.Append: драйвер не
// принимает указатель в Nullable(Float64), а nil — принимает.
func mplNullable(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}
