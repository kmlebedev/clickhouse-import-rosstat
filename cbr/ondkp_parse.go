package cbr

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Разбор прогнозной таблицы ОНДКП: заголовок раздела «в рамках базового сценария» →
// строка годов → привязка чисел к колонкам по позиции. Разведка и обоснование —
// research/2026-11-14-ondkp-extraction.md §3.1.
//
// Почему не по порядку токенов: у выпусков разное число колонок (2020–2024 — пять,
// 2025–2027 — четыре), а в одном PDF несколько таблиц с одинаковыми подписями строк
// (базовый сценарий, стрессовые, сопоставление винтажей). Заголовок раздела выбирает
// нужную таблицу, позиции годов привязывают значения к годам.
var (
	// ondkpSectionRe — заголовок раздела базового сценария. В 2020–2021 это «Основные
	// параметры прогноза Банка России в рамках базового сценария», в 2022–2027 —
	// «ПРОГНОЗ БАНКА РОССИИ В РАМКАХ БАЗОВОГО СЦЕНАРИЯ».
	ondkpSectionRe = regexp.MustCompile(`(?i)(?:ПРОГНОЗ|ПАРАМЕТРЫ\s+ПРОГНОЗА)\s+БАНКА\s+РОССИИ\s+В\s+РАМКАХ\s+БАЗОВОГО\s+СЦЕНАРИЯ`)
	ondkpYearRe    = regexp.MustCompile(`(20\d{2})`)

	ondkpKeyRateLabel   = regexp.MustCompile(`^\s*Ключевая ставка,\s*в среднем за год`)
	ondkpInflationLabel = regexp.MustCompile(`^\s*Инфляция,\s*в среднем за год`)
	// \b здесь не работает: в Go граница слова считается по ASCII, а «продукт» кончается
	// на кириллическую «т» — после неё границы нет, и строка ВВП не находилась вовсе.
	ondkpGDPLabel = regexp.MustCompile(`^\s*Валовой внутренний продукт(?:\s|$)`)

	// ondkpValueRe — значение или диапазон: «12,5», «12,5 – 13,5», «(-0,5)–0,5».
	// Надстрочная сноска («14,6¹») снимается до разбора, иначе число не парсится.
	ondkpValueRe = regexp.MustCompile(`\(?-?\d{1,2}(?:[.,]\d{1,2})?\)?(?:\s*[–-]\s*\(?-?\d{1,2}(?:[.,]\d{1,2})?\)?)?`)
)

// Индикаторы прогноза: имена колонок cbr_ondkp_forecast.
const (
	ondkpIndicatorKeyRate   = "key_rate"
	ondkpIndicatorInflation = "inflation"
	ondkpIndicatorGDP       = "gdp"
)

// ondkpForecastRow — одна прогнозная точка: выпуск, индикатор, год прогноза и границы.
// lo == hi — точка, иначе диапазон (ЦБ публикует ставку и ВВП диапазоном).
type ondkpForecastRow struct {
	releaseYear int
	indicator   string
	year        int
	lo, hi      float64
}

// ondkpStripFootnotes снимает надстрочные сноски и неразрывные пробелы: pdftotext
// приклеивает сноску к числу («14,6¹»).
func ondkpStripFootnotes(text string) string {
	return strings.NewReplacer("\u00a0", " ", "¹", "", "²", "", "³", "", "\t", " ").Replace(text)
}

// ondkpForecastRows разбирает текст выпуска в прогнозные точки за годы >= releaseYear
// (факт прошлых лет — не прогноз этого выпуска). Нет заголовка раздела — nil, nil: у
// выпусков 2000–2019 единой таблицы прогноза нет, и это ограничение корпуса, а не сбой.
func ondkpForecastRows(text []byte, releaseYear int) ([]ondkpForecastRow, error) {
	lines := strings.Split(ondkpStripFootnotes(string(text)), "\n")
	sectionIdx := -1
	for i, line := range lines {
		if ondkpSectionRe.MatchString(line) {
			sectionIdx = i
		}
	}
	if sectionIdx < 0 {
		return nil, nil
	}
	years, headerIdx, err := ondkpYearColumns(lines, sectionIdx)
	if err != nil {
		return nil, err
	}
	var rows []ondkpForecastRow
	limit := headerIdx + 40
	if limit > len(lines) {
		limit = len(lines)
	}
	for _, line := range lines[headerIdx+1 : limit] {
		var indicator string
		switch {
		case ondkpKeyRateLabel.MatchString(line):
			indicator = ondkpIndicatorKeyRate
		case ondkpInflationLabel.MatchString(line):
			indicator = ondkpIndicatorInflation
		case ondkpGDPLabel.MatchString(line):
			indicator = ondkpIndicatorGDP
		default:
			continue
		}
		for _, column := range ondkpColumnsOfLine(line, years) {
			if column.year < releaseYear {
				continue
			}
			rows = append(rows, ondkpForecastRow{
				releaseYear: releaseYear,
				indicator:   indicator,
				year:        column.year,
				lo:          column.lo,
				hi:          column.hi,
			})
		}
	}
	return rows, nil
}

// ondkpYearColumn — колонка года в строке заголовка: год и позиция начала в рунах.
type ondkpYearColumn struct {
	year int
	col  int
}

// ondkpYearColumns ищет строку годов после заголовка раздела: не меньше четырёх лет по
// возрастанию. Возвращает годы и индекс строки. Ограничение окна — 80 строк: заголовок
// раздела стоит вплотную к таблице, а дальше идут другие таблицы со своими годами.
func ondkpYearColumns(lines []string, from int) ([]ondkpYearColumn, int, error) {
	limit := from + 80
	if limit > len(lines) {
		limit = len(lines)
	}
	for j := from; j < limit; j++ {
		matches := ondkpYearRe.FindAllStringIndex(lines[j], -1)
		if len(matches) < 4 {
			continue
		}
		columns := make([]ondkpYearColumn, 0, len(matches))
		ascending := true
		for k, match := range matches {
			year, err := ondkpYearOf(lines[j][match[0]:match[1]])
			if err != nil {
				return nil, 0, fmt.Errorf("cbr_ondkp: строка годов %q: %w", lines[j], err)
			}
			if k > 0 && year <= columns[k-1].year {
				ascending = false
				break
			}
			columns = append(columns, ondkpYearColumn{year: year, col: utf8.RuneCountInString(lines[j][:match[0]])})
		}
		if ascending && len(columns) >= 4 {
			return columns, j, nil
		}
	}
	return nil, 0, fmt.Errorf("cbr_ondkp: после заголовка раздела (строка %d) не найдена строка годов — вёрстка изменилась", from+1)
}

// ondkpColTolerance — допуск привязки значения к колонке года, в рунах: значение
// относится к ближайшей левой колонке не дальше допуска. ЦБ печатает число с начала
// колонки, а длинный диапазон («12,5 – 13,5») залезает в соседнюю.
const ondkpColTolerance = 12

// ondkpCol — значение строки, привязанное к году.
type ondkpCol struct {
	year   int
	lo, hi float64
}

// ondkpColumnStep — типичный шаг колонок годов в рунах (12–14 в таблицах 2020–2027).
// По нему считается допуск привязки: половина шага.
func ondkpColumnStep(years []ondkpYearColumn) float64 {
	if len(years) < 2 {
		return float64(ondkpColTolerance)
	}
	sum := 0
	for i := 1; i < len(years); i++ {
		sum += years[i].col - years[i-1].col
	}
	return float64(sum) / float64(len(years)-1)
}

// ondkpColumnsOfLine раскладывает значения строки по колонкам годов. Привязка — по центру
// токена и центру колонки: числа в таблице ЦБ выровнены по правому краю, поэтому начало
// значения может стоять ЛЕВЕЕ начала года в заголовке, и привязка «по ближайшей левой
// колонке» уводит диапазон в предыдущий год (проверено на таблице 2027: «9,0 – 10,0»
// уезжало в 2027).
func ondkpColumnsOfLine(line string, years []ondkpYearColumn) []ondkpCol {
	if len(years) == 0 {
		return nil
	}
	step := ondkpColumnStep(years)
	tolerance := step / 2
	var out []ondkpCol
	for _, match := range ondkpValueRe.FindAllStringIndex(line, -1) {
		start := float64(utf8.RuneCountInString(line[:match[0]]))
		end := float64(utf8.RuneCountInString(line[:match[1]]))
		centre := (start + end) / 2
		best := -1
		bestDistance := tolerance
		for i, column := range years {
			// Центр колонки — середина четырёхзначного года.
			distance := centre - (float64(column.col) + 2)
			if distance < 0 {
				distance = -distance
			}
			if distance <= bestDistance {
				best = i
				bestDistance = distance
			}
		}
		if best < 0 {
			continue
		}
		lo, hi, ok := ondkpRange(line[match[0]:match[1]])
		if !ok {
			continue
		}
		out = append(out, ondkpCol{year: years[best].year, lo: lo, hi: hi})
	}
	return out
}

// ondkpRange разбирает значение или диапазон: «12,5» → 12.5/12.5, «12,5 – 13,5» →
// 12.5/13.5, «(-0,5)–0,5» → -0.5/0.5. Скобки и пробелы снимаются; порядок границ
// нормализуется.
func ondkpRange(raw string) (lo, hi float64, ok bool) {
	cleaned := strings.NewReplacer("–", "-", "—", "-", " ", "", "(", "", ")", "").Replace(strings.TrimSpace(raw))
	// Разделитель диапазона — дефис, а знак минуса — тоже дефис: режем по дефисам, не
	// стоящим в начале, и склеиваем обратно «-0,5» из «-» и «0,5».
	parts := strings.Split(cleaned, "-")
	values := make([]float64, 0, 2)
	for i := 0; i < len(parts); i++ {
		part := parts[i]
		if part == "" && i+1 < len(parts) {
			part = "-" + parts[i+1]
			i++
		} else if part == "" {
			continue
		}
		part = strings.ReplaceAll(part, ",", ".")
		value, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0, 0, false
		}
		values = append(values, value)
	}
	switch len(values) {
	case 0:
		return 0, 0, false
	case 1:
		return values[0], values[0], true
	default:
		if values[0] <= values[1] {
			return values[0], values[1], true
		}
		return values[1], values[0], true
	}
}
