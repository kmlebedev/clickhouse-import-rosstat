package polyus

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Числа в таблице могут выглядеть так:
//
//	8,723        (английский релиз: запятая — разделитель тысяч)
//	38,945
//	1 287        (русский релиз: пробел — разделитель тысяч)
//	3.99
//	0,87
//	38%
//	(16%)
//	(12)
//	-
//
// Скобки интерпретируются как отрицательное значение.
var valueTokenRegexp = regexp.MustCompile(
	`^\(?-?(?:\d{1,3}(?:(?:,|\s)\d{3})+|\d+)(?:[.,]\d+)?%?\)?$|^-$`,
)

// parseKPIPage разбирает извлечённый pdftotext текст одной страницы отчёта:
// находит строку-шапку с периодами (метрика "period"), затем каждую строку
// метрики и раскладывает её числовые токены по колонкам-периодам.
//
// page — номер страницы PDF, к которой относится textPath; он не вычисляется,
// а попадает в записи как есть.
func parseKPIPage(
	textPath string,
	sourceURL string,
	page int,
) (records []MetricRecord, err error) {
	file, err := os.Open(textPath)
	if err != nil {
		return nil, fmt.Errorf("open extracted page: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("close extracted page %s: %v", textPath, closeErr)
		}
	}()

	foundMetrics := make(map[string]bool)

	scanner := bufio.NewScanner(file)

	// Некоторые PDF-строки могут быть длиннее стандартных 64 KiB.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var periodColumns []PeriodColumn

	// Строки, не опознанные ни как шапка периодов, ни как метрика с числами,
	// пропускаются: в шапку, подписи и сноски попадает большинство строк
	// страницы. Счётчик ведётся, чтобы пропуски не были молчаливыми.
	skipped := 0

	for scanner.Scan() {
		line := normalizeLine(scanner.Text())

		if line == "" {
			continue
		}

		definition, remainder, found := matchMetric(line)
		if !found {
			skipped++
			continue
		}

		if foundMetrics[definition.Name] {
			// Защита от повторного совпадения в сносках.
			continue
		}

		if definition.Name == "period" {
			periodColumns = scanPeriodColumns(remainder)
			continue
		}

		tokens := extractValueTokens(remainder)

		foundMetrics[definition.Name] = true
		for _, column := range periodColumns {
			if column.TokenIndex >= len(tokens) {
				continue
			}

			value, err := parseNumericToken(tokens[column.TokenIndex])
			if err != nil {
				return nil, fmt.Errorf(
					"metric %s, period %s, token %q: %w",
					definition.Name,
					column.Period,
					tokens[column.TokenIndex],
					err,
				)
			}
			if value == nil {
				// Токен распознан, но значения нет ("-", "n/a").
				continue
			}

			records = append(records, MetricRecord{
				Company:    "PJSC Polyus",
				Metric:     definition.Name,
				Period:     column.Period,
				PeriodType: column.PeriodType,
				Value:      *value,
				Unit:       definition.Unit,
				SourceURL:  sourceURL,
				SourcePage: page,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan page: %w", err)
	}

	log.Printf("polyus KPI %s p.%d: %d records, %d non-metric lines skipped", sourceURL, page, len(records), skipped)

	return records, nil
}

// scanPeriodColumns разбирает шапку таблицы в список колонок-периодов.
//
// Шапка режется на периоды скользящим окном: форма периода занимает от одного
// до трёх слов («2026FY», «1H 2026», «1 п/г 2026»), поэтому одиночный токен
// недостаточен, а разбор всей строки целиком склеил бы соседние периоды в один
// (parsePeriod — матчер, а не строгий валидатор). Окно собирается по началу
// токена: как только parsePeriod разобрал префикс, окно закрывается.
//
// Индекс колонки — её номер среди найденных периодов, а не номер слова: он
// сравнивается с индексом числового токена в строке метрики, куда слова
// заголовка не попадают.
func scanPeriodColumns(header string) []PeriodColumn {
	tokens := strings.Fields(header)
	columns := make([]PeriodColumn, 0, 4)

	// maxPeriodTokens — самая длинная форма периода: «1 п/г 2026».
	const maxPeriodTokens = 3

	for i := 0; i < len(tokens); {
		info, width, err := matchPeriodAt(tokens, i, maxPeriodTokens)
		if err != nil {
			i++
			continue
		}

		columns = append(columns, PeriodColumn{len(columns), info.Period, info.Type})
		i += width
	}

	return columns
}

// matchPeriodAt ищет самый длинный префикс токенов, начинающийся с tokens[start],
// который parsePeriod разбирает целиком: от tokens[start:start+n] для n от
// maxPeriodTokens до 1. Возвращает ширину окна в токенах.
//
// Длинное окно проверяется первым, иначе «1 п/г 2026» распалось бы на «1»
// (ошибка разбора) и «2026» (голый год FY), и полугодие превратилось бы в год.
func matchPeriodAt(tokens []string, start, maxPeriodTokens int) (PeriodInfo, int, error) {
	for width := maxPeriodTokens; width > 0; width-- {
		if start+width > len(tokens) {
			continue
		}

		info, err := parsePeriod(strings.Join(tokens[start:start+width], " "))
		if err != nil {
			continue
		}

		return info, width, nil
	}

	return PeriodInfo{}, 0, fmt.Errorf("no period at token %d", start)
}

// matchMetric ищет метрику, строка которой начинается с одного из её
// префиксов, и возвращает остаток строки с числами.
//
// Строка приходит уже нормализованной, то есть без ведущих пробелов: русские
// релизы печатают метки с отступом, и без обрезки ни одна метрика не
// опозналась бы.
//
// Метка и числа могут оказаться в одной строке без разделителя (сноска-цифра
// приклеена к последней скобке: «...(тыс. унций)2   1 287»), поэтому после
// точного сравнения делается вторая, «мягкая» попытка с отброшенной сноской.
//
// Префиксы перебираются от длинных к коротким, иначе, например, "CAPEX"
// перехватывал бы "Stripping CAPEX".
func matchMetric(line string) (MetricDefinition, string, bool) {
	for _, definition := range metricsSortedByPrefixLength() {
		for _, prefix := range definition.Prefix {
			if !strings.HasPrefix(line, prefix) {
				continue
			}

			if remainder, ok := splitLabelAndValues(line[len(prefix):]); ok {
				return definition, remainder, true
			}
		}
	}

	return MetricDefinition{}, "", false
}

// splitLabelAndValues отделяет остаток строки после префикса от чисел.
//
// Точный разбор — по первому пробелу после метки. Если пробела нет, остаток
// слипся со сноской-цифрой («...(тыс. унций)2»): сноска отбрасывается, а
// разделителем считается следующий за ней пробел. Если разделителя нет вовсе,
// строку пропускаем — именно так раньше падал весь разбор.
//
// Заодно отбрасывается пробел, попавший в начало остатка: при обрезке метки
// по короткому префиксу («Капитальные затраты» против «Капитальные затраты по
// вскрышным работам») он иначе попадёт в первый числовой токен и разорвёт его.
func splitLabelAndValues(rest string) (string, bool) {
	if spaceIndex := strings.Index(rest, " "); spaceIndex >= 0 {
		return strings.TrimLeft(rest[spaceIndex:], " "), true
	}

	core := strings.TrimRight(rest, "\u00a0\u2007\u202f")
	if core == "" {
		return "", false
	}

	// Сноска стоит вплотную к последней скобке метки, то есть после цифр идёт
	// конец строки, а перед ними — не цифра: иначе цифра принадлежит метке
	// («EBITDA, x5») и разбирать нечего.
	if !isASCIIDigit(core[len(core)-1]) {
		return "", false
	}
	if end := len(core) - 1; end > 0 && isASCIIDigit(core[end-1]) {
		return "", false
	}
	if !utf8.RuneStart(core[len(core)-1]) {
		return "", false
	}

	spaceIndex := strings.Index(core[:len(core)-1], " ")
	if spaceIndex < 0 {
		return "", false
	}

	return strings.TrimLeft(core[spaceIndex:], " "), true
}

func isASCIIDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func metricsSortedByPrefixLength() []MetricDefinition {
	result := append([]MetricDefinition(nil), metrics...)

	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if len(result[j].Prefix) > len(result[i].Prefix) {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}

// normalizeLine приводит строку PDF к виду, пригодному для сопоставления:
// все виды пробелов (включая неразрывные) становятся обычными, тире
// унифицируются, управляющие символы выкидываются. Ведущие и хвостовые
// пробелы обрезаются — русские релизы печатают метки с отступом.
//
// Внутренние пробелы намеренно НЕ сжимаются: в русских таблицах пробел
// разделяет группы тысяч внутри числа («45 356»), и сжатие уничтожило бы
// границу между метрикой и первым значением. По той же причине обрезка
// идёт через TrimSpace, а не через strings.Fields.
func normalizeLine(line string) string {
	replacer := strings.NewReplacer(
		"\u00a0", " ",
		"\u2007", " ",
		"\u202f", " ",
		"\u2013", "–",
		"\u2014", "–",
	)

	line = replacer.Replace(line)

	// Удаляем управляющие символы, иногда попадающие из PDF.
	line = strings.Map(func(r rune) rune {
		switch {
		case r == '\t':
			return ' '
		case r >= 32:
			return r
		default:
			return -1
		}
	}, line)

	return strings.TrimSpace(line)
}

// extractValueTokens оставляет из остатка строки только числовые токены.
//
// Токен — не всегда одно поле: русские релизы разделяют тысячи пробелом
// («45 356», «1 287»), и такой токен склеивается обратно. Склейка идёт по
// шаблону тысяч (группа из трёх цифр), поэтому «1 287 1 311» даёт два токена,
// а «1 287» — один, а не три.
//
// Склейка проверяется до одиночного токена: «1» само по себе тоже валидный
// токен, и если проверять его первым, «1 287» распалось бы на «1» и «287».
//
// Склеиваются только соседние группы одного числа, то есть поля, разделённые
// узким пробелом. Числовые колонки в таблице разнесены на десятки символов,
// и склейка через такой разрыв соединила бы значения соседних колонок
// («946 932» вместо 946 и 932).
func extractValueTokens(text string) []string {
	fields := splitFieldsWithOffsets(text)

	result := make([]string, 0, len(fields))

	for i := 0; i < len(fields); i++ {
		current := strings.ToLower(strings.TrimSpace(fields[i].text))

		switch current {
		case "n/a", "n.a", "n.a.", "-":
			result = append(result, "0")
			continue
		}

		joined := current
		width := 0
		for i+width+1 < len(fields) {
			next := fields[i+width+1]
			if !isThousandsGroup(next.text) {
				break
			}

			// Разрыв считается от конца предыдущего поля: пробел между
			// группами одного числа оставляет запас в один байт.
			previous := fields[i+width]
			if next.start-(previous.start+len(previous.text)) > thousandsGroupGap {
				break
			}

			joined += next.text
			width++
		}

		switch {
		case width > 0 && valueTokenRegexp.MatchString(joined):
			result = append(result, joined)
			i += width
		case valueTokenRegexp.MatchString(current):
			result = append(result, current)
		}
	}

	return result
}

// thousandsGroupGap — максимальный разрыв между группами одного числа. Группы
// разделены одиночным пробелом, поэтому разрыв равен одному байту; всё, что
// шире, считается межколоночным промежутком.
const thousandsGroupGap = 2

// splitFieldsWithOffsets разбивает строку по пробелам, сохраняя позицию
// первого байта каждого поля — по разрыву между полями отличаются группы
// одного числа от соседних колонок.
func splitFieldsWithOffsets(text string) []fieldOffset {
	var fields []fieldOffset

	for i := 0; i < len(text); {
		if text[i] == ' ' {
			i++
			continue
		}

		start := i
		for i < len(text) && text[i] != ' ' {
			i++
		}

		fields = append(fields, fieldOffset{text: text[start:i], start: start})
	}

	return fields
}

// fieldOffset — поле строки вместе с его позицией в ней.
type fieldOffset struct {
	text  string
	start int
}

// isThousandsGroup сообщает, начинается ли поле ровно с трёх цифр — этим
// признаком продолжается число с разделёнными пробелами тысячами.
func isThousandsGroup(field string) bool {
	digits := 0
	for _, r := range field {
		if r < '0' || r > '9' {
			break
		}
		digits++
	}

	if digits != 3 {
		return false
	}

	rest := field[digits:]
	if rest == "" {
		return true
	}

	// «084» из «118 084» — продолжение одного числа; «946,» — тоже число, но
	// с десятичной частью, её принимает общий шаблон токена.
	return isDecimalTail(rest)
}

func isDecimalTail(rest string) bool {
	if rest == "" {
		return true
	}

	if rest[0] != '.' && rest[0] != ',' {
		return false
	}

	for _, r := range rest[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}

	return len(rest) > 1
}

// parseNumericToken превращает токен в число. Скобки означают отрицательное
// значение, запятая и точка равнозначны десятичному разделителю, пробелы и
// запятые — разделители тысяч.
func parseNumericToken(token string) (*float64, error) {
	token = strings.TrimSpace(token)

	if token == "" || token == "-" || strings.EqualFold(token, "n/a") {
		return nil, nil
	}

	negative := strings.HasPrefix(token, "(") &&
		strings.HasSuffix(token, ")")

	token = strings.TrimPrefix(token, "(")
	token = strings.TrimSuffix(token, ")")
	token = strings.TrimSuffix(token, "%")
	token = strings.Join(strings.Fields(token), "")

	// Запятая — разделитель тысяч в английских релизах и десятичный разделитель
	// в русских. Различить их можно по числу цифр после: три цифры означают
	// группу тысяч («1,696»), одна-две — дробную часть («0,87»). Точка в
	// токене тоже означает дробную часть, тогда запятые — только разделители
	// тысяч.
	token = normalizeDecimalComma(token)

	token = strings.ReplaceAll(token, ",", "")

	value, err := strconv.ParseFloat(token, 64)
	if err != nil {
		return nil, fmt.Errorf("parse float: %w", err)
	}

	if negative {
		value = -value
	}

	return &value, nil
}

// normalizeDecimalComma переводит десятичную запятую в точку, оставляя
// запятые-разделители тысяч на месте.
func normalizeDecimalComma(token string) string {
	if strings.Contains(token, ".") {
		return token
	}

	commaIndex := strings.LastIndex(token, ",")
	if commaIndex < 0 || isThousandsGroupTail(token[commaIndex+1:]) {
		return token
	}

	return token[:commaIndex] + "." + token[commaIndex+1:]
}

// isThousandsGroupTail сообщает, выглядит ли часть числа после запятой как
// группа тысяч — ровно три цифры. У группы тысяч слева при этом тоже стоят
// цифры, иначе «,696» был бы просто числом с ведущей запятой.
func isThousandsGroupTail(tail string) bool {
	if len(tail) != 3 {
		return false
	}

	for i := 0; i < len(tail); i++ {
		if !isASCIIDigit(tail[i]) {
			return false
		}
	}

	return true
}
