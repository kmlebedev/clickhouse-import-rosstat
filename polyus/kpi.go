package polyus

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	log "github.com/sirupsen/logrus"
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

// valueGroupGap — максимальный разрыв по X между словами одного числа, которое
// pdftotext разбил по пробелу-разделителю тысяч: «1 287» — это два слова с
// разрывом 1.80pt. Замерено по фикстурам: внутри числа разрыв не превышает
// 1.86pt, а наименьший промежуток между значениями соседних колонок — 14.26pt
// (FY2024, «3,002» → «2,799»). Порог 4.0pt лежит между ними с запасом в обе
// стороны и потому склеивает только группы одного числа.
const valueGroupGap = 4.0

// parseKPIPage разбирает извлечённый pdftotext снимок страницы отчёта: строит по
// координатам шапки колоночную модель таблицы и раскладывает значения строк-
// метрик по её колонкам.
//
// Разбор идёт по координатам, а не по порядку токенов: колонки-изменения
// («Изм. за год», «Y-o-Y») стоят в шапке между периодами, и позиционный разбор
// относил каждое следующее значение на одну колонку левее — значение колонки
// изменения выдавалось за значение периода, а настоящие значения уезжали в
// соседние периоды. Колонка значения определяется его Left через Column.covers.
//
// page — номер страницы PDF, к которой относится textPath; он не вычисляется,
// а попадает в записи как есть.
func parseKPIPage(
	textPath string,
	sourceURL string,
	page int,
) ([]MetricRecord, error) {
	lines, err := readTSVLines(textPath)
	if err != nil {
		return nil, err
	}

	records, _ := parseKPILines(lines, sourceURL, page)

	return records, nil
}

// parseKPILines разбирает уже собранные визуальные строки снимка: та же работа,
// что делает parseKPIPage, но над строками, а не над файлом. Нужна тестам,
// которые подают синтезированные последовательности строк — например, две
// таблицы со своими шапками подряд, как в склеенном многостраничном снимке, — и
// импортёру, который читает счётчик нераспределённых значений (см. guard.go).
//
// page — запасной номер страницы: он попадает в записи только тех строк, номер
// страницы которых не дошёл из TSV (синтезированные строки тестов). У строк из
// снимка номер берётся из самой строки: extractPDF склеивает все Report.Pages в
// один файл, и номер, переданный параметром, верен лишь для первой страницы.
//
// Второй результат — число значений, не ставших ни одной записью: числа в
// непериодных колонках и токены, которые не разобрались в число. Число читается
// как сигнал о расхождении шапки и разбора и уходит в итоговую строку импортёра.
func parseKPILines(lines []Line, sourceURL string, page int) ([]MetricRecord, int) {
	// На одной странице может быть несколько финансовых таблиц, и в склеенном
	// многостраничном снимке они идут подряд без разделителя страниц
	// (pdftotext -nopgbrk). Строку обслуживает ближайшая шапка СВЕРХУ, поэтому
	// колонки запоминаются и живут до следующей шапки.
	var columns []Column

	// Метрика попадает в результат один раз на ШАПКУ: её метка может встретиться
	// и в сносках той же таблицы, а строка сноски с числами дала бы вторую запись
	// того же ключа (metric, period) — ReplacingMergeTree схлопнула бы их молча
	// и оставила ту, что загружена позже. Проверка стоит на СТРОКЕ, а не на
	// записи: одна строка метрики законно даёт по записи на период.
	//
	// Счётчик сбрасывается на каждой шапке: две таблицы — это два независимых
	// набора строк, и метрика второй таблицы не дубль первой. Общий на всю
	// страницу счётчик терял бы записи второй таблицы (обе таблицы склеенного
	// снимка несут gold_output, но за разные периоды), тогда как повтор внутри
	// одной таблицы — по-прежнему дубль.
	//
	// Записи одного ключа (metric, period), пришедшие из РАЗНЫХ таблиц, здесь не
	// сливаются: их слияние — дело import.go (batchDedup), который знает, что
	// попадает в один батч.
	foundMetrics := make(map[string]bool)

	var records []MetricRecord

	// Строки, не опознанные ни как шапка, ни как метрика с числами, и строки
	// выше первой шапки пропускаются: в шапку, подписи и сноски попадает
	// большинство строк страницы. Счётчики ведутся, чтобы пропуски не были
	// молчаливыми.
	skipped := 0
	unassigned := 0

	for i := 0; i < len(lines); i++ {
		if isTableHeader(lines[i]) {
			band := headerBand(lines, i)
			columns = columnsFromHeader(band)
			foundMetrics = make(map[string]bool)

			i += len(band) - 1

			continue
		}

		if columns == nil {
			skipped++

			continue
		}

		// Метрика строки определяется до разбора её чисел: по ней решается,
		// обрабатывать ли строку вообще (см. foundMetrics).
		definition, _, isMetric := matchMetric(lineText(lines[i]))
		if isMetric && foundMetrics[definition.Name] {
			skipped++

			continue
		}

		lineRecords, lineUnassigned := recordsFromLine(
			lines[i],
			columns,
			"PJSC Polyus",
			sourceURL,
			linePage(lines[i], page),
		)
		if len(lineRecords) == 0 {
			skipped++

			continue
		}

		foundMetrics[definition.Name] = true

		records = append(records, lineRecords...)
		unassigned += lineUnassigned
	}

	log.Infof(
		"polyus KPI %s p.%d: %d records, %d non-metric lines skipped, %d values outside period columns",
		sourceURL,
		page,
		len(records),
		skipped,
		unassigned,
	)

	return records, unassigned
}

// linePage возвращает номер страницы визуальной строки: номер её первого слова,
// а если его не донесла сборка строки (синтетический вход тестов) — запасной.
// Номер страницы нужен записи как провенанс (source_page витрины): у склеенного
// многостраничного снимка строки разных страниц идут в одном списке, и номер,
// переданный в разбор параметром, верен только для первой страницы. Запасной
// номер оставлен ради синтезированных строк: у них колонка page_num не задана.
func linePage(line Line, fallback int) int {
	// Номер страницы одинаков у всех слов строки: groupByLine собирает строку
	// только из слов одной страницы.
	if page := pageOfLine(line); page != 0 {
		return page
	}

	return fallback
}

// pageOfLine возвращает номер страницы строки: первое слово с известным номером.
// Ноль означает, что номер не донесён сборкой — синтетический вход.
func pageOfLine(line Line) int {
	for _, word := range line.Words {
		if word.Page != 0 {
			return word.Page
		}
	}

	return 0
}

// readTSVLines читает TSV-снимок страницы и собирает его слова в визуальные
// строки.
//
// deltaTop == 0 — штатный режим groupByLine: слова одной напечатанной строки
// несут один и тот же Top. Шапка, разнесённая по нескольким строкам, склеивается
// отдельно (headerBand), и это НЕ тот же допуск: 2pt слили бы 11–18 настоящих
// строк содержимого.
func readTSVLines(textPath string) ([]Line, error) {
	file, err := os.Open(textPath)
	if err != nil {
		return nil, fmt.Errorf("open extracted page: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Warnf("close extracted page %s: %v", textPath, closeErr)
		}
	}()

	words, err := parseTSV(file)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", textPath, err)
	}

	return groupByLine(words, 0), nil
}

// recordsFromLine раскладывает одну строку таблицы по колонкам её шапки и
// возвращает записи строки вместе с числом значений, не ставших периодами.
//
// Метрика ищется по склеенному тексту строки, начиная с самого левого слова:
// метка занимает начало строки, а числа идут после неё. Граница метки берётся из
// самого сопоставления — matchMetric возвращает остаток строки, — а не из
// координат: метка из нескольких слов разделена теми же узкими промежутками,
// что и группы одного числа, и по координатам их не различить.
//
// Число может быть разбито на несколько слов («1 287»), поэтому соседние слова
// склеиваются обратно по малому разрыву по X (valueGroupGap). За значение
// отвечает Left его ПЕРВОГО слова: так число и выровнено под подписью колонки.
//
// Колонка находится через Column.covers. Границы колонок покрывают строку без
// дыр, поэтому колонка есть у каждого числа; значение, накрытое НЕПЕРИОДНОЙ
// колонкой (колонкой изменения или подписью метки единиц измерения), периода не
// даёт и считается нераспределённым. Возвращённое число нераспределённых
// значений — сигнал для guard (Task 7): именно по нему видно, что строка
// принесла значения колонки изменения.
func recordsFromLine(
	line Line,
	columns []Column,
	company string,
	sourceURL string,
	page int,
) ([]MetricRecord, int) {
	text := lineText(line)

	definition, remainder, found := matchMetric(text)
	if !found || definition.Name == "period" {
		return nil, 0
	}

	labelEnd := len(text) - len(remainder)

	var records []MetricRecord

	unassigned := 0

	for _, value := range valuesFromWords(line.Words, labelEnd) {
		column := columnAt(columns, value.left)
		if column.Period == "" {
			unassigned++

			continue
		}

		number, err := parseNumericToken(value.text)
		if err != nil {
			// Ошибка разбора значения не роняет страницу целиком: запись
			// теряется, но о ней сообщается.
			log.Warnf(
				"polyus KPI %s p.%d: metric %s, period %s, value %q: %v",
				sourceURL,
				page,
				definition.Name,
				column.Period,
				value.text,
				err,
			)

			continue
		}
		if number == nil {
			// Токен распознан, но значения нет ("-", "n/a").
			continue
		}

		records = append(records, MetricRecord{
			Company:    company,
			Metric:     definition.Name,
			Period:     column.Period,
			PeriodType: column.Type,
			Value:      *number,
			Unit:       definition.Unit,
			SourceURL:  sourceURL,
			SourcePage: page,
		})
	}

	return records, unassigned
}

// rowValue — число строки: текст и Left его первого слова.
type rowValue struct {
	text string
	left float64
}

// valuesFromWords собирает числа строки, пропуская слова метки.
//
// labelEnd — байтовое смещение в склеенном тексте строки, с которого начинаются
// числа; слова, начинающиеся левее, — часть метки и числами не считаются.
//
// Слово становится числом, если его текст — числовой токен. Группы одного числа,
// разбитые пробелом-разделителем тысяч («1» и «287»), склеиваются обратно по
// разрыву по X: узкий разрыв — пробел внутри числа, широкий — промежуток между
// колонками. Склейка проверяется раньше одиночного токена: «1» само по себе тоже
// валидный токен, и если проверять его первым, «1 287» распалось бы на «1» и
// «287», то есть на значение 1 и нераспределённое 287.
func valuesFromWords(words []Word, labelEnd int) []rowValue {
	var values []rowValue

	position := 0

	for i := 0; i < len(words); {
		wordStart := position
		position += len(words[i].Text) + 1

		if wordStart < labelEnd {
			// Слово метки — не число.
			i++

			continue
		}

		joined := normalizeLine(words[i].Text)
		last := words[i]
		width := 0

		for i+width+1 < len(words) {
			next := words[i+width+1]
			if !isThousandsGroup(normalizeLine(next.Text)) {
				break
			}
			if next.Left-(last.Left+last.Width) > valueGroupGap {
				break
			}

			joined += normalizeLine(next.Text)
			last = next
			width++
		}

		switch {
		case width > 0 && valueTokenRegexp.MatchString(joined):
			values = append(values, rowValue{text: joined, left: words[i].Left})
			i += width + 1

			continue
		case valueTokenRegexp.MatchString(joined):
			values = append(values, rowValue{text: joined, left: words[i].Left})
		}

		i++
	}

	return values
}

// columnAt возвращает колонку, накрывающую X.
//
// Расширенные границы колонок делят строку на промежутки без дыр (крайние
// колонки уходят в ±infiniteX, см. widenColumns), поэтому колонка есть всегда и
// ровно одна — колонки берутся из уже найденной шапки, то есть непустые.
// Ноль совпадений означал бы, что границы перестали покрывать строку, несколько —
// что они перекрылись; оба случая — ошибка в widenColumns, и о них сообщается в
// лог. Возвращается всё равно первое совпадение: молча терять значение нельзя, а
// обрыв разбора страницы хуже испорченной записи.
func columnAt(columns []Column, x float64) Column {
	var covering []Column

	for _, column := range columns {
		if column.covers(x) {
			covering = append(covering, column)
		}
	}

	if len(covering) != 1 {
		log.Warnf("polyus KPI: x=%v covered by %d columns, taking the first", x, len(covering))
	}

	return covering[0]
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

// matchPeriodAt ищет самый короткий префикс токенов, начинающийся с tokens[start],
// который parsePeriod разбирает целиком: от tokens[start:start+1] до
// tokens[start:start+n], n не больше maxPeriodTokens. Возвращает ширину окна в
// токенах.
//
// Окно проверяется от короткого к длинному, потому что parsePeriod — матчер, а не
// валидатор: он ищет форму периода ВНУТРИ строки и не сообщает, какую её часть
// разобрал. Длинное окно поэтому забирает себе лишние слова — «2H 2024 1H»
// разбирается как «2H 2024» с шириной 3, — и следующая подпись периода теряется
// вместе с вычеркнутыми ею колонками. Короткое окно закрывается там, где форма
// периода кончается на самом деле.
//
// Формы-префиксы при этом не распознаются раньше целых форм: «1» и «1 п/г»
// parsePeriod не разбирает, поэтому «1 п/г 2026» закрывается только на третьем
// слове, а не распадается на «1» и «2026» (голый год FY).
//
// Окно, содержащее слово подписи колонки изменения, периодом не считается:
// документ объявляет этим словом непериодную колонку, и она обязана остаться
// отдельной колонкой — иначе её числа («3%» на x=416.11 в MD&A за 2014) достаются
// соседнему периоду.
func matchPeriodAt(tokens []string, start, maxPeriodTokens int) (PeriodInfo, int, error) {
	for width := 1; width <= maxPeriodTokens; width++ {
		if start+width > len(tokens) {
			break
		}

		window := tokens[start : start+width]
		if windowHasChangeLabel(window) {
			continue
		}

		info, err := parsePeriod(strings.Join(window, " "))
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
// Префиксы перебираются от самого длинного к самому короткому, иначе, например,
// "CAPEX" перехватывал бы "Stripping CAPEX".
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

// metricsSortedByPrefixLength возвращает словарь, упорядоченный по убыванию
// длины самого длинного префикса каждой метрики.
//
// Порядок нужен matchMetric: он берёт первое совпадение, поэтому метрика с
// более длинной меткой обязана проверяться раньше. Иначе короткая метка
// перехватывает строки длинной — «Капитальные затраты» (capex) забирает строку
// «Капитальные затраты по вскрышным работам» (stripping_capex), и вторая
// метрика не попадает в результат вообще.
//
// Сортировка устойчивая, поэтому метрики с одинаковой длиной самого длинного
// префикса сохраняют порядок объявления в metrics.go — он и служит
// разрешением ничьих.
func metricsSortedByPrefixLength() []MetricDefinition {
	result := append([]MetricDefinition(nil), metrics...)

	sort.SliceStable(result, func(i, j int) bool {
		return longestPrefixLen(result[i]) > longestPrefixLen(result[j])
	})

	return result
}

// longestPrefixLen возвращает длину самого длинного префикса метрики в байтах.
// Префиксы сравниваются по длине в байтах так же, как их сравнивает
// strings.HasPrefix в matchMetric, поэтому единица измерения совпадает.
func longestPrefixLen(definition MetricDefinition) int {
	longest := 0
	for _, prefix := range definition.Prefix {
		if len(prefix) > longest {
			longest = len(prefix)
		}
	}

	return longest
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
