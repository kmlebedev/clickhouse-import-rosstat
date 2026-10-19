package cbr

import (
	"fmt"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Разбор PDF-версии «Табл. 1» выпусков «Комментария к среднесрочному прогнозу» и
// «Среднесрочного прогноза» ЦБ (импортёр cbr_mt_forecast, спека §3.5). Вход — текст, извлечённый
// `pdftotext -layout -enc UTF-8 -nopgbrk` (спека §3.3): колонки таблицы разделены сериями пробелов,
// а подписи, значения и сноски идут построчно. Сеть, `exec` и выпуск (meeting_date/release_date/url)
// остаются снаружи, в извлекателе и импортёре: здесь чистая функция «текст → плоский список точек»
// (стандарт §6.1), поэтому тесты корпуса офлайн и не требуют poppler (спека §7).
//
// Вёрстка PDF-выпусков дрейфует сильнее книги (спека §2, §3.5): комментарии печатают заголовок
// капсом, прогнозы — с заглавной; soft hyphen U+00AD стоит внутри слова «ПРОГНОЗА»; тире в ячейках
// бывает и дефисом, и en dash, и с пробелами вокруг (`15,0 – 16,0`), а номер сноски приезжает
// надстрочной цифрой (`14,5–14,6¹`) или приклеенной к последней цифре (`14,5–14,61`). Поэтому
// таблица ищется по якорю и шапке годов, а не по номерам строк, значения проходят общий
// mtForecastBounds, а подпись — общий mtForecastNormalizeLabel (спека §3.6).
const (
	// mtForecastPDFAnchor — якорь таблицы «Табл. 1»: заголовок раздела, под которым она стоит.
	// Источник печатает его капсом в комментариях и с заглавной в прогнозах, поэтому поиск идёт по
	// верхнему регистру строки. Якорь один на документ (проверено на 20 PDF, спека §3.5), поэтому
	// поиск по всем страницам безопасен — в том числе у comment_27022024_p1p2, где перед таблицей
	// лежит обложка.
	mtForecastPDFAnchor = "ОСНОВНЫЕ ПАРАМЕТРЫ ПРОГНОЗА БАНКА РОССИИ"

	// mtForecastPDFStop — строка конца «Табл. 1»: под таблицей и её сносками источник подписывает
	// «Источник: Банк России.». Строка открывает и «Табл. 2» той же страницы — она в объём не входит
	// (спека §8), и без остановки её строки («Экспорт», «Импорт» с другими единицами) подменили бы
	// точки первой таблицы.
	mtForecastPDFStop = "Источник: Банк России"

	// mtForecastPDFContext — общая часть сообщений разбора: таблица и раздел (стандарт §6.4).
	// Выпуск в сообщение не входит: на входе только текст, даты выпуска у разбора нет — их добавит
	// извлекатель (спека §3.3), а таблица и раздел остаются теми же для любого выпуска.
	mtForecastPDFContext = mtForecastTable + ", PDF «Табл. 1»"
)

var (
	// mtForecastPDFJoinRangesRe — склейка разрезанного диапазона (спека §3.5, п. 1): pdftotext может
	// развести границы по «колонкам» (`13,5 – 14,5`), и тогда разбиение строки по mtForecastPDFColumnGapRe
	// разрезало бы одну ячейку на две. RE2 не поддерживает lookbehind, поэтому в шаблон захвачены
	// соседние символы: цифра или закрывающая скобка слева, начало правой границы справа (скобка
	// отрицательного значения, минус, цифра). В таблице такая комбинация — только диапазон.
	mtForecastPDFJoinRangesRe = regexp.MustCompile(`([\d)])\s+[–—]\s+(\(?-?\d)`)

	// mtForecastPDFColumnGapRe — разделитель колонок `pdftotext -layout`: две и более позиции
	// пробельных символов. Одиночный пробел — часть подписи («в том числе», «– к населению») или
	// значений с пробелами вокруг тире, поэтому границей колонки он не считается.
	mtForecastPDFColumnGapRe = regexp.MustCompile(`\s{2,}`)

	// mtForecastPDFEndRe — строка-сноска под таблицей (спека §3.5, п. 4): номер сноски и пробел в
	// начале строки — арабский (`1 С учетом…`) или надстрочный (`¹ С учетом…`, в комментариях).
	// В прогнозах номер напечатан отдельной строкой без пробела после него: такая строка разбор не
	// останавливает (пропускается как однотокенная), и таблицу закрывает подпись источника. Ниже
	// таблицы сноски идут в той же колонке подписей, и без остановки каждая давала бы
	// предупреждение о незнакомой подписи.
	mtForecastPDFEndRe = regexp.MustCompile(`^(?:\d+|[¹²³⁴⁵⁶⁷⁸⁹])\s`)

	// mtForecastPDFSpaceReplacer — типографика PDF-строки (спека §3.5, п. 1): soft hyphen удаляется
	// (он режет «ПРОГНОЗА» и «предыдущему»), пробелы-двойники и служебные символы (`\t`, `\f`)
	// становятся обычным пробелом. Переводы строк здесь не заменяются: строка уже разрезана по `\n`.
	mtForecastPDFSpaceReplacer = strings.NewReplacer(
		"\u00a0", " ", "\u2002", " ", "\u2009", " ", "\u200a", " ", "\u202f", " ",
		"\t", " ", "\f", " ",
		"\u00ad", "",
	)
)

// mtForecastPDFNormalizeLine приводит строку текста PDF к сравнимому виду (спека §3.5, п. 1):
// снимает soft hyphen, заменяет пробелы-двойники и служебные символы пробелом и склеивает
// диапазоны, разведённые по колонкам. Порядок важен: склейка идёт после замены пробелов, иначе
// NBSP вокруг тире не совпал бы с `\s+` в шаблоне.
func mtForecastPDFNormalizeLine(line string) string {
	line = mtForecastPDFSpaceReplacer.Replace(line)
	return mtForecastPDFJoinRangesRe.ReplaceAllString(line, "$1–$2")
}

// mtForecastPDFYears — годы, найденные в строке при разбиении по колонкам: токены, целиком
// разбирающиеся mtForecastYear (целое 1900–2100). Пустой результат — строка не шапка; годы
// возвращаются в порядке строки, проверка возрастания — на стороне разбора. Факт-год в шапку не
// попадает: источник печатает его отдельной строкой выше (`2025` / `(факт)`), и порог
// mtForecastHeaderYears (2) отсекает её (спека §3.5, п. 3).
func mtForecastPDFYears(line string) []uint16 {
	fields := mtForecastPDFColumnGapRe.Split(strings.TrimSpace(line), -1)
	years := make([]uint16, 0, len(fields))
	for _, field := range fields {
		if year, ok := mtForecastYear(strings.TrimSpace(field)); ok {
			years = append(years, year)
		}
	}
	return years
}

// mtForecastParsePDF разбирает PDF-версию «Табл. 1» из текста `pdftotext -layout`: находит якорь
// заголовка раздела (после него таблица), ниже — шапку прогнозных годов, читает строки показателей
// до подписи «Источник: Банк России» или строки-сноски и отдаёт плоский список точек в порядке
// обхода: строки сверху вниз, внутри строки — прогнозные годы по возрастанию (спека §3.5).
// Первое значение строки — факт-колонка: она не импортируется (как и в xlsx), поэтому не
// проверяется даже на форму — в источнике там стоит `–²`.
//
// Fail-fast (стандарт §6.4, спека §3.5): нет якоря; после якоря нет шапки с годами; годы не
// возрастают или повторяются; число значений строки не равно «1 факт + число годов»; неразбираемая
// прогнозная ячейка; нет core-показателя; ни одной точки после разбора. Предупреждение и
// продолжение (§6.6, §12.3): незнакомая подпись строки (новые строки источника — ожидаемое
// изменение) и пропажа не-core показателя.
func mtForecastParsePDF(text []byte) ([]mtForecastPoint, error) {
	lines := strings.Split(string(text), "\n")
	for i := range lines {
		lines[i] = mtForecastPDFNormalizeLine(lines[i])
	}

	anchor := -1
	for i, line := range lines {
		if strings.Contains(strings.ToUpper(line), mtForecastPDFAnchor) {
			anchor = i
			break
		}
	}
	if anchor < 0 {
		return nil, fmt.Errorf("%s: якорь %q не найден — вёрстка PDF изменилась", mtForecastPDFContext, mtForecastPDFAnchor)
	}

	header := -1
	var years []uint16
	for i := anchor + 1; i < len(lines); i++ {
		candidate := mtForecastPDFYears(lines[i])
		if len(candidate) < mtForecastHeaderYears {
			continue
		}
		header, years = i, candidate
		break
	}
	if header < 0 {
		return nil, fmt.Errorf("%s: после якоря нет строки с годами прогноза (токенов-годов не менее %d) — разбор не знает прогнозных колонок",
			mtForecastPDFContext, mtForecastHeaderYears)
	}
	for i := 1; i < len(years); i++ {
		if years[i] <= years[i-1] {
			return nil, fmt.Errorf("%s, строка %d: годы шапки %v не возрастают — две колонки одного года или перевёрнутая шапка",
				mtForecastPDFContext, header+1, years)
		}
	}

	seen := make(map[string]bool, len(mtForecastIndicators))
	var points []mtForecastPoint
	for i := header + 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, mtForecastPDFStop) || mtForecastPDFEndRe.MatchString(line) {
			// Таблица кончилась: подпись источника или сноска, а ниже — «Табл. 2» той же страницы.
			break
		}
		fields := make([]string, 0, len(years)+1)
		for _, field := range mtForecastPDFColumnGapRe.Split(line, -1) {
			if field = strings.TrimSpace(field); field != "" {
				fields = append(fields, field)
			}
		}
		if len(fields) < 2 {
			// Пустая строка или строка из одного токена: номер сноски, которую источник напечатал
			// отдельной строкой, и обрывки текста сносок — не строки таблицы.
			continue
		}
		label := mtForecastNormalizeLabel(fields[0])
		if label == "" {
			continue
		}
		indicator, ok := mtForecastIndicatorByLabel(label)
		if !ok {
			log.Warnf("%s, строка %d: подпись %q не объявлена реестром показателей — строка пропущена, остальные разобраны (стандарт §6.6)",
				mtForecastPDFContext, i+1, label)
			continue
		}
		seen[indicator.Code] = true
		values := fields[1:]
		if len(values) != 1+len(years) {
			return nil, fmt.Errorf("%s, строка %d, подпись %q: значений %d, want %d (1 факт + %d прогнозных лет) — число колонок строки не совпало с шапкой",
				mtForecastPDFContext, i+1, label, len(values), 1+len(years), len(years))
		}
		for j, year := range years {
			// values[0] — факт-колонка, прогнозные годы идут следом по возрастанию (шапка уже
			// проверена), поэтому год строки — это years[j], а не позиция в строке.
			lo, hi, err := mtForecastBounds(values[j+1])
			if err != nil {
				return nil, fmt.Errorf("%s, строка %d, подпись %q, год %d: %w", mtForecastPDFContext, i+1, label, year, err)
			}
			points = append(points, mtForecastPoint{Indicator: indicator.Code, Year: year, Lo: lo, Hi: hi})
		}
	}

	missingCore, missingRest := mtForecastMissing(seen)
	if len(missingCore) > 0 {
		return nil, fmt.Errorf("%s: нет строк core-показателей %s — ради них импортёр и заведён (спека §3.2): эти ряды обязаны быть в каждом выпуске",
			mtForecastPDFContext, strings.Join(missingCore, ", "))
	}
	if len(missingRest) > 0 {
		log.Warnf("%s: строки показателей %s не найдены — подписи источника изменились? Ряды пропущены, остальные разобраны",
			mtForecastPDFContext, strings.Join(missingRest, ", "))
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: разбор не дал ни одной точки — ни один объявленный показатель не разобран", mtForecastPDFContext)
	}
	return points, nil
}
