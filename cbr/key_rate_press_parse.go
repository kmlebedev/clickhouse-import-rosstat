package cbr

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	log "github.com/sirupsen/logrus"
)

// Разбор пресс-релиза ЦБ по ключевой ставке и календаря заседаний (спека
// docs/superpowers/specs/2026-10-23-cbr-key-rate-press-design.md §2, §4).
//
// Источник — два документа: календарь https://www.cbr.ru/dkp/cal_mp/ (дата заседания и
// ссылка на релиз в одном блоке div.main-events_day) и страница релиза
// https://www.cbr.ru/press/pr/?file=… (текст в div.landing-text). Разбор чистый:
// сеть трогает вызывающий (util.GetPage), поэтому тесты офлайн на живых фикстурах.

// pressMeeting — заседание из календаря: дата и относительная ссылка на релиз.
// Href пуст у будущих заседаний: ЦБ печатает карточку раньше, чем релиз выходит.
type pressMeeting struct {
	Date time.Time
	Href string
}

// pressBlock — абзац релиза: Block — эвристический слаг (истина — Seq и Text),
// Seq — порядок абзаца на странице (с 1).
type pressBlock struct {
	Block string
	Seq   int
	Text  string
}

// pressQuarter — квартальный блок релиза: дословный абзац и разобранные из него
// числа. Указатели — NULL в ClickHouse: «числа нет» и «ноль» различаются.
type pressQuarter struct {
	Seq             int
	Period          string
	Form            string
	Headline        *float64
	HeadlinePrev    *float64
	HeadlinePrev2   *float64
	Core            *float64
	CorePrev        *float64
	CorePrev2       *float64
	AnnualInflation *float64
	AnnualAsof      *time.Time
	Parsed          bool
	Text            string
}

// pressDoc — разобранный релиз: решение и шапка, дословные абзацы и квартальные блоки.
type pressDoc struct {
	MeetingDate     time.Time
	Action          string
	Rate            float64
	EffectiveFrom   *time.Time
	NextMeetingDate *time.Time
	Signal          string
	Blocks          []pressBlock
	Quarters        []pressQuarter
}

// errPressNotPublished — страница без абзаца решения: релиз ещё не опубликован
// (страница-заглушка до 13:30 МСК). Импортёр отличает это состояние от сбоя разбора:
// для сегодняшнего заседания — warn и пропуск, для прошлого — ошибка (спека §5).
var errPressNotPublished = errors.New("абзац решения не найден: релиз ещё не опубликован")

const (
	// pressCalendarPath — календарь заседаний Совета директоров по ключевой ставке:
	// даты и ссылки на релизы. Адрес тот же, что читает events_calendar, но константа
	// своя: импортёры делят страницу, а не код (прецедент — cbr_mp_summary).
	pressCalendarPath = "/dkp/cal_mp/"

	// pressMeetingTitle — подпись карточки заседания в календаре: по ней блок
	// main-events_day отличается от прочих событий (пресс-конференции, доклады).
	pressMeetingTitle = "Заседание Совета директоров Банка России по ключевой ставке"

	// pressQuarterBlock — слаг квартального блока: по нему импортёр знает, что из
	// абзаца нужно построить строку cbr_key_rate_press_quarter (спека §4).
	pressQuarterBlock = "inflation_quarter"

	// pressFormQuarter/pressFormMonth/pressFormNone — значения form: квартал к
	// кварталу, месяц (месяцы) к среднему за прошлый квартал, чисел нет.
	pressFormQuarter = "quarter"
	pressFormMonth   = "month"
	pressFormNone    = "none"

	// pressBoilerplate — подпись источника под релизом, не текст документа.
	pressBoilerplate = "При использовании материала ссылка на Пресс-службу Банка России обязательна"
)

var (
	// pressSiteURL — корень сайта ЦБ: переменная, а не константа, ради теста
	// HTTP-слоя (httptest подменяет корень, разбор остаётся тем же). В проде —
	// cbr.ru; сетевых вызовов в init() нет.
	pressSiteURL = "https://www.cbr.ru"

	// pressDateRe — дата ЦБ в календаре и в тексте: «16 февраля 2024 года».
	// Пробелы нормализованы normMPText (NBSP в календаре — обычный пробел).
	pressDateRe = regexp.MustCompile(`^(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+года$`)

	// pressDecisionRe — ядро решения в пресс-релизе: инфинитив после «принял
	// решение» (в резюме — прошедшее время, у релиза — «сохранить/снизить/повысить»).
	// Шаг «на N б.п.» есть только у cut/hike; уровень — «до» или «на уровне».
	pressDecisionRe = regexp.MustCompile(`Совет\s+директоров\s+Банка\s+России\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+года\s+принял\s+решение\s+(сохранить|снизить|повысить)\s+ключевую\s+ставку(?:\s+на\s+(\d+)\s*б\.\s*п\.,?)?\s+(?:до|на\s+уровне)\s+(\d+(?:,\d+)?)\s*%\s+годовых`)

	// pressEffectiveRe — дата вступления ставки в силу: «с 28 октября 2024 года».
	pressEffectiveRe = regexp.MustCompile(`с\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+года`)

	// pressNextMeetingRe — следующее заседание: «…запланировано на 18 декабря 2026 года».
	pressNextMeetingRe = regexp.MustCompile(`(?s)Следующее\s+заседание\s+Совета\s+директоров.*?запланировано\s+на\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+года`)

	// pressPercentRe — процент в прозе: десятичная запятая, пробел перед знаком
	// допускается. Диапазоны («7,1–7,6%») дают только правую границу — поэтому
	// годовая инфляция читается не «первым процентом предложения», а после глагола.
	pressPercentRe = regexp.MustCompile(`(\d+(?:[.,]\d+)?)\s*%`)

	// pressPrevRe — предыдущее значение: «после 5,8%», «после в среднем 8,2%»,
	// «(по сравнению с 11,5%…)».
	pressPrevRe = regexp.MustCompile(`(?i)(?:после|по\s+сравнению\s+с)\s+(?:в\s+среднем\s+)?(\d+(?:,\d+)?)\s*%`)

	// pressSecondBaseRe — второй базис после первого: «… и 4,3% в 4к25».
	pressSecondBaseRe = regexp.MustCompile(`(?i)\s+и\s+(\d+(?:,\d+)?)\s*%`)

	// pressAsofRe — дата оценки годовой инфляции: «по оценке на 19 октября».
	pressAsofRe = regexp.MustCompile(`(?i)по\s+оценке\s+на\s+(\d{1,2})\s+([а-яё]+)`)

	// pressAnnualRe — предложение годовой инфляции: «Годовая инфляция…» и
	// «Показатель годовой инфляции…» (в релизах встречаются обе формы).
	pressAnnualRe = regexp.MustCompile(`(?i)годов(?:ая|ой)\s+инфляци(?:я|и)`)

	// pressAnnualVerbRe — глагол факта годовой инфляции: «составила», «составил».
	// Форма «сложится в диапазоне» — прогноз, а не факт, и сюда не попадает.
	pressAnnualVerbRe = regexp.MustCompile(`(?i)состав(?:ил|ила|ило)`)

	// pressTimestampRe — служебная строка времени публикации под релизом
	// («23.10.2026 13:30:00»): не абзац документа.
	pressTimestampRe = regexp.MustCompile(`^\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2}$`)

	// pressQuarterFormRe — период-квартал: «3к26», «1к25», «II квартал 2024».
	pressQuarterFormRe = regexp.MustCompile(`(?i)^(?:I|II|III|IV)\s+квартал|^\d+к\d\d`)

	// pressVerbAction — действие по глаголу решения.
	pressVerbAction = map[string]string{
		"сохранить": "hold",
		"снизить":   "cut",
		"повысить":  "hike",
	}

	// pressMonthNominative — месяцы в именительном падеже: по ним определяется форма
	// month («июль», «октябрь — ноябрь»). Предложный падеж периода приводится к
	// именительному pressPeriod.
	pressMonthNominative = []string{
		"январь", "февраль", "март", "апрель", "май", "июнь",
		"июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь",
	}

	// pressMonthForms — предложный падеж месяцев периода («в октябре») в именительный.
	pressMonthForms = strings.NewReplacer(
		"январе", "январь", "феврале", "февраль", "марте", "март", "апреле", "апрель",
		"мае", "май", "июне", "июнь", "июле", "июль", "августе", "август",
		"сентябре", "сентябрь", "октябре", "октябрь", "ноябре", "ноябрь", "декабре", "декабрь",
	)

	// pressSignalMarkers — начала предложений ЦБ про сигнал: сохраняются дословно,
	// потому что формулировка сигнала и есть наблюдаемая величина (как у cbr_mp_summary).
	// 2024 год не говорит «Банк России будет»: его сигнал — второй абзац про
	// «продолжительный период»/«дополнительное ужесточение», и он тоже дословный.
	pressSignalMarkers = []string{
		"банк россии будет принимать дальнейшие решения",
		"банк россии допускает",
		"банк россии будет поддерживать",
		"предполагают продолжительный период",
		"предполагает продолжительный период",
		"требуется дополнительное ужесточение",
	}
)

// pressParseDate разбирает дату ЦБ «16 февраля 2024 года» (пробелы нормализуются).
func pressParseDate(raw string) (time.Time, error) {
	m := pressDateRe.FindStringSubmatch(normMPText(raw))
	if m == nil {
		return time.Time{}, fmt.Errorf("дата %q: ожидался формат «16 февраля 2024 года»", raw)
	}
	month, ok := mpMonths[m[2]]
	if !ok {
		return time.Time{}, fmt.Errorf("дата %q: месяц %q не распознан", raw, m[2])
	}
	day, dayErr := strconv.Atoi(m[1])
	if dayErr != nil {
		return time.Time{}, fmt.Errorf("дата %q: день %q: %w", raw, m[1], dayErr)
	}
	year, yearErr := strconv.Atoi(m[3])
	if yearErr != nil {
		return time.Time{}, fmt.Errorf("дата %q: год %q: %w", raw, m[3], yearErr)
	}
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC), nil
}

// pressCalendarMeetings разбирает календарь заседаний: блок div.main-events_day, в
// котором есть подпись заседания по ключевой ставке. Дата — div.date, ссылка на
// релиз — первый href /press/pr/ того же блока. Заседание без ссылки возвращается с
// пустым Href (будущее заседание); решение, что с ним делать, принимает импортёр.
// Ноль заседаний — ошибка: структура страницы изменилась, тихая пустая дельта
// оставила бы источник неимпортированным.
func pressCalendarMeetings(page []byte) ([]pressMeeting, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("календарь: разбор html: %w", err)
	}
	var (
		meetings []pressMeeting
		dateErr  error
	)
	doc.Find("div.main-events_day").Each(func(_ int, day *goquery.Selection) {
		if dateErr != nil {
			return
		}
		if !strings.Contains(normMPText(day.Text()), pressMeetingTitle) {
			return
		}
		raw := normMPText(day.Find("div.date").First().Text())
		date, err := pressParseDate(raw)
		if err != nil {
			dateErr = fmt.Errorf("календарь: %w", err)
			return
		}
		href, _ := day.Find(`a[href*="/press/pr/"]`).First().Attr("href")
		meetings = append(meetings, pressMeeting{Date: date, Href: strings.TrimSpace(href)})
	})
	if dateErr != nil {
		return nil, dateErr
	}
	if len(meetings) == 0 {
		return nil, fmt.Errorf("календарь: ни одного заседания по ключевой ставке — вёрстка изменилась")
	}
	sort.SliceStable(meetings, func(i, j int) bool { return meetings[i].Date.Before(meetings[j].Date) })
	return meetings, nil
}

// pressParseDoc разбирает страницу релиза (спека §4): решение, сигнал, следующее
// заседание, абзацы и квартальные блоки. Даты решения и заседания сверяются —
// страница, переехавшая под другую дату, обязана быть ошибкой, а не вторым выпуском.
func pressParseDoc(page []byte, meeting time.Time) (*pressDoc, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("страница релиза: разбор html: %w", err)
	}
	container := doc.Find("div.landing-text").First()
	if container.Length() == 0 {
		return nil, fmt.Errorf("страница релиза: нет контейнера div.landing-text — вёрстка изменилась")
	}
	var paragraphs []string
	container.Find("p").Each(func(_ int, p *goquery.Selection) {
		text := normMPText(p.Text())
		if text == "" || pressSkipParagraph(text) {
			return
		}
		paragraphs = append(paragraphs, text)
	})
	if len(paragraphs) == 0 {
		return nil, fmt.Errorf("страница релиза: ни одного абзаца в div.landing-text — вёрстка изменилась")
	}

	var decision []string
	for _, text := range paragraphs {
		if m := pressDecisionRe.FindStringSubmatch(text); m != nil {
			decision = m
			break
		}
	}
	if decision == nil {
		return nil, errPressNotPublished
	}

	decisionDate, err := pressParseDate(decision[1] + " " + decision[2] + " " + decision[3] + " года")
	if err != nil {
		return nil, fmt.Errorf("страница релиза: дата решения: %w", err)
	}
	if !mpDay(decisionDate).Equal(mpDay(meeting)) {
		return nil, fmt.Errorf("страница релиза: дата решения %s не совпадает с датой заседания %s",
			mpDay(decisionDate).Format(pressDayLayout), mpDay(meeting).Format(pressDayLayout))
	}

	action, ok := pressVerbAction[decision[4]]
	if !ok {
		return nil, fmt.Errorf("страница релиза: действие решения %q не распознано", decision[4])
	}
	rate, err := pressNumber(decision[6])
	if err != nil {
		return nil, fmt.Errorf("страница релиза: ставка %q: %w", decision[6], err)
	}

	out := &pressDoc{
		MeetingDate: mpDay(meeting),
		Action:      action,
		Rate:        rate,
	}

	// Дата вступления в силу — только у cut/hike и сразу после уровня ставки:
	// «с 28 октября 2024 года». Ищем в хвосте того же абзаца, где стоит решение,
	// чтобы «с 1 января» из соседнего предложения не стало датой вступления.
	if action != "hold" {
		for _, text := range paragraphs {
			m := pressDecisionRe.FindStringSubmatchIndex(text)
			if m == nil {
				continue
			}
			tail := text[m[1]:]
			if end := strings.Index(tail, ". "); end >= 0 {
				tail = tail[:end]
			}
			if em := pressEffectiveRe.FindStringSubmatch(tail); em != nil {
				effective, effErr := pressParseDate(em[1] + " " + em[2] + " " + em[3] + " года")
				if effErr != nil {
					return nil, fmt.Errorf("страница релиза: дата вступления в силу: %w", effErr)
				}
				out.EffectiveFrom = &effective
			}
			break
		}
	}

	for _, text := range paragraphs {
		if m := pressNextMeetingRe.FindStringSubmatch(text); m != nil {
			next, nextErr := pressParseDate(m[1] + " " + m[2] + " " + m[3] + " года")
			if nextErr != nil {
				return nil, fmt.Errorf("страница релиза: дата следующего заседания: %w", nextErr)
			}
			out.NextMeetingDate = &next
		}
		if pressIsSignal(text) {
			if out.Signal != "" {
				out.Signal += " "
			}
			out.Signal += text
		}
	}

	for i, text := range paragraphs {
		seq := i + 1
		block := pressBlockSlug(text)
		if decision[0] != "" && strings.Contains(text, decision[0]) {
			block = "decision"
		}
		out.Blocks = append(out.Blocks, pressBlock{Block: block, Seq: seq, Text: text})
		if block == pressQuarterBlock {
			out.Quarters = append(out.Quarters, pressParseQuarter(text, seq, out.MeetingDate))
		}
	}
	return out, nil
}

// pressSkipParagraph — служебные абзацы страницы: пустая типографика, подпись
// источника и время публикации. Всё остальное — текст документа.
func pressSkipParagraph(text string) bool {
	return strings.HasPrefix(text, pressBoilerplate) || pressTimestampRe.MatchString(text)
}

// pressIsSignal — абзац с предложением про сигнал ДКП (дословно).
func pressIsSignal(text string) bool {
	low := strings.ToLower(text)
	for _, marker := range pressSignalMarkers {
		if strings.Contains(low, marker) {
			return true
		}
	}
	return false
}

// pressBlockSlug — эвристический слаг абзаца (спека §4). Порядок проверок важен:
// «денежно-кредитные условия» раньше «кредит», «банк россии будет» раньше
// «инфляционных ожиданий» (сигнал упоминает ожидания), «риски» раньше «бюджет».
// Истина — Seq и текст; слаг — удобство агента, поэтому новые формулировки
// получают other, а не ошибку.
func pressBlockSlug(text string) string {
	low := strings.ToLower(text)
	switch {
	case strings.Contains(low, "следующее заседание"):
		return "next_meeting"
	case strings.Contains(low, "опубликует"):
		return "publications"
	case strings.Contains(low, "среднесрочный прогноз"):
		return "forecast"
	case strings.Contains(low, "текущий рост цен") && strings.Contains(low, "поправкой на сезонность"):
		return pressQuarterBlock
	case pressIsSignal(low):
		return "guidance"
	case strings.Contains(low, "денежно-кредитн"):
		return "monetary"
	case strings.Contains(low, "кредитн"):
		return "credit"
	case strings.Contains(low, "рынке труда") || strings.Contains(low, "рынок труда") ||
		strings.Contains(low, "трудовых ресурсов") || strings.Contains(low, "зарплат") || strings.Contains(low, "безработиц"):
		return "labor"
	case strings.Contains(low, "риск"):
		return "risks"
	case strings.Contains(low, "бюджет"):
		return "budget"
	case strings.Contains(low, "инфляционные ожидания") || strings.Contains(low, "ценовые ожидания"):
		return "expectations"
	case strings.Contains(low, "экономик"):
		return "activity"
	default:
		return "other"
	}
}

// pressPercent — процент и его позиция в строке (байты): по позиции отличаются
// текущее значение, предыдущее и годовая инфляция в одном предложении.
type pressPercent struct {
	Value float64
	At    int
}

// pressPercents — все проценты строки в порядке текста; неразобранное число
// пропускается (ParseFloat ошибку не глотает вызывающий — здесь она означает, что
// regex захватил не число, и это не повод ронять разбор абзаца).
func pressPercents(s string) []pressPercent {
	var out []pressPercent
	for _, m := range pressPercentRe.FindAllStringSubmatchIndex(s, -1) {
		value, err := pressNumber(s[m[2]:m[3]])
		if err != nil {
			continue
		}
		out = append(out, pressPercent{Value: value, At: m[0]})
	}
	return out
}

// pressAnnualValue — значение годовой инфляции в предложении. Две формы источника:
// «составила 6,8%» (первый процент после глагола) и «увеличилась с 8,6% в июне до
// 9,0%, по оценке на 22 июля» (последний процент перед «по оценке»). Прогноз
// «сложится в диапазоне 7,1–7,6%» сюда не попадает: у него нет ни глагола факта, ни
// процента перед датой оценки.
func pressAnnualValue(sentence string) *float64 {
	percents := pressPercents(sentence)
	if len(percents) == 0 {
		return nil
	}
	if verb := pressAnnualVerbRe.FindStringIndex(sentence); verb != nil {
		for _, p := range percents {
			if p.At >= verb[1] {
				value := p.Value
				return &value
			}
		}
	}
	limit := len(sentence)
	if asof := pressAsofRe.FindStringIndex(sentence); asof != nil {
		limit = asof[0]
	}
	for i := len(percents) - 1; i >= 0; i-- {
		if percents[i].At < limit {
			value := percents[i].Value
			return &value
		}
	}
	return nil
}

// pressSentenceRange — границы предложения, содержащего байт at: от «. » слева до
// «. » справа. Точек в числах нет (десятичная запятая), поэтому граница однозначна.
func pressSentenceRange(text string, at int) (int, int) {
	start := strings.LastIndex(text[:at], ". ")
	if start < 0 {
		start = 0
	} else {
		start += 2
	}
	end := strings.Index(text[at:], ". ")
	if end < 0 {
		end = len(text)
	} else {
		end = at + end + 1
	}
	return start, end
}

// pressFirstPercentAfter — первый процент предложения не раньше позиции from и
// раньше позиции before (before < 0 — до конца предложения).
func pressFirstPercentAfter(sentence string, from, before int) *float64 {
	if before < 0 {
		before = len(sentence)
	}
	for _, p := range pressPercents(sentence) {
		if p.At >= from && p.At < before {
			value := p.Value
			return &value
		}
	}
	return nil
}

// pressPrevValue — предыдущее значение после «после»/«по сравнению с» в хвосте
// предложения и, если есть, второй базис «и Y%» сразу за ним. Хвост передаётся
// целиком: первое совпадение — предыдущее значение, второе — второй базис.
func pressPrevValue(tail string) (*float64, *float64) {
	m := pressPrevRe.FindStringSubmatchIndex(tail)
	if m == nil {
		return nil, nil
	}
	prev, err := pressNumber(tail[m[2]:m[3]])
	if err != nil {
		return nil, nil
	}
	out := &prev
	rest := tail[m[1]:]
	second := pressSecondBaseRe.FindStringSubmatch(rest)
	if second == nil {
		return out, nil
	}
	prev2, err := pressNumber(second[1])
	if err != nil {
		return out, nil
	}
	return out, &prev2
}

// pressParseQuarter разбирает квартальный абзац (спека §4): период и форму, текущий
// рост цен с предыдущим значением (и вторым базисом), базовую инфляцию и годовую
// инфляцию с датой оценки. Числа не выдумываются: если стандартная пара не
// разобралась, Parsed=false, числа NULL, в лог — warn (текст остаётся источником).
func pressParseQuarter(text string, seq int, meeting time.Time) pressQuarter {
	q := pressQuarter{Seq: seq, Form: pressFormNone, Text: text}
	low := strings.ToLower(text)
	at := strings.Index(low, "текущий рост цен")
	if at < 0 {
		return q
	}
	q.Period = pressPeriod(text[:at])
	q.Form = pressQuarterForm(q.Period)

	start, end := pressSentenceRange(text, at)
	sentence := text[start:end]
	rel := at - start

	before := strings.Index(strings.ToLower(sentence), "базовой инфляции")
	q.Headline = pressFirstPercentAfter(sentence, rel, before)

	// Предыдущее значение текущего роста — в том же предложении, после значения и
	// до начала предложения базовой инфляции (если она в этом же предложении): иначе
	// «после» базовой можно было бы принять за предыдущее значение текущего роста.
	if q.Headline != nil {
		if i := pressPercentIndexAfter(sentence, rel); i >= 0 {
			tail := sentence[i:]
			if before > 0 && before > i {
				tail = sentence[i:before]
			}
			q.HeadlinePrev, q.HeadlinePrev2 = pressPrevValue(tail)
		}
	}

	// Базовая инфляция — своё предложение (в 2024-09-13 оно отдельное, с «находился
	// на уровне» без «после»: тогда предыдущего значения нет).
	if coreAt := strings.Index(strings.ToLower(text), "базовой инфляции"); coreAt >= 0 {
		coreStart, coreEnd := pressSentenceRange(text, coreAt)
		coreSentence := text[coreStart:coreEnd]
		coreRel := coreAt - coreStart
		if q.Core = pressFirstPercentAfter(coreSentence, coreRel, -1); q.Core != nil {
			if i := pressPercentIndexAfter(coreSentence, coreRel); i >= 0 {
				q.CorePrev, q.CorePrev2 = pressPrevValue(coreSentence[i:])
			}
		}
	}

	// Годовая инфляция — только факт («составила», «увеличилась … до»), не прогноз
	// («сложится в диапазоне»); дата оценки — «по оценке на 19 октября».
	for _, sentence := range pressSentences(text) {
		if !pressAnnualRe.MatchString(sentence) {
			continue
		}
		if value := pressAnnualValue(sentence); value != nil {
			q.AnnualInflation = value
			if m := pressAsofRe.FindStringSubmatch(sentence); m != nil {
				if asof, err := pressAsofDate(m[1], m[2], meeting); err == nil {
					q.AnnualAsof = &asof
				}
			}
			break
		}
	}

	q.Parsed = q.Headline != nil && q.Core != nil
	if !q.Parsed {
		log.Warnf("%s: квартальный блок %s: стандартная пара «текущий рост/базовая» не разобрана — числа NULL, текст сохранён",
			pressName, meeting.Format(pressDayLayout))
	}
	return q
}

// pressPercentIndexAfter — позиция конца первого процента не раньше from: с неё
// начинается хвост с «после …» (граница значения, а не самого числа).
func pressPercentIndexAfter(s string, from int) int {
	for _, m := range pressPercentRe.FindAllStringSubmatchIndex(s, -1) {
		if m[0] < from {
			continue
		}
		return m[1]
	}
	return -1
}

// pressSentences — предложения текста: разделитель «. » (точек в числах нет).
func pressSentences(text string) []string {
	var out []string
	for _, s := range strings.Split(text, ". ") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// pressPeriod — период квартального блока в именительном падеже: «3к26», «июль»,
// «октябрь — ноябрь», «II квартал 2024». Снимаются только служебные «В»/«В среднем
// за»/«года» и падеж месяца: сам период остаётся дословным.
func pressPeriod(prefix string) string {
	p := normMPText(prefix)
	p = strings.TrimPrefix(p, "В среднем за ")
	p = strings.TrimPrefix(p, "В ")
	p = strings.TrimSuffix(p, " года")
	p = strings.ReplaceAll(p, "квартале", "квартал")
	p = pressMonthForms.Replace(p)
	return strings.TrimSpace(p)
}

// pressQuarterForm — форма сравнения: quarter («3к26», «II квартал 2024»), month
// («июль», «октябрь — ноябрь») или none (чисел нет / период не распознан).
func pressQuarterForm(period string) string {
	if pressQuarterFormRe.MatchString(period) {
		return pressFormQuarter
	}
	low := strings.ToLower(period)
	for _, month := range pressMonthNominative {
		if strings.Contains(low, month) {
			return pressFormMonth
		}
	}
	return pressFormNone
}

// pressAsofDate — дата оценки годовой инфляции: день и месяц из «по оценке на 19
// октября», год — год заседания; месяц больше месяца заседания означает прошлый год
// (январское заседание оценивает декабрь прошлого года).
func pressAsofDate(dayRaw, monthRaw string, meeting time.Time) (time.Time, error) {
	month, ok := mpMonths[monthRaw]
	if !ok {
		return time.Time{}, fmt.Errorf("месяц %q не распознан", monthRaw)
	}
	day, err := strconv.Atoi(dayRaw)
	if err != nil {
		return time.Time{}, fmt.Errorf("день %q: %w", dayRaw, err)
	}
	year := meeting.Year()
	if month > meeting.Month() {
		year--
	}
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC), nil
}

// pressNumber — число источника: десятичная запятая.
func pressNumber(s string) (float64, error) {
	return strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(s), ",", "."), 64)
}
