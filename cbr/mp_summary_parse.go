package cbr

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
)

// mpSummaryRef — карточка индекса материалов заседаний с резюме обсуждения ключевой ставки:
// дата публикации (document-regular_date) и href в том виде, как он записан в разметке
// (относительный; домен навешивает вызывающий — как в util.FindLink).
type mpSummaryRef struct {
	Published time.Time
	Href      string
}

// mpSummaryBlock — одна текстовая единица страницы резюме: абзац (Kind = paragraph) или
// пункт списка (Kind = bullet). Seq — сквозной номер внутри блока, Subsection — подзаголовок
// div.title_20 («Основные факты»/«Дискуссия»; в блоках 4–5 подзаголовков нет — пустая строка).
type mpSummaryBlock struct {
	Block      string
	Subsection string
	Kind       string
	Seq        int
	Text       string
}

// mpSummaryDoc — решение и полный текст страницы резюме. EffectiveFrom = nil, если дата
// вступления в силу в документе не указана (всегда у hold, у части cut/hike).
type mpSummaryDoc struct {
	MeetingDate   time.Time
	Action        string
	Rate          float64
	EffectiveFrom *time.Time
	Signal        string
	Blocks        []mpSummaryBlock
}

const (
	mpSummaryCardTitle  = "Резюме обсуждения ключевой ставки"
	mpSummaryHrefMarker = "summary_key_rate_"
	mpSummaryCardSel    = "div.document-regular"
	mpSummaryNameSel    = ".document-regular_name_visible"
	mpSummaryDateSel    = ".document-regular_date"

	// mpBlockConclusion — пятый блок: в нём решение по ключевой ставке и сигнал.
	mpBlockConclusion = "Выводы для денежно-кредитной политики и решение по ключевой ставке"

	mpKindParagraph = "paragraph"
	mpKindBullet    = "bullet"

	mpIndexDateLayout = "02.01.2006"
)

// mpSummaryBlocks — пять обязательных h2-блоков страницы резюме в порядке следования. Всё
// прочее игнорируется, в том числе «Дополнительные материалы» (h2 в 23092026 против
// h2.h3.materials_title внутри div.materials в 06112024/07052026).
var mpSummaryBlocks = []string{
	"Ситуация в экономике и инфляция",
	"Денежно-кредитные условия",
	"Внешние условия",
	"Инфляционные риски",
	mpBlockConclusion,
}

// mpSummaryBlockSet — те же имена для проверки принадлежности h2 пяти блокам.
var mpSummaryBlockSet = func() map[string]bool {
	set := make(map[string]bool, len(mpSummaryBlocks))
	for _, name := range mpSummaryBlocks {
		set[name] = true
	}
	return set
}()

var (
	// mpDecisionRe — закреплённое ядро решения: дата заседания и глагол действия. Начало фразы
	// варьируется («По итогам дискуссии…», «По итогам состоявшейся дискуссии…»), но ядро
	// «Совет директоров Банка России <D> <месяц> <YYYY> года <глагол>» стабильно во всех
	// трёх эпохах (hold/hike/cut).
	mpDecisionRe = regexp.MustCompile(`Совет\s+директоров\s+Банка\s+России\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+года\s+(сохранил|повысил|снизил)`)
	// mpLevelSetRe — вторая закреплённая форма того же ядра: направления в ней нет,
	// «принял решение установить ставку на уровне». Живая проба приёмки шага 1.2 (2026-10-14)
	// нашла её на двух страницах 2024 года — 27022024 (16.02.2024) и 01042024 (22.03.2024);
	// в реестре KEY_RATE_CHANNELS.md §1 оба заседания — «сохранить». Уровень объявлен «на уровне»,
	// а не «до»: «до» ЦБ пишет только о смене («повысил … до 21,00% годовых», «снизил … до
	// 14,50% годовых»), «на уровне» — о неизменном («сохранил … на уровне 14,00% годовых»),
	// поэтому форма читается как hold, а «с <дата>» в ней отбрасывается тем же правилом, что у
	// hold, — у сохранения ставки дата вступления срока не имеет. «Установить ставку до …%»
	// намеренно не распознаётся: это была бы смена уровня, и её разбор — отдельное решение.
	mpLevelSetRe = regexp.MustCompile(`Совет\s+директоров\s+Банка\s+России\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+года\s+принял\s+решение\s+установить\s+ставку\s+на\s+уровне`)
	// mpVerbRe — резервная форма: предложение с глаголом решения, но без узнаваемого ядра
	// («Банк России снизил ключевую ставку…»); дату заседания тогда даёт mpMeetingFallbackRe.
	mpVerbRe = regexp.MustCompile(`(сохранил|повысил|снизил)\s+ключевую\s+ставку`)
	// mpRateRe — уровень ставки после глагола: «14,00% годовых» (дробная часть может
	// отсутствовать: «16% годовых»).
	mpRateRe = regexp.MustCompile(`(\d+(?:,\d+)?)\s*%\s*годовых`)
	// mpEffectiveRe — дата вступления в силу после уровня ставки: «с 28 октября 2024 года».
	mpEffectiveRe = regexp.MustCompile(`с\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+года`)
	// mpMeetingFallbackRe — фраза-дублёр даты заседания в шапке страницы: «Суждения участников
	// основаны на данных, доступных к 11 сентября 2026 года» (в 07052026 — «доступных на»).
	mpMeetingFallbackRe = regexp.MustCompile(`данных,\s+доступных\s+(?:к|на)\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+года`)
)

// mpLevelSetAction — действие решения формы «принял решение установить ставку на уровне»
// (mpLevelSetRe): направления в формулировке нет, «на уровне» означат неизменный уровень.
const mpLevelSetAction = "hold"

// mpVerbAction — действие по глаголу решения.
var mpVerbAction = map[string]string{
	"сохранил": "hold",
	"повысил":  "hike",
	"снизил":   "cut",
}

// mpMonths — русские названия месяцев в родительном падеже, как они стоят в датах ЦБ.
var mpMonths = map[string]time.Month{
	"января":   time.January,
	"февраля":  time.February,
	"марта":    time.March,
	"апреля":   time.April,
	"мая":      time.May,
	"июня":     time.June,
	"июля":     time.July,
	"августа":  time.August,
	"сентября": time.September,
	"октября":  time.October,
	"ноября":   time.November,
	"декабря":  time.December,
}

// normMPText нормализует дословный текст резюме: любая серия пробельных символов (включая
// NBSP U+00A0, узкий NBSP U+202F и «волосяной» пробел U+200A, встречающийся вокруг тире)
// превращается в один обычный пробел, неразрывный дефис U+2011 — в ASCII-дефис, края
// обрезаются. Сочетаемые знаки (U+0301 в «бóльшая») сохраняются: текст должен остаться
// дословным, иначе сломается сверка цитат LLM-слоя (Task 5).
func normMPText(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	space := false
	for _, r := range s {
		if r == '\u2011' {
			r = '-'
		}
		if unicode.IsSpace(r) {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	return b.String()
}

// parseMPDecIndex разбирает индекс материалов заседаний
// (https://www.cbr.ru/dkp/mp_dec/decision_key_rate/): карточки «Резюме обсуждения ключевой
// ставки» в порядке страницы (от новых к старым) с датой публикации. Карточка отбирается по
// видимому названию (span.document-regular_name_visible), а не по точной строке класса: у
// шести карточек span записан как «document-regular_name_visible referenceable» + id.
// Ноль совпадений — ошибка: структура страницы изменилась, тихая пустая дельта недопустима.
func parseMPDecIndex(page []byte) ([]mpSummaryRef, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("индекс: разбор html: %w", err)
	}
	var (
		refs    []mpSummaryRef
		dateErr error
	)
	doc.Find(mpSummaryCardSel).Each(func(_ int, card *goquery.Selection) {
		href, ok := card.Find(`a[href*="` + mpSummaryHrefMarker + `"]`).First().Attr("href")
		if !ok || strings.TrimSpace(href) == "" {
			return
		}
		if normMPText(card.Find(mpSummaryNameSel).First().Text()) != mpSummaryCardTitle {
			return
		}
		raw := normMPText(card.Find(mpSummaryDateSel).First().Text())
		published, err := time.Parse(mpIndexDateLayout, raw)
		if err != nil {
			dateErr = fmt.Errorf("индекс: карточка %s: дата публикации %q: %w", href, raw, err)
			return
		}
		refs = append(refs, mpSummaryRef{Published: published, Href: href})
	})
	if dateErr != nil {
		return nil, dateErr
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("индекс: не найдено ни одной карточки «%s»", mpSummaryCardTitle)
	}
	return refs, nil
}

// parseMPSummary разбирает страницу резюме обсуждения: пять обязательных h2-блоков, текстовые
// узлы p/li в порядке следования с подзаголовками div.title_20, а из пятого блока — решение
// (дата заседания, действие, уровень ставки, дата вступления) и дословный сигнал. Любое
// отклонение структуры — ошибка до записи (fail-fast), а не частичный результат.
func parseMPSummary(page []byte) (mpSummaryDoc, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return mpSummaryDoc{}, fmt.Errorf("резюме: разбор html: %w", err)
	}

	var (
		blocks []mpSummaryBlock
		header []string // абзацы до первого h2: фраза-дублёр даты заседания
		counts = make(map[string]int, len(mpSummaryBlocks))
		block  string
		sub    string
		seq    int
	)
	// Линейный обход документа: h2 открывает блок (неизвестный — закрывает разбор контента),
	// div.title_20 меняет подзаголовок, p/li — текст. Пустые узлы пропускаются: в 06112024
	// часть <li> закрыта тегом </p>, и html-парсер создаёт из них пустые <p>, которых в
	// исходной разметке не было.
	doc.Find("h2, div.title_20, p, li").Each(func(_ int, node *goquery.Selection) {
		switch goquery.NodeName(node) {
		case "h2":
			title := normMPText(node.Text())
			if !mpSummaryBlockSet[title] {
				block, sub, seq = "", "", 0
				return
			}
			counts[title]++
			block, sub, seq = title, "", 0
			return
		case "div":
			sub = normMPText(node.Text())
			return
		}
		text := normMPText(node.Text())
		if text == "" {
			return
		}
		if block == "" {
			if goquery.NodeName(node) == "p" {
				header = append(header, text)
			}
			return
		}
		kind := mpKindParagraph
		if goquery.NodeName(node) == "li" {
			kind = mpKindBullet
		}
		seq++
		blocks = append(blocks, mpSummaryBlock{Block: block, Subsection: sub, Kind: kind, Seq: seq, Text: text})
	})

	for _, name := range mpSummaryBlocks {
		switch counts[name] {
		case 0:
			return mpSummaryDoc{}, fmt.Errorf("резюме: не найден блок %q", name)
		case 1:
		default:
			return mpSummaryDoc{}, fmt.Errorf("резюме: блок %q встречается %d раза", name, counts[name])
		}
	}
	for _, name := range mpSummaryBlocks {
		if !mpHasBlock(blocks, name) {
			return mpSummaryDoc{}, fmt.Errorf("резюме: в блоке %q нет ни одного абзаца", name)
		}
	}

	out, err := mpParseDecision(blocks, strings.Join(header, "\n"))
	if err != nil {
		return mpSummaryDoc{}, err
	}
	out.Blocks = blocks
	return out, nil
}

// mpHasBlock — есть ли в разобранном документе хотя бы одна текстовая единица блока name.
func mpHasBlock(blocks []mpSummaryBlock, name string) bool {
	for _, b := range blocks {
		if b.Block == name {
			return true
		}
	}
	return false
}

// mpParseDecision извлекает решение по ключевой ставке из пятого блока и собирает сигнал —
// дословные предложения этого блока со словом «сигнал» (регистронезависимо).
func mpParseDecision(blocks []mpSummaryBlock, header string) (mpSummaryDoc, error) {
	var conclusion []mpSummaryBlock
	for _, b := range blocks {
		if b.Block == mpBlockConclusion {
			conclusion = append(conclusion, b)
		}
	}
	meeting, action, rate, effective, err := mpFindDecision(conclusion, header)
	if err != nil {
		return mpSummaryDoc{}, err
	}
	return mpSummaryDoc{
		MeetingDate:   meeting,
		Action:        action,
		Rate:          rate,
		EffectiveFrom: effective,
		Signal:        mpSignal(conclusion),
	}, nil
}

// mpDecisionMatch — закреплённое ядро решения в предложении: сначала форма с глаголом
// направления, затем форма «принял решение установить ставку на уровне». Возвращает подматчи
// ядра (группы 1–3 — дата заседания, m[0] — якорь для ставки) и действие; nil, если ядра нет.
func mpDecisionMatch(sent string) ([]string, string) {
	if m := mpDecisionRe.FindStringSubmatch(sent); m != nil {
		return m, mpVerbAction[m[4]]
	}
	if m := mpLevelSetRe.FindStringSubmatch(sent); m != nil {
		return m, mpLevelSetAction
	}
	return nil, ""
}

// mpFindDecision ищет предложение с решением: сначала по закреплённому ядру, затем (если
// ядро не встретилось) по глаголу — тогда дата заседания берётся из фразы-дублёра в шапке.
func mpFindDecision(conclusion []mpSummaryBlock, header string) (time.Time, string, float64, *time.Time, error) {
	for _, b := range conclusion {
		for _, sent := range mpSentences(b.Text) {
			m, action := mpDecisionMatch(sent)
			if m == nil {
				continue
			}
			meeting, err := mpTextDate(m[1], m[2], m[3])
			if err != nil {
				return time.Time{}, "", 0, nil, fmt.Errorf("резюме: дата заседания: %w", err)
			}
			rate, rest, err := mpParseRate(sent, m[0])
			if err != nil {
				return time.Time{}, "", 0, nil, err
			}
			effective, err := mpParseEffective(rest, action)
			if err != nil {
				return time.Time{}, "", 0, nil, err
			}
			return meeting, action, rate, effective, nil
		}
	}
	for _, b := range conclusion {
		for _, sent := range mpSentences(b.Text) {
			m := mpVerbRe.FindStringSubmatch(sent)
			if m == nil {
				continue
			}
			fm := mpMeetingFallbackRe.FindStringSubmatch(header)
			if fm == nil {
				return time.Time{}, "", 0, nil, fmt.Errorf("резюме: не распознана дата заседания (%q)", sent)
			}
			meeting, err := mpTextDate(fm[1], fm[2], fm[3])
			if err != nil {
				return time.Time{}, "", 0, nil, fmt.Errorf("резюме: дата заседания: %w", err)
			}
			rate, rest, err := mpParseRate(sent, m[0])
			if err != nil {
				return time.Time{}, "", 0, nil, err
			}
			action := mpVerbAction[m[1]]
			effective, err := mpParseEffective(rest, action)
			if err != nil {
				return time.Time{}, "", 0, nil, err
			}
			return meeting, action, rate, effective, nil
		}
	}
	return time.Time{}, "", 0, nil, fmt.Errorf("резюме: не распознано решение по ключевой ставке")
}

// mpParseRate берёт уровень ставки из части предложения после ядра решения (after) и
// возвращает остаток предложения — в нём ищется дата вступления в силу.
func mpParseRate(sent, after string) (float64, string, error) {
	idx := strings.Index(sent, after)
	if idx < 0 {
		return 0, "", fmt.Errorf("резюме: ядро решения %q не найдено в предложении %q", after, sent)
	}
	rest := sent[idx+len(after):]
	m := mpRateRe.FindStringSubmatchIndex(rest)
	if m == nil {
		return 0, "", fmt.Errorf("резюме: не распознан уровень ключевой ставки в решении (%q)", sent)
	}
	rate, err := strconv.ParseFloat(strings.Replace(rest[m[2]:m[3]], ",", ".", 1), 64)
	if err != nil {
		return 0, "", fmt.Errorf("резюме: уровень ключевой ставки %q: %w", rest[m[2]:m[3]], err)
	}
	return rate, rest[m[1]:], nil
}

// mpParseEffective — дата вступления в силу «с <дата>» после уровня ставки. У hold её не
// бывает (сохранение ставки срока не имеет), у части cut/hike она не указана — тогда nil.
func mpParseEffective(rest, action string) (*time.Time, error) {
	if action == "hold" {
		return nil, nil
	}
	m := mpEffectiveRe.FindStringSubmatch(rest)
	if m == nil {
		return nil, nil
	}
	date, err := mpTextDate(m[1], m[2], m[3])
	if err != nil {
		return nil, fmt.Errorf("резюме: дата вступления в силу: %w", err)
	}
	return &date, nil
}

// mpSignal — дословные предложения пятого блока со словом «сигнал», склеенные переводом
// строки в порядке документа.
func mpSignal(conclusion []mpSummaryBlock) string {
	var out []string
	for _, b := range conclusion {
		for _, sent := range mpSentences(b.Text) {
			if strings.Contains(strings.ToLower(sent), "сигнал") {
				out = append(out, sent)
			}
		}
	}
	return strings.Join(out, "\n")
}

// mpSentences разбивает абзац или пункт на предложения: граница — точка, за которой (через
// пробелы) идёт заглавная буква или кавычка. «14,00% годовых» и «б.п.,» предложений не
// разрывают; нужно, чтобы сигнал содержал только предложения про сигнал, а не весь абзац.
func mpSentences(text string) []string {
	runes := []rune(text)
	var out []string
	start := 0
	for i := 0; i < len(runes); i++ {
		if runes[i] != '.' {
			continue
		}
		j := i + 1
		for j < len(runes) && runes[j] == ' ' {
			j++
		}
		if j >= len(runes) || !mpSentenceStart(runes[j]) {
			continue
		}
		out = append(out, strings.TrimSpace(string(runes[start:j])))
		start = j
		i = j - 1
	}
	if tail := strings.TrimSpace(string(runes[start:])); tail != "" {
		out = append(out, tail)
	}
	return out
}

// mpSentenceStart — начинает ли руна новое предложение после точки: заглавная буква
// (в том числе «Ё» и латиница) или открывающая кавычка.
func mpSentenceStart(r rune) bool {
	return unicode.IsUpper(r) || r == '«' || r == '"'
}

// mpTextDate — дата из словесной формы «11 сентября 2026» (UTC).
func mpTextDate(day, month, year string) (time.Time, error) {
	d, err := strconv.Atoi(day)
	if err != nil {
		return time.Time{}, fmt.Errorf("день %q: %w", day, err)
	}
	mo, ok := mpMonths[strings.ToLower(month)]
	if !ok {
		return time.Time{}, fmt.Errorf("неизвестный месяц %q", month)
	}
	y, err := strconv.Atoi(year)
	if err != nil {
		return time.Time{}, fmt.Errorf("год %q: %w", year, err)
	}
	return time.Date(y, mo, d, 0, 0, 0, 0, time.UTC), nil
}
