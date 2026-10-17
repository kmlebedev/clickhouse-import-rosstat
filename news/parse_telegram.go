package news

import (
	"bytes"
	"fmt"
	stdhtml "html"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// telegramMessage — одно сообщение публичного веб-превью канала. Channel и MessageID
// взяты из data-post (канал/id): по ним T2 сопоставляет сообщение с сохранёнными
// версиями, а URL собирается из тех же полей — ссылка на пост живёт в Telegram,
// а не в разметке превью.
type telegramMessage struct {
	Channel   string
	MessageID uint64
	Date      time.Time
	Title     string
	Text      string
	URL       string
}

// telegramPage — разобранное окно канала. SeenIDs несёт ID всех сообщений, у которых
// удалось прочитать data-post, включая пропущенные (медиа без текста, битая дата):
// иначе T2 объявил бы такое сообщение удалённым, хотя оно просто не имеет текста.
// Skipped — число узлов сообщений, не ставших позицией; ненулевое значение в логе —
// сигнал, что разметка или данные сообщения изменились.
type telegramPage struct {
	Messages []telegramMessage
	SeenIDs  map[uint64]struct{}
	Skipped  int
}

// Селекторы и имена атрибутов превью t.me/s/<channel> — константы, а не литералы
// на месте: смена разметки должна ронять тест парсера, а не давать тихий ноль.
const (
	telegramMessageSelector = "div.tgme_widget_message[data-post]"
	telegramTimeSelector    = "time[datetime]"
	telegramTextSelector    = "div.tgme_widget_message_text"

	telegramPostAttr     = "data-post"
	telegramDatetimeAttr = "datetime"

	// telegramTitleRunes — предел title в рунах. Первая строка молнии обычно короче;
	// предел нужен для дайджестов, где первая «строка» — весь абзац.
	telegramTitleRunes = 300
)

// Предварительно собранные выражения: парсер вызывается на каждое сообщение окна,
// и компиляция регулярки на строку была бы лишней работой на ровном месте.
var (
	telegramBreakRe     = regexp.MustCompile(`(?i)<br\s*/?>`)
	telegramBlockEndRe  = regexp.MustCompile(`(?i)</(?:p|div)>`)
	telegramTagRe       = regexp.MustCompile(`<[^>]*>`)
	telegramBlankRe     = regexp.MustCompile(`\n{3,}`)
	telegramSignatureRe = regexp.MustCompile(`^@[A-Za-z0-9_]{3,}$`)
)

// parseTelegramPage разбирает HTML публичного веб-превью канала
// (https://t.me/s/<channel>). source — ключ ленты (interfax_tg): он попадает в текст
// ошибки, чтобы сбой разметки был отнесён к ленте, а не к безымянной странице.
//
// Ноль узлов сообщений — ошибка (чужая страница, заглушка WAF, сменившийся селектор),
// и ноль распознанных сообщений — тоже ошибка (как пустая RSS-лента): пустой успех
// неотличим от сбоя источника. Отдельные битые сообщения не валят окно — они
// считаются в Skipped, но их ID (если data-post разобрался) остаются в SeenIDs.
func parseTelegramPage(source string, page []byte) (telegramPage, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return telegramPage{}, fmt.Errorf("%s: разбор html Telegram: %w", source, err)
	}

	nodes := doc.Find(telegramMessageSelector)
	if nodes.Length() == 0 {
		return telegramPage{}, fmt.Errorf(
			"%s: страница не похожа на превью Telegram: нет сообщений (%s), байт %d",
			source, telegramMessageSelector, len(page))
	}

	parsed := telegramPage{
		Messages: make([]telegramMessage, 0, nodes.Length()),
		SeenIDs:  make(map[uint64]struct{}, nodes.Length()),
	}
	nodes.Each(func(_ int, node *goquery.Selection) {
		post, ok := node.Attr(telegramPostAttr)
		if !ok || strings.TrimSpace(post) == "" {
			parsed.Skipped++
			return
		}
		channel, messageID, hasID, ok := parseTelegramPost(post)
		// ID запоминается до проверки остальных полей: медиа без текста или битая
		// дата не означают, что сообщение исчезло из окна.
		if hasID {
			parsed.SeenIDs[messageID] = struct{}{}
		}
		if !ok {
			parsed.Skipped++
			return
		}

		rawDate, ok := node.Find(telegramTimeSelector).First().Attr(telegramDatetimeAttr)
		if !ok {
			parsed.Skipped++
			return
		}
		date, err := time.Parse(time.RFC3339, strings.TrimSpace(rawDate))
		if err != nil {
			parsed.Skipped++
			return
		}

		textNode := node.Find(telegramTextSelector).First()
		if textNode.Length() == 0 {
			// Медиа без подписи: позиции нет, но ID уже в SeenIDs.
			parsed.Skipped++
			return
		}
		text, err := parseTelegramText(textNode)
		if err != nil || text == "" {
			parsed.Skipped++
			return
		}
		title := telegramTitle(text, telegramTitleRunes)
		if title == "" {
			parsed.Skipped++
			return
		}

		parsed.Messages = append(parsed.Messages, telegramMessage{
			Channel:   channel,
			MessageID: messageID,
			Date:      date,
			Title:     title,
			Text:      text,
			URL:       "https://t.me/" + channel + "/" + strconv.FormatUint(messageID, 10),
		})
	})

	if len(parsed.Messages) == 0 {
		return telegramPage{}, fmt.Errorf(
			"%s: пустая страница: нет распознанных сообщений (сообщений %d, пропущено %d)",
			source, nodes.Length(), parsed.Skipped)
	}
	return parsed, nil
}

// parseTelegramPost разбирает data-post формата «channel/id». hasID отделён от ok:
// у битого поста с читаемым числом (например, пустой канал) ID всё равно попадает
// в SeenIDs, чтобы T2 не принял его за удалённое сообщение.
func parseTelegramPost(post string) (channel string, id uint64, hasID, ok bool) {
	parts := strings.Split(post, "/")
	if len(parts) != 2 {
		return "", 0, false, false
	}
	channel = strings.TrimSpace(parts[0])
	id, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || id == 0 {
		return "", 0, false, false
	}
	return channel, id, true, channel != ""
}

// parseTelegramText превращает inner HTML текста сообщения в плоский текст.
// Разметка превью несёт только инлайн-теги (b/i/a/emoji) и <br/>; закрывающие
// </p> и </div> тоже дают перевод строки, потому что часть сообщений обёрнута
// в блочные контейнеры. Сущности раскрываются после снятия тегов: &quot; и &#036;
// не должны попасть в текст как есть. Подпись канала (@interfaxonline/@ifax_go)
// стоит последней строкой и удаляется — это подпись источника, а не текст молнии.
func parseTelegramText(node *goquery.Selection) (string, error) {
	inner, err := node.Html()
	if err != nil {
		return "", fmt.Errorf("внутренний HTML текста: %w", err)
	}
	text := telegramBreakRe.ReplaceAllString(inner, "\n")
	text = telegramBlockEndRe.ReplaceAllString(text, "\n")
	text = telegramTagRe.ReplaceAllString(text, "")
	text = stdhtml.UnescapeString(text)

	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")

	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	text = strings.TrimSpace(strings.Join(lines, "\n"))
	text = telegramBlankRe.ReplaceAllString(text, "\n\n")

	if idx := strings.LastIndexByte(text, '\n'); idx >= 0 {
		if telegramSignatureRe.MatchString(strings.TrimSpace(text[idx+1:])) {
			text = strings.TrimSpace(text[:idx])
		}
	} else if telegramSignatureRe.MatchString(text) {
		text = ""
	}
	return text, nil
}

// telegramTitle берёт первую непустую строку нормализованного текста и сжимает
// серии пробелов. Заголовок длиннее limit рун режется по последнему пробелу:
// обрывок слова в title хуже, чем чуть более короткий, но читаемый заголовок.
func telegramTitle(text string, limit int) string {
	for _, line := range strings.Split(text, "\n") {
		line = strings.Join(strings.Fields(line), " ")
		if line == "" {
			continue
		}
		runes := []rune(line)
		if len(runes) <= limit {
			return line
		}
		cut := string(runes[:limit])
		if i := strings.LastIndexByte(cut, ' '); i > 0 {
			cut = cut[:i]
		}
		return strings.TrimSpace(cut)
	}
	return ""
}

// telegramItems переводит разобранное окно в позиции news_events. Source задаёт
// вызывающий: Telegram-лента Интерфакса пишется под тем же source='interfax', что
// и его RSS, — источник один, а транспорт и канал различают url и поля версии.
func telegramItems(source string, page telegramPage) []newsItem {
	items := make([]newsItem, 0, len(page.Messages))
	for _, m := range page.Messages {
		items = append(items, newsItem{
			Date:        m.Date,
			Source:      source,
			Title:       m.Title,
			URL:         m.URL,
			Description: m.Text,
			ContentHash: contentHash(m.Title, m.Text),
			Channel:     m.Channel,
			MessageID:   m.MessageID,
		})
	}
	return items
}
