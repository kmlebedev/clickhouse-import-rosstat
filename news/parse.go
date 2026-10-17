package news

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"strings"
	"time"
)

// rssItem — позиция ленты в том подмножестве RSS, которое несёт news_events.
// Теги — имена элементов без префиксов: раскладка RSS 2.0 отдаёт их в default
// namespace, и лишние поля лент (category, rbc_news:*) нам не нужны. description
// разбирается: у smart-lab это полный текст поста, ради которого лента и добавлена
// в контур ключевой ставки; у новостных лент — краткий анонс в том же элементе.
type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	PubDate     string `xml:"pubDate"`
	Description string `xml:"description"`
}

// rssDoc — документ RSS. Поле XMLName с тегом «rss» обязательно и является
// контрактом util.GetXML (carry-решение Task 1): без пина 200-страница HTML
// (заглушка WAF) разбирается в нулевую структуру без ошибки — тот же «тихий ноль»,
// от которого GetXML защищается проверкой статуса, только теперь с честным 200.
// С пином чужой корень даёт ошибку декодирования, и вызывающий (fetchFeeds) видит,
// что пришёл не тот документ, а не пустую ленту.
type rssDoc struct {
	XMLName xml.Name `xml:"rss"`
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

// newsItem — нормализованная позиция ленты, готовая к записи в news_events
// (date, source, title, url, description). Дата хранит офсет источника: момент
// публикации из ленты, а не пересчёт в МСК — день по МСК считает витрина (date_msk).
type newsItem struct {
	Date        time.Time
	Source      string
	Title       string
	URL         string
	Description string
	// ContentHash — sha256 от title+"\n"+description; ключ версии для LLM-разметки.
	// У RSS он тоже заполняется: разметка generic по url+content_hash.
	ContentHash string
	// Channel и MessageID несут только Telegram-позиции (channel/id из data-post);
	// у RSS они пустые/нулевые — адрес позиции задаёт URL.
	Channel   string
	MessageID uint64
}

// contentHash считает версионный ключ текста: смена заголовка или описания меняет
// hash, а перечитывание того же текста — нет. Разделитель "\n" исключает склейку
// разных пар ("ab","c" и "a","bc") в один ключ.
func contentHash(title, description string) string {
	sum := sha256.Sum256([]byte(title + "\n" + description))
	return hex.EncodeToString(sum[:])
}

// pubDateLayouts — три раскладки pubDate в порядке проверки, от полной формы
// к запасной: сначала форма с секундами и числовым офсетом (её отдают все семь
// наших лент), затем та же форма без секунд (старые фиды с минутной точностью),
// затем RFC1123 с именованной зоной — эту раскладку принимает только parsePubDate,
// и только для GMT/UTC. Берётся первая успешная: перечень идёт от самой
// информативной формы к самой бедной, чтобы время не теряло точность.
var pubDateLayouts = []string{
	time.RFC1123Z,
	"Mon, 02 Jan 2006 15:04 -0700",
	time.RFC1123,
}

// parsePubDate разбирает pubDate одной из поддерживаемых раскладок. Вторая
// возвращаемая величина — признак успеха: нераспознанная дата не ошибка парсинга
// всей ленты, а битая позиция (её считает skipped). Формат даты из фида РБК
// (элемент rbc_news:date, «12.10.2026») осознанно не поддержан: день без времени
// и без зоны потерял бы порядок новостей внутри дня, а настоящий pubDate в ленте есть.
func parsePubDate(s string) (time.Time, bool) {
	for _, layout := range pubDateLayouts {
		t, err := time.Parse(layout, s)
		if err != nil {
			continue
		}
		// Именованную зону раскладки RFC1123 принимаем только там, где она однозначна —
		// GMT/UTC. Прочую аббревиатуру time.Parse не отвергает: ищет её в локальной базе
		// зон, а не найдя — подставляет зоной с нулевым смещением. Момент из-за этого
		// молча сдвигается на величину настоящего смещения (для МСК — на 3 часа),
		// причём по-разному на разных хостах: TZ=Europe/Moscow разберёт «MSK» как +03:00,
		// TZ=UTC — как +00:00, то есть машина разработчика и контейнер импортёра дадут
		// разные времена. Такую дату считаем нераспознанной, и позиция уходит в skipped:
		// видимый пропуск честнее тихого сдвига времени, который в news_events уже
		// никак не отличить от верного.
		if layout == time.RFC1123 {
			if name, _ := t.Zone(); name != "GMT" && name != "UTC" {
				continue
			}
		}
		return t, true
	}
	return time.Time{}, false
}

// itemsFromDoc превращает разобранный документ RSS в позиции news_events.
// source задаёт вызывающий (обе ленты ЦБ пишутся под «cbr», различает их url).
//
// Позиция пропускается (skipped++), если дата не распознана или заголовок пуст
// после нормализации. Счётчик пропусков возвращается не для красоты: смена формы
// ленты — единственная причина, по которой позиции начинают теряться, и молчаливое
// «в ленте нет новостей» неотличимо от сбоя источника. Вызывающий печатает skipped
// в лог, и ненулевое значение там — сигнал посмотреть ленту глазами.
//
// Заголовок нормализуется в одну строку (strings.Fields + Join): витрина и логи
// читают заголовок как единое значение, а переносы строк и табуляции внутри
// элемента title встречаются и в SQL-литералах, и в grep по логу выглядят мусором.
// Ноль валидных позиций — ошибка: пустой ответ не должен выглядеть успешным
// прогоном (то же правило, что у GetXML для пустого тела).
func itemsFromDoc(source string, doc rssDoc) (items []newsItem, skipped int, err error) {
	for _, it := range doc.Channel.Items {
		title := strings.Join(strings.Fields(it.Title), " ")
		// pubDate приходит текстовым узлом: pretty-print ленты вправе оставить вокруг
		// даты переводы строк и отступы, а это та же дата, а не новая форма ленты.
		date, ok := parsePubDate(strings.TrimSpace(it.PubDate))
		if !ok || title == "" {
			skipped++
			continue
		}
		// Описание тримится по краям, но берётся как есть — с разметкой и переводами
		// строк: у smart-lab это полный текст поста. Пустое — не пропуск и не ошибка:
		// у части позиций элемента description в ленте нет.
		description := strings.TrimSpace(it.Description)
		items = append(items, newsItem{
			Date:   date,
			Source: source,
			Title:  title,
			// Пустой link — не пропуск: заголовок без ссылки всё ещё событие тезиса.
			URL:         strings.TrimSpace(it.Link),
			Description: description,
			ContentHash: contentHash(title, description),
		})
	}
	if len(items) == 0 {
		return nil, skipped, fmt.Errorf("пустая лента: нет распознанных позиций (item-ов %d)", len(doc.Channel.Items))
	}
	return items, skipped, nil
}
