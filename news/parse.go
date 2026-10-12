package news

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"
)

// rssItem — позиция ленты в том подмножестве RSS, которое несёт news_events.
// Теги — имена элементов без префиксов: раскладка RSS 2.0 отдаёт их в default
// namespace, и лишние поля лент (description, category, rbc_news:*) нам не нужны —
// описание позиции витрина не публикует, а качать его в память незачем.
type rssItem struct {
	Title   string `xml:"title"`
	Link    string `xml:"link"`
	PubDate string `xml:"pubDate"`
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
// (date, source, title, url). Дата хранит офсет источника: момент публикации
// из ленты, а не пересчёт в МСК — день по МСК считает витрина (date_msk).
type newsItem struct {
	Date   time.Time
	Source string
	Title  string
	URL    string
}

// pubDateLayouts — раскладки pubDate в порядке проверки: сначала полная форма
// с секундами и числовым офсетом (её отдают все пять наших лент), затем та же
// форма без секунд (старые фиды с минутной точностью), затем RFC1123 с
// именованной зоной. Раскладки взаимоисключающие — секунды и вид офсета
// различают их однозначно, — а перечень идёт от самой информативной формы
// к самой бедной, чтобы время не теряло точность при нестрогом совпадении.
var pubDateLayouts = []string{
	time.RFC1123Z,
	"Mon, 02 Jan 2006 15:04 -0700",
	time.RFC1123,
}

// parsePubDate разбирает pubDate одной из поддерживаемых раскладок. Вторая
// возвращаемая величина — признак успеха: нераспознанная дата не ошибка парсинга
// всей ленты, а битая позиция (её считает skipped). Третий формат даты из фида РБК
// (элемент rbc_news:date, «12.10.2026») осознанно не поддержан: день без времени
// и без зоны потерял бы порядок новостей внутри дня, а настоящий pubDate в ленте есть.
func parsePubDate(s string) (time.Time, bool) {
	for _, layout := range pubDateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
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
		items = append(items, newsItem{
			Date:   date,
			Source: source,
			Title:  title,
			// Пустой link — не пропуск: заголовок без ссылки всё ещё событие тезиса.
			URL: strings.TrimSpace(it.Link),
		})
	}
	if len(items) == 0 {
		return nil, skipped, fmt.Errorf("пустая лента: нет распознанных позиций (item-ов %d)", len(doc.Channel.Items))
	}
	return items, skipped, nil
}
