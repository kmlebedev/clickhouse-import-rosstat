package cbr

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// publicationCalendarTopic* — значения topic календаря: две вкладки cbr.ru/calendar/.
const (
	publicationCalendarTopicStatistics = "statistics"
	publicationCalendarTopicAnalytics  = "analytics"
)

// publicationCalendarDateRe — дата строки статистического окна; строки ежедневных
// таблиц («ежедневно по рабочим дням») под шаблон не подходят и пропускаются.
var publicationCalendarDateRe = regexp.MustCompile(`^\d{2}\.\d{2}\.\d{4}$`)

// publicationCalendarBlockRe — заголовок аналитического блока «Календарь публикации
// на октябрь 2026 года».
var publicationCalendarBlockRe = regexp.MustCompile(`(?i)^календарь публикации на\s+([а-яё]+)\s+(\d{4})\s+года$`)

// publicationCalendarRow — одна строка календаря. Time — nil, если ЦБ время не указал
// («—»); Title/Period/SourceURL входят в ключ ReplacingMergeTree.
type publicationCalendarRow struct {
	Date      time.Time
	Time      *string
	Title     string
	Topic     string
	Period    string
	SourceURL string
	Planned   uint8
}

// publicationCalendarParse — результат разбора страницы: строки и счётчики для лога
// и приёмки (173 статистических строки, 22 аналитических блока на снимке 2026-10-20).
type publicationCalendarParse struct {
	Rows           []publicationCalendarRow
	StatsRows      int
	AnalyticBlocks int
	AnalyticRows   int
}

// parsePublicationCalendar разбирает страницу cbr.ru/calendar/:
//   - вкладка t1: таблица table.data с заголовком «Дата публикации» — статистическое
//     окно; строки без конкретной даты (ежедневные таблицы) пропускаются;
//   - вкладка t2: 22 блока «Календарь публикации на <месяц> <год> года», в каждом —
//     строки (название, дата).
//
// planned считается от capturedAt: 1 — дата была в будущем или сегодня на момент
// снимка. Пустой обязательный разбор — ошибка: молчаливый «0 rows» скрыл бы смену
// разметки, а календарь остался бы без обновления.
func parsePublicationCalendar(page []byte, capturedAt time.Time) (publicationCalendarParse, error) {
	var result publicationCalendarParse
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return result, fmt.Errorf("%s: разбор страницы календаря: %w", cbrPublicationCalendarTable, err)
	}

	statsTable := findPublicationStatsTable(doc)
	if statsTable == nil {
		return result, fmt.Errorf("%s: на странице cbr.ru/calendar/ не найдена таблица «Дата публикации»", cbrPublicationCalendarTable)
	}
	statsTable.Find("tr").Each(func(_ int, tr *goquery.Selection) {
		cells := tr.Find("td")
		if cells.Length() < 3 {
			return
		}
		date, ok := parseCBRDate(normalizePublicationText(cells.Eq(0).Text()))
		if !ok {
			return
		}
		info := cells.Eq(2)
		title := normalizePublicationText(info.Find("a").First().Text())
		if title == "" {
			title = normalizePublicationText(info.Text())
		}
		period := normalizePublicationText(info.Find("br").NextAll().Text())
		if period == "" {
			period = strings.TrimSpace(strings.TrimPrefix(normalizePublicationText(info.Text()), title))
		}
		href, _ := info.Find("a").First().Attr("href")
		result.Rows = append(result.Rows, publicationCalendarRow{
			Date:      date,
			Time:      parsePublicationTime(normalizePublicationText(cells.Eq(1).Text())),
			Title:     title,
			Topic:     publicationCalendarTopicStatistics,
			Period:    period,
			SourceURL: resolveCBRURL(href),
			Planned:   plannedFlag(date, capturedAt),
		})
		result.StatsRows++
	})
	if result.StatsRows == 0 {
		return result, fmt.Errorf("%s: статистическое окно не дало ни одной даты", cbrPublicationCalendarTable)
	}

	doc.Find("#tab_content_t2 .dropdown").Each(func(_ int, block *goquery.Selection) {
		title := normalizePublicationText(block.Find("a.dropdown_title").First().Text())
		if !strings.HasPrefix(strings.ToLower(title), "календарь публикации на") {
			return
		}
		year, month, ok := parsePublicationCalendarPeriod(title)
		if !ok {
			return
		}
		period := fmt.Sprintf("%04d-%02d", year, month)
		result.AnalyticBlocks++
		block.Find("table.data tr").Each(func(_ int, tr *goquery.Selection) {
			cells := tr.Find("td")
			if cells.Length() < 3 {
				return
			}
			date, ok := parseCBRDate(normalizePublicationText(cells.Last().Text()))
			if !ok {
				return
			}
			titleCell := cells.Eq(1)
			rowTitle := normalizePublicationText(titleCell.Find("a").First().Text())
			if rowTitle == "" {
				rowTitle = normalizePublicationText(titleCell.Text())
			}
			href, _ := titleCell.Find("a").First().Attr("href")
			result.Rows = append(result.Rows, publicationCalendarRow{
				Date:      date,
				Title:     rowTitle,
				Topic:     publicationCalendarTopicAnalytics,
				Period:    period,
				SourceURL: resolveCBRURL(href),
				Planned:   plannedFlag(date, capturedAt),
			})
			result.AnalyticRows++
		})
	})
	if result.AnalyticBlocks == 0 {
		return result, fmt.Errorf("%s: на странице не найдено ни одного аналитического блока", cbrPublicationCalendarTable)
	}
	if result.AnalyticRows == 0 {
		return result, fmt.Errorf("%s: аналитические блоки не дали ни одной строки", cbrPublicationCalendarTable)
	}
	return result, nil
}

// findPublicationStatsTable выбирает таблицу статистического окна по заголовкам:
// у ежедневных таблиц заголовок «Дата»/«Показатели»/«Время публикации», у окна —
// «Дата публикации»/«Время публикации»/«Информация — показатель / период наблюдения».
func findPublicationStatsTable(doc *goquery.Document) *goquery.Selection {
	var found *goquery.Selection
	doc.Find("table.data").EachWithBreak(func(_ int, table *goquery.Selection) bool {
		headers := table.Find("th").Text()
		if strings.Contains(headers, "Дата публикации") && strings.Contains(headers, "Время публикации") {
			found = table
			return false
		}
		return true
	})
	return found
}

// parsePublicationCalendarPeriod возвращает год и месяц из заголовка аналитического
// блока. Названия месяца в заголовке — в именительном падеже («октябрь»).
func parsePublicationCalendarPeriod(title string) (year int, month time.Month, ok bool) {
	m := publicationCalendarBlockRe.FindStringSubmatch(strings.ToLower(normalizePublicationText(title)))
	if m == nil {
		return 0, 0, false
	}
	month, ok = russianMonth(m[1])
	if !ok {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(m[2], "%d", &year); err != nil {
		return 0, 0, false
	}
	return year, month, true
}

// parsePublicationTime нормализует время публикации: «—» и пустая ячейка — nil,
// «до 16:00» — «16:00». Неразобранное значение остаётся строкой как есть: это
// подпись источника, а не дата, и терять её в NULL без следа не нужно.
func parsePublicationTime(raw string) *string {
	raw = strings.TrimSpace(normalizePublicationText(raw))
	if raw == "" || raw == "—" || raw == "-" {
		return nil
	}
	raw = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(raw), "до "))
	if raw == "" {
		return nil
	}
	return &raw
}

// plannedFlag — 1, если дата не раньше дня снимка (в MSK). Сравниваются компоненты
// даты, а не моменты: publication_date приходит из Date без времени.
func plannedFlag(date, capturedAt time.Time) uint8 {
	y, m, d := capturedAt.In(mskZone).Date()
	today := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	if !date.Before(today) {
		return 1
	}
	return 0
}

// parseCBRDate разбирает дату ЦБ формата DD.MM.YYYY. Ошибка не возвращается: строки
// без конкретной даты — штатное содержимое страницы (ежедневные таблицы).
func parseCBRDate(raw string) (time.Time, bool) {
	if !publicationCalendarDateRe.MatchString(raw) {
		return time.Time{}, false
	}
	date, err := time.Parse("02.01.2006", raw)
	if err != nil {
		return time.Time{}, false
	}
	return date, true
}

// normalizePublicationText убирает NBSP, тонкие пробелы и схлопывает пробельные
// серии: ЦБ щедро расставляет &#160; внутри названий и периодов.
func normalizePublicationText(raw string) string {
	raw = strings.ReplaceAll(raw, "\u00a0", " ")
	raw = strings.ReplaceAll(raw, "\u202f", " ")
	return strings.Join(strings.Fields(raw), " ")
}

// resolveCBRURL достраивает относительную ссылку ЦБ до абсолютной.
func resolveCBRURL(href string) string {
	href = strings.TrimSpace(href)
	switch {
	case href == "":
		return cbrPublicationCalendarURL
	case strings.HasPrefix(href, "http://"), strings.HasPrefix(href, "https://"):
		return href
	case strings.HasPrefix(href, "/"):
		return cbrUrl + href
	default:
		return cbrUrl + "/" + href
	}
}
