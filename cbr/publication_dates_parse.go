package cbr

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// mskZone — московское время. FixedZone, а не LoadLocation: репозиторий уже
// использует эту конвенцию (forecast/resolve_test.go, edisclosure), а tzdata в
// контейнере может отсутствовать. Для fallback 2002–2013 час исторического DST
// несущественен на фоне месячного буфера.
var mskZone = time.FixedZone("MSK", 3*60*60)

// publicationPeriod — отчётный месяц релиза: ключ поиска даты в архиве ЦБ.
type publicationPeriod struct {
	Year  int
	Month time.Month
}

// archivePublication — версия архива: дата публикации, ссылка и приоритет при
// дублях периода. NeedsPage — HTML-версия без tooltip: дату надо взять со страницы.
type archivePublication struct {
	Date      time.Time
	SourceURL string
	Priority  int
	NeedsPage bool
}

// Приоритеты версий одного периода: HTML-страница с датой точнее PDF-tooltip,
// XLSX-версия точнее PDF у инфляционных ожиданий.
const (
	archivePriorityPDF  = 1
	archivePriorityXLSX = 2
	archivePriorityHTML = 3
)

var (
	inflExpArchiveRe = regexp.MustCompile(`(?i)(?:Infl_exp|inFOM)_(\d{2})-(\d{2})\.(xlsx|pdf)`)
	cpdPDFArchiveRe  = regexp.MustCompile(`(?i)(?:INF|CPD)_(\d{4})-(\d{1,2})\.pdf$`)
	cpdPageArchiveRe = regexp.MustCompile(`(?i)/CPD_(\d{4})-(\d{1,2})/$`)
	surveyArchiveRe  = regexp.MustCompile(`(?i)/monitoring/(\d{2})_?(\d{2})/?$`)
	russianDateRe    = regexp.MustCompile(`(?i)(\d{1,2})\s+([а-яё]+)\s+(\d{4})`)
)

// russianMonths — месяцы в именительном и родительном падежах: заголовки блоков
// календаря — «октябрь 2026 года», даты релизов — «14 октября 2026 года».
var russianMonths = map[string]time.Month{
	"январь": time.January, "января": time.January,
	"февраль": time.February, "февраля": time.February,
	"март": time.March, "марта": time.March,
	"апрель": time.April, "апреля": time.April,
	"май": time.May, "мая": time.May,
	"июнь": time.June, "июня": time.June,
	"июль": time.July, "июля": time.July,
	"август": time.August, "августа": time.August,
	"сентябрь": time.September, "сентября": time.September,
	"октябрь": time.October, "октября": time.October,
	"ноябрь": time.November, "ноября": time.November,
	"декабрь": time.December, "декабря": time.December,
}

// parseInflExpArchive разбирает список версий инфляционных ожиданий: XLSX/PDF с
// data-tooltip-content="Опубликовано <дата>", период — из имени файла ГГ-ММ.
// HTML-страницы версий (<a class="versions_item" href=".../Infl_exp_26-09/">) идут
// без расширения и пропускаются: точная дата есть у файлов.
func parseInflExpArchive(page []byte) (map[publicationPeriod]archivePublication, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("infl_exp: разбор страницы архива: %w", err)
	}
	result := make(map[publicationPeriod]archivePublication)
	doc.Find("a.versions_item").Each(func(_ int, link *goquery.Selection) {
		href, _ := link.Attr("href")
		m := inflExpArchiveRe.FindStringSubmatch(href)
		if m == nil {
			return
		}
		date, ok := parseRussianDate(link.AttrOr("data-tooltip-content", ""))
		if !ok {
			return
		}
		year, err := strconv.Atoi("20" + m[1])
		if err != nil {
			return
		}
		month, err := strconv.Atoi(m[2])
		if err != nil || month < 1 || month > 12 {
			return
		}
		priority := archivePriorityPDF
		if strings.EqualFold(m[3], "xlsx") {
			priority = archivePriorityXLSX
		}
		addArchivePublication(result, publicationPeriod{Year: year, Month: time.Month(month)}, date, resolveCBRURL(href), priority)
	})
	if len(result) == 0 {
		return nil, fmt.Errorf("infl_exp: в архиве не найдено ни одной версии с датой публикации")
	}
	return result, nil
}

// parseCPDArchive разбирает список версий «Динамики потребительских цен»: PDF
// INF_YYYY-MM.pdf с tooltip-датой (2016–2022) и HTML-страницы CPD_YYYY-M
// (2021+), у которых даты в списке нет — её берём со страницы версии.
func parseCPDArchive(page []byte) (map[publicationPeriod]archivePublication, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("cpd: разбор страницы архива: %w", err)
	}
	result := make(map[publicationPeriod]archivePublication)
	doc.Find("a.versions_item").Each(func(_ int, link *goquery.Selection) {
		href, _ := link.Attr("href")
		if m := cpdPDFArchiveRe.FindStringSubmatch(href); m != nil {
			date, ok := parseRussianDate(link.AttrOr("data-tooltip-content", ""))
			if !ok {
				return
			}
			year, err := strconv.Atoi(m[1])
			if err != nil {
				return
			}
			month, err := strconv.Atoi(m[2])
			if err != nil || month < 1 || month > 12 {
				return
			}
			addArchivePublication(result, publicationPeriod{Year: year, Month: time.Month(month)}, date, resolveCBRURL(href), archivePriorityPDF)
			return
		}
		if m := cpdPageArchiveRe.FindStringSubmatch(href); m != nil {
			year, err := strconv.Atoi(m[1])
			if err != nil {
				return
			}
			month, err := strconv.Atoi(m[2])
			if err != nil || month < 1 || month > 12 {
				return
			}
			period := publicationPeriod{Year: year, Month: time.Month(month)}
			if date, ok := parseRussianDate(link.AttrOr("data-tooltip-content", "")); ok {
				addArchivePublication(result, period, date, resolveCBRURL(href), archivePriorityHTML)
				return
			}
			addArchivePage(result, period, resolveCBRURL(href))
		}
	})
	if len(result) == 0 {
		return nil, fmt.Errorf("cpd: в архиве не найдено ни одной версии")
	}
	return result, nil
}

// parseSurveyArchive собирает ссылки версий мониторинга предприятий:
// monitoring/<MMYY> (2024+) и monitoring/<MM_YY> (2022–2023). Даты в списке нет:
// её берём со страницы версии. Возвращается map period→URL, чтобы не угадывать
// формат ссылки при повторном запросе.
func parseSurveyArchive(page []byte) (map[publicationPeriod]string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("cbr_survey: разбор страницы архива: %w", err)
	}
	result := make(map[publicationPeriod]string)
	doc.Find("a.versions_item").Each(func(_ int, link *goquery.Selection) {
		href, _ := link.Attr("href")
		m := surveyArchiveRe.FindStringSubmatch(href)
		if m == nil {
			return
		}
		month, err := strconv.Atoi(m[1])
		if err != nil || month < 1 || month > 12 {
			return
		}
		year, err := strconv.Atoi("20" + m[2])
		if err != nil {
			return
		}
		period := publicationPeriod{Year: year, Month: time.Month(month)}
		if _, ok := result[period]; !ok {
			result[period] = resolveCBRURL(href)
		}
	})
	if len(result) == 0 {
		return nil, fmt.Errorf("cbr_survey: в архиве мониторинга не найдено ни одной версии")
	}
	return result, nil
}

// parsePublicationPageDate берёт дату публикации со страницы версии ЦБ
// (.news-info-line_date, «14 октября 2026 года»). Нет блока/даты — false:
// вызывающий оставляет fallback и не роняет прогон.
func parsePublicationPageDate(page []byte) (time.Time, bool) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return time.Time{}, false
	}
	date, ok := parseRussianDate(doc.Find(".news-info-line_date").First().Text())
	if ok {
		return date, true
	}
	return time.Time{}, false
}

// parseRussianDate разбирает «14 октября 2026 года» и «Опубликовано 12 февраля 2016».
func parseRussianDate(raw string) (time.Time, bool) {
	m := russianDateRe.FindStringSubmatch(strings.ToLower(normalizePublicationText(raw)))
	if m == nil {
		return time.Time{}, false
	}
	day, err := strconv.Atoi(m[1])
	if err != nil {
		return time.Time{}, false
	}
	month, ok := russianMonth(m[2])
	if !ok {
		return time.Time{}, false
	}
	year, err := strconv.Atoi(m[3])
	if err != nil {
		return time.Time{}, false
	}
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if date.Day() != day || date.Month() != month {
		return time.Time{}, false
	}
	return date, true
}

// russianMonth возвращает месяц по имени или родительному падежу.
func russianMonth(raw string) (time.Month, bool) {
	month, ok := russianMonths[strings.ToLower(strings.TrimSpace(raw))]
	return month, ok
}

// addArchivePublication оставляет версию с большим приоритетом; при равном —
// более позднюю дату. Версия NeedsPage с датой уступает любой датированной.
func addArchivePublication(dst map[publicationPeriod]archivePublication, period publicationPeriod, date time.Time, sourceURL string, priority int) {
	existing, ok := dst[period]
	replace := !ok
	if ok {
		switch {
		case existing.NeedsPage && !date.IsZero():
			replace = true
		case priority > existing.Priority:
			replace = true
		case priority == existing.Priority && date.After(existing.Date):
			replace = true
		}
	}
	if replace {
		dst[period] = archivePublication{Date: date, SourceURL: sourceURL, Priority: priority}
	}
}

// addArchivePage запоминает HTML-версию без даты, не затирая уже известную дату.
func addArchivePage(dst map[publicationPeriod]archivePublication, period publicationPeriod, sourceURL string) {
	existing, ok := dst[period]
	if ok && !existing.Date.IsZero() {
		return
	}
	dst[period] = archivePublication{SourceURL: sourceURL, Priority: archivePriorityHTML, NeedsPage: true}
}

// dateOnlyPublishedAt — effective-момент для date-only: 23:59:59 MSK этой даты.
// Компоненты берутся из самого time.Time без пересчёта зоны: ClickHouse Date
// приходит в зоне сессии (+05:00 на dev-хосте), и .In(MSK) сдвинул бы день назад.
func dateOnlyPublishedAt(date time.Time) time.Time {
	y, m, d := date.Date()
	return time.Date(y, m, d, 23, 59, 59, 0, mskZone)
}

// fallbackPublishedAt — консервативный fallback: последний день M+1 23:59:59 MSK.
// Компоненты — из Date как есть, без .In(MSK): иначе Date, прочитанная в +05:00,
// уезжает на предыдущий день и fallback становится концом того же месяца.
func fallbackPublishedAt(periodDate time.Time) time.Time {
	y, m, _ := periodDate.Date()
	firstNext := time.Date(y, m+1, 1, 0, 0, 0, 0, mskZone)
	return firstNext.AddDate(0, 1, 0).Add(-time.Second)
}

// calendarRowChanged говорит, надо ли писать новую версию строки календаря.
// Дата/title/period/source_url — ключ ReplacingMergeTree; меняются только
// время, тема и planned.
func calendarRowChanged(existing, desired publicationCalendarRow) bool {
	if (existing.Time == nil) != (desired.Time == nil) {
		return true
	}
	if existing.Time != nil && *existing.Time != *desired.Time {
		return true
	}
	return existing.Topic != desired.Topic || existing.Planned != desired.Planned
}

// seriesPublicationChanged разрешает только апгрейд качества (fallback → date →
// exact_time) и смену даты в рамках того же качества. Понижение запрещено:
// источник мог временно отдать 404, и это не повод терять точную дату.
func seriesPublicationChanged(existing, desired util.SeriesPublication) bool {
	existingRank := publicationQualityRank(existing.Quality)
	desiredRank := publicationQualityRank(desired.Quality)
	if existingRank > desiredRank {
		return false
	}
	if existingRank < desiredRank {
		return true
	}
	return !existing.PublishedAt.Equal(desired.PublishedAt)
}

func publicationQualityRank(quality string) int {
	switch quality {
	case "exact_time":
		return 3
	case "date":
		return 2
	case "fallback":
		return 1
	default:
		return 0
	}
}
