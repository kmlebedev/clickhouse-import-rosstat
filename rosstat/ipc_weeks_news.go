package rosstat

// PDF-фолбэк rosstat_ipc_weeks: недельные PDF-выпуски «Об оценке индекса потребительских
// цен» из новостной ленты central-news. Нужен, когда Nedel_ipc.xlsx отдаёт 404 или не
// содержит свежих недель (сайт Росстата с 13.03.2026 отдаёт снимок марта). Таблица PDF —
// те же 108 строк «Индексы потребительских цен», колонка «к предыдущей дате регистрации»,
// что и недельный процент XLSX; проверено на 2026-10-26 и 2026-11-02.
//
// Механизм цикла — общий util.Fallback: он читает даты таблицы, оставляет недостающие
// документы и на каждый зовёт шаблон (DataUrl + BodyFunc), поэтому здесь только источник
// документов, разбор PDF и запись строк. Сеть/exec — в Documents/Parse, чистый разбор
// текста — ipcWeeksNewsParse (тесты офлайн, без poppler).

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// ipcWeeksNewsSearch — поисковый запрос ленты для недельных выпусков. Месячные релизы
	// ищутся по «Об индексе потребительских цен в …» и товарной таблицы не содержат.
	ipcWeeksNewsSearch = "Об оценке индекса потребительских цен"
	// ipcWeeksNewsPerPage — первая страница ленты: 20 карточек, из них свежие PDF — первые.
	ipcWeeksNewsPerPage = "20"
	// ipcWeeksNewsTitleMarker — маркер недельного титула: отсекает месячные карточки, если
	// поиск когда-нибудь начнёт их отдавать.
	ipcWeeksNewsTitleMarker = "об оценке индекса потребительских цен"
	// ipcWeeksNewsTableAnchor — якорь таблицы в тексте PDF: по нему разбор понимает, что
	// вступительный текст закончился. Повторяется в шапке каждой страницы.
	ipcWeeksNewsTableAnchor = "Индексы потребительских цен"
	// ipcWeeksNewsContext — общая часть сообщений разбора CPI-PDF.
	ipcWeeksNewsContext = rosstatIPCWeeksTable + ", PDF-выпуск новостной ленты"
	// ipcWeeksPetrolSearch — поисковый запрос ленты для PDF «О потребительских ценах на
	// нефтепродукты»: там три марки бензина, которых нет в CPI-PDF.
	ipcWeeksPetrolSearch = "О потребительских ценах на нефтепродукты"
	// ipcWeeksPetrolTitleMarker — маркер карточки нефтепродуктов.
	ipcWeeksPetrolTitleMarker = "о потребительских ценах на нефтепродукты"
	// ipcWeeksPetrolContext — общая часть сообщений разбора PDF нефтепродуктов.
	ipcWeeksPetrolContext = rosstatIPCWeeksTable + ", PDF «О потребительских ценах на нефтепродукты»"
	// ipcWeeksNewsKindCPI / ipcWeeksNewsKindPetrol — метка документа: у одной недели два
	// PDF разного формата, и Parse выбирает разбор по ней.
	ipcWeeksNewsKindCPI    = "cpi"
	ipcWeeksNewsKindPetrol = "petrol"
	// Три марки бензина: канонические имена XLSX; в CPI-PDF их нет, в PDF нефтепродуктов —
	// строки «марки АИ-92/95/98 и выше».
	ipcWeeksPetrolAI92 = "Бензин автомобильный марки АИ-92, л"
	ipcWeeksPetrolAI95 = "Бензин автомобильный марки АИ-95, л"
	ipcWeeksPetrolAI98 = "Бензин автомобильный марки АИ-98 и выше, л"
)

// ipcWeeksNewsRelease — карточка ленты: дата последнего дня недели из титула, заголовок,
// разрешённый URL и признак PDF (HTML-выпуски фолбэк не разбирает).
type ipcWeeksNewsRelease struct {
	Date  time.Time
	Title string
	URL   string
	PDF   bool
}

var (
	// ipcWeeksNewsDateRe — дата шапки таблицы: «2 ноября 2026 г.» или «2 ноября 2026 года».
	ipcWeeksNewsDateRe = regexp.MustCompile(`^(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+(?:г\.|года)$`)
	// ipcWeeksNewsRowRe — строка таблицы: имя и три значения. Первая колонка — недельный
	// индекс, вторая/третья — к концу месяца/года; парсеру нужна только первая. «…» —
	// пропуск данных: строка не обрывает таблицу, но точкой не становится.
	ipcWeeksNewsRowRe = regexp.MustCompile(`^(.+?)\s+(…|\d{1,3}[.,]\d{1,2})\s+(…|\d{1,3}[.,]\d{1,2})\s+(…|\d{1,3}[.,]\d{1,2})$`)
)

// ipcWeeksNewsFeedURL — URL первой страницы ленты с поиском. Переменные ipcWeeksNewsURL и
// ipcWeeksPetrolURL — обёртки, чтобы тест мог подменить ленту без сети.
func ipcWeeksNewsFeedURL(search string) (string, error) {
	u, err := url.Parse(rosstatUrl + "/central-news")
	if err != nil {
		return "", fmt.Errorf("%s: URL ленты: %w", ipcWeeksNewsContext, err)
	}
	q := u.Query()
	q.Set("per_page", ipcWeeksNewsPerPage)
	q.Set("search", search)
	q.Set("date_from", "")
	q.Set("date_to", "")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

var (
	// ipcWeeksNewsURL — лента CPI-выпусков.
	ipcWeeksNewsURL = func() (string, error) { return ipcWeeksNewsFeedURL(ipcWeeksNewsSearch) }
	// ipcWeeksPetrolURL — лента PDF о потребительских ценах на нефтепродукты.
	ipcWeeksPetrolURL = func() (string, error) { return ipcWeeksNewsFeedURL(ipcWeeksPetrolSearch) }
)

// ipcWeeksNewsReleases разбирает карточки ленты. Месячные карточки отсекаются маркером
// титула; дата берётся из титула («с … по …»), а не из даты публикации карточки. Пустой
// список карточек — ошибка: это смена вёрстки, а не «новостей нет».
func ipcWeeksNewsReleases(page []byte) ([]ipcWeeksNewsRelease, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("%s: разбор страницы новостей: %w", ipcWeeksNewsContext, err)
	}
	cards := doc.Find(".news-card")
	if cards.Length() == 0 {
		return nil, fmt.Errorf("%s: карточки .news-card не найдены — вёрстка ленты изменилась", ipcWeeksNewsContext)
	}
	base, err := url.Parse(rosstatUrl + "/central-news")
	if err != nil {
		return nil, fmt.Errorf("%s: базовый URL ленты: %w", ipcWeeksNewsContext, err)
	}

	var releases []ipcWeeksNewsRelease
	var parseErr error
	cards.EachWithBreak(func(_ int, card *goquery.Selection) bool {
		link := card.Find(".news-card__title a").First()
		href, ok := link.Attr("href")
		title := strings.TrimSpace(link.Text())
		if !ok || strings.TrimSpace(href) == "" || title == "" {
			return true
		}
		if !strings.Contains(strings.ToLower(title), ipcWeeksNewsTitleMarker) {
			return true
		}
		date, _, err := ipcWeeksBulletinsTitleDate(title)
		if err != nil {
			parseErr = fmt.Errorf("%s: титул %q: %w", ipcWeeksNewsContext, title, err)
			return false
		}
		ref, err := url.Parse(strings.TrimSpace(href))
		if err != nil {
			parseErr = fmt.Errorf("%s: ссылка %q: %w", ipcWeeksNewsContext, href, err)
			return false
		}
		resolved := base.ResolveReference(ref).String()
		releases = append(releases, ipcWeeksNewsRelease{
			Date:  date,
			Title: title,
			URL:   resolved,
			PDF:   strings.HasSuffix(strings.ToLower(resolved), ".pdf"),
		})
		return true
	})
	if parseErr != nil {
		return nil, parseErr
	}
	return releases, nil
}

// ipcWeeksNewsDocuments — Documents общего util.Fallback: CPI-PDF и PDF о ценах на
// нефтепродукты. Сбой ленты нефтепродуктов не отменяет CPI-фолбэк: три марки бензина
// доберутся следующим прогоном, а остальные 108 рядов — уже сейчас. Сбой CPI-ленты —
// ошибка: без неё нет основной таблицы недели.
func ipcWeeksNewsDocuments(ctx context.Context) ([]util.FallbackDocument, error) {
	docs, err := ipcWeeksCPIDocuments(ctx)
	if err != nil {
		return nil, err
	}
	petrol, err := ipcWeeksPetrolDocuments(ctx)
	if err != nil {
		log.Warnf("%s: PDF-фолбэк нефтепродуктов пропущен: %v", ipcWeeksPetrolContext, err)
		return docs, nil
	}
	return append(docs, petrol...), nil
}

// ipcWeeksCPIDocuments — CPI-выпуски: первая страница ленты, только PDF. HTML-выпуски
// (старый формат недельных релизов) фолбэк не разбирает — о них только строка в журнале.
func ipcWeeksCPIDocuments(_ context.Context) ([]util.FallbackDocument, error) {
	feedURL, err := ipcWeeksNewsURL()
	if err != nil {
		return nil, err
	}
	page, err := util.GetFileBytesChecked(feedURL)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", ipcWeeksNewsContext, feedURL, err)
	}
	releases, err := ipcWeeksNewsReleases(page)
	if err != nil {
		return nil, err
	}
	docs := make([]util.FallbackDocument, 0, len(releases))
	html := 0
	for _, release := range releases {
		if !release.PDF {
			html++
			continue
		}
		docs = append(docs, util.FallbackDocument{
			Date: release.Date,
			URL:  release.URL,
			Kind: ipcWeeksNewsKindCPI,
		})
	}
	if html > 0 {
		log.Infof("%s: в ленте %d HTML-выпусков — PDF-фолбэк их не покрывает", ipcWeeksNewsContext, html)
	}
	return docs, nil
}

// ipcWeeksPetrolDocuments — PDF о ценах на нефтепродукты. Дату берём из титула самого PDF,
// а не из карточки: в ленте встречается карточка с чужим периодом (28.10.2026 показывает
// «с 27 октября по 2 ноября», а PDF внутри — за 20–26 октября). Документ скачивается здесь,
// чтобы прочитать титул; шаблон скачает его ещё раз на импорте — PDF небольшие, а гейт
// util.Fallback остаётся по датам.
func ipcWeeksPetrolDocuments(ctx context.Context) ([]util.FallbackDocument, error) {
	feedURL, err := ipcWeeksPetrolURL()
	if err != nil {
		return nil, err
	}
	page, err := util.GetFileBytesChecked(feedURL)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", ipcWeeksPetrolContext, feedURL, err)
	}
	links, err := ipcWeeksNewsPDFLinks(page, ipcWeeksPetrolTitleMarker, ipcWeeksPetrolContext)
	if err != nil {
		return nil, err
	}
	docs := make([]util.FallbackDocument, 0, len(links))
	for _, link := range links {
		body, err := util.GetFileBytesChecked(link)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", ipcWeeksPetrolContext, link, err)
		}
		text, err := util.PDFText(ctx, body, util.PDFLayout)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", ipcWeeksPetrolContext, link, err)
		}
		date, err := ipcWeeksPetrolTitleDate(string(text))
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", ipcWeeksPetrolContext, link, err)
		}
		docs = append(docs, util.FallbackDocument{
			Date: date,
			URL:  link,
			Kind: ipcWeeksNewsKindPetrol,
		})
	}
	return docs, nil
}

// ipcWeeksNewsPDFLinks — PDF-ссылки карточек ленты по маркеру. Карточную дату не разбираем:
// у нефтепродуктов она бывает неверной, настоящая дата — в титуле PDF.
func ipcWeeksNewsPDFLinks(page []byte, marker, context string) ([]string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("%s: разбор страницы новостей: %w", context, err)
	}
	cards := doc.Find(".news-card")
	if cards.Length() == 0 {
		return nil, fmt.Errorf("%s: карточки .news-card не найдены — вёрстка ленты изменилась", context)
	}
	base, err := url.Parse(rosstatUrl + "/central-news")
	if err != nil {
		return nil, fmt.Errorf("%s: базовый URL ленты: %w", context, err)
	}
	var links []string
	var parseErr error
	cards.EachWithBreak(func(_ int, card *goquery.Selection) bool {
		link := card.Find(".news-card__title a").First()
		href, ok := link.Attr("href")
		title := strings.TrimSpace(link.Text())
		if !ok || strings.TrimSpace(href) == "" || !strings.Contains(strings.ToLower(title), marker) {
			return true
		}
		ref, err := url.Parse(strings.TrimSpace(href))
		if err != nil {
			parseErr = fmt.Errorf("%s: ссылка %q: %w", context, href, err)
			return false
		}
		resolved := base.ResolveReference(ref).String()
		if strings.HasSuffix(strings.ToLower(resolved), ".pdf") {
			links = append(links, resolved)
		}
		return true
	})
	if parseErr != nil {
		return nil, parseErr
	}
	return links, nil
}

// ipcWeeksNewsRows — Parse общего util.Fallback: по метке документа выбирает CPI-таблицу
// или таблицу марок бензина. У одной недели два PDF разного формата.
func ipcWeeksNewsRows(ctx context.Context, doc util.FallbackDocument, body []byte) ([]util.FallbackRow, error) {
	if doc.Kind == ipcWeeksNewsKindPetrol {
		return ipcWeeksPetrolRows(ctx, doc, body)
	}
	return ipcWeeksCPIRows(ctx, doc, body)
}

// ipcWeeksCPIRows — Parse CPI-PDF: PDF-байты → текст с колонками → точки.
// Дата шапки обязана совпасть с датой титула: пере-нумерованный буфер отдал бы по той же
// ссылке чужой выпуск, и точки ушли бы не на свою неделю.
func ipcWeeksCPIRows(ctx context.Context, doc util.FallbackDocument, body []byte) ([]util.FallbackRow, error) {
	text, err := util.PDFText(ctx, body, util.PDFLayout)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", ipcWeeksNewsContext, doc.URL, err)
	}
	points, err := ipcWeeksNewsParse(string(text))
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", ipcWeeksNewsContext, doc.URL, err)
	}
	rows, err := ipcWeeksNewsFallbackRows(points, doc.Date)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", ipcWeeksNewsContext, doc.URL, err)
	}
	return rows, nil
}

// ipcWeeksNewsFallbackRows — чистая часть Parse: точки таблицы → строки общего
// util.Fallback. Дата шапки обязана совпасть с датой выпуска: пере-нумерованный буфер
// отдал бы по той же ссылке чужой выпуск, и точки ушли бы не на свою неделю.
func ipcWeeksNewsFallbackRows(points []ipcWeeksPoint, expected time.Time) ([]util.FallbackRow, error) {
	rows := make([]util.FallbackRow, 0, len(points))
	for _, point := range points {
		if !point.date.Equal(expected) {
			return nil, fmt.Errorf("%s: дата таблицы %s не совпала с датой выпуска %s",
				ipcWeeksNewsContext, point.date.Format("2006-01-02"), expected.Format("2006-01-02"))
		}
		rows = append(rows, util.FallbackRow{Series: point.product, Date: point.date, Value: point.value})
	}
	return rows, nil
}

// ipcWeeksPetrolRowRe — строка таблицы «Динамика потребительских цен на бензин»: марка и
// четыре значения. Колонки: предыдущая дата, текущая дата, к концу месяца, к концу года;
// недельный индекс — второе значение (m[3]).
var ipcWeeksPetrolRowRe = regexp.MustCompile(`^марки АИ-(92|95|98 и выше)\s+(\d{1,3}[.,]\d{1,2})\s+(\d{1,3}[.,]\d{1,2})\s+(\d{1,3}[.,]\d{1,2})\s+(\d{1,3}[.,]\d{1,2})$`)

// ipcWeeksPetrolSeries — марка из PDF → каноническое имя XLSX.
var ipcWeeksPetrolSeries = map[string]string{
	"92":        ipcWeeksPetrolAI92,
	"95":        ipcWeeksPetrolAI95,
	"98 и выше": ipcWeeksPetrolAI98,
}

// ipcWeeksPetrolTitleDate — дата последнего дня недели из титула PDF нефтепродуктов
// («С 27 ОКТЯБРЯ ПО 2 НОЯБРЯ 2026 ГОДА»). Титул — отдельная строка, регистр не важен.
func ipcWeeksPetrolTitleDate(text string) (time.Time, error) {
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		lower := strings.ToLower(line)
		if !strings.HasPrefix(lower, "с ") || !strings.Contains(lower, " по ") {
			continue
		}
		if date, _, err := ipcWeeksBulletinsTitleDate(line); err == nil {
			return date, nil
		}
	}
	return time.Time{}, fmt.Errorf("%s: титул «С … по …» не найден", ipcWeeksPetrolContext)
}

// ipcWeeksPetrolRows — Parse PDF нефтепродуктов: три марки бензина, значение — текущая
// неделя. Агрегат «Бензин автомобильный» и дизельное топливо уже приходят из CPI-PDF,
// поэтому здесь не пишутся: иначе округлённое значение нефтепродуктов перезаписало бы
// более точное.
func ipcWeeksPetrolRows(ctx context.Context, doc util.FallbackDocument, body []byte) ([]util.FallbackRow, error) {
	text, err := util.PDFText(ctx, body, util.PDFLayout)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", ipcWeeksPetrolContext, doc.URL, err)
	}
	rows, err := ipcWeeksPetrolParse(string(text), doc.Date)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", ipcWeeksPetrolContext, doc.URL, err)
	}
	return rows, nil
}

// ipcWeeksPetrolParse — чистая часть разбора. Дата титула обязана совпасть с датой
// документа: карточка ленты у источника иногда подписана чужим периодом, и титул PDF —
// единственный надёжный источник.
func ipcWeeksPetrolParse(text string, expected time.Time) ([]util.FallbackRow, error) {
	date, err := ipcWeeksPetrolTitleDate(text)
	if err != nil {
		return nil, err
	}
	if !date.Equal(expected) {
		return nil, fmt.Errorf("%s: дата титула %s не совпала с датой документа %s",
			ipcWeeksPetrolContext, date.Format("2006-01-02"), expected.Format("2006-01-02"))
	}
	rows := make([]util.FallbackRow, 0, 3)
	seen := make(map[string]bool, 3)
	for _, raw := range strings.Split(text, "\n") {
		line := strings.Join(strings.Fields(raw), " ")
		m := ipcWeeksPetrolRowRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		series, ok := ipcWeeksPetrolSeries[m[1]]
		if !ok {
			return nil, fmt.Errorf("%s: марка %q не описана", ipcWeeksPetrolContext, m[1])
		}
		if seen[series] {
			return nil, fmt.Errorf("%s: ряд %q встречается в выпуске дважды", ipcWeeksPetrolContext, series)
		}
		seen[series] = true
		value, err := ipcWeeksNewsValue(m[3])
		if err != nil {
			return nil, fmt.Errorf("%s: ряд %q: %w", ipcWeeksPetrolContext, series, err)
		}
		rows = append(rows, util.FallbackRow{Series: series, Date: date, Value: value})
	}
	if len(rows) != 3 {
		return nil, fmt.Errorf("%s: разобрано %d марок бензина, ожидалось 3", ipcWeeksPetrolContext, len(rows))
	}
	return rows, nil
}

// ipcWeeksNewsAppendRow — Append общего util.Fallback: три колонки rosstat_ipc_weeks.
func ipcWeeksNewsAppendRow(batch driver.Batch, row util.FallbackRow) error {
	return batch.Append(row.Series, row.Date, float32(row.Value))
}

// ipcWeeksNewsTableDate разбирает дату шапки таблицы. Второе значение — «строка похожа на
// дату»: иначе вызывающий буферизует её как часть имени. Незнакомый месяц/несуществующая
// дата — ошибка, а не молчаливый пропуск.
func ipcWeeksNewsTableDate(line string) (time.Time, bool, error) {
	m := ipcWeeksNewsDateRe.FindStringSubmatch(strings.ToLower(line))
	if m == nil {
		return time.Time{}, false, nil
	}
	day, err := strconv.Atoi(m[1])
	if err != nil {
		return time.Time{}, true, fmt.Errorf("%s: день %q шапки: %w", ipcWeeksNewsContext, m[1], err)
	}
	month, ok := util.MonthsToNum[m[2]]
	if !ok {
		return time.Time{}, true, fmt.Errorf("%s: месяц %q шапки не распознан", ipcWeeksNewsContext, m[2])
	}
	year, err := strconv.Atoi(m[3])
	if err != nil {
		return time.Time{}, true, fmt.Errorf("%s: год %q шапки: %w", ipcWeeksNewsContext, m[3], err)
	}
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if date.Day() != day || date.Month() != month || date.Year() != year {
		return time.Time{}, true, fmt.Errorf("%s: дата шапки «%d %s %d» не существует",
			ipcWeeksNewsContext, day, m[2], year)
	}
	return date, true, nil
}

// ipcWeeksNewsHeaderLine — строка шапки/номера страницы: сбрасывает буфер имени, чтобы
// шапка повторной страницы не приклеилась к следующему товару.
func ipcWeeksNewsHeaderLine(line string) bool {
	if line == "в процентах" {
		return true
	}
	if strings.HasPrefix(line, ipcWeeksNewsTableAnchor) ||
		strings.HasPrefix(line, "к предыдущей") ||
		strings.HasPrefix(line, "дате ") ||
		strings.HasPrefix(line, "регистрации ") ||
		strings.HasPrefix(line, "к концу") {
		return true
	}
	if _, err := strconv.Atoi(line); err == nil {
		return true
	}
	return false
}

// ipcWeeksNewsValue разбирает числовую ячейку. Пустая/«…» сюда не доходят: «…» отсекается
// до вызова, а пустая ячейка не матчится строкой таблицы. NaN/Inf ParseFloat принимает, а
// точкой ряда они быть не могут.
func ipcWeeksNewsValue(cell string) (float64, error) {
	value, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(cell), ",", "."), 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", cell, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение %q не конечное число", cell)
	}
	return value, nil
}

// ipcWeeksNewsParse разбирает `pdftotext -layout` одного PDF в точки. Чистая функция без
// сети/exec (§6.1): тесты офлайн. Конечный автомат:
//   - старт по строке-якорю «Индексы потребительских цен»;
//   - дата шапки задаёт дату всех точек; повторные шапки страниц обязаны совпасть;
//   - шапки/номера страниц сбрасывают буфер имени;
//   - имя может занимать две строки; значения — три последних токена, берётся первое;
//   - «…» в первой колонке — пропуск точки, но не конец таблицы;
//   - стоп по разделителю/«Информация подготовлена».
func ipcWeeksNewsParse(text string) ([]ipcWeeksPoint, error) {
	var (
		started bool
		date    time.Time
		pending []string
		points  []ipcWeeksPoint
		warned  = make(map[string]bool)
		seen    = make(map[string]bool)
	)
	for _, raw := range strings.Split(text, "\n") {
		line := strings.Join(strings.Fields(raw), " ")
		if line == "" {
			continue
		}
		if !started {
			if line == ipcWeeksNewsTableAnchor {
				started = true
			}
			continue
		}
		if strings.HasPrefix(line, "___") || strings.HasPrefix(line, "Информация подготовлена") {
			break
		}
		if tableDate, isDate, err := ipcWeeksNewsTableDate(line); err != nil {
			return nil, err
		} else if isDate {
			if !date.IsZero() && !date.Equal(tableDate) {
				return nil, fmt.Errorf("%s: дата шапки %s не совпала с первой %s",
					ipcWeeksNewsContext, tableDate.Format("2006-01-02"), date.Format("2006-01-02"))
			}
			date = tableDate
			pending = nil
			continue
		}
		if ipcWeeksNewsHeaderLine(line) {
			pending = nil
			continue
		}
		m := ipcWeeksNewsRowRe.FindStringSubmatch(line)
		if m == nil {
			pending = append(pending, line)
			continue
		}
		name := strings.Join(strings.Fields(strings.Join(append(pending, strings.TrimSpace(m[1])), " ")), " ")
		pending = nil
		if name == "" {
			return nil, fmt.Errorf("%s: пустое имя товара в строке %q", ipcWeeksNewsContext, line)
		}
		if m[2] == ipcWeeksNoData {
			continue
		}
		value, err := ipcWeeksNewsValue(m[2])
		if err != nil {
			return nil, fmt.Errorf("%s: товар %q: %w", ipcWeeksNewsContext, name, err)
		}
		series := ipcWeeksNewsSeriesName(name, warned)
		if seen[series] {
			return nil, fmt.Errorf("%s: ряд %q встречается в выпуске дважды", ipcWeeksNewsContext, series)
		}
		seen[series] = true
		points = append(points, ipcWeeksPoint{product: series, date: date, value: value})
	}
	if !started {
		return nil, fmt.Errorf("%s: таблица «%s» не найдена", ipcWeeksNewsContext, ipcWeeksNewsTableAnchor)
	}
	if date.IsZero() {
		return nil, fmt.Errorf("%s: дата шапки таблицы не найдена", ipcWeeksNewsContext)
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: разбор дал 0 точек", ipcWeeksNewsContext)
	}
	return points, nil
}

// ipcWeeksNewsSeriesBySourceName — перекрытия/алиасы имён PDF-выпусков 2026 под имена
// Nedel_ipc.xlsx того же года. Проверено на PDF 2026-11-02 и 2026-10-26 (108 строк):
// все имена либо покрыты этой картой, либо основной картой бюллетеней. Три ключа
// (Водка/Валидол/Нимесулид) намеренно перекрывают карту бюллетеней: она выбирала дозы и
// крепость 2022 года, а в XLSX 2026 действуют короткие имена.
var ipcWeeksNewsSeriesBySourceName = map[string]string{
	"Активированный уголь": "Активированный уголь, 10 таблеток",
	"Бананы":  "Бананы, кг",
	"Валидол": "Валидол, 10 таблеток",
	"Водка":   "Водка, л",
	"Восстановление зуба пломбой": "Восстановление зуба пломбой, услуга",
	"Кефир": "Кефир, кг",
	"Кроссовые туфли для взрослых":                                    "Кроссовые туфли для взрослых, пара",
	"Кроссовые туфли для детей":                                       "Кроссовые туфли для детей, пара",
	"Метамизол натрия":                                                "Метамизол натрия, 10 таблеток",
	"Мойка легкового автомобиля":                                      "Мойка легкового автомобиля, услуга",
	"Молоко питьевое цельное ультрапастеризованное 2,5-3,2% жирности": "Молоко питьевое цельное ультрапастеризованное 2,5-3,2% жирности, л",
	"Нафазолин": "Нафазолин, 0,1%, 15 мл",
	"Нимесулид": "Нимесулид, 10 таблеток",
	"Пенталгин": "Пенталгин, 24 таблетки",
	"Подгузники детские бумажные":                   "Подгузники детские бумажные, 10 шт.",
	"Поездка на Черноморское побережье России":      "Поездка на отдых на Черноморское побережье России, поездка",
	"Поездки в отдельные страны Юго-Восточной Азии": "Поездки в отдельные страны Юго-Восточной Азии, поездка",
	"Ренгалин":                           "Ренгалин, 20 таблеток",
	"Римантадин":                         "Римантадин, 10 таблеток",
	"Свекла столовая":                    "Свёкла столовая, кг",
	"Сигареты с фильтром":                "Сигареты с фильтром, пачка",
	"Стрижка модельная в женском зале":   "Стрижка модельная в женском зале, стрижка",
	"Стрижка модельная в мужском зале":   "Стрижка модельная в мужском зале, стрижка",
	"Сыры твердые, полутвердые и мягкие": "Сыры твердые, полутвердые и мягкие, кг",
	"Телевизор":                          "Телевизор, шт.",
	"Хлеб из ржаной муки и из смеси ржаной и пшеничной": "Хлеб из ржаной муки и из смеси муки ржаной и пшеничной, кг",
}

// ipcWeeksNewsSeriesKeys — карта перекрытий по ключам без пробелов/дефисов: тем же Key,
// что и основная карта бюллетеней, поэтому «ориентированно- стружечные» и регистр не
// ломают поиск.
var ipcWeeksNewsSeriesKeys = func() map[string]string {
	keys := make(map[string]string, len(ipcWeeksNewsSeriesBySourceName))
	for source, canonical := range ipcWeeksNewsSeriesBySourceName {
		keys[ipcWeeksBulletinsKey(source)] = canonical
	}
	return keys
}()

// ipcWeeksNewsSeriesName — имя ряда для строки PDF: перекрытие 2026 года, затем общая
// карта бюллетеней, затем исходное имя с предупреждением. Предупреждение один раз на имя.
func ipcWeeksNewsSeriesName(source string, warned map[string]bool) string {
	if canonical, ok := ipcWeeksNewsSeriesKeys[ipcWeeksBulletinsKey(source)]; ok {
		return canonical
	}
	if canonical, ok := ipcWeeksBulletinsCanonical(source); ok {
		return canonical
	}
	if !warned[source] {
		warned[source] = true
		log.Warnf("%s: товар %q не описан в ipcWeeksNewsSeriesBySourceName / ipcWeeksBulletinsSeriesBySourceName — точка записана под исходным именем, описание добавить в каталог",
			rosstatIPCWeeksTable, source)
	}
	return source
}
