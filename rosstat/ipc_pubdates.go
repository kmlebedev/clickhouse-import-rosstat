package rosstat

// Даты публикации месячного ИПЦ Росстата для point-in-time витрин (дефект Ф2 бэктеста
// economist-oracle, #622/#626). Таблица rosstat_ipc_pubdates хранит одну строку на
// отчётный месяц, backfill пишет generic series_publication (source='rosstat'), откуда
// v_ofz_signal берёт дату доступности ИПЦ вместо конца отчётного месяца.
//
// Источник — лента rosstat.gov.ru/central-news: карточка месяца («Об индексе
// потребительских цен в мае 2025 года») с датой публикации в .news-card__data и в имени
// файла релиза (<id>_DD-MM-YYYY.html|pdf). Обе даты обязаны совпасть, иначе разбор
// падает: расхождение означает смену вёрстки или подмену файла, а не данные.
//
// Глубина ленты — 2022-05 … нынешний релиз (на 2026-11-16 это 49 месяцев, 3 страницы).
// Более старые даты Росстат машинно не отдаёт: годовой план публикаций живёт только для
// текущего года, поиск по сайту нерелевантен, Wayback — сторонний источник. Для месяцев
// до 2022-05 v_ofz_signal берёт дату релиза ЦБ «Динамика потребительских цен»
// (series_publication, source='cbr'), а до 2016 — конец M+1.

import (
	"bytes"
	"context"
	"fmt"
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
	rosstatIPCPubdatesTable = "rosstat_ipc_pubdates"

	// ipcPubdatesDdl — DDL в %s-форме (имя подставляет шаблон). Ключ — отчётный месяц:
	// один релиз на месяц, повторная публикация заменяет строку версией loaded_at.
	ipcPubdatesDdl = `CREATE TABLE IF NOT EXISTS %s (
				  period_date Date
				, publication_date Date
				, title String
				, source_url String
				, loaded_at DateTime('Europe/Moscow') DEFAULT now()
			) ENGINE = ReplacingMergeTree(loaded_at) ORDER BY (period_date);
		`

	// ipcPubdatesSearch — поисковый запрос ленты. Недельные «оценки» ИПЦ тоже содержат
	// «потребительских цен», поэтому титул дополнительно проверяется регексом месяца.
	ipcPubdatesSearch = "Об индексе потребительских цен в"
	// ipcPubdatesPerPage — размер страницы ленты; вместе с page даёт обход архива.
	ipcPubdatesPerPage = "20"
	// ipcPubdatesMaxPages — предохранитель обхода: на 2026-11-16 архив укладывается в 3
	// страницы, четвёртая пуста.
	ipcPubdatesMaxPages = 20
	// ipcPubdatesSeries — ряд series_publication; совпадает с series_catalog
	// (source='rosstat', имя объявлено ipcMesSeriesNames).
	ipcPubdatesSeries = "Индексы потребительских цен на товары и услуги по Российской Федерации"
	// ipcPubdatesContext — общая часть сообщений об ошибках.
	ipcPubdatesContext = rosstatIPCPubdatesTable
)

var (
	// ipcPubdatesTitleRe — титул месячного релиза: «Об индексе потребительских цен в мае
	// 2025 года». Недельные карточки («Об оценке индекса…») под шаблон не подходят.
	// Месяц допускает латинские двойники кириллицы: в подписях Росстата встречается «мартa».
	ipcPubdatesTitleRe = regexp.MustCompile(`(?i)^об индексе потребительских цен\s+в\s+([а-яёa-z]+)\s+(\d{4})\s+года$`)
	// ipcPubdatesDateRe — дата карточки: «11 июня 2025» или «9 октября» (у текущего года
	// Росстат год не печатает).
	ipcPubdatesDateRe = regexp.MustCompile(`^(\d{1,2})\s+([а-яёa-z]+)(?:\s+(\d{4}))?$`)
	// ipcPubdatesFileDateRe — дата в имени файла релиза: 85_11-06-2025.html.
	ipcPubdatesFileDateRe = regexp.MustCompile(`_(\d{2})-(\d{2})-(\d{4})\.(?:html|pdf)$`)
)

// ipcPubdate — месячный релиз ИПЦ: отчётный месяц (последний день) и дата публикации.
type ipcPubdate struct {
	Period      time.Time
	Publication time.Time
	Title       string
	URL         string
}

// ipcPubdatesFeedURL — URL страницы ленты с поиском месячных релизов.
func ipcPubdatesFeedURL(page int) (string, error) {
	u, err := url.Parse(rosstatUrl + "/central-news")
	if err != nil {
		return "", fmt.Errorf("%s: URL ленты: %w", ipcPubdatesContext, err)
	}
	q := u.Query()
	q.Set("per_page", ipcPubdatesPerPage)
	q.Set("search", ipcPubdatesSearch)
	q.Set("page", strconv.Itoa(page))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// parseIpcPubdates разбирает страницу ленты. cards — число карточек (0 на странице за
// пределами архива); релизы — только месячные ИПЦ. Титул без месяца/года, карточка без
// даты или расхождение даты карточки с именем файла — ошибка: это смена вёрстки, а не
// «нет релизов».
func parseIpcPubdates(page []byte) (releases []ipcPubdate, cards int, err error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, 0, fmt.Errorf("%s: разбор страницы ленты: %w", ipcPubdatesContext, err)
	}
	seen := make(map[string]ipcPubdate)
	doc.Find(".news-card").EachWithBreak(func(_ int, card *goquery.Selection) bool {
		cards++
		link := card.Find(".news-card__title a").First()
		href, ok := link.Attr("href")
		title := strings.TrimSpace(link.Text())
		if !ok || href == "" || title == "" {
			return true
		}
		period, matched := ipcPubdatesPeriod(title)
		if !matched {
			return true
		}
		cardDate, hasYear, ok := ipcPubdatesCardDate(strings.TrimSpace(card.Find(".news-card__data").First().Text()))
		if !ok {
			err = fmt.Errorf("%s: карточка %q без даты публикации", ipcPubdatesContext, title)
			return false
		}
		fileDate, ok := ipcPubdatesFileDate(href)
		if !ok {
			err = fmt.Errorf("%s: в ссылке %q нет даты релиза", ipcPubdatesContext, href)
			return false
		}
		// Дата релиза — из имени файла (там всегда полная дата); подпись карточки —
		// сверка. У карточек текущего года года в подписи нет: сверяем день и месяц.
		switch {
		case hasYear && !cardDate.Equal(fileDate):
			err = fmt.Errorf("%s: %q: дата карточки %s не совпала с датой файла %s",
				ipcPubdatesContext, title, cardDate.Format("2006-01-02"), fileDate.Format("2006-01-02"))
			return false
		case !hasYear && (cardDate.Day() != fileDate.Day() || cardDate.Month() != fileDate.Month()):
			err = fmt.Errorf("%s: %q: день и месяц карточки %s не совпали с файлом %s",
				ipcPubdatesContext, title, cardDate.Format("01-02"), fileDate.Format("2006-01-02"))
			return false
		}
		published := fileDate
		if !published.After(period) {
			err = fmt.Errorf("%s: %q: публикация %s не позже отчётного месяца %s",
				ipcPubdatesContext, title, published.Format("2006-01-02"), period.Format("2006-01-02"))
			return false
		}
		key := period.Format("2006-01-02")
		if old, ok := seen[key]; ok {
			if !old.Publication.Equal(published) || old.URL != href {
				err = fmt.Errorf("%s: месяц %s пришёл дважды с разными датами (%s и %s)",
					ipcPubdatesContext, period.Format("2006-01"), old.Publication.Format("2006-01-02"), published.Format("2006-01-02"))
				return false
			}
			return true
		}
		row := ipcPubdate{Period: period, Publication: published, Title: title, URL: href}
		seen[key] = row
		releases = append(releases, row)
		return true
	})
	return releases, cards, err
}

// ipcPubdatesCyrillic заменяет латинские двойники кириллицы в названии месяца: в
// подписях Росстата встречается «13 мартa» с латинской «a» (карточка февраля 2026).
func ipcPubdatesCyrillic(raw string) string {
	return strings.NewReplacer(
		"a", "а", "e", "е", "o", "о", "p", "р", "c", "с", "x", "х", "y", "у",
	).Replace(raw)
}

// ipcPubdatesPeriod — отчётный месяц из титула: «в мае 2025 года» → 2025-05-31.
func ipcPubdatesPeriod(title string) (time.Time, bool) {
	m := ipcPubdatesTitleRe.FindStringSubmatch(strings.TrimSpace(title))
	if m == nil {
		return time.Time{}, false
	}
	month, ok := util.MonthsToNum[ipcPubdatesCyrillic(strings.ToLower(m[1]))]
	if !ok {
		return time.Time{}, false
	}
	year, err := strconv.Atoi(m[2])
	if err != nil {
		return time.Time{}, false
	}
	return time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1), true
}

// ipcPubdatesCardDate — дата карточки «11 июня 2025» или «9 октября». withYear=false —
// год в подписи не напечатан (карточки текущего года), тогда сверяется только день/месяц.
func ipcPubdatesCardDate(raw string) (date time.Time, withYear, ok bool) {
	m := ipcPubdatesDateRe.FindStringSubmatch(strings.ToLower(strings.TrimSpace(raw)))
	if m == nil {
		return time.Time{}, false, false
	}
	day, err := strconv.Atoi(m[1])
	if err != nil {
		return time.Time{}, false, false
	}
	month, ok := util.MonthsToNum[ipcPubdatesCyrillic(m[2])]
	if !ok {
		return time.Time{}, false, false
	}
	year := 0
	if m[3] != "" {
		if year, err = strconv.Atoi(m[3]); err != nil {
			return time.Time{}, false, false
		}
	}
	date = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if date.Day() != day || date.Month() != month {
		return time.Time{}, false, false
	}
	return date, m[3] != "", true
}

// ipcPubdatesFileDate — дата из имени файла релиза (<id>_DD-MM-YYYY.html|pdf).
func ipcPubdatesFileDate(href string) (time.Time, bool) {
	m := ipcPubdatesFileDateRe.FindStringSubmatch(strings.TrimSpace(href))
	if m == nil {
		return time.Time{}, false
	}
	day, err := strconv.Atoi(m[1])
	if err != nil {
		return time.Time{}, false
	}
	month, err := strconv.Atoi(m[2])
	if err != nil || month < 1 || month > 12 {
		return time.Time{}, false
	}
	year, err := strconv.Atoi(m[3])
	if err != nil {
		return time.Time{}, false
	}
	date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if date.Day() != day {
		return time.Time{}, false
	}
	return date, true
}

// fetchIpcPubdates обходит страницы ленты до первой без карточек. Пустая первая страница
// — ошибка (вёрстка/доступ), пустая следующая — конец архива.
func fetchIpcPubdates() ([]ipcPubdate, error) {
	var releases []ipcPubdate
	for page := 1; page <= ipcPubdatesMaxPages; page++ {
		feedURL, err := ipcPubdatesFeedURL(page)
		if err != nil {
			return nil, err
		}
		body, err := util.GetPage(feedURL)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", ipcPubdatesContext, feedURL, err)
		}
		pageReleases, cards, err := parseIpcPubdates(body)
		if err != nil {
			return nil, err
		}
		if cards == 0 {
			if page == 1 {
				return nil, fmt.Errorf("%s: на первой странице ленты нет карточек .news-card — вёрстка изменилась", ipcPubdatesContext)
			}
			break
		}
		releases = append(releases, pageReleases...)
	}
	if len(releases) == 0 {
		return nil, fmt.Errorf("%s: лента не дала ни одного месячного релиза ИПЦ", ipcPubdatesContext)
	}
	return releases, nil
}

// crawlIpcPubdates — полный цикл: DDL таблицы, обход ленты, дельта-запись.
func crawlIpcPubdates(_ string, conn driver.Conn) (int64, error) {
	ctx := context.Background()
	releases, err := fetchIpcPubdates()
	if err != nil {
		return 0, err
	}
	return writeIpcPubdates(ctx, conn, releases)
}

// ipcPubdateKey — ключ строки rosstat_ipc_pubdates.
func ipcPubdateKey(period time.Time) string {
	return period.Format("2006-01-02")
}

// loadIpcPubdates читает текущие строки FINAL в map по отчётному месяцу.
func loadIpcPubdates(ctx context.Context, conn driver.Conn) (map[string]ipcPubdate, error) {
	rows, err := conn.Query(ctx, "SELECT period_date, publication_date, title, source_url FROM "+
		rosstatIPCPubdatesTable+" FINAL")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string]ipcPubdate)
	for rows.Next() {
		var row ipcPubdate
		if err = rows.Scan(&row.Period, &row.Publication, &row.Title, &row.URL); err != nil {
			return nil, err
		}
		result[ipcPubdateKey(row.Period)] = row
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// writeIpcPubdates пишет только новые/изменившиеся месяцы: повторный прогон — 0 строк.
func writeIpcPubdates(ctx context.Context, conn driver.Conn, releases []ipcPubdate) (int64, error) {
	existing, err := loadIpcPubdates(ctx, conn)
	if err != nil {
		return 0, err
	}
	var changed []ipcPubdate
	for _, row := range releases {
		old, ok := existing[ipcPubdateKey(row.Period)]
		if ok && !ipcPubdateChanged(old, row) {
			continue
		}
		changed = append(changed, row)
	}
	if len(changed) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+rosstatIPCPubdatesTable+
		" (period_date, publication_date, title, source_url, loaded_at)")
	if err != nil {
		return 0, err
	}
	for _, row := range changed {
		if err = batch.Append(row.Period, row.Publication, row.Title, row.URL, time.Now()); err != nil {
			_ = batch.Abort()
			return 0, err
		}
	}
	if err = batch.Send(); err != nil {
		return 0, err
	}
	return int64(len(changed)), nil
}

// ipcPubdateChanged — писать ли новую версию строки. Даты сравниваются календарными
// компонентами: Date из ClickHouse приходит в зоне сессии (+05:00 на dev-хосте), и
// time.Equal на том же дне дал бы ложное расхождение.
func ipcPubdateChanged(existing, desired ipcPubdate) bool {
	return existing.Publication.Format("2006-01-02") != desired.Publication.Format("2006-01-02") ||
		existing.URL != desired.URL || existing.Title != desired.Title
}

// ipcPubdatesPublishedAt — effective-момент для date-only: 23:59:59 MSK этой даты.
// Компоненты берутся из time.Time без пересчёта зоны: ClickHouse Date приходит в зоне
// сессии (+05:00 на dev-хосте), и пересчёт сдвинул бы день.
func ipcPubdatesPublishedAt(date time.Time) time.Time {
	y, m, d := date.Date()
	return time.Date(y, m, d, 23, 59, 59, 0, time.FixedZone("MSK", 3*60*60))
}

// publishIpcPubdates — производные импортёра: generic series_publication для ряда ИПЦ.
// Качество всегда date; более точную строку (exact_time) не перезаписываем.
func publishIpcPubdates(ctx context.Context, conn driver.Conn) error {
	if err := util.EnsureSeriesPublication(ctx, conn); err != nil {
		return err
	}
	type pubdateRow struct {
		period      time.Time
		publication time.Time
		sourceURL   string
	}
	rows, err := conn.Query(ctx, "SELECT period_date, publication_date, source_url FROM "+
		rosstatIPCPubdatesTable+" FINAL")
	if err != nil {
		return err
	}
	var table []pubdateRow
	for rows.Next() {
		var row pubdateRow
		if err = rows.Scan(&row.period, &row.publication, &row.sourceURL); err != nil {
			_ = rows.Close()
			return err
		}
		table = append(table, row)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	existing, err := loadRosstatSeriesPublications(ctx, conn)
	if err != nil {
		return err
	}
	var changed []util.SeriesPublication
	for _, pubdate := range table {
		row := util.SeriesPublication{
			Source:      publicationSourceRosstat,
			Series:      ipcPubdatesSeries,
			PeriodDate:  pubdate.period,
			PublishedAt: ipcPubdatesPublishedAt(pubdate.publication),
			Quality:     "date",
			SourceURL:   pubdate.sourceURL,
		}
		old, ok := existing[ipcPubdateKey(pubdate.period)]
		if ok && old.Quality == "exact_time" {
			continue
		}
		if ok && old.PublishedAt.Equal(row.PublishedAt) && old.SourceURL == row.SourceURL {
			continue
		}
		changed = append(changed, row)
	}
	if len(changed) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+util.SeriesPublicationTable+
		" (source, series, period_date, published_at, quality, source_url)")
	if err != nil {
		return err
	}
	for _, row := range changed {
		if err = batch.Append(row.Source, row.Series, row.PeriodDate, row.PublishedAt, row.Quality, row.SourceURL); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	if err = batch.Send(); err != nil {
		return err
	}
	log.Infof("%s: новых/изменённых строк публикационных дат %d", util.SeriesPublicationTable, len(changed))
	return nil
}

// publicationSourceRosstat — source в series_publication и series_catalog.
const publicationSourceRosstat = "rosstat"

// loadRosstatSeriesPublications читает текущие строки series_publication нашего ряда.
func loadRosstatSeriesPublications(ctx context.Context, conn driver.Conn) (map[string]util.SeriesPublication, error) {
	rows, err := conn.Query(ctx, "SELECT period_date, published_at, quality, source_url FROM "+
		util.SeriesPublicationTable+" FINAL WHERE source = ? AND series = ?",
		publicationSourceRosstat, ipcPubdatesSeries)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string]util.SeriesPublication)
	for rows.Next() {
		var row util.SeriesPublication
		row.Source = publicationSourceRosstat
		row.Series = ipcPubdatesSeries
		if err = rows.Scan(&row.PeriodDate, &row.PublishedAt, &row.Quality, &row.SourceURL); err != nil {
			return nil, err
		}
		result[ipcPubdateKey(row.PeriodDate)] = row
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// ipcPubdatesImporter — даты публикации месячного ИПЦ; регистрируется реестром
// rosstat/importers.go. Без префлайта: источник — список карточек, а не файл; дельта по
// содержимому, повтор — Imported 0 rows.
var ipcPubdatesImporter = &ipcPubdatesStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   rosstatIPCPubdatesTable,
	CreateTable: []string{ipcPubdatesDdl},
	DataUrl:     rosstatUrl + "/central-news",
	CrawFunc:    crawlIpcPubdates,
	AfterImport: publishIpcPubdates,
}}

// ipcPubdatesStat — тип импортёра; отдельная структура нужна, чтобы Name() и Import()
// промотировались из шаблона и не совпадали с соседними импортёрами пакета.
type ipcPubdatesStat struct {
	util.ClickHouseImport
}
