package cbr

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	cbrPublicationCalendarTable = "cbr_publication_calendar"
	cbrPublicationCalendarURL   = "https://www.cbr.ru/calendar/"

	inflExpArchiveURL = "https://www.cbr.ru/analytics/dkp/inflationary_expectations/"
	cpdArchiveURL     = "https://www.cbr.ru/analytics/dkp/dinamic/"
	surveyArchiveURL  = "https://www.cbr.ru/analytics/dkp/monitoring/"

	// Каталожные имена источников: series_publication — generic-слой, его source
	// должен совпадать с source в v_series_catalog, чтобы lookup шёл по тем же
	// (source, series), что и чтение значения.
	publicationSourceCBR    = cbrSource
	publicationSourceSurvey = mpSurveySource
)

// cbrPublicationCalendarDdl — канонический DDL календаря (ARCHITECTURE §6.2).
// loaded_at — версия ReplacingMergeTree: строка с тем же ключом, но изменившимся
// planned/time, должна детерминированно заменять прежнюю.
const cbrPublicationCalendarDdl = `CREATE TABLE IF NOT EXISTS %s (
    publication_date Date,
    publication_time Nullable(String),      -- «16:00»; NULL — время не указано
    title String,
    topic LowCardinality(String),           -- statistics | analytics
    period String,                          -- период из строки ЦБ либо YYYY-MM блока
    source_url String,                      -- cbr.ru + href показателя/публикации
    planned UInt8,                          -- 1 — дата была в будущем на момент снимка
    loaded_at DateTime('Europe/Moscow')     -- версия ReplacingMergeTree
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (publication_date, title, period, source_url);`

// cbrPublicationCalendarSourceSeries — ряды, для которых backfill-им published_at.
// Мониторинг предприятий публикует ценовые ожидания в двух вариантах (сырой и SA)
// одной волной, поэтому дата у них общая.
var cbrPublicationCalendarSourceSeries = map[string][]string{
	"cbr_infl_exp":       cbrInflExpNames,
	"cbr_indicators_cpd": cbrIndicatorsCpdNames,
	mpSurveySource:       {"price_expectations", "price_expectations_sa"},
}

type publicationCalendarStat struct {
	util.ClickHouseImport
}

// cbrPublicationCalendarImporter — календарь публикаций ЦБ и исторические published_at.
// Один импортёр пишет две таблицы (прецедент cbr_mp_summary): обе — один публикационный
// контур, и приёмка issue — одна команда make import STAT=cbr_publication_calendar.
// CrawFunc, а не BodyFunc: HEAD-префлайт дневного календаря не даёт устойчивого no-op
// (окно сдвигается каждый день), а CrawFunc не архивирует 225-КБ страницу ежедневно.
var cbrPublicationCalendarImporter = &publicationCalendarStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   cbrPublicationCalendarTable,
	CreateTable: []string{cbrPublicationCalendarDdl},
	DataUrl:     cbrPublicationCalendarURL,
	CrawFunc:    crawlPublicationCalendar,
	AfterImport: func(ctx context.Context, conn driver.Conn) error {
		if _, err := util.CreateView(ctx, conn, cbrPublicationCalendarView); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, cbrSeriesPublicationView)
		return err
	},
}}

// crawlPublicationCalendar — полный цикл: DDL второй таблицы, разбор календаря,
// дельта-запись календаря, backfill series_publication. Возвращает число новых/
// изменённых строк обеих таблиц.
func crawlPublicationCalendar(url string, conn driver.Conn) (int64, error) {
	ctx := context.Background()
	if err := util.EnsureSeriesPublication(ctx, conn); err != nil {
		return 0, err
	}
	page, err := util.GetPage(url)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrPublicationCalendarTable, url, err)
	}
	parsed, err := parsePublicationCalendar(page, time.Now())
	if err != nil {
		return 0, err
	}
	calendarRows, err := writeCalendarDelta(ctx, conn, parsed.Rows)
	if err != nil {
		return 0, err
	}
	seriesRows, err := writeSeriesPublications(ctx, conn)
	if err != nil {
		return 0, err
	}
	log.Infof("%s: статистических строк %d, аналитических блоков %d (%d строк), новых/изменённых строк календаря %d",
		cbrPublicationCalendarTable, parsed.StatsRows, parsed.AnalyticBlocks, parsed.AnalyticRows, calendarRows)
	log.Infof("%s: новых/изменённых строк публикационных дат %d", util.SeriesPublicationTable, seriesRows)
	return calendarRows + seriesRows, nil
}

// publicationCalendarKey — ключ строки cbr_publication_calendar.
type publicationCalendarKey struct {
	Date, Title, Period, SourceURL string
}

// writeCalendarDelta читает текущие строки FINAL и пишет только новые/изменившиеся.
// Исчезнувшие из источника строки не удаляются: календарь — история, а не срез.
func writeCalendarDelta(ctx context.Context, conn driver.Conn, rows []publicationCalendarRow) (int64, error) {
	existing, err := loadCalendarRows(ctx, conn)
	if err != nil {
		return 0, err
	}
	desired := make(map[publicationCalendarKey]publicationCalendarRow, len(rows))
	for _, row := range rows {
		key := publicationCalendarKey{
			Date:      row.Date.Format("2006-01-02"),
			Title:     row.Title,
			Period:    row.Period,
			SourceURL: row.SourceURL,
		}
		desired[key] = row
	}
	var changed []publicationCalendarRow
	for key, row := range desired {
		old, ok := existing[key]
		if ok && !calendarRowChanged(old, row) {
			continue
		}
		changed = append(changed, row)
	}
	if len(changed) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+cbrPublicationCalendarTable+
		" (publication_date, publication_time, title, topic, period, source_url, planned, loaded_at)")
	if err != nil {
		return 0, err
	}
	for _, row := range changed {
		var publicationTime any
		if row.Time != nil {
			publicationTime = *row.Time
		}
		if err = batch.Append(row.Date, publicationTime, row.Title, row.Topic, row.Period, row.SourceURL, row.Planned, time.Now()); err != nil {
			_ = batch.Abort()
			return 0, err
		}
	}
	if err = batch.Send(); err != nil {
		return 0, err
	}
	return int64(len(changed)), nil
}

// loadCalendarRows читает текущее состояние календаря в map по ключу.
func loadCalendarRows(ctx context.Context, conn driver.Conn) (map[publicationCalendarKey]publicationCalendarRow, error) {
	rows, err := conn.Query(ctx, "SELECT publication_date, publication_time, title, topic, period, source_url, planned FROM "+
		cbrPublicationCalendarTable+" FINAL")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[publicationCalendarKey]publicationCalendarRow)
	for rows.Next() {
		var (
			row             publicationCalendarRow
			publicationTime sql.NullString
		)
		if err = rows.Scan(&row.Date, &publicationTime, &row.Title, &row.Topic, &row.Period, &row.SourceURL, &row.Planned); err != nil {
			return nil, err
		}
		if publicationTime.Valid {
			value := publicationTime.String
			row.Time = &value
		}
		key := publicationCalendarKey{
			Date:      row.Date.Format("2006-01-02"),
			Title:     row.Title,
			Period:    row.Period,
			SourceURL: row.SourceURL,
		}
		result[key] = row
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// seriesPublicationKey — ключ строки series_publication.
type seriesPublicationKey struct {
	Source, Series, Period string
}

// publicationPoint — точка ряда источника, для которой нужна дата публикации.
// Source — каталожный (cbr/cbr_survey); Archive — ключ релизного архива
// (cbr_infl_exp/cbr_indicators_cpd/cbr_survey), у двух рядов cbr архивы разные.
type publicationPoint struct {
	Source  string
	Series  string
	Date    time.Time
	Archive string
}

// writeSeriesPublications — backfill published_at для трёх релизов ЦБ. Точные даты
// берутся из релизных архивов, для старых периодов остаётся fallback конец M+1.
func writeSeriesPublications(ctx context.Context, conn driver.Conn) (int64, error) {
	points, err := loadPublicationPoints(ctx, conn)
	if err != nil {
		return 0, err
	}
	existing, err := loadExistingSeriesPublications(ctx, conn)
	if err != nil {
		return 0, err
	}
	archives := loadPublicationArchives(ctx, points, existing)
	desired := buildSeriesPublications(points, archives)
	var changed []util.SeriesPublication
	for _, row := range desired {
		key := seriesPublicationKey{
			Source: row.Source,
			Series: row.Series,
			Period: row.PeriodDate.Format("2006-01-02"),
		}
		old, ok := existing[key]
		if ok && !seriesPublicationChanged(old, row) {
			continue
		}
		changed = append(changed, row)
	}
	if len(changed) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+util.SeriesPublicationTable+
		" (source, series, period_date, published_at, quality, source_url)")
	if err != nil {
		return 0, err
	}
	for _, row := range changed {
		if err = batch.Append(row.Source, row.Series, row.PeriodDate, row.PublishedAt, row.Quality, row.SourceURL); err != nil {
			_ = batch.Abort()
			return 0, err
		}
	}
	if err = batch.Send(); err != nil {
		return 0, err
	}
	return int64(len(changed)), nil
}

// loadPublicationPoints собирает точки трёх источников. Ряды мониторинга предприятий
// лежат в общей macro_series, остальные — в своих таблицах; source — каталожный.
func loadPublicationPoints(ctx context.Context, conn driver.Conn) ([]publicationPoint, error) {
	var points []publicationPoint
	for _, source := range []string{cbrInflExpTable, cbrIndicatorsCpdTable} {
		rows, err := conn.Query(ctx, fmt.Sprintf("SELECT name, date FROM %s FINAL", source))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var (
				series string
				date   time.Time
			)
			if err = rows.Scan(&series, &date); err != nil {
				_ = rows.Close()
				return nil, err
			}
			points = append(points, publicationPoint{Source: publicationSourceCBR, Series: series, Date: date, Archive: source})
		}
		if err = rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}
	surveySeries := cbrPublicationCalendarSourceSeries[publicationSourceSurvey]
	rows, err := conn.Query(ctx, "SELECT series, date FROM macro_series FINAL WHERE source = ? AND series IN (?, ?)",
		publicationSourceSurvey, surveySeries[0], surveySeries[1])
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			series string
			date   time.Time
		)
		if err = rows.Scan(&series, &date); err != nil {
			return nil, err
		}
		points = append(points, publicationPoint{Source: publicationSourceSurvey, Series: series, Date: date, Archive: publicationSourceSurvey})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return points, nil
}

// loadExistingSeriesPublications читает текущие строки FINAL в map по ключу.
func loadExistingSeriesPublications(ctx context.Context, conn driver.Conn) (map[seriesPublicationKey]util.SeriesPublication, error) {
	rows, err := conn.Query(ctx, "SELECT source, series, period_date, published_at, quality, source_url FROM "+
		util.SeriesPublicationTable+" FINAL")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[seriesPublicationKey]util.SeriesPublication)
	for rows.Next() {
		var row util.SeriesPublication
		if err = rows.Scan(&row.Source, &row.Series, &row.PeriodDate, &row.PublishedAt, &row.Quality, &row.SourceURL); err != nil {
			return nil, err
		}
		key := seriesPublicationKey{
			Source: row.Source,
			Series: row.Series,
			Period: row.PeriodDate.Format("2006-01-02"),
		}
		result[key] = row
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// loadPublicationArchives качает только те индексы, для которых есть периоды без
// точной даты. Ошибка сети/разбора — warn и пустая карта: остаётся fallback, прогон
// не падает (следующий прогон повторит).
func loadPublicationArchives(ctx context.Context, points []publicationPoint, existing map[seriesPublicationKey]util.SeriesPublication) map[string]map[publicationPeriod]archivePublication {
	needed := map[string]map[publicationPeriod]bool{}
	for _, point := range points {
		key := seriesPublicationKey{
			Source: point.Source,
			Series: point.Series,
			Period: point.Date.Format("2006-01-02"),
		}
		if old, ok := existing[key]; ok && publicationQualityRank(old.Quality) >= publicationQualityRank("date") {
			continue
		}
		period := publicationPeriod{Year: point.Date.Year(), Month: point.Date.Month()}
		if needed[point.Archive] == nil {
			needed[point.Archive] = map[publicationPeriod]bool{}
		}
		needed[point.Archive][period] = true
	}
	result := map[string]map[publicationPeriod]archivePublication{}
	if periods := needed[cbrInflExpTable]; len(periods) > 0 {
		result[cbrInflExpTable] = fetchInflExpArchive(periods)
	}
	if periods := needed[cbrIndicatorsCpdTable]; len(periods) > 0 {
		result[cbrIndicatorsCpdTable] = fetchCPDArchive(periods)
	}
	if periods := needed[publicationSourceSurvey]; len(periods) > 0 {
		result[publicationSourceSurvey] = fetchSurveyArchive(periods)
	}
	return result
}

// fetchInflExpArchive разбирает индекс версий инфляционных ожиданий. Периоды вне
// индекса останутся fallback.
func fetchInflExpArchive(periods map[publicationPeriod]bool) map[publicationPeriod]archivePublication {
	page, err := util.GetPage(inflExpArchiveURL)
	if err != nil {
		log.Warnf("cbr_infl_exp: архив %s недоступен (%v) — fallback", inflExpArchiveURL, err)
		return map[publicationPeriod]archivePublication{}
	}
	all, err := parseInflExpArchive(page)
	if err != nil {
		log.Warnf("cbr_infl_exp: %v — fallback", err)
		return map[publicationPeriod]archivePublication{}
	}
	result := make(map[publicationPeriod]archivePublication, len(periods))
	for period := range periods {
		if publication, ok := all[period]; ok {
			result[period] = publication
		}
	}
	return result
}

// fetchCPDArchive разбирает индекс «Динамики потребительских цен»; для HTML-версий
// без tooltip качает страницу версии (с запасным zero-padded вариантом CPD_YYYY-MM,
// которым ЦБ чинит часть битых ссылок).
func fetchCPDArchive(periods map[publicationPeriod]bool) map[publicationPeriod]archivePublication {
	page, err := util.GetPage(cpdArchiveURL)
	if err != nil {
		log.Warnf("cbr_indicators_cpd: архив %s недоступен (%v) — fallback", cpdArchiveURL, err)
		return map[publicationPeriod]archivePublication{}
	}
	all, err := parseCPDArchive(page)
	if err != nil {
		log.Warnf("cbr_indicators_cpd: %v — fallback", err)
		return map[publicationPeriod]archivePublication{}
	}
	result := make(map[publicationPeriod]archivePublication, len(periods))
	for period := range periods {
		publication, ok := all[period]
		if !ok {
			continue
		}
		if publication.Date.IsZero() {
			publication = fetchCPDPage(period, publication)
		}
		result[period] = publication
	}
	return result
}

// fetchCPDPage пробует ссылку из индекса, затем zero-padded CPD_YYYY-MM. Обе 404 —
// версия остаётся без даты, вызывающий поставит fallback.
func fetchCPDPage(period publicationPeriod, publication archivePublication) archivePublication {
	urls := []string{publication.SourceURL, cpdPageURL(period, true)}
	for _, url := range urls {
		if url == "" {
			continue
		}
		page, err := util.GetPage(url)
		if err != nil {
			continue
		}
		if date, ok := parsePublicationPageDate(page); ok {
			return archivePublication{Date: date, SourceURL: url, Priority: archivePriorityHTML}
		}
	}
	log.Warnf("cbr_indicators_cpd: не найдена дата версии %04d-%02d — fallback", period.Year, period.Month)
	return archivePublication{SourceURL: publication.SourceURL, Priority: archivePriorityHTML}
}

// fetchSurveyArchive разбирает индекс мониторинга предприятий и качает страницы
// только дельта-периодов. Ссылки в индексе — и <MMYY>, и <MM_YY>.
func fetchSurveyArchive(periods map[publicationPeriod]bool) map[publicationPeriod]archivePublication {
	page, err := util.GetPage(surveyArchiveURL)
	if err != nil {
		log.Warnf("cbr_survey: архив %s недоступен (%v) — fallback", surveyArchiveURL, err)
		return map[publicationPeriod]archivePublication{}
	}
	links, err := parseSurveyArchive(page)
	if err != nil {
		log.Warnf("cbr_survey: %v — fallback", err)
		return map[publicationPeriod]archivePublication{}
	}
	result := make(map[publicationPeriod]archivePublication, len(periods))
	for period := range periods {
		url, ok := links[period]
		if !ok {
			continue
		}
		page, err := util.GetPage(url)
		if err != nil {
			log.Warnf("cbr_survey: версия %s недоступна (%v) — fallback", url, err)
			continue
		}
		date, ok := parsePublicationPageDate(page)
		if !ok {
			log.Warnf("cbr_survey: на странице %s не найдена дата — fallback", url)
			continue
		}
		result[period] = archivePublication{Date: date, SourceURL: url, Priority: archivePriorityHTML}
	}
	return result
}

// buildSeriesPublications превращает точки источников в строки series_publication:
// точная дата из архива → quality=date; иначе fallback конец M+1.
func buildSeriesPublications(points []publicationPoint, archives map[string]map[publicationPeriod]archivePublication) []util.SeriesPublication {
	result := make([]util.SeriesPublication, 0, len(points))
	for _, point := range points {
		period := publicationPeriod{Year: point.Date.Year(), Month: point.Date.Month()}
		row := util.SeriesPublication{
			Source:      point.Source,
			Series:      point.Series,
			PeriodDate:  point.Date,
			Quality:     "fallback",
			SourceURL:   archiveURLForSource(point.Archive),
			PublishedAt: fallbackPublishedAt(point.Date),
		}
		if publication, ok := archives[point.Archive][period]; ok && !publication.Date.IsZero() {
			row.PublishedAt = dateOnlyPublishedAt(publication.Date)
			row.Quality = "date"
			row.SourceURL = publication.SourceURL
		}
		result = append(result, row)
	}
	return result
}

// archiveURLForSource — ссылка на индекс архива, которая пишется в fallback-строке.
func archiveURLForSource(source string) string {
	switch source {
	case cbrInflExpTable:
		return inflExpArchiveURL
	case cbrIndicatorsCpdTable:
		return cpdArchiveURL
	case publicationSourceSurvey:
		return surveyArchiveURL
	default:
		return ""
	}
}

// cpdPageURL — HTML-страница версии; padded=true даёт CPD_YYYY-MM, которым ЦБ
// чинит часть битых ссылок вида CPD_2022-6.
func cpdPageURL(period publicationPeriod, padded bool) string {
	if padded {
		return fmt.Sprintf("%sCPD_%04d-%02d/", cpdArchiveURL, period.Year, int(period.Month))
	}
	return fmt.Sprintf("%sCPD_%04d-%d/", cpdArchiveURL, period.Year, int(period.Month))
}
