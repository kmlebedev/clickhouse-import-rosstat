package cbr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// ОНДКП — «Основные направления единой государственной денежно-кредитной политики»:
// 28 винтажей 2000–2027, у каждого — цель по инфляции и прогноз базового сценария.
// Две таблицы: реестр выпусков (cbr_ondkp) и прогнозные точки (cbr_ondkp_forecast).
// Разведка и правила разбора — research/2026-11-14-ondkp-extraction.md.
const (
	cbrOndkpName           = "cbr_ondkp"
	cbrOndkpTable          = "cbr_ondkp"
	cbrOndkpForecastTable  = "cbr_ondkp_forecast"
	cbrOndkpIndexUrl       = "https://www.cbr.ru/about_br/publ/ondkp/"
	cbrOndkpInsert         = "INSERT INTO " + cbrOndkpTable
	cbrOndkpForecastInsert = "INSERT INTO " + cbrOndkpForecastTable
	cbrOndkpDateFormat     = "02.01.2006"

	// Форматы артефактов: прямое PDF (2010–2020), страница выпуска с PDF (2021–2027),
	// проект документа (2022 — только он), ZIP с .doc в выпуске «Вестника» (2000–2009).
	cbrOndkpFormatPDF        = "pdf"
	cbrOndkpFormatPDFProject = "pdf-project"
	cbrOndkpFormatZipDoc     = "zip-doc"
)

// cbrOndkpDdl — реестр выпусков. sha256 — версия артефакта; published_at Nullable:
// у страниц выпусков 2021–2027 дата публикации на индексе не показана.
const cbrOndkpDdl = "CREATE TABLE IF NOT EXISTS %s (" +
	" release_year UInt16," +
	" published_at Nullable(Date)," +
	" url String," +
	" format LowCardinality(String)," +
	" sha256 String," +
	" bytes UInt64," +
	" forecast_rows UInt16" +
	") ENGINE = ReplacingMergeTree ORDER BY (release_year);"

// cbrOndkpForecastDdl — прогнозные точки. lo == hi — точка, иначе диапазон.
const cbrOndkpForecastDdl = "CREATE TABLE IF NOT EXISTS %s (" +
	" release_year UInt16," +
	" indicator LowCardinality(String)," +
	" year UInt16," +
	" lo Float64," +
	" hi Float64" +
	") ENGINE = ReplacingMergeTree ORDER BY (release_year, indicator, year);"

var (
	ondkpYearInTitleRe = regexp.MustCompile(`на\s*(20\d{2})\s*год`)
	ondkpYearPageRe    = regexp.MustCompile(`^/about_br/publ/ondkp/on_(20\d{2})_\d{4}/$`)
	ondkpPDFRe         = regexp.MustCompile(`^/Content/Document/File/\d+/on_(?:project_)?(20\d{2})\(`)
	ondkpVestnikRe     = regexp.MustCompile(`^/Queries/XsltBlock/File/\d+/-1/\d+$`)
	ondkpDateRe        = regexp.MustCompile(`(\d{2}\.\d{2}\.\d{4})`)
)

// ondkpRelease — выпуск корпуса: год политики, дата публикации, артефакт и формат.
// pageUrl непустой, когда артефакт лежит на странице выпуска (2021–2027).
type ondkpRelease struct {
	year         int
	publishedAt  *time.Time
	url          string
	pageUrl      string
	format       string
	sha256       string
	bytes        int
	forecastRows int
}

// ondkpRank — приоритет артефакта одного года: PDF разбирается, ZIP — нет.
func ondkpRank(format string) int {
	switch format {
	case cbrOndkpFormatPDFProject:
		// Проект документа — это явный файл, а не страница выпуска: он приоритетнее.
		return 4
	case cbrOndkpFormatPDF:
		return 3
	case cbrOndkpFormatZipDoc:
		return 1
	}
	return 2
}

// ondkpReleases собирает реестр корпуса со страницы индекса: 20 карточек документов
// (страницы 2020–2027 и PDF 2010–2020) плюс таблица «Вестника» (ZIP 2000–2009). Один
// выпуск может быть представлен двумя артефактами (2010 — ZIP и PDF), берётся лучший по
// ondkpRank. Сортировка — по году.
func ondkpReleases(page []byte) ([]ondkpRelease, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("cbr_ondkp: разбор страницы индекса: %w", err)
	}
	best := map[int]ondkpRelease{}
	add := func(candidate ondkpRelease) {
		if current, ok := best[candidate.year]; ok && ondkpRank(current.format) >= ondkpRank(candidate.format) {
			return
		}
		best[candidate.year] = candidate
	}
	doc.Find("div.document-regular").Each(func(_ int, block *goquery.Selection) {
		href, ok := block.Find("a[href]").First().Attr("href")
		if !ok {
			return
		}
		title := strings.Join(strings.Fields(block.Find(".document-regular_name_visible").First().Text()), " ")
		dateText := block.Find(".document-regular_date").First().Text()
		if candidate, ok := ondkpCandidate(href, title, dateText); ok {
			add(candidate)
		}
	})
	doc.Find("tr").Each(func(_ int, row *goquery.Selection) {
		href, ok := row.Find("a[href]").First().Attr("href")
		if !ok || !ondkpVestnikRe.MatchString(href) {
			return
		}
		// Ячейка года: «на 2009 год и период 2010 и 2011 годов»; дата — «от 26.11.2009».
		rowText := strings.Join(strings.Fields(row.Text()), " ")
		yearMatch := ondkpYearInTitleRe.FindStringSubmatch(rowText)
		if yearMatch == nil {
			return
		}
		year, convErr := strconv.Atoi(yearMatch[1])
		if convErr != nil {
			return
		}
		add(ondkpRelease{
			year:        year,
			publishedAt: ondkpDateOf(rowText),
			url:         cbrUrl + href,
			format:      cbrOndkpFormatZipDoc,
		})
	})
	if len(best) == 0 {
		return nil, fmt.Errorf("cbr_ondkp: на странице индекса не найден ни один выпуск — разметка изменилась")
	}
	releases := make([]ondkpRelease, 0, len(best))
	for _, release := range best {
		releases = append(releases, release)
	}
	sort.Slice(releases, func(i, j int) bool { return releases[i].year < releases[j].year })
	return releases, nil
}

// ondkpCandidate разбирает одну карточку документа: год из заголовка («на 2027 год»),
// формат и ссылку. Карточки не про ОНДКП (перечень изменений, брошюры) отбрасываются.
func ondkpCandidate(href, title, dateText string) (ondkpRelease, bool) {
	href = strings.TrimSpace(href)
	if !strings.HasPrefix(href, "/") {
		return ondkpRelease{}, false
	}
	if strings.Contains(href, "Brochure") || strings.Contains(href, "list_note") {
		return ondkpRelease{}, false
	}
	format := ""
	switch {
	case ondkpYearPageRe.MatchString(href):
		format = cbrOndkpFormatPDF
	case ondkpPDFRe.MatchString(href):
		format = cbrOndkpFormatPDF
		if strings.Contains(href, "on_project_") {
			format = cbrOndkpFormatPDFProject
		}
	default:
		return ondkpRelease{}, false
	}
	yearMatch := ondkpYearInTitleRe.FindStringSubmatch(strings.Join(strings.Fields(title), " "))
	if yearMatch == nil {
		return ondkpRelease{}, false
	}
	year, err := strconv.Atoi(yearMatch[1])
	if err != nil {
		return ondkpRelease{}, false
	}
	release := ondkpRelease{year: year, format: format, url: cbrUrl + href}
	if ondkpYearPageRe.MatchString(href) {
		// Страница выпуска: артефакт ищется на ней (Import), URL карточки — не файл.
		release.pageUrl = cbrUrl + href
		release.url = ""
	}
	release.publishedAt = ondkpDateOf(dateText)
	return release, true
}

// ondkpDateOf вытаскивает дату публикации из текста карточки/строки. Нет даты — nil.
func ondkpDateOf(text string) *time.Time {
	match := ondkpDateRe.FindStringSubmatch(text)
	if match == nil {
		return nil
	}
	date, err := time.Parse(cbrOndkpDateFormat, match[1])
	if err != nil {
		return nil
	}
	return &date
}

// ondkpYearPagePDF ищет на странице выпуска ссылку на PDF документа. Брошюра и перечень
// изменений не подходят: это не ОНДКП.
func ondkpYearPagePDF(page []byte, year int) (string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return "", fmt.Errorf("cbr_ondkp: разбор страницы выпуска: %w", err)
	}
	// Имя файла проекта документа отличается префиксом: on_project_2022(2023-2024).pdf.
	prefix := regexp.MustCompile(fmt.Sprintf(`/on_(?:project_)?%d\(`, year))
	var found string
	doc.Find("a[href]").EachWithBreak(func(_ int, link *goquery.Selection) bool {
		href, ok := link.Attr("href")
		if !ok {
			return true
		}
		if strings.Contains(href, "Brochure") {
			return true
		}
		if strings.HasPrefix(href, "/Content/Document/File/") && prefix.MatchString(href) && strings.HasSuffix(href, ".pdf") {
			found = cbrUrl + href
			return false
		}
		return true
	})
	if found == "" {
		return "", fmt.Errorf("cbr_ondkp: на странице выпуска %d не найдена ссылка на PDF документа", year)
	}
	return found, nil
}

// cbrOndkp — импортёр корпуса: свой Import (две таблицы и загрузка артефактов), поэтому
// шаблон util.ClickHouseImport не используется. Регистрируется в cbr/importers.go.
type cbrOndkp struct {
	util.ClickHouseImport
}

// Name — имя импортёра в CLICKHOUSE_IMPORT_STAT и в dagu.
func (s *cbrOndkp) Name() string { return cbrOndkpName }

// Import — DDL обеих таблиц → реестр со страницы → по каждому неизвестному году:
// скачать артефакт, посчитать sha256, для PDF — разобрать прогнозную таблицу → записать.
// Известность выпуска держит таблица cbr_ondkp (release_year), а не HEAD-префлайт:
// артефакт — набор из 28 файлов, и count() у известной версии не сошёлся бы.
func (s *cbrOndkp) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for table, ddl := range map[string]string{cbrOndkpTable: cbrOndkpDdl, cbrOndkpForecastTable: cbrOndkpForecastDdl} {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, table)); err != nil {
			return 0, fmt.Errorf("cbr_ondkp: DDL %s: %w", table, err)
		}
	}
	known, err := ondkpKnownYears(ctx, conn)
	if err != nil {
		return 0, err
	}
	page, err := util.GetPage(cbrOndkpIndexUrl)
	if err != nil {
		return 0, fmt.Errorf("cbr_ondkp: %s: %w", cbrOndkpIndexUrl, err)
	}
	releases, err := ondkpReleases(page)
	if err != nil {
		return 0, err
	}
	var fresh []ondkpRelease
	var forecasts []ondkpForecastRow
	for _, release := range releases {
		if known[release.year] {
			continue
		}
		artifactURL := release.url
		if artifactURL == "" && release.pageUrl != "" {
			pageHTML, pageErr := util.GetPage(release.pageUrl)
			if pageErr != nil {
				return 0, fmt.Errorf("cbr_ondkp: %s: %w", release.pageUrl, pageErr)
			}
			if artifactURL, err = ondkpYearPagePDF(pageHTML, release.year); err != nil {
				return 0, err
			}
		}
		if artifactURL == "" {
			return 0, fmt.Errorf("cbr_ondkp: у выпуска %d нет артефакта — разметка страницы изменилась", release.year)
		}
		// Артефакт, найденный на странице выпуска, может оказаться проектом документа
		// (2022 — на странице только он): формат уточняется по имени файла.
		if strings.Contains(artifactURL, "on_project_") {
			release.format = cbrOndkpFormatPDFProject
		}
		body, err := util.GetFileBytes(artifactURL)
		if err != nil {
			return 0, fmt.Errorf("cbr_ondkp: выпуск %d, %s: %w", release.year, artifactURL, err)
		}
		sum := sha256.Sum256(body)
		release.url = artifactURL
		release.sha256 = hex.EncodeToString(sum[:])
		release.bytes = len(body)
		if release.format != cbrOndkpFormatZipDoc {
			text, textErr := util.PDFText(ctx, body, util.PDFLayout)
			if textErr != nil {
				// Скан без текстового слоя не отменяет регистрацию выпуска: метаданные
				// полезны, а пропуск чисел фиксируется forecast_rows=0 и предупреждением.
				log.Warnf("cbr_ondkp: выпуск %d: %v — прогноз не разобран", release.year, textErr)
			} else {
				rows, parseErr := ondkpForecastRows(text, release.year)
				if parseErr != nil {
					return 0, fmt.Errorf("cbr_ondkp: выпуск %d: %w", release.year, parseErr)
				}
				forecasts = append(forecasts, rows...)
				release.forecastRows = len(rows)
			}
		}
		fresh = append(fresh, release)
	}
	// Производные (каталог трёх рядов прогноза и витрина) публикуются и на no-op-пути:
	// сбой каталога или витрины лечится следующим прогоном (ARCHITECTURE §8 п. 13).
	publishDerived := afterCbrSeries(cbrOndkpSeriesMeta, cbrOndkpForecastView)
	if len(fresh) == 0 {
		if err = publishDerived(ctx, conn); err != nil {
			return 0, err
		}
		log.Infof("%s: новых выпусков нет (%d в корпусе)", cbrOndkpTable, len(known))
		return 0, nil
	}
	if err = ondkpInsertReleases(ctx, conn, fresh); err != nil {
		return 0, err
	}
	if len(forecasts) > 0 {
		if err = ondkpInsertForecasts(ctx, conn, forecasts); err != nil {
			return 0, err
		}
	}
	if err = publishDerived(ctx, conn); err != nil {
		return 0, err
	}
	log.Infof("%s: %d выпусков, %d прогнозных строк", cbrOndkpTable, len(fresh), len(forecasts))
	return int64(len(fresh)), nil
}

// ondkpKnownYears — годы, уже записанные в реестр: повторный прогон артефакты не качает.
func ondkpKnownYears(ctx context.Context, conn driver.Conn) (map[int]bool, error) {
	rows, err := conn.Query(ctx, "SELECT release_year FROM "+cbrOndkpTable+" FINAL")
	if err != nil {
		return nil, fmt.Errorf("cbr_ondkp: известные выпуски: %w", err)
	}
	defer func() { _ = rows.Close() }()
	known := make(map[int]bool)
	for rows.Next() {
		var year uint16
		if err = rows.Scan(&year); err != nil {
			return nil, fmt.Errorf("cbr_ondkp: чтение release_year: %w", err)
		}
		known[int(year)] = true
	}
	return known, rows.Err()
}

// ondkpInsertReleases пишет реестр выпусков одним батчем.
func ondkpInsertReleases(ctx context.Context, conn driver.Conn, releases []ondkpRelease) error {
	batch, err := conn.PrepareBatch(ctx, cbrOndkpInsert)
	if err != nil {
		return err
	}
	for _, release := range releases {
		if err = batch.Append(uint16(release.year), release.publishedAt, release.url, release.format,
			release.sha256, uint64(release.bytes), uint16(release.forecastRows)); err != nil {
			if abortErr := batch.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки Append: %v", cbrOndkpTable, abortErr)
			}
			return fmt.Errorf("%s: Append (%d): %w", cbrOndkpTable, release.year, err)
		}
	}
	return batch.Send()
}

// ondkpInsertForecasts пишет прогнозные точки одним батчем.
func ondkpInsertForecasts(ctx context.Context, conn driver.Conn, rows []ondkpForecastRow) error {
	batch, err := conn.PrepareBatch(ctx, cbrOndkpForecastInsert)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err = batch.Append(uint16(row.releaseYear), row.indicator, uint16(row.year), row.lo, row.hi); err != nil {
			if abortErr := batch.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки Append: %v", cbrOndkpForecastTable, abortErr)
			}
			return fmt.Errorf("%s: Append (%d, %s, %d): %w", cbrOndkpForecastTable, row.releaseYear,
				row.indicator, row.year, err)
		}
	}
	return batch.Send()
}

// cbrOndkpImporter — реестр выпусков ОНДКП; регистрируется в cbr/importers.go.
var cbrOndkpImporter = &cbrOndkp{ClickHouseImport: util.ClickHouseImport{
	TableName: cbrOndkpTable,
}}
