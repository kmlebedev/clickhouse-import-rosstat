package cbr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
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

	// ondkpForecastCountsQuery — сверка записи: число строк прогноза по каждому выпуску после
	// вставки. FINAL обязателен — иначе count() увидел бы несмёрженные дубли, а группировка по
	// release_year отвечает на вопрос «все ли строки выпуска записаны» одним запросом.
	ondkpForecastCountsQuery = "SELECT release_year, count() FROM " + cbrOndkpForecastTable + " FINAL GROUP BY release_year"

	// Допустимый диапазон годов выпуска и прогноза. Корпус — 2000–2027, границы взяты с
	// запасом: вне диапазона не выпуск, а мусор разбора (ноль, обрезанный год). Значение
	// проверяется до приведения к UInt16, поэтому ошибка не становится переполнением.
	ondkpYearMin = 1990
	ondkpYearMax = 2100

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

// ondkpYearU16 проверяет год выпуска или прогноза и приводит его к типу колонки UInt16.
// Диапазон ondkpYearMin..ondkpYearMax уже, чем UInt16: голое приведение пропустило бы мусор
// разбора (отрицательный или обрезанный год) и завернуло бы его в правдоподобное число.
func ondkpYearU16(year int) (uint16, error) {
	if year < ondkpYearMin || year > ondkpYearMax {
		return 0, fmt.Errorf("год %d вне диапазона %d–%d", year, ondkpYearMin, ondkpYearMax)
	}
	return uint16(year), nil
}

// ondkpYearOf разбирает год, захваченный регулярным выражением, и проверяет тот же диапазон.
// Переполнение Atoi (слишком длинная цифровая группа) — ошибка разбора, а не ноль.
func ondkpYearOf(digits string) (int, error) {
	year, err := strconv.Atoi(digits)
	if err != nil {
		return 0, fmt.Errorf("год %q: %w", digits, err)
	}
	if _, err = ondkpYearU16(year); err != nil {
		return 0, err
	}
	return year, nil
}

// ondkpCountU16 проверяет число прогнозных строк release.forecastRows перед приведением к
// UInt16: без верхней границы значение завернулось бы молча (замечание CodeQL к вставке).
func ondkpCountU16(count int) (uint16, error) {
	if count < 0 || count > math.MaxUint16 {
		return 0, fmt.Errorf("число прогнозных строк %d вне диапазона 0–%d", count, math.MaxUint16)
	}
	return uint16(count), nil
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
	var parseErr error
	add := func(candidate ondkpRelease) {
		if current, ok := best[candidate.year]; ok && ondkpRank(current.format) >= ondkpRank(candidate.format) {
			return
		}
		best[candidate.year] = candidate
	}
	doc.Find("div.document-regular").Each(func(_ int, block *goquery.Selection) {
		if parseErr != nil {
			return
		}
		href, ok := block.Find("a[href]").First().Attr("href")
		if !ok {
			return
		}
		title := strings.Join(strings.Fields(block.Find(".document-regular_name_visible").First().Text()), " ")
		dateText := block.Find(".document-regular_date").First().Text()
		candidate, ok, candidateErr := ondkpCandidate(href, title, dateText)
		if candidateErr != nil {
			parseErr = candidateErr
			return
		}
		if ok {
			add(candidate)
		}
	})
	doc.Find("tr").Each(func(_ int, row *goquery.Selection) {
		if parseErr != nil {
			return
		}
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
		year, yearErr := ondkpYearOf(yearMatch[1])
		if yearErr != nil {
			parseErr = fmt.Errorf("cbr_ondkp: строка «Вестника» %q: %w", rowText, yearErr)
			return
		}
		add(ondkpRelease{
			year:        year,
			publishedAt: ondkpDateOf(rowText),
			url:         cbrUrl + href,
			format:      cbrOndkpFormatZipDoc,
		})
	})
	if parseErr != nil {
		return nil, parseErr
	}
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
// формат и ссылку. Карточки не про ОНДКП (перечень изменений, брошюры) отбрасываются
// (ok = false); неразбираемый или выпавший из диапазона год — ошибка: это порча разметки,
// а не повод молча пропустить выпуск.
func ondkpCandidate(href, title, dateText string) (ondkpRelease, bool, error) {
	href = strings.TrimSpace(href)
	if !strings.HasPrefix(href, "/") {
		return ondkpRelease{}, false, nil
	}
	if strings.Contains(href, "Brochure") || strings.Contains(href, "list_note") {
		return ondkpRelease{}, false, nil
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
		return ondkpRelease{}, false, nil
	}
	yearMatch := ondkpYearInTitleRe.FindStringSubmatch(strings.Join(strings.Fields(title), " "))
	if yearMatch == nil {
		return ondkpRelease{}, false, nil
	}
	year, err := ondkpYearOf(yearMatch[1])
	if err != nil {
		return ondkpRelease{}, false, fmt.Errorf("cbr_ondkp: карточка %q: %w", href, err)
	}
	release := ondkpRelease{year: year, format: format, url: cbrUrl + href}
	if ondkpYearPageRe.MatchString(href) {
		// Страница выпуска: артефакт ищется на ней (Import), URL карточки — не файл.
		release.pageUrl = cbrUrl + href
		release.url = ""
	}
	release.publishedAt = ondkpDateOf(dateText)
	return release, true, nil
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
//
// Ошибки выпусков копятся errors.Join (эталон — cbr_mt_forecast): выпуски независимы, и один
// 404 или битый PDF не отменяет остальные 27 скачиваний. Прогон при этом красный, а успешные
// выпуски записаны. PDF без извлекаемого текста — ошибка выпуска, а не регистрация с
// forecast_rows=0: гейт по release_year не перечитывает известный выпуск и заморозил бы дыру.
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
	var errs []error
	for _, release := range releases {
		if known[release.year] {
			continue
		}
		artifactURL := release.url
		if artifactURL == "" && release.pageUrl != "" {
			pageHTML, pageErr := util.GetPage(release.pageUrl)
			if pageErr != nil {
				errs = append(errs, fmt.Errorf("cbr_ondkp: выпуск %d: %s: %w", release.year, release.pageUrl, pageErr))
				continue
			}
			var linkErr error
			if artifactURL, linkErr = ondkpYearPagePDF(pageHTML, release.year); linkErr != nil {
				errs = append(errs, linkErr)
				continue
			}
		}
		if artifactURL == "" {
			errs = append(errs, fmt.Errorf("cbr_ondkp: у выпуска %d нет артефакта — разметка страницы изменилась", release.year))
			continue
		}
		// Артефакт, найденный на странице выпуска, может оказаться проектом документа
		// (2022 — на странице только он): формат уточняется по имени файла.
		if strings.Contains(artifactURL, "on_project_") {
			release.format = cbrOndkpFormatPDFProject
		}
		body, bodyErr := util.GetFileBytes(artifactURL)
		if bodyErr != nil {
			errs = append(errs, fmt.Errorf("cbr_ondkp: выпуск %d, %s: %w", release.year, artifactURL, bodyErr))
			continue
		}
		release.url = artifactURL
		analyzed, rows, analyzeErr := ondkpAnalyzeRelease(ctx, release, body)
		if analyzeErr != nil {
			errs = append(errs, analyzeErr)
			continue
		}
		forecasts = append(forecasts, rows...)
		fresh = append(fresh, analyzed)
	}
	if len(fresh) == 0 {
		if len(errs) > 0 {
			return 0, errors.Join(errs...)
		}
		log.Infof("%s: новых выпусков нет (%d в корпусе)", cbrOndkpTable, len(known))
		return 0, nil
	}
	// Порядок записи — часть контракта: прогноз раньше реестра. Реестр — гейт прогона, и,
	// записав его первым, сбой второго батча оставил бы forecast_rows≠факт навсегда (выпуск
	// больше не скачается). При обратном порядке сбой оставляет только строки прогноза без
	// реестра: следующий прогон повторит вставку идемпотентно (ключ ReplacingMergeTree тот же).
	if len(forecasts) > 0 {
		if insertErr := ondkpInsertForecasts(ctx, conn, forecasts); insertErr != nil {
			errs = append(errs, insertErr)
			return 0, errors.Join(errs...)
		}
	}
	if insertErr := ondkpInsertReleases(ctx, conn, fresh); insertErr != nil {
		errs = append(errs, insertErr)
		return 0, errors.Join(errs...)
	}
	// Сверка после записи: forecast_rows реестра обязан совпасть с числом строк прогноза,
	// записанных по этому выпуску (прецедент — mtForecastReleaseWritten). ReplacingMergeTree
	// схлопывает строки с одинаковым ключом (release_year, indicator, year), поэтому Send без
	// сверки мог бы записать меньше строк, чем обещает реестр.
	if verifyErr := ondkpVerifyForecastCounts(ctx, conn, fresh); verifyErr != nil {
		errs = append(errs, verifyErr)
		return 0, errors.Join(errs...)
	}
	log.Infof("%s: %d выпусков, %d прогнозных строк", cbrOndkpTable, len(fresh), len(forecasts))
	return int64(len(fresh)), errors.Join(errs...)
}

// ondkpAnalyzeRelease дополняет выпуск атрибутами скачанного артефакта (sha256 и размер;
// URL уже проставлен вызывающим) и разбирает прогнозную таблицу. Ошибка PDFText — ошибка
// выпуска, а не предупреждение: сентинела «штатная нечитаемость» util.PDFText не объявляет, а
// весь корпус 2000–2027 приходит с текстовым слоем, поэтому любой сбой извлечения означает
// подменённый/нечитаемый файл или отсутствие pdftotext. ZIP с .doc текстового слоя не имеет по
// формату: прогноза у него нет, и это не ошибка.
func ondkpAnalyzeRelease(ctx context.Context, release ondkpRelease, body []byte) (ondkpRelease, []ondkpForecastRow, error) {
	sum := sha256.Sum256(body)
	release.sha256 = hex.EncodeToString(sum[:])
	release.bytes = len(body)
	if release.format == cbrOndkpFormatZipDoc {
		return release, nil, nil
	}
	text, err := util.PDFText(ctx, body, util.PDFLayout)
	if err != nil {
		return ondkpRelease{}, nil, fmt.Errorf("cbr_ondkp: выпуск %d, %s: извлечение текста PDF: %w", release.year, release.url, err)
	}
	rows, err := ondkpForecastRows(text, release.year)
	if err != nil {
		return ondkpRelease{}, nil, fmt.Errorf("cbr_ondkp: выпуск %d: %w", release.year, err)
	}
	release.forecastRows = len(rows)
	return release, rows, nil
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

// ondkpInsertReleases пишет реестр выпусков одним батчем. Год и forecast_rows проверяются
// перед приведением к UInt16: голое приведение молча завернуло бы значение вне диапазона
// колонки (замечание CodeQL).
func ondkpInsertReleases(ctx context.Context, conn driver.Conn, releases []ondkpRelease) error {
	batch, err := conn.PrepareBatch(ctx, cbrOndkpInsert)
	if err != nil {
		return err
	}
	for _, release := range releases {
		year, yearErr := ondkpYearU16(release.year)
		if yearErr != nil {
			return ondkpAbortBatch(batch, cbrOndkpTable, fmt.Errorf("%s: %w", cbrOndkpTable, yearErr))
		}
		forecastRows, rowsErr := ondkpCountU16(release.forecastRows)
		if rowsErr != nil {
			return ondkpAbortBatch(batch, cbrOndkpTable,
				fmt.Errorf("%s: выпуск %d: %w", cbrOndkpTable, release.year, rowsErr))
		}
		if err = batch.Append(year, release.publishedAt, release.url, release.format,
			release.sha256, uint64(release.bytes), forecastRows); err != nil {
			return ondkpAbortBatch(batch, cbrOndkpTable,
				fmt.Errorf("%s: Append (%d): %w", cbrOndkpTable, release.year, err))
		}
	}
	return batch.Send()
}

// ondkpInsertForecasts пишет прогнозные точки одним батчем. Годы выпуска и прогноза
// проверяются перед приведением к UInt16 (замечание CodeQL).
func ondkpInsertForecasts(ctx context.Context, conn driver.Conn, rows []ondkpForecastRow) error {
	batch, err := conn.PrepareBatch(ctx, cbrOndkpForecastInsert)
	if err != nil {
		return err
	}
	for _, row := range rows {
		releaseYear, releaseErr := ondkpYearU16(row.releaseYear)
		if releaseErr != nil {
			return ondkpAbortBatch(batch, cbrOndkpForecastTable,
				fmt.Errorf("%s: %w", cbrOndkpForecastTable, releaseErr))
		}
		year, yearErr := ondkpYearU16(row.year)
		if yearErr != nil {
			return ondkpAbortBatch(batch, cbrOndkpForecastTable,
				fmt.Errorf("%s: выпуск %d, %s: %w", cbrOndkpForecastTable, row.releaseYear, row.indicator, yearErr))
		}
		if err = batch.Append(releaseYear, row.indicator, year, row.lo, row.hi); err != nil {
			return ondkpAbortBatch(batch, cbrOndkpForecastTable,
				fmt.Errorf("%s: Append (%d, %s, %d): %w", cbrOndkpForecastTable, row.releaseYear,
					row.indicator, row.year, err))
		}
	}
	return batch.Send()
}

// ondkpAbortBatch закрывает батч после ошибки: без Abort соединение останется в пуле
// (стандарт §5.3). Сбой Abort — предупреждение, а не замена первопричины (прецедент —
// mpAbortBatch в cbr/mp_summary.go).
func ondkpAbortBatch(batch driver.Batch, table string, cause error) error {
	if abortErr := batch.Abort(); abortErr != nil {
		log.Warnf("%s: не удалось закрыть батч после ошибки: %v", table, abortErr)
	}
	return cause
}

// ondkpVerifyForecastCounts сверяет forecast_rows реестра с числом строк, реально записанных в
// cbr_ondkp_forecast по этому выпуску: после вставки реестр не должен обещать строк, которых
// нет (прецедент — mtForecastReleaseWritten). Одного успешного Send мало: ReplacingMergeTree
// схлопывает совпавшие ключи (release_year, indicator, year), и прогноз с дублем записался бы
// меньшим числом строк, чем обещает реестр. FINAL обязателен — иначе count() увидел бы
// несмёрженные дубли. Ошибка запроса — тоже ошибка прогона: «считаем, что сошлось» скрыло бы
// расхождение.
func ondkpVerifyForecastCounts(ctx context.Context, conn driver.Conn, releases []ondkpRelease) error {
	rows, err := conn.Query(ctx, ondkpForecastCountsQuery)
	if err != nil {
		return fmt.Errorf("cbr_ondkp: сверка %s: %w", cbrOndkpForecastTable, err)
	}
	defer func() { _ = rows.Close() }()
	written := make(map[int]int)
	for rows.Next() {
		var year uint16
		var count uint64
		if err = rows.Scan(&year, &count); err != nil {
			return fmt.Errorf("cbr_ondkp: чтение сверки %s: %w", cbrOndkpForecastTable, err)
		}
		written[int(year)] = int(count)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("cbr_ondkp: сверка %s не дочитана: %w", cbrOndkpForecastTable, err)
	}
	var errs []error
	for _, release := range releases {
		if got := written[release.year]; got != release.forecastRows {
			errs = append(errs, fmt.Errorf("cbr_ondkp: выпуск %d: в %s записано %d строк, forecast_rows=%d — реестр и прогноз разошлись",
				release.year, cbrOndkpForecastTable, got, release.forecastRows))
		}
	}
	return errors.Join(errs...)
}

// cbrOndkpImporter — реестр выпусков ОНДКП; регистрируется в cbr/importers.go.
var cbrOndkpImporter = &cbrOndkp{ClickHouseImport: util.ClickHouseImport{
	TableName: cbrOndkpTable,
}}
