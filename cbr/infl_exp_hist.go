package cbr

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// История инфляционных ожиданий из архива бюллетеней ЦБ: каждая волна «Инфляционные
// ожидания и потребительские настроения населения» печатает таблицу «Инфляционные ожидания
// населения, %» с рядом месяцев, где последнее значение — значение этой волны. Тот же
// источник использует cbr_infl_exp (книга XLSX), поэтому волны пишутся в ту же таблицу под
// своим именем импортёра (прецедент — cbr_infl_exp_op).
//
// Книга XLSX с 2023 года стала короткой (лист «Данные для графиков» начинается с 2023-01),
// поэтому 2019–2022 в БД не было вовсе: таблица в бюллетенях есть с волны 2019-01, в
// выпусках 2017–2018 её нет (числа только на графиках с подписями, вёрстка двухколоночная).
// Разведка — research/2026-11-14-infl-exp-history-source.md.
const (
	cbrInflExpHistName = "cbr_infl_exp_hist"

	// cbrInflExpHistFrom — первая волна с таблицей «ФОМ (медиана)»: до 2019-01 числа
	// напечатаны только на графиках (см. разведку §3). Ранние волны не скачиваются.
	cbrInflExpHistFrom = "2019-01"

	// cbrInflExpHistInsert — та же таблица, что у книги и оперативного PDF: ключ (name, date).
	cbrInflExpHistInsert = "INSERT INTO " + cbrInflExpTable
)

var (
	// cbrInflExpHistHrefRe — ссылка волны в любом из двух видов: прямой PDF
	// /Collection/Collection/File/<id>/Infl_exp_ГГ-ММ.pdf или страница
	// …/inflationary_expectations/Infl_exp_ГГ-ММ/ (22-01 и позже, PDF лежит внутри страницы).
	cbrInflExpHistHrefRe = regexp.MustCompile(`href="([^"]*Infl_exp_(\d{2})-(\d{2})(?:\.pdf|/)?)"`)
	// cbrInflExpHistMonthRe — подпись месяца волны: имя файла или страницы.
	cbrInflExpHistMonthRe = regexp.MustCompile(`Infl_exp_(\d{2})-(\d{2})`)

	// Строки таблицы: «ФОМ (медиана) предыдущие 12 месяцев …» (наблюдаемая) и
	// «… следующие 12 месяцев …» (ожидаемая). В волнах 2019–2021 подпись подробнее:
	// «ФОМ (медиана, наблюдаемая инфляция)». Мягкий перенос (U+00AD) в подписи
	// встречается — «предыU+00ADдущие», поэтому переносы снимаются до разбора.
	cbrInflExpHistObservedRe = regexp.MustCompile(`^\s*ФОМ \(медиана[^)]*\)\s*преды\S*дущие\s+12\s+месяцев(.*)$`)
	cbrInflExpHistExpectedRe = regexp.MustCompile(`^\s*ФОМ \(медиана[^)]*\)\s*следующие\s+12\s+месяцев(.*)$`)
	// cbrInflExpHistValueRe — значение медианы: одна-две цифры после запятой, как печатает ЦБ.
	cbrInflExpHistValueRe = regexp.MustCompile(`\d{1,2},\d{1,2}`)
)

// inflExpHistWave — волна архива: месяц выпуска и ссылка на артефакт.
type inflExpHistWave struct {
	month time.Time
	url   string
}

// inflExpHistMonthOf разбирает «ГГ-ММ» волны в первое число месяца. Год двузначный, век —
// 2000-е: архив начинается 2013-09.
func inflExpHistMonthOf(href string) (time.Time, bool) {
	match := cbrInflExpHistMonthRe.FindStringSubmatch(href)
	if match == nil {
		return time.Time{}, false
	}
	year, err := strconv.Atoi(match[1])
	if err != nil {
		return time.Time{}, false
	}
	month, err := strconv.Atoi(match[2])
	if err != nil || month < 1 || month > 12 {
		return time.Time{}, false
	}
	return time.Date(2000+year, time.Month(month), 1, 0, 0, 0, 0, time.UTC), true
}

// inflExpHistWaves собирает все волны со страницы ожиданий: по одной на месяц, ссылка
// «прямой PDF» приоритетнее страницы волны (меньше запросов).
func inflExpHistWaves(page []byte) ([]inflExpHistWave, error) {
	byMonth := map[string]inflExpHistWave{}
	order := make([]string, 0, 256)
	for _, match := range cbrInflExpHistHrefRe.FindAllStringSubmatch(string(page), -1) {
		href := match[1]
		month, ok := inflExpHistMonthOf(href)
		if !ok {
			continue
		}
		key := month.Format("2006-01")
		current, exists := byMonth[key]
		if exists && strings.HasSuffix(current.url, ".pdf") {
			continue
		}
		if !exists {
			order = append(order, key)
		}
		byMonth[key] = inflExpHistWave{month: month, url: cbrUrl + href}
	}
	if len(byMonth) == 0 {
		return nil, fmt.Errorf("%s: на странице %s не найдено ни одной волны — разметка изменилась", cbrInflExpHistName, inflExpUrl)
	}
	waves := make([]inflExpHistWave, 0, len(byMonth))
	for _, key := range order {
		waves = append(waves, byMonth[key])
	}
	sort.Slice(waves, func(i, j int) bool { return waves[i].month.After(waves[j].month) })
	return waves, nil
}

// inflExpHistRows разбирает таблицу волны в две точки — наблюдаемую и ожидаемую инфляцию на
// 14-е число месяца волны (конвенция cbr_infl_exp). Нет таблицы — nil, nil: у выпусков
// 2017–2018 и части новых волн числа напечатаны только прозой/на графике, и это
// ограничение корпуса, а не сбой. Таблица есть, но значение не разобрано — ошибка.
func inflExpHistRows(text []byte, month time.Time) ([]util.FallbackRow, error) {
	date := inflExpPdfPointDate(month)
	var rows []util.FallbackRow
	for _, line := range strings.Split(normalizeInflExpHistText(text), "\n") {
		var series, rest string
		switch {
		case cbrInflExpHistObservedRe.MatchString(line):
			series = inflExpObservedName
			rest = cbrInflExpHistObservedRe.FindStringSubmatch(line)[1]
		case cbrInflExpHistExpectedRe.MatchString(line):
			series = inflExpExpectedName
			rest = cbrInflExpHistExpectedRe.FindStringSubmatch(line)[1]
		default:
			continue
		}
		values := cbrInflExpHistValueRe.FindAllString(rest, -1)
		if len(values) == 0 {
			return nil, fmt.Errorf("%s: волна %s, строка %q: в строке таблицы нет значений — разметка изменилась",
				cbrInflExpHistName, month.Format("2006-01"), strings.TrimSpace(line))
		}
		// Последнее значение строки — значение этой волны: колонки таблицы идут по месяцам
		// и заканчиваются текущим.
		value, err := strconv.ParseFloat(strings.ReplaceAll(values[len(values)-1], ",", "."), 32)
		if err != nil {
			return nil, fmt.Errorf("%s: волна %s, значение %q: %w", cbrInflExpHistName, month.Format("2006-01"), values[len(values)-1], err)
		}
		if value <= 0 || value >= 100 {
			return nil, fmt.Errorf("%s: волна %s, значение %v вне (0, 100) — разобрана не медиана",
				cbrInflExpHistName, month.Format("2006-01"), value)
		}
		rows = append(rows, util.FallbackRow{Series: series, Date: date, Value: value})
	}
	return rows, nil
}

// normalizeInflExpHistText снимает неразрывные пробелы и мягкие переносы: pdftotext печатает
// подпись «предыдущие» с U+00AD, и регулярка строки видит её как часть слова.
func normalizeInflExpHistText(text []byte) string {
	return strings.NewReplacer("\u00a0", " ", "\u00ad", "", "\r", "").Replace(string(text))
}

// cbrInflExpHist — импортёр истории ожиданий: свой Import (набор волн + общая таблица с
// книгой), поэтому шаблон util.ClickHouseImport не используется.
type cbrInflExpHist struct {
	util.ClickHouseImport
}

// Name — имя импортёра в CLICKHOUSE_IMPORT_STAT и в dagu; таблица — общая cbr_infl_exp.
func (s *cbrInflExpHist) Name() string { return cbrInflExpHistName }

// Import — страница ожиданий → волны → гейт по уже лежащим датам → по каждой недостающей
// волне: PDF (или страница волны → PDF) → таблица → строки. Ошибки волн копятся
// (errors.Join): сбой одной волны не отменяет остальные, но прогон красный. Производные
// (каталог и витрина v_cbr_macro) публикуются и на no-op-пути.
func (s *cbrInflExpHist) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	publishDerived := afterCbrMacro(cbrInflExpSeriesMeta)
	page, err := util.GetPage(inflExpUrl)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrInflExpHistName, inflExpUrl, err)
	}
	waves, err := inflExpHistWaves(page)
	if err != nil {
		return 0, err
	}
	known, err := cbrInflExpKnownDates(ctx, conn)
	if err != nil {
		return 0, err
	}
	from, err := time.Parse("2006-01", cbrInflExpHistFrom)
	if err != nil {
		return 0, fmt.Errorf("%s: начало истории %q: %w", cbrInflExpHistName, cbrInflExpHistFrom, err)
	}
	var rows []util.FallbackRow
	var waveErrs []error
	parsed, withoutTable := 0, 0
	for _, wave := range waves {
		if wave.month.Before(from) || known[inflExpPdfPointDate(wave.month)] {
			continue
		}
		before := len(rows)
		rows, err = s.importWave(ctx, wave, rows)
		if err != nil {
			waveErrs = append(waveErrs, err)
			continue
		}
		if len(rows) > before {
			parsed++
		} else {
			withoutTable++
		}
	}
	if len(rows) > 0 {
		if insertErr := cbrInflExpHistInsertRows(ctx, conn, rows); insertErr != nil {
			return 0, insertErr
		}
	}
	if err = publishDerived(ctx, conn); err != nil {
		return int64(len(rows)), err
	}
	log.Infof("%s: волн разобрано %d, без таблицы %d, точек %d", cbrInflExpHistName, parsed, withoutTable, len(rows))
	if len(waveErrs) > 0 {
		return int64(len(rows)), errors.Join(waveErrs...)
	}
	return int64(len(rows)), nil
}

// importWave скачивает артефакт волны и добавляет её точки к общему срезу. Страница волны
// (22-01 и позже) содержит ссылку на PDF — она ищется общим util.FindLink по суффиксу .pdf.
func (s *cbrInflExpHist) importWave(ctx context.Context, wave inflExpHistWave, rows []util.FallbackRow) ([]util.FallbackRow, error) {
	url := wave.url
	if !strings.HasSuffix(url, ".pdf") {
		wavePage, err := util.GetPage(url)
		if err != nil {
			return rows, fmt.Errorf("%s: волна %s, страница %s: %w", cbrInflExpHistName, wave.month.Format("2006-01"), url, err)
		}
		href, err := util.FindLink(wavePage, "a[href$=\".pdf\"]")
		if err != nil {
			return rows, fmt.Errorf("%s: волна %s: на странице %s не найдена ссылка на PDF: %w",
				cbrInflExpHistName, wave.month.Format("2006-01"), url, err)
		}
		url = cbrUrl + href
	}
	body, err := util.GetFileBytes(url)
	if err != nil {
		return rows, fmt.Errorf("%s: волна %s, %s: %w", cbrInflExpHistName, wave.month.Format("2006-01"), url, err)
	}
	text, err := util.PDFText(ctx, body, util.PDFLayout)
	if err != nil {
		return rows, fmt.Errorf("%s: волна %s, %s: %w", cbrInflExpHistName, wave.month.Format("2006-01"), url, err)
	}
	waveRows, err := inflExpHistRows(text, wave.month)
	if err != nil {
		return rows, err
	}
	return append(rows, waveRows...), nil
}

// cbrInflExpKnownDates — даты, уже лежащие в cbr_infl_exp (FINAL): гейт по множеству, а не по
// max(date), чтобы лечить и пропущенную середину. Книга и оперативный PDF пишут в ту же
// таблицу, поэтому их точки не перезаписываются.
func cbrInflExpKnownDates(ctx context.Context, conn driver.Conn) (map[time.Time]bool, error) {
	result, err := conn.Query(ctx, cbrInflExpKnownDatesQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: известные даты: %w", cbrInflExpHistName, err)
	}
	defer func() { _ = result.Close() }()
	known := map[time.Time]bool{}
	for result.Next() {
		var date time.Time
		if err = result.Scan(&date); err != nil {
			return nil, fmt.Errorf("%s: чтение date: %w", cbrInflExpHistName, err)
		}
		// Дата из clickhouse-go приходит с таймзоной сессии (Asia/Yekaterinburg, UTC+5):
		// .UTC() сдвинул бы её на предыдущие сутки и гейт перестал бы узнавать уже
		// импортированные месяцы (дефект пойман живым прогоном 2026-11-14 — импортёр
		// перекачивал весь архив и перезаписывал точки книги).
		known[time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)] = true
	}
	return known, result.Err()
}

// cbrInflExpHistInsertRows пишет точки истории одним батчем в общую cbr_infl_exp.
func cbrInflExpHistInsertRows(ctx context.Context, conn driver.Conn, rows []util.FallbackRow) error {
	batch, err := conn.PrepareBatch(ctx, cbrInflExpHistInsert)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err = batch.Append(row.Series, row.Date, row.Value); err != nil {
			if abortErr := batch.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки Append: %v", cbrInflExpTable, abortErr)
			}
			return fmt.Errorf("%s: Append (%s, %s): %w", cbrInflExpTable, row.Series, row.Date.Format("2006-01-02"), err)
		}
	}
	return batch.Send()
}

// cbrInflExpHistImporter — история ожиданий; регистрируется в cbr/importers.go.
var cbrInflExpHistImporter = &cbrInflExpHist{ClickHouseImport: util.ClickHouseImport{
	TableName:         cbrInflExpTable,
	PublicationSource: cbrInflExpHistName,
	// DDL не объявлен: таблицу создаёт книга cbr_infl_exp (как у оперативного PDF).
	AfterImport: afterCbrMacro(cbrInflExpSeriesMeta),
}}
