package cbr

import (
	"bytes"
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
	"github.com/xuri/excelize/v2"
)

// История инфляционных ожиданий из архива ЦБ: у каждой волны «Инфляционные ожидания и
// потребительские настроения населения» есть два артефакта — статистическое приложение
// `stat_Infl_exp_ГГ-ММ.xlsx` (таблица «Прямые оценки годовой инфляции: медианные значения»,
// машиночитаемая с 2016-12 по 2024-09, история внутри — с 2010-04) и PDF/страница волны
// (та же таблица текстом либо, у старых выпусков, проза). Тот же источник использует
// cbr_infl_exp (книга XLSX), поэтому волны пишутся в ту же таблицу под своим именем
// импортёра (прецедент — cbr_infl_exp_op).
//
// Книга XLSX с 2023 года стала короткой (лист «Данные для графиков» начинается с 2023-01),
// поэтому 2019–2022 в БД не было вовсе. Приложение волны закрывает пробел целиком и
// машиночитаемо: один свежий `stat_Infl_exp_*.xlsx` содержит ряд с 2010-04, включая
// 2017–2022. PDF остаётся резервом для волн новее последнего приложения. Разведка —
// research/2026-11-14-infl-exp-history-source.md.
const (
	cbrInflExpHistName = "cbr_infl_exp_hist"

	// cbrInflExpHistFrom — начало догона: 2017-01, критерий пункта key-rate/phase-2-infl-exp-history.
	// Источник отдаёт ряд раньше (с 2010-04, приложение волны), но методика инФОМ до 2017
	// менялась, и расширение границы — отдельное решение владельца, а не побочный эффект.
	cbrInflExpHistFrom = "2017-01"

	// cbrInflExpHistInsert — та же таблица, что у книги и оперативного PDF: ключ (name, date).
	cbrInflExpHistInsert = "INSERT INTO " + cbrInflExpTable
)

var (
	// cbrInflExpHistLinkRe — ссылка волны в любом из двух видов: прямой PDF
	// /Collection/Collection/File/<id>/Infl_exp_ГГ-ММ.pdf или страница
	// …/inflationary_expectations/Infl_exp_ГГ-ММ/ (22-01 и позже, PDF лежит внутри страницы).
	// Захватывается и подпись ссылки: имя файла не всегда совпадает с месяцем волны
	// (у ноября 2019 файл назван Infl_exp_19-10.pdf), а подпись — совпадает.
	cbrInflExpHistLinkRe = regexp.MustCompile(`(?s)<a\b[^>]*href="([^"]*Infl_exp_(\d{2})-(\d{2})(?:\.pdf|/)?)"[^>]*>(.*?)</a>`)
	// cbrInflExpHistAnnexRe — статистическое приложение волны: таблица медиан в XLSX.
	cbrInflExpHistAnnexRe = regexp.MustCompile(`(?s)<a\b[^>]*href="([^"]*stat_Infl_exp_(\d{2})-(\d{2})\.xlsx)"[^>]*>(.*?)</a>`)
	// cbrInflExpHistMonthRe — подпись месяца волны: имя файла или страницы.
	cbrInflExpHistMonthRe = regexp.MustCompile(`Infl_exp_(\d{2})-(\d{2})`)
	// cbrInflExpHistLabelWordRe — первое слово подписи: «Ноябрь», «Август 2021 года».
	cbrInflExpHistLabelWordRe = regexp.MustCompile(`(?i)[а-яё]+`)

	// Строки таблицы: «ФОМ (медиана) предыдущие 12 месяцев …» (наблюдаемая) и
	// «… следующие 12 месяцев …» (ожидаемая). В волнах 2019–2021 подпись подробнее:
	// «ФОМ (медиана, наблюдаемая инфляция)». Мягкий перенос (U+00AD) в подписи
	// встречается — «предыU+00ADдущие», поэтому переносы снимаются до разбора.
	cbrInflExpHistObservedRe = regexp.MustCompile(`^\s*ФОМ \(медиана[^)]*\)\s*преды\S*дущие\s+12\s+месяцев(.*)$`)
	cbrInflExpHistExpectedRe = regexp.MustCompile(`^\s*ФОМ \(медиана[^)]*\)\s*следующие\s+12\s+месяцев(.*)$`)
	// cbrInflExpHistValueRe — значение медианы: одна-две цифры после запятой, как печатает ЦБ.
	cbrInflExpHistValueRe = regexp.MustCompile(`\d{1,2},\d{1,2}`)

	// cbrInflExpHistAnnexMarker — заголовок таблицы медиан в статистическом приложении.
	// В части выпусков между словами двойной пробел, поэтому сравнение идёт по
	// нормализованной строке (strings.Fields), а не по литералу.
	cbrInflExpHistAnnexMarker = "прямые оценки годовой инфляции"
)

// inflExpHistWave — волна архива: месяц выпуска, документ волны (PDF или страница) и
// статистическое приложение с таблицей медиан.
type inflExpHistWave struct {
	month time.Time
	url   string
	annex string
}

// inflExpHistMonthOf разбирает «ГГ-ММ» ссылки в первое число месяца. Год двузначный, век —
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

// inflExpHistLabelMonth читает месяц волны из подписи ссылки: «Ноябрь», «Август 2021 года».
// Подпись — единственный признак, который не ломается, когда имя файла повторяет соседний
// месяц: у ноября 2019 файл назван Infl_exp_19-10.pdf, и разбор по basename терял волну.
func inflExpHistLabelMonth(label string) (time.Month, bool) {
	word := cbrInflExpHistLabelWordRe.FindString(strings.ToLower(normalizeInflExpHistText([]byte(label))))
	if word == "" {
		return 0, false
	}
	month, ok := util.MonthsToNum[word]
	return month, ok
}

// inflExpHistWaveOf собирает месяц волны из имени файла и подписи ссылки: год — из имени
// (архив не переходит через век), месяц — из подписи, если она его называет, иначе из имени.
// Расхождение имени файла и подписи — штатная особенность архива (ноябрь 2019), а не ошибка.
func inflExpHistWaveOf(href, label string) (time.Time, bool) {
	fromName, ok := inflExpHistMonthOf(href)
	if !ok {
		return time.Time{}, false
	}
	if month, ok := inflExpHistLabelMonth(label); ok {
		return time.Date(fromName.Year(), month, 1, 0, 0, 0, 0, time.UTC), true
	}
	return fromName, true
}

// inflExpHistWaves собирает волны со страницы ожиданий: по одной на месяц, с документом
// (прямой PDF приоритетнее страницы волны — меньше запросов) и статистическим приложением.
func inflExpHistWaves(page []byte) ([]inflExpHistWave, error) {
	byMonth := map[string]inflExpHistWave{}
	order := make([]string, 0, 256)
	add := func(month time.Time, apply func(*inflExpHistWave)) {
		key := month.Format("2006-01")
		wave, exists := byMonth[key]
		if !exists {
			order = append(order, key)
			wave.month = month
		}
		apply(&wave)
		byMonth[key] = wave
	}
	for _, match := range cbrInflExpHistLinkRe.FindAllStringSubmatch(string(page), -1) {
		href, label := match[1], match[4]
		month, ok := inflExpHistWaveOf(href, label)
		if !ok {
			continue
		}
		url := cbrUrl + href
		add(month, func(wave *inflExpHistWave) {
			if strings.HasSuffix(wave.url, ".pdf") {
				return
			}
			if !strings.HasSuffix(url, ".pdf") && wave.url != "" {
				return
			}
			wave.url = url
		})
	}
	for _, match := range cbrInflExpHistAnnexRe.FindAllStringSubmatch(string(page), -1) {
		href, label := match[1], match[4]
		month, ok := inflExpHistWaveOf(href, label)
		if !ok {
			continue
		}
		annex := cbrUrl + href
		add(month, func(wave *inflExpHistWave) { wave.annex = annex })
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
// 2017–2018 и части новых волн числа напечатаны только на графиках/в прозе, и это
// ограничение корпуса PDF, а не сбой (машиночитаемая история берётся из приложения волны).
// Таблица найдена, но значение не разобрано — ошибка: смена вёрстки не должна выглядеть
// как «в выпуске нет данных».
func inflExpHistRows(text []byte, month time.Time) ([]util.FallbackRow, error) {
	date := inflExpPdfPointDate(month)
	normalized := normalizeInflExpHistText(text)
	var rows []util.FallbackRow
	tableFound := false
	for _, line := range strings.Split(normalized, "\n") {
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
		tableFound = true
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
	if !tableFound && strings.Contains(strings.ToLower(normalized), "фом (медиана") {
		return nil, fmt.Errorf("%s: волна %s: шапка таблицы «ФОМ (медиана)» есть, строки не разобраны — разметка изменилась",
			cbrInflExpHistName, month.Format("2006-01"))
	}
	return rows, nil
}

// normalizeInflExpHistText снимает неразрывные пробелы и мягкие переносы: pdftotext печатает
// подпись «предыдущие» с U+00AD, и регулярка строки видит её как часть слова.
func normalizeInflExpHistText(text []byte) string {
	return strings.NewReplacer("\u00a0", " ", "\u00ad", "", "\r", "").Replace(string(text))
}

// inflExpHistAnnexRows разбирает статистическое приложение волны: во всех листах ищется
// заголовок «Прямые оценки годовой инфляции: медианные значения», под ним — две ориентации
// таблицы. 2016–2019 печатают её вертикально (колонка дат и по колонке на ряд), 2019+
// — строкой дат и двумя строками значений. Разбор охватывает весь ряд внутри приложения,
// а не одну волну: один свежий файл закрывает историю с 2010-04.
func inflExpHistAnnexRows(xlsx *excelize.File) ([]util.FallbackRow, error) {
	var rows []util.FallbackRow
	var blockErr error
	for _, sheet := range xlsx.GetSheetList() {
		grid, err := inflExpHistAnnexGrid(xlsx, sheet)
		if err != nil {
			return nil, fmt.Errorf("%s: лист %q: %w", cbrInflExpHistName, sheet, err)
		}
		for i, row := range grid {
			for _, cell := range row {
				if !inflExpHistAnnexMarkerCell(cell) {
					continue
				}
				block, err := inflExpHistAnnexBlock(grid, i)
				if err != nil {
					// Приложение повторяет заголовок таблицы ниже (легенда/вторая копия без
					// строк). Пропуск такого блока не ошибка, пока хоть одна таблица разобрана;
					// ошибкой он становится, только если не разобралась ни одна.
					if blockErr == nil {
						blockErr = fmt.Errorf("%s: лист %q: %w", cbrInflExpHistName, sheet, err)
					}
					continue
				}
				rows = append(rows, block...)
			}
		}
	}
	if len(rows) == 0 {
		if blockErr != nil {
			return nil, blockErr
		}
		return nil, fmt.Errorf("%s: в приложении не найдена таблица медиан — разметка книги изменилась", cbrInflExpHistName)
	}
	return rows, nil
}

// inflExpHistAnnexGrid читает лист приложения сырыми значениями (RawCellValue): формат
// ячеек ЦБ печатает даты как «Apr-10», а медианы — одним знаком после запятой, и
// отформатированное значение потеряло бы и месяц, и точность ряда. Сырые даты приходят
// серийным числом Excel, их разбирает inflExpHistAnnexDate.
func inflExpHistAnnexGrid(xlsx *excelize.File, sheet string) (grid [][]string, err error) {
	rows, err := xlsx.Rows(sheet)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		columns, colErr := rows.Columns(excelize.Options{RawCellValue: true})
		if colErr != nil {
			return nil, colErr
		}
		grid = append(grid, columns)
	}
	return grid, rows.Error()
}

// inflExpHistAnnexMarkerCell — ячейка с заголовком таблицы медиан. Сравнение по
// нормализованной строке: в части выпусков между словами двойной пробел.
func inflExpHistAnnexMarkerCell(cell string) bool {
	normalized := strings.Join(strings.Fields(strings.ToLower(normalizeInflExpHistText([]byte(cell)))), " ")
	return strings.Contains(normalized, cbrInflExpHistAnnexMarker) && strings.Contains(normalized, "медиан")
}

// inflExpHistAnnexBlock разбирает таблицу под заголовком: сначала горизонтальную
// ориентацию (строка дат, затем строки «наблюдаемая/ожидаемая инфляция»), затем
// вертикальную (шапка «даты опроса» и колонки рядов). Ни одна не разобралась — ошибка.
func inflExpHistAnnexBlock(grid [][]string, marker int) ([]util.FallbackRow, error) {
	if rows, ok := inflExpHistAnnexHorizontal(grid, marker); ok {
		return rows, nil
	}
	if rows, ok := inflExpHistAnnexVertical(grid, marker); ok {
		return rows, nil
	}
	return nil, fmt.Errorf("таблица медиан найдена, но ни одна ориентация не разобрана — разметка книги изменилась")
}

// inflExpHistAnnexHorizontal — таблица со строкой дат: 2019+ печатает её так. Строка дат
// бывает ниже заголовка (лист «Данные для графиков») и выше него — общей шапкой листа
// «Данные за все годы». Выравнивание идёт по порядку, а не по номеру колонки: в приложениях
// 2019–2021 строка дат сдвинута на колонку вправо от подписи ряда, в 2022+ — нет, и привязка
// по абсолютному номеру записала бы январь 2022 значением февраля.
func inflExpHistAnnexHorizontal(grid [][]string, marker int) ([]util.FallbackRow, bool) {
	dates, valueStart := inflExpHistAnnexDateRow(grid, marker)
	if len(dates) < 2 {
		return nil, false
	}
	var rows []util.FallbackRow
	for j := valueStart; j < len(grid) && j <= valueStart+12; j++ {
		labelColumn, label := inflExpHistAnnexLabelCell(grid[j])
		name := inflExpHistAnnexSeriesName(label)
		if name == "" {
			continue
		}
		for offset, month := range dates {
			column := labelColumn + 1 + offset
			if column >= len(grid[j]) {
				break
			}
			value, ok := inflExpHistAnnexValue(grid[j][column])
			if !ok {
				continue
			}
			rows = append(rows, util.FallbackRow{Series: name, Date: inflExpPdfPointDate(month), Value: value})
		}
	}
	if len(rows) > 0 {
		return rows, true
	}
	return nil, false
}

// inflExpHistAnnexDateRow ищет строку дат таблицы и начало строк значений: сначала под
// заголовком (до четырёх строк), затем — общую шапку листа выше заголовка. Второй случай
// обязателен для листа «Данные за все годы»: его строка дат стоит в начале листа, а под
// заголовком таблицы сразу идут значения. Из строк выше берётся самая длинная (шапка
// листа), а не ближайшая: у предыдущего блока бывает своя короткая строка дат, и привязка
// к ней сдвинула бы весь ряд.
func inflExpHistAnnexDateRow(grid [][]string, marker int) ([]time.Time, int) {
	for i := marker + 1; i < len(grid) && i <= marker+4; i++ {
		if dates := inflExpHistAnnexDates(grid[i]); len(dates) >= 2 {
			return dates, i + 1
		}
	}
	var best []time.Time
	for i := marker - 1; i >= 0 && i >= marker-200; i-- {
		if dates := inflExpHistAnnexDates(grid[i]); len(dates) > len(best) {
			best = dates
		}
	}
	if len(best) >= 2 {
		return best, marker + 1
	}
	return nil, 0
}

// inflExpHistAnnexVertical — таблица с колонкой дат: так печатали приложения 2016–2018.
func inflExpHistAnnexVertical(grid [][]string, marker int) ([]util.FallbackRow, bool) {
	for i := marker; i < len(grid) && i <= marker+8; i++ {
		dateColumn, columns := inflExpHistAnnexHeader(grid[i])
		if dateColumn < 0 || len(columns) == 0 {
			continue
		}
		var rows []util.FallbackRow
		for j := i + 1; j < len(grid); j++ {
			if dateColumn >= len(grid[j]) {
				break
			}
			month, ok := inflExpHistAnnexDate(grid[j][dateColumn])
			if !ok {
				break
			}
			for column, name := range columns {
				if column >= len(grid[j]) {
					continue
				}
				value, ok := inflExpHistAnnexValue(grid[j][column])
				if !ok {
					continue
				}
				rows = append(rows, util.FallbackRow{Series: name, Date: inflExpPdfPointDate(month), Value: value})
			}
		}
		if len(rows) > 0 {
			return rows, true
		}
	}
	return nil, false
}

// inflExpHistAnnexDates собирает даты строки заголовка таблицы в порядке колонок.
func inflExpHistAnnexDates(row []string) []time.Time {
	months := make([]time.Time, 0, len(row))
	for _, cell := range row {
		if month, ok := inflExpHistAnnexDate(cell); ok {
			months = append(months, month)
		}
	}
	return months
}

// inflExpHistAnnexLabelCell — колонка и текст подписи ряда: в приложении 2019–2021 подпись
// стоит в колонке 0, в 2022+ — тоже, но строка дат при этом сдвинута иначе, поэтому
// значения читаются не по абсолютному номеру колонки, а от подписи.
func inflExpHistAnnexLabelCell(row []string) (int, string) {
	for column, cell := range row {
		if strings.TrimSpace(cell) != "" {
			return column, cell
		}
	}
	return 0, ""
}

// inflExpHistAnnexHeader читает вертикальную шапку: колонку «даты опроса» и колонки рядов.
func inflExpHistAnnexHeader(row []string) (dateColumn int, columns map[int]string) {
	dateColumn = -1
	columns = map[int]string{}
	for column, cell := range row {
		if strings.Contains(strings.ToLower(normalizeInflExpHistText([]byte(cell))), "даты опроса") {
			dateColumn = column
			continue
		}
		if name := inflExpHistAnnexSeriesName(cell); name != "" {
			columns[column] = name
		}
	}
	return dateColumn, columns
}

// inflExpHistAnnexSeriesName сопоставляет подпись строки/колонки стабильному имени ряда.
// Сравнение точное (после снятия «(в %)» и регистра): в приложении рядом с основной таблицей
// идут подгруппы «наблюдаемая инфляция среди тех, кто имеет сбережения», и вхождение
// подстроки записало бы их значения в основной ряд.
func inflExpHistAnnexSeriesName(cell string) string {
	normalized := strings.ToLower(strings.Join(strings.Fields(normalizeInflExpHistText([]byte(cell))), " "))
	if index := strings.Index(normalized, "("); index >= 0 {
		normalized = strings.TrimSpace(normalized[:index])
	}
	switch normalized {
	case "наблюдаемая инфляция":
		return inflExpObservedName
	case "ожидаемая инфляция":
		return inflExpExpectedName
	default:
		return ""
	}
}

// inflExpHistAnnexDate разбирает ячейку даты приложения. excelize отдаёт дату в формате
// ячейки: «2017-01-01», «01.01.2017» или серийным числом Excel; все три вида встречаются.
func inflExpHistAnnexDate(cell string) (time.Time, bool) {
	trimmed := strings.TrimSpace(cell)
	if trimmed == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{"2006-01-02", "2006-01-02 15:04:05", "02.01.2006"} {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return time.Date(parsed.Year(), parsed.Month(), 1, 0, 0, 0, 0, time.UTC), true
		}
	}
	if serial, err := strconv.ParseFloat(strings.ReplaceAll(trimmed, ",", "."), 64); err == nil {
		if serial >= 20000 && serial <= 80000 {
			if parsed, err := excelize.ExcelDateToTime(serial, false); err == nil {
				return time.Date(parsed.Year(), parsed.Month(), 1, 0, 0, 0, 0, time.UTC), true
			}
		}
	}
	return time.Time{}, false
}

// inflExpHistAnnexValue разбирает значение медианы: запятая или точка как десятичный
// разделитель, пробелы по краям. Значение вне (0, 100) — не медиана, ячейка пропускается.
func inflExpHistAnnexValue(cell string) (float64, bool) {
	trimmed := strings.TrimSpace(cell)
	if trimmed == "" {
		return 0, false
	}
	value, err := strconv.ParseFloat(strings.ReplaceAll(trimmed, ",", "."), 64)
	if err != nil || value <= 0 || value >= 100 {
		return 0, false
	}
	return value, true
}

// cbrInflExpHist — импортёр истории ожиданий: свой Import (набор волн + общая таблица с
// книгой), поэтому шаблон util.ClickHouseImport не используется.
type cbrInflExpHist struct {
	util.ClickHouseImport
}

// Name — имя импортёра в CLICKHOUSE_IMPORT_STAT и в dagu; таблица — общая cbr_infl_exp.
func (s *cbrInflExpHist) Name() string { return cbrInflExpHistName }

// Import — страница ожиданий → волны → гейт по уже лежащим датам → история из приложения
// свежей волны (машиночитаемая таблица медиан с 2010-04) → PDF недостающих волн → батч.
// Ошибки волн копятся (errors.Join): сбой одной волны не отменяет остальные, но прогон
// красный. Производные (каталог и витрина v_cbr_macro) публикуются и на no-op-пути.
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
	covered := map[string]bool{}
	annexWaves, annexPoints := 0, 0
	if annex, ok := newestInflExpHistAnnex(waves); ok {
		annexRows, annexErr := s.importAnnex(ctx, annex)
		if annexErr != nil {
			waveErrs = append(waveErrs, annexErr)
		} else {
			for _, row := range annexRows {
				key := inflExpHistKey(row.Series, row.Date)
				if row.Date.Before(from) || known[key] || covered[key] {
					continue
				}
				covered[key] = true
				rows = append(rows, row)
				annexPoints++
			}
			annexWaves = 1
		}
	}
	parsed, withoutTable := 0, 0
	for _, wave := range waves {
		date := inflExpPdfPointDate(wave.month)
		knownObserved := known[inflExpHistKey(inflExpObservedName, date)] || covered[inflExpHistKey(inflExpObservedName, date)]
		knownExpected := known[inflExpHistKey(inflExpExpectedName, date)] || covered[inflExpHistKey(inflExpExpectedName, date)]
		// Волна считается закрытой, только когда известны оба ряда: приложение могло дать
		// один ряд периода, и тогда PDF — единственный шанс добрать второй.
		if wave.month.Before(from) || knownObserved && knownExpected {
			continue
		}
		before := len(rows)
		rows, err = s.importWave(ctx, wave, rows, known, covered)
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
	rows = uniqueInflExpHistRows(rows)
	if len(rows) > 0 {
		if insertErr := cbrInflExpHistInsertRows(ctx, conn, rows); insertErr != nil {
			return 0, insertErr
		}
	}
	if err = publishDerived(ctx, conn); err != nil {
		return int64(len(rows)), err
	}
	log.Infof("%s: приложений разобрано %d (%d точек), волн по PDF %d, без таблицы %d, точек %d",
		cbrInflExpHistName, annexWaves, annexPoints, parsed, withoutTable, len(rows))
	if len(waveErrs) > 0 {
		return int64(len(rows)), errors.Join(waveErrs...)
	}
	return int64(len(rows)), nil
}

// newestInflExpHistAnnex — свежая волна со статистическим приложением: её таблица медиан
// кумулятивна и содержит историю всего ряда, поэтому качать все 100 приложений не нужно.
func newestInflExpHistAnnex(waves []inflExpHistWave) (string, bool) {
	for _, wave := range waves {
		if wave.annex != "" {
			return wave.annex, true
		}
	}
	return "", false
}

// importAnnex скачивает и разбирает статистическое приложение волны.
func (s *cbrInflExpHist) importAnnex(ctx context.Context, url string) ([]util.FallbackRow, error) {
	body, err := util.GetFileBytes(url)
	if err != nil {
		return nil, fmt.Errorf("%s: приложение %s: %w", cbrInflExpHistName, url, err)
	}
	xlsx, err := excelize.OpenReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%s: приложение %s: %w", cbrInflExpHistName, url, err)
	}
	defer func() { _ = xlsx.Close() }()
	rows, err := inflExpHistAnnexRows(xlsx)
	if err != nil {
		return nil, fmt.Errorf("%s: приложение %s: %w", cbrInflExpHistName, url, err)
	}
	return rows, nil
}

// importWave скачивает артефакт волны и добавляет её недостающие точки к общему срезу.
// Страница волны (22-01 и позже) содержит ссылку на PDF — она ищется общим util.FindLink
// по суффиксу .pdf. Точки, уже лежащие в таблице или добавленные приложением, отбрасываются
// по ключу (ряд, дата): приложение могло дать только один ряд периода.
func (s *cbrInflExpHist) importWave(ctx context.Context, wave inflExpHistWave, rows []util.FallbackRow, known, covered map[string]bool) ([]util.FallbackRow, error) {
	url := wave.url
	if url == "" {
		return rows, fmt.Errorf("%s: волна %s: нет ни приложения, ни документа волны", cbrInflExpHistName, wave.month.Format("2006-01"))
	}
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
	for _, row := range waveRows {
		key := inflExpHistKey(row.Series, row.Date)
		if known[key] || covered[key] {
			continue
		}
		covered[key] = true
		rows = append(rows, row)
	}
	return rows, nil
}

// uniqueInflExpHistRows снимает дубли (series, date) внутри батча: приложение и PDF могут
// принести одну и ту же волну, а повторная строка в батче — лишняя работа движка.
func uniqueInflExpHistRows(rows []util.FallbackRow) []util.FallbackRow {
	seen := make(map[string]bool, len(rows))
	unique := rows[:0]
	for _, row := range rows {
		key := row.Series + "|" + row.Date.Format("2006-01-02")
		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, row)
	}
	return unique
}

// cbrInflExpKnownDates — точки, уже лежащие в cbr_infl_exp (FINAL), по ключу (ряд, дата):
// гейт по множеству, а не по max(date), чтобы лечить и пропущенную середину. Книга и
// оперативный PDF пишут в ту же таблицу, поэтому их точки не перезаписываются.
func cbrInflExpKnownDates(ctx context.Context, conn driver.Conn) (map[string]bool, error) {
	result, err := conn.Query(ctx, cbrInflExpHistKnownQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: известные точки: %w", cbrInflExpHistName, err)
	}
	defer func() { _ = result.Close() }()
	known := map[string]bool{}
	for result.Next() {
		var name string
		var date time.Time
		if err = result.Scan(&name, &date); err != nil {
			return nil, fmt.Errorf("%s: чтение точки: %w", cbrInflExpHistName, err)
		}
		// Дата из clickhouse-go приходит с таймзоной сессии (Asia/Yekaterinburg, UTC+5):
		// .UTC() сдвинул бы её на предыдущие сутки и гейт перестал бы узнавать уже
		// импортированные месяцы (дефект пойман живым прогоном 2026-11-14 — импортёр
		// перекачивал весь архив и перезаписывал точки книги).
		known[inflExpHistKey(name, time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC))] = true
	}
	return known, result.Err()
}

// inflExpHistKey — ключ гейта: ряд и дата. Гейт по одной дате отбрасывал второй ряд
// периода, когда приложение волны дало только первый.
func inflExpHistKey(series string, date time.Time) string {
	return series + "|" + date.Format("2006-01-02")
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
