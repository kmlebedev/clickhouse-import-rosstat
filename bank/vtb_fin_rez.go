package bank

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gocolly/colly/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (

	// https://www.vtb.ru/ir/statements/results/
	// Имена файлов содержат дату отчётности (rus-vtb-group-ifrs-as-of-31-october-2024.xlsx)
	// и меняются каждый отчётный период, поэтому список берётся со страницы результатов.
	vtbIfrsResultsUrl = "https://www.vtb.ru/ir/statements/results/"
	vtbIfrsTable      = "rus_vtb_group_ifrs"
	vtbIfrsTableDdl   = `CREATE TABLE IF NOT EXISTS ` + vtbIfrsTable + ` (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	vtbIfrsDataInsert = "INSERT INTO " + vtbIfrsTable + " VALUES (?, ?, ?)"
	// vtbIfrsDataField — первая строка балансового блока: якорь начала данных — и прежнего
	// разбора (export), и нового (parseVtbBookSheet): строка над ней читается как шапка
	// колонок-дат.
	vtbIfrsDataField  = "Денежные средства и краткосрочные активы"
	vtbIfrsTimeLayout = "01-02-06"
)

// Константы разбора книги (задача 3). Разведка живого файла 2026-10-17:
// rus-vtb-group-ifrs-as-of-31-august-2026.xlsx, 42 247 Б, четыре листа — «Ключевые показатели
// P&L», «Ключевые балансовые показатели», «Сегментный анализ_31.08.26», «Показатели и
// коэффициенты»; фикстура — bank/testdata/vtb_ifrs.xlsx (коммитится как есть, §11.2).
const (
	// vtbBookSheet — лист балансовых показателей: блок остатков от первой строки баланса до
	// «Итого собственные средства» включительно — «как прежде», те же 18 строк, что у базовой
	// линии таблицы (2026-10-16).
	vtbBookSheet = "Ключевые балансовые показатели"
	// vtbPnlSheet — лист отчёта о прибылях и убытках: разбирается целиком, оба раздела
	// («Ключевые показатели отчета о прибылях и убытках» и «Показатели эффективности»).
	vtbPnlSheet = "Ключевые показатели P&L"
	// vtbRatiosSheet — лист «Показатели и коэффициенты»: в живом файле это глоссарий (подпись
	// коэффициента и его определение), ни значений, ни дат на листе нет — точек проход не даёт
	// (почему — parseVtbRatiosSheet).
	vtbRatiosSheet = "Показатели и коэффициенты"
	// vtbBalanceStop — последняя строка балансового блока: «Итого собственные средства».
	vtbBalanceStop = "Итого собственные средства"
	// vtbTimeLayout — формат дат книги для сообщений: колонки баланса имеют числовой формат
	// mm-dd-yy (Go-layout «01-02-06»), как и у прежнего разбора. Читаются они при этом сырыми
	// серийными номерами Excel (§6.2) и переводятся excelize.ExcelDateToTime; подписи колонок
	// P&L («август'2026») — месяц-строка, для которой Go-layout не строится (vtbMonthNames).
	vtbTimeLayout = "01-02-06"
	// vtbMinYear…vtbMaxYear — окно лет серийного номера даты: проценты изменения (0–100) дают
	// 1900 год и датами не прикидываются (идиома cbr/mo_br_parse.go).
	vtbMinYear = 2000
	vtbMaxYear = 2100
)

// vtbMonthLabelRe — подпись месячной колонки листа P&L: «август'2026» (месяц источника —
// строчная кириллица). Накопленная колонка «8М'2026» (М — кириллическая) этой формой не
// покрывается: перед апострофом стоит цифра, а не буква, — и в разбор не идёт. Подпись той же
// формы с незнакомым месяцем — ошибка, а не пропуск колонки (§6.4).
var vtbMonthLabelRe = regexp.MustCompile(`^([а-яё]+)'(\d{4})$`)

// vtbMonthNames — русские названия месяцев в именительном падеже, как их пишет источник
// («август»); ключи — в нижнем регистре.
var vtbMonthNames = map[string]time.Month{
	"январь":   time.January,
	"февраль":  time.February,
	"март":     time.March,
	"апрель":   time.April,
	"май":      time.May,
	"июнь":     time.June,
	"июль":     time.July,
	"август":   time.August,
	"сентябрь": time.September,
	"октябрь":  time.October,
	"ноябрь":   time.November,
	"декабрь":  time.December,
}

// vtbPoint — точка разбора: имя ряда (подпись строки источника), дата и сырое значение (§6.2).
// Значение — как записано в файле, без умножений и делений: млрд ₽ у баланса и отчёта о
// прибылях, доли у коэффициентов (0.202 = 20,2 %); к точности Float32-колонки таблицы значение
// приводит запись (appendVtbPoints).
type vtbPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

// vtbColumn — колонка значений: индекс ячейки в строке и дата, которой принадлежат её числа.
type vtbColumn struct {
	col  int
	date time.Time
}

// parseVtbBook — разбор книги (импортёр rus_vtb_group_ifrs, МСФО группы ВТБ): три листа — тремя
// проходами. Разбор чистый (§6.1): на входе открытая книга, ни сети, ни БД, поэтому тесты
// офлайн на живой фикстуре (§11.3); значения — сырые ячейки (§6.2); текст ошибки называет
// таблицу и причину (§6.4).
//
// Проходы: балансовый лист — блок остатков до vtbBalanceStop включительно («как прежде»: те же
// 18 рядов, что у базовой линии таблицы); лист P&L — целиком, оба раздела; лист коэффициентов —
// в живом файле глоссарий без значений (parseVtbRatiosSheet), точек не даёт.
func parseVtbBook(xlsx *excelize.File) ([]vtbPoint, error) {
	balance, err := parseVtbBookSheet(xlsx)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", vtbIfrsTable, err)
	}
	pnl, err := parseVtbPnlSheet(xlsx)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", vtbIfrsTable, err)
	}
	if err = parseVtbRatiosSheet(xlsx); err != nil {
		return nil, fmt.Errorf("%s: %w", vtbIfrsTable, err)
	}
	return append(balance, pnl...), nil
}

// parseVtbBookSheet — балансовый блок листа «Ключевые балансовые показатели»: строки от первой
// строки баланса до «Итого собственные средства» включительно. Шапка блока — строка над первой
// строкой баланса; колонки значений — её ячейки с серийными номерами дат Excel (§6.2, перевод —
// excelize.ExcelDateToTime, идиома cbr/mp_survey_parse.go). Текстовые колонки шапки
// («изменение за месяц», «изменение с начала года») колонками значений не являются: их числа —
// изменения в долях, а не остатки. Строка блока без подписи, но со значениями — ошибка (§6.4):
// записать такие числа нечем, а молча потерять их хуже, чем упасть.
func parseVtbBookSheet(xlsx *excelize.File) (points []vtbPoint, err error) {
	sheet, err := vtbSheet(xlsx, vtbBookSheet)
	if err != nil {
		return nil, err
	}
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("лист %q: чтение строк: %w", sheet, err)
	}
	first := -1
	for i, row := range rows {
		if vtbSeriesName(vtbCell(row, 0)) == vtbIfrsDataField {
			first = i
			break
		}
	}
	if first < 1 {
		return nil, fmt.Errorf("лист %q: не найдена первая строка баланса %q", sheet, vtbIfrsDataField)
	}
	cols := vtbSerialColumns(rows[first-1])
	if len(cols) == 0 {
		return nil, fmt.Errorf("лист %q: в шапке баланса нет колонок с датами", sheet)
	}
	for i := first; i < len(rows); i++ {
		label := vtbSeriesName(vtbCell(rows[i], 0))
		if label == "" {
			if vtbRowHasValues(rows[i], cols) {
				return nil, fmt.Errorf("лист %q, строка %d: значения без подписи ряда", sheet, i+1)
			}
			continue
		}
		for _, c := range cols {
			raw := vtbCell(rows[i], c.col)
			if raw == "" {
				continue // пустая ячейка — не точка ряда (§6.7)
			}
			value, err := vtbFloat(raw)
			if err != nil {
				return nil, fmt.Errorf("лист %q, строка %d (%q): %w", sheet, i+1, label, err)
			}
			points = append(points, vtbPoint{Series: label, Date: c.date, Value: value})
		}
		if label == vtbBalanceStop {
			if len(points) == 0 {
				return nil, fmt.Errorf("лист %q: балансовый блок не дал ни одной точки", sheet)
			}
			return points, nil
		}
	}
	return nil, fmt.Errorf("лист %q: не найдена строка %q", sheet, vtbBalanceStop)
}

// parseVtbPnlSheet — лист «Ключевые показатели P&L» целиком: шапки разделов (их две — отчёт о
// прибылях и убытках и «Показатели эффективности») ищутся по признаку — подписям месячных
// колонок («август'2026»), а не по номерам строк (§6.3). В таблицу идут только месячные колонки:
// накопленные («8М'2026» — январь–август) и текстовые «изменение» точками не являются — ключ
// таблицы (name, date) держит одно значение на месяц, и накопленный итог за тот же месяц затёр
// бы месячное значение, тогда как словарь метрик (net_profit, roe, roa) — помесячный. Дата точки
// месячной колонки — последний день её месяца (как и у колонок баланса). Строка со значениями, но
// без подписи ряда — ошибка (§6.4); строки после последнего раздела (дисклеймеры источника)
// значений не несут и точек не дают.
func parseVtbPnlSheet(xlsx *excelize.File) (points []vtbPoint, err error) {
	sheet, err := vtbSheet(xlsx, vtbPnlSheet)
	if err != nil {
		return nil, err
	}
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("лист %q: чтение строк: %w", sheet, err)
	}
	var cols []vtbColumn // колонки текущего раздела; nil — до первой шапки
	for i, row := range rows {
		section, isHeader, err := vtbPnlHeader(sheet, row)
		if err != nil {
			return nil, err
		}
		if isHeader {
			cols = section
			continue
		}
		if cols == nil {
			continue // заголовок листа и пустые строки выше первой шапки — не данные
		}
		label := vtbSeriesName(vtbCell(row, 0))
		if label == "" {
			if vtbRowHasValues(row, cols) {
				return nil, fmt.Errorf("лист %q, строка %d: значения без подписи ряда", sheet, i+1)
			}
			continue
		}
		for _, c := range cols {
			raw := vtbCell(row, c.col)
			if raw == "" {
				continue // пустая ячейка — не точка ряда (§6.7)
			}
			value, err := vtbFloat(raw)
			if err != nil {
				return nil, fmt.Errorf("лист %q, строка %d (%q): %w", sheet, i+1, label, err)
			}
			points = append(points, vtbPoint{Series: label, Date: c.date, Value: value})
		}
	}
	if cols == nil {
		return nil, fmt.Errorf("лист %q: не найдена шапка с месячными колонками", sheet)
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("лист %q: не дал ни одной точки", sheet)
	}
	return points, nil
}

// parseVtbRatiosSheet — проход листа «Показатели и коэффициенты». В живом файле это глоссарий:
// колонка A — подпись коэффициента, колонка B — его определение; числовых значений и дат на
// листе нет, поэтому точек проход не даёт. Значения этих коэффициентов лежат на других листах
// книги: ROE/ROA — на листе P&L, нормативы достаточности капитала — в блоке «Достаточность
// капитала» балансового листа ниже vtbBalanceStop (рамка разбора — «как прежде», до «Итого
// собственные средства»; блок достаточности ею не покрыт). Разметка всё равно проверяется
// (§6.3, §6.4): заголовок листа и хотя бы одна строка «подпись — определение» обязаны быть на
// месте — иначе таблица значений, появившаяся на этом листе, прошла бы разбор молча.
func parseVtbRatiosSheet(xlsx *excelize.File) error {
	sheet, err := vtbSheet(xlsx, vtbRatiosSheet)
	if err != nil {
		return err
	}
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("лист %q: чтение строк: %w", sheet, err)
	}
	titled, definitions := false, 0
	for _, row := range rows {
		label := vtbSeriesName(vtbCell(row, 0))
		switch {
		case label == vtbRatiosSheet:
			titled = true
		case label != "" && vtbCell(row, 1) != "":
			definitions++
		}
	}
	if !titled {
		return fmt.Errorf("лист %q: не найден заголовок %q", sheet, vtbRatiosSheet)
	}
	if definitions == 0 {
		return fmt.Errorf("лист %q: не найдено ни одной строки «подпись — определение»", sheet)
	}
	return nil
}

// vtbPnlHeader — шапка раздела листа P&L: колонки с месячными подписями и их даты. Строка без
// месячных подписей — не шапка (ok=false). Подпись вида «<месяц>'<год>» с незнакомым месяцем —
// ошибка: колонка значений осталась бы без даты, а её потеря прошла бы молча (§6.4).
func vtbPnlHeader(sheet string, row []string) (cols []vtbColumn, ok bool, err error) {
	for col := 1; col < len(row); col++ {
		raw := vtbCell(row, col)
		m := vtbMonthLabelRe.FindStringSubmatch(raw)
		if m == nil {
			continue // «изменение», накопленные «8М'2026» и прочие текстовые ячейки шапки
		}
		month, known := vtbMonthNames[strings.ToLower(m[1])]
		if !known {
			return nil, false, fmt.Errorf("лист %q: подпись колонки %q: месяц %q не распознан", sheet, raw, m[1])
		}
		year, err := strconv.Atoi(m[2])
		if err != nil {
			return nil, false, fmt.Errorf("лист %q: подпись колонки %q: год не разобран: %w", sheet, raw, err)
		}
		cols = append(cols, vtbColumn{col: col, date: vtbLastDayOfMonth(year, month)})
	}
	return cols, len(cols) > 0, nil
}

// vtbSheet — имя листа книги по якорю (§6.3): лист ищется по вхождению якоря в имя, а не по
// номеру — добавленный источником лист сдвинул бы номер. Ошибка перечисляет листы книги.
func vtbSheet(xlsx *excelize.File, anchor string) (string, error) {
	sheets := xlsx.GetSheetList()
	for _, name := range sheets {
		if strings.Contains(name, anchor) {
			return name, nil
		}
	}
	return "", fmt.Errorf("в книге нет листа с якорем %q (листы: %s)", anchor, strings.Join(sheets, ", "))
}

// vtbCell — ячейка строки по индексу колонки: выход за границу среза — пустая ячейка, а не
// паника (§6.5). GetRows обрезает хвостовые пустые ячейки, и короткая строка — обычное дело.
func vtbCell(row []string, col int) string {
	if col < 0 || col >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[col])
}

// vtbSeriesName — имя ряда из подписи строки источника: NBSP и переносы строк (в подписях есть и
// то и другое: « -  Кредиты ЮЛ», «…кредитные убытки\n  и прочие резервы») сворачиваются в один
// пробел, служебный дефис подпункта отбрасывается. Имена рядов — стабильный ключ таблицы
// (name, date) (§6.8), и нормализация совпадает с базовой линией таблицы: прежний разбор снимал
// все дефисы и обрезал края, у 18 балансовых рядов результат тот же.
func vtbSeriesName(cell string) string {
	name := strings.Join(strings.Fields(strings.ReplaceAll(cell, "\u00a0", " ")), " ")
	name = strings.TrimPrefix(name, "- ")
	if name == "-" {
		name = "" // подпись-прочерк — не имя ряда
	}
	return name
}

// vtbSerialColumns — колонки значений шапки баланса: ячейки с серийными номерами дат Excel.
// Датой считается только номер, дающий год в окне vtbMinYear…vtbMaxYear: проценты изменения
// (0–100) дают 1900 год и отличаются от дат без отдельного признака формата (идиома
// cbr/mo_br_parse.go).
func vtbSerialColumns(header []string) []vtbColumn {
	var cols []vtbColumn
	for col := 1; col < len(header); col++ {
		serial, err := strconv.ParseFloat(vtbCell(header, col), 64)
		if err != nil {
			continue // текстовая колонка («изменение за месяц») — не колонка значений
		}
		date, err := excelize.ExcelDateToTime(serial, false)
		if err != nil || date.Year() < vtbMinYear || date.Year() > vtbMaxYear {
			continue
		}
		cols = append(cols, vtbColumn{col: col, date: date})
	}
	return cols
}

// vtbRowHasValues — есть ли в строке непустое значение в колонках: так строка без подписи ряда
// отличается от пустой — первую разбор отвергает (§6.4), вторую пропускает.
func vtbRowHasValues(row []string, cols []vtbColumn) bool {
	for _, c := range cols {
		if vtbCell(row, c.col) != "" {
			return true
		}
	}
	return false
}

// vtbFloat — число ячейки. «NaN»/«Inf» strconv.ParseFloat принимает без ошибки, но в
// Float32-колонку таблицы такое значение уехало бы порчей, которую видно только в витрине
// (прецедент — domrf_mortgage): это ошибка разбора (§6.4).
func vtbFloat(raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", raw, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение %q не конечно (NaN/Inf)", raw)
	}
	return value, nil
}

// vtbLastDayOfMonth — последний день месяца: нулевой день от первого числа следующего месяца.
func vtbLastDayOfMonth(year int, month time.Month) time.Time {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)
}

// appendVtbPoints пишет точки по одному Append на точку с проверкой ошибки (§5.3, §5.5): форма
// строки — (name, date, balance) схемы таблицы, значение приводится к точности Float32-колонки
// (legacy-схема, спека §4.5), ошибка возвращается вызывающему с именем точки.
func appendVtbPoints(batch driver.Batch, points []vtbPoint) error {
	for _, p := range points {
		if err := batch.Append(p.Series, p.Date, float32(p.Value)); err != nil {
			return fmt.Errorf("%s: запись точки %q за %s: %w",
				vtbIfrsTable, p.Series, p.Date.Format(vtbTimeLayout), err)
		}
	}
	return nil
}

// getVtbIfrsXlsDataUrl собирает ссылки на отчётные XLSX-файлы МСФО со страницы
// результатов ВТБ. Отбираются только основные отчёты (rus-vtb-group-ifrs-as-of-*.xlsx);
// вспомогательные файлы (financial_data_supplement_*) и legacy .xls отбрасываются.
func getVtbIfrsXlsDataUrl() (urls []string) {
	seen := make(map[string]bool)
	c := colly.NewCollector(colly.UserAgent(util.HttpUA))
	c.SetClient(util.HttpClient)
	c.OnHTML(`a[href*="rus-vtb-group-ifrs-as-of"]`, func(e *colly.HTMLElement) {
		href := e.Attr("href")
		if !strings.HasSuffix(strings.ToLower(href), ".xlsx") {
			return
		}
		if seen[href] {
			return
		}
		seen[href] = true
		urls = append(urls, href)
	})
	if err := c.Visit(vtbIfrsResultsUrl); err != nil {
		log.Errorf("Visit %v+", err)
	}
	c.Wait()
	return urls
}

const vtbIfrsXlsDataPrefix = "https://www.vtb.ru"

type VtbIfrs struct {
}

func (s *VtbIfrs) Name() string {
	return vtbIfrsTable
}

func (s *VtbIfrs) export() (table *[][]string, err error) {
	vtbIfrsXlsData := getVtbIfrsXlsDataUrl()
	if len(vtbIfrsXlsData) == 0 {
		return nil, fmt.Errorf("vtbIfrs: не найдены ссылки на rus-vtb-group-ifrs-as-of-*.xlsx на %s", vtbIfrsResultsUrl)
	}
	var xlsx *excelize.File
	table = new([][]string)
	for _, xlsName := range vtbIfrsXlsData {
		if xlsx, err = util.GetXlsx(vtbIfrsXlsDataPrefix + xlsName); err != nil {
			return nil, fmt.Errorf("get xlsx %s failed: %v", xlsName, err)
		}
		var rows [][]string
		if rows, err = xlsx.GetRows("Ключевые балансовые показатели"); err != nil {
			return nil, err
		}
		_ = xlsx.Close()
		fieldFound := 0
		// Строки с годами
		for i, row := range rows {
			if len(row) == 0 {
				continue
			}
			// fmt.Printf("file %s row: %s\n", xlsName, row[0])
			if strings.TrimSpace(row[0]) == vtbIfrsDataField {
				fieldFound = i - 1
			}
			if fieldFound == 0 {
				continue
			}
			if len(row) < 2 {
				break
			}
			if strings.TrimSpace(row[1]) == "" || strings.TrimSpace(row[1]) == "0" {
				continue
			}
			// Колонки с месяцами и пропуском кварталов
			for j, cell := range row[1:] {
				if cell == "" {
					continue
				}
				if j+1 >= len(rows[fieldFound]) {
					break
				}
				if strings.HasPrefix(strings.TrimSpace(rows[fieldFound][j+1]), "изменение") {
					continue
				}
				name := strings.TrimSpace(strings.ReplaceAll(row[0], "-", ""))
				balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
				//fmt.Printf("name %s date %v balance %s\n", name, rows[fieldFound][j+1], balance)
				if _, err = strconv.ParseFloat(balance, 32); err != nil {
					return nil, err
				}

				*table = append(*table, []string{name, rows[fieldFound][j+1], balance})
			}
			if row[0] == "Итого собственные средства" {
				break
			}
		}
	}

	return table, nil
}

func (s *VtbIfrs) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, vtbIfrsTableDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		// Calling Parse() method with its parameters
		date, err := time.Parse(vtbIfrsTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, vtbIfrsDataInsert, row[0], date, row[2]); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &VtbIfrs{})
}
