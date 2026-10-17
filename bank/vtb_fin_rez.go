package bank

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// vtbIfrsResultsUrl — страница результатов ВТБ, с которой берётся список отчётных файлов:
	// имена файлов несут дату отчётности (rus-vtb-group-ifrs-as-of-31-october-2024.xlsx) и
	// меняются каждый отчётный период, поэтому статичного адреса в коде нет (спека §3.3).
	vtbIfrsResultsUrl = "https://www.vtb.ru/ir/statements/results/"
	vtbIfrsTable      = "rus_vtb_group_ifrs"
	// vtbIfrsDataUrlPrefix — домен источника: href ссылок страницы относительный, абсолютный
	// адрес файла = домен + href. Литерал, а не vtbIfrsResultsUrl: тест сверяет сборку адреса
	// с литералом.
	vtbIfrsDataUrlPrefix = "https://www.vtb.ru"
	// vtbIfrsLinkSelector — ссылки на отчётные файлы в разметке страницы. Якорь — фрагмент имени
	// файла (rus-vtb-group-ifrs-as-of-…): под него не попадают ни сопутствующие файлы источника
	// (financial_data_supplement_*), ни прочие ссылки страницы. Расширение выбирает разбор: рядом
	// с xlsx источник выкладывает pdf-версии тех же отчётов — они отбрасываются.
	vtbIfrsLinkSelector = `a[href*="rus-vtb-group-ifrs-as-of"]`
	// vtbIfrsTableDdl — DDL таблицы в %s-форме шаблона (стандарт §5.1): имя подставляет
	// util.ClickHouseImport. Типы — legacy-схема таблицы (ARCHITECTURE.md §6.2): name — имя ряда
	// из подписи источника, date — последний день отчётного месяца, balance — сырое значение
	// (баланс и P&L — млрд ₽, коэффициенты — доли), Float32 прежней таблицы не меняется.
	vtbIfrsTableDdl = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	// vtbCountPoints — количественный чек точки контроля публикации: число точек своей таблицы.
	// Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL обязателен — без
	// него count() считал бы несмёрженные части. Число растёт с каждым отчётом, поэтому решением
	// о no-op оно не служит (это делает гейт, О3 спеки), но объявлено: без него не включились бы
	// винтажный архив версий файлов и строки source_publications (стандарт §9.6).
	vtbCountPoints = "SELECT count() FROM rus_vtb_group_ifrs FINAL"
	// vtbIfrsDataField — первая строка балансового блока: якорь начала данных разбора
	// (parseVtbBookSheet) — строка над ней читается как шапка колонок-дат.
	vtbIfrsDataField = "Денежные средства и краткосрочные активы"
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
	// P&L («август'2026») — месяц-строка, для которой Go-layout не строится (util.MonthsToNum).
	vtbTimeLayout = "01-02-06"
	// vtbMinYear…vtbMaxYear — окно лет серийного номера даты: проценты изменения (0–100) дают
	// 1900 год и датами не прикидываются (идиома cbr/mo_br_parse.go).
	vtbMinYear = 2000
	vtbMaxYear = 2100
)

// vtbMonthLabelRe — подпись месячной колонки листа P&L: «август'2026». Накопленная колонка
// «8М'2026» (М — кириллическая) этой формой не покрывается: перед апострофом стоит цифра, а не
// буква, — и точкой месячного ряда не становится (vtbYtdLabelRe). Подпись той же формы с
// незнакомым словарю месяцем — ошибка, а не пропуск колонки (§6.4).
var vtbMonthLabelRe = regexp.MustCompile(`^([а-яёА-ЯЁ]+)'(\d{4})$`)

// vtbYtdLabelRe — подпись накопительной колонки листа P&L: «8М'2026» (M — кириллическая U+041C;
// латинская принимается той же формой). Накопительное значение точкой месячного ряда не является,
// но такой подписью опознаётся шапка выпуска, у которого месячных колонок нет вовсе (выпуск от
// 28.02.2026 печатает только «2М'2026»/«2М'2025») — по ней разбор отличает этот законный вид
// вёрстки от сломанной шапки (см. parseVtbPnlSheet).
var vtbYtdLabelRe = regexp.MustCompile(`^\d+[МM]'\d{4}$`)

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
		rowPoints, err := vtbRowPoints(sheet, i+1, label, rows[i], cols)
		if err != nil {
			return nil, err
		}
		points = append(points, rowPoints...)
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
// прибылях и убытках и «Показатели эффективности») ищутся по признаку — подписям колонок-периодов
// («август'2026», «2М'2026»), а не по номерам строк (§6.3). В таблицу идут только месячные
// колонки: накопленные («8М'2026» — январь–август) и текстовые «изменение» точками не являются —
// ключ таблицы (name, date) держит одно значение на месяц, и накопленный итог за тот же месяц
// затёр бы месячное значение, тогда как словарь метрик (net_profit, roe, roa) — помесячный. Дата
// точки месячной колонки — последний день её месяца (как и у колонок баланса). Строка со
// значениями, но без подписи ряда — ошибка (§6.4); строки после последнего раздела (дисклеймеры
// источника) значений не несут и точек не дают.
//
// Выпуск без месячных колонок — законный вид вёрстки источника, а не сломанная шапка: выпуск от
// 28.02.2026 печатает в обоих разделах только накопительные «2М'2026»/«2М'2025» (§6.6 — не
// падать на ожидаемом виде выпуска). Точки P&L такого выпуска не импортируются (накопительное
// значение в ключ (name, date) месячного ряда не встаёт), балансовая часть файла при этом
// импортируется, а о пропуске разбор предупреждает оператора — молчаливый пропуск был бы
// неотличим от «данных нет». Шапки без единой подписи-периода — по-прежнему ошибка (§6.4).
func parseVtbPnlSheet(xlsx *excelize.File) (points []vtbPoint, err error) {
	sheet, err := vtbSheet(xlsx, vtbPnlSheet)
	if err != nil {
		return nil, err
	}
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("лист %q: чтение строк: %w", sheet, err)
	}
	var (
		cols     []vtbColumn // месячные колонки текущего раздела; nil — до первой шапки
		sections int         // найденные шапки разделов: по ним выпуск опознаётся целиком
		monthly  int         // из шапок с месячными колонками
	)
	for i, row := range rows {
		section, isHeader, err := vtbPnlHeader(sheet, row)
		if err != nil {
			return nil, err
		}
		if isHeader {
			cols = section
			sections++
			if len(section) > 0 {
				monthly++
			}
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
		rowPoints, err := vtbRowPoints(sheet, i+1, label, row, cols)
		if err != nil {
			return nil, err
		}
		points = append(points, rowPoints...)
	}
	if sections == 0 {
		return nil, fmt.Errorf("лист %q: не найдена шапка раздела (ни месячных, ни накопительных подписей колонок)", sheet)
	}
	if monthly == 0 {
		// Пропуск виден оператору: «Imported 0 rows» без предупреждения читался бы как «данных
		// нет», хотя выпуск их печатает — только накопленным итогом (стандарт §6.6).
		log.Warnf("%s: лист %q: у выпуска нет месячных колонок P&L (только накопительные) — точки P&L не импортируются, баланс файла берётся",
			vtbIfrsTable, sheet)
		return nil, nil
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
// (§6.3, §6.4): заголовок листа, хотя бы одна строка «подпись — определение» и текстовые
// определения — числовая ячейка в колонке B означает, что на листе появилась таблица значений,
// и разбор обязан упасть, а не пропустить её молча.
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
	for i, row := range rows {
		label := vtbSeriesName(vtbCell(row, 0))
		definition := vtbCell(row, 1)
		if _, err := vtbFloat(definition); err == nil {
			return fmt.Errorf("лист %q, строка %d (%q): значение %q — число, а на листе ожидается текст определения коэффициента",
				sheet, i+1, label, definition)
		}
		switch {
		case label == vtbRatiosSheet:
			titled = true
		case label != "" && definition != "":
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

// vtbRowPoints — точки ряда: значения колонок-дат строки листа. Пустая ячейка точкой не
// становится (§6.7); значение читается через vtbFloat. Ошибка называет лист, строку и подпись
// ряда — без них нечисловая ячейка неотличима от соседних строк листа.
func vtbRowPoints(sheet string, n int, label string, row []string, cols []vtbColumn) ([]vtbPoint, error) {
	var points []vtbPoint
	for _, c := range cols {
		raw := vtbCell(row, c.col)
		if raw == "" {
			continue // пустая ячейка — не точка ряда (§6.7)
		}
		value, err := vtbFloat(raw)
		if err != nil {
			return nil, fmt.Errorf("лист %q, строка %d (%q): %w", sheet, n, label, err)
		}
		points = append(points, vtbPoint{Series: label, Date: c.date, Value: value})
	}
	return points, nil
}

// vtbPnlHeader — шапка раздела листа P&L: колонки-периоды ряда и признак «это шапка». Шапкой
// ряд делает подпись периода — месячная («август'2026») или накопительная («2М'2026»: у выпуска
// может не быть месячных колонок вовсе, см. parseVtbPnlSheet); текстовые «изменение» и прочие
// ячейки шапки признаком не являются.
//
// Месяц берётся из общего словаря месяцев util.MonthsToNum (прецедент — cbr/bop_parse.go,
// cbr/int_rate_loans.go): он же знает сокращения, которыми источник иногда подписывает колонку
// («апр'2025» выпуска 30.04.2025 — остальные 12 выпусков печатают полное «апрель'2026»), и
// опечатки источника («авугст»). Подпись-месяц, которой в словаре нет, — ошибка: колонка
// значений осталась бы без даты, а её потеря прошла бы молча (§6.4).
func vtbPnlHeader(sheet string, row []string) (cols []vtbColumn, isHeader bool, err error) {
	for col := 1; col < len(row); col++ {
		raw := vtbCell(row, col)
		if vtbYtdLabelRe.MatchString(raw) {
			isHeader = true
			continue // накопительная колонка точкой месячного ряда не становится
		}
		m := vtbMonthLabelRe.FindStringSubmatch(raw)
		if m == nil {
			continue // «изменение» и прочие текстовые ячейки шапки
		}
		month, known := util.MonthsToNum[strings.ToLower(m[1])]
		if !known {
			return nil, false, fmt.Errorf("лист %q: подпись колонки %q: месяц %q не распознан", sheet, raw, m[1])
		}
		year, err := strconv.Atoi(m[2])
		if err != nil {
			return nil, false, fmt.Errorf("лист %q: подпись колонки %q: год не разобран: %w", sheet, raw, err)
		}
		cols = append(cols, vtbColumn{col: col, date: vtbLastDayOfMonth(year, month)})
		isHeader = true
	}
	return cols, isHeader, nil
}

// vtbSheet — имя листа книги по якорю (§6.3): лист ищется по вхождению якоря в имя, а не по
// номеру — добавленный источником лист сдвинул бы номер. Якорь обязан указывать ровно на один
// лист: несколько совпадений — ошибка со списком (молча выбранный первый мог бы оказаться не тем
// листом), ни одного — ошибка со списком листов книги.
func vtbSheet(xlsx *excelize.File, anchor string) (string, error) {
	sheets := xlsx.GetSheetList()
	var matches []string
	for _, name := range sheets {
		if strings.Contains(name, anchor) {
			matches = append(matches, name)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("в книге несколько листов с якорем %q: %s", anchor, strings.Join(matches, ", "))
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
// cbr/mo_br_parse.go). Не конечные числа («NaN»/«Inf») — не даты: серийным номером они не
// являются, и колонка точек не даёт.
func vtbSerialColumns(header []string) []vtbColumn {
	var cols []vtbColumn
	for col := 1; col < len(header); col++ {
		serial, err := strconv.ParseFloat(vtbCell(header, col), 64)
		if err != nil {
			continue // текстовая колонка («изменение за месяц») — не колонка значений
		}
		if math.IsNaN(serial) || math.IsInf(serial, 0) {
			continue // «NaN»/«Inf» ParseFloat принимает, но датой такой номер не становится
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

// vtbFloat — число ячейки. Запятая — десятичный разделитель (0,202 = 0.202): в русской локали
// Excel пишет дроби так; смешанная запись (1,234.56) остаётся ошибкой — тысячным разделением
// запятая не считается. Обратная сторона неоднозначности: тысячную запись 1,234 разбор прочитает
// как 1.234; сырые числовые ячейки книги ВТБ пишут точку, риск проверен живым прогоном и назван
// в спеке/PR. «NaN»/«Inf» strconv.ParseFloat принимает без ошибки, но в Float32-колонку таблицы
// такое значение уехало бы порчей, которую видно только в витрине (прецедент — domrf_mortgage):
// это ошибка разбора (§6.4).
func vtbFloat(raw string) (float64, error) {
	value, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(raw), ",", "."), 64)
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

// vtbLink — отчётный файл МСФО со страницы результатов: абсолютный адрес и дата отчётности,
// вынутая из имени файла.
type vtbLink struct {
	Url    string
	Report time.Time
}

// vtbIfrsReportRe — дата отчётности в имени файла: «as-of-31-august-2026». Форма без учёта
// регистра: источник может выложить файл с «As-Of» и «August». Совпадение ищется по всему href,
// поэтому хвостовое подчёркивание источника (31-october-2025_.xlsx) и его опечатка месяца
// (31-jule-2025.xlsx) форме не мешают.
var vtbIfrsReportRe = regexp.MustCompile(`(?i)as-of-(\d{1,2})-([a-z]+)-(\d{4})`)

// vtbIfrsMonthNames — английские названия месяцев в имени файла, как их пишет источник; ключи — в
// нижнем регистре. «jule» — опечатка источника для июля (31-jule-2025.xlsx): без неё живой отчёт
// июля 2025 выпал бы из списка молча.
var vtbIfrsMonthNames = map[string]time.Month{
	"january":   time.January,
	"february":  time.February,
	"march":     time.March,
	"april":     time.April,
	"may":       time.May,
	"june":      time.June,
	"july":      time.July,
	"jule":      time.July, // опечатка источника (31-jule-2025.xlsx)
	"august":    time.August,
	"september": time.September,
	"october":   time.October,
	"november":  time.November,
	"december":  time.December,
}

// vtbIfrsReportDate — дата отчётности из имени файла. Незнакомый месяц, неразбираемые день или
// год и несуществующая дата (31-april) — ошибка, а не пропуск ссылки: молча выпавший отчёт
// выглядел бы как его отсутствие у источника (§6.4).
func vtbIfrsReportDate(href string) (time.Time, error) {
	m := vtbIfrsReportRe.FindStringSubmatch(href)
	if m == nil {
		return time.Time{}, fmt.Errorf("имя файла %q: не найдена дата отчётности вида as-of-<день>-<месяц>-<год>", href)
	}
	month, known := vtbIfrsMonthNames[strings.ToLower(m[2])]
	if !known {
		return time.Time{}, fmt.Errorf("имя файла %q: месяц %q не распознан", href, m[2])
	}
	day, err := strconv.Atoi(m[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("имя файла %q: день %q: %w", href, m[1], err)
	}
	year, err := strconv.Atoi(m[3])
	if err != nil {
		return time.Time{}, fmt.Errorf("имя файла %q: год %q: %w", href, m[3], err)
	}
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if date.Day() != day || date.Month() != month || date.Year() != year {
		return time.Time{}, fmt.Errorf("имя файла %q: дата отчётности %q не существует", href, m[0])
	}
	return date, nil
}

// vtbIfrsXlsLinks — список отчётных файлов со страницы результатов: goquery по байтам страницы
// (образец — cbr/dip_page.go). Отбираются ссылки якоря vtbIfrsLinkSelector с расширением .xlsx
// (pdf-версии тех же отчётов отбрасываются), дубли href дедуплицируются, дата отчётности
// читается из имени файла, список сортируется по возрастанию даты отчётности: свежие отчёты
// вставляются последними и выигрывают у пересекающихся периодов (в свежее опубликованном отчёте
// есть колонки прошлых месяцев, и его значения — уточнение прежних, спека §3.3).
//
// Разбор чистый (§6.1): страницу скачивает вызывающий (Import импортёра, util.GetPage), поэтому
// функция проверяется офлайн на HTML-фикстуре (§11.3). Fail-fast (§6.4): неразбираемое имя файла
// и пустой список — ошибка с доменом и именем файла, а не «новых отчётов нет» при живом
// источнике.
func vtbIfrsXlsLinks(page []byte) ([]vtbLink, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("%s: страница %s: не разбирается как HTML: %w", vtbIfrsTable, vtbIfrsResultsUrl, err)
	}
	var (
		links    []vtbLink
		seen     = make(map[string]bool)
		parseErr error
	)
	doc.Find(vtbIfrsLinkSelector).Each(func(_ int, a *goquery.Selection) {
		if parseErr != nil {
			return
		}
		href, ok := a.Attr("href")
		href = strings.TrimSpace(href)
		if !ok || href == "" || !strings.HasSuffix(strings.ToLower(href), ".xlsx") {
			return // pdf тех же отчётов и прочие файлы с якорем — не xlsx-отчёты
		}
		if seen[href] {
			return // дубль ссылки — не второй отчёт
		}
		report, err := vtbIfrsReportDate(href)
		if err != nil {
			parseErr = fmt.Errorf("%s: страница %s: %w", vtbIfrsTable, vtbIfrsResultsUrl, err)
			return
		}
		seen[href] = true
		links = append(links, vtbLink{Url: vtbIfrsDataUrlPrefix + href, Report: report})
	})
	if parseErr != nil {
		return nil, parseErr
	}
	if len(links) == 0 {
		return nil, fmt.Errorf("%s: страница %s: не найдено ни одной ссылки на rus-vtb-group-ifrs-as-of-*.xlsx (селектор %s)",
			vtbIfrsTable, vtbIfrsResultsUrl, vtbIfrsLinkSelector)
	}
	// sort.SliceStable, а не sort.Slice: у двух отчётов с одной датой порядок страницы сохранится,
	// и результат детерминирован без уникальности дат.
	sort.SliceStable(links, func(i, j int) bool { return links[i].Report.Before(links[j].Report) })
	return links, nil
}

// VtbIfrs — импортёр rus_vtb_group_ifrs (МСФО группы ВТБ): встроенный util.ClickHouseImport
// ведёт таблицу, DDL, точку контроля публикации и производные (каталог и витрины — AfterImport,
// afterBankMacro), а Import переопределён: у импортёра не один артефакт, а набор отчётных файлов
// по одному на дату отчётности (спека §3.3, отступление О3). Решение «что
// уже импортировано» принимает гейт пофайловых версий, а не префлайт шаблона: отпечаток одного
// файла не отвечает на вопрос о наборе, а число точек всей таблицы не сойдётся ни с одним файлом
// — префлайт пере-импортировал бы весь набор на каждом прогоне. Name() не переопределён: имя
// импортёра совпадает с именем таблицы. Селектор ссылок регистрозависим (как и прежний
// селектор сбора ссылок): `*=` не найдёт имя с другим регистром (RUS-vtb-…).
type VtbIfrs struct {
	util.ClickHouseImport
}

// Import — полный цикл одного прогона (структура — образец cbr/dip_bulletin.go):
//  1. режим восстановления — ошибка до сети: restore резолвит один артефакт по url, а у этого
//     импортёра адрес отчёта не один и не статичен (спека §1.3);
//  2. страница результатов → список отчётных файлов по возрастанию даты отчётности;
//  3. цикл: HEAD-отпечаток файла (util.HeadFingerprint) и известность версии
//     (util.PublicationKnown) — известная версия пропускается, неизвестная импортируется
//     локальной копией шаблона: DataUrl — свойство файла, а общую структуру импортёра мутировать
//     нельзя (прецедент — cbr/mo_br.go) — её читают тест реестра и объявление точки контроля.
//     Любая неудача гейта — недоступный HEAD (отпечатка нет, версию записывать нельзя) или
//     нечитаемые версии — ведёт в полный импорт файла: «не смог проверить» не равно
//     «импортировано», а импорт идемпотентен;
//  4. производные ещё раз после цикла: гейт видит файлы известными и полный путь шаблона (а с
//     ним его AfterImport) больше не повторяет — без этого пустая дельта оставляла бы каталог и
//     витрину неопубликованными (урок rosstat_ipc_weeks_bulletins, спека §3.3).
//
// Сбой одного файла не отменяет остальные: файлы независимы (у каждого свой url, файл и строка
// версии), ошибки копятся errors.Join, прогон завершается ошибкой — dagu видит сбой и не пускает
// цепочку дальше. Архив и строки версий уже записанных файлов гейт не лечит — как у
// cbr_dip_bulletin, лечение ручное: удалить строку файла из source_publications и повторить
// прогон (ARCHITECTURE §8 п. 13). Ошибки разбора не оборачиваются именем таблицы второй раз:
// parseVtbBook уже называет её, и второй префикс дал бы дубль в тексте ошибки.
func (s *VtbIfrs) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// Режим читается один раз на прогон: значение не меняется посреди импорта, а env читается в
	// момент действия, не в init() (ARCHITECTURE §8.6).
	if util.ArchiveRestoreMode() {
		return 0, fmt.Errorf("%s: режим восстановления (%s=%s) не поддержан импортёром набора файлов: restore резолвит один артефакт по url, а адрес отчёта берётся со страницы %s",
			vtbIfrsTable, util.EnvArchiveAction, util.ArchiveActionRestore, vtbIfrsResultsUrl)
	}
	page, err := util.GetPage(vtbIfrsResultsUrl)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", vtbIfrsTable, vtbIfrsResultsUrl, err)
	}
	links, err := vtbIfrsXlsLinks(page) // ветки разбора уже называют таблицу и страницу
	if err != nil {
		return 0, err
	}

	var errs []error
	for _, link := range links {
		name := path.Base(link.Url)
		fp, herr := util.HeadFingerprint(link.Url)
		if herr != nil {
			log.Warnf("%s: HEAD %s недоступен (%v) — полный импорт без записи версии", vtbIfrsTable, link.Url, herr)
		} else {
			_, known, kerr := util.PublicationKnown(ctx, conn, vtbIfrsTable, name, fp)
			switch {
			case kerr != nil:
				// Сбой чтения версий не роняет прогон: полный импорт идемпотентен, а версия потом
				// перезапишется (та же политика, что у префлайта шаблона). На пустой базе
				// source_publications ещё нет — первый файл заводит её префлайтом шаблона.
				log.Warnf("%s: версия файла %s не проверена (%v) — полный импорт", vtbIfrsTable, name, kerr)
			case known:
				log.Infof("%s: файл %s (версия %s) уже импортирован — пропуск", vtbIfrsTable, name, fp)
				continue
			}
		}

		// Локальная копия шаблона приносит объявленный префлайт (PublicationCountPoints):
		// судьбу файла он не решает (решает гейт), но без него не было бы ни архива версии
		// файла, ни строки source_publications. HEAD у префлайта свой — на новый файл это
		// второй HEAD за прогон: отпечаток версии считает и записывает шаблон.
		local := s.ClickHouseImport
		local.DataUrl = link.Url
		points, importErr := local.Import(ctx, conn)
		if importErr != nil {
			errs = append(errs, fmt.Errorf("%s: %w", link.Url, importErr))
			continue
		}
		count += points
		log.Infof("%s: файл %s: импортировано %d точек", vtbIfrsTable, name, points)
	}

	// Производные публикуются и на пустой дельте (идемпотентно): гейт видит файлы известными и
	// полный путь шаблона больше не повторяет, поэтому сбой публикации каталога или витрины
	// прошлого прогона лечится следующим (урок rosstat_ipc_weeks_bulletins).
	if s.AfterImport != nil {
		if err = s.AfterImport(ctx, conn); err != nil {
			errs = append(errs, err)
		}
	}
	return count, errors.Join(errs...)
}

// vtbIfrsImportFunc — ImportFunc шаблона: книга из памяти → точки → батч. Ошибку разбора не
// оборачивает: parseVtbBook уже называет таблицу, и второй префикс дал бы дубль в тексте ошибки.
func vtbIfrsImportFunc(xlsx *excelize.File, batch driver.Batch) error {
	points, err := parseVtbBook(xlsx)
	if err != nil {
		return err
	}
	return appendVtbPoints(batch, points)
}

// vtbImporter — импортёр rus_vtb_group_ifrs для шаблона и реестра: таблица, DDL в %s-форме,
// разбор книги (ImportFunc) и объявление точки контроля публикации (vtbCountPoints). Оно
// включает винтажный архив и строки source_publications — их пишет шаблон в цикле Import,
// пофайлово. AfterImport — общий каталог рядов пакета и витрины (afterBankMacro): собственный
// Import() зовёт тот же хелпер после цикла файлов, поэтому каталог и витрины публикуются и на
// пустой дельте, когда гейт пропустил все файлы и шаблонный путь не выполнялся. Регистрацию
// держит реестр пакета (bank/importers.go) — единственная точка append в chimport.Stats.
var vtbImporter = &VtbIfrs{ClickHouseImport: util.ClickHouseImport{
	TableName:              vtbIfrsTable,
	CreateTable:            []string{vtbIfrsTableDdl},
	ImportFunc:             vtbIfrsImportFunc,
	PublicationCountPoints: vtbCountPoints,
	AfterImport:            afterBankMacro(bankSeriesMeta),
}}
