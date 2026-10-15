package cbr

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// Средневзвешенные процентные ставки по кредитам, предоставленным кредитными организациями
// нефинансовым организациям в рублях (ЦБ РФ): https://www.cbr.ru/statistics/pdko/int_rat/

const (
	cbrIntRateLoansTable = "cbr_int_rate_loans"
	cbrIntRateLoansSheet = "ставки_руб"
	// cbrIntRateLoansDataUrl — стабильная прямая ссылка на файл (стандарт §4.1, строка
	// «XLSX»): даты в ней нет, файл меняется месяцами, поэтому качает его шаблон — те же байты
	// уходят и в разбор, и в винтажный архив версии (§9.6).
	cbrIntRateLoansDataUrl = "https://www.cbr.ru/vfs/statistics/pdko/int_rat/loans_nonfin.xlsx"
	// cbrIntRateLoansCountPoints — количественный чек версии (спека §3.5): число точек своей
	// таблицы. Литерал, как и имя таблицы: запрос — часть утверждения «эти точки мои». FINAL
	// обязателен — иначе count() считал бы несмёрженные части.
	cbrIntRateLoansCountPoints = "SELECT count() FROM cbr_int_rate_loans FINAL"
	// cbrIntRateLoansHeaderAnchor — подпись шапки источника в первой колонке («% годовых»).
	// От неё отсчитываются строка групп и строка заголовков колонок: магических индексов строк
	// в разборе нет, номер шапки выводится из якоря (стандарт §6.3).
	cbrIntRateLoansHeaderAnchor = "% годовых"
)

// cbrIntRateLoansGroups — группы колонок в порядке листа: префикс имени ряда в коде и подпись
// группы в строке групп источника. У второй группы подпись в файле длиннее («в том числе
// субъектам малого и среднего предпринимательства»), в якорь взято её устойчивое начало —
// так же, как это делает спека §3.4.
var cbrIntRateLoansGroups = []struct {
	Prefix string
	Label  string
}{
	{Prefix: "Всего: ", Label: "Всего"},
	{Prefix: "СМП: ", Label: "в том числе субъектам малого"},
}

// cbrIntRateLoansTerms — восемь сроков кредита в порядке колонок внутри каждой группы. Наборы
// сроков двух групп идентичны, поэтому заголовок колонки сам по себе имя ряда не даёт: имя
// собирает код из префикса группы и срока (стандарт §6.8), а текст источника только сверяется
// с ожидаемым — расхождение означает смену структуры листа (§6.4), а не новый ряд.
var cbrIntRateLoansTerms = []string{
	"до 30 дней, включая «до востребования»",
	"от 31 до 90 дней",
	"от 91 до 180 дней",
	"от 181 дня до 1 года",
	"до 1 года, включая «до востребования»",
	"от 1 года до 3 лет",
	"свыше 3 лет",
	"свыше 1 года",
}

// cbrIntRateLoansNames — 16 имён рядов в порядке колонок листа: группа × срок. Список
// собирается из групп и сроков, а не переписывается вторым списком: имена объявляет код
// (стандарт §6.8), и вторая запись тех же имён стала бы вторым источником правды —
// переименование пришлось бы делать в двух местах, а расхождение заметил бы только разбор.
// Каталог рядов (cbr/series_meta.go) читает тот же список.
var cbrIntRateLoansNames = cbrIntRateLoansSeriesNames()

// cbrIntRateLoansSeriesNames собирает имена рядов из групп и сроков: внешний цикл — группы,
// внутренний — сроки, то есть порядок колонок листа.
func cbrIntRateLoansSeriesNames() []string {
	names := make([]string, 0, len(cbrIntRateLoansGroups)*len(cbrIntRateLoansTerms))
	for _, group := range cbrIntRateLoansGroups {
		for _, term := range cbrIntRateLoansTerms {
			names = append(names, group.Prefix+term)
		}
	}
	return names
}

// cbrIntRateLoansHeaderReplacer — типографика заголовков, не влияющая на смысл колонки:
// кавычки (в xlsx источника срок записан с апострофами по краям цитаты, в спеке — в кавычках
// «…»), неразрывный пробел ЦБ и дефис переноса слова («до востребова-ния»). Дефис убирается
// целиком: в наборе сроков он встречается только на месте переноса.
var cbrIntRateLoansHeaderReplacer = strings.NewReplacer(
	"«", "", "»", "", "„", "", "“", "", "”", "", "\"", "", "'", "",
	"-", "", "\u00a0", " ",
)

// cbrIntRateLoansHeaderKey приводит текст заголовка к сравнимому виду: регистр не значим,
// пробелы (в том числе неразрывные) схлопнуты, кавычки и переносы сняты. Сверка заголовка с
// ожидаемым сроком — это проверка «источник не сменил структуру листа» (стандарт §6.4): смена
// текста означала бы новую редакцию файла, а не новый ряд, и должна валить прогон, а не
// заводить колонку с чужим смыслом под прежним именем.
func cbrIntRateLoansHeaderKey(header string) string {
	return strings.Join(strings.Fields(cbrIntRateLoansHeaderReplacer.Replace(strings.ToLower(header))), " ")
}

// cbrIntRateLoansIsGroupLabel — подпись группы в ячейке строки групп: точное совпадение или
// совпадение по началу слова (длинная подпись СМП в якоре обрезана, и «малого» стоит в ней
// отдельным словом).
func cbrIntRateLoansIsGroupLabel(cell, label string) bool {
	key, label := cbrIntRateLoansHeaderKey(cell), cbrIntRateLoansHeaderKey(label)
	return key == label || strings.HasPrefix(key, label+" ")
}

// cbrIntRateLoansGroupStarts возвращает номера колонок, в которых стоят подписи групп, — в
// порядке, объявленном cbrIntRateLoansGroups. Подписи ищутся последовательно (сначала подпись
// первой группы, затем второй), поэтому неполный набор означает, что строка не та: nil, а не
// частичный список колонок, — разбор работает только с полным набором групп.
func cbrIntRateLoansGroupStarts(row []string) []int {
	starts := make([]int, 0, len(cbrIntRateLoansGroups))
	for i, cell := range row {
		if len(starts) == len(cbrIntRateLoansGroups) {
			break
		}
		if cbrIntRateLoansIsGroupLabel(cell, cbrIntRateLoansGroups[len(starts)].Label) {
			starts = append(starts, i)
		}
	}
	if len(starts) != len(cbrIntRateLoansGroups) {
		return nil
	}
	return starts
}

// cbrIntRateLoansColumns разбирает шапку листа и возвращает имена колонок данных в порядке
// листа и номер первой строки данных — следующей за шапкой. Якоря, а не магические индексы
// (стандарт §6.3): строка «% годовых» даёт начало шапки, в следующей за ней строке ищутся
// подписи групп, строка заголовков колонок — следующая за строкой групп. Колонки привязываются
// к группам позиционно (границы — от подписи группы до подписи следующей; merged-диапазоны не
// читаются), внутри группы — по порядку сроков: имена идут из кода, заголовок сверяется.
func cbrIntRateLoansColumns(rows [][]string) (names []string, firstDataRow int, err error) {
	anchor := -1
	for i, row := range rows {
		if len(row) > 0 && strings.TrimSpace(row[0]) == cbrIntRateLoansHeaderAnchor {
			anchor = i
			break
		}
	}
	if anchor < 0 {
		return nil, 0, fmt.Errorf("%s: в листе «%s» не найдена строка «%s» — шапка источника не распознана",
			cbrIntRateLoansTable, cbrIntRateLoansSheet, cbrIntRateLoansHeaderAnchor)
	}
	groupRow, starts := -1, []int(nil)
	for i := anchor + 1; i < len(rows); i++ {
		if rowStarts := cbrIntRateLoansGroupStarts(rows[i]); rowStarts != nil {
			groupRow, starts = i, rowStarts
			break
		}
	}
	if groupRow < 0 {
		return nil, 0, fmt.Errorf("%s: в листе «%s» не найдена строка с подписями групп «%s» и «%s» — шапка источника не распознана",
			cbrIntRateLoansTable, cbrIntRateLoansSheet, cbrIntRateLoansGroups[0].Label, cbrIntRateLoansGroups[1].Label)
	}
	// Строка заголовков колонок — следующая за строкой групп. Её отсутствие (строка групп
	// последняя) проверяется, а не предполагается: срез rows[firstDataRow:] ниже паникует на
	// индексе за концом слайса (стандарт §6.5).
	headerRow := groupRow + 1
	if headerRow >= len(rows) {
		return nil, 0, fmt.Errorf("%s: в листе «%s» нет строки заголовков колонок под строкой групп",
			cbrIntRateLoansTable, cbrIntRateLoansSheet)
	}
	names = make([]string, 0, len(cbrIntRateLoansNames))
	for g, start := range starts {
		end := len(rows[headerRow])
		if g+1 < len(starts) {
			end = starts[g+1]
		}
		// Границы группы проверяются до индексации: конец группы — либо начало следующей
		// подписи, либо конец строки заголовков, и оба случая обязаны укладываться в её длину.
		if end > len(rows[headerRow]) {
			return nil, 0, fmt.Errorf("%s: лист «%s», группа «%s»: строка заголовков колонок короче границ группы (%d колонок, группа кончается на %d) — источник изменил структуру листа",
				cbrIntRateLoansTable, cbrIntRateLoansSheet, cbrIntRateLoansGroups[g].Label, len(rows[headerRow]), end)
		}
		if end-start != len(cbrIntRateLoansTerms) {
			return nil, 0, fmt.Errorf("%s: лист «%s», группа «%s»: колонок %d, а сроков %d — источник изменил структуру листа",
				cbrIntRateLoansTable, cbrIntRateLoansSheet, cbrIntRateLoansGroups[g].Label, end-start, len(cbrIntRateLoansTerms))
		}
		for j, term := range cbrIntRateLoansTerms {
			header := rows[headerRow][start+j]
			if cbrIntRateLoansHeaderKey(header) != cbrIntRateLoansHeaderKey(term) {
				return nil, 0, fmt.Errorf("%s: лист «%s», группа «%s»: заголовок колонки %q не совпал с ожидаемым сроком %q — источник изменил структуру листа",
					cbrIntRateLoansTable, cbrIntRateLoansSheet, cbrIntRateLoansGroups[g].Label, header, term)
			}
			names = append(names, cbrIntRateLoansGroups[g].Prefix+term)
		}
	}
	return names, headerRow + 1, nil
}

// cbrIntRateLoansMonthDate превращает метку периода «Январь 2014» в дату точки — предпоследний
// день месяца отчётного периода: time.Date(year, month+1, -1, …). Конвенция та же, что у
// cbr_bank_int_rate (спека §3.3), поэтому ставки по вкладам и по кредитам сопоставимы на одной
// оси; месяц берётся из util.MonthsToNum — словарь месяцев один на пакет.
func cbrIntRateLoansMonthDate(label string) (time.Time, error) {
	parts := strings.Fields(label)
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("метка %q не разобрана как «Месяц Год»", label)
	}
	month, ok := util.MonthsToNum[strings.ToLower(parts[0])]
	if !ok {
		return time.Time{}, fmt.Errorf("неизвестный месяц %q в метке %q", parts[0], label)
	}
	year, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("год %q в метке %q: %w", parts[1], label, err)
	}
	return time.Date(year, month+1, -1, 0, 0, 0, 0, time.UTC), nil
}

// cbrIntRateLoansImport разбирает лист «ставки_руб»: метка строки «Январь 2014» — месяц
// отчётного периода, 16 колонок — ряды (группа × срок), значение — средневзвешенная ставка,
// % годовых. Пустая ячейка — пропуск точки (стандарт §6.7); строка сноски, начинающаяся с «*»
// и стоящая после данных, завершает разбор, любая другая неразобранная метка — ошибка: это
// смена структуры листа (§6.4), а не конец данных.
func cbrIntRateLoansImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows(cbrIntRateLoansSheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист «%s»: %w", cbrIntRateLoansTable, cbrIntRateLoansSheet, err)
	}
	names, firstDataRow, err := cbrIntRateLoansColumns(rows)
	if err != nil {
		return err
	}
	count := 0
	for _, row := range rows[firstDataRow:] {
		if len(row) == 0 {
			continue
		}
		label := strings.TrimSpace(row[0])
		if label == "" {
			continue
		}
		date, err := cbrIntRateLoansMonthDate(label)
		if err != nil {
			// Сноска источника начинается с «*» и стоит после данных — разбор закончен. Любая
			// другая неразобранная метка означает смену структуры листа: ошибка, а не «0 rows»,
			// иначе шаг DAG завершился бы успешно, а 16 рядов остались бы без обновления молча.
			if strings.HasPrefix(label, "*") {
				break
			}
			return fmt.Errorf("%s: лист «%s»: %w", cbrIntRateLoansTable, cbrIntRateLoansSheet, err)
		}
		for i, name := range names {
			// Колонка данных i — это ячейка i+1 строки: ячейка 0 занята меткой периода.
			column := i + 1
			// Строка короче шапки — у остальных колонок данных нет; недостающие ячейки
			// равносильны пустым и точками не становятся (§6.7).
			if column >= len(row) {
				break
			}
			cell := strings.TrimSpace(row[column])
			if cell == "" {
				continue
			}
			rate, err := strconv.ParseFloat(cell, 64)
			if err != nil {
				return fmt.Errorf("%s: лист «%s», метка %q, ряд «%s»: значение %q: %w",
					cbrIntRateLoansTable, cbrIntRateLoansSheet, label, name, cell, err)
			}
			if err = batch.Append(name, date, rate); err != nil {
				return err
			}
			count++
		}
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор листа «%s» не дал ни одной точки", cbrIntRateLoansTable, cbrIntRateLoansSheet)
	}
	return nil
}

// cbrIntRateLoans — средневзвешенные ставки по кредитам нефинансовым организациям в рублях:
// таблица, DDL и разбор листа «ставки_руб»; каталог рядов и витрину v_cbr_macro публикует
// шаблон через AfterImport. Сломанная шапка, пустой/короткий лист и ноль точек — ошибка
// (fail-fast): «0 rows» выглядели бы успешным прогоном, а шестнадцать рядов остались бы без
// обновления молча.
var cbrIntRateLoans = util.ClickHouseImport{
	TableName: cbrIntRateLoansTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, rate Float64
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`},
	DataUrl:                cbrIntRateLoansDataUrl,
	ImportFunc:             cbrIntRateLoansImport,
	AfterImport:            afterCbrMacro(cbrIntRateLoansSeriesMeta),
	PublicationCountPoints: cbrIntRateLoansCountPoints,
}

// cbrIntRateLoansImporter — ставки по кредитам юрлицам; регистрируется реестром
// cbr/importers.go.
var cbrIntRateLoansImporter = &cbrIntRateLoans
