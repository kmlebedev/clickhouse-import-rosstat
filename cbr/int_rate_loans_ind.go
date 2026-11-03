package cbr

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// Средневзвешенные процентные ставки по кредитам, предоставленным кредитными организациями
// физическим лицам в рублях (ЦБ РФ): https://www.cbr.ru/statistics/pdko/int_rat/

const (
	cbrIntRateLoansIndTable = "cbr_int_rate_loans_ind"
	cbrIntRateLoansIndSheet = "ставки_руб"
	// cbrIntRateLoansIndDataUrl — стабильная прямая ссылка на файл (стандарт §4.1, строка
	// «XLSX»): даты в ней нет, файл меняется месяцами, поэтому качает его шаблон — те же байты
	// уходят и в разбор, и в винтажный архив версии (§9.6).
	cbrIntRateLoansIndDataUrl = "https://www.cbr.ru/vfs/statistics/pdko/int_rat/loans_ind.xlsx"
	// cbrIntRateLoansIndCountPoints — количественный чек версии (спека §3.5): число точек своей
	// таблицы. Литерал, как и имя таблицы: запрос — часть утверждения «эти точки мои». FINAL
	// обязателен — иначе count() считал бы несмёрженные части.
	cbrIntRateLoansIndCountPoints = "SELECT count() FROM cbr_int_rate_loans_ind FINAL"
	// cbrIntRateLoansIndHeaderAnchor — подпись шапки источника в первой колонке («% годовых»).
	// От неё отсчитываются строка групп и строка заголовков колонок: магических индексов строк
	// в разборе нет, номер шапки выводится из якоря (стандарт §6.3).
	cbrIntRateLoansIndHeaderAnchor = "% годовых"
)

// cbrIntRateLoansIndGroups — группы колонок в порядке листа: префикс имени ряда в коде, подпись
// группы в строке групп источника и полный текст этой подписи для каталога рядов. В данных имя
// второй группы — «Автокредиты», а не дословная подпись источника «в том числе автокредиты»
// (спека §3.1); в origin каталога (series_meta.go) группа называется целиком (SourceLabel):
// иначе агент MCP прочитал бы сокращение как отдельную группу.
//
// Префикс первой группы отличается от спекового «Всего: » осознанно: под этим префиксом уже
// живут восемь рядов cbr_int_rate_loans (кредиты юрлицам), а series_catalog ключуется по
// (source='cbr', series), и одинаковые имена двух импортёров вытеснили бы описания друг друга
// (v_series_catalog — FINAL), а в v_cbr_macro ряды двух разных совокупностей стали бы
// неразличимы по series. Поэтому совокупность названа в самом префиксе: «Всего (кредиты
// физлицам): <срок>»; подгруппа «Автокредиты: <срок>» уникальна и оставлена спековой.
var cbrIntRateLoansIndGroups = []struct {
	Prefix      string
	Label       string
	SourceLabel string
}{
	{Prefix: "Всего (кредиты физлицам): ", Label: "Всего", SourceLabel: "Всего"},
	{Prefix: "Автокредиты: ", Label: "в том числе автокредиты", SourceLabel: "в том числе автокредиты"},
}

// cbrIntRateLoansIndTerms — восемь сроков кредита в порядке колонок внутри каждой группы. Наборы
// сроков двух групп идентичны, поэтому заголовок колонки сам по себе имя ряда не даёт: имя
// собирает код из префикса группы и срока (стандарт §6.8), а текст источника только сверяется
// с ожидаемым — расхождение означает смену структуры листа (§6.4), а не новый ряд.
var cbrIntRateLoansIndTerms = []string{
	"до 30 дней, включая «до востребования»",
	"от 31 до 90 дней",
	"от 91 до 180 дней",
	"от 181 дня до 1 года",
	"до 1 года, включая «до востребования»",
	"от 1 года до 3 лет",
	"свыше 3 лет",
	"свыше 1 года",
}

// cbrIntRateLoansIndNames — 16 имён рядов в порядке колонок листа: группа × срок. Список
// собирается из групп и сроков, а не переписывается вторым списком: имена объявляет код
// (стандарт §6.8), и вторая запись тех же имён стала бы вторым источником правды.
// Каталог рядов (cbr/series_meta.go) читает тот же список.
var cbrIntRateLoansIndNames = cbrIntRateLoansIndSeriesNames()

// cbrIntRateLoansIndSeriesNames собирает имена рядов из групп и сроков: внешний цикл — группы,
// внутренний — сроки, то есть порядок колонок листа.
func cbrIntRateLoansIndSeriesNames() []string {
	names := make([]string, 0, len(cbrIntRateLoansIndGroups)*len(cbrIntRateLoansIndTerms))
	for _, group := range cbrIntRateLoansIndGroups {
		for _, term := range cbrIntRateLoansIndTerms {
			names = append(names, group.Prefix+term)
		}
	}
	return names
}

// cbrIntRateLoansIndHeaderReplacer — типографика заголовков, не влияющая на смысл колонки:
// кавычки (в xlsx источника срок записан с апострофами по краям цитаты, в спеке — в кавычках
// «…»), сноски-звёздочки у сроков группы «Всего», неразрывный пробел ЦБ и дефис переноса слова
// («до востребова-ния»). Дефис убирается целиком: в наборе сроков он встречается только на
// месте переноса. Звёздочки снимаются вместе с кавычками: без этого заголовок
// «от 31 до 90 дней**» не совпал бы с ожидаемым сроком и валил бы прогон на живой книге.
var cbrIntRateLoansIndHeaderReplacer = strings.NewReplacer(
	"«", "", "»", "", "„", "", "“", "", "”", "", "\"", "", "'", "",
	"*", "", "-", "", "\u00a0", " ",
)

// cbrIntRateLoansIndHeaderKey приводит текст заголовка к сравнимому виду: регистр не значим,
// пробелы (в том числе неразрывные) схлопнуты, кавычки, звёздочки и переносы сняты. Сверка
// заголовка с ожидаемым сроком — это проверка «источник не сменил структуру листа»
// (стандарт §6.4): смена текста означала бы новую редакцию файла, а не новый ряд, и должна
// валить прогон, а не заводить колонку с чужим смыслом под прежним именем.
func cbrIntRateLoansIndHeaderKey(header string) string {
	return strings.Join(strings.Fields(cbrIntRateLoansIndHeaderReplacer.Replace(strings.ToLower(header))), " ")
}

// cbrIntRateLoansIndIsGroupLabel — подпись группы в ячейке строки групп: точное совпадение или
// совпадение по началу слова (длинная подпись группы в якоре может быть обрезана, и её последнее
// слово стоит отдельно).
func cbrIntRateLoansIndIsGroupLabel(cell, label string) bool {
	key, label := cbrIntRateLoansIndHeaderKey(cell), cbrIntRateLoansIndHeaderKey(label)
	return key == label || strings.HasPrefix(key, label+" ")
}

// cbrIntRateLoansIndGroupStarts возвращает номера колонок, в которых стоят подписи групп, — в
// порядке, объявленном cbrIntRateLoansIndGroups. Подписи ищутся последовательно (сначала подпись
// первой группы, затем второй), поэтому неполный набор означает, что строка не та: nil, а не
// частичный список колонок, — разбор работает только с полным набором групп.
func cbrIntRateLoansIndGroupStarts(row []string) []int {
	starts := make([]int, 0, len(cbrIntRateLoansIndGroups))
	for i, cell := range row {
		if len(starts) == len(cbrIntRateLoansIndGroups) {
			break
		}
		if cbrIntRateLoansIndIsGroupLabel(cell, cbrIntRateLoansIndGroups[len(starts)].Label) {
			starts = append(starts, i)
		}
	}
	if len(starts) != len(cbrIntRateLoansIndGroups) {
		return nil
	}
	return starts
}

// cbrIntRateLoansIndColumns разбирает шапку листа и возвращает имена колонок данных в порядке
// листа, номер первой колонки данных (base — колонка, в которой стоит первое имя) и номер первой
// строки данных — следующей за шапкой. Якоря, а не магические индексы (стандарт §6.3): строка
// «% годовых» даёт начало шапки, в следующей за ней строке ищутся подписи групп, строка
// заголовков колонок — следующая за строкой групп. Колонки привязываются к группам позиционно
// (границы — от подписи группы до подписи следующей; merged-диапазоны не читаются), внутри
// группы — по порядку сроков: имена идут из кода, заголовок сверяется. База тоже выводится из
// шапки: у колонки-метки слева от первой группы позиция в файле не фиксирована, а разбор данных
// считает колонку как base+i — при вставке колонки перед группой шапка «совпала» бы, и значения
// прочитались бы из сдвинутых колонок под правильными именами.
func cbrIntRateLoansIndColumns(rows [][]string) (names []string, base, firstDataRow int, err error) {
	anchor := -1
	for i, row := range rows {
		if len(row) > 0 && strings.TrimSpace(row[0]) == cbrIntRateLoansIndHeaderAnchor {
			anchor = i
			break
		}
	}
	if anchor < 0 {
		return nil, 0, 0, fmt.Errorf("%s: в листе «%s» не найдена строка «%s» — шапка источника не распознана",
			cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, cbrIntRateLoansIndHeaderAnchor)
	}
	groupRow, starts := -1, []int(nil)
	for i := anchor + 1; i < len(rows); i++ {
		if rowStarts := cbrIntRateLoansIndGroupStarts(rows[i]); rowStarts != nil {
			groupRow, starts = i, rowStarts
			break
		}
	}
	if groupRow < 0 {
		return nil, 0, 0, fmt.Errorf("%s: в листе «%s» не найдена строка с подписями групп «%s» и «%s» — шапка источника не распознана",
			cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, cbrIntRateLoansIndGroups[0].Label, cbrIntRateLoansIndGroups[1].Label)
	}
	// Строка заголовков колонок — следующая за строкой групп. Её отсутствие (строка групп
	// последняя) проверяется, а не предполагается: срез rows[firstDataRow:] ниже паникует на
	// индексе за концом слайса (стандарт §6.5).
	headerRow := groupRow + 1
	if headerRow >= len(rows) {
		return nil, 0, 0, fmt.Errorf("%s: в листе «%s» нет строки заголовков колонок под строкой групп",
			cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet)
	}
	// Первая колонка данных — та, в которой стоит подпись первой группы: строка групп и строка
	// заголовков колонок выровнены, и левее неё лежит только метка периода.
	base = starts[0]
	names = make([]string, 0, len(cbrIntRateLoansIndNames))
	for g, start := range starts {
		end := len(rows[headerRow])
		if g+1 < len(starts) {
			end = starts[g+1]
		}
		// Границы группы проверяются до индексации: конец группы — либо начало следующей
		// подписи, либо конец строки заголовков, и оба случая обязаны укладываться в её длину.
		if end > len(rows[headerRow]) {
			return nil, 0, 0, fmt.Errorf("%s: лист «%s», группа «%s»: строка заголовков колонок короче границ группы (%d колонок, группа кончается на %d) — источник изменил структуру листа",
				cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, cbrIntRateLoansIndGroups[g].Label, len(rows[headerRow]), end)
		}
		if end-start != len(cbrIntRateLoansIndTerms) {
			return nil, 0, 0, fmt.Errorf("%s: лист «%s», группа «%s»: колонок %d, а сроков %d — источник изменил структуру листа",
				cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, cbrIntRateLoansIndGroups[g].Label, end-start, len(cbrIntRateLoansIndTerms))
		}
		for j, term := range cbrIntRateLoansIndTerms {
			header := rows[headerRow][start+j]
			if cbrIntRateLoansIndHeaderKey(header) != cbrIntRateLoansIndHeaderKey(term) {
				return nil, 0, 0, fmt.Errorf("%s: лист «%s», группа «%s»: заголовок колонки %q не совпал с ожидаемым сроком %q — источник изменил структуру листа",
					cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, cbrIntRateLoansIndGroups[g].Label, header, term)
			}
			names = append(names, cbrIntRateLoansIndGroups[g].Prefix+term)
		}
	}
	// Инвариант, на который опирается разбор данных: база и все имена укладываются в строку
	// заголовков (column = base+i в cbrIntRateLoansIndImport). Границы групп выше уже проверены
	// каждая по отдельности — это не их повтор, а вывод из них, стерегущий возвращаемую базу:
	// будущая правка, сдвинувшая её за конец шапки, иначе увела бы разбор в чужие колонки молча.
	if base+len(names) > len(rows[headerRow]) {
		return nil, 0, 0, fmt.Errorf("%s: лист «%s»: %d колонок данных от колонки %d не укладываются в строку заголовков (%d колонок)",
			cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, len(names), base, len(rows[headerRow]))
	}
	return names, base, headerRow + 1, nil
}

// cbrIntRateLoansIndMonthDate превращает метку периода «Январь 2014» в дату точки —
// предпоследний день месяца отчётного периода: time.Date(year, month+1, -1, …). У ЦБ в колонке
// метки стоит месяц, а не день; привязка — та же конвенция контура, что у cbr_int_rate_loans и
// cbr_bank_int_rate (спека §3.3, 2014-01-30 … 2025-12-30), поэтому ставки по кредитам физлицам
// сопоставимы с другими рядами по toStartOfMonth(date). Месяц берётся из util.MonthsToNum —
// словарь месяцев один на пакет.
func cbrIntRateLoansIndMonthDate(label string) (time.Time, error) {
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

// cbrIntRateLoansIndImport разбирает лист «ставки_руб»: метка строки «Январь 2014» — месяц
// отчётного периода, 16 колонок — ряды (группа × срок), значение — средневзвешенная ставка,
// % годовых. Пустая ячейка, литеральный «-» и литеральный ноль — пропуск точки (стандарт §6.7:
// ноль и дефис в источнике означают «данных не имеется», а не 0 % годовых); строка сноски,
// начинающаяся с «*» и стоящая после данных, завершает разбор, любая другая неразобранная
// метка — ошибка: это смена структуры листа (§6.4), а не конец данных.
func cbrIntRateLoansIndImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows(cbrIntRateLoansIndSheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист «%s»: %w", cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, err)
	}
	names, base, firstDataRow, err := cbrIntRateLoansIndColumns(rows)
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
		date, err := cbrIntRateLoansIndMonthDate(label)
		if err != nil {
			// Сноска источника начинается с «*» и стоит после данных — разбор закончен. Любая
			// другая неразобранная метка означает смену структуры листа: ошибка, а не «0 rows»,
			// иначе шаг DAG завершился бы успешно, а 16 рядов остались бы без обновления молча.
			if strings.HasPrefix(label, "*") {
				break
			}
			return fmt.Errorf("%s: лист «%s»: %w", cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, err)
		}
		for i, name := range names {
			// Колонка данных i — ячейка base+i строки: база (первая колонка данных) выведена из
			// шапки в cbrIntRateLoansIndColumns, а нулевая ячейка занята меткой периода. Жёсткое i+1
			// читало бы соседние колонки, если источник вставит колонку перед первой группой.
			column := base + i
			// Строка короче набора колонок данных — у остальных колонок ячеек нет; недостающие
			// ячейки равносильны пустым и точками не становятся (§6.7).
			if column >= len(row) {
				break
			}
			cell := strings.TrimSpace(row[column])
			// Пустая ячейка и литеральный «-» — «данных не имеется» (стандарт §6.7): точкой
			// ряда они не становятся. Дефис в источнике именно литерал, а не число.
			if cell == "" || cell == "-" {
				continue
			}
			rate, err := strconv.ParseFloat(cell, 64)
			if err != nil {
				return fmt.Errorf("%s: лист «%s», метка %q, ряд «%s»: значение %q: %w",
					cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, label, name, cell, err)
			}
			// «NaN» и «Inf» из ячейки ParseFloat принимает, но ставкой они не являются: записанные
			// в колонку Float64, они отравили бы ряд (сравнения с NaN ложны, суммы — NaN). Ошибка та
			// же, что у нечисловой ячейки: смена структуры листа (§6.4), а не точка.
			if math.IsNaN(rate) || math.IsInf(rate, 0) {
				return fmt.Errorf("%s: лист «%s», метка %q, ряд «%s»: значение %q не конечно (NaN/Inf)",
					cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, label, name, cell)
			}
			// Литеральный 0 в источнике означает «данных не имеется» (стандарт §6.7) и точкой ряда
			// не становится — так же, как пустая ячейка: нулевой ставки по выданным кредитам не
			// бывает, а 0 % годовых агент прочитал бы как настоящее значение.
			if rate == 0 {
				continue
			}
			if err = batch.Append(name, date, rate); err != nil {
				return fmt.Errorf("%s: метка %q, ряд %q, %s: %w",
					cbrIntRateLoansIndTable, label, name, date.Format("2006-01-02"), err)
			}
			count++
		}
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор листа «%s» не дал ни одной точки", cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet)
	}
	return nil
}

// cbrIntRateLoansInd — средневзвешенные ставки по кредитам физлицам в рублях по срокам: таблица,
// DDL и разбор листа «ставки_руб»; каталог рядов и витрину v_cbr_macro публикует шаблон через
// AfterImport. Сломанная шапка, пустой/короткий лист и ноль точек — ошибка (fail-fast): «0 rows»
// выглядели бы успешным прогоном, а шестнадцать рядов остались бы без обновления молча.
var cbrIntRateLoansInd = util.ClickHouseImport{
	TableName: cbrIntRateLoansIndTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, rate Float64
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`},
	DataUrl:                cbrIntRateLoansIndDataUrl,
	ImportFunc:             cbrIntRateLoansIndImport,
	AfterImport:            afterCbrMacro(cbrIntRateLoansIndSeriesMeta),
	PublicationCountPoints: cbrIntRateLoansIndCountPoints,
}

// cbrIntRateLoansIndImporter — ставки по кредитам физлицам; регистрируется реестром
// cbr/importers.go.
var cbrIntRateLoansIndImporter = &cbrIntRateLoansInd
