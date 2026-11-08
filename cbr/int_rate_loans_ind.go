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
// физическим лицам в рублях (ЦБ РФ): https://www.cbr.ru/statistics/bank_sector/int_rat/

const (
	cbrIntRateLoansIndTable = "cbr_int_rate_loans_ind"
	cbrIntRateLoansIndSheet = "ставки_руб"
	// cbrIntRateLoansIndDataUrl — стабильная прямая ссылка на файл (стандарт §4.1, строка
	// «XLSX»): даты в ней нет, файл меняется месяцами, поэтому качает его шаблон — те же байты
	// уходят и в разбор, и в винтажный архив версии (§9.6). ЦБ перенёс раздел: страница
	// /statistics/pdko/int_rat/ отдаёт 404, рабочая — /statistics/bank_sector/int_rat/, и
	// выгрузка ставок по кредитам физлицам называется на ней loans_ind_new.xlsx. Прежний
	// loans_ind.xlsx жив, но заморожен на 2025-12 (рубрика «Ретроспективная информация»): он
	// остаётся origin восьми рядов автокредитов по срокам, публикацию которых источник прекратил
	// (сноска «***» листа).
	cbrIntRateLoansIndDataUrl = "https://www.cbr.ru/vfs/statistics/pdko/int_rat/loans_ind_new.xlsx"
	// cbrIntRateLoansIndCountPoints — количественный чек версии (спека §3.5): число точек своей
	// таблицы. Литерал, как и имя таблицы: запрос — часть утверждения «эти точки мои». FINAL
	// обязателен — иначе count() считал бы несмёрженные части. Счётчик считает всю таблицу,
	// включая замороженные ряды автокредитов по срокам (их источник больше не публикует, но
	// точки в таблице остались), поэтому на снимке 2026-11-08 это 2 464, а не 1 520 нового файла.
	cbrIntRateLoansIndCountPoints = "SELECT count() FROM cbr_int_rate_loans_ind FINAL"
	// cbrIntRateLoansIndHeaderAnchor — подпись шапки источника в первой колонке («% годовых»).
	// От неё отсчитываются строка групп и строка заголовков колонок: магических индексов строк
	// в разборе нет, номер шапки выводится из якоря (стандарт §6.3).
	cbrIntRateLoansIndHeaderAnchor = "% годовых"
	// cbrIntRateLoansIndTermGroup и cbrIntRateLoansIndAggregateGroup — подписи двух групп
	// в строке групп источника. Группа — позиционный якорь колонок, а не совокупность: в группе
	// «по всем срокам» лежат две разные совокупности (все кредиты физлицам и подгруппа
	// автокредитов), поэтому префикс имени ряда живёт в раскладке колонки, а не в группе.
	cbrIntRateLoansIndTermGroup      = "в разрезе сроков"
	cbrIntRateLoansIndAggregateGroup = "по всем срокам"
	// cbrIntRateLoansIndAllPrefix — префикс имён сроковых рядов: «Всего (кредиты физлицам):
	// <срок>». Под префиксом «Всего: » уже живут восемь рядов cbr_int_rate_loans (кредиты
	// юрлицам), а series_catalog ключуется по (source='cbr', series): одинаковые имена двух
	// импортёров вытеснили бы описания друг друга (v_series_catalog — FINAL), а в v_cbr_macro
	// ряды двух разных совокупностей стали бы неразличимы по series.
	cbrIntRateLoansIndAllPrefix = "Всего (кредиты физлицам): "
)

// cbrIntRateLoansIndTerms — восемь сроков кредита в порядке колонок группы «в разрезе сроков».
// Текст срока — единственная запись его написания: он сверяется с заголовком колонки и входит
// в имя ряда. Заголовок источника несёт сноски и переносы и меняется с редакцией файла,
// поэтому именем ряда он быть не может (стандарт §6.8), а расхождение с ожидаемым текстом —
// смена структуры листа (§6.4), а не новый ряд.
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

// cbrIntRateLoansIndAggregate — две колонки группы «по всем срокам»: у агрегата вместо срока
// собственное имя источника, поэтому здесь пара «заголовок × имя ряда». Хвост имени —
// «по всем срокам», а не дословное «все виды кредитов»: ось этой группы — срок, а не тип
// кредита, и для подгруппы автокредитов дословный заголовок читался бы как другая совокупность.
var cbrIntRateLoansIndAggregate = []struct {
	Header string
	Series string
}{
	{Header: "все виды кредитов", Series: cbrIntRateLoansIndAllPrefix + "по всем срокам"},
	{Header: "в том числе автокредиты", Series: "Автокредиты: по всем срокам"},
}

// cbrIntRateLoansIndColumn — колонка листа: имя ряда объявляет код (стандарт §6.8), подпись
// группы в строке групп задаёт позицию колонки в шапке, ожидаемый заголовок — проверку
// «источник не сменил структуру листа» (§6.4). Три факта, а не один: без подписи группы
// позиция колонки не выводится из шапки, а заголовок источника не может быть именем ряда.
type cbrIntRateLoansIndColumn struct {
	Series string
	Group  string
	Header string
}

// cbrIntRateLoansIndLayout — 10 колонок листа в порядке листа: восемь сроков группы «в разрезе
// сроков», затем два агрегата группы «по всем срокам». Собирается из сроков и агрегатов, а не
// переписывается вторым списком: имена и заголовки объявлены по одному разу, и вторая запись
// тех же текстов стала бы вторым источником правды.
var cbrIntRateLoansIndLayout = cbrIntRateLoansIndSheetLayout()

// cbrIntRateLoansIndSheetLayout собирает раскладку колонок: внешний цикл — сроки, затем
// агрегаты, то есть порядок колонок листа.
func cbrIntRateLoansIndSheetLayout() []cbrIntRateLoansIndColumn {
	cols := make([]cbrIntRateLoansIndColumn, 0, len(cbrIntRateLoansIndTerms)+len(cbrIntRateLoansIndAggregate))
	for _, term := range cbrIntRateLoansIndTerms {
		cols = append(cols, cbrIntRateLoansIndColumn{
			Series: cbrIntRateLoansIndAllPrefix + term,
			Group:  cbrIntRateLoansIndTermGroup,
			Header: term,
		})
	}
	for _, agg := range cbrIntRateLoansIndAggregate {
		cols = append(cols, cbrIntRateLoansIndColumn{
			Series: agg.Series,
			Group:  cbrIntRateLoansIndAggregateGroup,
			Header: agg.Header,
		})
	}
	return cols
}

// cbrIntRateLoansIndNames — имена рядов в порядке колонок листа. Список собирается из
// раскладки, а не переписывается вторым списком: имена объявляет код (стандарт §6.8), и
// вторая запись тех же имён стала бы вторым источником правды. Каталог рядов
// (cbr/series_meta.go) читает тот же список.
var cbrIntRateLoansIndNames = cbrIntRateLoansIndSeriesNames()

// cbrIntRateLoansIndSeriesNames собирает имена рядов из раскладки — в порядке колонок листа.
func cbrIntRateLoansIndSeriesNames() []string {
	names := make([]string, 0, len(cbrIntRateLoansIndLayout))
	for _, col := range cbrIntRateLoansIndLayout {
		names = append(names, col.Series)
	}
	return names
}

// cbrIntRateLoansIndGroupLabels — подписи групп в порядке колонок листа, выведенные из
// раскладки: группы в листе идут подряд, поэтому новая подпись — это смена Group у соседних
// колонок. Второго объявления подписей нет: иначе группа, добавленная в раскладку, не попала бы
// в строку групп и шапка «совпала» бы без неё.
var cbrIntRateLoansIndGroupLabels = cbrIntRateLoansIndSheetGroupLabels()

// cbrIntRateLoansIndSheetGroupLabels собирает подписи групп в порядке раскладки.
func cbrIntRateLoansIndSheetGroupLabels() []string {
	var labels []string
	for i, col := range cbrIntRateLoansIndLayout {
		if i == 0 || cbrIntRateLoansIndLayout[i-1].Group != col.Group {
			labels = append(labels, col.Group)
		}
	}
	return labels
}

// cbrIntRateLoansIndGroupOffsets — смещения подписей групп от первой колонки данных: у первой
// группы 0, у каждой следующей — число колонок предыдущих групп. Смещения выводятся из
// раскладки, а при разборе сверяются с шапкой: колонка, вставленная или удалённая внутри листа,
// сдвинула бы группу, и позиционная привязка значений к именам разошлась бы молча.
var cbrIntRateLoansIndGroupOffsets = cbrIntRateLoansIndSheetGroupOffsets()

// cbrIntRateLoansIndSheetGroupOffsets собирает смещения групп из раскладки.
func cbrIntRateLoansIndSheetGroupOffsets() []int {
	var offsets []int
	for i, col := range cbrIntRateLoansIndLayout {
		if i == 0 || cbrIntRateLoansIndLayout[i-1].Group != col.Group {
			offsets = append(offsets, i)
		}
	}
	return offsets
}

// cbrIntRateLoansIndHeaderReplacer — типографика заголовков, не влияющая на смысл колонки:
// кавычки (в xlsx источника срок записан с апострофами по краям цитаты, в спеке — в кавычках
// «…»), сноски-звёздочки (у сроков группы «в разрезе сроков» их две, у автокредитов — три),
// неразрывный пробел ЦБ и дефис переноса слова («до востребова-ния»). Дефис убирается целиком:
// в наборе заголовков он встречается только на месте переноса. Звёздочки снимаются вместе
// с кавычками: без этого заголовок «от 31 до 90 дней**» не совпал бы с ожидаемым сроком и
// валил бы прогон на живой книге.
var cbrIntRateLoansIndHeaderReplacer = strings.NewReplacer(
	"«", "", "»", "", "„", "", "“", "", "”", "", "\"", "", "'", "",
	"*", "", "-", "", "\u00a0", " ",
)

// cbrIntRateLoansIndHeaderKey приводит текст заголовка к сравнимому виду: регистр не значим,
// пробелы (в том числе неразрывные) схлопнуты, кавычки, звёздочки и переносы сняты. Сверка
// заголовка с ожидаемым — это проверка «источник не сменил структуру листа» (стандарт §6.4):
// смена текста означала бы новую редакцию файла, а не новый ряд, и должна валить прогон,
// а не заводить колонку с чужим смыслом под прежним именем.
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
// порядке, объявленном раскладкой. Подписи ищутся последовательно (сначала подпись первой
// группы, затем второй), поэтому неполный набор означает, что строка не та: nil, а не
// частичный список колонок, — разбор работает только с полным набором групп.
func cbrIntRateLoansIndGroupStarts(row []string) []int {
	starts := make([]int, 0, len(cbrIntRateLoansIndGroupLabels))
	for i, cell := range row {
		if len(starts) == len(cbrIntRateLoansIndGroupLabels) {
			break
		}
		if cbrIntRateLoansIndIsGroupLabel(cell, cbrIntRateLoansIndGroupLabels[len(starts)]) {
			starts = append(starts, i)
		}
	}
	if len(starts) != len(cbrIntRateLoansIndGroupLabels) {
		return nil
	}
	return starts
}

// cbrIntRateLoansIndColumns разбирает шапку листа и возвращает имена колонок данных в порядке
// листа, номер первой колонки данных (base — колонка, в которой стоит подпись первой группы)
// и номер первой строки данных — следующей за шапкой. Якоря, а не магические индексы
// (стандарт §6.3): строка «% годовых» даёт начало шапки, в следующей за ней строке ищутся
// подписи групп, строка заголовков колонок — следующая за строкой групп.
//
// Колонки привязываются к раскладке позиционно, но позиция не предполагается: начало каждой
// группы сверяется со смещением из раскладки, а каждый заголовок — с ожидаемым текстом. База
// тоже выводится из шапки: у колонки-метки слева от первой группы позиция в файле не
// фиксирована, а разбор данных считает колонку как base+i — при вставке колонки перед группой
// шапка «совпала» бы, и значения прочитались бы из сдвинутых колонок под правильными именами.
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
			cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, cbrIntRateLoansIndGroupLabels[0], cbrIntRateLoansIndGroupLabels[1])
	}
	// Первая колонка данных — та, в которой стоит подпись первой группы: строка групп и строка
	// заголовков колонок выровнены, и левее неё лежит только метка периода.
	base = starts[0]
	// Позиции групп сверяются с раскладкой до чтения заголовков: конец группы выводится из
	// начала следующей, и сдвиг любой из них увёл бы имена к чужим колонкам.
	for group, start := range starts {
		if offset := cbrIntRateLoansIndGroupOffsets[group]; start-base != offset {
			return nil, 0, 0, fmt.Errorf("%s: лист «%s», группа «%s»: начало в колонке %d при ожидаемом смещении %d от первой колонки данных (%d) — источник изменил структуру листа",
				cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, cbrIntRateLoansIndGroupLabels[group], start, offset, base)
		}
	}
	// Строка заголовков колонок — следующая за строкой групп. Её отсутствие (строка групп
	// последняя) проверяется, а не предполагается: ниже шапка индексируется по строке, и
	// выход за конец слайса был бы паникой (стандарт §6.5).
	headerRow := groupRow + 1
	if headerRow >= len(rows) {
		return nil, 0, 0, fmt.Errorf("%s: в листе «%s» нет строки заголовков колонок под строкой групп",
			cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet)
	}
	// Границы проверяются до индексации: колонка раскладки — base+i, а строка заголовков может
	// оказаться короче раскладки (источник урезал группу агрегатов).
	if base+len(cbrIntRateLoansIndLayout) > len(rows[headerRow]) {
		return nil, 0, 0, fmt.Errorf("%s: лист «%s»: %d колонок данных от колонки %d не укладываются в строку заголовков (%d колонок) — источник изменил структуру листа",
			cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, len(cbrIntRateLoansIndLayout), base, len(rows[headerRow]))
	}
	names = make([]string, 0, len(cbrIntRateLoansIndLayout))
	for i, col := range cbrIntRateLoansIndLayout {
		header := rows[headerRow][base+i]
		if cbrIntRateLoansIndHeaderKey(header) != cbrIntRateLoansIndHeaderKey(col.Header) {
			return nil, 0, 0, fmt.Errorf("%s: лист «%s», группа «%s»: заголовок колонки %q не совпал с ожидаемым %q — источник изменил структуру листа",
				cbrIntRateLoansIndTable, cbrIntRateLoansIndSheet, col.Group, header, col.Header)
		}
		names = append(names, col.Series)
	}
	return names, base, headerRow + 1, nil
}

// cbrIntRateLoansIndMonthDate превращает метку периода «Январь 2014» в дату точки —
// предпоследний день месяца отчётного периода: time.Date(year, month+1, -1, …). У ЦБ в колонке
// метки стоит месяц, а не день; привязка — та же конвенция контура, что у cbr_int_rate_loans и
// cbr_bank_int_rate (спека §3.3, 2014-01-30 … 2026-08-30), поэтому ставки по кредитам физлицам
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
// отчётного периода, 10 колонок — ряды (8 сроков + 2 агрегата «по всем срокам»), значение —
// средневзвешенная ставка, % годовых. Пустая ячейка, литеральный «-» и литеральный ноль —
// пропуск точки (стандарт §6.7: ноль и дефис в источнике означают «данных не имеется», а не
// 0 % годовых): в текущем файле пропусков нет, правила остаются защитой на будущую редакцию.
// Строка сноски, начинающаяся с «*» и стоящая после данных, завершает разбор, любая другая
// неразобранная метка — ошибка: это смена структуры листа (§6.4), а не конец данных.
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
			// иначе шаг DAG завершился бы успешно, а десять рядов остались бы без обновления молча.
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

// cbrIntRateLoansInd — средневзвешенные ставки по кредитам физлицам в рублях: таблица, DDL
// и разбор листа «ставки_руб»; каталог рядов и витрину v_cbr_macro публикует шаблон через
// AfterImport. Сломанная шапка, пустой/короткий лист и ноль точек — ошибка (fail-fast): «0 rows»
// выглядели бы успешным прогоном, а десять рядов остались бы без обновления молча.
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
