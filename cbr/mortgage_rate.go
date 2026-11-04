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

// Средневзвешенная ставка по жилищным (ипотечным) кредитам, предоставленным физическим
// лицам-резидентам в рублях (ЦБ РФ): https://www.cbr.ru/statistics/bank_sector/mortgage/
//
// Источник — файл 02_08_Rates_housing.xlsx, лист «ставка в рублях», строка «РОССИЙСКАЯ
// ФЕДЕРАЦИЯ» (спека §2.2). Риски источника, зафиксированные при разведке:
//   - рядом на той же странице живёт легаси-дубль 02_13_Rates_mortgage.xlsx — те же листы с
//     пересмотренными значениями (январь 2019 по сроку: 206.9 против 206.7). Импортёр жёстко
//     привязан к 02_08: подмена адреса на «свежий» файл — отдельное решение владельца, а не
//     побочный эффект правки ссылки, иначе один ряд молча смешал бы две редакции;
//   - в инвалютных листах книги значения записаны с десятичной запятой («0,00») — вне объёма;
//     в объёме только рублёвый лист ставки, где разделитель — точка. Если бы разбор читал
//     инвалютный лист, ParseFloat упал бы на «0,00»: это и есть граница объёма, а не дефект.
const (
	cbrMortgageRateTable = "cbr_mortgage_rate"
	// cbrMortgageRateSheet — лист источника: средневзвешенная ставка по жилищным кредитам,
	// предоставленным физлицам-резидентам в рублях. Остальные листы книги (срок в руб.,
	// срок и ставка в инвалюте) — вне объёма (спека §2.2/§6).
	cbrMortgageRateSheet = "ставка в рублях"
	// cbrMortgageRateDataUrl — стабильная прямая ссылка на файл (стандарт §4.1, строка
	// «XLSX»): даты в ней нет, файл меняется месяцами, поэтому качает его шаблон — те же байты
	// уходят и в разбор, и в винтажный архив версии (§9.6). Адрес именно 02_08, а не легаси-дубль
	// 02_13: см. комментарий выше.
	cbrMortgageRateDataUrl = "https://www.cbr.ru/vfs/statistics/BankSector/Mortgage/02_08_Rates_housing.xlsx"
	// cbrMortgageRateCountPoints — количественный чек версии (спека §3.5): число точек своей
	// таблицы. Литерал, как и имя таблицы: запрос — часть утверждения «эти точки мои». FINAL
	// обязателен — иначе count() считал бы несмёрженные части.
	cbrMortgageRateCountPoints = "SELECT count() FROM cbr_mortgage_rate FINAL"
	// cbrMortgageRateField — точное значение колонки A строки объёма: единственная строка листа,
	// которую читает импортёр (федеральные округа и субъекты — вне объёма, спека §2.2/§6).
	// Якорь по значению, а не по номеру строки: вставка или удаление строк других территорий не
	// сдвинет разбор, а исчезновение строки РФ (переименование территории, смена регистра) —
	// ошибка с именем таблицы, а не чтение чужого ряда под прежним именем.
	cbrMortgageRateField = "РОССИЙСКАЯ ФЕДЕРАЦИЯ"
	// cbrMortgageRateSeries — имя единственного ряда, объявленное кодом (стандарт §6.8):
	// заголовок источника («Средневзвешенная ставка по жилищным кредитам, предоставленным
	// физическим лицам-резидентам в рублях, %») несёт редакцию и может смениться, а имя ряда
	// меняться не должно. Оно же — ключ series в каталоге и name в таблице, поэтому каталог
	// (cbr/series_meta.go) читает ту же константу, а не повторяет литерал.
	cbrMortgageRateSeries = "Средневзвешенная ставка по жилищным кредитам, РФ"
)

// cbrMortgageRateRowLabel приводит метку территории к сравнимому виду: снимает пробелы по краям
// и заменяет неразрывный пробел ЦБ (в заголовках он встречается) обычным. Регистр не
// нормализуется: «РОССИЙСКАЯ ФЕДЕРАЦИЯ» — точное значение колонки A, и «Российская Федерация»
// в источнике означала бы смену разметки, а не тот же ряд; разбор обязан упасть (§6.4), а не
// угадать строку.
func cbrMortgageRateRowLabel(cell string) string {
	return strings.TrimSpace(strings.ReplaceAll(cell, "\u00a0", " "))
}

// cbrMortgageRateMonthDate превращает метку периода «Январь 2019» в дату точки —
// предпоследний день месяца отчётного периода: time.Date(year, month+1, -1, …). У ЦБ в
// заголовке колонки стоит месяц, а не день; привязка — та же конвенция контура, что у
// cbr_int_rate_loans, cbr_int_rate_loans_ind и cbr_bank_int_rate (спека §3.3), поэтому
// ипотечная ставка сопоставима с остальными рядами по toStartOfMonth(date). Месяц берётся из
// util.MonthsToNum — словарь месяцев один на пакет.
func cbrMortgageRateMonthDate(label string) (time.Time, error) {
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

// cbrMortgageRateDataRow находит строку объёма по точному значению колонки A. Это единственный
// якорь разбора данных: лист широкий (месяцы — колонками), поэтому номер строки РФ выводится из
// значения, а не пишется числом (стандарт §6.3). Не найдена — ошибка: «0 rows» выглядели бы
// успешным прогоном, а ряд остался бы без обновления молча.
func cbrMortgageRateDataRow(rows [][]string) (int, error) {
	for i, row := range rows {
		if len(row) > 0 && cbrMortgageRateRowLabel(row[0]) == cbrMortgageRateField {
			return i, nil
		}
	}
	return -1, fmt.Errorf("%s: в листе «%s» не найдена строка «%s» — ряд РФ не найден (источник изменил структуру листа)",
		cbrMortgageRateTable, cbrMortgageRateSheet, cbrMortgageRateField)
}

// cbrMortgageRateMonths находит строку меток месяцев над строкой данных и разбирает её:
// возвращает даты точек по колонкам и номер первой колонки данных (base). Строка меток —
// ближайшая вверх от строки РФ, все непустые ячейки которой после первой разбираются как
// «Месяц Год» (в живой книге это строка 3: колонка A пуста, B.. — «Январь 2019» …). Поиск вверх,
// а не индекс строки 3, — тот же принцип якоря: добавленная строка подписи между метками и
// данными не сдвинет разбор, а смена формата метки (например, «January 2019») не даст
// «0 rows» — строка не распознается, и прогон упадёт с именем таблицы.
//
// Дыра в середине строки меток — ошибка, а не «пропуск месяца»: метки и значения выровнены по
// колонкам, и пустая метка увела бы значения соседних колонок под чужие даты. Trailing-пустые
// ячейки excelize отрезает сам, поэтому до сюда они не доходят.
func cbrMortgageRateMonths(rows [][]string, dataRow int) (dates []time.Time, base int, err error) {
	for i := dataRow - 1; i >= 0; i-- {
		row := rows[i]
		first := -1
		for j := 1; j < len(row); j++ {
			if strings.TrimSpace(row[j]) != "" {
				first = j
				break
			}
		}
		if first < 0 {
			// Строка без меток правее колонки A — не строка месяцев (заголовок книги или
			// пустая строка): продолжаем поиск выше.
			continue
		}
		dates = make([]time.Time, 0, len(row)-first)
		ok := true
		for _, cell := range row[first:] {
			label := strings.TrimSpace(cell)
			if label == "" {
				ok = false
				break
			}
			date, monthErr := cbrMortgageRateMonthDate(label)
			if monthErr != nil {
				ok = false
				break
			}
			dates = append(dates, date)
		}
		if ok && len(dates) > 0 {
			return dates, first, nil
		}
	}
	return nil, 0, fmt.Errorf("%s: в листе «%s» над строкой «%s» не найдена строка меток месяцев («Январь 2019», …) — шапка источника не распознана",
		cbrMortgageRateTable, cbrMortgageRateSheet, cbrMortgageRateField)
}

// cbrMortgageRateImport разбирает лист «ставка в рублях» (широкий формат, не как у
// cbr_int_rate_loans_ind): месяцы стоят колонками в строке меток, значение ряда РФ — в строке
// «РОССИЙСКАЯ ФЕДЕРАЦИЯ» под своей меткой. Точка — (cbrMortgageRateSeries, предпоследний день
// месяца, ставка), % годовых. Пустая ячейка и литеральный «-» — «данных не имеется» (стандарт
// §6.7): точкой ряда они не становятся; на снимке 2026-11-03 у РФ пропусков нет (93 точки).
func cbrMortgageRateImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows(cbrMortgageRateSheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист «%s»: %w", cbrMortgageRateTable, cbrMortgageRateSheet, err)
	}
	dataRow, err := cbrMortgageRateDataRow(rows)
	if err != nil {
		return err
	}
	dates, base, err := cbrMortgageRateMonths(rows, dataRow)
	if err != nil {
		return err
	}
	row := rows[dataRow]
	count := 0
	for i, date := range dates {
		// Колонка месяца i — ячейка base+i строки РФ: база (первая колонка меток) выведена из
		// шапки в cbrMortgageRateMonths, а левее неё лежит только название территории.
		column := base + i
		// Строка короче строки меток — у остальных месяцев ячеек нет; недостающие ячейки
		// равносильны пустым и точками не становятся (§6.7).
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
			return fmt.Errorf("%s: лист «%s», строка «%s», ряд «%s», месяц %s: значение %q: %w",
				cbrMortgageRateTable, cbrMortgageRateSheet, cbrMortgageRateField, cbrMortgageRateSeries, date.Format("2006-01"), cell, err)
		}
		// «NaN» и «Inf» из ячейки ParseFloat принимает, но ставкой они не являются: записанные
		// в колонку Float64, они отравили бы ряд (сравнения с NaN ложны, суммы — NaN). Ошибка та
		// же, что у нечисловой ячейки: смена структуры листа (§6.4), а не точка.
		if math.IsNaN(rate) || math.IsInf(rate, 0) {
			return fmt.Errorf("%s: лист «%s», строка «%s», ряд «%s», месяц %s: значение %q не конечно (NaN/Inf)",
				cbrMortgageRateTable, cbrMortgageRateSheet, cbrMortgageRateField, cbrMortgageRateSeries, date.Format("2006-01"), cell)
		}
		// Литеральный 0 в источнике означает «данных не имеется» (стандарт §6.7) и точкой ряда
		// не становится — так же, как пустая ячейка: нулевой ставки по выданным кредитам не
		// бывает, а 0 % годовых агент прочитал бы как настоящее значение.
		if rate == 0 {
			continue
		}
		if err = batch.Append(cbrMortgageRateSeries, date, rate); err != nil {
			return fmt.Errorf("%s: строка «%s», ряд «%s», %s: %w",
				cbrMortgageRateTable, cbrMortgageRateField, cbrMortgageRateSeries, date.Format("2006-01-02"), err)
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор листа «%s» не дал ни одной точки", cbrMortgageRateTable, cbrMortgageRateSheet)
	}
	return nil
}

// cbrMortgageRate — средневзвешенная ставка по ипотечным жилищным кредитам физлицам-резидентам
// в рублях по РФ: таблица, DDL и разбор листа «ставка в рублях»; каталог ряда и витрину
// v_cbr_macro публикует шаблон через AfterImport. Сломанная шапка (нет строки РФ, нет меток
// месяцев), пустой/короткий лист и ноль точек — ошибка (fail-fast): «0 rows» выглядели бы
// успешным прогоном, а ряд остался бы без обновления молча.
var cbrMortgageRate = util.ClickHouseImport{
	TableName: cbrMortgageRateTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, rate Float64
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`},
	DataUrl:                cbrMortgageRateDataUrl,
	ImportFunc:             cbrMortgageRateImport,
	AfterImport:            afterCbrMacro(cbrMortgageRateSeriesMeta),
	PublicationCountPoints: cbrMortgageRateCountPoints,
}

// cbrMortgageRateImporter — ипотечная ставка РФ; регистрируется реестром cbr/importers.go.
var cbrMortgageRateImporter = &cbrMortgageRate
