package bank

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (
	// https://www.sberbank.com/ru/investor-relations/groupresults/navigator/rezultaty-rpbu
	// Страница источника отдаётся через JS-challenge (TSPD) и статичных ссылок не содержит:
	// ссылку меняем вручную при обновлении, либо нужен headless-браузер. Адрес файла при этом
	// статичен, поэтому импортёр идёт шаблонным путём (DataUrl + ImportFunc): байты качает шаблон,
	// и тот же срез книги уходит в разбор и в винтажный архив версии (стандарт §9.6). Прежний
	// CrawFunc качал файл сам — шаблон его байтов не видел, и версии этого импортёра в архиве не
	// было (документированное исключение §9.6, теперь снято).
	sberRpbuUrl          = "https://www.sberbank.com/common/img/uploaded/redirected/com/investor-relations/groupresults"
	sberRpbuXlsDataUrl   = sberRpbuUrl + "/sber_finansovie_rezultaty_ras_2022-2024.xlsx"
	sberRpbuDataUrlTable = "sber_finansovie_rezultaty"
	sberRpbuDataUrlDdl   = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	// sberRpbuCountPoints — количественный чек версии (спека §3.5): число точек своего ряда.
	// Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL обязателен —
	// без него count() считает несмёрженные части и растёт после каждого повторного импорта.
	sberRpbuCountPoints = "SELECT count() FROM sber_finansovie_rezultaty FINAL"
)

// Константы разбора книги. Разведка живого файла 2026-10-17 (56 947 Б): четыре листа —
// «Баланс», «Отчет о прибылях и убытках», «Капитал», «Показатели»; фикстура —
// bank/testdata/sber_finansovie_rezultaty.xlsx (коммитится как есть, §11.2).
const (
	// sberFinBalanceSheet — лист баланса: 13 рядов-остатков (млрд ₽) со строки 3 до строки 17
	// источника; это прежний разбор — те же 13 имён и 43 колонки-даты, что у базовой линии
	// таблицы (2026-10-16: 559 строк, 2022-12-31…2026-06-30).
	sberFinBalanceSheet = "Баланс"
	// sberFinPnlSheet — лист отчёта о прибылях и убытках: 7 статей (млрд ₽) за месяцы
	// «янв23»…«июн26»; из него словарь метрик берёт net_profit («Прибыль за период»).
	sberFinPnlSheet = "Отчет о прибылях и убытках"
	// sberFinCapitalSheet — лист капитала: строки капитала (млрд ₽) и нормативы достаточности
	// Н1.1/Н1.2/Н1.0 (доли: 0.146 = 14,6 %) за те же 43 колонки-месяца, что у баланса.
	sberFinCapitalSheet = "Капитал"
	// sberFinRatiosSheet — лист показателей: рентабельность капитала (roe) и активов (roa),
	// доля просроченной задолженности и отношение расходов к доходам (доли). Ниже месячного блока
	// на листе лежит блок накопительных значений («1М23»…«6М26») — в точки он не идёт
	// (почему — parseSberFinSheet).
	sberFinRatiosSheet = "Показатели"
	// sberFinLabelAnchor — подпись шапки листа (ячейка «Показатель, млрд ₽» / «Показатель»):
	// по ней шапка ищется в строках, а не по номеру строки (§6.3) — добавленная источником строка
	// над шапкой сдвинула бы номер.
	sberFinLabelAnchor = "Показатель"
	// sberFinLabelColumn — колонка B: подпись ряда в шапках и строках данных всех четырёх листов.
	sberFinLabelColumn = 1
	// sberFinTimeLayout — формат дат книги: колонки-остатки листов «Баланс» и «Капитал» имеют
	// числовой формат mm-dd-yy (Go-layout «01-02-06»), как и у прежнего разбора. Читаются они при
	// этом сырыми серийными номерами Excel (§6.2) и переводятся excelize.ExcelDateToTime; подписи
	// месячных колонок («янв23») — месяц-строка, для которой Go-layout не строится
	// (sberFinMonthNames).
	sberFinTimeLayout = "01-02-06"
	// sberFinMinYear…sberFinMaxYear — окно лет серийного номера даты: доли и проценты (0.13, 0.241)
	// дают 1900 год и датами не прикидываются (идиома cbr/mo_br_parse.go, как в vtbSerialColumns).
	sberFinMinYear = 2000
	sberFinMaxYear = 2100
)

// sberFinMonthRe — месячная подпись колонки листов P&L и показателей: «янв23» (три строчные
// буквы месяца и две цифры года без разделителя). Накопительные подписи («6М25») и колонки-годы
// («2023») этой формой не покрываются.
var sberFinMonthRe = regexp.MustCompile(`^([а-яё]{3})(\d{2})$`)

// sberFinCumulativeRe — подпись колонки накопительного блока («1М23»…«6М26», «6М25»): значение
// под ней считается с начала года, а не за месяц.
var sberFinCumulativeRe = regexp.MustCompile(`^\d{1,2}[МM]\d{2}$`)

// sberFinYearRe — подпись колонки-года («2023»–«2025»): годовой итог, а не месячное значение.
var sberFinYearRe = regexp.MustCompile(`^\d{4}$`)

// sberFinMonthNames — русские сокращения месяцев источника («янв», «фев», …) в именительном
// падеже; ключи — в нижнем регистре.
var sberFinMonthNames = map[string]time.Month{
	"янв": time.January,
	"фев": time.February,
	"мар": time.March,
	"апр": time.April,
	"май": time.May,
	"июн": time.June,
	"июл": time.July,
	"авг": time.August,
	"сен": time.September,
	"окт": time.October,
	"ноя": time.November,
	"дек": time.December,
}

// sberFinPoint — точка разбора: имя ряда (подпись строки источника), дата и сырое значение (§6.2).
// Значение — как записано в файле, без умножений и делений: млрд ₽ у баланса, отчёта о прибылях и
// капитала; доли у нормативов достаточности и показателей (0.146 = 14,6 %); к точности
// Float32-колонки таблицы значение приводит запись (appendSberFinPoints).
type sberFinPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

// sberFinColumn — колонка значений: индекс ячейки в строке и дата, которой принадлежат её числа.
type sberFinColumn struct {
	col  int
	date time.Time
}

// sberFinHead — колонки-даты строки-шапки и число подписей накопительного блока в ней.
type sberFinHead struct {
	cols       []sberFinColumn
	cumulative int
}

// parseSberFinBook — разбор книги (импортёр sber_finansovie_rezultaty, отчётность ПАО Сбербанк по
// РСБУ): четыре листа — четырьмя проходами. Разбор чистый (§6.1): на входе открытая книга, ни
// сети, ни БД, поэтому тесты офлайн на живой фикстуре (§11.3); значения — сырые ячейки (§6.2);
// текст ошибки называет таблицу и причину (§6.4). Прежний разбор читал только «Баланс» и
// форматированные ячейки; листы отчёта о прибылях, капитала и показателей добавлены в объём
// импортёра — из их подписей собирается помесячный словарь метрик (§3.7: net_profit, roe, roa,
// Н1.0/Н1.1/Н1.2).
func parseSberFinBook(xlsx *excelize.File) ([]sberFinPoint, error) {
	var points []sberFinPoint
	for _, anchor := range []string{sberFinBalanceSheet, sberFinPnlSheet, sberFinCapitalSheet, sberFinRatiosSheet} {
		part, perr := parseSberFinSheet(xlsx, anchor)
		if perr != nil {
			// Имя таблицы добавляется здесь один раз: ветки parseSberFinSheet называют лист и
			// строку, но не импортёра (контракт §6.4). Второй префикс в sberFinImportFunc повторил
			// бы имя таблицы в тексте ошибки.
			return nil, fmt.Errorf("%s: %w", sberRpbuDataUrlTable, perr)
		}
		points = append(points, part...)
	}
	return points, nil
}

// parseSberFinSheet — проход одного листа книги: шапка (ячейка «Показатель…»), её колонки-даты и
// строки данных под ней. Лист ищется по якорю имени, шапка — по подписи ячейки, колонки — по
// датам её ячеек, а не по номерам строк и колонок (§6.3).
//
// Точками становятся только месячные значения: на листах P&L и показателей шапка несёт, кроме
// месячных подписей, накопительные колонки («6М25»/«6М26») и колонки-годы («2023»–«2025») — они
// агрегаты, и в ключ (name, date) не идут: накопительный итог за тот же месяц затёр бы месячное
// значение, а помесячный словарь метрик (§3.7) читает именно месячные точки (та же конвенция,
// что у листа P&L ВТБ). По той же причине не идёт в точки и блок накопительных значений листа
// «Показатели»: его шапка (строка с подписями «1М23»…«6М26») переключает проход на блок без
// колонок-дат, и строки под ней пропускаются — их значения считаются с начала года, а не за месяц
// (строка 11 источника, «Стоимость кредитного риска…»; метрики словаря §3.7 в этом блоке нет).
//
// Имя ряда — подпись источника дословно, без нормализации: у двух балансовых рядов подпись
// кончается пробелом («Кредитные карты и овердрафты », «Резервы под кредитные убытки »), и
// прежний разбор записал их в таблицу именно так (сверка `make ch-sql`: длина имени — 54 байта,
// то есть с пробелом). Нормализация имени создала бы при повторном импорте второй ряд вместо
// обновления первого (§15.2), поэтому подпись берётся из ячейки как есть.
func parseSberFinSheet(xlsx *excelize.File, anchor string) (points []sberFinPoint, err error) {
	sheet, err := sberFinSheet(xlsx, anchor)
	if err != nil {
		return nil, err
	}
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("лист %q: чтение строк: %w", sheet, err)
	}
	header := -1
	for i, row := range rows {
		if strings.HasPrefix(sberFinCell(row, sberFinLabelColumn), sberFinLabelAnchor) {
			header = i
			break
		}
	}
	if header < 0 {
		return nil, fmt.Errorf("лист %q: не найдена строка шапки (ячейка с подписью %q)", sheet, sberFinLabelAnchor)
	}
	head, err := sberFinHeadRow(sheet, header+1, rows[header], true) // шапка листа: серийные даты и годы
	if err != nil {
		return nil, err
	}
	cols := head.cols
	if len(cols) == 0 {
		return nil, fmt.Errorf("лист %q: в шапке нет колонок с датами", sheet)
	}
	for i := header + 1; i < len(rows); i++ {
		label := sberFinRawCell(rows[i], sberFinLabelColumn)
		if strings.TrimSpace(label) == "" {
			// Строка без подписи ряда: шапка следующего блока («янв23»/«1М23»), служебная строка
			// или значения без подписи — последнее ошибка (§6.4). Серийных дат и колонок-годов в
			// такой строке не бывает (почему — sberFinHeadRow).
			next, err := sberFinHeadRow(sheet, i+1, rows[i], false) // шапка блока: только подписи-тексты
			if err != nil {
				return nil, err
			}
			switch {
			case len(next.cols) > 0:
				cols = next.cols // месячная шапка следующего блока
			case next.cumulative > 0:
				cols = nil // накопительный блок: значения ниже — с начала года, не месячные
			}
			continue
		}
		if len(cols) == 0 {
			continue // строка накопительного блока — не точка (см. докблок)
		}
		for _, c := range cols {
			raw := sberFinCell(rows[i], c.col)
			if raw == "" {
				continue // пустая ячейка — не точка ряда (§6.7)
			}
			value, err := sberFinFloat(raw)
			if err != nil {
				return nil, fmt.Errorf("лист %q, строка %d (%q): %w", sheet, i+1, label, err)
			}
			points = append(points, sberFinPoint{Series: label, Date: c.date, Value: value})
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("лист %q: разбор не дал ни одной точки", sheet)
	}
	return points, nil
}

// sberFinHeadRow — разбор строки-шапки листа: ячейки её колонок значений — даты (серийные номера
// Excel или подписи месяцев), подписи накопительного блока («1М23») или колонки-годы; всё
// остальное — либо значение без подписи ряда (числовая ячейка), либо подпись неизвестной формы.
// И то и другое ошибка с координатой строки (§6.4), а не молчаливый пропуск: источник обновляется
// вручную, и смену разметки надо видеть сразу.
//
// anchored — строка признана шапкой листа (её ячейка подписана «Показатель…»): только в ней законны
// серийные номера дат и колонки-годы («2023»). У безымянной строки-кандидата в шапку блока
// (anchored=false) законны только подписи-тексты («янв23», «1М23»): целое «1113» — не год, а
// значение (первое число остатка строки «Кредитные карты и овердрафты»), и пропустить его как год
// значило бы потерять точку молча; серийный номер в такой строке — та же ловушка (остаток
// «40622.1» млрд ₽ и серийный номер 40622.1 — одно число, год 2011 в окне sberFinMinYear…
// sberFinMaxYear), и принять за него строку значений значило бы потерять её точки без ошибки.
func sberFinHeadRow(sheet string, n int, row []string, anchored bool) (head sberFinHead, err error) {
	for col := sberFinLabelColumn + 1; col < len(row); col++ {
		raw := sberFinCell(row, col)
		if raw == "" {
			continue
		}
		if date, ok := sberFinMonthDate(raw); ok {
			head.cols = append(head.cols, sberFinColumn{col: col, date: date})
			continue
		}
		if anchored {
			if date, ok := sberFinSerialDate(raw); ok {
				head.cols = append(head.cols, sberFinColumn{col: col, date: date})
				continue
			}
		}
		if sberFinCumulativeRe.MatchString(raw) {
			head.cumulative++
			continue
		}
		if anchored && sberFinYearRe.MatchString(raw) {
			continue // колонка-год шапки листа: агрегат, колонкой значений не является
		}
		if _, ferr := strconv.ParseFloat(raw, 64); ferr == nil {
			return head, fmt.Errorf("лист %q, строка %d: значение %q без подписи ряда", sheet, n, raw)
		}
		return head, fmt.Errorf("лист %q, строка %d: подпись колонки %q не распознана", sheet, n, raw)
	}
	return head, nil
}

// sberFinSheet — имя листа книги по якорю (§6.3): лист ищется по вхождению якоря в имя, а не по
// номеру — добавленный источником лист сдвинул бы номер. Ошибка перечисляет листы книги.
func sberFinSheet(xlsx *excelize.File, anchor string) (string, error) {
	sheets := xlsx.GetSheetList()
	for _, name := range sheets {
		if strings.Contains(name, anchor) {
			return name, nil
		}
	}
	return "", fmt.Errorf("в книге нет листа с якорем %q (листы: %s)", anchor, strings.Join(sheets, ", "))
}

// sberFinRawCell — ячейка строки по индексу колонки без правки: подпись ряда берётся из книги
// дословно (хвостовые пробелы подписей — часть имени ряда, см. parseSberFinSheet). Выход за
// границу среза — пустая ячейка, а не паника (§6.5): GetRows обрезает хвостовые пустые ячейки.
func sberFinRawCell(row []string, col int) string {
	if col < 0 || col >= len(row) {
		return ""
	}
	return row[col]
}

// sberFinCell — ячейка строки по индексу колонки без крайних пробелов: так сравниваются ячейки
// шапки и читаются значения. Выход за границу среза — пустая ячейка, а не паника (§6.5).
func sberFinCell(row []string, col int) string {
	return strings.TrimSpace(sberFinRawCell(row, col))
}

// sberFinSerialDate — дата колонки шапки: серийный номер Excel, дающий год в окне
// sberFinMinYear…sberFinMaxYear. Датой считается только номер, проходящий окно: доли и проценты
// (0.13, 0.241) дают 1900 год и отличаются от дат без отдельного признака формата (идиома
// cbr/mo_br_parse.go).
func sberFinSerialDate(raw string) (time.Time, bool) {
	serial, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return time.Time{}, false
	}
	date, err := excelize.ExcelDateToTime(serial, false)
	if err != nil || date.Year() < sberFinMinYear || date.Year() > sberFinMaxYear {
		return time.Time{}, false
	}
	return date, true
}

// sberFinMonthDate — дата месячной подписи колонки («янв23»): последний день месяца. Две цифры
// года читаются как 20xx — таков источник (2023–2026). Незнакомая месячная подпись (три буквы и две
// цифры года) датой не считается и уходит в ошибку «подпись колонки не распознана» — колонка
// значений осталась бы без даты, а её потеря прошла бы молча (§6.4).
func sberFinMonthDate(raw string) (time.Time, bool) {
	m := sberFinMonthRe.FindStringSubmatch(raw)
	if m == nil {
		return time.Time{}, false
	}
	month, known := sberFinMonthNames[m[1]]
	if !known {
		return time.Time{}, false
	}
	year, err := strconv.Atoi(m[2])
	if err != nil {
		return time.Time{}, false
	}
	return sberFinLastDayOfMonth(2000+year, month), true
}

// sberFinLastDayOfMonth — последний день месяца: нулевой день от первого числа следующего месяца.
func sberFinLastDayOfMonth(year int, month time.Month) time.Time {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)
}

// sberFinFloat — число ячейки. «NaN»/«Inf» strconv.ParseFloat принимает без ошибки, но в
// Float32-колонку таблицы такое значение уехало бы порчей, которую видно только в витрине
// (прецедент — domrf_mortgage): это ошибка разбора (§6.4).
func sberFinFloat(raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", raw, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение %q не конечно (NaN/Inf)", raw)
	}
	return value, nil
}

// appendSberFinPoints пишет точки по одному Append на точку с проверкой ошибки (§5.3, §5.5): форма
// строки — (name, date, balance) схемы таблицы, значение приводится к точности Float32-колонки
// (legacy-схема, спека §4.5), ошибка возвращается вызывающему с именем точки. Дата в ошибке —
// в ISO-форме 2006-01-02: формат шапки 01-02-06 («06-30-26») читается неоднозначно, а дата —
// часть контракта ошибки (§6.4).
func appendSberFinPoints(batch driver.Batch, points []sberFinPoint) error {
	for _, p := range points {
		if err := batch.Append(p.Series, p.Date, float32(p.Value)); err != nil {
			return fmt.Errorf("%s: запись точки %q за %s: %w",
				sberRpbuDataUrlTable, p.Series, p.Date.Format("2006-01-02"), err)
		}
	}
	return nil
}

// sberFinImportFunc — ImportFunc шаблона: книга из памяти → точки → батч. Ошибку разбора не
// оборачивает: parseSberFinBook уже называет таблицу, второй префикс повторил бы её имя в тексте
// ошибки.
func sberFinImportFunc(xlsx *excelize.File, batch driver.Batch) error {
	points, err := parseSberFinBook(xlsx)
	if err != nil {
		return err
	}
	return appendSberFinPoints(batch, points)
}

// SberFinRezStat — импортёр sber_finansovie_rezultaty (отчётность ПАО Сбербанк по РСБУ): месячные
// ряды четырёх листов книги — баланс (13 рядов-остатков, млрд ₽), отчёт о прибылях и убытках
// (статьи, млрд ₽), капитал (строки капитала в млрд ₽ и нормативы достаточности Н1.1/Н1.2/Н1.0 в
// долях) и показатели (roe, roa и прочие доли). Дата точки — последний день отчётного месяца
// (серийные колонки Excel на балансе и капитале, месячные подписи «янв23» на P&L и показателях).
// Ссылка на файл статична и обновляется вручную (страница источника закрыта JS-challenge), а
// содержимое меняется реже недельного расписания шага, поэтому у артефакта есть точка контроля
// публикации; транспорт — шаблонный (DataUrl + ImportFunc), и версия файла попадает в винтажный
// архив (§9.6). Своей Name() нет: шаблон отдаёт TableName.
type SberFinRezStat struct {
	util.ClickHouseImport
}

// sberFinRezImporter — импортёр sber_finansovie_rezultaty для шаблона и реестра: таблица, DDL в
// %s-форме, статичный DataUrl, разбор книги (ImportFunc) и объявление точки контроля публикации
// (sberRpbuCountPoints). Производные — общий каталог рядов пакета и его витрины (afterBankMacro):
// каждый прогон публикует каталог целиком, поэтому ряды Сбера по РСБУ описаны у агента и когда
// остальные импортёры расписания в этом прогоне не работали. Регистрацию держит реестр пакета
// (bank/importers.go) — единственная точка append в chimport.Stats.
var sberFinRezImporter = &SberFinRezStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              sberRpbuDataUrlTable,
	CreateTable:            []string{sberRpbuDataUrlDdl},
	DataUrl:                sberRpbuXlsDataUrl,
	ImportFunc:             sberFinImportFunc,
	PublicationCountPoints: sberRpbuCountPoints,
	AfterImport:            afterBankMacro(bankSeriesMeta),
}}
