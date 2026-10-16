package bank

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (

	// Ипотека: ставки предложения топ-20 ипотечных банков по данным ДОМ.РФ. Разведка
	// 2026-10-16: страница аналитики отвечает 200 обычным серверным HTML (127 710 Б), а данные
	// её блоков лежат JSON-ом в атрибутах main#app — прежний комментарий про JS-challenge
	// (ServicePipe) не подтвердился, и ссылку больше не обновляют вручную. Файл прежнего
	// адреса (…/upload/iblock/aba/i34…xlsx) отдаёт 404; документ «Динамику ставок в разрезе
	// месяцев» жив отдельным блоком той же страницы, но импортёр читает блок «Топ ипотечных
	// банков» (спека §3.2): его книга даёт ставку каждого банка за отчётный месяц.

	// domrfAnalyticsUrl — страница аналитики, с которой берётся ссылка на файл; шаг транспорта
	// делает Import() импортёра (util.GetPage) — разбор обязан оставаться чистым (§6.1).
	domrfAnalyticsUrl = "https://xn--d1aqf.xn--p1ai/analytics/mortgage/"
	// domrfDataUrlPrefix — домен источника: абсолютный адрес файла = домен + src из JSON блока
	// (§5.4 шаг 3). Литерал, а не domrfAnalyticsUrl: тест сверяет сборку адреса с литералом.
	domrfDataUrlPrefix = "https://xn--d1aqf.xn--p1ai"
	// domrfPageBlockAnchor — заголовок блока страницы (props.title в JSON блока), из которого
	// берётся файл (file2.src). Признак — заголовок, а не имя блока («…Август 2026 г.»): имя
	// несёт отчётный месяц и меняется с каждым файлом.
	domrfPageBlockAnchor = "Топ ипотечных банков"
	// domrfSheetAnchor — фрагмент имени листа книги, который разбирает импортёр: «13 Ставки
	// предложения». Лист ищется по имени, а не по номеру: добавленный источником лист сдвинул
	// бы номер (§6.3).
	domrfSheetAnchor = "Ставки предложения"
	// domrfBankAnchor — ячейка шапки листа с именами банков: её колонка — колонка имени ряда.
	domrfBankAnchor = "Банк"
	// domrfProductAnchor — продукт, ставка которого идёт в ряд банка: «Новостройки» —
	// первичный рынок, тот же продукт читал прежний разбор (domrfMortgageField «Новостройка»).
	// Ставки остальных продуктов листа (готовые квартиры, готовые жилые дома, ИЖС,
	// рефинансирование, семейная ипотека) в таблицу не идут: ключ таблицы (name, date) хранит
	// одну ставку банка за отчётный месяц, и вторая ставка того же банка затёрла бы первую.
	domrfProductAnchor = "Новостройки"
	// domrfRateAnchor — подшапка колонки ставки («Ставка»); соседняя колонка группы —
	// «∆ к пред, месяцу, п.п.», она разбору не нужна. Подшапка — и признак «ставка лежит в
	// первой колонке группы продукта», и граница шапки: строки банков идут сразу под ней.
	domrfRateAnchor = "Ставка"
	// domrfTotalsAnchor — начало подписи агрегатной строки листа («Средневзвешенная ставка
	// предложения по крупнейшим банкам»): это средняя по банкам, а не банк, поэтому ряд из неё
	// не заводится (спека §3.2: имя ряда — банк источника).
	domrfTotalsAnchor = "Средневзвешенная ставка предложения"
	// domrfNoDataCell — «нет данных» источника: в колонках «∆» это текстовая ячейка «-», а в
	// колонках ставок — литеральный 0, который числовой формат листа печатает как «-».
	domrfNoDataCell = "-"
	// domrfTimeLayout — формат отчётной даты листа: она записана строкой в заголовке
	// («Ставки предложения по ипотеке на 31.08.2026 (последний день отчетного месяца)») —
	// серийного числа Excel в листе нет, конвертировать нечего.
	domrfTimeLayout = "02.01.2006"

	// Прежние константы импортёра: держатся до задачи 2, которая переписывает импортёр на
	// шаблон. Адрес файла — литерал, а не domrfAnalyticsUrl + путь: domrfAnalyticsUrl теперь
	// страница, а сам файл по этому адресу отдаёт 404 (разведка 2026-10-16).
	domrfMortgageXlsDataUrl = "https://xn--d1aqf.xn--p1ai/upload/iblock/aba/i34cz49h4zukhzduo11wi2d361dbwgb4.xlsx"
	domrfMortgageTable      = "domrf_mortgage"
	domrfMortgageDdl        = `CREATE TABLE IF NOT EXISTS ` + domrfMortgageTable + ` (
			  name LowCardinality(String)
			, date Date
			, rate Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	domrfMortgageInsert     = "INSERT INTO " + domrfMortgageTable + " VALUES (?, ?, ?)"
	domrfMortgageField      = "Новостройка"
	domrfMortgageTimeLayout = "01-02-06"
)

// domrfPoint — точка разбора: имя ряда (банк источника), отчётная дата листа и ставка в долях
// (0.187 = 18,7 % годовых; колонка rate таблицы — Float32, приведение делает запись).
type domrfPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

// domrfPageBlock — блок страницы аналитики: ДОМ.РФ кладёт данные блока JSON-ом в атрибут
// элемента (main#app), а не в текст страницы, поэтому блок разбирается json.Unmarshal значения
// атрибута, а не обходом вложенных элементов.
type domrfPageBlock struct {
	Props struct {
		Title string `json:"title"`
		File2 struct {
			Src string `json:"src"`
		} `json:"file2"`
	} `json:"props"`
}

// domrfXlsSrcFromPage отдаёт src XLSX-файла блока «Топ ипотечных банков» со страницы аналитики.
// Разбор чистый (§6.1): страницу скачивает вызывающий (Import импортёра, util.GetPage), поэтому
// функция проверяется офлайн на HTML-фикстуре (§11.3).
//
// Блок ищется по заголовку своего JSON (props.title): сама подпись «Топ ипотечных банков» есть
// и в меню страницы, а у меню нет ни props, ни файла. Признак — заголовок, а не имя блока: имя
// несёт отчётный месяц («Топ ипотечных банков Август 2026 г.») и меняется с каждым файлом.
// Пустой src — ошибка: без неё импортёр пошёл бы в GetXlsx("") и упал бы разбором книги, в
// котором не видно причины (§4.4, §6.4).
func domrfXlsSrcFromPage(page []byte) (src string, err error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return "", fmt.Errorf("domrf_mortgage: страница %s: не разбирается как HTML: %w", domrfAnalyticsUrl, err)
	}
	var block domrfPageBlock
	found := false
	doc.Find("*").EachWithBreak(func(_ int, el *goquery.Selection) bool {
		if len(el.Nodes) == 0 {
			return true
		}
		for _, attr := range el.Nodes[0].Attr {
			if !strings.Contains(attr.Val, domrfPageBlockAnchor) {
				continue
			}
			var candidate domrfPageBlock
			if json.Unmarshal([]byte(attr.Val), &candidate) != nil {
				continue
			}
			if strings.TrimSpace(candidate.Props.Title) != domrfPageBlockAnchor {
				continue
			}
			block, found = candidate, true
			return false
		}
		return true
	})
	if !found {
		return "", fmt.Errorf("domrf_mortgage: страница %s: не найден блок «%s» (JSON блока в атрибутах страницы)",
			domrfAnalyticsUrl, domrfPageBlockAnchor)
	}
	src = strings.TrimSpace(block.Props.File2.Src)
	if src == "" {
		return "", fmt.Errorf("domrf_mortgage: страница %s: у блока «%s» нет файла (пустое поле file2.src)",
			domrfAnalyticsUrl, domrfPageBlockAnchor)
	}
	return src, nil
}

// domrfSheet — имя листа книги по якорю в имени (§6.3). Ошибка называет таблицу и якорь, а не
// отдаёт «0 точек» (§6.4).
func domrfSheet(xlsx *excelize.File) (string, error) {
	sheets := xlsx.GetSheetList()
	for _, name := range sheets {
		if strings.Contains(name, domrfSheetAnchor) {
			return name, nil
		}
	}
	return "", fmt.Errorf("%s: в книге нет листа с якорем %q (листы: %s)",
		domrfMortgageTable, domrfSheetAnchor, strings.Join(sheets, ", "))
}

// domrfColIndex — индекс колонки ячейки с подписью anchor в строке; -1 — подписи нет.
func domrfColIndex(row []string, anchor string) int {
	for i, cell := range row {
		if strings.TrimSpace(cell) == anchor {
			return i
		}
	}
	return -1
}

// domrfCell — ячейка строки по индексу колонки: выход за границу среза — пустая ячейка, а не
// паника (§6.5). GetRows обрезает хвостовые пустые ячейки, и короткая строка — обычное дело.
func domrfCell(row []string, col int) string {
	if col < 0 || col >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[col])
}

// domrfReportDate — отчётная дата листа из строк заголовка над шапкой таблицы. Ячейка заголовка
// — предложение («Ставки предложения по ипотеке на 31.08.2026 (последний день отчетного
// месяца)»), поэтому дата ищется по словам: признак даты — сам формат domrfTimeLayout, и разбор
// не зависит от формулировки заголовка. Формат строгий (ровно dd.mm.yyyy), поэтому значение
// ячейки данных датой не прикинется.
func domrfReportDate(header [][]string) (time.Time, error) {
	for _, row := range header {
		for _, cell := range row {
			for _, word := range strings.Fields(cell) {
				if date, err := time.Parse(domrfTimeLayout, word); err == nil {
					return date, nil
				}
			}
		}
	}
	return time.Time{}, fmt.Errorf("в заголовке листа нет даты формата %q: отчётный месяц не найден", domrfTimeLayout)
}

// parseDomrfSheet разбирает лист «13 Ставки предложения» книги блока «Топ ипотечных банков»:
// строки банков (шапка — «Банк»/«Новостройки», подшапка — «Ставка») и их ставку за отчётный
// месяц. Разбор чистый (§6.1) и офлайн-тестируемый (§11.3), значения — сырые ячейки (§6.2),
// ошибка вместо «0 точек» на каждой сломанной ветке (§6.4).
//
// Ставка лежит в колонке продукта, но не в соседней колонке «∆»: ставка — первая колонка
// группы продукта, и это проверяется подшапкой «Ставка» — иначе в rate ушло бы изменение.
// Пустая ячейка и «нет данных» источника (литеральный 0, который числовой формат листа печатает
// как «-») — не точка ряда (§6.7), а агрегатная строка по банкам — не ряд (спека §3.2: имя ряда
// — банк источника).
func parseDomrfSheet(xlsx *excelize.File) (points []domrfPoint, err error) {
	sheet, err := domrfSheet(xlsx)
	if err != nil {
		return nil, err
	}
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("%s: лист %q: чтение строк: %w", domrfMortgageTable, sheet, err)
	}

	header, bankCol, productCol := -1, -1, -1
	for i, row := range rows {
		col := domrfColIndex(row, domrfBankAnchor)
		if col < 0 {
			continue
		}
		if product := domrfColIndex(row, domrfProductAnchor); product >= 0 {
			header, bankCol, productCol = i, col, product
			break
		}
	}
	if header < 0 {
		return nil, fmt.Errorf("%s: лист %q: не найдена шапка таблицы (%q и %q)",
			domrfMortgageTable, sheet, domrfBankAnchor, domrfProductAnchor)
	}
	date, err := domrfReportDate(rows[:header])
	if err != nil {
		return nil, fmt.Errorf("%s: лист %q: %w", domrfMortgageTable, sheet, err)
	}
	// Строки банков начинаются под подшапкой ставки: выше — шапка продукта и её подшапка.
	rateRow := -1
	for i := header + 1; i < len(rows); i++ {
		if domrfCell(rows[i], productCol) == domrfRateAnchor {
			rateRow = i
			break
		}
	}
	if rateRow < 0 {
		return nil, fmt.Errorf("%s: лист %q: в колонке продукта %q не найдена подшапка %q",
			domrfMortgageTable, sheet, domrfProductAnchor, domrfRateAnchor)
	}

	for i := rateRow + 1; i < len(rows); i++ {
		series := domrfCell(rows[i], bankCol)
		if series == "" {
			break // пустая строка — конец таблицы: ниже идут заметки листа
		}
		if strings.HasPrefix(series, domrfTotalsAnchor) {
			break // средневзвешенная ставка по банкам — агрегат, а не банк
		}
		cell := domrfCell(rows[i], productCol)
		if cell == "" || cell == domrfNoDataCell {
			continue // не точка ряда (§6.7)
		}
		value, err := strconv.ParseFloat(cell, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: лист %q, строка %d (%q): ставка %q не число: %w",
				domrfMortgageTable, sheet, i+1, series, cell, err)
		}
		if value == 0 {
			continue // литеральный 0 источника = «ставки нет» (§6.7)
		}
		// ParseFloat принимает литералы NaN/Inf без ошибки: это порча книги, но её не видно,
		// пока NaN не доедет до ClickHouse и не начнёт портить витрину.
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("%s: лист %q, строка %d (%q): ставка %q не конечна (NaN/Inf)",
				domrfMortgageTable, sheet, i+1, series, cell)
		}
		points = append(points, domrfPoint{Series: series, Date: date, Value: value})
	}
	if len(points) == 0 {
		// Шапка нашлась, а точек нет — для шаблона это молчаливый «0 строк», то есть ровно
		// тот дефект, ради которого заведён пункт (§6.4).
		return nil, fmt.Errorf("%s: лист %q: разбор не дал ни одной точки", domrfMortgageTable, sheet)
	}
	return points, nil
}

// appendDomrfPoints пишет точки по одному Append на точку с проверкой ошибки (§5.3, §5.5):
// значение приводится к точности Float32-колонки rate, как у импортёров Сбера (legacy-схема,
// спека §4.5), а ошибка возвращается вызывающему — проглоченная ошибка Append и была дефектом
// прежнего разбора.
func appendDomrfPoints(batch driver.Batch, points []domrfPoint) error {
	for _, p := range points {
		if err := batch.Append(p.Series, p.Date, float32(p.Value)); err != nil {
			return fmt.Errorf("%s: запись точки %q за %s: %w",
				domrfMortgageTable, p.Series, p.Date.Format(domrfTimeLayout), err)
		}
	}
	return nil
}

type DomrfMortgageStat struct {
}

func (s *DomrfMortgageStat) Name() string {
	return domrfMortgageTable
}

func (s *DomrfMortgageStat) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(domrfMortgageXlsDataUrl); err != nil {
		return nil, err
	}
	defer func() { _ = xlsx.Close() }()
	table = new([][]string)
	var rows [][]string
	if rows, err = xlsx.GetRows(xlsx.GetSheetList()[0]); err != nil {
		return nil, err
	}
	fieldFound := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) < 2 {
			continue
		}
		name := strings.TrimSpace(row[1])
		if name == "" || name == "0" {
			continue
		}
		fmt.Printf("name '%s'\n", name)
		if name == domrfMortgageField {
			fieldFound = i - 1
		}
		if fieldFound == 0 {
			continue
		}
		if len(row) < 3 {
			break
		}
		// Колонки с месяцами и пропуском кварталов
		startColNum := 3
		for j, cell := range row[startColNum:] {
			rate := strings.TrimRight(strings.TrimSpace(cell), "%")
			if rate == "" || rate == "-" {
				continue
			}
			date := rows[fieldFound][j+startColNum]
			if date == "изм. к пред. нед." {
				continue
			}
			fmt.Printf("name %s date %v cell %s\n", name, date, cell)
			if _, err = strconv.ParseFloat(rate, 32); err != nil {
				return nil, err
			}
			//                                name           year
			*table = append(*table, []string{name, date, rate})
		}
	}

	return table, nil
}

func (s *DomrfMortgageStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	if err = conn.Exec(ctx, domrfMortgageDdl); err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, domrfMortgageInsert)
	if err != nil {
		return count, err
	}
	for _, row := range *table {
		date, err := time.Parse(domrfMortgageTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		value, _ := strconv.ParseFloat(row[2], 32)
		if err = batch.Append(row[0], date, float32(value)); err != nil {
			return count, err
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &DomrfMortgageStat{})
}
