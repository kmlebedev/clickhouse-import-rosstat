package bank

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const (

	// Ипотека: ставка предложения топ-20 ипотечных банков по данным ДОМ.РФ. Импортёр читает блок
	// «Топ ипотечных банков» раздела аналитики (спека §3.2) — месячный снимок: его книга даёт
	// ставку каждого банка за отчётный месяц. Ссылка на файл берётся каждый прогон (имя файла
	// несёт отчётный месяц и меняется), поэтому статичного адреса в коде нет: файл прежнего
	// литерала (…/upload/iblock/aba/i34…xlsx) отдаёт 404.
	//
	// Источник ссылки — JSON-список раздела «Топ ипотечных банков» (section=838). HTML-страница
	// аналитики несёт те же блоки атрибутами main#app, но закрыта антиботом ServicePipe: на часть
	// запросов она отдаёт HTTP 200 с JS-оболочкой вместо HTML, и повторы в том же окне не помогают.
	// JSON-API того же раздела отдаёт props.file2.src без антибота, а сам файл и HEAD доступны
	// общим клиентом проекта.

	// domrfListUrl — JSON-список раздела «Топ ипотечных банков»: section=838 — id раздела,
	// include=Y включает вложенные props с файлами. Шаг транспорта делает Import() (util.GetJSON) —
	// разбор обязан оставаться чистым (§6.1).
	domrfListUrl = "https://xn--d1aqf.xn--p1ai/api/v2/content/analytics/list/?section=838&include=Y"
	// domrfDataUrlPrefix — домен источника: абсолютный адрес файла = домен + src из props.file2
	// (§5.4 шаг 3). Литерал, а не domrfListUrl: тест сверяет сборку адреса с литералом.
	domrfDataUrlPrefix = "https://xn--d1aqf.xn--p1ai"
	// domrfBlockAnchor — заголовок блока (props.title в элементе списка), из которого берётся
	// файл (props.file2.src). Признак — заголовок, а не имя блока («…Август 2026 г.»): имя несёт
	// отчётный месяц и меняется с каждым файлом.
	domrfBlockAnchor = "Топ ипотечных банков"
	// domrfSheetAnchor — фрагмент имени листа книги, который разбирает импортёр: «13 Ставки
	// предложения». Лист ищется по имени, а не по номеру: добавленный источником лист сдвинул
	// бы номер (§6.3).
	domrfSheetAnchor = "Ставки предложения"
	// domrfBankAnchor — ячейка шапки листа с именами банков: её колонка — колонка имени ряда.
	domrfBankAnchor = "Банк"
	// domrfProductAnchor — продукт, ставка которого идёт в ряд банка: «Новостройки» —
	// первичный рынок, тот же продукт читала и прежняя семантика таблицы. Ставки остальных пяти
	// продуктов листа (рефинансирование, готовые квартиры, готовые жилые дома, ИЖС, семейная
	// ипотека) в таблицу не идут: ключ таблицы (name, date) хранит одну ставку банка за отчётный
	// месяц, и вторая ставка того же банка затёрла бы первую.
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

	// domrfMortgageTable — имя таблицы импортёра: TableName шаблона, его Name() и ключ версии
	// точки контроля (PublicationKey). На неё опирается витрина v_bank_macro (Tables —
	// константами пакета).
	domrfMortgageTable = "domrf_mortgage"
	// domrfMortgageDdl — DDL в %s-форме (стандарт §5.1): имя таблицы подставляет шаблон —
	// fmt.Sprintf(sql, TableName) в util.ClickHouseImport.Import. Типы колонок — legacy-схема
	// таблицы (ARCHITECTURE.md §6.2): name — банк источника, date — отчётная дата месяца,
	// rate — сырая доля Float32 (0.187 = 18,7 % годовых).
	domrfMortgageDdl = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, rate Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	// domrfCountPoints — количественный чек точки контроля публикации (спека §3.2): число точек
	// своей таблицы. Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL
	// обязателен — без него count() считает несмёрженные части и растёт после каждого повторного
	// импорта. Файл месячный при недельном шаге, поэтому известная версия принимается no-op'ом
	// после HEAD, без скачивания книги.
	domrfCountPoints = "SELECT count() FROM domrf_mortgage FINAL"
)

// domrfPoint — точка разбора: имя ряда (банк источника), отчётная дата листа и ставка в долях
// (0.187 = 18,7 % годовых; колонка rate таблицы — Float32, приведение делает запись).
type domrfPoint struct {
	Series string
	Date   time.Time
	Value  float64
}

// domrfListResponse — ответ JSON-списка раздела «Топ ипотечных банков» (section=838): элементы
// раздела в порядке от новых к старым. API отдаёт тот же блок, что лежит атрибутом HTML-страницы,
// но без антибота ServicePipe; props.file2 — XLSX-приложение блока (у «Ипотеки на ИЖС» его нет).
type domrfListResponse struct {
	Success bool            `json:"success"`
	Data    []domrfListItem `json:"data"`
}

// domrfListItem — элемент списка: id, имя выпуска и props с файлами.
type domrfListItem struct {
	ID    string         `json:"id"`
	Name  string         `json:"name"`
	Props domrfListProps `json:"props"`
}

// domrfListProps — поля блока: заголовок (признак ряда), период и file2 (книга). Остальные поля
// ответа (file — PDF, category, link_to_report) разбору не нужны.
type domrfListProps struct {
	Title  string    `json:"title"`
	Period string    `json:"period"`
	File2  domrfFile `json:"file2"`
}

// domrfFile — файл блока: src — путь скачивания, extension — формат, name — имя файла источника.
type domrfFile struct {
	Src       string `json:"src"`
	Extension string `json:"extension"`
	Name      string `json:"name"`
	Date      string `json:"date"`
}

// domrfXlsSrcFromList отдаёт src XLSX-файла текущего блока «Топ ипотечных банков» из JSON-списка
// раздела. Разбор чистый (§6.1): список качает вызывающий (Import импортёра, util.GetJSON),
// поэтому функция проверяется офлайн на фикстуре ответа API (§11.3).
//
// Блок ищется по заголовку props.title: подпись «Топ ипотечных банков» есть и у пунктов меню, но
// в списке меню нет. Признак — заголовок, а не имя блока: имя несёт отчётный месяц («Топ
// ипотечных банков Август 2026 г.») и меняется с каждым файлом. Берётся первый (самый новый)
// блок: список отсортирован от новых к старым, и текущий отчёт — его первый элемент.
// file2 — приложение блока, и оно обязано быть XLSX: PDF лежит в file и книгой не является.
// Пустой src — ошибка: без неё импортёр пошёл бы в GetXlsx("") и упал бы разбором книги, в
// котором не видно причины (§4.4, §6.4). Ошибки называют список и блок, а не таблицу: префикс
// «domrf_mortgage:» добавляет Import() — один раз на всю ветку списка.
func domrfXlsSrcFromList(list domrfListResponse) (src string, err error) {
	if !list.Success {
		return "", fmt.Errorf("список %s: success=false", domrfListUrl)
	}
	for _, item := range list.Data {
		if strings.TrimSpace(item.Props.Title) != domrfBlockAnchor {
			continue
		}
		src = strings.TrimSpace(item.Props.File2.Src)
		if src == "" {
			return "", fmt.Errorf("список %s: у блока «%s» (%s) нет файла (пустое поле props.file2.src)",
				domrfListUrl, domrfBlockAnchor, item.Name)
		}
		if item.Props.File2.Extension != "xlsx" {
			return "", fmt.Errorf("список %s: у блока «%s» (%s) файл не xlsx: %q",
				domrfListUrl, domrfBlockAnchor, item.Name, item.Props.File2.Extension)
		}
		return src, nil
	}
	return "", fmt.Errorf("список %s: нет блока «%s»", domrfListUrl, domrfBlockAnchor)
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

// DomrfMortgageStat — импортёр domrf_mortgage: ставка предложения по ипотеке на новостройки
// каждого банка блока «Топ ипотечных банков» раздела аналитики ДОМ.РФ за отчётный месяц (лист
// «13 Ставки предложения» книги блока). Источник — месячный снимок: отчётная дата записана
// строкой в заголовке листа («31.08.2026», одна на всю книгу), значение — сырая доля (0,187 =
// 18,7 % годовых), ряд — банк источника. В таблицу идёт только продукт «Новостройки» —
// историческая семантика таблицы; остальные пять продуктов листа этим пунктом не импортируются
// (почему — в domrfProductAnchor). Своей Name() нет: шаблон отдаёт TableName.
//
// Смена семантики источника: прежний импортёр читал документ «Динамика ставок в разрезе месяцев»
// того же раздела. Документ жив по новому адресу, но отвечает на другой вопрос — это недельные
// рыночные средние по продуктам (ряд — продукт, дата — неделя), а не месячная ставка предложения
// банка. У таблицы теперь ряд — банк, дата — отчётный месяц; смена названа, чтобы читатель не
// ждал продолжения прежней раскладки.
//
// Транспорт — переопределённый Import() (образец cbr/mo_br.go): JSON-список раздела → ссылка из
// props.file2 → локальная копия шаблона с DataUrl. Разбор — parseDomrfSheet, запись —
// appendDomrfPoints (ImportFunc шаблона); точка контроля публикации объявлена в domrfImporter
// (domrfCountPoints).
type DomrfMortgageStat struct {
	util.ClickHouseImport
}

// Import — транспорт импортёра (стандарт §5.4): список раздела → ссылка → локальная копия шаблона.
// Источник ссылки — JSON-API, а не HTML-страница: страница аналитики закрыта антиботом ServicePipe
// и на часть запросов отдаёт JS-оболочку вместо HTML, а тот же раздел в API список отдаёт
// props.file2.src без антибота; сам файл и HEAD доступны общим клиентом проекта. Ссылка
// вычисляется каждый прогон (имя файла несёт отчётный месяц), поэтому DataUrl живёт только в
// копии: общую структуру импортёра мутировать нельзя — её читают тест реестра и объявление точки
// контроля, а префлайт шаблона видит вычисленный адрес копии.
func (s *DomrfMortgageStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	var list domrfListResponse
	if err = util.GetJSON(domrfListUrl, &list); err != nil {
		return count, fmt.Errorf("%s: %s: %w", domrfMortgageTable, domrfListUrl, err)
	}
	src, err := domrfXlsSrcFromList(list)
	if err != nil {
		return count, fmt.Errorf("%s: %w", domrfMortgageTable, err)
	}
	imp := s.ClickHouseImport // локальная копия: общую структуру импортёра мутировать нельзя
	imp.DataUrl = domrfDataUrlPrefix + src
	return imp.Import(ctx, conn)
}

// domrfImportFunc — ImportFunc шаблона: книга из памяти → точки → батч. Ошибки разбора и записи
// возвращаются без обёртки: имя таблицы называют сами parseDomrfSheet и appendDomrfPoints (§6.4),
// и второй префикс назвал бы её дважды.
func domrfImportFunc(xlsx *excelize.File, batch driver.Batch) error {
	points, err := parseDomrfSheet(xlsx)
	if err != nil {
		return err
	}
	return appendDomrfPoints(batch, points)
}

// domrfImporter — импортёр для шаблона и реестра: объявление точки контроля публикации
// (domrfCountPoints) стоит в литерале, а не в Import(), — его читает тест реестра у
// зарегистрированного импортёра, а префлайт шаблона включается самим полем. DataUrl в литерале
// нет: он вычисляется в Import() и живёт в локальной копии, поэтому Publication().Url пуст (см.
// util.PublicationDecl). AfterImport — общий каталог рядов пакета и витрины (afterBankMacro).
// Регистрацию держит реестр пакета (bank/importers.go) — единственная точка append в chimport.Stats.
var domrfImporter = &DomrfMortgageStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              domrfMortgageTable,
	CreateTable:            []string{domrfMortgageDdl},
	ImportFunc:             domrfImportFunc,
	PublicationCountPoints: domrfCountPoints,
	AfterImport:            afterBankMacro(bankSeriesMeta),
}}
