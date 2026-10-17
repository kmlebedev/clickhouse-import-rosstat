package bank

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// Файл импортёра tbank_group_ifrs (Т-Банк, МСФО группы): своя таблица и свои константы — развязка
// с общей таблицей ВТБ rus_vtb_group_ifrs (спека §3.5), транспорт шаблонный (DataUrl + ImportFunc),
// разбор переведён на батч механически. Источника-книги нет: текущая ссылка ведёт на PDF, поэтому
// импорт падает на открытии книги до разбора и вставки; фикстуры у разбора нет — он не
// верифицирован, и это названо отступлением О4 спеки (проверяемая реализация разбора — в отдельном
// пункте починки источника). Каталог рядов и витрина для него не заводятся: ряды неизвестны
// (спека §3.5).
const (
	// Отчётность по МСФО лежит на CDN со случайным UUID в имени файла, который меняется
	// с каждым релизом; листинга со ссылками у CDN нет — ссылку меняем вручную при обновлении.
	// Текущая ссылка ведёт на PDF.
	// https://cdn.tbank.ru/static/documents/c4ade295-d683-431f-a7ac-4f88c685d548.xlsx
	tbIfrsUrl   = "https://cdn.tbank.ru/static/documents/a27e2a89-0871-4d87-9847-46bce3a0ad3f.pdf"
	tbIfrsTable = "tbank_group_ifrs"
	// tbIfrsDdl — своя %s-форма (стандарт §5.1), а не vtbIfrsTableDdl: таблица Т-Банка отдельная
	// от общей таблицы ВТБ (спека §3.5), имя подставляет шаблон. Колонка значения — balance
	// Float64: таблица новая, а Float32 остаётся типом legacy-таблиц пакета (правило 5 AGENTS.md).
	tbIfrsDdl = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, balance Float64
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
`
	// tbIfrsSheet — лист книги, который читает разбор (имя — прежнее, как и весь разбор ниже:
	// О4, фикстуры нет).
	tbIfrsSheet = "Interest"
	// tbIfrsDataField — якорь начала данных разбора: подпись строки, над которой лежит шапка
	// колонок-дат (прежняя семантика, 1:1).
	tbIfrsDataField = "Новостройка"
	// tbIfrsTimeLayout — формат дат шапки (он же формат прежних построчных вставок): значение как
	// у ВТБ, но своя константа — после развязки чужой файл не заимствуется.
	tbIfrsTimeLayout = "01-02-06"
)

// TbIfrs — импортёр tbank_group_ifrs (Т-Банк, МСФО группы) на встроенном util.ClickHouseImport:
// шаблон ведёт свою таблицу (DDL в %s-форме, транспорт, запись батчем), своего Import() и Name() у
// импортёра нет — имя отдаёт TableName. Точки контроля публикации нет: ссылка ведёт на PDF и импорт
// падает до разбора — количественный чек объявлять не на чем (спека §3.5).
type TbIfrs struct {
	util.ClickHouseImport
}

// tbIfrsPoint — извлечение точки Т-Банка по индексам строк: значение из строки данных и дата из
// колонки шапки. Хелпер владеет именно извлечением точки: проверяет границы шапки, строки данных и
// колонки, разбирает значение и дату. Поиск якоря и границы строк/шапки остаются стражами цикла
// tbIfrsImportFunc — сюда они не переезжают. Значение — как прежде: запятая удаляется как тысячный
// разделитель; дата разбирается форматом tbIfrsTimeLayout. Ошибки не называют таблицу, лист,
// строку и колонку: этот контекст добавляет вызывающий, иначе он дублировался бы. Границы
// проверяются у шапки, строки данных и колонки (§6.5): выход за срез — ошибка, а не паника.
func tbIfrsPoint(rows [][]string, headerRow, dataRow, col int) (value float64, date time.Time, err error) {
	if headerRow < 0 || headerRow >= len(rows) {
		return 0, time.Time{}, fmt.Errorf("строка шапки вне книги")
	}
	if dataRow < 0 || dataRow >= len(rows) {
		return 0, time.Time{}, fmt.Errorf("строка данных вне книги")
	}
	header := rows[headerRow]
	if col < 0 || col >= len(header) {
		return 0, time.Time{}, fmt.Errorf("колонка вне шапки (%d ячеек)", len(header))
	}
	row := rows[dataRow]
	if col >= len(row) {
		return 0, time.Time{}, fmt.Errorf("колонка вне строки данных (%d ячеек)", len(row))
	}
	balance := strings.ReplaceAll(strings.TrimSpace(row[col]), ",", "")
	value, err = strconv.ParseFloat(balance, 64)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("значение %q не число: %w", balance, err)
	}
	date, err = time.Parse(tbIfrsTimeLayout, header[col])
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("дата колонки %q: %w", header[col], err)
	}
	return value, date, nil
}

// tbIfrsImportFunc — ImportFunc шаблона: книга из памяти → точки → батч. Разбор — механический
// перенос прежней семантики (тот же лист, тот же якорь, те же пропуски строк): источника-книги нет,
// фикстуры нет, поэтому полный разбор книги не верифицирован (отступление О4 спеки); тестами
// покрыты tbIfrsPoint (синтетические строки) и страж ячейки правее шапки (книга в памяти).
// Значение пишется в Float64-колонку новой
// таблицы (спека §3.5): число разбирается как float64, без приведения к точности Float32
// legacy-таблиц. Ошибки называют таблицу, лист, строку и колонку (§6.4). tbIfrsPoint владеет
// извлечением точки: проверяет границы шапки, строки данных и колонки, разбирает значение и дату.
// Поиск якоря и границы шапки остаются в этом цикле: row[2] (якорь), row[3] (страж пустого
// значения) и j+2 с len(rows[fieldFound]) (граница колонки шапки) проверяются здесь, до вызова
// хелпера; стражи коротких строк не пускают в rows индекс шапки -1 (якорь в первой строке книги).
// Непустая ячейка правее шапки дат — ошибка, а не молчаливый обрыв: точка без даты терялась бы
// незаметно (§6.4). Паника в разборе запрещена (§6.5).
func tbIfrsImportFunc(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows(tbIfrsSheet)
	if err != nil {
		return fmt.Errorf("%s: лист %q: чтение строк: %w", tbIfrsTable, tbIfrsSheet, err)
	}
	fieldFound := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) < 3 {
			// Прежний страж (len < 2) не покрывал колонку якоря: строка из двух ячеек падала на
			// row[2] (§6.5), а значений в ней разбор не ищет — точка читается из row[3] и ниже.
			continue
		}
		if strings.TrimSpace(row[2]) == tbIfrsDataField {
			fieldFound = i - 1
		}
		if fieldFound < 1 {
			// 0 — якорь не найден; -1 — якорь в первой строке книги, шапки колонок-дат над ним нет
			// (прежняя проверка «== 0» пускала -1 в rows[-1] — паника, §6.5).
			continue
		}
		if len(row) < 4 {
			// Сюда доходит только строка ровно из трёх ячеек при уже найденном якоре (fieldFound ≥ 1),
			// в которой нет колонки значения row[3], — это конец таблицы, разбор обрывается, как и
			// прежде. Строки из одной-двух ячеек страж len(row) < 3 выше пропускает и до, и после
			// якоря, поэтому обрывать таблицу они не могут.
			break
		}
		if v := strings.TrimSpace(row[3]); v == "" || v == "0" || v == "-" {
			continue // пустое значение, литеральный «0» и «-» источника — не точка ряда
		}
		// Колонки с месяцами и пропуском кварталов
		for j, cell := range row[2:] {
			if strings.TrimSpace(cell) == "" {
				continue // пустая и пробельная ячейка — не значение (и не «значение без даты»)
			}
			if j+2 >= len(rows[fieldFound]) {
				// Дата колонки лежит в rows[fieldFound][j+2], ячейка значения — в row[j+2]: ячейка
				// правее конца шапки — точка без даты. Прежний страж «j+1» пропускал j+2 == len и
				// падал на доступе к шапке (off-by-one, §6.5), а непустая ячейка за шапкой терялась
				// молча (break).
				return fmt.Errorf("%s: лист %q, строка %d, колонка %d: значение %q без даты в шапке",
					tbIfrsTable, tbIfrsSheet, i+1, j+3, strings.TrimSpace(cell))
			}
			value, date, err := tbIfrsPoint(rows, fieldFound, i, j+2)
			if err != nil {
				return fmt.Errorf("%s: лист %q, строка %d, колонка %d: %w",
					tbIfrsTable, tbIfrsSheet, i+1, j+3, err)
			}
			if err = batch.Append(row[1], date, value); err != nil {
				return fmt.Errorf("%s: запись точки %q за %s: %w",
					tbIfrsTable, row[1], date.Format(tbIfrsTimeLayout), err)
			}
		}
	}
	return nil
}

// tbIfrsImporter — импортёр tbank_group_ifrs для шаблона и реестра: своя таблица, DDL в %s-форме,
// статичный DataUrl (текущий PDF) и разбор книги (ImportFunc). Регистрацию держит реестр пакета
// (bank/importers.go) — единственная точка append в chimport.Stats; шага в dagu/bank.yaml у
// импортёра нет до починки источника (О2).
var tbIfrsImporter = &TbIfrs{ClickHouseImport: util.ClickHouseImport{
	TableName:   tbIfrsTable,
	CreateTable: []string{tbIfrsDdl},
	DataUrl:     tbIfrsUrl,
	ImportFunc:  tbIfrsImportFunc,
}}
