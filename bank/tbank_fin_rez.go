package bank

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
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

// tbIfrsImportFunc — ImportFunc шаблона: книга из памяти → точки → батч. Разбор — механический
// перенос прежней семантики (тот же лист, тот же якорь, те же пропуски строк), поэтому ни тестов
// разбора, ни фикстуры у него нет — источника-книги нет, разбор не верифицирован (отступление О4
// спеки). Значение пишется в Float64-колонку новой таблицы (спека §3.5): число разбирается как
// float64, без приведения к точности Float32 legacy-таблиц. Ошибки называют таблицу, лист и строку
// (§6.4); обращения за границы срезов прежнего разбора (строки из двух и трёх ячеек) превращены в
// проверки — точек такие строки не давали и прежде, а паника в разборе запрещена (§6.5).
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
			// Значение читается из row[3]: прежний страж (len < 3) читал его без проверки (§6.5);
			// короткая строка, как и прежде, обрывает разбор — точек такой ряд не даёт.
			break
		}
		if v := strings.TrimSpace(row[3]); v == "" || v == "0" || v == "-" {
			continue // пустое значение, литеральный «0» и «-» источника — не точка ряда
		}
		// Колонки с месяцами и пропуском кварталов
		for j, cell := range row[2:] {
			if cell == "" {
				continue
			}
			if j+1 >= len(rows[fieldFound]) {
				break
			}
			balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
			value, err := strconv.ParseFloat(balance, 64)
			if err != nil {
				return fmt.Errorf("%s: лист %q, строка %d: значение %q не число: %w",
					tbIfrsTable, tbIfrsSheet, i+1, balance, err)
			}
			date, err := time.Parse(tbIfrsTimeLayout, rows[fieldFound][j+2])
			if err != nil {
				return fmt.Errorf("%s: лист %q, строка %d: дата колонки %q: %w",
					tbIfrsTable, tbIfrsSheet, i+1, rows[fieldFound][j+2], err)
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
// статичный DataUrl (текущий PDF) и разбор книги (ImportFunc). Регистрация — персональным init()
// до задачи 8 (её снимает реестр).
var tbIfrsImporter = &TbIfrs{ClickHouseImport: util.ClickHouseImport{
	TableName:   tbIfrsTable,
	CreateTable: []string{tbIfrsDdl},
	DataUrl:     tbIfrsUrl,
	ImportFunc:  tbIfrsImportFunc,
}}

func init() {
	chimport.Stats = append(chimport.Stats, tbIfrsImporter)
}
