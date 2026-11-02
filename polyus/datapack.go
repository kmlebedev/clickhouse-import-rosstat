package polyus

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// datapackTable — имя таблицы витрины. Схема перенесена без изменений из
// legacy financial/gold_polyus.go: таблица уже наполнена в ClickHouse и
// читается dashboard/finance-polyus.json.
const datapackTable = "databook_polyus"

const datapackCreateTable = `CREATE TABLE IF NOT EXISTS ` + datapackTable + ` (
   table LowCardinality(String),
   name LowCardinality(String),
   data LowCardinality(String),
   date Date,
   value Float32
) ENGINE = ReplacingMergeTree()
ORDER BY (table, name, data, date)`

const datapackInsert = "INSERT INTO " + datapackTable

// datapackDataPath — локальная копия датапака Polyus в репозитории.
const datapackDataPath = "polyus/data/polyus_datapack_fy2025_new.xlsx"

// datapackSheet — лист с таблицами месторождений.
const datapackSheet = "Sheet1"

// datapackTableColNum — индекс колонки, в которой стоит имя таблицы/показателя.
const datapackTableColNum = 1

// datapackDateRowIdx — строка с подписями периодов (1Q'07, 1H'24, 2007).
const datapackDateRowIdx = 3

// datapackToData — суффикс подписи периода → тип периода.
var datapackToData = map[string]string{
	"20": "ANNUAL",
	"Q":  "QUARTERLY",
	"H":  "SEMI-ANNUAL",
}

// datapackTables — таблицы месторождений, присутствующие в датапаке (из init()
// legacy-импортёра). Значения — канонические имена: именно они попадают в
// колонку table.
var datapackTables = []string{
	"CONSOLIDATED OPERATING RESULTS", "OLIMPIADA", "BLAGODATNOYE", "TITIMUKHTA",
	"VERNINSKOYE2", "ALLUVIALS", "KURANAKH", "ZAPADNOYE", "NATALKA", "Sukhoi Log",
}

// datapackTableAliases — написания метки блока, которых нет в datapackTables.
// Сноска 1H25-датапака называет Вернинское `VERNINSKOYE**`; нормализация
// снимает звёздочки, а alias возвращает каноническое имя таблицы.
var datapackTableAliases = map[string]string{
	"VERNINSKOYE": "VERNINSKOYE2",
}

// datapackTablesByLabel — нормализованная метка блока → каноническое имя
// таблицы. Строится один раз: `Sukhoi Log` остаётся каноническим значением, а
// сравнение не зависит от регистра, пробелов и footnote-маркера.
var datapackTablesByLabel = func() map[string]string {
	tables := make(map[string]string, len(datapackTables))
	for _, table := range datapackTables {
		tables[datapackLabelKey(table)] = table
	}

	return tables
}()

// datapackLabelKey нормализует метку блока: trim, снятие хвостовых `*`
// (footnote-маркер `VERNINSKOYE**`), верхний регистр, схлопывание пробелов.
func datapackLabelKey(label string) string {
	label = strings.TrimSpace(label)
	label = strings.TrimRight(label, "*")
	label = strings.ToUpper(label)

	return strings.Join(strings.Fields(label), " ")
}

// datapackCanonicalTable возвращает каноническое имя таблицы для метки блока:
// сначала точное (нормализованное) совпадение с datapackTables, затем alias.
// Второй результат — метка вообще распознана.
func datapackCanonicalTable(label string) (string, bool) {
	key := datapackLabelKey(label)
	if table, ok := datapackTablesByLabel[key]; ok {
		return table, true
	}

	table, ok := datapackTableAliases[key]

	return table, ok
}

// datapackIsBlockHeader — строка-заголовок блока актива: колонка B непустая,
// колонка C пустая и все data-колонки пустые. Так же выглядят заголовки листа
// (`PJSC POLYUS`, `OPERATING RESULTS1`), секции (`Mining`, `Processing`, `Mill`,
// `Heap-leach`) и сноски (`1 - …`), поэтому блоком строку делает только
// следующий шаг — распознавание метки; остальные такие строки игнорируются.
func datapackIsBlockHeader(row []string) bool {
	if len(row) <= datapackTableColNum || strings.TrimSpace(row[datapackTableColNum]) == "" {
		return false
	}

	if len(row) > datapackTableColNum+1 && strings.TrimSpace(row[datapackTableColNum+1]) != "" {
		return false
	}

	dataStart := datapackTableColNum + 4 // B и C, затем D, E, дальше значения
	if len(row) > dataStart {
		for _, cell := range row[dataStart:] {
			if strings.TrimSpace(cell) != "" {
				return false
			}
		}
	}

	return true
}

// datapackSections — подзаголовки секций внутри блока актива. Они тоже стоят
// в колонке B без единицы и значений, но границей блока не являются.
var datapackSections = map[string]bool{
	"MINING":     true,
	"PROCESSING": true,
	"MILL":       true,
	"HEAP-LEACH": true,
}

// datapackIsSection — подзаголовок секции (`Mining`, `Processing`, `Mill`,
// `Heap-leach`) в нормализованном виде.
func datapackIsSection(label string) bool {
	return datapackSections[datapackLabelKey(label)]
}

// datapackIsFootnote — сноска под таблицей (`1 - …`, `2 - …`, `*`). Сноски
// стоят в колонке B без единицы и значений, но блоком актива не являются.
func datapackIsFootnote(label string) bool {
	for _, r := range label {
		if unicode.IsSpace(r) {
			continue
		}

		return unicode.IsDigit(r) || r == '*'
	}

	return false
}

// datapackIsUpperLabel — метка из заглавных букв, цифр и знаков, но содержит
// хотя бы одну букву. Секции (`Mining`, `Processing`) и сноски (`1 - …`, `*`)
// под правило не подпадают: в них есть строчные буквы или нет букв вовсе.
func datapackIsUpperLabel(label string) bool {
	var letter bool

	for _, r := range label {
		if unicode.IsLower(r) {
			return false
		}

		if unicode.IsLetter(r) {
			letter = true
		}
	}

	return letter
}

// datapackRowKey — ключ ReplacingMergeTree (table, name, data, date) в виде,
// пригодном для карты. Date — уже в формате колонки Date, чтобы не зависеть от
// location у time.Time.
type datapackRowKey struct {
	Table string
	Name  string
	Data  string
	Date  string
}

// datapackRow — одна точка датапака: значение показателя name таблицы table
// за период data с датой date (первое число месяца, следующего за концом периода).
type datapackRow struct {
	Table string
	Name  string
	Data  string
	Date  time.Time
	Value float32
}

// parseStats — счётчики разбора: сколько точек добавлено и сколько ячеек
// пропущено как нечисловые (вместо падения всего импорта).
type parseStats struct {
	Rows    int
	Skipped int
}

// parseDatapack разбирает датапак Polyus и возвращает точки ряда.
// Функция чистая (не трогает ClickHouse) и не падает на битой ячейке:
// нечисловое значение пропускается со счётчиком stats.Skipped и log.Warnf.
func parseDatapack(path string) (rows []datapackRow, stats parseStats, err error) {
	xlsx, err := excelize.OpenFile(path)
	if err != nil {
		return nil, stats, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = xlsx.Close() }()

	log.Debugf("Import sheet %s", datapackSheet)

	sheetRows, err := xlsx.GetRows(datapackSheet)
	if err != nil {
		return nil, stats, fmt.Errorf("get rows of %s: %w", datapackSheet, err)
	}
	log.Debugf("get rows: %d", len(sheetRows))

	if len(sheetRows) <= datapackDateRowIdx {
		return nil, stats, fmt.Errorf(
			"%s: sheet %s has %d rows, date row %d is missing",
			path, datapackSheet, len(sheetRows), datapackDateRowIdx,
		)
	}

	var (
		table     string
		block     int
		hasBlocks bool
		// seen — блок, в котором встретился ключ (Table, Name, Data, Date).
		// Повтор ключа в ДРУГОМ блоке — ошибка: так выглядит метка, которую
		// разбор не распознал и отнёс строки к предыдущему активу (на staging
		// 1H25 `VERNINSKOYE**` уехал в TITIMUKHTA). Повтор внутри одного блока —
		// штатная структура датапака: у KURANAKH секции `Mill` и `Heap-leach`
		// обе содержат `Ore processed`, и такие коллизии схлопывает
		// ReplacingMergeTree, поэтому ошибкой они не считаются.
		seen = make(map[datapackRowKey]int)
	)
	for i, row := range sheetRows {
		if len(row) <= datapackTableColNum || strings.TrimSpace(row[datapackTableColNum]) == "" {
			continue
		}
		label := strings.TrimSpace(row[datapackTableColNum])
		if datapackIsBlockHeader(row) {
			canonical, ok := datapackCanonicalTable(label)
			switch {
			case ok:
				table = canonical
				block++
				hasBlocks = true
				log.Debugf("Found table %s (%q)", table, label)
			case !hasBlocks || datapackIsSection(label) || datapackIsFootnote(label):
				// Заголовки листа (`PJSC POLYUS`, `OPERATING RESULTS1`) стоят до
				// первого актива, секции (`Mining`, `Processing`) и сноски
				// (`1 - …`) — не блоки: границу блока они не двигают.
			case datapackIsUpperLabel(label):
				// Неизвестная all-caps метка после первого актива означает, что
				// блок молча уехал бы к предыдущему активу (дефект 1H25),
				// поэтому разбор падает, а не портит данные.
				return nil, stats, fmt.Errorf(
					"%s: line %d: unknown asset block label %q",
					path, i+1, label,
				)
			default:
				// Метка без all-caps неизвестна: имя таблицы не меняем, но
				// границу блока фиксируем, чтобы дубль ключа в следующих
				// строках поймался ниже, а не растворился в предыдущем активе.
				block++
				log.Warnf("unknown asset block label %q at line %d, rows stay in %s", label, i+1, table)
			}
			continue
		}
		if len(row) <= datapackTableColNum+1 || row[datapackTableColNum+1] == "" {
			continue
		}
		// Строки данных бывают длиннее строки с датами, а индексы берутся
		// одни и те же, поэтому проверяем границу обеих строк.
		if len(sheetRows[datapackDateRowIdx]) < len(row) {
			return nil, stats, fmt.Errorf(
				"%s: date row %d is shorter than data row %d",
				path, datapackDateRowIdx, len(row),
			)
		}
		tableStartNum := datapackTableColNum + 4
		for key, data := range datapackToData {
			for j, colCell := range row[tableStartNum:] {
				dateCell := sheetRows[datapackDateRowIdx][j+tableStartNum]
				if len(dateCell) < 2 || !strings.Contains(dateCell[0:2], key) {
					continue
				}
				dateStr := dateCell // 2Q'07
				dateQ, err := strconv.Atoi(dateStr[0:1])
				if err != nil {
					return nil, stats, fmt.Errorf("period %q: %w", dateStr, err)
				}
				var date time.Time
				var parseErr error
				switch data {
				case "ANNUAL": // 2007
					date, parseErr = time.Parse("2006-01-02", fmt.Sprintf("%s-12-01", dateStr))
				case "QUARTERLY": // 2Q'07
					date, parseErr = time.Parse("06-01-02", fmt.Sprintf("%s-%02d-01", dateStr[len(dateStr)-2:], dateQ*3))
				case "SEMI-ANNUAL": // 2H'24
					date, parseErr = time.Parse("06-01-02", fmt.Sprintf("%s-%02d-01", dateStr[len(dateStr)-2:], dateQ*6))
				}
				if parseErr != nil {
					return nil, stats, fmt.Errorf("period %q: %w", dateStr, parseErr)
				}
				if colCell == "-" || colCell == "N/A" {
					continue
				}
				value, parseErr := strconv.ParseFloat(strings.ReplaceAll(strings.Trim(colCell, "%()"), ",", ""), 32)
				if parseErr != nil {
					// Битые ячейки (текст вместо числа) не роняют импорт:
					// считаем их и продолжаем разбор оставшихся точек.
					stats.Skipped++
					log.Warnf("skip non-numeric cell %q in %s/%s", colCell, table, row[datapackTableColNum])
					continue
				}
				if strings.HasPrefix(colCell, "(") && strings.HasSuffix(colCell, ")") {
					value = value * -1
				}
				rowDate := date.AddDate(0, 1, 0)
				rowKey := datapackRowKey{
					Table: table,
					Name:  row[datapackTableColNum],
					Data:  data,
					Date:  rowDate.Format("2006-01-02"),
				}
				if prevBlock, ok := seen[rowKey]; ok && prevBlock != block {
					return nil, stats, fmt.Errorf(
						"%s: line %d: duplicate key table=%q name=%q data=%q date=%s: block %d repeats block %d",
						path, i+1, rowKey.Table, rowKey.Name, rowKey.Data, rowKey.Date, block, prevBlock,
					)
				}
				seen[rowKey] = block
				rows = append(rows, datapackRow{
					Table: table,
					Name:  row[datapackTableColNum],
					Data:  data,
					Date:  rowDate,
					Value: float32(value),
				})
				stats.Rows++
			}
		}
	}
	return rows, stats, nil
}

// datapackImport импортирует операционный датапак Polyus в databook_polyus.
type datapackImport struct{}

func (s *datapackImport) Name() string {
	return datapackTable
}

func (s *datapackImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	rows, stats, err := parseDatapack(datapackDataPath)
	if err != nil {
		return 0, err
	}
	if err = conn.Exec(ctx, datapackCreateTable); err != nil {
		return 0, err
	}
	batch, err := conn.PrepareBatch(ctx, datapackInsert)
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if err = batch.Append(row.Table, row.Name, row.Data, row.Date, row.Value); err != nil {
			return 0, err
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return 0, err
	}
	log.Infof("Imported %d rows of %s (%d cells skipped)", count, datapackTable, stats.Skipped)
	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &datapackImport{})
}
