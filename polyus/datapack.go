package polyus

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

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
// legacy-импортёра).
var datapackTables = []string{
	"CONSOLIDATED OPERATING RESULTS", "OLIMPIADA", "BLAGODATNOYE", "TITIMUKHTA",
	"VERNINSKOYE2", "ALLUVIALS", "KURANAKH", "ZAPADNOYE", "NATALKA", "Sukhoi Log",
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

	var table string
	for _, row := range sheetRows {
		if len(row) <= datapackTableColNum || row[datapackTableColNum] == "" {
			continue
		}
		if slices.Contains(datapackTables, row[datapackTableColNum]) {
			table = row[datapackTableColNum]
			log.Debugf("Found table %s", table)
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
				rows = append(rows, datapackRow{
					Table: table,
					Name:  row[datapackTableColNum],
					Data:  data,
					Date:  date.AddDate(0, 1, 0),
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
