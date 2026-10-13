package cbr

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// Сведения о размещенных и привлеченных средствах https://www.cbr.ru/statistics/bank_sector/sors
	// Объем кредитов, предоставленных юридическим лицам
	// https://www.cbr.ru/vfs/statistics/BankSector/Loans_to_corporations/01_01_A_New_loans_corp_by_activity.xlsx
	loansToCorporationsXlsDataUrl = cbrStatsUrl + "/BankSector/Loans_to_corporations/01_01_A_New_loans_corp_by_activity.xlsx"
	loansToCorporationsTable      = "cbr_loans_to_corporations"
	loansToCorporationsDdl        = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	loansToCorporationsSheet = "итого"
	loansToCorporationsField = "ВСЕГО"
)

type LoansToCorporationsStat struct {
	util.ClickHouseImport
}

// cbrLoansMonthToDate превращает заголовок колонки «Январь 2019» в дату наблюдения — первое
// число следующего месяца: в таблице ЦБ дата сдвинута на месяц вперёд. Разбор общий для
// обоих кредитных импортёров (cbr_loans_to_corporations и cbr_loans_to_individuals).
func cbrLoansMonthToDate(header string) (time.Time, error) {
	parts := strings.Fields(header)
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("заголовок колонки %q не разобран как «Месяц Год»", header)
	}
	month, ok := util.MonthsToNum[strings.ToLower(parts[0])]
	if !ok {
		return time.Time{}, fmt.Errorf("неизвестный месяц %q в заголовке колонки %q", parts[0], header)
	}
	year, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("год %q в заголовке колонки %q: %w", parts[1], header, err)
	}
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0), nil
}

// cbrLoansToCorpImport разбирает лист «итого»: имя ряда — строка файла (вид деятельности),
// дата — начало месяца наблюдения, значение — объём новых кредитов, млн руб. Семантика
// прежнего export() сохранена: разбор идёт от строки «ВСЕГО», строки с пустой или нулевой
// первой ячейкой пропускаются, пустые ячейки внутри строки пропускаются по одной. Пустой
// лист/отсутствие строки «ВСЕГО»/ноль точек — ошибка (fail-fast).
func cbrLoansToCorpImport(xlsx *excelize.File, batch driver.Batch) error {
	return cbrLoansImport(xlsx, batch, loansToCorporationsTable, loansToCorporationsSheet, loansToCorporationsField)
}

// cbrLoansImport — общий разбор обоих файлов кредитов: они отличаются только таблицей,
// листом и строкой-маркером (юридические лица — «ВСЕГО», физические — «РОССИЙСКАЯ ФЕДЕРАЦИЯ»).
func cbrLoansImport(xlsx *excelize.File, batch driver.Batch, table, sheet, marker string) error {
	rows, err := xlsx.GetRows(sheet)
	if err != nil {
		return err
	}
	headerRow, fieldFound := 0, false
	count := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		if i > 0 && strings.TrimSpace(row[0]) == marker {
			headerRow, fieldFound = i-1, true
		}
		if !fieldFound {
			continue
		}
		// строка из одной ячейки: заголовок колонок не прочитан, дальше смысла нет
		if len(row) < 2 {
			break
		}
		if strings.TrimSpace(row[1]) == "" || strings.TrimSpace(row[1]) == "0" {
			continue
		}
		// Колонки с месяцами и пропуском кварталов
		for j, cell := range row[1:] {
			if cell == "" {
				continue
			}
			if j+1 >= len(rows[headerRow]) {
				break
			}
			date, err := cbrLoansMonthToDate(rows[headerRow][j+1])
			if err != nil {
				return fmt.Errorf("%s: %w", table, err)
			}
			balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
			value, err := strconv.ParseFloat(balance, 32)
			if err != nil {
				return fmt.Errorf("%s: строка %q, дата %s: значение %q: %w",
					table, strings.TrimSpace(row[0]), date.Format("2006-01-02"), balance, err)
			}
			if err = batch.Append(strings.TrimSpace(row[0]), date, value); err != nil {
				return err
			}
			count++
		}
	}
	if !fieldFound {
		return fmt.Errorf("%s: в листе «%s» не найдена строка «%s» с датами колонок", table, sheet, marker)
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор листа «%s» не дал ни одной точки", table, sheet)
	}
	log.Infof("%s: %d точек", table, count)
	return nil
}

func init() {
	loansToCorporations := LoansToCorporationsStat{ClickHouseImport: util.ClickHouseImport{
		TableName:   loansToCorporationsTable,
		CreateTable: []string{loansToCorporationsDdl},
		DataUrl:     loansToCorporationsXlsDataUrl,
		ImportFunc:  cbrLoansToCorpImport,
	}}
	chimport.Stats = append(chimport.Stats, &publishedStat{ImportStat: &loansToCorporations, meta: cbrLoansToCorpSeriesMeta})
}
