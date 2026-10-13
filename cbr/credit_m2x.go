package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

// Денежно-кредитная и финансовая статистика https://www.cbr.ru/statistics/macro_itm/dkfs/
// Приложение к материалу «Кредит экономике и денежная масса»
const cbrCreditM2xUrl = "https://www.cbr.ru/statistics/macro_itm/dkfs/"

// cbrCreditM2xLinkSelector — ссылка на актуальный credit_m2x.xlsx. Id файла в URL
// (/Content/Document/File/NNNNN/) меняется при обновлении, поэтому ссылку берём
// со страницы, а не хардкодим.
const cbrCreditM2xLinkSelector = `a.referenceable[href$="credit_m2x.xlsx"]`

// cbrCreditM2xTable и cbrCreditM2xSheet — таблица и лист импортёра: оба попадают в текст
// ошибки пустого разбора, чтобы дежурный по логу видел, чей ряд остался без обновления.
const (
	cbrCreditM2xTable = "cbr_credit_m2x"
	cbrCreditM2xSheet = "млн рублей"
	// cbrCreditM2xCountPoints — количественный чек версии (спека §3.5): число точек
	// своего ряда. Литерал, как и таблица: запрос — часть утверждения «эти точки
	// мои». FINAL обязателен — иначе count() считал бы несмёрженные части.
	cbrCreditM2xCountPoints = "SELECT count() FROM cbr_credit_m2x FINAL"
)

var cbrСreditM2x = util.ClickHouseImport{
	TableName: cbrCreditM2xTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`},
	ImportFunc: func(xlsx *excelize.File, batch driver.Batch) (err error) {
		var rows [][]string
		if rows, err = xlsx.GetRows(cbrCreditM2xSheet); err != nil {
			return err
		}
		// Пустой лист — сразу ошибка: rows[1:] ниже паникует на пустом срезе, а паника в
		// импортёре — не «0 rows», а упавший шаг DAG без понятного сообщения.
		if len(rows) == 0 {
			return fmt.Errorf("%s: лист «%s» пуст", cbrCreditM2xTable, cbrCreditM2xSheet)
		}
		count := 0
		// Строки с годами
		for _, row := range rows[1:] {
			if len(row) == 0 {
				continue
			}
			name := strings.TrimSpace(row[0])
			if name == "" {
				break
			}
			for j, cell := range row[1:] {
				if j+1 >= len(rows[0]) {
					break
				}
				date, err := time.Parse("01-02-06", strings.TrimSpace(rows[0][j+1]))
				if err != nil {
					return err
				}
				valueStr := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
				if valueStr == "" {
					continue
				}
				if value, err := strconv.ParseFloat(valueStr, 32); err != nil {
					return err
				} else {
					if err = batch.Append(name, date, value); err != nil {
						return err
					}
					count++
				}
			}
		}
		if count == 0 {
			return fmt.Errorf("%s: разбор листа «%s» не дал ни одной точки", cbrCreditM2xTable, cbrCreditM2xSheet)
		}
		return nil
	},
	AfterImport:            afterCbrMacro(cbrCreditM2xSeriesMeta),
	PublicationCountPoints: cbrCreditM2xCountPoints,
}

type cbrCreditM2xStat struct {
	util.ClickHouseImport
}

// Import берёт адрес актуального credit_m2x.xlsx со страницы ДКС. Ссылку ищем тем же
// кодом, что и тест на фикстуре (util.GetFile + findLink).
func (s *cbrCreditM2xStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	page, err := getPage(cbrCreditM2xUrl)
	if err != nil {
		return count, fmt.Errorf("cbrCreditM2x: %s: %w", cbrCreditM2xUrl, err)
	}
	href, err := findLink(page, cbrCreditM2xLinkSelector)
	if err != nil {
		return count, fmt.Errorf("cbrCreditM2x: не найдена ссылка на credit_m2x.xlsx на %s: %w", cbrCreditM2xUrl, err)
	}
	s.DataUrl = cbrUrl + href
	return s.ClickHouseImport.Import(ctx, conn)
}

// cbrCreditM2xImporter — кредит экономике и денежная масса: адрес актуального
// credit_m2x.xlsx берётся со страницы ДКС в Import() (ARCHITECTURE.md §8.6),
// регистрация — реестром cbr/importers.go.
var cbrCreditM2xImporter = &cbrCreditM2xStat{ClickHouseImport: cbrСreditM2x}
