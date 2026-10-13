package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
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

var cbrСreditM2x = util.ClickHouseImport{
	TableName: "cbr_credit_m2x",
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`},
	ImportFunc: func(xlsx *excelize.File, batch driver.Batch) (err error) {
		var rows [][]string
		if rows, err = xlsx.GetRows("млн рублей"); err != nil {
			return err
		}
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
				}
			}
		}
		return nil
	},
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

func init() {
	chimport.Stats = append(chimport.Stats, &publishedStat{ImportStat: &cbrCreditM2xStat{ClickHouseImport: cbrСreditM2x}, meta: cbrCreditM2xSeriesMeta})
}
