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

// https://www.cbr.ru/analytics/dkp/inflationary_expectations/
// Статистические данные
const inflExpUrl = "https://www.cbr.ru/analytics/dkp/inflationary_expectations/"

// inflExpLinkSelector — ссылка на актуальный XLSX в блоке версий. Имена файлов и id
// меняются при каждом обновлении (Infl_exp_ГГ-ММ.xlsx), ссылки идут от свежей к старой.
const inflExpLinkSelector = `a.versions_item[href$=".xlsx"]`

// inflExpSeriesByRowLabel сопоставляет подписи строк файла ЦБ стабильным именам рядов
// в ClickHouse. Подписи меняются между выпусками (например «наблюдаемая инфляция» →
// «годовая наблюдаемая инфляция»), поэтому маппинг явный, а имена в БД остаются
// неизменными — на них завязаны series_catalog и витрина v_cbr_macro.
var inflExpSeriesByRowLabel = map[string]string{
	"наблюдаемая инфляция":                  "наблюдаемая инфляция",
	"годовая наблюдаемая инфляция":          "наблюдаемая инфляция",
	"ожидаемая инфляция":                    "ожидаемая инфляция",
	"годовая инфляция, ожидаемая через год": "ожидаемая инфляция",
}

const inflExpTableMarker = "Прямые оценки годовой инфляции: медианные  значения"

func inflExpImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("Данные для графиков")
	if err != nil {
		return err
	}
	tableIsFoundRowNum := -1
	for i, row := range rows[1:] {
		if len(row) == 0 || row[0] == "" {
			continue
		}
		if row[0] == inflExpTableMarker {
			tableIsFoundRowNum = i
			continue
		}
		if tableIsFoundRowNum < 0 {
			continue
		}
		series, ok := inflExpSeriesByRowLabel[strings.TrimSpace(row[0])]
		if !ok {
			break
		}
		dates := rows[tableIsFoundRowNum+2]
		for j, rowCol := range row[1:] {
			if j+1 >= len(dates) || dates[j+1] == "" || rowCol == "" {
				continue
			}
			date, err := time.Parse("Jan-06", dates[j+1])
			if err != nil {
				return fmt.Errorf("inflExp: parse date %q: %w", dates[j+1], err)
			}
			value, err := strconv.ParseFloat(rowCol, 64)
			if err != nil {
				return fmt.Errorf("inflExp: parse value %q: %w", rowCol, err)
			}
			if err = batch.Append(series, date.AddDate(0, 0, 13), value); err != nil {
				return err
			}
		}
	}
	return nil
}

type inflExpStat struct {
	util.ClickHouseImport
}

// Import берёт адрес актуального XLSX со страницы инфляционных ожиданий: имени файла
// в коде нет, а сетевое обращение в init() запрещено (ARCHITECTURE.md §8.6). Ссылку
// ищем тем же кодом, что и тест на фикстуре (util.GetFile + findLink).
func (s *inflExpStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	page, err := getPage(inflExpUrl)
	if err != nil {
		return count, fmt.Errorf("inflExp: %s: %w", inflExpUrl, err)
	}
	href, err := findLink(page, inflExpLinkSelector)
	if err != nil {
		return count, fmt.Errorf("inflExp: не найдена ссылка на xlsx на %s: %w", inflExpUrl, err)
	}
	s.DataUrl = cbrUrl + href
	return s.ClickHouseImport.Import(ctx, conn)
}

func init() {
	inflExp := inflExpStat{ClickHouseImport: util.ClickHouseImport{
		TableName: "cbr_infl_exp",
		CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`},
		ImportFunc: inflExpImport,
	}}
	chimport.Stats = append(chimport.Stats, &publishedStat{ImportStat: &inflExp, meta: cbrInflExpSeriesMeta})
}
