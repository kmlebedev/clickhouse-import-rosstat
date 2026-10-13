package cbr

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// Денежно-кредитная и финансовая статистика https://www.cbr.ru/statistics/macro_itm/dkfs/
	// Сезонно скорректированные ряды денежных агрегатов
	// https://www.cbr.ru/vfs/statistics/credit_statistics/monetary_agg_SA.xlsx
	// https://www.cbr.ru/vfs/statistics/credit_statistics/M2-M2_SA.xlsx
	cbrM2XlsDataUrl = cbrStatsUrl + "/credit_statistics/monetary_agg_SA.xlsx"
	cbrM2Table      = "cbr_m2"
	cbrM2Ddl        = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, balance Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
	`
	cbrM2Sheet      = "Денежные агрегаты"
	cbrM2TimeLayout = "02/01/06"
	// cbrM2Column — колонка «М2 SA» (0-based индекс 22) с уровнем денежной массы; имя ряда
	// берётся из заголовка этой же колонки.
	cbrM2Column = 22
)

type cbrM2Stat struct {
	util.ClickHouseImport
}

// cbrM2Import разбирает лист «Денежные агрегаты»: одна точка на месяц, имя ряда — заголовок
// колонки (row[0][cbrM2Column], сейчас «М2 SA»), значение — уровень в млрд руб. Семантика
// прежнего export() сохранена: заголовок и строка единиц пропускаются, разбор идёт по
// непустым значениям колонки. Пустой лист/нет колонки/ноль точек — ошибка (fail-fast).
func cbrM2Import(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows(cbrM2Sheet)
	if err != nil {
		return err
	}
	if len(rows) < 3 || len(rows[0]) <= cbrM2Column {
		return fmt.Errorf("%s: в листе «%s» нет строк данных или колонки с уровнем М2", cbrM2Table, cbrM2Sheet)
	}
	name := rows[0][cbrM2Column]
	count := 0
	// Строки с годами
	for _, row := range rows[2:] {
		if len(row) == 0 {
			continue
		}
		// 22-й индекс — уровень М2; короткая строка означает конец таблицы (примечания)
		if len(row) <= cbrM2Column {
			break
		}
		if strings.TrimSpace(row[cbrM2Column]) == "" || strings.TrimSpace(row[cbrM2Column]) == "0" {
			continue
		}
		date, err := time.Parse(cbrM2TimeLayout, strings.TrimSpace(row[0]))
		if err != nil {
			return fmt.Errorf("%s: дата %q: %w", cbrM2Table, row[0], err)
		}
		balance := strings.ReplaceAll(strings.TrimSpace(row[cbrM2Column]), ",", "")
		value, err := strconv.ParseFloat(balance, 32)
		if err != nil {
			return fmt.Errorf("%s: дата %s: значение %q: %w", cbrM2Table, date.Format("2006-01-02"), balance, err)
		}
		if err = batch.Append(name, date, value); err != nil {
			return err
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор листа «%s» не дал ни одной точки", cbrM2Table, cbrM2Sheet)
	}
	log.Infof("%s: %d точек", cbrM2Table, count)
	return nil
}

// cbrM2Importer — сезонно сглаженные денежные агрегаты; регистрируется реестром
// cbr/importers.go.
var cbrM2Importer = &publishedStat{
	ImportStat: &cbrM2Stat{ClickHouseImport: util.ClickHouseImport{
		TableName:   cbrM2Table,
		CreateTable: []string{cbrM2Ddl},
		DataUrl:     cbrM2XlsDataUrl,
		ImportFunc:  cbrM2Import,
	}},
	meta: cbrM2SeriesMeta,
}
