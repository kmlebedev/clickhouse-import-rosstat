package fedstat

// fedstat_ipc — индексы потребительских цен на товары и услуги ЕМИСС (31074):
// 2002–2026, 1 105 товаров/услуг, тип «К предыдущему месяцу», только РФ.
// Один POST на год; полный прогон — 25 POST с паузой 30 с (ответ 200–275 КБ).
// 2023+ — РФ без учёта новых субъектов (1849012), до 2023 — РФ (1688487).

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	fedstatIPCTable = "fedstat_ipc"

	fedstatIPCDdl = `CREATE TABLE IF NOT EXISTS %s (
    product       LowCardinality(String),  -- товар/услуга; имя из Excel (1 105)
    indicator_type LowCardinality(String), -- 'К предыдущему месяцу'
    region        LowCardinality(String),
    date          Date,                    -- первое число месяца
    value         Float64                  -- индекс, процент
) ENGINE = ReplacingMergeTree ORDER BY (product, indicator_type, region, date);`

	fedstatIPCMinPoints = 204038
	fedstatIPCType      = "К предыдущему месяцу"
)

type fedstatIPCPoint struct {
	product       string
	indicatorType string
	region        string
	date          time.Time
	value         float64
}

func (p fedstatIPCPoint) key() string {
	return fedstatPointKey(p.product, p.indicatorType, p.region, p.date.Format("2006-01-02"))
}

var fedstatIPCSource = fedstatSourceSpec{
	indicator: "31074", title: "fedstat_ipc", layout: fedstatLayoutMatrix, labelCount: 2,
	left: []string{"57831", "58273"}, top: []string{"3", "33560", "57937"}, filter: []string{"30611"},
	unitDim: "30611", unitCode: "950473", regionDim: "57831",
	fixed:        []fedstatFilter{{"57937", "1704142"}},
	allDims:      []string{"58273"},
	years:        fedstatYearRange(2002, 2026),
	regionSwitch: true,
	pause:        fedstatIPCPause,
}

type FedstatIPCStat struct {
	util.ClickHouseImport
}

var fedstatIPCImporter = &FedstatIPCStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   fedstatIPCTable,
	CreateTable: []string{fedstatIPCDdl},
	AfterImport: afterFedstatIPCMacro,
}}

func (s *FedstatIPCStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", fedstatIPCTable, err)
		}
	}

	stored, maxDate, has, err := fedstatTableState(ctx, conn, fedstatIPCTable, "")
	if err != nil {
		return 0, err
	}
	full := !has || stored < fedstatIPCMinPoints
	years := fedstatIPCSource.years
	if !full {
		years = fedstatYearsFrom(maxDate.Year(), years)
	}

	client := util.NewDefaultTLSClient()
	pacer := &fedstatPacer{pause: fedstatIPCPause}
	points, err := fedstatFetchYears(ctx, client, pacer, fedstatIPCTable, fedstatIPCSource, years, fedstatIPCPointFrom)
	if err != nil {
		return 0, err
	}
	points, err = fedstatDedupPoints(fedstatIPCTable, points, func(p fedstatIPCPoint) string { return p.key() }, func(p fedstatIPCPoint) float64 { return p.value })
	if err != nil {
		return 0, err
	}
	if err = fedstatRequireFullFetch(fedstatIPCTable, fedstatIPCSource.indicator, full, len(points)); err != nil {
		return 0, err
	}
	if !full && len(points) > 0 {
		existing, err := fedstatIPCExistingKeys(ctx, conn, time.Date(maxDate.Year(), time.January, 1, 0, 0, 0, 0, time.UTC))
		if err != nil {
			return 0, err
		}
		filtered := points[:0]
		for _, point := range points {
			if existing[point.key()] {
				continue
			}
			filtered = append(filtered, point)
		}
		points = filtered
	}

	if len(points) == 0 {
		log.Infof("%s: новых точек нет", fedstatIPCTable)
		if err = s.AfterImport(ctx, conn); err != nil {
			return 0, err
		}
		log.Infof("Imported 0 rows of %s", s.Name())
		return 0, nil
	}
	count, err = fedstatInsert(ctx, conn, fedstatIPCTable, points, func(batch driver.Batch, point fedstatIPCPoint) error {
		return batch.Append(point.product, point.indicatorType, point.region, point.date, point.value)
	})
	if err != nil {
		return count, err
	}
	if err = s.AfterImport(ctx, conn); err != nil {
		return count, err
	}
	log.Infof("Imported %d rows of %s", count, s.Name())
	return count, nil
}

func fedstatIPCPointFrom(record fedstatRecord, usedRegion string) (fedstatIPCPoint, error) {
	if len(record.labels) < 2 {
		return fedstatIPCPoint{}, fmt.Errorf("в строке %d подписей, ожидалось 2", len(record.labels))
	}
	region := record.labels[0]
	if region == "" {
		region = fedstatRegionName(usedRegion)
	}
	return fedstatIPCPoint{
		product: record.labels[1], indicatorType: fedstatIPCType,
		region: region, date: record.date, value: record.value,
	}, nil
}

func fedstatIPCExistingKeys(ctx context.Context, conn driver.Conn, from time.Time) (map[string]bool, error) {
	rows, err := conn.Query(ctx, `SELECT product, indicator_type, region, date
FROM fedstat_ipc FINAL WHERE date >= ?`, from)
	if err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatIPCTable, err)
	}
	defer func() { _ = rows.Close() }()
	existing := make(map[string]bool)
	for rows.Next() {
		var product, indicatorType, region string
		var date time.Time
		if err = rows.Scan(&product, &indicatorType, &region, &date); err != nil {
			return nil, fmt.Errorf("%s: ключ не прочитан: %w", fedstatIPCTable, err)
		}
		existing[fedstatPointKey(product, indicatorType, region, date.Format("2006-01-02"))] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatIPCTable, err)
	}
	return existing, nil
}

func afterFedstatIPCMacro(ctx context.Context, conn driver.Conn) error {
	meta, err := fedstatIPCCatalog(ctx, conn)
	if err != nil {
		return err
	}
	if len(meta) > 0 {
		if err = util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
	}
	_, err = util.CreateView(ctx, conn, fedstatIPCView)
	return err
}

func fedstatIPCCatalog(ctx context.Context, conn driver.Conn) ([]util.SeriesMeta, error) {
	rows, err := conn.Query(ctx, `SELECT DISTINCT product FROM fedstat_ipc FINAL ORDER BY product`)
	if err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatIPCTable, err)
	}
	defer func() { _ = rows.Close() }()
	meta := make([]util.SeriesMeta, 0, 1105)
	for rows.Next() {
		var product string
		if err = rows.Scan(&product); err != nil {
			return nil, fmt.Errorf("%s: каталог: %w", fedstatIPCTable, err)
		}
		meta = append(meta, util.SeriesMeta{
			Source:      fedstatSource,
			Series:      "ipc31074/" + product,
			Title:       product,
			Unit:        "процент",
			Frequency:   "M",
			Origin:      "fedstat.ru/indicator/31074, all-dims 58273, тип 57937=1704142, region 57831",
			Description: fmt.Sprintf("Индекс потребительских цен на «%s» к предыдущему месяцу, процент; 100 = без изменения. 2002–2026; 2023+ — РФ без учёта новых субъектов (1849012), до 2023 — РФ (1688487). Дата — начало месяца наблюдения, не дата публикации. Другие виды показателя 31074 (г/г, к декабрю и т.п.) в этот ряд не входят.", product),
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatIPCTable, err)
	}
	return meta, nil
}
