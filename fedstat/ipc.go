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

	fedstatIPCType = "К предыдущему месяцу"
)

// fedstatIPCYearMin — минимальное число точек года по research 2026-10-21.
// Служит признаком «год записан полностью»: если count() года меньше, год
// перечитывается (самолечение прерванного прогона), а уже записанные ключи
// отфильтровываются. 2026 — текущий год, он перечитывается всегда.
var fedstatIPCYearMin = map[int]int64{
	2002: 6876, 2003: 7020, 2004: 7428, 2005: 7428, 2006: 7680,
	2007: 7488, 2008: 7620, 2009: 7992, 2010: 7964, 2011: 8082,
	2012: 8328, 2013: 8386, 2014: 8424, 2015: 8424, 2016: 8424,
	2017: 8412, 2018: 8448, 2019: 8460, 2020: 8592, 2021: 9048,
	2022: 9072, 2023: 9156, 2024: 9176, 2025: 9036, 2026: 7074,
}

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

	yearCounts, err := fedstatIPCYearCounts(ctx, conn)
	if err != nil {
		return 0, err
	}
	client := util.NewDefaultTLSClient()
	pacer := &fedstatPacer{pause: fedstatIPCPause}
	currentYear := time.Now().Year()
	for _, year := range fedstatIPCSource.years {
		stored := yearCounts[year]
		expected := fedstatIPCYearMin[year]
		if stored >= expected && year < currentYear {
			continue
		}
		points, err := fedstatFetchYears(ctx, client, pacer, fedstatIPCTable, fedstatIPCSource, []int{year}, fedstatIPCPointFrom)
		if err != nil {
			return count, err
		}
		points, err = fedstatDedupPoints(fedstatIPCTable, points, func(p fedstatIPCPoint) string { return p.key() }, func(p fedstatIPCPoint) float64 { return p.value })
		if err != nil {
			return count, err
		}
		if len(points) == 0 {
			log.Warnf("%s: %d: источник не отдал точек", fedstatIPCTable, year)
			continue
		}
		if stored > 0 {
			existing, err := fedstatIPCExistingKeysForYear(ctx, conn, year)
			if err != nil {
				return count, err
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
			continue
		}
		inserted, err := fedstatInsert(ctx, conn, fedstatIPCTable, points, func(batch driver.Batch, point fedstatIPCPoint) error {
			return batch.Append(point.product, point.indicatorType, point.region, point.date, point.value)
		})
		if err != nil {
			return count, err
		}
		count += inserted
		log.Infof("%s: %d: Imported %d rows", fedstatIPCTable, year, inserted)
	}

	if err = s.AfterImport(ctx, conn); err != nil {
		return count, err
	}
	log.Infof("Imported %d rows of %s", count, s.Name())
	return count, nil
}

// fedstatIPCYearCounts — число точек по годам: первый прогон 31074 идёт ~30 мин,
// поэтому каждая годовая порция пишется своим батчем. Обрыв процесса оставляет
// уже записанные годы, следующий запуск продолжает с первого неполного года.
func fedstatIPCYearCounts(ctx context.Context, conn driver.Conn) (map[int]int64, error) {
	rows, err := conn.Query(ctx, `SELECT toYear(date) AS y, count() AS n FROM fedstat_ipc FINAL GROUP BY y`)
	if err != nil {
		return nil, fmt.Errorf("%s: годы: %w", fedstatIPCTable, err)
	}
	defer func() { _ = rows.Close() }()
	counts := make(map[int]int64)
	for rows.Next() {
		var year uint16
		var n uint64
		if err = rows.Scan(&year, &n); err != nil {
			return nil, fmt.Errorf("%s: год не прочитан: %w", fedstatIPCTable, err)
		}
		counts[int(year)] = int64(n)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: годы: %w", fedstatIPCTable, err)
	}
	return counts, nil
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

func fedstatIPCExistingKeysForYear(ctx context.Context, conn driver.Conn, year int) (map[string]bool, error) {
	from := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(1, 0, 0)
	rows, err := conn.Query(ctx, `SELECT product, indicator_type, region, date
FROM fedstat_ipc FINAL WHERE date >= ? AND date < ?`, from, to)
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
