package fedstat

// fedstat_retail — оборот розничной торговли ЕМИСС: 31066 (индекс физического
// объёма) и 31260 (оборот, млн руб.). period — подпись периода источника:
// у накопительных периодов дата начала одна (1 января), без period
// ReplacingMergeTree схлопнул бы «январь-март»/«январь-июнь»/….
// 31066 содержит два вида показателя (м/м и к соответствующему периоду
// предыдущего года), поэтому indicator_type — часть ключа.

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	fedstatRetailTable = "fedstat_retail"

	fedstatRetailDdl = `CREATE TABLE IF NOT EXISTS %s (
    series        LowCardinality(String),  -- 'index' | 'turnover'
    indicator_type LowCardinality(String), -- 'К предыдущему месяцу' | 'К соответствующему периоду предыдущего года' | 'Оборот розничной торговли'
    subject       LowCardinality(String),  -- 'Всего' | 'Розничные рынки и ярмарки' | 'Торгующие организации'
    region        LowCardinality(String),
    period_type   LowCardinality(String),  -- 'M' | 'CUM'
    period        LowCardinality(String),  -- 'январь' | 'январь-март' | …
    date          Date,
    value         Float64,
    unit          LowCardinality(String)   -- 'процент' | 'миллион рублей'
) ENGINE = ReplacingMergeTree ORDER BY (series, indicator_type, subject, region, period_type, period, date);`

	fedstatRetailIndexMin    = 2799
	fedstatRetailTurnoverMin = 1839
)

type fedstatRetailPoint struct {
	series        string
	indicatorType string
	subject       string
	region        string
	periodType    string
	period        string
	date          time.Time
	value         float64
	unit          string
}

func (p fedstatRetailPoint) key() string {
	return fedstatPointKey(p.series, p.indicatorType, p.subject, p.region, p.periodType, p.period, p.date.Format("2006-01-02"))
}

type fedstatRetailSource struct {
	source        fedstatSourceSpec
	series        string
	indicatorType string
	unit          string
	minPoints     int64
}

var fedstatRetailSources = []fedstatRetailSource{
	{
		source: fedstatSourceSpec{
			indicator: "31066", title: "fedstat_retail", layout: fedstatLayoutMatrix, labelCount: 3,
			left: []string{"57937", "57831", "58851"}, top: []string{"3", "33560"}, filter: []string{"30611"},
			unitDim: "30611", unitCode: "950473", regionDim: "57831",
			allDims:      []string{"57937", "58851"},
			years:        fedstatYearRange(2000, 2026),
			regionSwitch: true,
			pause:        fedstatMacroPause,
		},
		series: "index", unit: "процент", minPoints: fedstatRetailIndexMin,
	},
	{
		source: fedstatSourceSpec{
			indicator: "31260", title: "fedstat_retail", layout: fedstatLayoutMatrix, labelCount: 2,
			left: []string{"57831", "58851"}, top: []string{"3", "33560"}, filter: []string{"30611"},
			unitDim: "30611", unitCode: "950353", regionDim: "57831",
			allDims:      []string{"58851"},
			years:        fedstatYearRange(2000, 2026),
			regionSwitch: true,
			pause:        fedstatMacroPause,
		},
		series: "turnover", indicatorType: "Оборот розничной торговли", unit: "миллион рублей", minPoints: fedstatRetailTurnoverMin,
	},
}

type FedstatRetailStat struct {
	util.ClickHouseImport
}

var fedstatRetailImporter = &FedstatRetailStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   fedstatRetailTable,
	CreateTable: []string{fedstatRetailDdl},
	AfterImport: afterFedstatRetailMacro,
}}

func (s *FedstatRetailStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", fedstatRetailTable, err)
		}
	}

	client := fedstatMacroClient()
	pacer := &fedstatPacer{pause: fedstatMacroPause}
	var all []fedstatRetailPoint
	for _, item := range fedstatRetailSources {
		points, err := fedstatRetailFetchSource(ctx, conn, client, pacer, item)
		if err != nil {
			return 0, err
		}
		all = append(all, points...)
	}

	if len(all) == 0 {
		log.Infof("%s: новых точек нет", fedstatRetailTable)
		if err = s.AfterImport(ctx, conn); err != nil {
			return 0, err
		}
		log.Infof("Imported 0 rows of %s", s.Name())
		return 0, nil
	}
	all, err = fedstatDedupPoints(fedstatRetailTable, all, func(p fedstatRetailPoint) string { return p.key() }, func(p fedstatRetailPoint) float64 { return p.value })
	if err != nil {
		return 0, err
	}
	count, err = fedstatInsert(ctx, conn, fedstatRetailTable, all, func(batch driver.Batch, point fedstatRetailPoint) error {
		return batch.Append(point.series, point.indicatorType, point.subject, point.region, point.periodType, point.period, point.date, point.value, point.unit)
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

func fedstatRetailFetchSource(ctx context.Context, conn driver.Conn, client *http.Client, pacer *fedstatPacer, item fedstatRetailSource) ([]fedstatRetailPoint, error) {
	where := fmt.Sprintf("series = '%s'", item.series)
	stored, maxDate, has, err := fedstatTableState(ctx, conn, fedstatRetailTable, where)
	if err != nil {
		return nil, err
	}
	full := !has || stored < item.minPoints
	years := item.source.years
	if !full {
		years = fedstatYearsFrom(maxDate.Year(), years)
	}
	if len(years) == 0 {
		return nil, nil
	}
	points, err := fedstatFetchYears(ctx, client, pacer, fedstatRetailTable, item.source, years, func(record fedstatRecord, usedRegion string) (fedstatRetailPoint, error) {
		return fedstatRetailPointFrom(item, record, usedRegion)
	})
	if err != nil {
		return nil, err
	}
	points, err = fedstatDedupPoints(fedstatRetailTable, points, func(p fedstatRetailPoint) string { return p.key() }, func(p fedstatRetailPoint) float64 { return p.value })
	if err != nil {
		return nil, err
	}
	if err = fedstatRequireFullFetch(fedstatRetailTable, item.series, full, len(points)); err != nil {
		return nil, err
	}
	if full || len(points) == 0 {
		return points, nil
	}
	existing, err := fedstatRetailExistingKeys(ctx, conn, item.series, time.Date(maxDate.Year(), time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		return nil, err
	}
	filtered := points[:0]
	for _, point := range points {
		if existing[point.key()] {
			continue
		}
		filtered = append(filtered, point)
	}
	return filtered, nil
}

func fedstatRetailPointFrom(item fedstatRetailSource, record fedstatRecord, usedRegion string) (fedstatRetailPoint, error) {
	point := fedstatRetailPoint{
		series: item.series, indicatorType: item.indicatorType, periodType: record.periodType,
		period: record.period, date: record.date, value: record.value, unit: item.unit,
	}
	switch item.series {
	case "index":
		if len(record.labels) < 3 {
			return fedstatRetailPoint{}, fmt.Errorf("в строке %d подписей, ожидалось 3", len(record.labels))
		}
		point.indicatorType = record.labels[0]
		point.region = record.labels[1]
		point.subject = record.labels[2]
	case "turnover":
		if len(record.labels) < 2 {
			return fedstatRetailPoint{}, fmt.Errorf("в строке %d подписей, ожидалось 2", len(record.labels))
		}
		point.region = record.labels[0]
		point.subject = record.labels[1]
	}
	if point.region == "" {
		point.region = fedstatRegionName(usedRegion)
	}
	return point, nil
}

func fedstatRetailExistingKeys(ctx context.Context, conn driver.Conn, series string, from time.Time) (map[string]bool, error) {
	rows, err := conn.Query(ctx, `SELECT series, indicator_type, subject, region, period_type, period, date
FROM fedstat_retail FINAL WHERE series = ? AND date >= ?`, series, from)
	if err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatRetailTable, err)
	}
	defer func() { _ = rows.Close() }()
	existing := make(map[string]bool)
	for rows.Next() {
		var seriesValue, indicatorType, subject, region, periodType, period string
		var date time.Time
		if err = rows.Scan(&seriesValue, &indicatorType, &subject, &region, &periodType, &period, &date); err != nil {
			return nil, fmt.Errorf("%s: ключ не прочитан: %w", fedstatRetailTable, err)
		}
		existing[fedstatPointKey(seriesValue, indicatorType, subject, region, periodType, period, date.Format("2006-01-02"))] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatRetailTable, err)
	}
	return existing, nil
}

func afterFedstatRetailMacro(ctx context.Context, conn driver.Conn) error {
	meta, err := fedstatRetailCatalog(ctx, conn)
	if err != nil {
		return err
	}
	if len(meta) > 0 {
		if err = util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
	}
	_, err = util.CreateView(ctx, conn, fedstatRetailView)
	return err
}

func fedstatRetailCatalog(ctx context.Context, conn driver.Conn) ([]util.SeriesMeta, error) {
	rows, err := conn.Query(ctx, `SELECT DISTINCT series, indicator_type, subject
FROM fedstat_retail FINAL ORDER BY series, indicator_type, subject`)
	if err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatRetailTable, err)
	}
	defer func() { _ = rows.Close() }()
	var meta []util.SeriesMeta
	for rows.Next() {
		var series, indicatorType, subject string
		if err = rows.Scan(&series, &indicatorType, &subject); err != nil {
			return nil, fmt.Errorf("%s: каталог: %w", fedstatRetailTable, err)
		}
		unit := "процент"
		indicator := "31066"
		what := "индекс физического объёма оборота розничной торговли"
		if series == "turnover" {
			unit = "миллион рублей"
			indicator = "31260"
			what = "оборот розничной торговли"
		}
		meta = append(meta, util.SeriesMeta{
			Source:      fedstatSource,
			Series:      fmt.Sprintf("retail/%s/%s/%s", series, indicatorType, subject),
			Title:       fmt.Sprintf("%s — %s — %s", series, indicatorType, subject),
			Unit:        unit,
			Frequency:   "M",
			Origin:      fmt.Sprintf("fedstat.ru/indicator/%s, all-dims 57937/58851, region 57831", indicator),
			Description: fmt.Sprintf("%s: %s. 2023+ — РФ без учёта новых субъектов (1849012), до 2023 — РФ (1688487). period_type M — месяц, CUM — накопительный период; значения CUM не складывать с месячными. %s", what, subject, "Не смешивать индекс и оборот в рублях без дефлятора."),
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatRetailTable, err)
	}
	return meta, nil
}
