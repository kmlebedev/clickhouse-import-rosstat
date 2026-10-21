package fedstat

// fedstat_agri — продукция сельского хозяйства ЕМИСС: 34128 (квартальный
// индекс, 2018–2026), 59001 (годовой индекс, 2017–2025), 31062 (годовой
// индекс, 1991–2016) и 43337 (стоимость, 1990–2011 фактически). Квартальный и
// годовой ряды не смешиваются: series различает index_q/index_y/value_y.

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
	fedstatAgriTable = "fedstat_agri"

	fedstatAgriDdl = `CREATE TABLE IF NOT EXISTS %s (
    series        LowCardinality(String),  -- 'index_q' | 'index_y' | 'value_y'
    activity      LowCardinality(String),  -- 'Сельское хозяйство' | 'Растениеводство' | 'Животноводство'
    category      LowCardinality(String),  -- 'Хозяйства всех категорий' | …
    region        LowCardinality(String),
    period_type   LowCardinality(String),  -- 'Q' | 'Y'
    date          Date,                    -- начало квартала / года
    value         Float64,
    unit          LowCardinality(String)   -- 'процент' | 'миллион рублей'
) ENGINE = ReplacingMergeTree ORDER BY (series, activity, category, region, date);`

	fedstatAgriQMin       = 60
	fedstatAgriYMin       = 108
	fedstatAgriHistoryMin = 312
	fedstatAgriValueMin   = 85
)

type fedstatAgriPoint struct {
	series     string
	activity   string
	category   string
	region     string
	periodType string
	date       time.Time
	value      float64
	unit       string
}

func (p fedstatAgriPoint) key() string {
	return fedstatPointKey(p.series, p.activity, p.category, p.region, p.date.Format("2006-01-02"))
}

type fedstatAgriSource struct {
	source    fedstatSourceSpec
	series    string
	minPoints int64
	activity  string
	unit      string
	frozen    bool
	where     string
}

var fedstatAgriSources = []fedstatAgriSource{
	{
		source: fedstatSourceSpec{
			indicator: "34128", title: "fedstat_agri", layout: fedstatLayoutMatrix, labelCount: 1,
			left: []string{"58423"}, top: []string{"3", "33560"}, filter: []string{"30611", "57831"},
			unitDim: "30611", unitCode: "950473", regionDim: "57831",
			allDims:      []string{"58423"},
			years:        fedstatYearRange(2018, 2026),
			regionSwitch: true,
			pause:        fedstatMacroPause,
		},
		series: "index_q", minPoints: fedstatAgriQMin, activity: "Сельское хозяйство", unit: "процент",
		where: "series = 'index_q'",
	},
	{
		source: fedstatSourceSpec{
			indicator: "59001", title: "fedstat_agri", layout: fedstatLayoutActivityColumns, labelCount: 2,
			left: []string{"57831", "58423"}, top: []string{"57940", "3"}, filter: []string{"30611", "33560"},
			unitDim: "30611", unitCode: "950473", regionDim: "57831",
			fixed:        []fedstatFilter{{"33560", "1558883"}},
			allDims:      []string{"57940", "58423"},
			years:        fedstatYearRange(2017, 2025),
			regionSwitch: true,
			pause:        fedstatMacroPause,
		},
		series: "index_y", minPoints: fedstatAgriYMin, unit: "процент",
		where: "series = 'index_y' AND date >= '2017-01-01'",
	},
	{
		source: fedstatSourceSpec{
			indicator: "31062", title: "fedstat_agri", layout: fedstatLayoutYearColumns, labelCount: 3,
			left: []string{"58423", "57833", "57831"}, top: []string{"3", "33560"}, filter: []string{"30611"},
			unitDim: "30611", unitCode: "950473", regionDim: "57831",
			allDims: []string{"58423", "57833"},
			years:   fedstatYearRange(1991, 2016),
			batch:   true,
			pause:   fedstatMacroPause,
		},
		series: "index_y", minPoints: fedstatAgriHistoryMin, unit: "процент", frozen: true,
		where: "series = 'index_y' AND date < '2017-01-01'",
	},
	{
		source: fedstatSourceSpec{
			indicator: "43337", title: "fedstat_agri", layout: fedstatLayoutYearColumns, labelCount: 3,
			left: []string{"58423", "57831", "30611"}, top: []string{"3", "33560"},
			unitDim: "30611", unitCode: "950353", regionDim: "57831",
			fixed:   []fedstatFilter{{"33560", "1558883"}},
			allDims: []string{"58423"},
			years:   fedstatYearRange(1990, 2011),
			batch:   true,
			pause:   fedstatMacroPause,
		},
		series: "value_y", minPoints: fedstatAgriValueMin, activity: "Сельское хозяйство", unit: "миллион рублей", frozen: true,
		where: "series = 'value_y'",
	},
}

type FedstatAgriStat struct {
	util.ClickHouseImport
}

var fedstatAgriImporter = &FedstatAgriStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   fedstatAgriTable,
	CreateTable: []string{fedstatAgriDdl},
	AfterImport: afterFedstatAgriMacro,
}}

func (s *FedstatAgriStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", fedstatAgriTable, err)
		}
	}

	client := fedstatMacroClient()
	pacer := &fedstatPacer{pause: fedstatMacroPause}
	var all []fedstatAgriPoint
	for _, item := range fedstatAgriSources {
		points, err := fedstatAgriFetchSource(ctx, conn, client, pacer, item)
		if err != nil {
			return 0, err
		}
		all = append(all, points...)
	}

	if len(all) == 0 {
		log.Infof("%s: новых точек нет", fedstatAgriTable)
		if err = s.AfterImport(ctx, conn); err != nil {
			return 0, err
		}
		log.Infof("Imported 0 rows of %s", s.Name())
		return 0, nil
	}
	all, err = fedstatDedupPoints(fedstatAgriTable, all, func(p fedstatAgriPoint) string { return p.key() }, func(p fedstatAgriPoint) float64 { return p.value })
	if err != nil {
		return 0, err
	}
	count, err = fedstatInsert(ctx, conn, fedstatAgriTable, all, func(batch driver.Batch, point fedstatAgriPoint) error {
		return batch.Append(point.series, point.activity, point.category, point.region, point.periodType, point.date, point.value, point.unit)
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

func fedstatAgriFetchSource(ctx context.Context, conn driver.Conn, client *http.Client, pacer *fedstatPacer, item fedstatAgriSource) ([]fedstatAgriPoint, error) {
	stored, maxDate, has, err := fedstatTableState(ctx, conn, fedstatAgriTable, item.where)
	if err != nil {
		return nil, err
	}
	full := !has || stored < item.minPoints
	if item.frozen && !full {
		log.Infof("%s: %s на месте (%d точек), сеть не нужна", fedstatAgriTable, item.source.indicator, stored)
		return nil, nil
	}

	var points []fedstatAgriPoint
	if item.source.batch {
		result, err := fedstatFetchBatch(ctx, client, item.source, pacer)
		if err != nil {
			return nil, err
		}
		for _, record := range result.records {
			point, err := fedstatAgriPointFrom(item, record, result.region)
			if err != nil {
				return nil, fmt.Errorf("%s: %s: %w", fedstatAgriTable, item.source.indicator, err)
			}
			points = append(points, point)
		}
		log.Infof("%s: %s: %d точек", fedstatAgriTable, item.source.indicator, len(result.records))
	} else {
		years := item.source.years
		if !full {
			years = fedstatYearsFrom(maxDate.Year(), years)
		}
		if len(years) == 0 {
			return nil, nil
		}
		points, err = fedstatFetchYears(ctx, client, pacer, fedstatAgriTable, item.source, years, func(record fedstatRecord, usedRegion string) (fedstatAgriPoint, error) {
			return fedstatAgriPointFrom(item, record, usedRegion)
		})
		if err != nil {
			return nil, err
		}
	}
	points, err = fedstatDedupPoints(fedstatAgriTable, points, func(p fedstatAgriPoint) string { return p.key() }, func(p fedstatAgriPoint) float64 { return p.value })
	if err != nil {
		return nil, err
	}
	if err = fedstatRequireFullFetch(fedstatAgriTable, item.source.indicator, full, len(points)); err != nil {
		return nil, err
	}
	if full || len(points) == 0 || item.source.batch {
		return points, nil
	}
	existing, err := fedstatAgriExistingKeys(ctx, conn, item.where, time.Date(maxDate.Year(), time.January, 1, 0, 0, 0, 0, time.UTC))
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

func fedstatAgriPointFrom(item fedstatAgriSource, record fedstatRecord, usedRegion string) (fedstatAgriPoint, error) {
	point := fedstatAgriPoint{
		series: item.series, activity: item.activity, periodType: record.periodType,
		date: record.date, value: record.value, unit: item.unit,
	}
	switch item.source.indicator {
	case "34128":
		if len(record.labels) < 1 {
			return fedstatAgriPoint{}, fmt.Errorf("в строке нет категории")
		}
		point.category = record.labels[0]
		point.region = fedstatRegionName(usedRegion)
	case "59001":
		if len(record.labels) < 2 {
			return fedstatAgriPoint{}, fmt.Errorf("в строке %d подписей, ожидалось 2", len(record.labels))
		}
		point.region = record.labels[0]
		point.category = record.labels[1]
		point.activity = record.activity
	case "31062":
		if len(record.labels) < 3 {
			return fedstatAgriPoint{}, fmt.Errorf("в строке %d подписей, ожидалось 3", len(record.labels))
		}
		point.category = record.labels[0]
		point.activity = record.labels[1]
		point.region = record.labels[2]
	case "43337":
		if len(record.labels) < 3 {
			return fedstatAgriPoint{}, fmt.Errorf("в строке %d подписей, ожидалось 3", len(record.labels))
		}
		point.category = record.labels[0]
		point.region = record.labels[1]
		if record.labels[2] != "" {
			point.unit = record.labels[2]
		}
	default:
		return fedstatAgriPoint{}, fmt.Errorf("неизвестный индикатор %s", item.source.indicator)
	}
	if point.region == "" {
		point.region = fedstatRegionName(usedRegion)
	}
	return point, nil
}

func fedstatAgriExistingKeys(ctx context.Context, conn driver.Conn, where string, from time.Time) (map[string]bool, error) {
	query := `SELECT series, activity, category, region, date FROM fedstat_agri FINAL WHERE ` + where + ` AND date >= ?`
	rows, err := conn.Query(ctx, query, from)
	if err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatAgriTable, err)
	}
	defer func() { _ = rows.Close() }()
	existing := make(map[string]bool)
	for rows.Next() {
		var series, activity, category, region string
		var date time.Time
		if err = rows.Scan(&series, &activity, &category, &region, &date); err != nil {
			return nil, fmt.Errorf("%s: ключ не прочитан: %w", fedstatAgriTable, err)
		}
		existing[fedstatPointKey(series, activity, category, region, date.Format("2006-01-02"))] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatAgriTable, err)
	}
	return existing, nil
}

func afterFedstatAgriMacro(ctx context.Context, conn driver.Conn) error {
	meta, err := fedstatAgriCatalog(ctx, conn)
	if err != nil {
		return err
	}
	if len(meta) > 0 {
		if err = util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
	}
	_, err = util.CreateView(ctx, conn, fedstatAgriView)
	return err
}

func fedstatAgriCatalog(ctx context.Context, conn driver.Conn) ([]util.SeriesMeta, error) {
	rows, err := conn.Query(ctx, `SELECT DISTINCT series, activity, category
FROM fedstat_agri FINAL ORDER BY series, activity, category`)
	if err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatAgriTable, err)
	}
	defer func() { _ = rows.Close() }()
	var meta []util.SeriesMeta
	for rows.Next() {
		var series, activity, category string
		if err = rows.Scan(&series, &activity, &category); err != nil {
			return nil, fmt.Errorf("%s: каталог: %w", fedstatAgriTable, err)
		}
		frequency := "A"
		unit := "процент"
		indicator := "59001/31062"
		if series == "index_q" {
			frequency = "Q"
			indicator = "34128"
		}
		if series == "value_y" {
			unit = "миллион рублей"
			indicator = "43337"
		}
		meta = append(meta, util.SeriesMeta{
			Source:    fedstatSource,
			Series:    fmt.Sprintf("agri/%s/%s/%s", series, activity, category),
			Title:     fmt.Sprintf("%s — %s — %s", series, activity, category),
			Unit:      unit,
			Frequency: frequency,
			Origin:    fmt.Sprintf("fedstat.ru/indicator/%s, all-dims 57940/58423, region 57831", indicator),
			Description: fmt.Sprintf("Продукция сельского хозяйства: %s, %s. %s. %s", activity, category,
				unitDescription(unit), agriNote(series)),
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatAgriTable, err)
	}
	return meta, nil
}

func unitDescription(unit string) string {
	if unit == "процент" {
		return "Индекс, процент; 100 = без изменения"
	}
	return "Стоимость продукции, миллион рублей"
}

func agriNote(series string) string {
	switch series {
	case "index_q":
		return "Квартальный индекс с 2018 г.; история до 2018 — только годовые ряды index_y. 2023+ — РФ без учёта новых субъектов (1849012)"
	case "index_y":
		return "Годовой индекс; 59001 — 2017–2025, 31062 — 1991–2016; ряды не склеивать без оговорки. 2023+ — РФ без учёта новых субъектов (1849012)"
	default:
		return "Стоимость в фактически действовавших ценах; фактические данные 1990–2011 (пробы 2012+ источник не отдаёт). 2023+ не публикуется"
	}
}
