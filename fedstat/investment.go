package fedstat

// fedstat_investment — инвестиции в основной капитал ЕМИСС: 58089 (индекс,
// 2017–2026), 58090 (объём, 2017–2026), 34129 (годовой индекс, 2000–2024),
// 40562 (годовой объём, 2005–2025), 34475 (годовой объём, 1998–2016).
// period — подпись периода: у накопительных периодов дата начала одна, без
// period ReplacingMergeTree схлопнул бы «январь-март»/«январь-июнь»/….
// Нули 58090 2024 Jan–Jun/Q2 = 0.0 сохраняются как в источнике.

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
	fedstatInvestmentTable = "fedstat_investment"

	fedstatInvestmentDdl = `CREATE TABLE IF NOT EXISTS %s (
    series        LowCardinality(String),  -- 'index' | 'value'
    indicator_id  LowCardinality(String),  -- '58089' | '58090' | '34129' | '40562' | '34475'
    summary_type  LowCardinality(String),  -- 'Полный круг' | 'Всего'
    region        LowCardinality(String),
    period_type   LowCardinality(String),  -- 'Q' | 'CUM' | 'Y'
    period        LowCardinality(String),  -- 'январь-июнь' | 'II квартал' | …
    date          Date,
    value         Float64,
    unit          LowCardinality(String)   -- 'процент' | 'тысяча рублей' | 'миллион рублей'
) ENGINE = ReplacingMergeTree ORDER BY (series, indicator_id, summary_type, region, period_type, period, date);`

	fedstatInvestment58089Min = 41
	fedstatInvestment58090Min = 47
	fedstatInvestment34129Min = 25
	fedstatInvestment40562Min = 21
	fedstatInvestment34475Min = 19
)

type fedstatInvestmentPoint struct {
	series      string
	indicatorID string
	summaryType string
	region      string
	periodType  string
	period      string
	date        time.Time
	value       float64
	unit        string
}

func (p fedstatInvestmentPoint) key() string {
	return fedstatPointKey(p.series, p.indicatorID, p.summaryType, p.region, p.periodType, p.period, p.date.Format("2006-01-02"))
}

type fedstatInvestmentSource struct {
	source      fedstatSourceSpec
	extra       fedstatSourceSpec // дополнительные годы вне batch (34129/40562)
	indicatorID string
	series      string
	summaryType string
	unit        string
	minPoints   int64
	frozen      bool
	where       string
}

var fedstatInvestmentSources = []fedstatInvestmentSource{
	{
		source: fedstatSourceSpec{
			indicator: "58089", title: "fedstat_investment", layout: fedstatLayoutMatrix, labelCount: 1,
			left: []string{"57831"}, top: []string{"3", "33560"}, filter: []string{"30611", "57936", "57940"},
			unitDim: "30611", unitCode: "950473", regionDim: "57831",
			fixed:        []fedstatFilter{{"57940", "1692933"}, {"57936", "1704497"}},
			years:        fedstatYearRange(2017, 2026),
			regionSwitch: true,
			pause:        fedstatMacroPause,
		},
		indicatorID: "58089", series: "index", summaryType: "Полный круг", unit: "процент",
		minPoints: fedstatInvestment58089Min, where: "indicator_id = '58089'",
	},
	{
		source: fedstatSourceSpec{
			indicator: "58090", title: "fedstat_investment", layout: fedstatLayoutMatrix, labelCount: 3,
			left: []string{"57831", "58121", "58097"}, top: []string{"3", "33560"},
			filter:  []string{"30611", "57936", "57940"},
			unitDim: "30611", unitCode: "950352", regionDim: "57831",
			fixed:        []fedstatFilter{{"57940", "1692933"}, {"57936", "1704497"}, {"58097", "1704559"}, {"58121", "1704611"}},
			years:        fedstatYearRange(2017, 2026),
			regionSwitch: true,
			pause:        fedstatMacroPause,
		},
		indicatorID: "58090", series: "value", summaryType: "Полный круг", unit: "тысяча рублей",
		minPoints: fedstatInvestment58090Min, where: "indicator_id = '58090'",
	},
	{
		source: fedstatSourceSpec{
			indicator: "34129", title: "fedstat_investment", layout: fedstatLayoutYearColumns, labelCount: 1,
			left: []string{"57831"}, top: []string{"3"}, filter: []string{"30611", "33560"},
			unitDim: "30611", unitCode: "950473", regionDim: "57831",
			fixed: []fedstatFilter{{"33560", "1558883"}},
			years: fedstatYearRange(2000, 2022),
			batch: true,
			pause: fedstatMacroPause,
		},
		extra: fedstatSourceSpec{
			indicator: "34129", title: "fedstat_investment", layout: fedstatLayoutYearColumns, labelCount: 1,
			left: []string{"57831"}, top: []string{"3"}, filter: []string{"30611", "33560"},
			unitDim: "30611", unitCode: "950473", regionDim: "57831",
			fixed:        []fedstatFilter{{"33560", "1558883"}},
			years:        fedstatYearRange(2023, 2024),
			regionSwitch: true,
			pause:        fedstatMacroPause,
		},
		indicatorID: "34129", series: "index", summaryType: "Всего", unit: "процент",
		minPoints: fedstatInvestment34129Min, frozen: true, where: "indicator_id = '34129'",
	},
	{
		source: fedstatSourceSpec{
			indicator: "40562", title: "fedstat_investment", layout: fedstatLayoutYearColumns, labelCount: 1,
			left: []string{"57831"}, top: []string{"3"}, filter: []string{"33560", "30611"},
			unitDim: "30611", unitCode: "950353", regionDim: "57831",
			fixed: []fedstatFilter{{"33560", "1558883"}},
			years: fedstatYearRange(2005, 2022),
			batch: true,
			pause: fedstatMacroPause,
		},
		extra: fedstatSourceSpec{
			indicator: "40562", title: "fedstat_investment", layout: fedstatLayoutYearColumns, labelCount: 1,
			left: []string{"57831"}, top: []string{"3"}, filter: []string{"33560", "30611"},
			unitDim: "30611", unitCode: "950353", regionDim: "57831",
			fixed:        []fedstatFilter{{"33560", "1558883"}},
			years:        fedstatYearRange(2023, 2025),
			regionSwitch: true,
			pause:        fedstatMacroPause,
		},
		indicatorID: "40562", series: "value", summaryType: "Всего", unit: "миллион рублей",
		minPoints: fedstatInvestment40562Min, frozen: true, where: "indicator_id = '40562'",
	},
	{
		source: fedstatSourceSpec{
			indicator: "34475", title: "fedstat_investment", layout: fedstatLayoutYearColumns, labelCount: 2,
			left: []string{"57831", "58121"}, top: []string{"3"}, filter: []string{"30611", "33560"},
			unitDim: "30611", unitCode: "950353", regionDim: "57831",
			fixed: []fedstatFilter{{"33560", "1558883"}, {"58121", "1704611"}},
			years: fedstatYearRange(1998, 2016),
			batch: true,
			pause: fedstatMacroPause,
		},
		indicatorID: "34475", series: "value", summaryType: "Полный круг", unit: "миллион рублей",
		minPoints: fedstatInvestment34475Min, frozen: true, where: "indicator_id = '34475'",
	},
}

type FedstatInvestmentStat struct {
	util.ClickHouseImport
}

var fedstatInvestmentImporter = &FedstatInvestmentStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   fedstatInvestmentTable,
	CreateTable: []string{fedstatInvestmentDdl},
	AfterImport: afterFedstatInvestmentMacro,
}}

func (s *FedstatInvestmentStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", fedstatInvestmentTable, err)
		}
	}

	client := fedstatMacroClient()
	pacer := &fedstatPacer{pause: fedstatMacroPause}
	var all []fedstatInvestmentPoint
	for _, item := range fedstatInvestmentSources {
		points, err := fedstatInvestmentFetchSource(ctx, conn, client, pacer, item)
		if err != nil {
			return 0, err
		}
		all = append(all, points...)
	}

	if len(all) == 0 {
		log.Infof("%s: новых точек нет", fedstatInvestmentTable)
		if err = s.AfterImport(ctx, conn); err != nil {
			return 0, err
		}
		log.Infof("Imported 0 rows of %s", s.Name())
		return 0, nil
	}
	all, err = fedstatDedupPoints(fedstatInvestmentTable, all, func(p fedstatInvestmentPoint) string { return p.key() }, func(p fedstatInvestmentPoint) float64 { return p.value })
	if err != nil {
		return 0, err
	}
	count, err = fedstatInsert(ctx, conn, fedstatInvestmentTable, all, func(batch driver.Batch, point fedstatInvestmentPoint) error {
		return batch.Append(point.series, point.indicatorID, point.summaryType, point.region, point.periodType, point.period, point.date, point.value, point.unit)
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

func fedstatInvestmentFetchSource(ctx context.Context, conn driver.Conn, client *http.Client, pacer *fedstatPacer, item fedstatInvestmentSource) ([]fedstatInvestmentPoint, error) {
	stored, maxDate, has, err := fedstatTableState(ctx, conn, fedstatInvestmentTable, item.where)
	if err != nil {
		return nil, err
	}
	full := !has || stored < item.minPoints
	if item.frozen && !full {
		log.Infof("%s: %s на месте (%d точек), сеть не нужна", fedstatInvestmentTable, item.indicatorID, stored)
		return nil, nil
	}
	var points []fedstatInvestmentPoint
	if item.source.batch {
		result, err := fedstatFetchBatch(ctx, client, item.source, pacer)
		if err != nil {
			return nil, err
		}
		for _, record := range result.records {
			point, err := fedstatInvestmentPointFrom(item, record, result.region)
			if err != nil {
				return nil, fmt.Errorf("%s: %s: %w", fedstatInvestmentTable, item.indicatorID, err)
			}
			points = append(points, point)
		}
		if len(item.extra.years) > 0 {
			extra, err := fedstatFetchYears(ctx, client, pacer, fedstatInvestmentTable, item.extra, item.extra.years, func(record fedstatRecord, usedRegion string) (fedstatInvestmentPoint, error) {
				return fedstatInvestmentPointFrom(item, record, usedRegion)
			})
			if err != nil {
				return nil, err
			}
			points = append(points, extra...)
		}
	} else {
		years := item.source.years
		if !full {
			years = fedstatYearsFrom(maxDate.Year(), years)
		}
		if len(years) == 0 {
			return nil, nil
		}
		points, err = fedstatFetchYears(ctx, client, pacer, fedstatInvestmentTable, item.source, years, func(record fedstatRecord, usedRegion string) (fedstatInvestmentPoint, error) {
			return fedstatInvestmentPointFrom(item, record, usedRegion)
		})
		if err != nil {
			return nil, err
		}
	}
	points, err = fedstatDedupPoints(fedstatInvestmentTable, points, func(p fedstatInvestmentPoint) string { return p.key() }, func(p fedstatInvestmentPoint) float64 { return p.value })
	if err != nil {
		return nil, err
	}
	if err = fedstatRequireFullFetch(fedstatInvestmentTable, item.indicatorID, full, len(points)); err != nil {
		return nil, err
	}
	if full || len(points) == 0 || item.source.batch {
		return points, nil
	}
	existing, err := fedstatInvestmentExistingKeys(ctx, conn, item.indicatorID, time.Date(maxDate.Year(), time.January, 1, 0, 0, 0, 0, time.UTC))
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

func fedstatInvestmentPointFrom(item fedstatInvestmentSource, record fedstatRecord, usedRegion string) (fedstatInvestmentPoint, error) {
	point := fedstatInvestmentPoint{
		series: item.series, indicatorID: item.indicatorID, summaryType: item.summaryType,
		periodType: record.periodType, period: record.period, date: record.date, value: record.value, unit: item.unit,
	}
	required := 1
	if item.indicatorID == "58090" {
		required = 3
	}
	if item.indicatorID == "34475" {
		required = 2
	}
	if len(record.labels) < required {
		return fedstatInvestmentPoint{}, fmt.Errorf("в строке %d подписей, ожидалось %d", len(record.labels), required)
	}
	point.region = record.labels[0]
	if point.region == "" {
		point.region = fedstatRegionName(usedRegion)
	}
	return point, nil
}

func fedstatInvestmentExistingKeys(ctx context.Context, conn driver.Conn, indicatorID string, from time.Time) (map[string]bool, error) {
	rows, err := conn.Query(ctx, `SELECT series, indicator_id, summary_type, region, period_type, period, date
FROM fedstat_investment FINAL WHERE indicator_id = ? AND date >= ?`, indicatorID, from)
	if err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatInvestmentTable, err)
	}
	defer func() { _ = rows.Close() }()
	existing := make(map[string]bool)
	for rows.Next() {
		var series, indicatorValue, summaryType, region, periodType, period string
		var date time.Time
		if err = rows.Scan(&series, &indicatorValue, &summaryType, &region, &periodType, &period, &date); err != nil {
			return nil, fmt.Errorf("%s: ключ не прочитан: %w", fedstatInvestmentTable, err)
		}
		existing[fedstatPointKey(series, indicatorValue, summaryType, region, periodType, period, date.Format("2006-01-02"))] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatInvestmentTable, err)
	}
	return existing, nil
}

func afterFedstatInvestmentMacro(ctx context.Context, conn driver.Conn) error {
	meta, err := fedstatInvestmentCatalog(ctx, conn)
	if err != nil {
		return err
	}
	if len(meta) > 0 {
		if err = util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
	}
	_, err = util.CreateView(ctx, conn, fedstatInvestmentView)
	return err
}

func fedstatInvestmentCatalog(ctx context.Context, conn driver.Conn) ([]util.SeriesMeta, error) {
	rows, err := conn.Query(ctx, `SELECT DISTINCT series, indicator_id, summary_type
FROM fedstat_investment FINAL ORDER BY series, indicator_id, summary_type`)
	if err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatInvestmentTable, err)
	}
	defer func() { _ = rows.Close() }()
	var meta []util.SeriesMeta
	for rows.Next() {
		var series, indicatorID, summaryType string
		if err = rows.Scan(&series, &indicatorID, &summaryType); err != nil {
			return nil, fmt.Errorf("%s: каталог: %w", fedstatInvestmentTable, err)
		}
		unit := "процент"
		frequency := "Q"
		switch indicatorID {
		case "58090":
			unit = "тысяча рублей"
		case "40562", "34475":
			unit = "миллион рублей"
			frequency = "A"
		case "34129":
			frequency = "A"
		}
		meta = append(meta, util.SeriesMeta{
			Source:    fedstatSource,
			Series:    fmt.Sprintf("investment/%s/%s/%s", series, indicatorID, summaryType),
			Title:     fmt.Sprintf("%s — %s — %s", series, indicatorID, summaryType),
			Unit:      unit,
			Frequency: frequency,
			Origin:    fmt.Sprintf("fedstat.ru/indicator/%s, all-dims 57940/57936, region 57831", indicatorID),
			Description: fmt.Sprintf("Инвестиции в основной капитал: %s, %s. %s. %s", series, summaryType,
				investUnitDescription(unit), investNote(indicatorID)),
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatInvestmentTable, err)
	}
	return meta, nil
}

func investUnitDescription(unit string) string {
	switch unit {
	case "процент":
		return "Индекс физического объёма, процент; 100 = без изменения"
	case "тысяча рублей":
		return "Объём инвестиций, тысяча рублей"
	default:
		return "Объём инвестиций, миллион рублей"
	}
}

func investNote(indicatorID string) string {
	switch indicatorID {
	case "58089", "58090":
		return "2017–2026; 2023+ — РФ без учёта новых субъектов (1849012), до 2023 — РФ (1688487). period_type Q — квартал, CUM — накопительный период; не смешивать. В 58090 2024 Jan–Jun/Q2 = 0.0 — сохраняется как в источнике, не использовать как уровень до подтверждения"
	case "34129":
		return "Годовой индекс 2000–2024. Ряд не продолжает 58089: другая методология и граница 2017 г."
	case "40562":
		return "Годовой объём 2005–2025, уточнённые данные. Ряд не продолжает 58090"
	default:
		return "Годовой объём 1998–2016 по полному кругу организаций; история до 2017 г."
	}
}
