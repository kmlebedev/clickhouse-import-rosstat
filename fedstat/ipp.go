package fedstat

// fedstat_ipp — индекс промышленного производства ЕМИСС: 43046 (ОКВЭД1,
// 2000–2016) и 57806 (ОКВЭД2, 2015–2026). Ряды не склеиваются: overlap
// 2015–2016 даёт mean abs diff 1,05 п.п. (max 3,8) — это шов классификаторов,
// а не продолжение одного ряда. 3 вида показателя: м/м, г/г, с начала года.
//
// 2023+ РФ публикуется под 1849012 («без учёта новых субъектов»), до 2023 —
// 1688487; region — часть ключа, потому что это разные определения совокупности.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	fedstatIPPTable = "fedstat_ipp"

	fedstatIPPDdl = `CREATE TABLE IF NOT EXISTS %s (
    okved_version LowCardinality(String),  -- 'OKVED1' | 'OKVED2'
    okved         LowCardinality(String),  -- вид деятельности; у 43046 — 'Промышленность'
    indicator_type LowCardinality(String), -- 'К предыдущему месяцу' | '…к соответствующему…' | '…с начала года…'
    region        LowCardinality(String),
    date          Date,                    -- первое число месяца
    value         Float64                  -- индекс, 100 = без изменения
) ENGINE = ReplacingMergeTree ORDER BY (okved_version, okved, indicator_type, region, date);`

	fedstatIPP43046Min = 612
	fedstatIPP57806Min = 55356
	fedstatIPPExpected = 55968
)

type fedstatIPPPoint struct {
	okvedVersion  string
	okved         string
	indicatorType string
	region        string
	date          time.Time
	value         float64
}

func (p fedstatIPPPoint) key() string {
	return fedstatPointKey(p.okvedVersion, p.okved, p.indicatorType, p.region, p.date.Format("2006-01-02"))
}

type fedstatIPPSource struct {
	source       fedstatSourceSpec
	okvedVersion string
	frozen       bool
	minPoints    int64
}

var fedstatIPPSources = []fedstatIPPSource{
	{
		source: fedstatSourceSpec{
			indicator: "43046", title: "fedstat_ipp", layout: fedstatLayoutLong, labelCount: 3,
			left: []string{"33560", "57937", "57831", "58071"}, top: []string{"3"},
			unitDim: "30611", unitCode: "950473", regionDim: "57831",
			fixed:   []fedstatFilter{{"58071", "1702101"}},
			allDims: []string{"57937"},
			years:   fedstatYearRange(2000, 2016),
			pause:   fedstatMacroPause,
		},
		okvedVersion: "OKVED1", frozen: true, minPoints: fedstatIPP43046Min,
	},
	{
		source: fedstatSourceSpec{
			indicator: "57806", title: "fedstat_ipp", layout: fedstatLayoutMatrix, labelCount: 2,
			left: []string{"57940", "57937"}, top: []string{"3", "33560"}, filter: []string{"30611"},
			unitDim: "30611", unitCode: "950473", regionDim: "57831",
			allDims:      []string{"57940", "57937"},
			years:        fedstatYearRange(2015, 2026),
			regionSwitch: true,
			pause:        fedstatMacroPause,
		},
		okvedVersion: "OKVED2", minPoints: fedstatIPP57806Min,
	},
}

// FedstatIPPStat — импортёр; шаблон даёт Name(), DDL и AfterImport.
type FedstatIPPStat struct {
	util.ClickHouseImport
}

var fedstatIPPImporter = &FedstatIPPStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   fedstatIPPTable,
	CreateTable: []string{fedstatIPPDdl},
	AfterImport: afterFedstatIPPMacro,
}}

// Import — DDL -> гейт по источникам -> загрузка -> дедуп -> батч -> производные.
func (s *FedstatIPPStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", fedstatIPPTable, err)
		}
	}

	client := util.NewDefaultTLSClient()
	pacer := &fedstatPacer{pause: fedstatMacroPause}
	var all []fedstatIPPPoint
	for _, item := range fedstatIPPSources {
		points, err := fedstatIPPFetchSource(ctx, conn, client, pacer, item)
		if err != nil {
			return 0, err
		}
		all = append(all, points...)
	}

	if len(all) == 0 {
		log.Infof("%s: новых точек нет", fedstatIPPTable)
		if err = s.AfterImport(ctx, conn); err != nil {
			return 0, err
		}
		log.Infof("Imported 0 rows of %s", s.Name())
		return 0, nil
	}
	all, err = fedstatIPPDedup(all)
	if err != nil {
		return 0, err
	}
	count, err = fedstatInsert(ctx, conn, fedstatIPPTable, all, func(batch driver.Batch, point fedstatIPPPoint) error {
		return batch.Append(point.okvedVersion, point.okved, point.indicatorType, point.region, point.date, point.value)
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

func fedstatIPPFetchSource(ctx context.Context, conn driver.Conn, client *http.Client, pacer *fedstatPacer, item fedstatIPPSource) ([]fedstatIPPPoint, error) {
	where := fmt.Sprintf("okved_version = '%s'", item.okvedVersion)
	stored, maxDate, has, err := fedstatTableState(ctx, conn, fedstatIPPTable, where)
	if err != nil {
		return nil, err
	}
	full := !has || stored < item.minPoints
	if item.frozen && !full {
		log.Infof("%s: %s на месте (%d точек), сеть не нужна", fedstatIPPTable, item.okvedVersion, stored)
		return nil, nil
	}
	years := item.source.years
	if !full {
		years = fedstatYearsFrom(maxDate.Year(), years)
	}
	if len(years) == 0 {
		return nil, nil
	}
	var points []fedstatIPPPoint
	for _, year := range years {
		result, err := fedstatFetchYear(ctx, client, item.source, year, pacer)
		if errors.Is(err, errFedstatNoData) {
			log.Warnf("%s: %s/%d: данных нет ни по одному коду РФ", fedstatIPPTable, item.okvedVersion, year)
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, record := range result.records {
			point, err := fedstatIPPPointFrom(item, record, result.region)
			if err != nil {
				return nil, fmt.Errorf("%s: %s/%d: %w", fedstatIPPTable, item.okvedVersion, year, err)
			}
			points = append(points, point)
		}
		log.Infof("%s: %s/%d: %d точек", fedstatIPPTable, item.okvedVersion, year, len(result.records))
	}
	points, err = fedstatIPPDedup(points)
	if err != nil {
		return nil, err
	}
	if err = fedstatRequireFullFetch(fedstatIPPTable, item.okvedVersion, full, len(points)); err != nil {
		return nil, err
	}
	if full || len(points) == 0 {
		return points, nil
	}
	existing, err := fedstatIPPExistingKeys(ctx, conn, item.okvedVersion, time.Date(maxDate.Year(), time.January, 1, 0, 0, 0, 0, time.UTC))
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

func fedstatIPPPointFrom(item fedstatIPPSource, record fedstatRecord, usedRegion string) (fedstatIPPPoint, error) {
	if item.okvedVersion == "OKVED1" {
		if len(record.labels) < 3 {
			return fedstatIPPPoint{}, fmt.Errorf("в строке %d подписей, ожидалось 3", len(record.labels))
		}
		region := record.labels[1]
		if region == "" {
			region = fedstatRegionName(usedRegion)
		}
		return fedstatIPPPoint{
			okvedVersion: "OKVED1", okved: "Промышленность", indicatorType: record.labels[0],
			region: region, date: record.date, value: record.value,
		}, nil
	}
	if len(record.labels) < 2 {
		return fedstatIPPPoint{}, fmt.Errorf("в строке %d подписей, ожидалось 2", len(record.labels))
	}
	return fedstatIPPPoint{
		okvedVersion: "OKVED2", okved: record.labels[0], indicatorType: record.labels[1],
		region: fedstatRegionName(usedRegion), date: record.date, value: record.value,
	}, nil
}

func fedstatIPPDedup(points []fedstatIPPPoint) ([]fedstatIPPPoint, error) {
	seen := make(map[string]float64, len(points))
	out := make([]fedstatIPPPoint, 0, len(points))
	for _, point := range points {
		key := point.key()
		if previous, ok := seen[key]; ok {
			if previous != point.value {
				return nil, fmt.Errorf("%s: дубль %s с разными значениями %v и %v", fedstatIPPTable, key, previous, point.value)
			}
			continue
		}
		seen[key] = point.value
		out = append(out, point)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out, nil
}

func fedstatIPPExistingKeys(ctx context.Context, conn driver.Conn, version string, from time.Time) (map[string]bool, error) {
	rows, err := conn.Query(ctx, `SELECT okved_version, okved, indicator_type, region, date
FROM fedstat_ipp FINAL WHERE okved_version = ? AND date >= ?`, version, from)
	if err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatIPPTable, err)
	}
	defer func() { _ = rows.Close() }()
	existing := make(map[string]bool)
	for rows.Next() {
		var versionValue, okved, indicatorType, region string
		var date time.Time
		if err = rows.Scan(&versionValue, &okved, &indicatorType, &region, &date); err != nil {
			return nil, fmt.Errorf("%s: ключ не прочитан: %w", fedstatIPPTable, err)
		}
		existing[fedstatPointKey(versionValue, okved, indicatorType, region, date.Format("2006-01-02"))] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: существующие ключи: %w", fedstatIPPTable, err)
	}
	return existing, nil
}

func fedstatYearsFrom(year int, years []int) []int {
	out := make([]int, 0, len(years))
	for _, candidate := range years {
		if candidate >= year {
			out = append(out, candidate)
		}
	}
	return out
}

func afterFedstatIPPMacro(ctx context.Context, conn driver.Conn) error {
	meta, err := fedstatIPPCatalog(ctx, conn)
	if err != nil {
		return err
	}
	if len(meta) > 0 {
		if err = util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
	}
	_, err = util.CreateView(ctx, conn, fedstatIPPView)
	return err
}

func fedstatIPPCatalog(ctx context.Context, conn driver.Conn) ([]util.SeriesMeta, error) {
	rows, err := conn.Query(ctx, `SELECT DISTINCT okved_version, okved, indicator_type
FROM fedstat_ipp FINAL ORDER BY okved_version, okved, indicator_type`)
	if err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatIPPTable, err)
	}
	defer func() { _ = rows.Close() }()
	var meta []util.SeriesMeta
	for rows.Next() {
		var version, okved, indicatorType string
		if err = rows.Scan(&version, &okved, &indicatorType); err != nil {
			return nil, fmt.Errorf("%s: каталог: %w", fedstatIPPTable, err)
		}
		indicator := "57806"
		note := "2015–2026, ОКВЭД2; 2023+ — РФ без учёта новых субъектов (1849012), до 2023 — РФ (1688487)."
		if version == "OKVED1" {
			indicator = "43046"
			note = "2000–2016, ОКВЭД1; агрегат «Промышленность». 2015–2016 пересекается с 57806, но ряды не идентичны (mean abs diff 1,05 п.п.) — шов классификаторов, не продолжение."
		}
		meta = append(meta, util.SeriesMeta{
			Source:      fedstatSource,
			Series:      fmt.Sprintf("ipp/%s/%s/%s", version, okved, indicatorType),
			Title:       okved + " — " + indicatorType,
			Unit:        "процент",
			Frequency:   "M",
			Origin:      fmt.Sprintf("fedstat.ru/indicator/%s, okved_version=%s, all-dims 57940/57937, region 57831", indicator, version),
			Description: fmt.Sprintf("Индекс промышленного производства: %s, вид показателя «%s». 100 = без изменения; значения — индексы, не рубли. Не складывать м/м, г/г и «с начала года» в один ряд. %s", okved, indicatorType, note),
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: каталог: %w", fedstatIPPTable, err)
	}
	return meta, nil
}
