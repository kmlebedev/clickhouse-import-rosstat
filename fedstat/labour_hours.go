package fedstat

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"path"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const fedstatLabourHoursTable = "fedstat_labour_hours"
const fedstatLabourHoursDdl = `CREATE TABLE IF NOT EXISTS %s (
    series LowCardinality(String),
    region LowCardinality(String),
    age LowCardinality(String),
    sex LowCardinality(String),
    hours_band LowCardinality(String),
    activity LowCardinality(String),
    employment_status LowCardinality(String),
    unit LowCardinality(String),
    date Date,
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (series, region, age, sex, hours_band, activity, employment_status, date);`

type labourHoursPoint struct {
	series, region, age, sex, hoursBand, activity, status, unit string
	date                                                        time.Time
	value                                                       float64
}

func (p labourHoursPoint) key() string {
	return fedstatPointKey(p.series, p.region, p.age, p.sex, p.hoursBand, p.activity, p.status, p.date.Format("2006-01-02"))
}
func (p labourHoursPoint) append(batch driver.Batch) error {
	return batch.Append(p.series, p.region, p.age, p.sex, p.hoursBand, p.activity, p.status, p.unit, p.date, p.value)
}

type labourHoursDocument struct {
	id, url string
	body    []byte
	points  uint64
}

type FedstatLabourHoursStat struct{ util.ClickHouseImport }

var fedstatLabourHoursImporter = &FedstatLabourHoursStat{util.ClickHouseImport{
	TableName: fedstatLabourHoursTable, CreateTable: []string{fedstatLabourHoursDdl}, AfterImport: afterFedstatLabourHours,
}}

// POST snapshots have no trustworthy HEAD fingerprint. Every run reads the bytes,
// validates all sources, compares FINAL values, and archives those exact bytes.
func (s *FedstatLabourHoursStat) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	if util.ArchiveRestoreMode() {
		return 0, fmt.Errorf("%s: archive restore is not supported", s.Name())
	}
	client := fedstatMacroClient()
	var docs []labourHoursDocument
	var points []labourHoursPoint
	pacer := &fedstatPacer{pause: fedstatMacroPause}
	for _, id := range []string{"59749", "59751", "60463"} {
		if err := pacer.wait(ctx); err != nil {
			return 0, err
		}
		body, err := fetchLabourHours(ctx, client, id)
		if err != nil {
			return 0, err
		}
		parsed, err := parseLabourHours(body, id)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", id, err)
		}
		docs = append(docs, labourHoursDocument{id, fedstatBaseURL + "/indicator/" + id + "/download", body, uint64(len(parsed))})
		points = append(points, parsed...)
	}
	return s.commitDocuments(ctx, conn, docs, points)
}

func (s *FedstatLabourHoursStat) commitDocuments(ctx context.Context, conn driver.Conn, docs []labourHoursDocument, points []labourHoursPoint) (int64, error) {
	if err := conn.Exec(ctx, fmt.Sprintf(fedstatLabourHoursDdl, s.TableName)); err != nil {
		return 0, err
	}
	if err := util.EnsurePublications(ctx, conn); err != nil {
		return 0, err
	}
	existing, err := readLabourHours(ctx, conn)
	if err != nil {
		return 0, err
	}
	delta, err := labourHoursDelta(points, existing)
	if err != nil {
		return 0, err
	}
	count, err := fedstatInsert(ctx, conn, s.TableName, delta, func(b driver.Batch, p labourHoursPoint) error { return p.append(b) })
	if err != nil {
		return count, err
	}
	if err = s.AfterImport(ctx, conn); err != nil {
		return count, err
	}
	store, err := archive.FromEnv(util.HttpClient)
	if err != nil {
		return count, err
	}
	var registry *archive.Registry
	if store != nil {
		registry = archive.NewRegistry(store)
	}
	// Publish version markers only after every archive succeeds.
	var versions []util.Publication
	for _, doc := range docs {
		sum := sha256.Sum256(doc.body)
		fingerprint := hex.EncodeToString(sum[:])
		source := s.Name() + "_" + doc.id
		if store != nil {
			key := archive.Key(source, path.Base(doc.url), fingerprint, sum)
			if err = store.Put(ctx, key, doc.body); err != nil {
				return count, err
			}
			if err = registry.Ensure(ctx, archive.Entry{Source: source, Name: path.Base(doc.url), Fingerprint: fingerprint, SHA256: fingerprint, Size: int64(len(doc.body)), URL: doc.url, Key: key, FirstSeen: time.Now()}); err != nil {
				return count, err
			}
		}
		versions = append(versions, util.Publication{Source: source, Name: path.Base(doc.url), Fingerprint: fingerprint, Url: doc.url, FirstSeen: time.Now(), Points: doc.points})
	}
	for _, v := range versions {
		_, known, err := util.PublicationKnown(ctx, conn, v.Source, v.Name, v.Fingerprint)
		if err != nil {
			return count, err
		}
		if !known {
			if err = util.SavePublication(ctx, conn, v); err != nil {
				return count, err
			}
		}
	}
	log.Infof("Imported %d rows of %s", count, s.Name())
	return count, nil
}

func readLabourHours(ctx context.Context, conn driver.Conn) ([]labourHoursPoint, error) {
	rows, err := conn.Query(ctx, "SELECT series,region,age,sex,hours_band,activity,employment_status,unit,date,value FROM fedstat_labour_hours FINAL")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []labourHoursPoint
	for rows.Next() {
		var p labourHoursPoint
		if err = rows.Scan(&p.series, &p.region, &p.age, &p.sex, &p.hoursBand, &p.activity, &p.status, &p.unit, &p.date, &p.value); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func labourHoursDelta(points, existing []labourHoursPoint) ([]labourHoursPoint, error) {
	incoming := make(map[string]labourHoursPoint, len(points))
	for _, p := range points {
		if old, ok := incoming[p.key()]; ok && (old.value != p.value || old.unit != p.unit) {
			return nil, fmt.Errorf("conflicting duplicate %s", p.key())
		}
		incoming[p.key()] = p
	}
	old := make(map[string]labourHoursPoint, len(existing))
	for _, p := range existing {
		if _, ok := incoming[p.key()]; !ok {
			return nil, fmt.Errorf("source removed existing key %s; refusing partial snapshot", p.key())
		}
		old[p.key()] = p
	}
	var delta []labourHoursPoint
	seen := map[string]bool{}
	for _, p := range points {
		if seen[p.key()] {
			continue
		}
		seen[p.key()] = true
		if prev, ok := old[p.key()]; !ok || prev.value != p.value || prev.unit != p.unit {
			delta = append(delta, p)
		}
	}
	return delta, nil
}

func validateLabourHours(points []labourHoursPoint, id string) error {
	if len(points) == 0 {
		return fmt.Errorf("empty numerical export")
	}
	groups := map[string]map[string]float64{}
	for _, p := range points {
		if math.IsNaN(p.value) || math.IsInf(p.value, 0) || p.value < 0 {
			return fmt.Errorf("invalid value")
		}
		if id == "59751" {
			if p.value > 168 {
				return fmt.Errorf("weekly hours exceed 168")
			}
			continue
		}
		key := fedstatPointKey(p.region, p.age, p.sex, p.activity, p.date.Format("2006"))
		code := strings.Fields(p.hoursBand)
		if id == "60463" {
			code = strings.Fields(p.status)
		}
		if len(code) == 0 {
			return fmt.Errorf("missing category")
		}
		if groups[key] == nil {
			groups[key] = map[string]float64{}
		}
		if _, ok := groups[key][code[0]]; ok {
			return fmt.Errorf("duplicate category %s", key)
		}
		groups[key][code[0]] = p.value
	}
	for key, g := range groups {
		if id == "59749" {
			if len(g) != 9 {
				return fmt.Errorf("incomplete hours bins %s", key)
			}
			for _, code := range []string{"13", "14", "15", "16", "2", "3", "4", "5", "6"} {
				if _, ok := g[code]; !ok {
					return fmt.Errorf("missing expected hours bin %s", code)
				}
			}
			total, ok := g["14"]
			if !ok {
				return fmt.Errorf("missing hours total")
			}
			var sum float64
			for k, v := range g {
				if k != "14" {
					sum += v
				}
			}
			if math.Abs(total-sum) > 0.450001 {
				return fmt.Errorf("hours bins do not sum: %s", key)
			}
		} else {
			total, ok := g["13"]
			hired, h := g["41"]
			other, o := g["12"]
			if len(g) != 3 || !ok || !h || !o || math.Abs(total-hired-other) > 0.150001 {
				return fmt.Errorf("employment status sum failed: %s", key)
			}
		}
	}
	return nil
}
