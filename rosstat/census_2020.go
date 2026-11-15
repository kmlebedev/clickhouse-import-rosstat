package rosstat

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const census2020Table = "rosstat_census_2020"
const census2020URL = "https://rosstat.gov.ru/storage/mediabank/Tom2_tab2_VPN-2020.xlsx"
const census2020Count = "SELECT count() FROM rosstat_census_2020 FINAL"
const census2020DDL = `CREATE TABLE IF NOT EXISTS %s (
    territory LowCardinality(String),
    territory_kind LowCardinality(String),
    settlement LowCardinality(String),
    sex LowCardinality(String),
    age_group LowCardinality(String),
    reference_date Date,
    value UInt64,
    loaded_at DateTime
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (territory, settlement, sex, age_group, reference_date);`

type RosstatCensus2020Stat struct{ util.ClickHouseImport }

var census2020Importer = &RosstatCensus2020Stat{util.ClickHouseImport{
	TableName: census2020Table, DataUrl: census2020URL, CreateTable: []string{census2020DDL},
	PublicationSource: census2020Table, PublicationCountPoints: census2020Count,
	ImportFunc: importCensus2020, AfterImport: afterCensus2020,
}}

func importCensus2020(book *excelize.File, batch driver.Batch) error {
	points, err := parseCensus2020(book)
	if err != nil {
		return err
	}
	loaded := time.Now()
	reference := time.Date(2021, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, p := range points {
		if err = batch.Append(p.territory, p.kind, p.settlement, p.sex, p.age, reference, p.value, loaded); err != nil {
			return fmt.Errorf("census append: %w", err)
		}
	}
	return nil
}
func afterCensus2020(ctx context.Context, conn driver.Conn) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, census2020Meta); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, census2020View)
	return err
}
