package surveys

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	mpSurveyName  = "cbr_mp_survey"
	mpSurveyURL   = "https://www.cbr.ru/Content/Document/File/135603/mp_survey_data.xlsx"
	mpSurveyTable = "macro_series"
	// mpSurveySource = "cbr_survey" объявлена в series_meta.go — повторно в пакете нельзя.
)

const mpSurveyDdl = `CREATE TABLE IF NOT EXISTS %s (
    source LowCardinality(String),   -- 'fred','bls','bea','eia','manual','webbridge','imf','wgc','bold','cbr_survey'
    series LowCardinality(String),   -- 'DFII10','distillate_stocks',...
    date Date32,                     -- Date32, т.к. Date не покрывает даты до 1970 (CPIAUCSL с 1947, FEDFUNDS с 1954)
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);`

type mpSurveyStat struct {
	util.ClickHouseImport
}

func (s *mpSurveyStat) Name() string { return mpSurveyName }

func (s *mpSurveyStat) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	count, err := s.ClickHouseImport.Import(ctx, conn)
	if err != nil {
		return count, err
	}
	if err = util.UpsertSeriesCatalog(ctx, conn, mpSurveySeriesMeta); err != nil {
		return count, err
	}
	_, err = util.CreateView(ctx, conn, cbrSurveyView)
	return count, err
}

var importer = &mpSurveyStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   mpSurveyTable,
	CreateTable: []string{mpSurveyDdl},
	DataUrl:     mpSurveyURL,
	ImportFunc: func(xlsx *excelize.File, batch driver.Batch) error {
		points, err := parseSurvey(xlsx, mpSurveySeries)
		if err != nil {
			return err
		}
		log.Infof("Собрано %d точек мониторинга предприятий", len(points))
		for _, p := range points {
			if err = batch.Append(mpSurveySource, p.Series, p.Date, p.Value); err != nil {
				return fmt.Errorf("append %s %s: %w", p.Series, p.Date.Format("2006-01-02"), err)
			}
		}
		return nil
	},
}}

func init() {
	chimport.Stats = append(chimport.Stats, importer)
}
