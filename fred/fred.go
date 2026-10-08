package fred

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type fredImport struct {
}

const (
	fredSource    = "fred"
	fredSeriesUrl = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=%s"
	fredTimeout   = 60 * time.Second
	fredTable     = "macro_series"
	fredDdl       = `CREATE TABLE IF NOT EXISTS ` + fredTable + ` (
			  source LowCardinality(String)
			, series LowCardinality(String)
			, date Date32
			, value Float64
		) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);
	`
	fredInsert     = "INSERT INTO " + fredTable
	fredDateLayout = "2006-01-02"
)

var fredSeries = []string{"DFII10", "DGS10", "FEDFUNDS", "DTWEXBGS", "CPIAUCSL", "T5YIE"}

type observation struct {
	date  time.Time
	value float64
}

func (s *fredImport) Name() string {
	return fredSource
}

func (s *fredImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, fredDdl); err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, fredInsert)
	if err != nil {
		return count, err
	}
	for _, id := range fredSeries {
		observations, err := fetchSeries(ctx, id)
		if err != nil {
			return count, fmt.Errorf("fred %s: %w", id, err)
		}
		for _, o := range observations {
			if err = batch.Append(fredSource, id, o.date, o.value); err != nil {
				return count, err
			}
			count++
		}
		log.Infof("Fetched %d observations of %s", len(observations), id)
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

func fetchSeries(ctx context.Context, id string) ([]observation, error) {
	ctx, cancel := context.WithTimeout(ctx, fredTimeout)
	defer cancel()
	// FRED стоит за Imperva: браузерный User-Agent рвёт соединение, поэтому UA не задаём
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(fredSeriesUrl, id), nil)
	if err != nil {
		return nil, err
	}
	resp, err := util.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	return parseSeries(resp.Body)
}

func parseSeries(body io.Reader) ([]observation, error) {
	records, err := csv.NewReader(body).ReadAll()
	if err != nil {
		return nil, err
	}
	observations := make([]observation, 0, len(records))
	for i, record := range records {
		if i == 0 {
			continue
		}
		// FRED оставляет значение пустым (или ".") для дат без наблюдения: праздники, нет публикации
		if record[1] == "" || record[1] == "." {
			continue
		}
		date, err := time.Parse(fredDateLayout, record[0])
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		value, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		observations = append(observations, observation{date: date, value: value})
	}
	return observations, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &fredImport{})
}
