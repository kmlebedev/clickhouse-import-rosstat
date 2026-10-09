package bls

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type blsImport struct {
}

const (
	blsSource             = "bls"
	blsTimeout            = 60 * time.Second
	blsStartYear          = 2006
	blsWindowYears        = 10
	blsRegistrationKeyEnv = "BLS_API_KEY"
	blsSucceeded          = "REQUEST_SUCCEEDED"
	blsTable              = "macro_series"
	blsDdl                = `CREATE TABLE IF NOT EXISTS ` + blsTable + ` (
				  source LowCardinality(String)
				, series LowCardinality(String)
				, date Date32
				, value Float64
			) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);
		`
	blsInsert = "INSERT INTO " + blsTable
)

var (
	blsBaseUrl = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
	blsSeries  = []string{
		"CUUR0000SA0",
		"CUSR0000SA0",
		"LNS14000000",
		"CES0000000001",
		"CES0500000003",
		"WPSFD4",
		"JTS000000000000000JOL",
	}
)

type blsRequest struct {
	SeriesIds       []string `json:"seriesid"`
	StartYear       string   `json:"startyear"`
	EndYear         string   `json:"endyear"`
	RegistrationKey string   `json:"registrationkey,omitempty"`
}

type blsResponse struct {
	Status  string   `json:"status"`
	Message []string `json:"message"`
	Results struct {
		Series []struct {
			SeriesId string `json:"seriesID"`
			Data     []struct {
				Year   string `json:"year"`
				Period string `json:"period"`
				Value  string `json:"value"`
			} `json:"data"`
		} `json:"series"`
	} `json:"Results"`
}

type observation struct {
	series string
	date   time.Time
	value  float64
}

func (s *blsImport) Name() string {
	return blsSource
}

func (s *blsImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, blsDdl); err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, blsInsert)
	if err != nil {
		return count, err
	}
	key := os.Getenv(blsRegistrationKeyEnv)
	for _, window := range yearWindows(blsStartYear, time.Now().Year()) {
		observations, err := fetchWindow(ctx, key, window[0], window[1])
		if err != nil {
			return count, fmt.Errorf("bls %d-%d: %w", window[0], window[1], err)
		}
		for _, o := range observations {
			if err = batch.Append(blsSource, o.series, o.date, o.value); err != nil {
				return count, err
			}
			count++
		}
		log.Infof("Fetched %d observations of %d series for %d-%d", len(observations), len(blsSeries), window[0], window[1])
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

func yearWindows(from, to int) [][2]int {
	var windows [][2]int
	for start := from; start <= to; start += blsWindowYears {
		end := min(start+blsWindowYears-1, to)
		windows = append(windows, [2]int{start, end})
	}
	return windows
}

func fetchWindow(ctx context.Context, key string, startYear, endYear int) ([]observation, error) {
	ctx, cancel := context.WithTimeout(ctx, blsTimeout)
	defer cancel()
	payload, err := json.Marshal(blsRequest{
		SeriesIds:       blsSeries,
		StartYear:       strconv.Itoa(startYear),
		EndYear:         strconv.Itoa(endYear),
		RegistrationKey: key,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, blsBaseUrl, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := util.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	return parseResponse(resp.Body)
}

func parseResponse(body io.Reader) ([]observation, error) {
	var response blsResponse
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return nil, err
	}
	if response.Status != blsSucceeded {
		return nil, fmt.Errorf("status %s: %s", response.Status, strings.Join(response.Message, "; "))
	}
	var observations []observation
	for _, series := range response.Results.Series {
		for _, item := range series.Data {
			// M13 — годовое среднее, не месяц; значения '-' — нет данных
			if !strings.HasPrefix(item.Period, "M") || item.Period == "M13" {
				continue
			}
			if item.Value == "" || item.Value == "-" {
				continue
			}
			year, err := strconv.Atoi(item.Year)
			if err != nil {
				return nil, fmt.Errorf("series %s: year %q: %w", series.SeriesId, item.Year, err)
			}
			month, err := strconv.Atoi(strings.TrimPrefix(item.Period, "M"))
			if err != nil {
				return nil, fmt.Errorf("series %s: period %q: %w", series.SeriesId, item.Period, err)
			}
			value, err := strconv.ParseFloat(item.Value, 64)
			if err != nil {
				return nil, fmt.Errorf("series %s: value %q: %w", series.SeriesId, item.Value, err)
			}
			observations = append(observations, observation{
				series: series.SeriesId,
				date:   time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC),
				value:  value,
			})
		}
	}
	return observations, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &blsImport{})
}
