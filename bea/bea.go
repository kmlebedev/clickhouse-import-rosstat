package bea

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type beaImport struct {
}

const (
	beaSource           = "bea"
	beaTimeout          = 60 * time.Second
	beaStartYear        = 2006
	beaKeyEnv           = "BEA_API_KEY"
	beaTable            = "T20804"
	beaDataset          = "NIPA"
	beaFrequency        = "M"
	beaMonthlyPeriodLen = 7
	beaTimeLayout       = "2006M01"
	beaSeriesTable      = "macro_series"
	beaDdl              = `CREATE TABLE IF NOT EXISTS ` + beaSeriesTable + ` (
				  source LowCardinality(String)
				, series LowCardinality(String)
				, date Date32
				, value Float64
			) ENGINE = ReplacingMergeTree ORDER BY (source, series, date);
		`
	beaInsert = "INSERT INTO " + beaSeriesTable
)

var beaViews = []string{
	`CREATE OR REPLACE VIEW v_bea_pce DEFINER = default SQL SECURITY DEFINER AS
		SELECT date, series, value FROM macro_series FINAL WHERE source = 'bea'`,
	`ALTER TABLE v_bea_pce MODIFY COMMENT 'Месячные индексы цен PCE из BEA NIPA T20804: PCE_PI — headline, PCE_PI_CORE — без продуктов питания и энергии. Уровни индекса (2017=100), не темпы: годовой темп = value / value 12 месяцев назад − 1. Описание каждого ряда — в v_series_catalog'`,
	`ALTER TABLE v_bea_pce COMMENT COLUMN date 'Месяц наблюдения (первый день месяца)'`,
	`ALTER TABLE v_bea_pce COMMENT COLUMN series 'PCE_PI (headline) или PCE_PI_CORE (core)'`,
	`ALTER TABLE v_bea_pce COMMENT COLUMN value 'Уровень индекса цен, 2017=100, сезонно скорректированный'`,
}

var (
	beaBaseUrl = "https://apps.bea.gov/api/data"
	// Line numbers of NIPA table T20804 (Table 2.8.4, price indexes for PCE, monthly)
	beaLines = map[string]string{
		"1":  "PCE_PI",
		"25": "PCE_PI_CORE",
	}
	beaSeriesMeta = []util.SeriesMeta{
		{
			Source:      beaSource,
			Series:      "PCE_PI",
			Title:       "PCE price index, headline (Fisher), SA",
			Unit:        "index, 2017=100",
			Frequency:   "M",
			Origin:      "BEA NIPA T20804, LineNumber 1, SeriesCode DPCERG",
			Description: "Индекс цен personal consumption expenditures (PCE), сезонно скорректированный. Уровень индекса, не темп: годовой темп инфляции = value / value 12 месяцев назад − 1. Данные ревизуются BEA, последнее значение публикуется с лагом около месяца",
		},
		{
			Source:      beaSource,
			Series:      "PCE_PI_CORE",
			Title:       "PCE price index excluding food and energy (core, Fisher), SA",
			Unit:        "index, 2017=100",
			Frequency:   "M",
			Origin:      "BEA NIPA T20804, LineNumber 25, SeriesCode DPCCRG",
			Description: "Core PCE — индекс цен PCE без продуктов питания и энергии; ориентир ФРС США для инфляции (цель 2% в годовом выражении). Уровень индекса, не темп: годовой темп = value / value 12 месяцев назад − 1",
		},
	}
)

type beaResponse struct {
	BEAAPI struct {
		Results struct {
			Data []struct {
				LineNumber string `json:"LineNumber"`
				TimePeriod string `json:"TimePeriod"`
				DataValue  string `json:"DataValue"`
			} `json:"Data"`
			Error *struct {
				APIErrorCode        string `json:"APIErrorCode"`
				APIErrorDescription string `json:"APIErrorDescription"`
			} `json:"Error"`
		} `json:"Results"`
	} `json:"BEAAPI"`
}

type observation struct {
	series string
	date   time.Time
	value  float64
}

func (s *beaImport) Name() string {
	return beaSource
}

func (s *beaImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	key := os.Getenv(beaKeyEnv)
	if key == "" {
		return count, fmt.Errorf("%s is not set", beaKeyEnv)
	}
	if err = conn.Exec(ctx, beaDdl); err != nil {
		return count, err
	}
	for _, stmt := range beaViews {
		if err = conn.Exec(ctx, stmt); err != nil {
			return count, err
		}
	}
	observations, err := fetchTable(ctx, key, beaStartYear, time.Now().Year())
	if err != nil {
		return count, fmt.Errorf("bea %s: %w", beaTable, err)
	}
	batch, err := conn.PrepareBatch(ctx, beaInsert)
	if err != nil {
		return count, err
	}
	for _, o := range observations {
		if err = batch.Append(beaSource, o.series, o.date, o.value); err != nil {
			return count, err
		}
		count++
	}
	log.Infof("Fetched %d observations of %s", len(observations), beaTable)
	if err = batch.Send(); err != nil {
		return count, err
	}
	if err = util.UpsertSeriesCatalog(ctx, conn, beaSeriesMeta); err != nil {
		return count, err
	}
	return count, nil
}

func fetchTable(ctx context.Context, key string, startYear, endYear int) ([]observation, error) {
	ctx, cancel := context.WithTimeout(ctx, beaTimeout)
	defer cancel()
	years := make([]string, 0, endYear-startYear+1)
	for y := startYear; y <= endYear; y++ {
		years = append(years, strconv.Itoa(y))
	}
	query := url.Values{}
	query.Set("UserID", key)
	query.Set("method", "GetData")
	query.Set("datasetname", beaDataset)
	query.Set("TableName", beaTable)
	query.Set("Frequency", beaFrequency)
	query.Set("Year", strings.Join(years, ","))
	query.Set("ResultFormat", "JSON")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, beaBaseUrl+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := util.HttpClient.Do(req)
	if err != nil {
		return nil, errors.New(strings.ReplaceAll(err.Error(), key, "***"))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	return parseResponse(resp.Body)
}

func parseResponse(body io.Reader) ([]observation, error) {
	var response beaResponse
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return nil, err
	}
	if e := response.BEAAPI.Results.Error; e != nil {
		return nil, fmt.Errorf("api error %s: %s", e.APIErrorCode, e.APIErrorDescription)
	}
	var observations []observation
	for _, item := range response.BEAAPI.Results.Data {
		series, ok := beaLines[item.LineNumber]
		if !ok {
			continue
		}
		// квартальные и годовые периоды (2006Q1, 2006) не импортируем
		if len(item.TimePeriod) != beaMonthlyPeriodLen || item.TimePeriod[4] != 'M' {
			continue
		}
		if item.DataValue == "" {
			continue
		}
		date, err := time.Parse(beaTimeLayout, item.TimePeriod)
		if err != nil {
			return nil, fmt.Errorf("line %s: period %q: %w", item.LineNumber, item.TimePeriod, err)
		}
		value, err := strconv.ParseFloat(strings.ReplaceAll(item.DataValue, ",", ""), 64)
		if err != nil {
			return nil, fmt.Errorf("line %s: value %q: %w", item.LineNumber, item.DataValue, err)
		}
		observations = append(observations, observation{series: series, date: date, value: value})
	}
	return observations, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &beaImport{})
}
