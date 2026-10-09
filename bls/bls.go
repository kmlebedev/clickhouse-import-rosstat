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

var blsViews = []string{
	`CREATE OR REPLACE VIEW v_bls_macro DEFINER = default SQL SECURITY DEFINER AS
		SELECT date, series, value FROM macro_series FINAL WHERE source = 'bls'`,
	`ALTER TABLE v_bls_macro MODIFY COMMENT 'Макро-ряды США из BLS: индексы CPI (NSA и SA), безработица, занятость вне сельского хозяйства (NFP), средняя почасовая зарплата, PPI final demand, вакансии JOLTS. Единицы и база каждого ряда — в v_series_catalog'`,
	`ALTER TABLE v_bls_macro COMMENT COLUMN date 'Месяц наблюдения (первый день месяца)'`,
	`ALTER TABLE v_bls_macro COMMENT COLUMN series 'Код ряда BLS: CUUR0000SA0, CUSR0000SA0, LNS14000000, CES0000000001, CES0500000003, WPSFD4, JTS000000000000000JOL'`,
	`ALTER TABLE v_bls_macro COMMENT COLUMN value 'Значение ряда в единицах из v_series_catalog (индекс, проценты, тысячи человек, доллары)'`,
}

var blsSeriesMeta = []util.SeriesMeta{
	{
		Source:      blsSource,
		Series:      "CUUR0000SA0",
		Title:       "CPI-U: All items in U.S. city average, not seasonally adjusted, monthly",
		Unit:        "index, 1982-84 = 100",
		Frequency:   "M",
		Origin:      "BLS API v2 seriesid CUUR0000SA0 (CPI-U, NSA)",
		Description: "Индекс потребительских цен CPI-U без сезонной корректировки. Уровень индекса, не темп: годовой темп инфляции = value / value 12 месяцев назад − 1. В октябре 2025 значение отсутствует из-за лапса финансирования BLS — в выборке пропуск, не ноль",
	},
	{
		Source:      blsSource,
		Series:      "CUSR0000SA0",
		Title:       "CPI-U: All items in U.S. city average, seasonally adjusted, monthly",
		Unit:        "index, 1982-84 = 100",
		Frequency:   "M",
		Origin:      "BLS API v2 seriesid CUSR0000SA0 (CPI-U, SA)",
		Description: "Индекс потребительских цен CPI-U с сезонной корректировкой — для анализа помесячной динамики. Уровень индекса, не темп: месячный темп = value / value предыдущего месяца − 1; годовой — к значению 12 месяцев назад",
	},
	{
		Source:      blsSource,
		Series:      "LNS14000000",
		Title:       "Unemployment rate, seasonally adjusted, monthly",
		Unit:        "percent",
		Frequency:   "M",
		Origin:      "BLS API v2 seriesid LNS14000000 (CPS, таблица A-1)",
		Description: "Уровень безработицы среди гражданского населения в возрасте 16+, сезонно скорректированный. Значение — проценты (не доли); изменение считать в п.п. разностью уровней",
	},
	{
		Source:      blsSource,
		Series:      "CES0000000001",
		Title:       "All employees, total nonfarm, seasonally adjusted, monthly",
		Unit:        "thousands of persons",
		Frequency:   "M",
		Origin:      "BLS API v2 seriesid CES0000000001 (CES, nonfarm payrolls)",
		Description: "Занятость вне сельского хозяйства (NFP), сезонно скорректированная, тысячи человек. Месячное изменение = разность уровней; публикуемые значения ревизуются BLS в последующие месяцы",
	},
	{
		Source:      blsSource,
		Series:      "CES0500000003",
		Title:       "Average hourly earnings of all employees, total private, seasonally adjusted, monthly",
		Unit:        "USD per hour",
		Frequency:   "M",
		Origin:      "BLS API v2 seriesid CES0500000003 (CES, average hourly earnings)",
		Description: "Средняя почасовая зарплата всех работников частного сектора в долларах США, сезонно скорректированная. Уровень, не темп: годовой рост = value / value 12 месяцев назад − 1",
	},
	{
		Source:      blsSource,
		Series:      "WPSFD4",
		Title:       "PPI commodity data for final demand, seasonally adjusted, monthly",
		Unit:        "index, base 2009-11 = 100",
		Frequency:   "M",
		Origin:      "BLS API v2 seriesid WPSFD4 (PPI final demand, SA)",
		Description: "Индекс цен производителей по конечному спросу (PPI final demand), сезонно скорректированный. База — Base Date 200911 (ноябрь 2009). Уровень индекса, не темп: годовой темп = value / value 12 месяцев назад − 1",
	},
	{
		Source:      blsSource,
		Series:      "JTS000000000000000JOL",
		Title:       "Job openings, total nonfarm, seasonally adjusted, monthly (JOLTS)",
		Unit:        "thousands of job openings",
		Frequency:   "M",
		Origin:      "BLS API v2 seriesid JTS000000000000000JOL (JOLTS, job openings level)",
		Description: "Число вакансий в экономике вне сельского хозяйства (JOLTS), сезонно скорректированное, тысячи. Последнее значение публикуется с лагом и ревизуется; сравнивать с безработицей (LNS14000000) для оценки напряжённости рынка труда",
	},
}

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
	for _, stmt := range blsViews {
		if err = conn.Exec(ctx, stmt); err != nil {
			return count, err
		}
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
	if err = util.UpsertSeriesCatalog(ctx, conn, blsSeriesMeta); err != nil {
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
