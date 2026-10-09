package gold

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type goldPrices struct {
}

const (
	goldTable = "gold_prices"
	goldVenue = "moex_fix_usd"
	goldDdl   = `CREATE TABLE IF NOT EXISTS ` + goldTable + ` (
			  venue LowCardinality(String)
			, date Date32
			, usd Float64
		) ENGINE = ReplacingMergeTree ORDER BY (venue, date);
	`
	goldInsert     = "INSERT INTO " + goldTable
	goldFixUrl     = "https://iss.moex.com/iss/history/engines/currency/markets/index/boards/FIXI/securities/GOLDFIXME.json?iss.meta=off&from=2024-01-01&start=%d"
	goldUsdRateSql = "SELECT date, price FROM cbr_currency_usd FINAL ORDER BY date"
	goldDateLayout = "2006-01-02"
	goldTimeout    = 60 * time.Second
	goldMaxRateAge = 10 * 24 * time.Hour
	troyOunceGrams = 31.1034768
)

type issHistory struct {
	History struct {
		Columns []string `json:"columns"`
		Data    [][]any  `json:"data"`
	} `json:"history"`
}

type fixPoint struct {
	date       time.Time
	rubPerGram float64
}

type usdRate struct {
	date  time.Time
	price float64
}

func (s *goldPrices) Name() string {
	return goldTable
}

func (s *goldPrices) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, goldDdl); err != nil {
		return count, err
	}
	rates, err := loadUsdRates(ctx, conn)
	if err != nil {
		return count, err
	}
	fixes, err := fetchFixes(ctx)
	if err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, goldInsert)
	if err != nil {
		return count, err
	}
	skipped := 0
	for _, f := range fixes {
		rate, ok := usdRateOn(rates, f.date)
		if !ok {
			skipped++
			continue
		}
		if err = batch.Append(goldVenue, f.date, f.rubPerGram*troyOunceGrams/rate); err != nil {
			return count, err
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("Fetched %d GOLDFIXME fixes from MOEX, %d skipped: no USD/RUB rate within %d days", len(fixes), skipped, int(goldMaxRateAge.Hours()/24))
	if err = util.UpsertSeriesCatalog(ctx, conn, goldSeriesMeta); err != nil {
		return count, err
	}
	_, err = util.CreateView(ctx, conn, goldPricesView)
	return count, err
}

func loadUsdRates(ctx context.Context, conn driver.Conn) ([]usdRate, error) {
	rows, err := conn.Query(ctx, goldUsdRateSql)
	if err != nil {
		return nil, fmt.Errorf("cbr_currency_usd (сначала запустите импорт cbr_currency_usd): %w", err)
	}
	defer func() { _ = rows.Close() }()
	var rates []usdRate
	for rows.Next() {
		var date time.Time
		var price float32
		if err = rows.Scan(&date, &price); err != nil {
			return nil, err
		}
		day := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
		rates = append(rates, usdRate{date: day, price: float64(price)})
	}
	return rates, rows.Err()
}

// ЦБ не публикует курс на понедельники: действует курс последнего известного дня
func usdRateOn(rates []usdRate, day time.Time) (float64, bool) {
	i := sort.Search(len(rates), func(i int) bool { return rates[i].date.After(day) })
	if i == 0 {
		return 0, false
	}
	last := rates[i-1]
	if day.Sub(last.date) > goldMaxRateAge {
		return 0, false
	}
	return last.price, true
}

func fetchFixes(ctx context.Context) ([]fixPoint, error) {
	var fixes []fixPoint
	for start := 0; ; {
		page, rows, err := fetchFixPage(ctx, start)
		if err != nil {
			return nil, err
		}
		if rows == 0 {
			break
		}
		fixes = append(fixes, page...)
		start += rows
	}
	return fixes, nil
}

func fetchFixPage(ctx context.Context, start int) (fixes []fixPoint, rows int, err error) {
	ctx, cancel := context.WithTimeout(ctx, goldTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(goldFixUrl, start), nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := util.HttpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("moex gold fix: http status %d", resp.StatusCode)
	}
	var history issHistory
	if err = json.NewDecoder(resp.Body).Decode(&history); err != nil {
		return nil, 0, fmt.Errorf("moex gold fix: %w", err)
	}
	dateIdx, priceIdx := -1, -1
	for i, column := range history.History.Columns {
		switch column {
		case "TRADEDATE":
			dateIdx = i
		case "CLOSE":
			priceIdx = i
		}
	}
	if dateIdx < 0 || priceIdx < 0 {
		return nil, 0, fmt.Errorf("moex gold fix: unexpected columns %v", history.History.Columns)
	}
	for _, row := range history.History.Data {
		rows++
		if len(row) <= dateIdx || len(row) <= priceIdx {
			return nil, 0, fmt.Errorf("moex gold fix: short row %v", row)
		}
		price, ok := row[priceIdx].(float64)
		if !ok || price == 0 {
			continue
		}
		dateStr, ok := row[dateIdx].(string)
		if !ok {
			return nil, 0, fmt.Errorf("moex gold fix: bad TRADEDATE %v", row[dateIdx])
		}
		date, err := time.Parse(goldDateLayout, dateStr)
		if err != nil {
			return nil, 0, fmt.Errorf("moex gold fix: %w", err)
		}
		fixes = append(fixes, fixPoint{date: date, rubPerGram: price})
	}
	return fixes, rows, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &goldPrices{})
}
