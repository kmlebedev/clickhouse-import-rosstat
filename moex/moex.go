package moex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const (
	moexSource    = "moex"
	moexTimeout   = 60 * time.Second
	moexDateTime  = "2006-01-02 15:04:05"
	moexDate      = "2006-01-02"
	moexPageLimit = 100
)

var moexBaseUrl = "https://iss.moex.com"

type issBlock struct {
	Columns []string `json:"columns"`
	Data    [][]any  `json:"data"`
}

type candle struct {
	date   time.Time
	open   float64
	close  float64
	high   float64
	low    float64
	volume float64
}

func columnIndex(columns []string, name string) int {
	for i, c := range columns {
		if c == name {
			return i
		}
	}
	return -1
}

func errNoColumns(columns []string) error {
	return fmt.Errorf("no columns tradedate/period/value in %v", columns)
}

func errBadCell(name string, cell any) error {
	return fmt.Errorf("bad %s: %v", name, cell)
}

func rowFloat(row []any, idx int) (float64, error) {
	if idx < 0 || len(row) <= idx {
		return 0, fmt.Errorf("short row %v", row)
	}
	v, ok := row[idx].(float64)
	if !ok {
		return 0, fmt.Errorf("not a number: %v", row[idx])
	}
	return v, nil
}

func getBlock(ctx context.Context, path string, block string) (issBlock, error) {
	ctx, cancel := context.WithTimeout(ctx, moexTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, moexBaseUrl+path, nil)
	if err != nil {
		return issBlock{}, err
	}
	resp, err := util.HttpClient.Do(req)
	if err != nil {
		return issBlock{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return issBlock{}, fmt.Errorf("%s: http status %d", path, resp.StatusCode)
	}
	var body map[string]issBlock
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return issBlock{}, fmt.Errorf("%s: %w", path, err)
	}
	b, ok := body[block]
	if !ok {
		return issBlock{}, fmt.Errorf("%s: no block %q in response", path, block)
	}
	return b, nil
}

// fetchCandles дневные свечи (interval=24) инструмента, пагинация параметром start.
// path — путь вида /iss/engines/<engine>/markets/<market>/securities/<secid>/candles.json
func fetchCandles(ctx context.Context, path string, from time.Time) ([]candle, error) {
	var candles []candle
	for start := 0; ; start += moexPageLimit {
		block, err := getBlock(ctx, fmt.Sprintf("%s?iss.meta=off&interval=24&limit=%d&from=%s&start=%d",
			path, moexPageLimit, from.Format(moexDate), start), "candles")
		if err != nil {
			return nil, err
		}
		if len(block.Data) == 0 {
			break
		}
		page, err := parseCandles(block)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		candles = append(candles, page...)
		if len(block.Data) < moexPageLimit {
			break
		}
	}
	return candles, nil
}

func parseCandles(block issBlock) ([]candle, error) {
	idx := map[string]int{}
	for _, name := range []string{"open", "close", "high", "low", "volume", "begin"} {
		idx[name] = columnIndex(block.Columns, name)
		if idx[name] < 0 {
			return nil, fmt.Errorf("no column %q in %v", name, block.Columns)
		}
	}
	var candles []candle
	for _, row := range block.Data {
		open, err := rowFloat(row, idx["open"])
		if err != nil {
			return nil, err
		}
		close_, err := rowFloat(row, idx["close"])
		if err != nil {
			return nil, err
		}
		high, err := rowFloat(row, idx["high"])
		if err != nil {
			return nil, err
		}
		low, err := rowFloat(row, idx["low"])
		if err != nil {
			return nil, err
		}
		volume, err := rowFloat(row, idx["volume"])
		if err != nil {
			return nil, err
		}
		begin, ok := row[idx["begin"]].(string)
		if !ok {
			return nil, fmt.Errorf("bad begin: %v", row[idx["begin"]])
		}
		date, err := time.Parse(moexDateTime, begin)
		if err != nil {
			return nil, err
		}
		candles = append(candles, candle{
			date:   time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC),
			open:   open,
			close:  close_,
			high:   high,
			low:    low,
			volume: volume,
		})
	}
	return candles, nil
}
