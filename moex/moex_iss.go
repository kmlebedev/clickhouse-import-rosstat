package moex

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type stockPrices struct {
}

const (
	stockPricesTable = "stock_prices"
	plzlCode         = "PLZL"
	// Схема совпадает с legacy-таблицей financial/db.go (Float32 — не меняем,
	// чтобы не ломать существующие дашборды legacy-контура)
	stockPricesDdl = `CREATE TABLE IF NOT EXISTS ` + stockPricesTable + ` (
			  code LowCardinality(String)
			, date Date
			, close Float32
			, open Float32
			, max Float32
			, min Float32
			, volume UInt64
		) ENGINE = ReplacingMergeTree()
		ORDER BY (code, date)
	`
	stockPricesInsert   = "INSERT INTO " + stockPricesTable
	plzlCandlesPath     = "/iss/engines/stock/markets/shares/securities/" + plzlCode + "/candles.json"
	stockPricesMinDate  = "2010-01-01"
	stockPricesDateStep = 24 * time.Hour
)

var stockPricesView = util.View{
	Name:   "v_stock_prices",
	Tables: []string{stockPricesTable},
	Select: `SELECT code, date, open, max, min, close, volume FROM ` + stockPricesTable + ` FINAL WHERE code = '` + plzlCode + `'`,
	Comment: "Дневные котировки акций Полюса (PLZL, MOEX): OHLC + объём. Свечи MOEX ISS (interval=24), " +
		"поля max/min — high/low дня. Цены в рублях за акцию. Единицы и происхождение — в v_series_catalog по source='moex'",
	Columns: map[string]string{
		"code":   "Тикер; сейчас только PLZL",
		"date":   "Торговый день (Date)",
		"open":   "Цена открытия, руб./акция",
		"max":    "Максимум дня (high), руб./акция",
		"min":    "Минимум дня (low), руб./акция",
		"close":  "Цена закрытия, руб./акция; для темпов и NAV к рыночной цене используется close",
		"volume": "Количество акций в сделках за день, шт.",
	},
}

func (s *stockPrices) Name() string {
	return stockPricesTable
}

func (s *stockPrices) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, stockPricesDdl); err != nil {
		return count, err
	}
	from, err := lastDate(ctx, conn, stockPricesTable, "code = '"+plzlCode+"'")
	if err != nil {
		return count, err
	}
	if from.IsZero() {
		from, _ = time.Parse(moexDate, stockPricesMinDate)
	} else {
		from = from.Add(stockPricesDateStep)
	}
	candles, err := fetchCandles(ctx, plzlCandlesPath, from)
	if err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, stockPricesInsert)
	if err != nil {
		return count, err
	}
	for _, c := range candles {
		if err = batch.Append(plzlCode, c.date, c.close, c.open, c.high, c.low, uint64(c.volume)); err != nil {
			return count, err
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("Fetched %d PLZL daily candles from MOEX ISS since %s", len(candles), from.Format(moexDate))
	if err = util.UpsertSeriesCatalog(ctx, conn, moexSeriesMeta); err != nil {
		return count, err
	}
	_, err = util.CreateView(ctx, conn, stockPricesView)
	return count, err
}

// lastDate — максимальная date таблицы по условию WHERE; пустая таблица — zero time.
// Возвращает дату в UTC: clickhouse-go отдаёт Date с таймзоной сессии, а даты из API — UTC.
func lastDate(ctx context.Context, conn driver.Conn, table string, where string) (time.Time, error) {
	row := conn.QueryRow(ctx, "SELECT max(date) FROM "+table+" WHERE "+where)
	var maxDate time.Time
	if err := row.Scan(&maxDate); err != nil {
		return time.Time{}, err
	}
	if maxDate.Year() <= 1970 {
		return time.Time{}, nil
	}
	return time.Date(maxDate.Year(), maxDate.Month(), maxDate.Day(), 0, 0, 0, 0, time.UTC), nil
}
