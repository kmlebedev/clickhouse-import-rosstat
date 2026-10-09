package moex

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

type ofzCurve struct {
}

const (
	ofzTable = "ofz_curve"
	ofzDdl   = `CREATE TABLE IF NOT EXISTS ` + ofzTable + ` (
			  date Date
			, tenor LowCardinality(String)
			, yield Float64
		) ENGINE = ReplacingMergeTree ORDER BY (date, tenor)
	`
	ofzInsert        = "INSERT INTO " + ofzTable
	rgbiCandlesPath  = "/iss/engines/stock/markets/index/securities/RGBI/candles.json"
	zcycPath         = "/iss/engines/stock/zcyc.json"
	rgbiTenor        = "RGBI"
	rgbiHistoryStart = "2010-01-01"
)

// zcycPeriodToTenor: годовые доходности параметризованной кривой МосБиржи (G-curve)
var zcycPeriodToTenor = map[float64]string{1: "1y", 3: "3y", 5: "5y", 10: "10y"}

type curvePoint struct {
	date  time.Time
	tenor string
	value float64
}

var ofzCurveView = util.View{
	Name:   "v_ofz_curve",
	Tables: []string{ofzTable},
	Select: `SELECT date, tenor, yield FROM ` + ofzTable + ` FINAL`,
	Comment: "Рублёвая безрисковая кривая: доходности ОФЗ по G-curve МосБиржи (теноры 1y/3y/5y/10y, % годовых) " +
		"и уровень индекса RGBI (тенор RGBI — НЕ доходность, а индекс, рост = рост цен облигаций). " +
		"Локальный контур ставки дисконтирования DCF (ОФЗ + премии). Единицы — в v_series_catalog по source='moex'",
	Columns: map[string]string{
		"date":  "Дата наблюдения (Date)",
		"tenor": "Тенор: 1y/3y/5y/10y — доходность G-curve МосБиржи, % годовых; RGBI — уровень индекса облигаций федерального займа",
		"yield": "Доходность, % годовых (для tenor RGBI — уровень индекса)",
	},
}

func (s *ofzCurve) Name() string {
	return ofzTable
}

func (s *ofzCurve) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, ofzDdl); err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, ofzInsert)
	if err != nil {
		return count, err
	}
	// RGBI: история свечей индекса с самого начала
	from, err := lastDate(ctx, conn, ofzTable, "tenor = '"+rgbiTenor+"'")
	if err != nil {
		return count, err
	}
	if from.IsZero() {
		from, _ = time.Parse(moexDate, rgbiHistoryStart)
	} else {
		from = from.Add(stockPricesDateStep)
	}
	rgbi, err := fetchCandles(ctx, rgbiCandlesPath, from)
	if err != nil {
		return count, err
	}
	for _, c := range rgbi {
		if err = batch.Append(c.date, rgbiTenor, c.close); err != nil {
			return count, err
		}
		count++
	}
	// G-curve: эндпоинт zcyc — только снимок текущего дня, истории у ISS нет;
	// собираем с момента первого запуска, инкремент по max(date) теноров 1y..10y
	yields, err := fetchYearYields(ctx)
	if err != nil {
		return count, err
	}
	if len(yields) > 0 {
		lastYieldDate, err := lastDate(ctx, conn, ofzTable, "tenor IN ('1y','3y','5y','10y')")
		if err != nil {
			return count, err
		}
		for _, p := range yields {
			if !lastYieldDate.IsZero() && !p.date.After(lastYieldDate) {
				continue
			}
			if err = batch.Append(p.date, p.tenor, p.value); err != nil {
				return count, err
			}
			count++
		}
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("Fetched RGBI candles and G-curve yields from MOEX ISS, %d rows total", count)
	if err = util.UpsertSeriesCatalog(ctx, conn, moexSeriesMeta); err != nil {
		return count, err
	}
	_, err = util.CreateView(ctx, conn, ofzCurveView)
	return count, err
}

func fetchYearYields(ctx context.Context) ([]curvePoint, error) {
	block, err := getBlock(ctx, zcycPath+"?iss.meta=off", "yearyields")
	if err != nil {
		return nil, err
	}
	return parseYearYields(block)
}

func parseYearYields(block issBlock) ([]curvePoint, error) {
	dateIdx := columnIndex(block.Columns, "tradedate")
	periodIdx := columnIndex(block.Columns, "period")
	valueIdx := columnIndex(block.Columns, "value")
	if dateIdx < 0 || periodIdx < 0 || valueIdx < 0 {
		return nil, errNoColumns(block.Columns)
	}
	var points []curvePoint
	for _, row := range block.Data {
		period, err := rowFloat(row, periodIdx)
		if err != nil {
			return nil, err
		}
		tenor, ok := zcycPeriodToTenor[period]
		if !ok {
			continue
		}
		value, err := rowFloat(row, valueIdx)
		if err != nil {
			return nil, err
		}
		tradedate, ok := row[dateIdx].(string)
		if !ok {
			return nil, errBadCell("tradedate", row[dateIdx])
		}
		date, err := time.Parse(moexDate, tradedate)
		if err != nil {
			return nil, err
		}
		points = append(points, curvePoint{date: date, tenor: tenor, value: value})
	}
	return points, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &stockPrices{}, &ofzCurve{})
}
