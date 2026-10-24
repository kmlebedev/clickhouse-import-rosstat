package ofz

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

var moscowLocation = time.FixedZone("MSK", 3*60*60)

const (
	ofzIssueAnalyticsTable = "ofz_issue_analytics"

	ofzIssueAnalyticsDdl = `CREATE TABLE IF NOT EXISTS %s (
    date Date,
    sec_code LowCardinality(String),
    isin String,
    board LowCardinality(String),
    source LowCardinality(String),
    short_name String,
    maturity_date Date,
    days_to_maturity UInt32,
    coupon_rate Nullable(Float64),
    coupon_period Nullable(UInt16),
    face_value Nullable(Float64),
    accrued_int Nullable(Float64),
    price Nullable(Float64),
    ytm Nullable(Float64),
    duration Nullable(Float64),
    modified_duration Nullable(Float64),
    turnover Nullable(Float64),
    trade_count Nullable(UInt32),
    avg_trade_size Nullable(Float64),
    days_since_last_trade Nullable(UInt32),
    spread_to_curve Nullable(Float64),
    curve_tenor LowCardinality(String),
    calculation_status LowCardinality(String),
    updated_at DateTime('Europe/Moscow')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (date, sec_code, board);`

	ofzIssueAnalyticsSourceDdl = `ALTER TABLE %s ADD COLUMN IF NOT EXISTS source LowCardinality(String) AFTER board`
)

// ofzIssueAnalytics reads the Transaq reference/trade tables and the CBR curve,
// computes issue-level analytics in Go and writes one daily snapshot.
type ofzIssueAnalytics struct{}

func (s *ofzIssueAnalytics) Name() string { return ofzIssueAnalyticsTable }

func (s *ofzIssueAnalytics) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	if err := conn.Exec(ctx, fmt.Sprintf(ofzIssueAnalyticsDdl, ofzIssueAnalyticsTable)); err != nil {
		return 0, fmt.Errorf("%s: create table: %w", ofzIssueAnalyticsTable, err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf(ofzIssueAnalyticsSourceDdl, ofzIssueAnalyticsTable)); err != nil {
		return 0, fmt.Errorf("%s: add source column: %w", ofzIssueAnalyticsTable, err)
	}

	settle := dateOnly(time.Now().In(moscowLocation))
	logSourceCount(ctx, conn, "SELECT count() FROM transaq_securities_info FINAL", "transaq_securities_info")
	logSourceCount(ctx, conn, "SELECT count() FROM transaq_securities FINAL WHERE board IN ('TQCB', 'TQOB') AND sectype = 'BOND'", "transaq_securities TQCB/TQOB BOND")
	logSourceCount(ctx, conn, "SELECT count() FROM transaq_trades FINAL WHERE board IN ('TQCB', 'TQOB')", "transaq_trades TQCB/TQOB")
	securities, err := readSecurities(ctx, conn)
	if err != nil {
		return 0, err
	}
	curve, err := readCurve(ctx, conn)
	if err != nil {
		return 0, err
	}
	transaqRows := buildAnalyticsRows(settle, securities, curve, time.Now().In(moscowLocation))
	moexRows, moexErr := buildMOEXRows(settle, curve, time.Now().In(moscowLocation))
	if moexErr != nil {
		log.Warnf("%s: MOEX ISS fallback: %v", ofzIssueAnalyticsTable, moexErr)
	}
	rows := mergeAnalyticsRows(transaqRows, moexRows)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+ofzIssueAnalyticsTable)
	if err != nil {
		return 0, fmt.Errorf("%s: prepare batch: %w", ofzIssueAnalyticsTable, err)
	}
	for _, row := range rows {
		if err = batch.Append(
			row.Date,
			row.SecCode,
			row.Isin,
			row.Board,
			row.Source,
			row.ShortName,
			row.MaturityDate,
			row.DaysToMaturity,
			nullableFloat(row.CouponRate),
			nullableUint16(row.CouponPeriod),
			nullableFloat(row.FaceValue),
			nullableFloat(row.AccruedInt),
			nullableFloat(row.Price),
			nullableFloat(row.YTM),
			nullableFloat(row.Duration),
			nullableFloat(row.ModifiedDuration),
			nullableFloat(row.Turnover),
			nullableUint32(row.TradeCount),
			nullableFloat(row.AvgTradeSize),
			nullableUint32(row.DaysSinceLastTrade),
			nullableFloat(row.SpreadToCurve),
			row.CurveTenor,
			row.CalculationStatus,
			row.UpdatedAt,
		); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("%s: append %s: %w", ofzIssueAnalyticsTable, row.SecCode, err)
		}
	}
	if err = batch.Send(); err != nil {
		return 0, fmt.Errorf("%s: send batch: %w", ofzIssueAnalyticsTable, err)
	}
	if err = publishIssueAnalytics(ctx, conn); err != nil {
		return 0, err
	}
	return int64(len(rows)), nil
}

type securitySource struct {
	SecCode       string
	Isin          string
	SecName       string
	Maturity      time.Time
	NextCoupon    time.Time
	CouponPeriod  uint16
	CouponValue   float64
	FaceValue     float64
	AccruedInt    float64
	Board         string
	ShortName     string
	LotSize       uint32
	LastPrice     float64
	LastTradeTime time.Time
	TradeCount    uint64
	Notional      float64
}

type analyticsRow struct {
	Date               time.Time
	SecCode            string
	Isin               string
	Board              string
	Source             string
	ShortName          string
	MaturityDate       time.Time
	DaysToMaturity     uint32
	CouponRate         *float64
	CouponPeriod       *uint16
	FaceValue          *float64
	AccruedInt         *float64
	Price              *float64
	YTM                *float64
	Duration           *float64
	ModifiedDuration   *float64
	Turnover           *float64
	TradeCount         *uint32
	AvgTradeSize       *float64
	DaysSinceLastTrade *uint32
	SpreadToCurve      *float64
	CurveTenor         string
	CalculationStatus  string
	UpdatedAt          time.Time
}

const readSecuritiesQuery = `SELECT
    si.sec_code,
    any(si.isin) AS isin,
    any(si.sec_name) AS sec_name,
    max(si.mat_date) AS mat_date,
    argMax(si.coupon_date, si.coupon_date) AS coupon_date,
    argMax(si.coupon_period, si.coupon_date) AS coupon_period,
    argMax(toFloat64(si.coupon_value), si.coupon_date) AS coupon_value,
    argMax(toFloat64(si.facevalue), si.coupon_date) AS facevalue,
    argMax(toFloat64(si.accruedint), si.coupon_date) AS accruedint,
    any(s.preferred_board) AS board,
    any(s.shortname) AS shortname,
    max(s.lotsize) AS lotsize,
    any(toFloat64(t.last_price)) AS last_price,
    any(t.last_trade_time) AS last_trade_time,
    any(t.trade_count_today) AS trade_count_today,
    any(toFloat64(t.notional_today)) AS notional_today
FROM transaq_securities_info AS si FINAL
LEFT JOIN (
    SELECT seccode,
           argMax(board, board = 'TQCB') AS preferred_board,
           any(shortname) AS shortname,
           max(lotsize) AS lotsize
    FROM transaq_securities FINAL
    WHERE board IN ('TQCB', 'TQOB') AND sectype = 'BOND'
    GROUP BY seccode
) AS s ON s.seccode = si.sec_code
LEFT JOIN (
    SELECT sec_code,
           argMax(price, time) AS last_price,
           max(time) AS last_trade_time,
           countIf(time >= today()) AS trade_count_today,
           sumIf(price * quantity, time >= today()) AS notional_today
    FROM transaq_trades FINAL
    WHERE board IN ('TQCB', 'TQOB')
    GROUP BY sec_code
) AS t ON t.sec_code = si.sec_code
WHERE si.mat_date >= today()
GROUP BY si.sec_code
ORDER BY si.sec_code`

func readSecurities(ctx context.Context, conn driver.Conn) ([]securitySource, error) {
	rows, err := conn.Query(ctx, readSecuritiesQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: read transaq securities: %w", ofzIssueAnalyticsTable, err)
	}
	defer func() { _ = rows.Close() }()

	var result []securitySource
	for rows.Next() {
		var row securitySource
		if err = rows.Scan(
			&row.SecCode,
			&row.Isin,
			&row.SecName,
			&row.Maturity,
			&row.NextCoupon,
			&row.CouponPeriod,
			&row.CouponValue,
			&row.FaceValue,
			&row.AccruedInt,
			&row.Board,
			&row.ShortName,
			&row.LotSize,
			&row.LastPrice,
			&row.LastTradeTime,
			&row.TradeCount,
			&row.Notional,
		); err != nil {
			return nil, fmt.Errorf("%s: scan security: %w", ofzIssueAnalyticsTable, err)
		}
		row.SecCode = cleanFixedString(row.SecCode)
		row.Board = cleanFixedString(row.Board)
		row.ShortName = strings.TrimSpace(row.ShortName)
		row.SecName = strings.TrimSpace(row.SecName)
		result = append(result, row)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: read transaq securities rows: %w", ofzIssueAnalyticsTable, err)
	}
	log.Infof("%s: securities after TQCB/TQOB join = %d", ofzIssueAnalyticsTable, len(result))
	return result, nil
}

func logSourceCount(ctx context.Context, conn driver.Conn, query, label string) {
	var count uint64
	if err := conn.QueryRow(ctx, query).Scan(&count); err != nil {
		log.Warnf("%s: count %s: %v", ofzIssueAnalyticsTable, label, err)
		return
	}
	log.Infof("%s: %s rows = %d", ofzIssueAnalyticsTable, label, count)
}

func readCurve(ctx context.Context, conn driver.Conn) ([]curvePoint, error) {
	rows, err := conn.Query(ctx, `SELECT tenor, argMax(yield, date) AS yield
FROM cbr_zcyc FINAL
WHERE date <= today()
GROUP BY tenor`)
	if err != nil {
		return nil, fmt.Errorf("%s: read cbr_zcyc: %w", ofzIssueAnalyticsTable, err)
	}
	defer func() { _ = rows.Close() }()

	var points []curvePoint
	for rows.Next() {
		var tenor string
		var yield float64
		if err = rows.Scan(&tenor, &yield); err != nil {
			return nil, fmt.Errorf("%s: scan cbr_zcyc: %w", ofzIssueAnalyticsTable, err)
		}
		years, parseErr := strconv.ParseFloat(strings.TrimSpace(tenor), 64)
		if parseErr != nil || years < 0 {
			log.Warnf("%s: cbr_zcyc: skip tenor %q: %v", ofzIssueAnalyticsTable, tenor, parseErr)
			continue
		}
		points = append(points, curvePoint{Years: years, Yield: yield, Tenor: tenor})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: read cbr_zcyc rows: %w", ofzIssueAnalyticsTable, err)
	}
	return points, nil
}

func buildAnalyticsRows(settle time.Time, securities []securitySource, curve []curvePoint, updatedAt time.Time) []analyticsRow {
	rows := make([]analyticsRow, 0, len(securities))
	for _, sec := range securities {
		maturity := dateOnly(sec.Maturity)
		daysToMaturity := daysBetween(settle, maturity)
		if daysToMaturity < 0 {
			continue
		}
		row := analyticsRow{
			Date:           settle,
			SecCode:        sec.SecCode,
			Isin:           sec.Isin,
			Board:          sec.Board,
			Source:         "transaq",
			ShortName:      sec.ShortName,
			MaturityDate:   maturity,
			DaysToMaturity: uint32(daysToMaturity),
			UpdatedAt:      updatedAt,
		}
		if sec.CouponPeriod > 0 {
			period := sec.CouponPeriod
			row.CouponPeriod = &period
		}
		if sec.FaceValue > 0 {
			face := sec.FaceValue
			row.FaceValue = &face
			accrued := sec.AccruedInt
			row.AccruedInt = &accrued
		}
		if rate, ok := couponRate(sec.CouponValue, sec.FaceValue, int(sec.CouponPeriod)); ok {
			row.CouponRate = &rate
		}

		hasTrade := sec.LastPrice > 0 && sec.LastTradeTime.Year() > 2000
		if hasTrade {
			price := sec.LastPrice
			row.Price = &price
			lastTradeDate := dateOnly(sec.LastTradeTime)
			daysSince := uint32(daysBetween(lastTradeDate, settle))
			row.DaysSinceLastTrade = &daysSince
		}
		tradeCount := uint32(0)
		turnover := 0.0
		if sec.TradeCount > 0 {
			tradeCount = uint32(sec.TradeCount)
			lotSize := sec.LotSize
			if lotSize == 0 {
				lotSize = 1
			}
			turnover = sec.Notional * sec.FaceValue / 100 * float64(lotSize)
		}
		row.TradeCount = &tradeCount
		row.Turnover = &turnover
		avgTradeSize := 0.0
		if tradeCount > 0 {
			avgTradeSize = turnover / float64(tradeCount)
		}
		row.AvgTradeSize = &avgTradeSize

		kind := classifyIssue(sec.ShortName, sec.SecName)
		if kind == issueUnknown {
			continue
		}
		row.CalculationStatus = kind.status()
		if kind != issueFixed {
			rows = append(rows, row)
			continue
		}
		if !hasTrade {
			row.CalculationStatus = "no_price"
			rows = append(rows, row)
			continue
		}
		flows, err := buildSchedule(couponTerms{
			Settlement:       settle,
			Maturity:         maturity,
			NextCoupon:       sec.NextCoupon,
			CouponPeriodDays: int(sec.CouponPeriod),
			CouponValue:      sec.CouponValue,
			FaceValue:        sec.FaceValue,
		})
		if err != nil {
			row.CalculationStatus = "missing_data"
			rows = append(rows, row)
			continue
		}
		dirtyPrice := *row.Price + sec.AccruedInt/sec.FaceValue*100
		yield, err := ytm(settle, dirtyPrice, flows)
		if err != nil {
			row.CalculationStatus = "no_solution"
			rows = append(rows, row)
			continue
		}
		duration, err := macaulayDuration(settle, yield, flows)
		if err != nil {
			row.CalculationStatus = "no_solution"
			rows = append(rows, row)
			continue
		}
		modifiedDuration := duration / (1 + yield/100)
		row.YTM = &yield
		row.Duration = &duration
		row.ModifiedDuration = &modifiedDuration
		row.CalculationStatus = "ok"

		if curveYield, lower, upper, ok := interpolateCurve(curve, float64(daysToMaturity)/365); ok {
			spread := yield - curveYield
			row.SpreadToCurve = &spread
			if lower == upper {
				row.CurveTenor = lower
			} else {
				row.CurveTenor = lower + "-" + upper
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func mergeAnalyticsRows(primary, fallback []analyticsRow) []analyticsRow {
	byKey := make(map[string]analyticsRow, len(primary)+len(fallback))
	for _, row := range primary {
		byKey[row.SecCode+"|"+row.Board] = row
	}
	for _, row := range fallback {
		key := row.SecCode + "|" + row.Board
		if existing, ok := byKey[key]; ok && existing.CalculationStatus == "ok" {
			continue
		}
		byKey[key] = row
	}
	rows := make([]analyticsRow, 0, len(byKey))
	for _, row := range byKey {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].SecCode != rows[j].SecCode {
			return rows[i].SecCode < rows[j].SecCode
		}
		return rows[i].Board < rows[j].Board
	})
	return rows
}

func cleanFixedString(value string) string {
	return strings.TrimRight(value, "\x00 ")
}

func nullableFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableUint16(value *uint16) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableUint32(value *uint32) any {
	if value == nil {
		return nil
	}
	return *value
}

func publishIssueAnalytics(ctx context.Context, conn driver.Conn) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, ofzSeriesMeta()); err != nil {
		return fmt.Errorf("%s: series catalog: %w", ofzIssueAnalyticsTable, err)
	}
	created, err := util.CreateView(ctx, conn, ofzIssueAnalyticsView)
	if err != nil {
		return fmt.Errorf("%s: view %s: %w", ofzIssueAnalyticsTable, ofzIssueAnalyticsView.Name, err)
	}
	if !created {
		return fmt.Errorf("%s: view %s was not created", ofzIssueAnalyticsTable, ofzIssueAnalyticsView.Name)
	}
	log.Infof("%s: view %s created", ofzIssueAnalyticsTable, ofzIssueAnalyticsView.Name)
	return nil
}
