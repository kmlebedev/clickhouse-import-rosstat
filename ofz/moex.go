package ofz

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const moexBondsBase = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards"

var moexBondURLs = []string{
	moexBondsBase + "/TQCB/securities.json?iss.meta=off&iss.only=securities",
	moexBondsBase + "/TQOB/securities.json?iss.meta=off&iss.only=securities",
}

type moexBondResponse struct {
	Securities struct {
		Columns []string `json:"columns"`
		Data    [][]any  `json:"data"`
	} `json:"securities"`
}

type moexBond struct {
	SecID        string
	Board        string
	ShortName    string
	Maturity     time.Time
	NextCoupon   time.Time
	CouponValue  float64
	CouponPeriod int
	CouponRate   float64
	AccruedInt   float64
	FaceValue    float64
	Price        float64
	Yield        float64
}

// buildMOEXRows is the public fallback when transaq_securities_info has no
// rows or no prices. MOEX ISS supplies the last session price and a yield; the
// duration is computed from the fixed-coupon schedule. Liquidity metrics are
// NULL: this source has no trade-count statistics.
func buildMOEXRows(settle time.Time, curve []curvePoint, updatedAt time.Time) ([]analyticsRow, error) {
	var bonds []moexBond
	for _, url := range moexBondURLs {
		var response moexBondResponse
		if err := util.GetJSON(url, &response); err != nil {
			return nil, fmt.Errorf("MOEX ISS %s: %w", url, err)
		}
		parsed, err := parseMOEXBonds(response)
		if err != nil {
			return nil, fmt.Errorf("MOEX ISS %s: %w", url, err)
		}
		bonds = append(bonds, parsed...)
	}
	log.Infof("%s: MOEX ISS bonds = %d", ofzIssueAnalyticsTable, len(bonds))
	return buildMOEXAnalyticsRows(settle, bonds, curve, updatedAt), nil
}

func parseMOEXBonds(response moexBondResponse) ([]moexBond, error) {
	index := make(map[string]int, len(response.Securities.Columns))
	for i, column := range response.Securities.Columns {
		index[column] = i
	}
	for _, required := range []string{"SECID", "BOARDID", "SHORTNAME", "MATDATE"} {
		if _, ok := index[required]; !ok {
			return nil, fmt.Errorf("missing column %s", required)
		}
	}
	bonds := make([]moexBond, 0, len(response.Securities.Data))
	for _, row := range response.Securities.Data {
		secID := stringCell(row, index["SECID"])
		maturity, ok := dateCell(row, index["MATDATE"])
		if secID == "" || !ok {
			continue
		}
		nextCoupon, _ := dateCell(row, index["NEXTCOUPON"])
		bonds = append(bonds, moexBond{
			SecID:        secID,
			Board:        stringCell(row, index["BOARDID"]),
			ShortName:    stringCell(row, index["SHORTNAME"]),
			Maturity:     maturity,
			NextCoupon:   nextCoupon,
			CouponValue:  floatCell(row, index["COUPONVALUE"]),
			CouponPeriod: int(floatCell(row, index["COUPONPERIOD"])),
			CouponRate:   floatCell(row, index["COUPONPERCENT"]),
			AccruedInt:   floatCell(row, index["ACCRUEDINT"]),
			FaceValue:    floatCell(row, index["FACEVALUE"]),
			Price:        firstPositive(floatCell(row, index["PREVWAPRICE"]), floatCell(row, index["PREVPRICE"])),
			Yield:        floatCell(row, index["YIELDATPREVWAPRICE"]),
		})
	}
	return bonds, nil
}

func stringCell(row []any, index int) string {
	if index < 0 || index >= len(row) || row[index] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(row[index]))
}

func floatCell(row []any, index int) float64 {
	if index < 0 || index >= len(row) || row[index] == nil {
		return 0
	}
	switch value := row[index].(type) {
	case float64:
		return value
	case int:
		return float64(value)
	case json.Number:
		parsed, _ := value.Float64()
		return parsed
	case string:
		parsed, _ := strconv.ParseFloat(strings.TrimSpace(value), 64)
		return parsed
	default:
		return 0
	}
}

func dateCell(row []any, index int) (time.Time, bool) {
	value := stringCell(row, index)
	if value == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func firstPositive(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func buildMOEXAnalyticsRows(settle time.Time, bonds []moexBond, curve []curvePoint, updatedAt time.Time) []analyticsRow {
	rows := make([]analyticsRow, 0, len(bonds))
	for _, bond := range bonds {
		if !isOFZ(bond) {
			continue
		}
		maturity := dateOnly(bond.Maturity)
		daysToMaturity := daysBetween(settle, maturity)
		if daysToMaturity < 0 {
			continue
		}
		kind := classifyIssue(bond.ShortName)
		if kind == issueUnknown {
			continue
		}
		row := analyticsRow{
			Date:           settle,
			SecCode:        bond.SecID,
			Board:          bond.Board,
			Source:         "moex",
			ShortName:      bond.ShortName,
			MaturityDate:   maturity,
			DaysToMaturity: uint32(daysToMaturity),
			UpdatedAt:      updatedAt,
		}
		if bond.CouponPeriod > 0 {
			period := uint16(bond.CouponPeriod)
			row.CouponPeriod = &period
		}
		if bond.FaceValue > 0 {
			face := bond.FaceValue
			row.FaceValue = &face
			accrued := bond.AccruedInt
			row.AccruedInt = &accrued
		}
		if rate, ok := couponRate(bond.CouponValue, bond.FaceValue, bond.CouponPeriod); ok {
			row.CouponRate = &rate
		} else if bond.CouponRate > 0 {
			rate := bond.CouponRate
			row.CouponRate = &rate
		}
		if bond.Price > 0 {
			price := bond.Price
			row.Price = &price
		}

		flows, err := buildSchedule(couponTerms{
			Settlement:       settle,
			Maturity:         maturity,
			NextCoupon:       bond.NextCoupon,
			CouponPeriodDays: bond.CouponPeriod,
			CouponValue:      bond.CouponValue,
			FaceValue:        bond.FaceValue,
		})
		if err != nil {
			row.CalculationStatus = "missing_data"
			rows = append(rows, row)
			continue
		}

		yield := bond.Yield
		if yield <= 0 && row.Price != nil && bond.FaceValue > 0 {
			dirty := *row.Price + bond.AccruedInt/bond.FaceValue*100
			if computed, computeErr := ytm(settle, dirty, flows); computeErr == nil {
				yield = computed
			}
		}
		if yield <= 0 {
			row.CalculationStatus = "no_price"
			rows = append(rows, row)
			continue
		}
		row.YTM = &yield
		row.CalculationStatus = kind.status()
		if kind == issueFixed {
			if duration, durationErr := macaulayDuration(settle, yield, flows); durationErr == nil {
				modified := duration / (1 + yield/100)
				row.Duration = &duration
				row.ModifiedDuration = &modified
				row.CalculationStatus = "ok"
			} else {
				row.CalculationStatus = "no_solution"
			}
		}
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

func isOFZ(bond moexBond) bool {
	upper := strings.ToUpper(bond.ShortName)
	return strings.HasPrefix(strings.ToUpper(bond.SecID), "SU") || strings.Contains(upper, "ОФЗ")
}
