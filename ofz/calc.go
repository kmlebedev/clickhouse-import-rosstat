package ofz

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

var (
	errMissingData = errors.New("missing coupon data")
	errNoSolution  = errors.New("no ytm solution")
)

type issueKind int

const (
	issueFixed issueKind = iota
	issueAmortizing
	issueVariable
	issueIndexed
	issueUnknown
)

func (k issueKind) status() string {
	switch k {
	case issueFixed:
		return "ok"
	case issueAmortizing:
		return "unsupported_amortizing"
	case issueVariable:
		return "unsupported_variable"
	case issueIndexed:
		return "unsupported_indexed"
	default:
		return "unsupported_unknown"
	}
}

// classifyIssue distinguishes fixed-coupon OFZ (supported) from amortizing,
// variable-coupon and indexed issues whose full cash-flow schedule is not
// available from transaq_securities_info.
func classifyIssue(names ...string) issueKind {
	for _, name := range names {
		upper := strings.ToUpper(strings.TrimSpace(name))
		switch {
		case strings.Contains(upper, "ОФЗ-АД"):
			return issueAmortizing
		case strings.Contains(upper, "ОФЗ-ПК"):
			return issueVariable
		case strings.Contains(upper, "ОФЗ-ИН"):
			return issueIndexed
		case strings.Contains(upper, "ОФЗ"):
			return issueFixed
		}
	}
	return issueUnknown
}

// classifySec prefers the national security code over the display name: MOEX
// short names like "ОФЗ 29008" carry no coupon-type marker, while the code
// prefix identifies the issue family (SU26 fixed, SU29 variable, SU46
// amortizing, SU52 indexed). Transaq fixed-width codes may contain NULs.
func classifySec(secCode string, names ...string) issueKind {
	code := strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(secCode, "\x00", "")))
	switch {
	case strings.HasPrefix(code, "SU29"):
		return issueVariable
	case strings.HasPrefix(code, "SU46"):
		return issueAmortizing
	case strings.HasPrefix(code, "SU52"):
		return issueIndexed
	case strings.HasPrefix(code, "SU26"):
		return issueFixed
	}
	return classifyIssue(names...)
}

type couponTerms struct {
	Settlement       time.Time
	Maturity         time.Time
	NextCoupon       time.Time
	CouponPeriodDays int
	CouponValue      float64
	FaceValue        float64
}

type cashflow struct {
	Date   time.Time
	Amount float64
}

type curvePoint struct {
	Years float64
	Yield float64
	Tenor string
}

// buildSchedule builds the future fixed-coupon cash flows in percent of face.
// transaq_securities_info only carries the next coupon and its period; for
// OFZ-PD a constant coupon is a valid assumption. Amortizing and variable
// issues are excluded by classifyIssue before this function is called.
func buildSchedule(terms couponTerms) ([]cashflow, error) {
	settlement := dateOnly(terms.Settlement)
	maturity := dateOnly(terms.Maturity)
	if terms.FaceValue <= 0 {
		return nil, fmt.Errorf("%w: face value is not positive", errMissingData)
	}
	if !maturity.After(settlement) {
		return nil, fmt.Errorf("%w: maturity %s is not after settlement %s",
			errMissingData, maturity.Format("2006-01-02"), settlement.Format("2006-01-02"))
	}
	if terms.CouponPeriodDays <= 0 || terms.CouponValue <= 0 {
		return nil, fmt.Errorf("%w: coupon period/value is not positive", errMissingData)
	}

	next := dateOnly(terms.NextCoupon)
	if next.IsZero() || next.Before(settlement) {
		return nil, fmt.Errorf("%w: next coupon %s is before settlement %s",
			errMissingData, next.Format("2006-01-02"), settlement.Format("2006-01-02"))
	}

	couponPct := terms.CouponValue / terms.FaceValue * 100
	var flows []cashflow
	for i := 0; i < 256 && !next.After(maturity); i++ {
		flows = append(flows, cashflow{Date: next, Amount: couponPct})
		next = next.AddDate(0, 0, terms.CouponPeriodDays)
	}
	if len(flows) == 0 {
		return []cashflow{{Date: maturity, Amount: 100}}, nil
	}
	if flows[len(flows)-1].Date.Before(maturity) {
		flows = append(flows, cashflow{Date: maturity, Amount: 100})
	} else {
		flows[len(flows)-1].Amount += 100
	}
	return flows, nil
}

// ytm returns the yield to maturity in percent per annum for a dirty price in
// percent of face. ACT/365 is used; the result is checked against MOEX/broker
// calculations during acceptance.
func ytm(settlement time.Time, dirtyPrice float64, flows []cashflow) (float64, error) {
	if dirtyPrice <= 0 || len(flows) == 0 {
		return 0, errNoSolution
	}
	presentValue := func(rate float64) float64 {
		var value float64
		for _, flow := range flows {
			t := dayCount(settlement, flow.Date)
			value += flow.Amount / math.Pow(1+rate, t)
		}
		return value
	}
	f := func(rate float64) float64 {
		return presentValue(rate) - dirtyPrice
	}

	lo, hi := -0.99, 10.0
	flo, fhi := f(lo), f(hi)
	if math.IsNaN(flo) || math.IsNaN(fhi) || flo*fhi > 0 {
		return 0, errNoSolution
	}
	for i := 0; i < 200; i++ {
		mid := (lo + hi) / 2
		fmid := f(mid)
		if math.Abs(fmid) < 1e-10 {
			return mid * 100, nil
		}
		if flo*fmid <= 0 {
			hi = mid
		} else {
			lo = mid
			flo = fmid
		}
	}
	return (lo + hi) / 2 * 100, nil
}

// macaulayDuration returns Macaulay duration in years.
func macaulayDuration(settlement time.Time, ytmPercent float64, flows []cashflow) (float64, error) {
	if len(flows) == 0 {
		return 0, errNoSolution
	}
	rate := ytmPercent / 100
	var present, weighted float64
	for _, flow := range flows {
		t := dayCount(settlement, flow.Date)
		value := flow.Amount / math.Pow(1+rate, t)
		present += value
		weighted += t * value
	}
	if present == 0 {
		return 0, errNoSolution
	}
	return weighted / present, nil
}

// interpolateCurve returns the linearly interpolated yield and the surrounding
// tenor labels. Outside the curve it returns the nearest point.
func interpolateCurve(points []curvePoint, years float64) (float64, string, string, bool) {
	if len(points) == 0 {
		return 0, "", "", false
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Years < points[j].Years })
	if years <= points[0].Years {
		return points[0].Yield, points[0].Tenor, points[0].Tenor, true
	}
	for i := 1; i < len(points); i++ {
		if years <= points[i].Years {
			prev, current := points[i-1], points[i]
			if current.Years == prev.Years {
				return current.Yield, prev.Tenor, current.Tenor, true
			}
			weight := (years - prev.Years) / (current.Years - prev.Years)
			return prev.Yield + weight*(current.Yield-prev.Yield), prev.Tenor, current.Tenor, true
		}
	}
	last := points[len(points)-1]
	return last.Yield, last.Tenor, last.Tenor, true
}

func couponRate(couponValue, faceValue float64, periodDays int) (float64, bool) {
	if couponValue <= 0 || faceValue <= 0 || periodDays <= 0 {
		return 0, false
	}
	return couponValue / faceValue * 365 / float64(periodDays) * 100, true
}

func dayCount(settlement, date time.Time) float64 {
	return float64(daysBetween(settlement, date)) / 365
}

func dateOnly(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func daysBetween(from, to time.Time) int {
	return int(dateOnly(to).Sub(dateOnly(from)).Hours() / 24)
}
