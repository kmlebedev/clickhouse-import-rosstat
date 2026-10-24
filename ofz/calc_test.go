package ofz

import (
	"errors"
	"math"
	"testing"
	"time"
)

func date(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func almostEqual(got, want, tolerance float64) bool {
	return math.Abs(got-want) <= tolerance
}

func TestClassifyIssue(t *testing.T) {
	tests := []struct {
		name  string
		names []string
		want  issueKind
	}{
		{name: "fixed", names: []string{"ОФЗ-ПД 26238"}, want: issueFixed},
		{name: "fixed short", names: []string{"ОФЗ 26238"}, want: issueFixed},
		{name: "amortizing", names: []string{"ОФЗ-АД 46020"}, want: issueAmortizing},
		{name: "variable", names: []string{"ОФЗ-ПК 29006"}, want: issueVariable},
		{name: "indexed", names: []string{"ОФЗ-ИН 52002"}, want: issueIndexed},
		{name: "corporate", names: []string{"Газпром БО-01"}, want: issueUnknown},
		{name: "empty", names: nil, want: issueUnknown},
		{name: "second name", names: []string{"", "ОФЗ-ПД 26238"}, want: issueFixed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyIssue(test.names...); got != test.want {
				t.Fatalf("classifyIssue(%q) = %v, want %v", test.names, got, test.want)
			}
		})
	}
}

func TestBuildScheduleFixedCoupon(t *testing.T) {
	flows, err := buildSchedule(couponTerms{
		Settlement:       date(2026, time.October, 24),
		Maturity:         date(2027, time.October, 24),
		NextCoupon:       date(2026, time.December, 1),
		CouponPeriodDays: 182,
		CouponValue:      40,
		FaceValue:        1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantDates := []time.Time{date(2026, time.December, 1), date(2027, time.June, 1), date(2027, time.October, 24)}
	wantAmounts := []float64{4, 4, 100}
	if len(flows) != len(wantDates) {
		t.Fatalf("flows = %d, want %d: %+v", len(flows), len(wantDates), flows)
	}
	for i, flow := range flows {
		if !flow.Date.Equal(wantDates[i]) {
			t.Fatalf("flow %d date = %s, want %s", i, flow.Date, wantDates[i])
		}
		if !almostEqual(flow.Amount, wantAmounts[i], 1e-9) {
			t.Fatalf("flow %d amount = %f, want %f", i, flow.Amount, wantAmounts[i])
		}
	}
}

func TestBuildScheduleCouponAtMaturity(t *testing.T) {
	flows, err := buildSchedule(couponTerms{
		Settlement:       date(2026, time.October, 24),
		Maturity:         date(2027, time.October, 24),
		NextCoupon:       date(2027, time.October, 24),
		CouponPeriodDays: 182,
		CouponValue:      40,
		FaceValue:        1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(flows) != 1 {
		t.Fatalf("flows = %d, want 1: %+v", len(flows), flows)
	}
	if !almostEqual(flows[0].Amount, 104, 1e-9) {
		t.Fatalf("amount = %f, want 104", flows[0].Amount)
	}
}

func TestBuildScheduleRejectsMissingData(t *testing.T) {
	base := couponTerms{
		Settlement:       date(2026, time.October, 24),
		Maturity:         date(2027, time.October, 24),
		NextCoupon:       date(2026, time.December, 1),
		CouponPeriodDays: 182,
		CouponValue:      40,
		FaceValue:        1000,
	}
	tests := []struct {
		name   string
		mutate func(*couponTerms)
	}{
		{name: "face", mutate: func(terms *couponTerms) { terms.FaceValue = 0 }},
		{name: "period", mutate: func(terms *couponTerms) { terms.CouponPeriodDays = 0 }},
		{name: "coupon", mutate: func(terms *couponTerms) { terms.CouponValue = 0 }},
		{name: "next before settlement", mutate: func(terms *couponTerms) {
			terms.NextCoupon = date(2026, time.October, 23)
		}},
		{name: "maturity before settlement", mutate: func(terms *couponTerms) {
			terms.Maturity = date(2026, time.October, 23)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			terms := base
			test.mutate(&terms)
			if _, err := buildSchedule(terms); !errors.Is(err, errMissingData) {
				t.Fatalf("error = %v, want errMissingData", err)
			}
		})
	}
}

func TestYTMKnownCashflow(t *testing.T) {
	settlement := date(2026, time.January, 1)
	flows := []cashflow{{Date: date(2027, time.January, 1), Amount: 105}}
	got, err := ytm(settlement, 100, flows)
	if err != nil {
		t.Fatal(err)
	}
	if !almostEqual(got, 5, 1e-6) {
		t.Fatalf("ytm = %f, want 5", got)
	}
}

func TestYTMRejectsUnsolvableInput(t *testing.T) {
	if _, err := ytm(date(2026, time.January, 1), 0, []cashflow{{Date: date(2027, time.January, 1), Amount: 105}}); !errors.Is(err, errNoSolution) {
		t.Fatalf("error = %v, want errNoSolution", err)
	}
	if _, err := ytm(date(2026, time.January, 1), 100, nil); !errors.Is(err, errNoSolution) {
		t.Fatalf("error = %v, want errNoSolution", err)
	}
}

func TestMacaulayDuration(t *testing.T) {
	settlement := date(2026, time.January, 1)
	flows := []cashflow{{Date: date(2027, time.January, 1), Amount: 105}}
	got, err := macaulayDuration(settlement, 5, flows)
	if err != nil {
		t.Fatal(err)
	}
	if !almostEqual(got, 1, 1e-6) {
		t.Fatalf("duration = %f, want 1", got)
	}
}

func TestInterpolateCurve(t *testing.T) {
	points := []curvePoint{
		{Years: 1, Yield: 10, Tenor: "1"},
		{Years: 10, Yield: 20, Tenor: "10"},
	}
	tests := []struct {
		years  float64
		want   float64
		lower  string
		upper  string
		wantOK bool
	}{
		{years: 0.5, want: 10, lower: "1", upper: "1", wantOK: true},
		{years: 5.5, want: 15, lower: "1", upper: "10", wantOK: true},
		{years: 20, want: 20, lower: "10", upper: "10", wantOK: true},
	}
	for _, test := range tests {
		got, lower, upper, ok := interpolateCurve(points, test.years)
		if ok != test.wantOK || !almostEqual(got, test.want, 1e-9) || lower != test.lower || upper != test.upper {
			t.Fatalf("interpolateCurve(%f) = (%f, %q, %q, %v), want (%f, %q, %q, %v)",
				test.years, got, lower, upper, ok, test.want, test.lower, test.upper, test.wantOK)
		}
	}
	if _, _, _, ok := interpolateCurve(nil, 1); ok {
		t.Fatal("empty curve returned ok")
	}
}

func TestCouponRate(t *testing.T) {
	got, ok := couponRate(40, 1000, 182)
	if !ok {
		t.Fatal("couponRate returned !ok")
	}
	if !almostEqual(got, 40.0/1000*365/182*100, 1e-9) {
		t.Fatalf("couponRate = %f", got)
	}
	if _, ok := couponRate(0, 1000, 182); ok {
		t.Fatal("zero coupon returned ok")
	}
}

func TestDaysBetween(t *testing.T) {
	if got := daysBetween(date(2026, time.January, 1), date(2027, time.January, 1)); got != 365 {
		t.Fatalf("days = %d, want 365", got)
	}
	if got := daysBetween(date(2024, time.January, 1), date(2025, time.January, 1)); got != 366 {
		t.Fatalf("leap days = %d, want 366", got)
	}
}
