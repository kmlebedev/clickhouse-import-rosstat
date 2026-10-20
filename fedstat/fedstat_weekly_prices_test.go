package fedstat

import (
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const (
	testProductBeef = "Говядина (кроме бескостного мяса), кг"
	testProductEggs = "Яйца куриные, 10 шт."
)

func TestFedstatDate(t *testing.T) {
	cases := []struct {
		label string
		year  int
		want  string
	}{
		{"на 9 января 2008 года", 2008, "2008-01-09"},
		{"на 6 октября 2008 года", 2008, "2008-10-06"},
		{"23 неделя (на 13 июня 2016 года)", 2016, "2016-06-13"},
		{"1 неделя (на 11 января 2016 года)", 2016, "2016-01-11"},
		{"37 неделя (на 17сентября 2018 года)", 2018, "2018-09-17"},
		{"41 неделя (на 14 октября )", 2018, "2018-10-14"},
		{"01 неделя ( на 10 января)", 2008, "2008-01-10"},
		// Год метки сильнее Time: 68 серий начала января имеют Time предыдущего года.
		{"2 неделя (на 16 января 2017 года)", 2016, "2017-01-16"},
		{"1 неделя 2021", 2021, "2021-01-11"},
		{"51 неделя 2021", 2021, "2021-12-27"},
		{"51 неделя 2022", 2022, "2022-12-26"},
		{"39 неделя 2026", 2026, "2026-10-05"},
		{"40 неделя 2026", 2026, "2026-10-12"},
	}
	for _, tc := range cases {
		got, err := fedstatDate(tc.label, tc.year)
		if err != nil {
			t.Errorf("fedstatDate(%q, %d): %v", tc.label, tc.year, err)
			continue
		}
		if got.Format("2006-01-02") != tc.want {
			t.Errorf("fedstatDate(%q, %d) = %s, want %s", tc.label, tc.year, got.Format("2006-01-02"), tc.want)
		}
	}
}

func TestFedstatDateRejectsBadLabels(t *testing.T) {
	cases := []struct {
		label string
		year  int
		want  string
	}{
		{"на 30 февраля 2020 года", 2020, "невалидная дата"},
		{"0 неделя 2021", 2021, "вне 1..53"},
		{"54 неделя 2021", 2021, "вне 1..53"},
		{"abc", 2020, "не распознана"},
	}
	for _, tc := range cases {
		_, err := fedstatDate(tc.label, tc.year)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("fedstatDate(%q, %d) error = %v, want contains %q", tc.label, tc.year, err, tc.want)
		}
	}
}

func TestFedstatPrice(t *testing.T) {
	cases := []struct {
		raw  string
		want float64
	}{
		{"311,8", 311.8},
		{"760", 760},
		{"0", 0},
	}
	for _, tc := range cases {
		got, err := fedstatPrice(tc.raw)
		if err != nil || math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("fedstatPrice(%q) = %v, %v; want %v", tc.raw, got, err, tc.want)
		}
	}
	for _, raw := range []string{"", "abc", "NaN", "Inf", "-1"} {
		if _, err := fedstatPrice(raw); err == nil {
			t.Errorf("fedstatPrice(%q) expected error", raw)
		}
	}
}

func TestFedstatParseFixtures(t *testing.T) {
	cases := []struct {
		fixture  string
		points   int
		spot     []fedstatPoint
		rawCount int
	}{
		{
			fixture: "fedstat_2008.xml",
			points:  6,
			spot: []fedstatPoint{
				{product: testProductBeef, region: fedstatRegionRFName, date: time.Date(2008, 1, 9, 0, 0, 0, 0, time.UTC), price: 140.86},
				{product: testProductEggs, region: fedstatRegionRFName, date: time.Date(2008, 1, 9, 0, 0, 0, 0, time.UTC), price: 35.94},
			},
		},
		{
			// Годовой ответ источника оборачивает Series в DataSet (см. fedstatSDMX).
			fixture: "fedstat_2008_dataset.xml",
			points:  6,
			spot: []fedstatPoint{
				{product: testProductBeef, region: fedstatRegionRFName, date: time.Date(2008, 1, 9, 0, 0, 0, 0, time.UTC), price: 140.86},
				{product: testProductEggs, region: fedstatRegionRFName, date: time.Date(2008, 1, 9, 0, 0, 0, 0, time.UTC), price: 35.94},
			},
		},
		{
			fixture: "fedstat_2016.xml",
			points:  6,
			spot: []fedstatPoint{
				{product: testProductBeef, region: fedstatRegionRFName, date: time.Date(2016, 6, 13, 0, 0, 0, 0, time.UTC), price: 311.8},
				{product: testProductBeef, region: fedstatRegionRFName, date: time.Date(2016, 1, 11, 0, 0, 0, 0, time.UTC), price: 313.02},
			},
		},
		{
			fixture: "fedstat_2018.xml",
			points:  3,
			spot: []fedstatPoint{
				{product: testProductBeef, region: fedstatRegionRFName, date: time.Date(2018, 1, 9, 0, 0, 0, 0, time.UTC), price: 321.22},
			},
		},
		{
			fixture: "fedstat_2021.xml",
			points:  4,
			spot: []fedstatPoint{
				{product: testProductBeef, region: fedstatRegionRFName, date: time.Date(2021, 1, 11, 0, 0, 0, 0, time.UTC), price: 361.85},
				{product: testProductBeef, region: fedstatRegionRFName, date: time.Date(2021, 12, 27, 0, 0, 0, 0, time.UTC), price: 416.49},
			},
		},
		{
			fixture: "fedstat_2026.xml",
			points:  4,
			spot: []fedstatPoint{
				{product: testProductBeef, region: fedstatRegionRFNewName, date: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC), price: 758.54},
				{product: testProductBeef, region: fedstatRegionRFNewName, date: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC), price: 760},
			},
		},
	}
	for _, tc := range cases {
		points := fedstatTestParse(t, tc.fixture)
		if len(points) != tc.points {
			t.Errorf("%s: %d points, want %d", tc.fixture, len(points), tc.points)
		}
		for _, want := range tc.spot {
			got := fedstatFindPoint(points, want.product, want.region, want.date)
			if got == nil {
				t.Errorf("%s: point %s/%s/%s not found", tc.fixture, want.product, want.region, want.date.Format("2006-01-02"))
				continue
			}
			if math.Abs(got.price-want.price) > 1e-9 {
				t.Errorf("%s: point %s/%s/%s = %v, want %v", tc.fixture, want.product, want.region, want.date.Format("2006-01-02"), got.price, want.price)
			}
		}
	}
}

func TestFedstatParseDeduplicatesExactRepeats(t *testing.T) {
	body := fedstatTestFixture(t, "fedstat_2016.xml")
	block := fedstatFirstSeriesBlock(t, body)
	text := strings.Replace(string(body), "</GenericData>", block+"\r\n</GenericData>", 1)
	points, err := fedstatParse([]byte(text))
	if err != nil {
		t.Fatalf("duplicate exact series: %v", err)
	}
	if len(points) != 6 {
		t.Errorf("duplicate exact series: %d points, want 6", len(points))
	}
}

func TestFedstatParseRejectsConflictingDuplicate(t *testing.T) {
	body := fedstatTestFixture(t, "fedstat_2016.xml")
	block := fedstatFirstSeriesBlock(t, body)
	conflict := strings.Replace(block, `ObsValue value="311,8"`, `ObsValue value="999,99"`, 1)
	if conflict == block {
		t.Fatal("fixture block does not contain 311,8")
	}
	text := strings.Replace(string(body), "</GenericData>", conflict+"\r\n</GenericData>", 1)
	if _, err := fedstatParse([]byte(text)); err == nil || !strings.Contains(err.Error(), "дубль") {
		t.Fatalf("conflicting duplicate: error = %v, want дубль", err)
	}
}

func TestFedstatParseRejectsAllEmptyValues(t *testing.T) {
	body := fedstatTestFixture(t, "fedstat_2016.xml")
	text := regexp.MustCompile(`ObsValue value="[^"]*"`).ReplaceAllString(string(body), `ObsValue value=""`)
	if _, err := fedstatParse([]byte(text)); err == nil || !strings.Contains(err.Error(), "0 точек") {
		t.Fatalf("all empty values: error = %v, want 0 точек", err)
	}
}

func TestFedstatParseRejectsUnknownProductAndRegion(t *testing.T) {
	body := fedstatTestFixture(t, "fedstat_2016.xml")
	if _, err := fedstatParse([]byte(strings.Replace(string(body), `concept="s_grtov" value="111"`, `concept="s_grtov" value="99999"`, 1))); err == nil || !strings.Contains(err.Error(), "s_grtov") {
		t.Fatalf("unknown product: error = %v, want s_grtov", err)
	}
	if _, err := fedstatParse([]byte(strings.Replace(string(body), `concept="s_OKATO" value="643"`, `concept="s_OKATO" value="999"`, 1))); err == nil || !strings.Contains(err.Error(), "s_OKATO") {
		t.Fatalf("unknown region: error = %v, want s_OKATO", err)
	}
}

func TestFedstatParseRejectsHTML(t *testing.T) {
	if _, err := fedstatParse([]byte("<html><body>no data</body></html>")); err == nil {
		t.Fatal("HTML body parsed as SDMX")
	}
}

func TestFedstatCheckCoverageRejectsPartial(t *testing.T) {
	points := []fedstatPoint{{product: testProductBeef, region: fedstatRegionRFName, date: time.Date(2016, 6, 13, 0, 0, 0, 0, time.UTC), price: 311.8}}
	if err := fedstatCheckCoverage(points); err == nil || !strings.Contains(err.Error(), "покрытие") {
		t.Fatalf("coverage error = %v, want покрытие", err)
	}
}

func fedstatTestFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return body
}

func fedstatTestParse(t *testing.T, name string) []fedstatPoint {
	t.Helper()
	points, err := fedstatParse(fedstatTestFixture(t, name))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return points
}

func fedstatFindPoint(points []fedstatPoint, product, region string, date time.Time) *fedstatPoint {
	for i := range points {
		if points[i].product == product && points[i].region == region && points[i].date.Equal(date) {
			return &points[i]
		}
	}
	return nil
}

func fedstatFirstSeriesBlock(t *testing.T, body []byte) string {
	t.Helper()
	match := regexp.MustCompile(`(?s)<generic:Series>.*?</generic:Series>`).Find(body)
	if match == nil {
		t.Fatal("no Series block in fixture")
	}
	return string(match)
}

func TestFedstatYearRegion(t *testing.T) {
	if got := fedstatYearRegion("2026"); got != fedstatRegionRFNew {
		t.Errorf("fedstatYearRegion(2026) = %q, want %q", got, fedstatRegionRFNew)
	}
	if got := fedstatYearRegion("2022"); got != fedstatRegionRF {
		t.Errorf("fedstatYearRegion(2022) = %q, want %q", got, fedstatRegionRF)
	}
}
