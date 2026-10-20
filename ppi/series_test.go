package ppi

import (
	"math"
	"testing"
	"time"
)

func month(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func almostEqual(t *testing.T, got, want, tolerance float64, what string) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %v, want %v (±%v)", what, got, want, tolerance)
	}
}

func TestEmbeddedMapping(t *testing.T) {
	mapping, err := parseMapping(ppiMappingCSV)
	if err != nil {
		t.Fatalf("parseMapping: %v", err)
	}
	if mapping.Version != ppiSATaxonomyVersion {
		t.Fatalf("taxonomy_version = %q, want %q", mapping.Version, ppiSATaxonomyVersion)
	}
	leaves := 0
	for _, codes := range mapping.LeavesByGroup {
		leaves += len(codes)
	}
	if leaves != 585 {
		t.Fatalf("mapped-листьев %d, want 585 (v2)", leaves)
	}
	if len(mapping.LeavesByGroup) != 45 {
		t.Fatalf("групп %d, want 45 (v2)", len(mapping.LeavesByGroup))
	}
	for _, group := range []string{"01.2.1", "09.3.2", "04.3.1.1.1", "04.3.1.1.10.1"} {
		if _, ok := mapping.LeavesByGroup[group]; !ok {
			t.Errorf("в mapping нет группы %s", group)
		}
	}
}

func TestEmbeddedWeights(t *testing.T) {
	mapping, err := parseMapping(ppiMappingCSV)
	if err != nil {
		t.Fatalf("parseMapping: %v", err)
	}
	weights, err := parseWeights(ppiWeightsCSV)
	if err != nil {
		t.Fatalf("parseWeights: %v", err)
	}
	if weights.ByPeriod["2022-04"] == nil {
		t.Fatal("в панели нет периода 2022-04")
	}
	if got := weights.ByPeriod[ppiSAWeightPeriod]["01.2.1"]; got != 0.286 {
		t.Errorf("вес 01.2.1 = %v, want 0.286", got)
	}
	if got := weights.ByPeriod[ppiSAWeightPeriod]["09.3.2"]; got != 0.456 {
		t.Errorf("вес 09.3.2 = %v, want 0.456", got)
	}
	if err := checkWeights(mapping, weights); err != nil {
		t.Fatalf("checkWeights: %v", err)
	}
}

func TestIsKIPCChild(t *testing.T) {
	cases := []struct {
		code, parent string
		want         bool
	}{
		{"04.3.1.1.10.1", "04.3.1.1.10", true},
		{"04.3.1.1.1", "04.3.1.1", true},
		{"04.3.1.1.1", "04.3.1.1.10", false},
		{"04.3.1.1.10", "04.3.1.1.1", false},
		{"01.2.1", "01.2", true},
		{"01.2", "01.2", false},
	}
	for _, tc := range cases {
		if got := isKIPCChild(tc.code, tc.parent); got != tc.want {
			t.Errorf("isKIPCChild(%q, %q) = %v, want %v", tc.code, tc.parent, got, tc.want)
		}
	}
}

func TestMembersForGroupsDedup(t *testing.T) {
	leaves := map[string][]string{
		"04.3.1.1":    {"A"},
		"04.3.1.1.1":  {"B"},
		"04.3.1.1.10": {"C"},
	}
	got := membersForGroups(leaves, []string{"04.3.1.1"})
	if len(got) != 1 {
		t.Fatalf("members = %v, want только родительскую группу: дочерние не должны считаться дважды", got)
	}
	if _, ok := got["04.3.1.1"]; !ok {
		t.Fatalf("members = %v, want 04.3.1.1", got)
	}

	withoutParent := map[string][]string{
		"04.3.1.1.1":  {"B"},
		"04.3.1.1.10": {"C"},
	}
	got = membersForGroups(withoutParent, []string{"04.3.1.1"})
	if len(got) != 2 {
		t.Fatalf("members = %v, want обе дочерние группы (соседи)", got)
	}
}

func TestAggregateScope(t *testing.T) {
	weights := map[string]float64{"03.1": 10, "03.1.4": 2, "03.2": 5, "03.2.2": 1}
	members := map[string][]string{"03.1": {"A", "B"}, "03.2": {"C"}}
	points := map[string]map[time.Time]float64{
		"A": {month(2021, 1, 31): 100, month(2021, 2, 28): 110},
		"B": {month(2021, 1, 31): 100, month(2021, 2, 28): 120},
		"C": {month(2021, 1, 31): 200, month(2021, 2, 28): 200},
	}
	got, err := aggregateScope(members, weights, points)
	if err != nil {
		t.Fatalf("aggregateScope: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("агрегат = %v, want 2 месяца", got)
	}
	// январь: 03.1 net 10-2=8, 03.2 net 5-1=4: (8*100 + 4*200) / 12
	almostEqual(t, got[0].Value, (8*100+4*200)/12.0, 1e-9, "январь")
	// февраль: 03.1 среднее (110+120)/2=115, вес 8; 03.2 = 200, вес 4
	almostEqual(t, got[1].Value, (8*115+4*200)/12.0, 1e-9, "февраль")
}

func TestAggregateScopeMissingWeight(t *testing.T) {
	_, err := aggregateScope(
		map[string][]string{"03.1": {"A"}},
		map[string]float64{"03.1": 10},
		map[string]map[time.Time]float64{"A": {month(2021, 1, 31): 100}},
	)
	if err == nil {
		t.Fatal("ожидалась ошибка отсутствующего веса вычитаемой группы 03.1.4")
	}
}

func TestChainSeries(t *testing.T) {
	points := []monthlyValue{
		{Date: month(2013, 1, 31), Value: 100},
		{Date: month(2013, 2, 28), Value: 110},
		{Date: month(2013, 3, 31), Value: 99},
	}
	got, err := chainSeries(points, month(2013, 1, 1))
	if err != nil {
		t.Fatalf("chainSeries: %v", err)
	}
	want := []float64{100, 110, 108.9}
	for i, w := range want {
		almostEqual(t, got[i].Value, w, 1e-9, "уровень")
	}
}

func TestChainSeriesGap(t *testing.T) {
	points := []monthlyValue{
		{Date: month(2013, 1, 31), Value: 100},
		{Date: month(2013, 3, 31), Value: 100},
	}
	if _, err := chainSeries(points, month(2013, 1, 1)); err == nil {
		t.Fatal("ожидалась ошибка пропуска месяца")
	}
}

func TestChainSeriesStartMismatch(t *testing.T) {
	points := []monthlyValue{{Date: month(2013, 2, 28), Value: 100}}
	if _, err := chainSeries(points, month(2013, 1, 1)); err == nil {
		t.Fatal("ожидалась ошибка несовпадения первого месяца")
	}
}

func TestParseMappingDuplicateCode(t *testing.T) {
	data := []byte("taxonomy_version,okpd2,is_leaf,kipc_group,status\n" +
		"v,01.1,1,01.1.1,mapped\n" +
		"v,01.1,1,01.1.2,mapped\n")
	if _, err := parseMapping(data); err == nil {
		t.Fatal("ожидалась ошибка дубля кода")
	}
}

func TestParseWeightsDuplicate(t *testing.T) {
	data := []byte("period,code,name,weight\n" +
		"2026,01.1.1,a,1\n" +
		"2026,01.1.1,a,2\n")
	if _, err := parseWeights(data); err == nil {
		t.Fatal("ожидалась ошибка дубля (period, code)")
	}
}

func TestBuildSeriesConstant(t *testing.T) {
	mapping, err := parseMapping(ppiMappingCSV)
	if err != nil {
		t.Fatalf("parseMapping: %v", err)
	}
	weights, err := parseWeights(ppiWeightsCSV)
	if err != nil {
		t.Fatalf("parseWeights: %v", err)
	}

	ved := make([]rawPoint, 0, 36)
	for i := 0; i < 36; i++ {
		date := time.Date(2013, time.Month(1+i), 1, 0, 0, 0, 0, time.UTC)
		ved = append(ved, rawPoint{Date: date, Code: ppiSATotalCode, Value: 100})
	}
	product := make([]rawPoint, 0, len(mapping.LeavesByGroup)*36)
	for _, codes := range mapping.LeavesByGroup {
		for _, code := range codes {
			for i := 0; i < 36; i++ {
				date := time.Date(2021, time.Month(1+i), 1, 0, 0, 0, 0, time.UTC)
				product = append(product, rawPoint{Date: date, Code: code, Value: 100})
			}
		}
	}
	series, err := buildSeries(ved, product, mapping, weights)
	if err != nil {
		t.Fatalf("buildSeries: %v", err)
	}
	if len(series) != len(ppiSAOrder) {
		t.Fatalf("рядов %d, want %d", len(series), len(ppiSAOrder))
	}
	for _, name := range ppiSAOrder {
		points := series[name]
		if len(points) != 36 {
			t.Errorf("%s: %d точек, want 36", name, len(points))
			continue
		}
		for _, point := range points {
			almostEqual(t, point.Value, 100, 1e-9, name)
		}
	}
	if got := series["total"][0].Date; got != month(2013, 1, 1) {
		t.Errorf("total начинается %s, want 2013-01-01", got.Format("2006-01-02"))
	}
	if got := series["food"][0].Date; got != month(2021, 1, 1) {
		t.Errorf("food начинается %s, want 2021-01-01", got.Format("2006-01-02"))
	}
}
