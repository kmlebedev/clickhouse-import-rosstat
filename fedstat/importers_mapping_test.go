package fedstat

import (
	"testing"
	"time"
)

func fedstatParseSourceFixture(t *testing.T, name string, source fedstatSourceSpec, year int) []fedstatRecord {
	t.Helper()
	records, err := fedstatParseExcel(fedstatFixture(t, name), source.layout, year, source.labelCount)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return records
}

func TestFedstatRetailMapping(t *testing.T) {
	indexSource := fedstatRetailSources[0]
	records := fedstatParseSourceFixture(t, "retail_31066_2000.xls", indexSource.source, 2000)
	points := make([]fedstatRetailPoint, 0, len(records))
	for _, record := range records {
		point, err := fedstatRetailPointFrom(indexSource, record, fedstatRegionRF)
		if err != nil {
			t.Fatalf("index point: %v", err)
		}
		points = append(points, point)
	}
	if len(points) != 105 {
		t.Fatalf("index points = %d, want 105", len(points))
	}
	cumulative := 0
	for _, point := range points {
		if point.periodType == "CUM" {
			cumulative++
		}
		if point.series != "index" || point.unit != "процент" {
			t.Errorf("point = %+v", point)
		}
	}
	if cumulative != 33 {
		t.Errorf("index CUM = %d, want 33", cumulative)
	}
	if len(fedstatRetailKeys(points)) != len(points) {
		t.Error("index keys not unique: cumulative periods collapsed")
	}

	turnoverSource := fedstatRetailSources[1]
	records = fedstatParseSourceFixture(t, "retail_31260_2000.xls", turnoverSource.source, 2000)
	turnover := make([]fedstatRetailPoint, 0, len(records))
	for _, record := range records {
		point, err := fedstatRetailPointFrom(turnoverSource, record, fedstatRegionRF)
		if err != nil {
			t.Fatalf("turnover point: %v", err)
		}
		turnover = append(turnover, point)
	}
	if len(turnover) != 69 {
		t.Fatalf("turnover points = %d, want 69", len(turnover))
	}
	if len(fedstatRetailKeys(turnover)) != len(turnover) {
		t.Error("turnover keys not unique: cumulative periods collapsed")
	}
	if turnover[0].indicatorType != "Оборот розничной торговли" || turnover[0].unit != "миллион рублей" {
		t.Errorf("turnover point = %+v", turnover[0])
	}
}

func fedstatRetailKeys(points []fedstatRetailPoint) map[string]bool {
	keys := make(map[string]bool, len(points))
	for _, point := range points {
		keys[point.key()] = true
	}
	return keys
}

func TestFedstatIPPMapping(t *testing.T) {
	oldSource := fedstatIPPSources[0]
	records := fedstatParseSourceFixture(t, "ipp_43046_2016.xls", oldSource.source, 2016)
	if len(records) != 36 {
		t.Fatalf("43046 records = %d, want 36", len(records))
	}
	point, err := fedstatIPPPointFrom(oldSource, records[0], fedstatRegionRF)
	if err != nil {
		t.Fatalf("43046 point: %v", err)
	}
	if point.okvedVersion != "OKVED1" || point.okved != "Промышленность" {
		t.Errorf("43046 point = %+v", point)
	}

	newSource := fedstatIPPSources[1]
	records = fedstatParseSourceFixture(t, "ipp_57806_2024.xls", newSource.source, 2024)
	if len(records) != 4608 {
		t.Fatalf("57806 records = %d, want 4608", len(records))
	}
	point, err = fedstatIPPPointFrom(newSource, records[0], fedstatRegionRFNew)
	if err != nil {
		t.Fatalf("57806 point: %v", err)
	}
	if point.okvedVersion != "OKVED2" || point.region != fedstatRegionRFNewName {
		t.Errorf("57806 point = %+v", point)
	}
}

func TestFedstatAgriMapping(t *testing.T) {
	source := fedstatAgriSources[0]
	records := fedstatParseSourceFixture(t, "agri_34128_2024.xls", source.source, 2024)
	if len(records) != 8 {
		t.Fatalf("34128 records = %d, want 8", len(records))
	}
	point, err := fedstatAgriPointFrom(source, records[0], fedstatRegionRFNew)
	if err != nil {
		t.Fatalf("34128 point: %v", err)
	}
	if point.activity != "Сельское хозяйство" || point.region != fedstatRegionRFNewName || point.series != "index_q" {
		t.Errorf("34128 point = %+v", point)
	}

	source = fedstatAgriSources[1]
	records = fedstatParseSourceFixture(t, "agri_59001_2024.xls", source.source, 2024)
	if len(records) != 12 {
		t.Fatalf("59001 records = %d, want 12", len(records))
	}
	point, err = fedstatAgriPointFrom(source, records[0], fedstatRegionRFNew)
	if err != nil {
		t.Fatalf("59001 point: %v", err)
	}
	if point.activity == "" || point.series != "index_y" {
		t.Errorf("59001 point = %+v", point)
	}

	source = fedstatAgriSources[2]
	records = fedstatParseSourceFixture(t, "agri_31062_1991-2016.xls", source.source, 0)
	if len(records) != 312 {
		t.Fatalf("31062 records = %d, want 312", len(records))
	}
	point, err = fedstatAgriPointFrom(source, records[0], fedstatRegionRF)
	if err != nil {
		t.Fatalf("31062 point: %v", err)
	}
	if point.category == "" || point.activity == "" || point.region == "" {
		t.Errorf("31062 point = %+v", point)
	}

	source = fedstatAgriSources[3]
	records = fedstatParseSourceFixture(t, "agri_43337_1990-2011.xls", source.source, 0)
	if len(records) != 85 {
		t.Fatalf("43337 records = %d, want 85", len(records))
	}
	point, err = fedstatAgriPointFrom(source, records[0], fedstatRegionRF)
	if err != nil {
		t.Fatalf("43337 point: %v", err)
	}
	if point.unit != "миллион рублей" || point.series != "value_y" {
		t.Errorf("43337 point = %+v", point)
	}
}

func TestFedstatInvestmentMapping(t *testing.T) {
	cases := []struct {
		index     int
		fixture   string
		year      int
		want      int
		summary   string
		unit      string
		indicator string
	}{
		{0, "investment_58089_2024.xls", 2024, 4, "Полный круг", "процент", "58089"},
		{1, "investment_58090_2024.xls", 2024, 4, "Полный круг", "тысяча рублей", "58090"},
		{2, "investment_34129_2000-2022.xls", 0, 23, "Всего", "процент", "34129"},
		{3, "investment_40562_2005-2022.xls", 0, 18, "Всего", "миллион рублей", "40562"},
		{4, "investment_34475_1998-2016.xls", 0, 19, "Полный круг", "миллион рублей", "34475"},
	}
	for _, test := range cases {
		item := fedstatInvestmentSources[test.index]
		records := fedstatParseSourceFixture(t, test.fixture, item.source, test.year)
		if len(records) != test.want {
			t.Fatalf("%s records = %d, want %d", test.indicator, len(records), test.want)
		}
		point, err := fedstatInvestmentPointFrom(item, records[0], fedstatRegionRF)
		if err != nil {
			t.Fatalf("%s point: %v", test.indicator, err)
		}
		if point.summaryType != test.summary || point.unit != test.unit || point.series == "" {
			t.Errorf("%s point = %+v", test.indicator, point)
		}
	}
}

func TestFedstatIPCMapping(t *testing.T) {
	records := fedstatParseSourceFixture(t, "ipc_31074_2002.xls", fedstatIPCSource, 2002)
	if len(records) != 6876 {
		t.Fatalf("IPC records = %d, want 6876", len(records))
	}
	points := make([]fedstatIPCPoint, 0, len(records))
	for _, record := range records {
		point, err := fedstatIPCPointFrom(record, fedstatRegionRF)
		if err != nil {
			t.Fatalf("IPC point: %v", err)
		}
		points = append(points, point)
	}
	keys := make(map[string]bool, len(points))
	for _, point := range points {
		keys[point.key()] = true
	}
	if len(keys) != len(points) {
		t.Errorf("IPC keys = %d, points = %d", len(keys), len(points))
	}
	for _, point := range points {
		if point.indicatorType != fedstatIPCType || point.date.Before(time.Date(2002, 1, 1, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("point = %+v", point)
		}
	}
}
