package rosstat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

func openPPIWeightsFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile("testdata/ppi_weights.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

func parsePPIWeightsFixture(t *testing.T) []ppiWeightPoint {
	t.Helper()
	points, err := parsePPIWeights(openPPIWeightsFixture(t))
	if err != nil {
		t.Fatalf("parsePPIWeights: %v", err)
	}
	return points
}

func ppiWeightFind(points []ppiWeightPoint, weightType string, year uint16, code string) (ppiWeightPoint, bool) {
	for _, point := range points {
		if point.weightType == weightType && point.year == year && point.code == code {
			return point, true
		}
	}
	return ppiWeightPoint{}, false
}

func TestPPIWeightsFindLink(t *testing.T) {
	page, err := os.ReadFile("testdata/price_page.html")
	if err != nil {
		t.Fatal(err)
	}
	href, err := util.FindLink(page, ppiWeightsLinkSelector)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
	}
	want := "/storage/mediabank/vesa_2006-2026.xlsx"
	if href != want {
		t.Errorf("FindLink() = %q, ожидалось %q", href, want)
	}
}

func TestPPIWeightsReadsFixture(t *testing.T) {
	points := parsePPIWeightsFixture(t)
	if len(points) != 758 {
		t.Fatalf("точек %d, ожидалось 758", len(points))
	}
	counts := make(map[string]int, 2)
	codes := make(map[string]map[string]bool, 2)
	years := make(map[string]map[uint16]bool, 2)
	for _, point := range points {
		counts[point.weightType]++
		if codes[point.weightType] == nil {
			codes[point.weightType] = make(map[string]bool)
			years[point.weightType] = make(map[uint16]bool)
		}
		codes[point.weightType][point.code] = true
		years[point.weightType][point.year] = true
	}
	for _, weightType := range []string{ppiWeightDomestic, ppiWeightDomesticExport} {
		if counts[weightType] != 379 {
			t.Errorf("%s: точек %d, ожидалось 379", weightType, counts[weightType])
		}
		if len(codes[weightType]) != 38 {
			t.Errorf("%s: кодов %d, ожидалось 38", weightType, len(codes[weightType]))
		}
		for year := uint16(2017); year <= 2026; year++ {
			if !years[weightType][year] {
				t.Errorf("%s: нет точек за %d", weightType, year)
			}
		}
	}

	for _, tc := range []struct {
		weightType string
		year       uint16
		want       float64
	}{
		{ppiWeightDomestic, 2017, 19.61},
		{ppiWeightDomestic, 2026, 16.27},
		{ppiWeightDomesticExport, 2017, 23.08},
		{ppiWeightDomesticExport, 2026, 19.56},
	} {
		point, ok := ppiWeightFind(points, tc.weightType, tc.year, "B")
		if !ok {
			t.Errorf("%s %d B: точки нет", tc.weightType, tc.year)
			continue
		}
		if point.value != tc.want {
			t.Errorf("%s %d B = %v, ожидалось %v", tc.weightType, tc.year, point.value, tc.want)
		}
	}

	for _, weightType := range []string{ppiWeightDomestic, ppiWeightDomesticExport} {
		for year := uint16(2017); year <= 2026; year++ {
			point, ok := ppiWeightFind(points, weightType, year, ppiWeightTotalCode)
			if !ok {
				t.Errorf("%s %d %s: точки нет", weightType, year, ppiWeightTotalCode)
				continue
			}
			if point.value != 100 {
				t.Errorf("%s %d %s = %v, ожидалось 100", weightType, year, ppiWeightTotalCode, point.value)
			}
		}
	}
}

func TestPPIWeightsType(t *testing.T) {
	cases := []struct {
		title string
		want  string
		ok    bool
	}{
		{"Структура весов ... (на товары, предназначенные для реализации на внутреннем рынке)", ppiWeightDomestic, true},
		{"Структура весов ... (на товары, предназначенные для реализации на внутреннем рынке и на экспорт)", ppiWeightDomesticExport, true},
		{"Содержание", "", false},
	}
	for _, tc := range cases {
		got, ok := ppiWeightSheetKind([][]string{{tc.title}})
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("ppiWeightSheetKind(%q) = (%q, %v), ожидалось (%q, %v)", tc.title, got, ok, tc.want, tc.ok)
		}
	}
}

func TestPPIWeightsImportAppends(t *testing.T) {
	rec := &ppiRecordingBatch{}
	if err := ppiWeightsImport(openPPIWeightsFixture(t), rec); err != nil {
		t.Fatalf("ppiWeightsImport: %v", err)
	}
	if len(rec.rows) != 758 {
		t.Fatalf("записано строк %d, ожидалось 758", len(rec.rows))
	}
	for _, row := range rec.rows {
		if len(row) != 5 {
			t.Fatalf("Append с %d аргументами, ожидалось 5", len(row))
		}
		if _, ok := row[0].(uint16); !ok {
			t.Fatalf("year %T, ожидалось uint16", row[0])
		}
		for _, i := range []int{1, 2, 3} {
			if _, ok := row[i].(string); !ok {
				t.Fatalf("колонка %d: %T, ожидалась строка", i, row[i])
			}
		}
		if _, ok := row[4].(float64); !ok {
			t.Fatalf("value %T, ожидалось float64", row[4])
		}
	}
}

func TestPPIWeightsDdl(t *testing.T) {
	for _, fragment := range []string{
		"ReplacingMergeTree",
		"year UInt16",
		"code LowCardinality(String)",
		"name LowCardinality(String)",
		"weight_type LowCardinality(String)",
		"value Float64",
		"ORDER BY (year, code, weight_type)",
	} {
		if !strings.Contains(ppiWeightsDdl, fragment) {
			t.Errorf("DDL не содержит %q:\n%s", fragment, ppiWeightsDdl)
		}
	}
}

func TestPPIWeightsRestoreRefused(t *testing.T) {
	t.Setenv(util.EnvArchiveAction, util.ArchiveActionRestore)
	importer := &PPIWeightsStat{}
	if _, err := importer.Import(context.Background(), nil); err == nil {
		t.Fatal("Import в режиме restore вернул nil, ожидалась ошибка")
	} else if !strings.Contains(err.Error(), "восстановления") {
		t.Errorf("ошибка restore не называет режим: %v", err)
	}
}

func TestPPIWeightsCatalog(t *testing.T) {
	points := parsePPIWeightsFixture(t)
	seen := make(map[string]bool)
	rows := make([]ppiWeightCatalogRow, 0, 76)
	for _, point := range points {
		key := point.code + "\x00" + point.weightType
		if seen[key] {
			continue
		}
		seen[key] = true
		rows = append(rows, ppiWeightCatalogRow{code: point.code, weightType: point.weightType, name: point.name})
	}
	metas := ppiWeightsSeriesMeta(rows)
	if len(metas) != 76 {
		t.Fatalf("серий каталога %d, ожидалось 76", len(metas))
	}
	series := make(map[string]bool, len(metas))
	for _, meta := range metas {
		if !strings.HasPrefix(meta.Series, "ved-weight:") {
			t.Errorf("series %q без префикса ved-weight:", meta.Series)
		}
		if series[meta.Series] {
			t.Errorf("series %q встречается дважды", meta.Series)
		}
		series[meta.Series] = true
		if meta.Source != rosstatSource || meta.Frequency != "A" || meta.Unit == "" || meta.Title == "" || meta.Description == "" {
			t.Errorf("неполная запись каталога: %+v", meta)
		}
	}
}

func TestPPIWeightsCoverageRejectsBadTotal(t *testing.T) {
	types := []string{ppiWeightDomestic, ppiWeightDomesticExport}
	points := make([]ppiWeightPoint, 0, 760)
	for _, weightType := range types {
		for year := uint16(2017); year <= 2026; year++ {
			for i := 0; i < ppiWeightCodes-1; i++ {
				points = append(points, ppiWeightPoint{year: year, code: fmt.Sprintf("code-%02d", i), weightType: weightType, value: 10})
			}
			points = append(points, ppiWeightPoint{year: year, code: ppiWeightTotalCode, weightType: weightType, value: 100})
		}
	}
	if err := ppiWeightsValidateCoverage(points); err != nil {
		t.Fatalf("корректные веса: %v", err)
	}
	for i := range points {
		if points[i].weightType == ppiWeightDomestic && points[i].year == 2020 && points[i].code == ppiWeightTotalCode {
			points[i].value = 90
			break
		}
	}
	if err := ppiWeightsValidateCoverage(points); err == nil {
		t.Fatal("ppiWeightsValidateCoverage принял сумму 90, ожидалась ошибка")
	}
}

func TestPPIWeightsKnownGap(t *testing.T) {
	points := parsePPIWeightsFixture(t)
	if len(points) != 758 {
		t.Fatalf("точек %d, ожидалось 758 (2 × (38 × 10 − 1))", len(points))
	}
	for _, weightType := range []string{ppiWeightDomestic, ppiWeightDomesticExport} {
		if point, ok := ppiWeightFind(points, weightType, 2017, "38"); ok {
			t.Errorf("%s 2017 код 38: источник даёт «-», а получена точка %+v", weightType, point)
		}
	}
}

func TestPPIWeightsEmptyWorkbookFails(t *testing.T) {
	xlsx := excelize.NewFile()
	t.Cleanup(func() { _ = xlsx.Close() })
	if _, err := parsePPIWeights(xlsx); err == nil {
		t.Fatal("parsePPIWeights на пустой книге вернул nil, ожидалась ошибка")
	}
}
