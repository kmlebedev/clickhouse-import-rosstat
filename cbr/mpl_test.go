package cbr

import (
	"os"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// TestMPLPage — landing МПЛ: 6 файлов лимитов и 4 файла границ, сегменты и
// сортировка по дате начала действия.
func TestMPLPage(t *testing.T) {
	body, err := os.ReadFile("testdata/mpl_page.html")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	limits, ranges, err := cbrMPLParsePage(body)
	if err != nil {
		t.Fatalf("parse page: %v", err)
	}
	if len(limits) != 6 {
		t.Fatalf("лимитов %d, want 6", len(limits))
	}
	if len(ranges) != 4 {
		t.Fatalf("границ %d, want 4", len(ranges))
	}
	if limits[0].Name != "Macroprudential_limits_banks.xlsx" {
		t.Fatalf("первый лимит %q", limits[0].Name)
	}
	if limits[len(limits)-1].Name != "Macroprudential_limits_mfo_01072025.xlsx" {
		t.Fatalf("последний лимит %q", limits[len(limits)-1].Name)
	}
	if ranges[0].Segment != "bank" || ranges[0].Kind != "ranges" {
		t.Fatalf("первые границы: %+v", ranges[0])
	}
}

// TestMPLRanges — границы v1 (две) и v2 (четыре): ключ периода и измерения,
// «х» — nil, а не ноль.
func TestMPLRanges(t *testing.T) {
	r2023 := parseRangesFile(t, "testdata/mpl_ranges_banks_2023.xlsx",
		mplFile{Name: "mpl_ranges_banks_2023.xlsx", Segment: "bank"})
	key := mplRangeKey{segment: "bank", dimension: "pdn",
		from: date(2023, 1, 1), to: date(2023, 3, 31)}
	r, ok := r2023[key]
	if !ok {
		t.Fatal("нет границ pdn 2023Q1")
	}
	if r.a != nil || r.b == nil || *r.b != 80 {
		t.Fatalf("2023Q1 pdn: a=%v b=%v", r.a, r.b)
	}

	r2024 := parseRangesFile(t, "testdata/mpl_ranges_banks_2024.xlsx",
		mplFile{Name: "mpl_ranges_banks_2024.xlsx", Segment: "bank"})
	key = mplRangeKey{segment: "bank", dimension: "pdn",
		from: date(2024, 4, 1), to: date(2024, 6, 30)}
	r, ok = r2024[key]
	if !ok {
		t.Fatal("нет границ pdn 2024Q2")
	}
	if r.a != nil || r.b != nil || r.c == nil || *r.c != 50 || r.d == nil || *r.d != 80 {
		t.Fatalf("2024Q2 pdn: a=%v b=%v c=%v d=%v", r.a, r.b, r.c, r.d)
	}
}

// TestMPLParse — три поколения формата: v3 (проценты, подстроки), v1 (доли,
// две полосы), v2 (доли, четыре полосы) и МФО v3.
func TestMPLParse(t *testing.T) {
	// v3 банк: текущий формат, подстроки наследуют лимит.
	v3 := parseLimitFile(t, "testdata/mpl_banks_2025.xlsx",
		mplFile{Name: "mpl_banks_2025.xlsx", Segment: "bank"}, nil)
	assertMPLPoint(t, v3, "consumer", date(2026, 10, 1), date(2026, 12, 31),
		51, "51.1", 0, 10, floatPtr(50), nil, "более 50")
	assertMPLPoint(t, v3, "mortgage", date(2025, 7, 1), date(2025, 9, 30),
		1, "1.1", 0, 5, floatPtr(0), floatPtr(50), "[0; 50]")
	assertMPLPoint(t, v3, "mortgage", date(2025, 7, 1), date(2025, 9, 30),
		1, "1.2", 0, 5, floatPtr(80), nil, "более 80")

	// v1 банк: 0.25 → 25 %, полоса «более 80».
	r1 := parseRangesFile(t, "testdata/mpl_ranges_banks_2023.xlsx",
		mplFile{Name: "mpl_ranges_banks_2023.xlsx", Segment: "bank"})
	v1 := parseLimitFile(t, "testdata/mpl_banks_2023.xlsx",
		mplFile{Name: "mpl_banks_2023.xlsx", Segment: "bank"}, r1)
	assertMPLPoint(t, v1, "consumer_with_limit", date(2023, 1, 1), date(2023, 3, 31),
		1, "", 1, 25, floatPtr(80), nil, "более 80")

	// v2 банк: 0.1 → 10 %, полоса 50…80.
	r2 := parseRangesFile(t, "testdata/mpl_ranges_banks_2024.xlsx",
		mplFile{Name: "mpl_ranges_banks_2024.xlsx", Segment: "bank"})
	v2 := parseLimitFile(t, "testdata/mpl_banks_2024.xlsx",
		mplFile{Name: "mpl_banks_2024.xlsx", Segment: "bank"}, r2)
	assertMPLPoint(t, v2, "consumer_with_limit", date(2024, 4, 1), date(2024, 6, 30),
		1, "", 2, 10, floatPtr(50), floatPtr(80), "(50; 80]")

	// v3 МФО: проценты, полоса ПДН.
	mfo := parseLimitFile(t, "testdata/mpl_mfo_2025.xlsx",
		mplFile{Name: "mpl_mfo_2025.xlsx", Segment: "mfo"}, nil)
	assertMPLPoint(t, mfo, "consumer_loan", date(2025, 7, 1), date(2025, 9, 30),
		1, "1.1", 0, 10, floatPtr(50), floatPtr(80), "(50; 80]")
}

func parseLimitFile(t *testing.T, path string, file mplFile, ranges mplRanges) []mplPoint {
	t.Helper()
	xlsx, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = xlsx.Close() }()
	if ranges == nil {
		ranges = mplRanges{}
	}
	points, err := cbrMPLParseBook(xlsx, file, ranges)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(points) == 0 {
		t.Fatalf("%s: 0 точек", path)
	}
	return points
}

func parseRangesFile(t *testing.T, path string, file mplFile) mplRanges {
	t.Helper()
	xlsx, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = xlsx.Close() }()
	ranges, err := cbrMPLParseRanges(xlsx, file)
	if err != nil {
		t.Fatalf("parse ranges %s: %v", path, err)
	}
	return ranges
}

func assertMPLPoint(t *testing.T, points []mplPoint, class string, from, to time.Time,
	rowNo uint32, subrow string, bandIdx uint8, limit float64, pdnMin, pdnMax *float64,
	pdnBand string) {
	t.Helper()
	for _, p := range points {
		if p.loanClass != class || !p.periodFrom.Equal(from) || !p.periodTo.Equal(to) ||
			p.rowNo != rowNo || p.subrowNo != subrow || p.bandIdx != bandIdx {
			continue
		}
		if p.limitPct != limit {
			t.Fatalf("%s %s..%s row %d/%s band %d: limit=%v, want %v",
				class, from.Format("2006-01-02"), to.Format("2006-01-02"), rowNo, subrow,
				bandIdx, p.limitPct, limit)
		}
		if p.pdnBand != pdnBand {
			t.Fatalf("%s row %d/%s: pdn_band=%q, want %q", class, rowNo, subrow, p.pdnBand, pdnBand)
		}
		if !floatPtrEqual(p.pdnMin, pdnMin) || !floatPtrEqual(p.pdnMax, pdnMax) {
			t.Fatalf("%s row %d/%s: pdn=%v..%v, want %v..%v", class, rowNo, subrow,
				ptrValue(p.pdnMin), ptrValue(p.pdnMax), ptrValue(pdnMin), ptrValue(pdnMax))
		}
		return
	}
	t.Fatalf("нет точки %s %s..%s row %d/%s band %d", class,
		from.Format("2006-01-02"), to.Format("2006-01-02"), rowNo, subrow, bandIdx)
}

func floatPtrEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func floatPtr(v float64) *float64 {
	return &v
}

func ptrValue(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}

func date(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
