package minfin

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

type ofzAuctionRow struct {
	date          time.Time
	format        string
	issue         string
	paperType     string
	maturity      time.Time
	days          uint32
	offer         float64
	cutoffPrice   any
	weightedPrice any
	cutoffYield   any
	weightedYield any
	demand        any
	placed        any
	revenue       any
	coverage      any
}

type ofzAuctionRecorder struct {
	driver.Batch
	rows []ofzAuctionRow
	err  error
}

func (r *ofzAuctionRecorder) Append(v ...any) error {
	if len(v) != 15 {
		r.err = fmt.Errorf("строка из %d значений, ожидалось 15", len(v))
		return r.err
	}
	row := ofzAuctionRow{}
	var ok bool
	if row.date, ok = v[0].(time.Time); !ok {
		return r.fail("date", v[0])
	}
	if row.format, ok = v[1].(string); !ok {
		return r.fail("format", v[1])
	}
	if row.issue, ok = v[2].(string); !ok {
		return r.fail("issue", v[2])
	}
	if row.paperType, ok = v[3].(string); !ok {
		return r.fail("paper_type", v[3])
	}
	if row.maturity, ok = v[4].(time.Time); !ok {
		return r.fail("maturity_date", v[4])
	}
	if row.days, ok = v[5].(uint32); !ok {
		return r.fail("days_to_maturity", v[5])
	}
	if row.offer, ok = v[6].(float64); !ok {
		return r.fail("offer_volume", v[6])
	}
	row.cutoffPrice, row.weightedPrice = v[7], v[8]
	row.cutoffYield, row.weightedYield = v[9], v[10]
	row.demand, row.placed, row.revenue, row.coverage = v[11], v[12], v[13], v[14]
	for name, value := range map[string]any{
		"cutoff_price": v[7], "weighted_price": v[8], "cutoff_yield": v[9],
		"weighted_yield": v[10], "demand": v[11], "placed": v[12],
		"revenue": v[13], "coverage": v[14],
	} {
		if value != nil {
			if _, ok := value.(float64); !ok {
				return r.fail(name, value)
			}
		}
	}
	r.rows = append(r.rows, row)
	return nil
}

func (r *ofzAuctionRecorder) fail(name string, value any) error {
	r.err = fmt.Errorf("%s пришёл как %T", name, value)
	return r.err
}

func openOfzAuctionFixture(t *testing.T, name string) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", name, err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

func ofzDay(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func ofzFloat(t *testing.T, value any, name string) float64 {
	t.Helper()
	if value == nil {
		t.Fatalf("%s: nil, ожидалось число", name)
	}
	f, ok := value.(float64)
	if !ok {
		t.Fatalf("%s: %T, ожидался float64", name, value)
	}
	return f
}

func TestOfzAuctionImport2023Layout(t *testing.T) {
	rec := &ofzAuctionRecorder{}
	if err := parseOfzAuction(openOfzAuctionFixture(t, "INTERNET_Auction_Results_rus_2023_20231231.xlsx"), rec); err != nil {
		t.Fatalf("parseOfzAuction 2023: %v", err)
	}
	if len(rec.rows) == 0 {
		t.Fatal("2023: ноль строк")
	}
	first := rec.rows[0]
	if !first.date.Equal(ofzDay(2023, time.January, 11)) {
		t.Errorf("2023: первая дата %s, ожидалась 2023-01-11", first.date.Format("2006-01-02"))
	}
	if first.format != "Аукцион" {
		t.Errorf("2023: format %q, ожидался «Аукцион» (в файле 2023 нет колонки «Формат»)", first.format)
	}
	if first.issue != "26241RMFS" || first.paperType != "ОФЗ-ПД" {
		t.Errorf("2023: выпуск/тип %q/%q", first.issue, first.paperType)
	}
	if first.days != 3598 {
		t.Errorf("2023: дней до погашения %d, ожидалось 3598", first.days)
	}
	if got := ofzFloat(t, first.coverage, "coverage"); got < 0.283 || got > 0.284 {
		t.Errorf("2023: coverage %v, ожидалось ≈0.2836", got)
	}
	last := rec.rows[len(rec.rows)-1]
	if !last.date.Equal(ofzDay(2023, time.December, 20)) {
		t.Errorf("2023: последняя дата %s, ожидалась 2023-12-20", last.date.Format("2006-01-02"))
	}
}

func TestOfzAuctionImport2026Layout(t *testing.T) {
	rec := &ofzAuctionRecorder{}
	if err := parseOfzAuction(openOfzAuctionFixture(t, "INTERNET_Auction_Results_rus_2026_20261015.xlsx"), rec); err != nil {
		t.Fatalf("parseOfzAuction 2026: %v", err)
	}
	if len(rec.rows) == 0 {
		t.Fatal("2026: ноль строк")
	}
	first := rec.rows[0]
	if !first.date.Equal(ofzDay(2026, time.January, 14)) {
		t.Errorf("2026: первая дата %s, ожидалась 2026-01-14", first.date.Format("2006-01-02"))
	}
	if first.format != "Аукцион" || first.issue != "26253RMFS" || first.paperType != "ОФЗ-ПД" {
		t.Errorf("2026: format/issue/type %q/%q/%q", first.format, first.issue, first.paperType)
	}
	if first.days != 4648 {
		t.Errorf("2026: дней до погашения %d, ожидалось 4648", first.days)
	}
	if got := ofzFloat(t, first.demand, "demand"); got < 44396.3 || got > 44396.4 {
		t.Errorf("2026: demand %v, ожидалось ≈44396.336", got)
	}
	if got := ofzFloat(t, first.placed, "placed"); got < 13460.8 || got > 13460.9 {
		t.Errorf("2026: placed %v, ожидалось ≈13460.899", got)
	}
	if got := ofzFloat(t, first.coverage, "coverage"); got < 0.303 || got > 0.304 {
		t.Errorf("2026: coverage %v, ожидалось ≈0.3032", got)
	}
	last := rec.rows[len(rec.rows)-1]
	if !last.date.Equal(ofzDay(2026, time.October, 14)) {
		t.Errorf("2026: последняя дата %s, ожидалась 2026-10-14", last.date.Format("2006-01-02"))
	}
}

func TestOfzAuctionImportNullableMarkers(t *testing.T) {
	rec := &ofzAuctionRecorder{}
	if err := parseOfzAuction(openOfzAuctionFixture(t, "INTERNET_Auction_Results_rus_2024_20241231.xlsx"), rec); err != nil {
		t.Fatalf("parseOfzAuction 2024: %v", err)
	}
	nulls := 0
	for _, row := range rec.rows {
		if row.cutoffPrice == nil || row.cutoffYield == nil || row.coverage == nil || row.demand == nil {
			nulls++
		}
	}
	if nulls == 0 {
		t.Fatal("2024: не найдено ни одного NULL — маркеры «-»/«****» не разобраны")
	}
}

func TestOfzAuctionImportMissingHeader(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "Лист1"); err != nil {
		t.Fatal(err)
	}
	if err := f.SetCellValue("Лист1", "A1", "не та шапка"); err != nil {
		t.Fatal(err)
	}
	if err := parseOfzAuction(f, &ofzAuctionRecorder{}); err == nil || !strings.Contains(err.Error(), "не найдена шапка") {
		t.Fatalf("ожидалась ошибка «не найдена шапка», получено %v", err)
	}
}

func TestOfzAuctionLinkFixtures(t *testing.T) {
	tagPage, err := os.ReadFile(filepath.Join("testdata", "ofz_tag_1349.html"))
	if err != nil {
		t.Fatalf("чтение ofz_tag_1349.html: %v", err)
	}
	docHref, err := util.FindLink(tagPage, ofzAuctionDocSelector)
	if err != nil {
		t.Fatalf("FindLink тега: %v", err)
	}
	if !strings.Contains(docHref, "315131") {
		t.Errorf("ссылка документа %q, ожидался id_4=315131", docHref)
	}
	docPage, err := os.ReadFile(filepath.Join("testdata", "ofz_auction_doc_2026.html"))
	if err != nil {
		t.Fatalf("чтение ofz_auction_doc_2026.html: %v", err)
	}
	fileHref, err := util.FindLink(docPage, ofzAuctionFileSelector)
	if err != nil {
		t.Fatalf("FindLink файла: %v", err)
	}
	if !strings.Contains(fileHref, "INTERNET_Auction_Results_rus_2026_20261015.xlsx") {
		t.Errorf("ссылка файла %q, ожидался INTERNET_Auction_Results_rus_2026_20261015.xlsx", fileHref)
	}
}
