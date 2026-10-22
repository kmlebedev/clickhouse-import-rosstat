package weeklysa

import (
	"database/sql"
	"strings"
	"testing"
)

func TestInputFingerprint(t *testing.T) {
	points := []rawPoint{
		{Date: weeklyDate(2009, 4, 6), Value: 100},
		{Date: weeklyDate(2009, 4, 13), Value: 100.1},
	}
	first := inputFingerprint(points)
	if second := inputFingerprint(points); first != second {
		t.Fatalf("отпечаток нестабилен: %s != %s", first, second)
	}

	changedValue := []rawPoint{
		{Date: weeklyDate(2009, 4, 6), Value: 100},
		{Date: weeklyDate(2009, 4, 13), Value: 100.2},
	}
	if inputFingerprint(changedValue) == first {
		t.Fatal("смена значения не изменила отпечаток")
	}

	changedDate := []rawPoint{
		{Date: weeklyDate(2009, 4, 6), Value: 100},
		{Date: weeklyDate(2009, 4, 20), Value: 100.1},
	}
	if inputFingerprint(changedDate) == first {
		t.Fatal("смена даты не изменила отпечаток")
	}

	if inputFingerprintWithScript(points, "other-script") == first {
		t.Fatal("смена пина R-скрипта не изменила отпечаток")
	}
}

func TestShouldSkip(t *testing.T) {
	if !shouldSkip(true, 209, 209) {
		t.Fatal("известная версия с тем же числом точек обязана давать no-op")
	}
	if shouldSkip(true, 209, 210) {
		t.Fatal("расхождение числа точек не должно давать no-op")
	}
	if shouldSkip(false, 209, 209) {
		t.Fatal("неизвестная версия не должна давать no-op")
	}
}

func TestBuildRows(t *testing.T) {
	months := []rawPoint{
		{Date: weeklyDate(2020, 1, 31), Value: 100.5},
		{Date: weeklyDate(2020, 2, 29), Value: 101.2},
	}
	sa := []saPoint{
		{Date: weeklyDate(2020, 1, 31), SA: 100},
		{Date: weeklyDate(2020, 2, 29), SA: 101},
	}
	published := sql.NullTime{Time: weeklyDate(2026, 10, 22), Valid: true}
	rows, err := buildRows(months, sa, published)
	if err != nil {
		t.Fatalf("buildRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("строк %d, ожидалось 2", len(rows))
	}
	for _, row := range rows {
		if row.Series != "weekly_cpi" || row.Method != "x13arima-seats" || row.SpecVersion != "weekly-cpi-sa-2026-v1" {
			t.Errorf("версия строки %s/%s/%s", row.Series, row.Method, row.SpecVersion)
		}
		if !row.PublishedAt.Valid || !row.PublishedAt.Time.Equal(published.Time) {
			t.Errorf("published_at %v", row.PublishedAt)
		}
	}
	almostEqual(t, rows[1].SA4W, 1, 1e-12, "sa_4w")
}

func TestSchemaLiterals(t *testing.T) {
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS weekly_cpi_sa",
		"ReplacingMergeTree(loaded_at)",
		"ORDER BY (date, series, method, spec_version)",
		"sa_index       Float64",
		"sa_4w          Float64",
		"saar           Float64",
		"published_at   Nullable(Date)",
	} {
		if !strings.Contains(weeklySADdl, fragment) {
			t.Errorf("DDL не содержит %q", fragment)
		}
	}
}

func TestCatalogAndView(t *testing.T) {
	meta := weeklySASeriesMeta()
	if len(meta) != 3 {
		t.Fatalf("рядов каталога %d, ожидалось 3", len(meta))
	}
	want := map[string]bool{
		"weekly_cpi:sa_index": false,
		"weekly_cpi:sa_4w":    false,
		"weekly_cpi:saar":     false,
	}
	for _, item := range meta {
		if item.Source != "weekly_cpi_sa" {
			t.Errorf("ряд %s: source %q", item.Series, item.Source)
		}
		if item.Frequency != "M" {
			t.Errorf("ряд %s: frequency %q", item.Series, item.Frequency)
		}
		if item.Title == "" || item.Unit == "" || item.Origin == "" || item.Description == "" {
			t.Errorf("ряд %s: неполное описание", item.Series)
		}
		if _, ok := want[item.Series]; !ok {
			t.Errorf("неожиданный ряд %q", item.Series)
			continue
		}
		want[item.Series] = true
	}
	for series, seen := range want {
		if !seen {
			t.Errorf("в каталоге нет ряда %q", series)
		}
	}

	for _, fragment := range []string{
		"'weekly_cpi:sa_index'",
		"'weekly_cpi:sa_4w'",
		"'weekly_cpi:saar'",
		"FROM weekly_cpi_sa FINAL",
	} {
		if !strings.Contains(weeklySAView.Select, fragment) {
			t.Errorf("витрина не содержит %q", fragment)
		}
	}
	if weeklySAView.Name != "v_weekly_cpi_sa" {
		t.Errorf("имя витрины %q", weeklySAView.Name)
	}
	if len(weeklySAView.Columns) != 6 {
		t.Errorf("комментариев колонок %d, ожидалось 6", len(weeklySAView.Columns))
	}
}
