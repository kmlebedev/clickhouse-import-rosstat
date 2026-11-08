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
	months := []monthPoint{
		{Date: weeklyDate(2020, 1, 31), Value: 100.5, AsOf: weeklyDate(2020, 1, 27)},
		{Date: weeklyDate(2020, 2, 29), Value: 101.2, AsOf: weeklyDate(2020, 2, 29), Partial: true},
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
	if !rows[0].AsOf.Equal(weeklyDate(2020, 1, 27)) || rows[0].Partial {
		t.Errorf("полный месяц: as_of=%s partial=%v", rows[0].AsOf.Format("2006-01-02"), rows[0].Partial)
	}
	if !rows[1].AsOf.Equal(weeklyDate(2020, 2, 29)) || !rows[1].Partial {
		t.Errorf("nowcast: as_of=%s partial=%v", rows[1].AsOf.Format("2006-01-02"), rows[1].Partial)
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
		"as_of          Date",
		"partial        UInt8",
		"published_at   Nullable(Date)",
	} {
		if !strings.Contains(weeklySADdl, fragment) {
			t.Errorf("DDL не содержит %q", fragment)
		}
	}
}

func TestCatalogAndView(t *testing.T) {
	meta := weeklySASeriesMeta()
	if len(meta) != 5 {
		t.Fatalf("рядов каталога %d, ожидалось 5 (3 месячных + 2 квартальных)", len(meta))
	}
	want := map[string]string{
		"weekly_cpi:sa_index": "M",
		"weekly_cpi:sa_4w":    "M",
		"weekly_cpi:saar":     "M",
		"weekly_cpi:saar_q":   "Q",
		"weekly_cpi:saar_3m":  "M",
	}
	for _, item := range meta {
		if item.Source != "weekly_cpi_sa" {
			t.Errorf("ряд %s: source %q", item.Series, item.Source)
		}
		if freq, ok := want[item.Series]; !ok {
			t.Errorf("неожиданный ряд %q", item.Series)
			continue
		} else if item.Frequency != freq {
			t.Errorf("ряд %s: frequency %q, ожидалось %q", item.Series, item.Frequency, freq)
		}
		if item.Title == "" || item.Unit == "" || item.Origin == "" || item.Description == "" {
			t.Errorf("ряд %s: неполное описание", item.Series)
		}
		delete(want, item.Series)
	}
	for series := range want {
		t.Errorf("в каталоге нет ряда %q", series)
	}

	for _, fragment := range []string{
		"'weekly_cpi:sa_index'",
		"'weekly_cpi:sa_4w'",
		"'weekly_cpi:saar'",
		"as_of",
		"partial",
		"FROM weekly_cpi_sa FINAL",
	} {
		if !strings.Contains(weeklySAView.Select, fragment) {
			t.Errorf("витрина не содержит %q", fragment)
		}
	}
	if weeklySAView.Name != "v_weekly_cpi_sa" {
		t.Errorf("имя витрины %q", weeklySAView.Name)
	}
	if len(weeklySAView.Columns) != 8 {
		t.Errorf("комментариев колонок %d, ожидалось 8", len(weeklySAView.Columns))
	}
}

// TestWeeklyQuarterlyAndRollingViews — две производные квартальные витрины из спеки
// 2026-11-08-cpi-quarterly-metrics §4.3/§4.4. Контракт проверяется фрагментами SQL и
// комментариями: потерянный FINAL, avg без partial, замена pow на ^ (в ClickHouse это XOR) или
// окно не на трёх строках меняют число, а не оформление.
func TestWeeklyQuarterlyAndRollingViews(t *testing.T) {
	if weeklySAQuarterlyView.Name != "v_weekly_cpi_quarterly" {
		t.Fatalf("имя витрины %q, ожидалось v_weekly_cpi_quarterly", weeklySAQuarterlyView.Name)
	}
	for _, fragment := range []string{
		"FROM weekly_cpi_sa FINAL",
		"toStartOfQuarter(date) AS quarter_start",
		"count() AS months",
		"countIf(partial = 0) AS full_months",
		"avg(saar) AS saar_q",
		"max(as_of) AS max_as_of",
		"max(partial) AS max_partial",
		"GROUP BY quarter_start",
	} {
		if !strings.Contains(weeklySAQuarterlyView.Select, fragment) {
			t.Errorf("SQL квартальной витрины не содержит %q", fragment)
		}
	}
	// alias, совпадающий с именем колонки, ClickHouse подставляет внутрь агрегата
	// (max(max(partial)) → ILLEGAL_AGGREGATION), поэтому агрегаты обязаны идти под другими именами.
	for _, forbidden := range []string{"max(as_of) AS as_of", "max(partial) AS partial"} {
		if strings.Contains(weeklySAQuarterlyView.Select, forbidden) {
			t.Errorf("SQL квартальной витрины содержит %q: alias совпадёт с колонкой", forbidden)
		}
	}
	for _, column := range []string{"quarter_start", "quarter", "months", "full_months", "saar_q", "as_of", "partial"} {
		if weeklySAQuarterlyView.Columns[column] == "" {
			t.Errorf("нет комментария колонки %q", column)
		}
	}
	for _, want := range []string{"8,02", "nowcast", "2026-10-01 = IV кв. 2026", "не оценка ЦБ"} {
		if !strings.Contains(weeklySAQuarterlyView.Comment, want) {
			t.Errorf("комментарий квартальной витрины не содержит %q", want)
		}
	}

	if weeklySARolling3mView.Name != "v_weekly_cpi_rolling3m" {
		t.Fatalf("имя витрины %q, ожидалось v_weekly_cpi_rolling3m", weeklySARolling3mView.Name)
	}
	for _, fragment := range []string{
		"FROM weekly_cpi_sa FINAL",
		"ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
		"sum(log(1 + sa_4w / 100))",
		"pow(exp(",
		"52.0 / 12.0",
		"NULL",
	} {
		if !strings.Contains(weeklySARolling3mView.Select, fragment) {
			t.Errorf("SQL скользящей витрины не содержит %q", fragment)
		}
	}
	if strings.Contains(weeklySARolling3mView.Select, "^") {
		t.Error("SQL скользящей витрины содержит ^: в ClickHouse это XOR, а не степень")
	}
	for _, column := range []string{"date", "as_of", "partial", "saar_3m"} {
		if weeklySARolling3mView.Columns[column] == "" {
			t.Errorf("нет комментария колонки %q", column)
		}
	}
	for _, want := range []string{"разрыв", "52/12", "тарифная неделя"} {
		if !strings.Contains(weeklySARolling3mView.Comment, want) {
			t.Errorf("комментарий скользящей витрины не содержит %q", want)
		}
	}
}
