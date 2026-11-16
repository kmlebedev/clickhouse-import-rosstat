package cbr

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

func readPublicationFixture(t *testing.T, name string) []byte {
	t.Helper()
	page, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", name, err)
	}
	return page
}

// TestParseInflExpArchive — даты из tooltip версий XLSX/PDF; проверены research §3.3.
func TestParseInflExpArchive(t *testing.T) {
	page := readPublicationFixture(t, "infl_exp_archive_2026-10-20.html")
	got, err := parseInflExpArchive(page)
	if err != nil {
		t.Fatalf("parseInflExpArchive: %v", err)
	}
	cases := []struct {
		period publicationPeriod
		want   string
	}{
		{publicationPeriod{Year: 2026, Month: time.September}, "2026-09-29"},
		{publicationPeriod{Year: 2025, Month: time.December}, "2025-12-24"},
		{publicationPeriod{Year: 2013, Month: time.September}, "2013-10-16"},
	}
	for _, tc := range cases {
		publication, ok := got[tc.period]
		if !ok {
			t.Errorf("период %04d-%02d не найден", tc.period.Year, tc.period.Month)
			continue
		}
		if publication.Date.Format("2006-01-02") != tc.want {
			t.Errorf("период %04d-%02d: дата %s, ожидалась %s", tc.period.Year, tc.period.Month, publication.Date.Format("2006-01-02"), tc.want)
		}
		if publication.SourceURL == "" {
			t.Errorf("период %04d-%02d: пустой source_url", tc.period.Year, tc.period.Month)
		}
	}
}

// TestParseCPDArchive — PDF-tooltip даёт дату; HTML-версии без tooltip помечаются
// NeedsPage, включая битые ссылки CPD_2025-4/5 и неполный CPD_2022-6.
func TestParseCPDArchive(t *testing.T) {
	page := readPublicationFixture(t, "cpd_archive_2026-10-20.html")
	got, err := parseCPDArchive(page)
	if err != nil {
		t.Fatalf("parseCPDArchive: %v", err)
	}
	pdf := got[publicationPeriod{Year: 2016, Month: time.January}]
	if pdf.Date.Format("2006-01-02") != "2016-02-10" {
		t.Errorf("INF_2016-01: дата %s, ожидалась 2016-02-10", pdf.Date.Format("2006-01-02"))
	}
	for _, period := range []publicationPeriod{
		{Year: 2022, Month: time.June},
		{Year: 2025, Month: time.April},
		{Year: 2025, Month: time.May},
		{Year: 2026, Month: time.September},
	} {
		publication, ok := got[period]
		if !ok {
			t.Errorf("period %04d-%02d не найден", period.Year, period.Month)
			continue
		}
		if !publication.NeedsPage {
			t.Errorf("%04d-%02d: NeedsPage=false, ожидалась HTML-версия без даты", period.Year, period.Month)
		}
		if !publication.Date.IsZero() {
			t.Errorf("%04d-%02d: дата %s, ожидался нулевой Date до запроса страницы", period.Year, period.Month, publication.Date)
		}
	}
}

// TestParseSurveyArchive — оба формата ссылок: 0926 (2024+) и 05_22 (2022–2023).
func TestParseSurveyArchive(t *testing.T) {
	page := readPublicationFixture(t, "mp_archive_2026-10-20.html")
	got, err := parseSurveyArchive(page)
	if err != nil {
		t.Fatalf("parseSurveyArchive: %v", err)
	}
	if len(got) != 54 {
		t.Errorf("версий %d, ожидалось 54", len(got))
	}
	cases := []struct {
		period publicationPeriod
		url    string
	}{
		{publicationPeriod{Year: 2026, Month: time.September}, "https://www.cbr.ru/analytics/dkp/monitoring/0926/"},
		{publicationPeriod{Year: 2022, Month: time.May}, "https://www.cbr.ru/analytics/dkp/monitoring/05_22/"},
	}
	for _, tc := range cases {
		if got[tc.period] != tc.url {
			t.Errorf("%04d-%02d: URL %q, ожидался %q", tc.period.Year, tc.period.Month, got[tc.period], tc.url)
		}
	}
}

// TestParsePublicationPageDate — дата на странице версии, оба релиза.
func TestParsePublicationPageDate(t *testing.T) {
	cases := []struct {
		fixture string
		want    string
	}{
		{"cpd_2026-09.html", "2026-10-14"},
		{"mp_0926.html", "2026-09-15"},
	}
	for _, tc := range cases {
		date, ok := parsePublicationPageDate(readPublicationFixture(t, tc.fixture))
		if !ok {
			t.Errorf("%s: дата не найдена", tc.fixture)
			continue
		}
		if date.Format("2006-01-02") != tc.want {
			t.Errorf("%s: дата %s, ожидалась %s", tc.fixture, date.Format("2006-01-02"), tc.want)
		}
	}
}

func TestDateOnlyPublishedAt(t *testing.T) {
	date := time.Date(2026, time.September, 29, 0, 0, 0, 0, time.UTC)
	got := dateOnlyPublishedAt(date)
	want := time.Date(2026, time.September, 29, 23, 59, 59, 0, mskZone)
	if !got.Equal(want) {
		t.Fatalf("получено %s, ожидалось %s", got, want)
	}
}

func TestFallbackPublishedAt(t *testing.T) {
	plusFive := time.FixedZone("UTC+5", 5*60*60)
	cases := []struct {
		period time.Time
		want   time.Time
	}{
		{time.Date(2026, time.September, 14, 0, 0, 0, 0, time.UTC), time.Date(2026, time.October, 31, 23, 59, 59, 0, mskZone)},
		{time.Date(2015, time.December, 31, 0, 0, 0, 0, time.UTC), time.Date(2016, time.January, 31, 23, 59, 59, 0, mskZone)},
		{time.Date(2002, time.January, 1, 0, 0, 0, 0, time.UTC), time.Date(2002, time.February, 28, 23, 59, 59, 0, mskZone)},
		// Регрессия: ClickHouse Date читается в зоне сессии +05:00; .In(MSK)
		// сдвигал 2002-01-01 на 2001-12-31 и fallback становился 31.01.2002.
		{time.Date(2002, time.January, 1, 0, 0, 0, 0, plusFive), time.Date(2002, time.February, 28, 23, 59, 59, 0, mskZone)},
	}
	for _, tc := range cases {
		if got := fallbackPublishedAt(tc.period); !got.Equal(tc.want) {
			t.Errorf("period %s: получено %s, ожидалось %s", tc.period.Format("2006-01-02"), got, tc.want)
		}
	}
}

func TestSeriesPublicationChanged(t *testing.T) {
	published := time.Date(2026, time.September, 29, 23, 59, 59, 0, mskZone)
	base := util.SeriesPublication{Source: "cbr", Series: "ожидаемая инфляция", PublishedAt: published, Quality: "date"}
	cases := []struct {
		name     string
		existing util.SeriesPublication
		desired  util.SeriesPublication
		want     bool
	}{
		{name: "same", existing: base, desired: base, want: false},
		{name: "fallback to date", existing: util.SeriesPublication{Quality: "fallback", PublishedAt: published.AddDate(0, 1, 30)}, desired: base, want: true},
		{name: "date to exact_time", existing: base, desired: util.SeriesPublication{Quality: "exact_time", PublishedAt: published.Add(-12 * time.Hour)}, want: true},
		{name: "no downgrade exact_time to date", existing: util.SeriesPublication{Quality: "exact_time", PublishedAt: published}, desired: base, want: false},
		{name: "no downgrade date to fallback", existing: base, desired: util.SeriesPublication{Quality: "fallback", PublishedAt: published}, want: false},
		{name: "same quality new date", existing: base, desired: util.SeriesPublication{Quality: "date", PublishedAt: published.Add(-time.Hour)}, want: true},
	}
	for _, tc := range cases {
		if got := seriesPublicationChanged(tc.existing, tc.desired); got != tc.want {
			t.Errorf("%s: получено %t, ожидалось %t", tc.name, got, tc.want)
		}
	}
}

func TestCalendarRowChanged(t *testing.T) {
	withTime := "16:00"
	base := publicationCalendarRow{Time: &withTime, Topic: "statistics", Planned: 1}
	if calendarRowChanged(base, base) {
		t.Fatal("одинаковые строки не должны считаться изменёнными")
	}
	changedPlanned := base
	changedPlanned.Planned = 0
	if !calendarRowChanged(base, changedPlanned) {
		t.Fatal("смена planned должна считаться изменением")
	}
	changedTime := base
	other := "11:00"
	changedTime.Time = &other
	if !calendarRowChanged(base, changedTime) {
		t.Fatal("смена времени должна считаться изменением")
	}
	if !calendarRowChanged(base, publicationCalendarRow{Topic: "statistics", Planned: 1}) {
		t.Fatal("появление/исчезновение времени должно считаться изменением")
	}
}

// TestSeriesPublicationExactTimeRank — exact_time присутствует в схеме и ранжируется
// выше date, даже если живые релизы ЦБ времени пока не дают.
func TestSeriesPublicationExactTimeRank(t *testing.T) {
	if publicationQualityRank("exact_time") <= publicationQualityRank("date") {
		t.Fatal("exact_time должен быть точнее date")
	}
	if publicationQualityRank("date") <= publicationQualityRank("fallback") {
		t.Fatal("date должен быть точнее fallback")
	}
}

// TestSpravkaDates — дата оперативной справки по месяцу волны: будущее отбрасывается,
// из дублей берётся поздняя, не-YYYY-MM и строки без ссылки пропускаются.
func TestSpravkaDates(t *testing.T) {
	today := time.Date(2026, time.November, 16, 0, 0, 0, 0, mskZone)
	rows := []spravkaRow{
		{Period: "2026-07", Date: time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC), SourceURL: "july-early"},
		{Period: "2026-07", Date: time.Date(2026, time.July, 28, 0, 0, 0, 0, time.UTC), SourceURL: "july-late"},
		{Period: "2026-11", Date: time.Date(2026, time.November, 16, 0, 0, 0, 0, time.UTC), SourceURL: "today"},
		{Period: "2026-11", Date: time.Date(2026, time.November, 19, 0, 0, 0, 0, time.UTC), SourceURL: "future"},
		{Period: "2026-13", Date: time.Date(2026, time.December, 1, 0, 0, 0, 0, time.UTC), SourceURL: "bad-month"},
		{Period: "2026-09", Date: time.Date(2026, time.September, 22, 0, 0, 0, 0, time.UTC)},
	}
	got := spravkaDates(rows, today)
	if len(got) != 2 {
		t.Fatalf("получено %d месяцев, ожидалось 2: %+v", len(got), got)
	}
	july := got[publicationPeriod{Year: 2026, Month: time.July}]
	if july.Date.Format("2006-01-02") != "2026-07-28" || july.SourceURL != "july-late" {
		t.Errorf("июль: получено %s (%s), ожидалась поздняя дата 2026-07-28", july.Date.Format("2006-01-02"), july.SourceURL)
	}
	november := got[publicationPeriod{Year: 2026, Month: time.November}]
	if november.Date.Format("2006-01-02") != "2026-11-16" || november.SourceURL != "today" {
		t.Errorf("ноябрь: получено %s (%s), ожидалась дата не позже сегодня", november.Date.Format("2006-01-02"), november.SourceURL)
	}
}

func TestBuildSeriesPublicationsPrefersSpravka(t *testing.T) {
	pointDate := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	period := publicationPeriod{Year: 2026, Month: time.July}
	spravka := map[publicationPeriod]archivePublication{
		period: {Date: time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC), SourceURL: "spravka"},
	}
	archives := map[string]map[publicationPeriod]archivePublication{
		cbrInflExpTable: {
			period: {Date: time.Date(2026, time.July, 28, 0, 0, 0, 0, time.UTC), SourceURL: "archive"},
		},
	}
	inflPoint := publicationPoint{Source: publicationSourceCBR, Series: "ожидаемая инфляция", Date: pointDate, Archive: cbrInflExpTable}
	cpdPoint := publicationPoint{Source: publicationSourceCBR, Series: "Все товары и услуги", Date: pointDate, Archive: cbrIndicatorsCpdTable}
	earlySpravka := map[publicationPeriod]archivePublication{
		period: {Date: time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC), SourceURL: "spravka-early"},
	}
	cases := []struct {
		name        string
		point       publicationPoint
		archives    map[string]map[publicationPeriod]archivePublication
		spravka     map[publicationPeriod]archivePublication
		wantDate    string
		wantQuality string
		wantURL     string
	}{
		{name: "справка бьёт tooltip", point: inflPoint, archives: archives, spravka: spravka, wantDate: "2026-07-21", wantQuality: "date", wantURL: "spravka"},
		{name: "справка раньше волны игнорируется", point: inflPoint, archives: archives, spravka: earlySpravka, wantDate: "2026-07-28", wantQuality: "date", wantURL: "archive"},
		{name: "без справки tooltip", point: inflPoint, archives: archives, wantDate: "2026-07-28", wantQuality: "date", wantURL: "archive"},
		{name: "без обоих fallback", point: inflPoint, wantDate: "2026-08-31", wantQuality: "fallback", wantURL: inflExpArchiveURL},
		{name: "cpd справку не берёт", point: cpdPoint, spravka: spravka, wantDate: "2026-08-31", wantQuality: "fallback", wantURL: cpdArchiveURL},
	}
	for _, tc := range cases {
		got := buildSeriesPublications([]publicationPoint{tc.point}, tc.archives, tc.spravka)
		if len(got) != 1 {
			t.Fatalf("%s: получено %d строк, ожидалась 1", tc.name, len(got))
		}
		row := got[0]
		if date := row.PublishedAt.Format("2006-01-02"); date != tc.wantDate {
			t.Errorf("%s: published_at %s, ожидалось %s", tc.name, date, tc.wantDate)
		}
		if row.Quality != tc.wantQuality {
			t.Errorf("%s: quality %s, ожидалось %s", tc.name, row.Quality, tc.wantQuality)
		}
		if row.SourceURL != tc.wantURL {
			t.Errorf("%s: source_url %s, ожидалось %s", tc.name, row.SourceURL, tc.wantURL)
		}
	}
}

// TestSeriesPublicationStale — для месяцев с оперативной справкой ссылка обновляется
// даже при совпавшей дате, иначе строка сохраняет URL полного комментария.
func TestSeriesPublicationStale(t *testing.T) {
	published := time.Date(2026, time.October, 21, 23, 59, 59, 0, mskZone)
	existing := util.SeriesPublication{Quality: "date", PublishedAt: published, SourceURL: "pdf"}
	spravka := util.SeriesPublication{Quality: "date", PublishedAt: published, SourceURL: "calendar"}
	if !seriesPublicationStale(existing, spravka, true) {
		t.Fatal("смена ссылки на справку должна считаться изменением")
	}
	if seriesPublicationStale(existing, spravka, false) {
		t.Fatal("без справки смена ссылки не должна переписывать строку")
	}
	earlier := spravka
	earlier.PublishedAt = published.Add(-24 * time.Hour)
	if !seriesPublicationStale(existing, earlier, false) {
		t.Fatal("смена даты должна считаться изменением")
	}
}
