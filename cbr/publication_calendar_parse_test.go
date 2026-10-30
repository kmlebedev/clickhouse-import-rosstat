package cbr

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const publicationCalendarFixture = "testdata/publication_calendar_2026-10-20.html"

// TestParsePublicationCalendar — разбор реального снимка 2026-10-20: 173 строки
// статистического окна и 22 аналитических блока (445 строк). Числа — из research
// §3.1; они же критерий приёмки issue.
func TestParsePublicationCalendar(t *testing.T) {
	page, err := os.ReadFile(filepath.Join(".", publicationCalendarFixture))
	if err != nil {
		t.Fatalf("чтение фикстуры: %v", err)
	}
	captured := time.Date(2026, time.October, 20, 12, 0, 0, 0, mskZone)
	got, err := parsePublicationCalendar(page, captured)
	if err != nil {
		t.Fatalf("parsePublicationCalendar: %v", err)
	}
	if got.StatsRows != 173 {
		t.Errorf("статистических строк %d, ожидалось 173", got.StatsRows)
	}
	if got.AnalyticBlocks != 22 {
		t.Errorf("аналитических блоков %d, ожидалось 22", got.AnalyticBlocks)
	}
	if got.AnalyticRows != 445 {
		t.Errorf("аналитических строк %d, ожидалось 445", got.AnalyticRows)
	}
	if len(got.Rows) != 618 {
		t.Errorf("всего строк %d, ожидалось 618", len(got.Rows))
	}
	if want := time.Date(2026, time.October, 20, 0, 0, 0, 0, time.UTC); !got.Rows[0].Date.Equal(want) {
		t.Errorf("первая дата %s, ожидалась %s", got.Rows[0].Date, want)
	}

	var statsFirst, statsLast, withTime, withoutTime *publicationCalendarRow
	for i := range got.Rows {
		row := &got.Rows[i]
		if row.Topic == publicationCalendarTopicStatistics {
			if statsFirst == nil {
				statsFirst = row
			}
			statsLast = row
			if row.Time == nil && withoutTime == nil {
				withoutTime = row
			}
			if row.Time != nil && *row.Time == "16:00" && withTime == nil {
				withTime = row
			}
		}
	}
	if statsFirst == nil || statsFirst.Date.Format("2006-01-02") != "2026-10-20" {
		t.Errorf("первая статистическая строка %+v, ожидалась 2026-10-20", statsFirst)
	}
	if statsLast == nil || statsLast.Date.Format("2006-01-02") != "2026-12-30" {
		t.Errorf("последняя статистическая строка %+v, ожидалась 2026-12-30", statsLast)
	}
	if withTime == nil {
		t.Error("не найдена строка со временем 16:00")
	}
	if withoutTime == nil || withoutTime.Time != nil {
		t.Error("не найдена строка без времени (NULL)")
	}
	if withTime != nil && withoutTime != nil && withTime.Planned != 1 {
		t.Errorf("planned у будущей строки %d, ожидалось 1", withTime.Planned)
	}
	if withoutTime != nil && !strings.HasPrefix(withoutTime.SourceURL, "https://www.cbr.ru/") {
		t.Errorf("source_url %q не абсолютный", withoutTime.SourceURL)
	}

	statsWithPeriod := findCalendarRow(got.Rows, publicationCalendarTopicStatistics, "Оценка предстоящих изменений международных резервов")
	if statsWithPeriod == nil {
		t.Fatal("не найдена строка «Оценка предстоящих изменений международных резервов»")
	}
	if statsWithPeriod.Period != "01.10.2026" {
		t.Errorf("period %q, ожидался 01.10.2026", statsWithPeriod.Period)
	}

	analytic := findCalendarRow(got.Rows, publicationCalendarTopicAnalytics, "Оценка трендовой инфляции")
	if analytic == nil {
		t.Fatal("не найдена аналитическая строка «Оценка трендовой инфляции»")
	}
	if analytic.Date.Format("2006-01-02") != "2026-10-14" {
		t.Errorf("дата аналитики %s, ожидалась 2026-10-14", analytic.Date.Format("2006-01-02"))
	}
	if analytic.Period != "2026-10" {
		t.Errorf("period аналитики %q, ожидался 2026-10", analytic.Period)
	}
	if analytic.Planned != 0 {
		t.Errorf("planned у прошедшей аналитики %d, ожидалось 0", analytic.Planned)
	}
	futureAnalytic := findCalendarRow(got.Rows, publicationCalendarTopicAnalytics, "Инфляционные ожидания и потребительские настроения")
	if futureAnalytic == nil {
		t.Fatal("не найдена будущая аналитическая строка «Инфляционные ожидания и потребительские настроения»")
	}
	if futureAnalytic.Date.Format("2006-01-02") != "2026-10-28" || futureAnalytic.Planned != 1 {
		t.Errorf("будущая аналитика %s planned=%d, ожидалась 2026-10-28 planned=1", futureAnalytic.Date.Format("2006-01-02"), futureAnalytic.Planned)
	}
}

func findCalendarRow(rows []publicationCalendarRow, topic, titlePrefix string) *publicationCalendarRow {
	for i := range rows {
		if rows[i].Topic == topic && strings.HasPrefix(rows[i].Title, titlePrefix) {
			return &rows[i]
		}
	}
	return nil
}

func TestParsePublicationCalendarEmptyPage(t *testing.T) {
	if _, err := parsePublicationCalendar([]byte("<html></html>"), time.Now()); err == nil {
		t.Fatal("пустая страница должна давать ошибку")
	}
}

// TestParsePublicationCalendarBlockPeriod — месяц в заголовке блока в именительном
// падеже; год берётся из заголовка.
func TestParsePublicationCalendarBlockPeriod(t *testing.T) {
	year, month, ok := parsePublicationCalendarPeriod("Календарь публикации на октябрь 2026 года")
	if !ok || year != 2026 || month != time.October {
		t.Fatalf("получено %d-%v ok=%t, ожидалось 2026-October", year, month, ok)
	}
	if _, _, ok := parsePublicationCalendarPeriod("Календарь публикации на октябрь"); ok {
		t.Fatal("заголовок без года не должен разбираться")
	}
}

func TestParsePublicationTime(t *testing.T) {
	cases := []struct {
		raw   string
		want  string
		isNil bool
	}{
		{raw: " — ", isNil: true},
		{raw: "", isNil: true},
		{raw: "16:00", want: "16:00"},
		{raw: "до 11:00", want: "11:00"},
	}
	for _, tc := range cases {
		got := parsePublicationTime(tc.raw)
		if tc.isNil {
			if got != nil {
				t.Errorf("%q: получено %q, ожидался NULL", tc.raw, *got)
			}
			continue
		}
		if got == nil || *got != tc.want {
			t.Errorf("%q: получено %v, ожидалось %q", tc.raw, got, tc.want)
		}
	}
}
