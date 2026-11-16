package rosstat

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func readIpcPubdatesFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", name, err)
	}
	return body
}

// TestParseIpcPubdates — месячные карточки дают отчётный месяц и дату релиза; недельная
// карточка отсекается титулом, дата карточки сверяется с именем файла.
func TestParseIpcPubdates(t *testing.T) {
	releases, cards, err := parseIpcPubdates(readIpcPubdatesFixture(t, "ipc_pubdates_feed.html"))
	if err != nil {
		t.Fatalf("parseIpcPubdates: %v", err)
	}
	if cards != 4 {
		t.Errorf("карточек %d, ожидалось 4", cards)
	}
	if len(releases) != 3 {
		t.Fatalf("релизов %d, ожидалось 3: %+v", len(releases), releases)
	}
	may := releases[0]
	if got := may.Period.Format("2006-01-02"); got != "2025-05-31" {
		t.Errorf("май: период %s, ожидался конец месяца 2025-05-31", got)
	}
	if got := may.Publication.Format("2006-01-02"); got != "2025-06-11" {
		t.Errorf("май: публикация %s, ожидалась 2025-06-11", got)
	}
	if !strings.HasSuffix(may.URL, "85_11-06-2025.html") {
		t.Errorf("май: ссылка %q", may.URL)
	}
	december := releases[1]
	if got := december.Period.Format("2006-01-02"); got != "2025-12-31" {
		t.Errorf("декабрь: период %s, ожидался 2025-12-31", got)
	}
	if got := december.Publication.Format("2006-01-02"); got != "2026-01-16" {
		t.Errorf("декабрь: публикация %s, ожидалась 2026-01-16", got)
	}
	// Карточка текущего года без года в подписи: дата берётся из имени файла.
	september := releases[2]
	if got := september.Period.Format("2006-01-02"); got != "2026-09-30" {
		t.Errorf("сентябрь: период %s, ожидался 2026-09-30", got)
	}
	if got := september.Publication.Format("2006-01-02"); got != "2026-10-09" {
		t.Errorf("сентябрь: публикация %s, ожидалась 2026-10-09", got)
	}
	if want := rosstatUrl + "/storage/mediabank/157_09-10-2026.pdf"; september.URL != want {
		t.Errorf("сентябрь: ссылка %q, ожидалась абсолютная %q", september.URL, want)
	}
}

func TestParseIpcPubdatesCardDateMismatch(t *testing.T) {
	page := strings.Replace(
		string(readIpcPubdatesFixture(t, "ipc_pubdates_feed.html")),
		"85_11-06-2025.html", "85_10-06-2025.html", 1)
	_, _, err := parseIpcPubdates([]byte(page))
	if err == nil || !strings.Contains(err.Error(), "не совпала") {
		t.Fatalf("ожидалась ошибка расхождения дат, получено %v", err)
	}
}

func TestParseIpcPubdatesWithoutDate(t *testing.T) {
	page := strings.Replace(
		string(readIpcPubdatesFixture(t, "ipc_pubdates_feed.html")),
		`<div class="news-card__data">11 июня 2025</div>`, "", 1)
	_, _, err := parseIpcPubdates([]byte(page))
	if err == nil || !strings.Contains(err.Error(), "без даты") {
		t.Fatalf("ожидалась ошибка отсутствия даты, получено %v", err)
	}
}

func TestParseIpcPubdatesEmptyPage(t *testing.T) {
	releases, cards, err := parseIpcPubdates([]byte(`<html><body><div class="row"></div></body></html>`))
	if err != nil {
		t.Fatalf("пустая страница — не ошибка разбора: %v", err)
	}
	if cards != 0 || len(releases) != 0 {
		t.Fatalf("пустая страница: cards=%d releases=%d", cards, len(releases))
	}
}

func TestIpcPubdatesPeriod(t *testing.T) {
	cases := []struct {
		title string
		want  string
		ok    bool
	}{
		{"Об индексе потребительских цен в мае 2025 года", "2025-05-31", true},
		{"Об индексе потребительских цен в феврале 2024 года", "2024-02-29", true},
		{"Об оценке индекса потребительских цен с 25 ноября по 1 декабря 2025 года", "", false},
		{"Об индексе потребительских цен в мае 2025", "", false},
	}
	for _, tc := range cases {
		got, ok := ipcPubdatesPeriod(tc.title)
		if ok != tc.ok {
			t.Errorf("%q: ok=%t, ожидалось %t", tc.title, ok, tc.ok)
			continue
		}
		if ok && got.Format("2006-01-02") != tc.want {
			t.Errorf("%q: период %s, ожидался %s", tc.title, got.Format("2006-01-02"), tc.want)
		}
	}
}

func TestIpcPubdatesFileDate(t *testing.T) {
	cases := []struct {
		href string
		want string
		ok   bool
	}{
		{"https://rosstat.gov.ru/storage/mediabank/85_11-06-2025.html", "2025-06-11", true},
		{"/storage/mediabank/157_09-10-2026.pdf", "2026-10-09", true},
		{"https://rosstat.gov.ru/storage/mediabank/Nedel_ipc.xlsx", "", false},
		{"https://rosstat.gov.ru/storage/mediabank/85_32-06-2025.html", "", false},
	}
	for _, tc := range cases {
		got, ok := ipcPubdatesFileDate(tc.href)
		if ok != tc.ok {
			t.Errorf("%q: ok=%t, ожидалось %t", tc.href, ok, tc.ok)
			continue
		}
		if ok && got.Format("2006-01-02") != tc.want {
			t.Errorf("%q: дата %s, ожидалась %s", tc.href, got.Format("2006-01-02"), tc.want)
		}
	}
}

// TestIpcPubdatesPublishedAt — date-only публикация получает 23:59:59 MSK, компоненты
// даты не пересчитываются зоной.
func TestIpcPubdatesPublishedAt(t *testing.T) {
	plusFive := time.FixedZone("UTC+5", 5*60*60)
	got := ipcPubdatesPublishedAt(time.Date(2025, time.June, 11, 0, 0, 0, 0, plusFive))
	want := time.Date(2025, time.June, 11, 23, 59, 59, 0, time.FixedZone("MSK", 3*60*60))
	if !got.Equal(want) {
		t.Fatalf("получено %s, ожидалось %s", got, want)
	}
}

// TestIpcPubdatesLatinLookalikeMonth — в подписи Росстата встречается месяц с латинской
// «a» («13 мартa»); двойник нормализуется, а не ломает разбор.
func TestIpcPubdatesLatinLookalikeMonth(t *testing.T) {
	date, withYear, ok := ipcPubdatesCardDate("13 мартa")
	if !ok || withYear {
		t.Fatalf("дата не разобрана: ok=%t withYear=%t", ok, withYear)
	}
	if date.Day() != 13 || date.Month() != 3 {
		t.Fatalf("получено %s, ожидалось 13 марта", date.Format("01-02"))
	}
}

// TestIpcPubdateChangedDateZone — Date из ClickHouse приходит в зоне сессии (+05:00);
// та же календарная дата не должна считаться изменением.
func TestIpcPubdateChangedDateZone(t *testing.T) {
	plusFive := time.FixedZone("UTC+5", 5*60*60)
	existing := ipcPubdate{
		Period:      time.Date(2025, time.May, 31, 0, 0, 0, 0, plusFive),
		Publication: time.Date(2025, time.June, 11, 0, 0, 0, 0, plusFive),
		Title:       "t",
		URL:         "u",
	}
	desired := ipcPubdate{
		Period:      time.Date(2025, time.May, 31, 0, 0, 0, 0, time.UTC),
		Publication: time.Date(2025, time.June, 11, 0, 0, 0, 0, time.UTC),
		Title:       "t",
		URL:         "u",
	}
	if ipcPubdateChanged(existing, desired) {
		t.Fatal("та же календарная дата в другой зоне — не изменение")
	}
	desired.Publication = time.Date(2025, time.June, 10, 0, 0, 0, 0, time.UTC)
	if !ipcPubdateChanged(existing, desired) {
		t.Fatal("другая дата должна считаться изменением")
	}
}

// TestFetchIpcPubdatesWithPages — обход идёт до пустой страницы; повтор месяца между
// страницами отбрасывается, пустая первая страница — ошибка.
func TestFetchIpcPubdatesWithPages(t *testing.T) {
	fixture := readIpcPubdatesFixture(t, "ipc_pubdates_feed.html")
	empty := []byte(`<html><body></body></html>`)
	got, err := fetchIpcPubdatesWith(func(page int) ([]byte, error) {
		switch page {
		case 1:
			return fixture, nil
		case 2:
			return fixture, nil // источник проигнорировал page — дубли не должны размножиться
		default:
			return empty, nil
		}
	})
	if err != nil {
		t.Fatalf("fetchIpcPubdatesWith: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("релизов %d, ожидалось 3 без дублей: %+v", len(got), got)
	}
}

func TestFetchIpcPubdatesEmptyFirstPage(t *testing.T) {
	_, err := fetchIpcPubdatesWith(func(int) ([]byte, error) {
		return []byte(`<html><body></body></html>`), nil
	})
	if err == nil || !strings.Contains(err.Error(), "первой странице") {
		t.Fatalf("ожидалась ошибка пустой первой страницы, получено %v", err)
	}
}

// TestIpcPubdatesSeriesMatchesCatalog — имя ряда в series_publication задано кодом и
// обязано совпадать с каталогом (ipcMesSeriesNames), иначе join витрины молча пуст.
func TestIpcPubdatesSeriesMatchesCatalog(t *testing.T) {
	if ipcPubdatesSeries != ipcMesSeriesNames[1] {
		t.Fatalf("ряд %q не совпадает с каталогом %q", ipcPubdatesSeries, ipcMesSeriesNames[1])
	}
}
