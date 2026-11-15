package cbr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

func TestOndkpAnnouncementDates(t *testing.T) {
	for year, want := range map[int]string{2023: "2022-11-02", 2024: "2023-11-02", 2026: "2025-10-27", 2027: "2026-10-28"} {
		date, err := ondkpAnnouncementDate(cbrOndkpFixture(t, fmt.Sprintf("testdata/ondkp_announcement_%d.html", year)), year, fmt.Sprintf("%s/about_br/publ/ondkp/on_%d_%d/", cbrUrl, year, year+2))
		if err != nil || ondkpDateString(date) != want {
			t.Fatalf("%d: %v %v, want%s", year, date, err, want)
		}
	}
	good := string(cbrOndkpFixture(t, "testdata/ondkp_announcement_2023.html"))
	for name, page := range map[string]string{
		"project title":   strings.Replace(good, "Основные направления", "Проект: Основные направления", 1),
		"unrelated title": strings.ReplaceAll(good, "денежно-кредитной политики", "банковской статистики"),
		"wrong year":      strings.ReplaceAll(good, "2023", "2024"),
		"wrong link":      strings.ReplaceAll(good, "on_2023_2025", "on_2022_2024"),
		"footer only":     strings.ReplaceAll(good, "news-info-line_date", "page-info_last-update"),
		"impossible day":  strings.ReplaceAll(good, "2 ноября", "32 ноября"),
		"invalid month":   strings.ReplaceAll(good, "ноября", "ноябри"),
		"duplicate date":  good + `<div class="news-info-line_date">3 ноября 2022 года</div>`,
		"navigation only": strings.ReplaceAll(good, "lead-text", "navigation"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ondkpAnnouncementDate([]byte(page), 2023, cbrUrl+"/about_br/publ/ondkp/on_2023_2025/"); err == nil {
				t.Fatal("accepted invalid evidence")
			}
		})
	}
	// A different date in body/footer must not override the actual news header.
	date, err := ondkpAnnouncementDate([]byte(good+`<footer>30 ноября 2022 года</footer>`), 2023, cbrUrl+"/about_br/publ/ondkp/on_2023_2025/")
	if err != nil || ondkpDateString(date) != "2022-11-02" {
		t.Fatal(date, err)
	}
}

func ondkpAnnouncementCase(t *testing.T) (ondkpRelease, []byte, []byte, []byte) {
	t.Helper()
	body := []byte("%PDF-1.4\nreviewed fixture bytes")
	release := ondkpRelease{year: 2023, url: cbrUrl + "/Content/Document/File/139691/on_2023(2024-2025).pdf", format: cbrOndkpFormatPDF, sha256: ondkpSHA(body), bytes: uint64(len(body)), forecastRows: 9}
	page := []byte(`<a href="/Content/Document/File/139691/on_2023(2024-2025).pdf">PDF</a><div class="page-info_last-update">22.08.2025</div>`)
	return release, page, body, cbrOndkpFixture(t, "testdata/ondkp_announcement_2023.html")
}

func TestOndkpAnnouncementRefresh(t *testing.T) {
	original, page, body, news := ondkpAnnouncementCase(t)
	legacy := time.Date(2022, 11, 30, 0, 0, 0, 0, time.UTC)
	original.publishedAt = &legacy
	now := func() time.Time { return time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC) }
	calls := 0
	getNews := func(string) ([]byte, error) { calls++; return news, nil }
	getFile := func(string) ([]byte, error) { calls++; return body, nil }
	updated, changed, err := ondkpWithAnnouncement(original, page, nil, getNews, getFile, now)
	if err != nil || !changed || calls != 2 {
		t.Fatal(changed, calls, err)
	}
	if ondkpDateString(updated.announcementDate) != "2022-11-02" {
		t.Fatal(updated.announcementDate)
	}
	var proof ondkpAnnouncementEvidence
	if err = json.Unmarshal([]byte(updated.announcementEvidence), &proof); err != nil {
		t.Fatal(err)
	}
	if proof.ArtifactSHA256 != original.sha256 || proof.NewsSHA256 != ondkpSHA(news) || proof.ReleasePageSHA256 != ondkpSHA(page) || proof.ObservedAt != now().Format(time.RFC3339Nano) {
		t.Fatalf("bad evidence: %+v", proof)
	}
	restored := updated
	restored.announcementDate = nil
	restored.announcementURL = ""
	restored.announcementEvidence = ""
	if !ondkpSameStored(original, restored) {
		t.Fatal("old registry fields changed")
	}
	repeated, changed, err := ondkpWithAnnouncement(updated, page, nil, getNews, getFile, now)
	if err != nil || changed || calls != 2 || !ondkpSameStored(updated, repeated) {
		t.Fatal("not a no-op", changed, calls, err)
	}
	// Missing source is distinct from failed source and must not invent a date.
	unknown := original
	unknown.year = 2022
	if _, changed, err = ondkpWithAnnouncement(unknown, page, nil, getNews, getFile, now); err != nil || changed || calls != 2 {
		t.Fatal(changed, calls, err)
	}
}

func TestOndkpAnnouncementRejectsUnverified(t *testing.T) {
	original, page, body, news := ondkpAnnouncementCase(t)
	now := func() time.Time { return time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC) }
	for _, name := range []string{"sha", "size", "signature", "project", "url", "news HTTP", "pdf HTTP", "future", "stored conflict"} {
		t.Run(name, func(t *testing.T) {
			release := original
			artifact := append([]byte(nil), body...)
			html := news
			sourceErr, pdfErr := error(nil), error(nil)
			switch name {
			case "sha":
				artifact = append(artifact, 'x')
			case "size":
				release.bytes++
			case "signature":
				artifact = []byte("not a PDF")
				release.sha256 = ondkpSHA(artifact)
				release.bytes = uint64(len(artifact))
			case "project":
				release.format = cbrOndkpFormatPDFProject
			case "url":
				release.url += "?changed"
			case "news HTTP":
				sourceErr = errors.New("503")
			case "pdf HTTP":
				pdfErr = errors.New("503")
			case "future":
				html = []byte(strings.ReplaceAll(string(news), "2022  года", "2028 года"))
			case "stored conflict":
				release.announcementDate = &time.Time{}
				release.announcementEvidence = "{}"
			}
			got, changed, err := ondkpWithAnnouncement(release, page, nil, func(string) ([]byte, error) { return html, sourceErr }, func(string) ([]byte, error) { return artifact, pdfErr }, now)
			if err == nil || changed || !ondkpSameStored(got, release) {
				t.Fatalf("accepted %s: changed%v err%v", name, changed, err)
			}
		})
	}
}

// Full importer fake exercises the same-URL gate, batch replacement, count and
// metadata verification, then repeat. Forecast insertion is rejected explicitly.
type ondkpTestConn struct {
	driver.Conn
	registry                    ondkpRelease
	writes                      int
	appendErr, sendErr, scanErr error
}

func (c *ondkpTestConn) Exec(context.Context, string, ...any) error { return nil }
func (c *ondkpTestConn) Query(_ context.Context, q string, _ ...any) (driver.Rows, error) {
	if q == ondkpForecastCountsQuery {
		return &ondkpTestRows{values: [][]any{{uint16(c.registry.year), uint64(c.registry.forecastRows)}}}, nil
	}
	if strings.Contains(q, "FROM cbr_ondkp FINAL") {
		r := c.registry
		return &ondkpTestRows{scanErr: c.scanErr, values: [][]any{{uint16(r.year), ondkpDateString(r.publishedAt), r.url, r.format, r.sha256, r.bytes, uint16(r.forecastRows), ondkpDateString(r.announcementDate), r.announcementURL, r.announcementEvidence}}}, nil
	}
	return nil, fmt.Errorf("unexpected query: %s", q)
}
func (c *ondkpTestConn) QueryRow(context.Context, string, ...any) driver.Row {
	return &pskFakeRow{count: 2}
}
func (c *ondkpTestConn) PrepareBatch(_ context.Context, q string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if q == cbrOndkpForecastInsert {
		return nil, errors.New("metadata refresh must not insert forecasts")
	}
	return &ondkpTestBatch{conn: c, registry: q == cbrOndkpInsert}, nil
}

type ondkpTestBatch struct {
	driver.Batch
	conn     *ondkpTestConn
	registry bool
	row      []any
	aborted  bool
}

func (b *ondkpTestBatch) Append(v ...any) error {
	if b.registry {
		if b.conn.appendErr != nil {
			return b.conn.appendErr
		}
		b.row = v
	}
	return nil
}
func (b *ondkpTestBatch) Abort() error { b.aborted = true; return nil }
func (b *ondkpTestBatch) Send() error {
	if !b.registry {
		return nil
	}
	if b.conn.sendErr != nil {
		return b.conn.sendErr
	}
	v := b.row
	b.conn.registry = ondkpRelease{year: int(v[0].(uint16)), publishedAt: v[1].(*time.Time), url: v[2].(string), format: v[3].(string), sha256: v[4].(string), bytes: v[5].(uint64), forecastRows: int(v[6].(uint16)), announcementDate: v[7].(*time.Time), announcementURL: v[8].(string), announcementEvidence: v[9].(string)}
	b.conn.writes++
	return nil
}

type ondkpTestRows struct {
	driver.Rows
	values  [][]any
	pos     int
	scanErr error
}

func (r *ondkpTestRows) Next() bool {
	if r.pos == len(r.values) {
		return false
	}
	r.pos++
	return true
}
func (r *ondkpTestRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(dest) != len(r.values[r.pos-1]) {
		return errors.New("wrong scan arity")
	}
	for i, v := range r.values[r.pos-1] {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(v))
	}
	return nil
}
func (r *ondkpTestRows) Close() error { return nil }
func (r *ondkpTestRows) Err() error   { return nil }

type ondkpTestTransport func(*http.Request) (*http.Response, error)

func (f ondkpTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOndkpImportMetadataOnly(t *testing.T) {
	original, page, body, news := ondkpAnnouncementCase(t)
	oldClient := util.HttpClient
	t.Cleanup(func() { util.HttpClient = oldClient })
	index := []byte(`<div class="document-regular"><a href="/about_br/publ/ondkp/on_2023_2025/"><span class="document-regular_name_visible">Основные направления на 2023 год</span></a></div>`)
	downloads := 0
	util.HttpClient = &http.Client{Transport: ondkpTestTransport(func(r *http.Request) (*http.Response, error) {
		var content []byte
		switch r.URL.String() {
		case cbrOndkpIndexUrl:
			content = index
		case cbrUrl + "/about_br/publ/ondkp/on_2023_2025/":
			content = page
		case original.url:
			content = body
			downloads++
		case ondkpAnnouncementSources[2023]:
			content = news
			downloads++
		default:
			return nil, fmt.Errorf("unexpected HTTP: %s", r.URL)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(content))), Header: make(http.Header), Request: r}, nil
	})}
	c := &ondkpTestConn{registry: original}
	n, err := cbrOndkpImporter.Import(context.Background(), c)
	if err != nil || n != 1 || c.writes != 1 || downloads != 2 {
		t.Fatal(n, c.writes, downloads, err)
	}
	n, err = cbrOndkpImporter.Import(context.Background(), c)
	if err != nil || n != 0 || c.writes != 1 || downloads != 2 {
		t.Fatal("repeat", n, c.writes, downloads, err)
	}
	t.Run("changed artifact URL preserves registry", func(t *testing.T) {
		prior := original
		date := time.Date(2022, 11, 30, 0, 0, 0, 0, time.UTC)
		prior.publishedAt = &date
		c := &ondkpTestConn{registry: prior}
		savedPage := page
		page = []byte(strings.ReplaceAll(string(page), "139691", "999999"))
		defer func() { page = savedPage }()
		if _, err := cbrOndkpImporter.Import(context.Background(), c); err == nil || c.writes != 0 || !ondkpSameStored(prior, c.registry) {
			t.Fatal("changed URL mutated registry", err, c.writes)
		}
	})
	for _, stage := range []string{"append", "send", "scan"} {
		t.Run(stage, func(t *testing.T) {
			c := &ondkpTestConn{registry: original}
			switch stage {
			case "append":
				c.appendErr = errors.New(stage)
			case "send":
				c.sendErr = errors.New(stage)
			case "scan":
				c.scanErr = errors.New(stage)
			}
			if _, err := cbrOndkpImporter.Import(context.Background(), c); err == nil || c.writes != 0 {
				t.Fatal("ignored error", err, c.writes)
			}
		})
	}
}
