package cbr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// Reviewed official announcements; dates are parsed from HTML, never inferred
// from release_year. Adding a source requires an artifact/version review (#598).
var ondkpAnnouncementSources = map[int]string{
	2023: "https://www.cbr.ru/press/event/?id=14277",
	2024: "https://www.cbr.ru/press/event/?id=17174",
	2026: "https://www.cbr.ru/press/event/?id=28050",
	2027: "https://www.cbr.ru/press/event/?id=32941",
}

type ondkpAnnouncementEvidence struct {
	Version           int    `json:"version"`
	ObservedAt        string `json:"observed_at"`
	ArtifactSHA256    string `json:"artifact_sha256"`
	NewsSHA256        string `json:"news_sha256"`
	ReleasePageSHA256 string `json:"release_page_sha256"`
	ReleasePageURL    string `json:"release_page_url"`
}

func ondkpSHA(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// Only the dated announcement header and its article link are evidence. Footer,
// dates of an earlier draft in the body, and navigation links are not fallbacks.
func ondkpAnnouncementDate(page []byte, year int, pageURL string) (*time.Time, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, err
	}
	title := strings.Join(strings.Fields(doc.Find("h1").Text()), " ")
	if doc.Find("h1").Length() != 1 || !strings.Contains(strings.ToLower(title), "денежно-кредитной политики") || strings.Contains(strings.ToLower(title), "проект") {
		return nil, fmt.Errorf("ondkp announcement: wrong title for %d", year)
	}
	linked := false
	doc.Find(".lead-text a[href], .landing-text a[href]").Each(func(_ int, link *goquery.Selection) {
		href, _ := link.Attr("href")
		if href == pageURL || cbrUrl+href == pageURL {
			linked = true
		}
	})
	if !linked {
		return nil, fmt.Errorf("ondkp announcement: no article link to %s", pageURL)
	}
	dates := doc.Find(".news-info-line_date")
	if dates.Length() != 1 {
		return nil, fmt.Errorf("ondkp announcement: expected one date, got %d", dates.Length())
	}
	fields := strings.Fields(dates.Text())
	if len(fields) != 4 || fields[3] != "года" {
		return nil, fmt.Errorf("ondkp announcement: invalid date %q", dates.Text())
	}
	months := map[string]time.Month{"января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6, "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12}
	month, ok := months[fields[1]]
	if !ok {
		return nil, fmt.Errorf("ondkp announcement: invalid month %q", fields[1])
	}
	date, err := time.Parse("2 1 2006", fields[0]+" "+strconv.Itoa(int(month))+" "+fields[2])
	if err != nil {
		return nil, fmt.Errorf("ondkp announcement: invalid date: %w", err)
	}
	return &date, nil
}

// ondkpWithAnnouncement fills only missing announcement metadata. published_at
// remains untouched: matching today's bytes does not prove historical availability.
// body may be reused from the new-release download; nil downloads it for refresh.
func ondkpWithAnnouncement(release ondkpRelease, pageHTML, body []byte,
	getPage, getFile func(string) ([]byte, error), now func() time.Time) (ondkpRelease, bool, error) {
	source, ok := ondkpAnnouncementSources[release.year]
	if !ok {
		return release, false, nil
	}
	if release.announcementDate != nil {
		var evidence ondkpAnnouncementEvidence
		if err := json.Unmarshal([]byte(release.announcementEvidence), &evidence); err != nil {
			return release, false, fmt.Errorf("ondkp %d: invalid stored announcement evidence: %w", release.year, err)
		}
		observed, parseErr := time.Parse(time.RFC3339Nano, evidence.ObservedAt)
		validHashes := true
		for _, hash := range []string{evidence.ArtifactSHA256, evidence.NewsSHA256, evidence.ReleasePageSHA256} {
			decoded, decodeErr := hex.DecodeString(hash)
			if decodeErr != nil || len(decoded) != sha256.Size {
				validHashes = false
			}
		}
		expectedPage := fmt.Sprintf("%s/about_br/publ/ondkp/on_%d_%d/", cbrUrl, release.year, release.year+2)
		if parseErr != nil || !validHashes || evidence.ReleasePageURL != expectedPage || release.announcementDate.After(observed) {
			return release, false, fmt.Errorf("ondkp %d: incomplete stored announcement evidence", release.year)
		}
		if release.announcementURL != source || evidence.Version != 1 || evidence.ArtifactSHA256 != release.sha256 {
			return release, false, fmt.Errorf("ondkp %d: conflicting stored announcement evidence", release.year)
		}
		return release, false, nil
	}
	if release.format != cbrOndkpFormatPDF || strings.Contains(release.url, "on_project_") {
		return release, false, fmt.Errorf("ondkp %d: final announcement cannot be attached to %s", release.year, release.format)
	}
	pageURL := fmt.Sprintf("%s/about_br/publ/ondkp/on_%d_%d/", cbrUrl, release.year, release.year+2)
	selected, err := ondkpYearPagePDF(pageHTML, release.year)
	if err != nil {
		return release, false, err
	}
	if selected != release.url {
		return release, false, fmt.Errorf("ondkp %d: selected artifact URL changed", release.year)
	}
	news, err := getPage(source)
	if err != nil {
		return release, false, fmt.Errorf("ondkp %d: announcement HTTP: %w", release.year, err)
	}
	date, err := ondkpAnnouncementDate(news, release.year, pageURL)
	if err != nil {
		return release, false, err
	}
	if date.After(now().UTC()) {
		return release, false, fmt.Errorf("ondkp %d: announcement date is in the future", release.year)
	}
	if body == nil {
		body, err = getFile(release.url)
		if err != nil {
			return release, false, fmt.Errorf("ondkp %d: artifact HTTP: %w", release.year, err)
		}
	}
	if !bytes.HasPrefix(body, []byte("%PDF-")) || ondkpSHA(body) != release.sha256 || uint64(len(body)) != release.bytes {
		return release, false, fmt.Errorf("ondkp %d: artifact signature, SHA or size differs from registry; no metadata refresh", release.year)
	}
	evidence, err := json.Marshal(ondkpAnnouncementEvidence{Version: 1, ObservedAt: now().UTC().Format(time.RFC3339Nano), ArtifactSHA256: release.sha256, NewsSHA256: ondkpSHA(news), ReleasePageSHA256: ondkpSHA(pageHTML), ReleasePageURL: pageURL})
	if err != nil {
		return release, false, fmt.Errorf("ondkp %d: encode evidence: %w", release.year, err)
	}
	release.announcementDate = date
	release.announcementURL = source
	release.announcementEvidence = string(evidence)
	return release, true, nil
}
