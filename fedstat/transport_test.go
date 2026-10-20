package fedstat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

func TestFedstatDownloadSendsExpectedForm(t *testing.T) {
	page := fedstatTestFixture(t, "indicator_37426.html")
	xmlBody := fedstatTestFixture(t, "fedstat_2016.xml")
	var got url.Values
	var gotReferer, gotOrigin string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indicator/37426":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(page)
		case "/indicator/downloadData.do":
			if r.Method != http.MethodPost {
				t.Errorf("method = %s, want POST", r.Method)
			}
			if r.URL.Query().Get("format") != "sdmx" {
				t.Errorf("format = %q, want sdmx", r.URL.Query().Get("format"))
			}
			if err := r.ParseForm(); err != nil {
				t.Errorf("ParseForm: %v", err)
			}
			got = r.PostForm
			gotReferer = r.Header.Get("Referer")
			gotOrigin = r.Header.Get("Origin")
			w.Header().Set("Content-Type", "text/xml")
			_, _ = w.Write(xmlBody)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	fedstatUseTestServer(t, server.URL)

	points, err := fedstatDownloadYear(context.Background(), util.NewDefaultTLSClient(), "2016")
	if err != nil {
		t.Fatalf("fedstatDownloadYear: %v", err)
	}
	if len(points) != 6 {
		t.Fatalf("points = %d, want 6", len(points))
	}
	if got.Get("id") != fedstatIndicatorID {
		t.Errorf("id = %q, want %q", got.Get("id"), fedstatIndicatorID)
	}
	if got.Get("token") != "TESTTOKEN0123456789ABCDEFGHIJKLMNOP" {
		t.Errorf("token = %q", got.Get("token"))
	}
	if got.Get("struts.token.name") != "token" {
		t.Errorf("struts.token.name = %q", got.Get("struts.token.name"))
	}
	if !fedstatContains(got["lineObjectIds"], fedstatRegionsID) || !fedstatContains(got["lineObjectIds"], fedstatProductsID) {
		t.Errorf("lineObjectIds = %v", got["lineObjectIds"])
	}
	if !fedstatContains(got["columnObjectIds"], fedstatYearsID) || !fedstatContains(got["columnObjectIds"], fedstatPeriodsID) {
		t.Errorf("columnObjectIds = %v", got["columnObjectIds"])
	}
	if !fedstatContains(got["filterObjectIds"], fedstatUnitID) {
		t.Errorf("filterObjectIds = %v", got["filterObjectIds"])
	}
	if !fedstatContains(got["selectedFilterIds"], fedstatRegionsID+"_"+fedstatRegionRF) ||
		!fedstatContains(got["selectedFilterIds"], fedstatUnitID+"_"+fedstatUnitRub) ||
		!fedstatContains(got["selectedFilterIds"], fedstatYearsID+"_2016") ||
		!fedstatContains(got["selectedFilterIds"], fedstatProductsID+"_1709756") {
		t.Errorf("selectedFilterIds missing required values: %v", got["selectedFilterIds"])
	}
	wantSelected := 1 + 1 + 1 + 1 + len(fedstatProducts)
	if len(got["selectedFilterIds"]) != wantSelected {
		t.Errorf("selectedFilterIds count = %d, want %d", len(got["selectedFilterIds"]), wantSelected)
	}
	var gotProducts []string
	for _, value := range got["selectedFilterIds"] {
		if strings.HasPrefix(value, fedstatProductsID+"_") {
			gotProducts = append(gotProducts, strings.TrimPrefix(value, fedstatProductsID+"_"))
		}
	}
	if len(gotProducts) != len(fedstatProductOrder) {
		t.Errorf("product filter order length = %d, want %d", len(gotProducts), len(fedstatProductOrder))
	} else {
		for i := range fedstatProductOrder {
			if gotProducts[i] != fedstatProductOrder[i] {
				t.Errorf("product filter order[%d] = %q, want %q", i, gotProducts[i], fedstatProductOrder[i])
				break
			}
		}
	}
	if want := server.URL + "/indicator/" + fedstatIndicatorID; gotReferer != want {
		t.Errorf("Referer = %q, want %q", gotReferer, want)
	}
	if gotOrigin != server.URL {
		t.Errorf("Origin = %q, want %q", gotOrigin, server.URL)
	}
}

func TestFedstatPostRetriesOn503(t *testing.T) {
	page := fedstatTestFixture(t, "indicator_37426.html")
	xmlBody := fedstatTestFixture(t, "fedstat_2016.xml")
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indicator/37426":
			_, _ = w.Write(page)
		case "/indicator/downloadData.do":
			if posts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("busy"))
				return
			}
			w.Header().Set("Content-Type", "text/xml")
			_, _ = w.Write(xmlBody)
		}
	}))
	defer server.Close()
	fedstatUseTestServer(t, server.URL)

	if _, err := fedstatDownloadYear(context.Background(), util.NewDefaultTLSClient(), "2016"); err != nil {
		t.Fatalf("fedstatDownload after 503: %v", err)
	}
	if posts.Load() != 2 {
		t.Errorf("POST attempts = %d, want 2", posts.Load())
	}
}

func TestFedstatDownloadRejectsHTML(t *testing.T) {
	page := fedstatTestFixture(t, "indicator_37426.html")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/indicator/37426" {
			_, _ = w.Write(page)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<html><body>no data</body></html>"))
	}))
	defer server.Close()
	fedstatUseTestServer(t, server.URL)

	_, err := fedstatDownloadYear(context.Background(), util.NewDefaultTLSClient(), "2016")
	if err == nil || !strings.Contains(err.Error(), "HTML") {
		t.Fatalf("error = %v, want HTML", err)
	}
}

func TestFedstatTokenMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("<html><body>no token</body></html>"))
	}))
	defer server.Close()
	fedstatUseTestServer(t, server.URL)

	_, err := fedstatDownloadYear(context.Background(), util.NewDefaultTLSClient(), "2016")
	if err == nil || !strings.Contains(err.Error(), "CSRF-токен") {
		t.Fatalf("error = %v, want CSRF-токен", err)
	}
}

func fedstatUseTestServer(t *testing.T, baseURL string) {
	t.Helper()
	oldBase, oldDelays := fedstatBaseURL, fedstatRetryDelays
	fedstatBaseURL = baseURL
	fedstatRetryDelays = []time.Duration{time.Millisecond}
	t.Cleanup(func() {
		fedstatBaseURL = oldBase
		fedstatRetryDelays = oldDelays
	})
}

func fedstatContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
