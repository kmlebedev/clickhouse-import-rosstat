package fedstat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

func TestFedstatPageConfigReadsRealFragment(t *testing.T) {
	page, err := os.ReadFile("testdata/macro/indicator_macro.html")
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	config, err := fedstatPageConfig(page, []string{"30611", "57937"})
	if err != nil {
		t.Fatalf("fedstatPageConfig: %v", err)
	}
	if got := config["30611"]; len(got) != 1 || got[0] != "950473" {
		t.Errorf("30611 = %v, want [950473]", got)
	}
	got := config["57937"]
	if len(got) != 9 {
		t.Fatalf("57937 values = %d, want 9", len(got))
	}
	if got[0] != "1707674" || got[1] != "1743488" || got[2] != "1704142" {
		t.Errorf("57937 order = %v, want 1707674, 1743488, 1704142", got[:3])
	}
}

func TestFedstatPageConfigMissingDim(t *testing.T) {
	page, err := os.ReadFile("testdata/macro/indicator_macro.html")
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if _, err := fedstatPageConfig(page, []string{"999999"}); err == nil || !strings.Contains(err.Error(), "не найдено") {
		t.Fatalf("error = %v, want missing dimension", err)
	}
}

func TestFedstatToken(t *testing.T) {
	page := []byte(`<input type="hidden" name="token" value="TESTTOKEN">`)
	token, err := fedstatToken(page, "31074")
	if err != nil {
		t.Fatalf("fedstatToken: %v", err)
	}
	if token != "TESTTOKEN" {
		t.Errorf("token = %q, want TESTTOKEN", token)
	}
	if _, err := fedstatToken([]byte("<html></html>"), "31074"); err == nil || !strings.Contains(err.Error(), "CSRF-токен") {
		t.Fatalf("error = %v, want CSRF-токен", err)
	}
}

func TestFedstatPostExcelAcceptsBIFF(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "excel" {
			t.Errorf("format = %q, want excel", r.URL.Query().Get("format"))
		}
		w.Header().Set("Content-Type", "application/vnd.ms-excel")
		_, _ = w.Write([]byte(fedstatBIFFMagic))
	}))
	defer server.Close()
	fedstatUseTestServer(t, server.URL)

	body, err := fedstatPostExcel(context.Background(), util.NewDefaultTLSClient(), "31074", []byte("id=31074"))
	if err != nil {
		t.Fatalf("fedstatPostExcel: %v", err)
	}
	if string(body) != fedstatBIFFMagic {
		t.Errorf("body = %q", body)
	}
}

func TestFedstatPostExcelNoDataSentinel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<html>Нет данных для выбранного сочетания признаков</html>"))
	}))
	defer server.Close()
	fedstatUseTestServer(t, server.URL)

	_, err := fedstatPostExcel(context.Background(), util.NewDefaultTLSClient(), "31074", []byte("id=31074"))
	if err == nil || !strings.Contains(err.Error(), "нет данных") {
		t.Fatalf("error = %v, want errFedstatNoData", err)
	}
}

func TestFedstatPostExcelRejectsUnexpectedHTML(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<html>blocked by WAF</html>"))
	}))
	defer server.Close()
	fedstatUseTestServer(t, server.URL)

	_, err := fedstatPostExcel(context.Background(), util.NewDefaultTLSClient(), "31074", []byte("id=31074"))
	if err == nil || !strings.Contains(err.Error(), "BIFF") {
		t.Fatalf("error = %v, want BIFF rejection", err)
	}
}

func TestFedstatPostExcelRetriesOn503(t *testing.T) {
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if posts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("busy"))
			return
		}
		w.Header().Set("Content-Type", "application/vnd.ms-excel")
		_, _ = w.Write([]byte(fedstatBIFFMagic))
	}))
	defer server.Close()
	fedstatUseTestServer(t, server.URL)

	if _, err := fedstatPostExcel(context.Background(), util.NewDefaultTLSClient(), "31074", []byte("id=31074")); err != nil {
		t.Fatalf("fedstatPostExcel after 503: %v", err)
	}
	if posts.Load() != 2 {
		t.Errorf("POST attempts = %d, want 2", posts.Load())
	}
}

func TestFedstatEncodeFieldsKeepsOrder(t *testing.T) {
	got := string(fedstatEncodeFields([]fedstatField{
		{"id", "31074"},
		{"selectedFilterIds", "0_31074"},
		{"selectedFilterIds", "30611_950473"},
	}))
	want := "id=31074&selectedFilterIds=0_31074&selectedFilterIds=30611_950473"
	if got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}
