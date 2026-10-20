package moex

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const futuresHistoryColumns = `["TRADEDATE","SECID","CLOSE","SETTLEPRICE","VOLUME","OPENPOSITION","SHORTNAME","ASSETCODE"]`

func TestFetchFuturesHistoryPaginates(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("start") == "0" {
			rows := make([]string, 0, moexPageLimit)
			for i := 0; i < moexPageLimit; i++ {
				rows = append(rows, `["2009-10-05","BRV9_2009",66.45,66.45,21486,24756,"BR-10.09","BR"]`)
			}
			_, _ = fmt.Fprintf(w, `{"history":{"columns":%s,"data":[%s]},`+
				`"history.cursor":{"columns":["INDEX","TOTAL","PAGESIZE"],"data":[[0,101,100]]}}`,
				futuresHistoryColumns, strings.Join(rows, ","))
			return
		}
		_, _ = fmt.Fprintf(w, `{"history":{"columns":%s,"data":[]},`+
			`"history.cursor":{"columns":["INDEX","TOTAL","PAGESIZE"],"data":[[0,101,100]]}}`, futuresHistoryColumns)
	}))
	defer server.Close()

	defer func(prev string) { moexBaseUrl = prev }(moexBaseUrl)
	moexBaseUrl = server.URL

	bars, err := fetchFuturesHistory(context.Background(), "BRV9_2009", "BR", mustDay(t, "2009-10-01"))
	if err != nil {
		t.Fatalf("fetchFuturesHistory: %v", err)
	}
	if len(bars) != moexPageLimit {
		t.Fatalf("got %d bars, want %d", len(bars), moexPageLimit)
	}
	if len(paths) != 2 || !strings.Contains(paths[0], "start=0") || !strings.Contains(paths[1], "start=100") {
		t.Fatalf("unexpected pagination requests %v", paths)
	}
	if !strings.Contains(paths[0], "from=2009-10-01") {
		t.Fatalf("missing from in %q", paths[0])
	}
	if !strings.Contains(paths[0], "/securities/BRV9_2009.json") {
		t.Fatalf("unexpected path %q", paths[0])
	}
}

func TestFetchFuturesHistoryRejectsBadSecid(t *testing.T) {
	if _, err := fetchFuturesHistory(context.Background(), "BRV9/../x", "BR", mustDay(t, "2009-10-01")); err == nil {
		t.Fatal("expected error")
	}
}

func TestDiscoverFuturesSecids(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, futuresSecuritiesPath):
			_, _ = fmt.Fprint(w, `{"securities":{"columns":["SECID","SHORTNAME","ASSETCODE"],"data":[`+
				`["BRZ6","BR-12.26","BR"],["BRF7","BR-1.27","BR"],["BRZ6BRF7","BR-12.26-1.27",""],["CEH7","COPPER-3.27","COPPER"]]}}`)
		case strings.HasPrefix(r.URL.Path, futuresByDatePath):
			if r.URL.Query().Get("start") != "0" {
				_, _ = fmt.Fprint(w, `{"history":{"columns":["SECID","SHORTNAME","ASSETCODE"],"data":[]},`+
					`"history.cursor":{"columns":["INDEX","TOTAL","PAGESIZE"],"data":[[0,2,100]]}}`)
				return
			}
			_, _ = fmt.Fprint(w, `{"history":{"columns":["SECID","SHORTNAME","ASSETCODE"],"data":[`+
				`["BRU6","BR-9.26","BR"],["BRU6BRV6","BR-9.26-10.26",""]]},`+
				`"history.cursor":{"columns":["INDEX","TOTAL","PAGESIZE"],"data":[[0,2,100]]}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	defer func(prev string) { moexBaseUrl = prev }(moexBaseUrl)
	moexBaseUrl = server.URL

	secids, err := discoverFuturesSecids(context.Background(), "BR", mustDay(t, "2026-09-01"))
	if err != nil {
		t.Fatalf("discoverFuturesSecids: %v", err)
	}
	want := map[string]bool{"BRZ6": true, "BRF7": true, "BRU6": true}
	if len(secids) != len(want) {
		t.Fatalf("got %v, want %v", secids, want)
	}
	for _, secid := range secids {
		if !want[secid] {
			t.Errorf("unexpected secid %q in %v", secid, secids)
		}
	}
}

func TestFetchFuturesSecidsOnDateStopsOnDuplicatePage(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"history":{"columns":["SECID","SHORTNAME","ASSETCODE"],"data":[["BRU6","BR-9.26","BR"]]},`+
			`"history.cursor":{"columns":["INDEX","TOTAL","PAGESIZE"],"data":[[0,100,100]]}}`)
	}))
	defer server.Close()

	defer func(prev string) { moexBaseUrl = prev }(moexBaseUrl)
	moexBaseUrl = server.URL

	secids, err := fetchFuturesSecidsOnDate(context.Background(), mustDay(t, "2026-10-15"), "BR")
	if err != nil {
		t.Fatalf("fetchFuturesSecidsOnDate: %v", err)
	}
	if len(secids) != 1 || secids[0] != "BRU6" {
		t.Fatalf("got %v, want [BRU6]", secids)
	}
	if requests > 3 {
		t.Fatalf("pagination did not stop on duplicate page: %d requests", requests)
	}
}

func TestFetchFuturesSecidsOnDateEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"history":{"columns":["SECID","SHORTNAME","ASSETCODE"],"data":[]},`+
			`"history.cursor":{"columns":["INDEX","TOTAL","PAGESIZE"],"data":[[0,0,100]]}}`)
	}))
	defer server.Close()

	defer func(prev string) { moexBaseUrl = prev }(moexBaseUrl)
	moexBaseUrl = server.URL

	secids, err := fetchFuturesSecidsOnDate(context.Background(), mustDay(t, "2026-10-15"), "BR")
	if err != nil {
		t.Fatalf("fetchFuturesSecidsOnDate: %v", err)
	}
	if len(secids) != 0 {
		t.Fatalf("got %v, want empty", secids)
	}
}
