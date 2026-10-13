package moex

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// useMoexServer подменяет moexBaseUrl тестовым сервером (сеть в тестах — только httptest)
// и восстанавливает его по завершении теста.
func useMoexServer(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	prev := moexBaseUrl
	t.Cleanup(func() { moexBaseUrl = prev })
	moexBaseUrl = server.URL
}

// writeIssBlocks отдаёт блоки ISS одним JSON-ответом.
func writeIssBlocks(t *testing.T, w http.ResponseWriter, blocks map[string]issBlock) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(blocks); err != nil {
		t.Errorf("write iss response: %v", err)
	}
}

func TestFetchKeyrateSecids(t *testing.T) {
	fixture := issFixture(t, "keyrate_securities.json")
	var paths []string
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		writeIssBlocks(t, w, fixture)
	})

	got, err := fetchKeyrateSecids(context.Background())
	if err != nil {
		t.Fatalf("fetchKeyrateSecids: %v", err)
	}
	want := []string{"KKV6", "KKZ6", "KKG7"}
	if !slices.Equal(sortedCopy(got), sortedCopy(want)) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if want := []string{"/iss/securities.json?q=KEYRATE&iss.meta=off"}; !slices.Equal(paths, want) {
		t.Fatalf("unexpected requests %v, want %v", paths, want)
	}
}

func TestFetchKeyrateContract(t *testing.T) {
	fixture := issFixture(t, "keyrate_description.json")
	var paths []string
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		writeIssBlocks(t, w, fixture)
	})

	got, err := fetchKeyrateContract(context.Background(), "KKV6")
	if err != nil {
		t.Fatalf("fetchKeyrateContract: %v", err)
	}
	want := keyrateContract{
		secid:      "KKV6",
		seriesName: "KEYRATE-10.26",
		firstTrade: moexDay("2026-09-29"),
		lastTrade:  moexDay("2026-10-23"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if want := []string{"/iss/securities/KKV6.json?iss.meta=off"}; !slices.Equal(paths, want) {
		t.Fatalf("unexpected requests %v, want %v", paths, want)
	}
}

func TestFetchKeyrateContractHTTPError(t *testing.T) {
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})

	if _, err := fetchKeyrateContract(context.Background(), "KKV6"); err == nil {
		t.Fatal("expected error on http 500")
	} else if !strings.Contains(err.Error(), "500") {
		t.Fatalf("error text does not name the status: %v", err)
	}
}

func TestFetchKeyrateHistoryPaginates(t *testing.T) {
	fixture := issFixture(t, "keyrate_history.json")
	columns := fixture["history"].Columns
	rows := fixture["history"].Data
	// Стенд отдаёт страницу короче TOTAL (=3): PAGESIZE у ISS (100) параметром не понижается,
	// поэтому вторая страница достижима только по TOTAL из history.cursor — так пагинация и проверяется.
	cursor := issBlock{
		Columns: fixture["history.cursor"].Columns,
		Data:    [][]any{{0.0, 3.0, float64(moexPageLimit)}},
	}
	var paths []string
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		switch r.URL.Query().Get("start") {
		case "0":
			writeIssBlocks(t, w, map[string]issBlock{"history": {Columns: columns, Data: rows[:2]}, "history.cursor": cursor})
		case "100":
			writeIssBlocks(t, w, map[string]issBlock{"history": {Columns: columns, Data: rows[2:]}, "history.cursor": cursor})
		default:
			t.Errorf("unexpected start %q", r.URL.Query().Get("start"))
			writeIssBlocks(t, w, map[string]issBlock{"history": {Columns: columns}, "history.cursor": cursor})
		}
	})

	from, _ := time.Parse(moexDate, "2026-09-29")
	days, err := fetchKeyrateHistory(context.Background(), "KKV6", from)
	if err != nil {
		t.Fatalf("fetchKeyrateHistory: %v", err)
	}
	want := []keyrateDay{
		{date: moexDay("2026-09-29"), close: 13.94, settle: 13.94, waprice: 13.94, volume: 28, numtrades: 19, openposition: 28},
		{date: moexDay("2026-09-30"), close: 13.94, settle: 13.95, waprice: 13.96, volume: 36, numtrades: 31, openposition: 68},
		{date: moexDay("2026-10-01"), close: 13.96, settle: 13.96, waprice: 13.95, volume: 33, numtrades: 10, openposition: 116},
	}
	if !reflect.DeepEqual(days, want) {
		t.Fatalf("got %+v, want %+v", days, want)
	}
	if len(paths) != 2 {
		t.Fatalf("got %d requests %v, want 2 (start=0, start=100)", len(paths), paths)
	}
	if !strings.HasPrefix(paths[0], "/iss/history/engines/futures/markets/forts/securities/KKV6.json?") ||
		!strings.Contains(paths[0], "from=2026-09-29") || !strings.Contains(paths[0], "start=0") {
		t.Fatalf("unexpected first request %q", paths[0])
	}
	if !strings.Contains(paths[1], "start=100") {
		t.Fatalf("second page must be requested with start=%d, got %q", moexPageLimit, paths[1])
	}
}

func TestFetchKeyrateHistoryEmpty(t *testing.T) {
	fixture := issFixture(t, "keyrate_history.json")
	columns := fixture["history"].Columns
	// TOTAL=0: дней по серии нет — запрос один, без второй страницы и без зацикливания.
	cursor := issBlock{
		Columns: fixture["history.cursor"].Columns,
		Data:    [][]any{{0.0, 0.0, float64(moexPageLimit)}},
	}
	var paths []string
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		if len(paths) > 3 {
			// Зацикливание обязано валить тест падением, а не висеть до таймаута go test.
			t.Errorf("fetchKeyrateHistory does not stop on empty page: %v", paths)
			http.Error(w, "too many requests", http.StatusInternalServerError)
			return
		}
		writeIssBlocks(t, w, map[string]issBlock{"history": {Columns: columns}, "history.cursor": cursor})
	})

	from, _ := time.Parse(moexDate, "2026-09-29")
	days, err := fetchKeyrateHistory(context.Background(), "KKV6", from)
	if err != nil {
		t.Fatalf("fetchKeyrateHistory: %v", err)
	}
	if len(days) != 0 {
		t.Fatalf("got %+v, want no days", days)
	}
	if len(paths) != 1 {
		t.Fatalf("got %d requests %v, want 1", len(paths), paths)
	}
}

func TestGetBlocksReturnsAllBlocks(t *testing.T) {
	// Страница history и её курсор приходят одним ответом: оба блока обязан отдать один запрос.
	fixture := issFixture(t, "keyrate_history.json")
	requests := 0
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		writeIssBlocks(t, w, fixture)
	})

	blocks, err := getBlocks(context.Background(), "/iss/history/engines/futures/markets/forts/securities/KKV6.json?iss.meta=off")
	if err != nil {
		t.Fatalf("getBlocks: %v", err)
	}
	if requests != 1 {
		t.Fatalf("got %d requests, want 1", requests)
	}
	if !reflect.DeepEqual(blocks["history"], fixture["history"]) ||
		!reflect.DeepEqual(blocks["history.cursor"], fixture["history.cursor"]) {
		t.Fatalf("unexpected blocks %+v", blocks)
	}
}

func TestGetBlockMissingBlock(t *testing.T) {
	fixture := issFixture(t, "keyrate_history.json")
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeIssBlocks(t, w, fixture)
	})

	_, err := getBlock(context.Background(), "/iss/history/fake/KKV6.json?iss.meta=off", "marketdata")
	if err == nil {
		t.Fatal("expected error for missing block")
	}
	if want := `no block "marketdata" in response`; !strings.Contains(err.Error(), want) {
		t.Fatalf("error text changed: %v", err)
	}
}
