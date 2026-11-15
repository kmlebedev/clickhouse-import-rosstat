package fedstat

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func hoursFixture(t *testing.T, id string) []byte {
	t.Helper()
	b, e := os.ReadFile("testdata/labour_hours/emiss-" + id + ".xls")
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestLabourHoursMinimalFixtures(t *testing.T) {
	for _, tc := range []struct {
		id       string
		n, first int
	}{{"59749", 9, 2024}, {"59751", 1, 2024}, {"60463", 3, 2024}} {
		t.Run(tc.id, func(t *testing.T) {
			p, e := parseLabourHours(hoursFixture(t, tc.id), tc.id)
			if e != nil {
				t.Fatal(e)
			}
			if len(p) != tc.n {
				t.Fatalf("points=%d want%d", len(p), tc.n)
			}
			first, last := 2100, 0
			found := false
			for _, v := range p {
				y := v.date.Year()
				first = min(first, y)
				last = max(last, y)
				if tc.id == "60463" && y == 2024 && strings.HasPrefix(v.activity, "101.АГ ") && strings.HasPrefix(v.status, "13 ") {
					found = true
					if v.value != 152767765.5 {
						t.Fatal(v)
					}
				}
			}
			if first != tc.first || last != 2024 {
				t.Fatalf("years %d..%d", first, last)
			}
			if tc.id == "60463" && !found {
				t.Fatal("missing national control")
			}
			d, e := labourHoursDelta(p, p)
			if e != nil || len(d) != 0 {
				t.Fatalf("repeat: %d %v", len(d), e)
			}
		})
	}
}
func TestLabourHoursDelta(t *testing.T) {
	p, e := parseLabourHours(hoursFixture(t, "59751"), "59751")
	if e != nil {
		t.Fatal(e)
	}
	old := append([]labourHoursPoint(nil), p...)
	p[0].value++
	d, e := labourHoursDelta(p, old)
	if e != nil || len(d) != 1 {
		t.Fatalf("revision: %d %v", len(d), e)
	}
	if _, e = labourHoursDelta(p[1:], old); e == nil {
		t.Fatal("accepted removed key")
	}
	conflict := append(append([]labourHoursPoint(nil), p...), old[0])
	if _, e = labourHoursDelta(conflict, nil); e == nil {
		t.Fatal("accepted conflict")
	}
}
func TestLabourHoursRejectMalformed(t *testing.T) {
	if _, e := parseLabourHours([]byte("<html>login</html>"), "59749"); e == nil {
		t.Fatal("accepted HTML")
	}
	for _, kind := range []string{"unit", "period", "header", "text", "negative", "nan", "missing-bin", "sum", "unknown-bin", "renamed-age"} {
		t.Run(kind, func(t *testing.T) {
			rows, e := fedstatReadSheet(hoursFixture(t, "59749"))
			if e != nil {
				t.Fatal(e)
			}
			r, c := -1, -1
			for i := 3; i < len(rows) && r < 0; i++ {
				for j := 6; j < len(rows[i]); j++ {
					if rows[i][j].numeric {
						r, c = i, j
						break
					}
				}
			}
			if r < 0 {
				t.Fatal("no data")
			}
			switch kind {
			case "unknown-bin":
				rows[r][4].text = "999 Unknown"
			case "renamed-age":
				rows[r][0].text = "138 15-64 лет"
			case "unit":
				rows[r][2].text = "356 час"
			case "period":
				rows[r][3].text = "квартал"
			case "header":
				rows[2][0].text = "unexpected"
			case "text":
				rows[r][c] = fedstatCell{text: ".."}
			case "negative":
				rows[r][c].number = -1
			case "nan":
				rows[r][c].number = math.NaN()
			case "missing-bin":
				rows[r][c] = fedstatCell{}
			case "sum":
				rows[r][c].number += 5
			}
			if _, e = parseLabourHoursRows(rows, "59749"); e == nil {
				t.Fatal("accepted malformed", kind)
			}
		})
	}
	// An actual numeric zero is retained; empty is not fabricated into zero.
	p := []labourHoursPoint{{series: "59751", value: 0}}
	if e := validateLabourHours(p, "59751"); e != nil {
		t.Fatal(e)
	}
}
func TestLabourHoursHTTP(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			fixture := hoursFixture(t, "59751")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					http.SetCookie(w, &http.Cookie{Name: "session", Value: "test"})
					_, _ = w.Write([]byte(`<input name="token" value="test-token">`))
					return
				}
				if r.URL.Path != "/indicator/59751/download" {
					t.Error(r.URL.Path)
				}
				if e := r.ParseForm(); e != nil {
					t.Error(e)
				}
				if r.Form.Get("token") != "test-token" || r.Form.Get("id") != "59751" || r.Form.Get("format") != "excel" {
					t.Error("bad form")
				}
				if _, e := r.Cookie("session"); e != nil {
					t.Error("missing cookie")
				}
				if bad {
					w.WriteHeader(403)
					return
				}
				_, _ = w.Write(fixture)
			}))
			defer server.Close()
			old := fedstatBaseURL
			fedstatBaseURL = server.URL
			defer func() { fedstatBaseURL = old }()
			b, e := fetchLabourHours(context.Background(), fedstatMacroClient(), "59751")
			if bad {
				if e == nil {
					t.Fatal("accepted 403")
				}
			} else if e != nil || len(b) != len(fixture) {
				t.Fatalf("fetch: %d %v", len(b), e)
			}
		})
	}
}
func TestLabourHoursDDLAndMetadata(t *testing.T) {
	b, e := os.ReadFile("../ARCHITECTURE.md")
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(b), fmt.Sprintf(fedstatLabourHoursDdl, "fedstat_labour_hours")) {
		t.Fatal("DDL drift")
	}
	if len(labourHoursMeta) != 3 || len(labourHoursView.Columns) != 12 {
		t.Fatal("incomplete metadata")
	}
	for _, m := range labourHoursMeta {
		if m.Source != "fedstat_labour_hours" || m.Frequency != "Y" || m.Description == "" || m.Unit == "" || m.Origin == "" {
			t.Fatal(m)
		}
	}
}
