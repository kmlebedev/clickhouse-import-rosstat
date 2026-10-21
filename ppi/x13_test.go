package ppi

import (
	"context"
	"encoding/csv"
	"os"
	"strings"
	"testing"
)

func TestParseX13Output(t *testing.T) {
	expected := ppiSeries{
		"food": {
			{Date: month(2021, 1, 31), Value: 100},
			{Date: month(2021, 2, 28), Value: 101},
		},
	}
	output := []byte("series,date,sa\nfood,2021-01-31,100.5\nfood,2021-02-28,101.5\n")
	got, err := parseX13Output(output, expected)
	if err != nil {
		t.Fatalf("parseX13Output: %v", err)
	}
	if len(got["food"]) != 2 {
		t.Fatalf("точек %d, want 2", len(got["food"]))
	}
	almostEqual(t, got["food"][0].SA, 100.5, 1e-9, "sa")
	almostEqual(t, got["food"][1].SA, 101.5, 1e-9, "sa")
}

func TestParseX13OutputMissingPoint(t *testing.T) {
	expected := ppiSeries{"food": {
		{Date: month(2021, 1, 31), Value: 100},
		{Date: month(2021, 2, 28), Value: 101},
	}}
	output := []byte("series,date,sa\nfood,2021-01-31,100.5\n")
	if _, err := parseX13Output(output, expected); err == nil {
		t.Fatal("ожидалась ошибка отсутствующей точки")
	}
}

func TestParseX13OutputNA(t *testing.T) {
	expected := ppiSeries{"food": {{Date: month(2021, 1, 31), Value: 100}}}
	output := []byte("series,date,sa\nfood,2021-01-31,NA\n")
	if _, err := parseX13Output(output, expected); err == nil {
		t.Fatal("ожидалась ошибка NA")
	}
}

func TestParseX13OutputUnknownSeries(t *testing.T) {
	expected := ppiSeries{"food": {{Date: month(2021, 1, 31), Value: 100}}}
	output := []byte("series,date,sa\ncore,2021-01-31,100\n")
	if _, err := parseX13Output(output, expected); err == nil {
		t.Fatal("ожидалась ошибка неизвестной серии")
	}
}

func TestParseX13OutputDuplicate(t *testing.T) {
	expected := ppiSeries{"food": {{Date: month(2021, 1, 31), Value: 100}}}
	output := []byte("series,date,sa\nfood,2021-01-31,100\nfood,2021-01-31,101\n")
	if _, err := parseX13Output(output, expected); err == nil {
		t.Fatal("ожидалась ошибка дубля точки")
	}
}

func TestEncodeSeriesCSV(t *testing.T) {
	series := ppiSeries{}
	for _, name := range ppiSAOrder {
		series[name] = []monthlyValue{{Date: month(2021, 1, 31), Value: 100}}
	}
	data, err := encodeSeriesCSV(series)
	if err != nil {
		t.Fatalf("encodeSeriesCSV: %v", err)
	}
	reader := csv.NewReader(strings.NewReader(string(data)))
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("csv: %v", err)
	}
	if len(rows) != 6 {
		t.Fatalf("строк %d, want 6", len(rows))
	}
	if rows[0][0] != "series" || rows[0][1] != "date" || rows[0][2] != "level" {
		t.Fatalf("шапка %v", rows[0])
	}
	for i, name := range ppiSAOrder {
		if rows[i+1][0] != name {
			t.Errorf("строка %d: серия %q, want %q", i+1, rows[i+1][0], name)
		}
		if rows[i+1][1] != "2021-01-31" {
			t.Errorf("строка %d: дата %q", i+1, rows[i+1][1])
		}
	}
}

func TestEmbeddedPins(t *testing.T) {
	if err := checkEmbeddedPins(); err != nil {
		t.Fatal(err)
	}
	if got := sha256Hex(ppiX13Script); got != ppiX13ScriptSHA256 {
		t.Fatalf("SHA-256 скрипта %s, пин %s", got, ppiX13ScriptSHA256)
	}
	script := string(ppiX13Script)
	for _, fragment := range []string{
		`transform.function = "log"`,
		`outlier = ""`,
		`regression.aictest = "td"`,
	} {
		if !strings.Contains(script, fragment) {
			t.Errorf("R-скрипт не содержит %q: спецификация разошлась с spec_version", fragment)
		}
	}
}

func TestRunX13MissingRscript(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if _, err := runX13(context.Background(), []byte("series,date,level\n")); err == nil {
		t.Fatal("ожидалась ошибка отсутствия Rscript")
	} else if !strings.Contains(err.Error(), "Rscript") {
		t.Fatalf("ошибка %q не упоминает Rscript", err)
	}
}

func TestREnvironmentExcludesSecrets(t *testing.T) {
	t.Setenv("PPI_SA_TEST_SECRET", "super-secret")
	t.Setenv("R_LIBS_USER", "/tmp/r-lib")
	joined := strings.Join(rEnvironment(), "\n")
	if strings.Contains(joined, "PPI_SA_TEST_SECRET") || strings.Contains(joined, "super-secret") {
		t.Fatal("окружение R содержит секрет конвейера")
	}
	if !strings.Contains(joined, "R_LIBS_USER=/tmp/r-lib") {
		t.Fatalf("окружение R не содержит R_LIBS_USER: %q", joined)
	}
}

func TestDataSyncWithResearch(t *testing.T) {
	cases := []struct {
		name string
		path string
		data []byte
	}{
		{"mapping", "../docs/research/2026-10-20-ppi-kipc-mapping.csv", ppiMappingCSV},
		{"weights", "../docs/research/2026-10-21-ppi-sa-weights-panel.csv", ppiWeightsCSV},
	}
	for _, tc := range cases {
		docs, err := os.ReadFile(tc.path)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if string(docs) != string(tc.data) {
			t.Errorf("%s: копия в ppi/data расходится с %s — обновите обе и пин", tc.name, tc.path)
		}
	}
}
