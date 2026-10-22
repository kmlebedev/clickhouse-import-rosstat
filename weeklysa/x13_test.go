package weeklysa

import (
	"context"
	"encoding/csv"
	"strings"
	"testing"
)

func TestEncodeX13Input(t *testing.T) {
	months := []rawPoint{
		{Date: weeklyDate(2020, 1, 31), Value: 100.5},
		{Date: weeklyDate(2020, 2, 29), Value: 101.25},
	}
	data, err := encodeX13Input(months)
	if err != nil {
		t.Fatalf("encodeX13Input: %v", err)
	}
	rows, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil {
		t.Fatalf("csv: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("строк %d, ожидалось 3", len(rows))
	}
	if rows[0][0] != "date" || rows[0][1] != "level" {
		t.Fatalf("шапка %v", rows[0])
	}
	if rows[1][0] != "2020-01-31" || rows[1][1] != "100.5" {
		t.Errorf("строка 1: %v", rows[1])
	}
	if rows[2][0] != "2020-02-29" {
		t.Errorf("строка 2: %v", rows[2])
	}
}

func TestParseX13Output(t *testing.T) {
	expected := []rawPoint{
		{Date: weeklyDate(2020, 1, 31), Value: 100.5},
		{Date: weeklyDate(2020, 2, 29), Value: 101.25},
	}
	output := []byte("date,sa_level\n2020-01-31,100.5\n2020-02-29,101.5\n")
	got, err := parseX13Output(output, expected)
	if err != nil {
		t.Fatalf("parseX13Output: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("точек %d, ожидалось 2", len(got))
	}
	almostEqual(t, got[0].SA, 100.5, 1e-9, "sa 1")
	almostEqual(t, got[1].SA, 101.5, 1e-9, "sa 2")
}

func TestParseX13OutputMissingPoint(t *testing.T) {
	expected := []rawPoint{
		{Date: weeklyDate(2020, 1, 31)},
		{Date: weeklyDate(2020, 2, 29)},
	}
	output := []byte("date,sa_level\n2020-01-31,100.5\n")
	if _, err := parseX13Output(output, expected); err == nil {
		t.Fatal("ожидалась ошибка отсутствующей точки")
	}
}

func TestParseX13OutputDuplicate(t *testing.T) {
	expected := []rawPoint{{Date: weeklyDate(2020, 1, 31)}}
	output := []byte("date,sa_level\n2020-01-31,100\n2020-01-31,101\n")
	if _, err := parseX13Output(output, expected); err == nil {
		t.Fatal("ожидалась ошибка дубля точки")
	}
}

func TestParseX13OutputNA(t *testing.T) {
	expected := []rawPoint{{Date: weeklyDate(2020, 1, 31)}}
	output := []byte("date,sa_level\n2020-01-31,NA\n")
	if _, err := parseX13Output(output, expected); err == nil {
		t.Fatal("ожидалась ошибка NA")
	}
}

func TestParseX13OutputNonPositive(t *testing.T) {
	expected := []rawPoint{{Date: weeklyDate(2020, 1, 31)}}
	output := []byte("date,sa_level\n2020-01-31,0\n")
	if _, err := parseX13Output(output, expected); err == nil {
		t.Fatal("ожидалась ошибка неположительного SA")
	}
}

func TestParseX13OutputShortRow(t *testing.T) {
	expected := []rawPoint{{Date: weeklyDate(2020, 1, 31)}}
	for name, output := range map[string][]byte{
		"short":     []byte("date,sa_level\n2020-01-31\n"),
		"reordered": []byte("sa_level,date\n5\n"),
	} {
		if _, err := parseX13Output(output, expected); err == nil {
			t.Errorf("%s: ожидалась ошибка короткой записи", name)
		}
	}
}

func TestEmbeddedPins(t *testing.T) {
	if err := checkEmbeddedPins(); err != nil {
		t.Fatal(err)
	}
	if got := sha256Hex(weeklyX13Script); got != weeklyX13ScriptSHA256 {
		t.Fatalf("SHA-256 скрипта %s, пин %s", got, weeklyX13ScriptSHA256)
	}
	script := string(weeklyX13Script)
	for _, fragment := range []string{
		`transform.function = "log"`,
		`outlier = ""`,
		`regression.aictest = "td"`,
		`frequency = 12`,
	} {
		if !strings.Contains(script, fragment) {
			t.Errorf("R-скрипт не содержит %q: спецификация разошлась с spec_version", fragment)
		}
	}
}

func TestRunX13MissingRscript(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if _, err := runX13(context.Background(), []byte("date,level\n")); err == nil {
		t.Fatal("ожидалась ошибка отсутствия Rscript")
	} else if !strings.Contains(err.Error(), "Rscript") {
		t.Fatalf("ошибка %q не упоминает Rscript", err)
	}
}
