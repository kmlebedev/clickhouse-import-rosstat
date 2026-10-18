package edisclosure

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
)

// TestArchiveNameAndExt — имя объекта архива собирается из ключа документа и
// санитизированного периода, а расширение определяется по сигнатуре тела
// (спека §5.5): zip/rar/pdf, всё прочее — .bin.
func TestArchiveNameAndExt(t *testing.T) {
	d := doc{Ticker: "PLZL", Section: "IFRS", Period: "2026, 6 месяцев", FileID: "1943701"}
	if got := archiveName(d, ".zip"); got != "PLZL_IFRS_2026_6_месяцев_1943701.zip" {
		t.Fatalf("archiveName = %q", got)
	}
	if got := bodyExt([]byte("PK\x03\x04")); got != ".zip" {
		t.Fatalf("ext = %q", got)
	}
	if got := bodyExt([]byte("Rar!\x1a\x07")); got != ".rar" {
		t.Fatalf("ext = %q", got)
	}
	if got := bodyExt([]byte("%PDF-1.7")); got != ".pdf" {
		t.Fatalf("ext = %q", got)
	}
	if got := bodyExt([]byte("other")); got != ".bin" {
		t.Fatalf("ext = %q", got)
	}
}

// TestArchiveNameShape — период без разделителей и пустой период: пустая
// строка не должна давать двойное подчёркивание или пустой компонент имени
// (пустой период — «na»), а расширение подставляется как есть.
func TestArchiveNameShape(t *testing.T) {
	cases := []struct {
		name string
		d    doc
		ext  string
		want string
	}{
		{"пустой период", doc{Ticker: "SBER", Section: "RAS", FileID: "42"}, ".pdf", "SBER_RAS_na_42.pdf"},
		{"один год", doc{Ticker: "CHMF", Section: "AGM", Period: "2025", FileID: "7"}, ".rar", "CHMF_AGM_2025_7.rar"},
		{"пустое расширение", doc{Ticker: "PLZL", Section: "IFRS", Period: "2025", FileID: "1"}, "", "PLZL_IFRS_2025_1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := archiveName(tc.d, tc.ext); got != tc.want {
				t.Errorf("archiveName = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestSanitizePeriod — период становится компонентом имени объекта: пробелы,
// запятые и слэши заменяются на '_', серии разделителей схлопываются в один,
// крайние разделители срезаются, пустой результат — «na».
func TestSanitizePeriod(t *testing.T) {
	cases := map[string]string{
		"2026, 6 месяцев": "2026_6_месяцев",
		"2026":            "2026",
		"2026/6":          "2026_6",
		"2026,   6":       "2026_6",
		"2026/6/30":       "2026_6_30",
		" 2026 ":          "2026",
		", , /":           "na",
		"":                "na",
		"   ":             "na",
		"31.12.2020":      "31.12.2020",
		"2026\u00a06":     "2026_6",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			if got := sanitizePeriod(in); got != want {
				t.Errorf("sanitizePeriod(%q) = %q, want %q", in, got, want)
			}
		})
	}
}

// TestArchiveKeyDeterministic — ключ версии детерминирован: один и тот же
// документ, sha256 и имя дают один ключ вида
// «edisclosure_documents/<sha16>_<name>» (спека §5.5), а другое содержимое —
// другой ключ, иначе новая версия вытеснила бы прежнюю.
func TestArchiveKeyDeterministic(t *testing.T) {
	d := doc{Ticker: "PLZL", Section: "IFRS", Period: "2026, 6 месяцев", FileID: "1943701"}
	name := archiveName(d, ".zip")
	sum := sha256.Sum256([]byte("PK\x03\x04data"))
	fingerprint := hex.EncodeToString(sum[:])

	first := archive.Key(edisclosureArchiveSource, name, fingerprint, sum)
	second := archive.Key(edisclosureArchiveSource, name, fingerprint, sum)
	if first != second {
		t.Fatalf("ключ недетерминирован: %q != %q", first, second)
	}
	if want := edisclosureArchiveSource + "/" + fingerprint[:16] + "_" + name; first != want {
		t.Fatalf("ключ = %q, want %q", first, want)
	}

	otherSum := sha256.Sum256([]byte("PK\x03\x04other"))
	other := archive.Key(edisclosureArchiveSource, name, hex.EncodeToString(otherSum[:]), otherSum)
	if first == other {
		t.Fatalf("разное содержимое дало один ключ %q", first)
	}
	if !strings.HasPrefix(other, edisclosureArchiveSource+"/") {
		t.Errorf("ключ %q не начинается с source %q", other, edisclosureArchiveSource)
	}
}
