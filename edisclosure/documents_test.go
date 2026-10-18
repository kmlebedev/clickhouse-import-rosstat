package edisclosure

import (
	"strings"
	"testing"
	"time"
)

// TestPlanFetches — чистый гейт повторного прогона (спека §5.3 п. 4, §5.4):
// документ скачивается, только если строки нет, изменились метаданные карточки,
// архив включён при пустом archive_key (самолечение) или задан RECHECK.
// Остальные документы пропускаются без сети.
func TestPlanFetches(t *testing.T) {
	d := doc{Ticker: "PLZL", Section: "IFRS", FileID: "1943701", DocType: "x",
		Period: "2025", URL: "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}
	key := docKey(d)
	cur := existingDoc{DocType: "x", Period: "2025", URL: d.URL,
		SHA256: "abc", ArchiveKey: "edisclosure_documents/abc_x.zip"}
	cases := []struct {
		name             string
		existing         map[string]existingDoc
		recheck, archive bool
		want             int
	}{
		{"no row", nil, false, false, 1},
		{"same metadata and archive", map[string]existingDoc{key: cur}, false, false, 0},
		{"changed period", map[string]existingDoc{key: func() existingDoc { e := cur; e.Period = "2024"; return e }()}, false, false, 1},
		{"changed publication date", map[string]existingDoc{key: func() existingDoc {
			e := cur
			e.PublicationDate = time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC)
			return e
		}()}, false, false, 1},
		{"changed doc type", map[string]existingDoc{key: func() existingDoc { e := cur; e.DocType = "y"; return e }()}, false, false, 1},
		{"changed url", map[string]existingDoc{key: func() existingDoc {
			e := cur
			e.URL = d.URL + "&extra=1"
			return e
		}()}, false, false, 1},
		{"archive enabled but key empty", map[string]existingDoc{key: func() existingDoc { e := cur; e.ArchiveKey = ""; return e }()}, false, true, 1},
		{"archive disabled and key empty", map[string]existingDoc{key: func() existingDoc { e := cur; e.ArchiveKey = ""; return e }()}, false, false, 0},
		{"recheck", map[string]existingDoc{key: cur}, true, false, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := planFetches([]doc{d}, tc.existing, tc.recheck, tc.archive)
			if len(plan) != tc.want {
				t.Fatalf("planFetches = %d документов, want %d", len(plan), tc.want)
			}
			if tc.want == 1 && plan[0].FileID != d.FileID {
				t.Errorf("в плане документ %s, want %s", plan[0].FileID, d.FileID)
			}
		})
	}
}

// TestPlanFetchesKeepsOrder — план сохраняет порядок манифеста: батч и лог
// должны идти в порядке входного файла, а не карты существующих строк.
func TestPlanFetchesKeepsOrder(t *testing.T) {
	docs := []doc{
		{Ticker: "PLZL", Section: "IFRS", FileID: "1", DocType: "x", Period: "2025"},
		{Ticker: "SBER", Section: "RAS", FileID: "2", DocType: "x", Period: "2025"},
		{Ticker: "CHMF", Section: "AGM", FileID: "3", DocType: "x", Period: "2025"},
	}
	existing := map[string]existingDoc{
		docKey(docs[1]): {DocType: "x", Period: "2025", ArchiveKey: "k"},
	}
	plan := planFetches(docs, existing, false, true)
	if len(plan) != 2 {
		t.Fatalf("planFetches = %d документов, want 2", len(plan))
	}
	if plan[0].FileID != "1" || plan[1].FileID != "3" {
		t.Errorf("порядок плана = [%s %s], want [1 3]", plan[0].FileID, plan[1].FileID)
	}
}

// TestMetadataMatches — сравнение метаданных карточки: doc_type, period, url и
// publication_date. Даты сравниваются как моменты, а zero time равен zero time:
// пустая дата в карточке не должна выглядеть изменением.
func TestMetadataMatches(t *testing.T) {
	pub := time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC)
	d := doc{DocType: "x", Period: "2025",
		URL: "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701", PublicationDate: pub}
	e := existingDoc{DocType: "x", Period: "2025", URL: d.URL,
		PublicationDate: pub.In(time.FixedZone("MSK", 3*60*60))}
	if !d.metadataMatches(e) {
		t.Fatal("один момент в разных зонах признан изменением")
	}
	d.PublicationDate = time.Time{}
	e.PublicationDate = time.Time{}
	if !d.metadataMatches(e) {
		t.Fatal("zero time не равен zero time")
	}
	for name, changed := range map[string]existingDoc{
		"doc_type": {DocType: "y", Period: e.Period, URL: e.URL},
		"period":   {DocType: e.DocType, Period: "2024", URL: e.URL},
		"url":      {DocType: e.DocType, Period: e.Period, URL: "https://e-disclosure.ru/other"},
		"publication_date": {
			DocType: e.DocType, Period: e.Period, URL: e.URL,
			PublicationDate: pub,
		},
	} {
		if d.metadataMatches(changed) {
			t.Errorf("%s: изменение не замечено", name)
		}
	}
}

// TestNeedsWrite — после скачивания строка переписывается только при смене
// версии содержимого или метаданных, при заполнении пустого archive_key и для
// новой строки; неизменившийся sha256 при RECHECK запись не порождает.
func TestNeedsWrite(t *testing.T) {
	d := doc{Ticker: "PLZL", Section: "IFRS", FileID: "1943701", DocType: "x",
		Period: "2025", URL: "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}
	cur := existingDoc{DocType: "x", Period: "2025", URL: d.URL,
		SHA256: "old", ArchiveKey: "k"}
	cases := []struct {
		name          string
		e             existingDoc
		ok            bool
		sha, key      string
		archive, want bool
	}{
		{"new row", existingDoc{}, false, "new", "k", true, true},
		{"same sha", cur, true, "old", "k", true, false},
		{"changed sha", cur, true, "new", "k", true, true},
		{"changed metadata", func() existingDoc { e := cur; e.Period = "2024"; return e }(), true, "old", "k", true, true},
		{"archive key filled", func() existingDoc { e := cur; e.ArchiveKey = ""; return e }(), true, "old", "k", true, true},
		{"archive disabled empty key", func() existingDoc { e := cur; e.ArchiveKey = ""; return e }(), true, "old", "", false, false},
	}
	for _, tc := range cases {
		if got := needsWrite(d, tc.e, tc.sha, tc.key, tc.archive); got != tc.want {
			t.Errorf("%s: needsWrite = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestValidateBody — тело скачанного файла обязано быть непустым и не быть
// HTML: FileLoad.ashx при ошибке отдаёт страницу, а не документ. Zip, rar, pdf
// и прочие бинарные тела допустимы.
func TestValidateBody(t *testing.T) {
	cases := []struct {
		name    string
		body    []byte
		wantErr bool
	}{
		{"nil", nil, true},
		{"пустое", []byte{}, true},
		{"только пробелы", []byte(" \n\t\r"), true},
		{"HTML с пробелами", []byte("  <!DOCTYPE html><html><body>"), true},
		{"HTML без пробелов", []byte("<html>"), true},
		{"BOM + HTML", []byte("\xef\xbb\xbf<html>"), true},
		{"пробелы + BOM + HTML", []byte(" \n\xef\xbb\xbf  <html>"), true},
		{"только BOM", []byte("\xef\xbb\xbf"), true},
		{"UTF-16 LE BOM", []byte{0xFF, 0xFE, '<', 0x00, 'h', 0x00}, true},
		{"UTF-16 BE BOM", []byte{0xFE, 0xFF, 0x00, '<', 0x00, 'h'}, true},
		{"BOM + zip", []byte("\xef\xbb\xbfPK\x03\x04data"), false},
		{"BOM + пробелы + zip", []byte("\xef\xbb\xbf \nPK\x03\x04data"), false},
		{"zip", []byte("PK\x03\x04data"), false},
		{"rar", []byte("Rar!\x1a\x07data"), false},
		{"pdf", []byte("%PDF-1.7"), false},
		{"бинарное", []byte{0x00, 0x01, 0x02}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBody(tc.body)
			if tc.wantErr && err == nil {
				t.Fatal("тело принято, ожидалась ошибка")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("тело отвергнуто: %v", err)
			}
		})
	}
	if err := validateBody([]byte(" <html>")); err == nil || !strings.Contains(err.Error(), "HTML") {
		t.Fatalf("ошибка %v не называет HTML", err)
	}
	if err := validateBody([]byte{0xFF, 0xFE, '<', 0x00}); err == nil || !strings.Contains(err.Error(), "UTF-16") {
		t.Fatalf("ошибка %v не называет UTF-16", err)
	}
}
