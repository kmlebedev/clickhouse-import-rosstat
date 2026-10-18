package edisclosure

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// validManifestJSON — минимальный валидный манифест для проверок строк: одна
// компания и document_count = 1. Полный манифест пилота — testdata/manifest.json.
const validManifestJSON = `{
  "schema_version": 1,
  "generated_at": "2026-10-18T12:00:00+03:00",
  "collector": {"name": "edisclosure_collect.py", "version": "1"},
  "sections": ["IFRS", "RAS", "AFFILIATES", "AGM"],
  "companies": [
    {"ticker": "PLZL", "company_id": "7832", "name": "ПАО «Полюс»",
     "url": "https://e-disclosure.ru/portal/company.aspx?id=7832"}
  ],
  "document_count": 1,
  "documents_file": "documents.jsonl"
}`

// validDocumentJSON — строка документа контракта спеки §4.2: без company_id
// (заполняется из манифеста) и с пустыми датами (zero time).
const validDocumentJSON = `{"ticker":"PLZL","section":"IFRS","doc_type":"x","period":"2025",` +
	`"basis_date":"","publication_date":"","fileid":"1943701",` +
	`"url":"https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701"}`

// testManifest — валидный манифест через тот же разбор, что и у импортёра:
// тесты строк не должны собирать структуру в обход контракта.
func testManifest(t *testing.T) manifest {
	t.Helper()
	m, err := parseManifest([]byte(validManifestJSON))
	if err != nil {
		t.Fatalf("parseManifest: %v", err)
	}
	return m
}

// TestReadInputFixture — контракт входа импортёра на живых строках пилота
// (спека §4.2): манифест и шесть документов, порядок строк фикстуры — порядок
// выборки, поэтому docs[0] — документ Fileid 1943701.
func TestReadInputFixture(t *testing.T) {
	m, docs, err := readInput("testdata")
	if err != nil {
		t.Fatalf("readInput: %v", err)
	}
	if m.DocumentCount != 6 || len(docs) != 6 {
		t.Fatalf("manifest=%d docs=%d, want 6/6", m.DocumentCount, len(docs))
	}
	first := docs[0]
	if first.Ticker != "PLZL" || first.Section != "IFRS" || first.FileID != "1943701" {
		t.Fatalf("first = %+v", first)
	}
	if got := first.PublicationDate.Format("2006-01-02"); got != "2026-08-31" {
		t.Fatalf("publication_date = %s", got)
	}
	// company_id в строках пилота отсутствует — он обязан прийти из манифеста.
	if first.CompanyID != "7832" {
		t.Errorf("company_id = %q, want 7832 (заполнен из manifest.companies)", first.CompanyID)
	}
	if got := first.BasisDate.Format("2006-01-02"); got != "2026-08-28" {
		t.Errorf("basis_date = %s, want 2026-08-28", got)
	}
	wantTickers := []string{"PLZL", "PLZL", "PLZL", "CHMF", "CHMF", "SBER"}
	for i, want := range wantTickers {
		if docs[i].Ticker != want {
			t.Errorf("docs[%d].ticker = %q, want %q (порядок строк фикстуры)", i, docs[i].Ticker, want)
		}
	}
}

// TestParseDocumentsRejects — строки, которые коллектор не должен был записать,
// отвергаются с номером строки и причиной (спека §4.2, §5.3 п. 1). Подстроки
// want — часть контракта ошибки: по ним видно, какое поле не сошлось.
func TestParseDocumentsRejects(t *testing.T) {
	base := validDocumentJSON
	cases := []struct{ name, row, want string }{
		{"bad ticker", strings.Replace(base, `"PLZL"`, `"GAZP"`, 1), "manifest"},
		{"lower ticker", strings.Replace(base, `"PLZL"`, `"plzl"`, 1), "верхний регистр"},
		{"bad section", strings.Replace(base, `"IFRS"`, `"OTHER"`, 1), "section"},
		{"empty doc_type", strings.Replace(base, `"x"`, `""`, 1), "doc_type"},
		{"bad fileid", strings.Replace(base, `"1943701"`, `"abc"`, 2), "fileid"},
		{"wrong host", strings.Replace(base, "e-disclosure.ru", "example.com", 1), "e-disclosure"},
		{"wrong path", strings.Replace(base, "/portal/FileLoad.ashx", "/portal/other", 1), "FileLoad"},
		{"fileid mismatch", strings.Replace(base, "Fileid=1943701", "Fileid=2", 1), "Fileid"},
		{"bad date", strings.Replace(base, `"publication_date":""`, `"publication_date":"31/08/2026"`, 1), "publication_date"},
		{"bad basis date", strings.Replace(base, `"basis_date":""`, `"basis_date":"2026-08-28"`, 1), "basis_date"},
	}
	m := testManifest(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			docs, err := parseDocuments([]byte(tc.row+"\n"), m)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разобрано документов: %d", len(docs))
			}
			if !strings.Contains(err.Error(), "строка 1") {
				t.Errorf("ошибка %q не называет номер строки", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ошибка %q не содержит %q", err, tc.want)
			}
			if docs != nil {
				t.Errorf("при ошибке разбор отдал %d документов — частичный вход хуже пустого", len(docs))
			}
		})
	}
}

// TestParseManifestRejects — правила манифеста спеки §5.3 п. 1: версия схемы,
// имя файла документов и непустой список компаний.
func TestParseManifestRejects(t *testing.T) {
	cases := []struct{ name, data, want string }{
		{"schema version", strings.Replace(validManifestJSON, `"schema_version": 1`, `"schema_version": 2`, 1), "schema_version"},
		{"documents file", strings.Replace(validManifestJSON, `"documents.jsonl"`, `"other.jsonl"`, 1), "documents_file"},
		{"empty companies", `{"schema_version":1,"companies":[],"document_count":0,"documents_file":"documents.jsonl"}`, "companies"},
		{"broken json", `{`, "json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := parseManifest([]byte(tc.data))
			if err == nil {
				t.Fatalf("ожидалась ошибка, разобран манифест: %+v", m)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ошибка %q не содержит %q", err, tc.want)
			}
		})
	}
}

// TestParseDocumentsDocumentCountMismatch — document_count манифеста обязан
// совпасть с числом уникальных строк: расхождение означает частичный снапшот.
func TestParseDocumentsDocumentCountMismatch(t *testing.T) {
	m := testManifest(t)
	m.DocumentCount = 2
	docs, err := parseDocuments([]byte(validDocumentJSON+"\n"), m)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разобрано документов: %d", len(docs))
	}
	if !strings.Contains(err.Error(), "document_count") {
		t.Errorf("ошибка %q не называет document_count", err)
	}
}

// TestParseDocumentsEmptyDates — пустые даты карточки допустимы и дают zero
// time: в таблицу они пишутся как NULL, а не как 1970-01-01.
func TestParseDocumentsEmptyDates(t *testing.T) {
	docs, err := parseDocuments([]byte(validDocumentJSON+"\n"), testManifest(t))
	if err != nil {
		t.Fatalf("parseDocuments: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("документов %d, want 1", len(docs))
	}
	if !docs[0].BasisDate.IsZero() || !docs[0].PublicationDate.IsZero() {
		t.Errorf("пустые даты = (%v, %v), want zero time",
			docs[0].BasisDate, docs[0].PublicationDate)
	}
}

// TestParseDocumentsCompanyID — company_id строки необязателен: пустой
// заполняется из манифеста, непустой сверяется с ним (спека §4.2).
func TestParseDocumentsCompanyID(t *testing.T) {
	withID := func(id string) string {
		return strings.Replace(validDocumentJSON, `{"ticker":"PLZL",`,
			`{"ticker":"PLZL","company_id":"`+id+`",`, 1)
	}
	for name, tc := range map[string]struct {
		row     string
		wantID  string
		wantErr string
	}{
		"нет company_id — берётся из манифеста": {row: validDocumentJSON, wantID: "7832"},
		"company_id совпадает":                  {row: withID("7832"), wantID: "7832"},
		"company_id расходится":                 {row: withID("9999"), wantErr: "company_id"},
	} {
		t.Run(name, func(t *testing.T) {
			docs, err := parseDocuments([]byte(tc.row+"\n"), testManifest(t))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseDocuments: %v", err)
			}
			if docs[0].CompanyID != tc.wantID {
				t.Errorf("company_id = %q, want %q", docs[0].CompanyID, tc.wantID)
			}
		})
	}
}

// TestParseDocumentsDuplicates — дубликат ключа (ticker, section, fileid)
// схлопывается только при полном совпадении полей; расхождение — ошибка, потому
// что какая из версий документа настоящая, разбор не знает (спека §4.2).
func TestParseDocumentsDuplicates(t *testing.T) {
	t.Run("идентичные схлопываются", func(t *testing.T) {
		// document_count = 1 при двух одинаковых строках: манифест считает
		// документы после дедупликации коллектора.
		docs, err := parseDocuments([]byte(validDocumentJSON+"\n"+validDocumentJSON+"\n"), testManifest(t))
		if err != nil {
			t.Fatalf("parseDocuments: %v", err)
		}
		if len(docs) != 1 {
			t.Errorf("документов %d, want 1 (идентичный дубликат схлопывается)", len(docs))
		}
	})
	t.Run("расходящиеся — ошибка", func(t *testing.T) {
		other := strings.Replace(validDocumentJSON, `"period":"2025"`, `"period":"2024"`, 1)
		docs, err := parseDocuments([]byte(validDocumentJSON+"\n"+other+"\n"), testManifest(t))
		if err == nil {
			t.Fatalf("ожидалась ошибка, разобрано документов: %d", len(docs))
		}
		for _, want := range []string{"дубликат", "расходится", "строка 2"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ошибка %q не содержит %q", err, want)
			}
		}
	})
	t.Run("разные size и table_num схлопываются", func(t *testing.T) {
		// size и table_num — аудит источника, а не содержание документа: две
		// строки одного fileid с разными метками обязаны схлопнуться.
		sameContent := strings.Replace(validDocumentJSON, `"fileid":"1943701"`,
			`"fileid":"1943701","size":"zip, 1 КБ","table_num":"7"`, 1)
		docs, err := parseDocuments([]byte(validDocumentJSON+"\n"+sameContent+"\n"), testManifest(t))
		if err != nil {
			t.Fatalf("parseDocuments: %v", err)
		}
		if len(docs) != 1 {
			t.Errorf("документов %d, want 1 (аудит-поля не различают документ)", len(docs))
		}
	})
}

// TestValidateDocumentURL — строгая форма ссылки на файл (спека §4.2): только
// https, host e-disclosure.ru без порта и userinfo, path FileLoad.ashx и
// Fileid, совпадающий с fileid строки.
func TestValidateDocumentURL(t *testing.T) {
	const fileID = "1943701"
	for name, tc := range map[string]struct {
		url     string
		wantErr string
	}{
		"валидный": {
			url: "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701",
		},
		"верхний регистр host": {
			url: "https://E-DISCLOSURE.RU/portal/FileLoad.ashx?Fileid=1943701",
		},
		"http": {
			url:     "http://e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701",
			wantErr: "https",
		},
		"порт": {
			url:     "https://e-disclosure.ru:8443/portal/FileLoad.ashx?Fileid=1943701",
			wantErr: "e-disclosure.ru",
		},
		"порт 443": {
			url:     "https://e-disclosure.ru:443/portal/FileLoad.ashx?Fileid=1943701",
			wantErr: "e-disclosure.ru",
		},
		"пустой порт": {
			url:     "https://e-disclosure.ru:/portal/FileLoad.ashx?Fileid=1943701",
			wantErr: "e-disclosure.ru",
		},
		"userinfo": {
			url:     "https://user:pass@e-disclosure.ru/portal/FileLoad.ashx?Fileid=1943701",
			wantErr: "userinfo",
		},
		"чужой host": {
			url:     "https://example.com/portal/FileLoad.ashx?Fileid=1943701",
			wantErr: "e-disclosure",
		},
		"чужой path": {
			url:     "https://e-disclosure.ru/portal/other?Fileid=1943701",
			wantErr: "FileLoad",
		},
		"нет Fileid": {
			url:     "https://e-disclosure.ru/portal/FileLoad.ashx",
			wantErr: "Fileid",
		},
		"чужой Fileid": {
			url:     "https://e-disclosure.ru/portal/FileLoad.ashx?Fileid=2",
			wantErr: "Fileid",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateDocumentURL(fileID, tc.url)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateDocumentURL: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ошибка = %v, want содержит %q", err, tc.wantErr)
			}
		})
	}
}

// TestParseDate — дата карточки: ДД.ММ.ГГГГ или пусто; всё прочее — ошибка,
// включая несуществующие даты и близкие форматы.
func TestParseDate(t *testing.T) {
	got, err := parseDate("31.08.2026")
	if err != nil {
		t.Fatalf("parseDate: %v", err)
	}
	if want := time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("parseDate(31.08.2026) = %v, want %v", got, want)
	}
	zero, err := parseDate("")
	if err != nil || !zero.IsZero() {
		t.Errorf("parseDate(\"\") = (%v, %v), want zero time без ошибки", zero, err)
	}
	for _, bad := range []string{"31/08/2026", "2026-08-31", "29.02.2025", "32.08.2026", "31.08.26", " 31.08.2026"} {
		if _, err := parseDate(bad); err == nil {
			t.Errorf("parseDate(%q) не вернул ошибку", bad)
		}
	}
}

// TestDocKey — ключ идентичности документа: (ticker, section, fileid) через NUL,
// чтобы значения полей не могли слипнуться в один ключ.
func TestDocKey(t *testing.T) {
	d := doc{Ticker: "PLZL", Section: "IFRS", FileID: "1943701"}
	if got, want := docKey(d), "PLZL\x00IFRS\x001943701"; got != want {
		t.Fatalf("docKey = %q, want %q", got, want)
	}
	other := d
	other.Section = "RAS"
	if docKey(d) == docKey(other) {
		t.Error("ключи разных разделов совпали")
	}
}

// TestReadInputManifestFirst — readInput читает manifest.json первым: без
// манифеста вход не разбирается, даже если documents.jsonl на месте.
func TestReadInputManifestFirst(t *testing.T) {
	if _, _, err := readInput(t.TempDir()); err == nil || !strings.Contains(err.Error(), "manifest.json") {
		t.Fatalf("readInput пустого каталога: ошибка = %v, want упоминание manifest.json", err)
	}
}

// TestReadInputMissingDocuments — манифест валиден, а documents.jsonl нет:
// ошибка называет файл, а не выглядит пустым прогоном.
func TestReadInputMissingDocuments(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(validManifestJSON), 0o600); err != nil {
		t.Fatalf("запись manifest.json: %v", err)
	}
	if _, _, err := readInput(dir); err == nil || !strings.Contains(err.Error(), "documents.jsonl") {
		t.Fatalf("readInput без documents.jsonl: ошибка = %v, want упоминание documents.jsonl", err)
	}
}
