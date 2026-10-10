package polyus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// pdfBytes минимально валидный PDF размером не меньше порога "suspiciously small"
// (1024 байта) из downloadPDF.
func pdfBytes(minLen int) []byte {
	body := []byte("%PDF-1.5\n% test payload\n")
	for len(body) < minLen {
		body = append(body, "x"...)
	}
	return body
}

func TestValidatePDFHeaderAcceptsPDF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ok.pdf")
	if err := os.WriteFile(path, []byte("%PDF-1.5\nrest of the file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validatePDFHeader(path); err != nil {
		t.Errorf("validatePDFHeader on a PDF: %v", err)
	}
}

func TestValidatePDFHeaderRejectsNonPDF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "error.html")
	html := "<!DOCTYPE html><html><body>404 Not Found</body></html>"
	if err := os.WriteFile(path, []byte(html), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validatePDFHeader(path); err == nil {
		t.Error("expected error for an HTML body served with a 200 status")
	}
}

func TestValidatePDFHeaderRejectsMissingFile(t *testing.T) {
	if err := validatePDFHeader(filepath.Join(t.TempDir(), "absent.pdf")); err == nil {
		t.Error("expected error for a missing file")
	}
}

func TestDownloadPDFRejectsHTMLContent(t *testing.T) {
	// Тело подделывает и размер (>= 1024), и заголовок %PDF-, так что отклонить
	// его может только проверка Content-Type — иначе тест проходил бы из-за
	// других guard'ов. Так выглядит HTML-страница ошибки с заголовком-приманкой.
	html := []byte("%PDF-1.5\n<!DOCTYPE html><html><body>maintenance")
	for len(html) < 2048 {
		html = append(html, " "...)
	}
	html = append(html, "</body></html>"...)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(html)
	}))
	defer server.Close()

	destination := filepath.Join(t.TempDir(), "report.pdf")
	if err := downloadPDF(context.Background(), server.URL, destination); err == nil {
		t.Error("expected error for HTML Content-Type")
	}
}

func TestDownloadPDFRejectsShortBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write(pdfBytes(64))
	}))
	defer server.Close()

	destination := filepath.Join(t.TempDir(), "report.pdf")
	if err := downloadPDF(context.Background(), server.URL, destination); err == nil {
		t.Error("expected error for a body under 1024 bytes")
	}
}

func TestDownloadPDFRejectsHTMLServedAsPDF(t *testing.T) {
	// Реалистичный отказ: сайт отдаёт HTML-страницу ошибки с 200 и
	// Content-Type: application/pdf. Ловит её только проверка заголовка %PDF-.
	html := []byte("<!DOCTYPE html><html><body>503 Service Unavailable")
	for len(html) < 2048 {
		html = append(html, " "...)
	}
	html = append(html, "</body></html>"...)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write(html)
	}))
	defer server.Close()

	destination := filepath.Join(t.TempDir(), "report.pdf")
	if err := downloadPDF(context.Background(), server.URL, destination); err == nil {
		t.Error("expected error for an HTML body served as application/pdf")
	}
}

func TestDownloadPDFWritesValidPDF(t *testing.T) {
	payload := pdfBytes(1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	destination := filepath.Join(t.TempDir(), "report.pdf")
	if err := downloadPDF(context.Background(), server.URL, destination); err != nil {
		t.Fatalf("downloadPDF: %v", err)
	}
	content, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if len(content) < 1024 {
		t.Errorf("downloadPDF wrote %d bytes, want >= 1024", len(content))
	}
	if got := string(content[:5]); got != "%PDF-" {
		t.Errorf("downloaded file starts with %q, want %%PDF-", got)
	}
}

// extractPDF идёт через внешнюю утилиту pdftotext, которой в CI может не быть,
// поэтому вызываем только путь, завершающийся до запуска команды.
func TestExtractPDFRejectsEmptyPageList(t *testing.T) {
	err := extractPDF(context.Background(), "nonexistent.pdf", filepath.Join(t.TempDir(), "out.txt"), nil)
	if err == nil {
		t.Fatal("expected error for empty page list")
	}
}
