package util

import (
	"context"
	"strings"
	"testing"
)

func TestPDFTextNotPDF(t *testing.T) {
	_, err := PDFText(context.Background(), []byte(`<html><body>Доступ ограничен</body></html>`), PDFLayout)
	if err == nil {
		t.Fatal("ожидалась ошибка: HTML-заглушка не является PDF")
	}
	if !strings.Contains(err.Error(), "не PDF") {
		t.Errorf("ошибка %q не называет подмену файла", err)
	}
}

func TestPDFTextToolMissing(t *testing.T) {
	old := PDFTool
	PDFTool = "pdftotext-kimi-code-missing"
	t.Cleanup(func() { PDFTool = old })

	_, err := PDFText(context.Background(), []byte("%PDF-1.4\n1 0 obj\n"), PDFLayout)
	if err == nil {
		t.Fatal("ожидалась ошибка отсутствующего pdftotext")
	}
	if !strings.Contains(err.Error(), "poppler-utils") {
		t.Errorf("ошибка %q не называет poppler-utils", err)
	}
}
