package cbr

import (
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// TestMacroprudentialFailFast — ожидаемые ошибки разбора, а не «0 строк»:
// неизвестная строка АЦН, нечисловая ячейка матрицы, неизвестная полоса,
// пустая подпись xlsx-ссылки.
func TestMacroprudentialFailFast(t *testing.T) {
	t.Run("ccyb_unknown_indicator", func(t *testing.T) {
		f := excelize.NewFile()
		defer func() { _ = f.Close() }()
		if err := f.SetSheetName("Sheet1", cbrCcybSheet); err != nil {
			t.Fatal(err)
		}
		_ = f.SetCellValue(cbrCcybSheet, "D2", time.Date(2008, 2, 1, 0, 0, 0, 0, time.UTC))
		_ = f.SetCellValue(cbrCcybSheet, "A3", 99)
		_ = f.SetCellValue(cbrCcybSheet, "B3", "неизвестный показатель")
		_ = f.SetCellValue(cbrCcybSheet, "D3", 1.0)
		if _, err := cbrCcybParse(f); err == nil {
			t.Fatal("неизвестный номер показателя должен давать ошибку")
		}
	})

	t.Run("risk_addon_cell", func(t *testing.T) {
		if v, ok, err := riskAddonCellValue("н/п"); err != nil || v != 0 || ok != 0 {
			t.Fatalf("н/п: %v/%d/%v", v, ok, err)
		}
		if v, ok, err := riskAddonCellValue("1,3"); err != nil || v != 1.3 || ok != 1 {
			t.Fatalf("1,3: %v/%d/%v", v, ok, err)
		}
		if _, _, err := riskAddonCellValue("abc"); err == nil {
			t.Fatal("нечисловая ячейка должна давать ошибку")
		}
	})

	t.Run("mpl_band_text", func(t *testing.T) {
		file := mplFile{Name: "x.xlsx"}
		if b, ok := mplParseBandText("(50; 80]", file, "s", 1); !ok || *b.min != 50 || *b.max != 80 {
			t.Fatalf("(50; 80]: %+v/%v", b, ok)
		}
		if b, ok := mplParseBandText("более 80", file, "s", 1); !ok || *b.min != 80 || b.max != nil {
			t.Fatalf("более 80: %+v/%v", b, ok)
		}
		if _, ok := mplParseBandText("странная полоса", file, "s", 1); ok {
			t.Fatal("неизвестная полоса должна вернуть false")
		}
	})

	t.Run("page_empty_caption", func(t *testing.T) {
		body := []byte(`<html><body><a href="/Collection/Collection/File/1/x.xlsx"></a></body></html>`)
		if _, err := cbrRiskWeightAddonsParsePage(body); err == nil ||
			!strings.Contains(err.Error(), "без подписи") {
			t.Fatalf("риск: ожидалась ошибка пустой подписи, got %v", err)
		}
		body = []byte(`<html><body><a href="/Content/Document/File/1/x.xlsx"></a></body></html>`)
		if _, _, err := cbrMPLParsePage(body); err == nil ||
			!strings.Contains(err.Error(), "без подписи") {
			t.Fatalf("МПЛ: ожидалась ошибка пустой подписи, got %v", err)
		}
	})
}
