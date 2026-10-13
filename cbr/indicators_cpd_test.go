package cbr

import (
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

// TestIndicatorsCpdFindLink: позиционный селектор страницы aipd находит indicators_cpd.xlsx.
func TestIndicatorsCpdFindLink(t *testing.T) {
	href, err := findLink(readCbrTestdata(t, "aipd_page.html"), indicatorsCpdLinkSelector)
	if err != nil {
		t.Fatalf("findLink: %v", err)
	}
	if !strings.HasSuffix(href, "indicators_cpd.xlsx") {
		t.Errorf("href %q должен указывать на indicators_cpd.xlsx", href)
	}
}

// TestIndicatorsCpdSeriesMetaCoversParsedRows: разбор фикстуры и каталог совпадают в обе стороны.
func TestIndicatorsCpdSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrIndicatorsCpdSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile("testdata/aipd.xlsx")
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return indicatorsCpdImport(f, rec)
	})
}

// TestIndicatorsCpdEmptySheetFails: пустой лист «Лист1» — ошибка, а не «0 rows» с зелёным
// dagu: разбор без строк «Все товары и услуги»/«Базовый ИПЦ» тихо записал бы ноль точек, и
// ряд остался бы без обновления. Имя таблицы в тексте ошибки сверяется литералом: оно —
// часть контракта для дежурного по логу.
func TestIndicatorsCpdEmptySheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "Лист1"); err != nil {
		t.Fatalf("SetSheetName: %v", err)
	}
	rec := newCbrRecorder()
	err := indicatorsCpdImport(f, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), "cbr_indicators_cpd") {
		t.Errorf("ошибка %q не называет таблицу cbr_indicators_cpd", err)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}
