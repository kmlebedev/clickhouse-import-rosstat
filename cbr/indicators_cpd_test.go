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
