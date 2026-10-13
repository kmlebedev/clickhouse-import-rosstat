package cbr

import (
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

// TestCreditM2xFindLink: селектор страницы ДКС находит credit_m2x.xlsx.
func TestCreditM2xFindLink(t *testing.T) {
	href, err := findLink(readCbrTestdata(t, "credit_m2x_page.html"), cbrCreditM2xLinkSelector)
	if err != nil {
		t.Fatalf("findLink: %v", err)
	}
	if !strings.HasSuffix(href, "credit_m2x.xlsx") {
		t.Errorf("href %q должен указывать на credit_m2x.xlsx", href)
	}
}

// TestCreditM2xSeriesMetaCoversParsedRows: все 22 имени разбора описаны в каталоге и наоборот.
func TestCreditM2xSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrCreditM2xSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile("testdata/credit_m2x.xlsx")
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return cbrСreditM2x.ImportFunc(f, rec)
	})
}
