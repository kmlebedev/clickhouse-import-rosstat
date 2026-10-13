package cbr

import (
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

// TestFindLinkMissingAnchor: страница без нужного якоря — ошибка, а не пустой href.
func TestFindLinkMissingAnchor(t *testing.T) {
	for name, tc := range map[string]struct {
		page []byte
		sel  string
	}{
		"пустая страница": {[]byte(`<html><body></body></html>`), inflExpLinkSelector},
		"чужой якорь":     {[]byte(`<html><body><a class="versions_item" href="/a.pdf">x</a></body></html>`), inflExpLinkSelector},
		"пустой href":     {[]byte(`<html><body><a class="versions_item" href="">x</a></body></html>`), inflExpLinkSelector},
	} {
		t.Run(name, func(t *testing.T) {
			if href, err := findLink(tc.page, tc.sel); err == nil {
				t.Errorf("ожидалась ошибка, получен href %q", href)
			}
		})
	}
}

// TestInflExpFindLink: на странице инфляционных ожиданий первая ссылка — свежий xlsx.
func TestInflExpFindLink(t *testing.T) {
	href, err := findLink(readCbrTestdata(t, "infl_exp_page.html"), inflExpLinkSelector)
	if err != nil {
		t.Fatalf("findLink: %v", err)
	}
	if !strings.HasSuffix(href, ".xlsx") || !strings.Contains(href, "Infl_exp_") {
		t.Errorf("href %q не похож на свежий Infl_exp_*.xlsx", href)
	}
}

// TestInflExpSeriesMetaCoversParsedRows: разбор фикстуры и каталог совпадают в обе стороны.
func TestInflExpSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrInflExpSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile("testdata/infl_exp.xlsx")
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return inflExpImport(f, rec)
	})
}
