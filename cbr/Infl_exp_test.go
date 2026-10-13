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

// TestInflExpEmptySheetFails: пустой лист «Данные для графиков» (ни маркера, ни строк
// данных) — ошибка, а не «0 rows» с зелёным dagu: молчаливый прогон выглядел бы успешным,
// а ряд остался бы без обновления. Имя таблицы в тексте ошибки — часть контракта, поэтому
// сверяется литералом: дежурный по логу должен видеть, чей разбор дал ноль.
func TestInflExpEmptySheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "Данные для графиков"); err != nil {
		t.Fatalf("SetSheetName: %v", err)
	}
	rec := newCbrRecorder()
	err := inflExpImport(f, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), "cbr_infl_exp") {
		t.Errorf("ошибка %q не называет таблицу cbr_infl_exp", err)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}
