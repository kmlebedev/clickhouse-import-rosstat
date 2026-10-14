package cbr

import (
	"strings"
	"testing"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// TestCreditM2xFindLink: селектор страницы ДКС находит credit_m2x.xlsx.
func TestCreditM2xFindLink(t *testing.T) {
	href, err := util.FindLink(readCbrTestdata(t, "credit_m2x_page.html"), cbrCreditM2xLinkSelector)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
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

// TestCreditM2xEmptySheetFails: пустой лист «млн рублей» — ошибка, а не «0 rows» с зелёным
// dagu: молчаливый прогон выдал бы успешный импорт без единой точки. Имя таблицы в тексте
// ошибки сверяется литералом: оно — часть контракта для дежурного по логу.
func TestCreditM2xEmptySheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "млн рублей"); err != nil {
		t.Fatalf("SetSheetName: %v", err)
	}
	rec := newCbrRecorder()
	err := cbrСreditM2x.ImportFunc(f, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), "cbr_credit_m2x") {
		t.Errorf("ошибка %q не называет таблицу cbr_credit_m2x", err)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}
