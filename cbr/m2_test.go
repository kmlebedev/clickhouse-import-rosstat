package cbr

import (
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

const cbrM2Fixture = "testdata/monetary_agg_SA.xlsx"

// TestCbrM2SeriesMetaCoversParsedRows: каталог cbr_m2 (один ряд «М2 SA») и разбор фикстуры
// совпадают в обе стороны.
func TestCbrM2SeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrM2SeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(cbrM2Fixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return cbrM2Import(f, rec)
	})
}

// TestCbrM2FixtureRows: фикстура даёт 309 месячных точек 2001-01-01..2026-09-01 — как
// cbr_m2 в baseline T1 (n = 309, d0 = 2001-01-01, d1 = 2026-09-01).
func TestCbrM2FixtureRows(t *testing.T) {
	rows := cbrFixtureRows(t, cbrM2Fixture, cbrM2Import)
	if len(rows) != 309 {
		t.Errorf("точек в разборе %d, want 309 (baseline T1)", len(rows))
	}
	if !rows[0].date.Equal(time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)) ||
		!rows[len(rows)-1].date.Equal(time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("ряд %s..%s, want 2001-01-01..2026-09-01",
			rows[0].date.Format("2006-01-02"), rows[len(rows)-1].date.Format("2006-01-02"))
	}
}

// TestCbrM2SpotValue: значение «М2 SA» на 2026-09-01 из baseline T1 (138263.90625 —
// Float32-округление ячейки 138 263.9) совпадает с разбором фикстуры.
func TestCbrM2SpotValue(t *testing.T) {
	rows := cbrFixtureRows(t, cbrM2Fixture, cbrM2Import)
	cbrCheckSpot(t, rows, "М2 SA", time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC), 138263.90625)
}

// TestCbrM2EmptySheetFails: пустой лист и отсутствующий лист — ошибка, а не «0 rows».
func TestCbrM2EmptySheetFails(t *testing.T) {
	for name, build := range map[string]func(t *testing.T) *excelize.File{
		"нет листа «Денежные агрегаты»": func(t *testing.T) *excelize.File {
			f := excelize.NewFile()
			t.Cleanup(func() { _ = f.Close() })
			return f
		},
		"лист пуст": func(t *testing.T) *excelize.File {
			f := excelize.NewFile()
			t.Cleanup(func() { _ = f.Close() })
			if err := f.SetSheetName("Sheet1", "Денежные агрегаты"); err != nil {
				t.Fatalf("SetSheetName: %v", err)
			}
			return f
		},
		"в листе только заголовок": func(t *testing.T) *excelize.File {
			f := excelize.NewFile()
			t.Cleanup(func() { _ = f.Close() })
			if err := f.SetSheetName("Sheet1", "Денежные агрегаты"); err != nil {
				t.Fatalf("SetSheetName: %v", err)
			}
			_ = f.SetCellValue("Денежные агрегаты", "A1", "Денежные агрегаты")
			_ = f.SetCellValue("Денежные агрегаты", "W1", "М2 SA")
			return f
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			if err := cbrM2Import(build(t), rec); err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if len(rec.rows) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.rows))
			}
		})
	}
}
