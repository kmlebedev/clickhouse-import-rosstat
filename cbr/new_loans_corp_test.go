package cbr

import (
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

const loansToCorpFixture = "testdata/new_loans_corp.xlsx"

// TestLoansToCorpSeriesMetaCoversParsedRows: двусторонний контракт каталога
// (26 имён видов деятельности) и разбора фикстуры.
func TestLoansToCorpSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrLoansToCorpSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(loansToCorpFixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return cbrLoansToCorpImport(f, rec)
	})
}

// TestLoansToCorpFixtureRows: фикстура даёт 26 имён по 92 месяца: 2019-02-01..2026-09-01
// (колонки файла «Январь 2019»..«Август 2026», дата сдвинута на месяц вперёд) — как
// cbr_loans_to_corporations в baseline T1 (n = 92, d0 = 2019-02-01, d1 = 2026-09-01).
func TestLoansToCorpFixtureRows(t *testing.T) {
	rows := cbrFixtureRows(t, loansToCorpFixture, cbrLoansToCorpImport)
	perName := map[string][]time.Time{}
	for _, row := range rows {
		perName[row.name] = append(perName[row.name], row.date)
	}
	if len(perName) != 26 {
		t.Errorf("имён в разборе %d, want 26", len(perName))
	}
	for name, dates := range perName {
		if len(dates) != 92 {
			t.Errorf("имя %q: точек %d, want 92", name, len(dates))
			continue
		}
		if !dates[0].Equal(time.Date(2019, time.February, 1, 0, 0, 0, 0, time.UTC)) ||
			!dates[len(dates)-1].Equal(time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("имя %q: ряд %s..%s, want 2019-02-01..2026-09-01", name,
				dates[0].Format("2006-01-02"), dates[len(dates)-1].Format("2006-01-02"))
		}
	}
}

// TestLoansToCorpSpotValues: споты baseline T1 на 2026-09-01 (колонка «Август 2026»).
func TestLoansToCorpSpotValues(t *testing.T) {
	rows := cbrFixtureRows(t, loansToCorpFixture, cbrLoansToCorpImport)
	date := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	cbrCheckSpot(t, rows, "ВСЕГО", date, 7183059)
	cbrCheckSpot(t, rows, "добыча полезных ископаемых, из них:", date, 139952)
}

// TestLoansToCorpEmptySheetFails: пустой лист, отсутствующий лист и лист без строки
// «ВСЕГО» — ошибка, а не «0 rows».
func TestLoansToCorpEmptySheetFails(t *testing.T) {
	for name, build := range map[string]func(t *testing.T) *excelize.File{
		"нет листа «итого»": func(t *testing.T) *excelize.File {
			f := excelize.NewFile()
			t.Cleanup(func() { _ = f.Close() })
			return f
		},
		"лист пуст": func(t *testing.T) *excelize.File {
			f := excelize.NewFile()
			t.Cleanup(func() { _ = f.Close() })
			if err := f.SetSheetName("Sheet1", "итого"); err != nil {
				t.Fatalf("SetSheetName: %v", err)
			}
			return f
		},
		"нет строки «ВСЕГО»": func(t *testing.T) *excelize.File {
			f := excelize.NewFile()
			t.Cleanup(func() { _ = f.Close() })
			if err := f.SetSheetName("Sheet1", "итого"); err != nil {
				t.Fatalf("SetSheetName: %v", err)
			}
			_ = f.SetCellValue("итого", "A1", "Виды экономической деятельности")
			_ = f.SetCellValue("итого", "B1", "Январь 2019")
			_ = f.SetCellValue("итого", "A2", "прочие виды деятельности")
			_ = f.SetCellValue("итого", "B2", "1 289 880")
			return f
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			if err := cbrLoansToCorpImport(build(t), rec); err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if len(rec.rows) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.rows))
			}
		})
	}
}
