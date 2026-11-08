package cbr

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// sorsFundsFixture — живая выгрузка ЦБ «Средства на счетах организаций, банковские депозиты
// (вклады) и другие привлеченные средства юридических лиц, физических лиц и индивидуальных
// предпринимателей (в целом по Российской Федерации)» на снимке 2026-11-08 (стандарт §11.1:
// реальный артефакт источника, не синтетическая книга).
const sorsFundsFixture = "testdata/02_01_Funds_all.xlsx"

// TestSorsFundsSeriesMetaCoversParsedRows: двусторонний контракт каталога и разбора фикстуры —
// каждый разобранный ряд описан в каталоге и каждый описанный ряд встречается в разборе.
// Имена объявлены кодом (стандарт §6.8), поэтому контракт двусторонний.
func TestSorsFundsSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrSorsFundsSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(sorsFundsFixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return sorsFundsImport(f, rec)
	})
}

// TestSorsFundsFixtureRows пинит структуру источника: 3 листа × 7 строк × 177 месячных дат =
// 3 177 точек; по 1 059 на лист (177 × 4 + 93 × 2 + 165: строки «без эскроу» и «эскроу» ЦБ
// публикует с 2019-02, средства ИП — с 2013-02, остальные с 2012-02). Молчаливая потеря листа,
// строки или даты не пройдёт.
func TestSorsFundsFixtureRows(t *testing.T) {
	rows := cbrFixtureRows(t, sorsFundsFixture, sorsFundsImport)
	if len(rows) != 3177 {
		t.Errorf("точек в разборе %d, want 3177 (3 листа × 1059)", len(rows))
	}
	names := map[string]bool{}
	perName := map[string]int{}
	perSuffix := map[string]int{}
	first, last := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, row := range rows {
		names[row.name] = true
		perName[row.name]++
		switch {
		case len(row.name) > 4 && row.name[len(row.name)-4:] == "_rub":
			perSuffix["_rub"]++
		case len(row.name) > 3 && row.name[len(row.name)-3:] == "_fx":
			perSuffix["_fx"]++
		default:
			perSuffix[""]++
		}
		if row.date.Before(first) {
			first = row.date
		}
		if row.date.After(last) {
			last = row.date
		}
	}
	if len(names) != 21 {
		t.Errorf("рядов в разборе %d, want 21 (7 строк × 3 листа)", len(names))
	}
	for _, suffix := range []string{"", "_rub", "_fx"} {
		if perSuffix[suffix] != 1059 {
			t.Errorf("точек на листе с суффиксом %q: %d, want 1059", suffix, perSuffix[suffix])
		}
	}
	for name, want := range map[string]int{
		"clients_total":       177,
		"households_deposits": 93,
		"escrow_accounts":     93,
		"ip_accounts":         165,
		"escrow_accounts_fx":  93,
	} {
		if perName[name] != want {
			t.Errorf("ряд %q: точек %d, want %d", name, perName[name], want)
		}
	}
	if !first.Equal(time.Date(2012, 2, 1, 0, 0, 0, 0, time.UTC)) || !last.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("крайние даты разбора %s..%s, want 2012-02-01..2026-10-01",
			first.Format("2006-01-02"), last.Format("2006-01-02"))
	}
}

// TestSorsFundsSpotValues сверяет точки с живым файлом: итог, валютные разрезы, ноль в валютном
// эскроу (значение, а не пропуск) и первую точку ряда, который начинается позже остальных.
func TestSorsFundsSpotValues(t *testing.T) {
	rows := cbrFixtureRows(t, sorsFundsFixture, sorsFundsImport)
	cbrCheckSpotFloat64(t, rows, "clients_total", time.Date(2012, 2, 1, 0, 0, 0, 0, time.UTC), 25865164)
	cbrCheckSpotFloat64(t, rows, "clients_total", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), 154200014)
	cbrCheckSpotFloat64(t, rows, "corp_deposits_fx", time.Date(2012, 2, 1, 0, 0, 0, 0, time.UTC), 1723058)
	cbrCheckSpotFloat64(t, rows, "org_accounts_rub", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), 21990559)
	cbrCheckSpotFloat64(t, rows, "households_deposits", time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC), 27855014)
	cbrCheckSpotFloat64(t, rows, "escrow_accounts_fx", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), 0)
}

// sorsFundsTestBook — синтетическая книга с разметкой источника: три листа, шапка с 12 месячными
// датами и семь подписей. Синтетика допустима только для веток ошибок (стандарт §11.1: разбор
// проверяется на живой фикстуре, а fail-fast — на управляемой книге).
func sorsFundsTestBook(t *testing.T) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	for i, sheet := range cbrSorsFundsSheets {
		if i == 0 {
			if err := f.SetSheetName("Sheet1", sheet.Name); err != nil {
				t.Fatalf("SetSheetName: %v", err)
			}
		} else if _, err := f.NewSheet(sheet.Name); err != nil {
			t.Fatalf("NewSheet %q: %v", sheet.Name, err)
		}
		for j := 0; j < 12; j++ {
			cell, err := excelize.CoordinatesToCellName(j+2, 2)
			if err != nil {
				t.Fatalf("CoordinatesToCellName: %v", err)
			}
			month := time.Month(int(time.January) + j)
			if err := f.SetCellValue(sheet.Name, cell, time.Date(2020, month, 1, 0, 0, 0, 0, time.UTC).Format(cbrSorsFundsDateLayout)); err != nil {
				t.Fatalf("SetCellValue %s: %v", cell, err)
			}
		}
		for k, series := range cbrSorsFundsSeries {
			row := k + 3
			cell, err := excelize.CoordinatesToCellName(1, row)
			if err != nil {
				t.Fatalf("CoordinatesToCellName: %v", err)
			}
			if err := f.SetCellValue(sheet.Name, cell, series.Label); err != nil {
				t.Fatalf("SetCellValue %s: %v", cell, err)
			}
			for j := 0; j < 12; j++ {
				cell, err := excelize.CoordinatesToCellName(j+2, row)
				if err != nil {
					t.Fatalf("CoordinatesToCellName: %v", err)
				}
				if err := f.SetCellValue(sheet.Name, cell, float64(1000+j)); err != nil {
					t.Fatalf("SetCellValue %s: %v", cell, err)
				}
			}
		}
	}
	return f
}

// TestSorsFundsEmptySheetFails: нет листа, пустой лист, нет строки дат, пропала подпись строки,
// нечисловая ячейка и ноль точек — ошибка, а не «0 rows» (§6.4): иначе молчаливый прогон
// оставил бы ряд без обновления, а витрина — без объяснения.
func TestSorsFundsEmptySheetFails(t *testing.T) {
	for name, build := range map[string]func(t *testing.T) *excelize.File{
		"нет листа «итого»": func(t *testing.T) *excelize.File {
			f := excelize.NewFile()
			t.Cleanup(func() { _ = f.Close() })
			return f
		},
		"лист «итого» пуст": func(t *testing.T) *excelize.File {
			f := excelize.NewFile()
			t.Cleanup(func() { _ = f.Close() })
			if err := f.SetSheetName("Sheet1", cbrSorsFundsSheets[0].Name); err != nil {
				t.Fatalf("SetSheetName: %v", err)
			}
			return f
		},
		"нет строки дат": func(t *testing.T) *excelize.File {
			f := sorsFundsTestBook(t)
			for _, sheet := range cbrSorsFundsSheets {
				for j := 0; j < 12; j++ {
					cell, err := excelize.CoordinatesToCellName(j+2, 2)
					if err != nil {
						t.Fatalf("CoordinatesToCellName: %v", err)
					}
					if err := f.SetCellValue(sheet.Name, cell, ""); err != nil {
						t.Fatalf("SetCellValue %s: %v", cell, err)
					}
				}
			}
			return f
		},
		"пропала подпись строки": func(t *testing.T) *excelize.File {
			f := sorsFundsTestBook(t)
			if err := f.SetCellValue(cbrSorsFundsSheets[0].Name, "A3", "другой показатель"); err != nil {
				t.Fatalf("SetCellValue: %v", err)
			}
			return f
		},
		"нечисловая ячейка": func(t *testing.T) *excelize.File {
			f := sorsFundsTestBook(t)
			if err := f.SetCellValue(cbrSorsFundsSheets[0].Name, "B3", "н/д"); err != nil {
				t.Fatalf("SetCellValue: %v", err)
			}
			return f
		},
		"ноль точек": func(t *testing.T) *excelize.File {
			f := sorsFundsTestBook(t)
			for _, sheet := range cbrSorsFundsSheets {
				for k := range cbrSorsFundsSeries {
					for j := 0; j < 12; j++ {
						cell, err := excelize.CoordinatesToCellName(j+2, k+3)
						if err != nil {
							t.Fatalf("CoordinatesToCellName: %v", err)
						}
						if err := f.SetCellValue(sheet.Name, cell, "-"); err != nil {
							t.Fatalf("SetCellValue %s: %v", cell, err)
						}
					}
				}
			}
			return f
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			if err := sorsFundsImport(build(t), rec); err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if len(rec.rows) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.rows))
			}
		})
	}
}

// TestSorsFundsSheetNameInError: ошибка пустого листа называет таблицу и лист — по логу дежурный
// видит, чей ряд остался без обновления (стандарт §6.4).
func TestSorsFundsSheetNameInError(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	err := sorsFundsImport(f, newCbrRecorder())
	if err == nil {
		t.Fatal("ожидалась ошибка об отсутствующем листе")
	}
	if want := fmt.Sprintf("%s: лист «%s»", cbrSorsFundsTable, cbrSorsFundsSheets[0].Name); !strings.Contains(err.Error(), want) {
		t.Errorf("ошибка %q не называет таблицу и лист %q", err, want)
	}
}
