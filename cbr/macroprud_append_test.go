package cbr

import (
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// TestMacroprudentialAppendShape — ImportFunc новых импортёров пишет строки
// ровно той длины и типов, что объявлены DDL (стандарт §11.6). Проверка ловит
// перестановку/потерю колонки, которую парсерные тесты не видят.
func TestMacroprudentialAppendShape(t *testing.T) {
	t.Run("ccyb", func(t *testing.T) {
		xlsx := openCbrFixture(t, "testdata/ccyb_indicators_time_series.xlsx")
		rec := newCbrShapeRecorder("series", "indicator_no", "indicator_group", "date", "value", "version")
		if err := cbrCcybImportFunc(1)(xlsx, rec); err != nil {
			t.Fatalf("ImportFunc: %v", err)
		}
		assertAppendLen(t, rec, 6)
		row := rec.raw[0]
		assertType(t, row[0], "")
		assertType(t, row[1], uint8(0))
		assertType(t, row[2], "")
		assertType(t, row[3], time.Time{})
		assertType(t, row[4], float64(0))
		assertType(t, row[5], uint64(0))
	})

	t.Run("risk_weight_addons", func(t *testing.T) {
		xlsx := openCbrFixture(t, "testdata/risk_weight_addons_table1_20260904.xlsx")
		file := riskAddonFile{Name: "table1_20260904.xlsx", Caption: "Таблица 1 с 04.09.2026"}
		rec := newCbrShapeRecorder("matrix", "valid_from", "valid_to", "table_no",
			"row_code", "col_code", "value", "is_applicable", "source_file",
			"source_sheet", "source_caption", "version")
		if err := cbrRiskWeightAddonsImportFunc(file, 1)(xlsx, rec); err != nil {
			t.Fatalf("ImportFunc: %v", err)
		}
		assertAppendLen(t, rec, 12)
		row := rec.raw[0]
		assertType(t, row[0], "")
		assertType(t, row[1], time.Time{})
		assertType(t, row[2], time.Time{})
		assertType(t, row[6], float64(0))
		assertType(t, row[7], uint8(0))
		assertType(t, row[11], uint64(0))
	})

	t.Run("mpl", func(t *testing.T) {
		xlsx := openCbrFixture(t, "testdata/mpl_banks_2025.xlsx")
		file := mplFile{Name: "mpl_banks_2025.xlsx", Segment: "bank"}
		rec := newCbrShapeRecorder(
			"segment", "loan_class", "period_from", "period_to", "source_file", "source_sheet",
			"row_no", "subrow_no", "band_idx", "loan_kind",
			"pdn_band", "pdn_min", "pdn_max", "term_band", "term_min", "term_max",
			"down_payment_band", "down_payment_min", "down_payment_max",
			"ltv_band", "ltv_min", "ltv_max", "amount_band", "amount_min", "amount_max",
			"limit_pct", "version")
		if err := cbrMPLImportFunc(file, mplRanges{}, 1)(xlsx, rec); err != nil {
			t.Fatalf("ImportFunc: %v", err)
		}
		assertAppendLen(t, rec, 27)
		row := rec.raw[0]
		assertType(t, row[0], "")
		assertType(t, row[2], time.Time{})
		assertType(t, row[6], uint32(0))
		assertType(t, row[8], uint8(0))
		assertType(t, row[25], float64(0))
		assertType(t, row[26], uint64(0))
	})
}

func openCbrFixture(t *testing.T, path string) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

func assertAppendLen(t *testing.T, rec *appendRecorder, want int) {
	t.Helper()
	if rec.err != nil {
		t.Fatalf("Append: %v", rec.err)
	}
	if len(rec.raw) == 0 {
		t.Fatal("ImportFunc не записал ни одной строки")
	}
	for i, row := range rec.raw {
		if len(row) != want {
			t.Fatalf("строка %d: %d колонок, want %d", i, len(row), want)
		}
	}
}

func assertType(t *testing.T, v any, want any) {
	t.Helper()
	switch want.(type) {
	case string:
		if _, ok := v.(string); !ok {
			t.Fatalf("значение %v: тип %T, want string", v, v)
		}
	case uint8:
		if _, ok := v.(uint8); !ok {
			t.Fatalf("значение %v: тип %T, want uint8", v, v)
		}
	case uint32:
		if _, ok := v.(uint32); !ok {
			t.Fatalf("значение %v: тип %T, want uint32", v, v)
		}
	case uint64:
		if _, ok := v.(uint64); !ok {
			t.Fatalf("значение %v: тип %T, want uint64", v, v)
		}
	case float64:
		if _, ok := v.(float64); !ok {
			t.Fatalf("значение %v: тип %T, want float64", v, v)
		}
	case time.Time:
		if _, ok := v.(time.Time); !ok {
			t.Fatalf("значение %v: тип %T, want time.Time", v, v)
		}
	default:
		t.Fatalf("неизвестный ожидаемый тип %T", want)
	}
}
