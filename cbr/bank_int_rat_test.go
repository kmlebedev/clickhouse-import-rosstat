package cbr

import (
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// bankIntRateFixture — выгрузка «Сведения о средних арифметических процентных ставках по
// вкладам физических лиц» (URL из cbrBankIntRate.DataUrl). В книге три листа («в рублях»,
// «в долларах США», «в евро»); фикстура урезана до листа «в рублях», который читает
// ImportFunc, — остальные листы выкинуты zip-хирургией (прецедент Task 6).
const bankIntRateFixture = "testdata/regulatory_rates.xlsx"

// bankIntRateMonths — месяцы листа «в рублях»: строки 4..62 файла, с октября 2021 по
// август 2026. У ЦБ в таблице стоит месяц отчётного периода, а ImportFunc берёт
// time.Date(year, month+1, -1) — предпоследний день этого месяца; поэтому месяцы разбора
// сверяются с правилом даты, а не с захардкоженным списком дней.
var bankIntRateMonths = func() []time.Time {
	start := time.Date(2021, time.October, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	var months []time.Time
	for m := start; !m.After(end); m = m.AddDate(0, 1, 0) {
		months = append(months, m)
	}
	return months
}()

// TestCbrBankIntRateSeriesMetaCoversParsedRows: каталог cbr_bank_int_rate (7 рядов по сроку
// вклада) и разбор фикстуры листа «в рублях» совпадают в обе стороны: имена рядов берутся
// из строки заголовка файла (rows[2]), поэтому любое переименование у ЦБ валит тест.
func TestCbrBankIntRateSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrBankIntRateSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(bankIntRateFixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return cbrBankIntRate.ImportFunc(f, rec)
	})
}

// TestCbrBankIntRateFixtureRows: разбор фикстуры даёт по 59 точек на каждый из 7 рядов —
// каждая колонка каждого из 59 месяцев (октябрь 2021 .. август 2026) заполнена. Как в
// baseline T1 (n = 59, d0 = 2021-10-30, d1 = 2026-08-30): базовая линия и свежая фикстура
// сходятся и по числу точек, и по крайним датам.
func TestCbrBankIntRateFixtureRows(t *testing.T) {
	rows := cbrFixtureRows(t, bankIntRateFixture, cbrBankIntRate.ImportFunc)
	if len(rows) != len(cbrBankIntRateNames)*len(bankIntRateMonths) {
		t.Errorf("строк разбора %d, want %d (7 сроков × %d месяцев)",
			len(rows), len(cbrBankIntRateNames)*len(bankIntRateMonths), len(bankIntRateMonths))
	}
	dates := map[string][]time.Time{}
	for _, row := range rows {
		dates[row.name] = append(dates[row.name], row.date)
	}
	// Дата разбора — предпоследний день месяца отчётного периода: у ЦБ в таблице стоит
	// месяц, а не день, и ImportFunc берёт day = -1 от первого числа следующего месяца.
	want := make([]time.Time, 0, len(bankIntRateMonths))
	for _, month := range bankIntRateMonths {
		want = append(want, time.Date(month.Year(), month.Month()+1, -1, 0, 0, 0, 0, time.UTC))
	}
	for _, name := range cbrBankIntRateNames {
		got := dates[name]
		if len(got) != len(want) {
			t.Errorf("ряд %q: точек %d, want %d", name, len(got), len(want))
			continue
		}
		for i := range want {
			if !got[i].Equal(want[i]) {
				t.Errorf("ряд %q: точка %d = %s, want %s (предпоследний день месяца)",
					name, i, got[i].Format("2006-01-02"), want[i].Format("2006-01-02"))
				break
			}
		}
	}
}

// TestCbrBankIntRateSpotValues: последние значения baseline T1 (2026-08-30) совпадают с
// разбором фикстуры в точности Float32 — по значению на каждый срок вклада.
func TestCbrBankIntRateSpotValues(t *testing.T) {
	rows := cbrFixtureRows(t, bankIntRateFixture, cbrBankIntRate.ImportFunc)
	date := time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	cbrCheckSpot(t, rows, "\"до востребования\"", date, 4.820000171661377)
	cbrCheckSpot(t, rows, "до 30 дней, кроме \"до востребования\"", date, 11.760000228881836)
	cbrCheckSpot(t, rows, "от 31 до 90 дней", date, 12.420000076293945)
	cbrCheckSpot(t, rows, "от 91 до 180 дней", date, 12.680000305175781)
	cbrCheckSpot(t, rows, "от 181 дня до 1 года", date, 12.170000076293945)
	cbrCheckSpot(t, rows, "от 1 года до 3 лет", date, 10.899999618530273)
	cbrCheckSpot(t, rows, "свыше 3 лет", date, 8.869999885559082)
}

// TestCbrBankIntRateMissingSheetFails: книга без листа «в рублях» (единственный, который
// читает ImportFunc) — ошибка, а не «0 rows».
func TestCbrBankIntRateMissingSheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	rec := newCbrRecorder()
	if err := cbrBankIntRate.ImportFunc(f, rec); err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}

// TestCbrBankIntRateEmptySheetFails: лист «в рублях» есть, но строк данных нет — ошибка, а
// не «0 rows» с зелёным dagu: разбор не нашёл ни одной пары (месяц, ставка), и семь рядов
// остались бы без обновления при успешном шаге DAG. Имя таблицы в тексте ошибки сверяется
// литералом: оно — часть контракта для дежурного по логу.
func TestCbrBankIntRateEmptySheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "в рублях"); err != nil {
		t.Fatalf("SetSheetName: %v", err)
	}
	rec := newCbrRecorder()
	err := cbrBankIntRate.ImportFunc(f, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), "cbr_bank_int_rate") {
		t.Errorf("ошибка %q не называет таблицу cbr_bank_int_rate", err)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}
