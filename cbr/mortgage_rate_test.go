package cbr

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// mortgageRateFixture — живой файл источника целиком: книга ~166 КБ (166 874 байт, 4 листа)
// коммитится как есть — стандарт §11.2, zip-хирургия нужна только крупным книгам. Разбор читает
// из неё единственный лист «ставка в рублях».
const mortgageRateFixture = "testdata/rates_housing.xlsx"

// mortgageRateMonths — месяцы листа «ставка в рублях»: с января 2019 по сентябрь 2026 (93
// месяца). У ЦБ в строке 3 стоит месяц отчётного периода (колонками), а разбор берёт
// предпоследний день этого месяца (time.Date(year, month+1, -1)), поэтому месяцы сверяются с
// правилом даты, а не с захардкоженным списком дней.
var mortgageRateMonths = func() []time.Time {
	start := time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	var months []time.Time
	for m := start; !m.After(end); m = m.AddDate(0, 1, 0) {
		months = append(months, m)
	}
	return months
}()

// mortgageRateTotalPoints — полный снимок count() разбора живой фикстуры (спека §2.2): у ряда
// РФ пропусков нет, 93 месяца — 93 точки.
const mortgageRateTotalPoints = 93

// mortgageRatePointDate — дата точки по правилу разбора: предпоследний день месяца отчётного
// периода (та же конвенция, что у cbr_int_rate_loans, cbr_int_rate_loans_ind и
// cbr_bank_int_rate). Для января 2019 это 2019-01-30, для сентября 2026 (30 дней) — 2026-09-29.
func mortgageRatePointDate(month time.Time) time.Time {
	return time.Date(month.Year(), month.Month()+1, -1, 0, 0, 0, 0, time.UTC)
}

// mortgageRateSheet — шапка листа «ставка в рублях» с дословной разметкой живой книги: пустая
// строка 1, заголовок книги (в колонке A), строка меток месяцев (A пуста, B.. — «Январь 2019»,
// «Февраль 2019») и строка объёма «РОССИЙСКАЯ ФЕДЕРАЦИЯ». Живая книга продолжается округами и
// субъектами, но разбор читает только эту строку; синтетическая шапка короче живой по числу
// месяцев — этого достаточно, чтобы проверить якоря и ветки fail-fast.
var mortgageRateSheet = [][]string{
	{},
	{"Средневзвешенная ставка по жилищным кредитам, предоставленным физическим лицам-резидентам в рублях, %"},
	{"", "Январь 2019", "Февраль 2019"},
	{"РОССИЙСКАЯ ФЕДЕРАЦИЯ", "9.89", "10.16"},
}

// mortgageRateSheetCopy — копия шапки: мутации в тестах ветвей fail-fast не должны менять шапку
// для соседних проверок.
func mortgageRateSheetCopy() [][]string {
	rows := make([][]string, 0, len(mortgageRateSheet))
	for _, row := range mortgageRateSheet {
		rows = append(rows, append([]string(nil), row...))
	}
	return rows
}

// mortgageRateBook собирает книгу с листом «ставка в рублях» из переданных строк.
func mortgageRateBook(t *testing.T, rows [][]string) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", cbrMortgageRateSheet); err != nil {
		t.Fatalf("SetSheetName: %v", err)
	}
	for i, row := range rows {
		cell, err := excelize.CoordinatesToCellName(1, i+1)
		if err != nil {
			t.Fatalf("CoordinatesToCellName(1, %d): %v", i+1, err)
		}
		if err = f.SetSheetRow(cbrMortgageRateSheet, cell, &row); err != nil {
			t.Fatalf("SetSheetRow(%s): %v", cell, err)
		}
	}
	return f
}

// TestCbrMortgageRateSeriesName — имя ряда совпадает со списком спеки §3.1 дословно: имя
// объявляет код (стандарт §6.8), и литерал здесь — единственное место, где расхождение со
// спекой (опечатка в константе) видит тест, а не глаз ревьюера.
func TestCbrMortgageRateSeriesName(t *testing.T) {
	const want = "Средневзвешенная ставка по жилищным кредитам, РФ"
	if cbrMortgageRateSeries != want {
		t.Errorf("имя ряда = %q, want %q", cbrMortgageRateSeries, want)
	}
}

// TestCbrMortgageRateImporterName: Name() — контракт фильтра `make import STAT=…` и id шага
// dagu/cbr.yaml; сверяется литералом, а не таблицей кода (стандарт §8.5).
func TestCbrMortgageRateImporterName(t *testing.T) {
	if got := cbrMortgageRateImporter.Name(); got != "cbr_mortgage_rate" {
		t.Errorf("Name() = %q, want %q", got, "cbr_mortgage_rate")
	}
}

// TestCbrMortgageRateDdl: DDL — контракт данных (правило 5 AGENTS.md: ценовые значения Float64;
// движок и ключ — общие для таблиц пакета). Проверяется литералами: DDL в %s-форме, имя таблицы
// в него подставляет шаблон, и потеря %s оставила бы запрос без имени таблицы.
func TestCbrMortgageRateDdl(t *testing.T) {
	if len(cbrMortgageRate.CreateTable) != 1 {
		t.Fatalf("DDL-инструкций %d, want 1", len(cbrMortgageRate.CreateTable))
	}
	ddl := fmt.Sprintf(cbrMortgageRate.CreateTable[0], "cbr_mortgage_rate")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS cbr_mortgage_rate",
		"name LowCardinality(String)",
		"date Date",
		"rate Float64",
		"ENGINE = ReplacingMergeTree ORDER BY (name, date)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL %q не содержит %q", ddl, want)
		}
	}
	if strings.Contains(ddl, "Float32") {
		t.Errorf("DDL %q объявляет Float32: правило 5 AGENTS.md требует Float64 для ценовых значений", ddl)
	}
}

// TestCbrMortgageRateSeriesMetaCoversParsedRows: каталог cbr_mortgage_rate (один ряд) и разбор
// фикстуры совпадают в обе стороны (стандарт §11.4): имя ряда объявлено кодом, поэтому проверка
// двусторонняя — и забытое имя в каталоге, и забытая строка в разборе видны одинаково.
func TestCbrMortgageRateSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrMortgageRateSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(mortgageRateFixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return cbrMortgageRate.ImportFunc(f, rec)
	})
}

// TestCbrMortgageRateFixtureRows: разбор живой фикстуры даёт полный снимок 93 точек одного ряда,
// а даты совпадают с правилом «предпоследний день месяца отчётного периода». Последняя точка —
// 2026-09-29, а не 30-е: в сентябре 30 дней, и предпоследний день — 29-е (для 31-дневного
// января 2019 — 30-е). Первый и последний месяцы проверяются литералами, чтобы сдвиг правила
// даты на день был виден тесту, а не только счётчику.
func TestCbrMortgageRateFixtureRows(t *testing.T) {
	if len(mortgageRateMonths) != mortgageRateTotalPoints {
		t.Fatalf("месяцев в снимке %d, want %d", len(mortgageRateMonths), mortgageRateTotalPoints)
	}
	rows := cbrFixtureRows(t, mortgageRateFixture, cbrMortgageRate.ImportFunc)
	if len(rows) != mortgageRateTotalPoints {
		t.Errorf("строк разбора %d, want %d (снимок 2026-11-03)", len(rows), mortgageRateTotalPoints)
	}
	for i, row := range rows {
		if row.name != cbrMortgageRateSeries {
			t.Errorf("строка %d: ряд %q, want %q", i, row.name, cbrMortgageRateSeries)
			continue
		}
		if i >= len(mortgageRateMonths) {
			continue
		}
		if want := mortgageRatePointDate(mortgageRateMonths[i]); !row.date.Equal(want) {
			t.Errorf("строка %d: дата %s, want %s (предпоследний день месяца)",
				i, row.date.Format("2006-01-02"), want.Format("2006-01-02"))
		}
	}
	if len(rows) > 0 {
		if got := rows[0].date.Format("2006-01-02"); got != "2019-01-30" {
			t.Errorf("первая точка %s, want 2019-01-30 (предпоследний день января)", got)
		}
		if got := rows[len(rows)-1].date.Format("2006-01-02"); got != "2026-09-29" {
			t.Errorf("последняя точка %s, want 2026-09-29 (предпоследний день сентября)", got)
		}
	}
}

// TestCbrMortgageRateSpotValues: значения из известных ячеек живой фикстуры — первый месяц и три
// последних. Значения литералами: сверка идёт с разбором, а не с константой кода. Таблица —
// Float64, поэтому сравнение точное (cbrCheckSpotFloat64), без приведения к Float32.
func TestCbrMortgageRateSpotValues(t *testing.T) {
	rows := cbrFixtureRows(t, mortgageRateFixture, cbrMortgageRate.ImportFunc)
	layout := "2006-01-02"
	for _, tc := range []struct {
		date string
		want float64
	}{
		{"2019-01-30", 9.89},
		{"2026-07-30", 10.53},
		{"2026-08-30", 10.09},
		{"2026-09-29", 8.61},
	} {
		date, err := time.Parse(layout, tc.date)
		if err != nil {
			t.Fatalf("дата спота %q: %v", tc.date, err)
		}
		cbrCheckSpotFloat64(t, rows, cbrMortgageRateSeries, date, tc.want)
	}
}

// TestCbrMortgageRateAppendsFloat64: разбор пишет ставку как float64 — тип колонки rate в DDL
// (правило 5 AGENTS.md). Запись float32 прошла бы в колонку Float64 молча, потеряв точность на
// шаге представления (8.61 в float32 — 8.609999656677246), и расхождение заметила бы только
// живая БД. Прецедент — cbr/int_rate_loans_ind_test.go.
func TestCbrMortgageRateAppendsFloat64(t *testing.T) {
	f, err := excelize.OpenFile(mortgageRateFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", mortgageRateFixture, err)
	}
	defer func() { _ = f.Close() }()
	rec := newCbrRecorder()
	if err = cbrMortgageRate.ImportFunc(f, rec); err != nil {
		t.Fatalf("разбор фикстуры: %v", err)
	}
	if rec.err != nil {
		t.Fatalf("разбор фикстуры: схема строк не совпала с разбором: %v", rec.err)
	}
	if len(rec.raw) == 0 {
		t.Fatal("разбор не дал ни одной точки")
	}
	for i, raw := range rec.raw {
		if len(raw) != 3 {
			t.Fatalf("строка %d: %d значений, ожидалась форма (name, date, rate)", i, len(raw))
		}
		if _, ok := raw[2].(float64); !ok {
			t.Fatalf("строка %d: ставка записана как %T, ожидался float64 (колонка rate — Float64)", i, raw[2])
		}
	}
}

// TestCbrMortgageRateMissingSheetFails: книга без листа «ставка в рублях» — ошибка с именем
// таблицы, а не «0 rows» и не паника.
func TestCbrMortgageRateMissingSheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	rec := newCbrRecorder()
	err := cbrMortgageRate.ImportFunc(f, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), cbrMortgageRateTable) {
		t.Errorf("ошибка %q не называет таблицу %s", err, cbrMortgageRateTable)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}

// mortgageRateOpenFixture — копия живой фикстуры, загруженная в память под мутацию: пропуск
// ячеек проверяется на ней, а не на книге, собранной с нуля (стандарт §11.1). Правки идут по
// копии в памяти — файл на диске не меняется.
func mortgageRateOpenFixture(t *testing.T, mutate func(t *testing.T, f *excelize.File)) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(mortgageRateFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", mortgageRateFixture, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	mutate(t, f)
	return f
}

// mortgageRateSetCell — правка ячейки: excelize сообщает о негодном адресе ошибкой, и проглотить
// её значило бы проверять немутнутый лист.
func mortgageRateSetCell(t *testing.T, f *excelize.File, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(cbrMortgageRateSheet, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s): %v", cell, err)
	}
}

// mortgageRateCell — адрес ячейки листа (колонка и строка, 1-based): строки и колонки мутаций
// выводятся из той же разметки, что и разбор (строка 4 — «РОССИЙСКАЯ ФЕДЕРАЦИЯ», колонка 2 —
// «Январь 2019»), а не читаются строкой «B4» в коде.
func mortgageRateCell(t *testing.T, col, row int) string {
	t.Helper()
	cell, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		t.Fatalf("CoordinatesToCellName(%d, %d): %v", col, row, err)
	}
	return cell
}

// mortgageRateHasPoint — есть ли в разборе точка (ряд, дата).
func mortgageRateHasPoint(rows []appendedRow, name string, date time.Time) bool {
	for _, row := range rows {
		if row.name == name && row.date.Equal(date) {
			return true
		}
	}
	return false
}

// TestCbrMortgageRateFailFastBranches: ветки, где разбор обязан вернуть ошибку, а не «0 строк»
// (стандарт §6.4): нет строки «РОССИЙСКАЯ ФЕДЕРАЦИЯ», нет строки меток месяцев, метка не месяц,
// значение не число или не конечно (NaN ParseFloat принимает молча), ноль точек. Книги
// синтетические (живых фикстур этих состояний нет), поэтому первым подтестом идёт контроль:
// правильная синтетическая книга разбирается — значит, ошибки ниже приходят от мутации, а не от
// сборки книги. Мутации выбраны в первых колонках шапки и данных: батч уходит в БД только по
// Send, поэтому при ошибке разбора в рекордере обязано быть 0 строк.
func TestCbrMortgageRateFailFastBranches(t *testing.T) {
	t.Run("правильная шапка разбирается", func(t *testing.T) {
		rows := mortgageRateSheetCopy()
		// Округ ниже строки РФ: разбор обязан прочитать только РФ, иначе точек было бы 4.
		rows = append(rows, []string{"МОСКВА", "99", "99"})
		rec := newCbrRecorder()
		if err := cbrMortgageRate.ImportFunc(mortgageRateBook(t, rows), rec); err != nil {
			t.Fatalf("разбор контрольной книги: %v", err)
		}
		if len(rec.rows) != 2 {
			t.Errorf("точек %d, want 2 (два месяца строки РФ; строки округов не читаются)", len(rec.rows))
		}
		for i, date := range []time.Time{
			time.Date(2019, time.January, 30, 0, 0, 0, 0, time.UTC),
			time.Date(2019, time.February, 27, 0, 0, 0, 0, time.UTC),
		} {
			if i >= len(rec.rows) {
				break
			}
			if rec.rows[i].name != cbrMortgageRateSeries || !rec.rows[i].date.Equal(date) {
				t.Errorf("точка %d = (%q, %s), want (%q, %s)",
					i, rec.rows[i].name, rec.rows[i].date.Format("2006-01-02"), cbrMortgageRateSeries, date.Format("2006-01-02"))
			}
		}
	})
	for name, mutate := range map[string]func(rows [][]string){
		"нет строки «РОССИЙСКАЯ ФЕДЕРАЦИЯ»": func(rows [][]string) { rows[3][0] = "МОСКВА" },
		"нет строки меток месяцев":          func(rows [][]string) { rows[2] = []string{""} },
		"метка месяца не разобрана":         func(rows [][]string) { rows[2][1] = "January 2019" },
		"значение не число":                 func(rows [][]string) { rows[3][1] = "н/д" },
		"значение NaN":                      func(rows [][]string) { rows[3][1] = "NaN" },
		"нулевые значения дают 0 точек":     func(rows [][]string) { rows[3][1], rows[3][2] = "0", "0" },
		"пустые значения дают 0 точек":      func(rows [][]string) { rows[3][1], rows[3][2] = "", "" },
	} {
		t.Run(name, func(t *testing.T) {
			rows := mortgageRateSheetCopy()
			mutate(rows)
			rec := newCbrRecorder()
			err := cbrMortgageRate.ImportFunc(mortgageRateBook(t, rows), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), cbrMortgageRateTable) {
				t.Errorf("ошибка %q не называет таблицу %s", err, cbrMortgageRateTable)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
			}
		})
	}
}

// TestCbrMortgageRateSkipsNonPointsOnFixture: ячейка, которая точкой не является, точек не даёт
// (стандарт §6.7): пустая, литеральный ноль (в источнике — «данных не имеется»), литеральный «-»
// и ячейка за концом короткой строки. Проверка на живой фикстуре: точек становится ровно на
// пропущенное меньше, а соседние месяцы остаются на месте.
func TestCbrMortgageRateSkipsNonPointsOnFixture(t *testing.T) {
	total := mortgageRateTotalPoints
	// Первая строка данных листа — «РОССИЙСКАЯ ФЕДЕРАЦИЯ»: шапка живой книги занимает те же
	// четыре строки, что mortgageRateSheet (пустая строка, заголовок, метки месяцев, строка РФ).
	firstDataRow := len(mortgageRateSheet)
	// Даты соседних месяцев по правилу разбора: предпоследний день января (30-е) и февраля
	// (27-е, 2019 год не високосный).
	jan := time.Date(2019, time.January, 30, 0, 0, 0, 0, time.UTC)
	feb := time.Date(2019, time.February, 27, 0, 0, 0, 0, time.UTC)
	last := time.Date(2026, time.September, 29, 0, 0, 0, 0, time.UTC)
	prevLast := time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		mutate   func(t *testing.T, f *excelize.File)
		wantRows int
		absent   time.Time
		present  time.Time
	}{
		"пустая ячейка": {
			// Колонка 2 — «Январь 2019», строка — «РОССИЙСКАЯ ФЕДЕРАЦИЯ».
			mutate: func(t *testing.T, f *excelize.File) {
				mortgageRateSetCell(t, f, mortgageRateCell(t, 2, firstDataRow), "")
			},
			wantRows: total - 1,
			absent:   jan,
			present:  feb,
		},
		"литеральный ноль": {
			mutate: func(t *testing.T, f *excelize.File) {
				mortgageRateSetCell(t, f, mortgageRateCell(t, 2, firstDataRow), 0)
			},
			wantRows: total - 1,
			absent:   jan,
			present:  feb,
		},
		"литеральный дефис": {
			mutate: func(t *testing.T, f *excelize.File) {
				mortgageRateSetCell(t, f, mortgageRateCell(t, 2, firstDataRow), "-")
			},
			wantRows: total - 1,
			absent:   jan,
			present:  feb,
		},
		"ячейка за концом короткой строки": {
			// Опустошена последняя колонка строки РФ (сентябрь 2026): строка становится короче
			// строки меток, и её последний месяц точек не даёт.
			mutate: func(t *testing.T, f *excelize.File) {
				mortgageRateSetCell(t, f, mortgageRateCell(t, 1+len(mortgageRateMonths), firstDataRow), "")
			},
			wantRows: total - 1,
			absent:   last,
			present:  prevLast,
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			if err := cbrMortgageRate.ImportFunc(mortgageRateOpenFixture(t, tc.mutate), rec); err != nil {
				t.Fatalf("разбор мутированной фикстуры: %v", err)
			}
			if rec.err != nil {
				t.Fatalf("схема строк не совпала с разбором: %v", rec.err)
			}
			if len(rec.rows) != tc.wantRows {
				t.Errorf("точек %d, want %d: пропущенная ячейка не даёт точки и не теряет соседей",
					len(rec.rows), tc.wantRows)
			}
			if mortgageRateHasPoint(rec.rows, cbrMortgageRateSeries, tc.absent) {
				t.Errorf("точка на %s записана, хотя ячейка пропущена", tc.absent.Format("2006-01-02"))
			}
			if !mortgageRateHasPoint(rec.rows, cbrMortgageRateSeries, tc.present) {
				t.Errorf("пропала точка на %s — пропуск одной ячейки соседний месяц не трогает",
					tc.present.Format("2006-01-02"))
			}
		})
	}
}

// TestCbrMortgageRateAppendErrorNamesRow: ошибка записи в батч не отдаётся голой — в ней есть
// таблица, строка источника, имя ряда и дата точки: по одной ошибке видно, какая точка не ушла.
// Схема приёмника из одной колонки заставляет его отвергнуть каждую строку (name, date, rate) —
// то же, что делает настоящий батч на негодной строке.
func TestCbrMortgageRateAppendErrorNamesRow(t *testing.T) {
	rec := newCbrRecorder("name")
	err := cbrMortgageRate.ImportFunc(mortgageRateOpenFixture(t, func(*testing.T, *excelize.File) {}), rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка записи, разбор дал %d строк", len(rec.rows))
	}
	for _, want := range []string{
		cbrMortgageRateTable,
		cbrMortgageRateField,
		cbrMortgageRateSeries,
		"2019-01-30",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
}

// TestCbrMortgageRateSeriesMetaOrigin: каталог одного ряда называет файл, лист и строку источника
// раздельно (правило 11 AGENTS.md): агент MCP обязан видеть, откуда взята ставка, и что это
// строка «РОССИЙСКАЯ ФЕДЕРАЦИЯ», а не регион. Формат сверяется подстроками, а не равенством:
// текст origin может уточняться.
func TestCbrMortgageRateSeriesMetaOrigin(t *testing.T) {
	if len(cbrMortgageRateSeriesMeta) != 1 {
		t.Fatalf("записей каталога %d, want 1", len(cbrMortgageRateSeriesMeta))
	}
	meta := cbrMortgageRateSeriesMeta[0]
	if meta.Series != cbrMortgageRateSeries || meta.Title != cbrMortgageRateSeries {
		t.Errorf("каталог описывает ряд %q (title %q), want %q", meta.Series, meta.Title, cbrMortgageRateSeries)
	}
	for _, want := range []string{
		"BankSector/Mortgage/02_08_Rates_housing.xlsx",
		"лист «" + cbrMortgageRateSheet + "»",
		"строка «" + cbrMortgageRateField + "»",
	} {
		if !strings.Contains(meta.Origin, want) {
			t.Errorf("origin %q не содержит %q", meta.Origin, want)
		}
	}
	if meta.Unit != "% годовых" {
		t.Errorf("unit = %q, want %q", meta.Unit, "% годовых")
	}
	if meta.Frequency != "M" {
		t.Errorf("frequency = %q, want M", meta.Frequency)
	}
	for _, want := range []string{"предпоследний день месяца", "toStartOfMonth(date)", "§6.7", "Средневзвешенная ставка"} {
		if !strings.Contains(meta.Description, want) {
			t.Errorf("description не содержит %q: агент не прочитает привязку даты или пропуск нуля", want)
		}
	}
}
