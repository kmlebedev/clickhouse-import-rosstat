package cbr

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// intRateLoansFixture — живой файл источника целиком: книга ~124 КБ (124 288 байт, 8 листов)
// коммитится как есть — стандарт §11.2, zip-хирургия нужна только крупным книгам. Разбор
// читает из неё единственный лист «ставки_руб».
const intRateLoansFixture = "testdata/loans_nonfin.xlsx"

// intRateLoansWantNames — 16 имён рядов литералами по спеке §3.2, в порядке колонок листа:
// две группы по восемь сроков. Литералы, а не ссылка на cbrIntRateLoansNames: имена в коде
// собираются из префиксов групп и сроков, и сверка с литералами — единственное место, где
// расхождение со спекой (опечатка в префиксе группы, перестановка сроков) видит тест, а не глаз
// ревьюера. Порядок: сначала все сроки группы «Всего» (колонки B–I), затем группы «СМП» (J–Q).
var intRateLoansWantNames = []string{
	"Всего: до 30 дней, включая «до востребования»",
	"Всего: от 31 до 90 дней",
	"Всего: от 91 до 180 дней",
	"Всего: от 181 дня до 1 года",
	"Всего: до 1 года, включая «до востребования»",
	"Всего: от 1 года до 3 лет",
	"Всего: свыше 3 лет",
	"Всего: свыше 1 года",
	"СМП: до 30 дней, включая «до востребования»",
	"СМП: от 31 до 90 дней",
	"СМП: от 91 до 180 дней",
	"СМП: от 181 дня до 1 года",
	"СМП: до 1 года, включая «до востребования»",
	"СМП: от 1 года до 3 лет",
	"СМП: свыше 3 лет",
	"СМП: свыше 1 года",
}

// intRateLoansMonths — месяцы листа «ставки_руб»: строки 6..157 файла, с января 2014 по август
// 2026 (152 месяца). У ЦБ в колонке метки стоит месяц отчётного периода, а разбор берёт
// предпоследний день этого месяца (time.Date(year, month+1, -1)), поэтому месяцы сверяются с
// правилом даты, а не с захардкоженным списком дней (образец — bank_int_rat_test.go).
var intRateLoansMonths = func() []time.Time {
	start := time.Date(2014, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	var months []time.Time
	for m := start; !m.After(end); m = m.AddDate(0, 1, 0) {
		months = append(months, m)
	}
	return months
}()

// intRateLoansSheet — шапка листа «ставки_руб» с правильным текстом заголовков (кавычки «» и
// слово без переноса). Синтетика уместна только здесь: живой фикстуры со сломанной шапкой не
// существует, а ветка «источник изменил структуру листа» (стандарт §6.4) обязана быть
// проверена. Строки 1–3 — заголовок файла, строка 4 — подписи групп (merged-ячейки несут текст
// только в первой колонке группы), строка 5 — 16 заголовков сроков.
var intRateLoansSheet = [][]string{
	{"Средневзвешенные процентные ставки по кредитам, предоставленным кредитными организациями нефинансовым организациям в рублях*"},
	{"(в целом по Российской Федерации)"},
	{"% годовых"},
	{"", "Всего", "", "", "", "", "", "", "", "в том числе субъектам малого и среднего предпринимательства"},
	{
		"",
		"до 30 дней, включая «до востребования»", "от 31 до 90 дней", "от 91 до 180 дней", "от 181 дня до 1 года",
		"до 1 года, включая «до востребования»", "от 1 года до 3 лет", "свыше 3 лет", "свыше 1 года",
		"до 30 дней, включая «до востребования»", "от 31 до 90 дней", "от 91 до 180 дней", "от 181 дня до 1 года",
		"до 1 года, включая «до востребования»", "от 1 года до 3 лет", "свыше 3 лет", "свыше 1 года",
	},
}

// intRateLoansBook собирает книгу с листом «ставки_руб» из переданных строк.
func intRateLoansBook(t *testing.T, rows [][]string) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "ставки_руб"); err != nil {
		t.Fatalf("SetSheetName: %v", err)
	}
	for i, row := range rows {
		cell, err := excelize.CoordinatesToCellName(1, i+1)
		if err != nil {
			t.Fatalf("CoordinatesToCellName(1, %d): %v", i+1, err)
		}
		if err = f.SetSheetRow("ставки_руб", cell, &row); err != nil {
			t.Fatalf("SetSheetRow(%s): %v", cell, err)
		}
	}
	return f
}

// intRateLoansSheetWithData — шапка листа плюс одна строка данных «Январь 2014»: во всех 16
// колонках 7.35. Копия, а не ссылка на intRateLoansSheet: мутации в тестах ветвей fail-fast не
// должны менять шапку для соседних проверок.
func intRateLoansSheetWithData() [][]string {
	rows := make([][]string, 0, len(intRateLoansSheet)+1)
	for _, row := range intRateLoansSheet {
		rows = append(rows, append([]string(nil), row...))
	}
	data := make([]string, len(intRateLoansSheet[4]))
	data[0] = "Январь 2014"
	for i := 1; i < len(data); i++ {
		data[i] = "7.35"
	}
	return append(rows, data)
}

// TestCbrIntRateLoansNames — имена рядов кода совпадают со списком спеки §3.2 дословно: и
// префиксы групп («Всего: », «СМП: »), и тексты сроков, и их порядок.
func TestCbrIntRateLoansNames(t *testing.T) {
	if len(cbrIntRateLoansNames) != len(intRateLoansWantNames) {
		t.Fatalf("имён рядов %d, want %d", len(cbrIntRateLoansNames), len(intRateLoansWantNames))
	}
	for i, want := range intRateLoansWantNames {
		if cbrIntRateLoansNames[i] != want {
			t.Errorf("имя ряда %d = %q, want %q", i, cbrIntRateLoansNames[i], want)
		}
	}
}

// TestCbrIntRateLoansImporterName: Name() — контракт фильтра `make import STAT=…` и id шага
// dagu/cbr.yaml; сверяется литералом, а не таблицей кода (стандарт §8.5).
func TestCbrIntRateLoansImporterName(t *testing.T) {
	if got := cbrIntRateLoansImporter.Name(); got != "cbr_int_rate_loans" {
		t.Errorf("Name() = %q, want %q", got, "cbr_int_rate_loans")
	}
}

// TestCbrIntRateLoansDdl: DDL — контракт данных (правило 5 AGENTS.md: ценовые значения Float64;
// движок и ключ — общие для таблиц пакета). Проверяется литералами: DDL в %s-форме, имя таблицы
// в него подставляет шаблон, и потеря %s оставила бы запрос без имени таблицы.
func TestCbrIntRateLoansDdl(t *testing.T) {
	if len(cbrIntRateLoans.CreateTable) != 1 {
		t.Fatalf("DDL-инструкций %d, want 1", len(cbrIntRateLoans.CreateTable))
	}
	ddl := fmt.Sprintf(cbrIntRateLoans.CreateTable[0], "cbr_int_rate_loans")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS cbr_int_rate_loans",
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

// TestCbrIntRateLoansSeriesMetaCoversParsedRows: каталог cbr_int_rate_loans (16 рядов) и разбор
// фикстуры совпадают в обе стороны (стандарт §11.4): имена рядов объявлены кодом (группа ×
// срок), поэтому проверка двусторонняя — и забытое имя в каталоге, и забытая колонка в разборе
// видны одинаково.
func TestCbrIntRateLoansSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrIntRateLoansSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(intRateLoansFixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return cbrIntRateLoans.ImportFunc(f, rec)
	})
}

// TestCbrIntRateLoansFixtureRows: разбор живой фикстуры даёт по 152 точки на каждый из 16 рядов
// (16 × 152 = 2 432): каждая колонка каждого месяца с января 2014 по август 2026 заполнена, а
// строка-сноска «*…» после данных разбор не продолжает (иначе точек было бы больше). Даты
// сверяются с правилом «предпоследний день месяца отчётного периода», а не списком дней.
func TestCbrIntRateLoansFixtureRows(t *testing.T) {
	rows := cbrFixtureRows(t, intRateLoansFixture, cbrIntRateLoans.ImportFunc)
	wantTotal := len(cbrIntRateLoansNames) * len(intRateLoansMonths)
	if len(rows) != wantTotal {
		t.Errorf("строк разбора %d, want %d (16 рядов × %d месяцев)",
			len(rows), wantTotal, len(intRateLoansMonths))
	}
	dates := map[string][]time.Time{}
	for _, row := range rows {
		dates[row.name] = append(dates[row.name], row.date)
	}
	// Дата разбора — предпоследний день месяца отчётного периода: у ЦБ в таблице стоит месяц, а
	// не день, и разбор берёт day = -1 от первого числа следующего месяца.
	want := make([]time.Time, 0, len(intRateLoansMonths))
	for _, month := range intRateLoansMonths {
		want = append(want, time.Date(month.Year(), month.Month()+1, -1, 0, 0, 0, 0, time.UTC))
	}
	for _, name := range intRateLoansWantNames {
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

// TestCbrIntRateLoansSpotValues: три значения из известных ячеек живой фикстуры — первый и
// последний месяц самого длинного ряда и последний месяц ряда СМП. Значения литералами: сверка
// идёт с разбором, а не с константой кода.
func TestCbrIntRateLoansSpotValues(t *testing.T) {
	rows := cbrFixtureRows(t, intRateLoansFixture, cbrIntRateLoans.ImportFunc)
	first := time.Date(2014, time.January, 30, 0, 0, 0, 0, time.UTC)
	last := time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	cbrCheckSpot(t, rows, "Всего: до 30 дней, включая «до востребования»", first, 7.35)
	cbrCheckSpot(t, rows, "Всего: до 30 дней, включая «до востребования»", last, 19.06)
	cbrCheckSpot(t, rows, "СМП: свыше 1 года", last, 14.22)
}

// TestCbrIntRateLoansAppendsFloat64: разбор пишет ставку как float64 — тип колонки rate в DDL
// (правило 5 AGENTS.md). Запись float32 прошла бы в колонку Float64 молча, потеряв точность на
// шаге представления (19.06 в float32 — 19.059999465942383), и расхождение заметила бы только
// живая БД. Прецедент — rosstat/labour_force_test.go (TestLabourForceImportAppendsFloat64).
func TestCbrIntRateLoansAppendsFloat64(t *testing.T) {
	f, err := excelize.OpenFile(intRateLoansFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", intRateLoansFixture, err)
	}
	defer func() { _ = f.Close() }()
	rec := newCbrRecorder()
	if err = cbrIntRateLoans.ImportFunc(f, rec); err != nil {
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

// TestCbrIntRateLoansMissingSheetFails: книга без листа «ставки_руб» (единственного, который
// читает разбор) — ошибка с именем таблицы, а не «0 rows» и не паника.
func TestCbrIntRateLoansMissingSheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	rec := newCbrRecorder()
	err := cbrIntRateLoans.ImportFunc(f, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), "cbr_int_rate_loans") {
		t.Errorf("ошибка %q не называет таблицу cbr_int_rate_loans", err)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}

// TestCbrIntRateLoansShortSheetFails: лист «ставки_руб» есть, но короче шапки — ошибка с именем
// таблицы, а не «0 rows» с зелёным dagu: без обновления остались бы все 16 рядов, и заметить это
// было бы нечем.
func TestCbrIntRateLoansShortSheetFails(t *testing.T) {
	for name, rows := range map[string][][]string{
		"пустой лист":        nil,
		"только заголовок":   intRateLoansSheet[:3],
		"заголовок и группы": intRateLoansSheet[:4],
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			err := cbrIntRateLoans.ImportFunc(intRateLoansBook(t, rows), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_int_rate_loans") {
				t.Errorf("ошибка %q не называет таблицу cbr_int_rate_loans", err)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
			}
		})
	}
}

// TestCbrIntRateLoansFailFastBranches: ветки, где разбор обязан вернуть ошибку, а не «0 строк»
// (стандарт §6.4): сломанная шапка, метка периода не месяц, значение не число или не конечно
// (NaN/Inf ParseFloat принимает молча). Книги синтетические (живых фикстур этих состояний нет),
// поэтому первым подтестом идёт контроль: правильная синтетическая книга разбирается — значит
// ошибки ниже приходят от мутации, а не от сборки книги или от нормализации заголовков. Мутации
// выбраны в первых колонках шапки и данных: батч уходит в БД только по Send, поэтому при ошибке
// разбора в рекордере обязано быть 0 строк — частично пройденная строка до таблицы не доходит.
func TestCbrIntRateLoansFailFastBranches(t *testing.T) {
	t.Run("правильная шапка разбирается", func(t *testing.T) {
		rec := newCbrRecorder()
		if err := cbrIntRateLoans.ImportFunc(intRateLoansBook(t, intRateLoansSheetWithData()), rec); err != nil {
			t.Fatalf("разбор контрольной книги: %v", err)
		}
		if len(rec.rows) != len(cbrIntRateLoansNames) {
			t.Errorf("точек %d, want %d (по одной на ряд)", len(rec.rows), len(cbrIntRateLoansNames))
		}
	})
	for name, mutate := range map[string]func(rows [][]string){
		"заголовок колонки не совпал с ожидаемым сроком": func(rows [][]string) { rows[4][3] = "от 32 до 90 дней" },
		"подпись группы не найдена":                      func(rows [][]string) { rows[3][1] = "Итого" },
		"метка периода не месяц":                         func(rows [][]string) { rows[5][0] = "Итого 2014" },
		"значение не число":                              func(rows [][]string) { rows[5][1] = "н/д" },
		"значение NaN":                                   func(rows [][]string) { rows[5][1] = "NaN" },
	} {
		t.Run(name, func(t *testing.T) {
			rows := intRateLoansSheetWithData()
			mutate(rows)
			rec := newCbrRecorder()
			err := cbrIntRateLoans.ImportFunc(intRateLoansBook(t, rows), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_int_rate_loans") {
				t.Errorf("ошибка %q не называет таблицу cbr_int_rate_loans", err)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
			}
		})
	}
}

// TestCbrIntRateLoansFootnoteEndsParsing: строка-сноска «*…» после данных завершает разбор, а
// не считается ошибкой структуры (спека §3.4); текст сноски в живой фикстуре многострочный,
// поэтому проверяются и перенос строки внутри ячейки, и отсутствие точек после сноски.
func TestCbrIntRateLoansFootnoteEndsParsing(t *testing.T) {
	rows := intRateLoansSheetWithData()
	rows = append(rows, []string{"* Cредневзвешенные ставки в годовом исчислении.\u00a0 \nДинамика определяется ставками и объёмами."})
	rows = append(rows, []string{"Январь 2015", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16"})
	rec := newCbrRecorder()
	if err := cbrIntRateLoans.ImportFunc(intRateLoansBook(t, rows), rec); err != nil {
		t.Fatalf("разбор книги со сноской: %v", err)
	}
	if len(rec.rows) != len(cbrIntRateLoansNames) {
		t.Errorf("точек %d, want %d: строка после сноски разобрана не должна быть",
			len(rec.rows), len(cbrIntRateLoansNames))
	}
}

// intRateLoansOpenFixture — копия живой фикстуры, загруженная в память под мутацию: ветки
// fail-fast и пропуск ячеек проверяются на ней, а не на книге, собранной с нуля (стандарт §11.1:
// синтетика кодирует допущения теста и остаётся зелёной, когда разбор разошёлся с источником).
// Правки идут по копии в памяти — файл на диске не меняется.
func intRateLoansOpenFixture(t *testing.T, mutate func(t *testing.T, f *excelize.File)) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(intRateLoansFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", intRateLoansFixture, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	mutate(t, f)
	return f
}

// intRateLoansSetCell — правка ячейки мутируемой фикстуры: excelize сообщает о негодном адресе
// ошибкой, и проглотить её значило бы проверять немутнутый лист.
func intRateLoansSetCell(t *testing.T, f *excelize.File, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(cbrIntRateLoansSheet, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s): %v", cell, err)
	}
}

// intRateLoansCell — адрес ячейки листа (колонка и строка, 1-based): колонки мутаций выводятся из
// той же разметки, что и разбор (1 — метка периода, 2…9 — сроки группы «Всего», 10…17 — «СМП»),
// а не читаются строкой «B6» в коде.
func intRateLoansCell(t *testing.T, col, row int) string {
	t.Helper()
	cell, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		t.Fatalf("CoordinatesToCellName(%d, %d): %v", col, row, err)
	}
	return cell
}

// intRateLoansHasPoint — есть ли в разборе точка (ряд, дата).
func intRateLoansHasPoint(rows []appendedRow, name string, date time.Time) bool {
	for _, row := range rows {
		if row.name == name && row.date.Equal(date) {
			return true
		}
	}
	return false
}

// TestCbrIntRateLoansStructureFailsFastOnFixture: смена структуры листа живой фикстуры — ошибка с
// именем таблицы, а не «0 rows» (стандарт §6.4): группа из 7 колонок вместо 8 (подпись второй
// группы сдвинута на колонку влево — текст шапки сходится, а границы группы другие) и лист, у
// которого осталась одна шапка. У второго случая проверяется и текст ошибки: молчаливый ноль
// точек оставил бы все 16 рядов без обновления.
func TestCbrIntRateLoansStructureFailsFastOnFixture(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate  func(t *testing.T, f *excelize.File)
		wantErr string
	}{
		"группа из 7 колонок вместо 8": {
			// Подпись второй группы перенесена на колонку влево (J4 → I4): текст шапки сходится,
			// а первая группа сужается до семи колонок — разбор обязан это заметить, а не
			// разложить сроки по сдвинутым колонкам. Объединённые ячейки строки групп сначала
			// разъединяются: excelize направляет запись в объединённый диапазон в его левую
			// верхнюю ячейку (запись в I4 легла бы в B4 и стёрла «Всего»), тогда как разбор
			// читает объединённые диапазоны как обычные ячейки — значение несёт только первая.
			mutate: func(t *testing.T, f *excelize.File) {
				if err := f.UnmergeCell(cbrIntRateLoansSheet, "B4", "J4"); err != nil {
					t.Fatalf("UnmergeCell(B4, J4): %v", err)
				}
				intRateLoansSetCell(t, f, "I4", cbrIntRateLoansGroups[1].SourceLabel)
				intRateLoansSetCell(t, f, "J4", "")
			},
			wantErr: "источник изменил структуру листа",
		},
		"шапка без строк данных": {
			// Строки данных и сноска удалены — шапка цела, разбирать нечего.
			mutate: func(t *testing.T, f *excelize.File) {
				rows, err := f.GetRows(cbrIntRateLoansSheet)
				if err != nil {
					t.Fatalf("GetRows: %v", err)
				}
				// Снизу вверх: RemoveRow сдвигает строки за удалённой, и обратный порядок избавляет
				// от пересчёта номера. Шапка — те же 5 строк, что в intRateLoansSheet.
				for row := len(rows); row > len(intRateLoansSheet); row-- {
					if err := f.RemoveRow(cbrIntRateLoansSheet, row); err != nil {
						t.Fatalf("RemoveRow(%d): %v", row, err)
					}
				}
			},
			wantErr: "не дал ни одной точки",
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			err := cbrIntRateLoans.ImportFunc(intRateLoansOpenFixture(t, tc.mutate), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_int_rate_loans") {
				t.Errorf("ошибка %q не называет таблицу cbr_int_rate_loans", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ошибка %q не содержит %q", err, tc.wantErr)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
			}
		})
	}
}

// TestCbrIntRateLoansSkipsNonPointsOnFixture: ячейка, которая точкой не является, точек не даёт
// (стандарт §6.7): пустая, литеральный ноль (в источнике — «данных не имеется») и ячейки за
// концом короткой строки данных. Проверка на живой фикстуре, где каждая из 2 432 ячеек на счету:
// точек становится ровно на пропущенное меньше, а соседние месяцы и ряды остаются на месте.
// Литеральный ноль до этой правки писался точкой 0 % годовых — подтест стережёт новую семантику.
func TestCbrIntRateLoansSkipsNonPointsOnFixture(t *testing.T) {
	total := len(cbrIntRateLoansNames) * len(intRateLoansMonths)
	// Первая строка данных листа — «Январь 2014»: шапка живой фикстуры занимает те же пять строк,
	// что intRateLoansSheet (заголовок файла 1–3, подписи групп, заголовки сроков).
	firstDataRow := len(intRateLoansSheet) + 1
	// Даты соседних месяцев по правилу разбора: предпоследний день месяца отчётного периода
	// (2014-01-30, а в феврале 2014 — 27-е: дней 28). Литералы — как в спот-проверках выше.
	jan := time.Date(2014, time.January, 30, 0, 0, 0, 0, time.UTC)
	feb := time.Date(2014, time.February, 27, 0, 0, 0, 0, time.UTC)
	allNames := intRateLoansWantNames[:len(cbrIntRateLoansTerms)]
	smpNames := intRateLoansWantNames[len(cbrIntRateLoansTerms):]
	for name, tc := range map[string]struct {
		mutate   func(t *testing.T, f *excelize.File)
		wantRows int
		absent   []string
	}{
		"пустая ячейка": {
			// Колонка 2 — первый срок группы «Всего» (B), строка — «Январь 2014».
			mutate: func(t *testing.T, f *excelize.File) {
				intRateLoansSetCell(t, f, intRateLoansCell(t, 2, firstDataRow), "")
			},
			wantRows: total - 1,
			absent:   []string{allNames[0]},
		},
		"литеральный ноль": {
			mutate: func(t *testing.T, f *excelize.File) {
				intRateLoansSetCell(t, f, intRateLoansCell(t, 2, firstDataRow), 0)
			},
			wantRows: total - 1,
			absent:   []string{allNames[0]},
		},
		"строка данных короче шапки": {
			// Опустошены колонки 10–17 (группа «СМП») строки «Январь 2014»: строка становится
			// короче набора колонок данных, и восемь её последних колонок точек не дают.
			mutate: func(t *testing.T, f *excelize.File) {
				for col := 2 + len(cbrIntRateLoansTerms); col <= 1+2*len(cbrIntRateLoansTerms); col++ {
					intRateLoansSetCell(t, f, intRateLoansCell(t, col, firstDataRow), "")
				}
			},
			wantRows: total - len(cbrIntRateLoansTerms),
			absent:   smpNames,
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			if err := cbrIntRateLoans.ImportFunc(intRateLoansOpenFixture(t, tc.mutate), rec); err != nil {
				t.Fatalf("разбор мутированной фикстуры: %v", err)
			}
			if rec.err != nil {
				t.Fatalf("схема строк не совпала с разбором: %v", rec.err)
			}
			if len(rec.rows) != tc.wantRows {
				t.Errorf("точек %d, want %d: пропущенная ячейка не даёт точки и не теряет соседей",
					len(rec.rows), tc.wantRows)
			}
			for _, absent := range tc.absent {
				if intRateLoansHasPoint(rec.rows, absent, jan) {
					t.Errorf("ряд %q: точка на %s записана, хотя ячейка пропущена",
						absent, jan.Format("2006-01-02"))
				}
				if !intRateLoansHasPoint(rec.rows, absent, feb) {
					t.Errorf("ряд %q: пропала точка на %s — пропуск одной ячейки соседний месяц не трогает",
						absent, feb.Format("2006-01-02"))
				}
			}
			// Соседняя колонка не сдвинулась: пропуск ячейки не меняет привязку значений к именам.
			if !intRateLoansHasPoint(rec.rows, allNames[1], jan) {
				t.Errorf("ряд %q: точка на %s пропала — привязка колонок к именам сдвинулась",
					allNames[1], jan.Format("2006-01-02"))
			}
		})
	}
}

// TestCbrIntRateLoansAppendErrorNamesRow: ошибка записи в батч не отдаётся голой — в ней есть
// таблица, метка периода, имя ряда и дата точки: по одной ошибке видно, какая строка не ушла.
// Схема приёмника из одной колонки заставляет его отвергнуть каждую строку (name, date, rate) —
// то же, что делает настоящий батч на негодной строке.
func TestCbrIntRateLoansAppendErrorNamesRow(t *testing.T) {
	rec := newCbrRecorder("name")
	err := cbrIntRateLoans.ImportFunc(intRateLoansOpenFixture(t, func(*testing.T, *excelize.File) {}), rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка записи, разбор дал %d строк", len(rec.rows))
	}
	for _, want := range []string{
		cbrIntRateLoansTable,
		"Январь 2014",
		intRateLoansWantNames[0],
		"2014-01-30",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
}

// TestCbrIntRateLoansOriginNamesGroupAndTerm: origin называет группу и срок раздельно и не
// укладывает составное имя ряда в кавычки целиком (правило 11 AGENTS.md): агент MCP обязан
// видеть, что «Всего: от 1 года до 3 лет» — это группа плюс срок, а не одна ячейка источника.
// Формат сверяется подстроками, а не равенством: текст origin может уточняться.
func TestCbrIntRateLoansOriginNamesGroupAndTerm(t *testing.T) {
	if len(cbrIntRateLoansSeriesMeta) != len(cbrIntRateLoansNames) {
		t.Fatalf("записей каталога %d, want %d", len(cbrIntRateLoansSeriesMeta), len(cbrIntRateLoansNames))
	}
	for i, meta := range cbrIntRateLoansSeriesMeta {
		group := cbrIntRateLoansGroups[i/len(cbrIntRateLoansTerms)]
		term := cbrIntRateLoansTerms[i%len(cbrIntRateLoansTerms)]
		if meta.Series != group.Prefix+term {
			t.Errorf("запись %d: series %q, want %q (порядок каталога — порядок колонок листа)",
				i, meta.Series, group.Prefix+term)
		}
		for _, want := range []string{
			"лист «" + cbrIntRateLoansSheet + "»",
			"группа «" + group.SourceLabel + "»",
			"срок «" + term + "»",
		} {
			if !strings.Contains(meta.Origin, want) {
				t.Errorf("ряд %q: origin %q не содержит %q", meta.Series, meta.Origin, want)
			}
		}
		if strings.Contains(meta.Origin, "«"+meta.Series+"»") {
			t.Errorf("ряд %q: origin %q уложил составное имя ряда в кавычки целиком — группа и срок неразличимы",
				meta.Series, meta.Origin)
		}
	}
}
