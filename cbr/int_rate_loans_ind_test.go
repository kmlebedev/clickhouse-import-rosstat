package cbr

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// intRateLoansIndFixture — живой файл источника целиком: книга ~104 КБ (104 546 байт, 6 листов)
// коммитится как есть — стандарт §11.2, zip-хирургия нужна только крупным книгам. Разбор читает
// из неё единственный лист «ставки_руб».
const intRateLoansIndFixture = "testdata/loans_ind.xlsx"

// intRateLoansIndWantNames — 16 имён рядов литералами по спеке §3.1, в порядке колонок листа:
// две группы по восемь сроков. Литералы, а не ссылка на cbrIntRateLoansIndNames: имена в коде
// собираются из префиксов групп и сроков, и сверка с литералами — единственное место, где
// расхождение со спекой (опечатка в префиксе группы, перестановка сроков) видит тест, а не глаз
// ревьюера. Порядок: сначала все сроки группы «Всего» (колонки B–I), затем «Автокредиты» (J–Q).
var intRateLoansIndWantNames = []string{
	"Всего (кредиты физлицам): до 30 дней, включая «до востребования»",
	"Всего (кредиты физлицам): от 31 до 90 дней",
	"Всего (кредиты физлицам): от 91 до 180 дней",
	"Всего (кредиты физлицам): от 181 дня до 1 года",
	"Всего (кредиты физлицам): до 1 года, включая «до востребования»",
	"Всего (кредиты физлицам): от 1 года до 3 лет",
	"Всего (кредиты физлицам): свыше 3 лет",
	"Всего (кредиты физлицам): свыше 1 года",
	"Автокредиты: до 30 дней, включая «до востребования»",
	"Автокредиты: от 31 до 90 дней",
	"Автокредиты: от 91 до 180 дней",
	"Автокредиты: от 181 дня до 1 года",
	"Автокредиты: до 1 года, включая «до востребования»",
	"Автокредиты: от 1 года до 3 лет",
	"Автокредиты: свыше 3 лет",
	"Автокредиты: свыше 1 года",
}

// intRateLoansIndMonths — месяцы листа «ставки_руб»: строки 6..149 файла, с января 2014 по
// декабрь 2025 (144 месяца). У ЦБ в колонке метки стоит месяц отчётного периода, а разбор берёт
// предпоследний день этого месяца (time.Date(year, month+1, -1)), поэтому месяцы сверяются с правилом
// даты, а не с захардкоженным списком дней.
var intRateLoansIndMonths = func() []time.Time {
	start := time.Date(2014, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, time.December, 1, 0, 0, 0, 0, time.UTC)
	var months []time.Time
	for m := start; !m.After(end); m = m.AddDate(0, 1, 0) {
		months = append(months, m)
	}
	return months
}()

// intRateLoansIndPointDate — дата точки по правилу разбора: предпоследний день месяца
// отчётного периода (та же конвенция, что у cbr_int_rate_loans и cbr_bank_int_rate).
func intRateLoansIndPointDate(month time.Time) time.Time {
	return time.Date(month.Year(), month.Month()+1, -1, 0, 0, 0, 0, time.UTC)
}

// intRateLoansIndWantCounts — число точек каждого ряда на живой фикстуре (снимок 2026-11-03).
// Восемь рядов «Всего» заполнены все 144 месяца; у «Автокредитов» краткосрочные ряды имеют
// пропуски (пустая ячейка и литеральный «-» в источнике), длинные — заполнены полностью:
// 33 + 86 + 105 + 5×144 = 944 точки автокредитов, 8×144 = 1 152 точки «Всего», всего 2 096.
// Карта литеральная: сдвиг колонок или потеря разбором пропусков меняет счёт конкретного ряда,
// а не только сумму.
var intRateLoansIndWantCounts = map[string]int{
	"Всего (кредиты физлицам): до 30 дней, включая «до востребования»": 144,
	"Всего (кредиты физлицам): от 31 до 90 дней":                       144,
	"Всего (кредиты физлицам): от 91 до 180 дней":                      144,
	"Всего (кредиты физлицам): от 181 дня до 1 года":                   144,
	"Всего (кредиты физлицам): до 1 года, включая «до востребования»":  144,
	"Всего (кредиты физлицам): от 1 года до 3 лет":                     144,
	"Всего (кредиты физлицам): свыше 3 лет":                            144,
	"Всего (кредиты физлицам): свыше 1 года":                           144,
	"Автокредиты: до 30 дней, включая «до востребования»":              33,
	"Автокредиты: от 31 до 90 дней":                                    86,
	"Автокредиты: от 91 до 180 дней":                                   105,
	"Автокредиты: от 181 дня до 1 года":                                144,
	"Автокредиты: до 1 года, включая «до востребования»":               144,
	"Автокредиты: от 1 года до 3 лет":                                  144,
	"Автокредиты: свыше 3 лет":                                         144,
	"Автокредиты: свыше 1 года":                                        144,
}

// intRateLoansIndTotalPoints — полный снимок count() разбора живой фикстуры.
const intRateLoansIndTotalPoints = 2096

// intRateLoansIndSheet — шапка листа «ставки_руб» с дословным текстом живой книги: строка 4 —
// подписи групп (merged-ячейки несут текст только в первой колонке группы), строка 5 — 16
// заголовков сроков. У группы «Всего» первые два заголовка несут сноски `**`, а у первого ещё и
// перенос «до востребова-ния» и апострофы вместо кавычек — разбор обязан нормализовать это.
// Синтетика уместна только здесь: живой фикстуры со сломанной шапкой не существует, а ветка
// «источник изменил структуру листа» (стандарт §6.4) обязана быть проверена.
var intRateLoansIndSheet = [][]string{
	{"Средневзвешенные процентные ставки по кредитам, предоставленным кредитными организациями физическим лицам в рублях*"},
	{"(в целом по Российской Федерации)"},
	{"% годовых"},
	{"", "Всего", "", "", "", "", "", "", "", "в том числе автокредиты"},
	{
		"",
		"до 30 дней, включая ''до востребова-ния**''", "от 31 до 90 дней**", "от 91 до 180 дней", "от 181 дня до 1 года",
		"до 1 года, включая ''до востребова-ния''", "от 1 года до 3 лет", "свыше 3 лет", "свыше 1 года",
		"до 30 дней, включая ''до востребова-ния''", "от 31 до 90 дней", "от 91 до 180 дней", "от 181 дня до 1 года",
		"до 1 года, включая ''до востребова-ния''", "от 1 года до 3 лет", "свыше 3 лет", "свыше 1 года",
	},
}

// intRateLoansIndBook собирает книгу с листом «ставки_руб» из переданных строк.
func intRateLoansIndBook(t *testing.T, rows [][]string) *excelize.File {
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

// intRateLoansIndSheetWithData — шапка листа плюс одна строка данных «Январь 2014»: во всех 16
// колонках 7.35. Копия, а не ссылка на intRateLoansIndSheet: мутации в тестах ветвей fail-fast
// не должны менять шапку для соседних проверок.
func intRateLoansIndSheetWithData() [][]string {
	rows := make([][]string, 0, len(intRateLoansIndSheet)+1)
	for _, row := range intRateLoansIndSheet {
		rows = append(rows, append([]string(nil), row...))
	}
	data := make([]string, len(intRateLoansIndSheet[4]))
	data[0] = "Январь 2014"
	for i := 1; i < len(data); i++ {
		data[i] = "7.35"
	}
	return append(rows, data)
}

// TestCbrIntRateLoansIndNames — имена рядов кода совпадают со списком спеки §3.1 дословно: и
// префиксы групп («Всего (кредиты физлицам): », «Автокредиты: »), и тексты сроков, и их порядок.
func TestCbrIntRateLoansIndNames(t *testing.T) {
	if len(cbrIntRateLoansIndNames) != len(intRateLoansIndWantNames) {
		t.Fatalf("имён рядов %d, want %d", len(cbrIntRateLoansIndNames), len(intRateLoansIndWantNames))
	}
	for i, want := range intRateLoansIndWantNames {
		if cbrIntRateLoansIndNames[i] != want {
			t.Errorf("имя ряда %d = %q, want %q", i, cbrIntRateLoansIndNames[i], want)
		}
	}
}

// TestCbrIntRateLoansIndImporterName: Name() — контракт фильтра `make import STAT=…` и id шага
// dagu/cbr.yaml; сверяется литералом, а не таблицей кода (стандарт §8.5).
func TestCbrIntRateLoansIndImporterName(t *testing.T) {
	if got := cbrIntRateLoansIndImporter.Name(); got != "cbr_int_rate_loans_ind" {
		t.Errorf("Name() = %q, want %q", got, "cbr_int_rate_loans_ind")
	}
}

// TestCbrIntRateLoansIndDdl: DDL — контракт данных (правило 5 AGENTS.md: ценовые значения
// Float64; движок и ключ — общие для таблиц пакета). Проверяется литералами: DDL в %s-форме,
// имя таблицы в него подставляет шаблон, и потеря %s оставила бы запрос без имени таблицы.
func TestCbrIntRateLoansIndDdl(t *testing.T) {
	if len(cbrIntRateLoansInd.CreateTable) != 1 {
		t.Fatalf("DDL-инструкций %d, want 1", len(cbrIntRateLoansInd.CreateTable))
	}
	ddl := fmt.Sprintf(cbrIntRateLoansInd.CreateTable[0], "cbr_int_rate_loans_ind")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS cbr_int_rate_loans_ind",
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

// TestCbrIntRateLoansIndSeriesMetaCoversParsedRows: каталог cbr_int_rate_loans_ind (16 рядов) и
// разбор фикстуры совпадают в обе стороны (стандарт §11.4): имена рядов объявлены кодом
// (группа × срок), поэтому проверка двусторонняя — и забытое имя в каталоге, и забытая колонка
// в разборе видны одинаково.
func TestCbrIntRateLoansIndSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrIntRateLoansIndSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile(intRateLoansIndFixture)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return cbrIntRateLoansInd.ImportFunc(f, rec)
	})
}

// TestCbrIntRateLoansIndFixtureRows: разбор живой фикстуры даёт полный снимок 2 096 точек, а
// раскладка по рядам совпадает с intRateLoansIndWantCounts. Даты сверяются с правилом
// «предпоследний день месяца отчётного периода», а не списком дней; полные ряды обязаны пройти все 144 месяца
// без пропусков, у краткосрочных автокредитов пропуски — свойство источника, и проверяется
// только монотонность и границы.
func TestCbrIntRateLoansIndFixtureRows(t *testing.T) {
	rows := cbrFixtureRows(t, intRateLoansIndFixture, cbrIntRateLoansInd.ImportFunc)
	if len(rows) != intRateLoansIndTotalPoints {
		t.Errorf("строк разбора %d, want %d (снимок 2026-11-03)", len(rows), intRateLoansIndTotalPoints)
	}
	dates := map[string][]time.Time{}
	for _, row := range rows {
		dates[row.name] = append(dates[row.name], row.date)
	}
	for _, name := range intRateLoansIndWantNames {
		got := dates[name]
		want, ok := intRateLoansIndWantCounts[name]
		if !ok {
			t.Errorf("ряд %q: снимок intRateLoansIndWantCounts не задаёт число точек", name)
			continue
		}
		if len(got) != want {
			t.Errorf("ряд %q: точек %d, want %d (снимок фикстуры)", name, len(got), want)
			continue
		}
		if !got[0].Equal(intRateLoansIndPointDate(intRateLoansIndMonths[0])) {
			t.Errorf("ряд %q: первая точка %s, want %s (предпоследний день первого месяца)",
				name, got[0].Format("2006-01-02"), intRateLoansIndPointDate(intRateLoansIndMonths[0]).Format("2006-01-02"))
		}
		for i, date := range got {
			if !date.Equal(intRateLoansIndPointDate(date)) {
				t.Errorf("ряд %q: точка %d (%s) не предпоследний день своего месяца", name, i, date.Format("2006-01-02"))
				break
			}
			if i > 0 && !got[i-1].Before(date) {
				t.Errorf("ряд %q: точка %d (%s) не позже предыдущей (%s)",
					name, i, date.Format("2006-01-02"), got[i-1].Format("2006-01-02"))
				break
			}
		}
		// Полный ряд — это ровно все 144 месяца: пропуск месяца в источнике (если он появится)
		// обязан быть виден тесту, а не прочитан как «ряд стал короче».
		if want == len(intRateLoansIndMonths) {
			allMonths := true
			for i, month := range intRateLoansIndMonths {
				if i >= len(got) || !got[i].Equal(intRateLoansIndPointDate(month)) {
					allMonths = false
					break
				}
			}
			if !allMonths {
				t.Errorf("ряд %q: полный ряд не совпал с набором месяцев 2014-01…2025-12", name)
			}
		}
	}
}

// TestCbrIntRateLoansIndSpotValues: значения из известных ячеек живой фикстуры — первый месяц
// обоих групп, последний месяц полного ряда «Всего» и последний месяц длинного ряда автокредитов.
// Значения литералами: сверка идёт с разбором, а не с константой кода. Таблица — Float64,
// поэтому сравнение точное (cbrCheckSpotFloat64), без приведения к Float32.
func TestCbrIntRateLoansIndSpotValues(t *testing.T) {
	rows := cbrFixtureRows(t, intRateLoansIndFixture, cbrIntRateLoansInd.ImportFunc)
	first := time.Date(2014, time.January, 30, 0, 0, 0, 0, time.UTC)
	last := time.Date(2025, time.December, 30, 0, 0, 0, 0, time.UTC)
	cbrCheckSpotFloat64(t, rows, "Всего (кредиты физлицам): до 30 дней, включая «до востребования»", first, 28.04)
	cbrCheckSpotFloat64(t, rows, "Автокредиты: свыше 3 лет", first, 16.12)
	cbrCheckSpotFloat64(t, rows, "Всего (кредиты физлицам): свыше 1 года", last, 14.43)
	cbrCheckSpotFloat64(t, rows, "Автокредиты: от 181 дня до 1 года", last, 2.45)
}

// TestCbrIntRateLoansIndAppendsFloat64: разбор пишет ставку как float64 — тип колонки rate в DDL
// (правило 5 AGENTS.md). Запись float32 прошла бы в колонку Float64 молча, потеряв точность на
// шаге представления (28.04 в float32 — 28.040000915527344), и расхождение заметила бы только
// живая БД. Прецедент — rosstat/labour_force_test.go (TestLabourForceImportAppendsFloat64).
func TestCbrIntRateLoansIndAppendsFloat64(t *testing.T) {
	f, err := excelize.OpenFile(intRateLoansIndFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", intRateLoansIndFixture, err)
	}
	defer func() { _ = f.Close() }()
	rec := newCbrRecorder()
	if err = cbrIntRateLoansInd.ImportFunc(f, rec); err != nil {
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

// TestCbrIntRateLoansIndMissingSheetFails: книга без листа «ставки_руб» (единственного, который
// читает разбор) — ошибка с именем таблицы, а не «0 rows» и не паника.
func TestCbrIntRateLoansIndMissingSheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	rec := newCbrRecorder()
	err := cbrIntRateLoansInd.ImportFunc(f, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), "cbr_int_rate_loans_ind") {
		t.Errorf("ошибка %q не называет таблицу cbr_int_rate_loans_ind", err)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}

// TestCbrIntRateLoansIndShortSheetFails: лист «ставки_руб» есть, но короче шапки — ошибка с
// именем таблицы, а не «0 rows» с зелёным dagu: без обновления остались бы все 16 рядов, и
// заметить это было бы нечем.
func TestCbrIntRateLoansIndShortSheetFails(t *testing.T) {
	for name, rows := range map[string][][]string{
		"пустой лист":            nil,
		"только заголовок":       intRateLoansIndSheet[:3],
		"заголовок и группы":     intRateLoansIndSheet[:4],
		"шапка без строк данных": intRateLoansIndSheet[:5],
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			err := cbrIntRateLoansInd.ImportFunc(intRateLoansIndBook(t, rows), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_int_rate_loans_ind") {
				t.Errorf("ошибка %q не называет таблицу cbr_int_rate_loans_ind", err)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
			}
		})
	}
}

// TestCbrIntRateLoansIndFailFastBranches: ветки, где разбор обязан вернуть ошибку, а не «0 строк»
// (стандарт §6.4): сломанная шапка (заголовок срока или подпись группы), метка периода не месяц,
// значение не число или не конечно (NaN/Inf ParseFloat принимает молча). Книги синтетические
// (живых фикстур этих состояний нет), поэтому первым подтестом идёт контроль: правильная
// синтетическая книга разбирается — значит, ошибки ниже приходят от мутации, а не от сборки
// книги или от нормализации заголовков. Мутации выбраны в первых колонках шапки и данных: батч
// уходит в БД только по Send, поэтому при ошибке разбора в рекордере обязано быть 0 строк.
func TestCbrIntRateLoansIndFailFastBranches(t *testing.T) {
	t.Run("правильная шапка разбирается", func(t *testing.T) {
		rec := newCbrRecorder()
		if err := cbrIntRateLoansInd.ImportFunc(intRateLoansIndBook(t, intRateLoansIndSheetWithData()), rec); err != nil {
			t.Fatalf("разбор контрольной книги: %v", err)
		}
		if len(rec.rows) != len(cbrIntRateLoansIndNames) {
			t.Errorf("точек %d, want %d (по одной на ряд)", len(rec.rows), len(cbrIntRateLoansIndNames))
		}
	})
	for name, mutate := range map[string]func(rows [][]string){
		"нет строки «% годовых»":                         func(rows [][]string) { rows[2][0] = "проценты" },
		"заголовок колонки не совпал с ожидаемым сроком": func(rows [][]string) { rows[4][3] = "от 32 до 90 дней" },
		"подпись группы не найдена":                      func(rows [][]string) { rows[3][1] = "Итого" },
		"метка периода не месяц":                         func(rows [][]string) { rows[5][0] = "Итого 2014" },
		"значение не число":                              func(rows [][]string) { rows[5][1] = "н/д" },
		"значение NaN":                                   func(rows [][]string) { rows[5][1] = "NaN" },
	} {
		t.Run(name, func(t *testing.T) {
			rows := intRateLoansIndSheetWithData()
			mutate(rows)
			rec := newCbrRecorder()
			err := cbrIntRateLoansInd.ImportFunc(intRateLoansIndBook(t, rows), rec)
			if err == nil {
				t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
			}
			if !strings.Contains(err.Error(), "cbr_int_rate_loans_ind") {
				t.Errorf("ошибка %q не называет таблицу cbr_int_rate_loans_ind", err)
			}
			if len(rec.raw) != 0 {
				t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
			}
		})
	}
}

// TestCbrIntRateLoansIndFootnoteEndsParsing: строка-сноска «*…» после данных завершает разбор, а
// не считается ошибкой структуры (спека §3.3); пустая строка между данными и сноской (в живой
// книге — строка 150) пропускается по пустой метке. Текст сноски в живой фикстуре многострочный,
// поэтому проверяются и перенос строки внутри ячейки, и отсутствие точек после сноски.
func TestCbrIntRateLoansIndFootnoteEndsParsing(t *testing.T) {
	rows := intRateLoansIndSheetWithData()
	// Пустая строка между данными и сноской, как строка 150 живой книги.
	rows = append(rows, []string{""})
	rows = append(rows, []string{"* Cредневзвешенные процентные ставки в годовом исчислении.\u00a0 \nДинамика определяется ставками и объёмами."})
	rows = append(rows, []string{"Январь 2015", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16"})
	rec := newCbrRecorder()
	if err := cbrIntRateLoansInd.ImportFunc(intRateLoansIndBook(t, rows), rec); err != nil {
		t.Fatalf("разбор книги со сноской: %v", err)
	}
	if len(rec.rows) != len(cbrIntRateLoansIndNames) {
		t.Errorf("точек %d, want %d: строка после сноски разобрана не должна быть",
			len(rec.rows), len(cbrIntRateLoansIndNames))
	}
}

// intRateLoansIndOpenFixture — копия живой фикстуры, загруженная в память под мутацию: пропуск
// ячеек проверяется на ней, а не на книге, собранной с нуля (стандарт §11.1: синтетика кодирует
// допущения теста и остаётся зелёной, когда разбор разошёлся с источником). Правки идут по копии
// в памяти — файл на диске не меняется.
func intRateLoansIndOpenFixture(t *testing.T, mutate func(t *testing.T, f *excelize.File)) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(intRateLoansIndFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", intRateLoansIndFixture, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	mutate(t, f)
	return f
}

// intRateLoansIndSetCell — правка ячейки мутируемой фикстуры: excelize сообщает о негодном
// адресе ошибкой, и проглотить её значило бы проверять немутнутый лист.
func intRateLoansIndSetCell(t *testing.T, f *excelize.File, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(cbrIntRateLoansIndSheet, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s): %v", cell, err)
	}
}

// intRateLoansIndCell — адрес ячейки листа (колонка и строка, 1-based): колонки мутаций выводятся
// из той же разметки, что и разбор (1 — метка периода, 2…9 — сроки группы «Всего», 10…17 —
// «Автокредиты»), а не читаются строкой «B6» в коде.
func intRateLoansIndCell(t *testing.T, col, row int) string {
	t.Helper()
	cell, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		t.Fatalf("CoordinatesToCellName(%d, %d): %v", col, row, err)
	}
	return cell
}

// intRateLoansIndHasPoint — есть ли в разборе точка (ряд, дата).
func intRateLoansIndHasPoint(rows []appendedRow, name string, date time.Time) bool {
	for _, row := range rows {
		if row.name == name && row.date.Equal(date) {
			return true
		}
	}
	return false
}

// TestCbrIntRateLoansIndSkipsNonPointsOnFixture: ячейка, которая точкой не является, точек не
// даёт (стандарт §6.7): пустая, литеральный ноль (в источнике — «данных не имеется»), литеральный
// «-» (им помечены пропуски краткосрочных автокредитов) и ячейки за концом короткой строки
// данных. Проверка на живой фикстуре: точек становится ровно на пропущенное меньше, а соседние
// месяцы и ряды остаются на месте.
func TestCbrIntRateLoansIndSkipsNonPointsOnFixture(t *testing.T) {
	total := intRateLoansIndTotalPoints
	// Первая строка данных листа — «Январь 2014»: шапка живой фикстуры занимает те же пять строк,
	// что intRateLoansIndSheet (заголовок файла 1–3, подписи групп, заголовки сроков).
	firstDataRow := len(intRateLoansIndSheet) + 1
	// Даты соседних месяцев по правилу разбора: предпоследний день месяца (2014-01-30, 2014-02-27).
	jan := time.Date(2014, time.January, 30, 0, 0, 0, 0, time.UTC)
	feb := time.Date(2014, time.February, 27, 0, 0, 0, 0, time.UTC)
	allNames := intRateLoansIndWantNames[:len(cbrIntRateLoansIndTerms)]
	autoNames := intRateLoansIndWantNames[len(cbrIntRateLoansIndTerms):]
	for name, tc := range map[string]struct {
		mutate   func(t *testing.T, f *excelize.File)
		wantRows int
		absent   []string
	}{
		"пустая ячейка": {
			// Колонка 2 — первый срок группы «Всего» (B), строка — «Январь 2014».
			mutate: func(t *testing.T, f *excelize.File) {
				intRateLoansIndSetCell(t, f, intRateLoansIndCell(t, 2, firstDataRow), "")
			},
			wantRows: total - 1,
			absent:   []string{allNames[0]},
		},
		"литеральный ноль": {
			mutate: func(t *testing.T, f *excelize.File) {
				intRateLoansIndSetCell(t, f, intRateLoansIndCell(t, 2, firstDataRow), 0)
			},
			wantRows: total - 1,
			absent:   []string{allNames[0]},
		},
		"литеральный дефис": {
			mutate: func(t *testing.T, f *excelize.File) {
				intRateLoansIndSetCell(t, f, intRateLoansIndCell(t, 2, firstDataRow), "-")
			},
			wantRows: total - 1,
			absent:   []string{allNames[0]},
		},
		"строка данных короче шапки": {
			// Опустошены колонки 10–17 (группа «Автокредиты») строки «Январь 2014»: строка
			// становится короче набора колонок данных, и восемь её последних колонок точек не дают.
			mutate: func(t *testing.T, f *excelize.File) {
				for col := 2 + len(cbrIntRateLoansIndTerms); col <= 1+2*len(cbrIntRateLoansIndTerms); col++ {
					intRateLoansIndSetCell(t, f, intRateLoansIndCell(t, col, firstDataRow), "")
				}
			},
			wantRows: total - len(cbrIntRateLoansIndTerms),
			absent:   autoNames,
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newCbrRecorder()
			if err := cbrIntRateLoansInd.ImportFunc(intRateLoansIndOpenFixture(t, tc.mutate), rec); err != nil {
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
				if intRateLoansIndHasPoint(rec.rows, absent, jan) {
					t.Errorf("ряд %q: точка на %s записана, хотя ячейка пропущена",
						absent, jan.Format("2006-01-02"))
				}
				if !intRateLoansIndHasPoint(rec.rows, absent, feb) {
					t.Errorf("ряд %q: пропала точка на %s — пропуск одной ячейки соседний месяц не трогает",
						absent, feb.Format("2006-01-02"))
				}
			}
			// Соседняя колонка не сдвинулась: пропуск ячейки не меняет привязку значений к именам.
			if !intRateLoansIndHasPoint(rec.rows, allNames[1], jan) {
				t.Errorf("ряд %q: точка на %s пропала — привязка колонок к именам сдвинулась",
					allNames[1], jan.Format("2006-01-02"))
			}
		})
	}
}

// TestCbrIntRateLoansIndAppendErrorNamesRow: ошибка записи в батч не отдаётся голой — в ней есть
// таблица, метка периода, имя ряда и дата точки: по одной ошибке видно, какая строка не ушла.
// Схема приёмника из одной колонки заставляет его отвергнуть каждую строку (name, date, rate) —
// то же, что делает настоящий батч на негодной строке.
func TestCbrIntRateLoansIndAppendErrorNamesRow(t *testing.T) {
	rec := newCbrRecorder("name")
	err := cbrIntRateLoansInd.ImportFunc(intRateLoansIndOpenFixture(t, func(*testing.T, *excelize.File) {}), rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка записи, разбор дал %d строк", len(rec.rows))
	}
	for _, want := range []string{
		cbrIntRateLoansIndTable,
		"Январь 2014",
		intRateLoansIndWantNames[0],
		"2014-01-30",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
}

// TestCbrIntRateLoansIndOriginNamesGroupAndTerm: origin называет группу и срок раздельно и не
// укладывает составное имя ряда в кавычки целиком (правило 11 AGENTS.md): агент MCP обязан
// видеть, что «Всего (кредиты физлицам): от 1 года до 3 лет» — это группа плюс срок, а не одна
// ячейка источника. Формат сверяется подстроками, а не равенством: текст origin может уточняться.
func TestCbrIntRateLoansIndOriginNamesGroupAndTerm(t *testing.T) {
	if len(cbrIntRateLoansIndSeriesMeta) != len(cbrIntRateLoansIndNames) {
		t.Fatalf("записей каталога %d, want %d", len(cbrIntRateLoansIndSeriesMeta), len(cbrIntRateLoansIndNames))
	}
	for i, meta := range cbrIntRateLoansIndSeriesMeta {
		group := cbrIntRateLoansIndGroups[i/len(cbrIntRateLoansIndTerms)]
		term := cbrIntRateLoansIndTerms[i%len(cbrIntRateLoansIndTerms)]
		if meta.Series != group.Prefix+term {
			t.Errorf("запись %d: series %q, want %q (порядок каталога — порядок колонок листа)",
				i, meta.Series, group.Prefix+term)
		}
		for _, want := range []string{
			"лист «" + cbrIntRateLoansIndSheet + "»",
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
