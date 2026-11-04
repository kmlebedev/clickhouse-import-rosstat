package bank

import (
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// Фикстура — живая книга квартального супердополнения ВТБ 9М'2026
// (financial_data_supplement_0926_rus.xlsx со страницы результатов, скачана 2026-11-04,
// 123 372 Б): четыре листа — «Содержание», «1.Ключевые показатели», «2.P&L», «3.Баланс »;
// разбирается только «1.Ключевые показатели», пять секций (спека 2026-11-04 §2.2, §1.3).
// Коммитится как есть (§11.1–11.2).
const vtbSuppBookFixture = "vtb_supplement_0926.xlsx"

// Даты колонок книги (разведка 2026-11-04). Периодные секции: «9М'26»/«3кв'26» и годом раньше —
// дата точки последний день сентября (2026-09-30 / 2025-09-30). Секции с датами: баланс и
// качество кредитов — 2026-09-30 и 2025-12-31; достаточность капитала — двенадцать квартальных
// дат первым числом месяца после квартала: 2026-10-01 … 2024-01-01 (так источник датирует
// нормативы — спека §4.3).
var (
	vtbSuppSep26 = time.Date(2026, time.September, 30, 0, 0, 0, 0, time.UTC)
	vtbSuppSep25 = time.Date(2025, time.September, 30, 0, 0, 0, 0, time.UTC)
	vtbSuppDec25 = time.Date(2025, time.December, 31, 0, 0, 0, 0, time.UTC)
	vtbSuppOct26 = time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC)
	vtbSuppJan24 = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// TestParseVtbSupplementBook: книга супердополнения 9М'2026 даёт точки пяти секций листа
// «1.Ключевые показатели»: 9 балансовых рядов × 2 даты (склейка с месячными именами), 8 P&L и
// 5 коэффициентов × 4 колонки-периода (9М/3кв × два года, суффиксные имена), 3 качества × 2
// даты, 2 норматива × 12 дат. Итого 100 точек / 40 рядов. Числа ожиданий — из независимой
// выкладки листа (разведка 2026-11-04), не из реализации.
func TestParseVtbSupplementBook(t *testing.T) {
	points, err := parseVtbBook(vtbOpenFixture(t, vtbSuppBookFixture))
	if err != nil {
		t.Fatalf("parseVtbBook: %v", err)
	}
	if len(points) != 100 {
		t.Errorf("точек %d, want 100 (18 баланс + 32 P&L + 20 коэффициентов + 6 качество + 24 капитал)", len(points))
	}
	// Месячные ряды супердополнение не переписывает: потоки живут под суффиксами «(9М)»/«(3кв)».
	for _, p := range points {
		switch p.Series {
		case "Чистая прибыль", "Чистые процентные доходы", "Возврат на капитал (ROE)":
			t.Errorf("потоковой ряд %q без суффикса гранулярности — ключ (name, date) перезаписал бы квартал и накопленное", p.Series)
		}
	}
	// Ключ таблицы — (name, date): дубль в одном разборе означал бы, что накопленная и
	// квартальная колонки одной статьи встали в один ряд. Тест ловит дубль до записи.
	seen := map[string]bool{}
	for _, p := range points {
		key := p.Series + "|" + p.Date.Format(vtbTimeLayout)
		if seen[key] {
			t.Errorf("точка %q за %s пришла дважды (ключ (name, date) не переживёт дубля)", p.Series, p.Date.Format(vtbTimeLayout))
		}
		seen[key] = true
	}
	// Ряды: 9 балансовых склеек + 16 суффиксных потоков (8 × 2) + 10 коэффициентов (5 × 2) +
	// 3 качества + 2 капитала = 40 — каталог обязан описывать каждый (контракт §11.4 —
	// TestBankSeriesMetaCoversFixtures); здесь — чтобы число не разошлось с ожиданием 100 точек.
	if names := vtbSeriesNames(points); len(names) != 40 {
		t.Errorf("рядов в разборе %d, want 40", len(names))
	}
	// Spot-значения (стандарт §11.5): склейка баланса с месячными именами, суффиксные потоки,
	// качество кредитов, нормативы с датой первого числа следующего месяца, сноски «(1)»/«(2)»
	// в подписях (строки 14/16 снимаются до карты).
	for _, spot := range []struct {
		series string
		date   time.Time
		want   float64
	}{
		{series: "Кредиты и авансы клиентам (до резервов)", date: vtbSuppSep26, want: 26047.5},
		{series: "Кредиты ЮЛ", date: vtbSuppSep26, want: 19121.4},
		{series: "Итого активы", date: vtbSuppSep26, want: 39095.9},
		{series: "Итого активы", date: vtbSuppDec25, want: 36884.3},
		{series: "Средства клиентов ФЛ", date: vtbSuppSep26, want: 13904.1},
		{series: "Итого собственные средства", date: vtbSuppDec25, want: 2730.8},
		{series: "Чистая прибыль (9М)", date: vtbSuppSep26, want: 407.5},
		{series: "Чистая прибыль (9М)", date: vtbSuppSep25, want: 380.8},
		{series: "Чистая прибыль (3кв)", date: vtbSuppSep26, want: 182.3},
		{series: "Чистая прибыль (3кв)", date: vtbSuppSep25, want: 100},
		{series: "Прочие операционные доходы (9М)", date: vtbSuppSep26, want: 230.8}, // сноска «(1)» снята
		{series: "Прочие операционные доходы (9М)", date: vtbSuppSep25, want: 325.9},
		{series: "Создание резервов под кредитные убытки и прочие резервы (3кв)", date: vtbSuppSep26, want: -65.2},
		{series: "Чистая процентная маржа (NIM) (9М)", date: vtbSuppSep26, want: 0.026},
		{series: "Чистая процентная маржа (NIM) (3кв)", date: vtbSuppSep26, want: 0.027},
		{series: "Возврат на капитал (ROE) (9М)", date: vtbSuppSep26, want: 0.191},
		{series: "Стоимость риска (CoR) (3кв)", date: vtbSuppSep26, want: 0.01},
		{series: "Резерв под обесценение кредитов", date: vtbSuppSep26, want: 0.054},
		{series: "Коэффициент неработающих кредитов (NPL)", date: vtbSuppSep26, want: 0.036},
		{series: "Коэффициент покрытия неработающих кредитов", date: vtbSuppSep26, want: 1.51},
		{series: "Общий уровень (Н20.0)", date: vtbSuppOct26, want: 0.105},
		{series: "Общий уровень (Н20.0)", date: vtbSuppJan24, want: 0.106},
		{series: "Базовый капитал (Н20.1)", date: vtbSuppOct26, want: 0.071},
	} {
		if got := vtbValue(t, points, spot.series, spot.date); math.Abs(got-spot.want) > vtbSpotEps {
			t.Errorf("%q за %s = %v, want %v (значение ячейки листа)",
				spot.series, spot.date.Format(vtbTimeLayout), got, spot.want)
		}
	}
}

// TestParseVtbSupplementBookFailFast: ветки, где разбор обязан вернуть ошибку, а не «0 точек»
// (§6.4): незнакомая подпись со значениями, значение не число, «NaN», значения без подписи,
// ни одной шапки, пустой разбор. Мутации — на копии живой фикстуры в памяти (§11.1); текст
// ошибки обязан называть таблицу. Ожидания — литералами (§8.5).
func TestParseVtbSupplementBookFailFast(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(t *testing.T, f *excelize.File)
		want   string
	}{
		"подпись не в карте": {
			mutate: func(t *testing.T, f *excelize.File) {
				vtbSetCell(t, f, vtbSuppSheet, "A39", "КРЕДИТНЫЙ ПОРТФЕЛЬ 2")
			},
			want: "не найдена в карте",
		},
		"переименованная подпись с остатком в окне серийных дат": {
			// 39095.9 и 36884.3 — дробные значения в окне 2000–2100 (как серийные номера они дали
			// бы 2006 и 2000 годы): строка обязана остаться данными, а не стать датовой шапкой.
			mutate: func(t *testing.T, f *excelize.File) {
				vtbSetCell(t, f, vtbSuppSheet, "A42", "ИТОГО АКТИВЫ ГРУППЫ")
			},
			want: "не найдена в карте",
		},
		"пропала шапка балансовой секции": {
			// Датовая шапка секции пуста: строки баланса читались бы колонками-периодами
			// предыдущей секции — вид подписи и вид колонок расходятся, разбор обязан упасть.
			mutate: func(t *testing.T, f *excelize.File) {
				for _, cell := range []string{"B36", "C36", "D36"} {
					vtbSetCell(t, f, vtbSuppSheet, cell, " ")
				}
			},
			want: "не соответствует колонкам секции",
		},
		"пропала первая шапка": {
			// Значения до первой шапки: секция P&L осталась без колонок, и её точки пропали бы
			// молча — разбор обязан упасть.
			mutate: func(t *testing.T, f *excelize.File) {
				for _, cell := range []string{"B9", "C9", "E9", "F9"} {
					vtbSetCell(t, f, vtbSuppSheet, cell, " ")
				}
			},
			want: "до первой шапки",
		},
		"значение не число": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbSuppSheet, "B39", "н/д") },
			want:   "не число",
		},
		"значение не конечно": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbSuppSheet, "B39", "NaN") },
			want:   "не конечно",
		},
		"значения без подписи": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbSuppSheet, "A39", " ") },
			want:   "без подписи",
		},
		"ни одной шапки": {
			// Все пять строк-шапок опустошены целиком (периодные подписи и серийные даты):
			// секции остаются со значениями, первая же строка данных упирается в отсутствие
			// шапки — разбор обязан упасть, а не пропустить секции молча.
			mutate: func(t *testing.T, f *excelize.File) {
				for _, row := range []int{9, 24, 36, 52, 62} {
					for col := 'B'; col <= 'N'; col++ {
						vtbSetCell(t, f, vtbSuppSheet, string(col)+strconv.Itoa(row), " ")
					}
				}
			},
			want: "до первой шапки",
		},
		"пустой разбор": {
			// Все строки данных всех секций опустошены: шапки на месте, точек нет.
			mutate: func(t *testing.T, f *excelize.File) {
				for row := 12; row <= 66; row++ {
					for col := 'B'; col <= 'N'; col++ {
						vtbSetCell(t, f, vtbSuppSheet, string(col)+strconv.Itoa(row), " ")
					}
				}
			},
			want: "ни одной точки",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := vtbOpenFixture(t, vtbSuppBookFixture)
			tc.mutate(t, f)
			_, err := parseVtbBook(f)
			if err == nil {
				t.Fatal("ожидалась ошибка, а разбор прошёл")
			}
			for _, want := range []string{"rus_vtb_group_ifrs", tc.want} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не называет %q", err, want)
				}
			}
		})
	}
}

// TestParseVtbBookDispatch: диспетчеризация по листам книги — обе семьи в одной книге или ни
// одной это ошибка со списком ожидаемых листов, а не молчаливый выбор пути (§6.4, спека §4.2).
// Месячная и квартальная фикстуры поодиночке разбираются своими путями (позитивные тесты —
// TestParseVtbBook и TestParseVtbSupplementBook).
func TestParseVtbBookDispatch(t *testing.T) {
	t.Run("обе семьи в одной книге — ошибка", func(t *testing.T) {
		f := vtbOpenFixture(t, vtbBookFixture)
		if _, err := f.NewSheet(vtbSuppSheet); err != nil {
			t.Fatalf("NewSheet(%q): %v", vtbSuppSheet, err)
		}
		_, err := parseVtbBook(f)
		if err == nil {
			t.Fatal("обе семьи: ожидалась ошибка, а разбор прошёл")
		}
		for _, want := range []string{"rus_vtb_group_ifrs", "обеих семей"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ошибка %q не называет %q", err, want)
			}
		}
	})
	t.Run("ни одной семьи — ошибка со списком листов", func(t *testing.T) {
		f := excelize.NewFile()
		t.Cleanup(func() { _ = f.Close() })
		_, err := parseVtbBook(f)
		if err == nil {
			t.Fatal("ни одной семьи: ожидалась ошибка, а разбор прошёл")
		}
		for _, want := range []string{"rus_vtb_group_ifrs", "Ключевые балансовые показатели", "1.Ключевые показатели", "Sheet1"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ошибка %q не называет %q", err, want)
			}
		}
	})
	t.Run("якорь супердополнения исчез — ошибка на живой книге", func(t *testing.T) {
		f := vtbOpenFixture(t, vtbSuppBookFixture)
		vtbRenameSheet(t, f, vtbSuppSheet, "Ключевые показатели")
		_, err := parseVtbBook(f)
		if err == nil {
			t.Fatal("без якоря супердополнения: ожидалась ошибка, а разбор прошёл")
		}
		for _, want := range []string{"rus_vtb_group_ifrs", "1.Ключевые показатели"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ошибка %q не называет %q", err, want)
			}
		}
	})
}

// TestVtbSuppSeriesLabel: нормализация подписи супердополнения перед картой — NBSP и переносы,
// служебный дефис подпункта, хвостовая сноска «(N)»; «(Н20.0)», «(NIM)», «(90+)» сносками не
// считаются и остаются частью ключа.
func TestVtbSuppSeriesLabel(t *testing.T) {
	for raw, want := range map[string]string{
		"         - КРЕДИТЫ ЮРИДИЧЕСКИМ ЛИЦАМ":    "КРЕДИТЫ ЮРИДИЧЕСКИМ ЛИЦАМ",
		"ПРОЧИЕ ОПЕРАЦИОННЫЕ ДОХОДЫ (1)":          "ПРОЧИЕ ОПЕРАЦИОННЫЕ ДОХОДЫ",
		"РАСХОДЫ НА РЕЗЕРВЫ (2)":                  "РАСХОДЫ НА РЕЗЕРВЫ",
		"ОБЩИЙ УРОВЕНЬ (Н20.0)":                   "ОБЩИЙ УРОВЕНЬ (Н20.0)",
		"ЧИСТАЯ ПРОЦЕНТНАЯ МАРЖА (NIM)":           "ЧИСТАЯ ПРОЦЕНТНАЯ МАРЖА (NIM)",
		"КОЭФФИЦИЕНТ НЕРАБОТАЮЩИХ КРЕДИТОВ (90+)": "КОЭФФИЦИЕНТ НЕРАБОТАЮЩИХ КРЕДИТОВ (90+)",
		"Стоимость риска (CoR)":                   "Стоимость риска (CoR)",
		"- ":                                      "",
	} {
		if got := vtbSuppSeriesLabel(raw); got != want {
			t.Errorf("vtbSuppSeriesLabel(%q) = %q, want %q", raw, got, want)
		}
	}
}

// TestVtbSuppReportDate: дата отчётности семьи из имени файла — последний день месяца ММГГ;
// месяц вне 01–12 и чужое имя — ошибки с именем файла (§6.4).
func TestVtbSuppReportDate(t *testing.T) {
	for href, want := range map[string]time.Time{
		"/media-files/vtb.ru/sitepages/ir/statements/results/financial_data_supplement_0926_rus.xlsx": vtbSuppSep26,
		"/media-files/vtb.ru/sitepages/ir/statements/results/financial_data_supplement_1225_rus.xlsx": vtbSuppDec25,
		"/media-files/vtb.ru/sitepages/ir/statements/results/financial_data_supplement_0326_rus.xlsx": time.Date(2026, time.March, 31, 0, 0, 0, 0, time.UTC),
	} {
		got, err := vtbSuppReportDate(href)
		if err != nil {
			t.Fatalf("vtbSuppReportDate(%q): %v", href, err)
		}
		if !got.Equal(want) {
			t.Errorf("vtbSuppReportDate(%q) = %s, want %s", href, got.Format(vtbTimeLayout), want.Format(vtbTimeLayout))
		}
	}
	for _, href := range []string{
		"/media-files/vtb.ru/sitepages/ir/statements/results/financial_data_supplement_1326_rus.xlsx",
		"/media-files/vtb.ru/sitepages/ir/statements/results/financial_data_supplement_0026_rus.xlsx",
		"/media-files/vtb.ru/sitepages/ir/statements/results/financial_data_supplement_0926_rus.pdf",
	} {
		if _, err := vtbSuppReportDate(href); err == nil {
			t.Errorf("vtbSuppReportDate(%q): ожидалась ошибка, а дата разобрана", href)
		} else if !strings.Contains(err.Error(), "financial_data_supplement") {
			t.Errorf("ошибка %q не называет файл", err)
		}
	}
}

// TestVtbSuppPeriodColumns: колонки-периоды шапки секций — накопленные «9М'26» и квартальные
// «3кв'26» с датой последнего дня периода и суффиксом имени ряда; «ИЗМЕНЕНИЕ» и серийные даты
// колонками-периодами не становятся; невалидный период — ошибка с подписью (§6.4).
func TestVtbSuppPeriodColumns(t *testing.T) {
	cols, err := vtbSuppPeriodColumns(vtbSuppSheet, []string{"(млрд рублей)", "9М'26", "9М'25", "ИЗМЕНЕНИЕ", "3кв'26", "3кв'25", "ИЗМЕНЕНИЕ"})
	if err != nil {
		t.Fatalf("vtbSuppPeriodColumns(периоды): %v", err)
	}
	if len(cols) != 4 {
		t.Fatalf("периодная шапка: колонок %d, want 4 (ИЗМЕНЕНИЕ не колонка)", len(cols))
	}
	for i, want := range []struct {
		date   time.Time
		suffix string
	}{
		{vtbSuppSep26, " (9М)"},
		{vtbSuppSep25, " (9М)"},
		{vtbSuppSep26, " (3кв)"},
		{vtbSuppSep25, " (3кв)"},
	} {
		if !cols[i].date.Equal(want.date) || cols[i].suffix != want.suffix {
			t.Errorf("колонка %d = %s %q, want %s %q",
				i, cols[i].date.Format(vtbTimeLayout), cols[i].suffix, want.date.Format(vtbTimeLayout), want.suffix)
		}
	}
	// Строка без периодных подписей — не периодная шапка (датовые шапки различает вызывающий).
	for _, row := range [][]string{
		{"%", "46295", "46022", "ИЗМЕНЕНИЕ"},   // датовая шапка
		{"ИТОГО АКТИВЫ", "39095.9", "36884.3"}, // строка данных
	} {
		if got, err := vtbSuppPeriodColumns(vtbSuppSheet, row); err != nil || got != nil {
			t.Errorf("строка %q: cols=%v err=%v — не периодная шапка и не ошибка", row, got, err)
		}
	}
	// Невалидный период — ошибка (§6.4).
	if _, err := vtbSuppPeriodColumns(vtbSuppSheet, []string{"(млрд рублей)", "13М'26"}); err == nil {
		t.Error("подпись «13М'26»: ожидалась ошибка, а колонка разобрана")
	} else if !strings.Contains(err.Error(), "13М'26") {
		t.Errorf("ошибка %q не называет подпись колонки", err)
	}
}

// TestVtbSuppMapComplete: карта имён согласована внутри себя — 27 подписей (9 балансовых +
// 8 P&L + 5 коэффициентов + 3 качества + 2 капитала), имена рядов не пусты и не повторяются;
// девять балансовых целей — имена месячных балансовых рядов (это и есть склейка), базы
// суффиксных рядов с ними не совпадают (иначе вид секции в карте был бы противоречив).
func TestVtbSuppMapComplete(t *testing.T) {
	if got := len(vtbSuppSeriesMap); got != 27 {
		t.Errorf("карта: записей %d, want 27 (подписи пяти секций листа)", got)
	}
	names := make(map[string]string, len(vtbSuppSeriesMap))
	balanceTargets := 0
	for key, value := range vtbSuppSeriesMap {
		if key == "" || value.name == "" {
			t.Errorf("карта содержит пустую пару %q -> %+v", key, value)
		}
		if prev, dup := names[value.name]; dup {
			t.Errorf("имя %q задано дважды: подписи %q и %q", value.name, prev, key)
		}
		names[value.name] = key
		if !value.flow {
			if slices.Contains(vtbBalanceSeriesNames, value.name) {
				balanceTargets++
			} else if !slices.Contains([]string{
				"Резерв под обесценение кредитов",
				"Коэффициент неработающих кредитов (NPL)",
				"Коэффициент покрытия неработающих кредитов",
				"Общий уровень (Н20.0)",
				"Базовый капитал (Н20.1)",
			}, value.name) {
				t.Errorf("датовый ряд %q (подпись %q) не из месячного баланса и не новый ряд качества/капитала", value.name, key)
			}
		}
	}
	if balanceTargets != 9 {
		t.Errorf("балансовых склеек %d, want 9 (все имена — месячные ряды vtbBalanceSeriesNames)", balanceTargets)
	}
	for key, value := range vtbSuppSeriesMap {
		if value.flow && slices.Contains(vtbBalanceSeriesNames, value.name) {
			t.Errorf("поток %q (подпись %q) назван именем месячного балансового ряда — суффиксный ряд столкнулся бы с остатком", value.name, key)
		}
	}
}

// TestVtbSuppDateColumns: датой-колонкой считается только целый серийный номер в окне лет —
// дробные значения остатков (39095.9 ≈ 2006 год) датами не становятся; «ИЗМЕНЕНИЕ» и NaN/Inf —
// не колонки.
func TestVtbSuppDateColumns(t *testing.T) {
	cols := vtbSuppDateColumns([]string{"%", "46295", "46022", "39095.9", "36884.3", "ИЗМЕНЕНИЕ", "NaN"})
	if len(cols) != 2 {
		t.Fatalf("колонок-дат %d, want 2 (46295 и 46022; дробные остатки, текст и NaN — не даты)", len(cols))
	}
	if !cols[0].date.Equal(vtbSuppSep26) || !cols[1].date.Equal(vtbSuppDec25) {
		t.Errorf("даты колонок = %s, %s; want %s, %s",
			cols[0].date.Format(vtbTimeLayout), cols[1].date.Format(vtbTimeLayout),
			vtbSuppSep26.Format(vtbTimeLayout), vtbSuppDec25.Format(vtbTimeLayout))
	}
}
