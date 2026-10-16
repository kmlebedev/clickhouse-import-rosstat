package bank

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// Фикстура — живой файл книги ВТБ «rus-vtb-group-ifrs-as-of-31-august-2026.xlsx» (скачан
// 2026-10-17, 42 247 Б): коммитится как есть — стандарт §11.2 разрешает не урезать книги в
// десятки килобайт, а урезание не должно менять разметку листов.
const vtbBookFixture = "vtb_ifrs.xlsx"

// Даты колонок книги (разведка 2026-10-17). Балансовый лист — колонки-остатки: август 2026 (B),
// июль 2026 (C) и «с начала года» — декабрь 2025 (E); это сырые серийные номера Excel
// 46265/46234/46022. Лист P&L — месячные колонки «август'2026» (B) и «август'2025» (C): дата
// точки — последний день месяца.
var (
	vtbFixtureAug26 = time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC)
	vtbFixtureJul26 = time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	vtbFixtureDec25 = time.Date(2025, time.December, 31, 0, 0, 0, 0, time.UTC)
	vtbFixtureAug25 = time.Date(2025, time.August, 31, 0, 0, 0, 0, time.UTC)
)

// vtbSpotEps — допуск сверки spot-значений: сырые числа источника в XML несут артефакты float64
// («71.900000000000006» вместо «71.9»), поэтому ожидание десятичной записью сверяется с допуском,
// а не равенством.
const vtbSpotEps = 1e-9

// vtbFixtureBalanceSeries — 18 рядов балансового блока (строки 3–20 листа «Ключевые балансовые
// показатели», до «Итого собственные средства» включительно; порядок источника). Имена — как их
// даёт нормализация подписей (подпункты « -  Кредиты ЮЛ» теряют служебный дефис): те же 18 имён у
// таблицы базовой линии от 2026-10-16.
var vtbFixtureBalanceSeries = []string{
	"Денежные средства и краткосрочные активы",
	"Финансовые инструменты",
	"Средства в банках",
	"Кредиты и авансы клиентам (до резервов)",
	"Кредиты ЮЛ",
	"Кредиты ФЛ",
	"Резервы под обесценение",
	"Прочие активы",
	"Итого активы",
	"Средства банков и прочие заемные средства",
	"Средства клиентов",
	"Средства клиентов ЮЛ",
	"Средства клиентов ФЛ",
	"Выпущенные долговые ценные бумаги",
	"Субординированная задолженность",
	"Прочие обязательства",
	"Итого обязательства",
	"Итого собственные средства",
}

// vtbFixturePnlSeries — 18 рядов листа «Ключевые показатели P&L»: 9 строк отчёта о прибылях и
// убытках (строки 7–15) и 9 строк «Показатели эффективности» (строки 17–25), порядок источника.
// Подписи подпунктов (« -  CoR ЮЛ») нормализуются в «CoR ЮЛ».
var vtbFixturePnlSeries = []string{
	"Чистые процентные доходы",
	"Чистые комиссионные доходы",
	"Прочие операционные доходы",
	"ЧОД до резервов",
	"Создание резервов под кредитные убытки и прочие резервы",
	"Расходы на содержание персонала и административные расходы",
	"Прибыль до налогообложения",
	"Расходы по налогу на прибыль",
	"Чистая прибыль",
	"Чистая процентная маржа (NIM)",
	"Чистая комиссионная маржа (NCM)",
	"Стоимость риска (CoR)",
	"CoR ЮЛ",
	"CoR ФЛ",
	"Расходы / Доходы (CIR)",
	"Расходы / Активы",
	"Возврат на активы (ROA)",
	"Возврат на капитал (ROE)",
}

// vtbOpenFixture открывает книгу-фикстуру: разбор получает *excelize.File — тот же вход, что у
// ImportFunc шаблона (стандарт §6.1), поэтому тест открывает файл сам и в сеть не ходит (§11.3).
func vtbOpenFixture(t *testing.T) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile("testdata/" + vtbBookFixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", vtbBookFixture, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// vtbRenameSheet — переименование листа копии фикстуры: ветка «в книге нет листа» проверяется на
// живой разметке остальных листов, а не на синтетической книге (§11.1).
func vtbRenameSheet(t *testing.T, f *excelize.File, from, to string) {
	t.Helper()
	if err := f.SetSheetName(from, to); err != nil {
		t.Fatalf("SetSheetName(%q, %q): %v", from, to, err)
	}
}

// vtbSetCell — правка ячейки копии фикстуры для негативных веток: ошибку excelize глотать
// нельзя, иначе тест проверит немутнутый лист.
func vtbSetCell(t *testing.T, f *excelize.File, sheet, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(sheet, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s!%s): %v", sheet, cell, err)
	}
}

// vtbSeriesNames — имена рядов разбора без повторов, в порядке первого появления.
func vtbSeriesNames(points []vtbPoint) []string {
	var names []string
	seen := map[string]bool{}
	for _, p := range points {
		if !seen[p.Series] {
			seen[p.Series] = true
			names = append(names, p.Series)
		}
	}
	return names
}

// vtbValue — значение точки (series, date); отсутствие точки — фатальная ошибка теста.
func vtbValue(t *testing.T, points []vtbPoint, series string, date time.Time) float64 {
	t.Helper()
	for _, p := range points {
		if p.Series == series && p.Date.Equal(date) {
			return p.Value
		}
	}
	t.Fatalf("в разборе нет точки %q за %s", series, date.Format(vtbTimeLayout))
	return 0
}

// TestParseVtbBook: книга даёт точки трёх проходов — балансового (до «Итого собственные средства»
// включительно, как читал прежний разбор), листа P&L (оба раздела целиком) и листа
// «Показатели и коэффициенты». Числа ожиданий — из независимой выкладки листов (unzip
// sharedStrings.xml + styles.xml + worksheets, 2026-10-17), а не из реализации. Точки: 18 строк
// баланса × 3 колонки-даты = 54 плюс 18 строк P&L × 2 месячные колонки = 36; лист
// «Показатели и коэффициенты» в живом файле — глоссарий без значений и дат, точек с него нет
// (разбор прохода — parseVtbRatiosSheet).
func TestParseVtbBook(t *testing.T) {
	points, err := parseVtbBook(vtbOpenFixture(t))
	if err != nil {
		t.Fatalf("parseVtbBook: %v", err)
	}
	if len(points) != 90 {
		t.Errorf("точек %d, want 90 (54 баланса + 36 P&L; лист коэффициентов — глоссарий без значений)", len(points))
	}
	// Ряды — обе стороны: ни пропущенных, ни лишних (18 балансовых + 18 P&L).
	want := append(slices.Clone(vtbFixtureBalanceSeries), vtbFixturePnlSeries...)
	got := vtbSeriesNames(points)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("ряды разбора %q, want %q", got, want)
	}
	// Ключ таблицы — (name, date): дубль в одном разборе означал бы, что один и тот же месяц
	// приехал дважды (например, месячная колонка P&L и накопленная «8М'2026» одной подписи) —
	// ReplacingMergeTree оставил бы из них одно значение молча. Тест ловит дубль до записи.
	seen := map[string]bool{}
	for _, p := range points {
		key := p.Series + "|" + p.Date.Format(vtbTimeLayout)
		if seen[key] {
			t.Errorf("точка %q за %s пришла дважды (ключ (name, date) не переживёт дубля)",
				p.Series, p.Date.Format(vtbTimeLayout))
		}
		seen[key] = true
	}
	// Даты — по блокам: балансовые ряды несут три колонки-остатка, ряды P&L — две месячные.
	balanceDates := []time.Time{vtbFixtureAug26, vtbFixtureJul26, vtbFixtureDec25}
	pnlDates := []time.Time{vtbFixtureAug26, vtbFixtureAug25}
	for _, p := range points {
		switch {
		case slices.Contains(vtbFixtureBalanceSeries, p.Series) && !slices.Contains(balanceDates, p.Date):
			t.Errorf("ряд %q: дата %s, want одну из %v (колонки баланса)",
				p.Series, p.Date.Format(vtbTimeLayout), fmtDates(balanceDates))
		case slices.Contains(vtbFixturePnlSeries, p.Series) && !slices.Contains(pnlDates, p.Date):
			t.Errorf("ряд %q: дата %s, want одну из %v (месячные колонки P&L)",
				p.Series, p.Date.Format(vtbTimeLayout), fmtDates(pnlDates))
		}
	}
	// Spot-значения (стандарт §11.5): балансовый блок и оба раздела листа P&L; лист
	// коэффициентов значений не несёт — спот с него невозможен (см. parseVtbRatiosSheet).
	// «Чистая прибыль» августа 2026 — месячное 49.2, а не накопленное 314.8 из «8М'2026».
	for _, spot := range []struct {
		series string
		date   time.Time
		want   float64
	}{
		{series: "Итого активы", date: vtbFixtureAug26, want: 39041.5},
		{series: "Итого собственные средства", date: vtbFixtureAug26, want: 2890.8},
		{series: "Средства клиентов ФЛ", date: vtbFixtureDec25, want: 14018.2},
		{series: "Чистая прибыль", date: vtbFixtureAug26, want: 49.2},
		{series: "Чистая прибыль", date: vtbFixtureAug25, want: 23.8},
		{series: "Возврат на активы (ROA)", date: vtbFixtureAug26, want: 0.015},
		{series: "Возврат на капитал (ROE)", date: vtbFixtureAug26, want: 0.202},
		{series: "Чистая процентная маржа (NIM)", date: vtbFixtureAug25, want: 0.013},
	} {
		if got := vtbValue(t, points, spot.series, spot.date); math.Abs(got-spot.want) > vtbSpotEps {
			t.Errorf("%q за %s = %v, want %v (значение ячейки листа)",
				spot.series, spot.date.Format(vtbTimeLayout), got, spot.want)
		}
	}
}

// fmtDates — даты для сообщений об ошибке.
func fmtDates(dates []time.Time) []string {
	out := make([]string, 0, len(dates))
	for _, d := range dates {
		out = append(out, d.Format(vtbTimeLayout))
	}
	return out
}

// TestParseVtbBookFailFast: ветки, где разбор обязан вернуть ошибку, а не «0 точек»
// (стандарт §6.4): нет любого из трёх листов, нет якоря разметки, значение не число, ячейка
// «NaN»/«Inf» (ParseFloat принимает их без ошибки, а в Float32-колонку они уедут порчей),
// значения без подписи ряда и пустой разбор. Мутации — на копии живой фикстуры в памяти
// (синтетика кодировала бы допущения автора, §11.1), а текст ошибки обязан называть таблицу.
func TestParseVtbBookFailFast(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(t *testing.T, f *excelize.File)
		want   string
	}{
		"нет листа P&L": {
			mutate: func(t *testing.T, f *excelize.File) { vtbRenameSheet(t, f, vtbPnlSheet, "Показатели PL") },
			want:   "Ключевые показатели P&L",
		},
		"нет балансового листа": {
			mutate: func(t *testing.T, f *excelize.File) { vtbRenameSheet(t, f, vtbBookSheet, "Баланс группы") },
			want:   "Ключевые балансовые показатели",
		},
		"нет листа коэффициентов": {
			mutate: func(t *testing.T, f *excelize.File) { vtbRenameSheet(t, f, vtbRatiosSheet, "Коэффициенты") },
			want:   "Показатели и коэффициенты",
		},
		"нет первой строки баланса": {
			mutate: func(t *testing.T, f *excelize.File) {
				vtbSetCell(t, f, vtbBookSheet, "A3", "Денежные средства")
			},
			want: "Денежные средства и краткосрочные активы",
		},
		"нет строки «Итого собственные средства»": {
			mutate: func(t *testing.T, f *excelize.File) {
				vtbSetCell(t, f, vtbBookSheet, "A20", "Итого собственный капитал")
			},
			want: "Итого собственные средства",
		},
		"в шапке баланса нет колонок с датами": {
			mutate: func(t *testing.T, f *excelize.File) {
				vtbSetCell(t, f, vtbBookSheet, "B2", "август 2026")
				vtbSetCell(t, f, vtbBookSheet, "C2", "июль 2026")
				vtbSetCell(t, f, vtbBookSheet, "E2", "декабрь 2025")
			},
			want: "колонок с датами",
		},
		"значение баланса без подписи ряда": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbBookSheet, "A4", " ") },
			want:   "без подписи",
		},
		"значение баланса не число": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbBookSheet, "B3", "н/д") },
			want:   "не число",
		},
		"балансовый блок без значений": {
			mutate: func(t *testing.T, f *excelize.File) {
				for row := 3; row <= 20; row++ {
					for _, col := range []string{"B", "C", "E"} {
						vtbSetCell(t, f, vtbBookSheet, fmt.Sprintf("%s%d", col, row), " ")
					}
				}
			},
			want: "ни одной точки",
		},
		"в шапке P&L нет месячных колонок": {
			mutate: func(t *testing.T, f *excelize.File) {
				for _, cell := range []string{"B6", "C6", "B16", "C16"} {
					vtbSetCell(t, f, vtbPnlSheet, cell, "изменение")
				}
			},
			want: "шапка",
		},
		"месяц в шапке P&L не распознан": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbPnlSheet, "B6", "авгус'2026") },
			want:   "авгус",
		},
		"значение P&L не число": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbPnlSheet, "B7", "н/д") },
			want:   "не число",
		},
		"значение P&L без подписи ряда": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbPnlSheet, "A7", " ") },
			want:   "без подписи",
		},
		"значение P&L не конечно": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbPnlSheet, "B7", "NaN") },
			want:   "не конечно",
		},
		"в глоссарии нет заголовка листа": {
			mutate: func(t *testing.T, f *excelize.File) {
				vtbSetCell(t, f, vtbRatiosSheet, "A1", "Коэффициенты")
			},
			want: "заголовок",
		},
		"в глоссарии нет определений": {
			mutate: func(t *testing.T, f *excelize.File) {
				for row := 2; row <= 13; row++ {
					vtbSetCell(t, f, vtbRatiosSheet, fmt.Sprintf("B%d", row), " ")
				}
			},
			want: "определение",
		},
		"в глоссарии число вместо определения": {
			mutate: func(t *testing.T, f *excelize.File) { vtbSetCell(t, f, vtbRatiosSheet, "B2", "0.202") },
			want:   "ожидается текст определения",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := vtbOpenFixture(t)
			tc.mutate(t, f)
			_, err := parseVtbBook(f)
			if err == nil {
				t.Fatal("ожидалась ошибка, а разбор прошёл")
			}
			// Ожидания — литералами, не константами импортёра (§8.5): ссылка на константу
			// сделала бы проверку тавтологичной и прошла бы ровно на той опечатке, от которой
			// обязана падать.
			for _, want := range []string{"rus_vtb_group_ifrs", tc.want} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не называет %q", err, want)
				}
			}
		})
	}
}

// TestAppendVtbPoints: запись идёт по одному Append на точку в форме (Series, Date, Value) —
// схема таблицы (name, date, balance) — и значение приводится к точности Float32-колонки
// (legacy-схема, спека §4.5). Ошибка Append не проглатывается (§5.5): расхождение схемы обязано
// вернуться вызывающему, а не пропасть в логе.
func TestAppendVtbPoints(t *testing.T) {
	points := []vtbPoint{
		{Series: "Чистая прибыль", Date: vtbFixtureAug26, Value: 49.2},
		{Series: "Итого активы", Date: vtbFixtureAug26, Value: 39041.5},
	}
	rec := newAppendRecorder("name", "date", "balance")
	if err := appendVtbPoints(rec, points); err != nil {
		t.Fatalf("appendVtbPoints: %v", err)
	}
	if len(rec.rows) != len(points) {
		t.Fatalf("записано строк %d, want %d", len(rec.rows), len(points))
	}
	for i, want := range points {
		got := rec.rows[i]
		if got.name != want.Series || !got.date.Equal(want.Date) || got.value != float64(float32(want.Value)) {
			t.Errorf("строка %d = %+v, want (Series, Date, Float32-значение) = %q, %s, %v",
				i, got, want.Series, want.Date.Format(vtbTimeLayout), float64(float32(want.Value)))
		}
	}
	// Чужая схема приёмника — ошибка, которую разбор обязан вернуть вызывающему.
	err := appendVtbPoints(newAppendRecorder("name", "date"), points)
	if err == nil {
		t.Fatal("Append с чужой схемой: ожидалась ошибка, а она проглочена")
	}
	if !strings.Contains(err.Error(), "Чистая прибыль") {
		t.Errorf("ошибка %q не называет точку, на которой запись остановилась", err)
	}
}
