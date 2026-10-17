package bank

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/xuri/excelize/v2"
)

// Фикстуры — живые файлы книги ВТБ со страницы результатов (скачаны 2026-10-17): выпуск от
// 31.08.2026 (42 247 Б, 90 точек — полный набор колонок) и два выпуска с нестандартной вёрсткой
// P&L, которую разбор обязан принимать как законный вид источника (§11.1 — артефакты коммитятся
// как есть, §11.2 — книги в десятки килобайт не урезаются):
//   - 30.04.2025 (44 509 Б): месячная колонка подписана сокращённо — «апр'2025»;
//   - 28.02.2026 (41 140 Б): месячных колонок нет вовсе («2М'2026»/«2М'2025» — накопленный
//     итог), и у балансового блока две колонки-даты вместо трёх.
const (
	vtbBookFixture          = "vtb_ifrs.xlsx"               // выпуск 31.08.2026: «август'2026»
	vtbBookApril2025Fixture = "vtb_ifrs_april_2025.xlsx"    // выпуск 30.04.2025: «апр'2025»
	vtbBookFeb2026Fixture   = "vtb_ifrs_february_2026.xlsx" // выпуск 28.02.2026: без месячных колонок
)

// Даты колонок книги (разведка 2026-10-17). Балансовый лист — колонки-остатки: август 2026 (B),
// июль 2026 (C) и «с начала года» — декабрь 2025 (E); это сырые серийные номера Excel
// 46265/46234/46022. Лист P&L — месячные колонки «август'2026» (B) и «август'2025» (C): дата
// точки — последний день месяца. Выпуск от 30.04.2025: колонки «апр'2025» (B) и «апр'2024» (C);
// выпуск от 28.02.2026: балансовые даты февраля 2026 (B) и декабря 2025 (C) — месячных колонок
// P&L у него нет.
var (
	vtbFixtureAug26 = time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC)
	vtbFixtureJul26 = time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	vtbFixtureDec25 = time.Date(2025, time.December, 31, 0, 0, 0, 0, time.UTC)
	vtbFixtureAug25 = time.Date(2025, time.August, 31, 0, 0, 0, 0, time.UTC)
	vtbFixtureApr25 = time.Date(2025, time.April, 30, 0, 0, 0, 0, time.UTC)
	vtbFixtureApr24 = time.Date(2024, time.April, 30, 0, 0, 0, 0, time.UTC)
	vtbFixtureFeb26 = time.Date(2026, time.February, 28, 0, 0, 0, 0, time.UTC)
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
func vtbOpenFixture(t *testing.T, fixture string) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile("testdata/" + fixture)
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", fixture, err)
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
// нельзя, иначе тест проверит нетронутый лист.
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
	points, err := parseVtbBook(vtbOpenFixture(t, vtbBookFixture))
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

// TestParseVtbBookReleaseVariants — два живых выпуска с нестандартной вёрсткой P&L (фикстуры —
// файлы со страницы результатов, скачаны 2026-10-17):
//   - 30.04.2025: месячная колонка подписана сокращённо («апр'2025»/«апр'2024»), а словарь
//     месяцев обязан знать сокращения (util.MonthsToNum, прецедент cbr/bop_parse.go) — разбор
//     даёт обычные 90 точек, дата месячной колонки — 30 апреля;
//   - 28.02.2026: месячных колонок нет вовсе (оба раздела подписаны накопительными
//     «2М'2026»/«2М'2025»), и у балансового блока две колонки-даты вместо трёх — это законный
//     вид выпуска (§6.6), разбор не падает: берёт балансовые точки, пропускает P&L и
//     предупреждает оператора о пропуске.
//
// Числа ожиданий — из независимой выкладки листов (unzip, 2026-10-17), не из реализации.
func TestParseVtbBookReleaseVariants(t *testing.T) {
	t.Run("сокращённый месяц «апр'2025»", func(t *testing.T) {
		points, err := parseVtbBook(vtbOpenFixture(t, vtbBookApril2025Fixture))
		if err != nil {
			t.Fatalf("parseVtbBook: %v", err)
		}
		if len(points) != 90 {
			t.Errorf("точек %d, want 90 (54 баланса + 36 P&L; «апр'2025» — та же месячная колонка, что «апрель'2025»)", len(points))
		}
		want := append(slices.Clone(vtbFixtureBalanceSeries), vtbFixturePnlSeries...)
		got := vtbSeriesNames(points)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("ряды разбора %q, want %q", got, want)
		}
		// Spot-значения: баланс апреля 2025 и месячный P&L апреля — «Чистая прибыль» 24.3
		// (а не накопленная с начала года 165.5 из колонки «4М'2025»).
		for _, spot := range []struct {
			series string
			date   time.Time
			want   float64
		}{
			{series: "Итого активы", date: vtbFixtureApr25, want: 35204.1},
			{series: "Итого собственные средства", date: vtbFixtureApr25, want: 2809},
			{series: "Чистая прибыль", date: vtbFixtureApr25, want: 24.3},
			{series: "Чистая прибыль", date: vtbFixtureApr24, want: 81},
			{series: "Возврат на капитал (ROE)", date: vtbFixtureApr25, want: 0.105},
		} {
			if got := vtbValue(t, points, spot.series, spot.date); math.Abs(got-spot.want) > vtbSpotEps {
				t.Errorf("%q за %s = %v, want %v (значение ячейки листа)",
					spot.series, spot.date.Format(vtbTimeLayout), got, spot.want)
			}
		}
	})

	t.Run("выпуск без месячных колонок P&L", func(t *testing.T) {
		hook := test.NewGlobal()
		t.Cleanup(func() { log.StandardLogger().ReplaceHooks(make(log.LevelHooks)) })
		points, err := parseVtbBook(vtbOpenFixture(t, vtbBookFeb2026Fixture))
		if err != nil {
			t.Fatalf("выпуск без месячных колонок — ожидаемый вид источника, ошибки быть не должно: %v", err)
		}
		// 18 строк баланса × 2 колонки-даты (февраль 2026 и декабрь 2025): январь в этом выпуске
		// не печатается вовсе, поэтому 36, а не 54. Ряды — только балансовые: накопительные
		// «2М'2026»/«2М'2025» месячными колонками P&L не становятся.
		if len(points) != 36 {
			t.Errorf("точек %d, want 36 (18 строк баланса × 2 колонки-даты; P&L выпуска — накопительный)", len(points))
		}
		want := slices.Clone(vtbFixtureBalanceSeries)
		got := vtbSeriesNames(points)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("ряды разбора %q, want %q (только балансовые)", got, want)
		}
		for _, spot := range []struct {
			series string
			want   float64
		}{
			{series: "Итого активы", want: 37964.3},
			{series: "Итого собственные средства", want: 2780.3},
			{series: "Средства клиентов ФЛ", want: 13609.2},
		} {
			if got := vtbValue(t, points, spot.series, vtbFixtureFeb26); math.Abs(got-spot.want) > vtbSpotEps {
				t.Errorf("%q за %s = %v, want %v (значение ячейки листа)",
					spot.series, vtbFixtureFeb26.Format(vtbTimeLayout), got, spot.want)
			}
		}
		// Пропуск P&L виден оператору: «Imported 0 rows» без предупреждения читался бы как
		// «данных нет» (стандарт §6.6).
		var warns []string
		for _, entry := range hook.AllEntries() {
			if entry.Level == log.WarnLevel {
				warns = append(warns, entry.Message)
			}
		}
		if !slices.ContainsFunc(warns, func(m string) bool { return strings.Contains(m, "нет месячных колонок") }) {
			t.Errorf("нет предупреждения о пропуске P&L (сообщения уровня warn: %q)", warns)
		}
	})
}

// TestParseVtbBookFailFast: ветки, где разбор обязан вернуть ошибку, а не «0 точек»
// (стандарт §6.4): нет любого из трёх листов, нет якоря разметки, значение не число, ячейка
// «NaN»/«Inf» (ParseFloat принимает их без ошибки, а в Float32-колонку они уедут порчей),
// значения без подписи ряда и пустой разбор. Мутации — на копии живой фикстуры в памяти
// (синтетика кодировала бы допущения автора, §11.1), а текст ошибки обязан называть таблицу.
// Подтесты «NaN — не колонка-дата» и «определение NaN/Inf — текст» — обратная сторона: в шапке
// и глоссарии такие ячейки разбор ронять не должны.
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
		"в шапке P&L нет подписей колонок": {
			// Месячных и накопительных подписей нет вовсе: шапка не опознаётся, а это уже не
			// «выпуск без месячных колонок» (тот опознаётся по накопительным «2М'2026» —
			// TestParseVtbBookReleaseVariants), а сломанная разметка (§6.4).
			mutate: func(t *testing.T, f *excelize.File) {
				for _, cell := range []string{"B6", "C6", "E6", "F6", "B16", "C16", "E16", "F16"} {
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
			f := vtbOpenFixture(t, vtbBookFixture)
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

	// «NaN» в шапке баланса — не колонка-дата: ParseFloat принимает такое значение, но датой
	// серийный номер не является, а колонка без даты точек не даёт. Разбор не падает.
	t.Run("в шапке баланса NaN — не колонка-дата", func(t *testing.T) {
		f := vtbOpenFixture(t, vtbBookFixture)
		vtbSetCell(t, f, vtbBookSheet, "B2", "NaN")
		points, err := parseVtbBook(f)
		if err != nil {
			t.Fatalf("NaN в шапке баланса — не дата, ошибки быть не должно: %v", err)
		}
		if want := 90 - len(vtbFixtureBalanceSeries); len(points) != want {
			t.Errorf("точек %d, want %d (колонка с шапкой NaN отброшена)", len(points), want)
		}
		for _, p := range points {
			if slices.Contains(vtbFixtureBalanceSeries, p.Series) && p.Date.Equal(vtbFixtureAug26) {
				t.Errorf("ряд %q: точка за %s из колонки, шапка которой «NaN»",
					p.Series, p.Date.Format(vtbTimeLayout))
			}
		}
	})

	// «NaN»/«Inf» в колонке определений — текст, а не числовая таблица: vtbFloat отвергает их как
	// нечисловое значение, поэтому строка остаётся определением, а разбор не падает.
	for _, definition := range []string{"NaN", "Inf"} {
		t.Run("в глоссарии определение "+definition+" — текст", func(t *testing.T) {
			f := vtbOpenFixture(t, vtbBookFixture)
			vtbSetCell(t, f, vtbRatiosSheet, "B2", definition)
			if _, err := parseVtbBook(f); err != nil {
				t.Fatalf("определение %q — текст, ошибки быть не должно: %v", definition, err)
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

// Фикстура — живая страница результатов ВТБ (https://www.vtb.ru/ir/statements/results/, скачана
// 2026-10-17, 357 115 Б): коммитится как есть, разметка не пересобирается (§11.2). Разведка
// 2026-10-17: 19 ссылок с якорем rus-vtb-group-ifrs-as-of — 13 xlsx и 6 pdf (pdf разбором
// отбрасываются); у двух имён хвостовое подчёркивание (31-october-2025_, 30-april-2025_), у июля
// 2025 — опечатка источника (31-jule-2025); порядок ссылок на странице — от свежих к старым и
// между прогонами меняется (2026-10-16 первыми шли март-2025, февраль-2026, апрель-2025),
// поэтому сортировка по дате отчёта — часть разбора.
const vtbResultsPageFixture = "vtb_results_page.html"

// vtbResultsPage — байты фикстуры страницы: разбор страницы чистый (§6.1) и проверяется офлайн
// на байтах живого документа (§11.3).
func vtbResultsPage(t *testing.T) []byte {
	t.Helper()
	page, err := os.ReadFile("testdata/" + vtbResultsPageFixture)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", vtbResultsPageFixture, err)
	}
	return page
}

// vtbPageWithDuplicate — копия страницы с повторённой ссылкой: дублируется реальный элемент
// разметки (вырезается по границам <a …>…</a>), а не собирается заново (§11.1). Живая страница
// дублей сейчас не несёт (все 19 href уникальны), а ветка дедупа обязана быть проверенной: с
// двух копий одного отчёта импортёр скачал бы файл дважды.
func vtbPageWithDuplicate(t *testing.T, page []byte, href string) []byte {
	t.Helper()
	at := bytes.Index(page, []byte(`href="`+href+`"`))
	if at < 0 {
		t.Fatalf("в фикстуре нет ссылки %q", href)
	}
	open := bytes.LastIndex(page[:at], []byte("<a "))
	if open < 0 {
		t.Fatalf("перед ссылкой %q не найдено открытие элемента <a", href)
	}
	closing := bytes.Index(page[at:], []byte("</a>"))
	if closing < 0 {
		t.Fatalf("у ссылки %q не найдено закрытие </a>", href)
	}
	end := at + closing + len("</a>")
	element := page[open:end]
	dup := make([]byte, 0, len(page)+len(element))
	dup = append(dup, page[:end]...)
	dup = append(dup, element...)
	return append(dup, page[end:]...)
}

// vtbFixtureLinks — 13 отчётных xlsx живой страницы в порядке возрастания даты отчётности:
// адреса — литералами (сверяется сборка «домен + href»), даты — как их даёт имя файла
// (месяц — английское название; у июля 2025 — опечатка источника jule). PDF-версии тех же
// отчётов (6 на странице) в список не входят.
var vtbFixtureLinks = []vtbLink{
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-october-2024.xlsx", Report: time.Date(2024, time.October, 31, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-30-november-2024.xlsx", Report: time.Date(2024, time.November, 30, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-30-april-2025_.xlsx", Report: time.Date(2025, time.April, 30, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-may-2025.xlsx", Report: time.Date(2025, time.May, 31, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-jule-2025.xlsx", Report: time.Date(2025, time.July, 31, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-august-2025.xlsx", Report: time.Date(2025, time.August, 31, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-october-2025_.xlsx", Report: time.Date(2025, time.October, 31, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-30-november-2025.xlsx", Report: time.Date(2025, time.November, 30, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-28-february-2026.xlsx", Report: time.Date(2026, time.February, 28, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-30-april-2026.xlsx", Report: time.Date(2026, time.April, 30, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-may-2026.xlsx", Report: time.Date(2026, time.May, 31, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-july-2026.xlsx", Report: time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)},
	{Url: "https://www.vtb.ru/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-31-august-2026.xlsx", Report: time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC)},
}

// TestVtbIfrsXlsLinks: список со страницы — только xlsx (pdf тех же отчётов отброшены), с
// дедупом по href, с абсолютными адресами и в порядке возрастания даты отчётности (порядок
// страницы другой: свежие первыми). «Грязные» имена источника разбираются: опечатка jule у
// июля 2025 и хвостовое подчёркивание у 31-october-2025_ / 30-april-2025_.
func TestVtbIfrsXlsLinks(t *testing.T) {
	page := vtbResultsPage(t)
	links, err := vtbIfrsXlsLinks(page)
	if err != nil {
		t.Fatalf("vtbIfrsXlsLinks: %v", err)
	}
	if len(links) != len(vtbFixtureLinks) {
		t.Fatalf("ссылок %d, want %d: только .xlsx (pdf отбрасываются), дедуп по href, дата отчёта из имени файла",
			len(links), len(vtbFixtureLinks))
	}
	for i, want := range vtbFixtureLinks {
		got := links[i]
		if got.Url != want.Url || !got.Report.Equal(want.Report) {
			t.Errorf("ссылка %d = %s (%s), want %s (%s)", i,
				got.Url, got.Report.Format("2006-01-02"), want.Url, want.Report.Format("2006-01-02"))
		}
	}
	// Дубль ссылки на странице — не второй отчёт: список обязан остаться прежним. Дубль —
	// копия реального элемента разметки (живая страница дублей не несёт).
	const dupHref = "/media-files/vtb.ru/sitepages/ir/statements/results/rus-vtb-group-ifrs-as-of-30-november-2024.xlsx"
	dupLinks, err := vtbIfrsXlsLinks(vtbPageWithDuplicate(t, page, dupHref))
	if err != nil {
		t.Fatalf("vtbIfrsXlsLinks с дублем ссылки: %v", err)
	}
	if len(dupLinks) != len(links) {
		t.Fatalf("с дублем ссылки разобрано %d, want %d (дедуп по href)", len(dupLinks), len(links))
	}
	for i := range dupLinks {
		if dupLinks[i].Url != links[i].Url || !dupLinks[i].Report.Equal(links[i].Report) {
			t.Errorf("с дублем ссылка %d = %s (%s), want %s (%s)", i,
				dupLinks[i].Url, dupLinks[i].Report.Format("2006-01-02"),
				links[i].Url, links[i].Report.Format("2006-01-02"))
		}
	}
}

// TestVtbIfrsXlsLinksEmpty: страница без ссылок — ошибка с доменом и ожидаемым именем файла, а не
// пустой список: пустая дельта означала бы «новых отчётов нет» при живом источнике (§6.4).
// Минимальная страница без ссылок допустима: проверяется ветка «ссылок нет», а не разметка
// источника (§11.1 касается фикстур успешных разборов).
func TestVtbIfrsXlsLinksEmpty(t *testing.T) {
	_, err := vtbIfrsXlsLinks([]byte(`<!doctype html><html><body><p>Результаты</p></body></html>`))
	if err == nil {
		t.Fatal("страница без ссылок: ожидалась ошибка, а разбор прошёл")
	}
	for _, want := range []string{"www.vtb.ru", "rus-vtb-group-ifrs-as-of"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не называет %q", err, want)
		}
	}
}

// TestVtbIfrsXlsLinksFailFast — ветки, где ссылка не разбирается: месяц имени не распознан и
// дата отчётности не существует. Правки — на копии живой фикстуры в памяти (§11.1), обе обязаны
// дать ошибку с именем файла: пропуск такой ссылки молча означал бы потерянный отчёт (§6.4).
func TestVtbIfrsXlsLinksFailFast(t *testing.T) {
	for name, tc := range map[string]struct{ from, to, want string }{
		"месяц не распознан": {from: "as-of-31-may-2026", to: "as-of-31-mayy-2026", want: "mayy"},
		"дата не существует": {from: "as-of-30-april-2026", to: "as-of-31-april-2026", want: "31-april-2026"},
	} {
		t.Run(name, func(t *testing.T) {
			page := bytes.Replace(vtbResultsPage(t), []byte(tc.from), []byte(tc.to), 1)
			_, err := vtbIfrsXlsLinks(page)
			if err == nil {
				t.Fatal("ожидалась ошибка, а разбор прошёл")
			}
			for _, want := range []string{"www.vtb.ru", tc.want} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не называет %q", err, want)
				}
			}
		})
	}
}

// TestVtbIfrsImportRestoreMode — режим восстановления у импортёра набора файлов не поддержан
// (спека §1.3): Import обязан упасть с понятным текстом до сети. Тест офлайновый ровно потому,
// что отказ стоит до первого сетевого вызова: с восстановлением в окружении вызов возвращает
// ошибку, не скачивая страницу. Клиент на случай съехавшего порядка подставлен падающий
// мгновенно — сеть из тестов запрещена (§11.3).
func TestVtbIfrsImportRestoreMode(t *testing.T) {
	t.Setenv("ARCHIVE_ACTION", "restore")
	client := util.HttpClient
	util.HttpClient = &http.Client{Timeout: time.Nanosecond}
	t.Cleanup(func() { util.HttpClient = client })

	_, err := (&VtbIfrs{}).Import(context.Background(), nil)
	if err == nil {
		t.Fatal("режим восстановления: ожидалась ошибка, а Import прошёл")
	}
	for _, want := range []string{"rus_vtb_group_ifrs", "ARCHIVE_ACTION=restore", "набора файлов"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не называет %q", err, want)
		}
	}
}

// TestVtbFloatComma: запятая — десятичный разделитель (русская локаль Excel), а смешанная
// запись «1,234.56» числом не становится: запятая не тысячное разделение.
func TestVtbFloatComma(t *testing.T) {
	got, err := vtbFloat("0,202")
	if err != nil {
		t.Fatalf("vtbFloat(\"0,202\"): %v", err)
	}
	if math.Abs(got-0.202) > vtbSpotEps {
		t.Errorf("vtbFloat(\"0,202\") = %v, want 0.202", got)
	}
	if _, err = vtbFloat("1,234.56"); err == nil {
		t.Fatal("vtbFloat(\"1,234.56\"): смешанная запись, ожидалась ошибка")
	}
}

// TestVtbSheetAmbiguous: два листа с якорем — ошибка со списком обоих. Молча выбранный первый
// лист мог бы оказаться не тем, что разбирает проход.
func TestVtbSheetAmbiguous(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	vtbRenameSheet(t, f, "Sheet1", vtbBookSheet)
	second := vtbBookSheet + "2" // 31 символ — предел excelize; якорь входит целиком
	if _, err := f.NewSheet(second); err != nil {
		t.Fatalf("NewSheet(%q): %v", second, err)
	}
	_, err := vtbSheet(f, vtbBookSheet)
	if err == nil {
		t.Fatal("два листа с якорем: ожидалась ошибка, а выбран первый")
	}
	for _, want := range []string{vtbBookSheet, second} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не перечисляет лист %q", err, want)
		}
	}
}

// TestVtbIfrsXlsLinksUppercase: месяц в имени файла разбирается без учёта регистра — источник
// может выложить файл как .../rus-vtb-group-ifrs-as-of-31-August-2026.xlsx. Якорь href остаётся
// в нижнем регистре: это конвенция имени современных файлов источника, а старые RUS-… вне T4.
func TestVtbIfrsXlsLinksUppercase(t *testing.T) {
	const name = "rus-vtb-group-ifrs-as-of-31-August-2026.xlsx"
	want := time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC)
	page := bytes.ReplaceAll(vtbResultsPage(t),
		[]byte("rus-vtb-group-ifrs-as-of-31-august-2026.xlsx"), []byte(name))
	links, err := vtbIfrsXlsLinks(page)
	if err != nil {
		t.Fatalf("vtbIfrsXlsLinks с месяцем в верхнем регистре: %v", err)
	}
	if len(links) != len(vtbFixtureLinks) {
		t.Fatalf("ссылок %d, want %d", len(links), len(vtbFixtureLinks))
	}
	found := false
	for _, link := range links {
		if strings.HasSuffix(link.Url, name) {
			found = true
			if !link.Report.Equal(want) {
				t.Errorf("ссылка %s: дата %s, want %s",
					link.Url, link.Report.Format("2006-01-02"), want.Format("2006-01-02"))
			}
		}
	}
	if !found {
		t.Errorf("ссылка %q не найдена среди %d ссылок страницы", name, len(links))
	}
}
