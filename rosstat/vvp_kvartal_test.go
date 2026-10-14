package rosstat

import (
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// openVvpKvartalFixture открывает реальную выгрузку Росстата (testdata/vvp_kvartal.xlsx):
// книга из 16 листов, из которых ряд собирают пять («2», «9», «10», «12», «14»), — и именно
// в той разметке, на которой прежний разбор падал в продакшене: заголовок листа лежит в A2
// (объединённая A2:D2, поэтому строка заголовка состоит из одной ячейки), годы — в колонках
// A, E, I, … (у поздних групп со сноской: «20262)»), подписи кварталов и данные — с колонки A.
func openVvpKvartalFixture(t *testing.T) *excelize.File {
	t.Helper()
	xlsx, err := excelize.OpenFile("testdata/vvp_kvartal.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = xlsx.Close() })
	return xlsx
}

// importVvpKvartalFixture прогоняет импортёр по фикстуре и отдаёт записанные точки:
// ни базы, ни сети тесту не нужно (§11.6).
func importVvpKvartalFixture(t *testing.T) []appendedRow {
	t.Helper()
	rec := &appendRecorder{}
	if err := vvpKvartalImport(openVvpKvartalFixture(t), rec); err != nil {
		t.Fatalf("vvpKvartalImport: %v", err)
	}
	return rec.rows
}

// countBySeries считает точки каждого ряда.
func countBySeries(rows []appendedRow) map[string]int {
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		counts[row.series]++
	}
	return counts
}

// seriesDates отдаёт даты точек одного ряда в порядке записи.
func seriesDates(rows []appendedRow, series string) []time.Time {
	dates := make([]time.Time, 0, len(rows))
	for _, row := range rows {
		if row.series == series {
			dates = append(dates, row.date)
		}
	}
	return dates
}

// vvpKvartalSheetFixtureRows — минимальный лист в разметке выгрузки: заголовок в A2, годы в
// A и E, подписи кварталов и данные с колонки A. Прежний разбор на такой разметке падал:
// строку заголовка он читал как rows[1][1], а в объединённой A2:D2 она одноячейковая.
func vvpKvartalSheetFixtureRows() [][]any {
	return [][]any{
		{"К содержанию"},
		{"Валовой внутренний продукт 1)  \n(в текущих ценах, млрд.руб.)"},
		{"2011", "", "", "", "2012"},
		{"I квартал", "II квартал", "III квартал", "IV квартал", "I квартал"},
		{"100", "101", "102", "103", "104"},
	}
}

func TestVvpKvartalReadsFixture(t *testing.T) {
	rows := importVvpKvartalFixture(t)
	// 302 точки: листы «2», «9», «10» — по 62 (2011-I … 2026-II), листы «12», «14» — по 58
	// (2012-I … 2026-II). Числа точные: выгрузка полная, пропусков в ней нет. Пропуск нулевой
	// колонки срезал бы начало каждой группы, а непрочитанный год — все кварталы, кроме I.
	if len(rows) != 302 {
		t.Fatalf("точек %d, ожидалось 302", len(rows))
	}
	counts := countBySeries(rows)
	if len(counts) != 5 {
		t.Fatalf("рядов %d, ожидалось 5", len(counts))
	}
	want := map[string]int{
		"2 Валовой внутрений продукт":                                 62,
		"9 Валовой внутрений продукт":                                 62,
		"10 Валовой внутренний продукт":                               62,
		"12 Индексы физического объема валового внутреннего продукта": 58,
		"14 Индексы - дефляторы валового внутреннего продукта":        58,
	}
	for name, n := range want {
		if counts[name] != n {
			t.Errorf("ряд %q: точек %d, ожидалось %d", name, counts[name], n)
		}
	}
	// Границы рядов: первая точка — I квартал первого года, последняя — II квартал 2026.
	// Проверяются оба конца: II–IV кварталы групп подписями дат не помечены, их год держится
	// только переносом года по колонкам. Лист «14» — сюда же: его последняя группа помечена
	// источником повторной меткой «20252)», год выведен из структуры таблицы (2026), и все 58
	// точек ряда различны — подробности в TestVvpKvartalRepeatedYearLabelAdvances.
	for _, tt := range []struct {
		series string
		first  string
		last   string
	}{
		{"2 Валовой внутрений продукт", "2011-04-01", "2026-07-01"},
		{"9 Валовой внутрений продукт", "2011-04-01", "2026-07-01"},
		{"10 Валовой внутренний продукт", "2011-04-01", "2026-07-01"},
		{"12 Индексы физического объема валового внутреннего продукта", "2012-04-01", "2026-07-01"},
		{"14 Индексы - дефляторы валового внутреннего продукта", "2012-04-01", "2026-07-01"},
	} {
		dates := seriesDates(rows, tt.series)
		if len(dates) == 0 {
			t.Errorf("ряд %q не найден", tt.series)
			continue
		}
		first, last := dates[0], dates[0]
		for _, date := range dates {
			if date.Before(first) {
				first = date
			}
			if date.After(last) {
				last = date
			}
		}
		if got := first.Format("2006-01-02"); got != tt.first {
			t.Errorf("ряд %q: первая точка %s, ожидалась %s", tt.series, got, tt.first)
		}
		if got := last.Format("2006-01-02"); got != tt.last {
			t.Errorf("ряд %q: последняя точка %s, ожидалась %s", tt.series, got, tt.last)
		}
	}
}

// TestVvpKvartalKeepsFirstGroup — I квартал 2011 — первая точка ряда листа «2», и она лежит в
// нулевой колонке листа. Прежний разбор обходил данные как rows[4][1:], теряя её и начало
// каждой следующей группы (та группа начинается после года в колонке A, E, I, …).
func TestVvpKvartalKeepsFirstGroup(t *testing.T) {
	const series = "2 Валовой внутрений продукт"
	rows := importVvpKvartalFixture(t)
	var first time.Time
	for _, date := range seriesDates(rows, series) {
		if first.IsZero() || date.Before(first) {
			first = date
		}
	}
	want := time.Date(2011, 4, 1, 0, 0, 0, 0, time.UTC)
	if !first.Equal(want) {
		t.Fatalf("первая точка ряда %q — %s, ожидалась %s", series, first.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

// TestVvpKvartalSpotValue — ячейка A5 листа «2» хранит 13024.799539024814, и разбор обязан
// отдать это сырое значение целиком (§6.2), а не форматированный текст ячейки «13,024.8»
// (допуск 1e-6 их различает). Проверка идёт по результату разбора, а не по батчу: колонка
// таблицы — Float32, и этот же допуск к float32 неприменим (шаг представления там ~1e-3).
func TestVvpKvartalSpotValue(t *testing.T) {
	table, err := parseVvpKvartal(openVvpKvartalFixture(t))
	if err != nil {
		t.Fatalf("parseVvpKvartal: %v", err)
	}
	const want = 13024.799539024814
	date := time.Date(2011, 4, 1, 0, 0, 0, 0, time.UTC)
	found := false
	for _, row := range table {
		if row.name != "2 Валовой внутрений продукт" || !row.date.Equal(date) {
			continue
		}
		found = true
		if math.Abs(row.vvp-want) > 1e-6 {
			t.Errorf("%s: значение %v, ожидалось %v", date.Format("2006-01-02"), row.vvp, want)
		}
	}
	if !found {
		t.Fatalf("точка %s ряда «2» не найдена", date.Format("2006-01-02"))
	}
}

// TestVvpKvartalSeriesMetaCoversParsedRows — двусторонний контракт каталога (§11.4): имя ряда
// объявлено кодом (§6.8, vvpKvartalSeriesNames) и обязано совпадать с каталогом дословно, а
// каждый описанный ряд — разбираться. Имя в каталоге записано литералом, поэтому сравнение не
// тавтологично: опечатка в карте («внутрений»/«внутренний») или лишняя строка каталога его
// ломают.
func TestVvpKvartalSeriesMetaCoversParsedRows(t *testing.T) {
	rows := importVvpKvartalFixture(t)
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.series)
	}
	requireParsedDescribed(t, names, describedSeries(t, vvpKvartalSeriesMeta))
}

// TestVvpKvartalMissingSheetFails — fail-fast (§6.4): книга без ожидаемого листа — ошибка с
// именем листа и таблицы, а не «ноль точек» и не тихий пропуск. Разбор идёт целиком до первой
// записи, поэтому батч обязан остаться пустым: частичная запись книги выглядела бы успешным
// импортом с неполным рядом.
func TestVvpKvartalMissingSheetFails(t *testing.T) {
	f := newTestWorkbook(t, "2", "10", "12", "14")
	for _, sheet := range []string{"2", "10", "12", "14"} {
		setSheetRows(t, f, sheet, vvpKvartalSheetFixtureRows())
	}
	rec := &appendRecorder{}
	err := vvpKvartalImport(f, rec)
	if err == nil {
		t.Fatal("книга без листа «9»: ожидалась ошибка, получен nil")
	}
	if !strings.Contains(err.Error(), `"9"`) {
		t.Errorf("в ошибке нет имени листа «9»: %v", err)
	}
	if !strings.Contains(err.Error(), rosstatVVPKvartalTable) {
		t.Errorf("в ошибке нет имени таблицы %s: %v", rosstatVVPKvartalTable, err)
	}
	if len(rec.rows) != 0 {
		t.Errorf("в батч записано %d строк, ожидалось 0", len(rec.rows))
	}
}

// TestVvpKvartalUnknownQuarterFails — прежний getQuarterDate на незнакомой подписи вызывал
// panic("Invalid quarter") и убивал прогон целиком; разбор обязан вернуть ошибку (§6.4).
// Строку подписей разбор распознаёт по первой ячейке, поэтому испорченная подпись стоит в
// следующей колонке — ровно там, где её читает обход.
func TestVvpKvartalUnknownQuarterFails(t *testing.T) {
	f := newTestWorkbook(t, "2")
	setSheetRows(t, f, "2", [][]any{
		{"К содержанию"},
		{"Валовой внутренний продукт 1)  \n(в текущих ценах, млрд.руб.)"},
		{"2011", "", "", "", "2012"},
		{"I квартал", "X квартал"},
		{"100", "101"},
	})
	rec := &appendRecorder{}
	err := vvpKvartalImport(f, rec)
	if err == nil {
		t.Fatal("подпись «X квартал»: ожидалась ошибка, получен nil (паника была бы убийством прогона)")
	}
	if !strings.Contains(err.Error(), "X") {
		t.Errorf("в ошибке нет подписи квартала: %v", err)
	}
	if len(rec.rows) != 0 {
		t.Errorf("в батч записано %d строк, ожидалось 0", len(rec.rows))
	}
}

// TestVvpKvartalFindLink — селектор проверяется на HTML-фикстуре страницы национальных счётов
// тем же util.FindLink, что и в Import() (§11.3): ссылка на файл ищется на странице, а не
// собирается из угаданного имени VVP_kvartal_s-1995-ГГГГ.xlsx, которое меняется при обновлении.
func TestVvpKvartalFindLink(t *testing.T) {
	page, err := os.ReadFile("testdata/accounts_page.html")
	if err != nil {
		t.Fatal(err)
	}
	href, err := util.FindLink(page, vvpKvartalLinkSelector)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(href, "VVP_kvartal") {
		t.Errorf("href %q не содержит VVP_kvartal", href)
	}
}

// TestVvpKvartalCarriesYearAcrossDataGroups — год выписан только в первой колонке группы
// (A, E, I, …), и кварталы II–IV обязаны брать его из предыдущей колонки. Регрессия чтения
// года со сдвигом (прежний разбор читал rows[2][i+1]) сдвинула бы год группы и дала бы
// 2011-04-01 вместо 2012-04-01 на I квартале следующего года.
func TestVvpKvartalCarriesYearAcrossDataGroups(t *testing.T) {
	f := newTestWorkbook(t, "2")
	setSheetRows(t, f, "2", [][]any{
		{"К содержанию"},
		{"Валовой внутренний продукт 1)  \n(в текущих ценах, млрд.руб.)"},
		{"2011", "", "", "", "2012", "", "", ""},
		{"I квартал", "II квартал", "III квартал", "IV квартал", "I квартал", "II квартал", "III квартал", "IV квартал"},
		{"1", "2", "3", "4", "5", "6", "7", "8"},
	})
	points, err := parseVvpKvartalSheet(f, "2")
	if err != nil {
		t.Fatalf("parseVvpKvartalSheet: %v", err)
	}
	want := []string{
		"2011-04-01", "2011-07-01", "2011-10-01", "2012-01-01",
		"2012-04-01", "2012-07-01", "2012-10-01", "2013-01-01",
	}
	if len(points) != len(want) {
		t.Fatalf("точек %d, ожидалось %d", len(points), len(want))
	}
	for i, point := range points {
		if got := point.date.Format("2006-01-02"); got != want[i] {
			t.Errorf("точка %d: дата %s, ожидалась %s", i+1, got, want[i])
		}
	}
}

// TestVvpKvartalRepeatedYearLabelAdvances — в выгрузке строка годов листа «14» помечает
// последнюю группу повторной меткой «20252)»: та же метка, что у предыдущей группы (колонки 53 и
// 57; колонка 57 — I квартал, дальше колонки пусты). У листов «2», «9», «10» того же выпуска
// метки правильные — «20242)» в колонке 53, «20252)» в 57, «20262)» в 61, у листа «12» —
// «20262)» в 57. Принять метку на веру нельзя: значения 2026-I и 2026-II легли бы на даты 2025-I
// и 2025-II и затёрли настоящие значения 2025 года — в таблице с ключом (name, date) это тихая
// подмена данных, а не пропуск. Год выводится из структуры таблицы (после IV квартала группы идёт
// I квартал следующего года), поэтому у ряда 58 различных дат, а не 56. Вывод законен только на
// I квартале: повтор метки на II–IV — ошибка, а не сдвиг (TestVvpKvartalSheetErrorBranches).
func TestVvpKvartalRepeatedYearLabelAdvances(t *testing.T) {
	const series = "14 Индексы - дефляторы валового внутреннего продукта"

	dates := seriesDates(importVvpKvartalFixture(t), series)
	if len(dates) != 58 {
		t.Fatalf("точек %d, ожидалось 58", len(dates))
	}
	distinct := make(map[string]bool, len(dates))
	for _, date := range dates {
		distinct[date.Format("2006-01-02")] = true
	}
	if len(distinct) != 58 {
		t.Errorf("различающихся дат %d, ожидалось 58: повторная метка года не выводится", len(distinct))
	}
	// Порядок точек — порядок записи: последние две — I и II кварталы выведенного 2026 года.
	tail := []string{dates[len(dates)-2].Format("2006-01-02"), dates[len(dates)-1].Format("2006-01-02")}
	if want := [2]string{"2026-04-01", "2026-07-01"}; tail[0] != want[0] || tail[1] != want[1] {
		t.Errorf("последние две даты %v, ожидались %v", tail, want)
	}

	// Значения сверяются по разбору: колонка таблицы — Float32, и допуск 1e-6 к ней неприменим.
	table, err := parseVvpKvartal(openVvpKvartalFixture(t))
	if err != nil {
		t.Fatalf("parseVvpKvartal: %v", err)
	}
	for _, spot := range []struct {
		date string
		want float64
	}{
		// Колонка 53 — I квартал 2025 (106.76184319561411) и колонка 57 — I квартал 2026
		// (104.16029833229091): обе точки обязаны существовать каждая со своим значением.
		{date: "2025-04-01", want: 106.7618431956141},
		{date: "2026-04-01", want: 104.1602983322909},
	} {
		found := 0
		for _, row := range table {
			if row.name != series || row.date.Format("2006-01-02") != spot.date {
				continue
			}
			found++
			if math.Abs(row.vvp-spot.want) > 1e-6 {
				t.Errorf("%s: значение %v, ожидалось %v", spot.date, row.vvp, spot.want)
			}
		}
		if found != 1 {
			t.Errorf("точек на %s — %d, ожидалась 1", spot.date, found)
		}
	}
}

// TestVvpKvartalSheetErrorBranches — ветки ошибок разбора листа, которых в полной выгрузке нет
// (§6.4): без них разбор молча записал бы точку без года, мусор вместо значения или прочитал
// бы ячейку мимо среза. Книга у каждого случая своя — проверяется ровно одна ветка.
func TestVvpKvartalSheetErrorBranches(t *testing.T) {
	const title = "Валовой внутренний продукт 1)  \n(в текущих ценах, млрд.руб.)"
	labels := []any{"I квартал", "II квартал", "III квартал", "IV квартал"}
	tests := []struct {
		name string
		rows [][]any
		want string
	}{
		{
			name: "строка подписей кварталов не найдена",
			rows: [][]any{{"К содержанию"}, {title}, {"2011"}, {"100"}},
			want: "подписей кварталов не найдена",
		},
		{
			name: "строка годов отсутствует",
			rows: [][]any{{"I квартал", "II квартал"}, {"100", "101"}},
			want: "строки годов нет",
		},
		{
			name: "строка данных отсутствует",
			rows: [][]any{{"К содержанию"}, {title}, {"2011", "", "", ""}, labels},
			want: "строки данных нет",
		},
		{
			name: "год данных не задан",
			rows: [][]any{{"К содержанию"}, {title}, {"", "", "", "", "2012"}, labels, {"100", "101", "102", "103", "104"}},
			want: "год данных не задан",
		},
		{
			name: "ячейка года короче четырёх цифр",
			rows: [][]any{{"К содержанию"}, {title}, {"20", "", "", ""}, labels, {"100", "101", "102", "103"}},
			want: `"20"`,
		},
		{
			name: "год не число",
			rows: [][]any{{"К содержанию"}, {title}, {"20хх", "", "", ""}, labels, {"100", "101", "102", "103"}},
			want: "20хх",
		},
		{
			// Повтор метки года на II квартале выводу не подлежит: сдвиг вперёд записал бы
			// значения группы под чужой год (см. TestVvpKvartalRepeatedYearLabelAdvances).
			name: "повтор метки года на квартале II",
			rows: [][]any{
				{"К содержанию"},
				{title},
				{"2011", "", "", "", "", "2011"},
				{"I квартал", "II квартал", "III квартал", "IV квартал", "I квартал", "II квартал"},
				{"100", "101", "102", "103", "104", "105"},
			},
			want: "повтор метки года 2011",
		},
		{
			name: "значение не число",
			rows: [][]any{{"К содержанию"}, {title}, {"2011", "", "", ""}, labels, {"100", "нет", "", ""}},
			want: "не число",
		},
		{
			name: "значение не конечное число",
			rows: [][]any{{"К содержанию"}, {title}, {"2011", "", "", ""}, labels, {"NaN", "101", "102", "103"}},
			want: "не конечное число",
		},
		{
			name: "подписей меньше, чем данных",
			rows: [][]any{{"К содержанию"}, {title}, {"2011", "", "", ""}, {"I квартал"}, {"100", "101"}},
			want: "подпис",
		},
		{
			name: "точек нет",
			rows: [][]any{{"К содержанию"}, {title}, {"2011", "", "", ""}, labels, {" "}},
			want: "0 точек",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTestWorkbook(t, "2")
			setSheetRows(t, f, "2", tt.rows)
			points, err := parseVvpKvartalSheet(f, "2")
			if err == nil {
				t.Fatalf("ожидалась ошибка, получено %d точек", len(points))
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("в ошибке нет %q: %v", tt.want, err)
			}
			if !strings.Contains(err.Error(), rosstatVVPKvartalTable) {
				t.Errorf("в ошибке нет имени таблицы %s: %v", rosstatVVPKvartalTable, err)
			}
			if len(points) != 0 {
				t.Errorf("при ошибке отдано %d точек, ожидалось 0", len(points))
			}
		})
	}
}

// TestGetQuarterDate — дата точки — 1-е число месяца, следующего за отчётным кварталом
// (I → апрель, II → июль, III → октябрь, IV → январь следующего года): семантика прежнего
// разбора сохранена дословно, но незнакомая подпись теперь ошибка, а не panic.
func TestGetQuarterDate(t *testing.T) {
	for _, tt := range []struct {
		quarter string
		want    string
	}{
		{"I", "2026-04-01"},
		{"II", "2026-07-01"},
		{"III", "2026-10-01"},
		{"IV", "2027-01-01"},
	} {
		date, err := getQuarterDate(2026, tt.quarter)
		if err != nil {
			t.Fatalf("квартал %s: %v", tt.quarter, err)
		}
		if got := date.Format("2006-01-02"); got != tt.want {
			t.Errorf("квартал %s: дата %s, ожидалась %s", tt.quarter, got, tt.want)
		}
	}
	if _, err := getQuarterDate(2026, "V"); err == nil {
		t.Error("незнакомая подпись квартала «V»: ожидалась ошибка, получен nil")
	}
}

// TestVvpKvartalAppendErrorNotSwallowed — ошибка записи доходит до вызывающего (§5.5, §11.6):
// сбой Append в середине батча обязан вернуть ошибку, а не выглядеть успешным импортом.
func TestVvpKvartalAppendErrorNotSwallowed(t *testing.T) {
	wantErr := errors.New("append сломан")
	rec := &appendRecorder{appendErr: wantErr}
	err := vvpKvartalImport(openVvpKvartalFixture(t), rec)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ошибка Append не вернулась: err = %v", err)
	}
	if len(rec.rows) != 0 {
		t.Errorf("в батч записано %d строк, ожидалось 0 (первый Append упал)", len(rec.rows))
	}
}
