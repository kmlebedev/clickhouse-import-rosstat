package polyus

import "testing"

func TestParseKPIReportRU(t *testing.T) {
	records, err := parseKPIPage("testdata/press_reliz_1h26_p1.tsv", "https://example.invalid/1h26.pdf", 1)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("no records parsed from russian report — english prefixes likely still in use")
	}
	byMetric := map[string]float64{}
	for _, r := range records {
		if r.Period == "2026H1" {
			byMetric[r.Metric] = r.Value
		}
	}
	if got, ok := byMetric["gold_output"]; !ok || got != 1287 {
		t.Errorf("gold_output for 2026H1 = %v (present=%v), want 1287", got, ok)
	}
	if got, ok := byMetric["tcc_per_ounce"]; !ok || got != 1069 {
		t.Errorf("tcc_per_ounce for 2026H1 = %v (present=%v), want 1069", got, ok)
	}
}

func TestParseKPIReportEN(t *testing.T) {
	records, err := parseKPIPage("testdata/press_release_hist_p1.tsv", "https://example.invalid/hist.pdf", 4)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("no records parsed from english report — transfer regressed")
	}
}

// TestParseKPIStrippingCapexDistinctFromCapex держит порядок префиксов в
// matchMetric. Русский релиз печатает две строки capex, и метка обычной строки —
// префикс метки вскрышной:
//
//	Капитальные затраты3                    946
//	Капитальные затраты по вскрышным работам 397
//
// Если метрики упорядочены по числу префиксов, а не по их длине, capex
// проверяется первым, забирает вторую строку себе, а stripping_capex не
// попадает в результат вообще — и 397 теряется.
func TestParseKPIStrippingCapexDistinctFromCapex(t *testing.T) {
	records, err := parseKPIPage("testdata/press_reliz_1h26_p1.tsv", "https://example.invalid/1h26.pdf", 1)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}

	byMetric := map[string]float64{}
	counts := map[string]int{}
	for _, r := range records {
		if r.Period != "2026H1" {
			continue
		}
		byMetric[r.Metric] = r.Value
		counts[r.Metric]++
	}

	got, ok := byMetric["stripping_capex"]
	if !ok {
		t.Fatalf("stripping_capex for 2026H1 missing entirely; 2026H1 metrics: %v", counts)
	}
	if got != 397 {
		t.Errorf("stripping_capex for 2026H1 = %v, want 397", got)
	}

	if got, ok := byMetric["capex"]; !ok || got != 946 {
		t.Errorf("capex for 2026H1 = %v (present=%v), want 946", got, ok)
	}

	// Обе метрики — отдельные записи: вскрышная строка не должна ни исчезнуть,
	// ни раствориться в обычной.
	if byMetric["capex"] == byMetric["stripping_capex"] {
		t.Errorf("capex and stripping_capex share the value %v, want distinct", byMetric["capex"])
	}
}

// TestMetricsSortedByPrefixLengthOrdersByLongestPrefix держит контракт, который
// обещает имя функции: сортировка по длине самого длинного префикса, а не по их
// количеству. Вспомогательные метрики подаются вне словаря metrics, поэтому
// тест не зависит от текущего состава словаря.
func TestMetricsSortedByPrefixLengthOrdersByLongestPrefix(t *testing.T) {
	saved := metrics
	defer func() { metrics = saved }()

	metrics = []MetricDefinition{
		// 10 символов, две записи в Prefix — при сортировке по количеству
		// префиксов эта метрика оказалась бы первой.
		{Name: "ten", Prefix: []string{"0123456789", "abcdefghij"}},
		// 40 символов, один префикс.
		{Name: "forty", Prefix: []string{"0123456789012345678901234567890123456789"}},
	}

	ordered := metricsSortedByPrefixLength()
	if len(ordered) != 2 {
		t.Fatalf("got %d definitions, want 2", len(ordered))
	}
	if ordered[0].Name != "forty" {
		t.Errorf("first = %s (longest prefix %d), want forty (%d)",
			ordered[0].Name, longestPrefixLen(ordered[0]), longestPrefixLen(ordered[1]))
	}
	if ordered[1].Name != "ten" {
		t.Errorf("second = %s, want ten", ordered[1].Name)
	}

	// Устойчивость: равные длины сохраняют порядок объявления, поэтому словарь
	// в metrics.go остаётся разрешением ничьих.
	metrics = []MetricDefinition{
		{Name: "first", Prefix: []string{"0123456789"}},
		{Name: "second", Prefix: []string{"abcdefghij"}},
	}
	ordered = metricsSortedByPrefixLength()
	if ordered[0].Name != "first" || ordered[1].Name != "second" {
		t.Errorf("equal-length prefixes reordered to %s, %s; want declaration order first, second",
			ordered[0].Name, ordered[1].Name)
	}

	// Длинный префикс не обязательно первый в списке метрики: берётся самый
	// длинный из них.
	metrics = []MetricDefinition{
		{Name: "shortest-of-mine", Prefix: []string{"0123456789"}},
		{Name: "has-long-one", Prefix: []string{"ab", "012345678901234567890123456789"}},
	}
	ordered = metricsSortedByPrefixLength()
	if ordered[0].Name != "has-long-one" {
		t.Errorf("first = %s, want has-long-one (its second prefix is the longest)", ordered[0].Name)
	}
}

// TestExtractValueTokensRussianNumbers держит русские разделители: пробел
// между группами тысяч, запятая как десятичный разделитель и скобки как знак
// минус. Английский разбор без этих правил вернул бы 1, 287, 674 и 59.
func TestExtractValueTokensRussianNumbers(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "space separated thousands stay one token",
			in:   "                   1 287                1 311                 (2%)                  1 218                 6%",
			want: []string{"1287", "1311", "(2%)", "1218", "6%"},
		},
		{
			name: "wide column gaps are not joined",
			in:   "     946                  932                  2%                    1 248               (24%)",
			want: []string{"946", "932", "2%", "1248", "(24%)"},
		},
		{
			name: "comma decimal is one token",
			in:   "                                                        0,87                 2,13                 (59%)",
			want: []string{"0,87", "2,13", "(59%)"},
		},
		{
			name: "three digit groups",
			in:   "   45 356                    38 945                       16%",
			want: []string{"45356", "38945", "16%"},
		},
		{
			name: "english commas still work",
			in:   "                              1,696         1,652           3%",
			want: []string{"1,696", "1,652", "3%"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := extractValueTokens(c.in)
			if len(got) != len(c.want) {
				t.Fatalf("extractValueTokens = %q, want %q", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("token %d = %q, want %q (all: %q)", i, got[i], c.want[i], got)
				}
			}
		})
	}
}

// TestParseNumericTokenRussianNumbers проверяет перевод токенов в числа:
// «1 287» — это 1287, «0,87» — 0,87, «(59%)» — -59.
func TestParseNumericTokenRussianNumbers(t *testing.T) {
	cases := []struct {
		token string
		want  float64
	}{
		{"1287", 1287},
		{"0,87", 0.87},
		{"(59%)", -59},
		{"(2%)", -2},
		{"45356", 45356},
		// Запятая, за которой идут ровно три цифры, — разделитель тысяч
		// английского релиза, а не десятичный разделитель.
		{"1,696", 1696},
		// Одна-две цифры после запятой — десятичный разделитель.
		{"(1,25)", -1.25},
		{"0.32", 0.32},
	}

	for _, c := range cases {
		t.Run(c.token, func(t *testing.T) {
			got, err := parseNumericToken(c.token)
			if err != nil {
				t.Fatalf("parseNumericToken(%q): %v", c.token, err)
			}
			if got == nil {
				t.Fatalf("parseNumericToken(%q) = nil, want %v", c.token, c.want)
			}
			if *got != c.want {
				t.Errorf("parseNumericToken(%q) = %v, want %v", c.token, *got, c.want)
			}
		})
	}
}

// TestScanPeriodColumnsRussianHeader держит разбор шапки: четырёхзначный год
// даёт 2026H1, двузначный — тот же 2026H1, а слова «Изм. за год» периодом не
// становятся. Без скользящего окна «1 п/г 2026» распадается на два токена,
// и первый столбец становится голым годом FY.
func TestScanPeriodColumnsRussianHeader(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   []PeriodColumn
	}{
		{
			name:   "four digit year",
			header: "1 п/г 2026           1 п/г 2025           Изм. за год           2 п/г 2025           Изм. за п/г",
			want: []PeriodColumn{
				{0, "2026H1", "H"},
				{1, "2025H1", "H"},
				{2, "2025H2", "H"},
			},
		},
		{
			name:   "two digit year",
			header: "1 п/г 26                   1 п/г 25                Изм. за год                  2 п/г 25                 Изм. за п/г",
			want: []PeriodColumn{
				{0, "2026H1", "H"},
				{1, "2025H1", "H"},
				{2, "2025H2", "H"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := scanPeriodColumns(c.header)
			if len(got) != len(c.want) {
				t.Fatalf("scanPeriodColumns = %+v, want %+v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("column %d = %+v, want %+v", i, got[i], c.want[i])
				}
			}
		})
	}
}

// TestMatchMetricTrimsIndentAndFootnote держит нормализацию: русская метка
// идёт с отступом, а сноска-цифра приклеена к последней скобке. Без обрезки
// отступа ни одна русская строка не опозналась бы, и разбор вернул бы ноль
// записей без ошибки; без отбрасывания сноски число прилипло бы к метке.
func TestMatchMetricTrimsIndentAndFootnote(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		metric  string
		wantRem string
	}{
		{
			name:    "indented label",
			line:    "    Выручка                                                                       4 674                3 688                 27%",
			metric:  "revenue",
			wantRem: "4 674                3 688                 27%",
		},
		{
			name:    "footnote glued to closing bracket",
			line:    "    Производство золота (тыс. унций)2                                                   1 287                1 311                 (2%)",
			metric:  "gold_output",
			wantRem: "1 287                1 311                 (2%)",
		},
		{
			name:    "footnote after dollar sign",
			line:    "    Общие денежные затраты (TCC) на проданную унцию ($)4                                1 069                 653                  64%",
			metric:  "tcc_per_ounce",
			wantRem: "1 069                 653                  64%",
		},
		{
			name:    "shorter prefix must not swallow the first value",
			line:    "    Капитальные затраты3                                                                 946                  932                  2%",
			metric:  "capex",
			wantRem: "946                  932                  2%",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			definition, remainder, found := matchMetric(normalizeLine(c.line))
			if !found {
				t.Fatalf("matchMetric(%q) = not found, want %s", c.line, c.metric)
			}
			if definition.Name != c.metric {
				t.Errorf("metric = %s, want %s", definition.Name, c.metric)
			}
			if remainder != c.wantRem {
				t.Errorf("remainder = %q, want %q", remainder, c.wantRem)
			}
		})
	}
}

// TestMatchMetricNoSeparatorSkipsLine держит замену опечатки `spaceIndex == -1`:
// строка без разделителя метки и значений пропускается, а не обрывает разбор.
func TestMatchMetricNoSeparatorSkipsLine(t *testing.T) {
	if _, _, found := matchMetric("Выручка"); found {
		t.Fatal("matchMetric(«Выручка») found a metric, want skip")
	}
}

func TestKPIValuesByColumnRuAllPeriods(t *testing.T) {
	recs, err := parseKPIPage("testdata/press_reliz_1h26_p1.tsv", "https://example.invalid/1h26.pdf", 1)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	byMetric := map[string]map[string]float64{}
	for _, r := range recs {
		if byMetric[r.Metric] == nil {
			byMetric[r.Metric] = map[string]float64{}
		}
		byMetric[r.Metric][r.Period] = r.Value
	}
	// TCC: все три периода прочитаны, включая слот 2 п/г 2025
	tcc := byMetric["tcc_per_ounce"]
	if tcc["2026H1"] != 1069 || tcc["2025H1"] != 653 || tcc["2025H2"] != 814 {
		t.Errorf("tcc_per_ounce = %v, want 2026H1=1069 2025H1=653 2025H2=814", tcc)
	}
	// производство золота по всем периодам
	gold := byMetric["gold_output"]
	if gold["2026H1"] != 1287 || gold["2025H1"] != 1311 || gold["2025H2"] != 1218 {
		t.Errorf("gold_output = %v, want 2026H1=1287 2025H1=1311 2025H2=1218", gold)
	}
	// колонка «Изм. за год» не породила значение: -2 больше не существует
	for _, r := range recs {
		if r.Metric == "gold_output" && r.Value == -2 {
			t.Errorf("change column value leaked as a period value: %+v", r)
		}
	}
}

func TestKPIValuesByColumnFY2014(t *testing.T) {
	recs, err := parseKPIPage("testdata/press_release_hist_p1.tsv", "https://example.invalid/fy2014.pdf", 4)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	byPeriod := map[string]float64{}
	for _, r := range recs {
		if r.Metric == "gold_output" {
			byPeriod[r.Period] = r.Value
		}
	}
	// 1,696 (FY2014) 1,652 (FY2013) 3% (change) 950 (2H2014) 746 (1H2014)
	want := map[string]float64{"2014FY": 1696, "2013FY": 1652, "2014H2": 950, "2014H1": 746}
	for period, v := range want {
		if byPeriod[period] != v {
			t.Errorf("gold_output %s = %v, want %v", period, byPeriod[period], v)
		}
	}
	if v, ok := byPeriod["2014H2"]; ok && v == 1652 {
		t.Error("2014H2 carries FY2013's value — column shift not fixed")
	}

	// «3%» из колонки изменения («y-o-y change» на 132.45, x=416.11) не стало
	// значением ни одного периода: подпись изменения объявляет непериодную
	// колонку, и её число не должно достаться соседнему периоду (2014H2).
	if len(byPeriod) != len(want) {
		t.Errorf("gold_output spans %v, want exactly %v", byPeriod, want)
	}
	for _, r := range recs {
		if r.Metric != "gold_output" {
			continue
		}
		if r.Period != "2014FY" && r.Period != "2013FY" && r.Period != "2014H2" && r.Period != "2014H1" {
			t.Errorf("gold_output %s = %v came from the change column", r.Period, r.Value)
		}
	}
}

func TestKPIFootnoteNotAValue(t *testing.T) {
	// метка «Производство золота (тыс. унций)2»: сноска 2 не должна стать значением
	recs, err := parseKPIPage("testdata/press_reliz_1h26_p1.tsv", "https://example.invalid/1h26.pdf", 1)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	for _, r := range recs {
		if r.Metric == "gold_output" && r.Period == "2026H1" && r.Value != 1287 {
			t.Errorf("gold_output 2026H1 = %v, want 1287 (footnote likely parsed as value)", r.Value)
		}
	}
}

// TestChangeColumnIsItsOwnColumn держит колонку изменения в колонкочной модели
// на самой строке данных: строка «Total gold production (koz)» MD&A за 2014
// раскладывается по колонкам полосы шапки, и «3%» (x=416.11) обязано попасть в
// непериодную колонку, а четыре числа — в четыре периода.
//
// Без этого «3%» уезжает в колонку 2014H2: подпись изменения стоит в шапке на
// 6.96pt ниже периодов, и если не включить её строку в полосу шапки, промежуток
// между «FY 2013» и «2H 2014» (383.51→441.10) остаётся целым, а 416.11 лежит
// внутри него.
func TestChangeColumnIsItsOwnColumn(t *testing.T) {
	lines := tsvLines(t, "testdata/press_release_hist_p1.tsv")

	start := firstHeaderLine(lines)
	if start < 0 {
		t.Fatal("no financial header found in fy2014 fixture")
	}

	cols := columnsFromHeader(headerBand(lines, start))

	// Подпись изменения — отдельная непериодная колонка на своём X.
	var changeCols int
	for _, c := range cols {
		if c.Period == "" && c.covers(416.11) {
			changeCols++
		}
	}
	if changeCols != 1 {
		t.Fatalf("x=416.11 (change value «3%%») covered by %d columns, want exactly 1 non-period column", changeCols)
	}

	row := findLineContaining(t, lines, "Total gold production")
	recs, unassigned := recordsFromLine(row, cols, "PJSC Polyus", "u", 4)

	got := map[string]float64{}
	for _, r := range recs {
		got[r.Period] = r.Value
	}

	want := map[string]float64{"2014FY": 1696, "2013FY": 1652, "2014H2": 950, "2014H1": 746}
	if len(got) != len(want) {
		t.Errorf("row parsed to %v, want %v", got, want)
	}
	for period, value := range want {
		if got[period] != value {
			t.Errorf("%s = %v, want %v", period, got[period], value)
		}
	}

	// «3%» — единственное значение строки, не ставшее периодом.
	if unassigned != 1 {
		t.Errorf("unassigned values = %d, want 1 (the change column «3%%»)", unassigned)
	}
}

// TestChangeValueNeverBecomesAPeriodValue держит исключение значения колонки
// изменения отдельной проверкой, которая может упасть независимо от геометрии
// колонок.
//
// Строка «Total gold production (koz)» MD&A за 2014 несёт «3%» (x=416.11) —
// значение колонки «y-o-y change». Оно обязано исчезнуть из разбора: если
// колонка изменения не станет отдельной непериодной колонкой (окно периода
// захватит подпись изменения), «3%» накроется колонкой периода 2014H2 и даст
// запись gold_output со значением 3 или -3.
//
// Проверка идёт по всему разбору страницы, а не по одной строке: так она ловит
// и захват подписи изменения в шапке, и любую другую подстановку, при которой
// процент изменения выдаёт себя за значение периода.
func TestChangeValueNeverBecomesAPeriodValue(t *testing.T) {
	recs, err := parseKPIPage("testdata/press_release_hist_p1.tsv", "https://example.invalid/fy2014.pdf", 4)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}

	gold := 0
	for _, r := range recs {
		if r.Metric != "gold_output" {
			continue
		}

		gold++

		if r.Value == 3 || r.Value == -3 {
			t.Errorf(
				"gold_output %s = %v: «3%%» from the change column became a period value",
				r.Period,
				r.Value,
			)
		}
	}

	if gold != 4 {
		t.Errorf("gold_output records = %d, want 4 (FY2014/FY2013/2H2014/1H2014)", gold)
	}
}

// TestUnassignedCountsChangeColumnValuesOnRUGoldRow держит число
// нераспределённых значений на строке русского релиза — единственной, где
// колонки изменения стоят между периодами.
//
// Строка (top=396.40): «Производство золота (тыс. унций) 1 287 1 311 (2%) 1 218 6%».
// Значения и их колонки, замерено по фикстуре:
//
//	1287  x=290.33 → 2026H1   (период)
//	1311  x=348.19 → 2025H1   (период)
//	(2%)  x=407.83 → колонка «Изм. за год»   (непериодная)
//	1218  x=464.14 → 2025H2   (период)
//	6%    x=526.18 → колонка «Изм. за п/г»   (непериодная)
//
// То есть периодами не стали РОВНО два числа — (2%) и 6%, обе колонки изменения.
// До перехода на колоночную модель (2%) стояло в слоте 2025H2 и запись
// gold_output 2025H2 = -2 попадала в витрину.
func TestUnassignedCountsChangeColumnValuesOnRUGoldRow(t *testing.T) {
	const wantUnassigned = 2

	lines := tsvLines(t, "testdata/press_reliz_1h26_p1.tsv")

	start := firstHeaderLine(lines)
	if start < 0 {
		t.Fatal("no financial header found in 1h26 fixture")
	}

	cols := columnsFromHeader(headerBand(lines, start))

	row := findLineContaining(t, lines, "Производство золота")

	recs, unassigned := recordsFromLine(row, cols, "PJSC Polyus", "u", 1)

	got := map[string]float64{}
	for _, r := range recs {
		got[r.Period] = r.Value
	}

	want := map[string]float64{"2026H1": 1287, "2025H1": 1311, "2025H2": 1218}
	if len(got) != len(want) {
		t.Errorf("row parsed to %v, want %v", got, want)
	}
	for period, value := range want {
		if got[period] != value {
			t.Errorf("%s = %v, want %v", period, got[period], value)
		}
	}

	if unassigned != wantUnassigned {
		t.Errorf(
			"unassigned values = %d, want %d: the row carries exactly the two change-column values (2%%) and 6%%",
			unassigned,
			wantUnassigned,
		)
	}
}

// TestSameMetricFromTwoHeadersBothSurvive держит счётчик найденных метрик,
// сбрасываемый на каждой шапке.
//
// Склеенный снимок многостраничного отчёта (pdftotext -nopgbrk, затем joinFiles)
// содержит несколько таблиц подряд, без разделителя страниц: у каждой своя шапка,
// и одна и та же метрика встречается в каждой (gold_output есть и в MD&A за 2014,
// и в FY2024). Счётчик, общий на всю страницу, оставлял бы записи только первой
// таблицы — 35 записей второй терялись молча.
//
// Семантика выбрана «обе таблицы выживают», а не «побеждает последняя»: таблицы
// независимы, их метрики относятся к разным периодам, и терять одну из них
// нельзя. Совпадение ключа (metric, period) при этом не исключено — но его
// слияние делает import.go (batchDedup), который один знает, что попадает в один
// батч, и считает такие повторы.
//
// Вторая копия сдвинута по Top на 1000pt (shiftedTops): координаты pdftotext
// начинаются заново на каждой странице, поэтому у двух настоящих страниц Top
// совпадают, и groupByLine слил бы строки двух таблиц в одну. Фикстура из двух
// копий одной и той же страницы на этом и рассыпается (0 записей), поэтому
// таблицы берутся из разных отчётов.
func TestSameMetricFromTwoHeadersBothSurvive(t *testing.T) {
	first := tsvLines(t, "testdata/press_release_hist_p1.tsv")
	second := tsvLines(t, "testdata/press_release_fy2024_p4.tsv")

	const shift = 1000.0

	both := make([]Line, 0, len(first)+len(second))
	both = append(both, first...)
	for _, line := range second {
		shifted := Line{Top: line.Top + shift}
		for _, w := range line.Words {
			w.Top += shift
			shifted.Words = append(shifted.Words, w)
		}

		both = append(both, shifted)
	}

	records, _, _ := parseKPILines(both, "https://example.invalid/joined.pdf", 1)

	byMetric := map[string]int{}
	byKey := map[string]float64{}
	for _, r := range records {
		byMetric[r.Metric]++
		byKey[r.Metric+"/"+r.Period] = r.Value
	}

	// 4 периода у MD&A за 2014 + 5 у FY2024: обе таблицы дали по периоду.
	if byMetric["gold_output"] != 9 {
		t.Errorf("gold_output records = %d, want 9 (4 from FY2014 + 5 from FY2024)", byMetric["gold_output"])
	}

	// Значения обеих таблиц на месте, и они разные — вторая не «победила» первую.
	want := map[string]float64{
		"gold_output/2014FY": 1696,
		"gold_output/2014H2": 950,
		"gold_output/2024FY": 3002,
		"gold_output/2024H2": 1529,
	}
	for key, value := range want {
		if byKey[key] != value {
			t.Errorf("%s = %v, want %v", key, byKey[key], value)
		}
	}
}

// TestParseKPILinesCountsUnassigned держит направление «разбор → счётчик»: число
// нераспределённых значений, которое parseKPILines отдаёт вторым результатом,
// обязано быть настоящим счётом строк фикстуры, а не нулём.
//
// Без этого теста счётчик ничем не закреплён: строка `unassigned += lineUnassigned`
// в parseKPILines удаляется, весь набор тестов остаётся зелёным, и импортёр
// печатает в итоговой строке ноль. Между тем счётчик — единственный выход
// резервного guard'а: он и есть сигнал, что шапка и разбор разошлись.
//
// Числа замерены по фикстурам и совпадают с живым прогоном импортёра (в его
// итоговой строке 26 значений вне колонок-периодов на трёх включённых отчётах,
// из них 18 у релиза 1 п/г 2026, 8 у MD&A за 2014 и 30 у релиза FY2024 — всего
// 56; в живой прогон FY2024-отчёт не входит, отсюда 26 = 18 + 8).
//
// Записи проверяются рядом: счётчик без них закреплял бы только «строка
// пропущена», тогда как сломанный разбор роняет и то, и другое. Значения
// нераспределённых — это проценты колонок изменения ((2%), 6%) и числа в
// непериодных колонках; они не должны ни превращаться в записи, ни теряться
// бесследно.
func TestParseKPILinesCountsUnassigned(t *testing.T) {
	cases := []struct {
		path       string
		page       int
		records    int
		unassigned int
	}{
		{path: "testdata/press_reliz_1h26_p1.tsv", page: 1, records: 27, unassigned: 18},
		{path: "testdata/press_release_hist_p1.tsv", page: 4, records: 32, unassigned: 8},
		{path: "testdata/press_release_fy2024_p4.tsv", page: 4, records: 53, unassigned: 30},
	}

	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			lines := tsvLines(t, c.path)

			records, unassigned, _ := parseKPILines(lines, "https://example.invalid/x.pdf", c.page)

			if unassigned != c.unassigned {
				t.Errorf(
					"unassigned = %d, want %d: the parser's counter is not wired to the rows it drops",
					unassigned,
					c.unassigned,
				)
			}
			if len(records) != c.records {
				t.Errorf("records = %d, want %d", len(records), c.records)
			}

			// Записи и счётчик не пересекаются: значение, попавшее в запись, не
			// может быть посчитано нераспределённым.
			if len(records)+unassigned == 0 {
				t.Error("fixture parsed to nothing at all: the test lost its subject")
			}
		})
	}
}

// TestParseKPILinesHeaderlessCount держит третий результат parseKPILines —
// число строк, совпавших с непериодной метрикой до первой распознанной шапки.
//
// Это единственный сигнал отказа «шапка не распознана»: страница без шапки
// разбирается в ноль записей, и оба прежних выхода при этом молчат. unassigned
// равен нулю ИМЕННО здесь (колонок нет — ни одно значение не может быть
// отнесено к непериодной колонке), а число записей равно нулю ровно так же,
// как у страницы без метрик. Без третьего результата эти два случая неразличимы,
// и импортёр считает нераспознанную страницу успешно разобранной.
//
// Фикстура строится так же, как это делал контролёр: из русского релиза 1 п/г
// 2026 удаляется единственная строка с меткой единиц «$ млн». Именно она —
// признак шапки (isTableHeader), поэтому без неё ни одна шапка не распознаётся,
// и все строки метрик остаются без колонок: счётчик растёт с 0 до 10.
//
// Ноль на целой странице существен не меньше десяти на повреждённой: счётчик
// считает строки строго ВЫШЕ первой шапки, а не «не ставшие записями строки
// метрик вообще». Иначе отказ срабатывал бы на здоровой английской МСФО-странице,
// где шапки финансовой таблицы нет вовсе: её девять строк метрик стоят с самого
// начала файла, то есть выше всего, и дали бы ровно тот же счётчик, что и
// нераспознанная страница. Отказ обязан остаться про ноль записей, а не про
// непустой счётчик.
func TestParseKPILinesHeaderlessCount(t *testing.T) {
	intact := tsvLines(t, "testdata/press_reliz_1h26_p1.tsv")

	records, _, headerless := parseKPILines(intact, "https://example.invalid/1h26.pdf", 1)

	if len(records) == 0 {
		t.Fatal("intact fixture parsed to zero records: the test lost its subject")
	}
	if headerless != 0 {
		t.Errorf(
			"headerless = %d on the intact fixture, want 0: the counter counts only lines ABOVE the first header, and no metric line precedes it on a healthy page",
			headerless,
		)
	}

	damaged := dropUnitsMarkerLine(t, intact)

	records, unassigned, headerless := parseKPILines(damaged, "https://example.invalid/1h26.pdf", 1)

	if len(records) != 0 {
		t.Errorf("records = %d on a page without a header, want 0", len(records))
	}
	if unassigned != 0 {
		t.Errorf("unassigned = %d, want 0: this failure is exactly when the unassigned counter is silent", unassigned)
	}
	if headerless != 10 {
		t.Errorf(
			"headerless = %d on a page whose header was not recognised, want 10 (the fixture's metric rows): the only signal of the failure is not wired to the rows that were lost",
			headerless,
		)
	}
}

// dropUnitsMarkerLine возвращает строки фикстуры без строки-метки единиц: без
// неё ни одна шапка не распознаётся (isTableHeader требует обе метки — единицы и
// периоды).
func dropUnitsMarkerLine(t *testing.T, lines []Line) []Line {
	t.Helper()

	kept := make([]Line, 0, len(lines))

	dropped := false

	for _, line := range lines {
		if unitsMarker(line) {
			dropped = true

			continue
		}

		kept = append(kept, line)
	}

	if !dropped {
		t.Fatal("the fixture carries no units-marker line: nothing to damage")
	}

	return kept
}
