package polyus

import "testing"

func TestParseKPIReportRU(t *testing.T) {
	records, err := parseKPIPage("testdata/press_reliz_1h26_p1.txt", "https://example.invalid/1h26.pdf", 1)
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
	records, err := parseKPIPage("testdata/press_release_hist_p1.txt", "https://example.invalid/hist.pdf", 4)
	if err != nil {
		t.Fatalf("parseKPIPage: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("no records parsed from english report — transfer regressed")
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
