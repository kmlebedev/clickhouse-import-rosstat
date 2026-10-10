package polyus

import "testing"

func TestParsePeriodAllForms(t *testing.T) {
	cases := []struct {
		in     string
		period string
		typ    string
	}{
		{"1H 2026", "2026H1", "H"},    // англ. пресс-релиз
		{"1 п/г 2026", "2026H1", "H"}, // рус. пресс-релиз
		{"2 п/г 2025", "2025H2", "H"},
		{"2H2025", "2025H2", "H"},
		{"2025", "2025FY", "FY"},
		{"4Q 2022", "2022Q4", "Q"},
		{"4Q2022", "2022Q4", "Q"},
		// Точная подстрока, извлечённая из строки заголовка
		// press_reliz-1h26-_tu_mda_2.pdf, стр. 1 (колонки разделены
		// множественными пробелами, пробелы по краям сохранены).
		{" 1 п/г 2026 ", "2026H1", "H"},
		// Те же формы, но с неразрывными пробелами вместо обычного:
		// Go-шный \s их не покрывает, поэтому эти кейсы держат
		// расширенный класс nonBreakingSpace.
		{"1\u00a0п/г\u00a02026", "2026H1", "H"}, // NBSP
		{"1\u2007п/г\u20072026", "2026H1", "H"}, // figure space
		{"1\u202fп/г\u202f2026", "2026H1", "H"}, // narrow no-break space
		{"2\u00a0п/г\u00a02025", "2025H2", "H"},
		{"1H\u00a02026", "2026H1", "H"},
		{"4Q\u00a02022", "2022Q4", "Q"},
		{"4Q\u202f2022", "2022Q4", "Q"},
		{"\u00a01H\u00a02026\u00a0", "2026H1", "H"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := parsePeriod(c.in)
			if err != nil {
				t.Fatalf("parsePeriod(%q): %v", c.in, err)
			}
			if got.Period != c.period || got.Type != c.typ {
				t.Errorf("parsePeriod(%q) = %+v, want {%s %s}", c.in, got, c.period, c.typ)
			}
		})
	}
}

func TestParsePeriodUnknownFormErrors(t *testing.T) {
	if _, err := parsePeriod("какой-то текст"); err == nil {
		t.Error("expected error for unknown period form")
	}
}
