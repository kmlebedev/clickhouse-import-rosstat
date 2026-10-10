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
