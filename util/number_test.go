package util

import "testing"

// TestParseRuNumber: форматы ячеек ЦБ — пробел как разделитель тысяч, запятая как
// десятичный разделитель, прочерк как отсутствие значения.
func TestParseRuNumber(t *testing.T) {
	cases := []struct {
		raw     string
		value   float64
		present bool
	}{
		{"3 481,8", 3481.8, true},
		{"3\u00a0481,8", 3481.8, true},
		{"3\u202f481,8", 3481.8, true},
		{"-2 803,1", -2803.1, true},
		{"0", 0, true},
		{"13,92", 13.92, true},
		{"—", 0, false},
		{"–", 0, false},
		{"", 0, false},
		// Прочерк, обёрнутый неразрывными пробелами: пробелы снимаются до сверки
		// с прочерком, иначе ячейка стала бы ошибкой импорта вместо «нет значения».
		{"\u00a0—\u00a0", 0, false},
	}
	for _, c := range cases {
		value, present, err := ParseRuNumber(c.raw)
		if err != nil {
			t.Errorf("%q: %v", c.raw, err)
			continue
		}
		if present != c.present || value != c.value {
			t.Errorf("%q = (%v, %v), want (%v, %v)", c.raw, value, present, c.value, c.present)
		}
	}
	if _, _, err := ParseRuNumber("нет данных"); err == nil {
		t.Error("нечисловая ячейка разобрана без ошибки")
	}
	// ParseFloat принимает NaN/Inf без ошибки, но значением ряда они быть не могут.
	for _, raw := range []string{"NaN", "Inf", "+Inf"} {
		if _, _, err := ParseRuNumber(raw); err == nil {
			t.Errorf("%q разобран без ошибки, want ошибку NaN/Inf", raw)
		}
	}
}
