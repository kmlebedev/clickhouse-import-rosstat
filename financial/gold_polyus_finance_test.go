package financial

import (
	"context"
	"strings"
	"testing"
)

func TestParseNumericToken(t *testing.T) {
	tests := []struct {
		in   string
		want *float64
	}{
		{"8,723", ptr(8723)},
		{"3.99", ptr(3.99)},
		{"38%", ptr(38)},
		{"(16%)", ptr(-16)},
		{"(12)", ptr(-12)},
		{"-", nil},
		{"n/a", nil},
		{"", nil},
	}

	for _, tt := range tests {
		got, err := parseNumericToken(tt.in)
		if err != nil {
			t.Fatalf("parseNumericToken(%q) error: %v", tt.in, err)
		}
		if (got == nil) != (tt.want == nil) {
			t.Fatalf("parseNumericToken(%q) = %v, want %v", tt.in, got, tt.want)
		}
		if got != nil && *got != *tt.want {
			t.Fatalf("parseNumericToken(%q) = %f, want %f", tt.in, *got, *tt.want)
		}
	}
}

func TestPDFParser_ParsePage(t *testing.T) {
	cfg := &ParserConfig{
		Metrics: []MetricDefinition{
			{
				Name:   "revenue",
				Unit:   "USD million",
				Prefix: []string{"Revenue"},
			},
		},
	}
	p := NewPDFParser(cfg)

	input := strings.NewReader(`
$ mln unless specified otherwise
Revenue 1,234 1,100 10%
`)

	records, err := p.parse(context.Background(), input, "http://test.pdf", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Value != 1234 {
		t.Errorf("expected 1234, got %f", records[0].Value)
	}
}

func ptr(f float64) *float64 { return &f }
