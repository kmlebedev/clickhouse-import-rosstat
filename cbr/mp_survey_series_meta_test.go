package cbr

import (
	"strings"
	"testing"
)

// TestSeriesMetaCompleteness — двусторонняя сверка определений рядов (mpSurveySeries)
// и записей каталога (mpSurveySeriesMeta): каждому ряду кода соответствует запись
// каталога и наоборот, у каждой записи заполнены все поля, флаг SA согласован с
// суффиксом _sa в имени ряда.
func TestSeriesMetaCompleteness(t *testing.T) {
	const want = 13
	if len(mpSurveySeries) != want {
		t.Fatalf("mpSurveySeries: %d определений, ожидалось %d", len(mpSurveySeries), want)
	}
	if len(mpSurveySeriesMeta) != want {
		t.Fatalf("mpSurveySeriesMeta: %d записей, ожидалось %d", len(mpSurveySeriesMeta), want)
	}

	defs := make(map[string]seriesDef, want)
	for _, d := range mpSurveySeries {
		if _, dup := defs[d.Name]; dup {
			t.Errorf("ряд %q определён дважды", d.Name)
		}
		defs[d.Name] = d
	}

	described := make(map[string]bool, want)
	for _, m := range mpSurveySeriesMeta {
		if m.Source != mpSurveySource {
			t.Errorf("series %q: source %q, ожидалось %q", m.Series, m.Source, mpSurveySource)
		}
		if m.Frequency != "M" && m.Frequency != "Q" {
			t.Errorf("series %q: частота %q, ожидалось M или Q", m.Series, m.Frequency)
		}
		for field, v := range map[string]string{
			"series": m.Series, "title": m.Title, "unit": m.Unit,
			"origin": m.Origin, "description": m.Description,
		} {
			if strings.TrimSpace(v) == "" {
				t.Errorf("series %q: пустое поле %s", m.Series, field)
			}
		}
		if described[m.Series] {
			t.Errorf("series %q описан дважды", m.Series)
		}
		described[m.Series] = true
	}

	var creditDefs, creditMeta int
	for name, d := range defs {
		if !described[name] {
			t.Errorf("ряд %s не описан в mpSurveySeriesMeta", name)
		}
		if wantSA := strings.HasSuffix(name, "_sa"); d.SA != wantSA {
			t.Errorf("ряд %s: SA=%v, ожидалось %v (по суффиксу _sa)", name, d.SA, wantSA)
		}
		if strings.HasPrefix(name, "credit_conditions") {
			creditDefs++
		}
	}
	for series := range described {
		if _, ok := defs[series]; !ok {
			t.Errorf("каталог описывает ряд %s, которого нет в mpSurveySeries", series)
		}
		if strings.HasPrefix(series, "credit_conditions") {
			creditMeta++
		}
	}
	// Условий кредитования в источнике нет сезонно сглаженного варианта
	// (сезонная составляющая отсутствует), поэтому запись строго одна.
	if creditDefs != 1 || creditMeta != 1 {
		t.Errorf("credit_conditions: %d определений и %d записей каталога, ожидалось по одной (без _sa)", creditDefs, creditMeta)
	}
}
