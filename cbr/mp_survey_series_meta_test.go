package cbr

import (
	"strings"
	"testing"
)

// TestSeriesMetaCompleteness — двусторонняя сверка определений рядов (13 агрегатных
// mpSurveySeries + 368 отраслевых mpSurveyIndustrySeries) и записей каталога: каждому ряду
// кода соответствует запись каталога и наоборот, у каждой записи заполнены все поля, флаг SA
// согласован с суффиксом _sa в имени, а у условий кредитования SA-пары нет ни в агрегате,
// ни на отраслевых листах.
func TestSeriesMetaCompleteness(t *testing.T) {
	const (
		wantAggregate = 13
		wantIndustry  = 368
		wantAll       = wantAggregate + wantIndustry
	)
	for name, got := range map[string]int{
		"mpSurveySeries":             len(mpSurveySeries),
		"mpSurveySeriesMeta":         len(mpSurveySeriesMeta),
		"mpSurveyIndustrySeries":     len(mpSurveyIndustrySeries),
		"mpSurveyIndustrySeriesMeta": len(mpSurveyIndustrySeriesMeta),
		"mpSurveyAllSeries":          len(mpSurveyAllSeries),
		"mpSurveyAllMeta":            len(mpSurveyAllMeta),
	} {
		want := wantAll
		if strings.Contains(name, "Industry") {
			want = wantIndustry
		} else if !strings.Contains(name, "All") {
			want = wantAggregate
		}
		if got != want {
			t.Errorf("%s: %d, ожидалось %d", name, got, want)
		}
	}

	defs := make(map[string]seriesDef, len(mpSurveyAllSeries))
	industryDefs := 0
	for _, d := range mpSurveyAllSeries {
		if _, dup := defs[d.Name]; dup {
			t.Errorf("ряд %q определён дважды", d.Name)
		}
		defs[d.Name] = d
		if strings.HasPrefix(d.Name, "industry_") {
			industryDefs++
		}
	}
	if industryDefs != wantIndustry {
		t.Errorf("отраслевых определений: %d, ожидалось %d", industryDefs, wantIndustry)
	}

	described := make(map[string]bool, len(mpSurveyAllMeta))
	for _, m := range mpSurveyAllMeta {
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
			t.Errorf("ряд %s не описан в каталоге", name)
		}
		if wantSA := strings.HasSuffix(name, "_sa"); d.SA != wantSA {
			t.Errorf("ряд %s: SA=%v, ожидалось %v (по суффиксу _sa)", name, d.SA, wantSA)
		}
		if name == "credit_conditions" || strings.HasSuffix(name, "_credit_conditions") {
			creditDefs++
		}
	}
	for series := range described {
		if _, ok := defs[series]; !ok {
			t.Errorf("каталог описывает ряд %s, которого нет в определениях", series)
		}
		if series == "credit_conditions" || strings.HasSuffix(series, "_credit_conditions") {
			creditMeta++
		}
	}
	// 1 агрегатный ряд + 16 отраслевых; у всех нет сезонной пары, поэтому _sa быть не должно.
	if creditDefs != 17 || creditMeta != 17 {
		t.Errorf("credit_conditions: %d определений и %d записей каталога, ожидалось по 17 (1 агрегатный + 16 отраслевых, без _sa)", creditDefs, creditMeta)
	}
}
