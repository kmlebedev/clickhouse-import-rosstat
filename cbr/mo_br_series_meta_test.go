package cbr

import (
	"sort"
	"strings"
	"testing"
)

// wantMoBrIndicators — 17 базовых имён рядов дословно из §3.3 спеки: номер листа → base.
var wantMoBrIndicators = map[string]string{
	"1":  "cpi_dec",
	"2":  "cpi_avg",
	"3":  "key_rate_avg",
	"4":  "gdp_yoy",
	"5":  "unemp_avg",
	"6":  "unemp_dec_nsa",
	"7":  "wage_nominal",
	"8":  "budget_balance",
	"9":  "exports",
	"10": "imports",
	"11": "usd_rub",
	"12": "oil_tax",
	"13": "brent",
	"14": "urals",
	"15": "cds_5y",
	"16": "neutral_rate",
	"17": "gdp_long_term",
}

// moBrStatSuffixes — суффиксы статистик в объёме (§2 спеки), отсортированные для
// детерминированного обхода.
func moBrStatSuffixes() []string {
	out := make([]string, 0, len(moBrStatSuffix))
	for _, suffix := range moBrStatSuffix {
		out = append(out, suffix)
	}
	sort.Strings(out)
	return out
}

// TestMoBrIndicatorsRegistry — реестр из 17 дескрипторов: номера листов 1…17 ровно по
// одному разу, базовые имена рядов уникальны и совпадают со спекой, заголовок и единица
// заполнены, признак «без годов» стоит только у листов 16–17 (нейтральная ставка и
// долгосрочный рост ВВП — без прогнозного года).
func TestMoBrIndicatorsRegistry(t *testing.T) {
	if len(moBrIndicators) != len(wantMoBrIndicators) {
		t.Fatalf("moBrIndicators: %d дескрипторов, ожидалось %d", len(moBrIndicators), len(wantMoBrIndicators))
	}

	seenSheet := make(map[string]bool, len(moBrIndicators))
	seenBase := make(map[string]string, len(moBrIndicators))
	noYears := map[string]bool{"16": true, "17": true}
	for _, ind := range moBrIndicators {
		wantBase, ok := wantMoBrIndicators[ind.Sheet]
		if !ok {
			t.Errorf("лист %q не ожидался в реестре", ind.Sheet)
			continue
		}
		if ind.Base != wantBase {
			t.Errorf("лист %q: base %q, ожидалось %q", ind.Sheet, ind.Base, wantBase)
		}
		if seenSheet[ind.Sheet] {
			t.Errorf("лист %q описан дважды", ind.Sheet)
		}
		seenSheet[ind.Sheet] = true
		if other, dup := seenBase[ind.Base]; dup {
			t.Errorf("base %q повторяется: листы %q и %q", ind.Base, other, ind.Sheet)
		}
		seenBase[ind.Base] = ind.Sheet
		if strings.TrimSpace(ind.Title) == "" {
			t.Errorf("лист %q: пустой заголовок показателя", ind.Sheet)
		}
		if strings.TrimSpace(ind.Unit) == "" {
			t.Errorf("лист %q: пустая единица измерения", ind.Sheet)
		}
		if ind.NoYears != noYears[ind.Sheet] {
			t.Errorf("лист %q: NoYears=%v, ожидалось %v", ind.Sheet, ind.NoYears, noYears[ind.Sheet])
		}
		if _, ok := moBrIndicatorBySheet(ind.Sheet); !ok {
			t.Errorf("лист %q: не найден поиском по имени листа", ind.Sheet)
		}
	}
	for sheet := range wantMoBrIndicators {
		if !seenSheet[sheet] {
			t.Errorf("лист %q реестра не описан", sheet)
		}
	}
	if _, ok := moBrIndicatorBySheet("99"); ok {
		t.Error("лист \"99\" найден в реестре — реестр шире 17 показателей")
	}
}

// TestMoBrStatTemplates — статистики в объёме (§2 спеки): шесть меток блоков колонки B,
// шесть суффиксов имён рядов, у каждой статистики заполнены расшифровка (для Description)
// и каноническая метка блока (для Origin), описания различаются между статистиками.
func TestMoBrStatTemplates(t *testing.T) {
	const want = 6
	if len(moBrStatSuffix) != want {
		t.Fatalf("moBrStatSuffix: %d меток, ожидалось %d", len(moBrStatSuffix), want)
	}
	if len(moBrStatDesc) != want {
		t.Fatalf("moBrStatDesc: %d расшифровок, ожидалось %d", len(moBrStatDesc), want)
	}
	if len(moBrStatLabel) != want {
		t.Fatalf("moBrStatLabel: %d меток, ожидалось %d", len(moBrStatLabel), want)
	}

	seenSuffix := make(map[string]string, want)
	for label, suffix := range moBrStatSuffix {
		if strings.TrimSpace(label) != label || label == "" {
			t.Errorf("метка %q не нормализована (пробелы должны быть срезаны)", label)
		}
		if strings.TrimSpace(suffix) == "" {
			t.Errorf("метка %q: пустой суффикс статистики", label)
		}
		if other, dup := seenSuffix[suffix]; dup {
			t.Errorf("суффикс %q назначен и метке %q, и метке %q", suffix, other, label)
		}
		seenSuffix[suffix] = label
	}
	descriptions := make(map[string]string, want)
	for suffix, desc := range moBrStatDesc {
		if _, ok := seenSuffix[suffix]; !ok {
			t.Errorf("описание статистики %q без метки блока", suffix)
		}
		if strings.TrimSpace(desc) == "" {
			t.Errorf("статистика %q: пустая расшифровка", suffix)
		}
		if other, dup := descriptions[desc]; dup {
			t.Errorf("расшифровки статистик %q и %q совпадают: %q", other, suffix, desc)
		}
		descriptions[desc] = suffix
		if strings.TrimSpace(moBrStatLabel[suffix]) == "" {
			t.Errorf("статистика %q: пустая каноническая метка блока", suffix)
		}
	}
	for suffix := range moBrStatLabel {
		if _, ok := seenSuffix[suffix]; !ok {
			t.Errorf("каноническая метка %q без метки блока", suffix)
		}
	}
}

// TestMoBrSeriesMetaFields — запись каталога для каждой комбинации (дескриптор × год ×
// статистика): Series по шаблону <base>_<year>_<stat> (у листов 16–17 без года), заполнены
// все поля, частота — волны опроса, Origin называет лист и блок, Description называет год
// и различается между статистиками одного ряда.
func TestMoBrSeriesMetaFields(t *testing.T) {
	suffixes := moBrStatSuffixes()
	for _, ind := range moBrIndicators {
		years := []string{"2021", "2026", "2029"}
		if ind.NoYears {
			years = []string{""}
		}
		for _, year := range years {
			descriptions := make(map[string]string, len(suffixes))
			for _, suffix := range suffixes {
				meta := moBrSeriesMeta(ind, year, suffix)
				if want := moBrSeriesName(ind.Base, year, suffix); meta.Series != want {
					t.Errorf("лист %q, год %q, стат %s: series %q, ожидалось %q",
						ind.Sheet, year, suffix, meta.Series, want)
				}
				if meta.Unit != ind.Unit {
					t.Errorf("series %q: unit %q, ожидалось %q", meta.Series, meta.Unit, ind.Unit)
				}
				if meta.Source != moBrSource {
					t.Errorf("series %q: source %q, ожидалось %q", meta.Series, meta.Source, moBrSource)
				}
				if meta.Frequency != moBrFrequency {
					t.Errorf("series %q: частота %q, ожидалось %q", meta.Series, meta.Frequency, moBrFrequency)
				}
				for field, v := range map[string]string{
					"series": meta.Series, "title": meta.Title, "unit": meta.Unit,
					"frequency": meta.Frequency, "origin": meta.Origin, "description": meta.Description,
				} {
					if strings.TrimSpace(v) == "" {
						t.Errorf("series %q: пустое поле %s", meta.Series, field)
					}
				}
				if !strings.Contains(meta.Origin, "лист «"+ind.Sheet+"»") {
					t.Errorf("series %q: origin %q не называет лист", meta.Series, meta.Origin)
				}
				if !strings.Contains(meta.Origin, "блок «"+moBrStatLabel[suffix]+"»") {
					t.Errorf("series %q: origin %q не называет блок статистики", meta.Series, meta.Origin)
				}
				if !strings.Contains(meta.Description, moBrStatDesc[suffix]) {
					t.Errorf("series %q: description %q не содержит расшифровку статистики", meta.Series, meta.Description)
				}
				if year != "" && !strings.Contains(meta.Description, year) {
					t.Errorf("series %q: description %q не называет год", meta.Series, meta.Description)
				}
				if other, dup := descriptions[meta.Description]; dup {
					t.Errorf("series %q: description совпадает с %q", meta.Series, other)
				}
				descriptions[meta.Description] = meta.Series
			}
		}
	}
}
