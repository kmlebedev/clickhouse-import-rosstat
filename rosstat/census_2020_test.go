package rosstat

import (
	"encoding/json"
	"fmt"
	"github.com/xuri/excelize/v2"
	"os"
	"strings"
	"testing"
)

func censusFixtureRows() ([][]string, map[string]censusTerritory) {
	rows := [][]string{
		{"2. НАСЕЛЕНИЕ ПО ВОЗРАСТНЫМ ГРУППАМ И ПОЛУ ПО СУБЪЕКТАМ РОССИЙСКОЙ ФЕДЕРАЦИИ"},
		{"", "Мужчины и женщины", "Мужчины", "Женщины", "В процентах к итогу", "", "", "Женщин на 1000 мужчин"},
		{}, {"Российская Федерация"},
	}
	for _, s := range []struct {
		label string
		scale int
	}{{"Городское и сельское население", 2}, {"Городское население", 1}, {"Сельское население", 1}} {
		rows = append(rows, []string{s.label, fmt.Sprint(54 * s.scale), fmt.Sprint(18 * s.scale), fmt.Sprint(36 * s.scale)}, []string{"в том числе в возрасте, лет:"})
		for i := 0; i < 18; i++ {
			label := fmt.Sprintf("%d - %d", i*5, i*5+4)
			if i == 17 {
				label = "85 и более"
			}
			rows = append(rows, []string{label, fmt.Sprint(3 * s.scale), fmt.Sprint(s.scale), fmt.Sprint(2 * s.scale)})
		}
		rows = append(rows, []string{"Средний возраст", "41.1", "38.6", "43.2"})
	}
	return rows, map[string]censusTerritory{"Российская Федерация": {Kind: "RF", Settlements: []string{"total", "urban", "rural"}}}
}
func TestCensus2020Partitions(t *testing.T) {
	rows, m := censusFixtureRows()
	p, e := parseCensusRows(rows, m)
	if e != nil {
		t.Fatal(e)
	}
	if len(p) != 171 {
		t.Fatalf("points=%d", len(p))
	}
	// Only urban is an observed source block: no total/rural synthesis.
	urban := [][]string{rows[0], rows[1], {}, {"г. Москва"}}
	urban = append(urban, []string{"Городское население", "54", "18", "36"})
	for i := 0; i < 18; i++ {
		label := fmt.Sprintf("%d-%d", i*5, i*5+4)
		if i == 17 {
			label = "85 и более"
		}
		urban = append(urban, []string{label, "3", "1", "2"})
	}
	p, e = parseCensusRows(urban, map[string]censusTerritory{"г. Москва": {Kind: "subject", Settlements: []string{"urban"}}})
	if e != nil || len(p) != 57 {
		t.Fatalf("urban-only: %d %v", len(p), e)
	}
	for _, v := range p {
		if v.settlement != "urban" {
			t.Fatal("fabricated settlement")
		}
	}
}
func TestCensus2020RejectMalformed(t *testing.T) {
	for _, kind := range []string{"title", "sex-header", "percent-header", "sex-sum", "fraction", "negative", "missing", "unknown-age", "age-order", "age-sum", "settlement-sum", "unknown-territory", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			rows, m := censusFixtureRows()
			switch kind {
			case "title":
				rows[0][0] = "different table"
			case "sex-header":
				rows[1][2] = "Мужчины и женщины"
			case "percent-header":
				rows[1][4] = "Дополнительное население"
			case "sex-sum":
				rows[6][1] = "7"
			case "fraction":
				rows[6][1] = "6.1"
			case "negative":
				rows[6][1] = "-1"
			case "missing":
				rows[6][1] = ""
			case "unknown-age":
				rows[6][0] = "Возраст не указан"
			case "age-order":
				rows[6], rows[7] = rows[7], rows[6]
			case "age-sum":
				rows[6][1], rows[6][2], rows[6][3] = "9", "3", "6"
			case "settlement-sum":
				for _, row := range rows[4:24] {
					if len(row) == 4 {
						for c := 1; c < 4; c++ {
							var n int
							if _, e := fmt.Sscan(row[c], &n); e == nil {
								row[c] = fmt.Sprint(n * 2)
							}
						}
					}
				}
			case "unknown-territory":
				rows[3][0] = "Новый регион"
			case "duplicate":
				rows = append(rows, rows[3:]...)
			}
			if _, e := parseCensusRows(rows, m); e == nil {
				t.Fatal("accepted malformed", kind)
			}
		})
	}
}
func TestCensus2020CanonicalContract(t *testing.T) {
	b, e := os.ReadFile("../ARCHITECTURE.md")
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(b), fmt.Sprintf(census2020DDL, "rosstat_census_2020")) {
		t.Fatal("DDL drift")
	}
	var mapping map[string]censusTerritory
	if e = json.Unmarshal(census2020TerritoryJSON, &mapping); e != nil {
		t.Fatal(e)
	}
	if len(mapping) != 96 {
		t.Fatalf("territories=%d", len(mapping))
	}
	n := 0
	counts := map[string]int{}
	for _, v := range mapping {
		n += len(v.Settlements)
		counts[v.Kind]++
	}
	if n != 284 || counts["RF"] != 1 || counts["FD"] != 8 || counts["subject"] != 83 || counts["composite"] != 2 || counts["exclAO"] != 2 {
		t.Fatalf("mapping=%v blocks=%d", counts, n)
	}
	decl := census2020Importer.Publication()
	if decl.CountPoints != "SELECT count() FROM rosstat_census_2020 FINAL" || decl.Url != census2020URL {
		t.Fatal(decl)
	}
	if len(census2020Meta) != 1 || census2020Meta[0].Unit != "человек" || len(census2020View.Columns) != 10 {
		t.Fatal("metadata incomplete")
	}
}

func TestCensus2020XLSXAndHierarchy(t *testing.T) {
	rows, m := censusFixtureRows()
	rows = append(rows[:4], append([][]string{{"в том числе:"}}, rows[4:]...)...)
	book := excelize.NewFile()
	defer func() { _ = book.Close() }()
	for r, row := range rows {
		cell, err := excelize.CoordinatesToCellName(1, r+1)
		if err != nil {
			t.Fatal(err)
		}
		if err = book.SetSheetRow("Sheet1", cell, &row); err != nil {
			t.Fatal(err)
		}
	}
	buf, err := book.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := excelize.OpenReader(buf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	decoded, err := reopened.GetRows("Sheet1", excelize.Options{RawCellValue: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = parseCensusRows(decoded, m); err != nil {
		t.Fatal(err)
	}
	decoded[4] = []string{"в том числе:", "123"}
	if _, err = parseCensusRows(decoded, m); err == nil {
		t.Fatal("accepted hierarchy value")
	}
}
func TestCensus2020FederalDistrictPartition(t *testing.T) {
	rows, m := censusFixtureRows()
	base, err := parseCensusRows(rows, m)
	if err != nil {
		t.Fatal(err)
	}
	var points []censusPoint
	for _, p := range base {
		p.value *= 8
		points = append(points, p)
	}
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("district%d", i)
		m[name] = censusTerritory{Kind: "FD", Settlements: []string{"total", "urban", "rural"}}
		for _, p := range base {
			p.territory = name
			p.kind = "FD"
			points = append(points, p)
		}
	}
	if err = validateCensusPoints(points, m); err != nil {
		t.Fatal(err)
	}
	for i := range points {
		if points[i].territory == "district0" {
			points[i].value *= 2
		}
	}
	if err = validateCensusPoints(points, m); err == nil || !strings.Contains(err.Error(), "federal district") {
		t.Fatalf("expected geographic failure: %v", err)
	}
}
