package rosstat

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

//go:embed census_2020_territories.json
var census2020TerritoryJSON []byte

type censusTerritory struct {
	Kind        string   `json:"kind"`
	Settlements []string `json:"settlements"`
}
type censusPoint struct {
	territory, kind, settlement, sex, age string
	value                                 uint64
}

func censusText(s string) string { return strings.Join(strings.Fields(s), " ") }
func censusAge(s string) string {
	s = censusText(strings.ReplaceAll(strings.ReplaceAll(s, "–", "-"), "—", "-"))
	s = strings.ReplaceAll(s, " ", "")
	if s == "85иболее" {
		return "85+"
	}
	for i := 0; i <= 80; i += 5 {
		v := fmt.Sprintf("%d-%d", i, i+4)
		if s == v {
			return v
		}
	}
	return ""
}
func censusCell(row []string, c int) string {
	if c >= len(row) {
		return ""
	}
	return censusText(row[c])
}
func parseCensus2020(book *excelize.File) ([]censusPoint, error) {
	var mapping map[string]censusTerritory
	if err := json.Unmarshal(census2020TerritoryJSON, &mapping); err != nil {
		return nil, err
	}
	sheets := book.GetSheetList()
	if len(sheets) != 1 {
		return nil, fmt.Errorf("expected one census sheet")
	}
	rows, err := book.GetRows(sheets[0], excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, err
	}
	points, err := parseCensusRows(rows, mapping)
	if err != nil {
		return nil, err
	}
	if len(points) != 16188 {
		return nil, fmt.Errorf("census shape changed: %d points", len(points))
	}
	for _, p := range points {
		if p.territory == "Российская Федерация" && p.settlement == "total" && p.age == "all" && p.sex == "both" && p.value != 147182123 {
			return nil, fmt.Errorf("national census control changed")
		}
	}
	return points, nil
}
func parseCensusRows(rows [][]string, mapping map[string]censusTerritory) ([]censusPoint, error) {
	if len(rows) < 3 || !strings.HasPrefix(censusCell(rows[0], 0), "2. НАСЕЛЕНИЕ ПО ВОЗРАСТНЫМ ГРУППАМ И ПОЛУ ПО СУБЪЕКТАМ РОССИЙСКОЙ ФЕДЕРАЦИИ") {
		return nil, fmt.Errorf("wrong census title")
	}
	for c, want := range []string{"Мужчины и женщины", "Мужчины", "Женщины"} {
		if censusCell(rows[1], c+1) != want {
			return nil, fmt.Errorf("wrong sex header")
		}
	}
	if censusCell(rows[1], 4) != "В процентах к итогу" || censusCell(rows[1], 7) != "Женщин на 1000 мужчин" {
		return nil, fmt.Errorf("unexpected non-count columns")
	}
	settlements := map[string]string{"Городское и сельское население": "total", "Городское население": "urban", "Сельское население": "rural"}
	skip := map[string]bool{"в том числе в возрасте, лет:": true, "Средний возраст": true, "Медианный возраст": true, "Из общей численности - население в возрасте1):": true, "0 - 6 лет": true, "0 - 13 лет": true, "0 - 17 лет": true, "7 - 14 лет": true, "16 - 29 лет": true, "14 - 35 лет": true, "16 - 35 лет": true, "моложе трудоспособного": true, "трудоспособном": true, "старше трудоспособного": true}
	territory, settlement := "", ""
	seenTerritories := map[string]bool{}
	footnote := false
	var points []censusPoint
	sequence := map[string]int{}
	for r := 2; r < len(rows); r++ {
		row := rows[r]
		label := censusCell(row, 0)
		hasValues := censusCell(row, 1) != "" || censusCell(row, 2) != "" || censusCell(row, 3) != ""
		if label == "" && !hasValues {
			continue
		}
		if footnote {
			return nil, fmt.Errorf("unexpected content after footnote row%d", r+1)
		}
		if strings.HasPrefix(label, "1) Население в возрасте моложе трудоспособного") && !hasValues {
			footnote = true
			continue
		}
		if _, ok := mapping[label]; ok {
			if hasValues || seenTerritories[label] {
				return nil, fmt.Errorf("duplicate/invalid territory %q", label)
			}
			territory = label
			settlement = ""
			seenTerritories[label] = true
			continue
		}
		if label == "в том числе:" {
			if hasValues {
				return nil, fmt.Errorf("unexpected numbers in hierarchy marker")
			}
			continue
		}
		if skip[label] {
			continue
		}
		age := censusAge(label)
		if value, ok := settlements[label]; ok {
			settlement = value
			age = "all"
		}
		if age == "" || territory == "" || settlement == "" {
			return nil, fmt.Errorf("unexpected census row%d: %q", r+1, label)
		}
		key := censusKey(territory, settlement)
		next := sequence[key]
		want := "all"
		if next > 0 && next < 18 {
			want = fmt.Sprintf("%d-%d", (next-1)*5, (next-1)*5+4)
		} else if next == 18 {
			want = "85+"
		}
		if next > 18 || age != want {
			return nil, fmt.Errorf("unexpected age order row%d", r+1)
		}
		sequence[key]++
		var v [3]uint64
		for c := range v {
			n, err := strconv.ParseUint(censusCell(row, c+1), 10, 64)
			if err != nil || n > 1000000000 {
				return nil, fmt.Errorf("invalid population row%d col%d", r+1, c+2)
			}
			v[c] = n
		}
		if v[0] != v[1]+v[2] {
			return nil, fmt.Errorf("sex sum row%d", r+1)
		}
		for c, sex := range []string{"both", "male", "female"} {
			points = append(points, censusPoint{territory, mapping[territory].Kind, settlement, sex, age, v[c]})
		}
	}
	if len(seenTerritories) != len(mapping) {
		return nil, fmt.Errorf("missing census territories: %d/%d", len(seenTerritories), len(mapping))
	}
	if err := validateCensusPoints(points, mapping); err != nil {
		return nil, err
	}
	return points, nil
}
func censusKey(parts ...string) string { return strings.Join(parts, "\x1f") }
func validateCensusPoints(points []censusPoint, mapping map[string]censusTerritory) error {
	values := map[string]uint64{}
	groups := map[string]map[string]uint64{}
	for _, p := range points {
		key := censusKey(p.territory, p.settlement, p.sex, p.age)
		if _, ok := values[key]; ok {
			return fmt.Errorf("duplicate census key")
		}
		values[key] = p.value
		group := censusKey(p.territory, p.settlement, p.sex)
		if groups[group] == nil {
			groups[group] = map[string]uint64{}
		}
		groups[group][p.age] = p.value
	}
	for territory, m := range mapping {
		for _, settlement := range m.Settlements {
			for _, sex := range []string{"both", "male", "female"} {
				group := groups[censusKey(territory, settlement, sex)]
				if len(group) != 19 {
					return fmt.Errorf("incomplete age partition %s/%s/%s", territory, settlement, sex)
				}
				var sum uint64
				for age, v := range group {
					if age != "all" {
						sum += v
					}
				}
				if total, ok := group["all"]; !ok || total != sum {
					return fmt.Errorf("age sum %s/%s", territory, settlement)
				}
			}
		}
		if len(m.Settlements) == 3 {
			for _, sex := range []string{"both", "male", "female"} {
				for age, total := range groups[censusKey(territory, "total", sex)] {
					if total != values[censusKey(territory, "urban", sex, age)]+values[censusKey(territory, "rural", sex, age)] {
						return fmt.Errorf("settlement sum %s", territory)
					}
				}
			}
		}
	}
	expected := 0
	for _, m := range mapping {
		expected += len(m.Settlements) * 3
	}
	if len(groups) != expected {
		return fmt.Errorf("unexpected settlement groups")
	}
	// Federal districts form a disjoint partition; subject rows deliberately overlap.
	fd := 0
	for _, m := range mapping {
		if m.Kind == "FD" {
			fd++
		}
	}
	if fd > 0 {
		if fd != 8 {
			return fmt.Errorf("expected eight federal districts")
		}
		for _, p := range points {
			if p.kind != "RF" {
				continue
			}
			var sum uint64
			for territory, m := range mapping {
				if m.Kind == "FD" {
					sum += values[censusKey(territory, p.settlement, p.sex, p.age)]
				}
			}
			if sum != p.value {
				return fmt.Errorf("federal district sum %s/%s/%s", p.settlement, p.sex, p.age)
			}
		}
	}
	return nil
}
