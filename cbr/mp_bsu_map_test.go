package cbr

import (
	"encoding/csv"
	"os"
	"strings"
	"testing"
)

// TestCbCompBSUCodes: список — контракт публичного архива (87 уникальных кодов). Дубликат
// сделал бы bsuMapRows неоднозначным, а пропажа кода — тихую дыру в кроссворде.
func TestCbCompBSUCodes(t *testing.T) {
	if len(cbCompBSUCodes) != 87 {
		t.Fatalf("кодов %d, want 87", len(cbCompBSUCodes))
	}
	seen := make(map[string]bool, len(cbCompBSUCodes))
	for _, code := range cbCompBSUCodes {
		if code == "" {
			t.Error("пустой код в cbCompBSUCodes")
		}
		if seen[code] {
			t.Errorf("код %q встречается дважды", code)
		}
		seen[code] = true
	}
}

// TestCbCompBSUFixture: офлайн-фикстура — те же 87 кодов с названиями и разделами из
// классификатора Росстата. Тест ловит расхождение Go-списка и публичного словаря до
// живого импорта; имена/разделы должны быть непустыми.
func TestCbCompBSUFixture(t *testing.T) {
	file, err := os.Open("testdata/cb_comp_bsu_codes.csv")
	if err != nil {
		t.Fatalf("открытие фикстуры: %v", err)
	}
	defer func() { _ = file.Close() }()
	reader := csv.NewReader(file)
	reader.Comma = ';'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("разбор фикстуры: %v", err)
	}
	if len(records) != 88 {
		t.Fatalf("строк фикстуры %d, want 88 (шапка + 87)", len(records))
	}
	if got := strings.Join(records[0], ";"); got != "code;name;section" {
		t.Fatalf("шапка %q, want code;name;section", got)
	}
	fixture := make(map[string]bool, len(records)-1)
	for i, record := range records[1:] {
		if len(record) != 3 {
			t.Fatalf("строка %d: полей %d, want 3", i+2, len(record))
		}
		code, name, section := record[0], record[1], record[2]
		if code == "" || name == "" || section == "" {
			t.Errorf("строка %d: пустое поле: %+v", i+2, record)
		}
		fixture[code] = true
	}
	for _, code := range cbCompBSUCodes {
		if !fixture[code] {
			t.Errorf("код %q из Go-списка отсутствует в фикстуре", code)
		}
	}
	if len(fixture) != len(cbCompBSUCodes) {
		t.Errorf("в фикстуре %d кодов, в Go-списке %d", len(fixture), len(cbCompBSUCodes))
	}
}

// TestPublicIndustryForCodeAll: каждый из 87 кодов маппится в существующий лист
// mpSurveyIndustrySheets; 61 rule + 26 manual, fallback не используется. Ручные
// подкатегории — предмет решения, поэтому их число закреплено: случайное расширение
// consumer-набора не должно проходить незамеченным.
func TestPublicIndustryForCodeAll(t *testing.T) {
	bySlug := make(map[string]bool, len(mpSurveyIndustrySheets))
	for _, sheet := range mpSurveyIndustrySheets {
		bySlug[sheet.Slug] = true
	}
	counts := map[string]int{}
	for _, code := range cbCompBSUCodes {
		industry, err := publicIndustryForCode(code)
		if err != nil {
			t.Errorf("publicIndustryForCode(%q): %v", code, err)
			continue
		}
		if !bySlug[industry.Slug] {
			t.Errorf("код %q: slug %q отсутствует в mpSurveyIndustrySheets", code, industry.Slug)
		}
		if industry.Sheet == "" || industry.Basis == "" {
			t.Errorf("код %q: пустой лист или обоснование: %+v", code, industry)
		}
		switch industry.Confidence {
		case "rule", "manual", "fallback":
			counts[industry.Confidence]++
		default:
			t.Errorf("код %q: неизвестный confidence %q", code, industry.Confidence)
		}
	}
	if counts["rule"] != 61 || counts["manual"] != 26 || counts["fallback"] != 0 {
		t.Errorf("confidence: rule=%d manual=%d fallback=%d, want 61/26/0", counts["rule"], counts["manual"], counts["fallback"])
	}
}

func TestPublicIndustryForCodeSpots(t *testing.T) {
	cases := []struct {
		code string
		slug string
	}{
		{"01.4", "agriculture"},
		{"05", "mining"},
		{"10.1", "manufacturing_consumer"},
		{"10.9", "manufacturing_intermediate"},
		{"29.10", "manufacturing_investment"},
		{"23", "manufacturing_intermediate"},
		{"35", "electricity"},
		{"43.2", "construction"},
		{"45.11", "trade_auto"},
		{"46.32", "trade_wholesale"},
		{"47.22", "trade_retail"},
		{"49.3", "transport"},
		{"86.90.4", "services"},
	}
	for _, tc := range cases {
		industry, err := publicIndustryForCode(tc.code)
		if err != nil {
			t.Errorf("publicIndustryForCode(%q): %v", tc.code, err)
			continue
		}
		if industry.Slug != tc.slug {
			t.Errorf("publicIndustryForCode(%q) = %q, want %q", tc.code, industry.Slug, tc.slug)
		}
	}
}

func TestPublicIndustryForCodeRejectsUnknown(t *testing.T) {
	for _, code := range []string{"", "1", "abc", "99", "44"} {
		if _, err := publicIndustryForCode(code); err == nil {
			t.Errorf("publicIndustryForCode(%q) вернул nil, want ошибку", code)
		}
	}
}

// TestBsuMapRows: 87 строк, ключ code уникален, источник — публичный архив, fallback не
// используется. Сортировка не проверяется: порядок повторяет модель, а витрина сортирует.
func TestBsuMapRows(t *testing.T) {
	rows, err := bsuMapRows()
	if err != nil {
		t.Fatalf("bsuMapRows: %v", err)
	}
	if len(rows) != 87 {
		t.Fatalf("строк %d, want 87", len(rows))
	}
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		if seen[row.Code] {
			t.Errorf("код %q встречается дважды", row.Code)
		}
		seen[row.Code] = true
		if row.Source != cbrMpBsuMapSource {
			t.Errorf("код %q: source %q, want %q", row.Code, row.Source, cbrMpBsuMapSource)
		}
		if row.Confidence == "fallback" {
			t.Errorf("код %q: fallback в текущем наборе не ожидался", row.Code)
		}
	}
}

func TestCbrMpBsuMapDdl(t *testing.T) {
	if len(cbrMpBsuMapImporter.CreateTable) != 1 {
		t.Fatalf("DDL-инструкций %d, want 1", len(cbrMpBsuMapImporter.CreateTable))
	}
	ddl := cbrMpBsuMapImporter.CreateTable[0]
	for _, want := range []string{
		"code           LowCardinality(String)",
		"industry_sheet LowCardinality(String)",
		"industry_slug  LowCardinality(String)",
		"basis          String",
		"confidence     LowCardinality(String)",
		"source         LowCardinality(String)",
		"ENGINE = ReplacingMergeTree(loaded_at)",
		"ORDER BY code",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL не содержит %q:\n%s", want, ddl)
		}
	}
}

func TestCbrMpBsuMapViews(t *testing.T) {
	if cbrMpBsuMapView.Name != "v_cbr_mp_bsu_map" {
		t.Errorf("view = %q, want v_cbr_mp_bsu_map", cbrMpBsuMapView.Name)
	}
	if len(cbrMpBsuMapView.Tables) != 2 || cbrMpBsuMapView.Tables[1] != "rosstat_okved2" {
		t.Errorf("v_cbr_mp_bsu_map.Tables = %v, want [cbr_mp_bsu_map rosstat_okved2]", cbrMpBsuMapView.Tables)
	}
	for _, want := range []string{"cbr_mp_bsu_map", "rosstat_okved2", "LEFT JOIN", "okved_name"} {
		if !strings.Contains(cbrMpBsuMapView.Select, want) {
			t.Errorf("v_cbr_mp_bsu_map не содержит %q", want)
		}
	}

	if cbrMpPublicPanelView.Name != "v_cbr_mp_public_panel" {
		t.Errorf("view = %q, want v_cbr_mp_public_panel", cbrMpPublicPanelView.Name)
	}
	if len(cbrMpPublicPanelView.Tables) != 2 || cbrMpPublicPanelView.Tables[1] != mpSurveyTable {
		t.Errorf("v_cbr_mp_public_panel.Tables = %v, want [cbr_mp_bsu_map macro_series]", cbrMpPublicPanelView.Tables)
	}
	for _, want := range []string{
		"cbr_survey", "_ibk_sa", "_price_expectations_sa", "_cost_change_sa",
		"'ibk' AS indicator", "'price_exp'", "'costs'",
	} {
		if !strings.Contains(cbrMpPublicPanelView.Select, want) {
			t.Errorf("v_cbr_mp_public_panel не содержит %q", want)
		}
	}
}
