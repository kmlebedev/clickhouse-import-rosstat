package rosstat

import (
	"os"
	"strings"
	"testing"

	"golang.org/x/text/encoding/charmap"
)

// TestParseOkved2Meta: meta.csv отдаётся в CP1251, а URL выпуска — латиница. Проверяется
// именно декодирование: без него property/value читаются, а кириллица в title ломается
// молча; тест фиксирует и выбор первой (самой свежей) строки data-*.csv.
func TestParseOkved2Meta(t *testing.T) {
	fixture := "property,value\r\n" +
		"standardversion,https://example.test/standard.pdf\r\n" +
		"title,\"Общероссийский классификатор видов экономической деятельности (ОКВЭД2)\"\r\n" +
		"modified,20261001\r\n" +
		"data-20261001T1110-structure.csv,https://example.test/data-20261001T1110.csv\r\n" +
		"data-20260801T1408-structure.csv,https://example.test/data-20260801T1408.csv\r\n"
	body, err := charmap.Windows1251.NewEncoder().Bytes([]byte(fixture))
	if err != nil {
		t.Fatalf("кодирование фикстуры в windows-1251: %v", err)
	}
	url, err := parseOkved2Meta(body)
	if err != nil {
		t.Fatalf("parseOkved2Meta: %v", err)
	}
	if want := "https://example.test/data-20261001T1110.csv"; url != want {
		t.Errorf("URL = %q, want %q (первая строка data-*.csv — самая свежая)", url, want)
	}
}

func TestParseOkved2MetaNoDataRow(t *testing.T) {
	body, err := charmap.Windows1251.NewEncoder().Bytes([]byte("property,value\r\nmodified,20261001\r\n"))
	if err != nil {
		t.Fatalf("кодирование: %v", err)
	}
	if _, err := parseOkved2Meta(body); err == nil {
		t.Fatal("parseOkved2Meta без data-*.csv вернул nil, want ошибку")
	}
}

// TestParseOkved2 проверяет уровни и родителей на структурной фикстуре и заодно
// чувствительность к BOM: файл источника начинается с UTF-8 BOM, без снятия которого
// первая колонка раздела стала бы "\ufeff\"A\"".
func TestParseOkved2(t *testing.T) {
	body, err := os.ReadFile("testdata/okved2_data.csv")
	if err != nil {
		t.Fatalf("чтение фикстуры: %v", err)
	}
	body = append([]byte("\xef\xbb\xbf"), body...)
	rows, err := parseOkved2(body)
	if err != nil {
		t.Fatalf("parseOkved2: %v", err)
	}
	if len(rows) != 12 {
		t.Fatalf("строк %d, want 12", len(rows))
	}
	if got := rows[0]; got.Section != "A" || got.Code != "" || got.Level != 1 || got.ParentCode != "" {
		t.Errorf("строка раздела = %+v, want section A, пустой code, level 1, пустой parent", got)
	}
	byCode := make(map[string]okved2Row, len(rows))
	for _, row := range rows {
		byCode[row.Code] = row
	}
	cases := []struct {
		code   string
		level  uint8
		parent string
	}{
		{"01", 2, ""},
		{"01.1", 3, "01"},
		{"01.11", 4, "01.1"},
		{"01.11.1", 5, "01.11"},
		{"01.11.11", 6, "01.11.1"},
		{"47.2", 3, "47"},
		{"95.1", 3, "95"},
	}
	for _, tc := range cases {
		row, ok := byCode[tc.code]
		if !ok {
			t.Errorf("код %q не найден", tc.code)
			continue
		}
		if row.Level != tc.level || row.ParentCode != tc.parent {
			t.Errorf("код %q: level=%d parent=%q, want level=%d parent=%q", tc.code, row.Level, row.ParentCode, tc.level, tc.parent)
		}
	}
	if name := byCode["47.2"].Name; !strings.Contains(name, ";") {
		t.Errorf("название 47.2 = %q, want точку с запятой внутри кавычек (csv не должен резать поле)", name)
	}
}

func TestParseOkved2RejectsBadRow(t *testing.T) {
	if _, err := parseOkved2([]byte("A;\"01\";\"Класс\";лишнее\n")); err == nil {
		t.Fatal("parseOkved2 с четырьмя полями вернул nil, want ошибку")
	}
	if _, err := parseOkved2(nil); err == nil {
		t.Fatal("parseOkved2 с пустым телом вернул nil, want ошибку")
	}
}

// TestRosstatOkved2Ddl: DDL — контракт данных (правило 5 AGENTS.md: движущая колонка
// ReplacingMergeTree, ключ (section, code), измерения LowCardinality). Литералы, а не
// ссылки на DDL: тест должен падать на правке типа/ключа, а не повторять её.
func TestRosstatOkved2Ddl(t *testing.T) {
	if len(rosstatOkved2Importer.CreateTable) != 1 {
		t.Fatalf("DDL-инструкций %d, want 1", len(rosstatOkved2Importer.CreateTable))
	}
	ddl := rosstatOkved2Importer.CreateTable[0]
	for _, want := range []string{
		"section        LowCardinality(String)",
		"code           LowCardinality(String)",
		"name           String",
		"level          UInt8",
		"parent_code    LowCardinality(String)",
		"source_version LowCardinality(String)",
		"ENGINE = ReplacingMergeTree(loaded_at)",
		"ORDER BY (section, code)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL не содержит %q:\n%s", want, ddl)
		}
	}
}

func TestRosstatOkved2View(t *testing.T) {
	if rosstatOkved2View.Name != "v_rosstat_okved2" {
		t.Errorf("view = %q, want v_rosstat_okved2", rosstatOkved2View.Name)
	}
	if len(rosstatOkved2View.Tables) != 1 || rosstatOkved2View.Tables[0] != rosstatOkved2Table {
		t.Errorf("view.Tables = %v, want [%s]", rosstatOkved2View.Tables, rosstatOkved2Table)
	}
	for _, col := range []string{"section", "code", "name", "level", "parent_code", "source_version"} {
		if _, ok := rosstatOkved2View.Columns[col]; !ok {
			t.Errorf("view.Columns без комментария для %q", col)
		}
	}
}
