package polyus

import (
	"os"
	"strings"
	"testing"
)

func TestParseTSVReadsWordsOnly(t *testing.T) {
	f, err := os.Open("testdata/press_reliz_1h26_p1.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	words, err := parseTSV(f)
	if err != nil {
		t.Fatalf("parseTSV: %v", err)
	}
	if len(words) == 0 {
		t.Fatal("no words parsed")
	}
	for _, w := range words {
		if w.Text == "" {
			t.Error("empty word text")
		}
		if strings.HasPrefix(w.Text, "###") {
			t.Errorf("service marker leaked into words: %q", w.Text)
		}
	}
	// «Производство» присутствует и имеет положительные координаты
	var found bool
	for _, w := range words {
		if w.Text == "Производство" {
			found = true
			if w.Left <= 0 || w.Top <= 0 {
				t.Errorf("Производство at non-positive coords: left=%v top=%v", w.Left, w.Top)
			}
		}
	}
	if !found {
		t.Error("Производство not found")
	}
}

// TestParseTSVSkipsRepeatedHeaderRow держит пропуск строки заголовка в середине
// склеенного файла.
//
// extractPDF извлекает страницы по отдельности, а joinFiles склеивает их, и в
// многостраничном отчёте (например, МСФО 1H2026 — страницы 6 и 7) заголовок TSV
// повторяется посередине файла. Заголовок печатает столько же колонок, сколько
// формат (12), поэтому отбрасывается он по имени первой колонки, а не по числу
// полей; иначе его поля уехали бы в разбор и Atoi("level") молча пропустил бы
// строку — то есть правило держалось бы на ошибке разбора, а не на формате.
func TestParseTSVSkipsRepeatedHeaderRow(t *testing.T) {
	const row = "5\t6\t0\t3\t0\t0\t70.94\t149.17\t16.93\t7.44\t100\tGold\n"

	words, err := parseTSV(strings.NewReader(tsvHeaderRow() + row + tsvHeaderRow() + row))
	if err != nil {
		t.Fatalf("parseTSV: %v", err)
	}
	if len(words) != 2 {
		t.Fatalf("got %d words, want 2: the repeated header row leaked into the parse", len(words))
	}
	for _, w := range words {
		if w.Text != "Gold" {
			t.Errorf("word text = %q, want Gold", w.Text)
		}
	}
}

// tsvHeaderRow возвращает строку заголовка TSV-снимка — ровно те 12 колонок,
// которые печатает pdftotext -tsv.
func tsvHeaderRow() string {
	return strings.Join(
		[]string{
			"level", "page_num", "par_num", "block_num", "line_num", "word_num",
			"left", "top", "width", "height", "conf", "text",
		},
		"\t",
	) + "\n"
}

func TestGroupByLineOrdersWordsByLeft(t *testing.T) {
	// три слова на одном Top, поданные в обратном порядке
	words := []Word{
		{Text: "674", Left: 296.21, Top: 434.80},
		{Text: "Выручка", Left: 56.64, Top: 434.80},
		{Text: "4", Left: 290.33, Top: 434.80},
	}
	lines := groupByLine(words, 0)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	got := []string{lines[0].Words[0].Text, lines[0].Words[1].Text, lines[0].Words[2].Text}
	want := []string{"Выручка", "4", "674"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("word %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestGroupByLineSeparatesDifferentTops(t *testing.T) {
	words := []Word{
		{Text: "a", Left: 10, Top: 100},
		{Text: "b", Left: 10, Top: 120},
	}
	if got := groupByLine(words, 0); len(got) != 2 {
		t.Errorf("got %d lines, want 2", len(got))
	}
}

func TestGroupByLineDeltaJoinsNearbyTops(t *testing.T) {
	// дрейф базовой линии 1.5pt склеивается при delta=2, но не при delta=0
	words := []Word{
		{Text: "a", Left: 10, Top: 100.0},
		{Text: "b", Left: 20, Top: 101.5},
	}
	if got := groupByLine(words, 2); len(got) != 1 {
		t.Errorf("delta=2: got %d lines, want 1", len(got))
	}
	if got := groupByLine(words, 0); len(got) != 2 {
		t.Errorf("delta=0: got %d lines, want 2", len(got))
	}
}
