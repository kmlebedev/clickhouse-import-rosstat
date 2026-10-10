package polyus

import (
	"strings"
	"testing"
)

func TestHeaderBandSpansBrokenHeader(t *testing.T) {
	// шапка 2019 занимает строки top=306.99, 312.03, 317.31; между ними
	// зазоры 5.04 и 5.28pt — больше прежнего критерия в 2pt.
	lines, err := readTSVLines("testdata/press_release_fy2019_p3.tsv")
	if err != nil {
		t.Fatal(err)
	}

	start := firstHeaderLine(lines)
	if start < 0 {
		t.Fatal("шапка не найдена")
	}
	if got := len(headerBand(lines, start)); got < 3 {
		t.Errorf("band = %d строк, want >= 3: разорванная шапка не собрана целиком", got)
	}
}

func TestHeaderBandKeepsSingleLineHeader(t *testing.T) {
	// у 2022–2024 шапка одной строкой: граница не должна её вырастить
	lines, err := readTSVLines("testdata/press_release_fy2024_p4.tsv")
	if err != nil {
		t.Fatal(err)
	}

	start := firstHeaderLine(lines)
	if start < 0 {
		t.Fatal("шапка не найдена")
	}
	if got := len(headerBand(lines, start)); got != 1 {
		t.Errorf("band = %d строк, want 1: цельная шапка не должна расти", got)
	}
}

func TestHeaderBandStopsAtDataRow(t *testing.T) {
	// строка данных (метрика + числа) закрывает шапку даже если несёт метку года
	lines, err := readTSVLines("testdata/press_release_fy2019_p3.tsv")
	if err != nil {
		t.Fatal(err)
	}
	start := firstHeaderLine(lines)
	band := headerBand(lines, start)
	for _, l := range band {
		if isDataLine(l) {
			t.Errorf("строка данных попала в шапку: %q", lineText(l))
		}
	}
}

func TestHeaderBandLooksPastFillerLine(t *testing.T) {
	// У MD&A за 2014 год расшифровка метки единиц разорвана, и её вторая
	// половина «(if not mentioned otherwise)» (top=126.71) стоит МЕЖДУ шапкой
	// (125.49) и подписью колонки изменения «change» (132.45). Заполнитель не
	// несёт ни меток колонок, ни чисел, и полоса, рвущаяся на нём, теряет
	// колонку изменения: «3%» на x=416.11 накрывается периодом «2H 2014».
	// Проверяется содержимое, а не ширина: важно, что «change» доехало; сам
	// заполнитель в полосу не входит — он пропускается заглядыванием.
	lines, err := readTSVLines("testdata/press_release_hist_p1.tsv")
	if err != nil {
		t.Fatal(err)
	}

	start := firstHeaderLine(lines)
	if start < 0 {
		t.Fatal("шапка не найдена")
	}

	band := headerBand(lines, start)

	var change bool

	for _, l := range band {
		if strings.Contains(lineText(l), "change") {
			change = true
		}
	}

	if !change {
		t.Errorf("полоса не дотянулась до «change»: %d строк, %v", len(band), bandTexts(band))
	}
}

// bandTexts возвращает тексты строк полосы — для сообщения об ошибке.
func bandTexts(band []Line) []string {
	texts := make([]string, 0, len(band))
	for _, l := range band {
		texts = append(texts, lineText(l))
	}

	return texts
}
