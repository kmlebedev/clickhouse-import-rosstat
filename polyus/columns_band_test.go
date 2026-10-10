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

// TestHeaderBandStopsAtBothPredicatesRow держит ветку isDataLine в первой
// проверке headerBand — единственную, которая отделяет настоящую строку данных
// от строки шапки, когда строка отвечает ОБОИМ признакам сразу.
//
// Метрика, в строке которой есть хотя бы две метки периода, делает строку данных
// ещё и строкой шапки: isPeriodHeaderLine считает периоды по словам и не смотрит,
// есть ли в строке числа, а isDataLine видит и метрику («Total revenue»), и
// число. Так выглядит, например, строка «Total revenue 2024 2023 4,674» —
// isPeriodHeaderLine и isDataLine на ней обе true (проверяется ниже отдельным
// утверждением, иначе тест перестал бы быть про этот случай).
//
// Без ветки isDataLine такая строка приклеилась бы к полосе (первое условие
// headerBand её пропускает), и метрика молча потерялась бы: строка ушла бы в
// columnsFromHeader шапкой, а её значение — нет. TestHeaderBandStopsAtDataRow это
// не ловит: он проверяет только, что в полосе нет строк с isDataLine == true, а
// это выполняется само собой, как только полоса на такой строке уже остановилась.
//
// Вход синтетический: фикстура не нужна — важна не геометрия, а совпадение двух
// признаков на одной строке. Достаточно, чтобы строка стояла ПОД якорем: цикл
// начинает с start+1 и якорь не перепроверяет.
func TestHeaderBandStopsAtBothPredicatesRow(t *testing.T) {
	const (
		anchorTop = 200.0
		dataTop   = 212.0
	)

	lines := []Line{
		{Top: anchorTop, Words: []Word{
			{Text: "$", Left: 60.0, Top: anchorTop, Width: 5.0},
			{Text: "million", Left: 68.0, Top: anchorTop, Width: 25.0},
			{Text: "2024", Left: 300.0, Top: anchorTop, Width: 18.0},
			{Text: "2023", Left: 400.0, Top: anchorTop, Width: 18.0},
		}},
		{Top: dataTop, Words: []Word{
			{Text: "Total", Left: 60.0, Top: dataTop, Width: 20.0},
			{Text: "revenue", Left: 84.0, Top: dataTop, Width: 30.0},
			{Text: "2024", Left: 300.0, Top: dataTop, Width: 18.0},
			{Text: "2023", Left: 400.0, Top: dataTop, Width: 18.0},
			{Text: "4,674", Left: 500.0, Top: dataTop, Width: 25.0},
		}},
	}

	start := firstHeaderLine(lines)
	if start != 0 {
		t.Fatalf("fixture broken: header anchor at index %d, want 0", start)
	}

	// Фикстура обязана грузить именно ту развилку, ради которой тест написан:
	// строка под якорем отвечает ОБОИМ признакам. Если это перестанет быть так,
	// тест перестанет проверять ветку isDataLine — и должен упасть здесь, а не
	// молча green.
	dataRow := lines[1]
	if !isPeriodHeaderLine(dataRow) || !isDataLine(dataRow) {
		t.Fatalf(
			"fixture no longer has both predicates: isPeriodHeaderLine=%v isDataLine=%v for %q",
			isPeriodHeaderLine(dataRow),
			isDataLine(dataRow),
			lineText(dataRow),
		)
	}

	band := headerBand(lines, start)

	for _, l := range band {
		if l.Top == dataTop {
			t.Fatalf("band swallowed the both-predicates data row: %q", lineText(l))
		}
	}

	// Вторая половина утверждения: из строки данных не выводится ни один период.
	// Даже если бы строка попала в полосу, «2024» и «2023» в ней дали бы колонки,
	// и метрика «Total revenue» встала бы под чужой период — этого не произошло.
	for _, c := range columnsFromHeader(band) {
		if c.Period == "" {
			continue
		}

		if c.Period != "2024FY" && c.Period != "2023FY" {
			t.Errorf("period %q was derived from the data row, want none", c.Period)
		}
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
