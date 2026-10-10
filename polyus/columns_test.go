package polyus

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// headerBandFrom открывает TSV-снимок и возвращает полосу шапки финансовой
// таблицы: строку с меткой единиц измерения вместе со строками, которые от неё
// неотличимо близки по вертикали и потому принадлежат той же строке PDF.
//
// Полоса, а не одна строка, потому что одна строка PDF может быть набрана
// разными кеглями и после groupByLine по точному Top распадается на несколько
// визуальных строк. В FY2024 так и есть: расшифровка «(if not mentioned
// otherwise)» набрана мельче и уехала на 0.77pt ниже подписей колонок — метка
// «$ million ( )» и ВСЕ пять периодов на 60.83, расшифровка на 61.60.
//
// Замерено на фикстуре, что именно даёт вторая строка:
//
//	полоса 60.83            — 9 колонок, периоды [2024FY 2023FY 2024H2 2024H1 2023H2]
//	полоса 60.83 + 61.60    — 13 колонок, периоды [2024FY 2023FY 2024H2 2024H1 2023H2]
//	границы периодов        — совпадают до последнего знака: 247.315/321.365/355.035/427.035/497.04
//
// То есть вторая строка НЕ нужна, чтобы увидеть периоды, — она уточняет геометрию
// непериодных колонок перед первым периодом (4 колонки вместо 8): слова
// «if not mentioned otherwise» попадают между «(» и «)» и разрезают промежуток
// 111.31→247.31 на отдельные колонки с границами 115.12 / 119.55 / 131.07 /
// 165.69 / 196.58. Подпись длинной метки вроде «Выручка» ложится в самый левый
// промежуток, и от того, разрезан ли он, зависит, останется она в первой колонке
// или уедет в колонку изменений. Тест TestColumnsPeriodsInvariantUnderBandWidth
// закрепляет обе половины этого факта: периоды от ширины полосы не зависят, а
// число колонок — зависит.
//
// Порог близости берётся отдельной константой и применяется здесь, а не в
// groupByLine: 2.0pt взято с запасом над единственным наблюдаемым разрывом
// шапки (0.77pt) и с большим отступлением от ближайшей настоящей строки
// содержимого. В русском релизе ближайшая строка ниже шапки — «Операционные
// показатели» на 383.42, то есть на 14.16pt ниже шапки (369.26), и это на
// порядок больше порога.
//
// const ниже — знаменатель той же оценки; менять его значение без нового замера
// по фикстурам не стоит.
func headerBandFrom(t *testing.T, path string) []Line {
	t.Helper()

	// headerContinuationGap — максимальный разрыв по вертикали, при котором
	// строка считается продолжением той же строки PDF. Единственный известный
	// разрыв внутри шапки — 0.77pt (FY2024); ближайшая строка содержимого —
	// на 14.16pt ниже (RU 1H2026), поэтому 2.0pt лежит между ними с запасом в
	// обе стороны.
	const headerContinuationGap = 2.0

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	words, err := parseTSV(f)
	if err != nil {
		t.Fatal(err)
	}

	lines := groupByLine(words, 0)

	start := -1
	for i, l := range lines {
		if unitsMarker(l) {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("no header line with '$ млн'/'$ mln' in %s", path)
	}

	band := []Line{lines[start]}
	for i := start + 1; i < len(lines); i++ {
		if lines[i].Top-lines[start].Top > headerContinuationGap {
			break
		}

		band = append(band, lines[i])
	}

	return band
}

// unitsMarker сообщает, что строка несёт метку единиц измерения таблицы. Слова
// склеиваются без пробела: в TSV маркер разбит на отдельные слова
// («$» + «млн»), а в остальном тексте страницы словосочетание не встречается.
func unitsMarker(l Line) bool {
	var glued strings.Builder
	for _, w := range l.Words {
		glued.WriteString(w.Text)
	}

	text := glued.String()

	return strings.Contains(text, "$млн") || strings.Contains(text, "$million")
}

// lineText склеивает слова строки в текст через пробел.
func lineText(l Line) string {
	parts := make([]string, 0, len(l.Words))
	for _, w := range l.Words {
		parts = append(parts, w.Text)
	}

	return strings.Join(parts, " ")
}

// findLineContaining ищет строку, в тексте которой есть подстрока.
func findLineContaining(t *testing.T, lines []Line, want string) Line {
	t.Helper()

	for _, l := range lines {
		if strings.Contains(lineText(l), want) {
			return l
		}
	}

	t.Fatalf("no line containing %q", want)

	return Line{}
}

// isPeriodHeaderLine сообщает, что строка — строка шапки таблицы: она несёт не
// меньше двух непересекающихся меток колонок-периодов либо подписи колонок
// изменений.
//
// Порог в две метки отсекает строки данных: «Объем горной массы, тыс. м³
// 118 967 108 064 10%» походит на набор периодов («108 064»), но непересекающихся
// меток в ней меньше двух. Шапка же всегда несёт минимум два периода — сравнивать
// больше нечего, если колонка одна.
//
// Метки периодов ищутся тем же разбором, что и в columnsFromHeader, и окно
// сдвигается на его ширину: иначе «1 п/г 2026» засчиталось бы трижды — как «1»,
// «1 п/г» и «1 п/г 2026».
func isPeriodHeaderLine(l Line) bool {
	text := lineText(l)
	for _, label := range []string{"Изм. за", "Y-o-Y", "H-o-H"} {
		if strings.Contains(text, label) {
			return true
		}
	}

	texts := make([]string, 0, len(l.Words))
	for _, w := range l.Words {
		texts = append(texts, w.Text)
	}

	for _, marker := range []string{"$млн", "$million"} {
		if strings.Contains(strings.Join(texts, ""), marker) {
			return true
		}
	}

	found := 0

	for i := 0; i < len(texts); {
		_, width, err := matchPeriodAt(texts, i, maxPeriodTokens)
		if err != nil {
			i++

			continue
		}

		found++

		i += width
	}

	return found >= 2
}

func TestColumnsFromHeaderRu(t *testing.T) {
	// реальная шапка финансовой таблицы русского релиза 1H2026 (y=369.26)
	h := headerBandFrom(t, "testdata/press_reliz_1h26_p1.tsv")
	cols := columnsFromHeader(h)

	var periods []string
	for _, c := range cols {
		if c.Period != "" {
			periods = append(periods, c.Period)
		}
	}

	want := []string{"2026H1", "2025H1", "2025H2"}
	if !slices.Equal(periods, want) {
		t.Errorf("periods = %v, want %v", periods, want)
	}

	// «Изм. за год» и «Изм. за п/г» — колонки без периода, но они обязаны
	// занимать место по X: без них значения справа сдвинулись бы влево. Считаем
	// их порядок: сначала идёт колонка-период, затем непериодная колонка
	// изменений, и так по всей шапке; колонки изменений — ровно две, по одной
	// после 2025H1 и после 2025H2. Непериодные колонки перед первым периодом —
	// слова метки «$ млн (если не указано иное)», а не столбцы таблицы.
	sub := columnsWithoutLeadingLabel(cols)

	shapes := make([]string, 0, len(sub))
	for _, c := range sub {
		if c.Period == "" {
			// Соседние непериодные слова — одна колонка изменений, а не
			// несколько: «Изм. за год» состоит из трёх слов.
			if len(shapes) > 0 && shapes[len(shapes)-1] == "change" {
				continue
			}

			shapes = append(shapes, "change")

			continue
		}

		shapes = append(shapes, c.Period)
	}

	wantShapes := []string{"2026H1", "2025H1", "change", "2025H2", "change"}
	if !slices.Equal(shapes, wantShapes) {
		t.Errorf("table columns = %v, want %v", shapes, wantShapes)
	}
}

// TestColumnsFromHeaderRuCoverChangedValues проверяет, что каждое значение строки
// TCC (y=515.92) находит ровно одну колонку: 1 069 под 2026H1, 653 под 2025H1,
// 64% под «Изм. за год», 814 под 2025H2, 31% под «Изм. за п/г».
func TestColumnsFromHeaderRuCoverChangedValues(t *testing.T) {
	cols := columnsFromHeader(headerBandFrom(t, "testdata/press_reliz_1h26_p1.tsv"))

	for _, valueX := range []float64{290.33, 351.07, 408.19, 467.02, 524.14} {
		var covering []Column
		for _, c := range cols {
			if c.covers(valueX) {
				covering = append(covering, c)
			}
		}

		if len(covering) != 1 {
			t.Errorf("x=%v covered by %d columns (%v), want exactly 1", valueX, len(covering), covering)
		}
	}
}

func TestColumnsFromHeaderWide(t *testing.T) {
	// шапка 2024FY: 5 периодов + 3 изменения без года
	h := headerBandFrom(t, "testdata/press_release_fy2024_p4.tsv")
	cols := columnsFromHeader(h)

	var periods []string
	for _, c := range cols {
		if c.Period != "" {
			periods = append(periods, c.Period)
		}
	}

	// ровно пять периодов в ожидаемом порядке: лишний распознанный период
	// (например, «Y-o-Y» или «H-o-H», разобранные как год) сдвинул бы список
	// и упал бы здесь, тогда как проверка «содержит» его бы пропустила.
	want := []string{"2024FY", "2023FY", "2024H2", "2024H1", "2023H2"}
	if !slices.Equal(periods, want) {
		t.Errorf("periods = %v, want %v", periods, want)
	}
}

// TestColumnsPeriodsInvariantUnderBandWidth закрепляет, что даёт вторая строка
// шапки FY2024 и что она НЕ даёт.
//
// Разрыв шапки на две визуальные строки (60.83 и 61.60) — следствие разного
// кегля: все пять подписей периодов лежат на 60.83 вместе с «$ million ( )»,
// а на 61.60 ушла только расшифровка «if not mentioned otherwise».
//
// Периоды от ширины полосы не зависят: обе полосы дают один и тот же список
// с теми же типами и теми же границами. Зависит геометрия непериодных колонок
// перед первым периодом: одна строка даёт 9 колонок, две — 13, потому что слова
// расшифровки докалывают промежуток 111.31→247.31 и разрезают его на отдельные
// колонки. Тест утверждает обе половины сразу: если сигнатуру columnsFromHeader
// когда-нибудь сведут обратно к одной Line, сломается вторая половина — и
// наоборот, если полосу начнут собирать шире, чем нужно, упадут обе.
func TestColumnsPeriodsInvariantUnderBandWidth(t *testing.T) {
	f, err := os.Open("testdata/press_release_fy2024_p4.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	words, err := parseTSV(f)
	if err != nil {
		t.Fatal(err)
	}

	var oneLine, twoLines []Line
	for _, l := range groupByLine(words, 0) {
		switch l.Top {
		case 60.83:
			oneLine = append(oneLine, l)
			twoLines = append(twoLines, l)
		case 61.60:
			twoLines = append(twoLines, l)
		}
	}
	if len(oneLine) != 1 || len(twoLines) != 2 {
		t.Fatalf("fixture layout changed: one-line band=%d, two-line band=%d", len(oneLine), len(twoLines))
	}

	narrow := columnsFromHeader(oneLine)
	wide := columnsFromHeader(twoLines)

	// Ширина полосы меняет число колонок: расшифровка докалывает промежуток
	// перед первым периодом.
	if len(narrow) != 9 || len(wide) != 13 {
		t.Errorf("column counts = %d (one line) / %d (two lines), want 9 / 13", len(narrow), len(wide))
	}

	wideNonPeriod := countNonPeriodColumns(wide)
	narrowNonPeriod := countNonPeriodColumns(narrow)
	if wideNonPeriod <= narrowNonPeriod {
		t.Errorf("non-period columns = %d (two lines) vs %d (one line), want the wide band to add some",
			wideNonPeriod, narrowNonPeriod)
	}

	// …но не меняет периоды: тот же список, те же типы, те же границы.
	narrowPeriods := periodColumns(narrow)
	widePeriods := periodColumns(wide)
	if !slices.Equal(narrowPeriods, widePeriods) {
		t.Errorf("periods depend on band width:\n one-line = %v\n two-line = %v", narrowPeriods, widePeriods)
	}
}

// periodColumns отбирает колонки-периоды: Period, Type, Left, Right. Сравнение
// целых структур включало бы расширенные границы непериодных колонок, поэтому
// берутся только периоды — они и должны совпадать при любой ширине полосы.
func periodColumns(cols []Column) []Column {
	periods := make([]Column, 0, len(cols))
	for _, c := range cols {
		if c.Period != "" {
			periods = append(periods, c)
		}
	}

	return periods
}

// countNonPeriodColumns считает непериодные колонки — те, что держат место по X
// для колонок изменений и метки единиц измерения.
func countNonPeriodColumns(cols []Column) int {
	count := 0
	for _, c := range cols {
		if c.Period == "" {
			count++
		}
	}

	return count
}

func TestColumnCoversValueX(t *testing.T) {
	// значение 2026H1 стоит на x=290.33, метка периода — на x=282.05
	col := Column{Period: "2026H1", Left: 282.05, Right: 329.0}
	if !col.covers(290.33) {
		t.Error("value at 290.33 must fall into the 2026H1 column")
	}

	col2 := Column{Period: "2025H1", Left: 339.91, Right: 386.0}
	if col2.covers(290.33) {
		t.Error("value at 290.33 must not fall into the 2025H1 column")
	}
}

func TestHeaderAppliesToRowsBelow(t *testing.T) {
	// На странице русского релиза ДВЕ шапки: производственная (y=206.51)
	// и финансовая с «$ млн» (y=369.26). Финансовые строки (y>369.26)
	// обязаны раскладываться по колонкам финансовой шапки, а не первой.
	f, err := os.Open("testdata/press_reliz_1h26_p1.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	words, err := parseTSV(f)
	if err != nil {
		t.Fatal(err)
	}

	lines := groupByLine(words, 0)

	// шапки — строки, в которых есть подписи колонок: производственная
	// (y=206.51, «1 п/г 26 …») и финансовая (y=369.26, «$ млн … 1 п/г 2026 …»)
	var headerTops []float64
	for _, l := range lines {
		if isPeriodHeaderLine(l) {
			headerTops = append(headerTops, l.Top)
		}
	}
	if len(headerTops) < 2 {
		t.Fatalf("expected at least 2 header lines on the page, got %v", headerTops)
	}

	// строка «Выручка» (y=434.80) обслуживается ближайшей шапкой сверху
	revenue := findLineContaining(t, lines, "Выручка")
	wantHeader := headerTops[len(headerTops)-1]
	for _, top := range headerTops {
		if top < revenue.Top && top > wantHeader-1e-9 {
			wantHeader = top
		}
	}

	if wantHeader != 369.26 {
		t.Errorf("Выручка served by header at y=%v, want 369.26", wantHeader)
	}
}

// TestHeaderColumnsCoverValueRow проверяет, что номер колонки, найденный по X
// значения, совпадает с номером колонки соответствующего периода. Это и есть
// требование задачи: колонка определяется координатой, а не порядком числа в
// строке — иначе колонка изменения («Изм. за год») сдвигала бы все значения
// слева от себя.
func TestHeaderColumnsCoverValueRow(t *testing.T) {
	cols := columnsFromHeader(headerBandFrom(t, "testdata/press_reliz_1h26_p1.tsv"))

	// строка TCC (y=515.92): все пять значений, Left значения и колонка, в
	// которую оно обязано попасть. Период — колонка периода, пустая строка —
	// непериодная колонка изменений («64%» под «Изм. за год», «31%» под
	// «Изм. за п/г»).
	cases := []struct {
		valueX  float64
		periodX float64
		period  string
	}{
		{290.33, 282.05, "2026H1"},
		{351.07, 339.91, "2025H1"},
		{408.19, 395.47, ""},
		{467.02, 455.83, "2025H2"},
		{524.14, 512.02, ""},
	}

	for _, c := range cases {
		want := columnCovering(t, cols, c.periodX)
		if want.Period != c.period {
			t.Fatalf("label at x=%v sits in column %q, want %q", c.periodX, want.Period, c.period)
		}

		got := columnCovering(t, cols, c.valueX)
		if got != want {
			t.Errorf("value at x=%v lands in %v, want the column at x=%v (%v)", c.valueX, got, c.periodX, want)
		}
	}
}

// columnCovering возвращает единственную колонку, накрывающую X. Несколько
// колонок (или ни одной) — ошибка теста: значение обязано попадать ровно в
// одну колонку, иначе разбор строки метрики неоднозначен.
func columnCovering(t *testing.T, cols []Column, x float64) Column {
	t.Helper()

	var covering []Column
	for _, c := range cols {
		if c.covers(x) {
			covering = append(covering, c)
		}
	}

	if len(covering) != 1 {
		t.Fatalf("x=%v covered by %d columns (%v), want exactly 1", x, len(covering), covering)
	}

	return covering[0]
}

// columnsWithoutLeadingLabel отбрасывает непериодные колонки, стоящие левее
// первого периода: это слова метки единиц измерения («$ млн …»), а не столбцы
// таблицы. Считаются так же, как их считает потребитель: до первого периода
// колонок таблицы нет.
func columnsWithoutLeadingLabel(cols []Column) []Column {
	for i, c := range cols {
		if c.Period != "" {
			return cols[i:]
		}
	}

	return nil
}
