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
// визуальных строк: в FY2024 расшифровка «(if not mentioned otherwise)» набрана
// мельче и уехала на 0.77pt ниже подписей колонок — подписи периодов остались на
// 60.83, а расшифровка на 61.60. Обе строки — шапка, и columnsFromHeader должен
// получить их вместе: слова внутри скобки «(if not mentioned otherwise)» делят
// одну строку с подписями периодов, и по X они стоят до первого периода.
//
// Порог близости берётся отдельной константой и применяется здесь, а не в
// groupByLine: у настоящих соседних строк таблицы разрыв на порядок больше
// (в русском релизе ближайшая строка ниже шапки — «Операционные показатели» на
// 383.42, то есть на 14pt ниже), и расширение общего допуска склеило бы строки
// таблицы по всей странице.
func headerBandFrom(t *testing.T, path string) []Line {
	t.Helper()

	// headerContinuationGap — максимальный разрыв по вертикали, при котором
	// строка считается продолжением той же строки PDF.
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

	for _, want := range []string{"2024FY", "2023FY", "2024H2", "2024H1", "2023H2"} {
		if !slices.Contains(periods, want) {
			t.Errorf("period %s missing from %v", want, periods)
		}
	}

	// метки Y-o-Y и H-o-H не стали периодами
	for _, c := range cols {
		if c.Period == "Y-o-Y" || c.Period == "H-o-H" {
			t.Errorf("change label became a period: %q", c.Period)
		}
	}
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

	// строка TCC (y=515.92): Left значения строки, Left подписи периода и сам период
	cases := []struct {
		valueX  float64
		periodX float64
		period  string
	}{
		{290.33, 282.05, "2026H1"},
		{351.07, 339.91, "2025H1"},
		{467.02, 455.83, "2025H2"},
	}

	for _, c := range cases {
		want := columnCovering(t, cols, c.periodX)
		if want.Period != c.period {
			t.Fatalf("label at x=%v sits in column %q, want %q", c.periodX, want.Period, c.period)
		}

		got := columnCovering(t, cols, c.valueX)
		if got != want {
			t.Errorf("value at x=%v lands in %v, want the column of %s (%v)", c.valueX, got, c.period, want)
		}
	}

	// значение «64%» стоит под колонкой изменения, а не под периодом
	if change := columnCovering(t, cols, 408.19); change.Period != "" {
		t.Errorf("value at x=408.19 landed in period column %q, want the change column", change.Period)
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
