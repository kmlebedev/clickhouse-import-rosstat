package polyus

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// headerBandFrom открывает TSV-снимок и возвращает полосу шапки финансовой
// таблицы: строку с меткой единиц измерения вместе со следующими за ней
// строками шапки (граница — по роли строки, см. headerBand).
//
// Само правило полосы (роль строки, подписи колонок изменений, признак метки
// единиц) живёт в production — columns.go, — и вызывается здесь как есть: разбор
// шапки и его проверка обязаны читать один и тот же код, иначе тест закреплял бы
// копию правила, а не правило.
//
// Помощник отдаёт ровно ту полосу, которую собрал бы production, и на фикстуре
// FY2024 это ОДНА строка: расшифровка «if not mentioned otherwise» (61.60) —
// строка-заполнитель, ни шапка, ни данные, и полоса на ней закрывается (см.
// TestHeaderBandKeepsSingleLineHeader). Что даёт эта вторая строка, если собрать
// полосу шире production, закрепляет TestColumnsPeriodsInvariantUnderBandWidth:
// периоды и их границы от ширины полосы не зависят (247.315/321.365/355.035/
// 427.035/497.04 у обеих полос), а геометрия непериодных колонок перед первым
// периодом — зависит.
func headerBandFrom(t *testing.T, path string) []Line {
	t.Helper()

	lines := tsvLines(t, path)

	start := firstHeaderLine(lines)
	if start < 0 {
		t.Fatalf("no header line with '$ млн'/'$ mln' in %s", path)
	}

	return headerBand(lines, start)
}

// tsvLines открывает TSV-снимок и возвращает его визуальные строки.
func tsvLines(t *testing.T, path string) []Line {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	words, err := parseTSV(f)
	if err != nil {
		t.Fatal(err)
	}

	return groupByLine(words, 0)
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
// перед первым периодом: одна строка даёт 12 колонок, две — 16, потому что слова
// расшифровки докалывают промежуток 111.31→247.31 и разрезают его на отдельные
// колонки. Тест утверждает обе половины сразу: если сигнатуру columnsFromHeader
// когда-нибудь сведут обратно к одной Line, сломается вторая половина — и
// наоборот, если полосу начнут собирать шире, чем нужно, упадут обе.
//
// Прежний замер давал 9 и 13 колонок. Числа выросли на подписи колонок изменений
// («Y-o-Y», «H-o-H», и «Y-o-Y» в конце полосы): matchPeriodAt перестал захватывать
// их в окно периода (см. его комментарий), и каждая подпись стала отдельной
// непериодной колонкой — той самой, которой требовала задача о KPI-парсере:
// иначе значение «5 ppts» колонки изменения попадало в колонку соседнего периода.
func TestColumnsPeriodsInvariantUnderBandWidth(t *testing.T) {
	var oneLine, twoLines []Line
	for _, l := range tsvLines(t, "testdata/press_release_fy2024_p4.tsv") {
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
	if len(narrow) != 12 || len(wide) != 16 {
		t.Errorf("column counts = %d (one line) / %d (two lines), want 12 / 16", len(narrow), len(wide))
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
	lines := tsvLines(t, "testdata/press_reliz_1h26_p1.tsv")

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

func TestColumnsFromBrokenHeader2019(t *testing.T) {
	// «4Q» стоит на top=306.99, а год «2019» — на top=317.31. Период
	// «4Q 2019» обязан собраться из двух разных строк по X-координате.
	lines, err := readTSVLines("testdata/press_release_fy2019_p3.tsv")
	if err != nil {
		t.Fatal(err)
	}

	cols := columnsFromHeader(headerBand(lines, firstHeaderLine(lines)))

	var periods []string
	for _, c := range cols {
		if c.Period != "" {
			periods = append(periods, c.Period)
		}
	}
	for _, want := range []string{"2019Q4", "2019Q3", "2019FY", "2018FY"} {
		if !slices.Contains(periods, want) {
			t.Errorf("период %s не распознан; получено %v", want, periods)
		}
	}
	// колонка-изменение Q-o-Q не должна стать периодом
	if slices.Contains(periods, "Q-o-Q") {
		t.Error("Q-o-Q стал периодом")
	}
}
