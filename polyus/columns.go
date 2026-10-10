package polyus

import (
	"sort"
	"strings"
)

// infiniteX — внешняя граница крайних колонок: она не ограничена ничем, потому
// что за пределами шапки нет соседа, с которым можно поделить промежуток.
// Бесконечность заменена большим, но конечным числом: арифметика с Inf
// превратила бы середину промежутка в NaN, и covers перестал бы работать.
const infiniteX = 1e18

// maxPeriodTokens — самая длинная форма периода в шапке: «1 п/г 2026» —
// ровно те же три слова, что разбирает matchPeriodAt.
const maxPeriodTokens = 3

// headerContinuationGap — максимальный разрыв по вертикали, при котором строка
// считается продолжением той же строки PDF, что и предыдущая строка шапки.
//
// Единственный известный разрыв внутри шапки — 0.77pt (FY2024: расшифровка
// «(if not mentioned otherwise)» набрана мельче подписей колонок); ближайшая
// строка содержимого — на 14.16pt ниже шапки (RU 1H2026: «Операционные
// показатели» на 383.42 против шапки на 369.26). Порог 2.0pt лежит между ними
// с запасом в обе стороны. Менять значение без нового замера по фикстурам не
// стоит.
const headerContinuationGap = 2.0

// Column — колонка таблицы, выведенная из координат шапки, а не из порядка слов.
//
// Left и Right — уже расширенные границы для covers: между соседними колонками
// граница проходит по середине промежутка между правым краем левой и левым
// краем правой, у крайних колонок внешняя граница бесконечна.
type Column struct {
	Period string // Формат: 2024FY или 2026H1; пусто у непериодной колонки
	Type   string // Тип: FY, H или Q; пусто у непериодной колонки
	Left   float64
	Right  float64
}

// covers сообщает, попадает ли X значения в колонку.
func (c Column) covers(left float64) bool {
	return left >= c.Left && left < c.Right
}

// columnsFromHeader выводит колонки таблицы из полосы шапки.
//
// Полоса, а не одна строка, потому что шапка реальных релизов разнесена по
// нескольким строкам: в 1H2026 подписи колонок и «$ млн» стоят на одном Top,
// а в FY2024 «$ mln (if not mentioned otherwise)» уехало на отдельную строку
// на 0.77pt выше подписей — при группировке по точному Top (штатный для
// расчёта режим) это две независимые строки, и по одной из них шапку не собрать.
//
// Слова всех строк полосы сливаются в одно координатное пространство и
// сортируются по Left: период и метка единиц измерения могут лежать на разных
// строках, а колонки — это интервалы по X, и их порядок задаёт только X.
//
// Идём слева направо, пробуя matchPeriodAt на окнах до трёх слов: длинное окно
// проверяется первым и забирает всё, что разобралось как период, — иначе
// «1 п/г 2026» распалось бы на «1» и «2026» и полугодие превратилось в год.
// Каждое слово, не вошедшее в период, становится отдельной непериодной
// колонкой: без них колонки справа сдвинулись бы влево, а covers начал бы
// относить значения соседям. Границы колонок считаются после разбора, потому
// что промежуток до правого соседа известен только тогда.
func columnsFromHeader(headerLines []Line) []Column {
	words := bandWords(headerLines)
	if len(words) == 0 {
		return nil
	}

	texts := make([]string, len(words))
	for i, word := range words {
		texts[i] = word.Text
	}

	columns := make([]Column, 0, len(words))

	for i := 0; i < len(words); {
		info, width, err := matchPeriodAt(texts, i, maxPeriodTokens)
		if err != nil {
			// Слово не начинает период: оно определяет границу соседних колонок.
			column := Column{Left: words[i].Left, Right: words[i].Left + words[i].Width}
			columns = append(columns, column)

			i++

			continue
		}

		last := words[i+width-1]
		column := Column{
			Period: info.Period,
			Type:   info.Type,
			Left:   words[i].Left,
			Right:  last.Left + last.Width,
		}
		columns = append(columns, column)

		i += width
	}

	return widenColumns(columns)
}

// bandWords сливает строки полосы шапки в один список слов, упорядоченный по
// Left. Слова копируются, а не переиспользуются: сортировка по Left переставила
// бы слова внутри строк, которые вызывающий код ещё использует (Task 5 ищет по
// ним строку метрики). Пустая, но не nil полоса возвращает пустой срез — у
// пустого заголовка нет колонок.
func bandWords(headerLines []Line) []Word {
	words := []Word{}
	for _, line := range headerLines {
		words = append(words, line.Words...)
	}

	sort.SliceStable(words, func(i, j int) bool {
		return words[i].Left < words[j].Left
	})

	return words
}

// widenColumns раздвигает границы колонок до середины промежутков между
// соседями: подпись колонки и значение под ней не совпадают по X (в 1H2026
// метка «2026» на x=282.05, а значение — на x=290.33, потому что числа
// выровнены по центру, а подписи по левому краю). Без расширения значение
// выпало бы из своей колонки.
//
// Крайние колонки расширяются наружу до бесконечности: вне шапки делить
// промежуток не с кем, а значение может уехать левее первой метки — так
// подпись строки метрики («Выручка») остаётся в первой колонке, а не считается
// значением периода.
//
// Границы считаются по исходным Left/Right всех колонок сразу: правка на месте
// сдвинула бы уже расширенный Right соседа и увела бы середину промежутка.
func widenColumns(columns []Column) []Column {
	// Ширина промежутка берётся из нерасширенных координат, поэтому правая
	// граница колонки запоминается до правки.
	rights := make([]float64, len(columns))
	for i, column := range columns {
		rights[i] = column.Right
	}

	for i := range columns {
		switch {
		case len(columns) == 1:
			columns[i].Left = -infiniteX
			columns[i].Right = infiniteX
		case i == 0:
			columns[i].Left = -infiniteX
			columns[i].Right = midpoint(rights[i], columns[i+1].Left)
		case i == len(columns)-1:
			columns[i].Left = midpoint(rights[i-1], columns[i].Left)
			columns[i].Right = infiniteX
		default:
			columns[i].Left = midpoint(rights[i-1], columns[i].Left)
			columns[i].Right = midpoint(rights[i], columns[i+1].Left)
		}
	}

	return columns
}

// midpoint возвращает середину промежутка между правым краем левой колонки и
// левым краем правой.
func midpoint(leftRight, rightLeft float64) float64 {
	return (leftRight + rightLeft) / 2
}

// lineText склеивает слова строки в текст через пробел — в том порядке, в каком
// они стоят на странице (слова Line уже упорядочены по Left).
func lineText(l Line) string {
	parts := make([]string, 0, len(l.Words))
	for _, w := range l.Words {
		parts = append(parts, w.Text)
	}

	return strings.Join(parts, " ")
}

// gluedLineText склеивает слова строки без разделителей. Нужен там, где
// словосочетание состоит из отдельных слов, а признак ищется по непрерывной
// строке: «$ млн» в TSV — это два слова, и только склейка даёт «$млн».
func gluedLineText(l Line) string {
	var glued strings.Builder
	for _, w := range l.Words {
		glued.WriteString(w.Text)
	}

	return glued.String()
}

// unitsMarkers — метки единиц измерения таблицы в проверенных релизах: русская
// («$ млн») и две английские — сокращённая («$ mln») и полная («$ million»).
// В TSV метка разбита на отдельные слова («$» + «млн»), поэтому ищется по
// склейке слов без разделителей.
var unitsMarkers = []string{"$млн", "$mln", "$million"}

// unitsMarker сообщает, что строка несёт метку единиц измерения таблицы.
//
// Именно она отличает финансовую таблицу от производственной: у производственной
// таблицы русского релиза 1H2026 (шапка на 206.51) метки единиц нет, и её колонки
// не должны обслуживать финансовые строки.
func unitsMarker(l Line) bool {
	glued := gluedLineText(l)

	for _, marker := range unitsMarkers {
		if strings.Contains(glued, marker) {
			return true
		}
	}

	return false
}

// isPeriodHeaderLine сообщает, что строка — строка шапки таблицы: она несёт не
// меньше двух непересекающихся меток колонок-периодов либо подписи колонок
// изменений.
//
// Порог в две метки отсекает строки данных: «Объем горной массы, тыс. м³
// 118 967 108 064 10%» походит на набор периодов («108 064»), но непересекающихся
// меток в ней меньше двух, и голый год заголовочной даты («1 2026» на 107.80
// русской страницы) тоже даёт одну метку. Шапка же всегда несёт минимум два
// периода — сравнивать больше нечего, если колонка одна.
//
// Метки периодов ищутся тем же разбором, что и в columnsFromHeader, и окно
// сдвигается на его ширину: иначе «1 п/г 2026» засчиталось бы трижды — как «1»,
// «1 п/г» и «1 п/г 2026».
func isPeriodHeaderLine(l Line) bool {
	text := lineText(l)
	for _, label := range changeLabels {
		if strings.Contains(text, label) {
			return true
		}
	}

	for _, marker := range unitsMarkers {
		if strings.Contains(gluedLineText(l), marker) {
			return true
		}
	}

	return countPeriodLabels(l) >= 2
}

// countPeriodLabels считает непересекающиеся метки периодов в строке — тем же
// разбором и с тем же сдвигом окна, что и columnsFromHeader.
func countPeriodLabels(l Line) int {
	texts := make([]string, 0, len(l.Words))
	for _, w := range l.Words {
		texts = append(texts, w.Text)
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

	return found
}

// changeLabels — подписи колонок изменений в проверенных отчётах. Колонка
// изменения стоит в шапке между периодами, и документ объявляет её сам; см.
// hasChangeLabel.
var changeLabels = []string{"Изм. за", "Y-o-Y", "H-o-H", "y-o-y", "change"}

// changeLabelWords — слова, которыми набраны подписи колонок изменений.
// Отдельный список нужен разбору окон: подпись может состоять из нескольких
// слов («Изм. за год»), но окно периода не имеет права начинаться с любого из
// них.
var changeLabelWords = []string{"Изм.", "change", "y-o-y", "h-o-h"}

// isChangeLabelWord сообщает, что слово — часть подписи колонки изменения.
func isChangeLabelWord(text string) bool {
	for _, word := range changeLabelWords {
		if strings.EqualFold(text, word) {
			return true
		}
	}

	return false
}

// windowHasChangeLabel сообщает, что окно слов содержит подпись колонки
// изменения. Такое окно периодом быть не может: parsePeriod — матчер, а не
// валидатор, и в окне «change 2H 2014» он находит «2H 2014», забирая слово
// «change» себе. Для MD&A за 2014 год это стоило колонки изменения: её числа
// («3%» на x=416.11) попадали в колонку 2014H2 и выдавали себя за её значение.
func windowHasChangeLabel(window []string) bool {
	for _, text := range window {
		if isChangeLabelWord(text) {
			return true
		}
	}

	return false
}

// hasChangeLabel сообщает, что строка несёт подпись колонки изменения.
//
// Подпись изменения может стоять НЕ в той строке, что подписи периодов, и
// заметно ниже порога headerContinuationGap: в MD&A за 2014 год периоды стоят
// на 125.49, а слово «change» — на 132.45, то есть на 6.96pt ниже. Строка с
// подписью изменения обязана попасть в полосу шапки независимо от разрыва:
// документ объявляет ею непериодную колонку, и без неё числа этой колонки
// («3%» на x=416.11) накрываются колонкой периода «2H 2014» и подменяют
// значения периодов — ровно тот сдвиг, ради которого колонки и введены.
func hasChangeLabel(l Line) bool {
	text := lineText(l)
	for _, label := range changeLabels {
		if strings.Contains(text, label) {
			return true
		}
	}

	return false
}

// firstHeaderLine возвращает индекс первой строки-шапки финансовой таблицы или
// -1, если шапки на странице нет.
func firstHeaderLine(lines []Line) int {
	for i, l := range lines {
		if isTableHeader(l) {
			return i
		}
	}

	return -1
}

// isTableHeader сообщает, что строка открывает шапку финансовой таблицы: она
// несёт метку единиц измерения и размечает не меньше двух колонок-периодов.
//
// Одной метки единиц мало (её несёт и строка данных, случайно склеившая «$» и
// «млн»), одного набора периодов тоже мало (его даёт производственная таблица
// без метки единиц) — нужны оба признака. Два признака сразу дают ровно те
// строки, чьи колонки и обслуживают финансовые строки страницы.
func isTableHeader(l Line) bool {
	return unitsMarker(l) && isPeriodHeaderLine(l)
}

// headerBand возвращает полосу шапки, начинающуюся строкой lines[start]:
// саму строку и следующие за ней строки той же напечатанной строки PDF.
//
// Полоса, а не одна строка, потому что шапка реальных релизов разнесена по
// нескольким строкам: в 1H2026 подписи колонок и «$ млн» стоят на одном Top,
// а в FY2024 «$ million (if not mentioned otherwise)» — на двух (60.83 и
// 61.60), потому что расшифровка набрана мельче. При группировке по точному
// Top это две независимые строки, и по одной из них шапку не собрать.
//
// Полоса тянется вниз от якоря и останавливается на первой строке, которая
// дальше headerContinuationGap и не несёт подписи колонки изменения
// (hasChangeLabel). Вверх полоса не тянется: строки выше якоря — это текст
// страницы, а не шапка.
func headerBand(lines []Line, start int) []Line {
	band := []Line{lines[start]}

	for i := start + 1; i < len(lines); i++ {
		if lines[i].Top-lines[start].Top > headerContinuationGap && !hasChangeLabel(lines[i]) {
			break
		}

		band = append(band, lines[i])
	}

	return band
}
