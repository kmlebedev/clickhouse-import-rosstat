package polyus

import "sort"

// infiniteX — внешняя граница крайних колонок: она не ограничена ничем, потому
// что за пределами шапки нет соседа, с которым можно поделить промежуток.
// Бесконечность заменена большим, но конечным числом: арифметика с Inf
// превратила бы середину промежутка в NaN, и covers перестал бы работать.
const infiniteX = 1e18

// maxPeriodTokens — самая длинная форма периода в шапке: «1 п/г 2026» —
// ровно те же три слова, что разбирает matchPeriodAt.
const maxPeriodTokens = 3

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
