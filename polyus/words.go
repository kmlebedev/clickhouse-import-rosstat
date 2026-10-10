package polyus

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// tsvFields — число колонок в снимке pdftotext -tsv:
// level, page_num, par_num, block_num, line_num, word_num, left, top,
// width, height, conf, text.
const tsvFields = 12

// wordLevel — значение колонки level для строки-слова. Остальные уровни
// (1 страница, 3 flow, 4 line) служебные: в их text лежат маркеры ###PAGE###,
// ###FLOW###, ###LINE### с conf = -1, а координаты задают рамку всего
// документа или абзаца, а не положение слова.
const wordLevel = 5

// wordScanBuf — начальный размер буфера сканера. Снимки страниц доходят до
// ~1200 строк; буфер задан с запасом и расширяется сканером дальше.
const wordScanBuf = 64 * 1024

// tsvHeaderField — значение первой колонки (level) в строке заголовка TSV.
// Повторяется в середине склеенного многостраничного файла: extractPDF
// извлекает страницы по отдельности, и joinFiles склеивает их вместе с
// заголовками.
const tsvHeaderField = "level"

// isTSVHeaderRow сообщает, что строка — строка заголовка TSV-снимка.
func isTSVHeaderRow(firstField string) bool {
	return firstField == tsvHeaderField
}

// Word — слово со страницы PDF: текст, номер страницы и рамка в точках, с
// началом координат в левом верхнем углу страницы.
//
// Page — колонка page_num снимка. Координаты страниц pdftotext начинает заново
// на каждой странице, поэтому без номера нельзя отличить строку седьмой страницы
// от строки шестой: Top у них совпадают. Номер нужен и для провенанса витрины
// (source_page), и для того, чтобы строки разных страниц склеенного файла не
// слились в одну при группировке по Top.
//
// Block и Line — структурные номера из TSV (block_num, line_num). Tesseract
// кладёт в них собственное членение на блоки и строки, которое не совпадает
// с визуальными строками таблицы, поэтому для сборки строки нужен groupByLine
// по Top, а не группировка по Line.
type Word struct {
	Text   string
	Page   int
	Left   float64
	Top    float64
	Width  float64
	Height float64
	Block  int
	Line   int
}

// Line — визуальная строка: слова с одной страницы и одинаковым (в пределах
// deltaTop) Top, упорядоченные по Left.
//
// Номер страницы в Line не дублируется: он один у всех её слов, и берётся из
// первого слова (pageOfLine). Отдельным полем он был бы третьим источником того
// же факта — рядом с номером, переданным разбору параметром.
type Line struct {
	Top   float64
	Words []Word
}

// parseTSV читает снимок pdftotext -tsv и возвращает слова уровня 5
// (wordLevel). Служебные строки уровней 1/3/4 и строку заголовка отбрасывает.
//
// Строка заголовка отбрасывается по имени первой колонки (tsvHeaderField), а не
// по ошибке разбора номера уровня: extractPDF извлекает страницы по отдельности,
// а joinFiles склеивает их, поэтому в многостраничном файле заголовок повторяется
// в середине файла. Заголовок печатает ровно столько же колонок, сколько формат
// (12), поэтому проверки «колонок меньше, чем полей» для него недостаточно.
//
// Ошибки разбора не проглатываются: номер строки файла добавляется в контекст.
func parseTSV(r io.Reader) ([]Word, error) {
	var words []Word

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, wordScanBuf), wordScanBuf)

	for lineNum := 1; scanner.Scan(); lineNum++ {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < tsvFields {
			// Мусорная строка: колонок меньше, чем полей в формате, разбирать
			// её нечего.
			continue
		}

		if isTSVHeaderRow(fields[0]) {
			continue
		}

		level, err := strconv.Atoi(fields[0])
		if err != nil {
			// Не заголовок и не число — разбирать нечего.
			continue
		}

		if level != wordLevel {
			continue
		}

		page, err := parseTSVInt(fields[1], lineNum, "page_num")
		if err != nil {
			return nil, err
		}

		left, err := parseTSVFloat(fields[6], lineNum, "left")
		if err != nil {
			return nil, err
		}

		top, err := parseTSVFloat(fields[7], lineNum, "top")
		if err != nil {
			return nil, err
		}

		width, err := parseTSVFloat(fields[8], lineNum, "width")
		if err != nil {
			return nil, err
		}

		height, err := parseTSVFloat(fields[9], lineNum, "height")
		if err != nil {
			return nil, err
		}

		block, err := parseTSVInt(fields[3], lineNum, "block_num")
		if err != nil {
			return nil, err
		}

		line, err := parseTSVInt(fields[4], lineNum, "line_num")
		if err != nil {
			return nil, err
		}

		words = append(words, Word{
			Text:   fields[11],
			Page:   page,
			Left:   left,
			Top:    top,
			Width:  width,
			Height: height,
			Block:  block,
			Line:   line,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read tsv: %w", err)
	}

	return words, nil
}

// parseTSVFloat разбирает вещественное поле TSV, добавляя в ошибку номер
// строки файла и имя колонки.
func parseTSVFloat(raw string, lineNum int, column string) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s at line %d: %w", column, lineNum, err)
	}

	return value, nil
}

// parseTSVInt разбирает целочисленное поле TSV, добавляя в ошибку номер строки
// файла и имя колонки.
func parseTSVInt(raw string, lineNum int, column string) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("parse %s at line %d: %w", column, lineNum, err)
	}

	return value, nil
}

// groupByLine собирает визуальные строки: сортирует слова по номеру страницы и
// Top, внутри строки — по Left, и накапливает группу, пока страница и Top слова
// не отличаются от начала группы (страница — точно, Top — не более чем на
// deltaTop).
//
// Номер страницы входит в ключ группы, потому что координаты pdftotext начинает
// заново на каждой странице: у строки шестой страницы и строки седьмой Top
// совпадают, и склеенный файл joinFiles слил бы их в одну строку. На снимке
// одной страницы номер у всех слов один, и группировка идёт ровно так же, как
// если бы его не было.
//
// deltaTop == 0 — штатный режим: у слов одной визуальной строки Top совпадает
// точно, и группировка идёт по точному значению. Ненулевой допуск нужен только
// как защита от дрейфа базовой линии у слов с другим размером шрифта; дрожание
// координат округлением не лечится — округление Top сломало бы группировку.
//
// Возвращаются группы в порядке страниц и возрастания Top, слова внутри — по
// Left.
func groupByLine(words []Word, deltaTop float64) []Line {
	if len(words) == 0 {
		return nil
	}

	sorted := make([]Word, len(words))
	copy(sorted, words)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Page != sorted[j].Page {
			return sorted[i].Page < sorted[j].Page
		}

		if sorted[i].Top != sorted[j].Top {
			return sorted[i].Top < sorted[j].Top
		}

		return sorted[i].Left < sorted[j].Left
	})

	var lines []Line

	start := 0
	for i := 1; i <= len(sorted); i++ {
		if i < len(sorted) &&
			sorted[i].Page == sorted[start].Page &&
			sorted[i].Top-sorted[start].Top <= deltaTop {
			continue
		}

		line := Line{Top: sorted[start].Top, Words: append([]Word(nil), sorted[start:i]...)}
		sort.SliceStable(line.Words, func(a, b int) bool {
			return line.Words[a].Left < line.Words[b].Left
		})

		lines = append(lines, line)
		start = i
	}

	return lines
}
