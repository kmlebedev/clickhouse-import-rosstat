package util

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// HTMLTable — HTML-таблица ЦБ без раскрытия colspan/rowspan: Headers хранит
// строки <th> как есть, Rows — строки <td>. Ячейки обрезаны TrimSpace; пустая
// ячейка остаётся "" — так таблицы ЦБ обозначают отсутствие значения, и решение,
// пропускать её или писать NULL, остаётся за импортёром.
type HTMLTable struct {
	Headers [][]string // строки <th> (ячейки TrimSpace)
	Rows    [][]string // строки <td> (ячейки TrimSpace, пустые ячейки — "")
}

// ErrHTMLTableNotFound — таблица по селектору не найдена. Вызывающий отличает
// «страница не та» от ошибки разбора, когда проверяет, что ЦБ отдал нужную форму
// (например, пустой результат UniDbQuery вместо WAF-заглушки).
var ErrHTMLTableNotFound = errors.New("HTML table not found")

// ParseHTMLTable разбирает HTML-страницу ЦБ и возвращает первую таблицу по
// селектору. Сеть не используется: страница передаётся целиком. Строка считается
// данными, если в ней есть хотя бы один <td>; иначе строка с <th> — заголовок.
// colspan/rowspan не раскрываются: у таблицы КБД две строки заголовка, и вторая
// содержит 12 ячеек теноров — раскрытие схлопнуло бы их в одну строку и потеряло
// привязку значений к срокам.
func ParseHTMLTable(body []byte, selector string) (HTMLTable, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	// Ветка защитная: на практике goquery терпим к битой разметке и ошибки разбора
	// не отдаёт, но без проверки nil-документ паниковал бы на doc.Find ниже.
	if err != nil {
		return HTMLTable{}, fmt.Errorf("ParseHTMLTable: разбор страницы: %w", err)
	}

	table := doc.Find(selector).First()
	if table.Length() == 0 {
		return HTMLTable{}, fmt.Errorf("%w: селектор %q", ErrHTMLTableNotFound, selector)
	}

	var result HTMLTable
	table.Find("tr").Each(func(_ int, row *goquery.Selection) {
		if cells := row.Find("td"); cells.Length() > 0 {
			result.Rows = append(result.Rows, trimmedTexts(cells))
			return
		}
		if cells := row.Find("th"); cells.Length() > 0 {
			result.Headers = append(result.Headers, trimmedTexts(cells))
		}
	})
	return result, nil
}

// trimmedTexts собирает тексты ячеек с TrimSpace; пустая ячейка остаётся "".
func trimmedTexts(cells *goquery.Selection) []string {
	texts := make([]string, 0, cells.Length())
	cells.Each(func(_ int, cell *goquery.Selection) {
		texts = append(texts, strings.TrimSpace(cell.Text()))
	})
	return texts
}
