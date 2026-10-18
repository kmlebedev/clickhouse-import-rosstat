package util

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

// TestParseHTMLTable — проверка на реальной странице ЦБ: у таблицы КБД две строки
// заголовка, вторая — 12 теноров; 8 строк данных. Значения с запятой — как на
// странице (импортёр сам решает, чем заменить десятичную запятую).
func TestParseHTMLTable(t *testing.T) {
	body, err := os.ReadFile("testdata/unidbquery.html")
	if err != nil {
		t.Fatalf("чтение фикстуры: %v", err)
	}

	table, err := ParseHTMLTable(body, "table.data.spaced")
	if err != nil {
		t.Fatalf("ParseHTMLTable: %v", err)
	}

	if len(table.Headers) != 2 {
		t.Fatalf("len(Headers) = %d, ожидалось 2 строки заголовка", len(table.Headers))
	}
	if len(table.Headers[1]) != 12 {
		t.Fatalf("len(Headers[1]) = %d, ожидалось 12 теноров", len(table.Headers[1]))
	}
	if got := table.Headers[1][0]; got != "0,25" {
		t.Errorf("Headers[1][0] = %q, ожидалось %q", got, "0,25")
	}
	if got := table.Headers[1][11]; got != "30" {
		t.Errorf("Headers[1][11] = %q, ожидалось %q", got, "30")
	}

	if len(table.Rows) != 8 {
		t.Fatalf("len(Rows) = %d, ожидалось 8 строк данных", len(table.Rows))
	}
	if got := table.Rows[0][0]; got != "30.12.2003" {
		t.Errorf("Rows[0][0] = %q, ожидалось %q", got, "30.12.2003")
	}
	if got := table.Rows[0][1]; got != "3,95" {
		t.Errorf("Rows[0][1] = %q, ожидалось %q", got, "3,95")
	}
	if got := table.Rows[len(table.Rows)-1][0]; got != "04.01.2003" {
		t.Errorf("Rows[%d][0] = %q, ожидалось %q", len(table.Rows)-1, got, "04.01.2003")
	}
}

// Селектор не совпал — ошибка оборачивает ErrHTMLTableNotFound и называет селектор:
// дежурный по логу должен видеть, чем именно страница не подошла, не открывая код.
func TestParseHTMLTableNotFound(t *testing.T) {
	const selector = "table.data.spaced"

	_, err := ParseHTMLTable([]byte(`<html><body><p>no table</p></body></html>`), selector)
	if err == nil {
		t.Fatal("ожидалась ошибка, а таблица найдена")
	}
	if !errors.Is(err, ErrHTMLTableNotFound) {
		t.Fatalf("errors.Is(err, ErrHTMLTableNotFound) = false, err = %v", err)
	}
	if !strings.Contains(err.Error(), selector) {
		t.Errorf("ошибка %q не называет селектор %q", err, selector)
	}
}

// Синтетическая разметка проверяет ветки разбора, а не раскладку источника:
// строка с <th> уходит в заголовки, строка с <td> — в данные, пустая строка
// пропускается, пустая ячейка остаётся "".
func TestParseHTMLTableSkipsNonDataRows(t *testing.T) {
	body := []byte(`<html><body><table id="d">
		<tr><th> Дата </th><th> Значение </th></tr>
		<tr></tr>
		<tr><td> 01.02.2003 </td><td></td></tr>
	</table></body></html>`)

	table, err := ParseHTMLTable(body, "#d")
	if err != nil {
		t.Fatalf("ParseHTMLTable: %v", err)
	}

	wantHeaders := [][]string{{"Дата", "Значение"}}
	if !reflect.DeepEqual(table.Headers, wantHeaders) {
		t.Errorf("Headers = %#v, ожидалось %#v", table.Headers, wantHeaders)
	}

	wantRows := [][]string{{"01.02.2003", ""}}
	if !reflect.DeepEqual(table.Rows, wantRows) {
		t.Errorf("Rows = %#v, ожидалось %#v", table.Rows, wantRows)
	}
}
