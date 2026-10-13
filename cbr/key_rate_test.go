package cbr

import (
	"math"
	"os"
	"testing"
	"time"
)

// readCbrTestdata читает фикстуру пакета сырыми байтами: тесты разбирают её тем же кодом,
// что и живой источник.
func readCbrTestdata(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// TestKeyRateRows: таблица «Ключевая ставка» разбирается целиком — ≥170 строк истории
// с 17.09.2013; строки идут от свежей к старой, поэтому первая точка — сегодняшняя,
// последняя — дата введения ключевой ставки.
func TestKeyRateRows(t *testing.T) {
	points, err := keyRateRows(readCbrTestdata(t, "key_rate.html"))
	if err != nil {
		t.Fatalf("keyRateRows: %v", err)
	}
	if len(points) < 170 {
		t.Fatalf("точек %d, want ≥ 170", len(points))
	}
	first := time.Date(2026, time.October, 13, 0, 0, 0, 0, time.UTC)
	if !points[0].date.Equal(first) {
		t.Errorf("первая дата %s, want %s", points[0].date.Format("2006-01-02"), first.Format("2006-01-02"))
	}
	if math.Abs(points[0].value-14.0) > 1e-6 {
		t.Errorf("ставка на %s = %v, want 14.0", points[0].date.Format("2006-01-02"), points[0].value)
	}
	last := time.Date(2013, time.September, 17, 0, 0, 0, 0, time.UTC)
	if !points[len(points)-1].date.Equal(last) {
		t.Errorf("последняя дата %s, want %s", points[len(points)-1].date.Format("2006-01-02"), last.Format("2006-01-02"))
	}
	if math.Abs(points[len(points)-1].value-5.5) > 1e-6 {
		t.Errorf("ставка на %s = %v, want 5.5", points[len(points)-1].date.Format("2006-01-02"), points[len(points)-1].value)
	}
}

// TestKeyRateRowsWithoutTableFails: страница без таблицы .data — ошибка (fail-fast),
// а не пустой ряд: смена структуры страницы не должна выглядеть успешным прогоном.
func TestKeyRateRowsWithoutTableFails(t *testing.T) {
	for name, page := range map[string]string{
		"пустая страница": `<html><body></body></html>`,
		"чужая таблица":   `<html><body><table class="data"><tr><td>13.10.2026</td><td>14,00</td></tr></table></body></html>`,
		"только заголовок": `<div class="table-wrapper"><div class="table"><table class="data">` +
			`<tr><th>Дата</th><th>Ставка</th></tr></table></table></div></div>`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := keyRateRows([]byte(page)); err == nil {
				t.Error("ожидалась ошибка, а разбор вернул пустой ряд без ошибки")
			}
		})
	}
}

// TestKeyRateRowsSkipsBlankAndFailsOnBroken: пустые дата или значение — пропуск,
// непустое непарсящееся — ошибка (как в XML-разборах T4).
func TestKeyRateRowsSkipsBlankAndFailsOnBroken(t *testing.T) {
	const head = `<div class="table-wrapper"><div class="table"><table class="data"><tr><th>Дата</th><th>Ставка</th></tr>`
	const tail = `</table></div></div>`
	points, err := keyRateRows([]byte(head +
		`<tr><td>13.10.2026</td><td>14,00</td></tr>` +
		`<tr><td></td><td>9,50</td></tr>` +
		`<tr><td>12.10.2026</td><td> </td></tr>` +
		`<tr><td>09.10.2026</td><td>16,20</td></tr>` + tail))
	if err != nil {
		t.Fatalf("keyRateRows: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("точек %d, want 2: строки с пустой датой или значением пропускаются", len(points))
	}
	for name, row := range map[string]string{
		"битая дата":     `<tr><td>13-10-2026</td><td>14,00</td></tr>`,
		"битое значение": `<tr><td>13.10.2026</td><td>14,00,5</td></tr>`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := keyRateRows([]byte(head + row + tail)); err == nil {
				t.Errorf("битая строка (%s) должна давать ошибку", name)
			}
		})
	}
}

// TestKeyRateSeriesMetaCoversParsedRows: имя ряда однорядной таблицы — имя таблицы.
func TestKeyRateSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrKeyRateSeriesMeta, func(rec *appendRecorder) error {
		rec.series = cbrKeyRateTable
		points, err := keyRateRows(readCbrTestdata(t, "key_rate.html"))
		if err != nil {
			return err
		}
		return appendCbrPoints(rec, points)
	})
}
