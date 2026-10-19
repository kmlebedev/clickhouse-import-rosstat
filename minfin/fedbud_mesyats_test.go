package minfin

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

type fedbudMesyatsRow struct {
	name  string
	date  time.Time
	value float64
}

// fedbudMesyatsRecorder — заглушка driver.Batch для офлайн-разбора XLSX.
type fedbudMesyatsRecorder struct {
	driver.Batch
	rows []fedbudMesyatsRow
	err  error
}

func (r *fedbudMesyatsRecorder) Append(v ...any) error {
	if len(v) != 3 {
		r.err = fmt.Errorf("строка из %d значений, ожидалось 3 (name, date, value)", len(v))
		return r.err
	}
	name, ok := v[0].(string)
	if !ok {
		r.err = fmt.Errorf("name пришёл как %T, ожидалась строка", v[0])
		return r.err
	}
	date, ok := v[1].(time.Time)
	if !ok {
		r.err = fmt.Errorf("date пришёл как %T, ожидался time.Time", v[1])
		return r.err
	}
	value, ok := v[2].(float64)
	if !ok {
		r.err = fmt.Errorf("value пришёл как %T, ожидался float64", v[2])
		return r.err
	}
	r.rows = append(r.rows, fedbudMesyatsRow{name: name, date: date, value: value})
	return nil
}

func fedbudMesyatsDay(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func fedbudMesyatsFind(t *testing.T, rows []fedbudMesyatsRow, name string, date time.Time) fedbudMesyatsRow {
	t.Helper()
	for _, row := range rows {
		if row.name == name && row.date.Equal(date) {
			return row
		}
	}
	t.Fatalf("нет точки %s @ %s", name, date.Format("2006-01-02"))
	return fedbudMesyatsRow{}
}

func TestFedbudMesyatsLinkFixture(t *testing.T) {
	page, err := os.ReadFile("testdata/conbud_execute.html")
	if err != nil {
		t.Fatalf("чтение HTML-фикстуры: %v", err)
	}
	href, err := util.FindLink(page, fedbudMesyatsLinkSelector)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
	}
	if !strings.Contains(href, "_mesyats.xlsx") || !strings.Contains(href, "2026/10") {
		t.Fatalf("FindLink вернул %q, ожидалась ссылка на выпуск 2026/10", href)
	}
	if _, err = util.FindLink([]byte("<html><body></body></html>"), fedbudMesyatsLinkSelector); err == nil {
		t.Fatal("ожидалась ошибка: ссылка не найдена")
	}
}

func TestFedbudMesyatsImport(t *testing.T) {
	xlsx, err := excelize.OpenFile("testdata/Prilozhenie_8_dannye_115-117_mesyats_202610.xlsx")
	if err != nil {
		t.Fatalf("открытие XLSX-фикстуры: %v", err)
	}
	defer func() { _ = xlsx.Close() }()

	rec := &fedbudMesyatsRecorder{}
	if err = fedbudMesyatsImport(xlsx, rec); err != nil {
		t.Fatalf("fedbudMesyatsImport: %v", err)
	}
	if rec.err != nil {
		t.Fatalf("append: %v", rec.err)
	}
	if len(rec.rows) != 884 {
		t.Fatalf("точек %d, ожидалось 884 (188×4 + 132)", len(rec.rows))
	}

	wantCounts := map[string]int{
		"Доходы, всего":            188,
		"Расходы, всего":           188,
		"Акцизы":                   188,
		"Национальная оборона":     132,
		"Дефицит (-)/Профицит (+)": 188,
	}
	for name, want := range wantCounts {
		got := 0
		for _, row := range rec.rows {
			if row.name == name {
				got++
			}
		}
		if got != want {
			t.Errorf("ряд %q: точек %d, ожидалось %d", name, got, want)
		}
	}
	for _, row := range rec.rows {
		if strings.Contains(row.name, "*") {
			t.Errorf("в имени ряда осталась сноска: %q", row.name)
		}
	}

	cases := []struct {
		name  string
		date  time.Time
		value float64
	}{
		{"Доходы, всего", fedbudMesyatsDay(2011, time.January, 1), 1115.85497612064},
		{"Доходы, всего", fedbudMesyatsDay(2026, time.August, 1), 6945.837101036304},
		{"Расходы, всего", fedbudMesyatsDay(2026, time.August, 1), 6472.204643774392},
		{"Акцизы", fedbudMesyatsDay(2026, time.August, 1), 286.6877654519035},
		{"Дефицит (-)/Профицит (+)", fedbudMesyatsDay(2026, time.August, 1), 473.63245726191235},
		{"Национальная оборона", fedbudMesyatsDay(2021, time.December, 1), 921.5970035821897},
	}
	for _, c := range cases {
		row := fedbudMesyatsFind(t, rec.rows, c.name, c.date)
		if math.Abs(row.value-c.value) > 1e-6 {
			t.Errorf("%s @ %s = %.12f, ожидалось %.12f", c.name, c.date.Format("2006-01-02"), row.value, c.value)
		}
	}
	// Максимальные даты: четыре ряда свежие до 2026-08, оборона — до 2021-12.
	maxDates := make(map[string]time.Time)
	for _, row := range rec.rows {
		if maxDates[row.name].Before(row.date) {
			maxDates[row.name] = row.date
		}
	}
	for _, name := range []string{"Доходы, всего", "Расходы, всего", "Акцизы", "Дефицит (-)/Профицит (+)"} {
		if !maxDates[name].Equal(fedbudMesyatsDay(2026, time.August, 1)) {
			t.Errorf("%s: max date %s, ожидалось 2026-08-01", name, maxDates[name].Format("2006-01-02"))
		}
	}
	if !maxDates["Национальная оборона"].Equal(fedbudMesyatsDay(2021, time.December, 1)) {
		t.Errorf("Национальная оборона: max date %s, ожидалось 2021-12-01", maxDates["Национальная оборона"].Format("2006-01-02"))
	}
}

func TestFedbudMesyatsImportMissingSheet(t *testing.T) {
	xlsx := excelize.NewFile()
	defer func() { _ = xlsx.Close() }()
	if err := fedbudMesyatsImport(xlsx, &fedbudMesyatsRecorder{}); err == nil {
		t.Fatal("ожидалась ошибка: лист «месяц» отсутствует")
	}
}
