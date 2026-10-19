package fao

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

type faoRow struct {
	series string
	date   time.Time
	value  float64
}

// faoRecorder — заглушка driver.Batch для офлайн-разбора: запоминает строки.
// Форма строки у парсера одна: (series string, date time.Time, value float64).
type faoRecorder struct {
	driver.Batch
	rows []faoRow
	err  error
}

func (r *faoRecorder) Append(v ...any) error {
	if len(v) != 3 {
		r.err = fmt.Errorf("строка из %d значений, ожидалось 3 (series, date, value)", len(v))
		return r.err
	}
	series, ok := v[0].(string)
	if !ok {
		r.err = fmt.Errorf("series пришёл как %T, ожидалась строка", v[0])
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
	r.rows = append(r.rows, faoRow{series: series, date: date, value: value})
	return nil
}

func faoDay(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func faoFind(rows []faoRow, series string, date time.Time) (faoRow, bool) {
	for _, row := range rows {
		if row.series == series && row.date.Equal(date) {
			return row, true
		}
	}
	return faoRow{}, false
}

func TestParseFaoFoodPrice(t *testing.T) {
	body, err := os.ReadFile("testdata/food_price_indices_data.csv")
	if err != nil {
		t.Fatalf("чтение фикстуры: %v", err)
	}
	rec := &faoRecorder{}
	if err = parseFaoFoodPrice(body, rec); err != nil {
		t.Fatalf("parseFaoFoodPrice: %v", err)
	}
	if rec.err != nil {
		t.Fatalf("append: %v", rec.err)
	}
	// 10 месяцев в фикстуре × 6 рядов.
	if len(rec.rows) != 60 {
		t.Fatalf("точек %d, ожидалось 60 (6 рядов × 10 месяцев)", len(rec.rows))
	}
	for _, series := range faoSeriesByHeader {
		n := 0
		for _, row := range rec.rows {
			if row.series == series.Series {
				n++
			}
		}
		if n != 10 {
			t.Errorf("ряд %q: точек %d, ожидалось 10", series.Series, n)
		}
	}

	cases := []struct {
		series string
		date   time.Time
		want   float64
	}{
		{"Food Price Index", faoDay(1990, time.February, 1), 64.4},
		{"Sugar", faoDay(1990, time.February, 1), 87.9},
		{"Food Price Index", faoDay(2026, time.October, 1), 136.0},
		{"Sugar", faoDay(2026, time.October, 1), 114.0},
	}
	for _, c := range cases {
		row, ok := faoFind(rec.rows, c.series, c.date)
		if !ok {
			t.Errorf("нет точки %s @ %s", c.series, c.date.Format("2006-01-02"))
			continue
		}
		if row.value != c.want {
			t.Errorf("%s @ %s = %v, ожидалось %v", c.series, c.date.Format("2006-01-02"), row.value, c.want)
		}
	}
}

func TestParseFaoFoodPriceFailFast(t *testing.T) {
	t.Run("пустое тело", func(t *testing.T) {
		if err := parseFaoFoodPrice(nil, &faoRecorder{}); err == nil {
			t.Fatal("ожидалась ошибка на пустом CSV")
		}
	})
	t.Run("нет шапки Date", func(t *testing.T) {
		if err := parseFaoFoodPrice([]byte("Date,Food Price Index\n2026-09,136.0\n"), &faoRecorder{}); err == nil {
			t.Fatal("ожидалась ошибка: в шапке нет обязательных колонок")
		}
	})
	t.Run("нечисло", func(t *testing.T) {
		body := "Date,Food Price Index,Meat,Dairy,Cereals,Oils,Sugar\n2026-09,oops,1,1,1,1,1\n"
		if err := parseFaoFoodPrice([]byte(body), &faoRecorder{}); err == nil {
			t.Fatal("ожидалась ошибка на нечисловом значении")
		}
	})
	t.Run("нет точек", func(t *testing.T) {
		body := "Date,Food Price Index,Meat,Dairy,Cereals,Oils,Sugar\n"
		if err := parseFaoFoodPrice([]byte(body), &faoRecorder{}); err == nil {
			t.Fatal("ожидалась ошибка: разбор не дал точек")
		}
	})
}

func TestFaoLinkFixture(t *testing.T) {
	page, err := os.ReadFile("testdata/foodpricesindex_ru.html")
	if err != nil {
		t.Fatalf("чтение HTML-фикстуры: %v", err)
	}
	href, err := util.FindLink(page, faoLinkSelector)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
	}
	if href == "" || !contains(href, "food_price_indices_data.csv") {
		t.Fatalf("FindLink вернул %q, ожидалась ссылка на food_price_indices_data.csv", href)
	}
	if _, err = util.FindLink([]byte("<html><body></body></html>"), faoLinkSelector); err == nil {
		t.Fatal("ожидалась ошибка: ссылка не найдена")
	}
}

func TestFaoAbsoluteURL(t *testing.T) {
	abs := "https://www.fao.org/media/docs/worldfoodsituationlibraries/wfs-library/food_price_indices_data.csv?sfvrsn=1"
	if got := faoAbsoluteURL(abs); got != abs {
		t.Errorf("абсолютный href изменён: %q", got)
	}
	root := "/media/docs/worldfoodsituationlibraries/wfs-library/food_price_indices_data.csv"
	want := "https://www.fao.org" + root
	if got := faoAbsoluteURL(root); got != want {
		t.Errorf("root-relative href = %q, ожидалось %q", got, want)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
