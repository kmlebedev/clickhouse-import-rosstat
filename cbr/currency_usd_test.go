package cbr

import (
	"math"
	"os"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// fixtureValCurs читает фикстуру XML_dynamic.asp сырыми байтами и разбирает её тем же
// ParseXML, что и CrawFunc: фикстура и живая лента идут одним кодом разбора.
func fixtureValCurs(t *testing.T) *cbrValCurs {
	t.Helper()
	body, err := os.ReadFile("testdata/currency_usd.xml")
	if err != nil {
		t.Fatal(err)
	}
	doc := new(cbrValCurs)
	if err = util.ParseXML(body, doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// parseCurrencyUSDBody разбирает XML из литерала теста (битые записи в фикстуру не кладём).
func parseCurrencyUSDBody(t *testing.T, body string) ([]cbrPoint, error) {
	t.Helper()
	doc := new(cbrValCurs)
	if err := util.ParseXML([]byte(body), doc); err != nil {
		t.Fatal(err)
	}
	return parseCurrencyUSD(doc)
}

func TestParseCurrencyUSD(t *testing.T) {
	points, err := parseCurrencyUSD(fixtureValCurs(t))
	if err != nil {
		t.Fatalf("parseCurrencyUSD: %v", err)
	}
	if len(points) != 4 {
		t.Fatalf("точек %d, want 4 (08–13.10.2026)", len(points))
	}
	want := time.Date(2026, time.October, 8, 0, 0, 0, 0, time.UTC)
	if !points[0].date.Equal(want) {
		t.Errorf("первая дата %s, want %s", points[0].date.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	// Сравниваем разобранное значение как float64 с точностью 1e-6: в БД столбец Float32,
	// и точнее хранимого не проверяем.
	if math.Abs(points[0].value-85.4811) > 1e-6 {
		t.Errorf("курс на %s = %v, want 85.4811", points[0].date.Format("2006-01-02"), points[0].value)
	}
}

// TestParseCurrencyUSDEmptyDocFails: пустой <ValCurs/> — ошибка (fail-fast), а не пустой ряд.
func TestParseCurrencyUSDEmptyDocFails(t *testing.T) {
	if _, err := parseCurrencyUSD(new(cbrValCurs)); err == nil {
		t.Error("пустой <ValCurs/> должен давать ошибку, а не 0 точек")
	}
}

// TestParseCurrencyUSDSkipsBlankRecords: пустые (после TrimSpace) дата или значение — пропуск.
func TestParseCurrencyUSDSkipsBlankRecords(t *testing.T) {
	points, err := parseCurrencyUSDBody(t, `<ValCurs>`+
		`<Record Date="08.10.2026"><Value>85,4811</Value></Record>`+
		`<Record Date=""><Value>85,5</Value></Record>`+
		`<Record Date="09.10.2026"><Value> </Value></Record>`+
		`</ValCurs>`)
	if err != nil {
		t.Fatalf("parseCurrencyUSD: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("точек %d, want 1: пустые записи должны пропускаться", len(points))
	}
}

// TestParseCurrencyUSDBrokenRecordFails: непустое непарсящееся значение или дата — ошибка,
// а не тихий пропуск (битая лента не должна выглядеть успешным прогоном).
func TestParseCurrencyUSDBrokenRecordFails(t *testing.T) {
	for name, body := range map[string]string{
		"value": `<ValCurs><Record Date="08.10.2026"><Value>85,48,11</Value></Record></ValCurs>`,
		"date":  `<ValCurs><Record Date="08-10-2026"><Value>85,4811</Value></Record></ValCurs>`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseCurrencyUSDBody(t, body); err == nil {
				t.Errorf("битая запись (%s) должна давать ошибку", name)
			}
		})
	}
}

// TestCurrencyUSDSeriesMetaCoversParsedRows: имя ряда однорядной таблицы — имя таблицы.
func TestCurrencyUSDSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrCurrencyUSDSeriesMeta, func(rec *appendRecorder) error {
		rec.series = cbrCurrencyUSDTable
		points, err := parseCurrencyUSD(fixtureValCurs(t))
		if err != nil {
			return err
		}
		return appendCbrPoints(rec, points)
	})
}
