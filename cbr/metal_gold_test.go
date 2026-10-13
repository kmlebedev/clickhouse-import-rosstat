package cbr

import (
	"math"
	"os"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// fixtureMetallDoc читает фикстуру xml_metall.asp сырыми байтами и разбирает её тем же
// ParseXML, что и CrawFunc: фикстура и живая лента идут одним кодом разбора.
func fixtureMetallDoc(t *testing.T) *cbrMetallDoc {
	t.Helper()
	body, err := os.ReadFile("testdata/metal_gold.xml")
	if err != nil {
		t.Fatal(err)
	}
	doc := new(cbrMetallDoc)
	if err = util.ParseXML(body, doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// parseMetalGoldBody разбирает XML из литерала теста (битые записи в фикстуру не кладём).
func parseMetalGoldBody(t *testing.T, body string) ([]cbrPoint, error) {
	t.Helper()
	doc := new(cbrMetallDoc)
	if err := util.ParseXML([]byte(body), doc); err != nil {
		t.Fatal(err)
	}
	return parseMetalGold(doc)
}

func TestParseMetalGold(t *testing.T) {
	points, err := parseMetalGold(fixtureMetallDoc(t))
	if err != nil {
		t.Fatalf("parseMetalGold: %v", err)
	}
	// В фикстуре 4 даты по 4 металла: остаётся только золото (Code=1).
	if len(points) != 4 {
		t.Fatalf("точек %d, want 4 (по одной на 08–13.10.2026)", len(points))
	}
	want := time.Date(2026, time.October, 8, 0, 0, 0, 0, time.UTC)
	if !points[0].date.Equal(want) {
		t.Errorf("первая дата %s, want %s", points[0].date.Format("2006-01-02"), want.Format("2006-01-02"))
	}
	// Сравниваем разобранное значение как float64 с точностью 1e-6: в БД столбец Float32,
	// и точнее хранимого не проверяем.
	if math.Abs(points[0].value-11422.95) > 1e-6 {
		t.Errorf("цена золота на %s = %v, want 11422.95", points[0].date.Format("2006-01-02"), points[0].value)
	}
	for _, p := range points {
		if p.value < 1000 {
			t.Errorf("цена на %s = %v: похоже, в ряд попал не золотой металл (Code != 1)", p.date.Format("2006-01-02"), p.value)
		}
	}
}

// TestParseMetalGoldEmptyDocFails: пустой <Metall/> — ошибка (fail-fast), а не пустой ряд.
func TestParseMetalGoldEmptyDocFails(t *testing.T) {
	if _, err := parseMetalGold(new(cbrMetallDoc)); err == nil {
		t.Error("пустой <Metall/> должен давать ошибку, а не 0 точек")
	}
}

// TestParseMetalGoldNoGoldFails: документ непустой, но золота (Code=1) в нём нет — ошибка:
// иначе импортёр молча не обновил бы ряд.
func TestParseMetalGoldNoGoldFails(t *testing.T) {
	body := `<Metall><Record Date="08.10.2026" Code="2"><Buy>167,65</Buy></Record></Metall>`
	if _, err := parseMetalGoldBody(t, body); err == nil {
		t.Error("документ без золота (Code=1) должен давать ошибку, а не 0 точек")
	}
}

// TestParseMetalGoldSkipsBlankRecords: пустые (после TrimSpace) дата или значение — пропуск.
func TestParseMetalGoldSkipsBlankRecords(t *testing.T) {
	points, err := parseMetalGoldBody(t, `<Metall>`+
		`<Record Date="08.10.2026" Code="1"><Buy>11422,95</Buy></Record>`+
		`<Record Date="" Code="1"><Buy>11422,95</Buy></Record>`+
		`<Record Date="09.10.2026" Code="1"><Buy> </Buy></Record>`+
		`</Metall>`)
	if err != nil {
		t.Fatalf("parseMetalGold: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("точек %d, want 1: пустые записи должны пропускаться", len(points))
	}
}

// TestParseMetalGoldBrokenRecordFails: непустое непарсящееся значение или дата — ошибка,
// а не тихий пропуск (битая лента не должна выглядеть успешным прогоном).
func TestParseMetalGoldBrokenRecordFails(t *testing.T) {
	for name, body := range map[string]string{
		"value": `<Metall><Record Date="08.10.2026" Code="1"><Buy>11 422,95</Buy></Record></Metall>`,
		"date":  `<Metall><Record Date="08-10-2026" Code="1"><Buy>11422,95</Buy></Record></Metall>`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseMetalGoldBody(t, body); err == nil {
				t.Errorf("битая запись (%s) должна давать ошибку", name)
			}
		})
	}
}

// TestMetalGoldSeriesMetaCoversParsedRows: имя ряда однорядной таблицы — имя таблицы.
func TestMetalGoldSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrGoldSeriesMeta, func(rec *appendRecorder) error {
		rec.series = cbrGoldTable
		points, err := parseMetalGold(fixtureMetallDoc(t))
		if err != nil {
			return err
		}
		return appendCbrPoints(rec, points)
	})
}
