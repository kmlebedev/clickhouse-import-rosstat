package bank

import (
	"bytes"
	"encoding/csv"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Фикстуры — урезанные живые выгрузки SberIndex (стандарт §11.1–11.2): шапка, BOM и
// разделитель `;` источника, как есть (урезание ничего не перекомпоновывает). Тесты пакета
// офлайн (§11.3) и идут из каталога пакета.
const (
	sberMonthlyFixture = "sber_consumper_spending_index.csv"
	sberWeeklyFixture  = "sber_izmenenie-trat-po-kategoriyam.csv"
)

// sberSpotEps — допуск сверки spot-значений: разбор идёт из того же текста, что и литерал
// теста, но допуск не позволяет ошибке в младших знаках пройти незамеченной.
const sberSpotEps = 1e-9

// sberSpot — ожидаемая точка фикстуры: ряд, дата и значение.
type sberSpot struct {
	series string
	date   time.Time
	want   float64
}

// sberDay — дата наблюдения в UTC (time.Parse из разбора тоже даёт UTC).
func sberDay(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// loadFixture читает фикстуру ровно теми же шагами, что транспорт util.GetCSV: снимает ведущий
// UTF-8 BOM (файл SberIndex начинается с ef bb bf), нормализует CRLF → LF и разбирает с
// разделителем `;`. Нормализация повторена здесь по коду GetCSV намеренно: тест не должен ходить
// в сеть (§11.3), а живой путь проверяется тестом транспорта util.TestGetCSVStripsBOM. При
// изменении нормализации в GetCSV поправить и этот хелпер.
func loadFixture(t *testing.T, name string) [][]string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("открыть фикстуру: %v", err)
	}
	r := csv.NewReader(bytes.NewBufferString(strings.ReplaceAll(strings.TrimPrefix(string(body), "\ufeff"), "\r", "\n")))
	r.Comma = ';'
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("разобрать фикстуру: %v", err)
	}
	return records
}

// sberAt — значение точки ряда на дату; отсутствие точки — фатальная ошибка теста.
func sberAt(t *testing.T, points []sberPoint, series string, date time.Time) float64 {
	t.Helper()
	for _, p := range points {
		if p.Series == series && p.Date.Equal(date) {
			return p.Value
		}
	}
	t.Fatalf("нет точки %q на %s", series, date.Format(sberCSVTimeLayout))
	return 0
}

// sberCheckSpots сверяет spot-значения по обоим наборам: значение из известной строки файла
// и его дата (§11.5).
func sberCheckSpots(t *testing.T, points []sberPoint, spots []sberSpot) {
	t.Helper()
	for _, s := range spots {
		got := sberAt(t, points, s.series, s.date)
		if math.Abs(got-s.want) > sberSpotEps {
			t.Errorf("ряд %q %s: значение %v, ожидалось %v",
				s.series, s.date.Format(sberCSVTimeLayout), got, s.want)
		}
	}
}

// TestSberCSIParse — месячная выгрузка: ровно 10 точек (5 категорий × 2 месяца), даты — первое
// число месяца, значения — индекс (100 = декабрь 2018). Фикстура — урезание живого файла:
// BOM источника сохранён (§11.1), а разбор переносит BOM в шапке, если транспорт его не снял.
func TestSberCSIParse(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", sberMonthlyFixture))
	if err != nil {
		t.Fatalf("открыть фикстуру: %v", err)
	}
	if !bytes.HasPrefix(raw, []byte("\ufeff")) {
		t.Error("фикстура потеряла BOM источника — это уже не урезание живого файла")
	}

	points, err := parseSberRows(loadFixture(t, sberMonthlyFixture))
	if err != nil {
		t.Fatalf("parseSberRows: %v", err)
	}
	if len(points) != 10 {
		t.Fatalf("точек %d, ожидалось 10", len(points))
	}
	sberCheckSpots(t, points, []sberSpot{
		{"Всего", sberDay(2018, time.December, 1), 100},
		{"Всего", sberDay(2026, time.September, 1), 128.99},
		{"Услуги", sberDay(2026, time.September, 1), 125.23},
		{"Общественное питание", sberDay(2026, time.September, 1), 194.21},
	})
	for _, p := range points {
		if p.Date.Day() != 1 {
			t.Errorf("ряд %q: дата %s — не первое число месяца",
				p.Series, p.Date.Format(sberCSVTimeLayout))
		}
	}

	// Шапку с неснятым BOM (файл без кавычек) разбор обязан распознать, а не падать
	// «неожиданная шапка»: так выглядит выгрузка, если транспорт BOM не снял.
	bomPoints, err := parseSberRows([][]string{
		{"\ufeffdate", "category", "value"},
		{"2026-09-01", "Всего", "100"},
	})
	if err != nil {
		t.Fatalf("parseSberRows с BOM в шапке: %v", err)
	}
	if len(bomPoints) != 1 || bomPoints[0].Series != "Всего" {
		t.Errorf("parseSberRows с BOM в шапке: %+v", bomPoints)
	}
}

// TestSberCSIWeekParse — недельная выгрузка: ровно 138 точек (46 категорий × 3 недели),
// дата — воскресенье ISO-недели, значения — % г/г, имена нормализованы: «Все категории »
// источника становится «Все категории».
func TestSberCSIWeekParse(t *testing.T) {
	points, err := parseSberRows(loadFixture(t, sberWeeklyFixture))
	if err != nil {
		t.Fatalf("parseSberRows: %v", err)
	}
	if len(points) != 138 {
		t.Fatalf("точек %d, ожидалось 138", len(points))
	}
	sberCheckSpots(t, points, []sberSpot{
		{"Все категории", sberDay(2026, time.October, 11), 7.46266724},
		{"Продовольственные товары", sberDay(2026, time.September, 27), 6.11935367},
		{"Ювелирные изделия и часы", sberDay(2026, time.October, 11), 6.48375314},
	})
	for _, p := range points {
		if p.Series == "" || strings.TrimSpace(p.Series) != p.Series {
			t.Errorf("имя категории %q не нормализовано", p.Series)
		}
		if p.Date.Weekday() != time.Sunday {
			t.Errorf("ряд %q: дата %s — не воскресенье ISO-недели",
				p.Series, p.Date.Format(sberCSVTimeLayout))
		}
	}
}

// TestParseSberRowsFailFast — каждый класс дефекта выгрузки даёт ошибку с контекстом записи,
// а не «0 строк» (§6.4–6.5). Подстроки ошибки указывают на ожидаемое место дефекта: иначе
// подтест прошёл бы на любой другой ошибке разбора.
func TestParseSberRowsFailFast(t *testing.T) {
	// htmlStub — тело HTML-оболочки SPA: так отвечает мёртвый эндпоинт sberindex.ru.
	const htmlStub = `<!doctype html><html lang="ru"><head><title>СберИндекс</title></head>` +
		`<body><div id="root"></div></body></html>`
	header := []string{"date", "category", "value"}
	const badRow = "запись 2" // номер записи в выгрузке: шапка — первая

	cases := []struct {
		name    string
		records [][]string
		wantIn  string // причина
		wantPos string // номер записи в тексте ошибки; пусто — не проверяется
	}{
		{"Empty", [][]string{}, "пустая выгрузка", ""},
		{"HeaderOnly", [][]string{header}, "пустая выгрузка", ""},
		{"HtmlStub", [][]string{{htmlStub}}, "HTML-заглушку", ""},
		{"BrokenDate", [][]string{header, {"2026-13-45", "Всего", "100"}}, "дата", badRow},
		{"NotANumber", [][]string{header, {"2026-09-01", "Всего", "сто"}}, "не число", badRow},
		// ParseFloat принимает литералы NaN/Inf/infinity молча: без отдельной проверки такая
		// точка уехала бы в ClickHouse как «значение».
		{"NaNValue", [][]string{header, {"2026-09-01", "Всего", "NaN"}}, "не конечно", badRow},
		{"InfValue", [][]string{header, {"2026-09-01", "Всего", "+Inf"}}, "не конечно", badRow},
		{"InfinityValue", [][]string{header, {"2026-09-01", "Всего", "infinity"}}, "не конечно", badRow},
		{"ShortRow", [][]string{header, {"2026-09-01", "Всего"}}, "полей", badRow},
		{"EmptyCategory", [][]string{header, {"2026-09-01", "   ", "100"}}, "пустое имя категории", badRow},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			points, err := parseSberRows(c.records)
			if err == nil {
				t.Fatalf("ожидалась ошибка, получено %d точек", len(points))
			}
			if !strings.Contains(err.Error(), c.wantIn) {
				t.Errorf("ошибка %q не упоминает %q", err, c.wantIn)
			}
			if c.wantPos != "" && !strings.Contains(err.Error(), c.wantPos) {
				t.Errorf("ошибка %q не называет место дефекта %q", err, c.wantPos)
			}
		})
	}
}

// TestSberUndescribed — точка, чьё имя отсутствует в каталоге, возвращается (импортёр
// журналирует warn и импортирует её, §6.6); описанная — нет. Имена идут в порядке первого
// появления и без дублей: предупреждение в журнале не должно повторяться на каждую точку.
func TestSberUndescribed(t *testing.T) {
	points := []sberPoint{
		{Series: "Всего", Date: sberDay(2026, time.September, 1), Value: 128.99},
		{Series: "Новая категория", Date: sberDay(2026, time.September, 1), Value: 1},
		{Series: "Новая категория", Date: sberDay(2026, time.August, 1), Value: 2},
		{Series: "Услуги", Date: sberDay(2026, time.September, 1), Value: 125.23},
		{Series: "Ещё одна новая", Date: sberDay(2026, time.September, 1), Value: 3},
	}
	meta := []util.SeriesMeta{
		{Source: "sber_izmenenie_trat", Series: "Всего"},
		{Source: "sber_izmenenie_trat", Series: "Услуги"},
	}

	got := sberUndescribed(points, meta)
	want := []string{"Новая категория", "Ещё одна новая"}
	if !slices.Equal(got, want) {
		t.Errorf("неописанные ряды %q, ожидались %q", got, want)
	}

	// Описанный ряд не возвращается: warn-а для него быть не должно.
	if got := sberUndescribed(points[:1], meta); len(got) != 0 {
		t.Errorf("описанный ряд вернулся как неописанный: %q", got)
	}
}
