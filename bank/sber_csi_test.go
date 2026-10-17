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

	points, skipped, err := parseSberRows(loadFixture(t, sberMonthlyFixture))
	if err != nil {
		t.Fatalf("parseSberRows: %v", err)
	}
	if skipped != 0 {
		t.Errorf("фикстура не содержит пустых значений, а разбор пропустил %d записей", skipped)
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
	bomPoints, bomSkipped, err := parseSberRows([][]string{
		{"\ufeffdate", "category", "value"},
		{"2026-09-01", "Всего", "100"},
	})
	if err != nil {
		t.Fatalf("parseSberRows с BOM в шапке: %v", err)
	}
	if len(bomPoints) != 1 || bomPoints[0].Series != "Всего" || bomSkipped != 0 {
		t.Errorf("parseSberRows с BOM в шапке: точки %+v, пропущено %d", bomPoints, bomSkipped)
	}
}

// TestSberCSIWeekParse — недельная выгрузка: ровно 138 точек (46 категорий × 3 недели),
// дата — воскресенье ISO-недели, значения — % г/г, имена нормализованы: «Все категории »
// источника становится «Все категории». Фикстуры обеих выгрузок сняты с живых файлов и пустых
// ячеек значения не содержат (`skipped == 0`): правка «пропуск вместо ошибки» их не касается.
func TestSberCSIWeekParse(t *testing.T) {
	points, skipped, err := parseSberRows(loadFixture(t, sberWeeklyFixture))
	if err != nil {
		t.Fatalf("parseSberRows: %v", err)
	}
	if skipped != 0 {
		t.Errorf("фикстура не содержит пустых значений, а разбор пропустил %d записей", skipped)
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
		// Пропуск записи с пустым значением (стандарт §6.7) не отменяет остальных проверок: у
		// записи с пустой ячейкой значения битая дата и пустая категория остаются ошибками —
		// иначе порча выгрузки пряталась бы за «пропуском».
		{"BrokenDateWithEmptyValue", [][]string{header, {"2026-13-45", "Всего", ""}}, "дата", badRow},
		{"EmptyCategoryWithEmptyValue", [][]string{header, {"2026-09-01", "   ", ""}}, "пустое имя категории", badRow},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			points, _, err := parseSberRows(c.records)
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

// TestParseSberRowsSkipsEmptyValues — запись с пустой ячейкой значения не валит прогон и не
// становится нулевой точкой (§6.7): такое наблюдение источника просто отсутствует. Это
// требование цепочки `type: chain` (dagu/bank.yaml): падение шага Сбера лишило бы данных шаги
// tbank/vtb (стандарт §12.3). Прежний недельный импортёр вёл себя так же (`continue`).
func TestParseSberRowsSkipsEmptyValues(t *testing.T) {
	header := []string{"date", "category", "value"}

	points, skipped, err := parseSberRows([][]string{
		header,
		{"2026-09-01", "Всего", ""},
		{"2026-08-01", "Всего", "   "}, // пробелы — та же пустая ячейка
		{"2026-07-01", "Всего", "99"},
		{"2026-09-01", "Услуги", "125.23"},
	})
	if err != nil {
		t.Fatalf("пустое значение не ошибка, получено: %v", err)
	}
	if skipped != 2 {
		t.Errorf("пропущено %d записей, ожидалось 2", skipped)
	}
	if len(points) != 2 {
		t.Fatalf("точек %d, ожидалось 2: %+v", len(points), points)
	}
	// Пропуск не превращается в точку: ни нулевого значения, ни точки на дату пропуска (иначе
	// пропуск источника прочитался бы агентом как «расходы упали до нуля»).
	for _, p := range points {
		if p.Value == 0 {
			t.Errorf("нулевая точка %+v: пропуск превратился в значение", p)
		}
		if p.Date.Equal(sberDay(2026, time.September, 1)) && p.Series == "Всего" {
			t.Errorf("точка на дату пропуска: %+v", p)
		}
	}
	if got := sberAt(t, points, "Всего", sberDay(2026, time.July, 1)); got != 99 {
		t.Errorf("значение уцелевшей точки %v, ожидалось 99", got)
	}

	// Выгрузка, где пусты значения всех записей, остаётся ошибкой: это не «0 строк» шаблона, а
	// подменённый или сломанный источник. Сообщение называет число пропущенных записей.
	allEmpty, skipped, err := parseSberRows([][]string{
		header,
		{"2026-09-01", "Всего", ""},
		{"2026-08-01", "Всего", "  "},
	})
	if err == nil {
		t.Fatalf("ожидалась ошибка «все значения пусты», получено %d точек", len(allEmpty))
	}
	if !strings.Contains(err.Error(), "пустая выгрузка") ||
		!strings.Contains(err.Error(), "пропущено записей с пустым значением: 2") {
		t.Errorf("ошибка %q не называет пустую выгрузку и число пропущенных записей", err)
	}
	if skipped != 2 || len(allEmpty) != 0 {
		t.Errorf("пропущено %d, точек %d — ожидалось 2 и 0", skipped, len(allEmpty))
	}
}

// sberFixtureSeries — имена рядов, разобранные из фикстуры: контракт каталога проверяется против
// разбора, а не против самого себя (стандарт §11.4). Дубли имён не схлопываются намеренно: набор
// имён для сверки строит вызывающий.
func sberFixtureSeries(t *testing.T, fixture string) []string {
	t.Helper()
	points, _, err := parseSberRows(loadFixture(t, fixture))
	if err != nil {
		t.Fatalf("parseSberRows(%s): %v", fixture, err)
	}
	names := make([]string, 0, len(points))
	for _, p := range points {
		names = append(names, p.Series)
	}
	return names
}

// TestSberSeriesMetaCoversFixtures — контракт каталога на живых фикстурах (стандарт §11.4):
// каждая категория, разобранная из выгрузки, описана в каталоге своего набора — месячная в
// sberIndexSeriesMeta, недельная в sberWeekSeriesMeta. Контракт односторонний («разобрано ⊆
// описано»): имена рядов даёт источник, а урезанная фикстура не доказывает, что описанный ряд ещё
// публикуется. Разные наборы проверяются по отдельности, потому что имена категорий у них
// пересекаются («Услуги», «Продовольственные товары»): описание месячной категории не описывает
// одноимённый недельный ряд, у которого другая единица (индекс против % г/г).
//
// Кроме покрытия сверяются: заполненность описания (пустое поле — ряд, о котором агенту нечего
// прочитать), уникальность имени внутри источника (дубль схлопнул бы два разных ряда в один ключ
// каталога), число описаний (5 и 46 — набор живой выгрузки, спека §4.3) и состав sberSeriesMeta,
// из которой конкатенацией собирается общий каталог пакета bankSeriesMeta: потерянный при сборке
// ряд не заметил бы больше ни один тест (стандарт §10.1, §10.6).
func TestSberSeriesMetaCoversFixtures(t *testing.T) {
	cases := []struct {
		fixture string
		meta    []util.SeriesMeta
		source  string
		series  int
		// titleSuffix — конец заголовка по шаблону набора (у недельного он собирается
		// fmt.Sprintf вместе с описанием): пустая строка — шаблона у набора нет.
		titleSuffix string
	}{
		{sberMonthlyFixture, sberIndexSeriesMeta, sberSourceIndex, 5, ""},
		{sberWeeklyFixture, sberWeekSeriesMeta, sberSourceWeek, 46, ", недельно, % г/г"},
	}

	described := make(map[string]bool, len(sberSeriesMeta))
	for _, c := range cases {
		if len(c.meta) != c.series {
			t.Errorf("%s: описаний %d, ожидалось %d", c.source, len(c.meta), c.series)
		}
		own := make(map[string]bool, len(c.meta))
		for _, m := range c.meta {
			if m.Source != c.source {
				t.Errorf("ряд %q: источник %q, ожидался %q", m.Series, m.Source, c.source)
			}
			if m.Series == "" || m.Title == "" || m.Unit == "" || m.Frequency == "" || m.Origin == "" || m.Description == "" {
				t.Errorf("описание ряда %q заполнено не полностью: %+v", m.Series, m)
			}
			// Описание недельного ряда собирает fmt.Sprintf: незакрытый знак процента превратил бы
			// текст в «%!(NOVERB)» — в каталоге у агента оказался бы мусор формата, а не фраза.
			if strings.Contains(m.Title+m.Unit+m.Description, "%!") {
				t.Errorf("ряд %q: в описании остался мусор формата (%q)", m.Series, m.Title)
			}
			if c.titleSuffix != "" && !strings.HasSuffix(m.Title, c.titleSuffix) {
				t.Errorf("ряд %q: заголовок %q не по шаблону набора (ожидался конец %q)", m.Series, m.Title, c.titleSuffix)
			}
			if own[m.Series] {
				t.Errorf("ряд %q описан в %s дважды", m.Series, c.source)
			}
			own[m.Series] = true
			described[c.source+"/"+m.Series] = true
		}
		for _, series := range sberFixtureSeries(t, c.fixture) {
			if !own[series] {
				t.Errorf("ряд %q из фикстуры %s не описан в %s", series, c.fixture, c.source)
			}
		}
	}

	if want := len(sberIndexSeriesMeta) + len(sberWeekSeriesMeta); len(sberSeriesMeta) != want {
		t.Errorf("sberSeriesMeta: описаний %d, ожидалось %d (оба набора)", len(sberSeriesMeta), want)
	}
	for _, m := range sberSeriesMeta {
		if !described[m.Source+"/"+m.Series] {
			t.Errorf("ряд %s/%s есть в sberSeriesMeta, но не в описаниях набора", m.Source, m.Series)
		}
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

// TestSberSpendingViewContract — форма витрины v_bank_sber_spending (bank/views.go) закреплена
// значениями, а не константами пакета: имя, обе таблицы-источника в Tables и оба source-литерала в
// Select — публичный контракт для агента (правило 11 AGENTS.md, спека §4.3, стандарт §10.3).
// Случайная правка витрины (выпавшая из Tables таблица, забытый UNION ALL, подменённый source)
// обязана краснить тест, а не менять форму витрины молча: агент читает оба набора одним запросом, и
// без второго source его ряд исчез бы из витрины, а без второй таблицы в Tables витрина собиралась
// бы из одного набора. Литералы здесь намеренно записаны, а не взяты из sberSourceIndex/
// sberSourceWeek: проверяется то, что реально увидит агент, — сверку с константами тест прошёл бы и
// при их переименовании, то есть при смене ключей каталога.
func TestSberSpendingViewContract(t *testing.T) {
	if got := sberSpendingView.Name; got != "v_bank_sber_spending" {
		t.Errorf("имя витрины %q, ожидалось v_bank_sber_spending", got)
	}
	for _, table := range []string{"sber_consumper_spending_index", "sber_izmenenie_trat"} {
		if !slices.Contains(sberSpendingView.Tables, table) {
			t.Errorf("таблица %q отсутствует в Tables (%v): util.CreateView пропустит витрину до "+
				"первого прогона импортёра набора или соберёт её без части данных",
				table, sberSpendingView.Tables)
		}
	}
	for _, source := range []string{"sber_consumper_spending_index", "sber_izmenenie_trat"} {
		if !strings.Contains(sberSpendingView.Select, "'"+source+"'") {
			t.Errorf("source-литерал %q отсутствует в Select: набор исчезнет из витрины или "+
				"сольётся с другим набором в один ключ", source)
		}
	}
}
