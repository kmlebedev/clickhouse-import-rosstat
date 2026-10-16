package bank

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// Фикстуры — живые артефакты источника (стандарт §11.1): страница раздела аналитики ДОМ.РФ как
// есть (127 710 Б, разведка 2026-10-16) и книга блока «Топ ипотечных банков» после хирургии zip
// (§11.2: оставлен лист-анкор с шапкой, строками банков, заметками и числовыми форматами).
const (
	domrfPageFixture = "domrf_mortgage_page.html"
	domrfBookFixture = "domrf_mortgage.xlsx"
)

// domrfSheetFixture — имя листа-анкора в книге: номер в имени (13) — часть имени источника, а
// разбор ищет лист по якорю «Ставки предложения» внутри имени (§6.3), потому что добавленный
// источником лист сдвинул бы номер.
const domrfSheetFixture = "13 Ставки предложения"

// domrfFixtureSrc — src файла блока «Топ ипотечных банков»: разбор страницы обязан вернуть тот
// же путь, что лежит в file2.src JSON блока.
const domrfFixtureSrc = "/api/v2/content/download/629470/135272296/"

// domrfFixtureUrl — абсолютный адрес файла: домен плюс src (так его собирает импортёр, §5.4).
const domrfFixtureUrl = "https://xn--d1aqf.xn--p1ai/api/v2/content/download/629470/135272296/"

// domrfSpotEps — допуск сверки spot-значений в машинной точности колонки: rate таблицы —
// Float32 (legacy-схема, спека §4.5), поэтому 0.187 ячейки и 0.187000006 … из батча — одно
// число.
const domrfSpotEps = 1e-6

// domrfFixtureDate — отчётная дата книги: заголовок листа «Ставки предложения по ипотеке на
// 31.08.2026 (последний день отчетного месяца)». Дата одна на всю книгу: лист — снимок месяца,
// а не динамика по колонкам.
var domrfFixtureDate = time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC)

// domrfFixtureSeries — банки, чья ставка по новостройкам в книге есть: 15 из 18 строк блока
// (порядок источника — он же рэнкинг). У Газпромбанка, УБРиР и Россельхозбанка в колонке
// «Новостройки» литеральный 0 источника, который числовой формат листа печатает как «-»: это
// «ставки нет», а не ноль процентов, поэтому точки у них нет (§6.7).
var domrfFixtureSeries = []string{
	"Сбербанк",
	"ВТБ",
	"Альфа-Банк",
	"Банк ДОМ.РФ",
	"Совкомбанк",
	"Т-Банк",
	"Банк Санкт-Петербург",
	"Уралсиб",
	"Промсвязьбанк",
	"Московский Кредитный Банк",
	"Металлинвестбанк",
	"Абсолют Банк",
	"Азиатско-Тихоокеанский Банк",
	"Кубань кредит",
	"БГ ТКБ",
}

// domrfOpenFixture открывает книгу-фикстуру: разбор получает *excelize.File — тот же вход, что
// у ImportFunc шаблона (стандарт §6.1), поэтому тест открывает файл сам и в сеть не ходит
// (§11.3).
func domrfOpenFixture(t *testing.T) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(filepath.Join("testdata", domrfBookFixture))
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", domrfBookFixture, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// domrfSetCell — правка ячейки копии фикстуры для негативных веток: ошибку excelize глотать
// нельзя, иначе тест проверит немутнутый лист.
func domrfSetCell(t *testing.T, f *excelize.File, cell string, value any) {
	t.Helper()
	if err := f.SetCellValue(domrfSheetFixture, cell, value); err != nil {
		t.Fatalf("SetCellValue(%s!%s): %v", domrfSheetFixture, cell, err)
	}
}

// domrfAt — ставка банка в разборе; отсутствие ряда — фатальная ошибка теста.
func domrfAt(t *testing.T, points []domrfPoint, series string) float64 {
	t.Helper()
	for _, p := range points {
		if p.Series == series {
			return p.Value
		}
	}
	t.Fatalf("в разборе нет ряда %q", series)
	return 0
}

// TestDomrfXlsSrcFromPage: ссылка на файл берётся со страницы аналитики (стандарт §4.4, §5.4) —
// из JSON блока «Топ ипотечных банков». Тест идёт по живому пути на HTML-фикстуре (§11.3):
// страница качается не тестом, а импортёром, разбор страницы чистый.
func TestDomrfXlsSrcFromPage(t *testing.T) {
	page, err := os.ReadFile(filepath.Join("testdata", domrfPageFixture))
	if err != nil {
		t.Fatalf("открытие фикстуры %s: %v", domrfPageFixture, err)
	}
	src, err := domrfXlsSrcFromPage(page)
	if err != nil {
		t.Fatalf("domrfXlsSrcFromPage: %v", err)
	}
	if src != domrfFixtureSrc {
		t.Errorf("src = %q, want %q (file2.src блока «Топ ипотечных банков»)", src, domrfFixtureSrc)
	}
	// Сборку абсолютного адреса пинит литерал: префикс домена — константа пакета, которую
	// подставляет импортёр (§5.4 шаг 3).
	if got := domrfDataUrlPrefix + src; got != domrfFixtureUrl {
		t.Errorf("DataUrl = %q, want %q", got, domrfFixtureUrl)
	}
}

// TestDomrfXlsSrcFromPageNoBlock: страница без блока выпусков — ошибка с именем блока, а не
// пустой src (стандарт §4.4, §6.4). Пустая ссылка ушла бы в GetXlsx(""), и вызывающий увидел
// бы только ошибку разбора книги, в которой ни причины, ни источника.
func TestDomrfXlsSrcFromPageNoBlock(t *testing.T) {
	for name, page := range map[string][]byte{
		"минимальная страница": []byte(`<html><body><main id="app"></main></body></html>`),
		"пустое тело":          nil,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := domrfXlsSrcFromPage(page)
			if err == nil {
				t.Fatal("ожидалась ошибка, а src найден")
			}
			if !strings.Contains(err.Error(), "Топ ипотечных банков") {
				t.Errorf("ошибка %q не называет блок «Топ ипотечных банков»", err)
			}
		})
	}
}

// TestParseDomrfSheet — разбор листа «13 Ставки предложения» книги блока «Топ ипотечных банков»
// (разведка 2026-10-16): шапка — строки 3 (продукты) и 5 («Ставка»/«∆»), строки 6–23 — банки,
// 24 — средневзвешенная ставка по банкам (агрегат, не ряд), ниже — заметки. Ставка ряда —
// колонка продукта «Новостройки» (первичный рынок: тот же продукт читал прежний разбор),
// отчётная дата — из заголовка листа. Точек 15 из 18 строк блока: три ячейки без ставки
// пропускаются по §6.7.
func TestParseDomrfSheet(t *testing.T) {
	points, err := parseDomrfSheet(domrfOpenFixture(t))
	if err != nil {
		t.Fatalf("parseDomrfSheet: %v", err)
	}
	if len(points) != 15 {
		t.Errorf("точек %d, want 15 (18 строк банков минус 3 без ставки)", len(points))
	}
	got := make([]string, 0, len(points))
	for _, p := range points {
		got = append(got, p.Series)
		if !p.Date.Equal(domrfFixtureDate) {
			t.Errorf("ряд %q: дата %s, want %s (отчётный месяц листа)",
				p.Series, p.Date.Format(domrfTimeLayout), domrfFixtureDate.Format(domrfTimeLayout))
		}
	}
	slices.Sort(got)
	want := slices.Clone(domrfFixtureSeries)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("ряды разбора %q, want %q (агрегат и банки без ставки — не ряды)", got, want)
	}
	// Spot-значения: первая и последняя точки колонки «Новостройки» (§11.5).
	for _, spot := range []struct {
		series string
		want   float64
	}{
		{series: "Сбербанк", want: 0.187},
		{series: "БГ ТКБ", want: 0.1685},
	} {
		if got := domrfAt(t, points, spot.series); math.Abs(got-spot.want) > domrfSpotEps {
			t.Errorf("ставка %q = %v, want %v (значение ячейки листа)", spot.series, got, spot.want)
		}
	}
}

// TestParseDomrfSheetFailFast: ветки, где разбор обязан вернуть ошибку, а не «0 точек»
// (стандарт §6.4). Мутации — на копии живой фикстуры в памяти (синтетика кодировала бы
// допущения автора, §11.1), а тексты ошибок обязаны называть таблицу и причину.
func TestParseDomrfSheetFailFast(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(t *testing.T, f *excelize.File)
		want   string
	}{
		"нет листа с якорем «Ставки предложения»": {
			mutate: func(t *testing.T, f *excelize.File) {
				if err := f.SetSheetName(domrfSheetFixture, "13 Предложения"); err != nil {
					t.Fatalf("SetSheetName: %v", err)
				}
			},
			want: "Ставки предложения",
		},
		"нет шапки таблицы": {
			mutate: func(t *testing.T, f *excelize.File) { domrfSetCell(t, f, "A3", "Банки") },
			want:   "Банк",
		},
		"нет отчётной даты в заголовке": {
			mutate: func(t *testing.T, f *excelize.File) {
				domrfSetCell(t, f, "A1", "Ставки предложения по ипотеке")
			},
			want: "02.01.2006",
		},
		"нет подшапки ставки": {
			mutate: func(t *testing.T, f *excelize.File) { domrfSetCell(t, f, "B5", "Предложение") },
			want:   "Ставка",
		},
		"ставок нет ни у одного банка": {
			mutate: func(t *testing.T, f *excelize.File) {
				for row := 6; row <= 23; row++ {
					domrfSetCell(t, f, fmt.Sprintf("B%d", row), 0)
				}
			},
			want: "ни одной точки",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := domrfOpenFixture(t)
			tc.mutate(t, f)
			_, err := parseDomrfSheet(f)
			if err == nil {
				t.Fatal("ожидалась ошибка, а разбор прошёл")
			}
			for _, want := range []string{"domrf_mortgage", tc.want} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не называет %q", err, want)
				}
			}
		})
	}
}

// TestAppendDomrfPoints: запись идёт по одному Append на точку в форме (Series, Date, Value) —
// схема таблицы (name, date, rate) — и значение приводится к точности Float32-колонки (спека
// §4.5), как у импортёров Сбера. Ошибка Append не проглатывается (§5.5): расхождение схемы
// обязано вернуться вызывающему, а не пропасть в логе.
func TestAppendDomrfPoints(t *testing.T) {
	points := []domrfPoint{
		{Series: "Сбербанк", Date: domrfFixtureDate, Value: 0.187},
		{Series: "БГ ТКБ", Date: domrfFixtureDate, Value: 0.1685},
	}
	rec := newAppendRecorder("name", "date", "rate")
	if err := appendDomrfPoints(rec, points); err != nil {
		t.Fatalf("appendDomrfPoints: %v", err)
	}
	if len(rec.rows) != len(points) {
		t.Fatalf("записано строк %d, want %d", len(rec.rows), len(points))
	}
	for i, want := range points {
		got := rec.rows[i]
		if got.name != want.Series || !got.date.Equal(want.Date) || got.value != float64(float32(want.Value)) {
			t.Errorf("строка %d = %+v, want (Series, Date, Float32-значение) = %q, %s, %v",
				i, got, want.Series, want.Date.Format(domrfTimeLayout), float64(float32(want.Value)))
		}
	}
	// Чужая схема приёмника — ошибка, которую разбор обязан вернуть вызывающему.
	err := appendDomrfPoints(newAppendRecorder("name", "date"), points)
	if err == nil {
		t.Fatal("Append с чужой схемой: ожидалась ошибка, а она проглочена")
	}
	if !strings.Contains(err.Error(), "Сбербанк") {
		t.Errorf("ошибка %q не называет точку, на которой запись остановилась", err)
	}
}
