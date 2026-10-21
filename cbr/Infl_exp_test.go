package cbr

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

// TestFindLinkMissingAnchor: страница без нужного якоря — ошибка, а не пустой href.
func TestFindLinkMissingAnchor(t *testing.T) {
	for name, tc := range map[string]struct {
		page []byte
		sel  string
	}{
		"пустая страница": {[]byte(`<html><body></body></html>`), inflExpLinkSelector},
		"чужой якорь":     {[]byte(`<html><body><a class="versions_item" href="/a.pdf">x</a></body></html>`), inflExpLinkSelector},
		"пустой href":     {[]byte(`<html><body><a class="versions_item" href="">x</a></body></html>`), inflExpLinkSelector},
	} {
		t.Run(name, func(t *testing.T) {
			if href, err := util.FindLink(tc.page, tc.sel); err == nil {
				t.Errorf("ожидалась ошибка, получен href %q", href)
			}
		})
	}
}

// TestInflExpFindLink: на странице инфляционных ожиданий первая ссылка — свежий xlsx.
func TestInflExpFindLink(t *testing.T) {
	href, err := util.FindLink(readCbrTestdata(t, "infl_exp_page.html"), inflExpLinkSelector)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
	}
	if !strings.HasSuffix(href, ".xlsx") || !strings.Contains(href, "Infl_exp_") {
		t.Errorf("href %q не похож на свежий Infl_exp_*.xlsx", href)
	}
}

// TestInflExpSeriesMetaCoversParsedRows: разбор фикстуры и каталог совпадают в обе стороны.
func TestInflExpSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrInflExpSeriesMeta, func(rec *appendRecorder) error {
		f, err := excelize.OpenFile("testdata/infl_exp.xlsx")
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return inflExpImport(f, rec)
	})
}

// TestInflExpEmptySheetFails: пустой лист «Данные для графиков» (ни маркера, ни строк
// данных) — ошибка, а не «0 rows» с зелёным dagu: молчаливый прогон выглядел бы успешным,
// а ряд остался бы без обновления. Имя таблицы в тексте ошибки — часть контракта, поэтому
// сверяется литералом: дежурный по логу должен видеть, чей разбор дал ноль.
func TestInflExpEmptySheetFails(t *testing.T) {
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", "Данные для графиков"); err != nil {
		t.Fatalf("SetSheetName: %v", err)
	}
	rec := newCbrRecorder()
	err := inflExpImport(f, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, разбор дал %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), "cbr_infl_exp") {
		t.Errorf("ошибка %q не называет таблицу cbr_infl_exp", err)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}

// inflExpPdfFixture — дословный вывод `pdftotext -bbox -enc UTF-8 -f 1 -l 1` по
// PDF «Оперативная справка» за октябрь 2026 (InFOM_26-10.pdf). Фикстура заморожена:
// равенства ниже ловят сдвиг координат и потерю подписей при правке разбора.
const inflExpPdfFixture = "infl_exp_pdf_26-10_bbox.xml"

// TestInflExpPdfPointsFixture: последняя колонка оси — октябрь 2026, три подписи
// сверху вниз — наблюдаемая / ожидаемая через год / ожидаемая через пять лет.
func TestInflExpPdfPointsFixture(t *testing.T) {
	points, err := inflExpPdfPoints(readCbrTestdata(t, inflExpPdfFixture))
	if err != nil {
		t.Fatalf("inflExpPdfPoints: %v", err)
	}
	if len(points) != 34 {
		t.Fatalf("колонок %d, want 34 (янв.24 … окт.26): разбор оси потерял месяцы", len(points))
	}
	last := points[len(points)-1]
	if last.Month.Format("2006-01") != "2026-10" {
		t.Errorf("месяц последней колонки %s, want 2026-10", last.Month.Format("2006-01"))
	}
	if last.Observed != 14.5 || last.Expected1y != 13.2 || last.Expected5y != 12.2 {
		t.Errorf("окт.26: %v/%v/%v, want 14.5/13.2/12.2", last.Observed, last.Expected1y, last.Expected5y)
	}
	first := points[0]
	if first.Month.Format("2006-01") != "2024-01" {
		t.Errorf("первая колонка %s, want 2024-01", first.Month.Format("2006-01"))
	}
}

// TestInflExpPdfHistoryMatchesXlsx: все исторические колонки PDF совпадают с книгой
// Infl_exp_26-09.xlsx до 0.1 п.п. Это доказательство, что колонка определяется по x,
// а ряд — по порядку подписей сверху вниз: ошибка в любом из двух сдвинула бы значения.
func TestInflExpPdfHistoryMatchesXlsx(t *testing.T) {
	points, err := inflExpPdfPoints(readCbrTestdata(t, inflExpPdfFixture))
	if err != nil {
		t.Fatalf("inflExpPdfPoints: %v", err)
	}
	f, err := excelize.OpenFile("testdata/infl_exp.xlsx")
	if err != nil {
		t.Fatalf("открытие книги: %v", err)
	}
	defer func() { _ = f.Close() }()
	rec := newCbrRecorder()
	if err = inflExpImport(f, rec); err != nil {
		t.Fatalf("inflExpImport: %v", err)
	}
	book := make(map[string][2]float64, len(rec.rows))
	for _, row := range rec.rows {
		key := row.date.Format("2006-01")
		values := book[key]
		if row.name == inflExpObservedName {
			values[0] = row.value
		} else if row.name == inflExpExpectedName {
			values[1] = row.value
		}
		book[key] = values
	}
	matched := 0
	for _, p := range points[:len(points)-1] {
		key := p.Month.Format("2006-01")
		want, ok := book[key]
		if !ok {
			t.Errorf("%s: месяца нет в книге — ось PDF и книга разошлись", key)
			continue
		}
		if diff := p.Observed - want[0]; diff > 0.05 || diff < -0.05 {
			t.Errorf("%s: наблюдаемая из PDF %v, из книги %v", key, p.Observed, want[0])
		}
		if diff := p.Expected1y - want[1]; diff > 0.05 || diff < -0.05 {
			t.Errorf("%s: ожидаемая из PDF %v, из книги %v", key, p.Expected1y, want[1])
		}
		matched++
	}
	if matched < 30 {
		t.Errorf("сверено %d месяцев, want не меньше 30: фикстура книги или PDF устарела", matched)
	}
}

// TestInflExpPdfNoTextLayerFails: старые выпуски — сканы без текстового слоя; разбор
// обязан вернуть ошибку с именем импортёра, а не пустой список (иначе шаг выглядел бы
// успешным «0 rows», а оперативная точка осталась бы незаписанной).
func TestInflExpPdfNoTextLayerFails(t *testing.T) {
	for name, tc := range map[string]struct {
		body []byte
		want string
	}{
		"пустой XHTML":   {[]byte(`<html><body><doc><page></page></doc></body></html>`), "текст"},
		"чужой документ": {[]byte(`<html><body><doc><page><flow><block><line><word xMin="1" yMin="1" xMax="2" yMax="2">Данные</word></line></block></flow></page></doc></body></html>`), "ось"},
	} {
		t.Run(name, func(t *testing.T) {
			points, err := inflExpPdfPoints(tc.body)
			if err == nil {
				t.Fatalf("ожидалась ошибка, получено %d колонок", len(points))
			}
			for _, want := range []string{cbrInflExpOpName, tc.want} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ошибка %q не содержит %q", err, want)
				}
			}
		})
	}
}

// inflExpPdfSynthetic — минимальная страница `-bbox` для негативных сценариев:
// ось из месяцев months (x = 100 + 12.5·i, y = 500) и подписи значений над ней.
func inflExpPdfSynthetic(months []string, values [][]string) []byte {
	var b strings.Builder
	b.WriteString(`<html><body><doc><page><flow><block><line>`)
	word := func(x, y float64, text string) {
		fmt.Fprintf(&b, `<word xMin="%.2f" yMin="%.2f" xMax="%.2f" yMax="%.2f">%s</word>`, x, y, x+10, y+10, text)
	}
	for i, month := range months {
		word(100+12.5*float64(i), 500, month)
	}
	for i, column := range values {
		for j, value := range column {
			word(100+12.5*float64(i), 200+10*float64(j), value)
		}
	}
	b.WriteString(`</line></block></flow></page></doc></body></html>`)
	return []byte(b.String())
}

// TestInflExpPdfColumnShapeFails: последняя колонка обязана иметь ровно три подписи
// в порядке убывания, а месяцы оси — идти подряд. Любое расхождение — ошибка, а не
// сдвиг рядов: иначе наблюдаемая и ожидаемая поменялись бы местами молча.
func TestInflExpPdfColumnShapeFails(t *testing.T) {
	months := []string{"янв.24", "фев.24", "мар.24", "апр.24", "май.24", "июн.24"}
	good := [][]string{{"15,1", "14,2", "10,7"}, {"15,1", "14,2", "10,7"}, {"15,1", "14,2", "10,7"},
		{"15,1", "14,2", "10,7"}, {"15,1", "14,2", "10,7"}, {"14,5", "13,2", "12,2"}}
	withLast := func(last ...string) [][]string {
		return append(append([][]string{}, good[:5]...), last)
	}
	for name, tc := range map[string]struct {
		months []string
		values [][]string
		want   string
	}{
		"две подписи в последней колонке": {months: months, values: withLast("14,5", "13,2"), want: "3"},
		"порядок рядов нарушен":           {months: months, values: withLast("13,2", "14,5", "12,2"), want: "поряд"},
		"месяцы оси не подряд": {
			months: []string{"янв.24", "фев.24", "мар.24", "апр.24", "май.24", "июл.24"},
			values: good,
			want:   "подряд",
		},
	} {
		t.Run(name, func(t *testing.T) {
			points, err := inflExpPdfPoints(inflExpPdfSynthetic(tc.months, tc.values))
			if err == nil {
				t.Fatalf("ожидалась ошибка, получено %d колонок", len(points))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ошибка %q не содержит %q", err, tc.want)
			}
		})
	}
}

// TestInflExpPdfNotPDFFails: WAF/HTML вместо файла распознаётся до exec по сигнатуре.
func TestInflExpPdfNotPDFFails(t *testing.T) {
	_, err := inflExpPdfBBox([]byte(`<html><body>Доступ ограничен</body></html>`))
	if err == nil {
		t.Fatal("ожидалась ошибка: HTML-заглушка не является PDF")
	}
	if !strings.Contains(err.Error(), "не PDF") {
		t.Errorf("ошибка %q не называет подмену файла", err)
	}
}

// TestInflExpPdfToolMissingFails: отсутствующий pdftotext — ошибка с подсказкой
// poppler-utils, а не «0 rows» и не паника exec.
func TestInflExpPdfToolMissingFails(t *testing.T) {
	old := inflExpPdfTool
	inflExpPdfTool = "pdftotext-kimi-code-missing"
	defer func() { inflExpPdfTool = old }()
	_, err := inflExpPdfBBox([]byte("%PDF-1.4\n1 0 obj\n"))
	if err == nil {
		t.Fatal("ожидалась ошибка отсутствующего pdftotext")
	}
	if !strings.Contains(err.Error(), "poppler-utils") {
		t.Errorf("ошибка %q не называет poppler-utils", err)
	}
}

// TestInflExpPdfNewer: гейт сравнивает 14-е число месяца PDF с уже записанной датой.
func TestInflExpPdfNewer(t *testing.T) {
	october := time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		known time.Time
		want  bool
	}{
		"пустая таблица": {time.Time{}, true},
		"тот же месяц":   {time.Date(2026, time.October, 14, 0, 0, 0, 0, time.UTC), false},
		"прошлый месяц":  {time.Date(2026, time.September, 14, 0, 0, 0, 0, time.UTC), true},
		"будущий месяц":  {time.Date(2026, time.November, 14, 0, 0, 0, 0, time.UTC), false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := inflExpPdfNewer(october, tc.known); got != tc.want {
				t.Errorf("inflExpPdfNewer(2026-10, %s) = %v, want %v", tc.known.Format("2006-01-02"), got, tc.want)
			}
		})
	}
}

// TestInflExpPdfMonthFromHref: месяц берётся из имени inFOM_ГГ-ММ.pdf; книга и чужие
// файлы — ошибка, потому что месяц из них не следует.
func TestInflExpPdfMonthFromHref(t *testing.T) {
	month, err := inflExpPdfMonth("/Collection/Collection/File/62459/inFOM_26-10.pdf")
	if err != nil {
		t.Fatalf("inflExpPdfMonth: %v", err)
	}
	if month.Format("2006-01") != "2026-10" {
		t.Errorf("месяц %s, want 2026-10", month.Format("2006-01"))
	}
	for name, href := range map[string]string{
		"книга":     "/Collection/Collection/File/62395/Infl_exp_26-09.xlsx",
		"чужой pdf": "/Collection/Collection/File/1/other.pdf",
		"без даты":  "/Collection/Collection/File/1/inFOM.pdf",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err = inflExpPdfMonth(href); err == nil {
				t.Errorf("href %q: ожидалась ошибка", href)
			}
		})
	}
}

// TestInflExpPdfFindLink: на странице инфляционных ожиданий первая inFOM-ссылка — свежий
// PDF; селектор xlsx при этом продолжает работать.
func TestInflExpPdfFindLink(t *testing.T) {
	page := readCbrTestdata(t, "infl_exp_page.html")
	href, err := util.FindLink(page, inflExpPdfLinkSelector)
	if err != nil {
		t.Fatalf("FindLink(pdf): %v", err)
	}
	if !strings.HasSuffix(href, "inFOM_26-10.pdf") {
		t.Errorf("свежий PDF %q, want inFOM_26-10.pdf", href)
	}
	xlsx, err := util.FindLink(page, inflExpLinkSelector)
	if err != nil {
		t.Fatalf("FindLink(xlsx): %v", err)
	}
	if !strings.HasSuffix(xlsx, ".xlsx") {
		t.Errorf("свежий XLSX %q", xlsx)
	}
}

// TestInflExpPdfAppendPoint: пишутся ровно два ряда — наблюдаемая и ожидаемая через год;
// пятилетний ряд остаётся только индикатором порядка подписей.
func TestInflExpPdfAppendPoint(t *testing.T) {
	rec := newCbrRecorder("name", "date", "value")
	point := inflExpPdfPoint{
		Month:      time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC),
		Observed:   14.5,
		Expected1y: 13.2,
		Expected5y: 12.2,
	}
	if err := inflExpPdfAppend(point, rec); err != nil {
		t.Fatalf("inflExpPdfAppend: %v", err)
	}
	if len(rec.rows) != 2 {
		t.Fatalf("строк %d, want 2", len(rec.rows))
	}
	want := map[string]float64{inflExpObservedName: 14.5, inflExpExpectedName: 13.2}
	for _, row := range rec.rows {
		if row.date.Format("2006-01-02") != "2026-10-14" {
			t.Errorf("дата %s, want 2026-10-14", row.date.Format("2006-01-02"))
		}
		if value, ok := want[row.name]; !ok || value != row.value {
			t.Errorf("ряд %q = %v, want %v", row.name, row.value, want[row.name])
		}
	}
}

// TestInflExpPdfAppendLatestMonthMismatch: файл за сентябрь, а график заканчивается
// октябрём — ошибка и ни одной строки: писать октябрьскую точку под сентябрьским
// выпуском значило бы сдвинуть месяц.
func TestInflExpPdfAppendLatestMonthMismatch(t *testing.T) {
	rec := newCbrRecorder("name", "date", "value")
	err := inflExpPdfAppendLatest(readCbrTestdata(t, inflExpPdfFixture), time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC), rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка, записано %d строк", len(rec.rows))
	}
	if !strings.Contains(err.Error(), "график") {
		t.Errorf("ошибка %q не называет расхождение графика и файла", err)
	}
	if len(rec.raw) != 0 {
		t.Errorf("при ошибке записано %d строк, want 0", len(rec.raw))
	}
}

// TestInflExpPdfAppendLatestRows: на фикстуре 26-10 пишутся две строки октября.
func TestInflExpPdfAppendLatestRows(t *testing.T) {
	rec := newCbrRecorder("name", "date", "value")
	if err := inflExpPdfAppendLatest(readCbrTestdata(t, inflExpPdfFixture), time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC), rec); err != nil {
		t.Fatalf("inflExpPdfAppendLatest: %v", err)
	}
	if len(rec.rows) != 2 {
		t.Fatalf("строк %d, want 2", len(rec.rows))
	}
	for _, row := range rec.rows {
		if row.date.Format("2006-01-02") != "2026-10-14" {
			t.Errorf("дата %s, want 2026-10-14", row.date.Format("2006-01-02"))
		}
	}
}

// TestInflExpPdfAppendError: сбой Append не проглатывается и называет импортёр и ряд.
func TestInflExpPdfAppendError(t *testing.T) {
	rec := newCbrRecorder("series")
	point := inflExpPdfPoint{Month: time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC), Observed: 14.5, Expected1y: 13.2}
	err := inflExpPdfAppend(point, rec)
	if err == nil {
		t.Fatalf("ожидалась ошибка записи, строк принято %d", len(rec.rows))
	}
	for _, want := range []string{cbrInflExpOpName, inflExpObservedName, "2026-10-14"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
}
