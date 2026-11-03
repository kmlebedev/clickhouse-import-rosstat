package cbr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

// Импортёр cbr_psk — квартальный корпус среднерыночных значений ПСК кредитных организаций:
// три эры формата (PDF 2014–2020, XLS 2020–2023, XLSX 2023–2026) и лента выпусков со страницы
// ЦБ. Ожидаемые значения ниже — литералами, а не константами пакета: тест, сверяющий таблицу с
// самой собой, прошёл бы и после правки разбора, то есть ровно на той правке, от которой обязан
// падать (стандарт §8.5). Фикстуры — живые файлы выпусков (стандарт §11.2): 14082026_ko.xlsx,
// 14082020_ko.xls, 16112018_ko.pdf, 14112014_ko.pdf (одно значение — предельного ещё нет),
// 14022022_ko.xls (данные в «Лист2», X в числовых колонках), 14022023_ko.xls (лист «Лист1»,
// NUL-байты в кодах), 13022015_ko.pdf (повреждённый текстовый слой) и живая лента psk_page.html.
const (
	pskXlsxFixture      = "testdata/psk_ko_latest.xlsx"
	pskXlsFixture       = "testdata/psk_ko_2020.xls"
	pskXls2022Fixture   = "testdata/psk_ko_2022.xls"
	pskXls2023Fixture   = "testdata/psk_ko_2023.xls"
	pskPDFFixture       = "testdata/psk_ko_2018.pdf"
	pskPDF2014Fixture   = "testdata/psk_ko_2014.pdf"
	pskPDFBrokenFixture = "testdata/psk_ko_2015_broken.pdf"
	pskPageFixture      = "testdata/psk_page.html"
)

// pskTestFixture читает фикстуру теста. Файл на диске тест не меняет никогда: мутации веток
// fail-fast собираются в памяти (стандарт §11.3 — тесты офлайн, фикстура остаётся живой).
func pskTestFixture(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", path, err)
	}
	return body
}

// pskCodes — коды точек в порядке разбора: сравнение набора с ожидаемым списком ловит и
// перестановку, и лишнюю секцию, попавшую в точки.
func pskCodes(points []cbrPskPoint) []string {
	codes := make([]string, 0, len(points))
	for _, point := range points {
		codes = append(codes, point.Code)
	}
	return codes
}

// pskSpot — точка по коду с проверкой, что она есть: spot-значения фикстур сверяются с живым
// файлом и не зависят от порядка остальных точек.
func pskSpot(t *testing.T, points []cbrPskPoint, code string) cbrPskPoint {
	t.Helper()
	for _, point := range points {
		if point.Code == code {
			return point
		}
	}
	t.Fatalf("в разборе нет категории %q (коды: %v)", code, pskCodes(points))
	return cbrPskPoint{}
}

// pskCheckValue — сравнение значения с точностью до представления float64: фикстура печатает
// десятичную дробь, и ожидание записано той же дробью.
func pskCheckValue(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s = %v, хотим %v", what, got, want)
	}
}

// pskFloatPtr — адрес ожидаемого предельного значения: psk_max в таблице Nullable, и nil означает
// «в выпуске не публиковалось».
func pskFloatPtr(value float64) *float64 { return &value }

// pskCheckMax — сравнение psk_max с nullable-ожиданием: nil — предельного значения в выпуске нет
// (PDF-эра 2014), число — есть.
func pskCheckMax(t *testing.T, what string, got, want *float64) {
	t.Helper()
	switch {
	case want == nil && got != nil:
		t.Errorf("%s = %v, ожидался NULL (в выпуске предельного значения не было)", what, *got)
	case want != nil && got == nil:
		t.Errorf("%s = NULL, ожидалось %v", what, *want)
	case want != nil && got != nil:
		pskCheckValue(t, what, *got, *want)
	}
}

// TestCbrPskParseXlsx — XLSX-эра (2023-11 …): лист «AVG», 20 листьев новой таксономии (1–8),
// период 2026Q2, точка-десятичный разделитель. Секции 2, 2.1, 6, 6.1 и т.п. точками не являются:
// у них код есть, а значений нет.
func TestCbrPskParseXlsx(t *testing.T) {
	period, points, err := cbrPskParseXlsx(pskTestFixture(t, pskXlsxFixture))
	if err != nil {
		t.Fatalf("cbrPskParseXlsx: %v", err)
	}
	if want := time.Date(2026, time.June, 30, 0, 0, 0, 0, time.UTC); !period.Equal(want) {
		t.Errorf("период = %s, ожидался конец квартала %s", period.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	wantCodes := []string{
		"1.1", "1.2", "2.1.1", "2.1.2", "2.2.1", "2.2.2", "3", "4.1", "4.2",
		"5.1", "5.2", "6.1.1", "6.1.2", "6.2", "7.1.1", "7.1.2", "7.2", "8.1", "8.2", "8.3",
	}
	if got, want := strings.Join(pskCodes(points), ","), strings.Join(wantCodes, ","); got != want {
		t.Fatalf("коды = %s, ожидались %s", got, want)
	}
	for _, section := range []string{"2", "2.1", "2.2", "6", "6.1", "7", "7.1", "8"} {
		for _, point := range points {
			if point.Code == section {
				t.Errorf("секция %q попала в точки: у неё нет значений", section)
			}
		}
	}
	for _, spot := range []struct {
		code     string
		avg, max float64
	}{
		{"6.1.1", 17.779, 23.705}, // ипотека по ПСК: приобретение жилья в МКД, ДДУ
		{"1.1", 19.508, 26.011},   // автокредит: пробег 0–1000 км
		{"8.3", 23.975, 31.967},   // иные с залогом: автотранспортное средство
	} {
		point := pskSpot(t, points, spot.code)
		pskCheckValue(t, "psk_avg "+spot.code, point.Avg, spot.avg)
		pskCheckMax(t, "psk_max "+spot.code, point.Max, pskFloatPtr(spot.max))
	}
}

// TestCbrPskParseXls — XLS-эра (2020-08 … 2023-08): BIFF-книга, лист «AVG», 20 листьев таксономии
// 1–5 эпохи (включая 2.4 и плоские 2.x/3.x/4.x), период 2020Q2, значения — числовые ячейки.
func TestCbrPskParseXls(t *testing.T) {
	period, points, err := cbrPskParseXls(pskTestFixture(t, pskXlsFixture))
	if err != nil {
		t.Fatalf("cbrPskParseXls: %v", err)
	}
	if want := time.Date(2020, time.June, 30, 0, 0, 0, 0, time.UTC); !period.Equal(want) {
		t.Errorf("период = %s, ожидался конец квартала %s", period.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	wantCodes := []string{
		"1.1", "1.2", "2.1", "2.2", "2.3",
		"3.1.1", "3.1.2", "3.1.3", "3.2.1", "3.2.2", "3.2.3",
		"4.1.1", "4.1.2", "4.1.3", "4.1.4", "4.2.1", "4.2.2", "4.2.3", "4.2.4", "5",
	}
	if got, want := strings.Join(pskCodes(points), ","), strings.Join(wantCodes, ","); got != want {
		t.Fatalf("коды = %s, ожидались %s", got, want)
	}
	for _, section := range []string{"1", "2", "3", "3.1", "3.2", "4", "4.1", "4.2"} {
		for _, point := range points {
			if point.Code == section {
				t.Errorf("секция %q попала в точки: у неё нет значений", section)
			}
		}
	}
	first := pskSpot(t, points, "1.1")
	pskCheckValue(t, "psk_avg 1.1", first.Avg, 12.655)
	pskCheckMax(t, "psk_max 1.1", first.Max, pskFloatPtr(16.873))
	last := pskSpot(t, points, "5")
	pskCheckValue(t, "psk_avg 5", last.Avg, 12.285)
	pskCheckMax(t, "psk_max 5", last.Max, pskFloatPtr(16.38))
}

// TestCbrPskParsePDF — PDF-эра (2014-11 … 2020-05): таблица из `pdftotext -layout`, 20 листьев
// таксономии 1–4 эпохи (ипотечных категорий нет), период 2018Q3, значения — десятичная запятая.
// Извлечение требует внешней утилиты poppler-utils; без неё подтест пропускается (как у
// cbr_mt_forecast: разбор проверяется по тексту, а извлечение — живой приёмкой). Сам разбор
// текста проверяется отдельно и офлайн.
func TestCbrPskParsePDF(t *testing.T) {
	if _, err := exec.LookPath(cbrPskPDFTool); err != nil {
		t.Skipf("pdftotext не найден (%v): разбор PDF-текста проверяется на фикстуре текста, извлечение — живой приёмкой", err)
	}
	body := pskTestFixture(t, pskPDFFixture)
	text, err := cbrPskPDFText(context.Background(), body)
	if err != nil {
		t.Fatalf("cbrPskPDFText: %v", err)
	}
	period, points, err := cbrPskParsePDFText(text)
	if err != nil {
		t.Fatalf("cbrPskParsePDFText: %v", err)
	}
	if want := time.Date(2018, time.September, 30, 0, 0, 0, 0, time.UTC); !period.Equal(want) {
		t.Errorf("период = %s, ожидался конец квартала %s", period.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	wantCodes := []string{
		"1.1", "1.2", "2.1", "2.2", "2.3", "2.4",
		"3.1.1", "3.1.2", "3.1.3", "3.2.1", "3.2.2", "3.2.3",
		"4.1.1", "4.1.2", "4.1.3", "4.1.4", "4.2.1", "4.2.2", "4.2.3", "4.2.4",
	}
	if got, want := strings.Join(pskCodes(points), ","), strings.Join(wantCodes, ","); got != want {
		t.Fatalf("коды = %s, ожидались %s", got, want)
	}
	for _, spot := range []struct {
		code     string
		avg, max float64
	}{
		{"1.1", 12.673, 16.897},
		{"3.1.1", 17.797, 23.729},
		{"4.2.4", 14.652, 19.536},
	} {
		point := pskSpot(t, points, spot.code)
		pskCheckValue(t, "psk_avg "+spot.code, point.Avg, spot.avg)
		pskCheckMax(t, "psk_max "+spot.code, point.Max, pskFloatPtr(spot.max))
	}
	// Многострочная категория 5 в 2018-м не печатается (таксономия 1–4), а ипотечных 6.x нет
	// вовсе: их появление в этой фикстуре означало бы, что разбор читает чужой выпуск.
	for _, absent := range []string{"5", "6.1.1"} {
		for _, point := range points {
			if point.Code == absent {
				t.Errorf("категория %q не должна встречаться в выпуске 2018 года", absent)
			}
		}
	}
}

// TestCbrPskParsePDF2014AvgOnly — ранний PDF (14112014_ko.pdf): таблица печатает только
// среднерыночное значение, предельного ещё нет. Парсер обязан взять одно число как psk_avg, а
// psk_max оставить NULL (а не 0 и не упасть); это и есть источник Nullable(Float64).
func TestCbrPskParsePDF2014AvgOnly(t *testing.T) {
	if _, err := exec.LookPath(cbrPskPDFTool); err != nil {
		t.Skipf("pdftotext не найден (%v): разбор PDF проверяется живой приёмкой", err)
	}
	text, err := cbrPskPDFText(context.Background(), pskTestFixture(t, pskPDF2014Fixture))
	if err != nil {
		t.Fatalf("cbrPskPDFText: %v", err)
	}
	period, points, err := cbrPskParsePDFText(text)
	if err != nil {
		t.Fatalf("cbrPskParsePDFText: %v", err)
	}
	if want := time.Date(2014, time.September, 30, 0, 0, 0, 0, time.UTC); !period.Equal(want) {
		t.Errorf("период = %s, ожидался %s", period.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	wantCodes := []string{
		"1.1", "1.2", "2.1", "2.2", "2.3", "2.4",
		"3.1.1", "3.1.2", "3.1.3", "3.2.1", "3.2.2", "3.2.3",
		"4.1.1", "4.1.2", "4.1.3", "4.1.4", "4.2.1", "4.2.2", "4.2.3", "4.2.4",
	}
	if got, want := strings.Join(pskCodes(points), ","), strings.Join(wantCodes, ","); got != want {
		t.Fatalf("коды = %s, ожидались %s", got, want)
	}
	for _, point := range points {
		if point.Max != nil {
			t.Errorf("%s: psk_max = %v, ожидался NULL (в выпуске 2014 предельного значения не было)", point.Code, *point.Max)
		}
	}
	first := pskSpot(t, points, "1.1")
	pskCheckValue(t, "psk_avg 1.1", first.Avg, 15.240)
	pskCheckMax(t, "psk_max 1.1", first.Max, nil)
	last := pskSpot(t, points, "4.2.4")
	pskCheckValue(t, "psk_avg 4.2.4", last.Avg, 19.523)
}

// TestCbrPskParsePDFBrokenText — PDF с повреждённым текстовым слоем (13022015_ko.pdf): pdftotext
// возвращает текст, но буквы и цифры в нём потеряны. Разбор обязан вернуть сентинел
// errCbrPskUnreadablePDF, по которому Import пропускает выпуск и сохраняет версию с 0 точек.
func TestCbrPskParsePDFBrokenText(t *testing.T) {
	if _, err := exec.LookPath(cbrPskPDFTool); err != nil {
		t.Skipf("pdftotext не найден (%v): сентинел проверяется синтетическим текстом", err)
	}
	body := pskTestFixture(t, pskPDFBrokenFixture)
	text, err := cbrPskPDFText(context.Background(), body)
	if err != nil {
		t.Fatalf("cbrPskPDFText: %v", err)
	}
	if _, _, err = cbrPskParsePDFText(text); !errors.Is(err, errCbrPskUnreadablePDF) {
		t.Fatalf("cbrPskParsePDFText = %v, ожидался сентинел errCbrPskUnreadablePDF", err)
	}
	release := cbrPskRelease{ID: "10400", FileName: "13022015_ko.pdf", Ext: "pdf"}
	if _, _, err = cbrPskParseRelease(context.Background(), body, release); !errors.Is(err, errCbrPskUnreadablePDF) {
		t.Errorf("cbrPskParseRelease = %v, ожидался сентинел errCbrPskUnreadablePDF", err)
	}
}

// TestCbrPskParseXls2022ContentSheet — XLS-выпуск 14022022: лист «AVG» — шаблон с одними
// названиями категорий, данные в «Лист2» (период 2021Q4), в числовых колонках секций стоят X.
// Лист данных обязан выбираться по содержимому (период + числа), X — «данных нет», не ошибка.
func TestCbrPskParseXls2022ContentSheet(t *testing.T) {
	period, points, err := cbrPskParseXls(pskTestFixture(t, pskXls2022Fixture))
	if err != nil {
		t.Fatalf("cbrPskParseXls: %v", err)
	}
	if want := time.Date(2021, time.December, 31, 0, 0, 0, 0, time.UTC); !period.Equal(want) {
		t.Errorf("период = %s, ожидался %s", period.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	wantCodes := []string{
		"1.1", "1.2", "2.1", "2.2", "2.3",
		"3.1.1", "3.1.2", "3.1.3", "3.2.1", "3.2.2", "3.2.3",
		"4.1.1", "4.1.2", "4.1.3", "4.1.4", "4.2.1", "4.2.2", "4.2.3", "4.2.4", "5",
	}
	if got, want := strings.Join(pskCodes(points), ","), strings.Join(wantCodes, ","); got != want {
		t.Fatalf("коды = %s, ожидались %s", got, want)
	}
	for _, section := range []string{"1", "2", "3", "3.1", "3.2", "4", "4.1", "4.2"} {
		for _, point := range points {
			if point.Code == section {
				t.Errorf("секция %q (X в числовых колонках) попала в точки", section)
			}
		}
	}
	first := pskSpot(t, points, "1.1")
	pskCheckValue(t, "psk_avg 1.1", first.Avg, 13.724)
	pskCheckMax(t, "psk_max 1.1", first.Max, pskFloatPtr(18.299))
	last := pskSpot(t, points, "5")
	pskCheckValue(t, "psk_avg 5", last.Avg, 12.574)
	pskCheckMax(t, "psk_max 5", last.Max, pskFloatPtr(16.765))
}

// TestCbrPskParseXls2023CleanCodes — XLS-выпуск 14022023: лист называется «Лист1» (не AVG), коды
// категорий приходят с хвостом NUL-байтов. Лист обязан выбраться по содержимому, коды — очиститься
// до «1.1»/«5»/«6», иначе точки потерялись бы или получили коды с управляющими символами.
func TestCbrPskParseXls2023CleanCodes(t *testing.T) {
	period, points, err := cbrPskParseXls(pskTestFixture(t, pskXls2023Fixture))
	if err != nil {
		t.Fatalf("cbrPskParseXls: %v", err)
	}
	if want := time.Date(2022, time.December, 31, 0, 0, 0, 0, time.UTC); !period.Equal(want) {
		t.Errorf("период = %s, ожидался %s", period.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	wantCodes := []string{
		"1.1", "1.2", "2.1", "2.2", "2.3",
		"3.1.2", "3.1.3", "3.2",
		"4.1.1", "4.1.2", "4.1.3", "4.1.4", "4.2.1", "4.2.2", "4.2.3", "4.2.4",
		"5", "6", "7", "8",
	}
	if got, want := strings.Join(pskCodes(points), ","), strings.Join(wantCodes, ","); got != want {
		t.Fatalf("коды = %s, ожидались %s", got, want)
	}
	for _, point := range points {
		if strings.ContainsRune(point.Code, 0) || strings.ContainsRune(point.Category, 0) {
			t.Errorf("в точке %q/%q остался NUL-байт: ячейки xls не очищены", point.Code, point.Category)
		}
		if !cbrPskCodeRe.MatchString(point.Code) {
			t.Errorf("код %q не соответствует шаблону категории после очистки", point.Code)
		}
	}
	first := pskSpot(t, points, "1.1")
	pskCheckValue(t, "psk_avg 1.1", first.Avg, 12.369)
	pskCheckMax(t, "psk_max 1.1", first.Max, pskFloatPtr(16.492))
	flat := pskSpot(t, points, "3.2")
	pskCheckValue(t, "psk_avg 3.2", flat.Avg, 16.035)
	pskCheckMax(t, "psk_max 3.2", flat.Max, pskFloatPtr(21.38))
	mortgage := pskSpot(t, points, "6")
	pskCheckValue(t, "psk_avg 6", mortgage.Avg, 11.255)
	pskCheckMax(t, "psk_max 6", mortgage.Max, pskFloatPtr(15.007))
}

// TestCbrPskSkipUnreadableSavesVersion — механика пропуска нечитаемого PDF: версия выпуска
// сохраняется в source_publications с 0 точек, а второй прогон по гейту её уже не скачивает
// (ключ — имя файла, fingerprint пустой допустим: cbrPskKnownReleases читает DISTINCT name).
func TestCbrPskSkipUnreadableSavesVersion(t *testing.T) {
	release := cbrPskRelease{
		ID:       "10400",
		FileName: "13022015_ko.pdf",
		Ext:      "pdf",
		URL:      cbrUrl + "/Collection/Collection/File/10400/13022015_ko.pdf",
	}
	batch := &pskFakeBatch{}
	conn := &pskFakeConn{batch: batch}
	if err := cbrPskSkipUnreadable(context.Background(), conn, release); err != nil {
		t.Fatalf("cbrPskSkipUnreadable: %v", err)
	}
	if len(batch.rows) != 1 {
		t.Fatalf("в source_publications записано %d строк, ожидалась 1", len(batch.rows))
	}
	row := batch.rows[0]
	if len(row) != 6 {
		t.Fatalf("строка версии из %d колонок, ожидалось 6", len(row))
	}
	if row[0] != cbrPskTable || row[1] != release.FileName || row[2] != "" {
		t.Errorf("source/name/fingerprint = %v/%v/%v, ожидались %q/%q/пусто",
			row[0], row[1], row[2], cbrPskTable, release.FileName)
	}
	if points, ok := row[4].(uint64); !ok || points != 0 {
		t.Errorf("points = %v (%T), ожидался uint64(0)", row[4], row[4])
	}
	if row[5] != release.URL {
		t.Errorf("url = %v, ожидался %q", row[5], release.URL)
	}

	// Второй прогон: гейт видит сохранённое имя и не ставит выпуск в план скачивания.
	conn.rows = &pskFakeRows{values: [][]any{{release.FileName}}}
	known, err := cbrPskKnownReleases(context.Background(), conn)
	if err != nil {
		t.Fatalf("cbrPskKnownReleases: %v", err)
	}
	if fresh := cbrPskNewReleases([]cbrPskRelease{release}, known); len(fresh) != 0 {
		t.Errorf("второй прогон поставил в план %d выпусков, ожидалось 0 (версия с 0 точек известна)", len(fresh))
	}
}

// TestCbrPskAppendBodyColumns — BodyFunc пишет строку в порядке колонок DDL: date,
// category_code, category, psk_avg, psk_max, source_file. Перестановка соседних строковых колонок
// (category_code ↔ category) прошла бы парсер, но записала бы код в наименование — этот контракт
// и пиннится.
func TestCbrPskAppendBodyColumns(t *testing.T) {
	body := pskTestFixture(t, pskXlsxFixture)
	release := cbrPskRelease{
		ID:        "62266",
		FileName:  "14082026_ko.xlsx",
		Ext:       "xlsx",
		Published: time.Date(2026, time.August, 14, 0, 0, 0, 0, time.UTC),
		URL:       cbrUrl + "/Collection/Collection/File/62266/14082026_ko.xlsx",
	}
	recorder := newCbrShapeRecorder("date", "category_code", "category", "psk_avg", "psk_max", "source_file")
	if err := cbrPskAppendBody(context.Background(), release)(body, recorder); err != nil {
		t.Fatalf("cbrPskAppendBody: %v", err)
	}
	if recorder.err != nil {
		t.Fatalf("форма строки: %v", recorder.err)
	}
	if len(recorder.raw) != 20 {
		t.Fatalf("записано строк %d, ожидалось 20", len(recorder.raw))
	}
	first := recorder.raw[0]
	date, ok := first[0].(time.Time)
	if !ok {
		t.Fatalf("колонка date = %T, ожидался time.Time", first[0])
	}
	if want := time.Date(2026, time.June, 30, 0, 0, 0, 0, time.UTC); !date.Equal(want) {
		t.Errorf("date = %s, ожидался конец периода %s", date.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	if first[1] != "1.1" {
		t.Errorf("category_code = %v, ожидался 1.1", first[1])
	}
	if first[2] != "автомобили с пробегом от 0 до 1000 км" {
		t.Errorf("category = %v, ожидалось наименование категории 1.1", first[2])
	}
	if avg, ok := first[3].(float64); !ok || math.Abs(avg-19.508) > 1e-9 {
		t.Errorf("psk_avg = %v (%T), ожидалось 19.508", first[3], first[3])
	}
	if maxValue, ok := first[4].(float64); !ok || math.Abs(maxValue-26.011) > 1e-9 {
		t.Errorf("psk_max = %v (%T), ожидалось 26.011", first[4], first[4])
	}
	if first[5] != release.FileName {
		t.Errorf("source_file = %v, ожидалось %q", first[5], release.FileName)
	}
}

// TestCbrPskAppendBodyNullableMax — ранний PDF пишется в batch с psk_max = nil, а не 0: колонка
// Nullable(Float64), и NULL значит «в выпуске предельного значения не было». Без cbrPskNullable
// драйвер получил бы *float64 и отверг вставку.
func TestCbrPskAppendBodyNullableMax(t *testing.T) {
	if _, err := exec.LookPath(cbrPskPDFTool); err != nil {
		t.Skipf("pdftotext не найден (%v): nullable-запись проверяется живой приёмкой", err)
	}
	body := pskTestFixture(t, pskPDF2014Fixture)
	release := cbrPskRelease{
		ID:       "10399",
		FileName: "14112014_ko.pdf",
		Ext:      "pdf",
		URL:      cbrUrl + "/Collection/Collection/File/9003/14112014_ko.pdf",
	}
	recorder := newCbrShapeRecorder("date", "category_code", "category", "psk_avg", "psk_max", "source_file")
	if err := cbrPskAppendBody(context.Background(), release)(body, recorder); err != nil {
		t.Fatalf("cbrPskAppendBody: %v", err)
	}
	if recorder.err != nil {
		t.Fatalf("форма строки: %v", recorder.err)
	}
	if len(recorder.raw) != 20 {
		t.Fatalf("записано строк %d, ожидалось 20", len(recorder.raw))
	}
	first := recorder.raw[0]
	if first[1] != "1.1" {
		t.Errorf("category_code = %v, ожидался 1.1", first[1])
	}
	if avg, ok := first[3].(float64); !ok || math.Abs(avg-15.240) > 1e-9 {
		t.Errorf("psk_avg = %v (%T), ожидалось 15.240", first[3], first[3])
	}
	if first[4] != nil {
		t.Errorf("psk_max = %v (%T), ожидался nil (NULL)", first[4], first[4])
	}
	if first[5] != release.FileName {
		t.Errorf("source_file = %v, ожидалось %q", first[5], release.FileName)
	}
}

// TestCbrPskPeriodEnd — заголовок периода трёх эр: XLSX-эра печатает день в «», XLS и PDF — без
// кавычек; месяц — в родительном падеже. Неразбирающийся заголовок — ошибка, а не fallback по дате
// файла: сдвиг на квартал записал бы точку под чужим периодом (спека §3.4).
func TestCbrPskPeriodEnd(t *testing.T) {
	cases := map[string]time.Time{
		"за период с «01» апреля 2026 г. по «30» июня 2026 г.":          time.Date(2026, time.June, 30, 0, 0, 0, 0, time.UTC),
		"за период с 1 апреля по 30 июня 2020 г.":                       time.Date(2020, time.June, 30, 0, 0, 0, 0, time.UTC),
		"                    за период с 1 июля по 30 сентября 2018 г.": time.Date(2018, time.September, 30, 0, 0, 0, 0, time.UTC),
		"за период с 1 октября по 31 декабря 2019 г.":                   time.Date(2019, time.December, 31, 0, 0, 0, 0, time.UTC),
		"за период с 1 января по 31 марта 2015 г.":                      time.Date(2015, time.March, 31, 0, 0, 0, 0, time.UTC),
	}
	for header, want := range cases {
		date, err := cbrPskPeriodEnd(header)
		if err != nil {
			t.Errorf("cbrPskPeriodEnd(%q): %v", header, err)
			continue
		}
		if !date.Equal(want) {
			t.Errorf("cbrPskPeriodEnd(%q) = %s, ожидался %s", header, date.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
		}
	}
	for _, broken := range []string{
		"без заголовка периода",
		"за период с 1 июля по 31 сен 2018 г.",  // сокращённый месяц
		"за период с 1 июля по 31 июня 2018 г.", // несуществующая дата
		"за период с 1 июля по 30 сентября 1899 г.",
	} {
		if date, err := cbrPskPeriodEnd(broken); err == nil {
			t.Errorf("cbrPskPeriodEnd(%q) = %s, ожидалась ошибка", broken, date.Format(cbrPskDayLayout))
		}
	}
}

// TestCbrPskParsePDFText — разбор текста PDF офлайн (без poppler): синтетический текст с
// многострочной секцией, секцией с кодом без чисел и сноской, после которой таблица кончается.
// Две величины — среднерыночное и предельное; одна — только среднерыночное (PDF-эра 2014:
// предельного ещё нет, psk_max = NULL). Повреждённый текстовый слой распознаётся сентинелом.
func TestCbrPskParsePDFText(t *testing.T) {
	text := []byte(` СРЕДНЕРЫНОЧНЫЕ ЗНАЧЕНИЯ ПОЛНОЙ СТОИМОСТИ ПОТРЕБИТЕЛЬСКИХ КРЕДИТОВ
                      за период с 1 июля по 30 сентября 2018 г.

 1          Потребительские кредиты на цели приобретения автотранспортных средств
            с залогом автотранспортного средства
 1.1         автомобили с пробегом от 0 до 1000 км                                      12,673      16,897
 2          Потребительские кредиты с лимитом кредитования
 2.1         до 30 тыс. руб.                                                             20,263      27,017
*Среднерыночные значения полной стоимости потребительских кредитов (займов) (ПСК) рассчитаны Банком России
`)
	period, points, err := cbrPskParsePDFText(text)
	if err != nil {
		t.Fatalf("cbrPskParsePDFText: %v", err)
	}
	if want := time.Date(2018, time.September, 30, 0, 0, 0, 0, time.UTC); !period.Equal(want) {
		t.Errorf("период = %s, ожидался %s", period.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	if got, want := strings.Join(pskCodes(points), ","), "1.1,2.1"; got != want {
		t.Fatalf("коды = %s, ожидались %s", got, want)
	}
	pskCheckValue(t, "psk_avg 1.1", points[0].Avg, 12.673)
	pskCheckMax(t, "psk_max 1.1", points[0].Max, pskFloatPtr(16.897))

	// Одно значение — законный вид раннего PDF: avg есть, max NULL, а не ошибка.
	oneValue := bytes.ReplaceAll(text, []byte("12,673      16,897"), []byte("12,673"))
	period, points, err = cbrPskParsePDFText(oneValue)
	if err != nil {
		t.Fatalf("cbrPskParsePDFText (одно значение): %v", err)
	}
	if want := time.Date(2018, time.September, 30, 0, 0, 0, 0, time.UTC); !period.Equal(want) {
		t.Errorf("период = %s, ожидался %s", period.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	point := pskSpot(t, points, "1.1")
	pskCheckValue(t, "psk_avg 1.1 (одно значение)", point.Avg, 12.673)
	pskCheckMax(t, "psk_max 1.1 (одно значение)", point.Max, nil)

	// Нечитаемый текстовый слой: межбуквенный **, управляющий байт на месте цифры и таблица без
	// единой точки (валидный PDF с периодом, но без данных — тоже нечитаемый слой).
	for name, broken := range map[string][]byte{
		"межбуквенный **":  bytes.ReplaceAll(text, []byte("12,673      16,897"), []byte("1**5**")),
		"управляющий байт": bytes.ReplaceAll(text, []byte("12,673"), []byte("1\x19,\x16")),
		"ноль точек":       []byte("за период с 1 июля по 30 сентября 2018 г.\n*Среднерыночные значения ПСК\n"),
	} {
		if _, _, err = cbrPskParsePDFText(broken); !errors.Is(err, errCbrPskUnreadablePDF) {
			t.Errorf("%s: ошибка = %v, ожидался сентинел errCbrPskUnreadablePDF", name, err)
		}
	}
}

// TestCbrPskParseXlsxFailFast — смена структуры выпуска валит прогон, а не даёт «0 rows»:
// нет листа «AVG», неразбирающийся заголовок периода, пустой лист (ни одной точки), предельное
// значение без среднерыночного. Среднерыночное без предельного — не ошибка (psk_max = NULL).
// Книги собираются в памяти: фикстуры не мутируются.
func TestCbrPskParseXlsxFailFast(t *testing.T) {
	validPeriod := []any{"за период с 1 апреля по 30 июня 2020 г."}
	header := []any{"Номер строки", "Категории потребительских кредитов (займов)", "Среднерыночные значения", "Предельные значения"}
	numbers := []any{1, 2, 3, 4}
	for name, tc := range map[string]struct {
		sheet string
		rows  [][]any
		want  string
	}{
		"нет листа AVG": {
			sheet: "Данные",
			rows:  [][]any{validPeriod},
			want:  "AVG",
		},
		"неразбирающийся период": {
			sheet: cbrPskSheet,
			rows: [][]any{
				{"СРЕДНЕРЫНОЧНЫЕ ЗНАЧЕНИЯ"},
				{"за период с 1 июля по 31 сен 2018 г."},
				header, numbers,
				{"1.1", "автомобили", "12.5", "16.8"},
			},
			want: "неизвестный месяц",
		},
		"ни одной точки": {
			sheet: cbrPskSheet,
			rows: [][]any{
				{"СРЕДНЕРЫНОЧНЫЕ ЗНАЧЕНИЯ"},
				validPeriod,
				header, numbers,
				{"1", "категория без значений"},
			},
			want: "ни одной точки",
		},
		"предельное без среднерыночного": {
			sheet: cbrPskSheet,
			rows: [][]any{
				{"СРЕДНЕРЫНОЧНЫЕ ЗНАЧЕНИЯ"},
				validPeriod,
				header, numbers,
				{"1.1", "автомобили", "", "16.8"},
			},
			want: "предельное значение без среднерыночного",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := cbrPskParseXlsx(pskBuildXlsx(t, tc.sheet, tc.rows))
			if err == nil {
				t.Fatalf("ожидалась ошибка (%s)", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ошибка %q не содержит %q", err, tc.want)
			}
		})
	}
	// Не-PDF байты отвергаются до запуска утилиты: WAF отдаёт HTML с кодом 200.
	if _, err := cbrPskPDFText(context.Background(), []byte("<html>заглушка</html>")); err == nil || !strings.Contains(err.Error(), "не PDF") {
		t.Errorf("cbrPskPDFText(HTML) = %v, ожидалась ошибка «не PDF»", err)
	}
}

// pskBuildXlsx собирает книгу в памяти: лист с заданным именем и строками. Нужна только веткам
// fail-fast — живые выпуски читаются из фикстур.
func pskBuildXlsx(t *testing.T, sheet string, rows [][]any) []byte {
	t.Helper()
	xlsx := excelize.NewFile()
	defer func() { _ = xlsx.Close() }()
	if sheet != xlsx.GetSheetName(0) {
		if err := xlsx.SetSheetName(xlsx.GetSheetName(0), sheet); err != nil {
			t.Fatalf("переименование листа в %q: %v", sheet, err)
		}
	}
	for i, row := range rows {
		cell, err := excelize.CoordinatesToCellName(1, i+1)
		if err != nil {
			t.Fatalf("адрес строки %d: %v", i+1, err)
		}
		if err = xlsx.SetSheetRow(sheet, cell, &row); err != nil {
			t.Fatalf("строка %d листа %q: %v", i+1, sheet, err)
		}
	}
	var buf bytes.Buffer
	if err := xlsx.Write(&buf); err != nil {
		t.Fatalf("запись книги в память: %v", err)
	}
	return buf.Bytes()
}

// TestCbrPskParseReleases — лента страницы ПСК: 48 выпусков ko без разрывов (23 PDF, 13 XLS,
// 12 XLSX), типы mfo/kpk/skpk/lomb не попадают, дубли версионных списков схлопываются по <id>,
// порядок — хронологический. Фикстура живая (снимок 2026-11-03).
func TestCbrPskParseReleases(t *testing.T) {
	releases, err := cbrPskParseReleases(pskTestFixture(t, pskPageFixture))
	if err != nil {
		t.Fatalf("cbrPskParseReleases: %v", err)
	}
	if len(releases) != 48 {
		t.Fatalf("выпусков %d, ожидалось 48 (23 PDF + 13 XLS + 12 XLSX)", len(releases))
	}
	extCounts := map[string]int{}
	ids := make(map[string]bool, len(releases))
	for _, release := range releases {
		extCounts[release.Ext]++
		if ids[release.ID] {
			t.Errorf("id %s встречается дважды: дедупликация по <id> не работает", release.ID)
		}
		ids[release.ID] = true
		if want := release.Published.Format(cbrPskReleaseLayout) + "_ko." + release.Ext; release.FileName != want {
			t.Errorf("имя файла %q не совпадает с датой публикации %s и расширением %s",
				release.FileName, release.Published.Format(cbrPskDayLayout), release.Ext)
		}
		if !strings.HasPrefix(release.URL, cbrUrl+"/Collection/Collection/File/") {
			t.Errorf("url %q не собран из домена и пути ленты", release.URL)
		}
		// Ключ гейта — имя файла, а шаблон пишет версию по basename URL: расхождение сломало бы
		// no-op (каждый прогон снова считал бы выпуск новым и качал его).
		if base := path.Base(release.URL); base != release.FileName {
			t.Errorf("basename URL %q не совпадает с ключом гейта %q", base, release.FileName)
		}
		if strings.Contains(release.FileName, "_mfo.") || strings.Contains(release.FileName, "_kpk.") ||
			strings.Contains(release.FileName, "_skpk.") || strings.Contains(release.FileName, "_lomb.") {
			t.Errorf("в корпус ko попал выпуск другого типа: %s", release.FileName)
		}
	}
	if got, want := fmt.Sprintf("%d/%d/%d", extCounts["pdf"], extCounts["xls"], extCounts["xlsx"]), "23/13/12"; got != want {
		t.Errorf("PDF/XLS/XLSX = %s, ожидалось %s", got, want)
	}
	if want := time.Date(2014, time.November, 14, 0, 0, 0, 0, time.UTC); !releases[0].Published.Equal(want) {
		t.Errorf("первый выпуск %s, ожидался %s", releases[0].Published.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
	if want := time.Date(2026, time.August, 14, 0, 0, 0, 0, time.UTC); !releases[len(releases)-1].Published.Equal(want) {
		t.Errorf("последний выпуск %s, ожидался %s", releases[len(releases)-1].Published.Format(cbrPskDayLayout), want.Format(cbrPskDayLayout))
	}
}

// TestCbrPskParseReleasesEdges — дедупликация по id, фильтр типа, конфликт id и дубль имени файла:
// один id с разными файлами или два id с одним именем — ошибка, а не выбор одного из двух (лента
// перестала быть набором выпусков, а ключ версии — уникальным).
func TestCbrPskParseReleasesEdges(t *testing.T) {
	duplicate := []byte(`<a href="/Collection/Collection/File/1/01012020_ko.xlsx">a</a>
<a href="/Collection/Collection/File/1/01012020_ko.xlsx">b</a>
<a href="/Collection/Collection/File/2/01012020_mfo.xlsx">c</a>
<a href="/Collection/Collection/File/3/01012020_kpk.xlsx">d</a>`)
	releases, err := cbrPskParseReleases(duplicate)
	if err != nil {
		t.Fatalf("cbrPskParseReleases: %v", err)
	}
	if len(releases) != 1 || releases[0].ID != "1" {
		t.Fatalf("выпуски = %+v, ожидался один ko-выпуск id 1: дубли схлопываются, mfo/kpk отбрасываются", releases)
	}

	if _, err := cbrPskParseReleases([]byte(`<html>нет ссылок</html>`)); err == nil {
		t.Error("пустая лента разобрана без ошибки: живой ЦБ без выпусков — смена источника, а не «новых выпусков нет»")
	}
	conflict := []byte(`<a href="/Collection/Collection/File/1/01012020_ko.xlsx">a</a>
<a href="/Collection/Collection/File/1/02012020_ko.xlsx">b</a>`)
	if _, err := cbrPskParseReleases(conflict); err == nil {
		t.Error("один id с разными файлами разобран без ошибки: лента перестала быть набором выпусков")
	}
	sameName := []byte(`<a href="/Collection/Collection/File/1/01012020_ko.xlsx">a</a>
<a href="/Collection/Collection/File/2/01012020_ko.xlsx">b</a>`)
	if _, err := cbrPskParseReleases(sameName); err == nil {
		t.Error("два id с одним именем файла разобраны без ошибки: ключ версии перестал быть уникальным")
	}
}

// TestCbrPskNewReleases — гейт прогона: из ленты остаются выпуски, которых нет в
// source_publications (ключ версии — имя файла). Цикл скачивания в Import идёт только по
// результату этой функции, поэтому известный выпуск не скачивается вовсе; сценарий «все
// известны» — no-op прогона. Порядок — хронологический (старые → новые).
func TestCbrPskNewReleases(t *testing.T) {
	releases, err := cbrPskParseReleases(pskTestFixture(t, pskPageFixture))
	if err != nil {
		t.Fatalf("cbrPskParseReleases: %v", err)
	}
	if fresh := cbrPskNewReleases(releases, map[string]bool{}); len(fresh) != len(releases) {
		t.Errorf("на чистом реестре новых выпусков %d, ожидалось %d", len(fresh), len(releases))
	}
	known := make(map[string]bool, len(releases))
	for _, release := range releases {
		known[release.FileName] = true
	}
	if fresh := cbrPskNewReleases(releases, known); len(fresh) != 0 {
		t.Errorf("при полном реестре новых выпусков %d, ожидалось 0 (no-op)", len(fresh))
	}
	known = map[string]bool{releases[0].FileName: true, releases[1].FileName: true}
	fresh := cbrPskNewReleases(releases, known)
	if len(fresh) != len(releases)-2 {
		t.Fatalf("новых выпусков %d, ожидалось %d", len(fresh), len(releases)-2)
	}
	if fresh[0].FileName != releases[2].FileName {
		t.Errorf("первый новый выпуск %q, ожидался %q: известные не должны попадать в план скачивания",
			fresh[0].FileName, releases[2].FileName)
	}
}

// TestCbrPskKnownReleases — гейт читает source_publications запросом с FINAL и DISTINCT: версии
// одного выпуска (файл перепубликовался) не должны раздувать множество, а WHERE source отделяет
// реестр cbr_psk от версий остальных импортёров.
func TestCbrPskKnownReleases(t *testing.T) {
	rows := &pskFakeRows{values: [][]any{{"14082026_ko.xlsx"}, {"15052026_ko.xlsx"}}}
	conn := &pskFakeConn{rows: rows}
	known, err := cbrPskKnownReleases(context.Background(), conn)
	if err != nil {
		t.Fatalf("cbrPskKnownReleases: %v", err)
	}
	if len(conn.queries) != 1 || conn.queries[0] != "SELECT DISTINCT name FROM source_publications FINAL WHERE source = 'cbr_psk'" {
		t.Errorf("запросы гейта = %v, ожидался один SELECT DISTINCT name FROM source_publications FINAL WHERE source = 'cbr_psk'", conn.queries)
	}
	if len(known) != 2 || !known["14082026_ko.xlsx"] || !known["15052026_ko.xlsx"] {
		t.Errorf("известные выпуски = %v, ожидались два имени файла", known)
	}
	if !rows.closed {
		t.Error("строки гейта не закрыты: соединение течёт на каждом прогоне")
	}

	queryErr := errors.New("фейковое соединение: запрос не выполнен")
	for name, tc := range map[string]struct {
		conn *pskFakeConn
		want error
	}{
		"ошибка Query": {conn: &pskFakeConn{queryErr: queryErr}, want: queryErr},
		"ошибка Scan":  {conn: &pskFakeConn{rows: &pskFakeRows{values: [][]any{{"x"}}, scanErr: queryErr}}, want: queryErr},
		"ошибка Err":   {conn: &pskFakeConn{rows: &pskFakeRows{values: [][]any{{"x"}}, err: queryErr}}, want: queryErr},
	} {
		t.Run(name, func(t *testing.T) {
			known, err := cbrPskKnownReleases(context.Background(), tc.conn)
			if err == nil {
				t.Fatalf("ожидалась ошибка, множество известных: %v", known)
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("ошибка %v не оборачивает %v: причина сбоя потеряна", err, tc.want)
			}
			if known != nil {
				t.Errorf("при ошибке возвращено множество из %d выпусков: пустое множество пере-импортировало бы корпус", len(known))
			}
		})
	}
}

// TestCbrPskReleaseWritten — проверка «выпуск записан»: число строк по source_file больше нуля —
// nil; ноль строк — ошибка с подсказкой про source_publications. Ключ проверки — имя файла, то же,
// что пишет версия шаблона, поэтому успешный импорт выпуска подтверждается его строками.
func TestCbrPskReleaseWritten(t *testing.T) {
	release := cbrPskRelease{ID: "62266", FileName: "14082026_ko.xlsx"}
	conn := &pskFakeConn{row: &pskFakeRow{count: 20}}
	if err := cbrPskReleaseWritten(context.Background(), conn, release); err != nil {
		t.Fatalf("cbrPskReleaseWritten: %v", err)
	}
	if len(conn.queries) != 1 || conn.queries[0] != "SELECT count() FROM cbr_psk FINAL WHERE source_file = ?" {
		t.Errorf("запрос проверки = %v, ожидался литерал «SELECT count() FROM cbr_psk FINAL WHERE source_file = ?»", conn.queries)
	}

	missing := &pskFakeConn{row: &pskFakeRow{}}
	err := cbrPskReleaseWritten(context.Background(), missing, release)
	if err == nil {
		t.Fatal("ноль строк выпуска принят за успех: следующий прогон считал бы выпуск новым")
	}
	for _, want := range []string{"cbr_psk", release.FileName, "no-op", "source_publications"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
}

// TestCbrPskCatalogMeta — динамический каталог (спека §3.5 C): по каждому коду из таблицы — ряд
// «ПСК <код>: <наименование>» с unit «% годовых», частотой Q и описанием капа по 353-ФЗ. Мета
// динамическая (набор кодов зависит от винтажа), поэтому контракт каталога проверяется здесь, а
// не через checkCbrSeriesMeta: после публикации в фейковую таблицу каталог обязан содержать все
// коды, включая ипотечные 6.1.1/6.1.2.
func TestCbrPskCatalogMeta(t *testing.T) {
	rows := &pskFakeRows{values: [][]any{
		{"1.1", "автомобили с пробегом от 0 до 1000 км"},
		{"6.1.1", "не более 1 года с даты получения разрешения на ввод в эксплуатацию, договор долевого участия"},
		{"6.1.2", "более 1 года с даты получения разрешения на ввод в эксплуатацию"},
		{"8.3", "автотранспортного средства"},
	}}
	conn := &pskFakeConn{rows: rows}
	meta, err := cbrPskCatalogMeta(context.Background(), conn)
	if err != nil {
		t.Fatalf("cbrPskCatalogMeta: %v", err)
	}
	if len(conn.queries) != 1 || conn.queries[0] != "SELECT category_code, argMax(category, date) FROM cbr_psk FINAL GROUP BY category_code ORDER BY category_code" {
		t.Errorf("запрос каталога = %v, ожидался литерал «SELECT category_code, argMax(category, date) FROM cbr_psk FINAL GROUP BY category_code ORDER BY category_code»", conn.queries)
	}
	if len(meta) != len(rows.values) {
		t.Fatalf("рядов каталога %d, ожидалось %d (по одному на код)", len(meta), len(rows.values))
	}
	for i, row := range rows.values {
		code, category := row[0].(string), row[1].(string)
		metaRow := meta[i]
		if want := fmt.Sprintf("ПСК %s: %s", code, category); metaRow.Series != want {
			t.Errorf("series = %q, ожидался %q", metaRow.Series, want)
		}
		if metaRow.Source != cbrSource {
			t.Errorf("source = %q, ожидался %q", metaRow.Source, cbrSource)
		}
		if metaRow.Unit != "% годовых" || metaRow.Frequency != "Q" {
			t.Errorf("%s: unit/frequency = %q/%q, ожидались «%% годовых»/Q", code, metaRow.Unit, metaRow.Frequency)
		}
		if want := "cbr.ru/statistics/bank_sector/psk, тип ko, категория " + code; metaRow.Origin != want {
			t.Errorf("%s: origin = %q, ожидался %q", code, metaRow.Origin, want)
		}
		for _, want := range []string{"353-ФЗ", "одну треть", "следующем календарном квартале", code} {
			if !strings.Contains(metaRow.Description, want) {
				t.Errorf("%s: description не содержит %q: агент не поймёт семантику капа", code, want)
			}
		}
		// Описание — fmt.Sprintf с аргументами: «% годовых» обязано остаться процентом, а не
		// превратиться в «%!г(MISSING)» (символ % в формате экранирован).
		if !strings.Contains(metaRow.Description, "% годовых") || strings.Contains(metaRow.Description, "%!") {
			t.Errorf("%s: description = %q: единица «%% годовых» потеряна при форматировании", code, metaRow.Description)
		}
	}
	if !rows.closed {
		t.Error("строки каталога не закрыты: соединение течёт на каждом прогоне")
	}
}

// TestAfterCbrPskPublishesCatalog — публикация производных (спека §3.5 C): после прогона
// v_cbr_psk пересоздаётся, а каталог получает по ряду на каждый код из таблицы — ипотечные
// категории 6.1.x входят в набор наравне с остальными. Проверяется на фейковом соединении:
// витрина — CreateView с комментариями, каталог — batch v_series_catalog.
func TestAfterCbrPskPublishesCatalog(t *testing.T) {
	rows := &pskFakeRows{values: [][]any{
		{"1.1", "автомобили с пробегом от 0 до 1000 км"},
		{"6.1.1", "не более 1 года с даты получения разрешения на ввод в эксплуатацию, договор долевого участия"},
		{"8.3", "автотранспортного средства"},
	}}
	batch := &pskFakeBatch{}
	conn := &pskFakeConn{
		rows:  rows,
		row:   &pskFakeRow{count: 1}, // system.tables: витрину создавать можно
		batch: batch,
	}
	if err := afterCbrPsk(context.Background(), conn); err != nil {
		t.Fatalf("afterCbrPsk: %v", err)
	}
	if !rows.closed {
		t.Error("строки каталога не закрыты: соединение течёт на каждом прогоне")
	}
	created := false
	for _, query := range conn.execs {
		if strings.Contains(query, "CREATE OR REPLACE VIEW v_cbr_psk") {
			created = true
		}
	}
	if !created {
		t.Errorf("витрина v_cbr_psk не создана: execs = %v", conn.execs)
	}
	if len(batch.rows) != len(rows.values) {
		t.Fatalf("в каталог записано %d рядов, ожидалось %d (по одному на код)", len(batch.rows), len(rows.values))
	}
	for i, row := range rows.values {
		if len(batch.rows[i]) != 7 {
			t.Fatalf("строка каталога %d: колонок %d, ожидалось 7", i, len(batch.rows[i]))
		}
		if want := fmt.Sprintf("ПСК %s: %s", row[0], row[1]); batch.rows[i][1] != want {
			t.Errorf("series каталога %d = %v, ожидался %q", i, batch.rows[i][1], want)
		}
	}
}

// TestCbrPskDdlFormat — DDL в %s-форме шаблона: единственный форматный глагол — имя таблицы.
// Символ «%» в комментариях колонок обязан быть экранирован: fmt.Sprintf(ddl, table) с сырым
// «% годовых» превратил бы комментарий в «%!г(MISSING)» и испортил бы описание колонки в
// system.tables — а по нему агент читает схему.
func TestCbrPskDdlFormat(t *testing.T) {
	ddl := fmt.Sprintf(cbrPskDdl, cbrPskTable)
	if !strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS "+cbrPskTable) {
		t.Errorf("DDL не содержит имя таблицы: %s", ddl)
	}
	if strings.Contains(ddl, "%!") || strings.Contains(ddl, "%%") {
		t.Errorf("DDL отформатирован с потерей комментариев: %s", ddl)
	}
	if !strings.Contains(ddl, "ключ") && !strings.Contains(ddl, "ReplacingMergeTree") {
		t.Errorf("DDL не объявляет движок: %s", ddl)
	}
	if !strings.Contains(ddl, "psk_max Nullable(Float64)") {
		t.Errorf("DDL не объявляет psk_max Nullable(Float64): ранние PDF не печатали предельное значение: %s", ddl)
	}
	alter := fmt.Sprintf(cbrPskAlterMaxNullable, cbrPskTable)
	if !strings.Contains(alter, "ALTER TABLE "+cbrPskTable+" MODIFY COLUMN psk_max Nullable(Float64)") {
		t.Errorf("миграция psk_max не называет таблицу и колонку: %s", alter)
	}
	if strings.Contains(alter, "%!") || strings.Contains(alter, "%%") {
		t.Errorf("миграция psk_max отформатирована с потерей текста: %s", alter)
	}
}

// pskFakeConn — фейковое соединение для гейта, каталога и публикации производных: объявлены
// только методы, которые вызывают эти пути; остальные унаследованы nil-заглушками driver.Conn
// (вызов — паника: фейк их не обещал).
type pskFakeConn struct {
	driver.Conn
	rows       driver.Rows
	row        driver.Row
	batch      driver.Batch
	queryErr   error
	execErr    error
	prepareErr error
	queries    []string
	execs      []string
}

func (c *pskFakeConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	c.queries = append(c.queries, query)
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return c.rows, nil
}

func (c *pskFakeConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	c.queries = append(c.queries, query)
	return c.row
}

func (c *pskFakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return c.execErr
}

func (c *pskFakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.execs = append(c.execs, query)
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	return c.batch, nil
}

// pskFakeRow — ответ QueryRow: число system.tables (витрину создавать можно) или число строк
// выпуска. Объявлен только Scan — единственный метод, который вызывают эти проверки.
type pskFakeRow struct {
	driver.Row
	count   uint64
	scanErr error
}

func (r *pskFakeRow) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(dest) != 1 {
		return fmt.Errorf("фейковый ответ: приёмников %d, ожидался один (count)", len(dest))
	}
	target, ok := dest[0].(*uint64)
	if !ok {
		return fmt.Errorf("фейковый ответ: приёмник %T, ожидался *uint64", dest[0])
	}
	*target = r.count
	return nil
}

// pskFakeBatch — приёмник каталога: строки batch v_series_catalog копятся в памяти.
type pskFakeBatch struct {
	driver.Batch
	rows [][]any
}

func (b *pskFakeBatch) Append(values ...any) error {
	b.rows = append(b.rows, values)
	return nil
}

func (b *pskFakeBatch) Send() error { return nil }

// pskFakeRows — ответ Query: строки значений, объявленные тестом. Поддерживаются приёмники
// *string (имя выпуска, код и наименование категории) и *uint64 (число строк выпуска).
type pskFakeRows struct {
	driver.Rows
	values  [][]any
	pos     int
	scanErr error
	err     error
	closed  bool
}

func (r *pskFakeRows) Next() bool {
	if r.pos >= len(r.values) {
		return false
	}
	r.pos++
	return true
}

func (r *pskFakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.pos == 0 {
		return errors.New("фейковая строка: Scan без Next")
	}
	row := r.values[r.pos-1]
	if len(dest) != len(row) {
		return fmt.Errorf("фейковая строка: приёмников %d, ожидалось %d", len(dest), len(row))
	}
	for i, value := range row {
		switch target := dest[i].(type) {
		case *string:
			text, ok := value.(string)
			if !ok {
				return fmt.Errorf("фейковая строка: значение %d = %T, ожидался string", i, value)
			}
			*target = text
		case *uint64:
			number, ok := value.(uint64)
			if !ok {
				return fmt.Errorf("фейковая строка: значение %d = %T, ожидался uint64", i, value)
			}
			*target = number
		default:
			return fmt.Errorf("фейковая строка: приёмник %d = %T, ожидался *string или *uint64", i, dest[i])
		}
	}
	return nil
}

func (r *pskFakeRows) Err() error { return r.err }

func (r *pskFakeRows) Close() error {
	r.closed = true
	return nil
}
