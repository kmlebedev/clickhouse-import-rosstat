package bold

import (
	"encoding/json"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Фикстуры bold/testdata — урезанные живые ответы bold.report от 2026-10-11:
// «последний день» как есть, история — три даты (2015-01-01 без units/aum, 2026-10-08,
// 2026-10-09). Тесты офлайн: сеть и httptest не нужны, читается только testdata/.
const (
	boldPinDate    = "2026-10-09"
	boldPinPrior   = "2026-10-08"
	boldPinEmpty   = "2015-01-01"
	boldPinUnits   = 113180843
	boldPinAPrior  = 113044763
	boldPinOunces  = 32150.74656862 // 1 т = 1e6/31.1034768 унции
	boldPinSummary = "bold_summary.json"
)

// testFixture читает фикстуру из testdata/ (тесты идут в каталоге пакета, сеть не нужна).
func testFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", name, err)
	}
	return body
}

// testMutateJSON отдаёт фикстуру с правкой data — так проверки «форма ответа сломалась»
// собираются из живых данных, а не из рукописного минимального JSON.
func testMutateJSON(t *testing.T, name string, fn func(data []map[string]any)) []byte {
	t.Helper()
	var doc struct {
		Version string           `json:"version"`
		Updated string           `json:"updated"`
		Data    []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(testFixture(t, name), &doc); err != nil {
		t.Fatalf("разбор фикстуры %s: %v", name, err)
	}
	fn(doc.Data)
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("сборка тела из %s: %v", name, err)
	}
	return body
}

func testDate(t *testing.T, s string) time.Time {
	t.Helper()
	d, err := parseBoldDate(s)
	if err != nil {
		t.Fatalf("дата %q не разобрана: %v", s, err)
	}
	return d
}

func testLatest(t *testing.T, name string, metric boldMetric) boldRow {
	t.Helper()
	row, err := parseLatest(testFixture(t, name), metric)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return row
}

func testHistory(t *testing.T, name string, metric boldMetric) []boldRow {
	t.Helper()
	rows, err := parseHistory(testFixture(t, name), metric)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return rows
}

func testFunds(t *testing.T) []boldFundRow {
	t.Helper()
	rows, err := parseFundSnapshot(testFixture(t, boldPinSummary))
	if err != nil {
		t.Fatalf("%s: %v", boldPinSummary, err)
	}
	return rows
}

// testMergedHistory сливает три файла истории (units, aum, flows) по дате — так же, как это
// делает полный путь импортёра (Task 2), — и отдаёт строки в порядке дат.
func testMergedHistory(t *testing.T) []boldRow {
	t.Helper()
	index := map[string]*boldRow{}
	var dates []string
	row := func(date string) *boldRow {
		if r, ok := index[date]; ok {
			return r
		}
		r := &boldRow{Date: date}
		index[date] = r
		dates = append(dates, date)
		return r
	}
	for _, r := range testHistory(t, "bold_units_history.json", boldMetricUnits) {
		row(r.Date).Units, row(r.Date).Price = r.Units, r.Price
	}
	for _, r := range testHistory(t, "bold_aum_history.json", boldMetricAum) {
		row(r.Date).Aum = r.Aum
	}
	for _, r := range testHistory(t, "bold_flows_history.json", boldMetricFlows) {
		row(r.Date).Culm7, row(r.Date).Culm30, row(r.Date).Culm90 = r.Culm7, r.Culm30, r.Culm90
	}
	sort.Strings(dates)
	merged := make([]boldRow, 0, len(dates))
	for _, date := range dates {
		merged = append(merged, *index[date])
	}
	return merged
}

// testDayRows оставляет строки только за указанную дату.
func testDayRows(t *testing.T, date string) []boldRow {
	t.Helper()
	var out []boldRow
	for _, r := range testMergedHistory(t) {
		if r.Date == date {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		t.Fatalf("в слитой истории нет строки за %s", date)
	}
	return out
}

func testPoints(t *testing.T, rows []boldRow, prevUnits *float64, prevDate time.Time) []point {
	t.Helper()
	points, err := boldETFSets(rows, prevUnits, prevDate)
	if err != nil {
		t.Fatalf("boldETFSets: %v", err)
	}
	return points
}

// testPoint ищет точку ряда за дату; отсутствие — фатально (диагностика печатает все точки).
func testPoint(t *testing.T, points []point, series, date string) point {
	t.Helper()
	want := testDate(t, date)
	for _, p := range points {
		if p.Series == series && p.Date.Equal(want) {
			return p
		}
	}
	t.Fatalf("нет точки %s за %s; есть: %s", series, date, testDescribe(points))
	return point{}
}

func testSeries(t *testing.T, points []point) []string {
	t.Helper()
	seen := map[string]bool{}
	for _, p := range points {
		seen[p.Series] = true
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func testDescribe(points []point) string {
	parts := make([]string, 0, len(points))
	for _, p := range points {
		parts = append(parts, p.Series+"@"+p.Date.Format("2006-01-02")+"="+strconv.FormatFloat(p.Value, 'f', 6, 64))
	}
	return strings.Join(parts, ", ")
}

func testNear(t *testing.T, label string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Fatalf("%s: получено %v, ожидалось %v (±%v)", label, got, want, tol)
	}
}

func testPtr(v float64) *float64 { return &v }

// TestParseLatestPinsFacts пинит факты 2026-10-09 из четырёх ответов «последнего дня».
func TestParseLatestPinsFacts(t *testing.T) {
	price := testLatest(t, "bold_price_latest.json", boldMetricPrice)
	if price.Date != boldPinDate || price.Value == nil {
		t.Fatalf("price-latest: дата %q, value %v", price.Date, price.Value)
	}
	testNear(t, "price", *price.Value, 4216.3, 1e-9)

	units := testLatest(t, "bold_units_latest.json", boldMetricUnits)
	// цена в units-latest дублирует price-latest: расхождение поймает импортёр (Task 2).
	testNear(t, "units", *units.Units, boldPinUnits, 1e-9)
	testNear(t, "units.price", *units.Price, 4216.3, 1e-9)

	aum := testLatest(t, "bold_aum_latest.json", boldMetricAum)
	testNear(t, "aum", *aum.Aum, 476677811738, 1e-6)

	flows := testLatest(t, "bold_flows_latest.json", boldMetricFlows)
	testNear(t, "culm7day", *flows.Culm7, 43740, 1e-9)
	testNear(t, "culm30day", *flows.Culm30, 991060, 1e-9)
	testNear(t, "culm90day", *flows.Culm90, 5376055, 1e-9)

	for name, row := range map[string]boldRow{"price": price, "units": units, "aum": aum, "flows": flows} {
		if row.Date != boldPinDate {
			t.Fatalf("%s-latest: дата %q, ожидалась %s", name, row.Date, boldPinDate)
		}
	}
}

// TestParseLatestRequiresMetricFields: отсутствие обязательного поля метрики — ошибка,
// называющая именно поле, а не строка с нулём.
func TestParseLatestRequiresMetricFields(t *testing.T) {
	if _, err := parseLatest([]byte(`{"version":"1","updated":"2026-10-11T04:32:05.131Z","date":"2026-10-09","price":4216.3}`), boldMetricUnits); err == nil {
		t.Fatal("units-latest без units обязан быть ошибкой")
	} else if !strings.Contains(err.Error(), "метрики units за 2026-10-09 нет обязательного поля units") {
		t.Fatalf("ошибка обязана называть метрику, дату и поле units: %v", err)
	}

	body := `{"version":"1","updated":"2026-10-11T04:32:05.028Z","date":"2026-10-09","culm7day":43740,"culm90day":5376055,"price":4216.3}`
	if _, err := parseLatest([]byte(body), boldMetricFlows); err == nil {
		t.Fatal("flows-latest без culm30day обязан быть ошибкой")
	} else if !strings.Contains(err.Error(), "нет обязательного поля culm30day") {
		t.Fatalf("ошибка обязана называть поле culm30day: %v", err)
	}
}

// TestParseLatestRejectsBadDate: дата не в формате 2006-01-02 — ошибка, а не нулевая дата.
func TestParseLatestRejectsBadDate(t *testing.T) {
	body := `{"version":"1","updated":"2026-10-11T04:32:05.131Z","date":"09/10/2026","value":4216.3}`
	if _, err := parseLatest([]byte(body), boldMetricPrice); err == nil {
		t.Fatal("дата 09/10/2026 обязана быть ошибкой")
	} else if !strings.Contains(err.Error(), "09/10/2026") {
		t.Fatalf("ошибка обязана называть дату: %v", err)
	}
}

// TestParseHistorySkipsNullUnits: 2015-01-01 без units/aum — строка остаётся с nil и в точки
// не идёт (вклад в точки дают 2 строки из 3); одинокие такие строки дают «нет точек».
func TestParseHistorySkipsNullUnits(t *testing.T) {
	rows := testHistory(t, "bold_units_history.json", boldMetricUnits)
	if len(rows) != 3 {
		t.Fatalf("строк в урезанной истории units: %d, ожидалось 3", len(rows))
	}
	if rows[0].Date != boldPinEmpty || rows[0].Units != nil || rows[0].Value != nil {
		t.Fatalf("2015-01-01: дата %q, units %v, value %v — ожидались nil", rows[0].Date, rows[0].Units, rows[0].Value)
	}
	if rows[1].Units == nil || rows[2].Units == nil {
		t.Fatalf("units за 08.10/09.10 обязаны быть: %v / %v", rows[1].Units, rows[2].Units)
	}
	testNear(t, "units 08.10", *rows[1].Units, boldPinAPrior, 1e-9)
	testNear(t, "units 09.10", *rows[2].Units, boldPinUnits, 1e-9)

	points := testPoints(t, rows, nil, time.Time{})
	// 08.10: только сток (предыдущего дня нет), 09.10: сток + поток в тоннах и USD.
	if len(points) != 4 {
		t.Fatalf("точек из одной истории units: %d, ожидалось 4 (%s)", len(points), testDescribe(points))
	}
	for _, p := range points {
		if p.Date.Format("2006-01-02") != boldPinDate && p.Date.Format("2006-01-02") != boldPinPrior {
			t.Fatalf("точка за %s: строка без units в точки не идёт", p.Date.Format("2006-01-02"))
		}
	}

	if _, err := boldETFSets(rows[:1], nil, time.Time{}); err == nil {
		t.Fatal("строка без units/aum/culm обязана давать ошибку «нет точек»")
	} else if !strings.Contains(err.Error(), "нет точек") {
		t.Fatalf("ошибка обязана говорить «нет точек»: %v", err)
	}

	body := `{"version":"1","updated":"2026-10-11T04:32:19.813Z","data":[{"date":"2015-01-01","price":1199.25}]}`
	if _, err := parseHistory([]byte(body), boldMetricUnits); err == nil {
		t.Fatal("история units без единой строки с units обязана быть ошибкой")
	} else if !strings.Contains(err.Error(), "в истории метрики units нет ни одной строки с её полями") {
		t.Fatalf("ошибка обязана говорить про метрику units и отсутствие строк с её полями: %v", err)
	}
}

// TestBoldETFSetsPinsFacts пинит факты 2026-10-09 на слитой истории: сток 3520.318 т,
// AUM 476.678 млрд $, окна 1.360/30.825/167.214 т, поток +4.233 т / 573.75 млн $.
func TestBoldETFSetsPinsFacts(t *testing.T) {
	rows := testMergedHistory(t)
	reversed := make([]boldRow, 0, len(rows))
	for i := len(rows) - 1; i >= 0; i-- { // обратный порядок: boldETFSets обязан отсортировать сам
		reversed = append(reversed, rows[i])
	}
	points := testPoints(t, reversed, nil, time.Time{})

	if len(points) != 12 {
		t.Fatalf("точек: %d, ожидалось 12 (5 за 08.10 + 7 за 09.10): %s", len(points), testDescribe(points))
	}
	want := []string{
		"etf_bold_aum_usd_bn", "etf_bold_flow_30d_tonnes", "etf_bold_flow_7d_tonnes", "etf_bold_flow_90d_tonnes",
		"etf_bold_flow_tonnes", "etf_bold_flow_usd_mln", "etf_bold_holdings_tonnes",
	}
	if got := testSeries(t, points); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ряды: %s, ожидались %s", strings.Join(got, ","), strings.Join(want, ","))
	}

	testNear(t, "holdings 09.10", testPoint(t, points, "etf_bold_holdings_tonnes", boldPinDate).Value, 3520.318, 1e-3)
	testNear(t, "aum 09.10", testPoint(t, points, "etf_bold_aum_usd_bn", boldPinDate).Value, 476.678, 1e-3)
	testNear(t, "flow 7d 09.10", testPoint(t, points, "etf_bold_flow_7d_tonnes", boldPinDate).Value, 1.360, 1e-3)
	testNear(t, "flow 30d 09.10", testPoint(t, points, "etf_bold_flow_30d_tonnes", boldPinDate).Value, 30.825, 1e-3)
	testNear(t, "flow 90d 09.10", testPoint(t, points, "etf_bold_flow_90d_tonnes", boldPinDate).Value, 167.214, 1e-3)
	testNear(t, "flow 09.10", testPoint(t, points, "etf_bold_flow_tonnes", boldPinDate).Value, 4.233, 1e-3)
	testNear(t, "flow usd 09.10", testPoint(t, points, "etf_bold_flow_usd_mln", boldPinDate).Value, 573.75, 1e-2)
	testNear(t, "holdings 08.10", testPoint(t, points, "etf_bold_holdings_tonnes", boldPinPrior).Value, 3516.085, 1e-3)

	// 08.10 — первая строка с units: предыдущего дня нет, потока нет.
	for _, p := range points {
		if p.Series != "etf_bold_flow_tonnes" && p.Series != "etf_bold_flow_usd_mln" {
			continue
		}
		if p.Date.Format("2006-01-02") == boldPinPrior {
			t.Fatalf("поток за 08.10 без предыдущего дня: %v", p)
		}
	}
	if _, err := boldETFSets([]boldRow{{Date: boldPinEmpty, Value: testPtr(1199.25)}}, nil, time.Time{}); err == nil {
		t.Fatal("строка 2015-01-01 без units/aum обязана давать «нет точек»")
	}
}

// TestBoldETFSetsSkipsFirstFlow: одна строка истории — ряда потока нет, остальные пять есть.
func TestBoldETFSetsSkipsFirstFlow(t *testing.T) {
	points := testPoints(t, testDayRows(t, boldPinDate), nil, time.Time{})
	if len(points) != 5 {
		t.Fatalf("точек одной строки: %d, ожидалось 5 (%s)", len(points), testDescribe(points))
	}
	want := "etf_bold_aum_usd_bn,etf_bold_flow_30d_tonnes,etf_bold_flow_7d_tonnes,etf_bold_flow_90d_tonnes,etf_bold_holdings_tonnes"
	if got := testSeries(t, points); strings.Join(got, ",") != want {
		t.Fatalf("ряды: %s, ожидались %s", strings.Join(got, ","), want)
	}
}

// TestBoldETFSetsUsesPrevUnitsFromDB: на инкременте предыдущие унции приходят из ClickHouse —
// поток 09.10 считается от 08.10 (136 080 oz → +4.233 т и 573.75 млн $).
func TestBoldETFSetsUsesPrevUnitsFromDB(t *testing.T) {
	points := testPoints(t, testDayRows(t, boldPinDate), testPtr(boldPinAPrior), testDate(t, boldPinPrior))
	if len(points) != 7 {
		t.Fatalf("точек: %d, ожидалось 7 (%s)", len(points), testDescribe(points))
	}
	testNear(t, "flow", testPoint(t, points, "etf_bold_flow_tonnes", boldPinDate).Value, 4.233, 1e-3)
	testNear(t, "flow usd", testPoint(t, points, "etf_bold_flow_usd_mln", boldPinDate).Value, 573.75, 1e-2)
}

// TestBoldETFSetsRejectsPrevDateNotOlder: prevDate (10-09, сток из ClickHouse) не старше первой
// строки среза (10-08) — дельта посчиталась бы с будущим стоком, поэтому это ошибка, а не тихий
// поток. Межэндпоинтная сверка дат (price за 10-09 при units за 10-08) живёт в Task 2
// (boldEtfFetchLatest): у строки boldETFSets одна общая Date на все метрики.
func TestBoldETFSetsRejectsPrevDateNotOlder(t *testing.T) {
	_, err := boldETFSets(testDayRows(t, boldPinPrior), testPtr(boldPinUnits), testDate(t, boldPinDate))
	if err == nil {
		t.Fatal("prevDate 10-09 при строке 10-08 обязан быть ошибкой")
	}
	for _, want := range []string{"предыдущий сток за " + boldPinDate, "не старше строки " + boldPinPrior} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("ошибка обязана содержать %q: %v", want, err)
		}
	}
}

// TestBoldETFSetsRejectsDuplicateDates: две строки одной даты в срезе — дубли точек, ошибка.
func TestBoldETFSetsRejectsDuplicateDates(t *testing.T) {
	day := testDayRows(t, boldPinDate)
	_, err := boldETFSets(append(append([]boldRow{}, day...), day...), nil, time.Time{})
	if err == nil {
		t.Fatal("повтор даты в срезе обязан быть ошибкой")
	}
	if !strings.Contains(err.Error(), "дата "+boldPinDate+" повторяется") {
		t.Fatalf("ошибка обязана называть повторяющуюся дату: %v", err)
	}
}

// TestBoldETFSetsRejectsPartialWindows: правило «все три окна или ни одного» действует и здесь —
// функция сама решает, какие точки строить, и не должна принимать строку с частью окон.
func TestBoldETFSetsRejectsPartialWindows(t *testing.T) {
	day := testDayRows(t, boldPinDate)
	day[0].Culm30 = nil
	_, err := boldETFSets(day, nil, time.Time{})
	if err == nil {
		t.Fatal("строка с частично заполненными окнами обязана быть ошибкой")
	}
	for _, want := range []string{"есть culm7day, culm90day", "нет culm30day"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("ошибка обязана содержать %q: %v", want, err)
		}
	}
}

// TestParseFundSnapshotPinsFacts: 73 фонда, у GLD (SPDR Gold Shares) 33 552 628 oz,
// ни одного пустого поля в срезе.
func TestParseFundSnapshotPinsFacts(t *testing.T) {
	rows := testFunds(t)
	if len(rows) != 73 {
		t.Fatalf("фондов: %d, ожидалось 73", len(rows))
	}
	gld := 0
	for _, r := range rows {
		if r.Ticker == "" || r.Name == "" {
			t.Fatalf("фонд без тикера/имени: %+v", r)
		}
		if r.Units == nil || r.UnitsChange == nil || r.Aum == nil || r.AumChange == nil ||
			r.Price == nil || r.PriceChange == nil || r.Shares == nil || r.SharesChange == nil {
			t.Fatalf("%s (%s): неполная строка среза", r.Ticker, r.Name)
		}
		if r.Ticker == "GLD" {
			gld++
		}
	}
	// Тикер не уникален: GLD встречается трижды (Бангкок, NewGold, SPDR) — известно и учтено
	// на стороне импортёра (ключ таблицы (date, ticker) столько строк не сохранит).
	if gld != 3 {
		t.Fatalf("строк с тикером GLD: %d, ожидалось 3", gld)
	}

	points, err := boldFundPoints(rows, testDate(t, boldPinDate))
	if err != nil {
		t.Fatalf("boldFundPoints: %v", err)
	}
	if len(points) != 73 {
		t.Fatalf("точек фондов: %d, ожидалось 73", len(points))
	}
	found := false
	for _, p := range points {
		if !p.Date.Equal(testDate(t, boldPinDate)) {
			t.Fatalf("точка фонда с датой %s, ожидалась %s", p.Date.Format("2006-01-02"), boldPinDate)
		}
		if p.Ticker != "GLD" || p.Units != 33552628 {
			continue
		}
		found = true
		if p.Name != "SPDR Gold Shares" {
			t.Fatalf("GLD с 33552628 oz: имя %q", p.Name)
		}
		testNear(t, "GLD unitsChange", p.UnitsChange, -68447.07, 1e-2)
		testNear(t, "GLD aum", p.Aum, 141684193497, 1e-3)
		testNear(t, "GLD price", p.Price, 384.58, 1e-9)
		testNear(t, "GLD shares", p.Shares, 369200004, 1e-6)
	}
	if !found {
		t.Fatal("в срезе нет GLD с 33 552 628 oz")
	}
}

// TestParseFundSnapshotRejectsMissingFields: срез — полный снимок; отсутствие любого поля
// (или пустой срез) — ошибка, сигнал смены формы ответа, а не ноль в точке.
func TestParseFundSnapshotRejectsMissingFields(t *testing.T) {
	body := testMutateJSON(t, boldPinSummary, func(data []map[string]any) {
		delete(data[0], "aum")
	})
	if _, err := parseFundSnapshot(body); err == nil {
		t.Fatal("строка среза без aum обязана быть ошибкой")
	} else if !strings.Contains(err.Error(), "нет обязательного поля aum") {
		t.Fatalf("ошибка обязана называть поле aum: %v", err)
	}
	if _, err := parseFundSnapshot([]byte(`{"version":"1","updated":"","data":[]}`)); err == nil {
		t.Fatal("пустой срез фондов обязан быть ошибкой")
	}
}

// TestBoldFundPointsRejectsNilFields: второй уровень защиты — прямой вызов boldFundPoints без
// разбора обязан падать на любом nil, а не подставлять ноль (Review Focus 1: «null — это
// отсутствие данных, а не ноль»).
func TestBoldFundPointsRejectsNilFields(t *testing.T) {
	full := testFunds(t)[0]
	full.Ticker, full.Name = "TEST", "Test Gold ETF"
	fields := []struct {
		name string
		drop func(f *boldFundRow)
	}{
		{"units", func(f *boldFundRow) { f.Units = nil }},
		{"unitsChange", func(f *boldFundRow) { f.UnitsChange = nil }},
		{"aum", func(f *boldFundRow) { f.Aum = nil }},
		{"aumChange", func(f *boldFundRow) { f.AumChange = nil }},
		{"price", func(f *boldFundRow) { f.Price = nil }},
		{"priceChange", func(f *boldFundRow) { f.PriceChange = nil }},
		{"shares", func(f *boldFundRow) { f.Shares = nil }},
		{"sharesChange", func(f *boldFundRow) { f.SharesChange = nil }},
	}
	if _, err := boldFundPoints([]boldFundRow{full}, testDate(t, boldPinDate)); err != nil {
		t.Fatalf("полная строка не должна падать: %v", err)
	}
	for _, field := range fields {
		fund := full
		field.drop(&fund)
		_, err := boldFundPoints([]boldFundRow{fund}, testDate(t, boldPinDate))
		if err == nil {
			t.Fatalf("nil в поле %s обязан быть ошибкой, а не нулём", field.name)
		}
		for _, want := range []string{"TEST", "нет обязательного поля " + field.name} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("ошибка обязана содержать %q для поля %s: %v", want, field.name, err)
			}
		}
	}
}

// TestOzToTonnes: унции → тонны по 1 т = 32150.74656862 унции (та же константа, что в gold/).
func TestOzToTonnes(t *testing.T) {
	testNear(t, "113180843 oz", ozToTonnes(boldPinUnits), 3520.318, 1e-3)
	testNear(t, "32150.74656862 oz", ozToTonnes(boldPinOunces), 1, 1e-9)
	testNear(t, "0 oz", ozToTonnes(0), 0, 1e-12)
}

// TestParseHistoryRejectsPartialWindows: правило источника «все три окна или ни одного» —
// частично заполненная строка ломает ряд окон и обязана падать (в живом flows/all 3138 строк:
// до 2015-04-01 ни одного culm*, с 2015-04-01 все три).
func TestParseHistoryRejectsPartialWindows(t *testing.T) {
	body := testMutateJSON(t, "bold_flows_history.json", func(data []map[string]any) {
		delete(data[len(data)-1], "culm30day")
	})
	_, err := parseHistory(body, boldMetricFlows)
	if err == nil {
		t.Fatal("строка с частью окон culm* обязана быть ошибкой")
	}
	for _, want := range []string{"окна заполнены частично", "есть culm7day, culm90day", "нет culm30day"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("ошибка обязана содержать %q: %v", want, err)
		}
	}
}

// TestParseHistoryRejectsDuplicateDates: одна дата дважды в одном файле истории — дубли точек.
// Без проверки строки только с aum дали бы две точки на дату молча.
func TestParseHistoryRejectsDuplicateDates(t *testing.T) {
	body := testMutateJSON(t, "bold_aum_history.json", func(data []map[string]any) {
		data[len(data)-1] = data[len(data)-2] // 09.10 подменена копией 08.10
	})
	_, err := parseHistory(body, boldMetricAum)
	if err == nil {
		t.Fatal("повтор даты в файле истории обязан быть ошибкой")
	}
	if !strings.Contains(err.Error(), "дата "+boldPinPrior+" повторяется (строки 2 и 3)") {
		t.Fatalf("ошибка обязана называть дату и номера строк: %v", err)
	}
}

// TestBoldEndpointsPinPaths финит эндпоинты, объявленные для импортёров (Task 2/3): молчаливая
// правка пути сломала бы и счётчики запросов, и расхождение дат «последнего дня». Здесь же
// пришпилен обязательный суффикс `.json`: без расширения объекта в источнике нет и живой
// запрос получает 403 AccessDenied, а httptest отвечает на любой путь и молчит об этом —
// именно так дефект и дожил до живой приёмки (fix round 1, Ruling 14).
func TestBoldEndpointsPinPaths(t *testing.T) {
	base, err := url.Parse(boldBaseURL)
	if err != nil {
		t.Fatalf("boldBaseURL %q не разбирается: %v", boldBaseURL, err)
	}
	if base.Scheme != "https" || base.Host != "bold.report" || base.Path != "/api/v1/" {
		t.Fatalf("boldBaseURL = %q, ожидался https://bold.report/api/v1/", boldBaseURL)
	}
	paths := []struct {
		name, got, want string
	}{
		{"boldPathPrice", boldPathPrice, "gold/price.json"},
		{"boldPathPriceLatest", boldPathPriceLatest, "gold/price-latest.json"},
		{"boldPathUnits", boldPathUnits, "gold/funds/units.json"},
		{"boldPathUnitsLatest", boldPathUnitsLatest, "gold/funds/units-latest.json"},
		{"boldPathAum", boldPathAum, "gold/funds/aum.json"},
		{"boldPathAumLatest", boldPathAumLatest, "gold/funds/aum-latest.json"},
		{"boldPathFlows", boldPathFlows, "gold/flows/all.json"},
		{"boldPathFlowsLatest", boldPathFlowsLatest, "gold/flows/all-latest.json"},
		{"boldPathSummary", boldPathSummary, "gold/summary.json"},
	}
	for _, p := range paths {
		if !strings.HasSuffix(p.got, ".json") {
			t.Fatalf("%s = %q: путь обязан оканчиваться на .json — без расширения объект в источнике не существует и CDN отвечает 403 AccessDenied, а httptest отвечает на любой путь и этого не ловит", p.name, p.got)
		}
		if p.got != p.want {
			t.Fatalf("%s = %q, ожидалось %q", p.name, p.got, p.want)
		}
		if strings.HasPrefix(p.got, "/") {
			t.Fatalf("%s = %q: путь склеивается с boldBaseURL без ведущего слэша", p.name, p.got)
		}
	}
}
