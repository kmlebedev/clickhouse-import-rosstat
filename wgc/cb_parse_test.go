package wgc

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	cbChangesFixturePath  = "testdata/Changes_latest_as_of_Oct2026_IFS.xlsx"
	cbHoldingsFixturePath = "testdata/World_official_gold_holdings_as_of_Oct2026_IFS.xlsx"

	// cbAnnualSheet — сверочный лист выгрузки Changes: годовые итоги по колонке года (строка 1).
	// Парсер его не читает — лист нужен только сверке «Annual 2026 == Σ Monthly 2026».
	cbAnnualSheet = "Annual"

	// cbTolerance — допуск сверки точечных значений фикстур, cbSumTolerance — сумм (§3.2).
	cbTolerance    = 1e-9
	cbSumTolerance = 1e-6
)

// Синтетические листы собираются теми же хелперами, что и ETF-тесты пакета: setEtfCell,
// setEtfDate, newEtfSheet.

// openCbFixture открывает фикстуру выгрузки ЦБ (закрывается после теста).
func openCbFixture(t *testing.T, path string) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatalf("excelize.OpenFile(%s): %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// cbDate — конец месяца в UTC (даты точек парсеров ЦБ).
func cbDate(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// cbBySeries индексирует точки ЦБ по имени ряда.
func cbBySeries(points []seriesPoint) map[string][]seriesPoint {
	bySeries := make(map[string][]seriesPoint, len(cbCountries)*2)
	for _, p := range points {
		bySeries[p.Series] = append(bySeries[p.Series], p)
	}
	return bySeries
}

// cbSeriesPoints — точки ряда в порядке разбора.
func cbSeriesPoints(points []seriesPoint, series string) []seriesPoint {
	var seriesPoints []seriesPoint
	for _, p := range points {
		if p.Series == series {
			seriesPoints = append(seriesPoints, p)
		}
	}
	return seriesPoints
}

// cbHasPoint — есть ли у ряда точка на дату (для проверки пропусков).
func cbHasPoint(points []seriesPoint, series string, date time.Time) bool {
	for _, p := range cbSeriesPoints(points, series) {
		if p.Date.Equal(date) {
			return true
		}
	}
	return false
}

// cbValue — значение ряда на дату; отсутствие точки — ошибка теста.
func cbValue(t *testing.T, points []seriesPoint, series string, date time.Time) float64 {
	t.Helper()
	for _, p := range cbSeriesPoints(points, series) {
		if p.Date.Equal(date) {
			return p.Value
		}
	}
	t.Fatalf("ряд %s: нет точки на %s", series, date.Format("2006-01-02"))
	return 0
}

// cbSumYear — сумма точек ряда за год (YTD-2026 = сумма января..августа фикстуры).
func cbSumYear(points []seriesPoint, series string, year int) float64 {
	var sum float64
	for _, p := range cbSeriesPoints(points, series) {
		if p.Date.Year() == year {
			sum += p.Value
		}
	}
	return sum
}

// cbAnnualColumn — колонка года на листе Annual (заголовки строки 1); год обязателен.
func cbAnnualColumn(t *testing.T, f *excelize.File, year string) int {
	t.Helper()
	maxCol, _, err := sheetSize(f, cbAnnualSheet)
	if err != nil {
		t.Fatalf("лист %s: размеры: %v", cbAnnualSheet, err)
	}
	for col := 1; col <= maxCol; col++ {
		raw, err := cellRaw(f, cbAnnualSheet, col, 1)
		if err != nil {
			t.Fatalf("лист %s: заголовок колонки %d: %v", cbAnnualSheet, col, err)
		}
		if strings.TrimSpace(raw) == year {
			return col
		}
	}
	t.Fatalf("лист %s: колонка года %s не найдена в строке 1", cbAnnualSheet, year)
	return 0
}

// cbAnnualRow — строка страны на листе Annual (колонка A): ровно одно совпадение, поэтому
// Turkey (net) не путается с Turkey* (gross).
func cbAnnualRow(t *testing.T, f *excelize.File, name string) int {
	t.Helper()
	_, maxRow, err := sheetSize(f, cbAnnualSheet)
	if err != nil {
		t.Fatalf("лист %s: размеры: %v", cbAnnualSheet, err)
	}
	row := 0
	for r := 2; r <= maxRow; r++ {
		raw, err := cellRaw(f, cbAnnualSheet, 1, r)
		if err != nil {
			t.Fatalf("лист %s: страна строки %d: %v", cbAnnualSheet, r, err)
		}
		if strings.TrimSpace(raw) != name {
			continue
		}
		if row != 0 {
			t.Fatalf("лист %s: страна %q встречается больше одного раза (строки %d и %d)", cbAnnualSheet, name, row, r)
		}
		row = r
	}
	if row == 0 {
		t.Fatalf("лист %s: страна %q не найдена в колонке A", cbAnnualSheet, name)
	}
	return row
}

// cbAnnualValue — числовое значение ячейки листа Annual.
func cbAnnualValue(t *testing.T, f *excelize.File, col, row int) float64 {
	t.Helper()
	raw, err := cellRaw(f, cbAnnualSheet, col, row)
	if err != nil {
		t.Fatalf("лист %s: колонка %d строка %d: %v", cbAnnualSheet, col, row, err)
	}
	value, ok, err := cellFloat(raw)
	if err != nil {
		t.Fatalf("лист %s: колонка %d строка %d = %q: %v", cbAnnualSheet, col, row, raw, err)
	}
	if !ok {
		t.Fatalf("лист %s: колонка %d строка %d пуста, ожидалось годовое значение", cbAnnualSheet, col, row)
	}
	return value
}

func TestCbCountriesOrder(t *testing.T) {
	want := []CbCountry{
		{ISO: "POL", Country: "Польша", Name: "Poland, Rep. of"},
		{ISO: "CZE", Country: "Чехия", Name: "Czech Rep."},
		{ISO: "TUR", Country: "Турция", Name: "Turkey"},
		{ISO: "RUS", Country: "Россия", Name: "Russian Federation"},
		{ISO: "KAZ", Country: "Казахстан", Name: "Kazakhstan, Rep. of"},
		{ISO: "UZB", Country: "Узбекистан", Name: "Uzbekistan, Rep. of"},
		{ISO: "CHN", Country: "Китай", Name: "China, P.R.: Mainland"},
	}
	got := CbCountries()
	if len(got) != len(want) {
		t.Fatalf("CbCountries(): %d стран, ожидалось %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("CbCountries()[%d] = %+v, ожидалось %+v", i, got[i], want[i])
		}
	}
}

func TestCbSeriesNames(t *testing.T) {
	want := []string{
		"cb_gold_change_tonnes_POL", "cb_gold_holdings_tonnes_POL",
		"cb_gold_change_tonnes_CZE", "cb_gold_holdings_tonnes_CZE",
		"cb_gold_change_tonnes_TUR", "cb_gold_holdings_tonnes_TUR",
		"cb_gold_change_tonnes_RUS", "cb_gold_holdings_tonnes_RUS",
		"cb_gold_change_tonnes_KAZ", "cb_gold_holdings_tonnes_KAZ",
		"cb_gold_change_tonnes_UZB", "cb_gold_holdings_tonnes_UZB",
		"cb_gold_change_tonnes_CHN", "cb_gold_holdings_tonnes_CHN",
	}
	got := CbSeriesNames()
	if len(got) != len(want) {
		t.Fatalf("CbSeriesNames(): %d имён, ожидалось %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("CbSeriesNames()[%d] = %q, ожидалось %q", i, got[i], want[i])
		}
	}
	if name := wgcCbSeriesName(cbMetricHoldingsTonnes, "POL"); name != "cb_gold_holdings_tonnes_POL" {
		t.Errorf("wgcCbSeriesName(holdings, POL) = %q, ожидалось cb_gold_holdings_tonnes_POL", name)
	}
}

func TestCbChangesLastMonthPinned(t *testing.T) {
	points, err := parseCbChanges(openCbFixture(t, cbChangesFixturePath))
	if err != nil {
		t.Fatalf("parseCbChanges: %v", err)
	}

	aug26 := cbDate(2026, time.August, 31)
	cases := []struct {
		iso  string
		want float64
	}{
		{"POL", 7.7781006189542943},
		{"CZE", 1.6793878884015905},
		{"KAZ", 7.3512767876582492},
		{"RUS", -6.2206463251531838},
		{"UZB", 7.7758079064414796},
		{"CHN", 20.217100556747845},
		{"TUR", 2.8932138038765629},
	}
	for _, tc := range cases {
		series := wgcCbSeriesName(cbMetricChangeTonnes, tc.iso)
		got := cbValue(t, points, series, aug26)
		if math.Abs(got-tc.want) > cbTolerance {
			t.Errorf("%s на 2026-08-31 = %v, ожидалось %v", series, got, tc.want)
		}
	}

	// В фикстуре рядом с Turkey (net) есть Turkey* (gross) с 18.910764828465794: если бы словарь
	// матчился неточно, TUR получил бы её значение.
	if got := cbValue(t, points, wgcCbSeriesName(cbMetricChangeTonnes, "TUR"), aug26); math.Abs(got-18.910764828465794) <= cbTolerance {
		t.Errorf("TUR = %v — это значение Turkey* (gross), а не Turkey (net)", got)
	}
}

func TestCbChangesYtd2026(t *testing.T) {
	points, err := parseCbChanges(openCbFixture(t, cbChangesFixturePath))
	if err != nil {
		t.Fatalf("parseCbChanges: %v", err)
	}

	cases := []struct {
		iso  string
		want float64
		tol  float64
	}{
		{"POL", 97.772267238966151, cbSumTolerance},
		{"CZE", 14.152996796367127, cbSumTolerance},
		{"KAZ", 36.063396410687105, cbSumTolerance},
		{"RUS", -34.213554788342506, cbSumTolerance},
		{"TUR", -81.606776196123377, cbSumTolerance},
		{"UZB", 48.210009019937111, cbSumTolerance},
		// Контроль блога WGC 10.2026 округлён до 80,246 — точное значение фикстуры
		// 80.246337594475591, поэтому допуск 1e-3 (как в брифе).
		{"CHN", 80.246, 1e-3},
	}
	for _, tc := range cases {
		series := wgcCbSeriesName(cbMetricChangeTonnes, tc.iso)
		got := cbSumYear(points, series, 2026)
		if math.Abs(got-tc.want) > tc.tol {
			t.Errorf("YTD-2026 %s = %v, ожидалось %v (|Δ| > %v)", series, got, tc.want, tc.tol)
		}
	}
}

func TestCbChangesEmptyAndZero(t *testing.T) {
	points, err := parseCbChanges(openCbFixture(t, cbChangesFixturePath))
	if err != nil {
		t.Fatalf("parseCbChanges: %v", err)
	}
	rus := wgcCbSeriesName(cbMetricChangeTonnes, "RUS")

	// Пустая ячейка — отсутствие данных, а не ноль: у RUS пусты 2025-12 и 2026-01..03, точек нет.
	for _, date := range []time.Time{
		cbDate(2025, time.December, 31),
		cbDate(2026, time.January, 31),
		cbDate(2026, time.February, 28),
		cbDate(2026, time.March, 31),
	} {
		if cbHasPoint(points, rus, date) {
			t.Errorf("ряд %s: у пустого месяца %s появилась точка", rus, date.Format("2006-01"))
		}
	}

	// Явный ноль — значение: POL 2024-12 в фикстуре записан «0» и должен стать точкой с нулём.
	pol := wgcCbSeriesName(cbMetricChangeTonnes, "POL")
	if got := cbValue(t, points, pol, cbDate(2024, time.December, 31)); got != 0 {
		t.Errorf("ряд %s на 2024-12-31 = %v, ожидалось 0 (явный ноль пишется)", pol, got)
	}
}

func TestCbChangesCoverage(t *testing.T) {
	points, err := parseCbChanges(openCbFixture(t, cbChangesFixturePath))
	if err != nil {
		t.Fatalf("parseCbChanges: %v", err)
	}

	cases := []struct {
		iso       string
		wantCount int
		wantFirst time.Time
	}{
		{"POL", 296, cbDate(2002, time.January, 31)},
		{"CZE", 296, cbDate(2002, time.January, 31)},
		{"TUR", 296, cbDate(2002, time.January, 31)},
		// RUS: 296 колонок-месяцев минус 4 пустых (2025-12 и 2026-01..03) = 292 точки.
		{"RUS", 292, cbDate(2002, time.January, 31)},
		{"KAZ", 296, cbDate(2002, time.January, 31)},
		// UZB: фактическое покрытие фикстуры — 2013-02..2026-08 без пропусков (163 точки), хотя
		// комментарий самого файла (C166) и оговорка спеки §3.2 говорят «данные с января 2018».
		// Фикстура — источник истины: расхождение зафиксировано здесь, а не подогнано.
		{"UZB", 163, cbDate(2013, time.February, 28)},
		{"CHN", 296, cbDate(2002, time.January, 31)},
	}
	for _, tc := range cases {
		series := wgcCbSeriesName(cbMetricChangeTonnes, tc.iso)
		seriesPoints := cbSeriesPoints(points, series)
		if len(seriesPoints) != tc.wantCount {
			t.Errorf("ряд %s: точек %d, ожидалось %d", series, len(seriesPoints), tc.wantCount)
		}
		if len(seriesPoints) == 0 {
			continue
		}
		if first := seriesPoints[0].Date.Format("2006-01-02"); first != tc.wantFirst.Format("2006-01-02") {
			t.Errorf("ряд %s: первая дата %s, ожидалась %s", series, first, tc.wantFirst.Format("2006-01-02"))
		}
	}

	// Рядов ровно по числу стран словаря: лишние ряды означали бы, что в словарь попало не то.
	if want := len(cbCountries); len(cbBySeries(points)) != want {
		t.Errorf("рядов %d, ожидалось %d", len(cbBySeries(points)), want)
	}

	// Даты точек — конец отчётного месяца в UTC (конвенция проекта).
	for _, p := range points {
		if p.Date.Location() != time.UTC || !p.Date.Equal(monthEnd(p.Date)) {
			t.Fatalf("точка %s: дата %v не конец месяца в UTC", p.Series, p.Date)
		}
	}
}

func TestCbChangesAnnualMatchesMonthly(t *testing.T) {
	f := openCbFixture(t, cbChangesFixturePath)
	points, err := parseCbChanges(f)
	if err != nil {
		t.Fatalf("parseCbChanges: %v", err)
	}
	col2026 := cbAnnualColumn(t, f, "2026")

	for _, c := range CbCountries() {
		annual := cbAnnualValue(t, f, col2026, cbAnnualRow(t, f, c.Name))
		ytd := cbSumYear(points, wgcCbSeriesName(cbMetricChangeTonnes, c.ISO), 2026)
		if math.Abs(annual-ytd) > cbSumTolerance {
			t.Errorf("%s: Annual 2026 = %v, Σ Monthly 2026 = %v (|Δ| > %v)", c.ISO, annual, ytd, cbSumTolerance)
		}
	}

	// Контрольные значения брифа: колонка 2026 для Turkey — net-строка (не Turkey*).
	if got := cbAnnualValue(t, f, col2026, cbAnnualRow(t, f, "Poland, Rep. of")); math.Abs(got-97.772267238966151) > cbSumTolerance {
		t.Errorf("Annual 2026 POL = %v, ожидалось 97.772267238966151", got)
	}
	if got := cbAnnualValue(t, f, col2026, cbAnnualRow(t, f, "Turkey")); math.Abs(got-(-81.606776196123377)) > cbSumTolerance {
		t.Errorf("Annual 2026 TUR = %v, ожидалось -81.606776196123377 (net Turkey, не Turkey*)", got)
	}
}

func TestCbHoldingsPinned(t *testing.T) {
	points, err := parseCbHoldings(openCbFixture(t, cbHoldingsFixturePath))
	if err != nil {
		t.Fatalf("parseCbHoldings: %v", err)
	}
	if len(points) != len(cbCountries) {
		t.Fatalf("точек %d, ожидалось %d (по одной на страну словаря)", len(points), len(cbCountries))
	}

	// Per-country serial 46264 = 2026-08-30 — нормализуется к концу месяца: у всех 7 стран 31.08.
	aug26 := cbDate(2026, time.August, 31)
	cases := []struct {
		iso  string
		want float64
	}{
		{"POL", 647.98366638673758},
		{"CHN", 2386.5509626450189},
		{"RUS", 2270.5359086809121},
		{"TUR", 532.09321380387655}, // имя в файле — «Turkey5)», сноска чистится
		{"UZB", 438.55556592329941},
		{"KAZ", 377.10696556872261},
		{"CZE", 85.759758638922577},
	}
	for _, tc := range cases {
		series := wgcCbSeriesName(cbMetricHoldingsTonnes, tc.iso)
		got := cbValue(t, points, series, aug26)
		if math.Abs(got-tc.want) > cbTolerance {
			t.Errorf("%s на 2026-08-31 = %v, ожидалось %v", series, got, tc.want)
		}
	}

	// Даты — конец месяца в UTC: serial стран нормализуется monthEnd, а не остаётся серединой месяца.
	for _, p := range points {
		if !p.Date.Equal(aug26) || p.Date.Location() != time.UTC {
			t.Errorf("точка %s: дата %v, ожидалось 2026-08-31 UTC", p.Series, p.Date)
		}
	}
}

func TestCbCountryLookupStrict(t *testing.T) {
	// Две строки Turkey (остальные страны словаря на месте): какая из них страна — неясно,
	// поэтому разбор должен упасть.
	f := newEtfSheet(t, cbChangesSheet)
	setEtfDate(t, f, cbChangesSheet, "D8", cbDate(2026, time.August, 31))
	row := 9
	for _, c := range CbCountries() {
		setEtfCell(t, f, cbChangesSheet, "B"+strconv.Itoa(row), c.Name)
		setEtfCell(t, f, cbChangesSheet, "D"+strconv.Itoa(row), 1)
		row++
	}
	setEtfCell(t, f, cbChangesSheet, "B"+strconv.Itoa(row), "Turkey")
	setEtfCell(t, f, cbChangesSheet, "D"+strconv.Itoa(row), 2)
	if _, err := parseCbChanges(f); err == nil {
		t.Fatal("две строки Turkey должны давать ошибку")
	} else if !strings.Contains(err.Error(), "Turkey") || !strings.Contains(err.Error(), "больше одного раза") {
		t.Errorf("ошибка не описывает дубль Turkey: %v", err)
	}

	// Turkey* (gross) — не Turkey (net): словарь ищется строгим равенством, страна набора
	// остаётся ненайденной и разбор падает.
	f = newEtfSheet(t, cbChangesSheet)
	setEtfDate(t, f, cbChangesSheet, "D8", cbDate(2026, time.August, 31))
	row = 9
	for _, c := range CbCountries() {
		name := c.Name
		if c.ISO == "TUR" {
			name = "Turkey*"
		}
		setEtfCell(t, f, cbChangesSheet, "B"+strconv.Itoa(row), name)
		setEtfCell(t, f, cbChangesSheet, "D"+strconv.Itoa(row), 1)
		row++
	}
	if _, err := parseCbChanges(f); err == nil {
		t.Fatal("строка Turkey* вместо Turkey должна давать ошибку")
	} else if !strings.Contains(err.Error(), `"Turkey"`) {
		t.Errorf("ошибка не называет ненайденную страну Turkey: %v", err)
	}
}

func TestCbChangesMissingCountryFails(t *testing.T) {
	f := newEtfSheet(t, cbChangesSheet)
	setEtfDate(t, f, cbChangesSheet, "D8", cbDate(2026, time.August, 31))
	setEtfCell(t, f, cbChangesSheet, "B9", "Turkey")
	setEtfCell(t, f, cbChangesSheet, "D9", 1)

	if _, err := parseCbChanges(f); err == nil {
		t.Fatal("пропавшая из словаря страна должна давать ошибку")
	} else if !strings.Contains(err.Error(), "Poland, Rep. of") {
		t.Errorf("ошибка не называет пропавшую страну: %v", err)
	}
}

func TestCbChangesTextDateHeaderFails(t *testing.T) {
	f := newEtfSheet(t, cbChangesSheet)
	setEtfCell(t, f, cbChangesSheet, "D8", "авг26")
	setEtfCell(t, f, cbChangesSheet, "B9", "Turkey")

	if _, err := parseCbChanges(f); err == nil {
		t.Fatal("непустой не-числовой заголовок-дата должен давать ошибку")
	} else if !strings.Contains(err.Error(), "D8") {
		t.Errorf("ошибка не называет адрес ячейки: %v", err)
	}
}

func TestCbChangesUndatedColumnWithValueFails(t *testing.T) {
	f := newEtfSheet(t, cbChangesSheet)
	setEtfDate(t, f, cbChangesSheet, "D8", cbDate(2026, time.August, 31))
	setEtfCell(t, f, cbChangesSheet, "B9", "Turkey")
	// Колонка E без заголовка-даты, но со значением: месяц не к чему привязать.
	setEtfCell(t, f, cbChangesSheet, "E9", 5)

	if _, err := parseCbChanges(f); err == nil {
		t.Fatal("значение в колонке без заголовка-даты должно давать ошибку")
	} else if !strings.Contains(err.Error(), "E9") {
		t.Errorf("ошибка не называет адрес ячейки: %v", err)
	}
}

func TestCbHoldingsMissingCountryWarns(t *testing.T) {
	f := newEtfSheet(t, cbHoldingsSheet)
	setEtfCell(t, f, cbHoldingsSheet, "C6", "Tonnes")
	setEtfCell(t, f, cbHoldingsSheet, "D6", "% of reserves**")
	setEtfCell(t, f, cbHoldingsSheet, "E6", "Holdings as of ")
	setEtfCell(t, f, cbHoldingsSheet, "A7", "1")
	setEtfCell(t, f, cbHoldingsSheet, "B7", "Poland, Rep. of")
	setEtfCell(t, f, cbHoldingsSheet, "C7", 647.98)
	setEtfDate(t, f, cbHoldingsSheet, "E7", cbDate(2026, time.August, 5))
	// Не наша страна в таблице: молча пропускается, это не сигнал — в топ-100 десятки стран.
	setEtfCell(t, f, cbHoldingsSheet, "A8", "2")
	setEtfCell(t, f, cbHoldingsSheet, "B8", "Germany")
	setEtfCell(t, f, cbHoldingsSheet, "C8", 3349.29)
	setEtfDate(t, f, cbHoldingsSheet, "E8", cbDate(2026, time.August, 5))

	var logged bytes.Buffer
	prevOut := log.StandardLogger().Out
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	points, err := parseCbHoldings(f)
	if err != nil {
		t.Fatalf("пропавшая в топ-100 страна словаря — warn, а не ошибка: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("точек %d, ожидалась 1 (только POL)", len(points))
	}
	pol := wgcCbSeriesName(cbMetricHoldingsTonnes, "POL")
	if got := cbValue(t, points, pol, cbDate(2026, time.August, 31)); math.Abs(got-647.98) > cbTolerance {
		t.Errorf("ряд %s = %v, ожидалось 647.98 (дата нормализуется monthEnd)", pol, got)
	}

	msg := logged.String()
	for _, missing := range []string{"Czech Rep.", "China, P.R.: Mainland", "Turkey"} {
		if !strings.Contains(msg, missing) {
			t.Errorf("нет warn о пропавшей стране %q: %q", missing, msg)
		}
	}
	if strings.Contains(msg, "Germany") {
		t.Errorf("страна вне словаря не должна попадать в warn: %q", msg)
	}
}

func TestCbHoldingsDuplicateCountryFails(t *testing.T) {
	f := newEtfSheet(t, cbHoldingsSheet)
	// Два блока с одной и той же страной: обе точки лягут на одну дату, и ReplacingMergeTree
	// оставит случайную — разбор должен упасть.
	for _, block := range []struct{ tonnes, name, value, asof string }{
		{"C6", "B7", "C7", "E7"},
		{"H6", "G7", "H7", "J7"},
	} {
		setEtfCell(t, f, cbHoldingsSheet, block.tonnes, "Tonnes")
		setEtfCell(t, f, cbHoldingsSheet, block.name, "Poland, Rep. of")
		setEtfCell(t, f, cbHoldingsSheet, block.value, 647.98)
		setEtfDate(t, f, cbHoldingsSheet, block.asof, cbDate(2026, time.August, 5))
	}

	if _, err := parseCbHoldings(f); err == nil {
		t.Fatal("дубль страны словаря должен давать ошибку")
	} else if !strings.Contains(err.Error(), "Poland, Rep. of") {
		t.Errorf("ошибка не называет страну: %v", err)
	}
}

func TestCbHoldingsWithoutTonnesHeaderFails(t *testing.T) {
	f := newEtfSheet(t, cbHoldingsSheet)
	// Подпись-заголовок листа есть, а заголовка блока Tonnes нет — раскладка изменилась.
	setEtfCell(t, f, cbHoldingsSheet, "A1", "WORLD OFFICIAL GOLD HOLDINGS")

	if _, err := parseCbHoldings(f); err == nil {
		t.Fatal("лист без заголовка Tonnes должен давать ошибку")
	} else if !strings.Contains(err.Error(), "Tonnes") {
		t.Errorf("ошибка не называет заголовок блока: %v", err)
	}
}

func TestCbHoldingsTextAsOfFails(t *testing.T) {
	f := newEtfSheet(t, cbHoldingsSheet)
	setEtfCell(t, f, cbHoldingsSheet, "C6", "Tonnes")
	setEtfCell(t, f, cbHoldingsSheet, "B7", "Poland, Rep. of")
	setEtfCell(t, f, cbHoldingsSheet, "C7", 647.98)
	// У страны словаря дата текстом («Mar 2025» бывает у стран со старыми данными): серийной
	// даты нет — точку датировать нечем, молча брать месяцы нельзя.
	setEtfCell(t, f, cbHoldingsSheet, "E7", "Mar 2025")

	if _, err := parseCbHoldings(f); err == nil {
		t.Fatal("текстовая дата Holdings as of у страны словаря должна давать ошибку")
	} else if !strings.Contains(err.Error(), "E7") {
		t.Errorf("ошибка не называет адрес ячейки: %v", err)
	}
}

func TestCbHoldingsEmptyTonnesFails(t *testing.T) {
	f := newEtfSheet(t, cbHoldingsSheet)
	setEtfCell(t, f, cbHoldingsSheet, "C6", "Tonnes")
	setEtfCell(t, f, cbHoldingsSheet, "B7", "Poland, Rep. of")
	setEtfDate(t, f, cbHoldingsSheet, "E7", cbDate(2026, time.August, 5))

	if _, err := parseCbHoldings(f); err == nil {
		t.Fatal("пустая колонка Tonnes у страны словаря должна давать ошибку")
	} else if !strings.Contains(err.Error(), "C7") {
		t.Errorf("ошибка не называет адрес ячейки: %v", err)
	}
}
