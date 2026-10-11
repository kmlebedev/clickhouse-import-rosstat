package wgc

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	etfFixturePath = "testdata/ETF_Flows_2026-10-02_1401.xlsx"

	// etfTolerance — допуск сверки значений, посчитанных из фикстуры.
	etfTolerance = 1e-6

	// etfChartRoundedTolerance — допуск для блока «Area chart - monthly holdings in tonnes»:
	// в фикстуре он хранит сток, округлённый до 2 знаков (2115.88 против 2115.88373719 в листах
	// фондов), поэтому сверка идёт с половиной последнего знака, а не с etfTolerance.
	etfChartRoundedTolerance = 5e-3

	// etfChartUsdToMln — делитель для сверки блоков «Charts Data» со знаком US$: они хранят
	// абсолютные доллары, а фондовые листы — млн долларов.
	etfChartUsdToMln = 1e6

	// etfChartsOverlapMonths — месяцы (2025-06..2026-09), на которых пересекаются все три
	// сверочных блока «Charts Data»: месячные блоки потоков содержат ровно 16 строк.
	etfChartsOverlapMonths = 16
)

// openEtfFixture открывает фикстуру выгрузки ETF (закрывается после теста).
func openEtfFixture(t *testing.T) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(etfFixturePath)
	if err != nil {
		t.Fatalf("excelize.OpenFile(%s): %v", etfFixturePath, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// etfDate — конец месяца в UTC (даты точек парсеров).
func etfDate(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// etfPointsByName индексирует точки по имени ряда.
func etfPointsByName(points []seriesPoint) map[string][]seriesPoint {
	byName := make(map[string][]seriesPoint, len(wgcEtfMetrics)*len(wgcEtfRegions))
	for _, p := range points {
		byName[p.Series] = append(byName[p.Series], p)
	}
	return byName
}

// etfValue — значение ряда на дату; отсутствие точки — ошибка теста.
func etfValue(t *testing.T, byName map[string][]seriesPoint, series string, date time.Time) float64 {
	t.Helper()
	for _, p := range byName[series] {
		if p.Date.Equal(date) {
			return p.Value
		}
	}
	t.Fatalf("ряд %s: нет точки на %s", series, date.Format("2006-01-02"))
	return 0
}

func TestEtfMonthlyValuesPinned(t *testing.T) {
	points, err := parseEtfWorkbook(openEtfFixture(t))
	if err != nil {
		t.Fatalf("parseEtfWorkbook: %v", err)
	}
	byName := etfPointsByName(points)

	sep26 := etfDate(2026, time.September, 30)
	cases := []struct {
		series string
		want   float64
	}{
		{"etf_flow_usd_mln_world", 9952.21564139},
		{"etf_holdings_tonnes_world", 4255.66981354},
		{"etf_aum_usd_bn_world", 574.23391418},
		{"etf_flow_usd_mln_north_america", 3961.93761905},
		{"etf_flow_usd_mln_europe", 3630.56023891},
		{"etf_flow_usd_mln_asia", 2255.40929756},
		{"etf_flow_usd_mln_other", 104.30848587},
		{"etf_holdings_tonnes_north_america", 2115.88373719},
		{"etf_holdings_tonnes_europe", 1521.36429276},
		{"etf_holdings_tonnes_asia", 541.88460453},
		{"etf_holdings_tonnes_other", 76.53717906},
	}
	for _, tc := range cases {
		got := etfValue(t, byName, tc.series, sep26)
		if math.Abs(got-tc.want) > etfTolerance {
			t.Errorf("%s на 2026-09-30 = %v, ожидалось %v", tc.series, got, tc.want)
		}
	}
}

func TestEtfAumRegionValuesPinned(t *testing.T) {
	points, err := parseEtfWorkbook(openEtfFixture(t))
	if err != nil {
		t.Fatalf("parseEtfWorkbook: %v", err)
	}
	byName := etfPointsByName(points)

	sep26 := etfDate(2026, time.September, 30)
	cases := []struct {
		series string
		want   float64
	}{
		{"etf_aum_usd_bn_north_america", 284.088446499},
		{"etf_aum_usd_bn_europe", 204.275334589},
		{"etf_aum_usd_bn_asia", 75.594060865},
		{"etf_aum_usd_bn_other", 10.276072228},
	}
	for _, tc := range cases {
		got := etfValue(t, byName, tc.series, sep26)
		if math.Abs(got-tc.want) > etfTolerance {
			t.Errorf("%s на 2026-09-30 = %v, ожидалось %v", tc.series, got, tc.want)
		}
	}
}

func TestEtfSeriesCoverage(t *testing.T) {
	points, err := parseEtfWorkbook(openEtfFixture(t))
	if err != nil {
		t.Fatalf("parseEtfWorkbook: %v", err)
	}
	byName := etfPointsByName(points)

	for _, metric := range wgcEtfMetrics {
		for _, region := range wgcEtfRegions {
			if name := wgcEtfSeriesName(metric, region); len(byName[name]) == 0 {
				t.Errorf("ряд %s отсутствует", name)
			}
		}
	}
	if want := len(wgcEtfMetrics) * len(wgcEtfRegions); len(byName) != want {
		t.Errorf("рядов %d, ожидалось %d", len(byName), want)
	}

	// Счётчики и первые даты — факт фикстуры, а не ожидания брифа (≥283/≥284 точек и первая
	// дата 2003-02-28): 283 месяца и 2003-02-28 — это полное покрытие листа и блока AUM,
	// тогда как world пишется только при значениях всех четырёх регионов (§3.1), а Азия
	// появляется лишь в 2007-03 — поэтому у world 235 точек с 2007-03-31.
	cases := []struct {
		series    string
		wantCount int
		wantFirst time.Time
	}{
		{"etf_flow_usd_mln_north_america", 283, etfDate(2003, time.March, 31)},
		{"etf_aum_usd_bn_north_america", 284, etfDate(2003, time.February, 28)},
		{"etf_flow_usd_mln_world", 235, etfDate(2007, time.March, 31)},
		{"etf_holdings_tonnes_world", 235, etfDate(2007, time.March, 31)},
		{"etf_aum_usd_bn_world", 235, etfDate(2007, time.March, 31)},
	}
	for _, tc := range cases {
		got := byName[tc.series]
		if len(got) != tc.wantCount {
			t.Errorf("ряд %s: точек %d, ожидалось %d", tc.series, len(got), tc.wantCount)
		}
		if len(got) == 0 {
			continue
		}
		if first := got[0].Date.Format("2006-01-02"); first != tc.wantFirst.Format("2006-01-02") {
			t.Errorf("ряд %s: первая дата %s, ожидалась %s", tc.series, first, tc.wantFirst.Format("2006-01-02"))
		}
	}

	// Даты точек — конец отчётного месяца в UTC (конвенция проекта).
	for _, p := range points {
		if p.Date.Location() != time.UTC || !p.Date.Equal(monthEnd(p.Date)) {
			t.Fatalf("точка %s: дата %v не конец месяца в UTC", p.Series, p.Date)
		}
	}
}

func TestEtfChartsMatchFundSums(t *testing.T) {
	f := openEtfFixture(t)
	fundPoints, err := parseEtfWorkbook(f)
	if err != nil {
		t.Fatalf("parseEtfWorkbook: %v", err)
	}
	fund := etfPointsByName(fundPoints)

	// Окно сверки — 16 месяцев месячных блоков потоков (2025-06..2026-09); на них же
	// сверяется блок запасов в тоннах, который сам тянется с 2003-02.
	flowsUsd, err := parseChartsBlock(f, "Bar chart - monthly flows in US$", etfMetricFlowUsdMln)
	if err != nil {
		t.Fatalf("parseChartsBlock(monthly flows in US$): %v", err)
	}
	window := make(map[time.Time]bool, etfChartsOverlapMonths)
	for _, p := range flowsUsd {
		window[p.Date] = true
	}
	if len(window) != etfChartsOverlapMonths {
		t.Fatalf("окно сверки: %d месяцев, ожидалось %d", len(window), etfChartsOverlapMonths)
	}

	blocks := []struct {
		header string
		metric string
		scale  float64
		tol    float64
	}{
		{"Bar chart - monthly flows in US$", etfMetricFlowUsdMln, etfChartUsdToMln, etfTolerance},
		{"Bar chart - monthly flows in tonnes", etfMetricFlowTonnes, 1, etfTolerance},
		{"Area chart - monthly holdings in tonnes", etfMetricHoldingsTonnes, 1, etfChartRoundedTolerance},
	}
	for _, b := range blocks {
		charts, err := parseChartsBlock(f, b.header, b.metric)
		if err != nil {
			t.Fatalf("parseChartsBlock(%q): %v", b.header, err)
		}
		byName := etfPointsByName(charts)
		for _, region := range wgcEtfRegions {
			if region == etfWorldRegion {
				continue
			}
			series := wgcEtfSeriesName(b.metric, region)
			fundByDate := make(map[time.Time]float64, len(fund[series]))
			for _, p := range fund[series] {
				fundByDate[p.Date] = p.Value
			}
			compared := 0
			for _, cp := range byName[series] {
				if !window[cp.Date] {
					continue
				}
				month := cp.Date.Format("2006-01-02")
				fv, ok := fundByDate[cp.Date]
				if !ok {
					t.Errorf("ряд %s: нет суммы фондов на %s", series, month)
					continue
				}
				compared++
				if diff := math.Abs(cp.Value/b.scale - fv); diff > b.tol {
					t.Errorf("ряд %s на %s: Charts %v, сумма фондов %v (|Δ| = %v > %v)", series, month, cp.Value/b.scale, fv, diff, b.tol)
				}
			}
			if compared != len(window) {
				t.Errorf("ряд %s: сверено месяцев %d, ожидалось %d", series, compared, len(window))
			}
		}
	}
}

// setEtfCell пишет значение ячейки синтетического листа.
func setEtfCell(t *testing.T, f *excelize.File, sheet, ref string, value any) {
	t.Helper()
	if err := f.SetCellValue(sheet, ref, value); err != nil {
		t.Fatalf("SetCellValue(%s!%s): %v", sheet, ref, err)
	}
}

// setEtfDate пишет дату синтетического листа как Excel-serial (1899-12-30 + дни) — ровно так,
// как её пишет выгрузка WGC.
func setEtfDate(t *testing.T, f *excelize.File, sheet, ref string, date time.Time) {
	t.Helper()
	base := time.Date(1899, time.December, 30, 0, 0, 0, 0, time.UTC)
	setEtfCell(t, f, sheet, ref, math.Round(date.Sub(base).Hours()/24))
}

// newEtfSheet создаёт однолистовой синтетический файл и возвращает открытый лист.
func newEtfSheet(t *testing.T, sheet string) *excelize.File {
	t.Helper()
	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	if err := f.SetSheetName("Sheet1", sheet); err != nil {
		t.Fatalf("SetSheetName(%s): %v", sheet, err)
	}
	return f
}

func TestEtfBlankCellIsNotZero(t *testing.T) {
	f := newEtfSheet(t, etfHoldingsSheet)
	// Шапка листа фондов: строка 4 — регион, строка 6 — имя фонда; данные — со строки 7.
	for ref, value := range map[string]string{
		"F4": "North America", "G4": "North America", "H4": "Europe", "I4": " Asia ", "J4": "Other",
		"F6": "Fund A", "G6": "Fund B", "H6": "Fund C", "I6": "Fund D", "J6": "Fund E",
	} {
		setEtfCell(t, f, etfHoldingsSheet, ref, value)
	}
	// (а) у Fund B ячейка пуста — вклад пропускается, а регион считается по присутствующим;
	// заодно регион « Asia » распознаётся с пробелами по краям.
	setEtfDate(t, f, etfHoldingsSheet, "A7", etfDate(2026, time.January, 31))
	for ref, value := range map[string]float64{"F7": 1.5, "H7": 2, "I7": 3, "J7": 4} {
		setEtfCell(t, f, etfHoldingsSheet, ref, value)
	}
	// (б) месяц, где пусто у всех фондов: точек нет ни у регионов, ни у world.
	setEtfDate(t, f, etfHoldingsSheet, "A8", etfDate(2026, time.February, 28))
	// (в) явные нули — значения, а не пустые ячейки: точки (в том числе world = 0) пишутся.
	setEtfDate(t, f, etfHoldingsSheet, "A9", etfDate(2026, time.March, 31))
	for _, ref := range []string{"F9", "H9", "I9", "J9"} {
		setEtfCell(t, f, etfHoldingsSheet, ref, 0)
	}

	points, err := parseFundSheet(f, etfHoldingsSheet, etfMetricHoldingsTonnes)
	if err != nil {
		t.Fatalf("parseFundSheet: %v", err)
	}
	byName := etfPointsByName(points)

	jan := etfDate(2026, time.January, 31)
	if got := etfValue(t, byName, "etf_holdings_tonnes_north_america", jan); got != 1.5 {
		t.Errorf("пустая ячейка Fund B: north_america = %v, ожидалось 1.5 (сумма присутствующих)", got)
	}
	if got := etfValue(t, byName, "etf_holdings_tonnes_world", jan); got != 10.5 {
		t.Errorf("world = %v, ожидалось 10.5", got)
	}

	feb := etfDate(2026, time.February, 28)
	for series, pts := range byName {
		for _, p := range pts {
			if p.Date.Equal(feb) {
				t.Errorf("месяц без данных фондов: точка %s = %v, ожидалось отсутствие", series, p.Value)
			}
		}
	}

	mar := etfDate(2026, time.March, 31)
	for _, region := range []string{"north_america", "europe", "asia", "other", "world"} {
		if got := etfValue(t, byName, wgcEtfSeriesName(etfMetricHoldingsTonnes, region), mar); got != 0 {
			t.Errorf("явный ноль: %s = %v, ожидалось 0", region, got)
		}
	}
}

func TestEtfUnknownRegionFails(t *testing.T) {
	f := newEtfSheet(t, etfFlowsSheet)
	setEtfCell(t, f, etfFlowsSheet, "F4", "Mars")
	setEtfCell(t, f, etfFlowsSheet, "F6", "Fund X")
	setEtfDate(t, f, etfFlowsSheet, "A7", etfDate(2026, time.January, 31))
	setEtfCell(t, f, etfFlowsSheet, "F7", 1)

	_, err := parseEtfWorkbook(f)
	if err == nil {
		t.Fatal("parseEtfWorkbook с регионом «Mars» должен вернуть ошибку")
	}
	if !strings.Contains(err.Error(), "Mars") {
		t.Errorf("ошибка не упоминает регион фонда: %v", err)
	}
}

func TestEtfUndatedFundRowFails(t *testing.T) {
	f := newEtfSheet(t, etfFlowsSheet)
	setEtfCell(t, f, etfFlowsSheet, "F4", "North America")
	setEtfCell(t, f, etfFlowsSheet, "F6", "Fund A")
	setEtfDate(t, f, etfFlowsSheet, "A7", etfDate(2026, time.January, 31))
	setEtfCell(t, f, etfFlowsSheet, "F7", 1)
	// Значение фонда в строке без даты (колонка A пуста): значение потерялось бы молча.
	setEtfCell(t, f, etfFlowsSheet, "F8", 5)

	_, err := parseFundSheet(f, etfFlowsSheet, etfMetricFlowUsdMln)
	if err == nil {
		t.Fatal("строка без даты со значением фонда должна давать ошибку")
	}
	if !strings.Contains(err.Error(), "F8") {
		t.Errorf("ошибка не называет адрес ячейки: %v", err)
	}
}

func TestEtfRegionlessFundColumnFails(t *testing.T) {
	f := newEtfSheet(t, etfFlowsSheet)
	setEtfCell(t, f, etfFlowsSheet, "F4", "North America")
	setEtfCell(t, f, etfFlowsSheet, "F6", "Fund A")
	setEtfCell(t, f, etfFlowsSheet, "G6", "Fund B")
	setEtfDate(t, f, etfFlowsSheet, "A7", etfDate(2026, time.January, 31))
	setEtfCell(t, f, etfFlowsSheet, "F7", 1)
	// Регион колонки G в строке 4 не заполнен, но значения в колонке есть: фонд выпал бы молча.
	setEtfCell(t, f, etfFlowsSheet, "G7", 2)

	_, err := parseFundSheet(f, etfFlowsSheet, etfMetricFlowUsdMln)
	if err == nil {
		t.Fatal("колонка со значением фонда без региона должна давать ошибку")
	}
	if !strings.Contains(err.Error(), "G7") {
		t.Errorf("ошибка не называет адрес ячейки: %v", err)
	}
}

func TestEtfUndatedChartsRowFails(t *testing.T) {
	f := newEtfSheet(t, etfChartsSheet)
	setEtfCell(t, f, etfChartsSheet, "A1", "Bar chart - monthly flows in US$")
	for ref, value := range map[string]string{
		"A2": "Date", "B2": "North America", "C2": "Europe", "D2": "Asia", "E2": "Other",
	} {
		setEtfCell(t, f, etfChartsSheet, ref, value)
	}
	setEtfDate(t, f, etfChartsSheet, "A3", etfDate(2026, time.January, 31))
	setEtfCell(t, f, etfChartsSheet, "B3", 1)
	// Значение региона в строке блока без даты: значение потерялось бы молча.
	setEtfCell(t, f, etfChartsSheet, "B4", 2)

	_, err := parseChartsBlock(f, "Bar chart - monthly flows in US$", etfMetricFlowUsdMln)
	if err == nil {
		t.Fatal("строка блока без даты со значением региона должна давать ошибку")
	}
	if !strings.Contains(err.Error(), "B4") {
		t.Errorf("ошибка не называет адрес ячейки: %v", err)
	}
}

func TestEtfHoldingsTotalMismatchWarns(t *testing.T) {
	f := newEtfSheet(t, etfHoldingsSheet)
	for ref, value := range map[string]string{
		"F4": "North America", "G4": "Europe", "H4": "Asia", "I4": "Other",
		"F6": "Fund A", "G6": "Fund B", "H6": "Fund C", "I6": "Fund D",
	} {
		setEtfCell(t, f, etfHoldingsSheet, ref, value)
	}
	setEtfDate(t, f, etfHoldingsSheet, "A7", etfDate(2026, time.January, 31))
	for ref, value := range map[string]float64{"F7": 1, "G7": 2, "H7": 3, "I7": 4} {
		setEtfCell(t, f, etfHoldingsSheet, ref, value)
	}
	// Итог листа (D) расходится с суммой фондов: это warn, а не ошибка, и точка берётся
	// из суммы фондов, а не из D.
	setEtfCell(t, f, etfHoldingsSheet, "D7", 999)

	var logged bytes.Buffer
	prevOut := log.StandardLogger().Out
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	points, err := parseFundSheet(f, etfHoldingsSheet, etfMetricHoldingsTonnes)
	if err != nil {
		t.Fatalf("расхождение итога колонки D не должно быть ошибкой: %v", err)
	}
	byName := etfPointsByName(points)
	jan := etfDate(2026, time.January, 31)
	if got := etfValue(t, byName, "etf_holdings_tonnes_north_america", jan); got != 1 {
		t.Errorf("north_america = %v, ожидалось 1 (сумма фондов, а не итог D)", got)
	}
	if got := etfValue(t, byName, "etf_holdings_tonnes_world", jan); got != 10 {
		t.Errorf("world = %v, ожидалось 10", got)
	}
	if msg := logged.String(); !strings.Contains(msg, "расхождение") {
		t.Errorf("расхождение итога колонки D не залогировано: %q", msg)
	}
}

func TestCellDateMonthEnd(t *testing.T) {
	got, err := cellDate("46295")
	if err != nil {
		t.Fatalf("cellDate(46295): %v", err)
	}
	if want := etfDate(2026, time.September, 30); !got.Equal(want) {
		t.Errorf("cellDate(46295) = %v, ожидалось %v UTC", got, want)
	}
	if got, want := monthEnd(etfDate(2026, time.August, 30)), etfDate(2026, time.August, 31); !got.Equal(want) {
		t.Errorf("monthEnd(2026-08-30) = %v, ожидалось %v", got, want)
	}
	for _, raw := range []string{"", "не serial"} {
		if _, err := cellDate(raw); err == nil {
			t.Errorf("cellDate(%q) должен вернуть ошибку", raw)
		}
	}
}

func TestCellFloatEmptyVsJunk(t *testing.T) {
	if v, ok, err := cellFloat(""); err != nil || ok || v != 0 {
		t.Errorf(`cellFloat("") = (%v, %v, %v), ожидалось (0, false, nil)`, v, ok, err)
	}
	if v, ok, err := cellFloat("1.5"); err != nil || !ok || v != 1.5 {
		t.Errorf(`cellFloat("1.5") = (%v, %v, %v), ожидалось (1.5, true, nil)`, v, ok, err)
	}
	if _, _, err := cellFloat("n/a"); err == nil {
		t.Error(`cellFloat("n/a") должен вернуть ошибку`)
	}
}
