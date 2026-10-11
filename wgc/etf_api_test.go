package wgc

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// etfAPITestNow — фиксированное «сейчас» тестов: от него зависит только break-cache,
// поэтому разбор с ним детерминирован.
var etfAPITestNow = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

// etfAPITestdata читает фикстуру из testdata: тесты обязаны быть офлайн, а ответы WGC
// лежат файлами рядом с пакетом (усечены до трёх месяцев 2003-02-28, 2026-08-31, 2026-09-30).
func etfAPITestdata(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", name, err)
	}
	return body
}

// etfAPITestServer поднимает httptest-сервер с фикстурами: /flows-chart2 и /holdings-chart2
// отдают переданные тела (nil — файл из testdata), остальные пути — 404. Заодно пин break-cache:
// без него CDN после публикации ещё отдаёт прошлый месяц, и обновление молча теряется.
func etfAPITestServer(t *testing.T, flows, holdings []byte) *httptest.Server {
	t.Helper()
	if flows == nil {
		flows = etfAPITestdata(t, "etf_flows_api.json")
	}
	if holdings == nil {
		holdings = etfAPITestdata(t, "etf_holdings_api.json")
	}
	wantBreakCache := etfAPITestNow.Format(etfAPIBreakCacheFormat)

	mux := http.NewServeMux()
	mux.HandleFunc("/"+etfAPIFlowsPath, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("break-cache"); got != wantBreakCache {
			t.Errorf("break-cache = %q, ожидалось %q", got, wantBreakCache)
		}
		_, _ = w.Write(flows)
	})
	mux.HandleFunc("/"+etfAPIHoldingsPath, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("break-cache"); got != wantBreakCache {
			t.Errorf("break-cache = %q, ожидалось %q", got, wantBreakCache)
		}
		_, _ = w.Write(holdings)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// etfAPITestPoints разбирает фикстуры (nil — файлы testdata) через etfPointsFromAPI.
func etfAPITestPoints(t *testing.T, flows, holdings []byte) []seriesPoint {
	t.Helper()
	srv := etfAPITestServer(t, flows, holdings)
	points, err := etfPointsFromAPI(srv.URL+"/", etfAPITestNow)
	if err != nil {
		t.Fatalf("etfPointsFromAPI: %v", err)
	}
	return points
}

// etfPoint ищет точку ряда на дату; ok=false — точки нет.
func etfPoint(points []seriesPoint, series string, date time.Time) (float64, bool) {
	for _, p := range points {
		if p.Series == series && p.Date.Equal(date) {
			return p.Value, true
		}
	}
	return 0, false
}

// etfRequirePoint — значение точки ряда на дату; точки нет — падение теста.
func etfRequirePoint(t *testing.T, points []seriesPoint, series string, date time.Time) float64 {
	t.Helper()
	value, ok := etfPoint(points, series, date)
	if !ok {
		t.Fatalf("нет точки %s за %s", series, date.Format("2006-01-02"))
	}
	return value
}

// TestEtfPointsFromAPIRecentMonth пинит значения последнего месяца фикстуры (сверены
// с xlsx-историей через v_gold_etf_flows за 2026-09-30) и календарь дат: делители
// 1e6/1e9 и сдвиг даты от сырого ms-таймстемпа — самые незаметные ошибки разбора.
func TestEtfPointsFromAPIRecentMonth(t *testing.T) {
	points := etfAPITestPoints(t, nil, nil)
	date := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		series string
		value  float64
	}{
		{"etf_flow_usd_mln_asia", 2255.40929756},
		{"etf_flow_tonnes_asia", 16.18610771},
		// Источник отдаёт тоннажные стоки округлёнными до 0,01 т, поэтому здесь 541.88, а не
		// 541.88460453 из xlsx-суммы фондов: ожидание не «чинить» обратно (спека §3).
		{"etf_holdings_tonnes_asia", 541.88},
		{"etf_aum_usd_bn_asia", 75.594060865},
		{"etf_flow_tonnes_world", 67.28075855},
	}
	for _, c := range cases {
		got := etfRequirePoint(t, points, c.series, date)
		if math.Abs(got-c.value) > 1e-6 {
			t.Errorf("%s за %s = %v, ожидалось %v", c.series, date.Format("2006-01-02"), got, c.value)
		}
	}

	// world потоков в JSON нет: это сумма четырёх регионов (существующий etfWorldValue).
	var regionsSum float64
	for _, region := range []string{etfRegionNorthAmerica, etfRegionEurope, etfRegionAsia, etfRegionOther} {
		regionsSum += etfRequirePoint(t, points, wgcEtfSeriesName(etfMetricFlowTonnes, region), date)
	}
	world := etfRequirePoint(t, points, wgcEtfSeriesName(etfMetricFlowTonnes, etfWorldRegion), date)
	if math.Abs(world-regionsSum) > 1e-6 {
		t.Errorf("etf_flow_tonnes_world = %v, сумма четырёх регионов = %v", world, regionsSum)
	}

	// Фикстура усечена до трёх месяцев: любая другая дата — сдвиг от сырого ms-таймстемпа
	// (локальная таймзона) или потерянный фильтр.
	wantDates := []time.Time{
		time.Date(2003, 2, 28, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC),
		date,
	}
	for _, p := range points {
		if strings.Contains(strings.ToLower(p.Series), "gold price") {
			t.Errorf("ряд %q — ценовой (%s), он должен пропускаться по имени", p.Series, etfAPIPriceSeries)
		}
		// Конец месяца проверяем по календарю, а не через monthEnd: иначе утверждение было бы
		// тождественно разбору и не поймало бы сдвиг даты на день.
		if next := p.Date.AddDate(0, 0, 1); next.Day() != 1 {
			t.Errorf("%s: дата %s не последний день месяца", p.Series, p.Date.Format("2006-01-02"))
		}
		// И ровно UTC: сырой time.UnixMilli лежит в Local, так что эта проверка ловит разбор
		// без monthEnd и пиннит «результат не зависит от таймзоны машины».
		if p.Date.Location() != time.UTC {
			t.Errorf("%s: дата %s не в UTC — результат зависел бы от таймзоны машины", p.Series, p.Date)
		}
		if !slices.Contains(wantDates, p.Date) {
			t.Errorf("%s: неожиданная дата %s, фикстура содержит только %v", p.Series, p.Date.Format("2006-01-02"), wantDates)
		}
	}
}

// TestEtfPointsFromAPIWorldCountsExplicitZeros: в потоках нули явные (0 — это значение,
// а не пропуск), поэтому за 2003-02-28 у всех четырёх регионов есть точки, и world = 0.
func TestEtfPointsFromAPIWorldCountsExplicitZeros(t *testing.T) {
	points := etfAPITestPoints(t, nil, nil)
	date := time.Date(2003, 2, 28, 0, 0, 0, 0, time.UTC)

	for _, metric := range []string{etfMetricFlowUsdMln, etfMetricFlowTonnes} {
		for _, region := range []string{etfRegionNorthAmerica, etfRegionEurope, etfRegionAsia, etfRegionOther} {
			series := wgcEtfSeriesName(metric, region)
			if value := etfRequirePoint(t, points, series, date); value != 0 {
				t.Errorf("%s за %s = %v, ожидался явный ноль", series, date.Format("2006-01-02"), value)
			}
		}
		world := wgcEtfSeriesName(metric, etfWorldRegion)
		if value := etfRequirePoint(t, points, world, date); value != 0 {
			t.Errorf("%s за %s = %v, ожидался 0 (явные нули всех четырёх регионов)", world, date.Format("2006-01-02"), value)
		}
	}
}

// TestEtfPointsFromAPINullHoldingsGivesNoPoint: null в стоках — отсутствие данных, а не 0,
// поэтому точки нет и world за месяц не появляется; явный ноль северной Америки точкой остаётся.
func TestEtfPointsFromAPINullHoldingsGivesNoPoint(t *testing.T) {
	points := etfAPITestPoints(t, nil, nil)
	date := time.Date(2003, 2, 28, 0, 0, 0, 0, time.UTC)

	for _, series := range []string{
		"etf_holdings_tonnes_asia",
		"etf_aum_usd_bn_asia",
		"etf_holdings_tonnes_world",
		"etf_aum_usd_bn_world",
	} {
		if _, ok := etfPoint(points, series, date); ok {
			t.Errorf("null в стоках не должен давать точку %s за %s", series, date.Format("2006-01-02"))
		}
	}

	series := "etf_holdings_tonnes_north_america"
	if value := etfRequirePoint(t, points, series, date); value != 0 {
		t.Errorf("%s за %s = %v, ожидался явный ноль", series, date.Format("2006-01-02"), value)
	}
}

// TestEtfPointsFromAPIRejectsUnknownSeries: имя вне словаря регионов — ошибка, а не ряд
// вида etf_flow_tonnes_antarctica и не молчаливый пропуск (симметрия с xlsx-парсером).
// Метрика usd здесь полна, чтобы ошибку дал именно неизвестный ряд, а не guard полноты.
func TestEtfPointsFromAPIRejectsUnknownSeries(t *testing.T) {
	flows := []byte(`{"system":{"cached":false},"chartData":{"asOfDate":"2026-09-30",` +
		`"data":{"Monthly":{"series":{` +
		`"usd":[{"name":"North America","data":[[1790726400000,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,1]]},` +
		`{"name":"Other","data":[[1790726400000,1]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}],` +
		`"tonnes":[{"name":"Antarctica","data":[[1790726400000,1]]}]}}}}}`)
	srv := etfAPITestServer(t, flows, nil)

	_, err := etfPointsFromAPI(srv.URL+"/", etfAPITestNow)
	if err == nil {
		t.Fatal("ряд Antarctica вне словаря регионов должен давать ошибку")
	}
	if !strings.Contains(err.Error(), "Antarctica") {
		t.Errorf("ошибка %v не называет неизвестный ряд", err)
	}
}

// TestEtfPointsFromAPINonOKStatusFails: 403 должен стать ошибкой с кодом статуса (уход
// в фолбэк в Task 3), а не «нулём точек» с зелёным dagu.
func TestEtfPointsFromAPINonOKStatusFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "<html>blocked</html>")
	}))
	t.Cleanup(srv.Close)

	_, err := etfPointsFromAPI(srv.URL+"/", etfAPITestNow)
	if err == nil {
		t.Fatal("403 должен давать ошибку, а не пустой разбор")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("ошибка %v не содержит код 403", err)
	}
}

// TestEtfPointsFromAPIEmptyDataFails: валидный конверт без данных — ошибка (fail-fast):
// тихий «0 точек» оставил бы dagu зелёным, а ряды — без обновления. Набор метрик и регионов
// здесь полон (обе метрики, по четыре региона, без точек), поэтому тест доходит именно
// до финальной проверки пустого разбора, а не до guard'а полноты.
func TestEtfPointsFromAPIEmptyDataFails(t *testing.T) {
	flows := []byte(`{"system":{"cached":false},"chartData":{"asOfDate":"2026-09-30",` +
		`"data":{"Monthly":{"series":{` +
		`"usd":[{"name":"North America"},{"name":"Europe"},{"name":"Asia"},{"name":"Other"},{"name":"Gold Price (rhs)"}],` +
		`"tonnes":[{"name":"North America"},{"name":"Europe"},{"name":"Asia"},{"name":"Other"},{"name":"Gold Price (rhs)"}]}}}}}`)
	holdings := []byte(`{"system":{"cached":false},"chartData":{"asOfDate":"2026-09-30",` +
		`"data":{"Monthly":{` +
		`"tonnes":{"columns":["Date","North America","Europe","Asia","Other","Gold, US$/oz"],"set":[]},` +
		`"usd":{"columns":["Date","North America","Europe","Asia","Other","Gold, US$/oz"],"set":[]}}}}}`)
	srv := etfAPITestServer(t, flows, holdings)

	_, err := etfPointsFromAPI(srv.URL+"/", etfAPITestNow)
	if err == nil {
		t.Fatal("выгрузка без точек должна давать ошибку")
	}
	if !strings.Contains(err.Error(), "без точек") {
		t.Errorf("ошибка %v не говорит о пустом разборе", err)
	}
}

// TestEtfPointsFromAPIMissingMetricFails: ответ без ключа usd — ошибка, а не частичный импорт:
// иначе все долларовые потоки молча остались бы без обновления, а dagu — зелёным.
func TestEtfPointsFromAPIMissingMetricFails(t *testing.T) {
	flows := []byte(`{"system":{"cached":false},"chartData":{"asOfDate":"2026-09-30",` +
		`"data":{"Monthly":{"series":{"tonnes":[` +
		`{"name":"North America","data":[[1790726400000,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,1]]},` +
		`{"name":"Other","data":[[1790726400000,1]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}]}}}}}`)
	srv := etfAPITestServer(t, flows, nil)

	_, err := etfPointsFromAPI(srv.URL+"/", etfAPITestNow)
	if err == nil {
		t.Fatal("ответ без ключа usd должен давать ошибку, а не импорт одной метрики")
	}
	// Текст пинит именно проверку наличия ключа: полнота регионов у nil-метрики тоже дала бы
	// ошибку, но менее внятную («распознано 0 из 4 регионов» вместо «нет метрики usd»).
	if !strings.Contains(err.Error(), "нет метрики usd") {
		t.Errorf("ошибка %v не называет пропавшую метрику usd", err)
	}
}

// TestEtfPointsFromAPIIncompleteRegionSetFails: три региона из четырёх у метрики — ошибка,
// а не частичный импорт: пропавший регион молча перестал бы обновляться (пятый, ценовой
// ряд Gold Price (rhs), регионом не считается и полноту не восполняет).
func TestEtfPointsFromAPIIncompleteRegionSetFails(t *testing.T) {
	flows := []byte(`{"system":{"cached":false},"chartData":{"asOfDate":"2026-09-30",` +
		`"data":{"Monthly":{"series":{` +
		`"usd":[{"name":"North America","data":[[1790726400000,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Other","data":[[1790726400000,1]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}],` +
		`"tonnes":[{"name":"North America","data":[[1790726400000,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,1]]},` +
		`{"name":"Other","data":[[1790726400000,1]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}]}}}}}`)
	srv := etfAPITestServer(t, flows, nil)

	_, err := etfPointsFromAPI(srv.URL+"/", etfAPITestNow)
	if err == nil {
		t.Fatal("метрика с тремя регионами должна давать ошибку, а не частичный импорт")
	}
	if !strings.Contains(err.Error(), "usd") || !strings.Contains(err.Error(), "3 из 4 регионов") {
		t.Errorf("ошибка %v не объясняет неполноту набора регионов метрики usd", err)
	}
}

// TestEtfPointsFromAPINullFlowValueGivesNoPoint: null в значении потока — отсутствие данных,
// а не нулевой поток: точки у региона нет вовсе, world за этот месяц не появляется (для него
// нужны все четыре региона), а явный 0 в том же месяце точкой остаётся.
func TestEtfPointsFromAPINullFlowValueGivesNoPoint(t *testing.T) {
	flows := []byte(`{"system":{"cached":false},"chartData":{"asOfDate":"2026-09-30",` +
		`"data":{"Monthly":{"series":{` +
		`"usd":[{"name":"North America","data":[[1790726400000,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,1]]},` +
		`{"name":"Other","data":[[1790726400000,1]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}],` +
		`"tonnes":[{"name":"North America","data":[[1790726400000,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,null]]},` +
		`{"name":"Other","data":[[1790726400000,0]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}]}}}}}`)
	srv := etfAPITestServer(t, flows, nil)

	points, err := etfPointsFromAPI(srv.URL+"/", etfAPITestNow)
	if err != nil {
		t.Fatalf("etfPointsFromAPI: %v", err)
	}
	date := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)

	if _, ok := etfPoint(points, "etf_flow_tonnes_asia", date); ok {
		t.Error("null в значении потока не должен давать точку etf_flow_tonnes_asia")
	}
	if _, ok := etfPoint(points, "etf_flow_tonnes_world", date); ok {
		t.Error("без Азии world за месяц неполон и точки быть не должно")
	}
	for _, series := range []string{"etf_flow_tonnes_north_america", "etf_flow_tonnes_europe"} {
		if value := etfRequirePoint(t, points, series, date); value != 1 {
			t.Errorf("%s = %v, ожидалось 1", series, value)
		}
	}
	// Явный 0 строкой выше — значение, а не пропуск: точка есть.
	if value := etfRequirePoint(t, points, "etf_flow_tonnes_other", date); value != 0 {
		t.Errorf("etf_flow_tonnes_other = %v, ожидался явный ноль точкой", value)
	}
	// Пропуск null не задевает вторую метрику: у неё набор полон, world на месте.
	if _, ok := etfPoint(points, "etf_flow_usd_mln_world", date); !ok {
		t.Error("пропуск null в tonnes не должен задевать usd: у полной метрики world обязан быть")
	}
}

// TestEtfPointsFromAPINullFlowDateFails: значение есть, а дата null — ошибка, а не точка
// 1970-01-31 из monthEnd(UnixMilli(0)): такая точка легла бы в ряд как настоящий месяц.
func TestEtfPointsFromAPINullFlowDateFails(t *testing.T) {
	flows := []byte(`{"system":{"cached":false},"chartData":{"asOfDate":"2026-09-30",` +
		`"data":{"Monthly":{"series":{` +
		`"usd":[{"name":"North America","data":[[null,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,1]]},` +
		`{"name":"Other","data":[[1790726400000,1]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}],` +
		`"tonnes":[{"name":"North America","data":[[1790726400000,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,1]]},` +
		`{"name":"Other","data":[[1790726400000,1]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}]}}}}}`)
	srv := etfAPITestServer(t, flows, nil)

	_, err := etfPointsFromAPI(srv.URL+"/", etfAPITestNow)
	if err == nil {
		t.Fatal("значение без даты (null вместо ms) должно давать ошибку")
	}
	if !strings.Contains(err.Error(), "без даты") {
		t.Errorf("ошибка %v не объясняет, что у точки нет даты", err)
	}
}

// TestEtfPointsFromAPIDuplicateRegionFails: два ряда одного региона в одной метрике —
// противоречивый набор: без ошибки второй ряд молча перезаписал бы значение первого
// (набор при этом выглядит полным, потому что регионов по-прежнему четыре).
func TestEtfPointsFromAPIDuplicateRegionFails(t *testing.T) {
	flows := []byte(`{"system":{"cached":false},"chartData":{"asOfDate":"2026-09-30",` +
		`"data":{"Monthly":{"series":{` +
		`"usd":[{"name":"North America","data":[[1790726400000,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,1]]},` +
		`{"name":"Other","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,2]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}],` +
		`"tonnes":[{"name":"North America","data":[[1790726400000,1]]},` +
		`{"name":"Europe","data":[[1790726400000,1]]},` +
		`{"name":"Asia","data":[[1790726400000,1]]},` +
		`{"name":"Other","data":[[1790726400000,1]]},` +
		`{"name":"Gold Price (rhs)","data":[[1790726400000,1]]}]}}}}}`)
	srv := etfAPITestServer(t, flows, nil)

	_, err := etfPointsFromAPI(srv.URL+"/", etfAPITestNow)
	if err == nil {
		t.Fatal("дубликат ряда региона должен давать ошибку, а не перезапись значения")
	}
	if !strings.Contains(err.Error(), "встречается дважды") || !strings.Contains(err.Error(), "Asia") {
		t.Errorf("ошибка %v не объясняет дубликат региона Asia", err)
	}
}
