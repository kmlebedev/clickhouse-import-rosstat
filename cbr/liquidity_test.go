package cbr

import (
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// cbrLiquidityFixtureBody читает сохранённую страницу формы: обе фикстуры — живые
// ответы cbr.ru (дата скачивания 2026-11-14), поэтому тест не ходит в сеть.
func cbrLiquidityFixtureBody(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", path, err)
	}
	return body
}

// cbrLiquidityMutated — копия живой фикстуры в памяти с одной заменой: ветки fail-fast
// проверяются на реальной разметке (стандарт §11.1), а файл фикстуры не меняется.
func cbrLiquidityMutated(t *testing.T, path, old, new string) []byte {
	t.Helper()
	body := string(cbrLiquidityFixtureBody(t, path))
	if !strings.Contains(body, old) {
		t.Fatalf("в фикстуре %s нет фрагмента %q — мутация не применилась", path, old)
	}
	return []byte(strings.Replace(body, old, new, 1))
}

// cbrLiquidityValues — значения всех рядов за одну дату: spot-проверки не зависят от
// порядка строк в форме (ЦБ отдаёт их от свежей к старой).
func cbrLiquidityValues(points []cbrLiquidityPoint, date time.Time) map[string]float64 {
	values := map[string]float64{}
	for _, point := range points {
		if point.date.Equal(date) {
			values[point.series] = point.value
		}
	}
	return values
}

// cbrLiquidityCheckSpot сверяет все spot-значения ряда и требует, чтобы рядов было ровно
// столько же, сколько задано: пропущенный ряд — тоже сдвиг привязки колонок.
func cbrLiquidityCheckSpot(t *testing.T, values, want map[string]float64, label string) {
	t.Helper()
	for series, value := range want {
		got, ok := values[series]
		if !ok {
			t.Errorf("%s: ряда %q нет", label, series)
			continue
		}
		if got != value {
			t.Errorf("%s: %s = %v, want %v", label, series, got, value)
		}
	}
	if len(values) != len(want) {
		t.Errorf("%s: рядов %d, want %d", label, len(values), len(want))
	}
}

// cbrLiquidityCheckIdentity сверяет тождество формы с допуском на округление слагаемых
// источника (0,1 млрд руб. на слагаемое).
func cbrLiquidityCheckIdentity(t *testing.T, got, want float64, formula string) {
	t.Helper()
	if math.Abs(got-want) > 0.2 {
		t.Errorf("тождество %s не сходится: %v, want %v", formula, got, want)
	}
}

// TestCbrLiquidityDailyFixture: дневная форма 01.01.2014..31.01.2014 разбирается в
// 18 дней × 14 рядов = 252 точки; пустых значений в январской выгрузке нет, поэтому
// короткая строка формы не съедает ряд. Spot-значения заданы для всех 14 рядов: если
// колонки формы поменяются местами (например, deposit_auction ↔ deposit_standing),
// разойдётся хотя бы одно значение, даже когда нумерация шапки осталась на месте.
func TestCbrLiquidityDailyFixture(t *testing.T) {
	points, err := cbrLiquidityRows(cbrLiquidityFixtureBody(t, "testdata/liquidity_daily.html"), cbrLiquidityKindDaily, cbrLiquidityDailySeries)
	if err != nil {
		t.Fatalf("разбор дневной формы: %v", err)
	}
	if len(points) != 252 {
		t.Fatalf("точек %d, want 252 (18 дней × 14 рядов)", len(points))
	}
	dates := map[string]struct{}{}
	for _, point := range points {
		// Литерал, а не константа: сравнивать вид с cbrLiquidityKindDaily значило бы
		// проверять константу ею же.
		if point.kind != "daily" {
			t.Errorf("вид точки %q, want %q", point.kind, "daily")
		}
		dates[point.date.Format("2006-01-02")] = struct{}{}
	}
	if len(dates) != 18 {
		t.Errorf("дат %d, want 18", len(dates))
	}

	// Значения последнего дня выгрузки (01.01.2014) — из ячеек формы, по одной на каждый
	// ряд. Перестановка любых двух колонок ломает хотя бы одно значение.
	values := cbrLiquidityValues(points, time.Date(2014, time.January, 1, 0, 0, 0, 0, time.UTC))
	cbrLiquidityCheckSpot(t, values, map[string]float64{
		"deficit":                         3481.8,
		"deficit_ex_corr":                 3957.7,
		"cb_claims":                       4471.8,
		"cb_claims_auction_repo":          2883.4,
		"cb_claims_auction_loans":         590.2,
		"cb_claims_standing_repo":         292.7,
		"cb_claims_standing_loans":        705.4,
		"cb_liabilities":                  517.6,
		"cb_liabilities_deposit_auction":  0,
		"cb_liabilities_deposit_standing": 517.6,
		"cb_liabilities_kobr":             0,
		"nonstandard_repo":                3.6,
		"corr_accounts":                   1270,
		"required_reserves":               794.1,
	}, "2014-01-01")

	// Балансовые тождества формы (нумерация колонок — нижняя строка шапки): требования ЦБ
	// равны сумме четырёх инструментов, обязательства — сумме депозитов аукционов,
	// депозитов постоянного действия и КОБР. Ошибка привязки колонок ломает тождество.
	cbrLiquidityCheckIdentity(t,
		values["cb_claims_auction_repo"]+values["cb_claims_auction_loans"]+
			values["cb_claims_standing_repo"]+values["cb_claims_standing_loans"],
		values["cb_claims"], "4 = 5 + 6 + 7 + 8")
	cbrLiquidityCheckIdentity(t,
		values["cb_liabilities_deposit_auction"]+values["cb_liabilities_deposit_standing"]+values["cb_liabilities_kobr"],
		values["cb_liabilities"], "9 = 10 + 11 + 12")
	cbrLiquidityCheckIdentity(t,
		values["cb_claims"]-values["cb_liabilities"]+values["nonstandard_repo"]-
			values["corr_accounts"]+values["required_reserves"],
		values["deficit"], "2 = 4 - 9 + 13 - 14 + 15")
	cbrLiquidityCheckIdentity(t,
		values["cb_claims"]-values["cb_liabilities"]+values["nonstandard_repo"],
		values["deficit_ex_corr"], "3 = 4 - 9 + 13")
}

// TestCbrLiquidityAvgFixture: форма периодов усреднения за 10.01.2018 даёт 11 точек одного
// периода; spot-значения заданы для всех 11 рядов — у плоской шапки нет нумерации, поэтому
// перестановку колонок (в том числе provided_auctions/standing_net/special_net) ловят
// только значения.
func TestCbrLiquidityAvgFixture(t *testing.T) {
	points, err := cbrLiquidityRows(cbrLiquidityFixtureBody(t, "testdata/liquidity_avg.html"), cbrLiquidityKindAvg, cbrLiquidityAvgSeries)
	if err != nil {
		t.Fatalf("разбор формы периодов усреднения: %v", err)
	}
	if len(points) != 11 {
		t.Fatalf("точек %d, want 11", len(points))
	}
	date := time.Date(2018, time.January, 10, 0, 0, 0, 0, time.UTC)
	for _, point := range points {
		if point.kind != "averaging" {
			t.Errorf("вид точки %q, want %q", point.kind, "averaging")
		}
		if !point.date.Equal(date) {
			t.Errorf("дата точки %s, want 2018-01-10", point.date.Format("2006-01-02"))
		}
	}
	cbrLiquidityCheckSpot(t, cbrLiquidityValues(points, date), map[string]float64{
		"corr_accounts_avg":         2011.1,
		"required_reserves_special": 506.2,
		"structural_deficit":        -2803.1,
		"liquidity_need":            -3126.7,
		"absorbed_auctions":         2848.6,
		"provided_auctions":         0,
		"standing_net":              -174.1,
		"special_net":               323.6,
		"key_rate_change_bp":        0,
		"ruonia_spread_bp":          -64,
		"ruonia_debt":               164.8,
	}, "2018-01-10")
}

// TestCbrLiquidityRowsEmptyRange: штатный пустой диапазон UniDbQuery — страница формы без
// таблицы .data: маркер формы есть, поэтому это nil, nil, а не ошибка. Фикстура
// zcyc_empty.html — реальный пустой ответ ЦБ того же механизма (стандарт §11.1).
func TestCbrLiquidityRowsEmptyRange(t *testing.T) {
	points, err := cbrLiquidityRows(readCbrTestdata(t, "zcyc_empty.html"), cbrLiquidityKindDaily, cbrLiquidityDailySeries)
	if err != nil {
		t.Fatalf("пустой диапазон: %v", err)
	}
	if len(points) != 0 {
		t.Errorf("точек %d, want 0 (пустой диапазон ЦБ)", len(points))
	}
}

// TestCbrLiquidityRowsBrokenPage: заглушка WAF без маркера формы UniDbQuery — ошибка, а не
// пустой ряд: подменённая страница не должна выглядеть успешным прогоном.
func TestCbrLiquidityRowsBrokenPage(t *testing.T) {
	page := []byte(`<html><body>forbidden</body></html>`)
	if _, err := cbrLiquidityRows(page, cbrLiquidityKindDaily, cbrLiquidityDailySeries); err == nil {
		t.Fatal("заглушка без маркера формы разобрана как пустой ряд без ошибки")
	}
}

// TestCbrLiquidityRowsBrokenMarkup: fail-fast на живой разметке (мутации в памяти) —
// короткая строка и непарсящаяся дата дают ошибку своей ветки с датой строки, а не тихий
// сдвиг рядов.
func TestCbrLiquidityRowsBrokenMarkup(t *testing.T) {
	const daily = "testdata/liquidity_daily.html"
	for name, tc := range map[string]struct{ old, new, wantErr string }{
		"короткая строка": {old: "<td>3 860,1</td>", new: "", wantErr: "ячеек"},
		"битая дата":      {old: "<td>31.01.2014</td>", new: "<td>31/01/2014</td>", wantErr: "дата"},
	} {
		t.Run(name, func(t *testing.T) {
			page := cbrLiquidityMutated(t, daily, tc.old, tc.new)
			_, err := cbrLiquidityRows(page, cbrLiquidityKindDaily, cbrLiquidityDailySeries)
			if err == nil {
				t.Fatalf("мутация %q: ожидалась ошибка разметки", name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("мутация %q: ошибка %q не называет ветку (%q)", name, err, tc.wantErr)
			}
		})
	}
}

// TestCbrLiquidityHeaderShape: шапка сверяется с набором рядов двусторонне, поэтому лишняя
// колонка (16-я в дневной форме), недостающая подпись (11 вместо 12 в форме усреднения) и
// перестановка нумерованных колонок — ошибка, а не молчаливый сдвиг рядов.
func TestCbrLiquidityHeaderShape(t *testing.T) {
	const (
		daily = "testdata/liquidity_daily.html"
		avg   = "testdata/liquidity_avg.html"
	)
	for name, tc := range map[string]struct {
		path   string
		kind   string
		series []string
		old    string
		new    string
	}{
		"дневная: 16-я колонка": {
			path: daily, kind: cbrLiquidityKindDaily, series: cbrLiquidityDailySeries,
			old: "<th>15</th>", new: "<th>15</th><th>16</th>",
		},
		"дневная: перестановка 10↔11": {
			path: daily, kind: cbrLiquidityKindDaily, series: cbrLiquidityDailySeries,
			old: "<th>10</th>\n        <th>11</th>", new: "<th>11</th>\n        <th>10</th>",
		},
		"дневная: подпись без номера": {
			path: daily, kind: cbrLiquidityKindDaily, series: cbrLiquidityDailySeries,
			old: "<th>1</th>", new: "<th>Дата</th>",
		},
		"усреднения: 11 подписей вместо 12": {
			path: avg, kind: cbrLiquidityKindAvg, series: cbrLiquidityAvgSeries,
			old: "<th>Задолженность по сделкам RUONIA</th>", new: "",
		},
		"усреднения: лишняя подпись": {
			path: avg, kind: cbrLiquidityKindAvg, series: cbrLiquidityAvgSeries,
			old: "<th>Задолженность по сделкам RUONIA</th>",
			new: "<th>Задолженность по сделкам RUONIA</th><th>Лишняя</th>",
		},
		"усреднения: дневной вид на плоской шапке": {
			path: avg, kind: cbrLiquidityKindDaily, series: cbrLiquidityDailySeries,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var page []byte
			if tc.old == "" {
				page = cbrLiquidityFixtureBody(t, tc.path)
			} else {
				page = cbrLiquidityMutated(t, tc.path, tc.old, tc.new)
			}
			_, err := cbrLiquidityRows(page, tc.kind, tc.series)
			if err == nil {
				t.Fatalf("ожидалась ошибка формы шапки")
			}
			if !strings.Contains(err.Error(), "шапк") {
				t.Errorf("ошибка %q не называет шапку формы", err)
			}
		})
	}
}

// TestCbrLiquidityPointsInRange — фильтр окна: ЦБ на пустой/будущий диапазон отдаёт
// последнюю доступную строку, и она не является точкой этого окна. Границы [from, to]
// включаются, точки до from и после to отбрасываются, порядок сохраняется.
func TestCbrLiquidityPointsInRange(t *testing.T) {
	day := func(d int) time.Time { return time.Date(2026, time.November, d, 0, 0, 0, 0, time.UTC) }
	from, to := day(10), day(12)
	points := []cbrLiquidityPoint{
		{kind: "daily", series: "deficit", date: day(9), value: 1},  // до окна
		{kind: "daily", series: "deficit", date: from, value: 2},    // граница from — включается
		{kind: "daily", series: "deficit", date: day(11), value: 3}, // внутри
		{kind: "daily", series: "deficit", date: to, value: 4},      // граница to — включается
		{kind: "daily", series: "deficit", date: day(13), value: 5}, // после to
	}
	kept := cbrLiquidityPointsInRange(points, from, to)
	if len(kept) != 3 {
		t.Fatalf("точек в окне %d, want 3 (обе границы и внутри)", len(kept))
	}
	for i, want := range []float64{2, 3, 4} {
		if kept[i].value != want || !kept[i].date.Equal(day(10+i)) {
			t.Errorf("точка %d = (%s, %v), want (%s, %v)", i,
				kept[i].date.Format("2006-01-02"), kept[i].value, day(10+i).Format("2006-01-02"), want)
		}
	}
	if got := cbrLiquidityPointsInRange(nil, from, to); len(got) != 0 {
		t.Errorf("пустой вход: точек %d, want 0", len(got))
	}
}

// TestCbrLiquidityValue: форматы ячеек формы — пробелы внутри числа как разделитель тысяч,
// запятая как десятичный разделитель, прочерк как отсутствие значения (не ноль). NaN/Inf
// ParseFloat принимает без ошибки, но значением баланса они быть не могут.
func TestCbrLiquidityValue(t *testing.T) {
	cases := []struct {
		raw     string
		value   float64
		present bool
	}{
		{"3 481,8", 3481.8, true},
		{"3\u00a0481,8", 3481.8, true}, // неразрывный пробел
		{"3\u202f481,8", 3481.8, true}, // узкий неразрывный пробел
		{"-2 803,1", -2803.1, true},
		{"0", 0, true},
		{"—", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		value, present, err := cbrLiquidityValue(c.raw)
		if err != nil {
			t.Errorf("%q: %v", c.raw, err)
			continue
		}
		if present != c.present || value != c.value {
			t.Errorf("%q = (%v, %v), want (%v, %v)", c.raw, value, present, c.value, c.present)
		}
	}
	if _, _, err := cbrLiquidityValue("нет данных"); err == nil {
		t.Error("нечисловая ячейка разобрана без ошибки")
	}
	for _, raw := range []string{"NaN", "Inf", "-Inf"} {
		_, _, err := cbrLiquidityValue(raw)
		if err == nil || !strings.Contains(err.Error(), "конечным числом") {
			t.Errorf("%q: err = %v, want ошибку о не конечном числе (NaN/Inf)", raw, err)
		}
	}
}
