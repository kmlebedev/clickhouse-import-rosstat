package bank

import (
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/polyus"
)

// bankCompanyDictionaryMetrics — 14 метрик словаря спеки §3.7 списком-литералом: это контракт
// словаря, а не производная от реализации (ссылка на константу прошла бы и на опечатке в ней).
var bankCompanyDictionaryMetrics = []string{
	"total_assets",
	"equity",
	"customer_loans",
	"customer_deposits",
	"loans_individuals",
	"loans_corporations",
	"deposits_individuals",
	"deposits_corporations",
	"net_profit",
	"roe",
	"roa",
	"capital_adequacy",
	"capital_adequacy_core",
	"capital_adequacy_base",
}

// bankCompanySpotEps — допуск сравнения spot-значений: сырые таблицы хранят значение в Float32,
// поэтому 65355,1 из книги приходит к тесту как 65355,1015625. Копейка на 65 355,1 млрд ₽ и
// 0,01 п.п. на 14,6 % любой содержательной ошибке (не та метрика, не та единица, не тот месяц)
// дают расхождение на порядки больше.
const bankCompanySpotEps = 0.01

// Словарь обязан быть полным и однозначным: каждая метрика — из списка спеки §3.7, единица — из
// двух единиц словаря, у каждой компании свой набор (Сбер — все 14, ВТБ — 11: достаточность
// капитала в разбираемой рамке книги ВТБ не публикуется, см. bankCompanyMetricSpecs), каждая
// пара (тикер, метрика) описана в каталоге, и ни одну метрику не дают две подписи одного
// источника — на ключе (company, metric, period) они затирали бы друг друга.
func TestBankCompanyMetricMapWellFormed(t *testing.T) {
	dictionary := make(map[string]bool, len(bankCompanyDictionaryMetrics))
	for _, metric := range bankCompanyDictionaryMetrics {
		dictionary[metric] = true
	}
	// Единицы — литералами (спека §3.7): у словаря всего две, и обе обязаны стоять в unit
	// ровно такими строками.
	units := map[string]bool{"млрд ₽": true, "%": true}

	perCompany := map[string]int{}
	pairs := map[string]bool{}
	for table, specs := range bankCompanyMetricSpecs {
		ticker, ok := bankCompanySourceCompany[table]
		if !ok {
			t.Fatalf("таблица %q есть в словаре, но у неё нет тикера (bankCompanySourceCompany)", table)
		}
		byLabel := bankCompanyMetricMap[table]
		if len(byLabel) != len(specs) {
			t.Errorf("%s: в карте маппинга %d подписей, а в словаре %d — подписи схлопнулись", table, len(byLabel), len(specs))
		}
		byMetric := map[string]string{}
		for _, spec := range specs {
			if !dictionary[spec.Metric] {
				t.Errorf("%s/%q: метрика %q вне списка метрик спеки §3.7", table, spec.Label, spec.Metric)
			}
			if !units[spec.Unit] {
				t.Errorf("%s/%q: единица %q не из словаря единиц", table, spec.Label, spec.Unit)
			}
			if label, dup := byMetric[spec.Metric]; dup {
				t.Errorf("%s: метрику %q дают две подписи — %q и %q", table, spec.Metric, label, spec.Label)
			}
			byMetric[spec.Metric] = spec.Label
			want := bankCompanyMetric{Metric: spec.Metric, Unit: spec.Unit}
			if got, ok := byLabel[spec.Label]; !ok || got != want {
				t.Errorf("%s/%q: карта маппинга даёт %+v, а словарь — метрику %q с единицей %q", table, spec.Label, got, spec.Metric, spec.Unit)
			}
			pairs[ticker+"_"+spec.Metric] = true
		}
		perCompany[ticker] = len(byMetric)
	}

	if got := perCompany["SBER"]; got != 14 {
		t.Errorf("SBER покрывает %d метрик, want 14 (все метрики словаря)", got)
	}
	if got := perCompany["VTBR"]; got != 11 {
		t.Errorf("VTBR покрывает %d метрик, want 11 (без capital_adequacy: Н20.x лежит ниже рамки разбора, Н1.1/Н1.2 ВТБ раздельно не публикует)", got)
	}

	described := map[string]bool{}
	for _, m := range bankCompanyMetricsMeta {
		if m.Source != bankCompanyMetricsName {
			t.Errorf("ряд %q каталога описан от другого источника: %q", m.Series, m.Source)
		}
		if m.Frequency != "M" {
			t.Errorf("%s/%s: частота %q, want M (банковские отчётности помесячны)", m.Source, m.Series, m.Frequency)
		}
		for _, field := range []struct{ name, value string }{
			{"Title", m.Title},
			{"Unit", m.Unit},
			{"Frequency", m.Frequency},
			{"Origin", m.Origin},
			{"Description", m.Description},
		} {
			if field.value == "" {
				t.Errorf("%s/%s: поле %s пустое", m.Source, m.Series, field.name)
			}
		}
		described[m.Series] = true
	}
	for series := range pairs {
		if !described[series] {
			t.Errorf("ряд %q не описан в bankCompanyMetricsMeta", series)
		}
	}
	for series := range described {
		if !pairs[series] {
			t.Errorf("каталог описывает ряд %q, которого нет в словаре метрик", series)
		}
	}
}

// Период company_financials — «2026M08» (год, литеральная M, двузначный месяц). Дата берётся у
// значения КАК ЕСТЬ: приведение .UTC() сдвинуло бы первое число месяца в предыдущий (драйвер
// отдаёт Date в зоне сессии; прецедент cbr/mp_summary.go:mpDay), и январь попал бы в декабрь.
func TestBankCompanyPeriod(t *testing.T) {
	for _, tc := range []struct {
		date time.Time
		want string
	}{
		{time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC), "2026M08"},
		{time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), "2026M01"},
		{time.Date(2025, time.December, 31, 0, 0, 0, 0, time.UTC), "2025M12"},
		{time.Date(2026, time.September, 1, 0, 0, 0, 0, time.FixedZone("MSK", 3*3600)), "2026M09"},
	} {
		if got := bankCompanyPeriod(tc.date); got != tc.want {
			t.Errorf("bankCompanyPeriod(%s) = %q, want %q", tc.date.Format("2006-01-02"), got, tc.want)
		}
	}
}

// Форма строки company_financials — контракт записи импортёра: (company, metric, period,
// period_type, source_kind, value, unit, source_url, source_page, loaded_at), метка loaded_at одна
// на прогон (она делает повторный прогон версией ReplacingMergeTree, а не ростом таблицы), а
// ключ (company, metric, period, source_kind, source_url) в одном прогоне не повторяется — иначе
// батч вёз бы строки, которые ClickHouse потом схлопнул бы молча. Строки собираются из обеих
// живых фикстур (разведка живых книг) тем же маппингом, что и Import.
func TestBankCompanyAppend(t *testing.T) {
	rows := bankCompanyFixtureRows(t)
	if len(rows) != 629 {
		t.Fatalf("словарь дал %d строк из фикстур, want 629 (SBER 599 + VTBR 30)", len(rows))
	}

	loadedAt := time.Date(2026, time.October, 17, 12, 0, 0, 0, time.UTC)
	rec := newCompanyRecorder(companyFinancialsColumnNames...)
	if err := appendBankCompanyRows(rec, rows, loadedAt); err != nil {
		t.Fatalf("запись строк company_financials: %v", err)
	}
	if rec.err != nil {
		t.Fatalf("форма строк не совпала с объявленной схемой: %v", rec.err)
	}
	if len(rec.company) != len(rows) {
		t.Fatalf("записано %d строк, а собрано %d", len(rec.company), len(rows))
	}

	units := make(map[string]string, len(bankCompanyMetricsMeta))
	for _, m := range bankCompanyMetricsMeta {
		units[m.Series] = m.Unit
	}
	// Идентификаторы документа-источника — часть ключа таблицы и провенанса строки: у Сбера это
	// статичный адрес файла книги, у ВТБ — страница результатов с пофайловыми выпусками.
	// Литералами: ссылка на константу прошла бы и на опечатке в самой ссылке.
	kinds := map[string]string{"SBER": "ras", "VTBR": "ifrs"}
	urls := map[string]string{
		"SBER": "https://www.sberbank.com/common/img/uploaded/redirected/com/investor-relations/groupresults/sber_finansovie_rezultaty_ras_2022-2024.xlsx",
		"VTBR": "https://www.vtb.ru/ir/statements/results/",
	}

	counts := map[string]int{}
	seen := map[string]bool{}
	for i, r := range rec.company {
		kind, known := kinds[r.company]
		if !known {
			t.Fatalf("строка %d: неизвестная компания %q", i, r.company)
		}
		counts[r.company]++
		// period_type — литералом: 'M' в колонке и есть контракт помесячности, ссылка на
		// константу прошла бы и на опечатке в ней (ту же букву держит и тест DDL).
		if r.periodType != "M" {
			t.Errorf("строка %d (%s %s %s): period_type %q, want %q", i, r.company, r.metric, r.period, r.periodType, "M")
		}
		if r.sourceKind != kind {
			t.Errorf("строка %d (%s %s %s): source_kind %q, want %q", i, r.company, r.metric, r.period, r.sourceKind, kind)
		}
		if r.sourceUrl != urls[r.company] {
			t.Errorf("строка %d (%s %s %s): source_url %q, want %q", i, r.company, r.metric, r.period, r.sourceUrl, urls[r.company])
		}
		if want := units[r.company+"_"+r.metric]; r.unit != want {
			t.Errorf("строка %d (%s %s %s): unit %q, а словарь даёт %q", i, r.company, r.metric, r.period, r.unit, want)
		}
		if r.sourcePage != 0 {
			t.Errorf("строка %d (%s %s %s): source_page %d, want 0 (у источника нет страниц)", i, r.company, r.metric, r.period, r.sourcePage)
		}
		if !r.loadedAt.Equal(loadedAt) {
			t.Errorf("строка %d (%s %s %s): loaded_at %s, want %s — метка одна на прогон", i, r.company, r.metric, r.period, r.loadedAt, loadedAt)
		}
		key := strings.Join([]string{r.company, r.metric, r.period, r.sourceKind, r.sourceUrl}, "|")
		if seen[key] {
			t.Errorf("дубль ключа company_financials в одном прогоне: %s", key)
		}
		seen[key] = true
	}
	for company, want := range map[string]int{"SBER": 599, "VTBR": 30} {
		if got := counts[company]; got != want {
			t.Errorf("%s: строк %d, want %d", company, got, want)
		}
	}

	// Spot-значения (разведка живых книг): числа книги, единицы словаря и период из даты вместе
	// ловят и не ту метрику, и не ту единицу, и не тот месяц. Процентные метрики проверяются по
	// проценту (0.146 книги = 14,6 строки): иначе единица '%' в колонке unit врала бы.
	byKey := make(map[string]float64, len(rec.company))
	for _, r := range rec.company {
		byKey[r.company+"|"+r.metric+"|"+r.period] = r.value
	}
	for _, tc := range []struct {
		company, metric, period string
		want                    float64
	}{
		{"SBER", "total_assets", "2026M06", 65355.1},
		{"SBER", "equity", "2026M06", 9113.5},
		{"SBER", "deposits_individuals", "2026M06", 34221.5},
		{"SBER", "net_profit", "2026M06", 168.1},
		{"SBER", "roe", "2023M01", 22.3},
		{"SBER", "roa", "2026M06", 3.1},
		{"SBER", "capital_adequacy", "2026M06", 14.6},
		{"SBER", "capital_adequacy_base", "2022M12", 13.0},
		{"VTBR", "total_assets", "2026M08", 39041.5},
		{"VTBR", "equity", "2026M08", 2890.8},
		{"VTBR", "customer_loans", "2026M08", 25939.3},
		{"VTBR", "deposits_individuals", "2026M08", 13867.9},
		{"VTBR", "net_profit", "2026M08", 49.2},
		{"VTBR", "roe", "2026M08", 20.2},
		{"VTBR", "roa", "2026M08", 1.5},
	} {
		key := tc.company + "|" + tc.metric + "|" + tc.period
		got, ok := byKey[key]
		if !ok {
			t.Errorf("нет строки %s", key)
			continue
		}
		if math.Abs(got-tc.want) > bankCompanySpotEps {
			t.Errorf("%s = %v, want %v (допуск %v)", key, got, tc.want, bankCompanySpotEps)
		}
	}
}

// Дубль (metric, period) в одном прогоне — ошибка, а не молчаливая запись: у двух таких точек
// совпал бы ключ company_financials (company, metric, period, source_kind, source_url), и
// ReplacingMergeTree схлопнул бы строки молча (спека §2.4). Текст обязан назвать таблицу,
// метрику, период и обе даты — по ним оператор найдёт исходные строки сырой таблицы. Точки
// разных месяцев той же метрики дублем не являются.
func TestBankCompanyMapPointsDuplicatePeriod(t *testing.T) {
	table := sberRpbuDataUrlTable
	june := time.Date(2026, time.June, 30, 0, 0, 0, 0, time.UTC)
	rows, missing, err := bankCompanyMapPoints(table, []bankCompanyPoint{
		{Name: "Активы", Date: june, Value: 65355.1},
		{Name: "Активы", Date: time.Date(2026, time.June, 15, 0, 0, 0, 0, time.UTC), Value: 65000},
	})
	if err == nil {
		t.Fatalf("две точки total_assets за 2026M06 приняты без ошибки: rows=%+v missing=%v", rows, missing)
	}
	for _, want := range []string{table, "total_assets", "2026M06", "2026-06-30", "2026-06-15"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не называет %q — оператор не найдёт строки сырой таблицы", err, want)
		}
	}

	if _, _, err := bankCompanyMapPoints(table, []bankCompanyPoint{
		{Name: "Активы", Date: june, Value: 65355.1},
		{Name: "Активы", Date: time.Date(2026, time.May, 31, 0, 0, 0, 0, time.UTC), Value: 64000},
	}); err != nil {
		t.Errorf("точки разных месяцев ошибочно приняты за дубль: %v", err)
	}
}

// Строка сырой таблицы вне словаря — не ошибка прогона (сырьё уже импортировано, маппинг —
// производное, спека §3.7): она пропускается. А подпись словаря, которой в таблице не нашлось,
// обязана быть названа вызывающему спекой словаря — иначе метрика исчезла бы из
// company_financials молча, и заметить это можно было бы только по витрине. Спека, а не голая
// подпись: warn обязан назвать и метрику (spec.Metric).
func TestBankCompanyMapPointsSkipsUnknownLabels(t *testing.T) {
	date := time.Date(2026, time.June, 30, 0, 0, 0, 0, time.UTC)
	rows, missing, err := bankCompanyMapPoints(sberRpbuDataUrlTable, []bankCompanyPoint{
		{Name: "Жилищное кредитование", Date: date, Value: 1113}, // сырьевой ряд вне словаря
		{Name: "Активы", Date: date, Value: 65355.1},             // подпись словаря
	})
	if err != nil {
		t.Fatalf("маппинг известных подписей: %v", err)
	}

	if len(rows) != 1 || rows[0].Metric != "total_assets" || rows[0].Period != "2026M06" {
		t.Errorf("маппинг дал %+v, want одну строку SBER/total_assets за 2026M06", rows)
	}
	// Порядок — словарный: missing собирается обходом bankCompanyMetricSpecs, и warn читается в
	// том же порядке, что и словарь.
	var wantMissing []string
	for _, spec := range bankCompanyMetricSpecs[sberRpbuDataUrlTable] {
		if spec.Label != "Активы" {
			wantMissing = append(wantMissing, spec.Metric)
		}
	}
	var gotMissing []string
	for _, spec := range missing {
		gotMissing = append(gotMissing, spec.Metric)
	}
	if !slices.Equal(gotMissing, wantMissing) {
		t.Errorf("не найденные спеки словаря %v, want %v", gotMissing, wantMissing)
	}
	if slices.ContainsFunc(missing, func(spec bankCompanyMetricSpec) bool { return spec.Label == "Активы" }) {
		t.Error("подпись «Активы» в таблице есть, а маппинг назвал её не найденной")
	}
	for _, label := range []string{"Достаточность общего капитала Н1.0", "Рентабельность капитала"} {
		if !slices.ContainsFunc(missing, func(spec bankCompanyMetricSpec) bool { return spec.Label == label }) {
			t.Errorf("подписи %q нет в таблице, а маппинг её не назвал: %v", label, missing)
		}
	}
	for _, spec := range missing {
		if spec.Metric == "" {
			t.Errorf("missing содержит спеку %+v без метрики — warn не сможет назвать метрику", spec)
		}
	}
}

// DDL импортёра — копия канонического DDL company_financials, и он исполняется сырым conn.Exec:
// ошибку в enum'е периода, имени таблицы или ключе поймал бы только живой прогон.
func TestBankCompanyDdlHasMonthEnum(t *testing.T) {
	if !strings.Contains(bankCompanyMetricsDdl, "'M' = 5") {
		t.Fatalf("DDL %s не содержит 'M' = 5 в period_type:\n%s", bankCompanyMetricsName, bankCompanyMetricsDdl)
	}
	if !strings.Contains(bankCompanyMetricsDdl, "CREATE TABLE IF NOT EXISTS "+bankCompanyMetricsTable) {
		t.Errorf("DDL заводит не ту таблицу (имя подставляется без шаблона):\n%s", bankCompanyMetricsDdl)
	}
	if !strings.Contains(bankCompanyMetricsDdl, "ORDER BY (company, metric, period, source_kind, source_url)") {
		t.Errorf("DDL без ключа (company, metric, period, source_kind, source_url):\n%s", bankCompanyMetricsDdl)
	}
}

// Контракт «словарь ↔ живые файлы» (спека §3.7): каждая подпись словаря обязана реально
// извлекаться разбором своей фикстуры. Опечатка в подписи иначе не всплыла бы нигде: маппинг
// пропускает неизвестные подписи с предупреждением в журнале, и метрика просто не появилась бы
// в company_financials.
func TestBankCompanyLabelsExistInFixtures(t *testing.T) {
	t.Run(sberRpbuDataUrlTable, func(t *testing.T) {
		points, err := parseSberFinBook(sberOpenFixture(t))
		if err != nil {
			t.Fatalf("разбор фикстуры %s: %v", sberFinFixture, err)
		}
		names := make(map[string]bool, len(points))
		for _, p := range points {
			names[p.Series] = true
		}
		checkBankCompanyLabels(t, sberRpbuDataUrlTable, sberFinFixture, names)
	})
	t.Run(vtbIfrsTable, func(t *testing.T) {
		points, err := parseVtbBook(vtbOpenFixture(t, vtbBookFixture))
		if err != nil {
			t.Fatalf("разбор фикстуры %s: %v", vtbBookFixture, err)
		}
		names := make(map[string]bool, len(points))
		for _, p := range points {
			names[p.Series] = true
		}
		checkBankCompanyLabels(t, vtbIfrsTable, vtbBookFixture, names)
	})
}

// checkBankCompanyLabels сверяет подписи словаря одной таблицы с именами рядов, которые дал
// разбор её фикстуры.
func checkBankCompanyLabels(t *testing.T, table, fixture string, names map[string]bool) {
	t.Helper()
	for _, spec := range bankCompanyMetricSpecs[table] {
		if !names[spec.Label] {
			t.Errorf("%s: подписи %q (метрика %s) нет в разборе фикстуры %s", table, spec.Label, spec.Metric, fixture)
		}
	}
}

// Копий DDL company_financials три: polyus/import.go (шаблон с %s), views/company.go (разрешённое
// имя) и bank/company_metrics.go (имя конкатенацией). Сверку polyus↔views держит
// views/company_test.go; здесь копия банковского импортёра сличается с шаблоном Полюса, и вместе
// два теста накрывают все три. Сравниваются разрешённые формы — шаблон имя таблицы подставляет.
func TestBankCompanyDdlMatchesPolyusTemplate(t *testing.T) {
	normalize := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	got, want := normalize(bankCompanyMetricsDdl), normalize(polyus.MetricsTableDDL())
	if got != want {
		t.Errorf("копии DDL company_financials разошлись:\nbank:   %s\npolyus: %s", got, want)
	}
}

// bankCompanyFixtureRows — строки company_financials из обеих живых фикстур (разведка живых книг):
// значения приведены к точности Float32-колонки balance сырой таблицы — ровно так, как их отдал бы
// SELECT из неё.
func bankCompanyFixtureRows(t *testing.T) []bankCompanyRow {
	t.Helper()
	var rows []bankCompanyRow
	for _, src := range []struct {
		table  string
		points []bankCompanyPoint
	}{
		{sberRpbuDataUrlTable, bankCompanySberPoints(t)},
		{vtbIfrsTable, bankCompanyVtbPoints(t)},
	} {
		mapped, missing, err := bankCompanyMapPoints(src.table, src.points)
		if err != nil {
			t.Fatalf("%s: маппинг точек фикстуры: %v", src.table, err)
		}
		if len(missing) > 0 {
			t.Errorf("%s: подписи словаря не найдены в разборе фикстуры: %v — метрики пропущены", src.table, missing)
		}
		rows = append(rows, mapped...)
	}
	return rows
}

// bankCompanySberPoints — точки разбора фикстуры Сбера в форме строки сырой таблицы.
func bankCompanySberPoints(t *testing.T) []bankCompanyPoint {
	t.Helper()
	points, err := parseSberFinBook(sberOpenFixture(t))
	if err != nil {
		t.Fatalf("разбор фикстуры %s: %v", sberFinFixture, err)
	}
	converted := make([]bankCompanyPoint, 0, len(points))
	for _, p := range points {
		converted = append(converted, bankCompanyPoint{Name: p.Series, Date: p.Date, Value: float64(float32(p.Value))})
	}
	return converted
}

// bankCompanyVtbPoints — точки разбора фикстуры ВТБ в форме строки сырой таблицы.
func bankCompanyVtbPoints(t *testing.T) []bankCompanyPoint {
	t.Helper()
	points, err := parseVtbBook(vtbOpenFixture(t, vtbBookFixture))
	if err != nil {
		t.Fatalf("разбор фикстуры %s: %v", vtbBookFixture, err)
	}
	converted := make([]bankCompanyPoint, 0, len(points))
	for _, p := range points {
		converted = append(converted, bankCompanyPoint{Name: p.Series, Date: p.Date, Value: float64(float32(p.Value))})
	}
	return converted
}
