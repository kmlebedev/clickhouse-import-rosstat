package polyus

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseIFRSReport(t *testing.T) {
	records, err := parseIFRSPage("testdata/en_msfo_p6.tsv", "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}
	got := map[string]float64{}
	for _, r := range records {
		got[r.Metric] = r.Value
	}
	// значения из otchetnost-6m2026_eng.pdf, страница 6 (в млн долларов США)
	if got["total_revenue"] != 4674 {
		t.Errorf("total_revenue = %v, want 4674", got["total_revenue"])
	}
	if got["profit_for_period"] != 829 {
		t.Errorf("profit_for_period = %v, want 829", got["profit_for_period"])
	}
	if got["eps_basic"] != 0.87 {
		t.Errorf("eps_basic = %v, want 0.87", got["eps_basic"])
	}
}

// TestParseIFRSReportFullPage держит весь набор статей страницы 6, а не только
// три проверяемые выше: сокращённая метка не должна перехватывать строку
// длинной, а скобки — превращаться в положительное значение.
func TestParseIFRSReportFullPage(t *testing.T) {
	records, err := parseIFRSPage("testdata/en_msfo_p6.tsv", "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}

	got := map[string]float64{}
	for _, r := range records {
		got[r.Metric] = r.Value
		if r.Company != "PLZL" {
			t.Errorf("company for %s = %q, want PLZL", r.Metric, r.Company)
		}
		if r.Period != "2026H1" {
			t.Errorf("period for %s = %q, want 2026H1", r.Metric, r.Period)
		}
	}

	want := map[string]float64{
		"gold_sales":         4569,
		"other_sales":        105,
		"total_revenue":      4674,
		"operating_expenses": -3405,
		"profit_before_tax":  1269,
		"income_tax_expense": -440,
		"profit_for_period":  829,
		"eps_basic":          0.87,
		"eps_diluted":        0.87,
	}

	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %v, want %v", name, got[name], value)
		}
	}

	if len(records) != len(want) {
		t.Errorf("got %d records, want %d: %v", len(records), len(want), got)
	}
}

// TestParseIFRSProfitForPeriodNotAttributable держит перекрытие меток: строка
// "Profit for the period attributable to:" начинается с метки profit_for_period,
// но чисел не содержит, поэтому записью стать не должна — иначе строка
// «Profit for the period attributable to:» подменила бы собой настоящий
// profit_for_period (или добавила бы дубль без значения).
func TestParseIFRSProfitForPeriodNotAttributable(t *testing.T) {
	records, err := parseIFRSPage("testdata/en_msfo_p6.tsv", "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}

	profitForPeriod := 0
	for _, r := range records {
		if r.Metric == "profit_for_period" {
			profitForPeriod++
			if r.Value != 829 {
				t.Errorf("profit_for_period = %v, want 829", r.Value)
			}
		}
	}
	if profitForPeriod != 1 {
		t.Errorf("profit_for_period records = %d, want 1 (attributable line must not add a duplicate)", profitForPeriod)
	}

	// Строка взвешенного числа акций начинается с того же маркера списка, что и
	// eps, но метку eps_basic не содержит: метрики по ней быть не должно.
	for _, r := range records {
		if r.Metric == "eps_basic" && r.Value == 950004 {
			t.Error("weighted average share count leaked into eps_basic")
		}
	}
}

// TestPeriodTypeCanonical проверяет вывод типа периода из канонической формы:
// МСФО-парсер получает период параметром уже склеенным ("2026H1"), а parsePeriod
// принимает только форму релиза ("1H2026").
func TestPeriodTypeCanonical(t *testing.T) {
	cases := map[string]string{
		"2026H1": "H",
		"2025H2": "H",
		"2026FY": "FY",
		"2021Q4": "Q",
		"":       "",
		"xxxxH1": "",
		"2026h1": "",
	}

	for period, want := range cases {
		if got := periodType(period); got != want {
			t.Errorf("periodType(%q) = %q, want %q", period, got, want)
		}
	}
}

func TestParseIFRSOnKPIPage(t *testing.T) {
	// перекрёстный случай: KPI-страница, поданная МСФО-парсеру
	records, err := parseIFRSPage("testdata/press_reliz_1h26_p1.tsv", "https://example.invalid/x.pdf", 1, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage must not fail on a KPI page: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected no IFRS metrics on a KPI page, got %d", len(records))
	}
}

// TestIFRSValuesByColumn держит раскладку МСФО-страницы: метка слева, два
// числовых столбца справа (отчётный и прошлый). Отчётный — первый по X после
// метки, прошлый — следующий за ним.
func TestIFRSValuesByColumn(t *testing.T) {
	recs, err := parseIFRSPage("testdata/en_msfo_p6.tsv", "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}
	got := map[string]float64{}
	for _, r := range recs {
		got[r.Metric] = r.Value
	}
	if got["gold_sales"] != 4569 {
		t.Errorf("gold_sales = %v, want 4569", got["gold_sales"])
	}
	if got["total_revenue"] != 4674 {
		t.Errorf("total_revenue = %v, want 4674", got["total_revenue"])
	}
	if got["operating_expenses"] != -3405 {
		t.Errorf("operating_expenses = %v, want -3405 (parenthesised negative)", got["operating_expenses"])
	}

	// Записи по метрике ровно одна: строка несёт ДВА значения, и в запись
	// попадает только первое — отчётный период. Значения прошлого столбца
	// (3,581; 3,688; (1,001); 2,687; (669); 2,018; 2.12/2.13) в разобранном
	// наборе появиться не должны ни под своей метрикой, ни под чужой.
	if len(recs) != 9 {
		t.Errorf("got %d records from page 6, want 9 (one per metric)", len(recs))
	}

	for _, r := range recs {
		if prior, ok := ifrsPriorPeriodValues[r.Metric]; ok && r.Value == prior {
			t.Errorf(
				"%s = %v: prior-period column leaked into the reporting-period record",
				r.Metric,
				r.Value,
			)
		}
	}
}

// ifrsPriorPeriodValues — значение СРАВНИТЕЛЬНОГО (прошлого) столбца страницы 6 по
// каждой метрике, замеренное по фикстуре testdata/en_msfo_p6.tsv. Ни одно из них
// не должно попасть в записи: отчётные значения — все остальные.
var ifrsPriorPeriodValues = map[string]float64{
	"gold_sales":         3581,  // 3,581 (x=520.54)
	"other_sales":        107,   // 107 (x=527.26)
	"total_revenue":      3688,  // 3,688 (x=520.54)
	"operating_expenses": -1001, // (1,001) (x=518.26)
	"profit_before_tax":  2687,  // 2,687 (x=520.54)
	"income_tax_expense": -669,  // (669) (x=524.86)
	"profit_for_period":  2018,  // 2,018 (x=520.54)
	"eps_basic":          2.13,  // 2.13 (x=524.98)
	"eps_diluted":        2.12,  // 2.12 (x=524.98)
}

// TestParseIFRSJoinedPages держит производственный вход этого парсера: он
// получает не одну страницу, а файл, склеенный extractPDF из всех
// Report.Pages, — для английского отчёта 1 п/г 2026 это страницы 6 и 7
// (pages.go). Склейка повторяется ровно так же, как в joinFiles: байты обеих
// страниц подряд в один файл.
//
// Проверяется весь набор метрик, а не пара значений: подмена «одна страница»
// на «две» не видна ни по отдельным значениям, ни по проверке длины в
// TestParseIFRSReportFullPage (её ожидания и так содержат только страницу 6).
// Полный набор видит и пропавшую строку, и метку второй страницы, которая
// столкнулась бы с меткой первой: foundMetrics page-global, и совпавшая метрика
// второй страницы не добавила бы запись вовсе.
func TestParseIFRSJoinedPages(t *testing.T) {
	joined := joinIFRSFixtures(t, "testdata/en_msfo_p6.tsv", "testdata/en_msfo_p7.tsv")

	records, err := parseIFRSPage(joined, "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}

	got := map[string]float64{}
	for _, r := range records {
		got[r.Metric] = r.Value
	}

	want := map[string]float64{
		// страница 6 — отчёт о прибылях и убытках
		"gold_sales":         4569,
		"other_sales":        105,
		"total_revenue":      4674,
		"operating_expenses": -3405,
		"profit_before_tax":  1269,
		"income_tax_expense": -440,
		"profit_for_period":  829,
		"eps_basic":          0.87,
		"eps_diluted":        0.87,
		// страница 7 — отчёт о финансовом положении
		"total_assets": 17818,
	}

	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %v, want %v", name, got[name], value)
		}
	}

	for name, value := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("unexpected metric %s = %v in the joined file", name, value)
		}
	}

	if len(records) != len(want) {
		t.Errorf("got %d records, want %d: %v", len(records), len(want), got)
	}

	// Период и компания у всех записей склеенного файла — из параметров
	// вызова, а не из шапки страницы (её на МСФО-странице нет).
	for _, r := range records {
		if r.Company != "PLZL" || r.Period != "2026H1" || r.PeriodType != "H" {
			t.Errorf(
				"%s: company %q, period %q (%q), want PLZL/2026H1/H",
				r.Metric,
				r.Company,
				r.Period,
				r.PeriodType,
			)
		}
	}

	// Провенанс берётся из данных, а не из параметра: у склеенного файла номер
	// страницы, переданный вызовом (6), верен только для первой страницы, и
	// total_assets — строка седьмой — обязан нести 7.
	for _, r := range records {
		wantPage := 6
		if r.Metric == "total_assets" {
			wantPage = 7
		}

		if r.SourcePage != wantPage {
			t.Errorf(
				"%s: source_page = %d, want %d (the page the row is printed on, not the page passed to the parser)",
				r.Metric,
				r.SourcePage,
				wantPage,
			)
		}
	}
}

// TestSourcePageComesFromRow держит провенанс записи: source_page — номер
// страницы строки, а не номер, переданный разбору параметром.
//
// На входе именно тот случай, который ломал провенанс: extractPDF склеивает в
// один файл все Report.Pages отчёта (для английского МСФО 1 п/г 2026 — страницы
// 6 и 7), а разбор получает номер первой из них (report.Pages[0] == 6).
//
// Склейка берётся производственная — joinIFRSFixtures повторяет joinFiles
// побайтово, — поэтому проверяется реальный путь, а не собранная руками строка.
func TestSourcePageComesFromRow(t *testing.T) {
	joined := joinIFRSFixtures(t, "testdata/en_msfo_p6.tsv", "testdata/en_msfo_p7.tsv")

	// параметр — 6, как report.Pages[0] в import.go; page-7 строки обязаны
	// получить 7 из данных, а не унаследовать параметр.
	records, err := parseIFRSPage(joined, "https://example.invalid/6m2026.pdf", 6, "2026H1")
	if err != nil {
		t.Fatalf("parseIFRSPage: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("joined snapshots parsed to zero records")
	}

	pages := map[int]int{}
	for _, record := range records {
		if record.SourcePage != 6 && record.SourcePage != 7 {
			t.Errorf("%s: source_page = %d, want 6 or 7", record.Metric, record.SourcePage)
		}
		pages[record.SourcePage]++
	}
	// обе страницы должны быть представлены: иначе тест не доказывает, что
	// провенанс берётся из данных, а не из параметра
	if pages[6] == 0 || pages[7] == 0 {
		t.Errorf("source_page spread = %v, want records from both page 6 and page 7", pages)
	}
}

// TestGroupByLineSeparatesPages держит ключ группировки строк: слова склеенного
// снимка группируются по (страница, Top), а не по одному Top.
//
// Координаты pdftotext начинает заново на каждой странице, поэтому слово шестой
// страницы с Top 6.00 и слово седьмой с тем же Top — разные строки. Ключ без
// номера страницы слил бы их в одну: в разборе это дало бы строку с метками
// обеих страниц, то есть потерянные или подменённые значения.
func TestGroupByLineSeparatesPages(t *testing.T) {
	words := []Word{
		{Text: "Total", Page: 6, Left: 70.94, Top: 6.00},
		{Text: "revenue", Page: 6, Left: 106.0, Top: 6.00},
		{Text: "Total", Page: 7, Left: 70.94, Top: 6.00},
		{Text: "assets", Page: 7, Left: 117.0, Top: 6.00},
	}

	lines := groupByLine(words, 0)

	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: words of different pages must not share a line", len(lines))
	}

	for i, line := range lines {
		for _, word := range line.Words {
			if word.Page != line.Words[0].Page {
				t.Errorf("line %d mixes pages %d and %d", i, line.Words[0].Page, word.Page)
			}
		}
	}
}

// TestLinePageFallsBackToParameter держит запасной номер страницы: у строки,
// собранной из слов без номера (синтетический вход тестов), source_page берётся
// из параметра разбора.
func TestLinePageFallsBackToParameter(t *testing.T) {
	line := Line{Top: 1, Words: []Word{{Text: "a", Left: 0, Top: 1}}}

	if got := linePage(line, 4); got != 4 {
		t.Errorf("linePage of a page-less line = %d, want the fallback 4", got)
	}

	line.Words[0].Page = 7

	if got := linePage(line, 4); got != 7 {
		t.Errorf("linePage = %d, want 7 from the row itself", got)
	}
}

// joinIFRSFixtures склеивает TSV-снимки страниц в один файл так же, как это
// делает joinFiles для вывода pdftotext: простой конкатенацией байтов, без
// вставки разделителя (в TSV его роль играет повторный заголовок, который
// parseTSV отбрасывает по имени первой колонки).
func joinIFRSFixtures(t *testing.T, paths ...string) string {
	t.Helper()

	var joined []byte

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}

		joined = append(joined, data...)
	}

	path := filepath.Join(t.TempDir(), "ifrs-joined.tsv")
	if err := os.WriteFile(path, joined, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	return path
}
