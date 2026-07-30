package financial

import (
	"bufio"
	"fmt"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type MetricDefinition struct {
	Name   string
	Unit   string
	Prefix []string
}

type MetricRecord struct {
	Company    string  `json:"company"`
	Metric     string  `json:"metric"`
	Period     string  `json:"period"`
	PeriodType string  `json:"period_type"`
	Value      float64 `json:"value"`
	Unit       string  `json:"unit"`
	SourceURL  string  `json:"source_url"`
	SourcePage int     `json:"source_page"`
}

type PeriodColumn struct {
	TokenIndex int
	Period     string
	PeriodType string
}

var metrics = []MetricDefinition{
	{
		Name: "period",
		Unit: "period_type",
		Prefix: []string{
			"$ mln unless specified otherwise",
			"$ million (if not mentioned otherwise)",
			"$ mln (if not mentioned otherwise)",
			"$ million (if not mentioned ",
			"$ mln (if not mentioned"},
	},
	{
		Name:   "gold_output",
		Unit:   "koz",
		Prefix: []string{"Gold output, koz", "Gold production (koz)"},
	},
	{
		Name:   "gold_sold",
		Unit:   "koz",
		Prefix: []string{"Gold sold, koz", "Gold sold (koz)"},
	},
	{
		Name:   "revenue",
		Unit:   "USD million",
		Prefix: []string{"Revenue", "Total revenue"},
	},
	{
		Name:   "operating_profit",
		Unit:   "USD million",
		Prefix: []string{"Operating profit"},
	},
	{
		Name:   "profit_for_period",
		Unit:   "USD million",
		Prefix: []string{"Profit for the period"},
	},
	{
		Name:   "eps_basic",
		Unit:   "USD/share",
		Prefix: []string{"Earnings per share – basic, $", "Earnings per share – basic (US\nDollar)"},
	},
	{
		Name:   "eps_diluted",
		Unit:   "USD/share",
		Prefix: []string{"Earnings per share – diluted, $", "Earnings per share – diluted (US\nDollar)"},
	},
	{
		Name:   "adjusted_net_profit",
		Unit:   "USD million",
		Prefix: []string{"Adjusted net profit"},
	},
	{
		Name:   "adjusted_net_profit_margin",
		Unit:   "percent",
		Prefix: []string{"Adjusted net profit margin, %"},
	},
	{
		Name:   "adjusted_ebitda",
		Unit:   "USD million",
		Prefix: []string{"Adjusted EBITDA from continuing operations"},
	},
	{
		Name:   "capex",
		Unit:   "USD million",
		Prefix: []string{"CAPEX", "Capital expenditure"},
	},
	{
		Name:   "stripping_capex",
		Unit:   "USD million",
		Prefix: []string{"Stripping CAPEX"},
	},
	{
		Name:   "tcc_per_ounce",
		Unit:   "USD/oz",
		Prefix: []string{"Total cash cost (TCC) per ounce sold, $/oz", "Total cash cost (TCC) per ounce\nsold ($/oz)"},
	},
	{
		Name:   "aisc_per_ounce",
		Unit:   "USD/oz",
		Prefix: []string{"All-in sustaining cash cost (AISC) per ounce sold, $/oz", "All-in sustaining cash cost (AISC)\nper ounce sold ($/oz)"},
	},
	{
		Name:   "cash_and_cash_equivalents",
		Unit:   "USD million",
		Prefix: []string{"Cash and cash equivalents"},
	},
	{
		Name:   "bank_deposits",
		Unit:   "USD million",
		Prefix: []string{"Bank deposits"},
	},
	{
		Name:   "net_debt_including_derivatives",
		Unit:   "USD million",
		Prefix: []string{"Net debt (incl. derivatives)"},
	},
	{
		Name:   "net_debt_to_adjusted_ebitda",
		Unit:   "ratio",
		Prefix: []string{"Net debt (incl. derivatives) / adjusted EBITDA, x", "Net debt/adjusted EBITDA (x)"},
	},
}

// Числа в таблице могут выглядеть так:
//
// 8,723
// 3.99
// 38%
// (16%)
// (12)
// -
//
// Скобки интерпретируются как отрицательное значение.
// Для самих KPI скобки встречаются редко, но это полезно для других таблиц.
var valueTokenRegexp = regexp.MustCompile(
	`^\(?-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?%?\)?$|^-$`,
)

var periodMatch = regexp.MustCompile(`([1-4][HQ])\s+(\d{4})`)
var reFY = regexp.MustCompile(`^(\d{4})$`)        // Строго 4 цифры года
var reH = regexp.MustCompile(`^([12])H(\d{4})$`)  // Формат XHYYYY (например, 2H2025)
var reQ = regexp.MustCompile(`^([1-4])Q(\d{4})$`) // Формат XHYYYY (например, 2H2025)

type PeriodInfo struct {
	Period string // Формат: 2024FY или 2025H2 или 2021Q4
	Type   string // Тип: FY или H2 (или H, как в вашем запросе)
}

func parsePeriod(input string) (PeriodInfo, error) {
	// 1. Проверяем формат полного года (например, "2024")
	if reFY.MatchString(input) {
		return PeriodInfo{
			Period: input + "FY",
			Type:   "FY",
		}, nil
	}

	// 2. Проверяем формат полугодия (например, "2H2025")
	if matches := reH.FindStringSubmatch(input); matches != nil {
		half := matches[1] // "1" или "2"
		year := matches[2] // например, "2025"

		return PeriodInfo{
			Period: fmt.Sprintf("%sH%s", year, half), // Склеиваем в 2025H2
			Type:   "H",                              // Базовый тип периода
		}, nil
	}

	// 3. Проверяем формат полугодия (например, "4Q2022")
	if matches := reQ.FindStringSubmatch(input); matches != nil {
		quart := matches[1] // от "1" до "2"
		year := matches[2]  // например, "2022"

		return PeriodInfo{
			Period: fmt.Sprintf("%sQ%s", year, quart), // Склеиваем в 2022Q2
			Type:   "Q",                               // Базовый тип периода
		}, nil
	}

	return PeriodInfo{}, fmt.Errorf("unknown format: %s", input)
}

func parseKPIPage(
	textPath string,
	sourceURL string,
	page int,
) (records []MetricRecord, err error) {
	file, err := os.Open(textPath)
	if err != nil {
		return nil, fmt.Errorf("open extracted page: %w", err)
	}
	defer file.Close()
	foundMetrics := make(map[string]bool)

	scanner := bufio.NewScanner(file)

	// Некоторые PDF-строки могут быть длиннее стандартных 64 KiB.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var periodColumns = []PeriodColumn{}

	for scanner.Scan() {
		lineRaw := scanner.Text()
		line := normalizeLine(lineRaw)

		if line == "" {
			continue
		}

		definition, remainder, found := matchMetric(line)
		if !found {
			if strings.Contains("line", "koz") {
				fmt.Printf("not found: %s\n", line)
			}
			continue
		}

		//fmt.Printf("found definition: %+v remainder: %s in line: %s\n", definition, remainder, line)
		if foundMetrics[definition.Name] {
			// Защита от повторного совпадения в сносках.
			continue
		}

		if definition.Name == "period" {
			var PeriodColumnFound bool
			if periodColumns, PeriodColumnFound = PeriodColumnMap[sourceURL]; PeriodColumnFound {
				continue
			}
			periods := strings.Split(periodMatch.ReplaceAllString(remainder, "${1}${2}"), " ")
			//  2025 2024 Y-o-Y 2H2025 1H2025 H-o-H 2H2024 Y-o-Y
			for idx, period := range periods {
				info, err := parsePeriod(period)
				if err != nil {
					continue
				}
				// {TokenIndex: 1, Period: "2024FY", PeriodType: "FY"},
				// {TokenIndex: 3, Period: "2025H2", PeriodType: "H"},
				periodColumns = append(periodColumns, PeriodColumn{idx, info.Period, info.Type})
			}
			fmt.Printf("%s\nperiods: %+v\nline: %+v\nlineRaw: %s\n", sourceURL, periodColumns, periods, lineRaw)
			continue
		}
		tokens := extractValueTokens(remainder)

		fmt.Printf("%s tokens(%d>%d) %+v\n", definition.Name, len(tokens), len(periodColumns), tokens)
		foundMetrics[definition.Name] = true
		for _, column := range periodColumns {
			if column.TokenIndex >= len(tokens) {
				continue
			}

			value, err := parseNumericToken(tokens[column.TokenIndex])
			if err != nil {
				return nil, fmt.Errorf(
					"metric %s, period %s, token %q: %w",
					definition.Name,
					column.Period,
					tokens[column.TokenIndex],
					err,
				)
			}

			record := MetricRecord{
				Company:    "PJSC Polyus",
				Metric:     definition.Name,
				Period:     column.Period,
				PeriodType: column.PeriodType,
				Value:      *value,
				Unit:       definition.Unit,
				SourceURL:  sourceURL,
				SourcePage: page,
			}

			records = append(records, record)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan page: %w", err)
	}

	return records, nil
}

func matchMetric(line string) (MetricDefinition, string, bool) {
	// Сначала проверяем самые длинные префиксы.
	// Это важно, например, для CAPEX и Stripping CAPEX.
	var index int
	var prefix string
	for _, definition := range metricsSortedByPrefixLength() {
		for _, prefix = range definition.Prefix {
			index = strings.Index(line, prefix)
			if index == 0 {
				break
			}
		}
		if index != 0 {
			continue
		}
		spaceIndex := strings.Index(line[len(prefix):], " ")
		if spaceIndex == -1 {
			continue
		}

		remainder := strings.TrimSpace(
			line[len(prefix)+spaceIndex:],
		)

		return definition, remainder, true
	}

	return MetricDefinition{}, "", false
}

func metricsSortedByPrefixLength() []MetricDefinition {
	result := append([]MetricDefinition(nil), metrics...)

	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if len(result[j].Prefix) > len(result[i].Prefix) {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}

func normalizeLine(line string) string {
	replacer := strings.NewReplacer(
		"\u00a0", " ",
		"\u2007", " ",
		"\u202f", " ",
		"\u2013", "–",
		"\u2014", "–",
	)

	line = replacer.Replace(line)

	// Удаляем управляющие символы, иногда попадающие из PDF.
	line = strings.Map(func(r rune) rune {
		switch {
		case r == '\t':
			return ' '
		case r >= 32:
			return r
		default:
			return -1
		}
	}, line)

	return strings.Join(strings.Fields(line), " ")
}

func extractValueTokens(text string) []string {
	fields := strings.Fields(text)

	result := make([]string, 0, len(fields))

	for _, field := range fields {
		field = strings.ToLower(strings.TrimSpace(field))

		switch field {
		case "n/a":
			field = "0"
		case "n.a":
			field = "0"
		case "n.a.":
			field = "0"
		case "-":
			field = "0"
		}
		if valueTokenRegexp.MatchString(field) {
			result = append(result, field)
		}
	}

	return result
}

func parseNumericToken(token string) (*float64, error) {
	token = strings.TrimSpace(token)

	if token == "" || token == "-" || strings.EqualFold(token, "n/a") {
		return nil, nil
	}

	negative := strings.HasPrefix(token, "(") &&
		strings.HasSuffix(token, ")")

	token = strings.TrimPrefix(token, "(")
	token = strings.TrimSuffix(token, ")")
	token = strings.TrimSuffix(token, "%")
	token = strings.ReplaceAll(token, ",", "")

	value, err := strconv.ParseFloat(token, 64)
	if err != nil {
		return nil, fmt.Errorf("parse float: %w", err)
	}

	if negative {
		value = -value
	}

	return &value, nil
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

var PeriodColumnMap = map[string][]PeriodColumn{
	"https://polyus.com/upload/iblock/7fb/report_management-discussion-and-analysis_financial-statements-_fy2014.pdf": {
		{0, "2014FY", "FY"},
		{1, "2013FY", "FY"},
		{3, "2014H2", "H"},
		{4, "2014H1", "H"},
	},
	"https://polyus.com/upload/iblock/9dd/mda_financial_statements_fy2015.pdf": {
		{0, "2015FY", "FY"},
		{1, "2014FY", "FY"},
		{3, "2015H2", "H"},
		{4, "2015H1", "H"},
	},
	"https://polyus.com/upload/iblock/d51/2017_mda_opinion_fs-_1_.pdf": {
		{0, "2017Q4", "Q"},
		{1, "2017Q3", "Q"},
		{3, "2017FY", "FY"},
		{4, "2016FY", "FY"},
	},
	"https://polyus.com/upload/iblock/d91/press_release_4q-fy2019_final-_1_.pdf": {
		{0, "2019Q4", "Q"},
		{1, "2019Q3", "Q"},
		{3, "2018Q4", "Q"},
		{5, "2019FY", "FY"},
		{6, "2018FY", "FY"},
	},
	"https://polyus.com/upload/iblock/f1f/press_release_4q_fy2020.pdf": {
		{0, "2020Q4", "Q"},
		{1, "2020Q3", "Q"},
		{3, "2019Q4", "Q"},
		{5, "2020FY", "FY"},
		{6, "2019FY", "FY"},
	},
	"https://polyus.com/upload/iblock/f4c/2022_03_01_press_release_4qfy2021-eng.pdf": {
		{0, "2021Q4", "Q"},
		{1, "2021Q3", "Q"},
		{3, "2020Q4", "Q"},
		{5, "2021FY", "FY"},
		{6, "2020FY", "FY"},
	},
}

func init() {
	FinDataBookPolyus := FinDataBook{
		name: "polyus_financial_metrics",
		// https://polyus.com/en/investors/results-and-reports/
		pdfPaths: map[string]int{
			"https://polyus.com/upload/iblock/7fb/report_management-discussion-and-analysis_financial-statements-_fy2014.pdf": 4,
			// "https://polyus.com/upload/iblock/9dd/mda_financial_statements_fy2015.pdf": 6,
			// "https://polyus.com/upload/iblock/965/fy2016-mda-fs-opinion.pdf":           5,
			// "https://polyus.com/upload/iblock/d51/2017_mda_opinion_fs-_1_.pdf": 5,
			// "https://polyus.com/upload/iblock/9bc/polyus-group-ifrs-cons-fs-18_e_signed.pdf": 5,
			//"https://polyus.com/upload/iblock/d91/press_release_4q-fy2019_final-_1_.pdf":     3,
			//"https://polyus.com/upload/iblock/f1f/press_release_4q_fy2020.pdf":                      4,
			//"https://polyus.com/upload/iblock/f4c/2022_03_01_press_release_4qfy2021-eng.pdf":        4,
			//"https://polyus.com/upload/iblock/737/2023_03_15_fy2022-financial-results_eng.pdf":      4,
			//"https://polyus.com/upload/iblock/cb5/2024_02_29_plzl_financial-results_fy2023_eng.pdf": 4,
			//"https://polyus.com/upload/iblock/bbb/2025_03_05_fr-12m-2024_eng.pdf":                   4,
			//"https://polyus.com/upload/iblock/eca/2026_03_16_2h25-_-tu-mda-eng.pdf":                 26,
		},
		tables:    map[string][]string{},
		insertRow: "INSERT INTO %s VALUES (?, ?, ?, ?, ?)",
		createTable: `CREATE TABLE IF NOT EXISTS %s
			(
				company LowCardinality(String),
				metric LowCardinality(String),
				period String,
				period_type Enum8(
					'Q' = 1,
					'H' = 2,
					'FY' = 3,
					'LTM' = 4
				),
				value Nullable(Float64),
				unit LowCardinality(String),
				source_url LowCardinality(String),
				source_page UInt16,
				loaded_at DateTime DEFAULT now()
			)
			ENGINE = ReplacingMergeTree(loaded_at)
			ORDER BY (
				company,
				metric,
				period
			);`,
		tableColNum:    1,
		pageImportFunc: parseKPIPage,
	}

	chimport.Stats = append(chimport.Stats, &FinDataBookPolyus)
}
