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
	Prefix string
}

type MetricRecord struct {
	Company        string   `json:"company"`
	Metric         string   `json:"metric"`
	Period         string   `json:"period"`
	PeriodType     string   `json:"period_type"`
	Value          *float64 `json:"value"`
	Unit           string   `json:"unit"`
	Currency       string   `json:"currency,omitempty"`
	SourceURL      string   `json:"source_url"`
	SourcePage     int      `json:"source_page"`
	SourceDocument string   `json:"source_document"`
}

type PeriodColumn struct {
	TokenIndex int
	Period     string
	PeriodType string
}

// На странице таблица имеет колонки:
//
// 2025 | 2024 | Y-o-Y | 2H 2025 | 1H 2025 | H-o-H | 2H 2024 | Y-o-Y
//
// Поэтому берём значения с индексами 0, 1, 3, 4 и 6.
// Процентные изменения с индексами 2, 5 и 7 игнорируем.
var periodColumns = []PeriodColumn{
	{TokenIndex: 0, Period: "2025FY", PeriodType: "FY"},
	{TokenIndex: 1, Period: "2024FY", PeriodType: "FY"},
	{TokenIndex: 3, Period: "2025H2", PeriodType: "H"},
	{TokenIndex: 4, Period: "2025H1", PeriodType: "H"},
	{TokenIndex: 6, Period: "2024H2", PeriodType: "H"},
}

var metrics = []MetricDefinition{
	{
		Name:   "gold_output",
		Unit:   "koz",
		Prefix: "Gold output, koz",
	},
	{
		Name:   "gold_sold",
		Unit:   "koz",
		Prefix: "Gold sold, koz",
	},
	{
		Name:   "revenue",
		Unit:   "USD million",
		Prefix: "Revenue",
	},
	{
		Name:   "operating_profit",
		Unit:   "USD million",
		Prefix: "Operating profit",
	},
	{
		Name:   "profit_for_period",
		Unit:   "USD million",
		Prefix: "Profit for the period",
	},
	{
		Name:   "eps_basic",
		Unit:   "USD/share",
		Prefix: "Earnings per share – basic, $",
	},
	{
		Name:   "eps_diluted",
		Unit:   "USD/share",
		Prefix: "Earnings per share – diluted, $",
	},
	{
		Name:   "adjusted_net_profit",
		Unit:   "USD million",
		Prefix: "Adjusted net profit",
	},
	{
		Name:   "adjusted_net_profit_margin",
		Unit:   "percent",
		Prefix: "Adjusted net profit margin, %",
	},
	{
		Name:   "adjusted_ebitda",
		Unit:   "USD million",
		Prefix: "Adjusted EBITDA from continuing operations",
	},
	{
		Name:   "capex",
		Unit:   "USD million",
		Prefix: "CAPEX",
	},
	{
		Name:   "stripping_capex",
		Unit:   "USD million",
		Prefix: "Stripping CAPEX",
	},
	{
		Name:   "tcc_per_ounce",
		Unit:   "USD/oz",
		Prefix: "Total cash cost (TCC) per ounce sold, $/oz",
	},
	{
		Name:   "aisc_per_ounce",
		Unit:   "USD/oz",
		Prefix: "All-in sustaining cash cost (AISC) per ounce sold, $/oz",
	},
	{
		Name:   "cash_and_cash_equivalents",
		Unit:   "USD million",
		Prefix: "Cash and cash equivalents",
	},
	{
		Name:   "bank_deposits",
		Unit:   "USD million",
		Prefix: "Bank deposits",
	},
	{
		Name:   "net_debt_including_derivatives",
		Unit:   "USD million",
		Prefix: "Net debt (incl. derivatives)",
	},
	{
		Name:   "net_debt_to_adjusted_ebitda",
		Unit:   "ratio",
		Prefix: "Net debt (incl. derivatives) / adjusted EBITDA, x",
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

func parseKPIPage(
	textPath string,
	sourceURL string,
	page int,
) ([]MetricRecord, error) {
	file, err := os.Open(textPath)
	if err != nil {
		return nil, fmt.Errorf("open extracted page: %w", err)
	}
	defer file.Close()

	var records []MetricRecord

	foundMetrics := make(map[string]bool)

	scanner := bufio.NewScanner(file)

	// Некоторые PDF-строки могут быть длиннее стандартных 64 KiB.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := normalizeLine(scanner.Text())

		if line == "" {
			continue
		}

		definition, remainder, found := matchMetric(line)
		if !found {
			continue
		}

		if foundMetrics[definition.Name] {
			// Защита от повторного совпадения в сносках.
			continue
		}

		tokens := extractValueTokens(remainder)

		// Ожидаем восемь колонок:
		// 2025, 2024, YoY, 2H25, 1H25, HoH, 2H24, YoY.
		if len(tokens) < 7 {
			fmt.Fprintf(
				os.Stderr,
				"warning: metric %q has only %d value tokens: %+v\nline: %q\n",
				definition.Name,
				len(tokens),
				tokens,
				line,
			)
			continue
		}

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
				Company:        "PJSC Polyus",
				Metric:         definition.Name,
				Period:         column.Period,
				PeriodType:     column.PeriodType,
				Value:          value,
				Unit:           definition.Unit,
				SourceURL:      sourceURL,
				SourcePage:     page,
				SourceDocument: "Financial Results & Trading Update FY2025",
			}

			if strings.HasPrefix(definition.Unit, "USD") {
				record.Currency = "USD"
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
	for _, definition := range metricsSortedByPrefixLength() {
		index := strings.Index(line, definition.Prefix)
		if index != 0 {
			continue
		}

		remainder := strings.TrimSpace(
			line[len(definition.Prefix):],
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

func init() {
	FinDataBookPolyus := FinDataBook{
		name: "polyus_financial_metrics",
		// https://polyus.com/en/investors/results-and-reports/
		pdfPaths: map[string]int{
			//"https://polyus.com/upload/iblock/eca/2026_03_16_2h25-_-tu-mda-eng.pdf": 26,
			//"https://polyus.com/upload/iblock/bbb/2025_03_05_fr-12m-2024_eng.pdf": 4,
			//"https://polyus.com/upload/iblock/cb5/2024_02_29_plzl_financial-results_fy2023_eng.pdf": 4,
			//"https://polyus.com/upload/iblock/737/2023_03_15_fy2022-financial-results_eng.pdf": 4,
			//"https://polyus.com/upload/iblock/f4c/2022_03_01_press_release_4qfy2021-eng.pdf": 4,
			//"https://polyus.com/upload/iblock/f1f/press_release_4q_fy2020.pdf": 4,
			// "https://polyus.com/upload/iblock/d91/press_release_4q-fy2019_final-_1_.pdf": 3,
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
				currency LowCardinality(String),
				source_url LowCardinality(String),
				source_page UInt16,
				source_document LowCardinality(String),
				loaded_at DateTime DEFAULT now()
			)
			ENGINE = ReplacingMergeTree(loaded_at)
			ORDER BY (
				company,
				metric,
				period,
				source_document
			);`,
		tableColNum:    1,
		pageImportFunc: parseKPIPage,
	}

	chimport.Stats = append(chimport.Stats, &FinDataBookPolyus)
}
