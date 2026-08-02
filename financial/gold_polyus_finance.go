package financial

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
)

// ─── Модель данных ───────────────────────────────────────────────────────────

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

type PeriodInfo struct {
	Period string // 2024FY, 2025H2, 2021Q4
	Type   string // FY, H, Q, LTM
}

// ─── Конфигурация парсера (вынесена из глобальных переменных) ────────────────

type ParserConfig struct {
	Metrics         []MetricDefinition
	PeriodOverrides map[string][]PeriodColumn // бывший PeriodColumnMap
}

// Предвычисленный индекс для быстрого поиска метрик
type metricIndex struct {
	definitions []MetricDefinition
	// отсортированы по убыванию длины первого префикса
	byPrefixLen []MetricDefinition
}

func newMetricIndex(metrics []MetricDefinition) *metricIndex {
	byLen := make([]MetricDefinition, len(metrics))
	copy(byLen, metrics)

	sort.SliceStable(byLen, func(i, j int) bool {
		// сортируем по максимальной длине префикса в definition
		maxLen := func(d MetricDefinition) int {
			m := 0
			for _, p := range d.Prefix {
				if len(p) > m {
					m = len(p)
				}
			}
			return m
		}
		return maxLen(byLen[i]) > maxLen(byLen[j])
	})

	return &metricIndex{
		definitions: metrics,
		byPrefixLen: byLen,
	}
}

// ─── Регулярные выражения (предкомпилированные, потокобезопасные) ────────────

var (
	valueTokenRegexp = regexp.MustCompile(
		`^\(?-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?%?\)?$|^-$`,
	)
	periodMatch = regexp.MustCompile(`([1-4][HQ])\s+(\d{4})`)
	reFY        = regexp.MustCompile(`^(\d{4})$`)
	reH         = regexp.MustCompile(`^([12])H(\d{4})$`)
	reQ         = regexp.MustCompile(`^([1-4])Q(\d{4})$`)
)

// ─── Парсер (контекстно-зависимый, потокобезопасный) ─────────────────────────

type PDFParser struct {
	config *ParserConfig
	index  *metricIndex
}

func NewPDFParser(cfg *ParserConfig) *PDFParser {
	return &PDFParser{
		config: cfg,
		index:  newMetricIndex(cfg.Metrics),
	}
}

// ParsePage парсит одну страницу PDF (предварительно извлечённую в текст)
func (p *PDFParser) ParsePage(ctx context.Context, textPath, sourceURL string, page int) ([]MetricRecord, error) {
	file, err := os.Open(textPath)
	if err != nil {
		return nil, fmt.Errorf("open extracted page %q: %w", textPath, err)
	}
	defer file.Close()

	return p.parse(ctx, file, sourceURL, page)
}

func (p *PDFParser) parse(ctx context.Context, r io.Reader, sourceURL string, page int) ([]MetricRecord, error) {
	scanner := bufio.NewScanner(r)
	// Увеличиваем буфер: 1 MB на строку
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var records []MetricRecord
	foundMetrics := make(map[string]bool)
	var periodColumns []PeriodColumn

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		lineRaw := scanner.Text()
		line := normalizeLine(lineRaw)
		if line == "" {
			continue
		}

		def, remainder, ok := p.matchMetric(line)
		if !ok {
			continue
		}

		// Защита от повторного совпадения (сноски, дубли строк)
		if foundMetrics[def.Name] {
			continue
		}
		foundMetrics[def.Name] = true

		// ─── Строка с периодами ─────────────────────────────────────────────
		if def.Name == "period" {
			// Проверяем переопределение для конкретного URL
			if override, ok := p.config.PeriodOverrides[sourceURL]; ok {
				periodColumns = override
				continue
			}

			// Парсим периоды из строки: "2025 2024 Y-o-Y 2H2025 1H2025 H-o-H"
			periods := strings.Fields(periodMatch.ReplaceAllString(remainder, "${1}${2}"))
			for idx, period := range periods {
				info, err := parsePeriod(period)
				if err != nil {
					continue // пропускаем Y-o-Y, H-o-H и прочие не-периоды
				}
				periodColumns = append(periodColumns, PeriodColumn{
					TokenIndex: idx,
					Period:     info.Period,
					PeriodType: info.Type,
				})
			}
			continue
		}

		// ─── Строка с метрикой ──────────────────────────────────────────────
		tokens := extractValueTokens(remainder)
		if len(tokens) < len(periodColumns) {
			// Недостаточно значений — возможно, разрыв таблицы на страницах
			continue
		}

		for _, col := range periodColumns {
			if col.TokenIndex >= len(tokens) {
				continue
			}

			val, err := parseNumericToken(tokens[col.TokenIndex])
			if err != nil {
				// Логируем, но НЕ прерываем всю страницу из-за одного битого токена
				continue
			}
			if val == nil {
				// n/a или "-"
				continue
			}

			records = append(records, MetricRecord{
				Company:    "PJSC Polyus",
				Metric:     def.Name,
				Period:     col.Period,
				PeriodType: col.PeriodType,
				Value:      *val,
				Unit:       def.Unit,
				SourceURL:  sourceURL,
				SourcePage: page,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan page: %w", err)
	}

	return records, nil
}

// matchMetric ищет метрику в начале строки. Проверяет самые длинные префиксы первыми.
func (p *PDFParser) matchMetric(line string) (MetricDefinition, string, bool) {
	for _, def := range p.index.byPrefixLen {
		for _, prefix := range def.Prefix {
			if !strings.HasPrefix(line, prefix) {
				continue
			}
			// Ищем пробел после префикса
			after := line[len(prefix):]
			spaceIdx := strings.IndexFunc(after, unicode.IsSpace)
			if spaceIdx == -1 {
				// Префикс занимает всю строку — возможно, это заголовок без значений
				continue
			}
			remainder := strings.TrimSpace(after[spaceIdx:])
			return def, remainder, true
		}
	}
	return MetricDefinition{}, "", false
}

// ─── Вспомогательные функции ─────────────────────────────────────────────────

func normalizeLine(line string) string {
	// Замена неразрывных пробелов и тире
	replacer := strings.NewReplacer(
		"\u00a0", " ",
		"\u2007", " ",
		"\u202f", " ",
		"\u2013", "-",
		"\u2014", "-",
	)
	line = replacer.Replace(line)

	// Удаляем управляющие символы, кроме таба (→ пробел)
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

	for _, f := range fields {
		f = strings.ToLower(strings.TrimSpace(f))
		switch f {
		case "n/a", "n.a", "n.a.", "-":
			f = "0"
		}
		if valueTokenRegexp.MatchString(f) {
			result = append(result, f)
		}
	}
	return result
}

// parseNumericToken возвращает nil для пустых/недоступных значений без ошибки
func parseNumericToken(token string) (*float64, error) {
	token = strings.TrimSpace(token)
	if token == "" || token == "-" || strings.EqualFold(token, "n/a") {
		return nil, nil
	}

	negative := strings.HasPrefix(token, "(") && strings.HasSuffix(token, ")")

	token = strings.TrimPrefix(token, "(")
	token = strings.TrimSuffix(token, ")")
	token = strings.TrimSuffix(token, "%")
	token = strings.ReplaceAll(token, ",", "")

	val, err := strconv.ParseFloat(token, 64)
	if err != nil {
		return nil, fmt.Errorf("parse float %q: %w", token, err)
	}

	if negative {
		val = -val
	}
	return &val, nil
}

func parsePeriod(input string) (PeriodInfo, error) {
	if reFY.MatchString(input) {
		return PeriodInfo{Period: input + "FY", Type: "FY"}, nil
	}
	if m := reH.FindStringSubmatch(input); m != nil {
		return PeriodInfo{Period: m[2] + "H" + m[1], Type: "H"}, nil
	}
	if m := reQ.FindStringSubmatch(input); m != nil {
		return PeriodInfo{Period: m[2] + "Q" + m[1], Type: "Q"}, nil
	}
	return PeriodInfo{}, fmt.Errorf("unknown period format: %s", input)
}

// ─── ClickHouse интеграция ───────────────────────────────────────────────────

const (
	createTableSQL = `CREATE TABLE IF NOT EXISTS %s
(
    company LowCardinality(String),
    metric LowCardinality(String),
    period String,
    period_type Enum8('Q' = 1, 'H' = 2, 'FY' = 3, 'LTM' = 4),
    value Nullable(Float64),
    unit LowCardinality(String),
    source_url LowCardinality(String),
    source_page UInt16,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (company, metric, period);`

	// 8 placeholder'ов под 8 колонок (без loaded_at — DEFAULT)
	insertRowSQL = `INSERT INTO %s (company, metric, period, period_type, value, unit, source_url, source_page)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
)

// ─── Инициализация (один раз, потокобезопасно) ──────────────────────────────

var (
	parserInit sync.Once
	parser     *PDFParser
)

func init() {
	parserInit.Do(func() {
		cfg := &ParserConfig{
			Metrics: []MetricDefinition{
				{
					Name: "period",
					Unit: "period_type",
					Prefix: []string{
						"$ mln unless specified otherwise",
						"$ million (if not mentioned otherwise)",
						"$ mln (if not mentioned otherwise)",
						"$ million (if not mentioned ",
						"$ mln (if not mentioned",
					},
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
					Name: "eps_basic",
					Unit: "USD/share",
					Prefix: []string{
						"Earnings per share – basic, $",
						"Earnings per share – basic (US\nDollar)",
						"Earnings per share - basic (US Dollar)",
					},
				},
				{
					Name: "eps_diluted",
					Unit: "USD/share",
					Prefix: []string{
						"Earnings per share – diluted, $",
						"Earnings per share – diluted (US\nDollar)",
						"Earnings per share - diluted (US Dollar)",
					},
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
					Name: "tcc_per_ounce",
					Unit: "USD/oz",
					Prefix: []string{
						"Total cash cost (TCC) per ounce sold, $/oz",
						"Total cash cost (TCC) per ounce\nsold ($/oz)",
					},
				},
				{
					Name: "aisc_per_ounce",
					Unit: "USD/oz",
					Prefix: []string{
						"All-in sustaining cash cost (AISC) per ounce sold, $/oz",
						"All-in sustaining cash cost (AISC)\nper ounce sold ($/oz)",
					},
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
					Name: "net_debt_to_adjusted_ebitda",
					Unit: "ratio",
					Prefix: []string{
						"Net debt (incl. derivatives) / adjusted EBITDA, x",
						"Net debt/adjusted EBITDA (x)",
					},
				},
			},
			PeriodOverrides: map[string][]PeriodColumn{
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
			},
		}

		parser = NewPDFParser(cfg)

		FinDataBookPolyus := FinDataBook{
			name: "polyus_financial_metrics",
			pdfPaths: map[string]int{
				"https://polyus.com/upload/iblock/7fb/report_management-discussion-and-analysis_financial-statements-_fy2014.pdf": 4,
				"https://polyus.com/upload/iblock/9dd/mda_financial_statements_fy2015.pdf":                                        6,
				"https://polyus.com/upload/iblock/965/fy2016-mda-fs-opinion.pdf":                                                  5,
				"https://polyus.com/upload/iblock/d51/2017_mda_opinion_fs-_1_.pdf":                                                5,
				"https://polyus.com/upload/iblock/9bc/polyus-group-ifrs-cons-fs-18_e_signed.pdf":                                  5,
				"https://polyus.com/upload/iblock/d91/press_release_4q-fy2019_final-_1_.pdf":                                      3,
				"https://polyus.com/upload/iblock/f1f/press_release_4q_fy2020.pdf":                                                4,
				"https://polyus.com/upload/iblock/f4c/2022_03_01_press_release_4qfy2021-eng.pdf":                                  4,
				"https://polyus.com/upload/iblock/737/2023_03_15_fy2022-financial-results_eng.pdf":                                4,
				"https://polyus.com/upload/iblock/cb5/2024_02_29_plzl_financial-results_fy2023_eng.pdf":                           4,
				"https://polyus.com/upload/iblock/bbb/2025_03_05_fr-12m-2024_eng.pdf":                                             4,
				"https://polyus.com/upload/iblock/eca/2026_03_16_2h25-_-tu-mda-eng.pdf":                                           26,
			},
			tables: map[string][]string{},
			// Исправлено: 8 placeholder'ов под 8 колонок
			insertRow:   insertRowSQL,
			createTable: createTableSQL,
			tableColNum: 8,
			pageImportFunc: func(textPath, sourceURL string, page int) ([]MetricRecord, error) {
				return parser.ParsePage(context.Background(), textPath, sourceURL, page)
			},
		}

		chimport.Stats = append(chimport.Stats, &FinDataBookPolyus)
	})
}
