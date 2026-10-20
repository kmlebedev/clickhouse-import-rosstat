package minfin

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

// ofzHeaderAliases — нормализованные подписи шапки → поле таблицы. Нормализация
// снимает регистр, пробелы, звёздочки-сноски и дефисы: в 2023 году первая колонка
// называется «Дата аукциона», в 2024+ — «Дата», а «Доходность по средневзве-
// шенной цене» приходит с переносом и дефисом.
var ofzHeaderAliases = map[string]string{
	"дата":                      "date",
	"датааукциона":              "date",
	"формат":                    "format",
	"кодвыпуска":                "issue",
	"типбумаги":                 "paper_type",
	"датапогашения":             "maturity_date",
	"днейдопогашения":           "days_to_maturity",
	"объемпредложения":          "offer_volume",
	"ценаотсечения":             "cutoff_price",
	"ценасредневзвешенная":      "weighted_price",
	"доходностьпоценеотсечения": "cutoff_yield",
	"доходностьпосредневзвешеннойцене":          "weighted_yield",
	"совокупныйобъемспросапономиналу":           "demand",
	"объемразмещенияпономиналу":                 "placed",
	"объемвыручки":                              "revenue",
	"коэффициентудовлетворенияспросанааукционе": "coverage",
}

var ofzHeaderReplacer = strings.NewReplacer(
	" ", "", "\u00a0", "", "*", "", "-", "", "—", "", "–", "",
)

var ofzNumberReplacer = strings.NewReplacer(" ", "", "\u00a0", "")

func normalizeOfzHeader(label string) string {
	return ofzHeaderReplacer.Replace(strings.ToLower(strings.TrimSpace(label)))
}

// parseOfzDate принимает сырой Excel-сериал и текст: GetRows(RawCellValue)
// отдаёт даты числом, а ячейки с текстовой датой — строкой. В отличие от
// parseFedbudDate, день сохраняется: аукцион привязан к конкретной дате, а не
// к первому числу месяца.
func parseOfzDate(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if serial, err := strconv.ParseFloat(raw, 64); err == nil {
		if serial < 20000 {
			return time.Time{}, fmt.Errorf("сериал Excel %q вне ожидаемого диапазона дат", raw)
		}
		t, err := excelize.ExcelDateToTime(serial, false)
		if err != nil {
			return time.Time{}, err
		}
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	}
	for _, layout := range []string{"2006-01-02", "02.01.2006", "2006-01-02 15:04:05", "02.01.2006 15:04:05"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
		}
	}
	return time.Time{}, fmt.Errorf("дата %q не разобрана", raw)
}

// parseOfzNumber возвращает (значение, есть ли точка, ошибка). Пустая ячейка,
// «-», «****», «×» и «н/п» — не точка: у несостоявшегося аукциона источник
// оставляет такие маркеры, и это NULL, а не ноль.
func parseOfzNumber(raw string) (float64, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "н/п" {
		return 0, false, nil
	}
	// «-», «****», «-****», «×», «x» — маркеры отсутствия. Заодно покрывается
	// сочетание дефиса и звёзд, которое источник ставит у несостоявшегося аукциона.
	if strings.Trim(raw, "-*×xX \t") == "" {
		return 0, false, nil
	}
	s := ofzNumberReplacer.Replace(raw)
	if strings.Contains(s, ",") && strings.Contains(s, ".") {
		s = strings.ReplaceAll(s, ",", "")
	} else {
		s = strings.ReplaceAll(s, ",", ".")
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false, fmt.Errorf("значение %q не число: %w", raw, err)
	}
	return value, true, nil
}

// isOfzDateCell отличает строку с датой от служебных строк файла: над шапкой
// идут пустая строка, единицы измерения и строка номеров колонок «1 2 3 …».
// Число меньше 20000 — не дата (это номер колонки), текст без точки/дефиса —
// не дата (единицы, сноски).
func isOfzDateCell(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.HasPrefix(raw, "*") {
		return false
	}
	if serial, err := strconv.ParseFloat(raw, 64); err == nil {
		return serial >= 20000
	}
	return (strings.Contains(raw, ".") || strings.Contains(raw, "-")) && len(raw) >= 6
}

func ofzNullable(raw string) (any, error) {
	value, ok, err := parseOfzNumber(raw)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return value, nil
}

// parseOfzAuction разбирает годовой файл аукционов. Шапка ищется по подписям, а
// не по номерам колонок: у 2023 года 14 колонок и нет «Формат», у 2024+ — 15.
// Строки-сноски и пустые строки пропускаются; отсутствие обязательной колонки,
// неразобранная дата или ноль точек — ошибка.
func parseOfzAuction(xlsx *excelize.File, batch driver.Batch) error {
	sheet := xlsx.GetSheetList()[0]
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист %q: %w", ofzAuctionTable, sheet, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("%s: лист %q пуст", ofzAuctionTable, sheet)
	}

	headerRow := -1
	columns := map[string]int{}
	for i, row := range rows {
		candidate := map[string]int{}
		for j, cell := range row {
			if field, ok := ofzHeaderAliases[normalizeOfzHeader(cell)]; ok {
				candidate[field] = j
			}
		}
		if _, ok := candidate["date"]; ok {
			if _, ok := candidate["issue"]; ok {
				headerRow, columns = i, candidate
				break
			}
		}
	}
	if headerRow < 0 {
		return fmt.Errorf("%s: лист %q: не найдена шапка с колонками «Дата»/«Код выпуска»", ofzAuctionTable, sheet)
	}

	required := []string{"date", "issue", "paper_type", "maturity_date", "days_to_maturity", "offer_volume", "demand", "placed", "revenue"}
	for _, field := range required {
		if _, ok := columns[field]; !ok {
			return fmt.Errorf("%s: лист %q: в шапке нет обязательной колонки %q", ofzAuctionTable, sheet, field)
		}
	}

	points := 0
	for _, row := range rows[headerRow+1:] {
		if len(row) == 0 {
			continue
		}
		rawDate := strings.TrimSpace(row[columns["date"]])
		if !isOfzDateCell(rawDate) {
			continue
		}
		date, err := parseOfzDate(rawDate)
		if err != nil {
			return fmt.Errorf("%s: лист %q: дата аукциона %q: %w", ofzAuctionTable, sheet, rawDate, err)
		}

		format := "Аукцион"
		if j, ok := columns["format"]; ok && j < len(row) && strings.TrimSpace(row[j]) != "" {
			format = strings.TrimSpace(row[j])
		}
		cell := func(field string) string {
			if j, ok := columns[field]; ok && j < len(row) {
				return strings.TrimSpace(row[j])
			}
			return ""
		}

		issue := cell("issue")
		if issue == "" {
			return fmt.Errorf("%s: лист %q, строка %s: пустой код выпуска", ofzAuctionTable, sheet, date.Format("2006-01-02"))
		}
		paperType := cell("paper_type")
		maturity, err := parseOfzDate(cell("maturity_date"))
		if err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: дата погашения %q: %w", ofzAuctionTable, sheet, issue, cell("maturity_date"), err)
		}
		days, ok, err := parseOfzNumber(cell("days_to_maturity"))
		if err != nil || !ok {
			return fmt.Errorf("%s: лист %q, выпуск %s: дней до погашения %q: %v", ofzAuctionTable, sheet, issue, cell("days_to_maturity"), err)
		}
		offer, ok, err := parseOfzNumber(cell("offer_volume"))
		if err != nil || !ok {
			return fmt.Errorf("%s: лист %q, выпуск %s: объём предложения %q: %v", ofzAuctionTable, sheet, issue, cell("offer_volume"), err)
		}

		cutoffPrice, err := ofzNullable(cell("cutoff_price"))
		if err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: цена отсечения: %w", ofzAuctionTable, sheet, issue, err)
		}
		weightedPrice, err := ofzNullable(cell("weighted_price"))
		if err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: средневзвешенная цена: %w", ofzAuctionTable, sheet, issue, err)
		}
		cutoffYield, err := ofzNullable(cell("cutoff_yield"))
		if err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: доходность отсечения: %w", ofzAuctionTable, sheet, issue, err)
		}
		weightedYield, err := ofzNullable(cell("weighted_yield"))
		if err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: средневзвешенная доходность: %w", ofzAuctionTable, sheet, issue, err)
		}
		demand, err := ofzNullable(cell("demand"))
		if err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: спрос: %w", ofzAuctionTable, sheet, issue, err)
		}
		placed, err := ofzNullable(cell("placed"))
		if err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: размещено: %w", ofzAuctionTable, sheet, issue, err)
		}
		revenue, err := ofzNullable(cell("revenue"))
		if err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: выручка: %w", ofzAuctionTable, sheet, issue, err)
		}
		coverage, err := ofzNullable(cell("coverage"))
		if err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: коэффициент покрытия: %w", ofzAuctionTable, sheet, issue, err)
		}

		if err = batch.Append(
			date, format, issue, paperType, maturity, uint32(days), offer,
			cutoffPrice, weightedPrice, cutoffYield, weightedYield,
			demand, placed, revenue, coverage,
		); err != nil {
			return fmt.Errorf("%s: лист %q, выпуск %s: запись строки: %w", ofzAuctionTable, sheet, issue, err)
		}
		points++
	}
	if points == 0 {
		return fmt.Errorf("%s: лист %q: разбор не дал ни одной точки", ofzAuctionTable, sheet)
	}
	return nil
}
