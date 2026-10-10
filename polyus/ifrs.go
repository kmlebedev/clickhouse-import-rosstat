package polyus

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

// parseIFRSPage разбирает извлечённый pdftotext текст одной страницы МСФО-отчёта
// (en_msfo_p6.txt/p7.txt, английская версия otchetnost-6m2026_eng.pdf).
//
// Форма строки — та же, что на KPI-страницах: метка статьи идёт первой, а за ней
// числовые столбцы отчётного и предыдущего периодов:
//
//	Gold sales                                                             4,569      3,581
//	Total revenue                                                          4,674      3,688
//	Operating expenses and other income / (expenses)                      (3,405)    (1,001)
//	   -    basic                                                           0.87       2.13
//
// (это проверено по живой фикстуре: pdftotext -layout печатает метку слева, а
// не справа). Раскладка та же, поэтому и разбор идёт тем же способом, что в
// parseKPIPage: matchMetric ищет самый длинный подходящий префикс, а значение
// берётся из ПЕРВОГО числового токена — он и есть отчётный период.
//
// Отличие от KPI-страниц только в источнике периода: строки МСФО не содержат
// шапки с периодами вовсе, поэтому период приходит параметром из метаданных
// отчёта (для страницы 6 это "2026H1"). Скобки означают отрицательное значение
// ((3,405) → -3405) — это уже умеет parseNumericToken.
//
// Страница без единой опознанной статьи — не ошибка: KPI-страница, поданная
// этому парсеру, возвращает пустой слайс (Review Focus #2). Метрики, которых на
// странице нет, не выдумываются.
func parseIFRSPage(
	textPath string,
	sourceURL string,
	page int,
	period string,
) (records []MetricRecord, err error) {
	file, err := os.Open(textPath)
	if err != nil {
		return nil, fmt.Errorf("open extracted page: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("close extracted page %s: %v", textPath, closeErr)
		}
	}()

	foundMetrics := make(map[string]bool)

	scanner := bufio.NewScanner(file)

	// Некоторые PDF-строки могут быть длиннее стандартных 64 KiB.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	skipped := 0

	for scanner.Scan() {
		line := normalizeLine(scanner.Text())

		if line == "" {
			continue
		}

		definition, remainder, found := matchIFRSMetric(line)
		if !found {
			skipped++
			continue
		}

		if foundMetrics[definition.Name] {
			// Защита от повторного совпадения в сносках.
			continue
		}

		tokens := extractValueTokens(remainder)
		if len(tokens) == 0 {
			skipped++
			continue
		}

		// Первый числовой токен — отчётный период, второй — предыдущий.
		// Нам нужен только отчётный.
		value, err := parseNumericToken(tokens[0])
		if err != nil {
			return nil, fmt.Errorf(
				"metric %s, token %q: %w",
				definition.Name,
				tokens[0],
				err,
			)
		}
		if value == nil {
			skipped++
			continue
		}

		foundMetrics[definition.Name] = true

		records = append(records, MetricRecord{
			Company:    "PLZL",
			Metric:     definition.Name,
			Period:     period,
			PeriodType: periodType(period),
			Value:      *value,
			Unit:       definition.Unit,
			SourceURL:  sourceURL,
			SourcePage: page,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan page: %w", err)
	}

	log.Printf("polyus IFRS %s p.%d: %d records, %d non-metric lines skipped", sourceURL, page, len(records), skipped)

	return records, nil
}

// matchIFRSMetric ищет статью МСФО, строка которой начинается с одного из её
// префиксов, и возвращает остаток строки с числами.
//
// Разбор повторяет matchMetric из kpi.go, но по словарю ifrsMetrics: строка
// нормализована (отступы срезаны, поэтому строка маркера списка «   -    basic»
// приходит как «-    basic»), метка и числа разделяются через splitLabelAndValues,
// а префиксы перебираются от длинного к короткому.
//
// Порядок «от длинного к короткому» — ключевой: он защищает от перекрытия меток.
// Проверенные по живой фикстуре пары, где строки начинаются одинаково:
//
//	"Profit for the period" (profit_for_period) — префикс строки
//	  "Profit for the period attributable to:", которая метрикой быть не должна;
//	"Profit before income tax" (profit_before_tax) против "Profit for the
//	  period" — расходятся на слове после «Profit», но проверяются обе;
//	"Total assets" (total_assets) против "Total equity and liabilities" и
//	  "Total liabilities" — общего префикса у них нет, но все три начинаются
//	  с «Total », и совпадение проверяется по полному тексту метки.
//
// Первая пара разрешается не порядком, а шагом извлечения токенов: у строки
// "...attributable to:" чисел нет, поэтому запись не создаётся. Порядок же
// гарантирует, что если в отчёте появится строка ровно "Profit for the period",
// её не перехватит никакая другая метрика. Обратный порядок сломал бы разбор
// так же, как «Капитальные затраты» ломала stripping_capex в kpi.go.
func matchIFRSMetric(line string) (MetricDefinition, string, bool) {
	for _, definition := range ifrsMetricsSortedByPrefixLength() {
		for _, prefix := range definition.Prefix {
			if !strings.HasPrefix(line, prefix) {
				continue
			}

			if remainder, ok := splitLabelAndValues(line[len(prefix):]); ok {
				return definition, remainder, true
			}
		}
	}

	return MetricDefinition{}, "", false
}

// ifrsMetricsSortedByPrefixLength возвращает словарь МСФО, упорядоченный по
// убыванию длины самого длинного префикса.
//
// Порядок нужен matchIFRSMetric: он берёт первое совпадение, поэтому метрика с
// более длинной меткой обязана проверяться раньше. Сортировка устойчивая, так
// что метрики с равной длиной сохраняют порядок объявления в ifrs_metrics.go —
// он и служит разрешением ничьих.
func ifrsMetricsSortedByPrefixLength() []MetricDefinition {
	result := append([]MetricDefinition(nil), ifrsMetrics...)

	sort.SliceStable(result, func(i, j int) bool {
		return longestPrefixLen(result[i]) > longestPrefixLen(result[j])
	})

	return result
}

// periodType выводит тип периода (FY, H или Q) из канонической формы
// "2026H1"/"2026FY"/"2021Q4", чтобы запись МСФО несла тот же PeriodType, что и
// записи KPI-парсера (там тип приходит из шапки, разобранной parsePeriod).
//
// parsePeriod здесь не переиспользуется: его вход — форма релиза ("1H2026",
// "1 п/г 2026"), а канонический период МСФО-парсера получает из метаданных
// отчёта в форме "2026H1", которую parsePeriod не принимает. Период приходит
// извне разбора, поэтому неизвестная форма — не ошибка: тип просто остаётся
// пустым.
func periodType(period string) string {
	if len(period) < 3 {
		return ""
	}

	kind := period[len(period)-2 : len(period)-1]
	switch kind {
	case "H", "Q":
		if isAllASCIIDigits(period[:len(period)-2]) && isAllASCIIDigits(period[len(period)-1:]) {
			return kind
		}
	case "F":
		if strings.HasSuffix(period, "FY") && isAllASCIIDigits(period[:len(period)-2]) {
			return "FY"
		}
	}

	return ""
}

// isAllASCIIDigits сообщает, что строка непуста и состоит только из цифр.
func isAllASCIIDigits(s string) bool {
	if s == "" {
		return false
	}

	for i := 0; i < len(s); i++ {
		if !isASCIIDigit(s[i]) {
			return false
		}
	}

	return true
}
