package polyus

import (
	"fmt"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
)

// parseIFRSPage разбирает TSV-снимок страниц МСФО-отчёта
// (en_msfo_p6.tsv/p7.tsv, английская версия otchetnost-6m2026_eng.pdf).
//
// Раскладка страницы обратна KPI-странице: МЕТКА СТОИТ СЛЕВА, а числа — правее
// неё, по одному столбцу на период:
//
//	Gold sales                                            4,569      3,581
//	Total revenue                                         4,674      3,688
//	Operating expenses and other income / (expenses)    (3,405)    (1,001)
//	-    basic                                            0.87       2.13
//
// Оба столбца — периоды отчётности (у страницы 6 это первое полугодие 2026 и
// первое полугодие 2025), поэтому шапки таблицы на страницу нет и колоночная
// модель columns.go здесь НЕ применяется: её шапка нужна там, где между
// периодами стоит колонка изменения («Изм. за год»), а на МСФО-странице такой
// колонки нет — столбцов ровно два. Период отчётности приходит параметром из
// метаданных отчёта (для страницы 6 это "2026H1").
//
// Без шапки период не с чем сопоставить, поэтому столбцы берутся позиционно:
// отчётный — ПЕРВОЕ число строки (крайнее левое после метки), прошлый — второе
// за ним. Первое число и есть отчётный период, потому что в отчёте столбец
// отчётного периода печатается первым слева (проверено по фикстуре: Gold sales
// 4,569 напротив 2026, 3,581 напротив 2025). Второй столбец в витрину не
// попадает вовсе: запись одна на метрику, и её период — период отчёта.
//
// Скобки означают отрицательное значение ((3,405) → -3405) — это уже умеет
// parseNumericToken, как и запятую-разделитель тысяч.
//
// Страница без единой опознанной статьи — не ошибка: KPI-страница, поданная
// этому парсеру, возвращает пустой слайс (Review Focus #2). Метрики, которых на
// странице нет, не выдумываются.
//
// page — запасной номер страницы: запись получает номер страницы СВОЕЙ строки
// (linePage), потому что extractPDF склеивает все Report.Pages в один файл — для
// английского отчёта 1 п/г 2026 это страницы 6 и 7, — и номер, переданный
// параметром, верен только для первой из них. Запасной номер попадает в запись
// лишь тогда, когда сборка строки не донесла номер (синтетический вход тестов).
func parseIFRSPage(
	textPath string,
	sourceURL string,
	page int,
	period string,
) ([]MetricRecord, error) {
	lines, err := readTSVLines(textPath)
	if err != nil {
		return nil, err
	}

	var records []MetricRecord

	// Метрика попадает в результат один раз: её метка может встретиться и в
	// сносках таблицы, а строка сноски с числами дала бы вторую запись того же
	// ключа (metric, period). Здесь этот счётчик — общий на весь файл, а не на
	// таблицу, как в parseKPILines: разбирается не страница целиком, а то, что
	// вернул extractPDF для Report.Pages, и на производственном входе это НЕ
	// одна таблица — склейка страниц 6 и 7 английского отчёта даёт в одном файле
	// и отчёт о прибылях и убытках, и отчёт о финансовом положении. Метка,
	// совпавшая на второй таблице, записи не добавит: ключ (metric, period) у
	// неё тот же, и повтором её отбросил бы ReplacingMergeTree.
	foundMetrics := make(map[string]bool)

	skipped := 0

	for _, line := range lines {
		text := lineText(line)

		definition, remainder, found := matchIFRSMetric(text)
		if !found {
			skipped++

			continue
		}

		if foundMetrics[definition.Name] {
			// Защита от повторного совпадения в сносках.
			continue
		}

		// Числа строки ищутся только правее метки: граница берётся из самого
		// сопоставления (matchIFRSMetric возвращает остаток строки), а не из
		// координат, потому что слова одной метки разделены теми же узкими
		// промежутками, что и группы одного числа.
		labelEnd := len(text) - len(remainder)

		values := valuesFromWords(line.Words, labelEnd)
		if len(values) == 0 {
			skipped++

			continue
		}

		// Первое число — отчётный период, второе — предыдущий. Нам нужен только
		// отчётный: второй столбец в витрину не попадает.
		value, err := parseNumericToken(values[0].text)
		if err != nil {
			return nil, fmt.Errorf(
				"metric %s, token %q: %w",
				definition.Name,
				values[0].text,
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
			SourceKind: "ifrs",
			Value:      *value,
			Unit:       definition.Unit,
			SourceURL:  sourceURL,
			SourcePage: linePage(line, page),
		})
	}

	log.Infof(
		"polyus IFRS %s p.%d: %d records, %d non-metric lines skipped",
		sourceURL,
		page,
		len(records),
		skipped,
	)

	return records, nil
}

// matchIFRSMetric ищет статью МСФО, строка которой начинается с одного из её
// префиксов, и возвращает остаток строки с числами.
//
// Разбор повторяет matchMetric из kpi.go, но по словарю ifrsMetrics: строка
// приходит склеенной из слов строки (lineText), поэтому метку из нескольких слов
// она несёт подряд, а метка и числа разделяются через splitLabelAndValues.
// Префиксы перебираются от длинного к короткому.
//
// Порядок «от длинного к короткому» — ключевой: он защищает от перекрытия меток.
// Проверенные по живой фикстуре пары, где строки начинаются одинаково:
//
//	"Profit for the period" (profit_for_period) — префикс строки
//	  "Profit for the period attributable to:", которая метрикой быть не должна;
//	"Profit before income tax" (profit_before_tax) против "Profit for the
//	  period" — расходятся на слове после «Profit», но проверяются обе;
//	"Total assets" (total_assets) против "Total equity and liabilities" —
//	  общего префикса у них нет, но обе начинаются с «Total », и совпадение
//	  проверяется по полному тексту метки.
//
// Первая пара разрешается не порядком, а шагом извлечения чисел: у строки
// "...attributable to:" чисел нет (метка кончается двоеточием), поэтому запись не
// создаётся. Порядок же гарантирует, что если в отчёте появится строка ровно
// "Profit for the period", её не перехватит никакая другая метрика. Обратный
// порядок сломал бы разбор так же, как «Капитальные затраты» ломала
// stripping_capex в kpi.go.
func matchIFRSMetric(line string) (MetricDefinition, string, bool) {
	// Префиксы перебираются от длинного к короткому: strings.HasPrefix сам по
	// себе не умеет выбирать самый длинный совпавший из нескольких метрик.
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
