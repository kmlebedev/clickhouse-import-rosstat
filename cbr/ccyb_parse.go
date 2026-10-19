package cbr

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// ccybPoint — точка ряда индикаторов кредитного цикла.
type ccybPoint struct {
	series      string
	indicatorNo uint8
	group       string
	date        time.Time
	value       float64
}

// ccybSeriesSpec — объявленный ряд: имя, номер показателя, группа и подпись
// строки источника. Имена рядов объявлены кодом, а не собираются из заголовка
// (стандарт §6.8): подписи источника содержат сноски и пробелы, а смена подписи
// не должна обрывать ряд.
type ccybSeriesSpec struct {
	no    uint8
	group string
	label string
	name  string
	title string
	unit  string
}

const ccybValueDescription = "Индикатор кредитного цикла Банка России: уровень " +
	"(для темпов прироста — процент за период, для кредитных гэпов — процентные пункты). " +
	"Дата — первое число отчётного месяца. Файл источника последний раз обновлялся " +
	"в декабре 2024 года, последняя точка — 2024-10-01: отсутствие более поздних точек " +
	"является свойством публикации, а не пропуском импорта. Пустая ячейка источника " +
	"точкой не является и нулём не считается."

// ccybGroupByLabel — группы источника: код группы и номер, который наследуют
// подстроки. «Условия кредитования» и верхний раздел — только заголовки секций,
// собственных рядов у них нет.
var ccybGroupByLabel = map[string]string{
	"Показатели достаточности капитала":                                     "bank_capital",
	"Темпы прироста кредитов нефинансовым организациям":                     "nfo_loan_growth",
	"Доля просроченной задолженности по кредитам нефинансовым организациям": "nfo_overdue",
	"Темпы прироста кредитов физическим лицам":                              "hh_loan_growth",
	"Доля ссуд с просроченной задолженностью свыше 90 дней":                 "hh_90d",
	"Кредитные гэпы": "credit_gaps",
}

// ccybSeriesSpecs — 24 ряда 13 показателей. Подстроки наследуют номер и группу
// родительской строки; самостоятельные показатели (8, 17, 21, 26, 31–34) несут
// свой номер и группу.
var ccybSeriesSpecs = []ccybSeriesSpec{
	{no: 1, group: "bank_capital", label: "совокупного капитала (Н1.0)", name: "n10_capital", title: "Достаточность совокупного капитала (Н1.0)", unit: "%"},
	{no: 1, group: "bank_capital", label: "базового капитала (Н1.1)", name: "n11_capital", title: "Достаточность базового капитала (Н1.1)", unit: "%"},
	{no: 1, group: "bank_capital", label: "основного капитала (Н1.2)", name: "n12_capital", title: "Достаточность основного капитала (Н1.2)", unit: "%"},

	{no: 2, group: "credit_gdp", label: "Кредиты/ВВП", name: "credits_gdp", title: "Кредиты/ВВП", unit: "%"},

	{no: 3, group: "nfo_loan_growth", label: "в рублях, годовые", name: "nfo_rub_yoy", title: "Темп прироста кредитов нефинансовым организациям в рублях, годовой", unit: "%"},
	{no: 3, group: "nfo_loan_growth", label: "в рублях, квартальные", name: "nfo_rub_qoq", title: "Темп прироста кредитов нефинансовым организациям в рублях, квартальный", unit: "%"},
	{no: 3, group: "nfo_loan_growth", label: "в рублях, месячные", name: "nfo_rub_mom", title: "Темп прироста кредитов нефинансовым организациям в рублях, месячный", unit: "%"},
	{no: 3, group: "nfo_loan_growth", label: "в иностранной валюте, годовые", name: "nfo_fx_yoy", title: "Темп прироста кредитов нефинансовым организациям в иностранной валюте, годовой", unit: "%"},
	{no: 3, group: "nfo_loan_growth", label: "в иностранной валюте, квартальные", name: "nfo_fx_qoq", title: "Темп прироста кредитов нефинансовым организациям в иностранной валюте, квартальный", unit: "%"},
	{no: 3, group: "nfo_loan_growth", label: "в иностранной валюте, месячные", name: "nfo_fx_mom", title: "Темп прироста кредитов нефинансовым организациям в иностранной валюте, месячный", unit: "%"},

	{no: 4, group: "nfo_fx_share", label: "Доля кредитов нефинансовым организациям в иностранной валюте", name: "nfo_fx_share", title: "Доля кредитов нефинансовым организациям в иностранной валюте", unit: "%"},

	{no: 5, group: "nfo_overdue", label: "в рублях", name: "nfo_overdue_rub", title: "Доля просроченной задолженности по кредитам нефинансовым организациям в рублях", unit: "%"},
	{no: 5, group: "nfo_overdue", label: "в иностранной валюте", name: "nfo_overdue_fx", title: "Доля просроченной задолженности по кредитам нефинансовым организациям в иностранной валюте", unit: "%"},

	{no: 6, group: "nfo_iv_v_share", label: "Доля кредитов нефинансовым организациям IV-V категории качества", name: "nfo_iv_v_share", title: "Доля кредитов нефинансовым организациям IV–V категории качества", unit: "%"},

	{no: 7, group: "hh_loan_growth", label: "годовые", name: "hh_yoy", title: "Темп прироста кредитов физическим лицам, годовой", unit: "%"},
	{no: 7, group: "hh_loan_growth", label: "квартальные", name: "hh_qoq", title: "Темп прироста кредитов физическим лицам, квартальный", unit: "%"},
	{no: 7, group: "hh_loan_growth", label: "месячные", name: "hh_mom", title: "Темп прироста кредитов физическим лицам, месячный", unit: "%"},

	{no: 8, group: "hh_overdue", label: "Доля просроченной задолженности по кредитам физическим лицам", name: "hh_overdue", title: "Доля просроченной задолженности по кредитам физическим лицам", unit: "%"},

	{no: 9, group: "hh_90d", label: "жилищные (в т.ч. ипотечные жилищные) ссуды", name: "hh_90d_mortgage", title: "Доля ссуд с просроченной задолженностью свыше 90 дней: жилищные (в т.ч. ипотечные) ссуды", unit: "%"},
	{no: 9, group: "hh_90d", label: "необеспеченные потребительские ссуды (кроме жилищных ссуд и автокредитов)", name: "hh_90d_unsecured", title: "Доля ссуд с просроченной задолженностью свыше 90 дней: необеспеченные потребительские ссуды", unit: "%"},

	{no: 10, group: "credit_gaps", label: "Кредитный гэп (широкое определение кредитного предложения)", name: "credit_gap_broad", title: "Кредитный гэп (широкое определение кредитного предложения)", unit: "п.п."},
	{no: 11, group: "credit_gaps", label: "Кредитный гэп (узкое определение кредитного предложения)", name: "credit_gap_narrow", title: "Кредитный гэп (узкое определение кредитного предложения)", unit: "п.п."},
	{no: 12, group: "credit_gaps", label: "Кредитный гэп (узкое определение кредитного предложения нефинансовым организациям)", name: "credit_gap_narrow_nfo", title: "Кредитный гэп (узкое определение кредитного предложения нефинансовым организациям)", unit: "п.п."},
	{no: 13, group: "credit_gaps", label: "Кредитный гэп (кредиты, предоставленные физическим лицам)", name: "credit_gap_hh", title: "Кредитный гэп (кредиты, предоставленные физическим лицам)", unit: "п.п."},
}

// ccybSpecByKey — ключ «номер|нормализованная подпись»: подпись «в рублях»
// встречается у разных показателей, поэтому одна подпись без номера неоднозначна.
var ccybSpecByKey = func() map[string]ccybSeriesSpec {
	m := make(map[string]ccybSeriesSpec, len(ccybSeriesSpecs))
	for _, s := range ccybSeriesSpecs {
		m[ccybSpecKey(s.no, s.label)] = s
	}
	return m
}()

func ccybSpecKey(no uint8, label string) string {
	return strconv.Itoa(int(no)) + "|" + ccybNormalizeLabel(label)
}

// ccybNormalizeLabel — типографика подписи, не влияющая на смысл: сноска ¹,
// неразрывные пробелы, лишние пробелы и регистр не должны менять имя ряда.
func ccybNormalizeLabel(s string) string {
	s = strings.ReplaceAll(s, "¹", "")
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.Join(strings.Fields(s), " ")
	return s
}

// cbrCcybParse — разбор книги индикаторов. На входе открытая книга, внутри нет
// сети и БД (стандарт §6.1). Пустая ячейка — точки нет; нечисловая непустая —
// ошибка; строка со значениями, которой нет в объявленном реестре, — ошибка.
func cbrCcybParse(xlsx *excelize.File) ([]ccybPoint, error) {
	rows, err := xlsx.GetRows(cbrCcybSheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("%s: лист %q: %w", cbrCcybTable, cbrCcybSheet, err)
	}
	if len(rows) < 3 {
		return nil, fmt.Errorf("%s: лист %q пуст или короче шапки", cbrCcybTable, cbrCcybSheet)
	}

	dateCols, dates, err := ccybDates(rows[1])
	if err != nil {
		return nil, err
	}

	var points []ccybPoint
	seen := make(map[string]bool, len(ccybSeriesSpecs))
	var currentNo uint8
	var currentGroup string

	for i := 2; i < len(rows); i++ {
		row := rows[i]
		label := ccybNormalizeLabel(ccybCell(row, 1))
		if label == "" {
			continue
		}
		noRaw := strings.TrimSpace(ccybCell(row, 0))
		hasValues := false
		for _, col := range dateCols {
			if strings.TrimSpace(ccybCell(row, col)) != "" {
				hasValues = true
				break
			}
		}

		if !hasValues {
			// Строка-заголовок группы: с номером — задаёт номер и группу,
			// без номера — либо секция «Кредитные гэпы», либо верхний раздел.
			if noRaw != "" {
				n, convErr := strconv.Atoi(noRaw)
				if convErr != nil || n < 1 || n > 13 {
					return nil, fmt.Errorf("%s: строка %d: номер показателя %q не число 1…13",
						cbrCcybTable, i+1, noRaw)
				}
				currentNo = uint8(n)
				group, ok := ccybGroupByLabel[label]
				if !ok {
					return nil, fmt.Errorf("%s: строка %d: неизвестная группа %q",
						cbrCcybTable, i+1, label)
				}
				currentGroup = group
				continue
			}
			if label == "Кредитные гэпы" {
				currentGroup = "credit_gaps"
			}
			continue
		}

		no := currentNo
		if noRaw != "" {
			n, convErr := strconv.Atoi(noRaw)
			if convErr != nil || n < 1 || n > 13 {
				return nil, fmt.Errorf("%s: строка %d: номер показателя %q не число 1…13",
					cbrCcybTable, i+1, noRaw)
			}
			no = uint8(n)
		}
		spec, ok := ccybSpecByKey[ccybSpecKey(no, label)]
		if !ok {
			return nil, fmt.Errorf("%s: строка %d: подпись %q (номер %d) не объявлена в реестре рядов",
				cbrCcybTable, i+1, label, no)
		}
		// Группа подстроки должна совпадать с родительской; у самостоятельного
		// показателя группа берётся из спеки. Это ловит переезд подстроки под
		// чужой заголовок при правке вёрстки источника.
		if noRaw == "" && currentGroup != "" && spec.group != currentGroup {
			return nil, fmt.Errorf("%s: строка %d: ряд %q объявлен в группе %q, а строка — в %q",
				cbrCcybTable, i+1, spec.name, spec.group, currentGroup)
		}

		for _, col := range dateCols {
			raw := strings.TrimSpace(ccybCell(row, col))
			if raw == "" {
				continue
			}
			value, convErr := strconv.ParseFloat(strings.ReplaceAll(raw, ",", "."), 64)
			if convErr != nil {
				return nil, fmt.Errorf("%s: строка %d, %s: значение %q не число",
					cbrCcybTable, i+1, dates[col].Format("2006-01-02"), raw)
			}
			points = append(points, ccybPoint{
				series:      spec.name,
				indicatorNo: spec.no,
				group:       spec.group,
				date:        dates[col],
				value:       value,
			})
			seen[spec.name] = true
		}
	}

	if len(points) == 0 {
		return nil, fmt.Errorf("%s: разбор дал 0 точек", cbrCcybTable)
	}
	var missing []string
	for _, spec := range ccybSeriesSpecs {
		if !seen[spec.name] {
			missing = append(missing, spec.name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s: в книге нет объявленных рядов: %s",
			cbrCcybTable, strings.Join(missing, ", "))
	}
	return points, nil
}

// ccybDates — даты из второй строки листа: колонки с непустой датой начиная с
// колонки D (индекс 3). Серийное число Excel читается сырым (RawCellValue),
// поэтому дата приходит числом и переводится ExcelDateToTime; текстовые даты
// принимаются двумя формами.
func ccybDates(header []string) ([]int, map[int]time.Time, error) {
	cols := make([]int, 0, len(header))
	dates := make(map[int]time.Time, len(header))
	for col := 3; col < len(header); col++ {
		raw := strings.TrimSpace(ccybCell(header, col))
		if raw == "" {
			continue
		}
		date, ok := ccybExcelDate(raw)
		if !ok {
			// В шапке после дат могут стоять пустые/служебные ячейки; первая
			// непустая не-дата — ошибка: иначе сдвиг шапки молча потеряет месяцы.
			return nil, nil, fmt.Errorf("%s: шапка, колонка %d: %q не дата",
				cbrCcybTable, col+1, raw)
		}
		cols = append(cols, col)
		dates[col] = date
	}
	if len(cols) == 0 {
		return nil, nil, fmt.Errorf("%s: в шапке нет дат", cbrCcybTable)
	}
	return cols, dates, nil
}

func ccybExcelDate(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if serial, err := strconv.ParseFloat(strings.ReplaceAll(raw, ",", "."), 64); err == nil {
		t, convErr := excelize.ExcelDateToTime(serial, false)
		if convErr != nil {
			return time.Time{}, false
		}
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), true
	}
	for _, layout := range []string{"02.01.2006", "2006-01-02"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// ccybCell — ячейка строки с проверкой границы: GetRows обрезает хвостовые
// пустые ячейки, и индекс за len(row) — штатная пустая ячейка, а не паника.
func ccybCell(row []string, col int) string {
	if col < 0 || col >= len(row) {
		return ""
	}
	return row[col]
}
