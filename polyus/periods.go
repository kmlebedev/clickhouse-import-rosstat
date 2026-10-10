package polyus

import (
	"fmt"
	"regexp"
	"strings"
)

// PeriodInfo — разобранный период отчёта Polyus.
type PeriodInfo struct {
	Period string // Формат: 2024FY или 2026H1 или 2021Q4
	Type   string // Тип: FY, H или Q
}

// nonBreakingSpace — символьный класс, покрывающий обычные и неразрывные
// пробелы (NBSP, figure space, narrow no-break space), которыми реальные
// заголовки отчётов разделяют колонки. Go-шный `\s` не совпадает ни с одним
// из неразрывных вариантов, поэтому класс расширен явно: иначе токен вида
// "1\u00a0п/г\u00a02026" молча не распознаётся. Класс уже заключён в
// квадратные скобки — квантификатор снаружи применяется ко всему набору.
// RE2 не знает escape-последовательности `\u`, поэтому символы записаны
// как `\x{...}`.
const nonBreakingSpace = `[\s\x{00a0}\x{2007}\x{202f}]`

var (
	reFY = regexp.MustCompile(`^(\d{4})$`)
	reH  = regexp.MustCompile(`^([12])H(\d{4})$`)  // 2H2025
	reQ  = regexp.MustCompile(`^([1-4])Q(\d{4})$`) // 4Q2022
	// reRuHalf и rePeriod видят пробелы между номером, единицей и годом,
	// поэтому используют расширенный класс nonBreakingSpace. Год русской формы
	// допускается и двузначным («1 п/г 26»): так печатают шапки в релизах,
	// где таблиц несколько и место экономится. Квантификатор `{2,4}` жаден,
	// поэтому четырёхзначный год по-прежнему разбирается целиком.
	reRuHalf      = regexp.MustCompile(`([12])` + nonBreakingSpace + `*п/г` + nonBreakingSpace + `+(\d{2,4})`)   // 1 п/г 2026, 1 п/г 26
	reRuShortYear = regexp.MustCompile(`^(\d{2})$`)                                                              // 26
	rePeriod      = regexp.MustCompile(`([1-4])` + nonBreakingSpace + `*([HQ])` + nonBreakingSpace + `+(\d{4})`) // 1H 2026
)

// parsePeriod разбирает период из трёх форм: английской пресс-релизной
// ("1H 2026", "4Q 2022"), компактной ("2H2025", "4Q2022") и русской
// ("1 п/г 2026"), а также голый год ("2025").
//
// Контракт входа: токен приходит из строки-заголовка PDF с множественными
// пробелами, поэтому вход тримится, а разделители внутри формы принимаются
// как обычные, так и неразрывные (nonBreakingSpace). Функция — матчер, а не
// строгий валидатор: она ищет совпадение внутри окружающего текста, поэтому
// вызывающий код не должен рассчитывать на отклонение некорректного ввода.
//
// Порядок проверок — от самой узкой формы к самой широкой: русская форма
// проверяется до голого года, а компактные формы — до разнесённых. Иначе
// «1 п/г 2026» разобралось бы как два независимых токена («1 п/г» отброшен,
// «2026» прочитан как полный год FY), и полугодие превратилось бы в год.
func parsePeriod(input string) (PeriodInfo, error) {
	input = strings.TrimSpace(input)

	// 1. Компактный формат полугодия (например, "2H2025")
	if matches := reH.FindStringSubmatch(input); matches != nil {
		half := matches[1] // "1" или "2"
		year := matches[2] // например, "2025"

		return PeriodInfo{
			Period: fmt.Sprintf("%sH%s", year, half), // Склеиваем в 2026H1
			Type:   "H",
		}, nil
	}

	// 2. Компактный формат квартала (например, "4Q2022")
	if matches := reQ.FindStringSubmatch(input); matches != nil {
		quart := matches[1] // от "1" до "4"
		year := matches[2]  // например, "2022"

		return PeriodInfo{
			Period: fmt.Sprintf("%sQ%s", year, quart), // Склеиваем в 2022Q4
			Type:   "Q",
		}, nil
	}

	// 3. Русский формат полугодия (например, "1 п/г 2026", "1 п/г 26") —
	// обязательно до голого года и до общей rePeriod, иначе `п/г` не совпадёт.
	if matches := reRuHalf.FindStringSubmatch(input); matches != nil {
		half := matches[1]
		year := expandShortYear(matches[2])

		return PeriodInfo{
			Period: fmt.Sprintf("%sH%s", year, half),
			Type:   "H",
		}, nil
	}

	// 4. Разнесённый формат полугодия/квартала ("1H 2026", "4Q 2022").
	if matches := rePeriod.FindStringSubmatch(input); matches != nil {
		num := matches[1]  // "1".."4"
		kind := matches[2] // "H" или "Q"
		year := matches[3] // например, "2026"

		if kind == "Q" {
			return PeriodInfo{
				Period: fmt.Sprintf("%sQ%s", year, num), // Склеиваем в 2022Q4
				Type:   "Q",
			}, nil
		}

		return PeriodInfo{
			Period: fmt.Sprintf("%sH%s", year, num), // Склеиваем в 2026H1
			Type:   "H",
		}, nil
	}

	// 5. Полный год (например, "2024").
	if reFY.MatchString(input) {
		return PeriodInfo{
			Period: input + "FY",
			Type:   "FY",
		}, nil
	}

	return PeriodInfo{}, fmt.Errorf("unknown period format: %s", input)
}

// expandShortYear дополняет двузначный год до четырёхзначного, относя его к
// 2000-м: «26» → «2026». Четырёхзначный год возвращается как есть.
func expandShortYear(year string) string {
	if reRuShortYear.MatchString(year) {
		return "20" + year
	}

	return year
}
