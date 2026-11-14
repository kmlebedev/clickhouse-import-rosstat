package util

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// ParseRuNumber разбирает число в формате таблиц ЦБ и Росстата: пробелы (включая
// неразрывный и узкий) — разделители тысяч, запятая — десятичный разделитель,
// прочерк любого вида — отсутствие значения (ok=false), а не ноль. Так формы ЦБ
// помечают «значения нет»: пустая ячейка, «—», «–», «-».
//
// Нечисловой текст — ошибка: молчаливый пропуск ячейки скрыл бы смену разметки.
func ParseRuNumber(raw string) (value float64, ok bool, err error) {
	// Пробельные символы снимаются до сверки с прочерками: ячейка может прийти с
	// пробелами внутри и по краям (включая неразрывные U+00A0/U+202F), а прочерк нужно
	// сверять уже с нормализованной строкой — иначе «—» с пробелами уйдёт в ParseFloat.
	// unicode.IsSpace покрывает все виды пробелов, которые могут разделять тысячи.
	normalized := strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, raw)
	switch normalized {
	case "", "—", "–", "-", "x", "х":
		return 0, false, nil
	}
	normalized = strings.ReplaceAll(normalized, ",", ".")
	parsed, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0, false, fmt.Errorf("не число: %w", err)
	}
	// ParseFloat принимает NaN/Inf без ошибки, но значением ряда они быть не могут:
	// это битая страница, и она не должна попасть в данные (эталон — cbr/zcyc_parse.go).
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, false, fmt.Errorf("не конечное число %q (NaN/Inf)", raw)
	}
	return parsed, true, nil
}
