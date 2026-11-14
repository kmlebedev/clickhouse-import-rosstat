package util

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseRuNumber разбирает число в формате таблиц ЦБ и Росстата: пробелы (включая
// неразрывный и узкий) — разделители тысяч, запятая — десятичный разделитель,
// прочерк любого вида — отсутствие значения (ok=false), а не ноль. Так формы ЦБ
// помечают «значения нет»: пустая ячейка, «—», «–», «-».
//
// Нечисловой текст — ошибка: молчаливый пропуск ячейки скрыл бы смену разметки.
func ParseRuNumber(raw string) (value float64, ok bool, err error) {
	trimmed := strings.TrimSpace(raw)
	switch trimmed {
	case "", "—", "–", "-", "x", "х":
		return 0, false, nil
	}
	normalized := strings.NewReplacer(" ", "", " ", "", " ", "").Replace(trimmed)
	normalized = strings.ReplaceAll(normalized, ",", ".")
	parsed, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0, false, fmt.Errorf("не число: %w", err)
	}
	return parsed, true, nil
}
