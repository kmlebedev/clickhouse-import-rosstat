package minfin

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

const (
	fedbudSheet        = "месяц"
	fedbudHeaderAnchor = "Показатель"
)

// fedbudLayout — различие двух бюджетных файлов: таблица (для ошибок) и набор
// подписей строк. Лист, шапка и правило накопления у федерального и
// консолидированного бюджетов одинаковые, поэтому разбор один.
type fedbudLayout struct {
	Table  string
	Fields []string
}

// dateReplacer переводит подписи месяцев источника в layout time.Parse("Jan-06").
// Все 12 месяцев обязательны: последняя ячейка шапки приходит текстом со сноской
// ("сен.26 **") и может оказаться любым месяцем.
var dateReplacer = strings.NewReplacer(
	".", "-",
	"янв", "Jan",
	"фев", "Feb",
	"мар", "Mar",
	"апр", "Apr",
	"май", "May",
	"июн", "Jun",
	"июл", "Jul",
	"авг", "Aug",
	"сен", "Sep",
	"окт", "Oct",
	"ноя", "Nov",
	"дек", "Dec",
)

// fedbudNumberReplacer убирает разделители разрядов: в raw-режиме excelize
// отдаёт точку как десятичный разделитель и пробелы/NBSP в текстовых ячейках.
var fedbudNumberReplacer = strings.NewReplacer(" ", "", "\u00a0", "")

// importFedbud разбирает лист «месяц» федерального и консолидированного бюджетов.
// Значения в источнике накоплены с начала года, таблица хранит месячные разности.
// RawCellValue обязателен: без него excelize округляет значение до одного знака
// (1115,9 вместо 1115,85). Fail-fast: нет листа, шапки, ожидаемой строки или
// точек — ошибка с таблицей, листом и колонкой, а не молчаливый частичный импорт.
func importFedbud(xlsx *excelize.File, batch driver.Batch, layout fedbudLayout) error {
	rows, err := xlsx.GetRows(fedbudSheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return fmt.Errorf("%s: лист «%s»: %w", layout.Table, fedbudSheet, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("%s: лист «%s» пуст", layout.Table, fedbudSheet)
	}

	headerRow := -1
	for i, row := range rows {
		if len(row) < 2 {
			continue
		}
		if normalizeFedbudLabel(row[1]) == fedbudHeaderAnchor {
			headerRow = i
			break
		}
	}
	if headerRow < 0 {
		return fmt.Errorf("%s: лист «%s»: не найдена шапка с якорем «%s» во втором столбце",
			layout.Table, fedbudSheet, fedbudHeaderAnchor)
	}

	header := rows[headerRow]
	dates := make(map[int]time.Time, len(header)-2)
	for col := 2; col < len(header); col++ {
		raw := strings.TrimSpace(header[col])
		if raw == "" {
			return fmt.Errorf("%s: лист «%s»: пустая дата в колонке %d", layout.Table, fedbudSheet, col+1)
		}
		date, err := parseFedbudDate(raw)
		if err != nil {
			return fmt.Errorf("%s: лист «%s», колонка %d: дата %q: %w",
				layout.Table, fedbudSheet, col+1, raw, err)
		}
		dates[col] = date
	}
	if len(dates) == 0 {
		return fmt.Errorf("%s: лист «%s»: в шапке нет ни одной даты", layout.Table, fedbudSheet)
	}

	found := make(map[string]bool, len(layout.Fields))
	pointsByField := make(map[string]int, len(layout.Fields))
	points := 0
	for _, row := range rows[headerRow+1:] {
		if len(row) < 2 {
			continue
		}
		label := normalizeFedbudLabel(row[1])
		if !slices.Contains(layout.Fields, label) {
			continue
		}
		if found[label] {
			return fmt.Errorf("%s: лист «%s»: строка «%s» встречается дважды",
				layout.Table, fedbudSheet, label)
		}
		found[label] = true

		var valuePrev float64
		for col := 2; col < len(row); col++ {
			date, ok := dates[col]
			if !ok {
				return fmt.Errorf("%s: лист «%s», строка «%s»: значение в колонке %d без даты в шапке",
					layout.Table, fedbudSheet, label, col+1)
			}
			raw := strings.TrimSpace(row[col])
			if raw == "" {
				return fmt.Errorf("%s: лист «%s», строка «%s», колонка %d: пустая ячейка внутри накопительного ряда",
					layout.Table, fedbudSheet, label, col+1)
			}
			valueNew, err := parseFedbudNumber(raw)
			if err != nil {
				return fmt.Errorf("%s: лист «%s», строка «%s», колонка %d: %w",
					layout.Table, fedbudSheet, label, col+1, err)
			}
			value := valueNew
			if date.Month() != time.January {
				value = valueNew - valuePrev
			}
			valuePrev = valueNew
			if err = batch.Append(label, date, value); err != nil {
				return fmt.Errorf("%s: лист «%s», строка «%s», колонка %d: запись точки: %w",
					layout.Table, fedbudSheet, label, col+1, err)
			}
			points++
			pointsByField[label]++
		}
	}

	for _, field := range layout.Fields {
		if !found[field] {
			return fmt.Errorf("%s: лист «%s»: не найдена ожидаемая строка «%s»",
				layout.Table, fedbudSheet, field)
		}
		if pointsByField[field] == 0 {
			return fmt.Errorf("%s: лист «%s»: строка «%s» не дала ни одной точки",
				layout.Table, fedbudSheet, field)
		}
	}
	if points == 0 {
		return fmt.Errorf("%s: лист «%s»: разбор не дал ни одной точки", layout.Table, fedbudSheet)
	}
	return nil
}

// normalizeFedbudLabel снимает хвостовые звёзды-сноски и схлопывает пробелы:
// в выпуске 2026/10 строка консолидированного бюджета называется «Акцизы ****»,
// а федерального — «Акцизы на ввозимые товары ****». Точное совпадение после
// нормализации не даёт чужой строке попасть в ряд «Акцизы».
func normalizeFedbudLabel(label string) string {
	return strings.TrimRight(strings.Join(strings.Fields(label), " "), "* ")
}

// parseFedbudDate принимает и сырой Excel-сериал, и текст: в raw-режиме excelize
// отдаёт даты числом, а ячейка со сноской — строкой «янв.23 ***». Оба варианта
// нормализуются к 1-му числу месяца UTC.
func parseFedbudDate(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if serial, err := strconv.ParseFloat(raw, 64); err == nil {
		if serial < 20000 {
			return time.Time{}, fmt.Errorf("сериал Excel %q вне ожидаемого диапазона дат", raw)
		}
		t, err := excelize.ExcelDateToTime(serial, false)
		if err != nil {
			return time.Time{}, err
		}
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC), nil
	}

	dateStr := strings.TrimRight(raw, "* \t")
	if fields := strings.Fields(dateStr); len(fields) > 0 {
		dateStr = fields[0]
	}
	for _, layout := range []string{"2006-01-02", "02.01.2006"} {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC), nil
		}
	}
	t, err := time.Parse("Jan-06", dateReplacer.Replace(dateStr))
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC), nil
}

// parseFedbudNumber принимает raw-значение (точка) и текстовое (запятая как
// десятичный разделитель); запятая при наличии точки считается разделителем
// разрядов. Ошибка называет исходную ячейку, а не преобразованную строку.
func parseFedbudNumber(raw string) (float64, error) {
	s := fedbudNumberReplacer.Replace(strings.TrimSpace(raw))
	if strings.Contains(s, ",") && strings.Contains(s, ".") {
		s = strings.ReplaceAll(s, ",", "")
	} else {
		s = strings.ReplaceAll(s, ",", ".")
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", raw, err)
	}
	return value, nil
}
