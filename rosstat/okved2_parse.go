package rosstat

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strings"

	"golang.org/x/text/encoding/charmap"
)

// okved2Row — одна строка классификатора: раздел, код, название и вычисленные уровень/родитель.
// У строки раздела Code пуст, Level=1, ParentCode пуст.
type okved2Row struct {
	Section    string
	Code       string
	Name       string
	Level      uint8
	ParentCode string
}

// parseOkved2Meta достаёт URL самого свежего выпуска data-*.csv из meta.csv. Файл
// отдаётся в CP1251 (в отличие от самого CSV, который UTF-8 с BOM), поэтому декодирование
// обязательно до csv.Reader: иначе кириллица в title/description станет мусором, а
// property/value (латиница) прочитаются, и ошибка осталась бы незамеченной.
func parseOkved2Meta(body []byte) (string, error) {
	decoded, err := charmap.Windows1251.NewDecoder().Bytes(body)
	if err != nil {
		return "", fmt.Errorf("meta.csv: windows-1251: %w", err)
	}
	reader := csv.NewReader(bytes.NewReader(decoded))
	reader.Comma = ','
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	records, err := reader.ReadAll()
	if err != nil {
		return "", fmt.Errorf("meta.csv: %w", err)
	}
	for i, record := range records {
		if len(record) < 2 {
			continue
		}
		key := strings.TrimSpace(record[0])
		value := strings.TrimSpace(record[1])
		if strings.HasPrefix(key, "data-") && strings.HasSuffix(key, ".csv") && strings.HasPrefix(value, "http") {
			return value, nil
		}
		if i == 0 && key != "property" {
			return "", fmt.Errorf("meta.csv: ожидалась шапка property,value, получено %q", key)
		}
	}
	return "", fmt.Errorf("meta.csv: не найдена строка data-*.csv с URL выпуска")
}

// parseOkved2 разбирает data-*.csv: UTF-8 BOM, разделитель ';', три поля Razdel/Code/Name.
// Строка раздела приходит с Code=" " и отличается от кодов пустым после TrimSpace кодом.
// Ошибка на строке с неожиданным числом полей/пустым названием — fail-fast: молчаливый
// пропуск сдвинул бы 3102 ожидаемые строки и сделал бы приёмку неотличимой от успеха.
func parseOkved2(body []byte) ([]okved2Row, error) {
	body = bytes.TrimPrefix(body, []byte("\xef\xbb\xbf"))
	reader := csv.NewReader(bytes.NewReader(body))
	reader.Comma = ';'
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("data.csv: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("data.csv: пустой файл")
	}
	rows := make([]okved2Row, 0, len(records))
	for i, record := range records {
		if len(record) != 3 {
			return nil, fmt.Errorf("data.csv: строка %d: ожидалось 3 поля, получено %d", i+1, len(record))
		}
		section := strings.TrimSpace(record[0])
		code := strings.TrimSpace(record[1])
		name := strings.TrimSpace(record[2])
		if section == "" || name == "" {
			return nil, fmt.Errorf("data.csv: строка %d: пустой раздел или название", i+1)
		}
		row := okved2Row{Section: section, Code: code, Name: name}
		if code == "" {
			row.Level = 1
		} else {
			digits := okved2Digits(code)
			row.Level = uint8(len(digits))
			row.ParentCode = okved2Parent(digits)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// okved2Digits — код без точек: '01.11.1' → '01111'. Уровень ОКВЭД2 — число цифр, а не число
// точек: '01.1' и '01.11' имеют по одной точке, но уровни 3 и 4.
func okved2Digits(code string) string {
	var b strings.Builder
	b.Grow(len(code))
	for _, r := range code {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// okved2Parent — родитель по уровню: у класса (2 цифры) родитель — раздел, кода нет; у
// подкласса 3 цифры → 2; у группы 4 → 3; у подгруппы 5 → 4; у вида 6 → 5. Формат
// восстанавливается по длине: '01111' (5) → '01.11.1', '0111' (4) → '01.11', '011' (3) → '01.1'.
func okved2Parent(digits string) string {
	if len(digits) <= 2 {
		return ""
	}
	parent := digits[:len(digits)-1]
	switch len(parent) {
	case 2:
		return parent
	case 3, 4:
		return parent[:2] + "." + parent[2:]
	case 5, 6:
		return parent[:2] + "." + parent[2:4] + "." + parent[4:]
	default:
		return parent
	}
}
