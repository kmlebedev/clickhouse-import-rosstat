package domrf

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// domrfWeeklyBlock — имя атрибута main#app с недельной динамикой ставок.
	domrfWeeklyBlock = "mortgage-dinamika-stavok"
	// domrfWeeklyDateLayout — формат даты недельного среза в блоке (дд.мм.гггг).
	domrfWeeklyDateLayout = "02.01.2006"
	// domrfWeeklyMinPoints — нижняя граница длины ряда: с 2021-09 по 2026-10 источник отдаёт 254
	// недели. Порог ловит обрезанный блок (смена вёрстки), не мешая источнику добавлять недели.
	domrfWeeklyMinPoints = 200
)

// domrfWeeklyProducts — продукты недельного блока в порядке источника. Каталог читает этот
// список, а разбор — имена из блока; тест сверяет, что наборы совпадают (смена состава продуктов
// должна ломать прогон, а не заводить продукт без описания).
var domrfWeeklyProducts = []string{"Новостройка", "Семейная ипотека", "Вторичный рынок", "Рефинансирование", "Льготная ипотека"}

// domrfWeeklyBlockJSON — блок mortgage-dinamika-stavok: массив дат по убыванию и строки CSV на
// продукт. Даты и значения выровнены от новой к старой, поэтому короткий ряд (завершившаяся
// программа) просто короче общего массива дат.
type domrfWeeklyBlockJSON struct {
	Date   []string          `json:"date"`
	Values map[string]string `json:"values"`
	Titles []string          `json:"titles"`
	Title  string            `json:"title"`
	Note   string            `json:"note"`
}

// domrfWeeklyPoint — точка недельного ряда: продукт источника, пятница недели и ставка в
// процентах годовых (18.76), как в источнике.
type domrfWeeklyPoint struct {
	Product string
	Date    time.Time
	Rate    float64
}

// domrfWeeklyPoints разбирает блок в точки. Чистая функция (§6.1): страницу качает вызывающий.
//
// Проверки: даты строго по убыванию без дублей (иначе «короткий ряд» обрезался бы не с той
// стороны); длина ряда продукта не больше числа дат (длиннее — источник добавил точку без даты);
// пустое или нечисловое значение — ошибка, а не пропуск: в этом блоке источник не ставит «-».
func domrfWeeklyPoints(block domrfWeeklyBlockJSON) ([]domrfWeeklyPoint, error) {
	if len(block.Date) < domrfWeeklyMinPoints {
		return nil, fmt.Errorf("дат %d, ожидалось не меньше %d", len(block.Date), domrfWeeklyMinPoints)
	}
	dates := make([]time.Time, len(block.Date))
	for i, value := range block.Date {
		date, err := time.Parse(domrfWeeklyDateLayout, strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("дата %q: %w", value, err)
		}
		dates[i] = date
		if i > 0 && !date.Before(dates[i-1]) {
			return nil, fmt.Errorf("даты не по убыванию: %s после %s", date.Format(domrfWeeklyDateLayout), dates[i-1].Format(domrfWeeklyDateLayout))
		}
	}
	if len(block.Titles) == 0 {
		return nil, fmt.Errorf("список продуктов пуст")
	}
	points := make([]domrfWeeklyPoint, 0, len(dates)*len(block.Titles))
	for _, product := range block.Titles {
		raw, ok := block.Values[product]
		if !ok {
			return nil, fmt.Errorf("продукт %q без значений", product)
		}
		values := strings.Split(raw, ",")
		if len(values) > len(dates) {
			return nil, fmt.Errorf("продукт %q: значений %d больше дат %d", product, len(values), len(dates))
		}
		for i, value := range values {
			rate, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil {
				return nil, fmt.Errorf("продукт %q, %s: значение %q не число: %w", product, dates[i].Format(domrfWeeklyDateLayout), value, err)
			}
			points = append(points, domrfWeeklyPoint{Product: product, Date: dates[i], Rate: rate})
		}
	}
	return points, nil
}
