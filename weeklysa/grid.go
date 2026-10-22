package weeklysa

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// rawPoint — недельная точка источника: дата регистрации и индекс (100 = без
// изменения к предыдущей дате).
type rawPoint struct {
	Date  time.Time
	Value float64
}

// saPoint — точка сезонно скорректированного 4-нед. индекса.
type saPoint struct {
	Date time.Time
	SA   float64
}

// metricPoint — строка weekly_cpi_sa: SA-индекс и две производные метрики.
type metricPoint struct {
	Date    time.Time
	SAIndex float64
	SA4W    float64
	SAAR    float64
}

// mergeSources сливает агрегат бюллетеней 2009–2021 и BI 2019+. На пересечении
// 2019–2021 BI перекрывает бюллетени — ровно как в v_rosstat_macro. Дубли дат
// внутри одного источника — ошибка: это дефект выгрузки, а не штатное
// пересечение. Результат отсортирован по дате.
func mergeSources(bulletin, bi []rawPoint) ([]rawPoint, error) {
	merged := make(map[time.Time]float64, len(bulletin)+len(bi))

	seen := make(map[time.Time]bool, len(bulletin))
	for _, point := range bulletin {
		date := normalizeDate(point.Date)
		if seen[date] {
			return nil, fmt.Errorf("%s: дубль даты %s", weeklySABulletinsSource, date.Format("2006-01-02"))
		}
		seen[date] = true
		if err := checkPointValue(point.Value, date, weeklySABulletinsSource); err != nil {
			return nil, err
		}
		merged[date] = point.Value
	}

	seenBI := make(map[time.Time]bool, len(bi))
	for _, point := range bi {
		date := normalizeDate(point.Date)
		if seenBI[date] {
			return nil, fmt.Errorf("%s: дубль даты %s", weeklySABISource, date.Format("2006-01-02"))
		}
		seenBI[date] = true
		if err := checkPointValue(point.Value, date, weeklySABISource); err != nil {
			return nil, err
		}
		// BI приоритетен: перекрывает точку бюллетеня на той же дате.
		merged[date] = point.Value
	}

	dates := make([]time.Time, 0, len(merged))
	for date := range merged {
		dates = append(dates, date)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })
	result := make([]rawPoint, 0, len(dates))
	for _, date := range dates {
		result = append(result, rawPoint{Date: date, Value: merged[date]})
	}
	return result, nil
}

func checkPointValue(value float64, date time.Time, source string) error {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("%s: неположительное или нечисловое значение %v на %s", source, value, date.Format("2006-01-02"))
	}
	return nil
}

// validateSeries проверяет политику входа: первый день, пол числа точек и
// строгую возрастаемость. Порог — пол, а не точное значение: ряд растёт с
// каждой недельной публикацией.
func validateSeries(points []rawPoint, firstDate time.Time, minPoints int) error {
	if len(points) < minPoints {
		return fmt.Errorf("точек входа %d, ожидалось не меньше %d", len(points), minPoints)
	}
	if !normalizeDate(points[0].Date).Equal(normalizeDate(firstDate)) {
		return fmt.Errorf("первая точка %s, ожидалась %s", points[0].Date.Format("2006-01-02"), firstDate.Format("2006-01-02"))
	}
	for i := 1; i < len(points); i++ {
		if !points[i].Date.After(points[i-1].Date) {
			return fmt.Errorf("даты не возрастают: %s после %s", points[i].Date.Format("2006-01-02"), points[i-1].Date.Format("2006-01-02"))
		}
	}
	return nil
}

// buildGrid строит регулярную 7-дневную сетку кумулятивного лог-уровня от первой
// до последней даты. Внутри интервала между публикациями — равномерный прирост
// по дням; накопленный уровень сохраняется. Алгоритм повторяет research #348
// (regular_grid) дословно.
func buildGrid(points []rawPoint) ([]rawPoint, error) {
	if len(points) < 2 {
		return nil, fmt.Errorf("для 7-дневной сетки нужно не меньше двух точек, получено %d", len(points))
	}
	cumulative := make([]float64, len(points))
	for i := 1; i < len(points); i++ {
		cumulative[i] = cumulative[i-1] + math.Log(points[i].Value/100)
	}

	last := points[len(points)-1].Date
	grid := make([]rawPoint, 0, int(last.Sub(points[0].Date).Hours()/24/7)+1)
	cursor := points[0].Date
	index := 0
	for !cursor.After(last) {
		for index+1 < len(points) && !points[index+1].Date.After(cursor) {
			index++
		}
		var level float64
		if cursor.Equal(points[index].Date) {
			level = cumulative[index]
		} else if index+1 < len(points) {
			span := daysBetween(points[index].Date, points[index+1].Date)
			offset := daysBetween(points[index].Date, cursor)
			level = cumulative[index] + (cumulative[index+1]-cumulative[index])*float64(offset)/float64(span)
		} else {
			level = cumulative[index]
		}
		grid = append(grid, rawPoint{Date: cursor, Value: level})
		cursor = cursor.AddDate(0, 0, 7)
	}
	return grid, nil
}

// fourWeekIndex — 4-нед. индекс на сетке: exp(L_t − L_{t−4}) × 100, ровно
// 28 дней. Первая точка — через 28 дней от начала ряда.
func fourWeekIndex(grid []rawPoint) ([]rawPoint, error) {
	if len(grid) <= weeklySAAggregateWeeks {
		return nil, fmt.Errorf("сетка из %d точек короче окна %d недель", len(grid), weeklySAAggregateWeeks)
	}
	result := make([]rawPoint, 0, len(grid)-weeklySAAggregateWeeks)
	for i := weeklySAAggregateWeeks; i < len(grid); i++ {
		result = append(result, rawPoint{
			Date:  grid[i].Date,
			Value: math.Exp(grid[i].Value-grid[i-weeklySAAggregateWeeks].Value) * 100,
		})
	}
	return result, nil
}

// monthlyLastInMonth берёт последнюю точку каждого месяца и ставит её на конец
// месяца. Месяц включается, только если последняя дата сетки дошла до его конца
// (lastGridDate >= monthEnd): текущий неполный месяц в ряд не попадает.
func monthlyLastInMonth(index []rawPoint, lastGridDate, firstMonth time.Time, minMonths int) ([]rawPoint, error) {
	if len(index) == 0 {
		return nil, fmt.Errorf("пустой ряд 4-нед. индекса")
	}
	type monthGroup struct {
		month time.Time
		value float64
	}
	groups := make([]monthGroup, 0, len(index)/4+1)
	for _, point := range index {
		month := monthStart(point.Date)
		if len(groups) == 0 || !groups[len(groups)-1].month.Equal(month) {
			groups = append(groups, monthGroup{month: month, value: point.Value})
			continue
		}
		groups[len(groups)-1].value = point.Value
	}
	for i := 1; i < len(groups); i++ {
		if !groups[i].month.Equal(groups[i-1].month.AddDate(0, 1, 0)) {
			return nil, fmt.Errorf("пропуск месяца после %s: следующая точка %s",
				groups[i-1].month.Format("2006-01"), groups[i].month.Format("2006-01"))
		}
	}
	if !groups[0].month.Equal(monthStart(firstMonth)) {
		return nil, fmt.Errorf("первый месяц %s, ожидался %s", groups[0].month.Format("2006-01"), firstMonth.Format("2006-01"))
	}

	lastGridDate = normalizeDate(lastGridDate)
	result := make([]rawPoint, 0, len(groups))
	for _, group := range groups {
		end := monthEnd(group.month)
		if lastGridDate.Before(end) {
			// Только последний месяц может быть неполным: у предыдущих
			// lastGridDate заведомо позже их конца.
			continue
		}
		result = append(result, rawPoint{Date: end, Value: group.value})
	}
	if len(result) < minMonths {
		return nil, fmt.Errorf("полных месяцев %d, ожидалось не меньше %d (последняя дата сетки %s)",
			len(result), minMonths, lastGridDate.Format("2006-01-02"))
	}
	return result, nil
}

// computeMetrics считает метрики из SA-уровня 4-нед. индекса:
//
//	sa_index = sa
//	sa_4w    = (sa/100 − 1) × 100          — % за 4 недели
//	saar     = ((sa/100)^13 − 1) × 100     — % годовых, 13 окон по 4 недели
func computeMetrics(months []rawPoint, sa []saPoint) ([]metricPoint, error) {
	if len(months) != len(sa) {
		return nil, fmt.Errorf("%d SA-точек против %d месяцев", len(sa), len(months))
	}
	result := make([]metricPoint, len(sa))
	for i, point := range sa {
		expected := normalizeDate(months[i].Date)
		if !normalizeDate(point.Date).Equal(expected) {
			return nil, fmt.Errorf("дата SA %s, ожидалась %s", point.Date.Format("2006-01"), expected.Format("2006-01"))
		}
		if point.SA <= 0 || math.IsNaN(point.SA) || math.IsInf(point.SA, 0) {
			return nil, fmt.Errorf("неположительный или нечисловой SA %v на %s", point.SA, point.Date.Format("2006-01"))
		}
		result[i] = metricPoint{
			Date:    expected,
			SAIndex: point.SA,
			SA4W:    (point.SA/100 - 1) * 100,
			SAAR:    (math.Pow(point.SA/100, weeklySAAnnualPeriods) - 1) * 100,
		}
	}
	return result, nil
}

func monthStart(value time.Time) time.Time {
	year, month, _ := normalizeDate(value).Date()
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
}

func monthEnd(month time.Time) time.Time {
	return monthStart(month).AddDate(0, 1, 0).AddDate(0, 0, -1)
}

func daysBetween(from, to time.Time) int {
	return int(normalizeDate(to).Sub(normalizeDate(from)).Hours() / 24)
}
