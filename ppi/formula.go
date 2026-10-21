package ppi

import (
	"fmt"
	"math"
	"time"
)

// saPoint — точка SA-уровня, полученная от X-13.
type saPoint struct {
	Date time.Time
	SA   float64
}

// metricPoint — строка ppi_sa: SA-уровень и производные метрики. Первые точки
// ряда не имеют предыдущего значения, поэтому метрики — указатели (NULL).
type metricPoint struct {
	Date  time.Time
	SA    float64
	MMSA  *float64
	SAAR  *float64
	SAAR3 *float64
}

// computeMetrics считает m/m SA, SAAR и 3-месячный SAAR как среднее трёх
// месячных SAAR (формула подтверждена research #275):
//
//	mm_sa = (sa_t / sa_{t-1} - 1) * 100
//	saar  = ((1 + mm_sa/100)^12 - 1) * 100
//	saar3 = mean доступных saar в окне t-2..t (на третьем месяце — двух,
//	        как na.rm = TRUE в research; с четвёртого — трёх)
//
// Пропуск месяца или неположительный SA — ошибка: молчаливое NULL скрыло бы
// разрыв ряда.
func computeMetrics(points []saPoint) ([]metricPoint, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("пустой SA-ряд")
	}
	sorted := append([]saPoint(nil), points...)
	for i := 1; i < len(sorted); i++ {
		if !sorted[i].Date.After(sorted[i-1].Date) {
			return nil, fmt.Errorf("SA-ряд не отсортирован: %s после %s", sorted[i].Date.Format("2006-01"), sorted[i-1].Date.Format("2006-01"))
		}
		if monthIndex(sorted[i].Date) != monthIndex(sorted[i-1].Date)+1 {
			return nil, fmt.Errorf("пропуск месяца после %s: следующая точка %s", sorted[i-1].Date.Format("2006-01"), sorted[i].Date.Format("2006-01"))
		}
	}
	for _, point := range sorted {
		if point.SA <= 0 || math.IsNaN(point.SA) || math.IsInf(point.SA, 0) {
			return nil, fmt.Errorf("неположительный или нечисловой SA %v на %s", point.SA, point.Date.Format("2006-01"))
		}
	}

	result := make([]metricPoint, len(sorted))
	saarValues := make([]float64, len(sorted))
	saarDefined := make([]bool, len(sorted))
	for i, point := range sorted {
		row := metricPoint{Date: point.Date, SA: point.SA}
		if i > 0 {
			mm := (point.SA/sorted[i-1].SA - 1) * 100
			saar := (math.Pow(1+mm/100, 12) - 1) * 100
			row.MMSA = floatPtr(mm)
			row.SAAR = floatPtr(saar)
			saarValues[i] = saar
			saarDefined[i] = true
		}
		if i >= 2 {
			// research: mean(saar[(i-2):i], na.rm = TRUE). На третьем месяце
			// первого SAAR ещё нет, поэтому среднее считается по двум доступным.
			sum := 0.0
			available := 0
			for j := i - 2; j <= i; j++ {
				if saarDefined[j] {
					sum += saarValues[j]
					available++
				}
			}
			if available > 0 {
				row.SAAR3 = floatPtr(sum / float64(available))
			}
		}
		result[i] = row
	}
	return result, nil
}

func floatPtr(value float64) *float64 {
	return &value
}
