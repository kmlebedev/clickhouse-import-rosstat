package views

import (
	"strings"
	"testing"
)

// Точность прогноза бессмысленна без имени механизма, который её считает: агент
// видит v_forecast_accuracy через MCP и по комментарию решает, можно ли верить
// is_resolved = 1 и откуда взялся actual. Механизм — импортёр forecast_resolve
// (пакет forecast/): окно сверки — квартал target_date, факт — среднее gold_prices
// с приоритетом venue comex_front > moex_fix_usd, metric 'nav' не резолвится
// (факта нет). Проверка идёт по комментарию таблицы И колонок actual/error_pct —
// какая-то из трёх строк обязана назвать forecast_resolve; «факт» без имени
// механизма агент примет за ручной ввод.
func TestForecastAccuracyViewDescribesResolution(t *testing.T) {
	v := forecastAccuracyView

	texts := map[string]string{
		"comment":   v.Comment,
		"actual":    v.Columns["actual"],
		"error_pct": v.Columns["error_pct"],
	}

	var named int

	for _, text := range texts {
		if strings.Contains(text, "forecast_resolve") {
			named++
		}
	}

	if named == 0 {
		t.Fatalf("%s: comment/actual/error_pct must name the forecast_resolve importer — иначе агент не знает, кто заполняет факт", v.Name)
	}

	combined := strings.Join([]string{v.Comment, v.Columns["actual"], v.Columns["error_pct"]}, " ")

	if !strings.Contains(combined, "comex") {
		t.Errorf("%s: resolution mechanism must name the price venue (comex_front → moex_fix_usd), got: %s", v.Name, combined)
	}

	if !strings.Contains(combined, "nav") {
		t.Errorf("%s: comments must say that metric 'nav' is never resolved (факта нет), got: %s", v.Name, combined)
	}
}
