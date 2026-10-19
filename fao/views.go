package fao

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

var faoFoodPriceView = util.View{
	Name:   "v_fao_food_price",
	Tables: []string{faoFoodPriceTable},
	Select: `SELECT 'fao' AS source
		, name AS series
		, date AS date
		, date - INTERVAL 1 MONTH AS period_date
		, toFloat64(price_index) AS value
	FROM ` + faoFoodPriceTable + ` FINAL`,
	Comment: "Индекс продовольственных цен ФАО: сводный ряд и пять подиндексов (Meat, Dairy, Cereals, Oils, Sugar). date — 1-е число месяца публикации, period_date — месяц наблюдения (date − 1 месяц); value — номинальный индекс 2014-2016=100. Real-индекса в источнике нет. Для темпа м/м сравнивать соседние period_date одного series",
	Columns: map[string]string{
		"source":      "Источник: всегда fao",
		"series":      "Ряд: Food Price Index, Meat, Dairy, Cereals, Oils или Sugar",
		"date":        "Дата публикации: 1-е число месяца, следующего за месяцем наблюдения",
		"period_date": "Месяц наблюдения (date − 1 месяц); по нему считать темпы и лаги",
		"value":       "Номинальный индекс цен, 2014-2016=100 (Float64)",
	},
}

// publishFao публикует производные пакета: каталог шести рядов и витрину.
func publishFao(ctx context.Context, conn driver.Conn) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, faoSeriesMeta); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, faoFoodPriceView)
	return err
}
