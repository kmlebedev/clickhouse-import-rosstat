package fedstat

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const fedstatSource = "fedstat"

// fedstatSeriesMeta — каталог всех 158 товаров источника (правило 11 AGENTS.md).
// Ряды, которых нет в 2008–2022 или в 2026, остаются в каталоге с явной
// оговоркой: MCP-агент обязан понимать каждый ряд источника, а не только те,
// у которых есть точки в текущем срезе.
func fedstatSeriesMeta() []util.SeriesMeta {
	meta := make([]util.SeriesMeta, 0, len(fedstatProducts))
	for _, product := range fedstatProducts {
		meta = append(meta, fedstatProductMeta(product))
	}
	return meta
}

func fedstatProductMeta(product fedstatProduct) util.SeriesMeta {
	description := fmt.Sprintf("Средняя потребительская цена (тариф) «%s» по РФ, рублей за единицу измерения из названия товара. Значение — уровень цены, не индекс: изменение за n недель = value_t / value_{t−n} − 1. Дата — дата регистрации цен (понедельник недели). 2008–2022 — РФ (s_OKATO=%s); 2026 недели 39–40 — РФ без учёта новых субъектов (s_OKATO=%s). 2023–2025 в источнике отсутствуют.", product.name, fedstatRegionRFCode, fedstatRegionRFNewCode)
	if !product.historical {
		description += " В 2008–2022 значений нет: товар появляется только в 2026."
	}
	if !product.y2026 {
		description += " В 2026 не публикуется."
	}
	return util.SeriesMeta{
		Source:      fedstatSource,
		Series:      product.name,
		Title:       product.name,
		Unit:        "рубль",
		Frequency:   "W",
		Origin:      fmt.Sprintf("fedstat.ru/indicator/37426, s_grtov=%s, s_OKATO=%s/%s; SDMX downloadData.do", product.code, fedstatRegionRFCode, fedstatRegionRFNewCode),
		Description: description,
	}
}

// afterFedstatMacro — производные импортёра: каталог и витрина. Публикуются и на
// no-op-пути: upsert каталога и CREATE OR REPLACE витрины идемпотентны, поэтому
// сбой публикации прошлого прогона лечится следующим.
func afterFedstatMacro(meta []util.SeriesMeta) func(context.Context, driver.Conn) error {
	return func(ctx context.Context, conn driver.Conn) error {
		if err := util.UpsertSeriesCatalog(ctx, conn, meta); err != nil {
			return err
		}
		_, err := util.CreateView(ctx, conn, fedstatWeeklyPricesView)
		return err
	}
}
