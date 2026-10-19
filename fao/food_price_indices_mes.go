package fao

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const (
	// faoUrl — страница индекса продовольственных цен ФАО. Ссылка на CSV меняет
	// sfvrsn при обновлении файла, поэтому адрес артефакта вычисляется в Import(),
	// а не хранится константой (стандарт §4.3).
	faoUrl = "https://www.fao.org/worldfoodsituation/foodpricesindex/ru/"
	// faoLinkSelector — селектор актуальной CSV-выгрузки. Тот же селектор
	// используется тестом на HTML-фикстуре (§4.5).
	faoLinkSelector = `a[href*="food_price_indices_data.csv"]`
	// faoFoodPriceTable — физическая таблица. Имя сохранено legacy-контрактом:
	// его читают scripts/cpi_nowcast.py и scripts/cpi_monthly.py.
	faoFoodPriceTable = "fao_food_price_Indices_monthly"
)

var faoFoodPrice = util.ClickHouseImport{
	TableName: faoFoodPriceTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, price_index Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`},
	BodyFunc:    parseFaoFoodPrice,
	AfterImport: publishFao,
}

type faoFoodPriceStat struct {
	util.ClickHouseImport
}

// Import вычисляет адрес CSV со страницы ФАО и отдаёт полный цикл шаблону.
func (s *faoFoodPriceStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	page, err := util.GetPage(faoUrl)
	if err != nil {
		return count, fmt.Errorf("fao: страница %s: %w", faoUrl, err)
	}
	href, err := util.FindLink(page, faoLinkSelector)
	if err != nil {
		return count, fmt.Errorf("fao: не найдена ссылка на food_price_indices_data.csv на %s: %w", faoUrl, err)
	}
	s.DataUrl = faoAbsoluteURL(href)
	return s.ClickHouseImport.Import(ctx, conn)
}

// faoAbsoluteURL принимает href как есть, если он абсолютный; иначе разрешает
// относительно страницы ФАО. FindLink отдаёт href без префикса домена.
func faoAbsoluteURL(href string) string {
	href = strings.TrimSpace(href)
	ref, err := url.Parse(href)
	if err != nil {
		return href
	}
	if ref.IsAbs() {
		return href
	}
	base, err := url.Parse(faoUrl)
	if err != nil {
		return href
	}
	return base.ResolveReference(ref).String()
}

// faoFoodPriceImporter — единственный импортёр пакета; регистрация — реестром
// fao/importers.go (стандарт §8).
var faoFoodPriceImporter = &faoFoodPriceStat{ClickHouseImport: faoFoodPrice}
