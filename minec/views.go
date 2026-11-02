package minec

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// minecMacroView — витрина пакета в long-форме (source, series, date, value).
// Tables — константой пакета (util.CreateView проверяет по ней наличие таблицы),
// Select — литералом: витрина публичный контракт агента, и переименование
// константы не должно молча увести её к другой таблице.
var minecMacroView = util.View{
	Name:   "v_minec_macro",
	Tables: []string{minecTable},
	Select: `SELECT date, series, value FROM macro_series FINAL WHERE source = 'minec'`,
	Comment: "Ряды Минэкономразвития из раздела «Конъюнктура мировых товарных рынков»: средняя за отчётный " +
		"месяц цена нефти сорта «Юралс», долл. США за баррель (2018-01 … 2026-10; в источнике нет " +
		"2018-06/2018-07 — разрыв, а не нули). Единицы и описание — v_series_catalog по (source='minec', series)",
	Columns: map[string]string{
		"date":   "1-е число отчётного месяца, за который посчитана средняя цена; не дата публикации (страница выходит в начале следующего месяца)",
		"series": "Ряд каталога: urals_avg_price",
		"value":  "Средняя за месяц цена нефти Urals, долл. США за баррель — уровень, а не темп",
	},
}

// afterMinecMacro публикует производные: каталог ряда и витрину. Ошибка отменяет
// запись строки версии в шаблоне, поэтому следующий прогон повторит полный путь.
func afterMinecMacro(ctx context.Context, conn driver.Conn) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, minecSeriesMeta); err != nil {
		return err
	}
	if _, err := util.CreateView(ctx, conn, minecMacroView); err != nil {
		return err
	}
	return nil
}
