package domrf

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// Ключи каталога рядов (source в series_catalog): имена таблиц пакета. Второй источник — строка
// недельных ставок: у неё своя таблица, своя частота и своя единица, поэтому общий source схлопнул
// бы разные ряды в один ключ каталога.
const (
	domrfSourceMarket = domrfMarketTable
	domrfSourceWeekly = domrfWeeklyTable
)

// domrfMarketOrigin / domrfWeeklyOrigin — происхождение ряда: единственное место, где видно, из
// какого блока страницы и какого файла пришло имя.
const (
	domrfMarketOrigin = "дом.рф /analytics/mortgage/, блок statistical_series, файл «Общая таблица по РФ», лист «общая таблица_помесячно»"
	domrfWeeklyOrigin = "дом.рф /analytics/mortgage/, блок mortgage-dinamika-stavok (JSON в атрибуте main#app)"
)

// domrfMarketMeta — описание ряда набора A: название, единица и как читать значение. Единицы
// разные (млн руб., единиц, тыс. руб., доля, % годовых) — без каталога агент не отличит долю от
// процента, а уровень от темпа.
type domrfMarketMetaEntry struct {
	Title       string
	Unit        string
	Description string
}

var domrfMarketMeta = map[string]domrfMarketMetaEntry{
	"issued_count_rub":     {Title: "Выдано ипотечных кредитов, рублёвые", Unit: "единиц", Description: "Количество выданных за месяц ипотечных кредитов в рублях. С 2009-01; до этого источник публиковал только общий итог. Дата точки — первое число отчётного месяца."},
	"issued_count_fx":      {Title: "Выдано ипотечных кредитов, валютные", Unit: "единиц", Description: "Количество выданных за месяц ипотечных кредитов в валюте. С 2019-02; валютная выдача с 2022 близка к нулю."},
	"issued_count_total":   {Title: "Выдано ипотечных кредитов, всего", Unit: "единиц", Description: "Количество выданных за месяц ипотечных кредитов, всего (рубли + валюта). С 2009-01; 2006–2008 источник даёт квартальными строками без количества."},
	"issued_count_ddu":     {Title: "Выдано ипотечных кредитов под ДДУ", Unit: "единиц", Description: "Количество кредитов, выданных под договоры долевого участия (первичный рынок). С 2016-02. Ключевой ряд инвестиционного канала: показывает, какая часть выдачи идёт в новостройки."},
	"issued_volume_rub":    {Title: "Объём выдачи ипотеки, рублёвые кредиты", Unit: "млн руб.", Description: "Объём выданных за месяц ипотечных кредитов в рублях. С 2019-02."},
	"issued_volume_fx":     {Title: "Объём выдачи ипотеки, валютные кредиты", Unit: "млн руб.", Description: "Объём выданных за месяц ипотечных кредитов в валюте. С 2019-02."},
	"issued_volume_total":  {Title: "Объём выдачи ипотеки, всего", Unit: "млн руб.", Description: "Объём выданных за месяц ипотечных кредитов, всего (рубли + валюта). С 2006-01 — самый длинный ряд набора."},
	"issued_volume_ddu":    {Title: "Объём выдачи ипотеки под ДДУ", Unit: "млн руб.", Description: "Объём кредитов под договоры долевого участия (первичный рынок). С 2016-02. Отношение к issued_volume_total — доля кредита, идущего в новостройки."},
	"avg_loan_size":        {Title: "Средний размер ипотечного кредита", Unit: "тыс. руб.", Description: "Средний размер выданного за месяц кредита в рублях. С 2009-01. Растёт и от цены жилья, и от инфляции — уровень несопоставим между годами без дефлятора."},
	"portfolio_total":      {Title: "Задолженность по выданным кредитам, всего", Unit: "млн руб.", Description: "Верхний итог блока задолженности: баланс банков + приобретённые права требований + ИЦБ вне баланса + ДОМ.РФ. С 2006-01."},
	"portfolio_bank":       {Title: "Задолженность на балансе банков", Unit: "млн руб.", Description: "Задолженность по выданным кредитам на балансе банков, всего (рубли + валюта). С 2006-01."},
	"portfolio_acquired":   {Title: "Приобретённые права требований", Unit: "млн руб.", Description: "Задолженность по приобретённым правам требований по кредитам (рублёвая часть). С 2009-01."},
	"portfolio_bank_total": {Title: "Банковский портфель", Unit: "млн руб.", Description: "Банковский портфель: баланс банков + приобретённые права требований. С 2006-01."},
	"portfolio_ddu":        {Title: "Задолженность по кредитам под ДДУ", Unit: "млн руб.", Description: "Часть банковского портфеля, выданная под договоры долевого участия (первичный рынок). С 2016-02."},
	"portfolio_mbs":        {Title: "ИЦБ вне баланса", Unit: "млн руб.", Description: "Ипотечные ценные бумаги, секьюритизированные вне баланса банков. С 2006-01."},
	"portfolio_domrf":      {Title: "Портфель ДОМ.РФ", Unit: "млн руб.", Description: "Задолженность, обслуживаемая ДОМ.РФ (выкупленные кредиты и закладные). С 2011-01."},
	"overdue_total":        {Title: "Просроченная задолженность, всего", Unit: "млн руб.", Description: "Задолженность с просрочкой в составе выданных кредитов, всего (рубли + валюта). С 2006-01."},
	"overdue_ddu":          {Title: "Просроченная задолженность по ДДУ", Unit: "млн руб.", Description: "Просроченная задолженность по кредитам под ДДУ. С 2016-02."},
	"overdue_share_total":  {Title: "Доля просроченной задолженности, всего", Unit: "доля", Description: "Доля просроченной задолженности в общем объёме задолженности на балансе банков (0.012 = 1,2 %). С 2006-01."},
	"overdue_share_ddu":    {Title: "Доля просроченной задолженности по ДДУ", Unit: "доля", Description: "Доля просроченной задолженности по кредитам под ДДУ (0.005 = 0,5 %). С 2016-02."},
	"rate_month":           {Title: "Средневзвешенная ставка, в течение месяца", Unit: "% годовых", Description: "Средневзвешенная ставка по выданным за месяц рублёвым ипотечным кредитам, % годовых. С 2009-02 (ранние ячейки — текст с десятичной запятой). Это ставка выдачи (ex-post), а не ставка предложения — с domrf_mortgage_weekly не смешивать."},
	"rate_ytd":             {Title: "Средневзвешенная ставка, с начала года", Unit: "% годовых", Description: "Средневзвешенная ставка по выданным с начала года рублёвым ипотечным кредитам, % годовых. С 2019-02."},
	"rate_primary":         {Title: "Средневзвешенная ставка, первичный рынок", Unit: "% годовых", Description: "Средневзвешенная ставка по кредитам на первичном рынке (новостройки), % годовых, за месяц. С 2016-02. Сравнивать с rate_secondary — это спред первички и вторички."},
	"rate_secondary":       {Title: "Средневзвешенная ставка, вторичный рынок", Unit: "% годовых", Description: "Средневзвешенная ставка по кредитам на вторичном рынке, % годовых, за месяц. С 2016-02."},
}

// domrfWeeklyMeta — описание продуктов недельного ряда. Ставка — процент годовых, как в
// источнике; это ставка предложения топ-20 банков, а не ставка выдачи.
var domrfWeeklyMeta = map[string]domrfMarketMetaEntry{
	"Новостройка":      {Title: "Ставка предложения, новостройки", Unit: "% годовых", Description: "Медианная ставка предложения топ-20 ипотечных банков по кредитам на новостройки, % годовых, недельный срез (пятница). Ряд начинается 2021-09-03."},
	"Семейная ипотека": {Title: "Ставка предложения, семейная ипотека", Unit: "% годовых", Description: "Ставка предложения по семейной ипотеке, % годовых. Для мониторинга источник берёт московскую семью с двумя детьми; ставка программы 2–12 % зависит от региона и числа детей."},
	"Вторичный рынок":  {Title: "Ставка предложения, вторичный рынок", Unit: "% годовых", Description: "Ставка предложения по кредитам на готовое жильё, % годовых, недельный срез."},
	"Рефинансирование": {Title: "Ставка предложения, рефинансирование", Unit: "% годовых", Description: "Ставка предложения по рефинансированию ипотеки, % годовых, недельный срез."},
	"Льготная ипотека": {Title: "Ставка предложения, льготная ипотека", Unit: "% годовых", Description: "Ставка предложения по программе льготной ипотеки, % годовых; ряд короче остальных — программа завершилась, источник перестал публиковать значения."},
}

// domrfMarketCatalog собирает описания рядов набора A в порядке колонок источника. Ряд без
// описания в каталог не попадает — тест сверяет, что описаны все (стандарт §10.1).
func domrfMarketCatalog() []util.SeriesMeta {
	meta := make([]util.SeriesMeta, 0, len(domrfMarketSeriesList))
	for _, series := range domrfMarketSeriesList {
		entry, ok := domrfMarketMeta[series.Series]
		if !ok {
			continue
		}
		meta = append(meta, util.SeriesMeta{
			Source: domrfSourceMarket, Series: series.Series, Title: entry.Title,
			Unit: entry.Unit, Frequency: "M", Origin: domrfMarketOrigin, Description: entry.Description,
		})
	}
	return meta
}

// domrfWeeklyCatalog собирает описания продуктов недельного ряда в порядке блока источника.
func domrfWeeklyCatalog() []util.SeriesMeta {
	meta := make([]util.SeriesMeta, 0, 5)
	for _, product := range domrfWeeklyProducts {
		entry, ok := domrfWeeklyMeta[product]
		if !ok {
			continue
		}
		meta = append(meta, util.SeriesMeta{
			Source: domrfSourceWeekly, Series: product, Title: entry.Title,
			Unit: entry.Unit, Frequency: "W", Origin: domrfWeeklyOrigin, Description: entry.Description,
		})
	}
	return meta
}

// domrfSeriesMeta — общий каталог пакета: 24 ряда рынка и 5 продуктов ставок.
func domrfSeriesMeta() []util.SeriesMeta {
	meta := domrfMarketCatalog()
	return append(meta, domrfWeeklyCatalog()...)
}

// afterDomrfMacro публикует производные импортёров пакета (образец rosstat/series_meta.go):
// каталог рядов и обе витрины. Витрины создаются только когда есть все их таблицы, поэтому
// прогон одного импортёра чужую витрину не создаёт и не ломает — её соберёт прогон, закрывший
// последнюю таблицу.
func afterDomrfMacro(ctx context.Context, conn driver.Conn) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, domrfSeriesMeta()); err != nil {
		return err
	}
	if _, err := util.CreateView(ctx, conn, domrfMortgageView); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, domrfPriceIndexView)
	return err
}
