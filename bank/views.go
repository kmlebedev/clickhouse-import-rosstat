package bank

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// sberSpendingView — витрина long-формы обоих наборов Сбера (стандарт §10.3, образец
// rosstat/series_meta.go): у наборов разные таблицы, разные единицы значения и разные имена
// колонки-ряда (`name` у месячного, `category` у недельного), поэтому агент читает оба ряда одним
// запросом, а различает их по (source, series) — сверяясь с v_series_catalog.
//
// Имена таблиц в Select — литералами, как в v_rosstat_macro: витрина публичный контракт для
// агента, и переименование константы пакета не должно молча уводить её к другой таблице. Колонки
// приводятся к Float64 (в таблицах legacy Float32, спека §4.5) — иначе ClickHouse отдал бы агенту
// float32-точность двух наборов по-разному.
//
// Tables — обе таблицы: util.CreateView пропускает витрину, пока не существует хотя бы одной из
// них (импортёры запускаются по одному шагами dagu/bank.yaml), поэтому первый прогон каждого
// импортёра витрину не создаёт, а последний — пересоздаёт её целиком.
var sberSpendingView = util.View{
	Name:   "v_bank_sber_spending",
	Tables: []string{sberCSITable, sberCSIWeekTable},
	Select: `SELECT 'sber_consumper_spending_index' AS source, name AS series, date, toFloat64(value) AS value FROM sber_consumper_spending_index FINAL
UNION ALL SELECT 'sber_izmenenie_trat', category, date, toFloat64(value) FROM sber_izmenenie_trat FINAL`,
	Comment: "Потребительские расходы по данным СберИндекса: месячный индекс реальных потребительских расходов (100 = декабрь 2018) и недельное изменение расходов по категориям (% к сопоставимой ISO-неделе прошлого года). Единицы у наборов разные — сверяться с v_series_catalog по (source, series)",
	Columns: map[string]string{
		"source": "Набор данных: sber_consumper_spending_index (месячный индекс) или sber_izmenenie_trat (недельный темп)",
		"series": "Категория расходов из файла источника; у месячного набора — Всего, Продовольственные товары, Непродовольственные товары, Общественное питание, Услуги; у недельного — 46 категорий, включая агрегат «Все категории». Категория, появившаяся в источнике и ещё не описанная в v_series_catalog, импортируется с предупреждением в журнале",
		"date":   "Дата точки: у месячного набора — первое число месяца, у недельного — воскресенье ISO-недели",
		"value":  "Значение: индекс (месячный набор, 100 = декабрь 2018) или процент г/г (недельный набор). Читать по v_series_catalog",
	},
}
