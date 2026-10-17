package bank

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// sberSpendingView — витрина long-формы обоих наборов Сбера (стандарт §10.3, образец
// rosstat/series_meta.go): у наборов разные таблицы, разные единицы значения и разные имена
// колонки-ряда (`name` у месячного, `category` у недельного), поэтому агент читает оба ряда одним
// запросом, а различает их по (source, series) — сверяясь с v_series_catalog.
//
// Источники витрины записаны в двух полях по-разному — ровно как в v_rosstat_macro
// (rosstat/series_meta.go): Tables — константами пакета (util.CreateView проверяет по ним наличие
// таблиц, поэтому переименование константы обязано тянуть за собой и проверку), а Select —
// литералами: витрина публичный контракт для агента, и переименование константы пакета не должно
// молча уводить её к другой таблице. Форма витрины (имя, состав Tables и оба source-литерала)
// закреплена лёгким тестом контракта в bank/sber_csi_test.go.
//
// Колонки приводятся к Float64 (в таблицах legacy Float32, спека §4.5) — иначе ClickHouse отдал бы
// агенту float32-точность двух наборов по-разному.
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

// bankMacroView — витрина long-формы всего банковского пакета (стандарт §10.3, образец —
// v_rosstat_macro в rosstat/series_meta.go и sberSpendingView выше): ряды пяти сырых таблиц —
// ставка предложения по ипотеке банков (ДОМ.РФ), отчётности группы ВТБ (МСФО) и ПАО Сбербанк
// (РСБУ) и оба набора СберИндекса. Источники записаны в двух полях по-разному — ровно как у
// v_rosstat_macro: Tables — константами пакета (util.CreateView проверяет по ним наличие таблиц,
// поэтому переименование константы обязано тянуть за собой и проверку), а Select — литералами:
// витрина публичный контракт для агента, и переименование константы пакета не должно молча уводить
// её к другой таблице. Форма витрины (имя, состав Tables и пять source-литералов) закреплена
// тестом контракта в bank/series_meta_test.go.
//
// Колонки приводятся к Float64 (в таблицах legacy Float32, спека §4.5) — иначе ClickHouse отдал бы
// агенту float32-точность наборов по-разному. У series разное имя колонки-ряда (name у четырёх
// таблиц, category у недельного набора) и разные единицы значения — агент читает все ряды одним
// запросом, а различает их по (source, series), сверяясь с v_series_catalog. Курированные метрики
// эмитентов эта витрина не заменяет: они лежат в company_financials (bank_company_metrics), а
// витрина сохраняет сырые строки отчётностей.
//
// Tables — все пять таблиц: util.CreateView пропускает витрину, пока не существует каждой из них
// (импортёры идут по одному шагами dagu/bank.yaml), поэтому прогон одного импортёра витрину не
// создаёт, а прогон, закрывший последнюю таблицу, собирает её целиком.
var bankMacroView = util.View{
	Name: "v_bank_macro",
	Tables: []string{
		domrfMortgageTable, vtbIfrsTable, sberRpbuDataUrlTable, sberCSITable, sberCSIWeekTable,
	},
	Select: `SELECT 'domrf_mortgage' AS source, name AS series, date, toFloat64(rate) AS value FROM domrf_mortgage FINAL
UNION ALL SELECT 'rus_vtb_group_ifrs', name, date, toFloat64(balance) FROM rus_vtb_group_ifrs FINAL
UNION ALL SELECT 'sber_finansovie_rezultaty', name, date, toFloat64(balance) FROM sber_finansovie_rezultaty FINAL
UNION ALL SELECT 'sber_consumper_spending_index', name, date, toFloat64(value) FROM sber_consumper_spending_index FINAL
UNION ALL SELECT 'sber_izmenenie_trat', category, date, toFloat64(value) FROM sber_izmenenie_trat FINAL`,
	Comment: "Ряды банковского пакета: ставка предложения по ипотеке на новостройки банков (ДОМ.РФ), отчётности группы ВТБ по МСФО и ПАО Сбербанк по РСБУ, потребительские расходы СберИндекса (месячный индекс и недельное изменение по категориям). У источников разные единицы значения и привязка даты: перед расчётом читать v_series_catalog по (source, series)",
	Columns: map[string]string{
		"source": "Набор данных: domrf_mortgage (ставки банков), rus_vtb_group_ifrs (отчётность группы ВТБ по МСФО), sber_finansovie_rezultaty (отчётность ПАО Сбербанк по РСБУ), sber_consumper_spending_index (месячный индекс СберИндекса), sber_izmenenie_trat (недельный темп СберИндекса)",
		"series": "Имя ряда из источника: банк, подпись строки отчётности (у Сбера — дословно, включая хвостовые пробелы двух подписей баланса) или категория расходов. У наборов СберИндекса имя, которого нет в v_series_catalog, импортируется с предупреждением в журнале; единицы и привязку даты любого ряда читать по v_series_catalog (source, series)",
		"date":   "Дата точки: у банковских отчётностей — последний день отчётного месяца, у месячного индекса СберИндекса — первое число месяца, у недельного набора — воскресенье ISO-недели",
		"value":  "Значение (Float64): млрд ₽, % годовых, доли коэффициентов или индекс — единицы и чтение зависят от ряда, см. unit и description в v_series_catalog",
	},
}
