package views

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// imfMacroView — ряды IMF (source='imf'): золотые резервы (monetary gold) центробанков
// из IRFCL. Пишутся сессионно через ingest (POST /v1/manual_series, source='imf').
var imfMacroView = util.View{
	Name:   "v_imf_macro",
	Tables: []string{"macro_series"},
	Select: `SELECT date, series, value FROM macro_series FINAL WHERE source = 'imf'`,
	Comment: "Ряды IMF (source='imf'): золотые резервы (monetary gold) центробанков из IRFCL. " +
		"Сейчас заполнен только Китай (ra_gold_chn) — IRFCL через datasource отдаёт нули/пропуски по остальным " +
		"наблюдаемым странам (POL/CZE/TUR — нули, RUS/KAZ — без индикатора, UZB — без данных); покупки ЦБ по ним закрыты " +
		"импортёром wgc_cb_reserves (source='wgc', тонны, витрина v_cb_gold_dynamics; follow-up фазы 3 GOLD_THESIS_DATA_ROADMAP). " +
		"Единица — как отдаёт IRFCLDT1_IRFCL40_USD " +
		"(USD, рыночная стоимость; не тонны). Описание ряда — в v_series_catalog",
	Columns: map[string]string{
		"date":   "Первый день месяца наблюдения (Date32)",
		"series": "Код ряда: ra_gold_<iso3> (сейчас ra_gold_chn)",
		"value":  "Значение IRFCLDT1_IRFCL40_USD (USD, рыночная стоимость; не тонны)",
	},
}
