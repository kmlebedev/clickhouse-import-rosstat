package views

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// imfMacroView — ряды IMF (source='imf'): золотые резервы (monetary gold) центробанков
// из IRFCL. Пишутся сессионно через ingest (POST /v1/manual_series, source='imf').
var imfMacroView = util.View{
	Name:   "v_imf_macro",
	Tables: []string{"macro_series"},
	Select: `SELECT date, series, value FROM macro_series FINAL WHERE source = 'imf'`,
	Comment: "Ряды IMF (source='imf'): золотые резервы (monetary gold) центробанков из IRFCL — " +
		"7 стран (POL, CHN, RUS, UZB, KAZ, CZE, TUR); млн USD по рыночной стоимости, месячная частота, " +
		"лаг публикации ~2 месяца. Дельта месяца — изменение стоимости резервов; для тонн делить на цену " +
		"золота. Описание каждого ряда и как читать дельты — в v_series_catalog",
	Columns: map[string]string{
		"date":   "Первый день месяца наблюдения (Date32)",
		"series": "Код ряда: ra_gold_<iso3> (ra_gold_pol, ra_gold_chn, ...)",
		"value":  "Значение в единицах v_series_catalog: млн USD, рыночная стоимость (не тонны)",
	},
}
