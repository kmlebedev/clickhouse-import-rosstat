package wgc

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// wgcImporter — запись реестра импортёров пакета: сам импортёр и таблица, в которую он
// пишет. Поле table читает только тест TestWgcImportersPrefixedAndRegistered: по нему
// проверяется, что импортёр пишет в таблицу из литерального allowlist'а владельцев
// (стандарт §8.2/§8.3(б)).
//
// Оба импортёра пишут в общую macro_series, поэтому Name() != TableName (стандарт §7.2).
// Точка контроля публикации не объявляется ни у одного (стандарт §9.3): wgc_etf_flows
// ходит в JSON API на каждом прогоне и ловит публикацию в день выхода, а wgc_cb_reserves
// читает вручную скачанные выгрузки (goldhub защищён Cloudflare) — HEAD-отпечатку
// источника нет.
type wgcImporter struct {
	chimport.ImportStat
	table string // литерал таблицы
}

// wgcImporters — единственная точка регистрации импортёров пакета; персональных
// init()-append в файлах импортёров нет (стандарт §8.1). Порядок повторяет dagu/wgc.yaml.
var wgcImporters = []wgcImporter{
	{ImportStat: &etfFlowsStat{}, table: "macro_series"},
	{ImportStat: &cbReservesStat{}, table: "macro_series"},
}

func init() {
	for _, imp := range wgcImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
