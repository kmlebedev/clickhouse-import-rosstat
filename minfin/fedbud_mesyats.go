package minfin

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

// Краткая ежемесячная информация об исполнении консолидированного бюджета
// Российской Федерации и государственных внебюджетных фондов
// (млрд. руб., накоплено с начала года).
// https://minfin.gov.ru/ru/document?id_4=93447-kratkaya_ezhemesyachnaya_informatsiya_ob_ispolnenii_konsolidirovannogo_byudzheta_rossiiskoi_federatsii_i_gosudarstvennykh_vnebyudzhetnykh_fondov_mlrd._rub._nakopleno_s_nachala_goda
// https://minfin.gov.ru/common/upload/library/2026/10/main/Prilozhenie_8_dannye_115-117_%E2%80%94_mesyats.xlsx
const (
	conbudExecutePath         = "/ru/statistics/conbud/execute"
	fedbudMesyatsTable        = "minfin_fed_bud_mesyats"
	fedbudMesyatsLinkSelector = `a[href*="_mesyats.xlsx"]`
)

var fedbudMesyatsFields = []string{"Доходы, всего", "Расходы, всего", "Акцизы", "Национальная оборона", "Дефицит (-)/Профицит (+)"}

// fedbudMesyats — консолидированный бюджет: таблица, ссылка со страницы каталога
// и общий с федеральным бюджетом разбор листа «месяц». Каталог рядов и витрину
// v_minfin_budget публикует AfterImport шаблона; регистрация — реестром
// minfin/importers.go (стандарт §8).
var fedbudMesyats = newMinfinFileImporter(
	fedbudMesyatsTable,
	minfinURL+conbudExecutePath,
	fedbudMesyatsLinkSelector,
	minfinSeriesCreate(true),
	fedbudMesyatsImport,
	fedbudMesyatsSeriesMeta,
	"",
)

func fedbudMesyatsImport(xlsx *excelize.File, batch driver.Batch) error {
	return importFedbud(xlsx, batch, fedbudLayout{Table: fedbudMesyatsTable, Fields: fedbudMesyatsFields})
}
