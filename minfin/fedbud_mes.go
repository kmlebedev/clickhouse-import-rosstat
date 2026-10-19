package minfin

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

// Краткая ежемесячная информация об исполнении федерального бюджета
// (млрд. руб., накоплено с начала года).
// https://minfin.gov.ru/ru/statistics/fedbud/execute?id_57=80042-kratkaya_ezhemesyachnaya_informatsiya_ob_ispolnenii_federalnogo_byudzheta_mlrd._rub._nakopleno_s_nachala_goda
// Таблица: https://minfin.gov.ru/common/upload/library/2025/08/main/Prilozhenie_3_dannye_109-111_%E2%80%94_mes.xlsx
const (
	fedbudExecutePath     = "/ru/statistics/fedbud/execute"
	fedbudMesTable        = "minfin_fed_bud_mes"
	fedbudMesLinkSelector = `.document_list a[href$="_mes.xlsx"]`
)

var fedbudMesFields = []string{"Доходы, всего", "Расходы, всего", "Акцизы", "Национальная оборона", "привлечение", "Дефицит (-)/Профицит (+)"}

// fedbudMes — федеральный бюджет: таблица, ссылка со страницы каталога, общий
// разбор листа «месяц» (накопление переводится в месячные разности). Каталог
// рядов и витрину v_minfin_budget публикует AfterImport шаблона; регистрация —
// реестром minfin/importers.go (стандарт §8).
var fedbudMes = newMinfinFileImporter(
	fedbudMesTable,
	minfinURL+fedbudExecutePath,
	fedbudMesLinkSelector,
	minfinSeriesCreate(true),
	fedBudImport,
	fedbudMesSeriesMeta,
	"",
)

func fedBudImport(xlsx *excelize.File, batch driver.Batch) error {
	return importFedbud(xlsx, batch, fedbudLayout{Table: fedbudMesTable, Fields: fedbudMesFields})
}
