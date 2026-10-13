package cbr

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// cbrImporter — запись реестра импортёров пакета: сам импортёр и таблица, в
// которую он пишет. chimport.ImportStat встроен интерфейсом, поэтому Name() и
// Import() делегируются обёртке (publishedStat публикует ряды и витрину,
// mpSurveyImporter — свои), а поле table видит только тест
// TestCbrImportersPrefixedAndRegistered: по нему проверяется, что импортёр не занял
// таблицу вне пространства cbr_.
type cbrImporter struct {
	chimport.ImportStat
	table string
}

// cbrImporters — единственная точка регистрации импортёров ЦБ: персональных
// init()-append по файлам импортёров больше нет (13 точек стали одной). Порядок
// повторяет dagu/cbr.yaml; в chimport.Stats порядок значения не имеет.
//
// Таблицы записаны литералами намеренно. Ссылка на константу импортёра
// (cbrKeyRateTable) сделала бы проверку префикса тавтологичной: тест сравнивал бы
// константу с самой собой, тогда как литерал падает ровно на переименовании
// таблицы в имя вне cbr_-пространства — то есть на правке, которая увела бы
// витрину v_cbr_macro к чужим данным. Литералы сверены с константами кода при
// переносе регистрации.
//
// cbr_mp_survey — единственная запись с таблицей вне cbr_: ряды мониторинга
// предприятий лежат в общей macro_series под source='cbr_survey'. Его Name()
// отличается от имени таблицы, поэтому в реестре они записаны раздельно.
var cbrImporters = []cbrImporter{
	{cbrCurrencyUSDImporter, "cbr_currency_usd"},
	{householdsBMesImporter, "cbr_households_b_mes"},
	{cbrKeyRateImporter, "cbr_key_rate"},
	{cbrM2Importer, "cbr_m2"},
	{cbrGoldImporter, "cbr_gold"},
	{loansToCorporationsImporter, "cbr_loans_to_corporations"},
	{loansToIndividualsImporter, "cbr_loans_to_individuals"},
	{cbrInflExpImporter, "cbr_infl_exp"},
	{cbrBankIntRateImporter, "cbr_bank_int_rate"},
	{cbrCreditM2xImporter, "cbr_credit_m2x"},
	{cbrIndicatorsCpdImporter, "cbr_indicators_cpd"},
	{cbrRuoniaImporter, "cbr_ruania"},
	{mpSurveyImporter, "macro_series"},
}

// init — единственная регистрация импортёров ЦБ в chimport.Stats. Порядок
// append совпадает с порядком реестра; main.go запускает всех (или фильтрует по
// CLICKHOUSE_IMPORT_STAT=Name()).
func init() {
	for _, imp := range cbrImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
