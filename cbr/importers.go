package cbr

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// cbrImporter — запись реестра импортёров пакета: сам импортёр, таблица, в которую
// он пишет, и запрос количественного чека версии (префлайта). Регистрируется
// конкретный тип пакета, а не обёртка: Name() и Import() — его собственные,
// производные (каталог рядов, витрину) публикует шаблон util.ClickHouseImport через
// поле AfterImport. Полей table и countPoints не видит никто, кроме теста
// TestCbrImportersPrefixedAndRegistered: по ним проверяется, что импортёр не занял
// таблицу вне пространства cbr_ и что объявленный в реестре префлайт совпадает с
// объявлением самого импортёра (Publication()).
type cbrImporter struct {
	chimport.ImportStat
	table string
	// countPoints — литерал PublicationCountPoints импортёра ("" — префлайта нет).
	// Литерал, а не метод Publication(): именно расхождение этих двух источников
	// значения и обязан ловить тест реестра.
	countPoints string
}

// cbrImporters — единственная точка регистрации импортёров ЦБ: персональных
// init()-append по файлам импортёров больше нет (14 записей в одном месте вместо
// 13 прежних). Порядок повторяет dagu/cbr.yaml; в chimport.Stats порядок значения
// не имеет.
//
// Таблицы записаны литералами намеренно. Ссылка на константу импортёра
// (cbrKeyRateTable) сделала бы проверку префикса тавтологичной: тест сравнивал бы
// константу с самой собой, тогда как литерал падает ровно на переименовании
// таблицы в имя вне cbr_-пространства — то есть на правке, которая увела бы
// витрину v_cbr_macro к чужим данным. Литералы сверены с константами кода при
// переносе регистрации.
//
// cbr_mp_survey и cbr_mo_br — записи с таблицей вне cbr_: ряды мониторинга
// предприятий (source='cbr_survey') и макроэкономического опроса (source='cbr_mo_br')
// лежат в общей macro_series. У обоих Name() отличается от имени таблицы, поэтому в
// реестре они записаны раздельно.
//
// Колонка countPoints — запрос количественного чека версии у импортёров, у которых
// точка контроля публикации объявлена (спека §3.5); пустая строка — префлайта нет.
// Литералы и здесь: запрос — часть утверждения «эти точки мои» (FINAL, а у
// владельцев общей macro_series — ещё и WHERE source), и тест реестра сверяет их с
// объявлением импортёра (Publication().CountPoints) — двумя независимыми записями
// одного утверждения. Незамеченная правка запроса в объявлении оставила бы реестр
// описывающим другой чек, а забытое объявление молча выключило бы префлайт.
var cbrImporters = []cbrImporter{
	{cbrCurrencyUSDImporter, "cbr_currency_usd", ""},
	{householdsBMesImporter, "cbr_households_b_mes", "SELECT count() FROM cbr_households_b_mes FINAL"},
	{cbrKeyRateImporter, "cbr_key_rate", ""},
	{cbrM2Importer, "cbr_m2", "SELECT count() FROM cbr_m2 FINAL"},
	{cbrGoldImporter, "cbr_gold", ""},
	{loansToCorporationsImporter, "cbr_loans_to_corporations", "SELECT count() FROM cbr_loans_to_corporations FINAL"},
	{loansToIndividualsImporter, "cbr_loans_to_individuals", "SELECT count() FROM cbr_loans_to_individuals FINAL"},
	{cbrInflExpImporter, "cbr_infl_exp", "SELECT count() FROM cbr_infl_exp FINAL"},
	{cbrBankIntRateImporter, "cbr_bank_int_rate", "SELECT count() FROM cbr_bank_int_rate FINAL"},
	{cbrCreditM2xImporter, "cbr_credit_m2x", "SELECT count() FROM cbr_credit_m2x FINAL"},
	{cbrIndicatorsCpdImporter, "cbr_indicators_cpd", "SELECT count() FROM cbr_indicators_cpd FINAL"},
	{cbrRuoniaImporter, "cbr_ruania", ""},
	{mpSurveyImporter, "macro_series", "SELECT count() FROM macro_series FINAL WHERE source = 'cbr_survey'"},
	{moBrImporter, "macro_series", "SELECT count() FROM macro_series FINAL WHERE source = 'cbr_mo_br'"},
}

// init — единственная регистрация импортёров ЦБ в chimport.Stats. Порядок
// append совпадает с порядком реестра; main.go запускает всех (или фильтрует по
// CLICKHOUSE_IMPORT_STAT=Name()).
func init() {
	for _, imp := range cbrImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
