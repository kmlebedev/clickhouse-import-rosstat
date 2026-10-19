package cbr

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// cbrImporter — запись реестра импортёров пакета: сам импортёр, таблица, в которую
// он пишет, и запрос количественного чека версии (префлайта). Регистрируется сам
// импортёр пакета — значение или указатель на его структуру, — а не отдельный
// безымянный тип-обёртка: Name() и Import() принадлежат импортёру (у шаблонных
// промотированы из util.ClickHouseImport), а производные (каталог рядов, витрину)
// публикует шаблон util.ClickHouseImport через поле AfterImport. У cbr_mp_summary
// собственный Import(): у импортёра три свои таблицы, а шаблон ведёт одну. Полей
// table и countPoints не видит никто, кроме теста
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
// init()-append по файлам импортёров больше нет (22 записи в одном месте вместо
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
//
// У cbr_mp_summary префлайта нет сознательно (спека §3.1): дельта считается по индексу
// материалов заседаний, а известность документа — тройная: его url есть в шапке И по дате
// его meeting_date есть хотя бы одна строка блоков (cbr_mp_summary_block) И хотя бы одна
// regex-строка каналов (cbr_mp_summary_channel, method='regex'); HEAD-отпечаток страницы
// резюме ответил бы только за неё саму — у импортёра три свои таблицы, а не один артефакт
// версии.
//
// У cbr_dip_bulletin префлайт объявлен, но повторный прогон решает не он, а гейт: артефакт —
// набор из 49 выпусков, и чек известной версии по счёту всей таблицы не сходился бы ни разу
// (каждый прогон пере-импортировал бы все файлы). Гейт — SELECT DISTINCT release_id по своей
// таблице: отвечает на тот же вопрос («этот выпуск уже импортирован?») одним запросом и не
// делает ни одного HEAD на повторном прогоне (спека §3.5, §5.2).
//
// У cbr_mt_forecast — то же решение по той же причине: артефакт — набор выпусков (4 в год),
// и чек известной версии по счёту всей таблицы не сходился бы ни разу. Гейт — SELECT DISTINCT
// release_date: известность выпуска — его дата публикации, а не имя файла (в имени дата есть,
// но <id> файла меняется от выпуска к выпуску, спека §3.4). Следствие названо в спеке §3.6:
// подмена файла под тем же release_date гейтом не отслеживается (лечение — удалить строки
// выпуска и повторить прогон), а «выпуск пропал из таблицы» самолечится.
//
// У cbr_form101 объявление тоже есть, но Import() свой (RAR → DBF, прецедент cbr_dip_bulletin),
// и no-op держит гейт по max(report_date): форма 101 выходит раз в месяц при ежедневном шаге,
// а известность выпуска — его отчётная дата из имени файла 101-YYYYMMDD.rar. Шаблонный цикл
// здесь не вызывается вовсе, поэтому объявление участвует только как points версии и как
// предмет сверки с реестром (тест-энфорсер).
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
	{cbrIntRateLoansImporter, "cbr_int_rate_loans", "SELECT count() FROM cbr_int_rate_loans FINAL"},
	{cbrCreditM2xImporter, "cbr_credit_m2x", "SELECT count() FROM cbr_credit_m2x FINAL"},
	{cbrIndicatorsCpdImporter, "cbr_indicators_cpd", "SELECT count() FROM cbr_indicators_cpd FINAL"},
	{cbrRuoniaImporter, "cbr_ruania", ""},
	{mpSurveyImporter, "macro_series", "SELECT count() FROM macro_series FINAL WHERE source = 'cbr_survey'"},
	{moBrImporter, "macro_series", "SELECT count() FROM macro_series FINAL WHERE source = 'cbr_mo_br'"},
	{mpSummaryImporter, "cbr_mp_summary", ""},
	{cbrBopEstImporter, "cbr_bop_est", "SELECT count() FROM cbr_bop_est FINAL"},
	{cbrBopStandardImporter, "cbr_bop_standard", "SELECT count() FROM cbr_bop_standard FINAL"},
	{cbrDipBulletinImporter, "cbr_dip_bulletin", "SELECT count() FROM cbr_dip_bulletin FINAL"},
	{cbrMtForecastImporter, "cbr_mt_forecast", "SELECT count() FROM cbr_mt_forecast FINAL"},
	{cbrForm101Importer, "cbr_form101", "SELECT count() FROM cbr_form101 FINAL"},
	{cbrZcycImporter, "cbr_zcyc", ""},
	{cbrCcybImporter, "cbr_ccyb_indicators", "SELECT count() FROM cbr_ccyb_indicators FINAL"},
	{cbrRiskWeightAddonsImporter, "cbr_risk_weight_addons", "SELECT count() FROM cbr_risk_weight_addons FINAL"},
	{cbrMPLImporter, "cbr_mpl", "SELECT count() FROM cbr_mpl FINAL"},
}

// init — единственная регистрация импортёров ЦБ в chimport.Stats. Порядок
// append совпадает с порядком реестра; main.go запускает всех (или фильтрует по
// CLICKHOUSE_IMPORT_STAT=Name()).
func init() {
	for _, imp := range cbrImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
