package bank

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// bankImporter — запись реестра импортёров пакета: сам импортёр, таблица, в которую
// он пишет, и запрос количественного чека версии (префлайта). Регистрируется сам
// импортёр пакета — значение или указатель на его структуру (шесть шаблонных и
// внутренний маппер &BankCompanyMetrics{}), — а не отдельный безымянный тип-обёртка:
// Name() и Import() принадлежат импортёру (у шаблонных промотированы из
// util.ClickHouseImport), а производные (каталог рядов, витрины) публикует шаблон
// util.ClickHouseImport через поле AfterImport. У bank_company_metrics собственный
// Import(): внешнего артефакта у него нет, он читает уже импортированные таблицы
// пакета. Полей table и countPoints не видит никто, кроме теста
// TestBankImportersNamedAndRegistered: по ним проверяется, что импортёр занимает
// таблицу утверждённого набора (О1) и что объявленный в реестре префлайт совпадает с
// объявлением самого импортёра (Publication()).
type bankImporter struct {
	chimport.ImportStat
	table string
	// countPoints — литерал PublicationCountPoints импортёра ("" — префлайта нет).
	// Литерал, а не метод Publication(): именно расхождение этих двух источников
	// значения и обязан ловить тест реестра.
	countPoints string
}

// bankImporters — единственная точка регистрации импортёров пакета (семь записей в
// одном месте; персональных init()-append по файлам больше нет). Порядок повторяет
// dagu/bank.yaml; в chimport.Stats порядок значения не имеет. tbank_group_ifrs идёт
// последним из импортёров и вне расписания (О2) — шага в dagu/bank.yaml у него нет;
// маппер bank_company_metrics — последняя запись и последний шаг расписания (спека §3.8).
//
// Таблицы записаны литералами намеренно: префикса пакета у банка нет (О1: имена шести
// таблиц утверждены владельцем как есть), поэтому ссылка на константу импортёра
// (sberCSITable) сделала бы проверку тавтологичной. Литерал падает ровно на
// переименовании таблицы в имя вне утверждённого набора — правке, которая увела бы
// витрину v_bank_macro к чужим данным; литералы сверены с константами кода при переносе
// регистрации.
//
// bank_company_metrics — седьмая запись с таблицей вне набора: маппер пишет
// курированные метрики Сбера и ВТБ в общую company_financials (её же ведут
// polyus/import.go и views/company.go). Name() отличается от имени таблицы, поэтому
// запись раздельная (прецедент — cbr_mp_survey и cbr_mo_br в macro_series).
//
// Колонка countPoints — запрос количественного чека версии (префлайт, спека §3.1).
// Три префлайта объявлены у sber_finansovie_rezultaty, rus_vtb_group_ifrs и
// domrf_mortgage; у остальных четырёх пустая строка осознанно: у недельных наборов
// Сбера HEAD отвечает 403 (спека §4.2), у tbank_group_ifrs артефакт — PDF и импорт
// падает до разбора (О2, О4), у bank_company_metrics внешнего артефакта нет. У
// rus_vtb_group_ifrs объявление есть, но no-op решает гейт пофайловых версий (О3), а не
// оно (объявление включает архив версий и строки source_publications — §9.6). Литералы
// и здесь: тест реестра сверяет запрос с объявлением импортёра
// (Publication().CountPoints) — двумя независимыми записями одного утверждения;
// незамеченная правка запроса в объявлении оставила бы реестр описывающим другой чек,
// а забытое объявление молча выключило бы префлайт.
var bankImporters = []bankImporter{
	{sberFinRezImporter, "sber_finansovie_rezultaty", "SELECT count() FROM sber_finansovie_rezultaty FINAL"},
	{sberCSIImporter, "sber_consumper_spending_index", ""},
	{sberCSIWeekImporter, "sber_izmenenie_trat", ""},
	{vtbImporter, "rus_vtb_group_ifrs", "SELECT count() FROM rus_vtb_group_ifrs FINAL"},
	{domrfImporter, "domrf_mortgage", "SELECT count() FROM domrf_mortgage FINAL"},
	{tbIfrsImporter, "tbank_group_ifrs", ""},
	{&BankCompanyMetrics{}, "company_financials", ""},
}

// init — единственная регистрация импортёров пакета в chimport.Stats. Порядок
// append совпадает с порядком реестра; main.go запускает всех (или фильтрует по
// CLICKHOUSE_IMPORT_STAT=Name()).
func init() {
	for _, imp := range bankImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
