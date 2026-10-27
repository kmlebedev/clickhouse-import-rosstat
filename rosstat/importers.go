package rosstat

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// rosstatImporter — запись реестра импортёров пакета: сам импортёр, таблица, в которую
// он пишет, и запрос количественного чека версии (префлайта). Регистрируется сам
// импортёр пакета — значение или указатель на его структуру, — а не отдельный
// безымянный тип-обёртка: все одиннадцать импортёров объявлены структурами со встроенным
// util.ClickHouseImport, поэтому Name() и Import() (цикл «DDL → префлайт → точки →
// производные → версия») промотированы из шаблона, а публикация производных (каталог
// рядов и витрина v_rosstat_macro — поле AfterImport; у rosstat_okved2 — отдельная
// витрина v_rosstat_okved2) идёт через шаблон; свой Import()
// оставляет тот, кому адрес артефакта нужно взять со страницы (rosstat_ipc_weeks,
// rosstat_ppi_ved, rosstat_ppi_product, rosstat_ppi_weights), у кого артефакт — не один
// файл, а набор выпусков (rosstat_ipc_weeks_bulletins), кому нужен явный отказ от restore
// при вычисляемом URL (rosstat_okved2, rosstat_ppi_product, rosstat_ppi_weights), либо
// кому export-URL выдаёт RPC и меняется на каждый прогон (rosstat_bi_cpi_weekly). Полей
// table и countPoints не видит никто, кроме теста
// TestRosstatImportersPrefixedAndRegistered: по ним проверяется, что импортёр не занял
// таблицу вне пространства rosstat_ и что объявленный в реестре префлайт совпадает с
// объявлением самого импортёра (Publication()).
type rosstatImporter struct {
	chimport.ImportStat
	table string
	// countPoints — литерал PublicationCountPoints импортёра ("" — префлайта нет).
	// Литерал, а не метод Publication(): именно расхождение этих двух источников
	// значения и обязан ловить тест реестра.
	countPoints string
}

// rosstatImporters — единственная точка регистрации импортёров Росстата: персональных
// init()-append по файлам импортёров больше нет. Порядок повторяет dagu/rosstat.yaml;
// в chimport.Stats порядок значения не имеет.
//
// Таблицы записаны литералами намеренно. Ссылка на константу импортёра
// (rosstatIPCMesTable) сделала бы проверку префикса тавтологичной: тест сравнивал бы
// константу с самой собой, тогда как литерал падает ровно на переименовании таблицы в
// имя вне rosstat_-пространства — то есть на правке, которая увела бы витрины
// v_rosstat_macro и v_model_inputs (вход контура золота, грант агенту) к чужим данным.
// Литералы сверены с константами кода при переносе регистрации.
//
// rosstat_ipc_weeks и rosstat_bi_cpi_weekly — две записи с пустым countPoints, и
// сознательно. У rosstat_ipc_weeks файл Nedel_ipc.xlsx перезаписывается по тому же URL в
// недельном темпе, равном темпу шага расписания, поэтому no-op недостижим, а HEAD был бы
// чистыми накладными расходами (спека §3.7, стандарт §9.3). У rosstat_bi_cpi_weekly
// export-URL — новый tmp/exp<uuid> на каждый POST, поэтому шаблонного HEAD-префлайта нет;
// версию содержимого импортёр считает сам (SHA-256 разобранных точек) и пишет её в
// source_publications, а повтор становится no-op-ом с «Imported 0 rows» (спека §3.2).
// Поля PublicationCountPoints/PublicationSource у обоих не заполнены, поэтому
// Publication() даёт нулевое объявление — ровно то, что сверяет пустая колонка реестра.
//
// Колонка countPoints — запрос количественного чека версии у импортёров, у которых
// точка контроля публикации объявлена (спека §3.5); пустая строка — префлайта нет.
// Литералы и здесь: запрос — часть утверждения «эти точки мои» (FINAL обязателен, иначе
// count() считал бы несмёрженные части и менялся бы от прогона к прогону), и тест реестра
// сверяет их с объявлением импортёра (Publication().CountPoints) — двумя независимыми
// записями одного утверждения. Незамеченная правка запроса в объявлении оставила бы
// реестр описывающим другой чек, а забытое объявление молча выключило бы префлайт.
var rosstatImporters = []rosstatImporter{
	{ipcMesImporter, "rosstat_ipc_mes", "SELECT count() FROM rosstat_ipc_mes FINAL"},
	{ipcWeeksImporter, "rosstat_ipc_weeks", ""},
	{salariesMesImporter, "rosstat_salaries_mes", "SELECT count() FROM rosstat_salaries_mes FINAL"},
	{vvpKvartalImporter, "rosstat_vvp_kvartal", "SELECT count() FROM rosstat_vvp_kvartal FINAL"},
	{labourForceImporter, "rosstat_labour_force_mes", "SELECT count() FROM rosstat_labour_force_mes FINAL"},
	{ipcWeeksBulletinsImporter, "rosstat_ipc_weeks_bulletins", "SELECT count() FROM rosstat_ipc_weeks_bulletins FINAL"},
	{rosstatOkved2Importer, "rosstat_okved2", "SELECT count() FROM rosstat_okved2 FINAL"},
	{ppiVedImporter, "rosstat_ppi_ved", "SELECT count() FROM rosstat_ppi_ved FINAL"},
	{biCpiWeeklyImporter, "rosstat_bi_cpi_weekly", ""},
	{ppiProductImporter, "rosstat_ppi_product", "SELECT count() FROM rosstat_ppi_product FINAL"},
	{ppiWeightsImporter, "rosstat_ppi_weights", "SELECT count() FROM rosstat_ppi_weights FINAL"},
}

// init — единственная регистрация импортёров Росстата в chimport.Stats. Порядок
// append совпадает с порядком реестра; main.go запускает всех (или фильтрует по
// CLICKHOUSE_IMPORT_STAT=Name()).
func init() {
	for _, imp := range rosstatImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
