package rosstat

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// rosstatImporter — запись реестра импортёров пакета: сам импортёр, таблица, в которую
// он пишет, и запрос количественного чека версии (префлайта). Регистрируется конкретный
// тип пакета, а не обёртка: Name() и Import() — его собственные, производные (каталог
// рядов, витрину v_rosstat_macro) публикует шаблон util.ClickHouseImport через поле
// AfterImport. Полей table и countPoints не видит никто, кроме теста
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
// rosstat_ipc_weeks — единственная запись с пустым countPoints, и сознательно: файл
// Nedel_ipc.xlsx перезаписывается по тому же URL в недельном темпе, равном темпу шага
// расписания, поэтому no-op недостижим, а HEAD был бы чистыми накладными расходами
// (спека §3.7). Порт импортёра на util.ClickHouseImport — отдельная задача; до неё он
// регистрируется как есть, со своим Name() и Import().
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
}

// init — единственная регистрация импортёров Росстата в chimport.Stats. Порядок
// append совпадает с порядком реестра; main.go запускает всех (или фильтрует по
// CLICKHOUSE_IMPORT_STAT=Name()).
func init() {
	for _, imp := range rosstatImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
