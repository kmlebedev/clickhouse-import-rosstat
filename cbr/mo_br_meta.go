package cbr

import (
	"fmt"
	"sort"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// moBrSource — источник рядов макроэкономического опроса Банка России (импортёр cbr_mo_br):
// в macro_series это source, в series_catalog — Source.
const moBrSource = "cbr_mo_br"

// moBrFrequency — частота выпуска волн опроса ЦБ (8 раз в год, полевая неделя раз в ~6 недель).
// Отличается от кодов частот M/Q/R: значения — прогнозы аналитиков по волнам, а не регулярный
// ряд, поэтому частота записана текстом и в series_catalog видна агенту как есть.
const moBrFrequency = "8/год (волны опроса)"

// moBrIndicator — дескриптор листа-показателя книги макроэкономического опроса ЦБ (full.xlsx):
// номер листа (Sheet), базовое имя ряда в macro_series (Base — §3.3 спеки), заголовок каталога
// (Title — показатель вместе с единицей измерения, как в mpSurveySeriesMeta) и единица (Unit).
// NoYears отмечает листы 16–17: там блок статистики занимает одну строку, а прогнозного года
// нет, поэтому имя ряда — <base>_<stat> без года.
type moBrIndicator struct {
	Sheet, Base, Title, Unit string
	NoYears                  bool
}

// moBrIndicators — реестр 17 показателей опроса, дословно по таблице §2 спеки
// (номер листа, показатель, единица). Незнакомый лист-показатель в книге — ошибка разбора:
// структура источника изменилась, реестр нужно дополнять кодом.
var moBrIndicators = []moBrIndicator{
	{Sheet: "1", Base: "cpi_dec", Title: "ИПЦ, % дек. к дек. пред. года", Unit: "% дек. к дек. пред. года"},
	{Sheet: "2", Base: "cpi_avg", Title: "ИПЦ, % к пред. году, в среднем за год", Unit: "% к пред. году, в среднем за год"},
	{Sheet: "3", Base: "key_rate_avg", Title: "Ключевая ставка, % годовых, в среднем за год (с учётом выходных дней)", Unit: "% годовых, в среднем за год (с учётом выходных дней)"},
	{Sheet: "4", Base: "gdp_yoy", Title: "ВВП, %, г/г", Unit: "%, г/г"},
	{Sheet: "5", Base: "unemp_avg", Title: "Уровень безработицы, %, в среднем за год", Unit: "%, в среднем за год"},
	{Sheet: "6", Base: "unemp_dec_nsa", Title: "Уровень безработицы, %, дек., без исключения сезонности", Unit: "%, дек., без исключения сезонности"},
	{Sheet: "7", Base: "wage_nominal", Title: "Номинальная заработная плата, %, г/г, в среднем за год", Unit: "%, г/г, в среднем за год"},
	{Sheet: "8", Base: "budget_balance", Title: "Баланс консолидированного бюджета, % ВВП за соответствующий год", Unit: "% ВВП за соответствующий год"},
	{Sheet: "9", Base: "exports", Title: "Экспорт товаров и услуг, млрд долл. США в год", Unit: "млрд долл. США в год"},
	{Sheet: "10", Base: "imports", Title: "Импорт товаров и услуг, млрд долл. США в год", Unit: "млрд долл. США в год"},
	{Sheet: "11", Base: "usd_rub", Title: "Курс USD/RUB, руб. за долл., в среднем за год", Unit: "руб. за долл., в среднем за год"},
	{Sheet: "12", Base: "oil_tax", Title: "Цена нефти для налогообложения, долл. США за баррель, в среднем за год", Unit: "долл. США за баррель, в среднем за год"},
	{Sheet: "13", Base: "brent", Title: "Цена на нефть марки Brent, долл. США за баррель, в среднем за год", Unit: "долл. США за баррель, в среднем за год"},
	{Sheet: "14", Base: "urals", Title: "Цена на нефть марки Urals, долл. США за баррель, в среднем за год", Unit: "долл. США за баррель, в среднем за год"},
	{Sheet: "15", Base: "cds_5y", Title: "CDS-спред по России, б.п., в среднем за год", Unit: "б.п., в среднем за год"},
	{Sheet: "16", Base: "neutral_rate", Title: "Нейтральная ключевая ставка, % годовых", Unit: "% годовых", NoYears: true},
	{Sheet: "17", Base: "gdp_long_term", Title: "Долгосрочный рост ВВП, %, г/г", Unit: "%, г/г", NoYears: true},
}

// moBrStatSuffix — метки блоков статистик колонки B → суффиксы имён рядов. В файле метки
// приходят с хвостовыми пробелами («Медиана »), поэтому парсер сопоставляет по trim; метки
// вне набора («Среднее», 3-й и 1-й квартили) молча пропускаются — они вне объёма (§2 спеки).
var moBrStatSuffix = map[string]string{
	"Медиана":         "median",
	"90-й процентиль": "p90",
	"10-й процентиль": "p10",
	"Мин":             "min",
	"Макс":            "max",
	"Всего ответов":   "answers",
}

// moBrStatLabel — каноническое имя блока статистики для Origin каталога и сообщений.
var moBrStatLabel = map[string]string{
	"median":  "Медиана",
	"p90":     "90-й процентиль",
	"p10":     "10-й процентиль",
	"min":     "Мин",
	"max":     "Макс",
	"answers": "Всего ответов",
}

// moBrStatDesc — расшифровка статистики для Description каталога. Шаблоны различаются по
// статистикам: агент должен видеть, что median/p10/p90/min/max — прогнозы аналитиков,
// а answers — число ответов, а не прогноз (§3.5, риск «число ответов — не прогноз»).
var moBrStatDesc = map[string]string{
	"median":  "медиана прогнозов аналитиков",
	"p10":     "10-й процентиль — нижняя граница центральной тенденции прогнозов",
	"p90":     "90-й процентиль — верхняя граница центральной тенденции прогнозов",
	"min":     "минимум прогнозов — нижняя граница всего диапазона ответов",
	"max":     "максимум прогнозов — верхняя граница всего диапазона ответов",
	"answers": "число ответов участников опроса по этому показателю (не прогноз)",
}

// moBrIndicatorBySheet — дескриптор листа-показателя по имени листа; ok=false, если лист
// не из реестра 17 (разбор такого листа обязан упасть).
func moBrIndicatorBySheet(sheet string) (moBrIndicator, bool) {
	for _, ind := range moBrIndicators {
		if ind.Sheet == sheet {
			return ind, true
		}
	}
	return moBrIndicator{}, false
}

// moBrStatLabels — метки блоков статистик в объёме, отсортированные (для сообщений об ошибке).
func moBrStatLabels() []string {
	out := make([]string, 0, len(moBrStatSuffix))
	for label := range moBrStatSuffix {
		out = append(out, label)
	}
	sort.Strings(out)
	return out
}

// moBrSeriesName — имя ряда в macro_series и series_catalog: <base>_<year>_<stat> для годовых
// показателей и <base>_<stat> для листов 16–17 (год — четыре цифры даты прогнозного года).
func moBrSeriesName(base, year, stat string) string {
	if year == "" {
		return base + "_" + stat
	}
	return base + "_" + year + "_" + stat
}

// moBrSeriesMeta — запись каталога series_catalog для комбинации (дескриптор, год, статистика).
// Description — шаблон «прогноз аналитиков: <title> на <year> год; <расшифровка статистики>»
// (для листов 16–17 без года), Origin — источник, файл, лист и блок статистики.
func moBrSeriesMeta(ind moBrIndicator, year, stat string) util.SeriesMeta {
	origin := fmt.Sprintf("ЦБ, макроэкономический опрос (full.xlsx, mo_br), лист «%s», блок «%s»",
		ind.Sheet, moBrStatLabel[stat])
	description := fmt.Sprintf("прогноз аналитиков: %s; %s", ind.Title, moBrStatDesc[stat])
	if year != "" {
		origin += fmt.Sprintf(", год %s", year)
		description = fmt.Sprintf("прогноз аналитиков: %s на %s год; %s", ind.Title, year, moBrStatDesc[stat])
	}
	return util.SeriesMeta{
		Source:      moBrSource,
		Series:      moBrSeriesName(ind.Base, year, stat),
		Title:       ind.Title,
		Unit:        ind.Unit,
		Frequency:   moBrFrequency,
		Origin:      origin,
		Description: description,
	}
}
