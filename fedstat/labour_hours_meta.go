package fedstat

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

var labourHoursMeta = []util.SeriesMeta{
	{Source: fedstatLabourHoursTable, Series: "59749", Title: "Занятые по фактическим недельным часам основной работы", Unit: "тысяча человек", Frequency: "Y", Origin: "ЕМИСС 59749, полный XLS, обследование рабочей силы", Description: "Годовое распределение занятых по полу/возрасту и фактическим часам основной работы. Значение — люди, не часы. Включает временно отсутствовавших (0 часов). Всего и его бины не суммировать вместе. Дата 1 января обозначает год; region сохраняет территориальное определение. Исторический PIT не подтверждён."},
	{Source: fedstatLabourHoursTable, Series: "59751", Title: "Средние фактические недельные часы основной работы", Unit: "час", Frequency: "Y", Origin: "ЕМИСС 59751, полный XLS, обследование рабочей силы", Description: "Годовая частота, средние недельные часы на всех занятых данной группы, включая временно отсутствовавших. Не оплаченные/обычные часы и не все работы. Не умножать на текущую месячную занятость без согласования охвата. Дата 1 января обозначает год; исторический PIT не подтверждён."},
	{Source: fedstatLabourHoursTable, Series: "60463", Title: "Годовые фактические трудозатраты всех работ", Unit: "тысяча человеко-часов", Frequency: "Y", Origin: "ЕМИСС 60463, полный XLS, затраты труда в производстве товаров и услуг", Description: "Все работы, разрез отрасль/статус занятости; пола/возраста нет. Вложенные ОКВЭД и Всего не суммировать. 1 млрд человеко-часов = 1000000 единиц значения. Дата 1 января обозначает год; region — исходное территориальное определение. Исторический PIT не подтверждён."},
}
var labourHoursView = util.View{
	Name: "v_fedstat_labour_hours", Tables: []string{fedstatLabourHoursTable},
	Select:  "SELECT 'fedstat_labour_hours' AS source, series, region, age, sex, hours_band, activity, employment_status, unit, 'Y' AS frequency, date, value FROM fedstat_labour_hours FINAL",
	Comment: "Годовые часы/трудозатраты ЕМИСС. Три разных единицы и охвата: читать каталог. Пустые измерения неприменимы; совместной панели нет. Не суммировать вложенные категории; не исторический PIT.",
	Columns: map[string]string{
		"source": "Источник: fedstat_labour_hours", "series": "ID ЕМИСС 59749/59751/60463; определение в v_series_catalog",
		"region":            "Код и подпись территории источника; 643 и 643004.АГ не склеивать без проверки сопоставимости",
		"age":               "Исходная возрастная группа с кодом; пусто у 60463 — неприменимо",
		"sex":               "Код и подпись пола; пусто у 60463 — неприменимо",
		"hours_band":        "Группа фактических часов основной работы, только 59749; Всего включает остальные бины",
		"activity":          "ОКВЭД2 с кодом и подписью, только 60463; категории вложены, нельзя суммировать все строки",
		"employment_status": "Статус занятости с кодом, только 60463; Всего = по найму + не по найму",
		"unit":              "Тысяча человек / час / тысяча человеко-часов; не смешивать",
		"frequency":         "Y — годовая частота, в том числе для средних недельных часов",
		"date":              "1 января наблюдаемого года; не дата публикации",
		"value":             "Исходное значение. Ноль сохранён; отсутствующее наблюдение не записано. См. unit и каталог",
	},
}

func afterFedstatLabourHours(ctx context.Context, conn driver.Conn) error {
	if err := util.UpsertSeriesCatalog(ctx, conn, labourHoursMeta); err != nil {
		return err
	}
	_, err := util.CreateView(ctx, conn, labourHoursView)
	return err
}
