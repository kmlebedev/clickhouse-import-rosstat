package weeklysa

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// weeklySAView — витрина МCP-агента над производной таблицей. Длинная форма:
// одна строка на (source, series, date); source/series совпадают с
// v_series_catalog, поэтому описание ряда ищется по тому же ключу. Витрину
// публикует импортёр (и на no-op-пути), отдельный шаг не нужен.
var weeklySAView = util.View{
	Name:   "v_weekly_cpi_sa",
	Tables: []string{weeklySATable},
	Select: `SELECT 'weekly_cpi_sa' AS source,
       'weekly_cpi:sa_index' AS series,
       date,
       toNullable(toFloat64(sa_index)) AS value,
       'индекс (100 = без изменения за 4 недели)' AS unit,
       'M' AS frequency
FROM weekly_cpi_sa FINAL
UNION ALL
SELECT 'weekly_cpi_sa', 'weekly_cpi:sa_4w', date, toNullable(toFloat64(sa_4w)), '% за 4 недели', 'M'
FROM weekly_cpi_sa FINAL
UNION ALL
SELECT 'weekly_cpi_sa', 'weekly_cpi:saar', date, toNullable(toFloat64(saar)), '% годовых', 'M'
FROM weekly_cpi_sa FINAL`,
	Comment: "Недельный SA-агрегат ИПЦ (производный): 4-нед. индекс, темп за 4 недели и SAAR по официальному недельному агрегату Росстата (бюллетени 2009–2021 + BI 2019+), X-13ARIMA-SEATS seats_log_cal. Одна строка на (source, series, date); source/series совпадают с v_series_catalog — описание, единицы и оговорки читать там. Ряд индикативный, не официальный SA ЦБ; 2022+ alone в research не значим. Сырая weekly_cpi_sa агенту не выдана; витрина читает её как DEFINER.",
	Columns: map[string]string{
		"source":    "Источник: всегда weekly_cpi_sa — производный ряд; совпадает с source в v_series_catalog",
		"series":    "Имя ряда из v_series_catalog: weekly_cpi:<sa_index|sa_4w|saar>",
		"date":      "Последний день отчётного месяца (включаются только полные месяцы)",
		"value":     "Значение ряда: индекс 100 = без изменения за 4 недели, % за 4 недели или % годовых — см. unit и v_series_catalog",
		"unit":      "Единица значения: индекс или %; точная формулировка — в v_series_catalog",
		"frequency": "Частота: M — месяц (ряд построен на месячной сетке, источник — недельный)",
	},
}
