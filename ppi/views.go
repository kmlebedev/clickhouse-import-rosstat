package ppi

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// ppiView — витрина МCP-агента над четырьмя таблицами ИЦП: сырыми рядами по
// ВЭД/товарам/весам и производным SA-слоем. Длинная форма: одна строка на
// (source, series, date); source/series совпадают с series_catalog, поэтому
// описание ряда ищется в v_series_catalog по тому же ключу.
//
// Витрину публикует импортёр ppi_sa: он последний писатель, к его прогону все
// четыре таблицы уже существуют. util.CreateView пропускает витрину, пока нет
// хотя бы одной таблицы, поэтому отдельный шаг/импортёр не нужен.
var ppiView = util.View{
	Name: "v_rosstat_ppi",
	Tables: []string{
		"rosstat_ppi_ved",
		"rosstat_ppi_product",
		"rosstat_ppi_weights",
		ppiSATable,
	},
	Select: `SELECT 'rosstat' AS source,
       concat('ИЦП по ВЭД, РФ, ', index_type) AS series,
       date,
       toNullable(toFloat64(value)) AS value,
       multiIf(index_type = 'm/m', 'индекс, % к предыдущему месяцу (100 = без изменения)',
               index_type = 'ytd', 'индекс, % к декабрю предыдущего года (100 = без изменения)',
               index_type = 'y/y', 'индекс, % к соответствующему месяцу предыдущего года (100 = без изменения)',
               index_type = 'period', 'индекс, % к соответствующему периоду предыдущего года (100 = без изменения)',
               'индекс, % (100 = без изменения)') AS unit,
       'M' AS frequency
FROM rosstat_ppi_ved FINAL
UNION ALL
SELECT 'rosstat',
       concat('okpd2:', okpd2, ':', index_type),
       date,
       toNullable(toFloat64(value)),
       multiIf(index_type = 'm/m', 'индекс, % к предыдущему месяцу (100 = без изменения)',
               index_type = 'ytd', 'индекс, % к декабрю предыдущего года (100 = без изменения)',
               index_type = 'y/y', 'индекс, % к соответствующему месяцу предыдущего года (100 = без изменения)',
               index_type = 'period', 'индекс, % к соответствующему периоду предыдущего года (100 = без изменения)',
               'индекс, % (100 = без изменения)'),
       'M'
FROM rosstat_ppi_product FINAL
UNION ALL
SELECT 'rosstat',
       concat('ved-weight:', code, ':', weight_type),
       makeDate(year, 1, 1),
       toNullable(toFloat64(value)),
       '% (доля в структуре весов ИЦП)',
       'A'
FROM rosstat_ppi_weights FINAL
UNION ALL
SELECT 'ppi_sa', concat('ppi_sa:', series, ':sa'), date, toNullable(sa),
       'индекс (100 = первый месяц ряда)', 'M'
FROM ppi_sa FINAL
UNION ALL
SELECT 'ppi_sa', concat('ppi_sa:', series, ':mm_sa'), date, mm_sa, '%', 'M'
FROM ppi_sa FINAL
UNION ALL
SELECT 'ppi_sa', concat('ppi_sa:', series, ':saar'), date, saar, '%', 'M'
FROM ppi_sa FINAL
UNION ALL
SELECT 'ppi_sa', concat('ppi_sa:', series, ':saar3'), date, saar3, '%', 'M'
FROM ppi_sa FINAL`,
	Comment: "ИЦП Росстата в long-форме: сырые ряды по ВЭД, товарам/ОКПД2 и веса (source='rosstat') плюс производный SA-слой ppi_sa (source='ppi_sa', 5 рядов × 4 метрики). Одна строка на (source, series, date); source/series совпадают с v_series_catalog — описание, единицы и частоту читать там. У ppi_sa value NULL на краях: mm_sa/saar первого месяца и saar3 первых двух. Основная масса строк — товары/ОКПД2 (сотни тысяч): фильтруйте по source/series. Сырые rosstat_ppi_ved, rosstat_ppi_product, rosstat_ppi_weights и ppi_sa агенту не выданы; витрина читает их как DEFINER.",
	Columns: map[string]string{
		"source":    "Источник: rosstat — сырые ИЦП/веса; ppi_sa — производный SA-слой; совпадает с source в v_series_catalog",
		"series":    "Имя ряда из v_series_catalog: 'ИЦП по ВЭД, РФ, <m/m|ytd|y/y|period>', 'okpd2:<код>:<тип>', 'ved-weight:<код>:<база>', 'ppi_sa:<total|food|nonfood|all|core>:<sa|mm_sa|saar|saar3>'",
		"date":      "Дата точки: у месячных рядов — последний день отчётного месяца; у весов — 1 января года",
		"value":     "Значение (Nullable): у ppi_sa NULL на краях (mm_sa/saar первого месяца, saar3 первых двух); единицы — в v_series_catalog",
		"unit":      "Единица значения: индекс/% для ИЦП, % для весов; точная формулировка — в v_series_catalog",
		"frequency": "Частота: M — месяц, A — год (веса)",
	},
}
