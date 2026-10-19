package cbr

import (
	"fmt"
	"strings"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// cbrZcycLabel — запись тенора как в источнике: десятичный разделитель — запятая
// (0.25 → 0,25, 10 → 10).
func cbrZcycLabel(tenor string) string {
	return strings.ReplaceAll(tenor, ".", ",")
}

// cbrZcycSeriesMeta — каталог 12 рядов КБД ЦБ, по одному на канонический тенор.
// Имена рядов объявлены кодом, а не извлекаются из заголовка таблицы (стандарт §6.8).
var cbrZcycSeriesMeta = cbrZcycMetaFromTenors(cbrZcycTenors)

// cbrZcycMetaFromTenors собирает описания рядов из списка теноров: Series — сам
// тенор в канонической записи, Origin называет таблицу и колонку источника.
func cbrZcycMetaFromTenors(tenors []string) []util.SeriesMeta {
	meta := make([]util.SeriesMeta, 0, len(tenors))
	for _, tenor := range tenors {
		label := cbrZcycLabel(tenor)
		meta = append(meta, util.SeriesMeta{
			Source:    cbrSource,
			Series:    tenor,
			Title:     "КБД ЦБ: доходность ОФЗ, тенор " + label,
			Unit:      "% годовых",
			Frequency: "D",
			Origin: "cbr.ru/hd_base/zcyc_params, таблица «Кривая бескупонной доходности " +
				"государственных облигаций», колонка «Срок до погашения, лет» = " + label,
			Description: fmt.Sprintf("Доходность ОФЗ по кривой бескупонной доходности ЦБ РФ "+
				"для тенора %s: уровень на торговый день, а не темп (изменение за период — "+
				"разница в процентных пунктах). История с 04.01.2003, одна точка на торговый "+
				"день; пропуск дня означает, что публикации не было. Пустая ячейка таблицы "+
				"источником точки не является и нулём не считается. Сравнение с G-curve "+
				"МосБиржи — витрина v_ofz_curve_compare; спреды к ключевой ставке и реальная "+
				"доходность — витрина v_ofz_signal. Имена рядов объявлены кодом, а не "+
				"извлекаются из заголовка таблицы (стандарт §6.8).", label),
		})
	}
	return meta
}
