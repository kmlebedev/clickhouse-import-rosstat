package cbr

import (
	"fmt"

	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// cbrCcybSeriesMeta — каталог 24 рядов индикаторов кредитного цикла. Имена,
// номера и группы объявлены кодом (ccybSeriesSpecs); каталог собирается из того
// же реестра, поэтому новый ряд не может попасть в витрину без описания.
var cbrCcybSeriesMeta = ccybMetaFromSpecs(ccybSeriesSpecs)

func ccybMetaFromSpecs(specs []ccybSeriesSpec) []util.SeriesMeta {
	meta := make([]util.SeriesMeta, 0, len(specs))
	for _, s := range specs {
		meta = append(meta, util.SeriesMeta{
			Source:    cbrCcybTable,
			Series:    s.name,
			Title:     fmt.Sprintf("АЦН-индикатор %d: %s", s.no, s.title),
			Unit:      s.unit,
			Frequency: "M",
			Origin: "cbr.ru/vfs/analytics/fin_stab/indicators/ccyb_indicators_time_series.xlsx, " +
				"лист «Ряды данных», строка «" + s.label + "»",
			Description: ccybValueDescription + " Группа источника: " + s.group + ".",
		})
	}
	return meta
}
