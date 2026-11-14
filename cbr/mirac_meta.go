package cbr

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// cbrMiracMetaTenors — канонический порядок теноров в каталоге (как в форме ЦБ).
var cbrMiracMetaTenors = []string{"1d", "2-7d", "8-30d", "31-90d", "91-180d", "181d-1y"}

// cbrMiracMetaInstruments — инструменты и их смысл. MIACR — все московские банки,
// MIACR-IG — банки с высоким кредитным рейтингом, MIACR-B — со спекулятивным.
var cbrMiracMetaInstruments = []struct {
	name  string
	title string
}{
	{"MIACR", "MIACR — средневзвешенная фактическая ставка по межбанковским кредитам (все московские банки)"},
	{"MIACR-IG", "MIACR-IG — то же по банкам с высоким кредитным рейтингом (investment grade)"},
	{"MIACR-B", "MIACR-B — то же по банкам со спекулятивным кредитным рейтингом (B-grade)"},
}

// cbrMiracMetaTenorUnits — расшифровка срока в подписи ряда.
var cbrMiracMetaTenorUnits = map[string]string{
	"1d":      "1 день",
	"2-7d":    "от 2 до 7 дней",
	"8-30d":   "от 8 до 30 дней",
	"31-90d":  "от 31 до 90 дней",
	"91-180d": "от 91 до 180 дней",
	"181d-1y": "от 181 дня до 1 года",
}

// cbrMiracSeriesMeta — каталог 18 рядов (3 инструмента × 6 сроков). Ряды MIACR-IG и
// MIACR-B начинаются с 11.01.2010, MIACR — с 01.08.2000: до этих дат точек нет, это
// история источника, а не пропуск импорта.
var cbrMiracSeriesMeta = func() []util.SeriesMeta {
	meta := make([]util.SeriesMeta, 0, len(cbrMiracMetaInstruments)*len(cbrMiracMetaTenors))
	for _, instrument := range cbrMiracMetaInstruments {
		for _, tenor := range cbrMiracMetaTenors {
			meta = append(meta, util.SeriesMeta{
				Source:    "cbr_mirac",
				Series:    instrument.name + "/" + tenor,
				Title:     instrument.title + ", срок " + cbrMiracMetaTenorUnits[tenor],
				Unit:      "% годовых",
				Frequency: "D",
				Origin:    "cbr.ru/hd_base/mkr/mkr_base — таблица «" + instrument.name + "», колонка «" + cbrMiracMetaTenorUnits[tenor] + "»",
				Description: "Средневзвешенная фактическая ставка по рублёвым межбанковским кредитам на срок " +
					cbrMiracMetaTenorUnits[tenor] + ", % годовых. Пустая ячейка формы («—») в БД не пишется: прочерк — отсутствие значений за день, " +
					"а не нулевая ставка. Обороты по тем же сделкам (млн руб.) в ряд не входят — это отдельные таблицы формы.",
			})
		}
	}
	return meta
}()
