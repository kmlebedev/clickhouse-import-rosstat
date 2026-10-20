package fedstat

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// fedstatImporter — запись реестра: сам импортёр, литерал таблицы и литерал
// запроса количественного чека. countPoints пуст: точки контроля публикации нет
// (стандарт §9.3), no-op держит гейт по своей таблице; расхождение поймал бы
// тест-энфорсер.
type fedstatImporter struct {
	chimport.ImportStat
	table       string
	countPoints string
}

// fedstatImporters — единственная точка регистрации пакета. Таблица — литерал,
// а не ссылка на константу: ссылка сделала бы проверку префикса тавтологичной
// (стандарт §8.2).
var fedstatImporters = []fedstatImporter{
	{fedstatWeeklyPricesImporter, "fedstat_weekly_prices", ""},
}

func init() {
	for _, imp := range fedstatImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
