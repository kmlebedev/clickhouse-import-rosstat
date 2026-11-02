package minec

import "github.com/kmlebedev/clickhouse-import-rosstat/chimport"

// minecImporter — запись реестра: сам импортёр, таблица, в которую он пишет, и
// запрос количественного чека версии. table и countPoints видит только тест-
// энфорсер: по ним проверяется, что импортёр не занял чужую таблицу и не объявил
// префлайт, которого у него нет (стандарт §8.2).
type minecImporter struct {
	chimport.ImportStat
	table       string
	countPoints string
}

// minecImporters — единственная точка регистрации пакета. Таблица записана
// литералом намеренно: ссылка на константу сделала бы проверку тавтологичной.
// macro_series — общая таблица, поэтому запись с Name() != table; префлайта нет:
// артефакт — набор страниц, известность считает гейт по max(date) (стандарт §9.3).
var minecImporters = []minecImporter{
	{minecUralsImporter, "macro_series", ""},
}

func init() {
	for _, imp := range minecImporters {
		chimport.Stats = append(chimport.Stats, imp)
	}
}
