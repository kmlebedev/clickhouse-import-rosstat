package rosstat

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

var census2020Meta = []util.SeriesMeta{{
	Source: census2020Table, Series: "population", Title: "Население по полу и возрасту, ВПН-2020 (1 октября 2021)", Unit: "человек", Frequency: "irregular",
	Origin:      census2020URL + "; Том2 таблица2; counts columns B/C/D",
	Description: "Перепись с референсной датой 2021-10-01, несмотря на имя ВПН-2020. 18 непересекающихся возрастных групп 0–4…85+ и отдельный all; обе группы пола и итог both перекрываются. Поселение total/urban/rural; Москва и СПб только urban, отсутствующие срезы не заполнены нулями. РФ/ФО/субъекты и области с/без АО перекрываются: не суммировать все территории. Это не текущая годовая оценка, не нормативный трудоспособный возраст, не месячная рабочая сила. Муниципалитетов нет; loaded_at не дата публикации, исторический PIT не доказан.",
}}
var census2020View = util.View{
	Name: "v_rosstat_census_2020", Tables: []string{census2020Table},
	Select:  "SELECT 'rosstat_census_2020' AS source, 'population' AS series, territory, territory_kind, settlement, sex, age_group, reference_date AS date, value, loaded_at FROM rosstat_census_2020 FINAL",
	Comment: "Перепись ВПН-2020 на 2021-10-01: люди, не текущая демография. Отдельные итоги и вложенные территории не суммировать. Муниципального уровня нет. Историческая публикационная доступность не доказана.",
	Columns: map[string]string{
		"source": "Источник rosstat_census_2020", "series": "population — человек, frequency irregular; читать v_series_catalog",
		"territory":      "Название территории по книге, нормализованы только пробелы",
		"territory_kind": "RF — Россия; FD — округ; subject — субъект без составного итога; composite — область включая АО; exclAO — область без АО. Эти строки перекрываются",
		"settlement":     "total/urban/rural; Москва и Санкт-Петербург только urban, отсутствие не равно нулю",
		"sex":            "both/male/female; both=male+female, не суммировать три значения",
		"age_group":      "all либо 18 пятилетних групп 0-4…80-84/85+; all перекрывает группы. Не нормативный трудоспособный возраст",
		"date":           "Референсная дата переписи 2021-10-01; не год названия и не дата публикации",
		"value":          "Число людей, точное неотрицательное целое",
		"loaded_at":      "Время загрузки, не published_at; не использовать как историческую доступность",
	},
}
