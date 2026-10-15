package cbr

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// Каталог рядов платёжного баланса РФ (стандарт §10.1): секция cbr_bop_est — оценка ключевых
// агрегатов (cbr/bop_est.go). Источник каталога равен имени таблицы (спека §3.7), а не
// литералу 'cbr': в общей витрине v_cbr_macro источник — литерал 'cbr', и имена рядов двух
// выгрузок ЦБ («Счет текущих операций») в нём бы столкнулись. Секцию cbr_bop_standard
// (34 ряда: те же 17 агрегатов по листам «Кварталы» и «Годы») добавляет следующая задача пункта
// своим объявлением рядом с этим.

// cbrBopEstSeriesMeta — каталог оценки ключевых агрегатов: 17 агрегатов × 3 вида периода = 51
// ряд, в порядке агрегатов листа и видов в cbrBopEstKinds — том же, в каком имена собирает
// разбор (cbrBopEstName). Каталог и разбор не могут разойтись ни по составу рядов, ни по их
// порядку, и двусторонний контракт проверяется тестом (стандарт §11.4).
var cbrBopEstSeriesMeta = cbrBopEstMeta()

// cbrBopEstOrigin — источник рядов: файл и лист в форме origin каталога. Имя листа в файле несёт
// дату выпуска («Оценка ПБ (14.09.2026)») и меняется с ним, поэтому origin называет префикс
// листа и объясняет дату; подпись строки источника (и блок для повторяющихся подписей)
// дописывается в цикле.
const cbrBopEstOrigin = "cbr.ru/vfs/statistics/credit_statistics/bop/bop_est.xlsx, " +
	"лист «Оценка ПБ …» (дата выпуска в имени листа)"

// cbrBopEstKindDates — привязка даты точки по виду периода (спека §3.4), в description ряда:
// у месяца и квартала — конец периода, у накопленного итога — конец последнего покрытого месяца.
var cbrBopEstKindDates = map[string]string{
	cbrBopKindMonth:      "Дата точки — последний день отчётного месяца: сопоставлять по toStartOfMonth(date).",
	cbrBopKindQuarter:    "Дата точки — последний день отчётного квартала: сопоставлять по toStartOfQuarter(date).",
	cbrBopKindCumulative: "Накопленный итог с начала года: значение накапливается внутри года (в декабре равно году), дата точки — последний день последнего месяца итога («январь - июль 2026» → 2026-07-31), сопоставлять по toStartOfMonth(date).",
}

// cbrBopEstKindFrequency — частота ряда в каталоге: у накопленного итога — месячная (M) с
// объяснением в описании, а не годовая: точки приходят каждый месяц (стандарт §10.1, спека §3.7).
var cbrBopEstKindFrequency = map[string]string{
	cbrBopKindMonth:      "M",
	cbrBopKindQuarter:    "Q",
	cbrBopKindCumulative: "M",
}

// cbrBopEstDescriptionTail — общий хвост description всех 51 ряда: единицы, знак, ноль и ревизия
// оценки. Отдельным текстом, а не частью Measures: четыре факта одинаковы у всех рядов листа, и
// их правка — одна, а не 51 (описание каждого агрегата задано в cbrBopEstAggregates).
const cbrBopEstDescriptionTail = " Единица — млрд долларов США. Знак — по методологии " +
	"платёжного баланса: положительное значение — превышение поступлений над выплатами (или " +
	"чистый прирост требований), отрицательное — обратное; отрицательные значения в файле есть. " +
	"Ноль в источнике — настоящее значение (сальдо равно нулю), а не «данных не имеется»: " +
	"точкой не становится только пустая ячейка, и в текущем выпуске таких нет. Значения — " +
	"оценка ЦБ (подпись колонки содержит «(оценка)»): следующим выпуском ЦБ их ревизует, а " +
	"импортёр перезаписывает точку по ключу (name, date), поэтому историю ревизий хранит только " +
	"винтажный архив файла."

// cbrBopEstMeta собирает 51 запись каталога из объявлений кода — агрегатов листа
// (cbrBopEstAggregates) и видов периода (cbrBopEstKinds): тот же порядок, что у разбора.
func cbrBopEstMeta() []util.SeriesMeta {
	meta := make([]util.SeriesMeta, 0, len(cbrBopEstAggregates)*len(cbrBopEstKinds))
	for _, agg := range cbrBopEstAggregates {
		for _, kind := range cbrBopEstKinds {
			name := cbrBopEstName(agg.Code, kind)
			origin := cbrBopEstOrigin + ", строка «" + agg.Label + "»"
			if agg.Block != "" {
				origin += " блока «" + agg.Block + "»"
			}
			meta = append(meta, util.SeriesMeta{
				Source:      cbrBopEstTable,
				Series:      name,
				Title:       name,
				Unit:        cbrBopEstUnit,
				Frequency:   cbrBopEstKindFrequency[kind],
				Origin:      origin,
				Description: agg.Measures + " " + cbrBopEstKindDates[kind] + cbrBopEstDescriptionTail,
			})
		}
	}
	return meta
}
