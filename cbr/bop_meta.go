package cbr

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// Каталог рядов платёжного баланса РФ (стандарт §10.1): секция cbr_bop_est — оценка ключевых
// агрегатов (cbr/bop_est.go), секция cbr_bop_standard — стандартная презентация
// (cbr/bop_standard.go). Источник каталога равен имени таблицы (спека §3.7), а не литералу 'cbr':
// в общей витрине v_cbr_macro источник — литерал 'cbr', и имена рядов двух выгрузок ЦБ
// («Счет текущих операций») в нём бы столкнулись.

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

// cbrBopStandardSeriesMeta — каталог стандартной презентации: 17 верхних агрегатов × 2 листа = 34
// ряда, в порядке листов cbrBopStandardSheets и агрегатов листа — том же, в каком точки пишет
// разбор (cbrBopStandardReadSheet). Каталог и разбор не могут разойтись ни по составу рядов, ни
// по их порядку, и двусторонний контракт проверяется тестом (стандарт §11.4).
var cbrBopStandardSeriesMeta = cbrBopStandardMeta()

// cbrBopStandardOrigin — источник рядов: файл и лист в форме origin каталога. Имена листов у этой
// выгрузки постоянны («Кварталы», «Годы»), поэтому origin называет лист целиком, а подпись строки
// источника (и блок для повторяющихся подписей) дописывается в цикле. Опечатка «standart» —
// в имени файла источника, и она названа тут и в описании ряда: агент, ищущий файл на сайте ЦБ,
// иначе принял бы имя из origin за ошибку выгрузки.
const cbrBopStandardOrigin = "cbr.ru/vfs/statistics/credit_statistics/bop/bal_of_payments_standart.xlsx " +
	"(«standart» — опечатка в имени файла ЦБ), лист "

// cbrBopStandardKindDates — привязка даты точки по виду периода (спека §3.4), в description ряда:
// у квартала — последний день квартала, у года — 31 декабря.
var cbrBopStandardKindDates = map[string]string{
	cbrBopKindQuarter: "Дата точки — последний день отчётного квартала: сопоставлять по toStartOfQuarter(date).",
	cbrBopKindYear:    "Дата точки — 31 декабря отчётного года: сопоставлять по toYear(date).",
}

// cbrBopStandardKindFrequency — частота ряда в каталоге: лист «Кварталы» — Q, лист «Годы» — A
// (стандарт §10.1, спека §3.7).
var cbrBopStandardKindFrequency = map[string]string{
	cbrBopKindQuarter: "Q",
	cbrBopKindYear:    "A",
}

// cbrBopStandardDescriptionTail — общий хвост description всех 34 рядов: единицы, знак, ноль,
// методология 2022 года и ревизия очередным выпуском. Отдельным текстом, а не частью Measures:
// эти факты одинаковы у всех рядов выгрузки, и их правка — одна, а не 34 (описание каждого
// агрегата задано в cbrBopStandardAggregates). Единицы здесь другие, чем у оценки (cbr_bop_est):
// млн против млрд долларов США, — и об этом сказано явно, потому что имена рядов у двух выгрузок
// совпадают до вида периода.
const cbrBopStandardDescriptionTail = " Единица — млн долларов США (в 1000 раз мельче, чем у " +
	"оценки cbr_bop_est, где те же агрегаты идут в млрд долларов США). Знак — по методологии " +
	"платёжного баланса: положительное значение — превышение поступлений над выплатами (или " +
	"чистый прирост требований), отрицательное — обратное; отрицательные значения в файле есть. " +
	"Ноль в источнике — настоящее значение (сальдо равно нулю), а не «данных не имеется»: " +
	"точкой не становится только пустая ячейка. Выгрузка — стандартная презентация по методологии " +
	"платёжного баланса 2022 года; рядом ЦБ держит файл bal_of_payments_standart_before_2022.xlsx " +
	"с прежней методологией (в этом импортёре он не разбирается, и сопоставление значений до " +
	"2022 года с ним требует пересчёта). Выпуск квартальный, и ЦБ пересчитывает историю: очередной " +
	"выпуск ревизует прошлые точки, а импортёр перезаписывает точку по ключу (name, date), поэтому " +
	"историю ревизий хранит только винтажный архив файла."

// cbrBopStandardMeta собирает 34 записи каталога из объявлений кода — листов выгрузки
// (cbrBopStandardSheets) и агрегатов листа (cbrBopStandardAggregates): тот же порядок, что у
// разбора.
func cbrBopStandardMeta() []util.SeriesMeta {
	meta := make([]util.SeriesMeta, 0, len(cbrBopStandardAggregates)*len(cbrBopStandardSheets))
	for _, sheet := range cbrBopStandardSheets {
		for _, agg := range cbrBopStandardAggregates {
			name := cbrBopStandardName(agg.Code, sheet.Kind)
			origin := cbrBopStandardOrigin + "«" + sheet.Sheet + "», строка «" + agg.Label + "»"
			if agg.Block != "" {
				origin += " блока «" + agg.Block + "»"
			}
			meta = append(meta, util.SeriesMeta{
				Source:      cbrBopStandardTable,
				Series:      name,
				Title:       name,
				Unit:        cbrBopStandardUnit,
				Frequency:   cbrBopStandardKindFrequency[sheet.Kind],
				Origin:      origin,
				Description: agg.Measures + " " + cbrBopStandardKindDates[sheet.Kind] + cbrBopStandardDescriptionTail,
			})
		}
	}
	return meta
}
