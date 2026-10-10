package polyus

import (
	log "github.com/sirupsen/logrus"
)

// guardRecords — резервный фильтр между разбором страницы и batch.Append.
//
// Раньше guard был рабочим механизмом: разбор шёл по порядку токенов, и
// колонка-изменение шапки («Изм. за год», «y-o-y change») сдвигала значения
// влево — значение колонки изменения выдавалось за значение соседнего периода.
// Guard отбрасывал записи последнего найденного периода, чьи значения пришли из
// такой колонки. Колоночная модель (columns.go) устранила причину: колонка
// значения определяется его X через Column.covers, а колонки-изменения остаются
// отдельными непериодными колонками, чьи числа в период не попадают вовсе.
//
// Поэтому guard стал резервом и по умолчанию не отбрасывает НИЧЕГО: легитимный
// период теперь неотличим от «неправильного», и любой отброс по периоду — это
// возврат к угадыванию. Единственное, что он делает, — не пропускает в витрину
// запись с пустым Period.
//
// Пустой период — не «неизвестный период», а ошибка разбора: ключ витрины
// ORDER BY (company, metric, period) у всех таких записей один и тот же
// (metric, ""), и ReplacingMergeTree молча оставила бы одну строку вместо целого
// набора. Такая запись не должна дойти до batch.Append ни при каких условиях,
// но и произойти её не должно: recordsFromLine берёт период из колонки-периода,
// а колонки лежат вне периода (Column.Period == "") только у непериодных
// колонок, чьи значения в записи не превращаются вовсе.
//
// Возвращается число отброшенных записей — оно идёт в итоговую строку импортёра
// рядом с остальными счётчиками.
func guardRecords(records []MetricRecord) (kept []MetricRecord, dropped int) {
	kept = make([]MetricRecord, 0, len(records))

	for _, record := range records {
		if record.Period != "" {
			kept = append(kept, record)

			continue
		}

		dropped++

		log.Warnf(
			"drop %s with an empty period, value %v from %s: a record without a period is a parser bug, and its key would collide with every other record of %s",
			record.Metric,
			record.Value,
			record.SourceURL,
			record.Metric,
		)
	}

	return kept, dropped
}

// warnUnassignedCounts сообщает о значениях, которые разбор не отнёс ни к одному
// периоду.
//
// Счётчик приходит от парсера, а не считается здесь: нераспределённым считается
// значение, не ставшее записью, — неразобранный числовой токен или число в
// непериодной колонке (колонке изменения, подписи единиц). На строке
// «Производство золота» русского релиза 1 п/г 2026 таких ровно два — (2%) и 6%
// из колонок изменения.
//
// Ноль не логируется: на здоровом разборе счётчик нулевой, и предупреждение на
// каждый отчёт перестало бы что-либо значить. Ненулевой счётчик — не повод
// отбрасывать записи (значения колонок изменения и должны остаться за бортом),
// но повод посмотреть на шапку: он растёт, когда шапка и разбор расходятся.
//
// reports — число разобранных KPI-страниц, отдавших счётчики: без него сумма
// нечитаема (два значения на одной странице и по одному на двух — одно и то же
// число).
func warnUnassignedCounts(counts []int, reports int) {
	total := countUnassigned(counts)
	if total == 0 {
		return
	}

	log.WithFields(log.Fields{
		"unassigned": total,
		"reports":    reports,
	}).Warnf(
		"%d report(s) left %d value(s) outside period columns: values from change columns are expected here, but a growing count means the header and the parser disagree",
		reports,
		total,
	)
}

// countUnassigned суммирует счётчики нераспределённых значений по отчётам.
func countUnassigned(counts []int) int {
	total := 0

	for _, count := range counts {
		total += count
	}

	return total
}
