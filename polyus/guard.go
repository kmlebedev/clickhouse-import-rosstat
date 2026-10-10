package polyus

import (
	"bufio"
	"os"

	log "github.com/sirupsen/logrus"
)

// guardKPIRecords отбрасывает записи, чьи значения пришли из колонки-изменения, и
// возвращает оставшиеся записи вместе с числом отброшенных.
//
// Шапки KPI-таблиц Polyus содержат колонки-изменения между периодами: в русском
// релизе за 1 п/г 2026 это «Изм. за год» между «1 п/г 2025» и «2 п/г 2025», в
// годовом MD&A за 2014 — «y-o-y change» между «FY 2013» и «2H 2014». Слова
// «Изм. …»/«y-o-y change» периодом не становятся (parsePeriod их не разбирает),
// поэтому parseKPIPage находит меньше колонок, чем столбцов в таблице, и
// нумерует найденные подряд:
//
//	шапка:      FY 2014   FY 2013   [y-o-y change]   2H 2014   1H 2014
//	данные:       2 239     2 329          (4%)        1 232      1 007
//	токен:           0         1            2            3          4
//	колонки разбора: [0]       [1]                      [2]
//	                 ↑         ↑                        ↑
//	                 FY2014    H2? ← сюда попадает токен 2, то есть «(4%)»
//
// Колонка-изменение стоит в шапке не последней: «Изм. за …» печатают между
// периодами, а завершает шапку настоящий период. Поэтому настоящий период,
// оказавшийся последним среди найденных, — это тот, чей числовой токен взят из
// колонки-изменения, и он недостоверен. Его период берётся из шапки разбираемой
// страницы, а не из общего списка: годовой отчёт обязан выжить, и старый фильтр
// по глобальному списку полугодий выбрасывал такой отчёт целиком (TestGuardKeepsAnnualPeriods).
//
// Отбрасывается запись целиком, а не один её период: в сдвинутом слоте лежит не
// значение показателя, а чужая колонка, и по самой записи этого не видно.
// Ошибка здесь односторонняя — отброшенное значение не попадает в витрину,
// тогда как принятое изменение выдало бы себя за данные отчёта. Логируется
// каждый отброс на уровне Warn: молчаливая потеря строк недопустима.
//
// Известное ограничение: колонка, стоящая в шапке перед колонкой-изменением,
// остаётся в витрине под своим периодом, хотя несёт значение предыдущего
// периода шапки (в MD&A за 2014 «2014H2» = 2 329, то есть данные «FY 2013»).
// Период этой колонки потерян ещё при разборе шапки, и индексным сдвигом его не
// восстановить — это отдельная задача о выравнивании колонок.
func guardKPIRecords(records []MetricRecord, periods []PeriodColumn) (kept []MetricRecord, dropped int) {
	untrustworthy := untrustworthyPeriod(periods)
	if untrustworthy == "" {
		return records, 0
	}

	for _, record := range records {
		if record.Period != untrustworthy {
			kept = append(kept, record)
			continue
		}

		dropped++
		log.Warnf(
			"drop %s %s = %v: column follows a change column in the header, so its values belong to the next period",
			record.Metric,
			record.Period,
			record.Value,
		)
	}

	return kept, dropped
}

// untrustworthyPeriod возвращает период последней колонки шапки — той, чьи
// значения parseKPIPage берёт из следующего столбца таблицы, потому что между
// ними стоит неопознанная колонка-изменение.
//
// Пустая строка означает, что отбрасывать нечего: колонок меньше двух. Одна
// колонка — это шапка без изменений, сдвигу взяться неоткуда, и объявлять её
// период недостоверным было бы ложной тревогой.
func untrustworthyPeriod(periods []PeriodColumn) string {
	if len(periods) < 2 {
		return ""
	}

	return periods[len(periods)-1].Period
}

// headerPeriods читает извлечённый текст страницы и возвращает периоды,
// объявленные её шапкой, в том порядке, в каком их нашёл scanPeriodColumns.
//
// Помощник нужен импортёру: filter применяется к записям страницы по её же
// шапке, а не по глобальному списку периодов. Страница без шапки (в том числе
// KPI-страница без строки «$ mln…») даёт пустой слайс — это не ошибка: записей
// на такой странице быть не может, потому что период каждой берётся из шапки.
func headerPeriods(textPath string) []PeriodColumn {
	file, err := os.Open(textPath)
	if err != nil {
		return nil
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Warnf("close extracted page %s: %v", textPath, closeErr)
		}
	}()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		definition, remainder, found := matchMetric(normalizeLine(scanner.Text()))
		if !found {
			continue
		}
		if definition.Name == "period" {
			return scanPeriodColumns(remainder)
		}
	}

	return nil
}
