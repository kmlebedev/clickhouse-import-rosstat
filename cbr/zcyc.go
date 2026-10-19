package cbr

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// cbrZcycTable — история кривой бескупонной доходности государственных
	// облигаций ЦБ РФ: одна точка на (дата публикации, тенор).
	cbrZcycTable = "cbr_zcyc"
	// cbrZcycBaseUrl — страница КБД; параметры окна дописывает util.UniDbQueryURL.
	cbrZcycBaseUrl = "https://www.cbr.ru/hd_base/zcyc_params/"
	// cbrZcycHistoryFrom — начало истории КБД (04.01.2003).
	cbrZcycHistoryFrom = "2003-01-04"
	// cbrZcycChunkPause — пауза между окнами бэкфилла: ЦБ ограничивает диапазон
	// одного запроса ≈5 годами (окна — по cbrZcycChunkYears), а пауза снижает
	// риск троттлинга/WAF на серии запросов.
	cbrZcycChunkPause = 500 * time.Millisecond
	cbrZcycDdl        = `CREATE TABLE IF NOT EXISTS %s (
    tenor LowCardinality(String),
    date Date,
    yield Float64
) ENGINE = ReplacingMergeTree ORDER BY (date, tenor);`
	cbrZcycInsert = "INSERT INTO " + cbrZcycTable
)

type cbrZcyc struct {
	util.ClickHouseImport
}

// cbrZcycImporter — история КБД ЦБ; регистрируется реестром cbr/importers.go.
// Точки контроля публикации нет: URL каждого окна содержит сегодняшнюю дату, и
// отпечаток версии меняется каждый прогон — no-op по HEAD недостижим (та же
// причина, что у cbr_key_rate, cbr_ruania, cbr_currency_usd и cbr_gold).
var cbrZcycImporter = &cbrZcyc{ClickHouseImport: util.ClickHouseImport{
	TableName:   cbrZcycTable,
	CreateTable: []string{cbrZcycDdl},
	DataUrl:     cbrZcycBaseUrl,
	CrawFunc:    cbrZcycCraw,
	AfterImport: afterCbrZcyc(cbrZcycSeriesMeta),
}}

// cbrZcycHistoryFromDate — разобранная константа начала истории. Разбор на
// инициализации пакета безопасен: сетевых вызовов здесь нет (ARCHITECTURE §8.6),
// а ошибка в константе должна падать сразу, а не выглядеть пустым бэкфиллом.
var cbrZcycHistoryFromDate = func() time.Time {
	from, err := time.Parse("2006-01-02", cbrZcycHistoryFrom)
	if err != nil {
		panic(fmt.Sprintf("%s: начало истории %q: %v", cbrZcycTable, cbrZcycHistoryFrom, err))
	}
	return from
}()

// zcycDateUTC приводит дату к полуночи UTC: clickhouse-go отдаёт Date с таймзоной
// сессии (например, Asia/Yekaterinburg, UTC+5), а zcycRows разбирает даты как UTC.
// Без нормализации границы окон сдвигаются на предыдущие сутки, и фильтр окна
// отбрасывает точки последнего дня многолетнего инкремента.
func zcycDateUTC(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// zcycStart — начало импорта от последней записанной даты: пустая или
// доисторическая таблица (max = 1970-01-01 либо раньше 04.01.2003) — первый
// прогон с начала истории, иначе следующий календарный день после last. last
// нормализуется в UTC-дату: AddDate и сравнение идут по дате, а не по инстанту
// с таймзоной сессии.
func zcycStart(last time.Time) (from time.Time, firstRun bool) {
	last = zcycDateUTC(last)
	if last.Before(cbrZcycHistoryFromDate) {
		return cbrZcycHistoryFromDate, true
	}
	return last.AddDate(0, 0, 1), false
}

// cbrZcycCraw — инкрементальный импорт истории КБД: max(date) → окна по
// cbrZcycChunkYears → один батч. Все окна разбираются до первого PrepareBatch,
// поэтому пустое историческое окно (ЦБ ужесточил лимит диапазона или сломал
// страницу) не записывает частичный бэкфилл. Точки последнего окна могут быть
// пустыми — это штатные выходные/праздник, а не сбой. Повтор при
// max(date) = сегодня делает один запрос max(date) и не ходит в сеть;
// AfterImport на этом пути вызывает шаблон — каталог и витрины самолечатся.
func cbrZcycCraw(crawUrl string, conn driver.Conn) (count int64, err error) {
	var last time.Time
	if err = conn.QueryRow(ctx, "SELECT max(date) FROM "+cbrZcycTable+" FINAL").Scan(&last); err != nil {
		return 0, fmt.Errorf("%s: max(date): %w", cbrZcycTable, err)
	}
	// clickhouse-go отдаёт Date с таймзоной сессии, zcycRows — UTC: без
	// нормализации границы окон уезжают на предыдущие сутки.
	last = zcycDateUTC(last)
	from, firstRun := zcycStart(last)

	to := zcycDateUTC(time.Now())
	chunks := zcycChunks(from, to)
	if len(chunks) == 0 {
		log.Infof("%s: новых данных нет", cbrZcycTable)
		return 0, nil
	}

	var allPoints []zcycPoint
	for i, chunk := range chunks {
		if i > 0 {
			time.Sleep(cbrZcycChunkPause)
		}
		start, end := chunk[0], chunk[1]
		window := start.Format("2006-01-02") + ".." + end.Format("2006-01-02")
		url, err := util.UniDbQueryURL(crawUrl, start, end)
		if err != nil {
			return 0, fmt.Errorf("%s: окно %s: %w", cbrZcycTable, window, err)
		}
		page, err := util.GetPage(url)
		if err != nil {
			return 0, fmt.Errorf("%s: окно %s, %s: %w", cbrZcycTable, window, url, err)
		}
		points, err := zcycRows(page)
		if err != nil {
			return 0, fmt.Errorf("%s: окно %s, %s: %w", cbrZcycTable, window, url, err)
		}
		// ЦБ игнорирует пустой/будущий диапазон и отдаёт последнюю доступную
		// строку — вне окна она точкой этого импорта не является.
		points = zcycPointsInRange(points, start, end)
		if len(points) == 0 && i < len(chunks)-1 {
			return 0, fmt.Errorf("%s: пустое историческое окно %s (%s): нет точек в диапазоне; таблица отсутствовала или все строки вне окна", cbrZcycTable, window, url)
		}
		allPoints = append(allPoints, points...)
	}

	if firstRun && len(allPoints) == 0 {
		return 0, fmt.Errorf("%s: пустой бэкфилл: за %s..%s ЦБ не отдал ни одной точки", cbrZcycTable,
			chunks[0][0].Format("2006-01-02"), chunks[len(chunks)-1][1].Format("2006-01-02"))
	}
	if len(allPoints) == 0 {
		log.Infof("%s: новых данных нет", cbrZcycTable)
		return 0, nil
	}

	batch, err := conn.PrepareBatch(ctx, cbrZcycInsert)
	if err != nil {
		return 0, err
	}
	for _, p := range allPoints {
		if err = batch.Append(p.tenor, p.date, p.value); err != nil {
			// Соединение возвращается в пул только через Send или Abort (прецедент
			// util.abortBatch): серия сбоев Append иначе исчерпает пул драйвера.
			if abortErr := batch.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки Append: %v", cbrZcycTable, abortErr)
			}
			return 0, fmt.Errorf("%s: Append: %w", cbrZcycTable, err)
		}
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("%s: %d точек", cbrZcycTable, count)
	return count, nil
}
