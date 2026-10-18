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

	from, parseErr := time.Parse("2006-01-02", cbrZcycHistoryFrom)
	if parseErr != nil {
		// Константа объявлена в коде и разбирается всегда; проверка — требование
		// стиля пакета (каждый Parse* с проверкой err), а не ожидаемый сценарий.
		return 0, fmt.Errorf("%s: начало истории %q: %w", cbrZcycTable, cbrZcycHistoryFrom, parseErr)
	}
	firstRun := last.Before(from)
	if !firstRun {
		from = last.AddDate(0, 0, 1)
	}

	now := time.Now()
	to := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	chunks := zcycChunks(from, to)
	if len(chunks) == 0 {
		log.Infof("%s: новых данных нет", cbrZcycTable)
		return 0, nil
	}

	var points []zcycPoint
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
		rows, err := zcycRows(page)
		if err != nil {
			return 0, fmt.Errorf("%s: окно %s, %s: %w", cbrZcycTable, window, url, err)
		}
		if len(rows) == 0 && i < len(chunks)-1 {
			return 0, fmt.Errorf("%s: пустое историческое окно %s (%s): ЦБ не отдал таблицу, хотя диапазон не последний", cbrZcycTable, window, url)
		}
		points = append(points, rows...)
	}

	if firstRun && len(points) == 0 {
		return 0, fmt.Errorf("%s: пустой бэкфилл: за %s..%s ЦБ не отдал ни одной точки", cbrZcycTable,
			chunks[0][0].Format("2006-01-02"), chunks[len(chunks)-1][1].Format("2006-01-02"))
	}
	if len(points) == 0 {
		log.Infof("%s: новых данных нет", cbrZcycTable)
		return 0, nil
	}

	batch, err := conn.PrepareBatch(ctx, cbrZcycInsert)
	if err != nil {
		return 0, err
	}
	for _, p := range points {
		if err = batch.Append(p.tenor, p.date, p.value); err != nil {
			return 0, err
		}
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("%s: %d точек", cbrZcycTable, count)
	return count, nil
}
