package bank

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const (
	// Старый путь /proxy/services/node-services/v1/... мёртв: отдаёт HTML-оболочку SPA (200 text/html),
	// из-за чего таблица годами оставалась пустой. Живая выгрузка — пункт «…в .csv» кнопки «Скачать»
	// дашборда sberindex.ru/ru/dashboards/consumper-spending-index-sa (проверено 2026-10-15).
	// Резерв при исчезновении пути: /api/dataset/v1/download/consumper-spending-index-sa/csv (ZIP с CSV).
	sberCSIDataUrl = "https://sberindex.ru/api/dataset/deprecated/csv/ru/consumper-spending-index-sa.csv"
	sberCSITable   = "sber_consumper_spending_index"
	// DDL в %s-форме: имя таблицы подставляет шаблон (стандарт §5.1). Колонки — прежние, legacy
	// Float32 (спека §4.5): схема не меняется, CREATE TABLE IF NOT EXISTS существующую не трогает.
	sberCSIDdl = `CREATE TABLE IF NOT EXISTS %s (
			  name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);
    `
)

// SberCSI — месячный индекс реальных потребительских расходов СберИндекса (100 = декабрь 2018),
// 5 категорий, дата точки — первое число месяца. Импортёр построен на шаблоне
// util.ClickHouseImport: DDL и полный цикл Import — его (стандарт §5.1–5.2), а Name() даёт
// встроенный ClickHouseImport (TableName), поэтому CLICKHOUSE_IMPORT_STAT не меняется.
//
// Точки контроля публикации нет (спека §4.2, стандарт §9.3): HEAD источника отвечает 403, а
// Last-Modified на GET равен времени запроса — отпечаток версии построить нельзя, и префлайт
// деградировал бы в полный импорт на каждом прогоне.
type SberCSI struct {
	util.ClickHouseImport
}

// sberCSICraw — полный путь точек: скачать → разобрать → записать одним батчем. CSV-случая в
// шаблоне нет (ImportFunc — только XLSX), поэтому весь цикл живёт здесь (стандарт §5.1, спека
// §4.1). Адрес приходит от шаблона (DataUrl), сеть — только через util (стандарт §4.1).
func sberCSICraw(dataUrl string, conn driver.Conn) (count int64, err error) {
	var records [][]string
	if records, err = util.GetCSV(dataUrl); err != nil {
		return 0, fmt.Errorf("sber %s: %w", sberCSITable, err)
	}
	// Разбор отделён от сети (стандарт §6.1) и падает громко на HTML-заглушке или пустой
	// выгрузке — молчаливый «0 строк» и был дефектом, ради которого заведён пункт (стандарт §6.4).
	var points []sberPoint
	if points, err = parseSberRows(records); err != nil {
		return 0, fmt.Errorf("sber %s: %w", sberCSITable, err)
	}
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", sberCSITable))
	if err != nil {
		return 0, err
	}
	// Один PrepareBatch на прогон (стандарт §5.3, правило 2 AGENTS.md): построчный conn.Exec в
	// цикле запрещён.
	for _, p := range points {
		if err = batch.Append(p.Series, p.Date, p.Value); err != nil {
			_ = batch.Abort()
			return count, fmt.Errorf("sber %s: %w", sberCSITable, err)
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, fmt.Errorf("sber %s: %w", sberCSITable, err)
	}
	return count, nil
}

// sberCSIImporter — импортёр пакета; регистрируется init() ниже (реестр пакета — отдельная работа).
var sberCSIImporter = &SberCSI{ClickHouseImport: util.ClickHouseImport{
	TableName:   sberCSITable,
	CreateTable: []string{sberCSIDdl},
	DataUrl:     sberCSIDataUrl,
	CrawFunc:    sberCSICraw,
}}

func init() {
	chimport.Stats = append(chimport.Stats, sberCSIImporter)
}
