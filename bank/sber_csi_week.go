package bank

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// Старый путь /proxy/services/node-services/v1/... мёртв ровно как у месячного набора: отдаёт
	// HTML-оболочку SPA (200 text/html), из-за чего таблица оставалась пустой. Живая выгрузка —
	// пункт «…в .csv» кнопки «Скачать» дашборда
	// sberindex.ru/ru/dashboards/ver-izmenenie-trat-po-kategoriyam (проверено 2026-10-15).
	// «Расширенный CSV» того же дашборда отвечает {"error":"File not found"}, поэтому этот
	// «deprecated»-файл — единственная машиночитаемая выгрузка набора (спека §3, п. 4).
	sberCSIWeekDataUrl = "https://sberindex.ru/api/dataset/deprecated/csv/ru/ver-izmenenie-trat-po-kategoriyam.csv"
	sberCSIWeekTable   = "sber_izmenenie_trat"
	// DDL в %s-форме: имя таблицы подставляет шаблон (стандарт §5.1). Колонки — прежние, legacy
	// Float32 (спека §4.5).
	sberCSIWeekDdl = `CREATE TABLE IF NOT EXISTS %s (
			  category LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (category, date);
    `
)

// SberCSIWeek — недельное изменение потребительских расходов СберИндекса (% к сопоставимой
// ISO-неделе прошлого года), 46 категорий, дата точки — воскресенье ISO-недели. Импортёр построен
// на шаблоне util.ClickHouseImport: DDL и полный цикл Import — его (стандарт §5.1–5.2), а Name()
// даёт встроенный ClickHouseImport (TableName), поэтому CLICKHOUSE_IMPORT_STAT не меняется.
//
// Точки контроля публикации нет (спека §4.2, стандарт §9.3): HEAD источника — 403 (отпечаток
// версии построить нельзя), а темп файла равен темпу недельного шага — no-op недостижим и вреден.
type SberCSIWeek struct {
	util.ClickHouseImport
}

// sberCSIWeekCraw — полный путь точек: скачать → разобрать → записать одним батчем. CSV-случая в
// шаблоне нет (ImportFunc — только XLSX), поэтому весь цикл живёт здесь (стандарт §5.1, спека
// §4.1). Адрес приходит от шаблона (DataUrl), сеть — только через util (стандарт §4.1).
func sberCSIWeekCraw(dataUrl string, conn driver.Conn) (count int64, err error) {
	var records [][]string
	if records, err = util.GetCSV(dataUrl); err != nil {
		return 0, fmt.Errorf("sber %s: %w", sberCSIWeekTable, err)
	}
	// Разбор общий с месячным набором (bank/sber_csi_parse.go): формат `date;category;value` у
	// выгрузок один, включая TrimSpace категории («Все категории » источника), fail-fast на
	// HTML-заглушке и пустом результате (стандарт §6.1, §6.4) и пропуск записей с пустым
	// значением (стандарт §6.7) — прежний код пропускал такие строки молча, теперь о пропуске
	// сообщает журнал.
	var points []sberPoint
	var skipped int
	if points, skipped, err = parseSberRows(records); err != nil {
		return 0, fmt.Errorf("sber %s: %w", sberCSIWeekTable, err)
	}
	if skipped > 0 {
		log.Warnf("sber %s: пропущено %d записей с пустым значением (не точки ряда, стандарт §6.7)", sberCSIWeekTable, skipped)
	}
	// Незнакомая категория — ожидаемое изменение источника (новый товар/услуга), а не порча
	// выгрузки: импорт продолжается, но след в журнале остаётся (стандарт §6.6, спека §4.4). Иначе
	// ряд жил бы в таблице и витрине без описания у агента (стандарт §10.6), а падение шага в
	// цепочке dagu лишило бы данных tbank/vtb (стандарт §12.3). Имена перечисляются целиком и без
	// дублей: по одному warn-у на новую категорию, а не на каждую её неделю.
	if unknown := sberUndescribed(points, sberWeekSeriesMeta); len(unknown) > 0 {
		log.Warnf("sber %s: нет описания в каталоге для %q — импорт продолжен, описание догонять правкой каталога",
			sberCSIWeekTable, unknown)
	}
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", sberCSIWeekTable))
	if err != nil {
		return 0, err
	}
	// Один PrepareBatch на прогон (стандарт §5.3, правило 2 AGENTS.md).
	for _, p := range points {
		if err = batch.Append(p.Series, p.Date, p.Value); err != nil {
			_ = batch.Abort()
			return count, fmt.Errorf("sber %s: %w", sberCSIWeekTable, err)
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, fmt.Errorf("sber %s: %w", sberCSIWeekTable, err)
	}
	return count, nil
}

// sberCSIWeekImporter — импортёр пакета; регистрируется init() ниже (реестр пакета — отдельная работа).
var sberCSIWeekImporter = &SberCSIWeek{ClickHouseImport: util.ClickHouseImport{
	TableName:   sberCSIWeekTable,
	CreateTable: []string{sberCSIWeekDdl},
	DataUrl:     sberCSIWeekDataUrl,
	CrawFunc:    sberCSIWeekCraw,
	// Производные (каталог рядов и витрина) публикует шаблон после Send точек и до записи строки
	// версии (стандарт §5.2, §10.2) — тем же хелпером, что месячный импортёр: каталог и витрина
	// общие на оба набора, поэтому каждый прогон перезаписывает все 51 описание и витрину целиком.
	AfterImport: afterSberMacro(sberSeriesMeta),
}}

func init() {
	chimport.Stats = append(chimport.Stats, sberCSIWeekImporter)
}
