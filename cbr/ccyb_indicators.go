package cbr

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// cbrCcybTable — индикаторы кредитного цикла ЦБ: одна точка на (ряд, месяц).
	cbrCcybTable = "cbr_ccyb_indicators"
	// cbrCcybUrl — стабильная прямая ссылка на xlsx «Ряды значений индикаторов
	// кредитного цикла». Файл обновляется редко (последняя версия на 2026-10-19 —
	// Last-Modified 2024-12-09), поэтому объявлена точка контроля публикации.
	cbrCcybUrl = "https://www.cbr.ru/vfs/analytics/fin_stab/indicators/ccyb_indicators_time_series.xlsx"
	// cbrCcybSheet — единственный лист книги.
	cbrCcybSheet = "Ряды данных"
	// cbrCcybCountPoints — количественный чек версии: число точек своей таблицы.
	// FINAL обязателен: иначе count() считал бы несмёрженные части.
	cbrCcybCountPoints = "SELECT count() FROM cbr_ccyb_indicators FINAL"
	cbrCcybDdl         = `CREATE TABLE IF NOT EXISTS %s (
    series LowCardinality(String),
    indicator_no UInt8,
    indicator_group LowCardinality(String),
    date Date,
    value Float64,
    version UInt64
) ENGINE = ReplacingMergeTree(version) ORDER BY (series, date);`
)

// cbrCcybStat — импортёр cbr_ccyb_indicators: встроенный util.ClickHouseImport
// ведёт таблицу, DDL и точку контроля публикации; Import переопределён, чтобы
// (а) после полного импорта опубликовать каталог и витрину и (б) честно вернуть
// 0 на no-op. Шаблон на no-op возвращает записанное число точек, поэтому его
// возврат для счёта прогона не используется: признак полного импорта — строки с
// версией этого прогона.
type cbrCcybStat struct {
	util.ClickHouseImport
}

// Import — DDL → префлайт/полный путь шаблона → подсчёт строк своей версии →
// производные. DDL и префлайт выполняет шаблон; локальная копия нужна только
// чтобы подставить ImportFunc с версией прогона и не публиковать производные
// дважды. Порядок «точки → производные → строка версии» сохраняет шаблон:
// AfterImport локальной копии пуст, а каталог и витрина публикуются после
// успешного Import — в том числе на no-op (самолечение).
func (s *cbrCcybStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	version := uint64(time.Now().UnixNano())

	local := s.ClickHouseImport
	local.ImportFunc = cbrCcybImportFunc(version)
	local.AfterImport = nil
	if _, err = local.Import(ctx, conn); err != nil {
		return 0, err
	}

	var written uint64
	if err = conn.QueryRow(ctx,
		"SELECT count() FROM "+cbrCcybTable+" WHERE version = ?", version).Scan(&written); err != nil {
		return 0, fmt.Errorf("%s: число строк версии не прочитано: %w", cbrCcybTable, err)
	}
	if err = afterCbrCcyb(cbrCcybSeriesMeta)(ctx, conn); err != nil {
		return int64(written), err
	}
	// Сторож свежести: ряд не обновляется с 2024-10, и без предупреждения это видно только
	// в аудите. Ошибка чтения max(date) не отменяет импорт — она лишь лишает прогон
	// диагностики, поэтому логируется, а не возвращается.
	if staleErr := warnCbrCcybStale(ctx, conn, time.Now()); staleErr != nil {
		log.Warnf("%s: проверка свежести не выполнена: %v", cbrCcybTable, staleErr)
	}
	if written == 0 {
		log.Infof("%s: новых данных нет", cbrCcybTable)
	}
	return int64(written), nil
}

// cbrCcybImportFunc — ImportFunc шаблона: разбор книги и запись точек одной
// версией. Ошибка разбора отменяет батч (шаблон вызывает Abort), поэтому
// частичная запись файла невозможна.
func cbrCcybImportFunc(version uint64) func(*excelize.File, driver.Batch) error {
	return func(xlsx *excelize.File, batch driver.Batch) error {
		points, err := cbrCcybParse(xlsx)
		if err != nil {
			return err
		}
		for _, p := range points {
			if err = batch.Append(p.series, p.indicatorNo, p.group, p.date, p.value, version); err != nil {
				return fmt.Errorf("%s: ряд %q, %s: %w",
					cbrCcybTable, p.series, p.date.Format("2006-01-02"), err)
			}
		}
		return nil
	}
}

// cbrCcybImporter — запись реестра cbr/importers.go. DataUrl статический,
// точка контроля публикации объявлена: файл обновляется реже ежедневного шага.
var cbrCcybImporter = &cbrCcybStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              cbrCcybTable,
	CreateTable:            []string{cbrCcybDdl},
	DataUrl:                cbrCcybUrl,
	PublicationCountPoints: cbrCcybCountPoints,
}}

// cbrCcybStaleLimit — порог предупреждения о замороженном ряде, в днях. Файл ЦБ
// обновляется раз в месяц-квартал, поэтому 120 дней — это больше двух пропущенных
// публикаций; меньше — ложные срабатывания на нормальном лаге.
const cbrCcybStaleLimit = 120

// cbrCcybStaleDays — разрыв между последней точкой ряда и сегодняшним днём в днях.
// Считается по календарным компонентам дат, а не через .UTC()/.Truncate: clickhouse-go
// отдаёт Date в таймзоне сессии (Asia/Yekaterinburg, UTC+5), поэтому
// 2024-10-01T00:00+05:00 после .UTC() становится 2024-09-30T19:00Z и разрыв выходит на
// день больше земляного dateDiff('day', max(date), today()) — на границе порога
// предупреждение выстреливало бы на день раньше. Та же нормализация по календарным
// компонентам уже есть в пакете: cbr/zcyc.go (zcycDateUTC) и cbr/mp_summary.go (mpDay).
// Отрицательный разрыв (точка «из будущего») даёт 0.
func cbrCcybStaleDays(last, now time.Time) int {
	lastDay := time.Date(last.Year(), last.Month(), last.Day(), 0, 0, 0, 0, time.UTC)
	nowDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	days := int(nowDay.Sub(lastDay).Hours() / 24)
	if days < 0 {
		return 0
	}
	return days
}

// warnCbrCcybStale пишет предупреждение, если ряд не обновлялся дольше порога. Обрыв
// задокументирован в docs/IMPORTERS.md (файл не обновлялся с декабря 2024, последняя
// точка 2024-10-01) и в issue #578; проверка измеряет только max(date), поэтому причину
// в логе формулирует как гипотезу. Данные и схема не меняются: предупреждение делает
// обрыв видимым в каждом прогоне, а не только в аудите.
func warnCbrCcybStale(ctx context.Context, conn driver.Conn, now time.Time) error {
	var last time.Time
	if err := conn.QueryRow(ctx, "SELECT max(date) FROM "+cbrCcybTable+" FINAL").Scan(&last); err != nil {
		return fmt.Errorf("%s: max(date): %w", cbrCcybTable, err)
	}
	// max(date) по пустой Date-колонке ClickHouse возвращает 1970-01-01, а не NULL,
	// поэтому IsZero() сам по себе пустую таблицу не отсекает.
	if last.IsZero() || last.Year() <= 1970 {
		return nil
	}
	if days := cbrCcybStaleDays(last, now); days > cbrCcybStaleLimit {
		log.Warnf("%s: последняя точка %s — %d дн. назад (порог %d); гипотеза: файл источника не обновлялся с 12.2024 — см. docs/IMPORTERS.md; проверьте Last-Modified",
			cbrCcybTable, last.Format("2006-01-02"), days, cbrCcybStaleLimit)
	}
	return nil
}
