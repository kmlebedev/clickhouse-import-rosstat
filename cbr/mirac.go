package cbr

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// MIACR — средневзвешенные фактические ставки по кредитам, предоставленным московскими
// банками (MIACR, MIACR-IG, MIACR-B), форма ЦБ /hd_base/mkr/mkr_base/. Разведка —
// research/2026-11-14-cbr-html-tables-recon.md §2.
//
// Форма рендерит шесть таблиц: три со ставками (% годовых) и три с оборотами (млн рублей).
// Инструмент опознаётся по подписи .table-caption («MIACR -», «MIACR-IG -», «MIACR-B -»),
// а не по порядку таблиц: порядок страницы — деталь разметки, подпись — контракт.
const (
	cbrMiracTable  = "cbr_mirac"
	cbrMiracInsert = "INSERT INTO " + cbrMiracTable

	// cbrMiracBaseUrl — страница формы с обязательными параметрами: без st/ob/sk таблиц
	// на странице нет вовсе (проверено 2026-11-14). Currency=1 — только рубли: валютные
	// таблицы (Currency=-1) дают ещё четыре, и они вне объёма пункта.
	cbrMiracBaseUrl = "https://www.cbr.ru/hd_base/mkr/mkr_base/?" +
		"UniDbQuery.st=SF&UniDbQuery.st=HR&UniDbQuery.st=MB&" +
		"UniDbQuery.ob=OB_MIACR_0&UniDbQuery.ob=OB_MIACR_IG&UniDbQuery.ob=OB_MIACR_B&" +
		"UniDbQuery.Currency=1&" +
		"UniDbQuery.sk=Dd1_&UniDbQuery.sk=Dd7&UniDbQuery.sk=Dd30&" +
		"UniDbQuery.sk=Dd90&UniDbQuery.sk=Dd180&UniDbQuery.sk=Dd360"

	// cbrMiracHistoryFrom — первая дата данных формы. Сама форма клампит From к
	// 13.07.2000, но строк за июль 2000 у неё нет: первая точка MIACR — 01.08.2000
	// (проверено 2026-11-14 запросом 01.07.2000..31.07.2000 — 0 строк). Бэкфилл начинается
	// с фактической первой даты, чтобы не запрашивать заведомо пустое окно.
	cbrMiracHistoryFrom = "2000-08-01"

	cbrMiracChunkPause = 500 * time.Millisecond

	cbrMiracTimeLayout = "02.01.2006"

	cbrMiracDdl = "CREATE TABLE IF NOT EXISTS %s (" +
		" name LowCardinality(String)," +
		" tenor LowCardinality(String)," +
		" date Date," +
		" value Float64" +
		") ENGINE = ReplacingMergeTree ORDER BY (name, tenor, date);"
)

// cbrMiracTenors — колонки сроков формы → каноническая запись. Теноры те же, что
// публикует форма; неизвестная подпись — ошибка разметки, а не пропуск колонки.
var cbrMiracTenors = map[string]string{
	"1 день":               "1d",
	"от 2 до 7 дн.":        "2-7d",
	"от 8 до 30 дн.":       "8-30d",
	"от 31 до 90 дн.":      "31-90d",
	"от 91 до 180 дн.":     "91-180d",
	"от 181 дн. до 1 года": "181d-1y",
}

// cbrMiracPoint — точка MIACR: инструмент, канонический тенор, торговый день и ставка.
type cbrMiracPoint struct {
	name  string
	tenor string
	date  time.Time
	value float64
}

// cbrMiracCaption — подпись таблицы: текст ближайшего .table-caption той же обёртки.
func cbrMiracCaption(table *goquery.Selection) string {
	caption := table.Parent().Parent().Find(".table-caption").First().Text()
	return strings.Join(strings.Fields(caption), " ")
}

// cbrMiracInstrument опознаёт инструмент по подписи таблицы. Таблицы оборотов (подпись
// «MIACR млн рублей») не содержат «% годовых» и отбрасываются — это не ставки.
func cbrMiracInstrument(caption string) (string, bool) {
	if !strings.Contains(caption, "% годовых") {
		return "", false
	}
	switch {
	case strings.HasPrefix(caption, "MIACR-IG"):
		return "MIACR-IG", true
	case strings.HasPrefix(caption, "MIACR-B"):
		return "MIACR-B", true
	case strings.HasPrefix(caption, "MIACR"):
		return "MIACR", true
	}
	return "", false
}

// cbrMiracRows разбирает страницу формы: три таблицы ставок → точки. Нет таблиц, но
// страница — форма UniDbQuery: nil, nil (пустой диапазон); нет таблиц на чужой странице —
// ошибка. Неизвестная подпись срока или строка короче заголовка — ошибка разметки:
// сдвинутая колонка записала бы ставку чужого срока.
func cbrMiracRows(body []byte) ([]cbrMiracPoint, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("cbr_mirac: разбор страницы: %w", err)
	}
	tables := doc.Find("table.data")
	if tables.Length() == 0 {
		if util.IsUniDbQueryPage(body) {
			return nil, nil
		}
		return nil, fmt.Errorf("cbr_mirac: таблицы формы не найдены — страница не похожа на форму UniDbQuery (WAF-заглушка?)")
	}
	var points []cbrMiracPoint
	var instruments []string
	var parseErr error
	tables.Each(func(_ int, table *goquery.Selection) {
		if parseErr != nil {
			return
		}
		name, ok := cbrMiracInstrument(cbrMiracCaption(table))
		if !ok {
			return
		}
		tablePoints, err := cbrMiracTablePoints(table, name)
		if err != nil {
			parseErr = fmt.Errorf("cbr_mirac: %s: %w", name, err)
			return
		}
		instruments = append(instruments, name)
		points = append(points, tablePoints...)
	})
	if parseErr != nil {
		return nil, parseErr
	}
	if len(instruments) == 0 {
		return nil, fmt.Errorf("cbr_mirac: ни одной таблицы MIACR со ставками — подписи таблиц изменились")
	}
	return points, nil
}

// cbrMiracTablePoints разбирает одну таблицу ставок: заголовок задаёт теноры, строки —
// даты. Пустая ячейка (в форме это «—») — отсутствие значения, а не ноль.
func cbrMiracTablePoints(table *goquery.Selection, name string) ([]cbrMiracPoint, error) {
	header := table.Find("tr").First()
	headerCells := header.Find("th")
	if headerCells.Length() < 2 {
		return nil, fmt.Errorf("таблица без строки заголовка — разметка изменилась")
	}
	tenors := make([]string, 0, headerCells.Length()-1)
	for i := 1; i < headerCells.Length(); i++ {
		label := strings.Join(strings.Fields(headerCells.Eq(i).Text()), " ")
		tenor, ok := cbrMiracTenors[label]
		if !ok {
			return nil, fmt.Errorf("неизвестная колонка срока %q — разметка формы изменилась", label)
		}
		tenors = append(tenors, tenor)
	}
	var points []cbrMiracPoint
	var parseErr error
	table.Find("tr").Each(func(_ int, row *goquery.Selection) {
		if parseErr != nil {
			return
		}
		cells := row.Find("td")
		if cells.Length() == 0 {
			return
		}
		rawDate := strings.Join(strings.Fields(cells.Eq(0).Text()), " ")
		date, err := time.Parse(cbrMiracTimeLayout, rawDate)
		if err != nil {
			parseErr = fmt.Errorf("дата %q: %w", rawDate, err)
			return
		}
		if cells.Length() < len(tenors)+1 {
			parseErr = fmt.Errorf("в строке %s %d ячеек, ожидалось не меньше %d — разметка изменилась",
				rawDate, cells.Length(), len(tenors)+1)
			return
		}
		for i, tenor := range tenors {
			value, ok, err := util.ParseRuNumber(cells.Eq(i + 1).Text())
			if err != nil {
				parseErr = fmt.Errorf("значение %q (%s, %s): %w", cells.Eq(i+1).Text(), rawDate, tenor, err)
				return
			}
			if !ok {
				continue
			}
			points = append(points, cbrMiracPoint{name: name, tenor: tenor, date: date, value: value})
		}
	})
	if parseErr != nil {
		return nil, parseErr
	}
	return points, nil
}

// cbrMiracCraw — инкрементальный импорт: max(date) → окна → один батч. Окна режет
// общий zcycChunks по cbrZcycChunkYears (4 года), как у cbr_zcyc: своего переключателя
// длины окна у MIACR нет. Все окна разбираются до PrepareBatch: пустое историческое окно
// не должно записать частичный бэкфилл. Точки вне окна отбрасываются: на пустой/будущий
// диапазон форма отдаёт последнюю доступную строку.
func cbrMiracCraw(_ string, conn driver.Conn) (count int64, err error) {
	var last time.Time
	if err = conn.QueryRow(ctx, "SELECT max(date) FROM "+cbrMiracTable+" FINAL").Scan(&last); err != nil {
		return 0, fmt.Errorf("%s: max(date): %w", cbrMiracTable, err)
	}
	last = zcycDateUTC(last)
	historyFrom, err := time.Parse("2006-01-02", cbrMiracHistoryFrom)
	if err != nil {
		return 0, fmt.Errorf("%s: начало истории %q: %w", cbrMiracTable, cbrMiracHistoryFrom, err)
	}
	firstRun := last.Before(historyFrom)
	start := historyFrom
	if !firstRun {
		start = last.AddDate(0, 0, 1)
	}
	to := zcycDateUTC(time.Now())
	if start.After(to) {
		log.Infof("%s: новых данных нет", cbrMiracTable)
		return 0, nil
	}
	chunks := zcycChunks(start, to)
	var all []cbrMiracPoint
	for i, chunk := range chunks {
		if i > 0 {
			time.Sleep(cbrMiracChunkPause)
		}
		begin, end := chunk[0], chunk[1]
		window := begin.Format("2006-01-02") + ".." + end.Format("2006-01-02")
		url, err := util.UniDbQueryURL(cbrMiracBaseUrl, begin, end)
		if err != nil {
			return 0, fmt.Errorf("%s: окно %s: %w", cbrMiracTable, window, err)
		}
		page, err := util.GetPage(url)
		if err != nil {
			return 0, fmt.Errorf("%s: окно %s, %s: %w", cbrMiracTable, window, url, err)
		}
		points, err := cbrMiracRows(page)
		if err != nil {
			return 0, fmt.Errorf("%s: окно %s, %s: %w", cbrMiracTable, window, url, err)
		}
		kept := cbrMiracPointsInRange(points, begin, end)
		if len(kept) == 0 && i < len(chunks)-1 {
			return 0, fmt.Errorf("%s: пустое историческое окно %s (%s): нет точек в диапазоне", cbrMiracTable, window, url)
		}
		all = append(all, kept...)
	}
	if firstRun && len(all) == 0 {
		return 0, fmt.Errorf("%s: пустой бэкфилл за %s..%s", cbrMiracTable,
			chunks[0][0].Format("2006-01-02"), chunks[len(chunks)-1][1].Format("2006-01-02"))
	}
	if len(all) == 0 {
		log.Infof("%s: новых данных нет", cbrMiracTable)
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, cbrMiracInsert)
	if err != nil {
		return 0, err
	}
	for _, point := range all {
		if err = batch.Append(point.name, point.tenor, point.date, point.value); err != nil {
			if abortErr := batch.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки Append: %v", cbrMiracTable, abortErr)
			}
			return 0, fmt.Errorf("%s: Append (%s, %s, %s): %w", cbrMiracTable, point.name, point.tenor,
				point.date.Format("2006-01-02"), err)
		}
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("%s: %d точек", cbrMiracTable, count)
	return count, nil
}

// cbrMiracPointsInRange оставляет точки окна [from, to] включительно.
func cbrMiracPointsInRange(points []cbrMiracPoint, from, to time.Time) []cbrMiracPoint {
	var kept []cbrMiracPoint
	for _, point := range points {
		if point.date.Before(from) || point.date.After(to) {
			continue
		}
		kept = append(kept, point)
	}
	return kept
}

type cbrMirac struct {
	util.ClickHouseImport
}

// cbrMiracImporter — ставки денежного рынка MIACR/MIACR-IG/MIACR-B; регистрируется
// реестром cbr/importers.go. Точки контроля публикации нет: URL окна содержит сегодняшнюю
// дату, инкремент держит max(date).
var cbrMiracImporter = &cbrMirac{ClickHouseImport: util.ClickHouseImport{
	TableName:   cbrMiracTable,
	CreateTable: []string{cbrMiracDdl},
	DataUrl:     cbrMiracBaseUrl,
	CrawFunc:    cbrMiracCraw,
}}
