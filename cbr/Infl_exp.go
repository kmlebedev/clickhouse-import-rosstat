package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"strconv"
	"strings"
	"time"
)

// https://www.cbr.ru/analytics/dkp/inflationary_expectations/
// Статистические данные
const inflExpUrl = "https://www.cbr.ru/analytics/dkp/inflationary_expectations/"

// cbrInflExpTable и inflExpSheet — таблица и лист импортёра: оба попадают в текст ошибки
// пустого разбора, чтобы дежурный по логу видел, чей ряд остался без обновления.
const (
	cbrInflExpTable = "cbr_infl_exp"
	inflExpSheet    = "Данные для графиков"
	// cbrInflExpCountPoints — количественный чек версии (спека §3.5): число точек
	// своего ряда. Литерал, как и таблица: запрос — часть утверждения «эти точки
	// мои». FINAL обязателен — иначе count() считал бы несмёрженные части.
	cbrInflExpCountPoints = "SELECT count() FROM cbr_infl_exp FINAL"
)

// Имена двух рядов, которые пишут оба источника (xlsx и оперативный PDF). Константами,
// а не литералами: на них завязаны маппинг подписей книги, разбор PDF, каталог и витрины.
const (
	inflExpObservedName = "наблюдаемая инфляция"
	inflExpExpectedName = "ожидаемая инфляция"
)

// cbrInflExpOpName — имя оперативного PDF-импортёра: Name(), ключ версии в
// source_publications и id шага dagu/cbr.yaml. Отличается от таблицы намеренно: в
// cbr_infl_exp пишут два артефакта — книга (точные значения) и PDF (оперативная точка).
const cbrInflExpOpName = "cbr_infl_exp_op"

// cbrInflExpDdl — канонический DDL таблицы в %s-форме шаблона, общий для книги и PDF:
// второй писатель создаёт ту же таблицу идемпотентно (CREATE IF NOT EXISTS), а не заводит
// свою копию литерала.
const cbrInflExpDdl = `CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`

// cbrInflExpMaxDateQuery — гейт оперативного импортёра: максимальная уже записанная дата.
// PDF выходит раз в месяц, dagu ходит ежедневно, поэтому в обычный день это единственное
// чтение своей таблицы и ни одного сетевого обращения к файлу.
const cbrInflExpMaxDateQuery = "SELECT max(date) FROM cbr_infl_exp FINAL"

// inflExpLinkSelector — ссылка на актуальный XLSX в блоке версий. Имена файлов и id
// меняются при каждом обновлении (Infl_exp_ГГ-ММ.xlsx), ссылки идут от свежей к старой.
const inflExpLinkSelector = `a.versions_item[href$=".xlsx"]`

// inflExpSeriesByRowLabel сопоставляет подписи строк файла ЦБ стабильным именам рядов
// в ClickHouse. Подписи меняются между выпусками (например «наблюдаемая инфляция» →
// «годовая наблюдаемая инфляция»), поэтому маппинг явный, а имена в БД остаются
// неизменными — на них завязаны series_catalog и витрина v_cbr_macro.
var inflExpSeriesByRowLabel = map[string]string{
	"наблюдаемая инфляция":                  inflExpObservedName,
	"годовая наблюдаемая инфляция":          inflExpObservedName,
	"ожидаемая инфляция":                    inflExpExpectedName,
	"годовая инфляция, ожидаемая через год": inflExpExpectedName,
}

const inflExpTableMarker = "Прямые оценки годовой инфляции: медианные  значения"

// inflExpImport разбирает лист «Данные для графиков»: строки после маркера
// inflExpTableMarker разложены по рядам из inflExpSeriesByRowLabel, даты берутся из строки
// заголовка (маркер + 2). Пустой лист/отсутствие маркера/ноль точек — ошибка (fail-fast):
// иначе «0 rows» выглядели бы успешным прогоном, а ряды остались бы без обновления.
func inflExpImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows(inflExpSheet)
	if err != nil {
		return err
	}
	// Пустой лист — сразу ошибка: rows[1:] ниже паникует на пустом срезе, а паника в
	// импортёре — не «0 rows», а упавший шаг DAG без понятного сообщения.
	if len(rows) == 0 {
		return fmt.Errorf("%s: лист «%s» пуст", cbrInflExpTable, inflExpSheet)
	}
	tableIsFoundRowNum := -1
	count := 0
	for i, row := range rows[1:] {
		if len(row) == 0 || row[0] == "" {
			continue
		}
		if row[0] == inflExpTableMarker {
			tableIsFoundRowNum = i
			continue
		}
		if tableIsFoundRowNum < 0 {
			continue
		}
		series, ok := inflExpSeriesByRowLabel[strings.TrimSpace(row[0])]
		if !ok {
			break
		}
		dates := rows[tableIsFoundRowNum+2]
		for j, rowCol := range row[1:] {
			if j+1 >= len(dates) || dates[j+1] == "" || rowCol == "" {
				continue
			}
			date, err := time.Parse("Jan-06", dates[j+1])
			if err != nil {
				return fmt.Errorf("inflExp: parse date %q: %w", dates[j+1], err)
			}
			value, err := strconv.ParseFloat(rowCol, 64)
			if err != nil {
				return fmt.Errorf("inflExp: parse value %q: %w", rowCol, err)
			}
			if err = batch.Append(series, date.AddDate(0, 0, 13), value); err != nil {
				return err
			}
			count++
		}
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор листа «%s» не дал ни одной точки", cbrInflExpTable, inflExpSheet)
	}
	return nil
}

type inflExpStat struct {
	util.ClickHouseImport
}

// Import берёт адрес актуального XLSX со страницы инфляционных ожиданий: имени файла
// в коде нет, а сетевое обращение в init() запрещено (ARCHITECTURE.md §8.6). Ссылку
// ищем тем же кодом, что и тест на фикстуре (util.GetPage + util.FindLink).
func (s *inflExpStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	page, err := util.GetPage(inflExpUrl)
	if err != nil {
		return count, fmt.Errorf("inflExp: %s: %w", inflExpUrl, err)
	}
	href, err := util.FindLink(page, inflExpLinkSelector)
	if err != nil {
		return count, fmt.Errorf("inflExp: не найдена ссылка на xlsx на %s: %w", inflExpUrl, err)
	}
	s.DataUrl = cbrUrl + href
	return s.ClickHouseImport.Import(ctx, conn)
}

// cbrInflExpImporter — инфляционные ожидания: ссылка на актуальный XLSX ищется в
// Import() (ARCHITECTURE.md §8.6), регистрация — реестром cbr/importers.go.
// Своего кода префлайта у импортёра нет: Import() выставляет s.DataUrl до
// делегирования в шаблон, а HEAD и количественный чек шаблон делает по готовому URL
// (так же устроены cbr_credit_m2x и cbr_indicators_cpd).
var cbrInflExpImporter = &inflExpStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              cbrInflExpTable,
	CreateTable:            []string{cbrInflExpDdl},
	ImportFunc:             inflExpImport,
	AfterImport:            afterCbrMacro(cbrInflExpSeriesMeta),
	PublicationCountPoints: cbrInflExpCountPoints,
}}

// inflExpOpStat — оперативный импортёр cbr_infl_exp_op: та же таблица, что у книги, но
// другой артефакт (PDF «Оперативная справка»), поэтому и своё имя, и свой ключ версии.
type inflExpOpStat struct {
	util.ClickHouseImport
}

// Name — имя импортёра, а не таблицы: в cbr_infl_exp пишут два артефакта, и
// CLICKHOUSE_IMPORT_STAT/шаг dagu должны различать книгу и PDF. Промотированный из
// util.ClickHouseImport Name() вернул бы TableName — общее имя для обоих.
func (s *inflExpOpStat) Name() string { return cbrInflExpOpName }

// Import — цикл оперативного импортёра: DDL → страница → свежий PDF → месяц из имени →
// гейт по max(date) → шаблон (префлайт, скачивание, архив, каталог, строка версии).
// Гейт стоит до скачивания: если PDF за этот месяц уже разобран (или книга пришла
// раньше), прогон — «0 строк» без единого обращения к файлу. Ошибка разбора PDF не
// трогает книгу: у неё свой шаг и своя строка версии.
func (s *inflExpOpStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", cbrInflExpOpName, err)
		}
	}
	page, err := util.GetPage(inflExpUrl)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrInflExpOpName, inflExpUrl, err)
	}
	href, err := util.FindLink(page, inflExpPdfLinkSelector)
	if err != nil {
		return 0, fmt.Errorf("%s: не найдена ссылка на PDF на %s: %w", cbrInflExpOpName, inflExpUrl, err)
	}
	month, err := inflExpPdfMonth(href)
	if err != nil {
		return 0, err
	}
	var known time.Time
	if err = conn.QueryRow(ctx, cbrInflExpMaxDateQuery).Scan(&known); err != nil {
		return 0, fmt.Errorf("%s: максимум date не прочитан: %w", cbrInflExpOpName, err)
	}
	if !inflExpPdfNewer(month, known) {
		log.Infof("%s: PDF за %s уже импортирован (max=%s) — скачивание не требуется",
			cbrInflExpOpName, month.Format("2006-01"), known.Format("2006-01-02"))
		return 0, nil
	}
	s.DataUrl = cbrUrl + href
	return s.ClickHouseImport.Import(ctx, conn)
}

// cbrInflExpOpImporter — оперативная точка инфляционных ожиданий из PDF «Оперативная
// справка». CreateTable намеренно пуст: DDL общей cbr_infl_exp выполняет собственный
// Import() (иначе шаблон выполнил бы его второй раз). TableName — общая таблица,
// PublicationSource — своё имя, чтобы версия PDF не смешивалась с версией книги.
var cbrInflExpOpImporter = newInflExpOpImporter()

func newInflExpOpImporter() *inflExpOpStat {
	s := &inflExpOpStat{ClickHouseImport: util.ClickHouseImport{
		TableName:              cbrInflExpTable,
		PublicationSource:      cbrInflExpOpName,
		AfterImport:            afterCbrMacro(cbrInflExpSeriesMeta),
		PublicationCountPoints: cbrInflExpCountPoints,
	}}
	// BodyFunc — метод импортёра: месяц ожидаемой точки он берёт из s.DataUrl, который
	// Import() уже выставил по свежей ссылке. Отдельного состояния это не заводит.
	s.BodyFunc = s.importPDF
	return s
}

// importPDF — BodyFunc импортёра: PDF-байты → `-bbox` → колонки → две строки батчем.
// Месяц последней колонки обязан совпасть с месяцем из имени файла (см. appendLatest).
func (s *inflExpOpStat) importPDF(body []byte, batch driver.Batch) error {
	expected, err := inflExpPdfMonth(s.DataUrl)
	if err != nil {
		return err
	}
	bbox, err := inflExpPdfBBox(body)
	if err != nil {
		return err
	}
	return inflExpPdfAppendLatest(bbox, expected, batch)
}

// inflExpPdfAppendLatest разбирает страницу и пишет две строки оперативной точки. Месяц
// последней колонки обязан совпасть с месяцем из имени файла: расхождение значит, что
// график не успел за выпуском или ось сдвинулась, и писать точку под чужим месяцем нельзя.
func inflExpPdfAppendLatest(bbox []byte, expected time.Time, batch driver.Batch) error {
	points, err := inflExpPdfPoints(bbox)
	if err != nil {
		return err
	}
	last := points[len(points)-1]
	if last.Month.Year() != expected.Year() || last.Month.Month() != expected.Month() {
		return fmt.Errorf("%s: последняя точка графика %s, а файл за %s — график не успел за выпуском или ось сдвинулась",
			inflExpPdfContext, last.Month.Format("2006-01"), expected.Format("2006-01"))
	}
	return inflExpPdfAppend(last, batch)
}
