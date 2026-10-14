package rosstat

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// ToDo update data source Потребительские цены https://rosstat.gov.ru/statistics/price
	// Индексы потребительских цен на товары и услуги по Российской Федерации, месяцы (с 1991 г.)
	rosstatIPCMesTable = "rosstat_ipc_mes"
	ipcMesDdl          = `CREATE TABLE IF NOT EXISTS %s (
				  name LowCardinality(String)
				, date Date
				, percent Float32
			) ENGINE = ReplacingMergeTree ORDER BY (name, date);
		`
	// ipcMesField — якорь блока месячных значений: строка с этой подписью в первой колонке.
	// Разметка ищется по признаку, а не по номеру строки (§6.3): сноски и вставленные строки
	// сдвигают лист, и жёсткий индекс читал бы не те строки.
	ipcMesField = "к концу предыдущего месяца"
	// ipcMesLinkSelector — селектор ссылки на актуальный xlsx: константа рядом с импортёром, а
	// не литерал в теле функции (§4.5) — тот же селектор проверяет тест на HTML-фикстуре.
	// Прежняя позиционная CSS-цепочка из номеров вложенных элементов ломалась от любой правки
	// вёрстки страницы; на странице цен лежат и другие выгрузки `ipc_*`, поэтому селектор
	// ограничен `ipc_mes_`.
	ipcMesLinkSelector = `a[href*="ipc_mes_"]`
	ipcMesTimeLayout   = "2006-01"
	// ipcMesCountPoints — количественный чек версии (спека §3.5): число точек своего ряда.
	// Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL обязателен —
	// без него count() считает несмёрженные части и растёт после каждого повторного импорта,
	// то есть чек сравнивал бы величину, меняющуюся от прогона к прогону.
	ipcMesCountPoints = "SELECT count() FROM rosstat_ipc_mes FINAL"
)

// ipcMesSeriesNames — имя ряда для листа, объявленное кодом (§6.8). Имя не читается из заголовка
// листа: заголовок несёт сноску («…на товары и услуги1)…») и годы выгрузки («…в 1991-2026*)гг.»),
// которые Росстат меняет при каждом обновлении файла, — ряд из заголовка оборвался бы на первой же
// редакции, а таблица получила бы новый ряд. Заголовок не читается вовсе; из файла берутся только
// значения и годы строки над якорем. Значения совпадают с ipcMesSeriesMeta[].Series дословно —
// эти строки и есть тождество ряда в каталоге, совпадение проверяют тесты.
var ipcMesSeriesNames = map[int]string{
	1: "Индексы потребительских цен на товары и услуги по Российской Федерации",
	2: "Индексы потребительских цен на продовольственные товары по Российской Федерации",
	3: "Индексы потребительских цен на непродовольственные товары по Российской Федерации",
	4: "Индексы потребительских цен на услуги по Российской Федерации",
}

type IpcMesStat struct {
	util.ClickHouseImport
}

// getXlsDataUrl ищет ссылку на актуальный ipc_mes_*.xlsx на странице цен Росстата: имени файла в
// коде нет (ipc_mes_ММ-ГГГГ.xlsx меняется при каждом обновлении), а сетевое обращение в init()
// запрещено (ARCHITECTURE.md §8.6). Транспорт — через util (§4.1), и ошибка не проглатывается
// (§4.4): без ссылки импорт обязан упасть до записи в БД, а не писать «0 строк» по устаревшему
// адресу.
func (s *IpcMesStat) getXlsDataUrl() (string, error) {
	pageUrl := fmt.Sprintf("%s/statistics/price", rosstatUrl)
	page, err := util.GetPage(pageUrl)
	if err != nil {
		return "", fmt.Errorf("ipcMes: %s: %w", pageUrl, err)
	}
	href, err := util.FindLink(page, ipcMesLinkSelector)
	if err != nil {
		return "", fmt.Errorf("ipcMes: ссылка на ipc_mes_*.xlsx не найдена на %s: %w", pageUrl, err)
	}
	return rosstatUrl + href, nil
}

// ipcMesPoint — точка листа: дата записи (последний день отчётного месяца) и сырое значение
// индекса. Значение остаётся float64 до записи в батч: сужение до Float32 делает шаблон (колонка
// таблицы), поэтому тест на сырое значение ячейки (§6.2) видит значение целиком.
type ipcMesPoint struct {
	date  time.Time
	value float64
}

// ipcMes — точка книги: имя ряда объявлено кодом (§6.8), лист даёт дату и значение.
type ipcMes struct {
	name  string
	date  time.Time
	value float64
}

// ipcMesYears читает годы из строки над якорем: колонки 1… до первой пустой ячейки. Год берётся
// из файла, а не из константы: строка годов содержит годы выгрузки и сдвигается при её обновлении,
// и константа (1991) сдвинула бы весь ряд, а в таблице с ключом (name, date) это тихая подмена
// данных. Пустая строка годов — ошибка (§6.4): без годов точки получили бы дату «0000-01-01».
func ipcMesYears(row []string) ([]int, error) {
	var years []int
	for i := 1; i < len(row); i++ {
		cell := strings.TrimSpace(row[i])
		if cell == "" {
			break
		}
		year, err := strconv.Atoi(cell)
		if err != nil {
			return nil, fmt.Errorf("ячейка года %q (колонка %d) не число: %w", cell, i+1, err)
		}
		years = append(years, year)
	}
	if len(years) == 0 {
		return nil, fmt.Errorf("строка годов пуста")
	}
	return years, nil
}

// ipcMesValue разбирает ячейку значения. Ячейка приходит сырым текстом (§6.2): форматированная
// строка формата `0.00` — представление, а не данные. NaN и бесконечность, которые принимает
// ParseFloat, отбрасываются, как у соседних рядов пакета: такой мусор уехал бы в витрину точкой
// ряда (§6.4).
func ipcMesValue(cell string) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(cell), 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", cell, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение %q не конечное число", cell)
	}
	return value, nil
}

// ipcMesDate переводит год и месяц в дату точки — последний день отчётного месяца (январь 1992 —
// 1992-01-31). Семантика прежнего разбора сохранена дословно: месяц разбирается макетом
// ipcMesTimeLayout, затем к первому числу месяца прибавляется месяц и отнимается день.
func ipcMesDate(year int, month time.Month) (time.Time, error) {
	first, err := time.Parse(ipcMesTimeLayout, fmt.Sprintf("%d-%02d", year, int(month)))
	if err != nil {
		return time.Time{}, fmt.Errorf("дата %d-%02d: %w", year, int(month), err)
	}
	return first.AddDate(0, 1, -1), nil
}

// parseIpcMesSheet читает один лист книги. Разметка листа: A1 — заголовок со сносками и годами (в
// имени ряда не участвует), ниже — строка годов (колонки B…, то есть индексы 1…), под ней — якорь
// «к концу предыдущего месяца», под якорем — строки «январь»…«декабрь» с одним значением на
// колонку года. Ряд идёт от начала блока месяцев: строка короче двух колонок или строка, первая
// ячейка которой не месяц, блок замыкает. Так из ряда выпадают второй якорь «к декабрю предыдущего
// года» с годовым декабрём (другой показатель: в листе «01» за 1992 год 2608.84 против месячного
// 125.2) и сноски внизу листа.
func parseIpcMesSheet(xlsx *excelize.File, sheet string) ([]ipcMesPoint, error) {
	// Сырые значения ячеек (§6.2): форматированная строка отдала бы значение по формату ячейки,
	// а не то, что хранит файл.
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("%s: лист %q: %w", rosstatIPCMesTable, sheet, err)
	}
	anchor := -1
	for i, row := range rows {
		if len(row) > 0 && strings.TrimSpace(row[0]) == ipcMesField {
			anchor = i
			break
		}
	}
	if anchor < 0 {
		return nil, fmt.Errorf("%s: лист %q: строка «%s» не найдена", rosstatIPCMesTable, sheet, ipcMesField)
	}
	// Годы — строкой выше якоря. Проверка границы обязательна: выход за срез — паника, а разбор
	// обязан отдавать ошибку (§6.5).
	if anchor == 0 {
		return nil, fmt.Errorf("%s: лист %q: строки годов нет над строкой «%s» (строка %d)", rosstatIPCMesTable, sheet, ipcMesField, anchor+1)
	}
	years, err := ipcMesYears(rows[anchor-1])
	if err != nil {
		return nil, fmt.Errorf("%s: лист %q, строка %d: %w", rosstatIPCMesTable, sheet, anchor, err)
	}
	var points []ipcMesPoint
	monthNum := 0
	for _, row := range rows[anchor+1:] {
		if len(row) < 2 {
			break
		}
		month, ok := util.MonthsToNum[strings.ToLower(strings.TrimSpace(row[0]))]
		if !ok {
			break
		}
		// Месяцы блока перечисляются по возрастанию: повтор или откат подписи — сломанная
		// разметка, а не данные, и читать дальше нельзя (§6.4).
		if int(month) <= monthNum {
			return nil, fmt.Errorf("%s: лист %q: месяц %q идёт после месяца %d — месяцы обязаны идти по возрастанию",
				rosstatIPCMesTable, sheet, strings.TrimSpace(row[0]), monthNum)
		}
		monthNum = int(month)
		for i := 1; i < len(row); i++ {
			cell := strings.TrimSpace(row[i])
			if cell == "" {
				// Пустая ячейка — не точка: у последних месяцев нового года данных ещё нет,
				// и пропуск означает отсутствие значения, а не ноль (§6.7).
				continue
			}
			if i > len(years) {
				return nil, fmt.Errorf("%s: лист %q: колонка %d вне строки годов (%d колонок)",
					rosstatIPCMesTable, sheet, i+1, len(years))
			}
			value, err := ipcMesValue(cell)
			if err != nil {
				return nil, fmt.Errorf("%s: лист %q, %d-%02d: %w", rosstatIPCMesTable, sheet, years[i-1], month, err)
			}
			date, err := ipcMesDate(years[i-1], month)
			if err != nil {
				return nil, fmt.Errorf("%s: лист %q: %w", rosstatIPCMesTable, sheet, err)
			}
			points = append(points, ipcMesPoint{date: date, value: value})
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: лист %q: разбор дал 0 точек", rosstatIPCMesTable, sheet)
	}
	return points, nil
}

// parseIpcMes читает книгу целиком и отдаёт плоскую таблицу точек: листы обходятся в порядке
// книги, имя ряда — из ipcMesSeriesNames (§6.8). Числовой лист вне карты — предупреждение и
// пропуск (§6.6): добавленный источником лист — ожидаемое изменение выгрузки и не должен валить
// недельную цепочку, но и молчать о нём нельзя. Отсутствие ожидаемого листа — ошибка (§6.4):
// книга без листа означала бы тихий обрыв ряда, который в витрине выглядит полным. Разбор идёт
// целиком до первой записи в батч: ошибка любого листа оставляет батч пустым.
func parseIpcMes(xlsx *excelize.File) ([]ipcMes, error) {
	var table []ipcMes
	seen := make(map[int]bool, len(ipcMesSeriesNames))
	for _, sheet := range xlsx.GetSheetList() {
		number, err := strconv.Atoi(sheet)
		if err != nil {
			continue // «Содержание» и прочие нечисловые листы рядами не являются
		}
		name, ok := ipcMesSeriesNames[number]
		if !ok {
			log.Warnf("%s: лист %q не описан в ipcMesSeriesNames — лист пропущен", rosstatIPCMesTable, sheet)
			continue
		}
		points, err := parseIpcMesSheet(xlsx, sheet)
		if err != nil {
			return nil, err
		}
		for _, point := range points {
			table = append(table, ipcMes{name: name, date: point.date, value: point.value})
		}
		seen[number] = true
	}
	// Номера пропавших листов сортируются: обход карты в Go случаен, и текст ошибки менялся бы
	// от прогона к прогону.
	missing := make([]int, 0, len(ipcMesSeriesNames))
	for number := range ipcMesSeriesNames {
		if !seen[number] {
			missing = append(missing, number)
		}
	}
	if len(missing) > 0 {
		sort.Ints(missing)
		names := make([]string, 0, len(missing))
		for _, number := range missing {
			names = append(names, fmt.Sprintf("«%02d»", number))
		}
		return nil, fmt.Errorf("%s: в книге нет ожидаемых листов %s", rosstatIPCMesTable, strings.Join(names, ", "))
	}
	return table, nil
}

// ipcMesImport разбирает файл и пишет точки в batch: семантика прежнего разбора сохранена — имя
// ряда объявлено кодом (§6.8), дата — последний день отчётного месяца (ipcMesDate), значение —
// индекс к концу предыдущего месяца. Разбор идёт целиком до первой записи: ошибка листа оставляет
// батч пустым, а не пишет половину книги.
func ipcMesImport(xlsx *excelize.File, batch driver.Batch) error {
	table, err := parseIpcMes(xlsx)
	if err != nil {
		return err
	}
	for _, row := range table {
		if err = batch.Append(row.name, row.date, float32(row.value)); err != nil {
			return err
		}
	}
	return nil
}

// Import берёт адрес актуального xlsx со страницы цен Росстата: имени файла в коде нет
// (ipc_mes_ММ-ГГГГ.xlsx меняется при каждом обновлении), а сетевое обращение в init() запрещено
// (ARCHITECTURE.md §8.6). Ошибка обхода страницы не проглатывается (§4.4): иначе прогон дошёл бы
// до GetXlsx("") и упал невнятно. Дальше — полный цикл шаблона: DDL → префлайт по HEAD (файл
// обновляется раз в месяц, шаг расписания — недельный) → точки → производные (AfterImport) →
// строка версии.
func (s *IpcMesStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	xlsDataUrl, err := s.getXlsDataUrl()
	if err != nil {
		return count, err
	}
	s.DataUrl = xlsDataUrl
	return s.ClickHouseImport.Import(ctx, conn)
}

// ipcMesImporter — помесячные ИПЦ Росстата; регистрируется реестром rosstat/importers.go.
var ipcMesImporter = &IpcMesStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatIPCMesTable,
	CreateTable:            []string{ipcMesDdl},
	ImportFunc:             ipcMesImport,
	AfterImport:            afterRosstatMacro(ipcMesSeriesMeta),
	PublicationCountPoints: ipcMesCountPoints,
}}
