package rosstat

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

const (
	// ToDo update data source https://rosstat.gov.ru/statistics/price
	// Еженедельные индексы потребительских цен (тарифов) на отдельные товары и услуги по Российской
	rosstatIPCWeeksTable = "rosstat_ipc_weeks"
	// ipcWeeksDdl — DDL в %s-форме: имя таблицы подставляет шаблон util.ClickHouseImport
	// (fmt.Sprintf). Литеральное имя в DDL, как было, дублировало бы TableName — второй
	// источник истины о таблице, который расходится при переименовании.
	ipcWeeksDdl = `CREATE TABLE IF NOT EXISTS %s (
				  name LowCardinality(String)
				, date Date
				, percent Float32
			) ENGINE = ReplacingMergeTree ORDER BY (name, date);
		`
	// ipcWeeksField — якорь шапки листа года: строка с этой подписью в первой колонке.
	// Разметка ищется по признаку, а не по номеру строки (§6.3): сноски и вставленные
	// строки сдвигают лист, и жёсткий индекс прочитал бы не те строки.
	ipcWeeksField = "Наименование"
	// ipcWeeksYearStart — нижняя граница года листа: имя листа обязано быть числом не меньше
	// этого года. «Содержание» и прочие нечисловые листы рядами не являются и пропускаются.
	ipcWeeksYearStart = 1997
	// ipcWeeksLinkSelector — селектор ссылки на актуальный Nedel_ipc.xlsx: константа рядом с
	// импортёром, а не литерал в теле функции (§4.5) — тот же селектор проверяет тест на
	// HTML-фикстуре. Прежняя позиционная CSS-цепочка из номеров вложенных элементов
	// ломалась от любой правки вёрстки страницы; имя файла (Nedel_ipc.xlsx) постоянное,
	// поэтому селектор — по фрагменту href.
	ipcWeeksLinkSelector = `a[href*="Nedel_ipc"]`
	// ipcWeeksTimeLayout — макет даты точки: день и месяц приходят из подписи колонки
	// («на 12 января**»), год — из имени листа.
	ipcWeeksTimeLayout = "2006-1-2"
	// ipcWeeksNoData — подпись источника «данных не имеется»: так названа сноска листа
	// («… - данных не имеется»), и ею же помечены недели, по которым значений нет. Пустая
	// ячейка и эта подпись — не точка ряда (§6.7): прежний разбор писал по ним ноль. Точкой
	// ячейка не становится, но строку и лист не обрывает: пропуск данных у одного товара —
	// не конец таблицы, ниже стоят строки других товаров.
	ipcWeeksNoData = "…"
)

type IpcWeeksStat struct {
	util.ClickHouseImport
}

// ipcWeeksPoint — точка листа: товар, дата регистрации и значение недельного индекса.
// Значение остаётся float64 до записи в батч: сужение до Float32 делает колонка таблицы.
type ipcWeeksPoint struct {
	product string
	date    time.Time
	value   float64
}

// ipcWeeksKnownProducts — множество товаров, описанных в каталоге (ipcWeeksProducts,
// series_meta.go): по нему разбор отличает ожидаемое изменение источника (товар, которого в
// каталоге ещё нет) от сломанной выгрузки (§6.6). Множество выводится из того же списка, а не
// повторяется литералами: описания товаров остаются в одном месте.
var ipcWeeksKnownProducts = func() map[string]bool {
	known := make(map[string]bool, len(ipcWeeksProducts))
	for _, product := range ipcWeeksProducts {
		known[product] = true
	}
	return known
}()

// getXlsDataUrl ищет ссылку на актуальный Nedel_ipc.xlsx на странице цен Росстата.
// Транспорт — через util (§4.1), а ошибка не проглатывается (§4.4): без ссылки импорт
// обязан упасть до записи в БД, а не писать «0 строк» по устаревшему адресу (прежний
// транспорт логировал ошибку обхода и возвращал пустой url как успех).
func (s *IpcWeeksStat) getXlsDataUrl() (string, error) {
	pageUrl := fmt.Sprintf("%s/statistics/price", rosstatUrl)
	page, err := util.GetPage(pageUrl)
	if err != nil {
		return "", fmt.Errorf("ipcWeeks: %s: %w", pageUrl, err)
	}
	href, err := util.FindLink(page, ipcWeeksLinkSelector)
	if err != nil {
		return "", fmt.Errorf("ipcWeeks: ссылка на Nedel_ipc*.xlsx не найдена на %s: %w", pageUrl, err)
	}
	return rosstatUrl + href, nil
}

// ipcWeeksDate разбирает подпись колонки («на 12 января**», «26 декабря») в дату точки.
// Семантика прежнего разбора сохранена: «*» (метка первой колонки года) и префикс «на »
// отбрасываются, остаток делится по пробелу на день и месяц, месяц берётся из
// util.MonthsToNum. Незнакомый месяц теперь ошибка, а не месяц 0 (§6.4): нулевой месяц
// доходил до time.Parse и возвращался ошибкой даты, а не причиной — подписью колонки.
// Год берётся из имени листа.
func ipcWeeksDate(year int, label string) (time.Time, error) {
	parts := strings.Split(strings.ReplaceAll(strings.ReplaceAll(label, "*", ""), "на ", ""), " ")
	if len(parts) < 2 {
		return time.Time{}, fmt.Errorf("подпись колонки %q не «день месяц»", label)
	}
	day, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return time.Time{}, fmt.Errorf("день %q подписи колонки %q не число: %w", parts[0], label, err)
	}
	month, ok := util.MonthsToNum[strings.ToLower(strings.TrimSpace(parts[1]))]
	if !ok {
		return time.Time{}, fmt.Errorf("месяц %q подписи колонки %q не распознан", parts[1], label)
	}
	date, err := time.Parse(ipcWeeksTimeLayout, fmt.Sprintf("%d-%d-%d", year, int(month), day))
	if err != nil {
		return time.Time{}, fmt.Errorf("подпись колонки %q: %w", label, err)
	}
	return date, nil
}

// ipcWeeksHeaderDates разбирает шапку листа в даты колонок: dates[j] — дата колонки j,
// длина среза совпадает с шапкой. Шапка разбирается целиком до чтения строк данных: сломанная
// подпись — ошибка (§6.4) и без значений, а не паника на разборе подписи посреди книги (§6.5).
// Ячейка данных за последней колонкой шапки даты не получает и точкой не становится.
func ipcWeeksHeaderDates(sheet string, year int, header []string) ([]time.Time, error) {
	dates := make([]time.Time, len(header))
	for j := 1; j < len(header); j++ {
		date, err := ipcWeeksDate(year, header[j])
		if err != nil {
			return nil, fmt.Errorf("%s: лист %q, колонка %d: %w", rosstatIPCWeeksTable, sheet, j+1, err)
		}
		dates[j] = date
	}
	return dates, nil
}

// ipcWeeksValue разбирает ячейку значения. Ячейка приходит сырым текстом (§6.2); ошибка
// разбора не проглатывается — прежний разбор делал `percent, _ := strconv.ParseFloat(...)`
// и писал по нечисловой ячейке ноль (в живой витрине таких нулевых точек было ровно 28).
// NaN и бесконечность ParseFloat принимает, а точкой ряда быть не могут (§6.4).
func ipcWeeksValue(cell string) (float64, error) {
	value, err := strconv.ParseFloat(cell, 64)
	if err != nil {
		return 0, fmt.Errorf("значение ячейки %q не число: %w", cell, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение ячейки %q не конечное число", cell)
	}
	return value, nil
}

// ipcWeeksImport разбирает книгу и пишет точки в batch. Разбор идёт целиком до первой записи:
// ошибка любого листа оставляет батч пустым, а не пишет половину книги.
//
// Товар вне каталога — предупреждение и импорт (§6.6): набор товаров меняется между
// выгрузками Росстата, и падение недельного шага (он первый в цепочке dagu/rosstat.yaml)
// лишило бы данных все последующие шаги (§12.3). Предупреждение выводится один раз на товар,
// а не на каждую его точку: у недельного ряда точек сотни.
func ipcWeeksImport(xlsx *excelize.File, batch driver.Batch) error {
	var points []ipcWeeksPoint
	warned := make(map[string]bool)
	for _, sheet := range xlsx.GetSheetList() {
		year, err := strconv.Atoi(sheet)
		if err != nil || year < ipcWeeksYearStart {
			continue // «Содержание» и прочие нечисловые листы рядами не являются
		}
		// Сырые значения ячеек (§6.2): форматированная строка отдала бы представление, а не
		// данные — формат выгрузки `0.00` превратил бы 100.126 в «100.13».
		rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
		if err != nil {
			return fmt.Errorf("%s: лист %q: %w", rosstatIPCWeeksTable, sheet, err)
		}
		fieldIdx := -1
		for i, row := range rows {
			if len(row) > 0 && strings.TrimSpace(row[0]) == ipcWeeksField {
				fieldIdx = i
				break
			}
		}
		if fieldIdx < 0 {
			return fmt.Errorf("%s: лист %q: строка «%s» не найдена", rosstatIPCWeeksTable, sheet, ipcWeeksField)
		}
		dates, err := ipcWeeksHeaderDates(sheet, year, rows[fieldIdx])
		if err != nil {
			return err
		}
		for _, row := range rows[fieldIdx+1:] {
			// Терминатор таблицы листа — строка-сноска: у неё заполнена одна ячейка
			// («* Без учета…», «** В % к концу предыдущего года», «… - данных не имеется»),
			// поэтому признак конца — строка короче двух ячеек. Пустой первый столбец —
			// не строка таблицы: точки ушли бы в ряд с пустым именем, которого каталог не
			// описывает (§10.6). Нечисловая ячейка значения терминатором не является:
			// источник помечает «…» пропуск данных у отдельного товара, и строки ниже
			// остаются таблицей (обрыв по ней терял и товар, и его значения).
			if len(row) < 2 || strings.TrimSpace(row[0]) == "" {
				break
			}
			product := strings.TrimSpace(row[0])
			if !ipcWeeksKnownProducts[product] && !warned[product] {
				warned[product] = true
				log.Warnf("%s: товар %q не описан в ipcWeeksProducts — точка записана, описание товара добавить в каталог", rosstatIPCWeeksTable, product)
			}
			// Ячейки за последней колонкой шапки даты не описывает, и точкой они не
			// становятся: прежний разбор индексировал шапку без проверки границы
			// (rows[fieldIdx][j+1]) и падал паникой на строке длиннее шапки (§6.5).
			for j := 1; j < len(row) && j < len(dates); j++ {
				cell := strings.TrimSpace(row[j])
				if cell == "" || cell == ipcWeeksNoData {
					continue // нет данных: недели, по которым значений не публиковалось (§6.7)
				}
				value, err := ipcWeeksValue(cell)
				if err != nil {
					return fmt.Errorf("%s: лист %q, %s, товар %q: %w",
						rosstatIPCWeeksTable, sheet, dates[j].Format("2006-01-02"), product, err)
				}
				points = append(points, ipcWeeksPoint{product: product, date: dates[j], value: value})
			}
		}
	}
	if len(points) == 0 {
		return fmt.Errorf("%s: разбор дал 0 точек — в книге нет листов года с данными (строка «%s» не найдена или выгрузка пуста)", rosstatIPCWeeksTable, ipcWeeksField)
	}
	for _, point := range points {
		if err := batch.Append(point.product, point.date, float32(point.value)); err != nil {
			return err
		}
	}
	return nil
}

// Import берёт адрес актуального Nedel_ipc.xlsx со страницы цен Росстата: имени файла в коде
// нет (ссылка берётся со страницы), а сетевое обращение в init() запрещено
// (ARCHITECTURE.md §8.6). Дальше — полный цикл шаблона: DDL → точки → производные
// (AfterImport: каталог рядов и витрина v_rosstat_macro) → строка версии. Префлайта нет:
// файл перезаписывается по тому же URL в недельном темпе, равном темпу шага расписания,
// поэтому no-op недостижим (§9.3), а HEAD был бы чистыми накладными расходами.
func (s *IpcWeeksStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	xlsDataUrl, err := s.getXlsDataUrl()
	if err != nil {
		return count, err
	}
	s.DataUrl = xlsDataUrl
	return s.ClickHouseImport.Import(ctx, conn)
}

// ipcWeeksImporter — еженедельные индексы цен Росстата; регистрируется реестром
// rosstat/importers.go. PublicationCountPoints и PublicationSource не заполняются (§9.3):
// у недельного файла точке контроля публикации места нет, и реестр держит для него пустой
// countPoints — расхождение поймал бы тест реестра.
var ipcWeeksImporter = &IpcWeeksStat{ClickHouseImport: util.ClickHouseImport{
	TableName:   rosstatIPCWeeksTable,
	CreateTable: []string{ipcWeeksDdl},
	ImportFunc:  ipcWeeksImport,
	AfterImport: afterRosstatMacro(ipcWeeksSeriesMeta()),
}}
