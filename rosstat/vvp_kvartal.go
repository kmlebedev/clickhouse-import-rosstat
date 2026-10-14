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

// getQuarterDate переводит подпись квартала в дату точки — 1-е число месяца, следующего за
// отчётным (I → 1 апреля, II → 1 июля, III → 1 октября, IV → 1 января следующего года).
// Семантика прежнего разбора сохранена дословно; изменён только отказ: незнакомая подпись —
// ошибка, а не panic, который убивал весь прогон импортёра.
func getQuarterDate(year int, quarter string) (time.Time, error) {
	var startMonth time.Month
	switch quarter {
	case "I":
		startMonth = time.January
	case "II":
		startMonth = time.April
	case "III":
		startMonth = time.July
	case "IV":
		startMonth = time.October
	default:
		return time.Time{}, fmt.Errorf("незнакомая подпись квартала %q", quarter)
	}
	return time.Date(year, startMonth+3, 1, 0, 0, 0, 0, time.UTC), nil
}

const (
	// Национальные счета https://rosstat.gov.ru/statistics/accounts
	// ВВП кварталы (с 1995 г.) — имя файла содержит диапазон лет (VVP_kvartal_s_1995-2025.xlsx)
	// и меняется при обновлении, поэтому ссылка берётся со страницы.
	// Валовой внутренний продукт 1) (в ценах 2021 г., млрд руб., с исключением сезонного фактора)
	rosstatVVPKvartalTable = "rosstat_vvp_kvartal"
	vvpKvartalDdl          = `CREATE TABLE IF NOT EXISTS %s (
				  name LowCardinality(String)
				, date Date
				, vvp Float32
			) ENGINE = ReplacingMergeTree ORDER BY (name, date);
		`
	// vvpKvartalLinkSelector — селектор ссылки на актуальный xlsx: константа рядом с
	// импортёром, а не литерал в теле функции (§4.5) — тот же селектор проверяет тест на
	// HTML-фикстуре страницы национальных счетов.
	vvpKvartalLinkSelector = `a[href*="VVP_kvartal"]`
	// vvpKvartalCountPoints — количественный чек версии (спека §3.5): число точек своего
	// ряда. Литерал, как и таблица: запрос — часть утверждения «эти точки мои». FINAL
	// обязателен — иначе count() считал бы несмёрженные части.
	vvpKvartalCountPoints = "SELECT count() FROM rosstat_vvp_kvartal FINAL"
)

// vvpKvartalSheetNames — порядок обхода листов. Именно слайс, а не ключи карты имён: обход
// карты в Go случаен, и порядок записи точек менялся бы от прогона к прогону (§6.8).
var vvpKvartalSheetNames = []string{"2", "9", "10", "12", "14"}

// vvpKvartalSeriesNames — имя ряда для листа, объявленное кодом (§6.8). Заголовок листа (A2)
// в имени не участвует: он несёт сноски и редакции выгрузки («1)», «в текущих ценах», «в
// ценах 2021г.») и на первой же редакции файла оборвал бы ряд. Значения совпадают с
// vvpKvartalSeriesMeta[].Series дословно, включая опечатки источника: «внутрений» на листах
// «2» и «9», «внутренний» на листе «10» — эти строки и есть тождество ряда в каталоге.
var vvpKvartalSeriesNames = map[string]string{
	"2":  "2 Валовой внутрений продукт",
	"9":  "9 Валовой внутрений продукт",
	"10": "10 Валовой внутренний продукт",
	"12": "12 Индексы физического объема валового внутреннего продукта",
	"14": "14 Индексы - дефляторы валового внутреннего продукта",
}

// getVvpKvartalXlsDataUrl возвращает адрес актуального VVP_kvartal_*.xlsx со страницы
// национальных счетов Росстата: имя файла содержит диапазон лет (VVP_kvartal_s-1995-ГГГГ.xlsx)
// и меняется при обновлении, поэтому ссылка берётся со страницы. Транспорт — через util (§4.1),
// а ошибка не проглатывается (§4.4): без ссылки импорт обязан упасть до записи в БД, а не
// писать «0 строк» по устаревшему адресу.
func getVvpKvartalXlsDataUrl() (string, error) {
	pageUrl := fmt.Sprintf("%s/statistics/accounts", rosstatUrl)
	page, err := util.GetPage(pageUrl)
	if err != nil {
		return "", fmt.Errorf("vvpKvartal: %s: %w", pageUrl, err)
	}
	href, err := util.FindLink(page, vvpKvartalLinkSelector)
	if err != nil {
		return "", fmt.Errorf("vvpKvartal: ссылка на VVP_kvartal_*.xlsx не найдена на %s: %w", pageUrl, err)
	}
	return rosstatUrl + href, nil
}

type vvpKvartalDdlStat struct {
	util.ClickHouseImport
}

type vvpKvartal struct {
	name string
	date time.Time
	vvp  float64
}

// vvpKvartalPoint — точка листа: дата записи и сырое значение выгрузки. Значение остаётся
// float64 до записи в батч: сужение до Float32 делает шаблон (колонка таблицы), поэтому тест
// на сырое значение ячейки (§6.2) видит значение целиком.
type vvpKvartalPoint struct {
	date  time.Time
	value float64
}

// vvpKvartalIsLabelRow — строка подписей кварталов узнаётся по содержимому, а не по номеру:
// первая ячейка начинается с «I », «II », «III » или «IV » и содержит «квартал». Номер строки в
// выгрузке плавает (сноски, вставленные строки), и жёсткий индекс — ровно тот дефект, из-за
// которого прежний разбор читал не те строки.
func vvpKvartalIsLabelRow(row []string) bool {
	if len(row) == 0 {
		return false
	}
	cell := strings.TrimSpace(row[0])
	if !strings.Contains(cell, "квартал") {
		return false
	}
	for _, prefix := range []string{"I ", "II ", "III ", "IV "} {
		if strings.HasPrefix(cell, prefix) {
			return true
		}
	}
	return false
}

// vvpKvartalYear разбирает ячейку года: год записан первыми четырьмя цифрами, у поздних групп
// со сноской («20262)»). Мусор в ячейке года — ошибка (§6.4), а не молчаливый ноль: год 0 дал
// бы дату «0000-01-01», которую ClickHouse отвергает.
func vvpKvartalYear(cell string) (int, error) {
	cell = strings.TrimSpace(cell)
	if len(cell) < 4 {
		return 0, fmt.Errorf("ячейка года %q короче четырёх цифр", cell)
	}
	year, err := strconv.Atoi(cell[:4])
	if err != nil {
		return 0, fmt.Errorf("ячейка года %q не число: %w", cell, err)
	}
	return year, nil
}

// vvpKvartalValue разбирает ячейку данных. Ячейка приходит сырым текстом (§6.2), поэтому
// значение берётся как есть — «13024.799539024814», а не формат ячейки «13,024.8». NaN и
// бесконечность отбрасываются, как и в соседнем rosstat_salaries_mes: ParseFloat их принимает,
// и мусор уехал бы в витрину точкой ряда.
func vvpKvartalValue(cell string) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(cell), 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q не число: %w", cell, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение %q не конечное число", cell)
	}
	return value, nil
}

// parseVvpKvartalSheet читает один лист книги. Разметка листа («2», «9», «10», «12», «14»):
// A2 — заголовок листа (объединённая A2:D2, то есть строка из одной ячейки; в имени ряда он не
// участвует), ниже — строка годов в колонках A, E, I, … (первая колонка каждой группы
// кварталов), ниже — строка подписей «I квартал»…«IV квартал», под ними — строка данных; данные
// и подписи начинаются с колонки A.
func parseVvpKvartalSheet(xlsx *excelize.File, sheet string) ([]vvpKvartalPoint, error) {
	// Сырые значения ячеек (§6.2): форматированная строка отдала бы «13,024.8» вместо
	// «13024.799539024814», и ряд потерял бы точность значений.
	rows, err := xlsx.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		// Отсутствие ожидаемого листа — ошибка с именем листа (§6.4), а не «0 точек»: книга без
		// листа означала бы оборванный ряд, который в витрине выглядит полным.
		return nil, fmt.Errorf("%s: лист %q: %w", rosstatVVPKvartalTable, sheet, err)
	}
	labelRow := -1
	for i, row := range rows {
		if vvpKvartalIsLabelRow(row) {
			labelRow = i
			break
		}
	}
	if labelRow < 0 {
		return nil, fmt.Errorf("%s: лист %q: строка подписей кварталов не найдена", rosstatVVPKvartalTable, sheet)
	}
	// Годы — строкой выше подписей, данные — строкой ниже. Проверка границ обязательна: выход за
	// срез — это паника, а разбор обязан отдавать ошибку.
	if labelRow == 0 {
		return nil, fmt.Errorf("%s: лист %q: строки годов нет над подписями кварталов (строка %d)",
			rosstatVVPKvartalTable, sheet, labelRow+1)
	}
	if labelRow+1 >= len(rows) {
		return nil, fmt.Errorf("%s: лист %q: строки данных нет под подписями кварталов (строка %d)",
			rosstatVVPKvartalTable, sheet, labelRow+1)
	}
	years, labels, data := rows[labelRow-1], rows[labelRow], rows[labelRow+1]
	var points []vvpKvartalPoint
	year := 0
	// Обход колонок строго с нуля и до длины среза данных: таблица листа начинается с колонки A,
	// и пропуск нулевой колонки терял бы I квартал каждой группы — первую точку ряда
	// (2011-04-01 на листах «2», «9», «10»). Пустая ячейка замыкает таблицу: excelize обрезает
	// хвостовые пустые ячейки, поэтому индекс за пределами data гарантированно пуст.
	for i := 0; i < len(data); i++ {
		cell := data[i]
		if strings.TrimSpace(cell) == "" {
			break
		}
		// Год выписан в первой колонке группы (A, E, I, …), в колонках II–IV кварталов ячейка
		// годов пуста: непустая ячейка задаёт текущий год, пустые его переносят. Без переноса
		// у II–IV кварталов года не было бы вовсе (год 0 → дата «0000-01-01»).
		if i < len(years) && strings.TrimSpace(years[i]) != "" {
			labelYear, err := vvpKvartalYear(years[i])
			if err != nil {
				return nil, fmt.Errorf("%s: лист %q, колонка %d: %w", rosstatVVPKvartalTable, sheet, i+1, err)
			}
			// Метка года не больше текущего года — это повтор (или откат) метки, и год выводится
			// из структуры таблицы, а не угадывается: группы кварталов идут I→IV и продолжаются
			// I следующего года, поэтому колонка под повторившейся меткой — I квартал следующего
			// года. Так помечен лист «14» выгрузки: на его последней группе стоит «20252)» (то же
			// значение, что у предыдущей группы), тогда как листы «2», «9», «10», «12» того же
			// выпуска Росстата на этой колонке несут «20262)». Принять метку на веру нельзя:
			// значения 2026-I и 2026-II записались бы датами 2025-I и 2025-II и затёрли настоящие
			// значения 2025 года — в таблице с ключом (name, date) это тихая подмена данных, а не
			// пропуск. Когда источник исправит метку, вывод совпадёт с ней и предупреждение
			// исчезнет само. Нулевая метка (year == 0, года ещё не было) выводу не подлежит:
			// пустой год остаётся ошибкой «год данных не задан» ниже.
			if year != 0 && labelYear <= year {
				log.Warnf("vvpKvartal: %s: лист %q, колонка %d: метка года %q не больше текущего %d — год выведен как %d",
					rosstatVVPKvartalTable, sheet, i+1, strings.TrimSpace(years[i]), year, year+1)
				labelYear = year + 1
			}
			year = labelYear
		}
		if year == 0 {
			return nil, fmt.Errorf("%s: лист %q, колонка %d: год данных не задан", rosstatVVPKvartalTable, sheet, i+1)
		}
		if i >= len(labels) {
			return nil, fmt.Errorf("%s: лист %q, колонка %d: подписи квартала нет", rosstatVVPKvartalTable, sheet, i+1)
		}
		fields := strings.Fields(labels[i])
		if len(fields) == 0 {
			return nil, fmt.Errorf("%s: лист %q, колонка %d: подпись квартала пуста", rosstatVVPKvartalTable, sheet, i+1)
		}
		date, err := getQuarterDate(year, fields[0])
		if err != nil {
			return nil, fmt.Errorf("%s: лист %q, колонка %d: %w", rosstatVVPKvartalTable, sheet, i+1, err)
		}
		value, err := vvpKvartalValue(cell)
		if err != nil {
			return nil, fmt.Errorf("%s: лист %q, колонка %d: %w", rosstatVVPKvartalTable, sheet, i+1, err)
		}
		points = append(points, vvpKvartalPoint{date: date, value: value})
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("%s: лист %q: разбор дал 0 точек", rosstatVVPKvartalTable, sheet)
	}
	return points, nil
}

// parseVvpKvartal читает всю книгу и отдаёт плоскую таблицу точек: имя ряда — из
// vvpKvartalSeriesNames (§6.8), листы обходятся в порядке слайса. Разбор идёт целиком до первой
// записи в батч: ошибка любого листа оставляет батч пустым, а не записывает половину книги.
func parseVvpKvartal(xlsx *excelize.File) ([]vvpKvartal, error) {
	var table []vvpKvartal
	for _, sheet := range vvpKvartalSheetNames {
		name, ok := vvpKvartalSeriesNames[sheet]
		if !ok {
			return nil, fmt.Errorf("%s: лист %q не описан в vvpKvartalSeriesNames", rosstatVVPKvartalTable, sheet)
		}
		points, err := parseVvpKvartalSheet(xlsx, sheet)
		if err != nil {
			return nil, err
		}
		for _, point := range points {
			table = append(table, vvpKvartal{name: name, date: point.date, vvp: point.value})
		}
	}
	return table, nil
}

// vvpKvartalImport разбирает файл и пишет точки в batch: семантика прежнего разбора сохранена —
// имя ряда объявлено кодом (§6.8), дата — 1-е число месяца, следующего за отчётным кварталом
// (getQuarterDate), значение — ВВП или индекс квартала.
func vvpKvartalImport(xlsx *excelize.File, batch driver.Batch) error {
	table, err := parseVvpKvartal(xlsx)
	if err != nil {
		return err
	}
	for _, r := range table {
		if err = batch.Append(r.name, r.date, float32(r.vvp)); err != nil {
			return err
		}
	}
	return nil
}

// Import берёт адрес актуального xlsx со страницы национальных счётов Росстата: имя файла
// содержит диапазон лет (VVP_kvartal_s-1995-ГГГГ.xlsx) и меняется при обновлении, а сетевое
// обращение в init() запрещено (ARCHITECTURE.md §8.6). Дальше — полный цикл шаблона: DDL →
// префлайт по HEAD (ряд обновляется раз в квартал, шаг расписания — недельный) → точки →
// производные (AfterImport) → строка версии.
func (s *vvpKvartalDdlStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	xlsDataUrl, err := getVvpKvartalXlsDataUrl()
	if err != nil {
		return count, err
	}
	s.DataUrl = xlsDataUrl
	return s.ClickHouseImport.Import(ctx, conn)
}

// vvpKvartalImporter — квартальный ВВП РФ; регистрируется реестром rosstat/importers.go.
var vvpKvartalImporter = &vvpKvartalDdlStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              rosstatVVPKvartalTable,
	CreateTable:            []string{vvpKvartalDdl},
	ImportFunc:             vvpKvartalImport,
	AfterImport:            afterRosstatMacro(vvpKvartalSeriesMeta),
	PublicationCountPoints: vvpKvartalCountPoints,
}}
